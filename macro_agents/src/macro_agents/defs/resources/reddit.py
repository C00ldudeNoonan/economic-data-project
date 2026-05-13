"""Reddit web scraping resource with aggressive rate limiting."""

import random
import re
import time
from datetime import datetime, timezone

import polars as pl
import requests
from bs4 import BeautifulSoup, Tag
from dagster import ConfigurableResource
from pydantic import Field
from tenacity import retry, stop_after_attempt, wait_exponential


class RedditResource(ConfigurableResource):
    """Resource for scraping Reddit with aggressive rate limiting to stay under the radar."""

    # Conservative delays - more conservative than Fed scraping
    min_delay: float = Field(
        default=8.0, description="Minimum delay between requests (seconds)"
    )
    max_delay: float = Field(
        default=20.0, description="Maximum delay between requests (seconds)"
    )

    # User agent rotation to appear as different browsers
    user_agents: list[str] = Field(
        default_factory=lambda: [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ]
    )

    @staticmethod
    def _get_attr_str(tag: Tag, key: str, default: str = "") -> str:
        value = tag.get(key)
        if isinstance(value, list):
            return value[0] if value else default
        if value is None:
            return default
        return str(value)

    @classmethod
    def _get_attr_int(cls, tag: Tag, key: str, default: int = 0) -> int:
        value = cls._get_attr_str(tag, key, str(default))
        try:
            return int(value)
        except ValueError:
            return default

    def _random_delay(self):
        """Introduce random delay between min_delay and max_delay to appear human."""
        delay = random.uniform(self.min_delay, self.max_delay)
        time.sleep(delay)

    def _get_headers(self):
        """Get realistic headers with rotated user agent to appear human."""
        return {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Referer": "https://old.reddit.com/",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
        }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=30, max=300))
    def get_top_posts(
        self,
        subreddit: str,
        time_filter: str = "day",
        limit: int = 100,
        context=None,
    ) -> pl.DataFrame:
        """
        Scrape top posts from a subreddit with rate limiting.

        Args:
            subreddit: Subreddit name (without r/ prefix)
            time_filter: Time filter (hour, day, week, month, year, all)
            limit: Maximum number of posts to fetch
            context: Dagster execution context for logging

        Returns:
            Polars DataFrame with post data
        """
        # Random delay before request
        self._random_delay()

        url = f"https://old.reddit.com/r/{subreddit}/top/?t={time_filter}&limit={limit}"

        if context:
            context.log.debug(f"Scraping {url}")

        response = requests.get(url, headers=self._get_headers(), timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "lxml")
        posts_data = []

        # Parse post listings from old.reddit.com
        # Each post is in a div with class 'thing' and data attributes
        for thing in soup.find_all("div", class_="thing"):
            try:
                # Skip promoted/ad posts (Reddit ads appear as u_* subreddits)
                thing_subreddit = self._get_attr_str(thing, "data-subreddit")
                if thing_subreddit.startswith("u_"):
                    continue

                post_data = self._parse_post(thing, subreddit)
                if post_data:
                    posts_data.append(post_data)
            except Exception as e:
                if context:
                    context.log.warning(f"Failed to parse post: {e}")
                continue

        if context:
            context.log.debug(f"Scraped {len(posts_data)} posts from r/{subreddit}")

        # Return empty DataFrame with correct schema if no posts found
        if not posts_data:
            return pl.DataFrame(
                schema={
                    "post_id": pl.Utf8,
                    "title": pl.Utf8,
                    "score": pl.Int64,
                    "num_comments": pl.Int64,
                    "created_utc": pl.Datetime,
                    "author": pl.Utf8,
                    "url": pl.Utf8,
                    "permalink": pl.Utf8,
                    "subreddit": pl.Utf8,
                    "domain": pl.Utf8,
                }
            )

        return pl.DataFrame(posts_data)

    def _parse_post(self, thing, subreddit: str) -> dict | None:
        """Parse a single post from the 'thing' div element."""
        # Extract post ID from data-fullname (e.g., "t3_abc123" -> "abc123")
        fullname = thing.get("data-fullname", "")
        post_id = fullname.replace("t3_", "") if fullname else ""
        if not post_id:
            return None

        # Extract title
        title_elem = thing.find("a", class_="title")
        title = title_elem.text.strip() if title_elem else ""

        # Extract score safely
        try:
            score = int(thing.get("data-score", 0))
        except (ValueError, TypeError):
            score = 0

        # Extract comment count
        num_comments = self._extract_num_comments(thing)

        # Extract timestamp
        created_utc = self._extract_timestamp(thing)

        # Extract author — treat [deleted] and [removed] as deleted
        author = thing.get("data-author", "[deleted]") or "[deleted]"

        # Extract URL
        url = thing.get("data-url", "") or ""

        # Extract permalink
        permalink = thing.get("data-permalink", "") or ""

        # Extract domain
        domain = thing.get("data-domain", "") or ""

        # Get subreddit from data attribute or use provided subreddit
        # Normalize to lowercase for consistent querying (Reddit returns display casing)
        post_subreddit = (thing.get("data-subreddit", subreddit) or subreddit).lower()

        return {
            "post_id": post_id,
            "title": title,
            "score": score,
            "num_comments": num_comments,
            "created_utc": created_utc,
            "author": author,
            "url": url,
            "permalink": permalink,
            "subreddit": post_subreddit,
            "domain": domain,
        }

    def _extract_num_comments(self, thing) -> int:
        """Extract comment count from post."""
        comments_elem = thing.find("a", class_="comments")
        if not comments_elem:
            return 0

        text = comments_elem.text
        # Extract number from "123 comments" or "1 comment"
        match = re.search(r"(\d+)", text)
        return int(match.group(1)) if match else 0

    def _extract_timestamp(self, thing) -> datetime:
        """Extract creation timestamp from post."""
        time_elem = thing.find("time")
        if time_elem and time_elem.has_attr("datetime"):
            try:
                # Parse ISO format timestamp
                timestamp_str = time_elem["datetime"]
                # Handle both Unix timestamp and ISO format
                if timestamp_str.replace(".", "").isdigit():
                    # Unix timestamp
                    return datetime.fromtimestamp(float(timestamp_str), tz=timezone.utc)
                else:
                    # ISO format
                    return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            except (ValueError, KeyError):
                pass

        # Fallback: try to find Unix timestamp in data attributes
        timestamp = thing.get("data-timestamp")
        if timestamp:
            try:
                return datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc)
            except (ValueError, TypeError):
                pass

        # Last resort: return current time
        return datetime.now(timezone.utc)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=30, max=300))
    def get_post_comments(
        self,
        permalink: str,
        limit: int = 500,
        context=None,
    ) -> pl.DataFrame:
        """
        Scrape all comments from a Reddit post.

        Args:
            permalink: The post permalink (e.g., "/r/economics/comments/abc123/title/")
            limit: Maximum number of comments to fetch (Reddit limits apply)
            context: Dagster execution context for logging

        Returns:
            Polars DataFrame with comment data including:
            - comment_id, author, body, score, created_utc, parent_id, depth, links
        """
        self._random_delay()

        # Build full URL - use ?limit=500 to get more comments
        url = f"https://old.reddit.com{permalink}?limit={limit}"

        if context:
            context.log.debug(f"Scraping comments from {url}")

        response = requests.get(url, headers=self._get_headers(), timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "lxml")
        comments_data = []

        # Parse comment area - comments are in divs with class 'comment'
        comment_area = soup.find("div", class_="commentarea")
        if isinstance(comment_area, Tag):
            for comment in comment_area.find_all("div", class_="thing", recursive=True):
                if not isinstance(comment, Tag):
                    continue
                # Skip non-comment things (like "load more" elements)
                thing_id = self._get_attr_str(comment, "data-fullname")
                if not thing_id.startswith("t1_"):
                    continue

                try:
                    comment_data = self._parse_comment(comment)
                    if comment_data:
                        comments_data.append(comment_data)
                except Exception as e:
                    if context:
                        context.log.warning(f"Failed to parse comment: {e}")
                    continue

        if context:
            context.log.debug(f"Scraped {len(comments_data)} comments")

        # Return empty DataFrame with correct schema if no comments found
        if not comments_data:
            return pl.DataFrame(
                schema={
                    "comment_id": pl.Utf8,
                    "author": pl.Utf8,
                    "body": pl.Utf8,
                    "score": pl.Int64,
                    "created_utc": pl.Datetime,
                    "parent_id": pl.Utf8,
                    "depth": pl.Int64,
                    "links": pl.List(pl.Utf8),
                }
            )

        return pl.DataFrame(comments_data)

    def _parse_comment(self, comment: Tag) -> dict | None:
        """Parse a single comment from the comment div element."""
        # Extract comment ID from data-fullname (e.g., "t1_abc123" -> "abc123")
        fullname = self._get_attr_str(comment, "data-fullname")
        comment_id = fullname.replace("t1_", "") if fullname else ""
        if not comment_id:
            return None

        # Extract author — normalize deleted/removed
        author = self._get_attr_str(comment, "data-author", "[deleted]") or "[deleted]"

        # Extract body text — handle deleted/removed comments
        body = ""
        body_elem = comment.find("div", class_="md")
        if isinstance(body_elem, Tag):
            body = body_elem.get_text(separator=" ", strip=True)
        # Treat "[deleted]" and "[removed]" body content as empty
        if body in ("[deleted]", "[removed]"):
            body = ""

        # Extract links from comment body
        links = []
        if isinstance(body_elem, Tag):
            for link in body_elem.find_all("a", href=True):
                if not isinstance(link, Tag):
                    continue
                href = self._get_attr_str(link, "href")
                # Filter out reddit internal links and anchors
                if href and not href.startswith("#") and not href.startswith("/"):
                    links.append(href)

        # Extract score
        score_elem = comment.find("span", class_="score")
        score = 0
        if isinstance(score_elem, Tag):
            score_text = self._get_attr_str(score_elem, "title", "0")
            try:
                score = int(score_text)
            except ValueError:
                # Try parsing from text like "5 points"
                match = re.search(r"(-?\d+)", score_elem.text)
                score = int(match.group(1)) if match else 0

        # Extract timestamp
        created_utc = self._extract_timestamp(comment)

        # Extract parent ID
        parent_id = self._get_attr_str(comment, "data-parent-fullname")

        # Calculate depth from nesting
        depth = 0
        parent = comment.parent
        while isinstance(parent, Tag):
            class_value = parent.get("class")
            if isinstance(class_value, list):
                has_thing = "thing" in class_value
            elif isinstance(class_value, str):
                has_thing = "thing" in class_value.split()
            else:
                has_thing = False

            if parent.name == "div" and has_thing:
                depth += 1
            parent = parent.parent

        return {
            "comment_id": comment_id,
            "author": author,
            "body": body,
            "score": score,
            "created_utc": created_utc,
            "parent_id": parent_id,
            "depth": depth,
            "links": links,
        }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=30, max=300))
    def get_post_content(
        self,
        permalink: str,
        context=None,
    ) -> dict:
        """
        Fetch full post content including selftext and links.

        Args:
            permalink: The post permalink (e.g., "/r/economics/comments/abc123/title/")
            context: Dagster execution context for logging

        Returns:
            Dict with post content: post_id, title, selftext, links, author, score, etc.
        """
        self._random_delay()

        url = f"https://old.reddit.com{permalink}"

        if context:
            context.log.debug(f"Fetching post content from {url}")

        response = requests.get(url, headers=self._get_headers(), timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "lxml")

        # Find the main post (first 'thing' with t3_ prefix)
        post = soup.find(
            "div", class_="thing", attrs={"data-fullname": re.compile(r"^t3_")}
        )

        if not isinstance(post, Tag):
            return {
                "post_id": "",
                "title": "",
                "selftext": "",
                "links": [],
                "author": "",
                "score": 0,
                "url": "",
                "created_utc": datetime.now(timezone.utc),
            }

        post_id = self._get_attr_str(post, "data-fullname").replace("t3_", "")
        title = ""
        title_elem = post.find("a", class_="title")
        if title_elem:
            title = title_elem.text.strip()

        # Extract selftext (for text posts) — handle deleted/removed
        selftext = ""
        selftext_elem = soup.find("div", class_="usertext-body")
        if isinstance(selftext_elem, Tag):
            md_elem = selftext_elem.find("div", class_="md")
            if isinstance(md_elem, Tag):
                selftext = md_elem.get_text(separator="\n", strip=True)
        if selftext in ("[deleted]", "[removed]"):
            selftext = ""

        # Extract all links from the post
        links = []
        # Link from URL field (for link posts)
        post_url = self._get_attr_str(post, "data-url")
        if post_url and not post_url.startswith("/r/"):
            links.append(post_url)

        # Links from selftext
        if isinstance(selftext_elem, Tag):
            md_elem = selftext_elem.find("div", class_="md")
            if isinstance(md_elem, Tag):
                for link in md_elem.find_all("a", href=True):
                    if not isinstance(link, Tag):
                        continue
                    href = self._get_attr_str(link, "href")
                    if href and not href.startswith("#") and not href.startswith("/"):
                        if href not in links:
                            links.append(href)

        return {
            "post_id": post_id,
            "title": title,
            "selftext": selftext,
            "links": links,
            "author": self._get_attr_str(post, "data-author", "[deleted]"),
            "score": self._get_attr_int(post, "data-score", 0),
            "url": post_url,
            "created_utc": self._extract_timestamp(post),
        }
