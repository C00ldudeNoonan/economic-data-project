"""Unit tests for RedditResource."""

from datetime import datetime
from unittest.mock import Mock, patch

import polars as pl
from macro_agents.defs.resources.reddit import RedditResource


class TestRedditResource:
    """Test cases for RedditResource."""

    def test_initialization(self):
        """Test resource initialization with default values."""
        resource = RedditResource()

        assert resource.min_delay == 8.0
        assert resource.max_delay == 20.0
        assert len(resource.user_agents) == 3

    def test_custom_delays(self):
        """Test resource initialization with custom delay values."""
        resource = RedditResource(min_delay=15.0, max_delay=30.0)

        assert resource.min_delay == 15.0
        assert resource.max_delay == 30.0

    def test_get_headers(self):
        """Test headers generation with user agent rotation and Referer."""
        resource = RedditResource()

        headers1 = resource._get_headers()

        # Check required headers are present
        assert "User-Agent" in headers1
        assert "Accept" in headers1
        assert "DNT" in headers1
        assert "Referer" in headers1
        assert headers1["DNT"] == "1"
        assert headers1["Referer"] == "https://old.reddit.com/"

        # User agent should be one of the configured ones
        assert headers1["User-Agent"] in resource.user_agents

    @patch("macro_agents.defs.resources.reddit.time.sleep")
    def test_random_delay(self, mock_sleep):
        """Test that random delay is called with appropriate range."""
        resource = RedditResource(min_delay=8.0, max_delay=20.0)

        resource._random_delay()

        # Check sleep was called once
        assert mock_sleep.call_count == 1

        # Check delay is in expected range
        delay = mock_sleep.call_args[0][0]
        assert 8.0 <= delay <= 20.0

    def test_extract_num_comments(self):
        """Test extracting comment count from post element."""
        from bs4 import BeautifulSoup

        resource = RedditResource()

        # Test with comments
        html = '<div><a class="comments">123 comments</a></div>'
        soup = BeautifulSoup(html, "lxml")
        thing = soup.find("div")
        assert resource._extract_num_comments(thing) == 123

        # Test with single comment
        html = '<div><a class="comments">1 comment</a></div>'
        soup = BeautifulSoup(html, "lxml")
        thing = soup.find("div")
        assert resource._extract_num_comments(thing) == 1

        # Test with no comments
        html = '<div><a class="comments">comment</a></div>'
        soup = BeautifulSoup(html, "lxml")
        thing = soup.find("div")
        assert resource._extract_num_comments(thing) == 0

        # Test with missing comments element
        html = "<div></div>"
        soup = BeautifulSoup(html, "lxml")
        thing = soup.find("div")
        assert resource._extract_num_comments(thing) == 0

    def test_extract_timestamp(self):
        """Test extracting timestamp from post element."""
        from bs4 import BeautifulSoup

        resource = RedditResource()

        # Test with time element (ISO format)
        html = '<div><time datetime="2024-01-15T12:30:00Z">1 day ago</time></div>'
        soup = BeautifulSoup(html, "lxml")
        thing = soup.find("div")
        timestamp = resource._extract_timestamp(thing)
        assert isinstance(timestamp, datetime)
        assert timestamp.year == 2024
        assert timestamp.month == 1

        # Test with Unix timestamp in data attribute
        html = '<div data-timestamp="1705320600000"></div>'
        soup = BeautifulSoup(html, "lxml")
        thing = soup.find("div")
        timestamp = resource._extract_timestamp(thing)
        assert isinstance(timestamp, datetime)

    @patch("macro_agents.defs.resources.reddit.requests.get")
    @patch("macro_agents.defs.resources.reddit.time.sleep")
    def test_get_top_posts_empty(self, mock_sleep, mock_get):
        """Test fetching posts from subreddit with no results."""
        mock_context = Mock()
        mock_context.log = Mock()

        # Mock HTML response with no posts
        mock_html = """
        <html>
            <body>
                <div id="siteTable">
                    <!-- No posts -->
                </div>
            </body>
        </html>
        """

        mock_response = Mock()
        mock_response.content = mock_html.encode("utf-8")
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        resource = RedditResource()
        df = resource.get_top_posts("test", context=mock_context)

        # Should return empty DataFrame with correct schema
        assert isinstance(df, pl.DataFrame)
        assert df.height == 0
        assert "post_id" in df.columns
        assert "title" in df.columns
        assert "score" in df.columns

    @patch("macro_agents.defs.resources.reddit.requests.get")
    @patch("macro_agents.defs.resources.reddit.time.sleep")
    def test_get_top_posts_with_data(self, mock_sleep, mock_get):
        """Test fetching posts from subreddit with data."""
        mock_context = Mock()
        mock_context.log = Mock()

        # Mock HTML response with posts (simplified old.reddit.com structure)
        mock_html = """
        <html>
            <body>
                <div class="thing" data-fullname="t3_abc123" data-score="150"
                     data-author="testuser" data-url="https://example.com"
                     data-permalink="/r/test/comments/abc123/test_post/"
                     data-subreddit="test" data-domain="example.com">
                    <a class="title">Test Post Title</a>
                    <a class="comments">42 comments</a>
                    <time datetime="2024-01-15T12:00:00Z">1 day ago</time>
                </div>
                <div class="thing" data-fullname="t3_def456" data-score="300"
                     data-author="anotheruser" data-url="https://test.com"
                     data-permalink="/r/test/comments/def456/another_post/"
                     data-subreddit="test" data-domain="test.com">
                    <a class="title">Another Post</a>
                    <a class="comments">100 comments</a>
                    <time datetime="2024-01-14T10:00:00Z">2 days ago</time>
                </div>
            </body>
        </html>
        """

        mock_response = Mock()
        mock_response.content = mock_html.encode("utf-8")
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        resource = RedditResource()
        df = resource.get_top_posts("test", time_filter="day", context=mock_context)

        # Check DataFrame structure
        assert isinstance(df, pl.DataFrame)
        assert df.height == 2
        assert "post_id" in df.columns
        assert "title" in df.columns
        assert "score" in df.columns
        assert "num_comments" in df.columns

        # Check first post data
        first_post = df.row(0, named=True)
        assert first_post["post_id"] == "abc123"
        assert first_post["title"] == "Test Post Title"
        assert first_post["score"] == 150
        assert first_post["num_comments"] == 42
        assert first_post["author"] == "testuser"
        assert first_post["domain"] == "example.com"

        # Check second post data
        second_post = df.row(1, named=True)
        assert second_post["post_id"] == "def456"
        assert second_post["score"] == 300
        assert second_post["num_comments"] == 100

        # Verify request was made to correct URL
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert "old.reddit.com/r/test/top" in call_args[0][0]
        assert "t=day" in call_args[0][0]

    @patch("macro_agents.defs.resources.reddit.requests.get")
    @patch("macro_agents.defs.resources.reddit.time.sleep")
    def test_get_top_posts_retry_on_failure(self, mock_sleep, mock_get):
        """Test that get_top_posts retries on failure."""
        mock_context = Mock()
        mock_context.log = Mock()

        # First two calls fail, third succeeds
        success_html = """
        <html><body>
            <div class="thing" data-fullname="t3_abc123" data-score="100">
                <a class="title">Test</a>
            </div>
        </body></html>
        """

        mock_get.side_effect = [
            Exception("Network error"),
            Exception("Network error"),
            Mock(content=success_html.encode("utf-8"), raise_for_status=Mock()),
        ]

        resource = RedditResource()
        df = resource.get_top_posts("test", context=mock_context)

        # Should have retried and eventually succeeded
        assert mock_get.call_count == 3
        assert isinstance(df, pl.DataFrame)

    def test_parse_post_with_missing_data(self):
        """Test parsing post with missing data attributes."""
        from bs4 import BeautifulSoup

        resource = RedditResource()

        # Post with minimal data
        html = """
        <div class="thing" data-fullname="t3_abc123">
            <a class="title">Test Title</a>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        thing = soup.find("div", class_="thing")

        post = resource._parse_post(thing, "test")

        assert post is not None
        assert post["post_id"] == "abc123"
        assert post["title"] == "Test Title"
        assert post["score"] == 0  # Default when missing
        assert post["num_comments"] == 0  # Default when missing
        assert post["author"] == "[deleted]"  # Default when missing
        assert post["subreddit"] == "test"

    def test_parse_post_no_id(self):
        """Test parsing post without ID returns None."""
        from bs4 import BeautifulSoup

        resource = RedditResource()

        # Post without data-fullname
        html = '<div class="thing"><a class="title">Test</a></div>'
        soup = BeautifulSoup(html, "lxml")
        thing = soup.find("div", class_="thing")

        post = resource._parse_post(thing, "test")

        assert post is None

    def test_parse_comment(self):
        """Test parsing a single comment element."""
        from bs4 import BeautifulSoup

        resource = RedditResource()

        html = """
        <div class="thing" data-fullname="t1_abc123" data-author="commenter"
             data-parent-fullname="t3_post123">
            <div class="md"><p>This is a comment with a <a href="https://example.com">link</a>.</p></div>
            <span class="score" title="42">42 points</span>
            <time datetime="2024-01-15T12:00:00Z">1 day ago</time>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        comment_elem = soup.find("div", class_="thing")

        comment = resource._parse_comment(comment_elem)

        assert comment is not None
        assert comment["comment_id"] == "abc123"
        assert comment["author"] == "commenter"
        assert "This is a comment" in comment["body"]
        assert comment["score"] == 42
        assert comment["parent_id"] == "t3_post123"
        assert "https://example.com" in comment["links"]

    def test_parse_comment_no_id(self):
        """Test parsing comment without ID returns None."""
        from bs4 import BeautifulSoup

        resource = RedditResource()

        html = '<div class="thing"><div class="md">No ID comment</div></div>'
        soup = BeautifulSoup(html, "lxml")
        comment_elem = soup.find("div", class_="thing")

        comment = resource._parse_comment(comment_elem)

        assert comment is None

    def test_parse_comment_with_multiple_links(self):
        """Test parsing comment with multiple links."""
        from bs4 import BeautifulSoup

        resource = RedditResource()

        html = """
        <div class="thing" data-fullname="t1_xyz789" data-author="linkposter">
            <div class="md">
                <p>Check out <a href="https://first.com">first</a> and
                <a href="https://second.com">second</a> and
                <a href="#anchor">anchor</a> and
                <a href="/r/test">subreddit</a>.</p>
            </div>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        comment_elem = soup.find("div", class_="thing")

        comment = resource._parse_comment(comment_elem)

        assert comment is not None
        # Should have 2 external links (not anchor or reddit internal)
        assert len(comment["links"]) == 2
        assert "https://first.com" in comment["links"]
        assert "https://second.com" in comment["links"]

    @patch("macro_agents.defs.resources.reddit.requests.get")
    @patch("macro_agents.defs.resources.reddit.time.sleep")
    def test_get_post_comments_empty(self, mock_sleep, mock_get):
        """Test fetching comments from post with no comments."""
        mock_context = Mock()
        mock_context.log = Mock()

        mock_html = """
        <html>
            <body>
                <div class="commentarea">
                    <!-- No comments -->
                </div>
            </body>
        </html>
        """

        mock_response = Mock()
        mock_response.content = mock_html.encode("utf-8")
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        resource = RedditResource()
        df = resource.get_post_comments(
            "/r/test/comments/abc123/test/", context=mock_context
        )

        assert isinstance(df, pl.DataFrame)
        assert df.height == 0
        assert "comment_id" in df.columns
        assert "body" in df.columns
        assert "links" in df.columns

    @patch("macro_agents.defs.resources.reddit.requests.get")
    @patch("macro_agents.defs.resources.reddit.time.sleep")
    def test_get_post_comments_with_data(self, mock_sleep, mock_get):
        """Test fetching comments from post with comments."""
        mock_context = Mock()
        mock_context.log = Mock()

        mock_html = """
        <html>
            <body>
                <div class="commentarea">
                    <div class="thing" data-fullname="t1_comment1" data-author="user1"
                         data-parent-fullname="t3_post123">
                        <div class="md"><p>First comment with <a href="https://link1.com">link</a></p></div>
                        <span class="score" title="10">10 points</span>
                        <time datetime="2024-01-15T12:00:00Z">1 day ago</time>
                    </div>
                    <div class="thing" data-fullname="t1_comment2" data-author="user2"
                         data-parent-fullname="t1_comment1">
                        <div class="md"><p>Reply to first comment</p></div>
                        <span class="score" title="5">5 points</span>
                        <time datetime="2024-01-15T13:00:00Z">1 day ago</time>
                    </div>
                </div>
            </body>
        </html>
        """

        mock_response = Mock()
        mock_response.content = mock_html.encode("utf-8")
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        resource = RedditResource()
        df = resource.get_post_comments(
            "/r/test/comments/abc123/test/", context=mock_context
        )

        assert isinstance(df, pl.DataFrame)
        assert df.height == 2

        first_comment = df.row(0, named=True)
        assert first_comment["comment_id"] == "comment1"
        assert first_comment["author"] == "user1"
        assert "First comment" in first_comment["body"]
        assert first_comment["score"] == 10
        assert "https://link1.com" in first_comment["links"]

        second_comment = df.row(1, named=True)
        assert second_comment["comment_id"] == "comment2"
        assert second_comment["parent_id"] == "t1_comment1"

    @patch("macro_agents.defs.resources.reddit.requests.get")
    @patch("macro_agents.defs.resources.reddit.time.sleep")
    def test_get_post_content(self, mock_sleep, mock_get):
        """Test fetching full post content."""
        mock_context = Mock()
        mock_context.log = Mock()

        mock_html = """
        <html>
            <body>
                <div class="thing" data-fullname="t3_abc123" data-author="poster"
                     data-score="500" data-url="https://external-link.com">
                    <a class="title">Test Post Title</a>
                    <time datetime="2024-01-15T12:00:00Z">1 day ago</time>
                </div>
                <div class="usertext-body">
                    <div class="md">
                        <p>This is the selftext with a <a href="https://embedded-link.com">link</a>.</p>
                    </div>
                </div>
            </body>
        </html>
        """

        mock_response = Mock()
        mock_response.content = mock_html.encode("utf-8")
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        resource = RedditResource()
        content = resource.get_post_content(
            "/r/test/comments/abc123/test/", context=mock_context
        )

        assert content["post_id"] == "abc123"
        assert content["title"] == "Test Post Title"
        assert content["author"] == "poster"
        assert content["score"] == 500
        assert "This is the selftext" in content["selftext"]
        assert "https://external-link.com" in content["links"]
        assert "https://embedded-link.com" in content["links"]

    @patch("macro_agents.defs.resources.reddit.requests.get")
    @patch("macro_agents.defs.resources.reddit.time.sleep")
    def test_get_post_content_no_post(self, mock_sleep, mock_get):
        """Test fetching post content when post not found."""
        mock_context = Mock()
        mock_context.log = Mock()

        mock_html = "<html><body></body></html>"

        mock_response = Mock()
        mock_response.content = mock_html.encode("utf-8")
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        resource = RedditResource()
        content = resource.get_post_content(
            "/r/test/comments/missing/", context=mock_context
        )

        assert content["post_id"] == ""
        assert content["title"] == ""
        assert content["selftext"] == ""
        assert content["links"] == []

    @patch("macro_agents.defs.resources.reddit.requests.get")
    @patch("macro_agents.defs.resources.reddit.time.sleep")
    def test_get_post_content_link_post(self, mock_sleep, mock_get):
        """Test fetching content from a link post (no selftext)."""
        mock_context = Mock()
        mock_context.log = Mock()

        mock_html = """
        <html>
            <body>
                <div class="thing" data-fullname="t3_linkpost" data-author="linkposter"
                     data-score="100" data-url="https://news-article.com/story">
                    <a class="title">Breaking News Article</a>
                    <time datetime="2024-01-15T12:00:00Z">1 day ago</time>
                </div>
            </body>
        </html>
        """

        mock_response = Mock()
        mock_response.content = mock_html.encode("utf-8")
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        resource = RedditResource()
        content = resource.get_post_content(
            "/r/news/comments/linkpost/", context=mock_context
        )

        assert content["post_id"] == "linkpost"
        assert content["title"] == "Breaking News Article"
        assert content["selftext"] == ""
        assert "https://news-article.com/story" in content["links"]
        assert content["url"] == "https://news-article.com/story"

    @patch("macro_agents.defs.resources.reddit.requests.get")
    @patch("macro_agents.defs.resources.reddit.time.sleep")
    def test_get_top_posts_filters_ads(self, mock_sleep, mock_get):
        """Test that promoted/ad posts (u_* subreddits) are filtered out."""
        mock_context = Mock()
        mock_context.log = Mock()

        mock_html = """
        <html>
            <body>
                <div class="thing" data-fullname="t3_real1" data-score="100"
                     data-subreddit="economics" data-permalink="/r/economics/comments/real1/">
                    <a class="title">Real Post</a>
                    <a class="comments">10 comments</a>
                </div>
                <div class="thing" data-fullname="t3_ad1" data-score="0"
                     data-subreddit="u_CapitalOne" data-permalink="/u/CapitalOne/comments/ad1/">
                    <a class="title">Promoted Ad</a>
                    <a class="comments">0 comments</a>
                </div>
                <div class="thing" data-fullname="t3_real2" data-score="50"
                     data-subreddit="economics" data-permalink="/r/economics/comments/real2/">
                    <a class="title">Another Real Post</a>
                    <a class="comments">5 comments</a>
                </div>
            </body>
        </html>
        """

        mock_response = Mock()
        mock_response.content = mock_html.encode("utf-8")
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        resource = RedditResource()
        df = resource.get_top_posts("economics", context=mock_context)

        # Should only have 2 real posts, ad filtered out
        assert df.height == 2
        assert "ad1" not in df["post_id"].to_list()
        assert "real1" in df["post_id"].to_list()
        assert "real2" in df["post_id"].to_list()

    def test_parse_post_none_attributes(self):
        """Test parsing post where data attributes return None."""
        from bs4 import BeautifulSoup

        resource = RedditResource()

        html = """
        <div class="thing" data-fullname="t3_abc123" data-score="not_a_number"
             data-author="" data-url="" data-permalink="" data-domain="">
            <a class="title">Post With Nones</a>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        thing = soup.find("div", class_="thing")
        post = resource._parse_post(thing, "test")

        assert post is not None
        assert post["post_id"] == "abc123"
        assert post["score"] == 0  # Invalid score defaults to 0
        assert post["author"] == "[deleted]"  # Empty author normalizes
        assert post["url"] == ""
        assert post["subreddit"] == "test"

    def test_parse_comment_deleted_body(self):
        """Test parsing comment with [deleted] body content."""
        from bs4 import BeautifulSoup

        resource = RedditResource()

        html = """
        <div class="thing" data-fullname="t1_del123" data-author="">
            <div class="md"><p>[deleted]</p></div>
            <span class="score" title="0">0 points</span>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        comment_elem = soup.find("div", class_="thing")
        comment = resource._parse_comment(comment_elem)

        assert comment is not None
        assert comment["comment_id"] == "del123"
        assert comment["body"] == ""  # [deleted] body normalized to empty
        assert comment["author"] == "[deleted]"

    def test_parse_comment_removed_body(self):
        """Test parsing comment with [removed] body content."""
        from bs4 import BeautifulSoup

        resource = RedditResource()

        html = """
        <div class="thing" data-fullname="t1_rem456" data-author="[deleted]">
            <div class="md"><p>[removed]</p></div>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        comment_elem = soup.find("div", class_="thing")
        comment = resource._parse_comment(comment_elem)

        assert comment is not None
        assert comment["body"] == ""  # [removed] body normalized to empty

    @patch("macro_agents.defs.resources.reddit.requests.get")
    @patch("macro_agents.defs.resources.reddit.time.sleep")
    def test_get_post_content_deleted_selftext(self, mock_sleep, mock_get):
        """Test fetching post content where selftext is [deleted]."""
        mock_html = """
        <html>
            <body>
                <div class="thing" data-fullname="t3_del789" data-author="[deleted]"
                     data-score="0" data-url="/r/test/comments/del789/deleted_post/">
                    <a class="title">Deleted Post</a>
                    <time datetime="2024-01-15T12:00:00Z">1 day ago</time>
                </div>
                <div class="usertext-body">
                    <div class="md"><p>[deleted]</p></div>
                </div>
            </body>
        </html>
        """
        mock_response = Mock()
        mock_response.content = mock_html.encode("utf-8")
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        resource = RedditResource()
        content = resource.get_post_content("/r/test/comments/del789/")

        assert content["post_id"] == "del789"
        assert content["selftext"] == ""  # [deleted] normalized to empty

    def test_extract_timestamp_invalid_formats(self):
        """Test timestamp extraction with various invalid formats."""
        from bs4 import BeautifulSoup

        resource = RedditResource()

        # No time element, no data-timestamp
        html = "<div></div>"
        soup = BeautifulSoup(html, "lxml")
        thing = soup.find("div")
        ts = resource._extract_timestamp(thing)
        assert isinstance(ts, datetime)  # Falls back to now()

        # Invalid data-timestamp value
        html = '<div data-timestamp="not_a_number"></div>'
        soup = BeautifulSoup(html, "lxml")
        thing = soup.find("div")
        ts = resource._extract_timestamp(thing)
        assert isinstance(ts, datetime)  # Falls back to now()
