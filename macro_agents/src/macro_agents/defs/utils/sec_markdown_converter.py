"""SEC filing HTML to Markdown conversion utilities.

Converts SEC EDGAR filing HTML into clean, structured Markdown suitable
for AI agent consumption. Handles XBRL tags, nested tables, SEC-specific
boilerplate, and preserves document structure (headings, lists, tables).
"""

import re

from bs4 import BeautifulSoup, NavigableString, PageElement, Tag


class SECMarkdownConverter:
    """Converts SEC filing HTML to clean Markdown."""

    # XBRL/iXBRL namespace prefixes to strip
    _XBRL_PREFIXES = ("ix:", "xbrli:", "xbrldi:", "us-gaap:", "dei:")

    # Tags to remove entirely (content and all)
    _REMOVE_TAGS = {"script", "style", "meta", "link", "noscript", "object", "embed"}

    # Block-level tags that get newlines
    _BLOCK_TAGS = {
        "div",
        "p",
        "section",
        "article",
        "blockquote",
        "pre",
        "hr",
        "br",
        "tr",
        "li",
    }

    def convert(self, html: str, form_type: str = "10-K") -> str:
        """Convert SEC filing HTML to Markdown.

        Args:
            html: Raw HTML content from SEC filing
            form_type: Filing type (10-K, 10-Q) for context

        Returns:
            Clean Markdown string
        """
        soup = BeautifulSoup(html, "html.parser")

        self._strip_xbrl_tags(soup)
        self._remove_unwanted_tags(soup)
        self._remove_hidden_elements(soup)

        lines = []
        self._process_node(soup, lines)
        markdown = "\n".join(lines)

        markdown = self._clean_whitespace(markdown)
        return markdown.strip()

    def convert_section(self, html: str, section_name: str) -> str:
        """Convert a single section's HTML to Markdown with a heading."""
        md = self.convert(html)
        if md:
            return f"## {section_name}\n\n{md}"
        return ""

    def _strip_xbrl_tags(self, soup: BeautifulSoup) -> None:
        """Unwrap XBRL/iXBRL inline tags, keeping their text content."""
        for prefix in self._XBRL_PREFIXES:
            for tag in soup.find_all(re.compile(f"^{re.escape(prefix)}")):
                tag.unwrap()

        # Also handle generic ix: tags
        for tag in soup.find_all(re.compile(r"^ix:")):
            tag.unwrap()

    def _remove_unwanted_tags(self, soup: BeautifulSoup) -> None:
        """Remove script, style, and other non-content tags."""
        for tag_name in self._REMOVE_TAGS:
            for tag in soup.find_all(tag_name):
                tag.decompose()

    def _remove_hidden_elements(self, soup: BeautifulSoup) -> None:
        """Remove elements with display:none or visibility:hidden."""
        for tag in soup.find_all(style=re.compile(r"display\s*:\s*none", re.I)):
            tag.decompose()
        for tag in soup.find_all(style=re.compile(r"visibility\s*:\s*hidden", re.I)):
            tag.decompose()

    def _process_node(self, node: PageElement, lines: list[str]) -> None:
        """Recursively convert DOM nodes to Markdown lines."""
        if isinstance(node, NavigableString):
            text = str(node).strip()
            if text:
                lines.append(text)
            return

        if not isinstance(node, Tag):
            return

        tag_name = node.name.lower() if node.name else ""

        # Headings
        if tag_name in ("h1", "h2", "h3", "h4", "h5", "h6"):
            level = int(tag_name[1])
            text = node.get_text(" ", strip=True)
            if text:
                lines.append("")
                lines.append(f"{'#' * level} {text}")
                lines.append("")
            return

        # Tables
        if tag_name == "table":
            table_md = self._convert_table(node)
            if table_md:
                lines.append("")
                lines.append(table_md)
                lines.append("")
            return

        # Lists
        if tag_name in ("ul", "ol"):
            lines.append("")
            for i, li in enumerate(node.find_all("li", recursive=False)):
                text = li.get_text(" ", strip=True)
                if text:
                    prefix = f"{i + 1}." if tag_name == "ol" else "-"
                    lines.append(f"{prefix} {text}")
            lines.append("")
            return

        # Bold
        if tag_name in ("b", "strong"):
            text = node.get_text(" ", strip=True)
            if text:
                lines.append(f"**{text}**")
            return

        # Italic
        if tag_name in ("i", "em"):
            text = node.get_text(" ", strip=True)
            if text:
                lines.append(f"*{text}*")
            return

        # Horizontal rule
        if tag_name == "hr":
            lines.append("\n---\n")
            return

        # Line break
        if tag_name == "br":
            lines.append("")
            return

        # Paragraph
        if tag_name == "p":
            text = node.get_text(" ", strip=True)
            if text:
                lines.append("")
                lines.append(text)
                lines.append("")
            return

        # All other tags: recurse into children
        for child in node.children:
            self._process_node(child, lines)

        if tag_name in self._BLOCK_TAGS:
            lines.append("")

    def _convert_table(self, table: Tag) -> str:
        """Convert an HTML table to Markdown table format."""
        rows = table.find_all("tr")
        if not rows:
            return ""

        parsed_rows = []
        for row in rows:
            cells = row.find_all(["td", "th"])
            cell_texts = [
                cell.get_text(" ", strip=True).replace("|", "\\|") for cell in cells
            ]
            parsed_rows.append(cell_texts)

        if not parsed_rows:
            return ""

        # Determine column count from widest row
        max_cols = max(len(r) for r in parsed_rows)
        if max_cols == 0:
            return ""

        # Pad rows to same width
        for row in parsed_rows:
            while len(row) < max_cols:
                row.append("")

        md_lines = []
        # Header row
        md_lines.append("| " + " | ".join(parsed_rows[0]) + " |")
        md_lines.append("| " + " | ".join(["---"] * max_cols) + " |")

        # Data rows
        for row in parsed_rows[1:]:
            md_lines.append("| " + " | ".join(row) + " |")

        return "\n".join(md_lines)

    def _clean_whitespace(self, text: str) -> str:
        """Normalize whitespace in the final Markdown output."""
        # Collapse 3+ consecutive blank lines to 2
        text = re.sub(r"\n{4,}", "\n\n\n", text)
        # Remove trailing whitespace per line
        text = re.sub(r"[ \t]+$", "", text, flags=re.MULTILINE)
        # Remove leading blank lines
        text = text.lstrip("\n")
        return text
