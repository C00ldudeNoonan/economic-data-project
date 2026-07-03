"""SEC filing text extraction utilities."""

import re
from dataclasses import dataclass

from bs4 import BeautifulSoup, Tag


@dataclass
class ExtractedSection:
    """Represents an extracted section from an SEC filing."""

    name: str
    item_number: str
    content: str
    word_count: int
    start_position: int
    end_position: int


# Standard 10-K sections
SECTION_10K_PATTERNS = {
    "item_1": {
        "pattern": r"item\s*1[.\s]*business",
        "name": "Business",
        "item": "1",
    },
    "item_1a": {
        "pattern": r"item\s*1a[.\s]*risk\s*factors",
        "name": "Risk Factors",
        "item": "1A",
    },
    "item_1b": {
        "pattern": r"item\s*1b[.\s]*unresolved\s*staff\s*comments",
        "name": "Unresolved Staff Comments",
        "item": "1B",
    },
    "item_2": {
        "pattern": r"item\s*2[.\s]*properties",
        "name": "Properties",
        "item": "2",
    },
    "item_3": {
        "pattern": r"item\s*3[.\s]*legal\s*proceedings",
        "name": "Legal Proceedings",
        "item": "3",
    },
    "item_4": {
        "pattern": r"item\s*4[.\s]*mine\s*safety",
        "name": "Mine Safety Disclosures",
        "item": "4",
    },
    "item_5": {
        "pattern": r"item\s*5[.\s]*market\s*for",
        "name": "Market for Common Equity",
        "item": "5",
    },
    "item_6": {
        "pattern": r"item\s*6[.\s]*(?:reserved|\[reserved\]|selected)",
        "name": "Reserved/Selected Financial Data",
        "item": "6",
    },
    "item_7": {
        "pattern": r"item\s*7[.\s]*management['\u2019]?s?\s*discussion",
        "name": "Management Discussion and Analysis",
        "item": "7",
    },
    "item_7a": {
        "pattern": r"item\s*7a[.\s]*quantitative",
        "name": "Market Risk Disclosures",
        "item": "7A",
    },
    "item_8": {
        "pattern": r"item\s*8[.\s]*financial\s*statements",
        "name": "Financial Statements",
        "item": "8",
    },
    "item_9": {
        "pattern": r"item\s*9[.\s]*changes\s*in\s*and\s*disagreements",
        "name": "Accountant Changes",
        "item": "9",
    },
    "item_9a": {
        "pattern": r"item\s*9a[.\s]*controls\s*and\s*procedures",
        "name": "Controls and Procedures",
        "item": "9A",
    },
    "item_9b": {
        "pattern": r"item\s*9b[.\s]*other\s*information",
        "name": "Other Information",
        "item": "9B",
    },
    "item_10": {
        "pattern": r"item\s*10[.\s]*directors",
        "name": "Directors and Officers",
        "item": "10",
    },
    "item_11": {
        "pattern": r"item\s*11[.\s]*executive\s*compensation",
        "name": "Executive Compensation",
        "item": "11",
    },
    "item_12": {
        "pattern": r"item\s*12[.\s]*security\s*ownership",
        "name": "Security Ownership",
        "item": "12",
    },
    "item_13": {
        "pattern": r"item\s*13[.\s]*certain\s*relationships",
        "name": "Related Transactions",
        "item": "13",
    },
    "item_14": {
        "pattern": r"item\s*14[.\s]*principal\s*account",
        "name": "Accountant Fees",
        "item": "14",
    },
    "item_15": {
        "pattern": r"item\s*15[.\s]*exhibit",
        "name": "Exhibits",
        "item": "15",
    },
}

# Standard 10-Q sections
SECTION_10Q_PATTERNS = {
    "part1_item1": {
        "pattern": r"part\s*i.*item\s*1[.\s]*financial\s*statements",
        "name": "Financial Statements",
        "item": "Part I, Item 1",
    },
    "part1_item2": {
        "pattern": r"part\s*i.*item\s*2[.\s]*management['\u2019]?s?\s*discussion",
        "name": "Management Discussion and Analysis",
        "item": "Part I, Item 2",
    },
    "part1_item3": {
        "pattern": r"part\s*i.*item\s*3[.\s]*quantitative",
        "name": "Market Risk Disclosures",
        "item": "Part I, Item 3",
    },
    "part1_item4": {
        "pattern": r"part\s*i.*item\s*4[.\s]*controls",
        "name": "Controls and Procedures",
        "item": "Part I, Item 4",
    },
    "part2_item1": {
        "pattern": r"part\s*ii.*item\s*1[.\s]*legal",
        "name": "Legal Proceedings",
        "item": "Part II, Item 1",
    },
    "part2_item1a": {
        "pattern": r"part\s*ii.*item\s*1a[.\s]*risk",
        "name": "Risk Factors",
        "item": "Part II, Item 1A",
    },
    "part2_item2": {
        "pattern": r"part\s*ii.*item\s*2[.\s]*unregistered",
        "name": "Unregistered Sales",
        "item": "Part II, Item 2",
    },
    "part2_item6": {
        "pattern": r"part\s*ii.*item\s*6[.\s]*exhibit",
        "name": "Exhibits",
        "item": "Part II, Item 6",
    },
}


class SECTextExtractor:
    """Extract and clean text from SEC filing HTML documents."""

    def __init__(self):
        """Initialize the extractor."""
        self.section_patterns_10k = SECTION_10K_PATTERNS
        self.section_patterns_10q = SECTION_10Q_PATTERNS

    def extract_text_from_html(self, html_content: str) -> str:
        """
        Extract clean text from HTML content.

        Args:
            html_content: Raw HTML string

        Returns:
            Cleaned text content
        """
        soup = BeautifulSoup(html_content, "lxml")

        # Remove script and style elements
        for element in soup(["script", "style", "meta", "link", "head"]):
            element.decompose()

        # Get text
        text = soup.get_text(separator=" ", strip=True)

        # Clean up whitespace
        text = self._clean_text(text)

        return text

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text."""
        # Replace multiple spaces with single space
        text = re.sub(r"[ \t]+", " ", text)

        # Replace multiple newlines with double newline
        text = re.sub(r"\n\s*\n", "\n\n", text)

        # Remove leading/trailing whitespace from lines
        lines = [line.strip() for line in text.split("\n")]
        text = "\n".join(lines)

        # Remove very short lines that are likely noise
        lines = text.split("\n")
        lines = [line for line in lines if len(line) > 2 or line == ""]
        text = "\n".join(lines)

        return text.strip()

    def extract_sections(
        self, html_content: str, form_type: str = "10-K"
    ) -> list[ExtractedSection]:
        """
        Extract individual sections from an SEC filing.

        Args:
            html_content: Raw HTML string
            form_type: Type of filing ("10-K" or "10-Q")

        Returns:
            List of ExtractedSection objects
        """
        # Get clean text first
        full_text = self.extract_text_from_html(html_content)
        full_text_lower = full_text.lower()

        # Select appropriate patterns
        if "10-Q" in form_type.upper():
            patterns = self.section_patterns_10q
        else:
            patterns = self.section_patterns_10k

        # Find all section positions
        section_positions = []
        for key, section_info in patterns.items():
            pattern = section_info["pattern"]
            matches = list(re.finditer(pattern, full_text_lower, re.IGNORECASE))

            for match in matches:
                section_positions.append(
                    {
                        "key": key,
                        "name": section_info["name"],
                        "item": section_info["item"],
                        "start": match.start(),
                        "match_text": match.group(),
                    }
                )

        # Sort by position
        section_positions.sort(key=lambda x: x["start"])

        # Remove duplicates (keep LAST occurrence of each section to skip TOC entries)
        # TOC entries appear first in the document, actual section content appears later
        last_occurrence: dict[str, dict] = {}
        for pos in section_positions:
            section_key = pos["key"]
            if isinstance(section_key, str):
                last_occurrence[section_key] = pos

        # Convert back to list and sort by position
        unique_positions = sorted(last_occurrence.values(), key=lambda x: x["start"])

        # Extract content between sections
        sections = []
        for i, pos in enumerate(unique_positions):
            start = pos["start"]

            # End is either next section or end of document
            if i + 1 < len(unique_positions):
                end = unique_positions[i + 1]["start"]
            else:
                end = len(full_text)

            content = full_text[start:end].strip()
            word_count = len(content.split())

            sections.append(
                ExtractedSection(
                    name=pos["name"],
                    item_number=pos["item"],
                    content=content,
                    word_count=word_count,
                    start_position=start,
                    end_position=end,
                )
            )

        return sections

    def extract_key_sections(
        self, html_content: str, form_type: str = "10-K"
    ) -> dict[str, ExtractedSection]:
        """
        Extract key sections most useful for business intelligence.

        Args:
            html_content: Raw HTML string
            form_type: Type of filing ("10-K" or "10-Q")

        Returns:
            Dict mapping section name to ExtractedSection
        """
        all_sections = self.extract_sections(html_content, form_type)

        # Key sections for business intelligence
        key_section_names = {
            "Business",
            "Risk Factors",
            "Management Discussion and Analysis",
            "Properties",
            "Legal Proceedings",
        }

        key_sections = {}
        for section in all_sections:
            if section.name in key_section_names:
                key_sections[section.name] = section

        return key_sections

    def get_full_text_with_metadata(
        self, html_content: str, form_type: str = "10-K"
    ) -> dict:
        """
        Extract full text and metadata from filing.

        Args:
            html_content: Raw HTML string
            form_type: Type of filing

        Returns:
            Dict with full_text, sections, and metadata
        """
        full_text = self.extract_text_from_html(html_content)
        sections = self.extract_sections(html_content, form_type)

        return {
            "full_text": full_text,
            "total_word_count": len(full_text.split()),
            "total_characters": len(full_text),
            "sections": [
                {
                    "name": s.name,
                    "item_number": s.item_number,
                    "word_count": s.word_count,
                    "content": s.content,
                }
                for s in sections
            ],
            "section_count": len(sections),
        }

    def extract_tables(self, html_content: str) -> list[dict]:
        """
        Extract tables from HTML content.

        Args:
            html_content: Raw HTML string

        Returns:
            List of dicts with table data
        """
        soup = BeautifulSoup(html_content, "lxml")
        tables = []

        for i, table in enumerate(soup.find_all("table")):
            if not isinstance(table, Tag):
                continue
            rows = []
            for tr in table.find_all("tr"):
                if not isinstance(tr, Tag):
                    continue
                cells = []
                for td in tr.find_all(["td", "th"]):
                    if not isinstance(td, Tag):
                        continue
                    cell_text = td.get_text(separator=" ", strip=True)
                    cells.append(cell_text)
                if cells:
                    rows.append(cells)

            if rows:
                tables.append(
                    {
                        "table_index": i,
                        "row_count": len(rows),
                        "rows": rows[:100],  # Limit rows to prevent huge data
                    }
                )

        return tables

    def extract_links(self, html_content: str) -> list[dict]:
        """
        Extract links from HTML content.

        Args:
            html_content: Raw HTML string

        Returns:
            List of dicts with link data
        """
        soup = BeautifulSoup(html_content, "lxml")
        links = []

        for a in soup.find_all("a", href=True):
            if not isinstance(a, Tag):
                continue
            href_value = a.get("href", "")
            if isinstance(href_value, list):
                href = href_value[0] if href_value else ""
            else:
                href = str(href_value)
            text = a.get_text(strip=True)

            # Filter out internal anchors and empty links
            if href and not href.startswith("#") and text:
                links.append({"url": href, "text": text[:200]})

        return links
