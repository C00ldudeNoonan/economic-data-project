"""Shared PDF processing resource using pdfplumber."""

from io import BytesIO

import pdfplumber
from dagster import ConfigurableResource
from pydantic import Field


class PDFResource(ConfigurableResource):
    """Resource for extracting text and metadata from PDF files."""

    # pdfplumber settings
    x_tolerance: int = Field(
        default=3, description="Horizontal tolerance for character grouping"
    )
    y_tolerance: int = Field(
        default=3, description="Vertical tolerance for line grouping"
    )

    def extract_text(self, pdf_bytes: bytes) -> str:
        """Extract full text from a PDF.

        Args:
            pdf_bytes: Raw PDF file content

        Returns:
            Extracted text with pages separated by newlines
        """
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text(
                    x_tolerance=self.x_tolerance,
                    y_tolerance=self.y_tolerance,
                )
                if text:
                    pages.append(text)
            return "\n\n".join(pages)

    def extract_pages(self, pdf_bytes: bytes) -> list[dict]:
        """Extract text from each page with metadata.

        Args:
            pdf_bytes: Raw PDF file content

        Returns:
            List of dicts with page_number, text, and word_count
        """
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            pages = []
            for i, page in enumerate(pdf.pages, start=1):
                text = page.extract_text(
                    x_tolerance=self.x_tolerance,
                    y_tolerance=self.y_tolerance,
                )
                if text:
                    pages.append(
                        {
                            "page_number": i,
                            "text": text,
                            "word_count": len(text.split()),
                        }
                    )
            return pages

    def get_metadata(self, pdf_bytes: bytes) -> dict:
        """Extract PDF metadata (page count, author, etc).

        Args:
            pdf_bytes: Raw PDF file content

        Returns:
            Dict with page_count and any available PDF metadata
        """
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            return {
                "page_count": len(pdf.pages),
                "metadata": pdf.metadata or {},
            }
