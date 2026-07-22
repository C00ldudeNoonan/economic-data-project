"""GCS path conventions for SEC filing storage.

Single source of truth for the bucket layout. All path-building functions
in helpers.py follow these conventions; this module documents them as
importable constants so tooling and tests can reference the schema without
hard-coding strings.

Target bucket layout
====================

sec_filings/
    catalog.json                              # root catalog (CIK/accession index)
    {symbol}/
        manifest.json                         # per-company filing inventory
        {form_type}/{year}/{cik}/{accession}/
            {primary_document}                # raw filing JSON envelope
            filing.md                         # full markdown conversion
            sections/
                {section_name}.md             # per-section markdown
            extracted/
                full_text.json                # full extracted text
                section_{n}_{item}.json       # per-section extracted text

Current bucket state (verified 2026-07-10)
------------------------------------------
The bucket holds a MIX of layouts. Newer downloads follow the target
layout above (with the {symbol} level); older objects still live at the
legacy ``sec_filings/{form_type}/{year}/{cik}/{accession}/`` paths with no
symbol level (84 of 109 primary documents at time of writing). The
migration for the legacy objects is implemented in ``migration.py`` but has
not run against the bucket. No ``filing.md`` / ``sections/*.md`` or
manifest/catalog JSONs exist yet — the markdown conversion in
``markdown.py`` and the manifest assets have not run either. The primary
document is a JSON envelope ({"content": html, "metadata": {...}}) despite
its ``.htm`` name. Readers are unaffected by the path mix because they
resolve object paths from the ``gcs_path`` column of the ``sec_filings``
table, not from these templates. Consumers listing the bucket directly
(e.g. the dbt-ml ``document_extraction`` project) must handle both layouts
until the migration runs.

Encoding rules
--------------
- **form_type**: forward slashes replaced with dashes (``10-K/A`` -> ``10-K-A``)
- **accession**: dashes stripped in GCS paths (``0000320193-24-000008``
  -> ``000032019324000008``). The original dashed format is preserved in
  the database ``accession_number`` column and in catalog/manifest JSON
  for cross-system correlation.
- **year**: first four characters of ``filing_date``; falls back to
  ``"unknown"`` when the date is missing.
- **section_name**: lowercased, spaces and slashes replaced with
  underscores.
"""

# ---------------------------------------------------------------------------
# Path prefixes
# ---------------------------------------------------------------------------

ROOT_PREFIX = "sec_filings"
"""Top-level GCS prefix for all SEC filing objects."""

CATALOG_PATH = f"{ROOT_PREFIX}/catalog.json"
"""Root-level catalog mapping CIK -> symbol -> accession -> GCS paths."""

# ---------------------------------------------------------------------------
# Per-company paths (relative to ROOT_PREFIX/{symbol}/)
# ---------------------------------------------------------------------------

COMPANY_MANIFEST_FILENAME = "manifest.json"
"""Per-company manifest listing all filings and their GCS locations."""

# ---------------------------------------------------------------------------
# Per-filing sub-paths (relative to the base filing path)
# ---------------------------------------------------------------------------

FILING_MARKDOWN_FILENAME = "filing.md"
"""Full filing markdown conversion."""

SECTIONS_DIR = "sections"
"""Directory containing per-section markdown files."""

EXTRACTED_DIR = "extracted"
"""Directory containing extracted text JSON files."""

EXTRACTED_FULL_TEXT_FILENAME = "full_text.json"
"""Full extracted text for the filing."""

# ---------------------------------------------------------------------------
# Template strings (for documentation; actual path building is in helpers.py)
# ---------------------------------------------------------------------------

BASE_PATH_TEMPLATE = (
    f"{ROOT_PREFIX}/{{symbol}}/{{form_type}}/{{year}}/{{cik}}/{{accession}}"
)
"""Template for the base filing path. See ``helpers.build_filing_gcs_path()``."""

COMPANY_MANIFEST_TEMPLATE = f"{ROOT_PREFIX}/{{symbol}}/{COMPANY_MANIFEST_FILENAME}"
"""Template for per-company manifest path."""
