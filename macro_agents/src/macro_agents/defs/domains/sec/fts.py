"""DuckDB full-text search index for SEC filings.

Creates and maintains a FTS index over sec_filing_content, enabling
keyword-based queries like "find 10-K filings mentioning supply chain risk".
Complements the vector/semantic search in search.py.
"""

import dagster as dg
import polars as pl

from macro_agents.defs.domains.sec.tables import ensure_sec_filing_content_table
from macro_agents.defs.domains.sec.text import sec_filing_text_extracted
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.resources.motherduck import MotherDuckResource

FTS_TABLE = "sec_filing_fts_content"


def _ensure_fts_content_table(conn) -> None:
    """Create the denormalized FTS source table if it doesn't exist.

    DuckDB FTS indexes require a single table with the text columns.
    We join filing metadata with content text so keyword searches
    return symbol, form_type, filing_date alongside matches.
    """
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {FTS_TABLE} (
            content_id VARCHAR PRIMARY KEY,
            filing_id VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            form_type VARCHAR,
            filing_date DATE,
            section_name VARCHAR,
            content_text TEXT,
            word_count INTEGER,
            indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


@dg.asset(
    group_name="transformation",
    kinds={"duckdb", "search"},
    deps=[sec_filing_text_extracted],
    description=(
        "Build DuckDB full-text search index over SEC filing content. "
        "Enables keyword queries across filing sections with metadata filtering."
    ),
)
def sec_filing_fts_index(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    gcs: GCSResource,
) -> dg.MaterializeResult:
    """
    Build and refresh the full-text search index.

    Steps:
    1. Populate a denormalized FTS source table with content + metadata
    2. Download section text from GCS for rows missing content_text
    3. Create/recreate the DuckDB FTS index using PRAGMA create_fts_index
    """
    conn = None
    try:
        conn = md.get_connection()
        ensure_sec_filing_content_table(conn)
        _ensure_fts_content_table(conn)

        # Find content rows not yet in the FTS table
        new_content = pl.read_database(
            f"""
            SELECT c.content_id, c.filing_id, c.section_name,
                   c.word_count, c.gcs_path,
                   f.symbol, f.form_type, f.filing_date
            FROM sec_filing_content c
            JOIN sec_filings f ON f.filing_id = c.filing_id
            WHERE c.gcs_path IS NOT NULL
            AND c.word_count > 10
            AND c.content_id NOT IN (
                SELECT content_id FROM {FTS_TABLE}
            )
            ORDER BY f.filing_date DESC
            LIMIT 200
            """,
            connection=conn,
        )

        if new_content.is_empty():
            context.log.debug("FTS table already up to date")
        else:
            context.log.info(f"Loading {len(new_content)} sections into FTS table")
            rows_inserted = 0
            errors = []

            for row in new_content.iter_rows(named=True):
                try:
                    section_data = gcs.download_json(row["gcs_path"])
                    content_text = section_data.get("content", "")
                    if not content_text:
                        continue

                    conn.execute(
                        f"""
                        INSERT INTO {FTS_TABLE}
                        (content_id, filing_id, symbol, form_type,
                         filing_date, section_name, content_text, word_count)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (content_id) DO NOTHING
                        """,
                        [
                            row["content_id"],
                            row["filing_id"],
                            row["symbol"],
                            row["form_type"],
                            row["filing_date"],
                            row["section_name"],
                            content_text,
                            row["word_count"],
                        ],
                    )
                    rows_inserted += 1

                except Exception as e:
                    errors.append(f"{row['symbol']}/{row['section_name']}: {e}")
                    continue

            conn.commit()
            context.log.info(
                f"Inserted {rows_inserted} rows into FTS table, {len(errors)} errors"
            )

        # Install and load FTS extension, then create/recreate the index
        conn.execute("INSTALL fts")
        conn.execute("LOAD fts")

        # Drop existing index before recreating
        conn.execute(
            f"PRAGMA drop_fts_index('{FTS_TABLE}')",
        )

        # Create FTS index on content_text plus metadata fields
        conn.execute(f"""
            PRAGMA create_fts_index(
                '{FTS_TABLE}',
                'content_id',
                'content_text', 'symbol', 'section_name',
                overwrite=1
            )
        """)
        context.log.info("FTS index created successfully")

        # Get total indexed rows
        total_row = conn.execute(f"SELECT COUNT(*) FROM {FTS_TABLE}").fetchone()
        total_indexed = total_row[0] if total_row else 0

        return dg.MaterializeResult(
            metadata={
                "status": "completed",
                "total_indexed_sections": total_indexed,
                "new_sections_added": len(new_content),
            }
        )

    finally:
        if conn:
            conn.close()
