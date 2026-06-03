"""DuckDB full-text search index for SEC filings.

Creates and maintains a FTS index over sec_filing_content, enabling
keyword-based queries like "find 10-K filings mentioning supply chain risk".
Complements the vector/semantic search in search.py.
"""

import dagster as dg
import polars as pl
from metaxy.ext.dagster import metaxify
from metaxy.metadata_store.base import MetadataStore

from macro_agents.defs.domains.sec import lineage  # noqa: F401 — register features
from macro_agents.defs.domains.sec.tables import ensure_sec_filing_content_table
from macro_agents.defs.domains.sec.text import sec_filing_text_extracted
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource

FTS_TABLE = "sec_filing_fts_content"


def _ensure_fts_content_table(conn) -> None:
    """Create the denormalized FTS source table if it doesn't exist.

    DuckDB FTS indexes require a single table with the text columns.
    We join filing metadata with content text so keyword searches
    return symbol, form_type, filing_date alongside matches.
    """
    conn.query(f"""
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
    """).result()


@metaxify(key="sec_filing_fts_index")
@dg.asset(
    group_name="transformation",
    kinds={"duckdb", "search"},
    deps=[sec_filing_text_extracted],
    description=(
        "Build DuckDB full-text search index over SEC filing content. "
        "Enables keyword queries across filing sections with metadata filtering."
    ),
    metadata={"metaxy/feature": "sec/fts_index"},
)
def sec_filing_fts_index(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
    gcs: GCSResource,
    metaxy_store: dg.ResourceParam[MetadataStore],
) -> dg.MaterializeResult:
    """
    Build and refresh the full-text search index.

    Steps:
    1. Populate a denormalized FTS source table with content + metadata
    2. Download section text from GCS for rows missing content_text
    3. Create/recreate the DuckDB FTS index using PRAGMA create_fts_index
    """
    conn = None
    metaxy_stale_count: int | None = None
    metaxy_divergence_count: int | None = None
    metaxy_shadow_error: str | None = None
    try:
        conn = bq.get_connection()
        ensure_sec_filing_content_table(conn)
        _ensure_fts_content_table(conn)

        # Find content rows not yet in the FTS table
        new_content = bq.execute_query(
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
            """
        )

        # Shadow mode (issue #46): expansion lineage. Roll content-level
        # new+stale rows up to filing_ids for comparison with the
        # content-level boolean query above (collapsed via unique filing_id).
        try:
            boolean_stale_filing_ids = (
                set(new_content["filing_id"].unique().to_list())
                if not new_content.is_empty()
                else set()
            )
            with metaxy_store:
                increment = metaxy_store.resolve_update("sec/fts_index").to_polars()
                metaxy_stale_filing_ids: set[str] = set()
                for frame in (increment.new, increment.stale):
                    if "filing_id" in frame.columns:
                        metaxy_stale_filing_ids.update(
                            frame["filing_id"].unique().to_list()
                        )
            metaxy_stale_count = len(metaxy_stale_filing_ids)
            metaxy_divergence_count = len(
                metaxy_stale_filing_ids.symmetric_difference(boolean_stale_filing_ids)
            )
        except Exception as e:  # noqa: BLE001 — shadow mode must never fail the asset
            metaxy_shadow_error = f"{type(e).__name__}: {e}"
            context.log.warning(
                f"Metaxy shadow comparison failed: {metaxy_shadow_error}"
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

                    conn.query(
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
                    ).result()
                    rows_inserted += 1

                except Exception as e:
                    errors.append(f"{row['symbol']}/{row['section_name']}: {e}")
                    continue
            context.log.info(
                f"Inserted {rows_inserted} rows into FTS table, {len(errors)} errors"
            )

        # Install and load FTS extension, then create/recreate the index
        conn.query("INSTALL fts").result()
        conn.query("LOAD fts").result()

        # Drop existing index before recreating
        conn.query(
            f"PRAGMA drop_fts_index('{FTS_TABLE}')",
        ).result()

        # Create FTS index on content_text plus metadata fields
        conn.query(f"""
            PRAGMA create_fts_index(
                '{FTS_TABLE}',
                'content_id',
                'content_text', 'symbol', 'section_name',
                overwrite=1
            )
        """).result()
        context.log.info("FTS index created successfully")

        # Get total indexed rows
        total_row = bq.fetchone(f"SELECT COUNT(*) FROM {FTS_TABLE}")
        total_indexed = total_row[0] if total_row else 0

        return dg.MaterializeResult(
            metadata={
                "status": "completed",
                "total_indexed_sections": total_indexed,
                "new_sections_added": len(new_content),
                "metaxy_stale_count": metaxy_stale_count,
                "metaxy_divergence_count": metaxy_divergence_count,
                "metaxy_shadow_error": metaxy_shadow_error,
            }
        )

    finally:
        if conn:
            conn.close()
