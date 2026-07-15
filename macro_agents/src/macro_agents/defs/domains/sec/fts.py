"""BigQuery full-text search source for SEC filings.

Maintains a denormalized ``sec_filing_fts_content`` table over
sec_filing_content and (best-effort) a BigQuery SEARCH INDEX, enabling
keyword queries like "find 10-K filings mentioning supply chain risk" via
the ``SEARCH()`` function. Complements the vector/semantic search in search.py.
"""

import dagster as dg
from metaxy.ext.dagster import metaxify
from metaxy.metadata_store.base import MetadataStore

from macro_agents.defs.domains.sec import lineage  # noqa: F401 — register features
from macro_agents.defs.domains.sec.tables import (
    ensure_sec_filing_content_table,
    ensure_sec_filing_fts_content_table,
)
from macro_agents.defs.domains.sec.text import sec_filing_text_extracted
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource

FTS_TABLE = "sec_filing_fts_content"
FTS_SEARCH_INDEX = "sec_filing_fts_content_search_idx"


@metaxify(key="sec_filing_fts_index")
@dg.asset(
    group_name="transformation",
    kinds={"bigquery", "search"},
    deps=[sec_filing_text_extracted],
    description=(
        "Build BigQuery full-text search source over SEC filing content. "
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
    Build and refresh the full-text search source table.

    Steps:
    1. Populate a denormalized FTS source table with content + metadata
    2. Download section text from GCS for rows missing content_text
    3. Ensure a BigQuery SEARCH INDEX exists (best-effort) so SEARCH() queries
       are index-accelerated; SEARCH() still works via full scan without it.
    """
    conn = None
    metaxy_stale_count: int | None = None
    metaxy_divergence_count: int | None = None
    metaxy_shadow_error: str | None = None
    try:
        conn = bq.get_connection()
        ensure_sec_filing_content_table(conn, bq.dataset)
        ensure_sec_filing_fts_content_table(conn, bq.dataset)

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

                    bq.execute_query(
                        f"""
                        MERGE {FTS_TABLE} AS target
                        USING (
                            SELECT
                                @content_id AS content_id,
                                @filing_id AS filing_id,
                                @symbol AS symbol,
                                @form_type AS form_type,
                                @filing_date AS filing_date,
                                @section_name AS section_name,
                                @content_text AS content_text,
                                @word_count AS word_count
                        ) AS source
                        ON target.content_id = source.content_id
                        WHEN NOT MATCHED THEN INSERT (
                            content_id, filing_id, symbol, form_type,
                            filing_date, section_name, content_text, word_count
                        ) VALUES (
                            source.content_id, source.filing_id, source.symbol,
                            source.form_type, source.filing_date,
                            source.section_name, source.content_text,
                            source.word_count
                        )
                        """,
                        read_only=False,
                        params={
                            "content_id": row["content_id"],
                            "filing_id": row["filing_id"],
                            "symbol": row["symbol"],
                            "form_type": row["form_type"],
                            "filing_date": row["filing_date"],
                            "section_name": row["section_name"],
                            "content_text": content_text,
                            "word_count": row["word_count"],
                        },
                    )
                    rows_inserted += 1

                except Exception as e:
                    errors.append(f"{row['symbol']}/{row['section_name']}: {e}")
                    continue
            context.log.info(
                f"Inserted {rows_inserted} rows into FTS table, {len(errors)} errors"
            )

        # Ensure a BigQuery SEARCH INDEX over content_text (best-effort).
        # SEARCH() works without an index (full scan), so index failures —
        # unsupported region, quota, permissions — must not fail the asset.
        search_index_status = "created_or_exists"
        try:
            conn.query(
                f"""
                CREATE SEARCH INDEX IF NOT EXISTS {FTS_SEARCH_INDEX}
                ON `{conn.project}.{bq.dataset}.{FTS_TABLE}`(content_text)
                """
            ).result()
            context.log.info("FTS search index ensured")
        except Exception as e:  # noqa: BLE001 — index is an optimization, not required
            search_index_status = f"skipped: {type(e).__name__}"
            context.log.warning(
                f"Could not create SEARCH INDEX (SEARCH() still works): {e}"
            )

        # Get total indexed rows
        total_row = bq.fetchone(f"SELECT COUNT(*) FROM {FTS_TABLE}")
        total_indexed = total_row[0] if total_row else 0

        return dg.MaterializeResult(
            metadata={
                "status": "completed",
                "total_indexed_sections": total_indexed,
                "new_sections_added": len(new_content),
                "search_index_status": search_index_status,
                "metaxy_stale_count": metaxy_stale_count,
                "metaxy_divergence_count": metaxy_divergence_count,
                "metaxy_shadow_error": metaxy_shadow_error,
            }
        )

    finally:
        if conn:
            conn.close()
