import re

import dagster as dg
import polars as pl

from macro_agents.defs.domains.sec.config import BATCH_SIZE_STANDARD
from macro_agents.defs.domains.sec.helpers import build_filing_gcs_path
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource

# Old path format: sec_filings/{form_type}/{year}/{cik}/{accession}/...
# New path format: sec_filings/{symbol}/{form_type}/{year}/{cik}/{accession}/...
_OLD_PATH_RE = re.compile(
    r"^sec_filings/(10-[KQ](?:/A|-A)?)/(\d{4}|unknown)/(\d+)/(\d+)/"
)


def _is_old_format(gcs_path: str | None, symbol: str) -> bool:
    """Check if a GCS path uses the old format (no symbol segment)."""
    if not gcs_path:
        return False
    # New format has symbol right after sec_filings/
    return not gcs_path.startswith(f"sec_filings/{symbol}/")


@dg.asset(
    name="sec_filing_gcs_migration",
    group_name="sec_maintenance",
    kinds={"gcs", "bigquery"},
    description="Migrate SEC filing GCS paths from old format to new symbol-based format",
)
def sec_filing_gcs_migration(
    context: dg.AssetExecutionContext,
    gcs: GCSResource,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """Migrate GCS objects and DB paths from old to new format.

    Old: sec_filings/{form_type}/{year}/{cik}/{accession}/...
    New: sec_filings/{symbol}/{form_type}/{year}/{cik}/{accession}/...

    Idempotent: skips filings already at the new path.
    """
    # sec_filing_content only exists once the text-extraction pipeline
    # has populated it; skip content migration when it's absent.
    migrate_content = bq.table_exists("sec_filing_content")
    if not migrate_content:
        context.log.warning(
            "sec_filing_content table does not exist — skipping extracted "
            "content path migration"
        )

    filings_df = bq.execute_query("""
        SELECT filing_id, cik, symbol, accession_number, form_type,
               filing_date, primary_document, gcs_path
        FROM sec_filings
        WHERE gcs_path IS NOT NULL
        AND processed = TRUE
        """)

    if filings_df.is_empty():
        context.log.info("No processed filings to check for migration")
        return dg.MaterializeResult(
            metadata={"status": "no_filings", "migrated": 0, "skipped": 0}
        )

    to_migrate = []
    for row in filings_df.iter_rows(named=True):
        if _is_old_format(row["gcs_path"], row["symbol"]):
            to_migrate.append(row)

    if not to_migrate:
        context.log.info("All filings already use new path format")
        return dg.MaterializeResult(
            metadata={
                "status": "up_to_date",
                "migrated": 0,
                "skipped": len(filings_df),
            }
        )

    context.log.info(f"Found {len(to_migrate)} filings to migrate")

    batch_size = BATCH_SIZE_STANDARD
    total_migrated = 0
    total_errors = 0
    error_details: list[str] = []

    for i in range(0, len(to_migrate), batch_size):
        batch = to_migrate[i : i + batch_size]

        for row in batch:
            filing_id = row["filing_id"]
            symbol = row["symbol"]
            old_gcs_path = row["gcs_path"]

            try:
                new_base = build_filing_gcs_path(
                    symbol=symbol,
                    form_type=row["form_type"],
                    filing_date=row["filing_date"],
                    cik=row["cik"],
                    accession=row["accession_number"],
                )
                primary_doc = row["primary_document"]
                new_gcs_path = f"{new_base}/{primary_doc}"

                # Copy primary document in GCS
                if not gcs.file_exists(new_gcs_path, context=context):
                    data = gcs.download_json(old_gcs_path, context=context)
                    if data:
                        gcs.upload_json(new_gcs_path, data, context=context)

                # Update sec_filings row
                bq.execute_query(
                    """
                    UPDATE sec_filings
                    SET gcs_path = @gcs_path
                    WHERE symbol = @symbol AND filing_id = @filing_id
                    """,
                    read_only=False,
                    params={
                        "gcs_path": new_gcs_path,
                        "symbol": symbol,
                        "filing_id": filing_id,
                    },
                )

                # Migrate extracted content paths
                if migrate_content:
                    content_df = bq.execute_query(
                        """
                        SELECT content_id, gcs_path
                        FROM sec_filing_content
                        WHERE filing_id = @filing_id AND gcs_path IS NOT NULL
                        """,
                        params={"filing_id": filing_id},
                    )
                else:
                    content_df = pl.DataFrame(
                        schema={"content_id": pl.Utf8, "gcs_path": pl.Utf8}
                    )

                for content_row in content_df.iter_rows(named=True):
                    old_content_path = content_row["gcs_path"]
                    if not old_content_path or not _is_old_format(
                        old_content_path, symbol
                    ):
                        continue

                    # Derive new content path: replace old base with new base
                    # Old: sec_filings/{form_type}/{year}/{cik}/{acc}/extracted/...
                    # New: sec_filings/{symbol}/{form_type}/{year}/{cik}/{acc}/extracted/...
                    old_base_match = _OLD_PATH_RE.match(old_content_path)
                    if old_base_match:
                        suffix = old_content_path[old_base_match.end() :]
                        new_content_path = f"{new_base}/{suffix}"
                    else:
                        # Fallback: replace the non-symbol prefix
                        new_content_path = old_content_path.replace(
                            old_gcs_path.rsplit("/", 1)[0],
                            new_base,
                            1,
                        )

                    if not gcs.file_exists(new_content_path, context=context):
                        content_data = gcs.download_json(
                            old_content_path, context=context
                        )
                        if content_data:
                            gcs.upload_json(
                                new_content_path, content_data, context=context
                            )

                    bq.execute_query(
                        """
                        UPDATE sec_filing_content
                        SET gcs_path = @gcs_path
                        WHERE content_id = @content_id
                        """,
                        read_only=False,
                        params={
                            "gcs_path": new_content_path,
                            "content_id": content_row["content_id"],
                        },
                    )
                total_migrated += 1
                context.log.debug(f"Migrated {symbol}/{filing_id}")

            except Exception as e:
                error_msg = f"Error migrating {symbol}/{filing_id}: {e}"
                context.log.error(error_msg)
                error_details.append(error_msg)
                total_errors += 1

        context.log.info(
            f"Batch progress: {min(i + batch_size, len(to_migrate))}/{len(to_migrate)}"
        )

    remaining = len(to_migrate) - total_migrated - total_errors
    return dg.MaterializeResult(
        metadata={
            "status": "completed",
            "migrated": total_migrated,
            "errors": total_errors,
            "remaining": remaining,
            "error_details": error_details[:10],
        }
    )
