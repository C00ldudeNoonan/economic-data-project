"""SEC filing document download assets.

Provides two assets for downloading SEC filing documents to GCS:
1. sec_filing_documents — dynamically partitioned by filing_id for on-demand downloads
2. sec_filing_documents_batch — unpartitioned batch asset for processing up to 25 filings per run
"""

import dagster as dg

from macro_agents.defs.domains.sec.config import MAX_ERROR_DETAILS
from macro_agents.defs.domains.sec.filing_downloader import FilingDownloader
from macro_agents.defs.domains.sec.metadata import sec_filing_metadata
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.resources.sec_edgar import SECEdgarResource

sec_filing_documents_partitions = dg.DynamicPartitionsDefinition(
    name="sec_filing_documents"
)


@dg.asset(
    name="sec_filing_documents",
    group_name="sec_ingestion",
    kinds={"api", "gcs", "duckdb"},
    partitions_def=sec_filing_documents_partitions,
    deps=[sec_filing_metadata],
    description="Download a single SEC filing document to GCS, partitioned by filing_id",
)
def sec_filing_documents(
    context: dg.AssetExecutionContext,
    sec_edgar: SECEdgarResource,
    gcs: GCSResource,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """Download a single SEC filing document to GCS.

    Partition key is the filing_id. Uses FilingDownloader for download logic.
    """
    filing_id = context.partition_key
    downloader = FilingDownloader(sec_edgar, gcs, bq, context.log)

    filing_df = downloader.query_filing_by_id(filing_id)

    if filing_df.is_empty():
        context.log.warning(f"No filing found for filing_id={filing_id}")
        return dg.MaterializeResult(
            metadata={
                "status": "not_found",
                "filing_id": filing_id,
                "documents_downloaded": 0,
            }
        )

    # Process all rows for this filing_id (may span multiple symbols
    # if a CIK/accession-derived ID maps to more than one symbol)
    rows = filing_df.to_dicts()
    total_downloaded = 0
    last_result = None

    for row in rows:
        if downloader.is_filing_processed(filing_id, row["symbol"]):
            context.log.debug(
                f"Filing {filing_id} already processed for {row['symbol']}"
            )
            continue

        last_result = downloader.download_filing(row)
        if last_result.status == "downloaded":
            total_downloaded += 1

    if last_result is None:
        return dg.MaterializeResult(
            metadata={
                "status": "already_processed",
                "filing_id": filing_id,
                "symbol": rows[0]["symbol"],
                "documents_downloaded": 0,
            }
        )

    return dg.MaterializeResult(
        metadata={
            "status": last_result.status,
            "filing_id": last_result.filing_id,
            "symbol": last_result.symbol,
            "gcs_path": last_result.gcs_path,
            "documents_downloaded": total_downloaded,
            "error": last_result.error,
        }
    )


@dg.asset(
    name="sec_filing_documents_batch",
    group_name="sec_ingestion",
    kinds={"api", "gcs", "duckdb"},
    deps=[sec_filing_metadata],
    description="Batch download unprocessed SEC filing documents to GCS (processes up to 25 per run)",
)
def sec_filing_documents_batch(
    context: dg.AssetExecutionContext,
    sec_edgar: SECEdgarResource,
    gcs: GCSResource,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """Download unprocessed SEC filing documents in batch mode.

    Queries sec_filings for up to 25 unprocessed filings across all companies,
    downloads each primary document to GCS, and marks them processed.
    """
    downloader = FilingDownloader(sec_edgar, gcs, bq, context.log)

    filings_to_process = downloader.query_unprocessed_filings()

    if filings_to_process.is_empty():
        context.log.debug("No unprocessed filings to download")
        return dg.MaterializeResult(
            metadata={"status": "no_filings", "documents_downloaded": 0}
        )

    context.log.info(f"Downloading documents for {len(filings_to_process)} filings")

    result = downloader.download_batch(filings_to_process)

    return dg.MaterializeResult(
        metadata={
            "status": "completed",
            "documents_downloaded": result.total_downloaded,
            "errors": result.total_errors,
            "remaining_to_process": result.remaining,
            "error_details": (
                result.error_details[:MAX_ERROR_DETAILS] if result.error_details else []
            ),
        }
    )
