from datetime import datetime, timezone

import dagster as dg
import polars as pl

from macro_agents.defs.domains.sec.gcs_schema import CATALOG_PATH
from macro_agents.defs.domains.sec.helpers import PIPELINE_VERSION
from macro_agents.defs.domains.sec.metadata import sec_filing_metadata
from macro_agents.defs.domains.sec.tables import ensure_sec_filing_markdown_table
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


def _derive_base_gcs_path(gcs_path: str | None) -> str | None:
    """Strip the trailing primary-document filename to get the base filing path."""
    if not gcs_path:
        return None
    # gcs_path looks like sec_filings/.../accession/primary_doc.htm
    last_slash = gcs_path.rfind("/")
    return gcs_path[:last_slash] if last_slash > 0 else gcs_path


def build_company_manifest(
    symbol: str,
    company_name: str,
    cik: str,
    filings: list[dict],
) -> dict:
    """Build a manifest dict for a single company."""
    return {
        "symbol": symbol,
        "company_name": company_name,
        "cik": cik,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pipeline_version": PIPELINE_VERSION,
        "filing_count": len(filings),
        "filings": [
            {
                "filing_id": f["filing_id"],
                "accession_number": f.get("accession_number"),
                "form_type": f["form_type"],
                "filing_date": str(f["filing_date"]) if f["filing_date"] else None,
                "gcs_path": f["gcs_path"],
                "base_gcs_path": _derive_base_gcs_path(f["gcs_path"]),
                "markdown_gcs_path": f.get("markdown_gcs_path"),
                "processed": bool(f["processed"]),
                "section_count": f["section_count"] or 0,
            }
            for f in filings
        ],
    }


@dg.asset(
    name="sec_filing_gcs_manifest",
    group_name="sec_ingestion",
    kinds={"gcs", "duckdb"},
    deps=[sec_filing_metadata],
    description="Generate per-company manifest.json files in GCS listing all filings",
)
def sec_filing_gcs_manifest(
    context: dg.AssetExecutionContext,
    gcs: GCSResource,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """Build a manifest.json per company symbol under sec_filings/{symbol}/manifest.json."""
    conn = None
    try:
        conn = bq.get_connection()
        ensure_sec_filing_markdown_table(conn)

        companies_df = bq.execute_query("""
            SELECT c.symbol, c.company_name, c.cik
            FROM sec_company_cik c
            WHERE c.symbol IS NOT NULL
            ORDER BY c.symbol
            """)

        if companies_df.is_empty():
            context.log.info("No companies found in sec_company_cik")
            return dg.MaterializeResult(
                metadata={"status": "no_companies", "manifests_written": 0}
            )

        filings_df = bq.execute_query("""
            SELECT f.symbol, f.filing_id, f.accession_number, f.cik,
                   f.form_type, f.filing_date,
                   f.gcs_path, f.processed,
                   (SELECT COUNT(*) FROM sec_filing_content c
                    WHERE c.filing_id = f.filing_id) AS section_count,
                   m.markdown_gcs_path
            FROM sec_filings f
            LEFT JOIN sec_filing_markdown m ON m.filing_id = f.filing_id
            WHERE f.symbol IS NOT NULL
            ORDER BY f.symbol, f.filing_date DESC
            """)

        filings_by_symbol: dict[str, list[dict]] = {}
        for row in filings_df.iter_rows(named=True):
            symbol = row["symbol"]
            if symbol not in filings_by_symbol:
                filings_by_symbol[symbol] = []
            filings_by_symbol[symbol].append(row)

        company_info: dict[str, dict[str, str]] = {}
        for row in companies_df.iter_rows(named=True):
            company_info[row["symbol"]] = {
                "company_name": row["company_name"] or "",
                "cik": row.get("cik", ""),
            }

        manifests_written = 0
        errors = 0

        for row in companies_df.iter_rows(named=True):
            symbol = row["symbol"]
            info = company_info.get(symbol, {"company_name": "", "cik": ""})

            try:
                symbol_filings = filings_by_symbol.get(symbol, [])
                manifest = build_company_manifest(
                    symbol, info["company_name"], info["cik"], symbol_filings
                )

                manifest_path = f"sec_filings/{symbol}/manifest.json"
                gcs.upload_json(manifest_path, manifest, context=context)
                manifests_written += 1

            except Exception as e:
                context.log.error(f"Error writing manifest for {symbol}: {e}")
                errors += 1

        context.log.info(f"Wrote {manifests_written} manifests ({errors} errors)")

        return dg.MaterializeResult(
            metadata={
                "status": "completed",
                "manifests_written": manifests_written,
                "errors": errors,
                "total_companies": len(companies_df),
            }
        )

    finally:
        if conn:
            conn.close()


@dg.asset(
    name="sec_filing_gcs_catalog",
    group_name="sec_ingestion",
    kinds={"gcs", "duckdb"},
    deps=[sec_filing_gcs_manifest],
    description="Generate root-level catalog.json with CIK and accession indexes for discoverability",
)
def sec_filing_gcs_catalog(
    context: dg.AssetExecutionContext,
    gcs: GCSResource,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """Build a catalog.json at sec_filings/catalog.json for cross-system lookups.

    Contains:
    - companies: symbol -> company info + manifest pointer
    - cik_index: CIK -> symbol for CIK-based discovery
    - accession_index: dashed accession -> symbol/form_type/gcs_path
    """
    conn = None
    try:
        conn = bq.get_connection()

        companies_df = bq.execute_query("""
            SELECT c.symbol, c.company_name, c.cik,
                   (SELECT COUNT(*) FROM sec_filings f
                    WHERE f.symbol = c.symbol) AS filing_count
            FROM sec_company_cik c
            WHERE c.symbol IS NOT NULL
            ORDER BY c.symbol
            """)

        filings_df = bq.execute_query("""
            SELECT f.symbol, f.accession_number, f.cik,
                   f.form_type, f.filing_date, f.gcs_path
            FROM sec_filings f
            WHERE f.symbol IS NOT NULL
            AND f.gcs_path IS NOT NULL
            """)

        # Build company directory
        companies: dict[str, dict] = {}
        cik_index: dict[str, str] = {}

        for row in companies_df.iter_rows(named=True):
            symbol = row["symbol"]
            cik = row.get("cik", "")
            companies[symbol] = {
                "cik": cik,
                "company_name": row["company_name"] or "",
                "filing_count": row["filing_count"],
                "manifest_path": f"sec_filings/{symbol}/manifest.json",
            }
            if cik:
                cik_index[cik] = symbol

        # Build accession index (dashed accession -> filing info)
        accession_index: dict[str, dict] = {}
        for row in filings_df.iter_rows(named=True):
            accession = row.get("accession_number")
            if not accession:
                continue
            gcs_path = row["gcs_path"]
            base_path = _derive_base_gcs_path(gcs_path)
            accession_index[accession] = {
                "symbol": row["symbol"],
                "form_type": row["form_type"],
                "filing_date": str(row["filing_date"]) if row["filing_date"] else None,
                "gcs_path": base_path,
            }

        catalog = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "pipeline_version": PIPELINE_VERSION,
            "company_count": len(companies),
            "filing_count": len(accession_index),
            "companies": companies,
            "cik_index": cik_index,
            "accession_index": accession_index,
        }

        gcs.upload_json(CATALOG_PATH, catalog, context=context)
        context.log.info(
            f"Wrote catalog: {len(companies)} companies, "
            f"{len(cik_index)} CIKs, {len(accession_index)} accessions"
        )

        return dg.MaterializeResult(
            metadata={
                "status": "completed",
                "company_count": len(companies),
                "cik_index_entries": len(cik_index),
                "accession_index_entries": len(accession_index),
            }
        )

    finally:
        if conn:
            conn.close()
