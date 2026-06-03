"""Historical CIK mapping for corporate actions (renames, rebrands).

Extracts former names from SEC EDGAR submissions to track company identity
changes. Note: SEC CIK numbers do NOT change on company renames (e.g.,
Facebook→Meta kept CIK 1326801). This table records name history under the
same CIK. For companies that changed CIK via acquisition (e.g., Activision
absorbed into Microsoft), the acquired entity's CIK is a separate lookup
that would require manual mapping or an external data source.
"""

import hashlib
import json
from datetime import datetime, timezone

import dagster as dg
import polars as pl

from macro_agents.defs.domains.sec.cik import sp500_cik_enriched
from macro_agents.defs.domains.sec.config import MAX_ERROR_DETAILS
from macro_agents.defs.domains.sec.tables import ensure_sec_company_cik_history_table
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource
from macro_agents.defs.resources.sec_edgar import SECEdgarResource


def _generate_history_id(symbol: str, cik: str, effective_from: str) -> str:
    hash_input = f"{symbol}_{cik}_{effective_from}"
    return hashlib.sha256(hash_input.encode()).hexdigest()[:16]


@dg.asset(
    group_name="sec_ingestion",
    kinds={"api", "duckdb"},
    deps=[sp500_cik_enriched],
    description=(
        "Track company name history from SEC EDGAR former names. "
        "Records name changes under the same CIK (e.g., Facebook→Meta). "
        "Re-checks every 90 days for newly reported corporate actions."
    ),
)
def sec_company_cik_history(
    context: dg.AssetExecutionContext,
    sec_edgar: SECEdgarResource,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """
    Extract former names from SEC EDGAR submissions for each S&P 500 company.

    For each company in sec_company_cik, calls the SEC submissions API and
    extracts the formerNames field. Each former name becomes a history record
    linking the current symbol to the CIK that filed under that name.

    The current CIK is also stored as a 'current' record so metadata.py can
    query a single table for all CIKs to fetch.
    """
    conn = None
    try:
        conn = bq.get_connection()
        ensure_sec_company_cik_history_table(conn)

        companies_df = bq.execute_query("SELECT symbol, cik, cik_padded, company_name FROM sec_company_cik")

        if companies_df.is_empty():
            context.log.warning("No companies found in sec_company_cik")
            return dg.MaterializeResult(
                metadata={"status": "no_companies", "records_created": 0}
            )

        # Check which companies already have recent history records
        # Re-process symbols whose records are older than 90 days
        try:
            existing_df = bq.execute_query("""
                SELECT current_symbol, MAX(created_at) as last_updated
                FROM sec_company_cik_history
                GROUP BY current_symbol
                HAVING MAX(created_at) > CURRENT_TIMESTAMP - INTERVAL '90 days'
                """)
            recent_symbols = set(existing_df["current_symbol"].to_list())
        except Exception:
            recent_symbols = set()

        # Process companies without recent history
        new_companies = companies_df.filter(
            ~pl.col("symbol").is_in(list(recent_symbols))
        )

        if new_companies.is_empty():
            context.log.debug("All companies have up-to-date CIK history records")
            return dg.MaterializeResult(
                metadata={
                    "status": "up_to_date",
                    "records_created": 0,
                    "companies_skipped": len(recent_symbols),
                }
            )

        context.log.info(
            f"Processing CIK history for {len(new_companies)} new companies "
            f"({len(recent_symbols)} already up-to-date)"
        )

        now = datetime.now(timezone.utc)
        records = []
        errors = []
        companies_with_history = 0

        for row in new_companies.iter_rows(named=True):
            symbol = row["symbol"]
            cik = row["cik_padded"] or row["cik"]
            company_name = row["company_name"] or ""

            try:
                submissions = sec_edgar.get_company_submissions(cik, context)
                history = sec_edgar.extract_company_history(submissions)

                # Always add current CIK as a 'current' record
                records.append(
                    {
                        "id": _generate_history_id(symbol, cik, "current"),
                        "current_symbol": symbol,
                        "cik": str(cik).lstrip("0") or "0",
                        "cik_padded": cik,
                        "company_name": company_name,
                        "former_names": None,
                        "relationship_type": "current",
                        "effective_from": None,
                        "effective_to": None,
                        "source": "sec_edgar",
                        "created_at": now,
                    }
                )

                # Add former name records
                for former in history["former_names"]:
                    former_name = former["name"]
                    from_date = former.get("from_date", "")
                    to_date = former.get("to_date", "")

                    records.append(
                        {
                            "id": _generate_history_id(
                                symbol, cik, from_date or "unknown"
                            ),
                            "current_symbol": symbol,
                            "cik": str(cik).lstrip("0") or "0",
                            "cik_padded": cik,
                            "company_name": former_name,
                            "former_names": json.dumps(
                                [former_name], ensure_ascii=False
                            ),
                            "relationship_type": "renamed_from",
                            "effective_from": from_date or None,
                            "effective_to": to_date or None,
                            "source": "sec_edgar",
                            "created_at": now,
                        }
                    )

                if history["former_names"]:
                    companies_with_history += 1
                    context.log.debug(
                        f"{symbol}: found {len(history['former_names'])} former names"
                    )

            except Exception as e:
                error_msg = f"Error processing {symbol} (CIK: {cik}): {e}"
                context.log.warning(error_msg)
                errors.append(error_msg)
                continue

        if records:
            records_df = pl.DataFrame(records)
            # Cast date columns properly
            records_df = records_df.with_columns(
                [
                    pl.col("effective_from").cast(pl.Date, strict=False),
                    pl.col("effective_to").cast(pl.Date, strict=False),
                ]
            )
            bq.upsert_data(
                "sec_company_cik_history", records_df, ["id"], context=context
            )
            context.log.info(f"Upserted {len(records)} CIK history records")

        return dg.MaterializeResult(
            metadata={
                "status": "completed",
                "companies_processed": len(new_companies),
                "companies_with_former_names": companies_with_history,
                "records_created": len(records),
                "errors": len(errors),
                "error_details": errors[:MAX_ERROR_DETAILS] if errors else [],
            }
        )

    finally:
        if conn:
            conn.close()
