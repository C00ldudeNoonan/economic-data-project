"""Data quality verification assets using Google Sheets and GOOGLEFINANCE."""

import dagster as dg
import polars as pl

from macro_agents.defs.resources.google_sheets import GoogleSheetsResource
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource

DATA_QUALITY_GROUP = "data_quality"


@dg.asset(
    group_name=DATA_QUALITY_GROUP,
    kinds={"google_sheets", "duckdb"},
    description=(
        "Write data quality anomalies from MotherDuck to a Google Sheet "
        "with GOOGLEFINANCE formulas for price verification."
    ),
    deps=[dg.AssetKey("data_quality_anomalies")],
)
def dq_anomalies_to_sheets(
    context: dg.AssetExecutionContext,
    md: BigQueryWarehouseResource,
    google_sheets: GoogleSheetsResource,
) -> dg.MaterializeResult:
    """Export anomalies from the dbt data_quality_anomalies table to Google Sheets.

    The sheet includes GOOGLEFINANCE formulas so the user can verify
    flagged prices against Google's data. After review, the user marks
    rows as verified='yes' and the corrections can be read back.
    """
    if not md.table_exists("data_quality_anomalies"):
        context.log.warning("data_quality_anomalies table does not exist yet")
        return dg.MaterializeResult(
            metadata={"rows_written": 0, "status": "table not found"}
        )

    anomalies_df = md.execute_query(
        "SELECT * FROM data_quality_anomalies ORDER BY source_table, symbol, date"
    )

    context.log.info(f"Found {len(anomalies_df)} anomalies to export")

    rows_written = google_sheets.write_anomalies(anomalies_df)

    return dg.MaterializeResult(
        metadata={
            "rows_written": rows_written,
            "check_types": str(
                anomalies_df["check_type"].unique().to_list()
                if not anomalies_df.is_empty()
                else []
            ),
            "source_tables": str(
                anomalies_df["source_table"].unique().to_list()
                if not anomalies_df.is_empty()
                else []
            ),
        }
    )


@dg.asset(
    group_name=DATA_QUALITY_GROUP,
    kinds={"google_sheets", "duckdb"},
    description=(
        "Read verified corrections from Google Sheets and update "
        "the corresponding rows in MotherDuck raw tables."
    ),
    deps=[dg.AssetKey("dq_anomalies_to_sheets")],
)
def dq_apply_corrections(
    context: dg.AssetExecutionContext,
    md: BigQueryWarehouseResource,
    google_sheets: GoogleSheetsResource,
) -> dg.MaterializeResult:
    """Read back verified corrections from Google Sheets and update MotherDuck.

    Only processes rows where verified='yes'. Uses the GOOGLEFINANCE values
    (gf_close, gf_open, gf_high, gf_low) to overwrite the flagged prices
    in the raw source tables.
    """
    corrections = google_sheets.read_verified_corrections()

    if corrections.is_empty():
        context.log.info("No verified corrections to apply")
        return dg.MaterializeResult(
            metadata={"corrections_applied": 0, "status": "no verified rows"}
        )

    total_applied = 0

    # Group corrections by source table
    for source_table in corrections["source_table"].unique().to_list():
        table_corrections = corrections.filter(pl.col("source_table") == source_table)

        # Determine if this is a commodity table or OHLC table
        is_commodity = "commodities" in source_table and source_table not in (
            "us_sector_etfs_raw",
            "currency_etfs_raw",
            "major_indices_raw",
            "fixed_income_etfs_raw",
            "global_markets_raw",
            "sp500_companies_prices_raw",
            "nasdaq_companies_prices_raw",
        )

        for row in table_corrections.iter_rows(named=True):
            symbol = row.get("symbol", "")
            date_val = row.get("date", "")
            gf_close = row.get("gf_close")
            gf_open = row.get("gf_open")
            gf_high = row.get("gf_high")
            gf_low = row.get("gf_low")

            if not symbol or not date_val:
                context.log.warning(f"Skipping row with missing symbol/date: {row}")
                continue

            if is_commodity:
                if gf_close and str(gf_close).strip():
                    query = (
                        f"UPDATE {source_table} "
                        f"SET commodity_price = '{gf_close}' "
                        f"WHERE commodity_name = '{symbol}' "
                        f"AND CAST(date AS DATE) = '{date_val}'"
                    )
                    md.execute_query(query, read_only=False)
                    total_applied += 1
            else:
                # Build SET clause for non-empty GOOGLEFINANCE values
                set_parts = []
                if gf_close and str(gf_close).strip():
                    set_parts.append(f"close = {gf_close}, adj_close = {gf_close}")
                if gf_open and str(gf_open).strip():
                    set_parts.append(f"open = {gf_open}, adj_open = {gf_open}")
                if gf_high and str(gf_high).strip():
                    set_parts.append(f"high = {gf_high}, adj_high = {gf_high}")
                if gf_low and str(gf_low).strip():
                    set_parts.append(f"low = {gf_low}, adj_low = {gf_low}")

                if set_parts:
                    set_clause = ", ".join(set_parts)
                    query = (
                        f"UPDATE {source_table} "
                        f"SET {set_clause} "
                        f"WHERE symbol = '{symbol}' "
                        f"AND CAST(date AS DATE) = '{date_val}'"
                    )
                    md.execute_query(query, read_only=False)
                    total_applied += 1

        context.log.info(
            f"Applied {len(table_corrections)} corrections to {source_table}"
        )

    return dg.MaterializeResult(
        metadata={
            "corrections_applied": total_applied,
            "tables_updated": str(corrections["source_table"].unique().to_list()),
        }
    )
