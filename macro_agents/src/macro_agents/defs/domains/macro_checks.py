from datetime import date, timedelta

import dagster as dg

from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


@dg.asset_check(asset="fred_raw")
def fred_raw_data_check(bq: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate FRED data has recent observations and numeric values."""
    if not bq.table_exists("fred_raw"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "fred_raw table does not exist"},
        )

    df = bq.execute_query(
        """
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT series_code) AS series_count,
            MAX(date) AS max_date,
            SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS null_values
        FROM fred_raw
        """,
        read_only=True,
    )

    if df.is_empty() or df["row_count"][0] == 0:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "fred_raw table is empty"},
        )

    row_count = int(df["row_count"][0])
    series_count = int(df["series_count"][0])
    max_date = str(df["max_date"][0])
    null_values = int(df["null_values"][0])
    null_pct = round(null_values / row_count * 100, 2) if row_count > 0 else 0

    passed = series_count > 0 and null_pct < 10

    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "row_count": row_count,
            "series_count": series_count,
            "max_date": max_date,
            "null_value_pct": null_pct,
        },
    )


@dg.asset_check(asset="treasury_yields_raw")
def treasury_yields_data_check(bq: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate treasury yields data exists and has recent data."""
    if not bq.table_exists("treasury_yields_raw"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "treasury_yields_raw table does not exist"},
        )

    cutoff = date.today() - timedelta(days=30)
    df = bq.execute_query(
        f"""
        SELECT
            COUNT(*) AS row_count,
            MAX(date) AS max_date,
            (CASE WHEN bc_10year IS NOT NULL THEN 1 ELSE 0 END
             + CASE WHEN bc_2year IS NOT NULL THEN 1 ELSE 0 END
             + CASE WHEN bc_5year IS NOT NULL THEN 1 ELSE 0 END
             + CASE WHEN bc_30year IS NOT NULL THEN 1 ELSE 0 END) AS key_series_populated
        FROM treasury_yields_raw
        WHERE CAST(date AS DATE) >= '{cutoff}'
        LIMIT 1
        """,
        read_only=True,
    )

    if df.is_empty() or df["row_count"][0] == 0:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.WARN,
            metadata={
                "error": "No treasury yields data in the last 30 days",
                "cutoff": str(cutoff),
            },
        )

    return dg.AssetCheckResult(
        passed=True,
        metadata={
            "recent_row_count": int(df["row_count"][0]),
            "max_date": str(df["max_date"][0]),
            "key_series_populated": int(df["key_series_populated"][0]),
        },
    )


@dg.asset_check(asset="fomc_minutes_raw")
def fomc_minutes_data_check(bq: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate FOMC minutes data exists with recent meeting dates."""
    if not bq.table_exists("fomc_minutes_raw"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "fomc_minutes_raw table does not exist"},
        )

    df = bq.execute_query(
        """
        SELECT
            COUNT(*) AS row_count,
            MAX(meeting_date) AS latest_meeting,
            SUM(CASE WHEN gcs_path IS NULL OR gcs_path = '' THEN 1 ELSE 0 END) AS missing_gcs_path
        FROM fomc_minutes_raw
        """,
        read_only=True,
    )

    if df.is_empty() or df["row_count"][0] == 0:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "fomc_minutes_raw table is empty"},
        )

    row_count = int(df["row_count"][0])
    missing_gcs_path = int(df["missing_gcs_path"][0])

    return dg.AssetCheckResult(
        passed=missing_gcs_path == 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "row_count": row_count,
            "latest_meeting": str(df["latest_meeting"][0]),
            "missing_gcs_path_count": missing_gcs_path,
        },
    )


macro_checks = [
    fred_raw_data_check,
    treasury_yields_data_check,
    fomc_minutes_data_check,
]
