from datetime import date, timedelta

import dagster as dg

from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


def _check_signal_table(
    table_name: str,
    value_column: str,
    bq: BigQueryWarehouseResource,
    freshness_days: int = 14,
) -> dg.AssetCheckResult:
    """Reusable check for signal tables: freshness, non-null values, valid range."""
    if not bq.table_exists(table_name):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": f"{table_name} table does not exist"},
        )

    cols_df = bq.execute_query(
        f"SELECT column_name FROM (DESCRIBE {table_name})", read_only=True
    )
    actual_columns = cols_df["column_name"].to_list()
    if value_column not in actual_columns:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={
                "error": f"Column '{value_column}' not found in {table_name}",
                "available_columns": ", ".join(actual_columns),
            },
        )

    cutoff = date.today() - timedelta(days=freshness_days)
    df = bq.execute_query(
        f"""
        SELECT
            COUNT(*) AS row_count,
            MAX(date) AS max_date,
            SUM(CASE WHEN {value_column} IS NULL THEN 1 ELSE 0 END) AS null_values,
            SUM(CASE WHEN ISNAN({value_column}) OR ISINF({value_column}) THEN 1 ELSE 0 END) AS invalid_values
        FROM {table_name}
        WHERE CAST(date AS DATE) >= '{cutoff}'
        """,
        read_only=True,
    )

    row_count = int(df["row_count"][0]) if not df.is_empty() else 0
    if row_count == 0:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.WARN,
            metadata={
                "error": f"No data in last {freshness_days} days",
                "cutoff": str(cutoff),
            },
        )

    null_values = int(df["null_values"][0])
    invalid_values = int(df["invalid_values"][0])
    passed = null_values == 0 and invalid_values == 0

    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "recent_row_count": row_count,
            "max_date": str(df["max_date"][0]),
            "null_values": null_values,
            "invalid_values_nan_inf": invalid_values,
        },
    )


@dg.asset_check(asset="absorption_ratio_signals")
def absorption_ratio_data_check(bq: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate absorption ratio signal has recent, valid values."""
    return _check_signal_table(
        "absorption_ratio_signals", "absorption_ratio", md, freshness_days=14
    )


@dg.asset_check(asset="turbulence_index_signals")
def turbulence_index_data_check(bq: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate turbulence index signal has recent, valid values."""
    return _check_signal_table(
        "turbulence_index_signals", "turbulence_raw", md, freshness_days=7
    )


@dg.asset_check(asset="fear_greed_signals")
def fear_greed_data_check(bq: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate fear & greed signal has recent, valid values."""
    return _check_signal_table(
        "fear_greed_signals", "fear_greed_score", md, freshness_days=7
    )


@dg.asset_check(asset="entropy_complexity_signals")
def entropy_complexity_data_check(bq: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate entropy complexity signal has recent, valid values."""
    return _check_signal_table(
        "entropy_complexity_signals", "spy_perm_entropy", md, freshness_days=7
    )


@dg.asset_check(asset="network_correlation_signals")
def network_correlation_data_check(bq: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate network correlation signal has recent, valid values."""
    return _check_signal_table(
        "network_correlation_signals", "mst_total_length", md, freshness_days=14
    )


signal_checks = [
    absorption_ratio_data_check,
    turbulence_index_data_check,
    fear_greed_data_check,
    entropy_complexity_data_check,
    network_correlation_data_check,
]
