import dagster as dg

from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


@dg.asset_check(asset="financial_conditions_index")
def fci_data_check(bq: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate financial conditions index has recent values in valid range."""
    if not bq.table_exists("financial_conditions_index"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "financial_conditions_index table does not exist"},
        )

    df = bq.execute_query(
        """
        SELECT
            COUNT(*) AS row_count,
            MAX(date) AS max_date,
            MIN(fci_value) AS min_fci,
            MAX(fci_value) AS max_fci,
            SUM(CASE WHEN fci_value IS NULL THEN 1 ELSE 0 END) AS null_values
        FROM financial_conditions_index
        """,
        read_only=True,
    )

    if df.is_empty() or df["row_count"][0] == 0:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "financial_conditions_index table is empty"},
        )

    row_count = int(df["row_count"][0])
    null_values = int(df["null_values"][0])

    return dg.AssetCheckResult(
        passed=null_values == 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "row_count": row_count,
            "max_date": str(df["max_date"][0]),
            "fci_range": f"{df['min_fci'][0]:.4f} to {df['max_fci'][0]:.4f}",
            "null_values": null_values,
        },
    )


@dg.asset_check(asset="fci_weights_config")
def fci_weights_data_check(bq: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate FCI weights configuration table exists and has entries."""
    if not bq.table_exists("fci_weights_config"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "fci_weights_config table does not exist"},
        )

    df = bq.execute_query(
        "SELECT COUNT(*) AS row_count FROM fci_weights_config",
        read_only=True,
    )

    row_count = int(df["row_count"][0]) if not df.is_empty() else 0
    return dg.AssetCheckResult(
        passed=row_count > 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={"row_count": row_count},
    )


transformation_checks = [
    fci_data_check,
    fci_weights_data_check,
]
