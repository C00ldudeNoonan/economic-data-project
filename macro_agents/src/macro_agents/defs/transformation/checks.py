import os

import dagster as dg

from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


def _dbt_dataset(base_name: str) -> str:
    environment = os.getenv("ENVIRONMENT", "dev")
    suffix = {"prod": "", "staging": "_staging"}.get(environment, "_dev")
    return f"{base_name}{suffix}"


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


@dg.asset_check(asset="current_data_coverage")
def current_data_coverage_health_check(
    bq: BigQueryWarehouseResource,
) -> dg.AssetCheckResult:
    """Validate semantic-layer source coverage has no stale or missing sources."""
    table_ref = f"{bq.project}.{_dbt_dataset('economics_marts')}.current_data_coverage"
    if not bq.table_exists(table_ref):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": f"{table_ref} table does not exist"},
        )

    df = bq.execute_query(
        f"""
        SELECT
            source_name,
            source_domain,
            coverage_date,
            coverage_pct,
            freshness_lag_days,
            coverage_status
        FROM `{table_ref}`
        WHERE coverage_status != 'healthy'
        ORDER BY
            CASE coverage_status
                WHEN 'stale' THEN 1
                WHEN 'coverage_gap' THEN 2
                WHEN 'partial' THEN 3
                WHEN 'lagging' THEN 4
                ELSE 5
            END,
            source_name
        """,
        read_only=True,
    )

    if df.is_empty():
        return dg.AssetCheckResult(
            passed=True,
            metadata={"table": table_ref, "unhealthy_sources": 0},
        )

    unhealthy_sources = df.to_dicts()
    statuses = {row["coverage_status"] for row in unhealthy_sources}
    has_error = "stale" in statuses
    has_warning = bool(statuses & {"coverage_gap", "partial", "lagging"})

    return dg.AssetCheckResult(
        passed=not (has_error or has_warning),
        severity=(
            dg.AssetCheckSeverity.ERROR if has_error else dg.AssetCheckSeverity.WARN
        ),
        metadata={
            "table": table_ref,
            "unhealthy_sources": unhealthy_sources,
            "unhealthy_source_count": len(unhealthy_sources),
        },
    )


transformation_checks = [
    fci_data_check,
    fci_weights_data_check,
    current_data_coverage_health_check,
]
