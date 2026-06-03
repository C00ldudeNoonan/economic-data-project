import dagster as dg

from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


@dg.asset_check(asset="telemetry_events_raw")
def telemetry_events_data_check(bq: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate telemetry events have been replicated."""
    if not bq.table_exists("telemetry.telemetry_events_raw"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "telemetry.telemetry_events_raw table does not exist"},
        )

    df = bq.execute_query(
        """
        SELECT
            COUNT(*) AS row_count,
            MAX(created_at) AS latest_event
        FROM telemetry.telemetry_events_raw
        """,
        read_only=True,
    )

    row_count = int(df["row_count"][0]) if not df.is_empty() else 0
    return dg.AssetCheckResult(
        passed=row_count > 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "row_count": row_count,
            "latest_event": str(df["latest_event"][0]) if row_count > 0 else "N/A",
        },
    )


@dg.asset_check(asset="users_raw")
def users_raw_data_check(bq: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate users table has been replicated."""
    table_name = "telemetry.users_raw"
    try:
        df = bq.execute_query(
            f"SELECT COUNT(*) AS row_count FROM {table_name}",
            read_only=True,
        )
        row_count = int(df["row_count"][0]) if not df.is_empty() else 0
    except Exception:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.WARN,
            metadata={"error": f"{table_name} table does not exist or is inaccessible"},
        )

    return dg.AssetCheckResult(
        passed=True,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={"row_count": row_count},
    )


telemetry_checks = [
    telemetry_events_data_check,
    users_raw_data_check,
]
