from datetime import date

import dagster as dg

from macro_agents.defs.resources.motherduck import MotherDuckResource


@dg.asset_check(asset="housing_inventory_raw")
def housing_inventory_data_check(md: MotherDuckResource) -> dg.AssetCheckResult:
    """Validate housing inventory data is non-empty and has current year data."""
    if not md.table_exists("housing_inventory_raw"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "housing_inventory_raw table does not exist"},
        )

    current_year = str(date.today().year)
    df = md.execute_query(
        f"""
        SELECT
            COUNT(*) AS row_count,
            MAX(year) AS max_year,
            COUNT(DISTINCT year) AS year_count
        FROM housing_inventory_raw
        WHERE year = '{current_year}'
        """,
        read_only=True,
    )

    row_count = int(df["row_count"][0]) if not df.is_empty() else 0
    passed = row_count > 0

    metadata = {"current_year_rows": row_count, "year_checked": current_year}
    if passed:
        metadata["max_year"] = str(df["max_year"][0])
        metadata["year_count"] = int(df["year_count"][0])

    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.WARN,
        metadata=metadata,
    )


@dg.asset_check(asset="housing_pulse_raw")
def housing_pulse_data_check(md: MotherDuckResource) -> dg.AssetCheckResult:
    """Validate housing pulse data has current-year cycles."""
    if not md.table_exists("housing_pulse_raw"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "housing_pulse_raw table does not exist"},
        )

    current_year = str(date.today().year)
    df = md.execute_query(
        f"""
        SELECT
            COUNT(*) AS row_count,
            MAX(week_ending) AS max_week
        FROM housing_pulse_raw
        WHERE CAST(week_ending AS VARCHAR) LIKE '{current_year}%'
        """,
        read_only=True,
    )

    row_count = int(df["row_count"][0]) if not df.is_empty() else 0
    passed = row_count > 0

    metadata = {
        "current_year_rows": row_count,
        "year_checked": current_year,
    }
    if passed:
        metadata["max_week"] = str(df["max_week"][0])

    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.WARN,
        metadata=metadata,
    )


@dg.asset_check(asset="realtor_raw")
def realtor_data_check(md: MotherDuckResource) -> dg.AssetCheckResult:
    """Validate realtor.com data exists across geographic levels."""
    expected_tables = [
        "RDC_Inventory_Core_Metrics_Country_History",
        "RDC_Inventory_Core_Metrics_State_History",
        "RDC_Inventory_Core_Metrics_Metro_History",
        "RDC_Inventory_Core_Metrics_County_History",
        "RDC_Inventory_Core_Metrics_Zip_History",
    ]

    found_tables = []
    total_rows = 0

    for table_name in expected_tables:
        if md.table_exists(table_name):
            found_tables.append(table_name)
            df = md.execute_query(
                f'SELECT COUNT(*) AS row_count FROM "{table_name}"',
                read_only=True,
            )
            if not df.is_empty():
                total_rows += int(df["row_count"][0])

    return dg.AssetCheckResult(
        passed=len(found_tables) > 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "tables_found": len(found_tables),
            "tables_expected": len(expected_tables),
            "total_rows": total_rows,
            "found": ", ".join(found_tables) if found_tables else "none",
        },
    )


housing_checks = [
    housing_inventory_data_check,
    housing_pulse_data_check,
    realtor_data_check,
]
