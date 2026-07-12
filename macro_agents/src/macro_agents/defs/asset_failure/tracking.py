"""Failure-tracking table access and asset/check discovery."""

import dagster as dg
from google.api_core.exceptions import GoogleAPIError


def initialize_failure_tracking_table(md, context):
    """Create the failure tracking table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS `asset_check_failures` (
        asset_key STRING NOT NULL,
        check_name STRING NOT NULL DEFAULT '',
        failure_type STRING NOT NULL,
        consecutive_failures INT64 NOT NULL DEFAULT 0,
        last_failure_timestamp TIMESTAMP NOT NULL,
        last_success_timestamp TIMESTAMP,
        github_issue_number INT64,
        github_issue_url STRING,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
    )
    """

    md.execute_query(create_table_query, read_only=False)
    context.log.debug("Initialized asset_check_failures table")


def get_all_asset_check_keys(context):
    """Extract all asset check keys from the current definitions."""
    from macro_agents.definitions import defs

    check_keys = []
    for check in defs.asset_checks or []:
        try:
            check_key = check.check_key
            check_keys.append(check_key)
        except AttributeError as e:
            context.log.warning(
                f"Could not extract check key from asset check {check}: {e}. Skipping."
            )
            continue

    return check_keys


def get_all_asset_keys() -> list[dg.AssetKey]:
    """Resolve asset keys without loading definitions at module import time."""
    from macro_agents.definitions import defs

    return list(defs.resolve_all_asset_keys())


def get_failure_tracking_record(
    md, asset_key: str, failure_type: str, check_name: str | None = None, context=None
):
    """Retrieve the current failure tracking record."""
    check_name_value = check_name if check_name else ""

    query = """
    SELECT * FROM asset_check_failures
    WHERE asset_key = @asset_key
      AND failure_type = @failure_type
      AND check_name = @check_name
    """

    try:
        result = md.execute_query(
            query,
            read_only=True,
            params={
                "asset_key": asset_key,
                "failure_type": failure_type,
                "check_name": check_name_value,
            },
        )
        if len(result) > 0:
            return result.to_dicts()[0]
        return None
    except GoogleAPIError as e:
        if context:
            context.log.warning(f"Error querying failure tracking record: {e}")
        return None
