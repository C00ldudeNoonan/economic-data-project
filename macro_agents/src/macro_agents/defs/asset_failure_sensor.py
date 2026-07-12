"""Dagster asset that monitors asset/asset-check failures and escalates.

The bookkeeping, GitHub notifications, and tracking-table access live in the
``macro_agents.defs.asset_failure`` package; this module wires them into the
scheduled ``asset_failure_monitor`` asset, its job, and Definitions.
"""

from datetime import datetime

import dagster as dg

from macro_agents.defs.asset_failure._common import UTC_TZ
from macro_agents.defs.asset_failure.github_issues import (  # noqa: F401 — re-exported for test patching / callers
    create_github_issue_for_failure,
    update_github_issue_for_failure,
)
from macro_agents.defs.asset_failure.handlers import (
    handle_check_failure,
    handle_check_success,
    handle_materialization_failure,
    handle_materialization_success,
)
from macro_agents.defs.asset_failure.tracking import (
    get_all_asset_check_keys,
    get_all_asset_keys,
    get_failure_tracking_record,
    initialize_failure_tracking_table,
)
from macro_agents.defs.resources.github import github_resource


@dg.asset(
    name="asset_failure_monitor",
    description="Monitor asset and asset check failures and materializations, create GitHub issues after 3+ consecutive failures",
    automation_condition=dg.AutomationCondition.on_cron("0 */6 * * *"),
    required_resource_keys={"bq", "github"},
)
def asset_failure_monitor(context):
    """
    Asset that:
    1. Queries all asset checks and assets for their latest execution status
    2. Updates failure tracking in BigQuery
    3. Creates GitHub issues for asset checks/assets with 3+ consecutive failures
    4. Resets counters and closes issues when asset checks/assets pass

    This asset is designed to be resilient - it will log errors but always materialize
    to avoid masking the actual asset/check failures being monitored.

    Note: GitHub issue creation requires GITHUB_TOKEN to be set. If not configured,
    the monitor will still track failures but won't create issues.
    """
    asset_checks_evaluated = 0
    assets_evaluated = 0
    github_enabled = False
    github_errors = []

    try:
        md = context.resources.bq
        github = context.resources.github

        # Try to initialize GitHub - if it fails, continue without it
        try:
            github.setup_for_execution(context)
            github_enabled = True
            context.log.info(
                "GitHub integration enabled - will create issues for failures"
            )
        except (ValueError, RuntimeError) as e:
            github_enabled = False
            error_msg = str(e)
            github_errors.append(error_msg)
            context.log.warning(
                f"GitHub integration disabled: {error_msg}. "
                "Failures will still be tracked but issues won't be created. "
                "Set GITHUB_TOKEN to enable issue creation."
            )

        initialize_failure_tracking_table(md, context)

        check_keys = get_all_asset_check_keys(context)
        asset_checks_evaluated = len(check_keys)

        summary_records = (
            context.instance.event_log_storage.get_asset_check_summary_records(
                check_keys
            )
        )

        for check_key in check_keys:
            try:
                summary_record = summary_records.get(check_key)

                if not summary_record or not summary_record.last_check_execution_record:
                    context.log.debug(f"No execution record found for {check_key}")
                    continue

                execution_record = summary_record.last_check_execution_record

                asset_key_str = str(check_key.asset_key)
                check_name = check_key.name
                timestamp = datetime.fromtimestamp(
                    execution_record.create_timestamp, tz=UTC_TZ
                )

                is_failed = execution_record.status.value == "FAILED"

                current_record = get_failure_tracking_record(
                    md, asset_key_str, "asset_check", check_name, context
                )

                if is_failed:
                    handle_check_failure(
                        md=md,
                        github=github if github_enabled else None,
                        asset_key=asset_key_str,
                        check_name=check_name,
                        current_record=current_record,
                        timestamp=timestamp,
                        execution_record=execution_record,
                        context=context,
                    )
                else:
                    handle_check_success(
                        md=md,
                        github=github if github_enabled else None,
                        asset_key=asset_key_str,
                        check_name=check_name,
                        current_record=current_record,
                        timestamp=timestamp,
                        context=context,
                    )
            except Exception as e:
                # Intentional resilience boundary: one bad check must not crash
                # the monitor (see docstring). Logged, not silently swallowed.
                context.log.warning(
                    f"Error processing asset check {check_key}: {e}. Continuing with next check."
                )
                continue

        all_asset_keys = get_all_asset_keys()

        for asset_key in all_asset_keys:
            asset_key_str = str(asset_key)
            assets_evaluated += 1

            try:
                latest_event = context.instance.get_latest_materialization_event(
                    asset_key
                )

                if latest_event:
                    timestamp = datetime.fromtimestamp(
                        latest_event.timestamp, tz=UTC_TZ
                    )

                    is_success = (
                        latest_event.dagster_event.event_type_value
                        == "ASSET_MATERIALIZATION"
                    )

                    current_record = get_failure_tracking_record(
                        md, asset_key_str, "asset_materialization", None, context
                    )

                    if is_success:
                        handle_materialization_success(
                            md=md,
                            github=github if github_enabled else None,
                            asset_key=asset_key_str,
                            current_record=current_record,
                            timestamp=timestamp,
                            context=context,
                        )
                    else:
                        # Handle materialization failure
                        handle_materialization_failure(
                            md=md,
                            github=github if github_enabled else None,
                            asset_key=asset_key_str,
                            current_record=current_record,
                            timestamp=timestamp,
                            context=context,
                        )
            except Exception as e:
                # Intentional resilience boundary (see docstring): keep monitoring
                # remaining assets even if one lookup fails. Logged, not silent.
                context.log.warning(
                    f"Error processing materialization status for {asset_key}: {e}"
                )
                continue

        return dg.MaterializeResult(
            metadata={
                "status": "ok",
                "asset_checks_evaluated": asset_checks_evaluated,
                "assets_evaluated": assets_evaluated,
                "github_enabled": github_enabled,
                "github_errors": github_errors if github_errors else None,
            }
        )
    except Exception as e:
        # Top-level resilience boundary: always materialize so a monitor bug
        # never masks the asset/check failures it is meant to surface.
        context.log.error(
            "Error in asset_failure_monitor execution: "
            f"{e}. Asset will materialize to avoid masking monitored failures."
        )
        return dg.MaterializeResult(
            metadata={
                "status": "error",
                "error": str(e),
                "asset_checks_evaluated": asset_checks_evaluated,
                "assets_evaluated": assets_evaluated,
                "github_enabled": github_enabled,
                "github_errors": github_errors if github_errors else None,
            }
        )


asset_failure_monitor_job = dg.define_asset_job(
    name="asset_failure_monitor_job",
    tags={"dagster/priority": "0", "dagster/max_runtime": 600},
    selection=dg.AssetSelection.assets("asset_failure_monitor"),
    description="Monitor asset and asset check failures on a scheduled cadence",
)


defs = dg.Definitions(
    assets=[asset_failure_monitor],
    jobs=[asset_failure_monitor_job],
    resources={
        "github": github_resource,
    },
)
