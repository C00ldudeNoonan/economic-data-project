from datetime import datetime, tzinfo
from typing import cast

import dagster as dg
import pytz

from macro_agents.defs.resources.github import github_resource

UTC_TZ = cast(tzinfo, pytz.UTC)


def _initialize_failure_tracking_table(md, context):
    """Create the failure tracking table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS asset_check_failures (
        asset_key VARCHAR NOT NULL,
        check_name VARCHAR NOT NULL DEFAULT '',
        failure_type VARCHAR NOT NULL,
        consecutive_failures INTEGER NOT NULL DEFAULT 0,
        last_failure_timestamp TIMESTAMP NOT NULL,
        last_success_timestamp TIMESTAMP,
        github_issue_number INTEGER,
        github_issue_url VARCHAR,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (asset_key, failure_type, check_name)
    )
    """

    md.execute_query(create_table_query, read_only=False)
    context.log.debug("Initialized asset_check_failures table")


def _get_all_asset_check_keys(context):
    """Extract all asset check keys from the current definitions."""
    from macro_agents.definitions import defs

    check_keys = []
    for check in defs.asset_checks or []:
        try:
            check_key = check.check_key
            check_keys.append(check_key)
        except Exception as e:
            context.log.warning(
                f"Could not extract check key from asset check {check}: {e}. Skipping."
            )
            continue

    return check_keys


def _get_failure_tracking_record(
    md, asset_key: str, failure_type: str, check_name: str | None = None, context=None
):
    """Retrieve the current failure tracking record."""
    check_name_value = check_name if check_name else ""

    query = """
    SELECT * FROM asset_check_failures
    WHERE asset_key = ? AND failure_type = ? AND check_name = ?
    """

    try:
        result = md.execute_query(
            query, read_only=True, params=[asset_key, failure_type, check_name_value]
        )
        if len(result) > 0:
            return result.to_dicts()[0]
        return None
    except Exception as e:
        if context:
            context.log.warning(f"Error querying failure tracking record: {e}")
        return None


def _handle_check_failure(
    md,
    github,
    asset_key,
    check_name,
    current_record,
    timestamp,
    execution_record,
    context,
):
    """
    Handle a failed check:
    1. Increment consecutive failures
    2. Create GitHub issue if threshold (3) is reached
    """
    if current_record:
        new_count = current_record["consecutive_failures"] + 1

        update_query = """
        UPDATE asset_check_failures
        SET consecutive_failures = ?,
            last_failure_timestamp = ?,
            updated_at = CURRENT_TIMESTAMP()
        WHERE asset_key = ? AND failure_type = 'asset_check' AND check_name = ?
        """

        md.execute_query(
            update_query,
            read_only=False,
            params=[new_count, timestamp.isoformat(), asset_key, check_name],
        )

        context.log.info(
            f"Check {check_name} for {asset_key} failed. Consecutive failures: {new_count}"
        )

        if new_count >= 3 and not current_record.get("github_issue_number"):
            if github:
                _create_github_issue_for_failure(
                    github=github,
                    md=md,
                    asset_key=asset_key,
                    check_name=check_name,
                    failure_type="asset_check",
                    consecutive_failures=new_count,
                    execution_record=execution_record,
                    context=context,
                )
            else:
                context.log.warning(
                    f"Check {check_name} for {asset_key} has {new_count} consecutive failures "
                    "but GitHub is not configured - no issue created"
                )
        elif new_count > 3 and current_record.get("github_issue_number") and github:
            _update_github_issue_for_failure(
                github=github,
                issue_number=current_record["github_issue_number"],
                asset_key=asset_key,
                check_name=check_name,
                failure_type="asset_check",
                consecutive_failures=new_count,
                timestamp=timestamp,
                context=context,
            )
    else:
        upsert_query = """
        INSERT INTO asset_check_failures
        (asset_key, check_name, failure_type, consecutive_failures, last_failure_timestamp)
        VALUES (?, ?, 'asset_check', 1, ?)
        ON CONFLICT (asset_key, failure_type, check_name) DO UPDATE SET
            consecutive_failures = asset_check_failures.consecutive_failures + 1,
            last_failure_timestamp = EXCLUDED.last_failure_timestamp,
            updated_at = CURRENT_TIMESTAMP()
        """

        md.execute_query(
            upsert_query,
            read_only=False,
            params=[asset_key, check_name, timestamp.isoformat()],
        )

        context.log.info(f"Failure recorded for check {check_name} on {asset_key}")


def _handle_check_success(
    md, github, asset_key, check_name, current_record, timestamp, context
):
    """
    Handle a successful check:
    1. Reset consecutive failures to 0
    2. Close GitHub issue if one exists
    """
    if current_record and current_record["consecutive_failures"] > 0:
        context.log.info(
            f"Check {check_name} for {asset_key} passed after "
            f"{current_record['consecutive_failures']} consecutive failures"
        )

        if current_record.get("github_issue_number") and github:
            github.close_issue(
                issue_number=current_record["github_issue_number"],
                comment=f"✅ Asset check is now passing as of {timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}",
                context=context,
            )

        update_query = """
        UPDATE asset_check_failures
        SET consecutive_failures = 0,
            last_success_timestamp = ?,
            github_issue_number = NULL,
            github_issue_url = NULL,
            updated_at = CURRENT_TIMESTAMP()
        WHERE asset_key = ? AND failure_type = 'asset_check' AND check_name = ?
        """

        md.execute_query(
            update_query,
            read_only=False,
            params=[timestamp.isoformat(), asset_key, check_name],
        )


def _handle_materialization_failure(
    md, github, asset_key, current_record, timestamp, context
):
    """Handle a failed asset materialization."""
    if current_record:
        new_count = current_record["consecutive_failures"] + 1

        update_query = """
        UPDATE asset_check_failures
        SET consecutive_failures = ?,
            last_failure_timestamp = ?,
            updated_at = CURRENT_TIMESTAMP()
        WHERE asset_key = ? AND failure_type = 'asset_materialization' AND check_name = ''
        """

        md.execute_query(
            update_query,
            read_only=False,
            params=[new_count, timestamp.isoformat(), asset_key],
        )

        context.log.info(
            f"Asset {asset_key} failed to materialize. Consecutive failures: {new_count}"
        )

        if new_count >= 3 and not current_record.get("github_issue_number"):
            if github:
                _create_github_issue_for_failure(
                    github=github,
                    md=md,
                    asset_key=asset_key,
                    check_name=None,
                    failure_type="asset_materialization",
                    consecutive_failures=new_count,
                    execution_record=None,
                    context=context,
                )
            else:
                context.log.warning(
                    f"Asset {asset_key} has {new_count} consecutive materialization failures "
                    "but GitHub is not configured - no issue created"
                )
        elif new_count > 3 and current_record.get("github_issue_number") and github:
            _update_github_issue_for_failure(
                github=github,
                issue_number=current_record["github_issue_number"],
                asset_key=asset_key,
                check_name=None,
                failure_type="asset_materialization",
                consecutive_failures=new_count,
                timestamp=timestamp,
                context=context,
            )
    else:
        upsert_query = """
        INSERT INTO asset_check_failures
        (asset_key, check_name, failure_type, consecutive_failures, last_failure_timestamp)
        VALUES (?, '', 'asset_materialization', 1, ?)
        ON CONFLICT (asset_key, failure_type, check_name) DO UPDATE SET
            consecutive_failures = asset_check_failures.consecutive_failures + 1,
            last_failure_timestamp = EXCLUDED.last_failure_timestamp,
            updated_at = CURRENT_TIMESTAMP()
        """

        md.execute_query(
            upsert_query,
            read_only=False,
            params=[asset_key, timestamp.isoformat()],
        )

        context.log.info(f"Materialization failure recorded for asset {asset_key}")


def _handle_materialization_success(
    md, github, asset_key, current_record, timestamp, context
):
    """Handle a successful asset materialization."""
    if current_record and current_record["consecutive_failures"] > 0:
        context.log.info(
            f"Asset {asset_key} materialized successfully after "
            f"{current_record['consecutive_failures']} consecutive failures"
        )

        if current_record.get("github_issue_number") and github:
            github.close_issue(
                issue_number=current_record["github_issue_number"],
                comment=f"✅ Asset is now materializing successfully as of {timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}",
                context=context,
            )

        update_query = """
        UPDATE asset_check_failures
        SET consecutive_failures = 0,
            last_success_timestamp = ?,
            github_issue_number = NULL,
            github_issue_url = NULL,
            updated_at = CURRENT_TIMESTAMP()
        WHERE asset_key = ? AND failure_type = 'asset_materialization' AND check_name = ''
        """

        md.execute_query(
            update_query,
            read_only=False,
            params=[timestamp.isoformat(), asset_key],
        )


def _create_github_issue_for_failure(
    github,
    md,
    asset_key,
    check_name,
    failure_type,
    consecutive_failures,
    execution_record,
    context,
):
    """Create a GitHub issue for a check or asset that has failed 3+ times consecutively."""

    if failure_type == "asset_check":
        metadata_str = ""
        if (
            execution_record
            and hasattr(execution_record, "event")
            and hasattr(execution_record.event, "metadata_entries")
        ):
            metadata = execution_record.event.metadata_entries
            metadata_str = "\n".join([f"- **{m.label}**: {m.value}" for m in metadata])

        title = f"🚨 Asset Check Failure: {check_name} ({asset_key})"

        body = f"""## Asset Check Consecutive Failure Alert

**Asset**: `{asset_key}`
**Check**: `{check_name}`
**Consecutive Failures**: {consecutive_failures}
**Last Failure**: {datetime.fromtimestamp(execution_record.create_timestamp if execution_record else datetime.now().timestamp(), tz=UTC_TZ).strftime("%Y-%m-%d %H:%M:%S UTC")}

### Check Details

{metadata_str if metadata_str else "No additional metadata available"}

### Next Steps

1. Review the asset check definition and recent execution logs
2. Investigate the underlying data quality issue
3. Fix the root cause
4. Re-run the asset check to verify the fix

---
*This issue was automatically created by the Dagster asset check failure sensor.*
"""
    else:
        title = f"🚨 Asset Materialization Failure: {asset_key}"

        body = f"""## Asset Materialization Consecutive Failure Alert

**Asset**: `{asset_key}`
**Consecutive Failures**: {consecutive_failures}
**Last Failure**: {datetime.now(UTC_TZ).strftime("%Y-%m-%d %H:%M:%S UTC")}

### Next Steps

1. Review the asset definition and recent execution logs in Dagster
2. Check for upstream dependency failures
3. Investigate error messages in the Dagster UI
4. Fix the root cause
5. Re-materialize the asset to verify the fix

---
*This issue was automatically created by the Dagster asset failure sensor.*
"""

    issue_number = github.create_issue(
        title=title, body=body, labels=["dagster-failure", "automated"], context=context
    )

    issue_url = f"https://github.com/{github.repo_owner}/{github.repo_name}/issues/{issue_number}"

    if failure_type == "asset_check":
        update_query = """
        UPDATE asset_check_failures
        SET github_issue_number = ?,
            github_issue_url = ?,
            updated_at = CURRENT_TIMESTAMP()
        WHERE asset_key = ? AND failure_type = 'asset_check' AND check_name = ?
        """
        md.execute_query(
            update_query,
            read_only=False,
            params=[issue_number, issue_url, asset_key, check_name],
        )
    else:
        update_query = """
        UPDATE asset_check_failures
        SET github_issue_number = ?,
            github_issue_url = ?,
            updated_at = CURRENT_TIMESTAMP()
        WHERE asset_key = ? AND failure_type = 'asset_materialization' AND check_name = ''
        """
        md.execute_query(
            update_query,
            read_only=False,
            params=[issue_number, issue_url, asset_key],
        )

    context.log.info(
        f"Created GitHub issue #{issue_number} for {asset_key}: {issue_url}"
    )


def _update_github_issue_for_failure(
    github,
    issue_number,
    asset_key,
    check_name,
    failure_type,
    consecutive_failures,
    timestamp,
    context,
):
    """Add a comment to an existing GitHub issue for continued failures."""

    if failure_type == "asset_check":
        comment = f"""### Continued Failure Update

**Consecutive Failures**: {consecutive_failures}
**Latest Failure**: {timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")}

The asset check `{check_name}` for `{asset_key}` continues to fail. Please investigate urgently.
"""
    else:
        comment = f"""### Continued Failure Update

**Consecutive Failures**: {consecutive_failures}
**Latest Failure**: {timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")}

The asset `{asset_key}` continues to fail materialization. Please investigate urgently.
"""

    github.add_comment(issue_number=issue_number, comment=comment, context=context)


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

        _initialize_failure_tracking_table(md, context)

        check_keys = _get_all_asset_check_keys(context)
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

                current_record = _get_failure_tracking_record(
                    md, asset_key_str, "asset_check", check_name, context
                )

                if is_failed:
                    _handle_check_failure(
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
                    _handle_check_success(
                        md=md,
                        github=github if github_enabled else None,
                        asset_key=asset_key_str,
                        check_name=check_name,
                        current_record=current_record,
                        timestamp=timestamp,
                        context=context,
                    )
            except Exception as e:
                context.log.warning(
                    f"Error processing asset check {check_key}: {e}. Continuing with next check."
                )
                continue

        from macro_agents.definitions import defs

        all_asset_keys = list(defs.resolve_all_asset_keys())

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

                    current_record = _get_failure_tracking_record(
                        md, asset_key_str, "asset_materialization", None, context
                    )

                    if is_success:
                        _handle_materialization_success(
                            md=md,
                            github=github if github_enabled else None,
                            asset_key=asset_key_str,
                            current_record=current_record,
                            timestamp=timestamp,
                            context=context,
                        )
                    else:
                        # Handle materialization failure
                        _handle_materialization_failure(
                            md=md,
                            github=github if github_enabled else None,
                            asset_key=asset_key_str,
                            current_record=current_record,
                            timestamp=timestamp,
                            context=context,
                        )
            except Exception as e:
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
