"""Per-event failure/success bookkeeping and escalation."""

from macro_agents.defs.asset_failure.github_issues import (
    create_github_issue_for_failure,
    update_github_issue_for_failure,
)


def handle_check_failure(
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
        SET consecutive_failures = @consecutive_failures,
            last_failure_timestamp = @last_failure_timestamp,
            updated_at = CURRENT_TIMESTAMP()
        WHERE asset_key = @asset_key
          AND failure_type = 'asset_check'
          AND check_name = @check_name
        """

        md.execute_query(
            update_query,
            read_only=False,
            params={
                "consecutive_failures": new_count,
                "last_failure_timestamp": timestamp,
                "asset_key": asset_key,
                "check_name": check_name,
            },
        )

        context.log.info(
            f"Check {check_name} for {asset_key} failed. Consecutive failures: {new_count}"
        )

        if new_count >= 3 and not current_record.get("github_issue_number"):
            if github:
                create_github_issue_for_failure(
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
            update_github_issue_for_failure(
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
        MERGE `asset_check_failures` AS target
        USING (
            SELECT
                @asset_key AS asset_key,
                @check_name AS check_name,
                @last_failure_timestamp AS last_failure_timestamp
        ) AS source
        ON target.asset_key = source.asset_key
           AND target.failure_type = 'asset_check'
           AND target.check_name = source.check_name
        WHEN MATCHED THEN UPDATE SET
            consecutive_failures = target.consecutive_failures + 1,
            last_failure_timestamp = source.last_failure_timestamp,
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            asset_key,
            check_name,
            failure_type,
            consecutive_failures,
            last_failure_timestamp
        ) VALUES (
            source.asset_key,
            source.check_name,
            'asset_check',
            1,
            source.last_failure_timestamp
        )
        """

        md.execute_query(
            upsert_query,
            read_only=False,
            params={
                "asset_key": asset_key,
                "check_name": check_name,
                "last_failure_timestamp": timestamp,
            },
        )

        context.log.info(f"Failure recorded for check {check_name} on {asset_key}")


def handle_check_success(
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
            last_success_timestamp = @last_success_timestamp,
            github_issue_number = NULL,
            github_issue_url = NULL,
            updated_at = CURRENT_TIMESTAMP()
        WHERE asset_key = @asset_key
          AND failure_type = 'asset_check'
          AND check_name = @check_name
        """

        md.execute_query(
            update_query,
            read_only=False,
            params={
                "last_success_timestamp": timestamp,
                "asset_key": asset_key,
                "check_name": check_name,
            },
        )


def handle_materialization_failure(
    md, github, asset_key, current_record, timestamp, context
):
    """Handle a failed asset materialization."""
    if current_record:
        new_count = current_record["consecutive_failures"] + 1

        update_query = """
        UPDATE asset_check_failures
        SET consecutive_failures = @consecutive_failures,
            last_failure_timestamp = @last_failure_timestamp,
            updated_at = CURRENT_TIMESTAMP()
        WHERE asset_key = @asset_key
          AND failure_type = 'asset_materialization'
          AND check_name = ''
        """

        md.execute_query(
            update_query,
            read_only=False,
            params={
                "consecutive_failures": new_count,
                "last_failure_timestamp": timestamp,
                "asset_key": asset_key,
            },
        )

        context.log.info(
            f"Asset {asset_key} failed to materialize. Consecutive failures: {new_count}"
        )

        if new_count >= 3 and not current_record.get("github_issue_number"):
            if github:
                create_github_issue_for_failure(
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
            update_github_issue_for_failure(
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
        MERGE `asset_check_failures` AS target
        USING (
            SELECT
                @asset_key AS asset_key,
                @last_failure_timestamp AS last_failure_timestamp
        ) AS source
        ON target.asset_key = source.asset_key
           AND target.failure_type = 'asset_materialization'
           AND target.check_name = ''
        WHEN MATCHED THEN UPDATE SET
            consecutive_failures = target.consecutive_failures + 1,
            last_failure_timestamp = source.last_failure_timestamp,
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            asset_key,
            check_name,
            failure_type,
            consecutive_failures,
            last_failure_timestamp
        ) VALUES (
            source.asset_key,
            '',
            'asset_materialization',
            1,
            source.last_failure_timestamp
        )
        """

        md.execute_query(
            upsert_query,
            read_only=False,
            params={
                "asset_key": asset_key,
                "last_failure_timestamp": timestamp,
            },
        )

        context.log.info(f"Materialization failure recorded for asset {asset_key}")


def handle_materialization_success(
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
            last_success_timestamp = @last_success_timestamp,
            github_issue_number = NULL,
            github_issue_url = NULL,
            updated_at = CURRENT_TIMESTAMP()
        WHERE asset_key = @asset_key
          AND failure_type = 'asset_materialization'
          AND check_name = ''
        """

        md.execute_query(
            update_query,
            read_only=False,
            params={
                "last_success_timestamp": timestamp,
                "asset_key": asset_key,
            },
        )
