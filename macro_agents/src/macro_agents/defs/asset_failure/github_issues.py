"""GitHub issue creation/updates for sustained asset failures."""

from datetime import datetime

from macro_agents.defs.asset_failure._common import UTC_TZ


def create_github_issue_for_failure(
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
        SET github_issue_number = @github_issue_number,
            github_issue_url = @github_issue_url,
            updated_at = CURRENT_TIMESTAMP()
        WHERE asset_key = @asset_key
          AND failure_type = 'asset_check'
          AND check_name = @check_name
        """
        md.execute_query(
            update_query,
            read_only=False,
            params={
                "github_issue_number": issue_number,
                "github_issue_url": issue_url,
                "asset_key": asset_key,
                "check_name": check_name or "",
            },
        )
    else:
        update_query = """
        UPDATE asset_check_failures
        SET github_issue_number = @github_issue_number,
            github_issue_url = @github_issue_url,
            updated_at = CURRENT_TIMESTAMP()
        WHERE asset_key = @asset_key
          AND failure_type = 'asset_materialization'
          AND check_name = ''
        """
        md.execute_query(
            update_query,
            read_only=False,
            params={
                "github_issue_number": issue_number,
                "github_issue_url": issue_url,
                "asset_key": asset_key,
            },
        )

    context.log.info(
        f"Created GitHub issue #{issue_number} for {asset_key}: {issue_url}"
    )


def update_github_issue_for_failure(
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
