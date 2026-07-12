"""Asset/asset-check failure monitoring, split by concern.

- ``tracking``: the BigQuery failure-tracking table and asset/check discovery.
- ``handlers``: per-event failure/success bookkeeping and escalation.
- ``github_issues``: creating/updating GitHub issues for sustained failures.

The Dagster asset, job, and Definitions live in
``macro_agents.defs.asset_failure_sensor``. Import specific helpers from their
module — this package has no barrel re-exports.
"""
