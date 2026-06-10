#!/usr/bin/env bash
# Loop 2 — quality and uptime
# Monitors job failures and data quality signals, investigates root causes,
# and drafts fixes for human review.
# Usage: ./scripts/loop-2.sh [--project <name>] [--run-id <id>]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

PROJECT_ARG=""
RUN_ID=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT_ARG="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    *) log "Unknown argument: $1"; exit 1 ;;
  esac
done

require_wizard
get_project "$PROJECT_ARG"
WIZARD_CONTEXT="$(get_wizard_context)"

log "Starting Loop 2 — quality and uptime"

# Step 1: Triage
log "Step 1: Triage"
wizard exec \
  --dangerously-bypass-approvals-and-sandbox \
  --dangerously-bypass-hook-trust \
  "${WIZARD_CONTEXT} Read health/triage.md to understand current state. \
Query the project metadata index for: \
  - Failing tests \
  - Stale models (not refreshed in > 24 hours) \
  - Broken contracts \
  - Models in marts with no tests \
Classify each finding using the severity levels in AGENTS.md 'Pipeline Incident Triage'. \
Cross-reference AGENTS.md 'Common Failure Patterns' to identify likely root causes. \
De-duplicate against existing findings in health/triage.md. \
Write new findings to health/triage.md. \
Append a summary row to health/uptime-log.md. \
Commit the updated state files."

commit_state "loop-2"

# Step 2: Investigate P1 and P2
log "Step 2: Investigate P1 and P2 findings"
RUN_ID_CONTEXT=""
if [[ -n "$RUN_ID" ]]; then
  RUN_ID_CONTEXT="Also investigate dbt run ID: ${RUN_ID}."
fi

wizard exec \
  --dangerously-bypass-approvals-and-sandbox \
  --dangerously-bypass-hook-trust \
  "${WIZARD_CONTEXT} Read health/triage.md. ${RUN_ID_CONTEXT} \
For each finding with severity P1 or P2 and status 'new': \
  Use the investigator subagent to trace lineage and identify the root cause. \
  Follow the patterns in AGENTS.md 'Common Failure Patterns'. \
  Write the investigation to health/investigations/<id>.md. \
  Update the finding status in health/triage.md. \
Commit the updated state and investigation files."

commit_state "loop-2"

# Step 3: Fix low-risk issues
log "Step 3: Fix low-risk issues"
wizard exec \
  --dangerously-bypass-approvals-and-sandbox \
  --dangerously-bypass-hook-trust \
  "${WIZARD_CONTEXT} Read health/triage.md and all investigation files in health/investigations/. \
For each investigated finding where: \
  - Root cause is confirmed (not 'unknown') \
  - The fix is rated low or medium risk per AGENTS.md 'Fix Risk Classification' \
Use the maker subagent to implement the fix in an isolated worktree. \
Follow conventions in AGENTS.md and .claude/skills/dbt-development/dbt_skill.md. \
Use the verifier subagent to review the maker's output. \
If the verifier approves: \
  - Low risk: open a PR labeled 'wizard-auto-fix'. \
  - Medium risk: open a draft PR labeled 'wizard-auto-fix' and 'needs-review'. \
If the verifier rejects or validation fails: \
  Write to health/needs-human/<id>.md. \
Update health/triage.md with PR numbers or escalation status. \
Commit all state file updates."

commit_state "loop-2"

# Step 4: Weekly coverage (Mondays only)
DAY_OF_WEEK="$(date +%u)"
if [[ "$DAY_OF_WEEK" == "1" ]]; then
  log "Step 4: Weekly coverage (Monday)"
  wizard exec \
    --dangerously-bypass-approvals-and-sandbox \
    --dangerously-bypass-hook-trust \
    "${WIZARD_CONTEXT} Read health/triage.md. \
Collect all P3 findings (coverage gaps). \
For each P3 finding, use the maker subagent to add the missing tests. \
Follow test standards in .claude/skills/testing-strategies/SKILL.md. \
Batch all test additions into a single branch 'chore/weekly-coverage'. \
Run the validation loop on all changes. \
If validation passes, open a single PR titled 'Weekly test coverage improvements' \
  with a summary of all tests added. \
Update health/triage.md to mark addressed P3 findings as 'pr-open'. \
Commit state file updates."

  commit_state "loop-2"
else
  log "Step 4: Skipping weekly coverage (not Monday)"
fi

log "Loop 2 complete"
