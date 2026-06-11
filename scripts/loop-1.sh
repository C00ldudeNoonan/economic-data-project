#!/usr/bin/env bash
# Loop 1 — request to pipeline
# Picks up GitHub Issues labeled 'data-request', scopes them against the dbt
# project, builds models and tests, and opens a PR for review.
# Usage: ./scripts/loop-1.sh [--project <name>]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

PROJECT_ARG=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT_ARG="$2"; shift 2 ;;
    *) log "Unknown argument: $1"; exit 1 ;;
  esac
done

require_wizard
get_project "$PROJECT_ARG"
WIZARD_CONTEXT="$(get_wizard_context)"

log "Starting Loop 1 — request to pipeline"

# Step 1: Discover and scope requests
log "Step 1: Discover and scope requests"
wizard exec \
  --dangerously-bypass-approvals-and-sandbox \
  --dangerously-bypass-hook-trust \
  "${WIZARD_CONTEXT} Read requests/backlog.md to understand current state. \
Then read all open GitHub Issues labeled 'data-request' in this repo. \
For each issue not already in the backlog: \
  1. Parse the issue — identify which dbt domain it affects (staging, markets, government, \
     commodities, signals, analysis, backtesting, data_quality, agents_preprocess). \
  2. Add a row to requests/backlog.md with status 'new'. \
  3. Scope the request against the project: identify affected models, upstream sources, \
     and downstream consumers using lineage. \
  4. Write the scoping note to requests/scoped/<id>.md. \
  5. Update the backlog row status to 'scoped' (or 'blocked' if it cannot be scoped). \
     See AGENTS.md 'Data Request Scoping' for evaluation criteria. \
Commit the updated state files to this branch."

commit_state "loop-1"

# Step 2: Build scoped requests
log "Step 2: Build scoped requests"
wizard exec \
  --dangerously-bypass-approvals-and-sandbox \
  --dangerously-bypass-hook-trust \
  "${WIZARD_CONTEXT} Read requests/backlog.md. For each request with status 'scoped': \
  1. Read the scoping note from requests/scoped/<id>.md. \
  2. Use the maker subagent to build the model in an isolated worktree. \
     Follow conventions in AGENTS.md and .claude/skills/dbt-development/dbt_skill.md. \
  3. Run the validation loop: compile, build to dev, run tests. \
  4. If validation passes, use the verifier subagent to review the diff. \
  5. If the verifier approves, open a PR with the scoping note as the description. \
     Update backlog status to 'in-review'. \
  6. If validation fails after 3 retries or the verifier rejects: \
     Write to requests/blocked/<id>.md with the error. \
     Update backlog status to 'blocked'. \
Commit all state file updates."

commit_state "loop-1"

log "Loop 1 complete"
