#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_FILE="${REPO_ROOT}/wizard-loops.yml"

log() {
  echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] $*"
}

require_wizard() {
  if ! command -v wizard &>/dev/null; then
    log "ERROR: dbt Wizard CLI not found. Install it: https://docs.getdbt.com/docs/dbt-ai/wizard-cli"
    exit 1
  fi
}

read_config_value() {
  local key="$1"
  if command -v yq &>/dev/null; then
    yq eval "$key" "$CONFIG_FILE" 2>/dev/null
  else
    _read_config_fallback "$key"
  fi
}

_read_config_fallback() {
  local key="$1"
  case "$key" in
    .defaults.active_project)
      grep -A1 'active_project' "$CONFIG_FILE" | tail -1 | sed 's/.*: *//' | tr -d '"' | tr -d "'"
      ;;
    *)
      echo ""
      ;;
  esac
}

_read_project_field() {
  local project_name="$1"
  local field="$2"
  if command -v yq &>/dev/null; then
    yq eval ".projects[] | select(.name == \"${project_name}\") | .${field}" "$CONFIG_FILE" 2>/dev/null
  else
    _read_project_field_fallback "$project_name" "$field"
  fi
}

_read_project_field_fallback() {
  local project_name="$1"
  local field="$2"
  local in_project=false
  while IFS= read -r line; do
    if echo "$line" | grep -q "name:.*${project_name}"; then
      in_project=true
      continue
    fi
    if $in_project; then
      if echo "$line" | grep -q "^ *- name:"; then
        break
      fi
      if echo "$line" | grep -q "^ *${field}:"; then
        echo "$line" | sed "s/.*${field}: *//" | tr -d '"' | tr -d "'"
        return
      fi
    fi
  done < "$CONFIG_FILE"
  echo "null"
}

get_project() {
  local project_name="${1:-}"

  if [[ -z "$project_name" ]]; then
    project_name="$(read_config_value '.defaults.active_project')"
  fi

  if [[ -z "$project_name" || "$project_name" == "null" ]]; then
    log "ERROR: No project specified and no defaults.active_project set in ${CONFIG_FILE}"
    exit 1
  fi

  PROJECT_NAME="$project_name"
  PROJECT_TYPE="$(_read_project_field "$project_name" "type")"
  PROJECT_PATH="$(_read_project_field "$project_name" "path")"
  PROJECT_ID="$(_read_project_field "$project_name" "project_id")"
  ENVIRONMENT_ID="$(_read_project_field "$project_name" "environment_id")"

  if [[ "$PROJECT_TYPE" == "null" || -z "$PROJECT_TYPE" ]]; then
    log "ERROR: Project '${project_name}' not found in ${CONFIG_FILE}"
    exit 1
  fi

  export PROJECT_NAME PROJECT_TYPE PROJECT_PATH PROJECT_ID ENVIRONMENT_ID
  log "Project: ${PROJECT_NAME} (type: ${PROJECT_TYPE})"
}

get_wizard_context() {
  local context=""
  case "$PROJECT_TYPE" in
    local)
      context="Working with the dbt project at ${PROJECT_PATH}."
      ;;
    platform)
      context="Working with dbt platform project ${PROJECT_ID} in environment ${ENVIRONMENT_ID}."
      ;;
    *)
      log "ERROR: Unknown project type '${PROJECT_TYPE}'"
      exit 1
      ;;
  esac
  context="${context} Reference AGENTS.md for project conventions, incident triage, and fix risk levels."
  echo "$context"
}

commit_state() {
  local loop_name="${1:-wizard}"
  cd "$REPO_ROOT"

  if git diff --quiet requests/ health/ 2>/dev/null && git diff --cached --quiet requests/ health/ 2>/dev/null; then
    if [[ -z "$(git ls-files --others --exclude-standard requests/ health/ 2>/dev/null)" ]]; then
      log "No state file changes to commit"
      return 0
    fi
  fi

  git add requests/ health/
  git commit -m "wizard: update state files [${loop_name}]" || true
  log "State files committed"
}
