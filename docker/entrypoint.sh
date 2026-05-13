#!/bin/bash
set -e

# Forward SIGTERM to child process for graceful shutdown of in-flight runs
# Docker sends SIGTERM on container stop; we relay it so the gRPC server
# can notify Dagster to checkpoint running ops before SIGKILL arrives.
_term() {
    if [ -n "$CHILD_PID" ]; then
        kill -TERM "$CHILD_PID" 2>/dev/null
        wait "$CHILD_PID"
    fi
}
trap _term SIGTERM SIGINT

# Write GCP credentials from environment variable to file
# GOOGLE_APPLICATION_CREDENTIALS_JSON contains the JSON content
# GOOGLE_APPLICATION_CREDENTIALS will be set to the file path
if [ -n "$GOOGLE_APPLICATION_CREDENTIALS_JSON" ]; then
    echo "$GOOGLE_APPLICATION_CREDENTIALS_JSON" > /opt/dagster/gcp/service-account.json
    export GOOGLE_APPLICATION_CREDENTIALS=/opt/dagster/gcp/service-account.json
    echo "GCP credentials written to $GOOGLE_APPLICATION_CREDENTIALS"
fi

# Generate dbt manifest if dbt project exists
DBT_DIR="${DBT_PROJECT_DIR:-/opt/dagster/dbt_project}"
if [ -d "$DBT_DIR" ] && [ -f "$DBT_DIR/dbt_project.yml" ]; then
    echo "Generating dbt manifest..."
    cd "$DBT_DIR"
    # Only run dbt deps if packages dir is missing; skip network call if offline
    if [ ! -d "$DBT_DIR/dbt_packages" ]; then
        echo "dbt_packages not found, attempting dbt deps (15s timeout)..."
        timeout 15 dbt deps --target "${DBT_TARGET:-local}" 2>/dev/null || echo "Warning: dbt deps failed or timed out, continuing without packages"
    fi
    dbt parse --target "${DBT_TARGET:-local}" 2>/dev/null || echo "Warning: dbt parse failed, dbt assets may not load"
    cd /opt/dagster/app
fi

# Execute the main command in background so we can forward signals
"$@" &
CHILD_PID=$!
wait "$CHILD_PID"
