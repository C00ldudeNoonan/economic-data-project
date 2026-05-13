#!/usr/bin/env bash
# Validate Dagster asset definitions before creating a PR.
#
# Usage:
#   ./scripts/validate_assets.sh              # check definitions only
#   ./scripts/validate_assets.sh --materialize # also materialize smoke-test assets
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
MACRO_AGENTS_DIR="$PROJECT_ROOT/macro_agents"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

MATERIALIZE=false
for arg in "$@"; do
    case $arg in
        --materialize) MATERIALIZE=true ;;
    esac
done

echo "============================================"
echo " Dagster Asset Validation"
echo "============================================"
echo ""

# ------------------------------------------------------------------
# Step 1: Check that definitions load without errors
# ------------------------------------------------------------------
echo -e "${YELLOW}[1/2] Checking Dagster definitions...${NC}"
cd "$MACRO_AGENTS_DIR"

if uv run dg check defs 2>&1; then
    echo -e "${GREEN}  ✓ Definitions loaded successfully${NC}"
else
    echo -e "${RED}  ✗ Definition check failed${NC}"
    echo ""
    echo "Fix the errors above before creating a PR."
    exit 1
fi

echo ""

# ------------------------------------------------------------------
# Step 2 (optional): Materialize smoke-test assets
# ------------------------------------------------------------------
if [ "$MATERIALIZE" = true ]; then
    echo -e "${YELLOW}[2/2] Materializing smoke-test assets...${NC}"

    # Smoke-test assets that are fast and exercise core paths.
    # These should work with sampled local data.
    SMOKE_ASSETS=(
        "calendar_dates"
        "fred_series_mapping"
    )

    FAILED=0
    for asset in "${SMOKE_ASSETS[@]}"; do
        echo -n "  Materializing $asset... "
        if ENVIRONMENT=dev uv run dagster asset materialize \
            --select "$asset" \
            -m macro_agents.definitions 2>&1 | tail -1; then
            echo -e "${GREEN}✓${NC}"
        else
            echo -e "${RED}✗${NC}"
            FAILED=$((FAILED + 1))
        fi
    done

    if [ "$FAILED" -gt 0 ]; then
        echo ""
        echo -e "${RED}$FAILED smoke-test asset(s) failed to materialize.${NC}"
        exit 1
    fi
else
    echo -e "${YELLOW}[2/2] Skipping materialization (pass --materialize to enable)${NC}"
fi

echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN} All checks passed!${NC}"
echo -e "${GREEN}============================================${NC}"
