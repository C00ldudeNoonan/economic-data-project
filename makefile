ruff:
	-ruff check --fix . --exclude .claude
	ruff format . --exclude .claude

lint:
	cd macro_agents && uv sync --extra dev && uv run sqlfluff lint ../dbt_project/models --disable-progress-bar --processes 4

fix:
	cd macro_agents && uv sync --extra dev && uv run sqlfluff fix ../dbt_project/models --disable-progress-bar --processes 4

test: dbt-manifest
	cd macro_agents && uv sync --extra dev && uv run pytest tests/ -v

test-dagster: dbt-manifest
	cd macro_agents && uv sync --extra dev && uv run pytest tests/ -v

typecheck-dagster:
	cd macro_agents && uv sync --extra dev && uv run ty check --extra-search-path src \
		--exclude "src/macro_agents/defs/analysis/**" \
		--exclude "src/macro_agents/defs/backtesting/**" \
		--exclude "src/macro_agents/defs/resources/**" \
		--exclude "src/macro_agents/defs/telemetry/**" \
		--exclude "src/macro_agents/defs/transformation/**" \
		--exclude "src/macro_agents/defs/utils/**" \
		src/macro_agents

dbt-manifest:
	cd macro_agents && uv sync --extra dev && uv run dbt parse \
		--project-dir ../dbt_project \
		--profiles-dir ../dbt_project

# =============================================================================
# Local Development
# =============================================================================

# Dagster Setup (uses uv)
setup-dagster:
	cd macro_agents && uv sync --extra dev
	@echo "Dagster environment synced with uv"
	@echo "Run 'make run-dagster' to start Dagster"

# Run Dagster
run-dagster:
	cd macro_agents && uv run dagster dev

# =============================================================================
# Local Dagster with Docker (dev-friendly, no Docker-in-Docker)
# =============================================================================

# Start Dagster locally with Docker Compose (uses DefaultRunLauncher)
run-dagster-local:
	@echo "Starting local Dagster with Docker Compose..."
	@if [ ! -f .env ]; then \
		echo "Warning: .env file not found. Create one from .env.dagster.example"; \
	fi
	docker compose -f docker-compose.yml -f docker-compose.local.yml up -d
	@echo ""
	@echo "=== Local Dagster started! ==="
	@echo "Dagster UI: http://localhost:$${DAGSTER_UI_PORT:-3000}"
	@echo ""
	@echo "Useful commands:"
	@echo "  make dagster-local-logs     # View logs"
	@echo "  make dagster-local-stop     # Stop services"
	@echo "  make dagster-local-restart  # Restart user code (after code changes)"

# Stop local Dagster
dagster-local-stop:
	@echo "Stopping local Dagster..."
	docker compose -f docker-compose.yml -f docker-compose.local.yml down
	@echo "Local Dagster stopped"

# View local Dagster logs
dagster-local-logs:
	docker compose -f docker-compose.yml -f docker-compose.local.yml logs -f

# Rebuild user code container (after dependency changes)
dagster-local-rebuild:
	@echo "Rebuilding Dagster user code container..."
	docker compose -f docker-compose.yml -f docker-compose.local.yml build dagster_user_code
	docker compose -f docker-compose.yml -f docker-compose.local.yml up -d dagster_user_code
	@echo "User code container rebuilt and restarted"

# Restart user code only (source is volume-mounted, so no rebuild needed)
dagster-local-restart:
	@echo "Restarting Dagster user code..."
	docker compose -f docker-compose.yml -f docker-compose.local.yml restart dagster_user_code
	@echo "User code restarted"

# =============================================================================
# Dagster OSS with Docker (production)
# =============================================================================

# Start Dagster OSS with Docker Compose
run-dagster-oss:
	@echo "Starting Dagster OSS with Docker Compose..."
	@if [ ! -f .env ]; then \
		echo "Warning: .env file not found. Make sure to create one from .env.dagster.example"; \
	fi
	docker compose up -d
	@echo ""
	@echo "=== Dagster OSS started! ==="
	@echo "Dagster UI: http://localhost:$${DAGSTER_UI_PORT:-3000}"
	@echo ""
	@echo "Useful commands:"
	@echo "  make dagster-oss-logs    # View logs"
	@echo "  make dagster-oss-stop    # Stop services"
	@echo "  make dagster-oss-status  # Check status"

# Stop Dagster OSS
dagster-oss-stop:
	@echo "Stopping Dagster OSS..."
	docker compose down
	@echo "Dagster OSS stopped"

# View Dagster OSS logs
dagster-oss-logs:
	docker compose logs -f

# View logs for specific service
dagster-oss-logs-webserver:
	docker compose logs -f dagster_webserver

dagster-oss-logs-daemon:
	docker compose logs -f dagster_daemon

dagster-oss-logs-user-code:
	docker compose logs -f dagster_user_code

dagster-oss-logs-postgres:
	docker compose logs -f dagster_postgresql

# Check status of Dagster OSS services
dagster-oss-status:
	@echo "=== Dagster OSS Service Status ==="
	docker compose ps
	@echo ""
	@echo "=== Service Health ==="
	@docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" || true

# Rebuild user code container (after code changes)
dagster-oss-rebuild:
	@echo "Rebuilding Dagster user code container..."
	docker compose build dagster_user_code
	@echo "Restarting user code service..."
	docker compose up -d dagster_user_code
	@echo "User code container rebuilt and restarted"

# Restart all Dagster OSS services
dagster-oss-restart:
	@echo "Restarting all Dagster OSS services..."
	docker compose restart
	@echo "Services restarted"

# Clean up Docker volumes (WARNING: This will delete all data!)
dagster-oss-clean:
	@echo "WARNING: This will delete all Dagster data including PostgreSQL!"
	@read -p "Are you sure? Type 'yes' to continue: " confirm && [ "$$confirm" = "yes" ] || exit 1
	docker compose down -v
	@echo "All volumes and data deleted"

# Access PostgreSQL database
dagster-oss-db:
	docker compose exec dagster_postgresql psql -U dagster_user -d dagster
