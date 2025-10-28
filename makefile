ruff:
	-ruff check --fix .
	ruff format .

lint:
	sqlfluff lint ./dbt_project/models --disable-progress-bar --processes 4

fix:
	sqlfluff fix ./dbt_project/models --disable-progress-bar --processes 4

test:
	cd macro_agents && uv sync && source .venv/bin/activate && python -m pytest tests/ -v

dbt-manifest:
	cd dbt_project && dbt parse

pre-pr:
	@echo "Running pre-PR checks..."
	@echo "1. Installing dependencies..."
	cd macro_agents && uv sync && source .venv/bin/activate && pip install -e .[dev] pytest-cov pytest-xdist mypy bandit safety
	@echo "2. Running ruff linting..."
	cd macro_agents && source .venv/bin/activate && ruff check macro_agents/
	@echo "3. Running ruff formatting check..."
	cd macro_agents && source .venv/bin/activate && ruff format --check macro_agents/
	@echo "4. Running mypy type checking..."
	cd macro_agents && source .venv/bin/activate && mypy macro_agents/ --ignore-missing-imports
	@echo "5. Running pytest with coverage..."
	cd macro_agents && source .venv/bin/activate && python -m pytest tests/ -v --cov=macro_agents --cov-report=term-missing
	@echo "6. Running security scan with bandit..."
	cd macro_agents && source .venv/bin/activate && bandit -r macro_agents/ -f json -o bandit-report.json || true
	@echo "7. Running safety check..."
	cd macro_agents && source .venv/bin/activate && safety check --json --output safety-report.json || true
	@echo "8. Testing Dagster definitions..."
	cd macro_agents && source .venv/bin/activate && dg check defs
	@echo "9. Testing Dagster asset materialization (dry run)..."
	cd macro_agents && source .venv/bin/activate && dg materialize --dry-run --select "*"
	@echo "10. Testing DBT models..."
	cd dbt_project && dbt deps && dbt compile && dbt parse
	@echo "11. Running SQL linting..."
	sqlfluff lint ./dbt_project/models --disable-progress-bar --processes 4
	@echo "✅ All pre-PR checks completed successfully!"