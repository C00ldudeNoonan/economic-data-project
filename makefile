ruff:
	-ruff check --fix .
	ruff format .

lint:
	sqlfluff lint ./dbt_project/models --disable-progress-bar --processes 4

fix:
	sqlfluff fix ./dbt_project/models --disable-progress-bar --processes 4

test:
	cd macro_agents && uv sync --extra dev && uv run pytest tests/ -v

dbt-manifest:
	cd dbt_project && dbt parse

pre-pr:
	@echo "Running pre-PR checks..."
	@echo "1. Installing dependencies..."
	cd macro_agents && uv sync --extra dev && uv pip install pytest-cov pytest-xdist mypy bandit safety
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

# GitHub Actions Local Testing (requires act and Docker)
.PHONY: act-simulate act-list act-test act-test-run act-test-setup act-test-setup-dry act-test-job act-test-python act-validate help-act

act-simulate:
	@echo "🚀 Running full GitHub Actions CI/CD pipeline simulation..."
	@echo "⚠️  This requires Docker to be running"
	@echo ""
	@echo "Validating workflow syntax first..."
	@act -l --container-architecture linux/amd64 > /dev/null && echo "✅ Workflow syntax is valid!" || (echo "❌ Workflow syntax error!" && exit 1)
	@echo ""
	@echo "Running complete workflow..."
	@act push --container-architecture linux/amd64

act-list:
	@echo "Listing available GitHub Actions workflows..."
	act -l

act-validate:
	@echo "Validating GitHub Actions workflow syntax..."
	@act -l > /dev/null && echo "✅ Workflow syntax is valid!" || echo "❌ Workflow syntax error!"

act-test:
	@echo "Testing GitHub Actions workflow (dry run)..."
	@echo "Use 'make act-simulate' to actually run the workflow (requires Docker)"
	act -n

act-test-run:
	@echo "Running GitHub Actions workflow (requires Docker to be running)..."
	act push --container-architecture linux/amd64

act-test-setup:
	@echo "Testing setup job (extracts Python versions from pyproject.toml)..."
	act -j setup --container-architecture linux/amd64

act-test-setup-dry:
	@echo "Testing setup job (dry run)..."
	act -j setup -n

act-test-job:
	@echo "Testing specific job (usage: make act-test-job JOB=test)"
	@if [ -z "$(JOB)" ]; then \
		echo "❌ Please specify a job name: make act-test-job JOB=<job-name>"; \
		echo "Available jobs: setup, test, integration-test, security-scan, build, deploy"; \
		exit 1; \
	fi
	act -j $(JOB) --container-architecture linux/amd64

act-test-python:
	@echo "Testing test job with Python 3.11..."
	act -j test --matrix python-version:3.11 --container-architecture linux/amd64

help-act:
	@echo "GitHub Actions Local Testing Commands:"
	@echo ""
	@echo "  make act-simulate          - 🚀 Run full CI/CD pipeline simulation (main command)"
	@echo ""
	@echo "Other commands:"
	@echo "  make act-list              - List all available workflows and jobs"
	@echo "  make act-validate          - Validate workflow syntax"
	@echo "  make act-test              - Dry run of the workflow"
	@echo "  make act-test-run          - Actually run the workflow (needs Docker)"
	@echo "  make act-test-setup        - Test the setup job"
	@echo "  make act-test-setup-dry    - Test the setup job (dry run)"
	@echo "  make act-test-job JOB=name - Test a specific job"
	@echo "  make act-test-python       - Test test job with Python 3.11"
	@echo ""
	@echo "Note: Most commands require Docker to be running."
	@echo "See .github/ACT_USAGE.md for more details."