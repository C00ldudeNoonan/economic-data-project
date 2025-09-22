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
