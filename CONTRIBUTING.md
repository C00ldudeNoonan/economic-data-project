# Contributing

Thanks for your interest. This is a personal project, but contributions are welcome.

## Local setup

1. Clone the repo and copy the example env files:
   ```bash
   cp .env.example .env
   cp .env.dagster.example .env.dagster
   ```
   Fill in your own API keys (FRED, BLS, MarketStack, MotherDuck, etc.).
2. Install Python dependencies with `uv sync` in `macro_agents/` and any other subproject you need.
3. Start the local stack with `docker-compose -f docker-compose.local.yml up` or run Dagster directly with `dg dev` from `macro_agents/`.

See `Readme.md` and `DAGSTER_OSS_QUICKSTART.md` for more detail.

## Branch and PR conventions

- Branch off `main` using a prefixed name:
  - `feat/issue-{number}-{short-description}`
  - `fix/issue-{number}-{short-description}`
  - `docs/{short-description}`, `refactor/{short-description}`, `test/{short-description}`, `chore/{short-description}`
- Never push directly to `main`. Always open a PR.
- Keep PRs focused — one feature or fix per PR.
- Run `ruff`, `sqlfluff`, and the relevant test suite before pushing.

## Issues

Bug reports and feature requests are tracked on GitHub Issues. Please include reproduction steps and the affected component (Dagster asset, dbt model, frontend page, etc.).
