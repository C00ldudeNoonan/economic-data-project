# Repository Guidelines

## Project Structure & Module Organization
- `macro_agents/`: Dagster project and AI agents (`src/macro_agents/`), tests in `macro_agents/tests/`.
- `dbt_project/`: dbt models and configs (`models/` for staging, markets, analysis, etc.).
- Root `makefile`: common dev, test, lint, and setup commands.

## Build, Test, and Development Commands
- `make setup-dagster`: install Dagster dependencies via `uv`.
- `make run-dagster`: start Dagster UI on `:3000`.
- `make test`: run Dagster tests via `pytest` (uses `uv`).
- `make ruff`: lint/format Python with Ruff.
- `make lint` / `make fix`: lint or fix dbt SQL with SQLFluff.
- `make typecheck-dagster`: run `ty` type-checker over `macro_agents`.
- `make dbt-manifest`: parse dbt project.

## Worktrunk Hooks (wt)
- Project hooks live in `.config/wt.toml`; user hooks live in `~/.config/worktrunk/config.toml`. Project hooks require first-run approval.
- Hook types include `pre-commit` (formatters/linters/type checks before the merge commit) and `pre-merge` (tests/build verification after rebase, before merge).
- Hooks run automatically during `wt merge`; manually run with `wt hook pre-commit`, `wt hook pre-merge`, or `wt hook show`. Use `--no-verify` to skip hooks or `--yes` to auto-approve.
- Recommended alias: add `alias wsc='wt switch --create --execute=claude'` to your shell config, then use `wsc <branch>` or `wsc <branch> -- 'task'` to create/switch a worktree, run hooks, and launch Claude with forwarded args.

## Git Worktrees (Recommended For Parallel Sessions)
- Use a dedicated worktree per Claude session to avoid clobbering edits in the same working directory.
- Create a new worktree from `main` using an issue-based branch name: `git worktree add -b feat/issue-123-signal-charts ../economic-data-project-full-wt-issue-123 main`
- List worktrees: `git worktree list`
- Remove a worktree when done (from the primary repo): `git worktree remove ../economic-data-project-full-wt-issue-123`

## Dagster `dg` Cheat Sheet
- `cd macro_agents` before running `dg` commands.
- Start local Dagster UI: `uv run dg dev`
- Validate definitions: `uv run dg check --module-name macro_agents.definitions`
- List jobs/assets: `uv run dg list`
- Launch a job: `uv run dg launch --module-name macro_agents.definitions --job dspy_analysis_job`
- Launch selected assets (comma-separated): `uv run dg launch --module-name macro_agents.definitions --job dspy_analysis_job --assets analyze_economy_state,generate_economy_state_charts`
- Launch with config file: `uv run dg launch --module-name macro_agents.definitions --job dspy_analysis_job --config /tmp/econ_state_charts.yaml`

## GitHub CLI (gh) Cheat Sheet
- Check auth: `gh auth status`
- List issues: `gh issue list`
- Create issue: `gh issue create`
- Create PR: `gh pr create`
- View PR: `gh pr view`
- List PRs: `gh pr list`
- Check PR status: `gh pr checks`
- Add PR comment: `gh pr comment`
- Checkout PR: `gh pr checkout <number>`

## Coding Style & Naming Conventions
- Python: 4-space indentation; use Ruff for formatting/linting (`make ruff`).
- dbt SQL: SQLFluff enforced style (`make lint`, `make fix`).
- Naming: keep module names and functions descriptive; follow existing patterns in each subproject.

## Testing Guidelines
- Framework: `pytest` for Python (`macro_agents`).
- Run Dagster tests: `cd macro_agents && uv run pytest tests/ -v`.
- Test standards:
  - Prefer behavior-focused assertions (status, metadata, materialized values) over snapshots.
  - Cover loading, empty, error, and populated states.
  - Use deterministic fixtures and local resources (temp DuckDB, ephemeral Dagster instances); avoid network calls.
  - For Dagster assets, use `DagsterInstance.ephemeral()` and `build_op_context` with mocked resources.
  - Keep tests isolated and fast; clean up temp files and avoid shared mutable state.

## Commit & Pull Request Guidelines
- Commit messages are typically short and imperative; many use Conventional Commit prefixes (`feat:`, `fix:`, `chore:`).
- Prefer small, focused commits with clear intent.
- PRs should include a concise summary, linked issues (if applicable), and screenshots for UI changes.
- Never merge PRs. Always leave merging to a human reviewer.
- Add a PR comment tagging `@codex` to request review immediately after opening every PR. Use `gh pr comment <PR_NUMBER> --body "@codex please review this PR."` and mention the review request in the handoff.

## Data Request Scoping (Wizard Loop 1)

A GitHub issue is dbt-workable if it involves models, tests, sources, metrics, schema,
SQL transformations, data quality, or freshness. When scoping a request:

- **Identify the domain**: staging, markets, government, commodities, signals, analysis,
  backtesting, data_quality, agents_preprocess, telemetry, SEC, FOMC, social/Reddit.
- **Identify affected models**: which models need to be created or modified?
- **Trace upstream**: which sources or raw tables feed the affected area?
- **Trace downstream**: which marts, signals, or agent inputs depend on it?
- **Estimate scope**: single model addition, multi-model refactor, or new domain?

Mark as **blocked** if the request requires new data source ingestion (Dagster asset work),
infrastructure changes, or is too ambiguous to scope without human clarification.

## Pipeline Incident Triage (Wizard Loop 2)

| Severity | Criteria | Response |
|----------|----------|----------|
| **P1** | Production data incorrect — wrong values in marts, signals, or agent inputs | Investigate immediately, escalate to human if fix is high-risk |
| **P2** | Test failures, stale models (>24hr not refreshed), broken contracts | Investigate and draft fix PR within the loop run |
| **P3** | Coverage gaps — models with no tests, missing descriptions, undocumented columns | Batch into weekly coverage PR (Mondays) |

## Common Failure Patterns

| Source | Pattern | Likely cause | Diagnosis |
|--------|---------|--------------|-----------|
| FRED API | `429 Too Many Requests` | Rate limit (30 req/min) | Check ingestion batch size, add backoff |
| FRED API | Series returns empty | Series discontinued or ID changed | Verify series ID at fred.stlouisfed.org |
| MarketStack | Timeout on batch request | Too many tickers in one call | Reduce partition batch size |
| MarketStack | Missing weekend/holiday data | Market closed | Not a bug — filter expected gaps |
| BigQuery | `quotaExceeded` | Project quota hit | Check BQ console, reduce concurrent queries |
| BigQuery | `notFound` on table | Schema drift or dataset mismatch | Verify dataset name matches `DBT_TARGET` environment |
| SEC EDGAR | `403 Forbidden` / slow responses | 10 req/sec throttle | Check rate limiting in `sec_edgar.py` |
| dbt | Incremental model stale | Full refresh needed after schema change | Run `dbt run --full-refresh --select model_name` |
| dbt | Source freshness failure | Upstream ingestion didn't run | Check Dagster asset materialization status |
| Dagster | Asset materialization timeout | Long-running query or API delay | Check `dagster.yaml` timeout config (2hr default) |

## Fix Risk Classification

| Risk level | PR type | Examples |
|------------|---------|----------|
| **Low** (auto-PR) | Regular PR, labeled `wizard-auto-fix` | Add missing test, update description, fix typo in schema.yml, add missing `not_null` test |
| **Medium** (draft PR, needs review) | Draft PR, labeled `wizard-auto-fix` + `needs-review` | Fix SQL logic bug, add missing model, change source config, update freshness thresholds |
| **High** (escalate to human) | No PR — write to `health/needs-human/` | Change materialization strategy, modify grain, alter contracted model, change incremental strategy, delete model |

## Configuration & Secrets
- Create `macro_agents/.env` with required API keys (FRED, MarketStack, OpenAI, MotherDuck).
- Keep secrets out of git; use environment variables or local `.env` files.

## File Storage Guidelines
- **Production**: Use cloud storage (S3, GCS, Azure Blob) or database connections. Never save files to local Dagster storage in production environments.
- **Local Development**: Local file storage is acceptable for development/testing purposes only.
- **Best Practice**: Load files directly into memory (using `BytesIO` or similar) from cloud storage or APIs, then write directly to databases. Avoid intermediate local file writes.
- **Examples**:
  - ✅ Load CSV from S3 directly into Polars DataFrame, then to DuckDB
  - ✅ Download file content from API into memory, parse, write to database
  - ✅ Use `tempfile` only in tests, not in production assets
  - ❌ Save files to local disk before processing in production
  - ❌ Use local file paths in production Dagster assets

# Structuring Your Codebase for LLM Coding Agents

## Why This Matters

LLM coding agents read, re-read, and iterate on your files across multi-step loops. Every file load costs tokens. A single task might load the same file 3-5+ times (read → edit → validate → fix). Your codebase structure directly controls how expensive and effective those loops are.

## File Size and Scoping

**Target 100-400 lines per file.** This balances token cost per read against navigation overhead.

- Each file should have a single, clear responsibility
- If a file requires scrolling through unrelated logic to find what matters, split it
- If splitting creates files that are always loaded together, keep them combined
- Cohesion beats small size — 10 tightly coupled 30-line files cost more than one 300-line file because the agent loads all 10 anyway

## Directory Structure

**Make the structure self-documenting.** Agents spend tokens exploring your repo to orient themselves. Reduce that cost.

- Use descriptive directory and file names that telegraph contents
- Group by domain/feature, not by type (e.g., `billing/` over `models/`, `services/`, `utils/`)
- Keep nesting shallow — 2-3 levels max
- Place related files close together in the tree

## Reduce Agent Navigation Tokens

- Maintain a project map file (this file, `CLAUDE.md`, `AGENTS.md`, `CONVENTIONS.md`) at the repo root
- Document non-obvious architecture decisions inline where they apply
- Use consistent naming conventions so the agent can predict file locations
- Keep imports explicit — barrel files and re-exports obscure dependency chains

## Code Conventions That Help Agents

- Write clear function and class names that describe behavior
- Keep functions short and single-purpose (easier to target edits)
- Use type hints and type annotations — they give agents context without reading implementations
- Place constants and config near where they're used, not in distant shared files
- Prefer explicit over clever — agents parse straightforward code faster and more reliably

## What to Avoid

- Monolith files (1000+ lines) — agents pay full token cost even for one-line changes
- Circular dependencies — agents get stuck in loops trying to resolve context
- Deep inheritance chains — forces loading many files to understand one class
- Magic strings and implicit behavior — agents can't infer what they can't see
- Overly DRY abstractions that scatter logic across many files for a single concept

## Project Map Template

Keep this at your repo root. It gives agents a cheap orientation pass instead of exploratory file reads.

```markdown
# Project: [Name]

## Quick Reference
- Language: [e.g., Python 3.12]
- Framework: [e.g., FastAPI, Next.js]
- Package manager: [e.g., uv, pnpm]
- Test runner: [e.g., pytest, vitest]

## Architecture
[2-3 sentences on how the app is structured]

## Key Directories
- `src/api/` — Route handlers and request validation
- `src/core/` — Business logic and domain models
- `src/db/` — Database models, migrations, queries
- `src/integrations/` — Third-party service clients

## Common Tasks
- Run tests: `[command]`
- Start dev server: `[command]`
- Run migrations: `[command]`

## Conventions
- [List project-specific patterns the agent should follow]
- [e.g., "All API endpoints return Pydantic response models"]
- [e.g., "Use repository pattern for database access"]
```

## The Token Math

| Scenario | File Size | Agent Reads | Tokens Used |
|---|---|---|---|
| Monolith | 2,000 lines | 4x per task | ~8,000 lines |
| Well-scoped | 200 lines | 4x per task | ~800 lines |
| Over-split | 30 lines × 8 files | 4x per task | ~960 lines + navigation overhead |

The well-scoped approach isn't just cheaper — it's more reliable. Smaller, focused context means fewer hallucinations and more accurate edits.

## Skills
- `context-doc-maintainer`: Automated agent that reviews and updates all Claude context files when code changes are made. (`.claude/skills/context-doc-maintainer/SKILL.md`)
- `create-custom-dagster-component`: Create a custom Dagster Component with demo mode support, realistic asset structure, and optional custom scaffolder using the dg CLI. Use this skill if there is no Component included in an existing integration or if Dagster does not have the integration. (`.claude/skills/create-custom-dagster-component/SKILL.md`)
- `dagster-expert`: Expert guidance for Dagster and the `dg` CLI. Use before any Dagster-specific task or question. (`.claude/skills/dagster-expert/SKILL.md`)
- `dagster-development`: Project-specific Dagster patterns for assets, resources, schedules, sensors, and testing. Use alongside `dagster-expert` when editing this repo. (`.claude/skills/dagster-development/SKILL.md`)
- `dagster-init`: Initialize a dagster project using the create-dagster cli. Create a dagster project, uv virtual environment, and everything needed for a user to run dg dev or dg check defs successfully. (project) (`.claude/skills/dagster-init/SKILL.md`)
- `dbt-development`: Expert guidance for developing dbt projects including project structure, model patterns, testing, and best practices. Use when working with dbt models, building data transformations, or setting up dbt projects. (`.claude/skills/dbt-development/dbt_skill.md`)
- `design-philosophy`: Canonical data-first visualization guidance combining Tufte + Jobs/Ive principles for React/Nivo dashboards. (`.claude/skills/design-philosophy/SKILL.md`)
- `design-philosphy`: Deprecated alias for `design-philosophy`. (`.claude/skills/design-philosphy/SKILL.md`)
- `dignified-python`: Production Python coding standards for 3.10-3.13 (general Python, not Dagster-specific). (`.claude/skills/dignified-python/SKILL.md`)
- `docker-env`: Standardized Docker/Dagster environment debugging workflows. (`.claude/skills/docker-env/SKILL.md`)
- `git-pr-workflow`: Automates the complete git and GitHub workflow: initializes a git repository, creates a private GitHub repository if needed, commits changes, creates a pull request, monitors GitHub Actions for completion, and merges the PR if all checks pass. (`.claude/skills/git-pr-workflow/SKILL.md`)
- `github-explorer`: General-purpose GitHub exploration and analysis tool for searching issues, creating PRs, analyzing git history, and reviewing PR comments. (`.claude/skills/github-explorer/SKILL.md`)
- `nivo-charts`: Deprecated alias for `design-philosophy`. (`.claude/skills/nivo-charts/SKILL.md`)
- `react-component-architecture` (React Data Dashboard Architecture): React component patterns, hooks, and state management for data visualization dashboards. (`.claude/skills/react-component-architecture/SKILL.md`)
- `start-issue`: Issue → plan → branch workflow automation. (`.claude/skills/start-issue/SKILL.md`)
- `tailwind-css-data-viz` (Tailwind CSS Data Visualization Patterns): Tailwind CSS configurations and patterns for building data-dense financial dashboard UIs. (`.claude/skills/tailwind-css-data-viz/SKILL.md`)
- `testing-strategies`: Comprehensive testing patterns and examples for Dagster, dbt, FastAPI, and React. (`.claude/skills/testing-strategies/SKILL.md`)
- `tufte-visualization-principles`: Deprecated alias for `design-philosophy`. (`.claude/skills/tufte-visualization/SKILL.md`)
- `typescript-financial-data-modeling` (TypeScript Financial Data Modeling): Type-safe data modeling patterns for financial and economic data visualization applications. (`.claude/skills/typescript-financial-data-modeling/SKILL.md`)
