# Economic Data Project

An end-to-end data platform that ingests, transforms, and analyzes economic and financial market data. Pipelines run on self-hosted Dagster OSS (Docker) and store everything in Google BigQuery. Data is queried and explored via AI agents using the dbt-index MCP server's warehouse tools.

## Architecture

```
External APIs → Dagster (ingestion) → Google BigQuery
                 └→ GCS documents → dbt-ml extraction → BigQuery
                                          ↓
                              dbt (SQL transformations)
                                          ↓
                         DSPy AI agents (analysis & signals)
                                          ↓
                         Claude + dbt-index MCP (query interface)
```

## Project Structure

```
economic-data-project/
├── macro_agents/                    # Dagster project
│   ├── src/macro_agents/defs/
│   │   ├── domains/                 # Ingestion assets by domain
│   │   │   ├── macro.py             # FRED, Treasury yields, FOMC minutes
│   │   │   ├── markets/             # MarketStack — indices, sectors, ETFs, commodities
│   │   │   ├── housing.py           # Realtor.com + housing pulse data
│   │   │   ├── social.py            # Reddit ingestion
│   │   │   ├── fomc_transcripts.py  # FOMC transcript PDFs
│   │   │   └── sec/                 # SEC 10-K/10-Q filings
│   │   ├── signals/                 # Computed market signals
│   │   │   ├── turbulence_index.py  # Mahalanobis market turbulence
│   │   │   ├── absorption_ratio.py  # PCA systemic risk signal
│   │   │   ├── fear_greed_composite.py
│   │   │   ├── entropy_complexity.py
│   │   │   └── network_correlation.py
│   │   ├── analysis/                # AI analysis pipelines
│   │   │   ├── economy_state/       # Economic cycle classification
│   │   │   ├── investments/         # Investment recommendations + charts
│   │   │   ├── narratives/          # Plain-English economic narratives
│   │   │   └── news/                # Reddit + FOMC weekly summaries
│   │   ├── transformation/          # dbt orchestration + FCI
│   │   ├── backtesting/             # Strategy backtesting
│   │   ├── resources/               # Shared resources (BigQuery, GCS, etc.)
│   │   └── telemetry/               # Pipeline monitoring assets
│   └── tests/                       # 440+ test suite
├── dbt_project/                     # SQL transformations
│   └── models/
│       ├── staging/                 # Raw → clean
│       ├── markets/                 # Returns, summaries
│       ├── government/              # FRED, housing, yields
│       ├── commodities/             # Commodity analysis
│       └── analysis/                # Cross-domain analytics
├── document_extraction/              # dbt-ml SEC/FOMC document extraction
├── docker-compose.yml               # Local/production deployment
├── dagster.yaml                     # Dagster OSS config
└── makefile                         # Common commands
```

## Quick Start (Local)

### Prerequisites
- Docker Desktop
- API keys (see `.env.example`)
- Google Cloud project with BigQuery enabled + a service account (see `GOOGLE_APPLICATION_CREDENTIALS`)

### Setup

```bash
# 1. Clone and configure
git clone https://github.com/C00ldudeNoonan/economic-data-project.git
cd economic-data-project
cp .env.example .env
# Fill in your API keys in .env

# 2. Build and start
docker compose --env-file .env build
docker compose --env-file .env up
```

Dagster UI: `http://localhost:3000`

### Development (without Docker)

```bash
# From the repository root, install Python and locked dbt dependencies
make setup-dagster
make dbt-manifest

cd macro_agents

# Validate definitions
uv run dg check defs

# Run tests
uv run pytest tests/ -v

# Lint
uv run ruff check .
uv run ruff format .

# Validate dbt-ml document extraction without materializing data
cd ../document_extraction
uv sync
uv run dbt-ml compile --target dev
```

## Data Sources

| Source | Data |
|--------|------|
| FRED | 70+ economic series — GDP, inflation, employment, yield curve |
| MarketStack | S&P 500 / NASDAQ prices, ETFs, indices, commodities, currencies |
| Treasury / FRED | Yield curve (1m → 30yr) |
| SEC EDGAR | 10-K / 10-Q filings for S&P 500 companies |
| Federal Reserve | FOMC minutes and transcripts |
| Reddit | r/investing, r/stocks, r/economics, r/wallstreetbets |
| Realtor.com | Housing inventory by geography |
| BLS / Census | Labor market and population data |

## Querying the Data

The platform is designed to be queried via AI agents using the dbt-index MCP server (see `.mcp.json`), whose `warehouse` tool runs SQL directly against BigQuery. With that server configured, Claude Code can query any table directly in conversation.

```bash
# Example: ask Claude about the data
# "What's the current turbulence index reading?"
# "Show me the latest FOMC sentiment scores"
# "Which S&P 500 sectors had the best returns this month?"
```

## Deployment (GCP)

See [DAGSTER_OSS_QUICKSTART.md](./DAGSTER_OSS_QUICKSTART.md) for full GCP VM deployment instructions.

The production setup runs the same `docker-compose.yml` on a GCP VM with a persistent PostgreSQL disk for the Dagster event log.

## Environment Variables

Copy `.env.example` to `.env`. Required keys:

| Variable | Description |
|----------|-------------|
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to a GCP service-account JSON file (or inline JSON) for BigQuery |
| `BIGQUERY_PROJECT` | GCP project ID hosting the BigQuery datasets |
| `BIGQUERY_DATASET` | Default BigQuery dataset (defaults to `economics_raw*` per environment) |
| `FRED_API_KEY` | FRED API key |
| `MARKETSTACK_API_KEY` | MarketStack API key |
| `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` | LLM provider for AI agents |
| `GITHUB_TOKEN` | For asset failure issue creation |
| `GITHUB_REPO_OWNER` | GitHub username |
| `GITHUB_REPO_NAME` | GitHub repo name |

Optional: `GCS_BUCKET_NAME`, `GOOGLE_APPLICATION_CREDENTIALS`, `SEC_EDGAR_CONTACT_EMAIL`, `CENSUS_API_KEY`

## Common Commands

```bash
# Docker
docker compose --env-file .env up          # Start stack
docker compose --env-file .env down        # Stop stack
docker compose --env-file .env build       # Rebuild images
docker compose logs -f dagster_user_code   # Tail user code logs

# Development
make dbt-deps                            # Install locked dbt packages
make dbt-manifest                        # Generate the local manifest
cd macro_agents
uv run pytest tests/ -v                    # Run tests
uv run ruff check . && ruff format .       # Lint + format
uv run dg check defs                       # Validate Dagster definitions
```

## Tech Stack

- **[Dagster](https://docs.dagster.io)** — asset orchestration, schedules, sensors
- **[dbt](https://docs.getdbt.com)** — SQL transformations
- **[Google BigQuery](https://cloud.google.com/bigquery/docs)** — serverless analytical data warehouse
- **[DSPy](https://dspy.ai)** — LLM pipeline framework for AI agents
- **[Polars](https://docs.pola.rs)** — dataframe processing
- **[uv](https://docs.astral.sh/uv/)** — Python package management
