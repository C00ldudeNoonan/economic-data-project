# Economic Data Project

An end-to-end data application that ingests, transforms, and analyzes economic and financial market data using modern open-source tools. The application combines traditional data engineering workflows with AI-powered analysis agents to provide insights into economic cycles, market trends, and asset allocation strategies.

## Tools and Technologies

### Core Frameworks
- **Dagster**: Orchestration framework for data pipelines, asset management, schedules, and sensors
  - [Dagster Documentation](https://docs.dagster.io)
- **dbt**: SQL-based transformation framework for data modeling and analytics
  - [dbt Documentation](https://docs.getdbt.com)
- **DSPy**: Framework for building and optimizing AI agents with LLMs
  - [DSPy Documentation](https://dspy-docs.vercel.app)
- **DuckDB/MotherDuck**: Embedded analytical database with cloud sync capabilities
  - [DuckDB Documentation](https://duckdb.org/docs)
  - [MotherDuck Documentation](https://motherduck.com/docs)

### Supporting Technologies
- **Polars**: High-performance dataframe library
  - [Polars Documentation](https://docs.pola.rs)
- **Sling**: Data replication tool for syncing between databases
  - [Sling Documentation](https://docs.slingdata.io)
- **BigQuery**: Cloud data warehouse for replication target
  - [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- **Python**: Primary programming language (3.10-3.13)

## Data Sources

All data is sourced from publicly available APIs:

### Economic Data
- **Federal Reserve Economic Data (FRED)**: Comprehensive economic indicators including GDP, inflation, employment, housing, trade, and financial conditions
  - [FRED API Documentation](https://fred.stlouisfed.org/docs/api/fred/)
- **Bureau of Labor Statistics (BLS)**: Employment and labor market data
  - [BLS API Documentation](https://www.bls.gov/developers/api_signature.htm)
- **Census Bureau**: Population and demographic data
  - [Census Bureau API Documentation](https://www.census.gov/data/developers/data-sets.html)

### Market Data
- **Market Stack API**: Stock market data for major indices, sectors, global markets, currencies, fixed income, and commodities
  - [Market Stack API Documentation](https://marketstack.com/documentation)
- **Treasury Yields**: U.S. Treasury bond yield curve data
- **Realtor.com**: Housing market inventory and pricing data
  - [Realtor.com Research Data](https://www.realtor.com/research/data/)

## Project Structure

```
economic-data-project/
├── macro_agents/                    # Main Dagster project
│   ├── src/macro_agents/
│   │   ├── definitions.py            # Central Dagster definitions
│   │   └── defs/
│   │       ├── ingestion/           # Data ingestion assets
│   │       │   ├── fred.py          # FRED economic data
│   │       │   ├── bls.py           # Bureau of Labor Statistics
│   │       │   ├── market_stack.py  # Market data API
│   │       │   └── treasury_yields.py
│   │       ├── transformation/     # Data transformation
│   │       │   ├── dbt.py           # dbt integration
│   │       │   └── financial_condition_index.py
│   │       ├── agents/              # AI analysis agents (DSPy)
│   │       │   ├── analysis_agent.py
│   │       │   ├── economic_cycle_analyzer.py
│   │       │   ├── asset_allocation_analyzer.py
│   │       │   └── backtesting.py
│   │       ├── resources/           # Dagster resources
│   │       │   ├── motherduck.py   # DuckDB/MotherDuck connection
│   │       │   ├── fred.py          # FRED API resource
│   │       │   └── market_stack.py  # Market Stack API resource
│   │       ├── replication/         # Data replication
│   │       │   └── sling.py         # Sling replication to BigQuery
│   │       └── schedules.py         # Dagster schedules, sensors, jobs
│   └── tests/                       # Test suite
├── dbt_project/                     # dbt transformation project
│   ├── dbt_project.yml              # dbt configuration
│   ├── profiles.yml                 # Connection profiles
│   └── models/
│       ├── staging/                 # Staging layer models
│       ├── government/              # Government data models
│       ├── markets/                 # Market data models
│       ├── commodities/             # Commodity data models
│       └── analysis/                # Analysis layer models
├── dagster_cloud.yaml               # Dagster Cloud deployment config
└── makefile                         # Build and automation commands
```

## Data Flow

### 1. Ingestion Layer (Dagster Assets)
Raw data assets pull from external APIs and store in DuckDB/MotherDuck:
- **FRED Data**: Partitioned by 70+ economic series codes, scheduled weekly
- **Market Data**: Partitioned by ticker and month for indices, sectors, commodities, currencies
- **Treasury Yields**: Daily yield curve data
- **Housing Data**: Inventory and pricing data from BLS and Realtor.com

### 2. Transformation Layer (dbt Models)
SQL-based transformations organized in layers:
- **Staging**: Standardizes and cleans raw data (`stg_*` models)
- **Government**: Aggregates economic indicators (`fred_*`, `housing_*` models)
- **Markets**: Analyzes market returns and summaries (`*_summary`, `*_analysis_return` models)
- **Commodities**: Commodity-specific analysis
- **Analysis**: Combines economic and market data for advanced analytics (`base_historical_analysis`, `leading_econ_return_indicator`)

### 3. AI Analysis Layer (DSPy Agents)
AI-powered analysis agents that operate on transformed data:
- **Economic Cycle Analysis**: Identifies economic phases (expansion, peak, contraction, trough)
- **Asset Allocation**: Generates portfolio recommendations based on economic conditions
- **Backtesting**: Tests investment strategies against historical data
- **Model Evaluation**: Continuous improvement of AI models using DSPy metrics

### 4. Replication Layer (Sling)
Replicates transformed data from MotherDuck to BigQuery for downstream consumption.

## Environment Variables

Create a `.env` file in the `macro_agents` directory with the following variables:

### Required
- `MODEL_NAME`: OpenAI model to use (e.g., `gpt-4-turbo-preview`, `gpt-3.5-turbo`)
- `OPENAI_API_KEY`: OpenAI API authentication key
- `FRED_API_KEY`: Federal Reserve Economic Data API key
- `MARKETSTACK_API_KEY`: Market Stack API key
- `MOTHERDUCK_TOKEN`: MotherDuck authentication token (for cloud sync)

### Optional (Development)
- `ENVIRONMENT`: Environment setting (`dev` or `prod`, defaults to `dev`)
- `DBT_TARGET`: dbt target environment (`local`, `dev`, or `prod`, defaults to `local`)
- `DBT_PROJECT_DIR`: Path to dbt project directory (auto-detected if not set)

### Optional (Production/Replication)
- `MOTHERDUCK_DATABASE`: MotherDuck database name
- `MOTHERDUCK_PROD_SCHEMA`: MotherDuck production schema
- `BIGQUERY_PROJECT_ID`: Google Cloud project ID for BigQuery
- `BIGQUERY_LOCATION`: BigQuery dataset location
- `BIGQUERY_DATASET`: BigQuery dataset name
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to Google Cloud service account credentials JSON file
- `CENSUS_API_KEY`: Census Bureau API key (if using Census data)

## Quick Start

### Prerequisites
- Python 3.10-3.13
- uv (recommended) or pip for package management
- DuckDB and MotherDuck account (for cloud sync)
- API keys for data sources
- OpenAI API key for AI agents

### Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd economic-data-project
```

2. **Install dependencies**
```bash
cd macro_agents
uv sync  # or pip install -e .[dev]
```

3. **Install dbt packages**
```bash
cd ../dbt_project
dbt deps
```

4. **Set up environment variables**
Create a `.env` file in the `macro_agents` directory with required variables (see Environment Variables section above).

5. **Validate setup**
```bash
# Test Dagster definitions
cd macro_agents
dg check defs

# Test dbt models
cd ../dbt_project
dbt compile
dbt parse
```

### Running Locally

**Start Dagster UI:**
```bash
cd macro_agents
dagster dev
```
Navigate to `http://localhost:3000` to view and materialize assets.

**Run dbt models manually:**
```bash
cd dbt_project
dbt run          # Run all models
dbt run --select staging.*  # Run specific layer
```

**Run tests:**
```bash
cd macro_agents
pytest tests/ -v
# Or use the makefile
make test
```

## Deployment

The project is configured for deployment on Dagster Cloud using the `dagster_cloud.yaml` configuration file. The deployment builds from the `macro_agents` directory and uses `macro_agents.definitions` as the entry point.

## Development

### Common Commands

```bash
# Run tests
make test

# Lint Python code
make ruff

# Lint SQL code
make lint

# Fix SQL linting issues
make fix

# Run pre-PR checks (linting, type checking, tests, security scans)
make pre-pr
```

### First Run Workflow

1. **Materialize ingestion assets** - Start with FRED data or Market Stack data via Dagster UI
2. **Run dbt transformations** - Transform raw data through staging → marts → analysis layers (automated via eager assets)
3. **Run analysis agents** - Execute DSPy agents on transformed data via Dagster UI
4. **View results** - Check DuckDB/MotherDuck for analysis outputs

## Automation

- **Ingestion Assets**: Scheduled weekly on Mondays at midnight (FRED data)
- **dbt Models**: Eager automation (run automatically when upstream data changes)
- **Analysis Agents**: On-demand or scheduled via Dagster jobs
- **Replication**: Monthly partitioned replication to BigQuery via Sling

## Testing

Test suite located in `macro_agents/tests/`:
- Unit tests for analysis agents
- Integration tests for end-to-end workflows
- Tests for Dagster asset descriptions
- Tests for dbt model descriptions
- Resource and schedule tests

Run tests using `make test` or `pytest tests/ -v` from the `macro_agents` directory.
