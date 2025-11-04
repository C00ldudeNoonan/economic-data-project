# Economic Data Application

## Overview

This project is an end-to-end data application that ingests, transforms, and analyzes economic and financial market data using modern open-source tools. The application combines traditional data engineering workflows with AI-powered analysis agents to provide insights into economic cycles, market trends, and asset allocation strategies.

## Tech Stack

### Core Frameworks
- **Dagster**: Orchestration framework for data pipelines, asset management, schedules, and sensors
- **dbt**: SQL-based transformation framework for data modeling and analytics
- **DSPy**: Framework for building and optimizing AI agents with LLMs
- **DuckDB/MotherDuck**: Embedded analytical database with cloud sync capabilities

### Supporting Technologies
- **Python**: Primary programming language
- **Polars**: High-performance dataframe library (mentioned in README)
- **OpenAI API**: LLM integration for analysis agents

## Project Structure

```
economic-data-project/
├── macro_agents/                          # Main Dagster project
│   ├── src/macro_agents/
│   │   ├── definitions.py                 # Central Dagster definitions
│   │   └── defs/
│   │       ├── ingestion/                 # Data ingestion assets
│   │       │   ├── fred.py                # Federal Reserve Economic Data
│   │       │   ├── bls.py                 # Bureau of Labor Statistics
│   │       │   ├── market_stack.py        # Market data API
│   │       │   └── treasury_yields.py     # Treasury yield data
│   │       ├── transformation/            # Data transformation
│   │       │   ├── dbt.py                 # dbt integration
│   │       │   └── financial_condition_index.py
│   │       ├── agents/                    # AI analysis agents (DSPy)
│   │       │   ├── analysis_agent.py
│   │       │   ├── economic_cycle_analyzer.py
│   │       │   ├── enhanced_economic_cycle_analyzer.py
│   │       │   ├── asset_allocation_analyzer.py
│   │       │   ├── backtesting.py
│   │       │   ├── backtesting_visualization.py
│   │       │   ├── dspy_evaluation.py
│   │       │   ├── model_improvement_pipeline.py
│   │       │   └── economic_dashboard.py
│   │       ├── resources/                 # Dagster resources
│   │       │   ├── motherduck.py          # DuckDB/MotherDuck connection
│   │       │   ├── fred.py                # FRED API resource
│   │       │   └── market_stack.py        # Market Stack API resource
│   │       ├── constants/                 # Configuration constants
│   │       │   ├── fred_series_lists.py
│   │       │   └── market_stack_constants.py
│   │       └── schedules.py               # Dagster schedules, sensors, jobs
│   └── tests/                             # Test suite
│       ├── test_analysis_agents.py
│       ├── test_dagster_assets_descriptions.py
│       ├── test_dbt_models_descriptions.py
│       ├── test_integration.py
│       ├── test_schedules.py
│       └── test_resources.py
├── dbt_project/                           # dbt transformation project
│   ├── dbt_project.yml                    # dbt configuration
│   ├── profiles.yml                       # Connection profiles
│   └── models/
│       ├── staging/                       # Staging layer models
│       │   ├── stg_fred_series.sql
│       │   ├── stg_housing_inventory.sql
│       │   ├── stg_major_indices.sql
│       │   ├── stg_us_sectors.sql
│       │   ├── stg_global_markets.sql
│       │   ├── stg_currency.sql
│       │   ├── stg_fixed_income.sql
│       │   └── stg_treasury_yields.sql
│       ├── government/                    # Government data models
│       │   ├── fred_series_grain.sql
│       │   ├── fred_monthly_diff.sql
│       │   ├── fred_quarterly_roc.sql
│       │   ├── fred_series_latest_aggregates.sql
│       │   ├── housing_inventory.sql
│       │   ├── housing_mortgage_rates.sql
│       │   └── housing_inventory_and_population.sql
│       ├── markets/                       # Market data models
│       │   ├── major_indicies_summary.sql
│       │   ├── major_indicies_analysis_return.sql
│       │   ├── us_sector_summary.sql
│       │   ├── us_sector_analysis_return.sql
│       │   ├── global_markets_summary.sql
│       │   ├── global_markets_analysis_return.sql
│       │   ├── currency_summary.sql
│       │   ├── currency_analysis_return.sql
│       │   └── fixed_income_analysis_return.sql
│       └── analysis/                      # Analysis layer models
│           ├── base_historical_analysis.sql
│           ├── market_economic_analysis.sql
│           └── leading_econ_return_indicator.sql
├── dagster_cloud.yaml                     # Dagster Cloud deployment config
└── makefile                               # Build and automation commands
```

## Data Sources

All data is sourced from publicly available APIs:

### Economic Data
- **Federal Reserve Economic Data (FRED)**: Comprehensive economic indicators including GDP, inflation, employment, etc.
- **Bureau of Labor Statistics (BLS)**: Employment and labor market data
- **Census Bureau**: Population and demographic data

### Market Data
- **Market Stack API**: Stock market data for major indices, sectors, and global markets
- **Treasury Yields**: U.S. Treasury bond yield curve data
- **Realtor.com**: Housing market data

## Key Components

### 1. Data Ingestion (Dagster Assets)
Located in `macro_agents/src/macro_agents/defs/ingestion/`:
- Pulls data from various APIs on configured schedules
- Stores raw data in DuckDB/MotherDuck database
- Handles data quality and validation

### 2. Data Transformation (dbt Models)
Located in `dbt_project/models/`:
- **Staging Layer**: Standardizes and cleans raw data
- **Government Layer**: Aggregates and calculates metrics for economic indicators
- **Markets Layer**: Analyzes market returns, summaries, and trends
- **Analysis Layer**: Combines economic and market data for advanced analytics

### 3. AI Analysis Agents (DSPy)
Located in `macro_agents/src/macro_agents/defs/agents/`:

#### EconomicAnalyzer
General-purpose economic analysis using LLMs

#### EconomicCycleAnalyzer / EnhancedEconomicCycleAnalyzer
Identifies and analyzes economic cycles (expansion, peak, contraction, trough)

#### AssetAllocationAnalyzer
Provides asset allocation recommendations based on economic conditions

#### BacktestingEngine
Tests investment strategies against historical data

#### BacktestingVisualizer
Creates visualizations of backtesting results

#### FinancialEvaluator
Evaluates the performance of AI models using DSPy metrics

#### PromptOptimizer
Optimizes prompts for better AI agent performance

#### ModelImprovementPipeline
Continuous improvement pipeline for AI models

### 4. Resources
Located in `macro_agents/src/macro_agents/defs/resources/`:
- **motherduck_resource**: DuckDB/MotherDuck database connection
- **fred_resource**: FRED API client
- **marketstack_resource**: Market Stack API client
- **dbt_cli_resource**: dbt command-line integration

### 5. Orchestration
Located in `macro_agents/src/macro_agents/defs/schedules.py`:
- Schedules for automated data refreshes
- Sensors for event-driven workflows
- Jobs for batch processing

## Quick Start Guide

### Prerequisites
- Python 3.9 - 3.13
- uv (recommended) or pip for package management
- DuckDB and MotherDuck account (for cloud sync)
- API keys for data sources (FRED, Market Stack, etc.)
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
Create a `.env` file in the `macro_agents` directory:
```bash
MODEL_NAME=gpt-4-turbo-preview
OPENAI_API_KEY=your_openai_key
FRED_API_KEY=your_fred_key
BLS_API_KEY=your_bls_key
MARKETSTACK_API_KEY=your_marketstack_key
MOTHERDUCK_TOKEN=your_motherduck_token
DBT_TARGET=dev  # or prod
```

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
Then navigate to `http://localhost:3000`

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

### First Run Workflow

1. **Materialize ingestion assets** - Start with FRED data or Market Stack data
2. **Run dbt transformations** - Transform raw data through staging → marts → analysis layers
3. **Run analysis agents** - Execute DSPy agents on transformed data
4. **View results** - Check DuckDB/MotherDuck for analysis outputs

## Environment Variables

Required environment variables for the application:
```
MODEL_NAME          # OpenAI model to use (e.g., gpt-4, gpt-3.5-turbo)
OPENAI_API_KEY      # OpenAI API authentication key
FRED_API_KEY        # Federal Reserve Economic Data API key
BLS_API_KEY         # Bureau of Labor Statistics API key (if used)
MARKETSTACK_API_KEY # Market Stack API key
MOTHERDUCK_TOKEN    # MotherDuck authentication token
DBT_TARGET          # dbt target environment (dev or prod)
```

## Deployment

The project is configured for deployment on Dagster Cloud using the `dagster_cloud.yaml` configuration file. The deployment:
- Builds from the `macro_agents` directory
- Uses the module `macro_agents.definitions` as the entry point
- Sets working directory to `./macro_agents`

## Asset Dependencies & Data Flow

### High-Level Architecture

```
┌─────────────────────┐
│   External APIs     │
│  (FRED, BLS, etc.)  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Dagster Ingestion   │◄── Scheduled weekly (Mondays @ midnight)
│    Assets           │    Partitioned by series/ticker
│  (raw data tables)  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  dbt Staging Layer  │◄── Eager automation
│   (stg_* models)    │    Clean & standardize
└──────────┬──────────┘
           │
           ▼
┌─────────────────────────────────────────┐
│       dbt Transformation Layers         │
│  ┌────────────┐  ┌──────────────────┐  │◄── Eager automation
│  │ Government │  │     Markets      │  │    Calculate metrics
│  │   Models   │  │     Models       │  │    & aggregations
│  └──────┬─────┘  └────────┬─────────┘  │
│         └──────────┬───────┘            │
│                    ▼                     │
│         ┌──────────────────┐            │
│         │  Analysis Models │            │
│         │  (join layers)   │            │
│         └──────────────────┘            │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│         DSPy Analysis Agents            │◄── Depends on dbt models
│  (EconomicAnalyzer, BacktestingEngine,  │    Runs on-demand or scheduled
│   AssetAllocationAnalyzer, etc.)        │
└─────────────────────────────────────────┘
```

### Detailed Data Flow

#### 1. Ingestion Layer (Dagster Assets)
Raw data assets that pull from external APIs:

**FRED Economic Data:**
- `fred_raw` → Partitioned by 70+ series codes
- Schedule: Weekly on Mondays @ midnight (cron: `0 0 * * 1`)
- Outputs to: `fred_raw` table in DuckDB/MotherDuck
- Key series: GDP, CPI, employment, housing, etc.

**Market Data:**
- `market_stack_raw` → Stock/ETF market data
- `treasury_yields_raw` → US Treasury yield curves
- Output tables: `{source}_raw`

**Housing Data:**
- `bls_raw` → Bureau of Labor Statistics
- Various housing inventory sources

#### 2. dbt Staging Layer
Located in `dbt_project/models/staging/`:

**Dependencies:** Raw ingestion tables from Dagster
**Automation:** Eager (runs when upstream data changes)
**Purpose:** Standardize schemas, clean data, join with mapping tables

Example flow:
```
fred_raw (Dagster)
  → stg_fred_series.sql
  → Joins with fred_series_mapping
  → Adds series_name, category metadata
```

#### 3. dbt Mart Layers

**Government Models** (`models/government/`):
- `fred_series_grain` → Time-series grain normalization
- `fred_monthly_diff` → Month-over-month changes
- `fred_quarterly_roc` → Quarterly rate of change
- `fred_series_latest_aggregates` → Most recent values by series
- `housing_*` → Housing-specific marts

**Markets Models** (`models/markets/`):
- `major_indicies_summary` → S&P 500, DJIA, etc.
- `us_sector_summary` → Sector performance (Tech, Finance, etc.)
- `global_markets_summary` → International markets
- `currency_summary` → FX pairs
- `*_analysis_return` → Return calculations (daily, monthly, quarterly)

#### 4. dbt Analysis Layer
Located in `models/analysis/`:

**base_historical_analysis:**
- Combines economic indicators with market data
- Creates time-aligned dataset for correlation analysis
- Grain: One row per symbol, month, economic series

**leading_econ_return_indicator:**
- Correlates economic changes with future returns (Q1, Q2, Q3 forward)
- Calculates quintile performance
- Identifies predictive indicators
- **This is the primary input for DSPy agents**

**market_economic_analysis:**
- Additional joined analysis of markets + economic data

#### 5. AI Analysis Assets (DSPy)
Located in `macro_agents/src/macro_agents/defs/agents/`:

**sector_inflation_analysis** (analysis_agent.py):
- Depends on: `leading_econ_return_indicator` dbt model
- Analyzes correlations by sector
- Outputs to: `economic_analysis_results` table
- Uses: EconomicAnalyzer resource with DSPy

**Economic Cycle Detection** (economic_cycle_analyzer.py):
- Analyzes current economic phase (expansion, peak, contraction, trough)
- Multiple implementations: basic and enhanced versions

**Asset Allocation** (asset_allocation_analyzer.py):
- Generates portfolio recommendations based on cycle

**Backtesting** (backtesting.py):
- Tests strategies against historical data
- Depends on: transformed market + economic data

### Key Dependency Chains

**Complete Flow Example:**
```
FRED API
  → fred_raw (Dagster asset, partitioned)
  → stg_fred_series (dbt staging)
  → fred_monthly_diff (dbt government)
  → base_historical_analysis (dbt analysis)
  → leading_econ_return_indicator (dbt analysis)
  → sector_inflation_analysis (DSPy agent)
  → economic_analysis_results (output table)
```

**Market Analysis Flow:**
```
Market Stack API
  → market_stack_raw (Dagster)
  → stg_major_indices (dbt staging)
  → major_indicies_summary (dbt markets)
  → major_indicies_analysis_return (dbt markets)
  → base_historical_analysis (joins with economic)
  → AI analysis agents
```

### Automation Strategy

- **Ingestion Assets**: Scheduled (weekly on Mondays)
- **dbt Models**: Eager automation (run when upstream changes)
- **Analysis Agents**: On-demand or scheduled via Dagster jobs
- **Partitioning**: Used for FRED series to enable incremental updates

## Development Workflow

1. **Data Ingestion**: Dagster assets pull data from APIs and store in DuckDB
2. **Data Transformation**: dbt models transform raw data into analysis-ready tables
3. **AI Analysis**: DSPy agents analyze transformed data and generate insights
4. **Backtesting**: Historical performance testing of strategies
5. **Optimization**: Continuous improvement of AI models and prompts

## Testing

Test suite located in `macro_agents/tests/`:
- Unit tests for analysis agents
- Integration tests for end-to-end workflows
- Tests for Dagster asset descriptions
- Tests for dbt model descriptions
- Resource and schedule tests

Run tests using the makefile or pytest directly.

## Key Features

- Automated data collection from multiple economic and market sources
- SQL-based data transformations using dbt
- AI-powered economic cycle detection and analysis
- Asset allocation recommendations based on economic conditions
- Strategy backtesting with historical data
- Model evaluation and continuous improvement
- Scheduled and event-driven data pipelines
- Cloud deployment on Dagster Cloud

## Examples & Usage

### Example 1: Complete Data Pipeline Run

```python
# Start with ingesting FRED data for GDP series
# In Dagster UI or via CLI:
# dagster asset materialize -m macro_agents.definitions -s fred_raw --partition GDPC1

# The flow automatically triggers:
# 1. stg_fred_series (dbt staging)
# 2. fred_series_grain (dbt government)
# 3. base_historical_analysis (dbt analysis)
# 4. leading_econ_return_indicator (dbt analysis)
# 5. sector_inflation_analysis (DSPy agent) - if scheduled or manually triggered
```

### Example 2: Creating a New Data Ingestion Asset

```python
# File: macro_agents/src/macro_agents/defs/ingestion/new_source.py

import dagster as dg
from macro_agents.defs.resources.motherduck import MotherDuckResource

@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * 1"),  # Weekly
    description="Raw data from New Data Source API",
)
def new_source_raw(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource
) -> dg.MaterializeResult:
    # 1. Fetch data from API
    data = fetch_from_api()  # Returns polars DataFrame

    # 2. Upsert to DuckDB
    md.upsert_data(
        table_name="new_source_raw",
        data=data,
        unique_keys=["date", "identifier"]
    )

    # 3. Return metadata
    return dg.MaterializeResult(
        metadata={
            "num_records": len(data),
            "max_date": str(data["date"].max()),
        }
    )
```

### Example 3: Creating a New dbt Model

```sql
-- File: dbt_project/models/staging/stg_new_source.sql

{{ config(
    materialized='view',
    description='Standardized new source data'
) }}

SELECT
    date,
    identifier,
    value,
    UPPER(category) as category,
    CURRENT_TIMESTAMP as loaded_at
FROM {{ source('staging', 'new_source_raw') }}
WHERE value IS NOT NULL
```

Don't forget to add the source in `models/sources.yml`:
```yaml
sources:
  - name: staging
    tables:
      - name: new_source_raw
        description: "Raw data from new source"
```

### Example 4: Creating a DSPy Analysis Agent

```python
# File: macro_agents/src/macro_agents/defs/agents/custom_analyzer.py

import dspy
import dagster as dg
from pydantic import Field
from macro_agents.defs.resources.motherduck import MotherDuckResource

class CustomAnalysisSignature(dspy.Signature):
    """Analyze custom economic patterns."""

    input_data: str = dspy.InputField(desc="Economic data to analyze")
    analysis: str = dspy.OutputField(desc="Detailed analysis results")

class CustomAnalyzer(dg.ConfigurableResource):
    model_name: str = Field(default="gpt-4-turbo-preview")
    openai_api_key: str = Field(description="OpenAI API key")

    def setup_for_execution(self, context) -> None:
        lm = dspy.LM(model=self.model_name, api_key=self.openai_api_key)
        dspy.settings.configure(lm=lm)
        self._analyzer = dspy.ChainOfThought(CustomAnalysisSignature)

    def analyze(self, data: str) -> str:
        result = self._analyzer(input_data=data)
        return result.analysis

@dg.asset(
    kinds={"dspy", "duckdb"},
    group_name="analysis",
    description="Custom economic analysis"
)
def custom_analysis_asset(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    custom_analyzer: CustomAnalyzer
) -> dict:
    # Query data from DuckDB
    data = md.query_to_csv("SELECT * FROM my_analysis_table LIMIT 100")

    # Run analysis
    analysis_result = custom_analyzer.analyze(data)

    # Save results
    results = [{
        "analysis": analysis_result,
        "timestamp": context.run.start_time.isoformat()
    }]
    md.write_results_to_table(results, "custom_analysis_results", "append")

    return {"status": "complete"}
```

Register the resource in `definitions.py`:
```python
resources={
    "custom_analyzer": CustomAnalyzer(
        model_name=dg.EnvVar("MODEL_NAME"),
        openai_api_key=dg.EnvVar("OPENAI_API_KEY"),
    ),
}
```

### Example 5: Querying Results

```python
# Using DuckDB CLI
duckdb motherduck_database.db

# View analysis results
SELECT
    category,
    analysis,
    model_name,
    analysis_date
FROM economic_analysis_results
WHERE analysis_date >= '2024-01-01'
ORDER BY analysis_date DESC;

# Check latest economic indicators
SELECT
    series_name,
    category,
    latest_value,
    as_of_date
FROM fred_series_latest_aggregates
WHERE category = 'Inflation';
```

### Example 6: Running Specific dbt Models

```bash
# Run only staging models
dbt run --select staging.*

# Run a specific model and its downstream dependencies
dbt run --select fred_series_grain+

# Run only models that have changed
dbt run --select state:modified+

# Test a specific model
dbt test --select leading_econ_return_indicator
```

## Common Tasks & How-To Guides

### Adding a New FRED Series

1. **Add series code to partition definition**

   Edit `macro_agents/src/macro_agents/defs/ingestion/fred.py`:
   ```python
   fred_series_partition = dg.StaticPartitionsDefinition([
       "BAMLH0A0HYM2",
       "YOUR_NEW_SERIES",  # Add here
       # ... other series
   ])
   ```

2. **Add series metadata to mapping**

   Add to the FRED series mapping (seed file or constant):
   ```python
   {
       "code": "YOUR_NEW_SERIES",
       "series_name": "Human Readable Name",
       "category": "Employment"  # or Inflation, GDP, etc.
   }
   ```

3. **Materialize the new partition**
   ```bash
   dagster asset materialize -s fred_raw --partition YOUR_NEW_SERIES
   ```

### Modifying a dbt Model

1. **Read the current model** to understand structure
   ```bash
   cat dbt_project/models/government/fred_monthly_diff.sql
   ```

2. **Make your changes** to the SQL file

3. **Test compilation**
   ```bash
   cd dbt_project
   dbt compile --select fred_monthly_diff
   ```

4. **Run the model**
   ```bash
   dbt run --select fred_monthly_diff
   ```

5. **Test the model** (if tests exist)
   ```bash
   dbt test --select fred_monthly_diff
   ```

### Creating a New DSPy Analysis Agent

1. **Define the signature** (prompt structure)
   ```python
   class MyAnalysisSignature(dspy.Signature):
       """Clear description of what this analysis does."""
       input_field: str = dspy.InputField(desc="What goes in")
       output_field: str = dspy.OutputField(desc="What comes out")
   ```

2. **Create the resource class**
   ```python
   class MyAnalyzer(dg.ConfigurableResource):
       model_name: str
       openai_api_key: str

       def setup_for_execution(self, context):
           # Initialize DSPy
   ```

3. **Create the asset** that uses the analyzer
   ```python
   @dg.asset(group_name="analysis", kinds={"dspy", "duckdb"})
   def my_analysis(context, md, my_analyzer):
       # Implementation
   ```

4. **Register in definitions.py**
   ```python
   resources={"my_analyzer": MyAnalyzer(...)}
   ```

5. **Test the asset**
   ```bash
   dagster asset materialize -s my_analysis
   ```

### Running the Project Locally

**Full local development setup:**
```bash
# 1. Start Dagster UI
cd macro_agents
dagster dev

# 2. In another terminal, watch dbt compilation
cd dbt_project
dbt compile --watch

# 3. Materialize an asset through UI or CLI
# Navigate to http://localhost:3000
```

**Quick iteration on a single asset:**
```bash
# Materialize just one asset
cd macro_agents
dagster asset materialize -m macro_agents.definitions -s fred_raw --partition GDPC1
```

### Debugging Failed Assets

1. **Check logs in Dagster UI** at http://localhost:3000/runs

2. **Check DuckDB for data issues**
   ```bash
   duckdb my_database.db
   SELECT * FROM fred_raw WHERE series_code = 'GDPC1' LIMIT 10;
   ```

3. **Test dbt model in isolation**
   ```bash
   cd dbt_project
   dbt run --select stg_fred_series --full-refresh
   ```

4. **Check resource connections**
   ```python
   # Test MotherDuck connection
   cd macro_agents
   python -c "from macro_agents.defs.resources.motherduck import motherduck_resource; print('Connection works')"
   ```

### Adding a Schedule

Edit `macro_agents/src/macro_agents/defs/schedules.py`:

```python
from dagster import ScheduleDefinition, AssetSelection

my_schedule = ScheduleDefinition(
    name="my_daily_refresh",
    cron_schedule="0 2 * * *",  # 2 AM daily
    target=AssetSelection.groups("ingestion"),
    default_status=DefaultScheduleStatus.RUNNING,
)

schedules = [my_schedule, ...]
```

### Running Tests

```bash
# All tests
cd macro_agents
pytest tests/ -v

# Specific test file
pytest tests/test_analysis_agents.py -v

# With coverage
pytest tests/ --cov=macro_agents --cov-report=html

# Use makefile
make test
```

### Linting and Formatting

```bash
# Python (ruff)
make ruff

# SQL (sqlfluff)
make lint      # Check only
make fix       # Auto-fix issues
```

### Pre-PR Checklist

Run the comprehensive pre-PR check:
```bash
make pre-pr
```

This runs:
- Dependency installation
- Ruff linting and formatting
- MyPy type checking
- Pytest with coverage
- Security scans (bandit, safety)
- Dagster definition validation
- dbt compilation and parsing
- SQL linting

## Notes for Development

- All models are materialized as tables in dbt by default
- The database profile is named "econ_database" in dbt
- dbt packages used: dbt_utils (1.1.1), dbt-duckdb (1.9.2)
- AI agents require OpenAI API credentials
- Data is stored in DuckDB locally or synced to MotherDuck cloud
- Asset groups help organize assets in Dagster UI: `ingestion`, `staging`, `government`, `markets`, `analysis`
- dbt models use eager automation by default (run when upstream changes)
- Partitioned assets (like FRED series) allow for efficient incremental updates
