# Economic Data Application - Full Stack Data Platform

## Overview

This project is a comprehensive, end-to-end data platform that ingests, transforms, analyzes, and visualizes economic and financial market data. The application combines modern data engineering workflows (Dagster + dbt), AI-powered analysis agents (DSPy), a RESTful API backend (FastAPI), and an interactive web interface (React) to provide insights into economic cycles, market trends, and asset allocation strategies.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          External Data Sources                       │
│        (FRED, BLS, Market Stack, Treasury, Realtor.com)             │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      DATA PIPELINE LAYER                             │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ Dagster Orchestration                                        │  │
│  │  - Ingestion Assets (scheduled weekly)                       │  │
│  │  - dbt Transformation Assets (eager automation)              │  │
│  │  - DSPy AI Analysis Agents (on-demand/scheduled)             │  │
│  │  - Google Docs Publisher (automated reporting)               │  │
│  │  - Schedules, Sensors, Jobs                                  │  │
│  └──────────────────────────┬───────────────────────────────────┘  │
│                             │                                        │
│                             ▼                                        │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ DuckDB/MotherDuck Database                                   │  │
│  │  - Raw ingestion tables                                      │  │
│  │  - dbt-transformed analytics tables                          │  │
│  │  - AI analysis results                                       │  │
│  │  - Publication tracking                                      │  │
│  └──────────────────────────┬───────────────────────────────────┘  │
└─────────────────────────────┼──────────────────────────────────────┘
                              │
             ┌────────────────┴────────────────┬──────────────────────┐
             │                                 │                      │
             ▼                                 ▼                      ▼
┌─────────────────────┐          ┌──────────────────────┐  ┌─────────────────┐
│ Google Cloud        │          │   API SERVER          │  │ Sling           │
│ - Google Docs       │          │   (FastAPI)           │  │ Replication     │
│ - GCS (Models)      │          │                       │  │ → BigQuery      │
│ - BigQuery          │          │ Routers:              │  │                 │
│                     │          │ - /economy            │  └─────────────────┘
│ Publishes:          │          │ - /markets            │
│ - Economic reports  │          │ - /recommendations    │
│ - Analysis by       │          │ - /visualizations     │
│   personality       │          │ - /settings           │
│   (bearish/neutral/ │          │                       │
│    bullish)         │          │ Database: SQLite      │
└─────────────────────┘          │  (user settings)      │
                                 └──────────┬────────────┘
                                            │
                                            │ HTTP/REST API
                                            │
                                            ▼
                                 ┌─────────────────────┐
                                 │   FRONTEND          │
                                 │   (React + Vite)    │
                                 │                     │
                                 │ Features:           │
                                 │ - Economic Overview │
                                 │ - Market Analysis   │
                                 │ - Recommendations   │
                                 │ - Data Viz (Nivo)   │
                                 │ - Settings/Config   │
                                 │                     │
                                 │ Tech:               │
                                 │ - React 19          │
                                 │ - TailwindCSS 4     │
                                 │ - React Router 7    │
                                 │ - TanStack Query    │
                                 │ - Nivo Charts       │
                                 └─────────────────────┘
```

## Tech Stack

### Core Data Platform
- **Dagster**: Orchestration framework for data pipelines, asset management, schedules, and sensors
- **dbt**: SQL-based transformation framework for data modeling and analytics
- **DSPy**: Framework for building and optimizing AI agents with LLMs
- **DuckDB/MotherDuck**: Embedded analytical database with cloud sync capabilities

### Backend (API Server)
- **FastAPI**: Modern, high-performance Python web framework for building APIs
- **SQLite**: Lightweight database for user settings and preferences
- **Uvicorn**: ASGI server for running FastAPI
- **Pydantic**: Data validation using Python type hints

### Frontend (Web Application)
- **React 19**: JavaScript library for building user interfaces
- **Vite**: Next-generation frontend build tool
- **TailwindCSS 4**: Utility-first CSS framework
- **React Router 7**: Declarative routing for React
- **TanStack Query**: Powerful data synchronization for React
- **Nivo**: Data visualization library (charts, graphs)
- **Axios**: HTTP client for API requests
- **TypeScript**: Typed superset of JavaScript

### Supporting Technologies
- **Python 3.10-3.13**: Primary programming language for backend
- **Polars**: High-performance dataframe library
- **Sling**: Data replication tool for syncing to BigQuery
- **Google Cloud Platform**:
  - **Google Cloud Storage (GCS)**: Storage for optimized DSPy models
  - **Google Docs API**: Automated report publishing
  - **BigQuery**: Cloud data warehouse for replication
- **OpenAI/Anthropic/Gemini APIs**: LLM providers for AI agents

### Development & CI/CD
- **GitHub Actions**: CI/CD workflows
- **uv**: Fast Python package manager
- **npm**: Node package manager for frontend
- **pytest**: Python testing framework
- **Vitest**: Unit testing framework for Vite
- **Ruff**: Fast Python linter and formatter
- **ESLint**: JavaScript/TypeScript linter
- **SQLFluff**: SQL linter

## Project Structure

```
economic-data-project-full/
├── macro_agents/                          # Dagster orchestration project
│   ├── src/macro_agents/
│   │   ├── definitions.py                 # Central Dagster definitions
│   │   └── defs/
│   │       ├── ingestion/                 # Data ingestion assets
│   │       │   ├── fred.py                # FRED economic data
│   │       │   ├── bls.py                 # Bureau of Labor Statistics
│   │       │   ├── market_stack.py        # Market data (stocks, commodities, FX)
│   │       │   └── treasury_yields.py     # Treasury yield curves
│   │       ├── transformation/            # Data transformation
│   │       │   ├── dbt.py                 # dbt integration
│   │       │   └── financial_condition_index.py
│   │       ├── agents/                    # AI analysis agents (DSPy)
│   │       │   ├── economy_state_analyzer.py
│   │       │   ├── asset_class_relationship_analyzer.py
│   │       │   ├── investment_recommendations.py
│   │       │   ├── google_docs_publisher.py    # NEW: Google Docs publishing
│   │       │   ├── backtest_economy_state_analyzer.py
│   │       │   ├── backtest_asset_class_relationship_analyzer.py
│   │       │   ├── backtest_investment_recommendations.py
│   │       │   ├── backtest_evaluator.py
│   │       │   ├── backtest_optimizer.py
│   │       │   ├── ai_models_fetcher.py
│   │       │   └── backtest_utils.py
│   │       ├── resources/                 # Dagster resources
│   │       │   ├── motherduck.py          # DuckDB/MotherDuck connection
│   │       │   ├── fred.py                # FRED API client
│   │       │   ├── market_stack.py        # Market Stack API client
│   │       │   ├── gcs.py                 # Google Cloud Storage
│   │       │   └── google_docs.py         # NEW: Google Docs API integration
│   │       ├── constants/                 # Configuration constants
│   │       │   ├── fred_series_lists.py
│   │       │   └── market_stack_constants.py
│   │       └── schedules.py               # Schedules, sensors, jobs
│   ├── tests/                             # Test suite
│   │   ├── test_analysis_agents.py
│   │   ├── test_dspy_modules.py
│   │   ├── test_integration.py
│   │   ├── test_schedules.py
│   │   ├── test_resources.py
│   │   └── test_google_docs_resource.py   # NEW: Google Docs tests
│   ├── pyproject.toml                     # Python dependencies
│   └── uv.lock                            # Locked dependencies
│
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
│       │   ├── stg_treasury_yields.sql
│       │   ├── stg_energy_commodities.sql
│       │   ├── stg_input_commodities.sql
│       │   └── stg_agriculture_commodities.sql
│       ├── government/                    # Government data models
│       │   ├── fred_series_grain.sql
│       │   ├── fred_monthly_diff.sql
│       │   ├── fred_quarterly_roc.sql
│       │   ├── fred_series_latest_aggregates.sql
│       │   ├── housing_inventory.sql
│       │   ├── housing_inventory_latest_aggregates.sql
│       │   ├── housing_mortgage_rates.sql
│       │   └── housing_inventory_and_population.sql
│       ├── markets/                       # Market data models
│       │   ├── major_indicies_summary.sql
│       │   ├── major_indicies_analysis_return.sql
│       │   ├── us_sector_summary.sql
│       │   ├── us_sector_analysis_return.sql
│       │   ├── global_markets_summary.sql
│       │   ├── currency_summary.sql
│       │   └── fixed_income_analysis_return.sql
│       ├── commodities/                   # Commodity data models
│       │   ├── energy_commodities_summary.sql
│       │   ├── energy_commodities_analysis_return.sql
│       │   ├── input_commodities_summary.sql
│       │   ├── agriculture_commodities_summary.sql
│       │   └── agriculture_commodities_analysis_return.sql
│       ├── backtesting/                   # Backtesting snapshot models
│       │   ├── fred_series_latest_aggregates_snapshot.sql
│       │   ├── us_sector_summary_snapshot.sql
│       │   └── leading_econ_return_indicator_snapshot.sql
│       └── analysis/                      # Analysis layer models
│           ├── base_historical_analysis.sql
│           ├── market_economic_analysis.sql
│           └── leading_econ_return_indicator.sql
│
├── api/                                   # FastAPI backend server
│   ├── main.py                            # FastAPI application entry point
│   ├── config.py                          # API configuration
│   ├── database/                          # Database connection
│   │   ├── connection.py
│   │   └── __init__.py
│   ├── models/                            # Pydantic models
│   │   └── __init__.py
│   ├── routers/                           # API route handlers
│   │   ├── economy.py                     # Economic data endpoints
│   │   ├── markets.py                     # Market data endpoints
│   │   ├── recommendations.py             # Investment recommendations endpoints
│   │   ├── visualizations.py              # Data visualization endpoints
│   │   ├── settings.py                    # User settings endpoints
│   │   └── __init__.py
│   ├── services/                          # Business logic services
│   │   ├── data_service.py                # Data access layer
│   │   └── __init__.py
│   └── tests/                             # API tests
│       ├── conftest.py                    # Pytest configuration
│       ├── test_data_service.py
│       ├── test_economy.py
│       ├── test_markets.py
│       ├── test_recommendations.py
│       ├── test_settings.py
│       └── test_visualizations.py
│
├── frontend/                              # React web application
│   ├── src/
│   │   ├── components/                    # React components
│   │   ├── pages/                         # Page components
│   │   ├── services/                      # API client services
│   │   ├── hooks/                         # Custom React hooks
│   │   ├── types/                         # TypeScript type definitions
│   │   ├── App.tsx                        # Main App component
│   │   └── main.tsx                       # Application entry point
│   ├── tests/                             # Frontend tests (Vitest)
│   ├── public/                            # Static assets
│   ├── package.json                       # Node dependencies
│   ├── vite.config.ts                     # Vite configuration
│   ├── tailwind.config.js                 # TailwindCSS configuration
│   ├── tsconfig.json                      # TypeScript configuration
│   └── eslint.config.js                   # ESLint configuration
│
├── .github/                               # GitHub Actions workflows
│   └── workflows/
│       ├── dagster-ci.yml                 # Dagster testing/linting
│       ├── dagster-deploy.yml             # Dagster Cloud deployment
│       ├── api-ci.yml                     # API testing/linting
│       ├── frontend-ci.yml                # Frontend testing/linting
│       ├── branch_deployments.yml         # Branch-based deployments
│       └── sync-to-public.yml             # Public repo synchronization
│
├── .env.example                           # Environment variables template
├── dagster_cloud.yaml                     # Dagster Cloud deployment config
├── makefile                               # Build and automation commands
└── Readme.md                              # Project documentation
```

## Data Sources

All data is sourced from publicly available APIs:

### Economic Data
- **Federal Reserve Economic Data (FRED)**: Comprehensive economic indicators including GDP, inflation, employment, housing, trade, and financial conditions
- **Bureau of Labor Statistics (BLS)**: Employment and labor market data
- **Census Bureau**: Population and demographic data

### Market Data
- **Market Stack API**: Stock market data for major indices, sectors, global markets, currencies, fixed income, and commodities (energy, industrial, agricultural)
- **Treasury Yields**: U.S. Treasury bond yield curve data
- **Realtor.com**: Housing market inventory and pricing data

## Key Components

### 1. Data Ingestion Layer (Dagster Assets)

Located in `macro_agents/src/macro_agents/defs/ingestion/`

Pulls data from various APIs on configured schedules and stores raw data in DuckDB/MotherDuck:

- **fred.py**: FRED economic data (70+ series codes, partitioned, weekly schedule)
- **bls.py**: Bureau of Labor Statistics employment data
- **market_stack.py**: Stock, commodity, currency, and fixed income market data
- **treasury_yields.py**: US Treasury yield curve data

**Key Features:**
- Scheduled weekly ingestion (Mondays @ midnight)
- Partitioned assets for efficient incremental updates
- Data quality validation
- Automatic retries and error handling

### 2. Data Transformation Layer (dbt Models)

Located in `dbt_project/models/`

SQL-based transformations organized in layers with eager automation:

**Staging Layer** (`staging/`):
- Standardizes and cleans raw data
- Joins with mapping tables
- Adds metadata and categories

**Government Layer** (`government/`):
- Aggregates economic indicators
- Calculates month-over-month and quarterly changes
- Latest value aggregates for economic series
- Housing market analysis with population context

**Markets Layer** (`markets/`):
- Market summaries and return calculations
- Major indices (S&P 500, DJIA, NASDAQ)
- US sector performance (Tech, Finance, Healthcare, etc.)
- Global markets and currencies
- Fixed income analysis

**Commodities Layer** (`commodities/`):
- Energy commodities (oil, gas, coal)
- Industrial/input commodities (metals, materials)
- Agricultural commodities (grains, livestock)

**Analysis Layer** (`analysis/`):
- `leading_econ_return_indicator`: Primary dataset for AI agents
- Correlates economic changes with future market returns
- Quintile performance analysis
- **This is the main input for DSPy agents**

### 3. AI Analysis Agents (DSPy)

Located in `macro_agents/src/macro_agents/defs/agents/`

**Production Analysis Assets:**

- **economy_state_analyzer.py**: Analyzes current economic state with multiple personalities (skeptical/neutral/bullish)
  - Depends on: FRED aggregates, leading economic indicators, financial conditions index, commodity data, housing data, treasury yields
  - Outputs to: `economy_state_analysis` table

- **asset_class_relationship_analyzer.py**: Analyzes relationships between asset classes
  - Depends on: Economic state analysis and market data
  - Outputs to: `asset_class_relationship_analysis` table

- **investment_recommendations.py**: Generates investment recommendations
  - Depends on: Economy state analysis and asset class relationships
  - Outputs to: `investment_recommendations` table

- **google_docs_publisher.py**: **NEW** - Publishes analysis reports to Google Docs
  - Depends on: All three production analysis assets
  - Automatically runs after monthly analysis jobs
  - Organizes reports by personality (Bearish/Neutral/Bullish) and date
  - Outputs to: Google Docs document + `google_docs_publications` tracking table

**Backtesting & Optimization Assets:**

- **backtest_economy_state_analyzer.py**: Historical backtests of economy state analysis
- **backtest_asset_class_relationship_analyzer.py**: Historical backtests of asset class analysis
- **backtest_investment_recommendations.py**: Historical backtests of recommendations
- **backtest_evaluator.py**: Evaluates backtest accuracy using DSPy metrics
- **backtest_optimizer.py**: Optimizes DSPy modules using MIPROv2 optimizer
- **ai_models_fetcher.py**: Fetches available models from OpenAI, Anthropic, and Gemini APIs

### 4. Resources (Dagster)

Located in `macro_agents/src/macro_agents/defs/resources/`

- **motherduck.py**: DuckDB/MotherDuck database connection and query execution
- **fred.py**: FRED API client resource
- **market_stack.py**: Market Stack API client resource
- **gcs.py**: Google Cloud Storage resource for storing optimized DSPy models
- **google_docs.py**: **NEW** - Google Docs API resource for automated report publishing
- **dbt_cli_resource**: dbt command-line integration (in `transformation/dbt.py`)

### 5. API Server (FastAPI)

Located in `api/`

RESTful API backend that serves economic data and analysis results to the frontend.

**Main Application** (`api/main.py`):
- FastAPI application with CORS middleware
- Health check endpoint
- Automatic API documentation (Swagger/ReDoc)

**Routers** (`api/routers/`):
- **/api/economy**: Economic indicators and FRED data endpoints
- **/api/markets**: Market data, indices, sectors, commodities endpoints
- **/api/recommendations**: Investment recommendations from AI analysis
- **/api/visualizations**: Data visualization configurations and datasets
- **/api/settings**: User settings and preferences

**Database** (`api/database/`):
- SQLite database for user settings
- Connection pooling and session management

**Services** (`api/services/`):
- **data_service.py**: Data access layer that queries MotherDuck/DuckDB for analysis results
- Business logic for data aggregation and formatting

**Key Features:**
- Fast, asynchronous request handling
- Automatic request/response validation with Pydantic
- CORS support for frontend integration
- Comprehensive error handling
- API documentation at `/docs` (Swagger UI)

**Running the API Server:**
```bash
cd api
uvicorn main:app --reload --port 8000
```

### 6. Frontend Application (React + Vite)

Located in `frontend/`

Modern, responsive web application for visualizing economic data and AI analysis results.

**Technology Stack:**
- **React 19**: Component-based UI
- **Vite**: Lightning-fast development server and build tool
- **TailwindCSS 4**: Utility-first styling
- **React Router 7**: Client-side routing
- **TanStack Query**: Server state management and caching
- **Nivo**: Beautiful, responsive data visualizations
- **Axios**: HTTP client for API requests
- **TypeScript**: Type-safe JavaScript

**Pages** (`frontend/src/pages/`):
- **Economic Overview**: Dashboard with key economic indicators
- **Market Analysis**: Market performance, sectors, commodities
- **Recommendations**: AI-generated investment recommendations
- **Visualizations**: Interactive charts and graphs
- **Settings**: User preferences and configuration

**Components** (`frontend/src/components/`):
- Reusable UI components
- Chart components (Line, Bar, Area charts using Nivo)
- Data tables and grids
- Navigation and layout components

**Services** (`frontend/src/services/`):
- API client services for each endpoint category
- Type-safe API request/response handling

**Key Features:**
- Responsive design (mobile, tablet, desktop)
- Real-time data updates with TanStack Query
- Interactive data visualizations
- Dark mode support (via TailwindCSS)
- Fast hot module replacement (HMR) with Vite

**Running the Frontend:**
```bash
cd frontend
npm install
npm run dev
```
Navigate to `http://localhost:5173`

### 7. Orchestration (Dagster)

Located in `macro_agents/src/macro_agents/defs/schedules.py`

**Schedules:**
- Weekly ingestion schedule (Mondays @ midnight)
- Monthly analysis jobs for each personality (skeptical, neutral, bullish)
  - Each job now includes Google Docs publisher for automated reporting

**Sensors:**
- Event-driven workflows
- Upstream dependency sensors

**Jobs:**
- `monthly_economic_analysis_job_skeptical`
- `monthly_economic_analysis_job_neutral`
- `monthly_economic_analysis_job_bullish`
- Backtesting jobs
- Optimization jobs

### 8. Google Cloud Integrations

**Google Cloud Storage (GCS)**:
- Stores optimized DSPy model artifacts
- Used by backtest optimizer for model versioning

**Google Docs API**:
- Automated publishing of economic analysis reports
- Organized by personality (Bearish/Neutral/Bullish) and date
- Document structure: H1 (Personality) → H2 (Date) → H3 (Analysis Types)
- Tracks publications in `google_docs_publications` table

**BigQuery** (Optional):
- Data replication target via Sling
- Cloud data warehouse for analytics
- Enables SQL queries on cloud-synced data

### 9. CI/CD Workflows (GitHub Actions)

Located in `.github/workflows/`

**Dagster Workflows:**
- `dagster-ci.yml`: Linting, formatting, testing for Dagster code
- `dagster-deploy.yml`: Automated deployment to Dagster Cloud

**API Workflows:**
- `api-ci.yml`: Testing and linting for FastAPI backend

**Frontend Workflows:**
- `frontend-ci.yml`: Testing, linting, and build validation for React frontend

**Other Workflows:**
- `branch_deployments.yml`: Branch-based deployment previews
- `sync-to-public.yml`: Synchronization to public repository

## Data Flow (End-to-End)

### Complete Flow Example:

```
1. External APIs (FRED, Market Stack, etc.)
   ↓
2. Dagster Ingestion Assets (weekly schedule)
   → Raw tables in DuckDB/MotherDuck
   ↓
3. dbt Staging Layer (eager automation)
   → Cleaned and standardized tables
   ↓
4. dbt Transformation Layers (Government, Markets, Commodities)
   → Aggregated metrics and calculations
   ↓
5. dbt Analysis Layer
   → leading_econ_return_indicator (primary AI input)
   ↓
6. DSPy AI Analysis Agents (scheduled monthly)
   → Economy state analysis (with personality: skeptical/neutral/bullish)
   → Asset class relationships
   → Investment recommendations
   → Outputs to analysis result tables
   ↓
7. Google Docs Publisher (automatic after analysis)
   → Publishes formatted reports to Google Docs
   → Organized by personality and date
   ↓
8. FastAPI Server
   → Queries DuckDB/MotherDuck for analysis results
   → Exposes REST API endpoints
   ↓
9. React Frontend
   → Fetches data from API
   → Displays interactive visualizations
   → User interacts with economic insights
```

### API to Frontend Flow:

```
Frontend (React) → HTTP Request → FastAPI Router
                                       ↓
                                  Data Service
                                       ↓
                              MotherDuck Query
                                       ↓
                              Analysis Results
                                       ↓
                              JSON Response
                                       ↓
Frontend (React) ← HTTP Response ← FastAPI Router
     ↓
TanStack Query (caching)
     ↓
React Components (render)
     ↓
Nivo Charts (visualization)
```

## Environment Variables

Create a `.env` file in the project root with the following variables:

### Core Application
```bash
ENVIRONMENT=dev  # dev or prod
```

### Database (DuckDB/MotherDuck)
```bash
MOTHERDUCK_TOKEN=your_motherduck_token_here
MOTHERDUCK_DATABASE=your_database_name
MOTHERDUCK_PROD_SCHEMA=public
LOCAL_DUCKDB_PATH=local.duckdb
```

### Data Source APIs
```bash
FRED_API_KEY=your_fred_api_key_here
MARKETSTACK_API_KEY=your_marketstack_api_key_here
CENSUS_API_KEY=your_census_api_key_here
```

### LLM / AI Providers
```bash
LLM_PROVIDER=openai  # openai, anthropic, or gemini
MODEL_NAME=gpt-4-turbo-preview

# OpenAI
OPENAI_API_KEY=your_openai_api_key_here

# Anthropic (optional)
ANTHROPIC_API_KEY=your_anthropic_api_key_here

# Google Gemini (optional)
GEMINI_API_KEY=your_gemini_api_key_here
```

### Google Cloud Platform
```bash
# Service account credentials (JSON file path or JSON string)
# Used for: GCS storage, Google Docs API, and BigQuery
# Required scopes for Google Docs:
#   - https://www.googleapis.com/auth/documents
#   - https://www.googleapis.com/auth/drive
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
GCS_BUCKET_NAME=your_gcs_bucket_name

### dbt
```bash
DBT_TARGET=dev  # dev, local, or prod
# DBT_PROJECT_DIR=/path/to/dbt_project  # Optional, auto-detected
```

### API Server
```bash
API_BASE_URL=http://localhost:8000
SQLITE_DB_PATH=api/database/users.db
```

### Frontend
Create `frontend/.env`:
```bash
VITE_API_BASE_URL=http://localhost:8000
```

## Quick Start Guide

### Prerequisites
- Python 3.10-3.13
- Node.js 18+ and npm
- uv (recommended) or pip for Python package management
- DuckDB and MotherDuck account (for cloud sync)
- API keys for data sources (FRED, Market Stack, etc.)
- OpenAI/Anthropic/Gemini API key for AI agents
- Google Cloud credentials (for Google Docs and GCS)

### Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd economic-data-project-full
```

2. **Install Python dependencies (Dagster/dbt)**
```bash
cd macro_agents
uv sync  # or pip install -e .[dev]
```

3. **Install dbt packages**
```bash
cd ../dbt_project
dbt deps
```

4. **Install API dependencies**
```bash
cd ../api
pip install -r requirements.txt
```

5. **Install Frontend dependencies**
```bash
cd ../frontend
npm install
```

6. **Set up environment variables**
Copy `.env.example` to `.env` and fill in your API keys and credentials.

7. **Validate setup**
```bash
# Test Dagster definitions
cd macro_agents
dg check defs

# Test dbt models
cd ../dbt_project
dbt compile
dbt parse

# Test API
cd ../api
pytest tests/ -v

# Test Frontend
cd ../frontend
npm run test
```

### Running the Full Stack Locally

**Terminal 1 - Dagster UI:**
```bash
cd macro_agents
dagster dev
```
Navigate to `http://localhost:3000`

**Terminal 2 - API Server:**
```bash
cd api
uvicorn main:app --reload --port 8000
```
API available at `http://localhost:8000`
API docs at `http://localhost:8000/docs`

**Terminal 3 - Frontend:**
```bash
cd frontend
npm run dev
```
Navigate to `http://localhost:5173`

**Terminal 4 - dbt (optional, for manual runs):**
```bash
cd dbt_project
dbt run          # Run all models
dbt run --select staging.*  # Run specific layer
```

### First Run Workflow

1. **Materialize ingestion assets** in Dagster UI
   - Start with FRED data or Market Stack data
   - Assets will populate raw tables in DuckDB

2. **Run dbt transformations** (automatic with eager automation)
   - Or manually: `dbt run`
   - Transforms raw data through staging → marts → analysis layers

3. **Run analysis agents** in Dagster UI
   - Execute DSPy agents on transformed data
   - Monthly jobs automatically include Google Docs publishing

4. **View results**
   - Check Dagster UI for asset materialization status
   - Query DuckDB/MotherDuck for analysis outputs
   - View API endpoints at `http://localhost:8000/docs`
   - Explore frontend at `http://localhost:5173`

## MCP Server Setup (Claude Code Integration)

This project includes MCP (Model Context Protocol) server configuration for enhanced Claude Code capabilities.

### MotherDuck MCP Server

The MotherDuck MCP server allows Claude Code to directly query the DuckDB/MotherDuck database for troubleshooting Dagster pipelines and development assistance.

**Configuration File**: `.mcp.json` (project root)

```json
{
  "mcpServers": {
    "motherduck": {
      "type": "http",
      "url": "https://mcp.motherduck.com/mcp",
      "headers": {
        "Authorization": "Bearer ${MOTHERDUCK_TOKEN}"
      }
    }
  }
}
```

**Setup Steps:**

1. **Get MotherDuck Token**
   - Log in to [MotherDuck Console](https://motherduck.com/)
   - Generate an API token
   - Copy the token for next step

2. **Set Environment Variable**

Add to your `.env` or `.env.local` file (both are gitignored):
```bash
MOTHERDUCK_TOKEN=your_actual_token_here
ANTHROPIC_API_KEY=your_anthropic_key_here
```

**Note**: You can use either:
- `.env` - Standard environment file (gitignored)
- `.env.local` - Local overrides file (gitignored)
- Both files work - choose whichever you prefer

Alternatively, add to your shell profile for persistent access:
```bash
# Add to ~/.bashrc, ~/.zshrc, or equivalent
export MOTHERDUCK_TOKEN="your_actual_token"
export ANTHROPIC_API_KEY="your_anthropic_key"
```

3. **Verify Setup**

```bash
# Start Claude Code
claude

# Check MCP server status
> /mcp

# Test database query
> "Query MotherDuck to show all tables in the database"
> "What's the schema of the economy_state_analysis table?"
```

**Capabilities:**

- **SQL Query Execution**: Run queries against DuckDB/MotherDuck
  - `"Query the asset_class_relationship_analysis table for recent entries"`
  - `"Show me failed Dagster runs from the last 24 hours"`

- **Schema Inspection**: Explore database structure
  - `"What tables are in our MotherDuck database?"`
  - `"Show me the columns in the investment_recommendations table"`

- **Pipeline Troubleshooting**: Analyze Dagster pipeline data
  - `"Find assets that haven't materialized recently"`
  - `"Check the asset lineage for economy_state_analyzer"`

- **Development Support**: Query data during development
  - `"How many records are in the FRED economic indicators table?"`
  - `"Compare data volumes between staging and production"`

**Security Notes:**
- Both `.env` and `.env.local` are gitignored (can contain sensitive tokens)
- `.mcp.json` is committed (configuration only, no secrets)
- Each team member sets their own `MOTHERDUCK_TOKEN` in either `.env` or `.env.local`
- Rotate tokens periodically for security

## Development Workflow

### Git Workflow (Start Here)

**IMPORTANT: Always create a new branch before beginning any work.**

Before starting any development task (features, bug fixes, documentation, etc.):

```bash
# Create and switch to a new branch with a descriptive name
git checkout -b feature/your-feature-name
# or
git checkout -b fix/your-bug-fix-name
# or
git checkout -b docs/your-documentation-update
```

**Branch Naming Conventions:**
- `feature/` - New features or enhancements
- `fix/` - Bug fixes
- `docs/` - Documentation updates
- `refactor/` - Code refactoring
- `test/` - Adding or updating tests
- `chore/` - Maintenance tasks (dependencies, config, etc.)

**Example:**
```bash
# Before adding a new Dagster asset
git checkout -b feature/add-commodity-ingestion-asset

# Before fixing a bug
git checkout -b fix/treasury-yields-parsing-error

# Before updating documentation
git checkout -b docs/update-api-endpoints
```

After completing your work, commit changes and create a pull request following the standard git workflow.

### For Data Pipeline Development (Dagster + dbt)

1. **Data Ingestion**: Create new ingestion assets in `macro_agents/src/macro_agents/defs/ingestion/`
2. **Data Transformation**: Add new dbt models in `dbt_project/models/`
3. **AI Analysis**: Create new DSPy agents in `macro_agents/src/macro_agents/defs/agents/`
4. **Testing**: Add tests in `macro_agents/tests/` and `dbt_project/tests/`
5. **Validation**: Run `dg check defs` and `dbt compile`

### For API Development

1. **Add new endpoints**: Create routers in `api/routers/`
2. **Business logic**: Add services in `api/services/`
3. **Data models**: Define Pydantic models in `api/models/`
4. **Testing**: Add tests in `api/tests/`
5. **Validation**: Run `pytest api/tests/ -v`

### For Frontend Development

1. **Add new pages**: Create page components in `frontend/src/pages/`
2. **Add new components**: Create reusable components in `frontend/src/components/`
3. **API integration**: Add service methods in `frontend/src/services/`
4. **Styling**: Use TailwindCSS utility classes
5. **Testing**: Add tests in `frontend/tests/`
6. **Validation**: Run `npm run lint` and `npm run test`

## Testing

**Dagster/dbt Testing:**
```bash
# Use makefile for Python
make ruff  # Format and lint
make test  # Run all tests

# Use dbt for SQL
make lint  # SQL linting
make fix   # SQL auto-fix

# Or direct commands
cd macro_agents
pytest tests/ -v

cd dbt_project
dbt test
```

**API Testing:**
```bash
cd api
pytest tests/ -v

# With coverage
pytest tests/ --cov=api --cov-report=html
```

**Frontend Testing:**
```bash
cd frontend
npm run test
npm run test:ui  # Interactive test UI
```

**Integration Testing:**
```bash
# Run all tests across the stack
make pre-pr  # Comprehensive pre-PR check
```

## Deployment

### Dagster Cloud Deployment

The project is configured for deployment on Dagster Cloud using `dagster_cloud.yaml`:

```yaml
locations:
  - location_name: macro_agents
    code_source:
      module_name: macro_agents.definitions
    working_directory: ./macro_agents
```

**GitHub Actions automatically deploys to Dagster Cloud on push to main.**

## Code Style Guidelines

### Python (Dagster, dbt, API)

**CRITICAL: Do not use comments in code. Use descriptive names instead.**

**Good:**
```python
def calculate_monthly_return_percentage(
    previous_value: float,
    current_value: float
) -> float:
    return (current_value - previous_value) / previous_value * 100
```

**Bad:**
```python
# Calculate the monthly return percentage
def calc(x, y):
    return (y - x) / x * 100
```

### TypeScript/JavaScript (Frontend)

- Use descriptive variable and function names
- Leverage TypeScript for type safety
- Use functional components with hooks
- Follow React best practices
- Use TailwindCSS utility classes (no custom CSS unless necessary)

### SQL (dbt Models)

- Do not use inline SQL comments
- Use descriptive table, column, and CTE names
- Add descriptions in `schema.yml` for documentation

### Formatting and Linting

**Always use makefile commands:**
```bash
make ruff      # Python formatting/linting
make lint      # SQL linting
make fix       # SQL auto-fix
make test      # All tests
make typecheck-backend # Typcheck Backend
```

## Common Tasks

### Adding a New API Endpoint

1. Create a new router or add to existing router in `api/routers/`
2. Define request/response models with Pydantic
3. Implement business logic in `api/services/`
4. Add tests in `api/tests/`
5. Update API documentation (automatic with FastAPI)

### Adding a New Frontend Page

1. Create page component in `frontend/src/pages/`
2. Add route in `frontend/src/App.tsx`
3. Create API service method in `frontend/src/services/`
4. Use TanStack Query for data fetching
5. Add Nivo charts for visualizations
6. Style with TailwindCSS

### Adding a New Data Source

1. Create ingestion asset in `macro_agents/src/macro_agents/defs/ingestion/`
2. Create dbt staging model in `dbt_project/models/staging/`
3. Add transformation models as needed
4. Create API endpoint in `api/routers/`
5. Create frontend components to display the data

### Running Specific Components

```bash
# Dagster only
cd macro_agents && dagster dev

# API only
cd api && uvicorn main:app --reload

# Frontend only
cd frontend && npm run dev

# dbt only
cd dbt_project && dbt run

# Full stack
# Run all three commands in separate terminals
```

## Key Features

- **Automated Data Pipeline**: Scheduled ingestion, transformation, and analysis
- **AI-Powered Insights**: DSPy agents for economic cycle detection and investment recommendations
- **Multiple Analytical Perspectives**: Skeptical (bearish), neutral, and bullish analysis modes
- **Automated Reporting**: Google Docs publishing with organized sections by personality and date
- **RESTful API**: FastAPI backend for flexible data access
- **Modern Web Interface**: React frontend with interactive visualizations
- **Backtesting Framework**: Historical performance testing and model optimization
- **Cloud Integration**: Google Cloud Storage, Google Docs API, BigQuery replication
- **CI/CD Pipeline**: Automated testing, linting, and deployment via GitHub Actions
- **Scalable Architecture**: Dagster orchestration with eager automation

## Notes for Development

- **Always use makefile commands** for consistency: `make ruff`, `make lint`, `make fix`, `make test`
- **Create at least one test** for every new component (asset, model, endpoint, component)
- **Run `make pre-pr`** before creating pull requests
- **Use descriptive names** instead of comments
- **Follow existing patterns** when adding new features
- **Update this documentation** when adding major new components

## Links and Resources

- **Dagster Documentation**: https://docs.dagster.io
- **dbt Documentation**: https://docs.getdbt.com
- **DSPy Documentation**: https://dspy-docs.vercel.app
- **FastAPI Documentation**: https://fastapi.tiangolo.com
- **React Documentation**: https://react.dev
- **Vite Documentation**: https://vitejs.dev
- **TailwindCSS Documentation**: https://tailwindcss.com
- **Nivo Documentation**: https://nivo.rocks
