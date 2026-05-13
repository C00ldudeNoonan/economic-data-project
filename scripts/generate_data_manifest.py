# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pyyaml>=6.0",
# ]
# ///
"""
Generate a data platform manifest for Claude AI consumption via MotherDuck MCP.

Produces a structured YAML document that gives Claude knowledge of:
- Tier 1 (PREFERRED): Processed, quality-checked, agent-ready views
- Tier 2 (AVAILABLE): Analytical models with quality checks
- Tier 3 (RAW): Unprocessed source data (use only when processed data is insufficient)

Introspects dbt schema.yml files to compute per-table confidence scores
based on test coverage, test types, and upstream lineage.

Usage:
    uv run scripts/generate_data_manifest.py
    uv run scripts/generate_data_manifest.py --output manifest.yaml
    uv run scripts/generate_data_manifest.py --format text
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import yaml

# ---------------------------------------------------------------------------
# dbt project root (relative to this script)
# ---------------------------------------------------------------------------
DBT_ROOT = Path(__file__).resolve().parent.parent / "dbt_project"

SCHEMA_FILES = [
    "models/staging/schema.yml",
    "models/staging/telemetry/schema.yml",
    "models/analysis/schema.yml",
    "models/analysis/dispersion/schema.yml",
    "models/signals/signals_schema.yml",
    "models/markets/schema.yml",
    "models/commodities/schema.yml",
    "models/government/schema.yml",
    "models/data_quality/schema.yml",
    "models/backtesting/schema.yml",
    "models/agents_preprocess/schema.yml",
    "models/analytics/telemetry/schema.yml",
]

# Test types ranked by how much confidence they add (weight)
TEST_WEIGHTS: dict[str, float] = {
    "not_null": 1.0,
    "unique": 2.0,
    "unique_combination": 2.0,
    "accepted_values": 1.5,
    "ohlc_consistency": 2.5,
    "usd_currency_only": 1.0,
    "positive_price": 1.0,
    "value_in_range": 1.5,
    "dbt_utils.expression_is_true": 1.5,
}
DEFAULT_TEST_WEIGHT = 1.0

# Layer multipliers — upstream layers are less trusted
LAYER_MULTIPLIER: dict[str, float] = {
    "signals": 1.0,
    "analysis": 1.0,
    "analysis/dispersion": 1.0,
    "markets": 1.0,
    "commodities": 1.0,
    "government": 0.9,
    "data_quality": 1.0,
    "backtesting": 0.95,
    "agents_preprocess": 0.85,  # pass-throughs — score inherited from upstream
    "analytics/telemetry": 0.5,  # internal telemetry, not user-facing
    "staging": 0.7,
    "staging/telemetry": 0.4,
}


# ---------------------------------------------------------------------------
# Test coverage scanner
# ---------------------------------------------------------------------------
class ModelCoverage:
    """Test coverage data for a single dbt model."""

    def __init__(self, name: str, layer: str, description: str = "") -> None:
        self.name = name
        self.layer = layer
        self.description = description
        self.column_tests: list[str] = []  # test type names
        self.model_tests: list[str] = []
        self.tested_columns: set[str] = set()
        self.total_columns_documented: int = 0

    @property
    def total_test_count(self) -> int:
        return len(self.column_tests) + len(self.model_tests)

    @property
    def test_types(self) -> set[str]:
        return set(self.column_tests) | set(self.model_tests)

    @property
    def weighted_score(self) -> float:
        """Compute a weighted test score."""
        score = 0.0
        for t in self.column_tests + self.model_tests:
            score += TEST_WEIGHTS.get(t, DEFAULT_TEST_WEIGHT)
        return score

    @property
    def confidence(self) -> str:
        """Confidence rating: high / medium / low / untested."""
        raw = self.weighted_score * LAYER_MULTIPLIER.get(self.layer, 0.8)
        if raw >= 5.0:
            return "high"
        if raw >= 2.0:
            return "medium"
        if raw > 0:
            return "low"
        return "untested"

    def summary(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "confidence": self.confidence,
            "total_tests": self.total_test_count,
        }
        if self.test_types:
            result["test_types"] = sorted(self.test_types)
        if self.tested_columns:
            result["tested_columns"] = sorted(self.tested_columns)
        return result


def _extract_test_names(tests: list[Any]) -> list[str]:
    """Extract test type names from a dbt test list."""
    names: list[str] = []
    for t in tests:
        if isinstance(t, str):
            names.append(t)
        elif isinstance(t, dict):
            # e.g. {"accepted_values": {"arguments": ...}}
            # or {"not_null": {"config": ...}}
            for key in t:
                names.append(key)
    return names


def _layer_from_path(schema_path: str) -> str:
    """Derive layer name from schema file path."""
    # e.g. "models/staging/schema.yml" -> "staging"
    # e.g. "models/analysis/dispersion/schema.yml" -> "analysis/dispersion"
    parts = schema_path.replace("\\", "/").split("/")
    # Remove "models/" prefix and "schema.yml" / "*_schema.yml" suffix
    layer_parts = parts[1:-1]  # skip "models" and filename
    return "/".join(layer_parts) if layer_parts else "unknown"


def scan_test_coverage() -> dict[str, ModelCoverage]:
    """Parse all schema.yml files and return coverage data per model."""
    coverage: dict[str, ModelCoverage] = {}

    for schema_rel in SCHEMA_FILES:
        schema_path = DBT_ROOT / schema_rel
        if not schema_path.exists():
            continue

        layer = _layer_from_path(schema_rel)
        data = yaml.safe_load(schema_path.read_text(encoding="utf-8"))
        if not data or "models" not in data:
            continue

        for model in data["models"]:
            name = model.get("name", "")
            if not name:
                continue

            mc = ModelCoverage(
                name=name,
                layer=layer,
                description=model.get("description", ""),
            )

            # Model-level tests
            if "tests" in model:
                mc.model_tests = _extract_test_names(model["tests"])

            # Column-level tests
            for col in model.get("columns", []):
                mc.total_columns_documented += 1
                col_name = col.get("name", "")
                col_tests = col.get("tests", [])
                if col_tests:
                    mc.tested_columns.add(col_name)
                    mc.column_tests.extend(_extract_test_names(col_tests))

            coverage[name] = mc

    # Also find SQL models with NO schema.yml entry
    for sql_file in (DBT_ROOT / "models").rglob("*.sql"):
        model_name = sql_file.stem
        if model_name not in coverage:
            rel = sql_file.relative_to(DBT_ROOT / "models")
            parts = list(rel.parts[:-1])
            layer = "/".join(parts) if parts else "unknown"
            coverage[model_name] = ModelCoverage(
                name=model_name,
                layer=layer,
                description="",
            )

    return coverage


def _enrich_table(table_entry: dict[str, Any], coverage: dict[str, ModelCoverage]) -> None:
    """Add coverage data to a table entry dict in-place."""
    name = table_entry.get("table", "")
    if name in coverage:
        mc = coverage[name]
        cov = mc.summary()
        table_entry["confidence"] = cov["confidence"]
        table_entry["test_coverage"] = {
            "total_tests": cov["total_tests"],
        }
        if "test_types" in cov:
            table_entry["test_coverage"]["test_types"] = cov["test_types"]
        if "tested_columns" in cov:
            table_entry["test_coverage"]["tested_columns"] = cov["tested_columns"]


def _enrich_manifest(manifest: dict[str, Any], coverage: dict[str, ModelCoverage]) -> None:
    """Walk the manifest tree and enrich all table entries with coverage."""
    if isinstance(manifest, dict):
        if "table" in manifest:
            _enrich_table(manifest, coverage)
        for v in manifest.values():
            _enrich_manifest(v, coverage)
    elif isinstance(manifest, list):
        for item in manifest:
            _enrich_manifest(item, coverage)


# ---------------------------------------------------------------------------
# Coverage summary for the manifest header
# ---------------------------------------------------------------------------
def _build_coverage_summary(coverage: dict[str, ModelCoverage]) -> dict[str, Any]:
    """Build a summary of test coverage across the platform."""
    by_confidence: dict[str, list[str]] = {"high": [], "medium": [], "low": [], "untested": []}
    by_layer: dict[str, dict[str, int]] = {}

    for mc in coverage.values():
        by_confidence[mc.confidence].append(mc.name)
        layer = mc.layer
        if layer not in by_layer:
            by_layer[layer] = {"total": 0, "tested": 0, "tests": 0}
        by_layer[layer]["total"] += 1
        if mc.total_test_count > 0:
            by_layer[layer]["tested"] += 1
        by_layer[layer]["tests"] += mc.total_test_count

    layer_summary = {}
    for layer in sorted(by_layer):
        info = by_layer[layer]
        pct = round(info["tested"] / info["total"] * 100) if info["total"] else 0
        layer_summary[layer] = f"{info['tested']}/{info['total']} models tested ({pct}%), {info['tests']} total checks"

    return {
        "total_models": len(coverage),
        "by_confidence": {
            "high": len(by_confidence["high"]),
            "medium": len(by_confidence["medium"]),
            "low": len(by_confidence["low"]),
            "untested": len(by_confidence["untested"]),
        },
        "untested_models": sorted(by_confidence["untested"]),
        "by_layer": layer_summary,
    }


# ---------------------------------------------------------------------------
# Manifest builder
# ---------------------------------------------------------------------------
def build_manifest(coverage: dict[str, ModelCoverage]) -> dict[str, Any]:
    """Build the complete data platform manifest."""
    manifest = {
        "data_platform_manifest": {
            "version": "1.1",
            "database": "MotherDuck (DuckDB)",
            "schema": "main",
            "instructions": {
                "overview": (
                    "This is a multi-layered economic and financial data platform. "
                    "Data flows from raw sources through staging, quality checks, "
                    "domain models, analysis, and finally agent-ready views. "
                    "Always prefer higher-tier tables over lower-tier ones."
                ),
                "query_priority": [
                    "TIER 1 (PREFERRED): Agent-preprocessed views and signals — cleaned, unified, quality-checked",
                    "TIER 2 (AVAILABLE): Analysis and domain models — processed with tests but may need joins",
                    "TIER 3 (RAW): Staging/source tables — only when processed data lacks needed granularity",
                ],
                "confidence_ratings": {
                    "description": (
                        "Each table has a confidence rating based on dbt test coverage. "
                        "Prefer tables with higher confidence when multiple options exist."
                    ),
                    "high": "5+ weighted test points — well-validated with multiple test types (uniqueness, accepted values, OHLC consistency, etc.)",
                    "medium": "2-4 weighted test points — basic validation (typically not_null checks)",
                    "low": "1 weighted test point — minimal validation",
                    "untested": "No dbt tests defined — use with caution, cross-reference with tested tables",
                },
                "time_periods": {
                    "performance_summaries": ["12_weeks", "6_months", "1_year", "5_years"],
                    "rolling_windows": ["1_month", "3_months", "6_months", "9_months", "1_year"],
                    "signal_frequencies": {
                        "daily": "market technicals, breadth, volatility, cross-asset",
                        "weekly": "net liquidity, treasury yields",
                        "monthly": "economic indicators, regime, sentiment, labor, housing, inflation",
                        "quarterly": "fiscal signals, GDP",
                    },
                },
                "tips": [
                    "Filter agent_market_performance by market_category ('sector' or 'major_index') and time_period",
                    "Filter agent_commodity_performance by commodity_category ('energy', 'agriculture', 'input') and time_period",
                    "Signal tables have _status columns with severity: 'high', 'medium', 'low', 'normal'",
                    "For point-in-time historical analysis, use _snapshot tables to avoid look-ahead bias",
                    "economic_regime_classification values: Expansion, Slowdown, Contraction, Recovery",
                    "Use data_quality_anomalies to check for known data issues before drawing conclusions",
                    "FRED series are identified by series_code (e.g., 'CPIAUCSL', 'UNRATE', 'GDP')",
                    "Sector ETFs: XLK (Tech), XLF (Financials), XLE (Energy), XLV (Health), XLY (Cons Disc), XLP (Cons Staples), XLI (Industrials), XLB (Materials), XLRE (Real Estate), XLC (Comms), XLU (Utilities)",
                ],
            },
            "test_coverage_summary": _build_coverage_summary(coverage),
            "tier_1_preferred": _build_tier_1(),
            "tier_2_available": _build_tier_2(),
            "tier_3_raw": _build_tier_3(),
        }
    }

    # Enrich all table entries with introspected coverage
    _enrich_manifest(manifest, coverage)

    return manifest


def _build_tier_1() -> dict[str, Any]:
    """Agent-preprocessed views and signal models — always prefer these."""
    return {
        "description": "Cleaned, unified, quality-checked tables optimized for AI consumption. Use these first.",
        "agent_views": {
            "market_data": [
                {
                    "table": "agent_market_performance",
                    "description": "Unified market performance across sectors and major indices",
                    "use_when": "Asking about stock market returns, sector performance, or index performance",
                    "key_columns": [
                        "symbol", "ticker", "name", "time_period", "market_category",
                        "total_return_pct", "avg_daily_return_pct", "volatility_pct",
                        "annualized_volatility_pct", "win_rate_pct",
                        "best_day_pct_change", "worst_day_pct_change",
                        "period_start_price", "period_end_price",
                    ],
                    "filter_by": {
                        "market_category": ["sector", "major_index"],
                        "time_period": ["12_weeks", "6_months", "1_year", "5_years"],
                    },
                },
                {
                    "table": "agent_commodity_performance",
                    "description": "Unified commodity performance across energy, agriculture, and industrial inputs",
                    "use_when": "Asking about commodity prices, returns, or volatility",
                    "key_columns": [
                        "commodity_name", "commodity", "commodity_unit", "time_period",
                        "commodity_category", "total_return_pct", "volatility_pct",
                        "annualized_volatility_pct", "win_rate_pct",
                        "best_day_pct_change", "worst_day_pct_change",
                        "period_start_price", "period_end_price",
                    ],
                    "filter_by": {
                        "commodity_category": ["energy", "agriculture", "input"],
                        "time_period": ["12_weeks", "6_months", "1_year", "5_years"],
                    },
                },
                {
                    "table": "agent_treasury_yield_curve_spreads",
                    "description": "Treasury yield curve with key spreads and inversion detection",
                    "use_when": "Asking about interest rates, yield curve, or recession signals from bonds",
                    "key_columns": [
                        "date", "yield_1m", "yield_3m", "yield_6m", "yield_1y",
                        "yield_2y", "yield_5y", "yield_10y", "yield_20y", "yield_30y",
                        "spread_10y_2y", "spread_10y_3m", "spread_2y_3m", "spread_30y_2y",
                        "curve_shape", "inversion_status",
                    ],
                    "filter_by": {
                        "curve_shape": ["Steep", "Normal", "Flat", "Inverted"],
                        "inversion_status": ["Inverted", "Normal"],
                    },
                },
            ],
            "economic_data": [
                {
                    "table": "agent_fred_series_latest_aggregates",
                    "description": "Latest snapshot of all FRED economic indicators with percentage changes",
                    "use_when": "Asking about current economic data values or recent changes in indicators",
                    "key_columns": [
                        "series_code", "series_name", "month", "current_value",
                        "pct_change_3m", "pct_change_6m", "pct_change_1y", "date_grain",
                    ],
                },
                {
                    "table": "agent_fred_monthly_diff",
                    "description": "FRED economic series with month-over-month differences",
                    "use_when": "Analyzing trends or direction of economic indicators over time",
                    "key_columns": [
                        "series_code", "series_name", "date", "value", "period_diff", "data_source",
                    ],
                },
                {
                    "table": "agent_financial_conditions_index",
                    "description": "Financial conditions composite index with sub-scores",
                    "use_when": "Assessing overall financial stress or tightness",
                    "key_columns": [
                        "date", "fci", "equity_score", "housing_score", "treasury_10yr_score",
                    ],
                },
                {
                    "table": "agent_housing_inventory_latest_aggregates",
                    "description": "Latest housing inventory metrics with percentage changes",
                    "use_when": "Asking about housing supply, inventory levels, or housing market conditions",
                    "key_columns": [
                        "series_code", "series_name", "month", "current_value",
                        "pct_change_3m", "pct_change_6m", "pct_change_1y", "date_grain",
                    ],
                },
                {
                    "table": "agent_housing_mortgage_rates",
                    "description": "Mortgage rates with monthly payment calculations at different down payments",
                    "use_when": "Asking about mortgage rates, housing affordability, or payment estimates",
                    "key_columns": [
                        "date", "mortgage_rate",
                        "median_price_no_down_payment", "median_price_20_pct_down_payment",
                        "monthly_payment_no_down_payment", "monthly_payment_20_pct_down_payment",
                    ],
                },
                {
                    "table": "agent_leading_econ_return_indicator",
                    "description": "Correlation analysis of economic indicator changes vs future stock returns",
                    "use_when": "Asking which economic indicators predict market returns or about economic-market relationships",
                    "key_columns": [
                        "symbol", "series_name", "analysis_type", "economic_category",
                    ],
                },
            ],
            "sentiment_data": [
                {
                    "table": "agent_reddit_sentiment_trends",
                    "description": "Daily sentiment trends by subreddit with rolling averages",
                    "use_when": "Asking about market sentiment, retail investor mood, or social media trends",
                    "key_columns": [
                        "date", "subreddit", "avg_score", "total_posts",
                        "weekly_avg_score", "score_momentum_pct",
                        "avg_vader_sentiment", "pct_positive", "pct_negative",
                        "sentiment_momentum", "sentiment_trend",
                    ],
                    "filter_by": {
                        "subreddit": ["investing", "stocks", "wallstreetbets", "economics", "economy"],
                        "sentiment_trend": ["increasing", "decreasing", "stable"],
                    },
                },
                {
                    "table": "agent_reddit_posts_daily",
                    "description": "Daily Reddit posts with engagement metrics",
                    "use_when": "Looking at specific Reddit discussions or top posts",
                    "key_columns": [
                        "title", "score", "num_comments", "subreddit", "author", "url", "partition_date",
                    ],
                },
            ],
            "historical_snapshots": [
                {
                    "table": "agent_market_performance_snapshot",
                    "description": "Monthly point-in-time snapshots of market performance",
                    "use_when": "Backtesting or analyzing what was known at a specific past date",
                    "key_columns": ["snapshot_date", "symbol", "market_category", "time_period", "total_return_pct"],
                },
                {
                    "table": "agent_commodity_performance_snapshot",
                    "description": "Monthly point-in-time snapshots of commodity performance",
                    "use_when": "Backtesting commodity strategies or historical commodity analysis",
                    "key_columns": ["snapshot_date", "commodity_name", "commodity_category", "time_period", "total_return_pct"],
                },
                {
                    "table": "agent_fred_series_latest_aggregates_snapshot",
                    "description": "Monthly point-in-time snapshots of FRED economic data",
                    "use_when": "Backtesting economic analysis or comparing what was known at different dates",
                    "key_columns": ["snapshot_date", "series_code", "series_name", "current_value", "pct_change_3m"],
                },
                {
                    "table": "agent_leading_econ_return_indicator_snapshot",
                    "description": "Monthly snapshots of economic-market correlation data",
                    "use_when": "Analyzing how economic-market relationships evolved over time",
                    "key_columns": ["snapshot_date", "symbol", "series_name"],
                },
            ],
        },
        "signal_models": {
            "description": (
                "Daily/weekly/monthly signal tables with severity-rated status columns. "
                "Each signal has a _status column rated: high (alert), medium (watch), low (note), normal. "
                "Use these to assess current market and economic conditions."
            ),
            "macroeconomic_signals": [
                {
                    "table": "inflation_signals",
                    "frequency": "monthly",
                    "description": "CPI momentum, Core PCE vs 2% target, breakeven inflation spreads",
                    "key_columns": [
                        "date", "cpi_3m_annualized", "cpi_12m_yoy", "cpi_momentum_spread",
                        "core_pce_yoy", "pce_deviation_from_target", "breakeven_5y_10y_spread",
                        "cpi_momentum_status", "core_pce_status", "breakeven_status",
                    ],
                },
                {
                    "table": "labor_signals",
                    "frequency": "monthly",
                    "description": "Job openings/unemployed ratio, Sahm Rule, initial claims, quits rate",
                    "key_columns": [
                        "date", "jo_unemployed_ratio", "sahm_approx",
                        "jo_ratio_status", "claims_trend_status", "sahm_approx_status", "quits_trend_status",
                    ],
                },
                {
                    "table": "housing_signals",
                    "frequency": "monthly",
                    "description": "Housing starts momentum, permits pipeline, mortgage stress, months of supply",
                    "key_columns": [
                        "date", "starts_yoy_pct", "permits_starts_ratio",
                        "avg_mortgage_rate", "months_of_supply",
                        "starts_momentum_status", "permits_pipeline_status",
                        "mortgage_stress_status", "supply_status",
                    ],
                },
                {
                    "table": "financial_conditions_signals",
                    "frequency": "weekly/monthly",
                    "description": "NFCI level/trend, bank lending standards, credit sub-indices, financial stress",
                    "key_columns": [
                        "date", "nfci_value", "nfci_credit", "nfci_leverage",
                        "anfci_value", "stl_fsi_value", "lending_standards_avg",
                        "cc_delinquency_rate",
                        "nfci_status", "nfci_trend_status", "lending_status",
                    ],
                },
                {
                    "table": "liquidity_signals",
                    "frequency": "monthly",
                    "description": "M2 money supply growth, business loans, consumer credit",
                    "key_columns": [
                        "date", "m2_yoy_growth", "m2_3m_annualized",
                        "busloans_yoy_growth",
                        "m2_growth_status", "busloans_growth_status", "consumer_credit_status",
                    ],
                },
                {
                    "table": "sentiment_signals",
                    "frequency": "monthly",
                    "description": "Consumer confidence vs sentiment divergence, ISM PMI, stagflation indicator",
                    "key_columns": [
                        "date", "umcsent", "confidence", "pmi", "ism_inventories",
                        "confidence_sentiment_divergence", "orders_prices_spread",
                        "orders_inventories_spread",
                        "divergence_status", "pmi_status", "stagflation_status",
                        "new_orders_status", "orders_inventories_status",
                    ],
                },
                {
                    "table": "fiscal_signals",
                    "frequency": "quarterly",
                    "description": "Debt-to-GDP, interest payment burden, deficit-to-GDP",
                    "key_columns": [
                        "date", "debt_gdp_pct", "debt_gdp_1y_change",
                        "interest_payment", "interest_yoy_growth",
                        "deficit_gdp_pct",
                        "debt_level_status", "debt_trajectory_status",
                        "interest_burden_status", "deficit_status",
                    ],
                },
                {
                    "table": "trade_signals",
                    "frequency": "monthly",
                    "description": "Dollar momentum (broad + EM), trade balance, EM stress",
                    "key_columns": [
                        "date", "dollar_broad_avg", "em_dollar_avg", "trade_balance",
                        "dollar_3m_pct_change", "dollar_12m_pct_change", "em_broad_divergence",
                        "dollar_momentum_status", "trade_deficit_status", "em_stress_status",
                    ],
                },
                {
                    "table": "economic_acceleration_signals",
                    "frequency": "monthly",
                    "description": "Second derivatives of payrolls, CPI, GDP — catches inflection points 3-6 months early",
                    "key_columns": [
                        "date", "payems_mom_pct", "payems_acceleration",
                        "payems_consecutive_negative", "cpi_acceleration",
                        "gdp_acceleration", "composite_accel_zscore",
                        "payems_accel_status", "cpi_accel_status", "gdp_accel_status",
                    ],
                },
                {
                    "table": "net_liquidity_signals",
                    "frequency": "weekly",
                    "description": "Fed balance sheet net liquidity (WALCL - TGA - RRP), proxy for market liquidity",
                    "key_columns": [
                        "date", "walcl", "wtregen", "rrpontsyd", "net_liquidity",
                        "net_liquidity_4w_avg", "net_liquidity_13w_avg",
                        "net_liquidity_zscore", "net_liquidity_trend",
                        "net_liquidity_status", "rrp_depletion_status",
                    ],
                },
                {
                    "table": "diffusion_index_signals",
                    "frequency": "monthly",
                    "description": "Breadth of economic improvement across 20 FRED indicators",
                    "key_columns": [
                        "date", "total_count", "improving_count", "diffusion_pct",
                        "diffusion_6m_avg", "diffusion_zscore", "breadth_trend",
                        "diffusion_status",
                    ],
                },
            ],
            "market_technical_signals": [
                {
                    "table": "momentum_signals",
                    "frequency": "daily",
                    "description": "Time-series momentum, dual momentum allocation, Faber TAA, sector rotation, trend score",
                    "key_columns": [
                        "date", "tsmom_return", "tsmom_signal",
                        "dual_momentum_position", "faber_invested_count",
                        "sector_dispersion", "top_sector", "bottom_sector", "trend_score",
                        "tsmom_status", "dual_momentum_status", "faber_taa_status",
                        "sector_rotation_status", "trend_score_status",
                    ],
                },
                {
                    "table": "market_breadth_signals",
                    "frequency": "daily",
                    "description": "S&P 500 breadth: % above moving averages, McClellan Oscillator, Zweig Thrust, sector participation",
                    "key_columns": [
                        "date", "pct_above_200_ma", "pct_above_50_ma",
                        "ad_ratio", "mcclellan_oscillator", "mcclellan_summation_index",
                        "zweig_thrust_signal", "breadth_divergence_signal",
                        "sector_participation_pct", "avg_pair_correlation_63d", "return_dispersion",
                    ],
                },
                {
                    "table": "market_volatility_signals",
                    "frequency": "daily",
                    "description": "VIX statistics, realized volatility (SPY/QQQ), Parkinson/Garman-Klass estimators, variance risk premium",
                    "key_columns": [
                        "date", "vix_close", "vix_avg_20d",
                        "spy_realized_vol_20d", "qqq_realized_vol_20d",
                        "spy_parkinson_vol_20d", "spy_gk_vol_20d",
                        "spy_vrp_20d", "spy_vrp_30d",
                    ],
                },
                {
                    "table": "technical_signals",
                    "frequency": "daily",
                    "description": "RSI(14/2), Bollinger Bands with squeeze detection, z-score, VIX percentile rank",
                    "key_columns": [
                        "date", "adj_close", "rsi_14", "rsi_2",
                        "bb_bandwidth", "bb_bandwidth_pctile", "bb_position",
                        "zscore_60d", "vix_percentile_1yr", "vix_zscore",
                        "rsi_status", "bollinger_status", "zscore_status", "vix_mean_reversion_status",
                    ],
                },
                {
                    "table": "cross_asset_divergences",
                    "frequency": "daily",
                    "description": "Cross-asset divergence flags: HY-equity, stock-bond correlation, defensive/cyclical ratio, gold-real yield, small/large cap, copper/gold, Dow Theory, semis leadership",
                    "key_columns": [
                        "date", "hy_equity_divergence_flag", "hy_spread_divergence_flag",
                        "stock_bond_corr_regime", "defensive_ratio_uptrend_flag",
                        "gold_real_residual_zscore", "iwm_spy_ratio", "rsp_spy_ratio",
                        "copper_gold_ratio", "aud_risk_divergence_flag",
                        "dow_non_confirmation_flag", "semis_divergence_flag",
                    ],
                },
                {
                    "table": "factor_signals",
                    "frequency": "daily",
                    "description": "Value/Growth ratio (IWD/IWF) and Small/Large cap ratio (IWM/SPY) with moving averages",
                    "key_columns": [
                        "date", "iwd_iwf_ratio", "iwd_iwf_sma_50", "iwd_iwf_sma_200",
                        "iwm_spy_ratio", "iwm_spy_sma_50", "iwm_spy_sma_200",
                    ],
                },
            ],
        },
    }


def _build_tier_2() -> dict[str, Any]:
    """Analysis and domain models with quality checks."""
    return {
        "description": "Processed analytical models with dbt tests. Use when agent views lack needed detail.",
        "economic_regime_analysis": [
            {
                "table": "economic_regime_classification",
                "description": "Monthly regime: Expansion, Slowdown, Contraction, Recovery based on growth/employment/inflation/financial conditions",
                "key_columns": ["month_date", "regime"],
            },
            {
                "table": "factor_tilts",
                "description": "Recommended factor tilts (value, momentum, quality, low_vol, size) by economic regime",
                "key_columns": ["month_date", "regime", "value_tilt", "momentum_tilt", "quality_tilt", "low_vol_tilt", "size_tilt", "notes"],
            },
            {
                "table": "sector_regime_performance",
                "description": "Historical sector returns/volatility/win rates by economic regime",
                "key_columns": ["symbol", "regime"],
            },
        ],
        "sector_sensitivity": [
            {
                "table": "sector_indicator_sensitivity",
                "description": "Rolling correlations between FRED indicators and sector ETF returns with lag analysis",
                "key_columns": ["symbol", "series_code", "sensitivity_score"],
            },
            {
                "table": "sector_sensitivity_summary",
                "description": "Aggregated sector-indicator sensitivity rankings and percentiles",
                "key_columns": ["symbol", "series_code"],
            },
            {
                "table": "ticker_sector_sensitivity",
                "description": "Individual stock -> GICS sector -> sector ETF -> macro sensitivity mapping",
                "key_columns": [
                    "symbol", "company_name", "gics_sector", "sub_industry",
                    "sector_etf", "avg_sensitivity_score", "macro_exposure_level",
                    "top_sensitive_indicators", "sector_type",
                ],
            },
            {
                "table": "portfolio_macro_factors",
                "description": "Sector exposures to 8 macro factors: Inflation, Employment, Growth, Housing, Consumer, Rates, Financial, Business",
                "key_columns": [
                    "symbol", "sector_name", "macro_factor",
                    "avg_sensitivity", "factor_exposure_score", "factor_rank",
                ],
            },
        ],
        "correlation_analysis": [
            {
                "table": "correlation_analysis_enhanced",
                "description": "Statistically significant economic-market correlations with p-values and quality ratings",
                "key_columns": ["symbol", "series_code", "quality_rating"],
            },
            {
                "table": "indicator_market_response",
                "description": "Event study: sector response to economic indicator releases (pro-cyclical, counter-cyclical, neutral)",
                "key_columns": ["symbol", "series_code", "response_type"],
            },
            {
                "table": "market_economic_analysis",
                "description": "Market returns joined with FRED indicators for regime and stress analysis",
                "key_columns": ["year_month", "month_date"],
            },
            {
                "table": "base_historical_analysis",
                "description": "Unified market data across all asset categories with FRED joins",
                "key_columns": [
                    "symbol", "date", "current_price", "category",
                    "pct_change_1mo", "pct_change_3mo", "pct_change_6mo", "pct_change_1yr",
                    "series_name", "value", "period_diff",
                ],
                "filter_by": {
                    "category": ["currency", "fixed_income", "global_markets", "major_indicies", "sector"],
                },
            },
        ],
        "dispersion_analysis": [
            {
                "table": "sector_dispersion_analysis",
                "description": "Per-sector return dispersion, best/worst performers, and spread metrics",
                "key_columns": ["symbol"],
            },
            {
                "table": "sector_breadth_timeseries",
                "description": "Weekly percentage of stocks above 200-day MA by sector",
                "key_columns": ["date"],
            },
        ],
        "reddit_analysis": [
            {
                "table": "reddit_sentiment_trends",
                "description": "Daily sentiment by subreddit with VADER scores and momentum",
                "key_columns": ["partition_date", "subreddit", "num_posts", "avg_score", "sentiment_trend"],
            },
            {
                "table": "reddit_cross_subreddit_activity",
                "description": "Cross-subreddit engagement patterns",
            },
            {
                "table": "reddit_thread_structure",
                "description": "Thread popularity and reply depth patterns",
            },
        ],
        "government_economic": [
            {
                "table": "fred_series_latest_aggregates",
                "description": "Current snapshot of all FRED series with percentage changes over 3m/6m/1y",
                "key_columns": ["series_code", "series_name", "month", "current_value", "pct_change_3m", "pct_change_6m", "pct_change_1y", "date_grain"],
            },
            {
                "table": "fred_monthly_diff",
                "description": "Monthly FRED series with period-over-period differences",
                "key_columns": ["series_code", "series_name", "date", "value", "period_diff"],
            },
            {
                "table": "housing_mortgage_rates",
                "description": "30-year mortgage rates with monthly payment calculations",
                "key_columns": ["date", "mortgage_rate", "monthly_payment_no_down_payment", "monthly_payment_20_pct_down_payment"],
            },
            {
                "table": "housing_inventory",
                "description": "Historical housing inventory levels",
                "key_columns": ["series_code", "series_name"],
            },
            {
                "table": "housing_inventory_and_population",
                "description": "Housing inventory relative to population",
            },
        ],
        "market_domain": [
            {
                "table": "major_indicies_summary",
                "description": "S&P 500, NASDAQ, Dow Jones performance by time period",
                "key_columns": ["symbol", "time_period", "total_return_pct", "volatility_pct"],
            },
            {
                "table": "us_sector_summary",
                "description": "11 sector ETF performance by time period",
                "key_columns": ["symbol", "time_period", "total_return_pct", "volatility_pct"],
            },
            {
                "table": "currency_summary",
                "description": "Currency pair ETF performance",
            },
            {
                "table": "global_markets_summary",
                "description": "International market ETF performance",
            },
            {
                "table": "sp500_companies_summary",
                "description": "Individual S&P 500 stock performance (large table)",
                "note": "Use ticker_sector_sensitivity for stock-to-sector mapping instead of scanning this",
            },
            {
                "table": "nasdaq_companies_summary",
                "description": "Individual NASDAQ stock performance (large table)",
            },
        ],
        "commodity_domain": [
            {
                "table": "energy_commodities_summary",
                "description": "Oil, gas, energy commodity performance",
            },
            {
                "table": "agriculture_commodities_summary",
                "description": "Grain, livestock, softs commodity performance",
            },
            {
                "table": "input_commodities_summary",
                "description": "Metals, industrial input commodity performance",
            },
        ],
        "data_quality": [
            {
                "table": "data_quality_anomalies",
                "description": "Unified anomaly report — check this before drawing conclusions from other tables",
                "key_columns": ["source_table", "symbol", "date", "check_type", "failure_reason", "detected_at"],
                "check_types": ["stale_price", "return_spike", "zscore", "invalid_price"],
            },
        ],
        "backtesting_snapshots": [
            {
                "table": "us_sector_summary_snapshot",
                "description": "Monthly sector performance snapshots for point-in-time analysis",
            },
            {
                "table": "leading_econ_return_indicator_snapshot",
                "description": "Monthly economic correlation snapshots",
            },
            {
                "table": "fred_series_latest_aggregates_snapshot",
                "description": "Monthly FRED value snapshots",
            },
            {
                "table": "energy_commodities_summary_snapshot",
                "description": "Monthly energy commodity snapshots",
            },
            {
                "table": "input_commodities_summary_snapshot",
                "description": "Monthly industrial commodity snapshots",
            },
            {
                "table": "agriculture_commodities_summary_snapshot",
                "description": "Monthly agriculture commodity snapshots",
            },
        ],
    }


def _build_tier_3() -> dict[str, Any]:
    """Raw and staging tables — use only when processed data is insufficient."""
    return {
        "description": (
            "Raw and staging tables. Only query these when Tier 1/2 tables lack the granularity "
            "or specific columns you need. Raw tables have minimal quality checks."
        ),
        "staging_market_data": [
            {"table": "stg_us_sectors", "description": "Daily OHLCV for 11 sector ETFs"},
            {"table": "stg_major_indices", "description": "Daily OHLCV for S&P 500, NASDAQ, Dow Jones"},
            {"table": "stg_currency", "description": "Daily OHLCV for currency pair ETFs"},
            {"table": "stg_fixed_income", "description": "Daily OHLCV for bond/fixed income ETFs"},
            {"table": "stg_global_markets", "description": "Daily OHLCV for international market ETFs"},
            {"table": "stg_sp500_companies_prices", "description": "Daily OHLCV for ~500 S&P 500 stocks (large)"},
            {"table": "stg_nasdaq_companies_prices", "description": "Daily OHLCV for NASDAQ stocks (large)"},
            {"table": "stg_fred_series", "description": "Raw FRED data (date, series_code, value)"},
            {"table": "stg_treasury_yields", "description": "Raw treasury yield curve data by maturity"},
        ],
        "staging_commodities": [
            {"table": "stg_energy_commodities", "description": "Raw energy commodity prices"},
            {"table": "stg_agriculture_commodities", "description": "Raw agriculture commodity prices"},
            {"table": "stg_input_commodities", "description": "Raw industrial input commodity prices"},
        ],
        "staging_housing": [
            {"table": "stg_housing_inventory", "description": "BLS housing inventory data"},
            {"table": "stg_housing_pulse", "description": "Biweekly BLS housing pulse survey"},
            {"table": "stg_realtor_country_history", "description": "Realtor.com country-level housing"},
            {"table": "stg_realtor_state_history", "description": "Realtor.com state-level housing"},
            {"table": "stg_realtor_metro_history", "description": "Realtor.com metro-level housing"},
            {"table": "stg_realtor_county_history", "description": "Realtor.com county-level housing"},
            {"table": "stg_realtor_zip_history", "description": "Realtor.com ZIP code-level housing"},
        ],
        "staging_sentiment": [
            {"table": "stg_reddit_posts", "description": "Cleaned Reddit posts from finance subreddits"},
            {"table": "stg_reddit_comments", "description": "Reddit comment threads"},
            {"table": "stg_reddit_post_content", "description": "Full post text and links"},
            {"table": "stg_reddit_ticker_mentions", "description": "Extracted stock ticker mentions ($AAPL etc)"},
            {"table": "stg_reddit_sentiment", "description": "VADER sentiment scores per post/comment"},
        ],
        "staging_events": [
            {"table": "stg_economic_calendar", "description": "Economic event calendar (growth, inflation, employment, etc.)"},
            {"table": "stg_earnings_calendar", "description": "Earnings announcement calendar"},
            {"table": "stg_fomc_minutes", "description": "FOMC meeting minutes metadata"},
        ],
        "staging_fomc": [
            {"table": "stg_fomc_transcripts", "description": "Full FOMC transcript text"},
            {"table": "stg_fomc_meeting_summaries", "description": "AI-generated meeting summaries"},
            {"table": "stg_fomc_meetings_enhanced", "description": "Enhanced FOMC calendar with transcript availability"},
            {"table": "stg_transcript_sections", "description": "Parsed transcript sections with speaker attribution"},
            {"table": "stg_transcript_topics", "description": "Extracted topics and sentiment from transcripts"},
        ],
        "staging_corporate": [
            {"table": "corporate_actions", "description": "Stock splits and dividends (API + heuristic detection)"},
            {"table": "split_adjusted_prices", "description": "Historical prices adjusted for splits"},
            {"table": "stg_sp500_companies_active", "description": "Current S&P 500 constituents with GICS classification"},
        ],
        "raw_tables_avoid": {
            "description": "These are raw ingestion tables. Prefer staging or higher layers instead.",
            "tables": [
                "fred_raw", "financial_conditions_index",
                "housing_inventory_raw", "housing_pulse_raw", "treasury_yields_raw",
                "realtor_country_raw", "realtor_state_raw", "realtor_metro_raw",
                "realtor_county_raw", "realtor_zip_raw",
                "us_sector_etfs_raw", "currency_etfs_raw", "major_indices_raw",
                "fixed_income_etfs_raw", "global_markets_raw",
                "sp500_companies_prices_raw", "nasdaq_companies_prices_raw", "sp500_splits_raw",
                "agriculture_commodities_raw", "energy_commodities_raw", "input_commodities_raw",
                "reddit_posts_raw", "reddit_post_content_raw", "reddit_comments_raw",
                "reddit_content_embeddings", "reddit_sentiment_scored", "reddit_ticker_mentions",
                "economic_calendar", "earnings_calendar",
                "fomc_minutes_metadata", "fomc_transcripts", "transcript_sections",
                "fomc_meeting_summaries", "transcript_topics", "transcript_search_index",
                "member_voting_history", "fomc_meetings_enhanced",
                "sp500_companies_raw",
            ],
        },
    }


# ---------------------------------------------------------------------------
# Text renderer
# ---------------------------------------------------------------------------
def render_text(manifest: dict[str, Any], indent: int = 0) -> str:
    """Render manifest as readable plain text."""
    lines: list[str] = []
    _render_node(manifest, lines, indent)
    return "\n".join(lines)


def _render_node(node: Any, lines: list[str], indent: int) -> None:
    prefix = "  " * indent
    if isinstance(node, dict):
        for key, value in node.items():
            if isinstance(value, str):
                lines.append(f"{prefix}{key}: {value}")
            elif isinstance(value, (int, float)):
                lines.append(f"{prefix}{key}: {value}")
            elif isinstance(value, list) and all(isinstance(v, str) for v in value):
                lines.append(f"{prefix}{key}:")
                for item in value:
                    lines.append(f"{prefix}  - {item}")
            else:
                lines.append(f"{prefix}{key}:")
                _render_node(value, lines, indent + 1)
    elif isinstance(node, list):
        for item in node:
            if isinstance(item, dict) and "table" in item:
                table_name = item["table"]
                desc = item.get("description", "")
                confidence = item.get("confidence", "")
                confidence_tag = f" [{confidence}]" if confidence else ""
                lines.append(f"{prefix}- {table_name}{confidence_tag}: {desc}")
                for k, v in item.items():
                    if k in ("table", "description", "confidence"):
                        continue
                    if isinstance(v, list):
                        lines.append(f"{prefix}    {k}: {', '.join(str(x) for x in v)}")
                    elif isinstance(v, dict):
                        lines.append(f"{prefix}    {k}:")
                        for fk, fv in v.items():
                            if isinstance(fv, list):
                                lines.append(f"{prefix}      {fk}: {', '.join(str(x) for x in fv)}")
                            else:
                                lines.append(f"{prefix}      {fk}: {fv}")
                    else:
                        lines.append(f"{prefix}    {k}: {v}")
            elif isinstance(item, str):
                lines.append(f"{prefix}- {item}")
            else:
                _render_node(item, lines, indent)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Generate data platform manifest for Claude AI")
    parser.add_argument(
        "--output", "-o",
        default="scripts/data_platform_manifest.yaml",
        help="Output file path (default: scripts/data_platform_manifest.yaml)",
    )
    parser.add_argument(
        "--format", "-f",
        choices=["yaml", "text"],
        default="yaml",
        help="Output format (default: yaml)",
    )
    args = parser.parse_args()

    # Introspect dbt test coverage
    coverage = scan_test_coverage()
    print(f"Scanned {len(coverage)} models from dbt schema files")

    # Print coverage summary
    by_conf = {"high": 0, "medium": 0, "low": 0, "untested": 0}
    for mc in coverage.values():
        by_conf[mc.confidence] += 1
    print(f"  Confidence: {by_conf['high']} high, {by_conf['medium']} medium, {by_conf['low']} low, {by_conf['untested']} untested")

    manifest = build_manifest(coverage)

    output_path = Path(args.output)
    if args.format == "yaml":
        if not output_path.suffix:
            output_path = output_path.with_suffix(".yaml")
        content = yaml.dump(manifest, default_flow_style=False, sort_keys=False, width=120, allow_unicode=True)
    else:
        if output_path.suffix == ".yaml":
            output_path = output_path.with_suffix(".txt")
        content = render_text(manifest)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")
    print(f"Manifest written to {output_path} ({len(content):,} bytes)")


if __name__ == "__main__":
    main()
