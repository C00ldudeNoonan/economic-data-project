"""Dagster assets for generating economic indicator narratives.

This module provides Dagster assets that generate plain-English narratives
for economic indicator releases and store them in the database for UI consumption.
"""

import io
from datetime import datetime
from typing import Any

import dagster as dg
import polars as pl
from pydantic import Field

from macro_agents.defs.analysis.narratives.economic_narrative_generator import (
    EconomicNarrativeModule,
    IndicatorForecastModule,
    INDICATOR_CONFIGS,
    get_indicator_config,
)
from macro_agents.defs.analysis.economy_state.economy_state_analyzer import (
    EconomicAnalysisResource,
    _estimate_tokens,
    _check_rate_limit,
)
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


class NarrativeGenerationConfig(dg.Config):
    """Configuration for economic narrative generation."""

    personality: str = Field(
        default="neutral",
        description="Analytical personality: 'skeptical', 'neutral', or 'bullish'",
    )
    model_provider: str | None = Field(
        default=None,
        description="LLM provider override: 'openai', 'anthropic', or 'gemini'",
    )
    model_name: str | None = Field(
        default=None,
        description="LLM model name override",
    )
    indicators_to_process: list[str] | None = Field(
        default=None,
        description="List of indicator series codes to process. If None, processes all configured indicators.",
    )
    force_regenerate: bool = Field(
        default=False,
        description="If True, regenerate narratives even if they exist for today",
    )


def get_indicator_data(
    md_resource: BigQueryWarehouseResource,
    series_code: str,
    max_periods: int = 24,
) -> dict[str, Any]:
    """Fetch current and historical data for an indicator.

    Returns dict with:
    - current_value: Latest value
    - previous_value: Previous period value
    - historical_data: CSV of historical values
    - related_data: CSV of related indicator values
    """
    # Get latest values for this indicator
    latest_query = f"""
    SELECT
        series_code,
        series_name,
        month,
        current_value,
        pct_change_3m,
        pct_change_6m,
        pct_change_1y
    FROM agent_fred_series_latest_aggregates
    WHERE series_code = '{series_code}'
    ORDER BY month DESC
    LIMIT 2
    """

    latest_df = md_resource.execute_query(latest_query, read_only=True)

    if latest_df.is_empty():
        return {
            "current_value": "N/A",
            "previous_value": "N/A",
            "historical_data": "",
            "related_data": "",
            "series_name": series_code,
        }

    rows = latest_df.to_dicts()
    current_value = str(rows[0].get("current_value", "N/A"))
    previous_value = (
        str(rows[1].get("current_value", "N/A")) if len(rows) > 1 else "N/A"
    )
    series_name = rows[0].get("series_name", series_code)
    # Get the actual data release date (month) from the indicator data
    release_month = rows[0].get("month")
    release_date = release_month.strftime("%Y-%m-%d") if release_month else None

    # Get historical data
    historical_query = f"""
    SELECT
        series_code,
        date,
        value,
        period_diff
    FROM agent_fred_monthly_diff
    WHERE series_code = '{series_code}'
    ORDER BY date DESC
    LIMIT {max_periods}
    """

    historical_df = md_resource.execute_query(historical_query, read_only=True)
    historical_csv = ""
    if not historical_df.is_empty():
        csv_buffer = io.BytesIO()
        historical_df.write_csv(csv_buffer)
        historical_csv = csv_buffer.getvalue().decode("utf-8")

    # Get related indicators data
    config = get_indicator_config(series_code)
    related_series = config.get("related_series", [])
    related_csv = ""

    if related_series:
        related_list = "', '".join(related_series)
        related_query = f"""
        SELECT
            series_code,
            series_name,
            month,
            current_value,
            pct_change_3m,
            pct_change_6m,
            pct_change_1y
        FROM agent_fred_series_latest_aggregates
        WHERE series_code IN ('{related_list}')
        ORDER BY series_code, month DESC
        """
        related_df = md_resource.execute_query(related_query, read_only=True)
        if not related_df.is_empty():
            csv_buffer = io.BytesIO()
            related_df.write_csv(csv_buffer)
            related_csv = csv_buffer.getvalue().decode("utf-8")

    return {
        "current_value": current_value,
        "previous_value": previous_value,
        "historical_data": historical_csv,
        "related_data": related_csv,
        "series_name": series_name,
        "release_date": release_date,
    }


def get_fed_policy_context(md_resource: BigQueryWarehouseResource) -> str:
    """Get current Fed policy stance from recent FOMC data."""
    try:
        # Try to get recent FOMC summary if available
        fed_query = """
        SELECT
            series_code,
            series_name,
            current_value
        FROM agent_fred_series_latest_aggregates
        WHERE series_code IN ('FEDFUNDS', 'DFF')
        ORDER BY month DESC
        LIMIT 1
        """
        fed_df = md_resource.execute_query(fed_query, read_only=True)

        if not fed_df.is_empty():
            rows = fed_df.to_dicts()
            rate = rows[0].get("current_value", "N/A")
            return f"Current Fed Funds Rate: {rate}%"
        return "Fed policy context not available"
    except Exception:
        return "Fed policy context not available"


def create_narrative_table_if_not_exists(md_resource: BigQueryWarehouseResource) -> None:
    """Create the economic_indicator_narratives table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS economic_indicator_narratives (
        id VARCHAR PRIMARY KEY,
        indicator_code VARCHAR NOT NULL,
        indicator_name VARCHAR NOT NULL,
        indicator_category VARCHAR,
        release_date DATE NOT NULL,
        current_value VARCHAR,
        previous_value VARCHAR,
        expected_value VARCHAR,
        headline TEXT,
        summary TEXT,
        market_implications TEXT,
        retail_investor_takeaway TEXT,
        historical_comparison TEXT,
        confidence_level VARCHAR,
        personality VARCHAR,
        model_name VARCHAR,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        dagster_run_id VARCHAR
    )
    """
    md_resource.execute_query(create_table_query, read_only=False)


def create_indicator_forecast_table_if_not_exists(
    md_resource: BigQueryWarehouseResource,
) -> None:
    """Create the economic_indicator_forecasts table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS economic_indicator_forecasts (
        id VARCHAR PRIMARY KEY,
        indicator_code VARCHAR NOT NULL,
        indicator_name VARCHAR NOT NULL,
        forecast_date DATE NOT NULL,
        next_release_forecast TEXT,
        three_month_outlook TEXT,
        forecast_rationale TEXT,
        personality VARCHAR,
        model_name VARCHAR,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        dagster_run_id VARCHAR
    )
    """
    md_resource.execute_query(create_table_query, read_only=False)


@dg.asset(
    kinds={"dspy", "duckdb"},
    group_name="economic_narratives",
    description="Generate plain-English narratives for economic indicator releases and store in database",
    deps=[
        dg.AssetKey(["agent_fred_series_latest_aggregates"]),
        dg.AssetKey(["agent_fred_monthly_diff"]),
    ],
)
def generate_economic_narratives(
    context: dg.AssetExecutionContext,
    config: NarrativeGenerationConfig,
    bq: BigQueryWarehouseResource,
    economic_analysis: EconomicAnalysisResource,
) -> dg.MaterializeResult:
    """
    Generate plain-English narratives for economic indicators.

    This asset:
    1. Fetches latest economic indicator data
    2. Generates narratives using DSPy modules
    3. Stores results in economic_indicator_narratives table

    The narratives include:
    - Headline summary
    - Detailed plain-English explanation
    - Market implications
    - Retail investor takeaways
    - Historical context comparisons
    """
    context.log.info(
        f"Starting economic narrative generation (personality: {config.personality})..."
    )

    # Setup DSPy
    economic_analysis.setup_for_execution(
        context,
        provider_override=config.model_provider,
        model_name_override=config.model_name,
    )

    # Create table if not exists
    create_narrative_table_if_not_exists(md)

    # Initialize narrative module
    narrative_module = EconomicNarrativeModule(personality=config.personality)

    # Determine which indicators to process
    indicators_to_process = config.indicators_to_process or list(
        INDICATOR_CONFIGS.keys()
    )

    context.log.info(f"Processing {len(indicators_to_process)} indicators...")

    narratives_generated = 0
    narratives_skipped = 0
    errors = []

    today = datetime.now().strftime("%Y-%m-%d")

    for series_code in indicators_to_process:
        try:
            indicator_config = get_indicator_config(series_code)
            indicator_name = indicator_config.get("name", series_code)
            indicator_category = indicator_config.get("category", "other")

            # Get indicator data first to obtain actual release date
            indicator_data = get_indicator_data(bq, series_code)

            if indicator_data["current_value"] == "N/A":
                context.log.warning(f"No data found for {series_code}, skipping...")
                narratives_skipped += 1
                continue

            # Use actual release date from the data, fallback to today
            release_date = indicator_data.get("release_date") or today

            # Check if narrative already exists for this release date (unless force_regenerate)
            if not config.force_regenerate:
                existing_check = f"""
                SELECT COUNT(*) as cnt FROM economic_indicator_narratives
                WHERE indicator_code = '{series_code}'
                AND release_date = '{release_date}'
                AND personality = '{config.personality}'
                """
                try:
                    result = bq.execute_query(existing_check, read_only=True)
                    if not result.is_empty() and result.to_dicts()[0].get("cnt", 0) > 0:
                        context.log.info(
                            f"Skipping {series_code} - narrative already exists for release date {release_date}"
                        )
                        narratives_skipped += 1
                        continue
                except Exception:
                    pass  # Table might not exist yet, continue

            context.log.info(
                f"Generating narrative for {indicator_name} ({series_code}) - release date: {release_date}..."
            )

            # Get Fed policy context
            fed_context = get_fed_policy_context(md)

            # Check rate limits
            combined_input = (
                indicator_data["historical_data"]
                + indicator_data["related_data"]
                + fed_context
            )
            estimated_tokens = _estimate_tokens(combined_input)
            _check_rate_limit(
                economic_analysis._get_provider(),
                economic_analysis._get_model_name(),
                estimated_tokens,
                context,
            )

            # Generate narrative
            result = narrative_module(
                indicator_name=indicator_name,
                indicator_category=indicator_category,
                current_value=indicator_data["current_value"],
                previous_value=indicator_data["previous_value"],
                expected_value="N/A",  # Would need consensus data source
                historical_context=indicator_data["historical_data"],
                related_indicators=indicator_data["related_data"],
                current_fed_policy=fed_context,
                personality=config.personality,
            )

            # Create unique ID using actual release date
            narrative_id = f"{series_code}_{release_date}_{config.personality}"

            # Prepare data for insertion
            narrative_record = {
                "id": narrative_id,
                "indicator_code": series_code,
                "indicator_name": indicator_name,
                "indicator_category": indicator_category,
                "release_date": release_date,
                "current_value": indicator_data["current_value"],
                "previous_value": indicator_data["previous_value"],
                "expected_value": "N/A",
                "headline": result.headline,
                "summary": result.summary,
                "market_implications": result.market_implications,
                "retail_investor_takeaway": result.retail_investor_takeaway,
                "historical_comparison": result.historical_comparison,
                "confidence_level": result.confidence_level,
                "personality": config.personality,
                "model_name": economic_analysis._get_model_name(),
                "created_at": datetime.now().isoformat(),
                "dagster_run_id": context.run_id,
            }

            # Insert into database
            df = pl.DataFrame([narrative_record])
            bq.upsert_data(
                table_name="economic_indicator_narratives",
                data=df,
                key_columns=["id"],
                context=context,
            )

            narratives_generated += 1
            context.log.info(
                f"Generated narrative for {indicator_name}: {result.headline[:50]}..."
            )

        except Exception as e:
            error_msg = f"Error generating narrative for {series_code}: {str(e)}"
            context.log.error(error_msg)
            errors.append(error_msg)

    result_metadata = {
        "narratives_generated": narratives_generated,
        "narratives_skipped": narratives_skipped,
        "indicators_processed": len(indicators_to_process),
        "errors": len(errors),
        "error_details": errors[:5] if errors else [],  # Limit error details
        "personality": config.personality,
        "model_name": economic_analysis._get_model_name(),
        "output_table": "economic_indicator_narratives",
    }

    context.log.info(f"Narrative generation complete: {result_metadata}")
    return dg.MaterializeResult(metadata=result_metadata)


@dg.asset(
    kinds={"dspy", "duckdb"},
    group_name="economic_narratives",
    description="Generate short-term forecasts for economic indicators",
    deps=[
        dg.AssetKey(["agent_fred_series_latest_aggregates"]),
        dg.AssetKey(["agent_fred_monthly_diff"]),
        dg.AssetKey(["analyze_economy_state"]),
    ],
)
def generate_indicator_forecasts(
    context: dg.AssetExecutionContext,
    config: NarrativeGenerationConfig,
    bq: BigQueryWarehouseResource,
    economic_analysis: EconomicAnalysisResource,
) -> dg.MaterializeResult:
    """
    Generate short-term forecasts for economic indicators.

    This asset:
    1. Fetches historical indicator data
    2. Gets current economic conditions summary
    3. Generates forecasts using DSPy modules
    4. Stores results in economic_indicator_forecasts table
    """
    context.log.info(
        f"Starting indicator forecast generation (personality: {config.personality})..."
    )

    # Setup DSPy
    economic_analysis.setup_for_execution(
        context,
        provider_override=config.model_provider,
        model_name_override=config.model_name,
    )

    # Create table if not exists
    create_indicator_forecast_table_if_not_exists(md)

    # Initialize forecast module
    forecast_module = IndicatorForecastModule(personality=config.personality)

    # Get current economic conditions summary
    economic_conditions = ""
    try:
        conditions_query = """
        SELECT analysis_content
        FROM economy_state_analysis
        ORDER BY analysis_timestamp DESC
        LIMIT 1
        """
        conditions_df = bq.execute_query(conditions_query, read_only=True)
        if not conditions_df.is_empty():
            economic_conditions = conditions_df.to_dicts()[0].get(
                "analysis_content", ""
            )[:2000]
    except Exception as e:
        context.log.warning(f"Could not get economic conditions: {e}")

    # Determine which indicators to process
    indicators_to_process = config.indicators_to_process or list(
        INDICATOR_CONFIGS.keys()
    )

    context.log.info(
        f"Processing forecasts for {len(indicators_to_process)} indicators..."
    )

    forecasts_generated = 0
    forecasts_skipped = 0
    errors = []

    today = datetime.now().strftime("%Y-%m-%d")

    for series_code in indicators_to_process:
        try:
            indicator_config = get_indicator_config(series_code)
            indicator_name = indicator_config.get("name", series_code)

            # Check if forecast already exists for today
            if not config.force_regenerate:
                existing_check = f"""
                SELECT COUNT(*) as cnt FROM economic_indicator_forecasts
                WHERE indicator_code = '{series_code}'
                AND forecast_date = '{today}'
                AND personality = '{config.personality}'
                """
                try:
                    result = bq.execute_query(existing_check, read_only=True)
                    if not result.is_empty() and result.to_dicts()[0].get("cnt", 0) > 0:
                        context.log.info(
                            f"Skipping {series_code} forecast - already exists for today"
                        )
                        forecasts_skipped += 1
                        continue
                except Exception:
                    pass

            context.log.info(
                f"Generating forecast for {indicator_name} ({series_code})..."
            )

            # Get historical data
            indicator_data = get_indicator_data(bq, series_code, max_periods=36)

            if not indicator_data["historical_data"]:
                context.log.warning(
                    f"No historical data for {series_code}, skipping forecast..."
                )
                forecasts_skipped += 1
                continue

            # Check rate limits
            estimated_tokens = _estimate_tokens(
                indicator_data["historical_data"] + economic_conditions
            )
            _check_rate_limit(
                economic_analysis._get_provider(),
                economic_analysis._get_model_name(),
                estimated_tokens,
                context,
            )

            # Generate forecast
            result = forecast_module(
                indicator_name=indicator_name,
                historical_data=indicator_data["historical_data"],
                leading_indicators=indicator_data["related_data"],
                current_economic_conditions=economic_conditions,
                personality=config.personality,
            )

            # Create unique ID
            forecast_id = f"{series_code}_{today}_{config.personality}_forecast"

            # Prepare data for insertion
            forecast_record = {
                "id": forecast_id,
                "indicator_code": series_code,
                "indicator_name": indicator_name,
                "forecast_date": today,
                "next_release_forecast": result.next_release_forecast,
                "three_month_outlook": result.three_month_outlook,
                "forecast_rationale": result.forecast_rationale,
                "personality": config.personality,
                "model_name": economic_analysis._get_model_name(),
                "created_at": datetime.now().isoformat(),
                "dagster_run_id": context.run_id,
            }

            # Insert into database
            df = pl.DataFrame([forecast_record])
            bq.upsert_data(
                table_name="economic_indicator_forecasts",
                data=df,
                key_columns=["id"],
                context=context,
            )

            forecasts_generated += 1
            context.log.info(f"Generated forecast for {indicator_name}")

        except Exception as e:
            error_msg = f"Error generating forecast for {series_code}: {str(e)}"
            context.log.error(error_msg)
            errors.append(error_msg)

    result_metadata = {
        "forecasts_generated": forecasts_generated,
        "forecasts_skipped": forecasts_skipped,
        "indicators_processed": len(indicators_to_process),
        "errors": len(errors),
        "error_details": errors[:5] if errors else [],
        "personality": config.personality,
        "model_name": economic_analysis._get_model_name(),
        "output_table": "economic_indicator_forecasts",
    }

    context.log.info(f"Forecast generation complete: {result_metadata}")
    return dg.MaterializeResult(metadata=result_metadata)
