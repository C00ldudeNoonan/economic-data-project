import dspy
from typing import Dict, Any, Optional
from datetime import datetime
import dagster as dg
from pydantic import Field
import re
import os
import io
import time
from collections import defaultdict

from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.resources.gcs import GCSResource


ANTHROPIC_RATE_LIMITS = {
    "claude-3-5-sonnet-20241022": {
        "input_tokens_per_minute": 200000,
        "output_tokens_per_minute": 200000,
        "requests_per_minute": 5000,
    },
    "claude-3-5-haiku-20241022": {
        "input_tokens_per_minute": 50000,
        "output_tokens_per_minute": 50000,
        "requests_per_minute": 5000,
    },
    "claude-3-opus-20240229": {
        "input_tokens_per_minute": 50000,
        "output_tokens_per_minute": 50000,
        "requests_per_minute": 5000,
    },
    "claude-3-sonnet-20240229": {
        "input_tokens_per_minute": 200000,
        "output_tokens_per_minute": 200000,
        "requests_per_minute": 5000,
    },
    "claude-3-haiku-20240307": {
        "input_tokens_per_minute": 50000,
        "output_tokens_per_minute": 50000,
        "requests_per_minute": 5000,
    },
}

_rate_limit_tracking = defaultdict(lambda: {"tokens": [], "requests": []})


def _estimate_tokens(text: str) -> int:
    """Rough token estimation: ~4 characters per token."""
    return len(text) // 4


def _check_rate_limit(
    provider: str,
    model_name: str,
    estimated_input_tokens: int,
    context: Optional[dg.AssetExecutionContext] = None,
) -> None:
    """Check if request would exceed rate limits and wait if necessary."""
    if provider != "anthropic":
        return

    current_time = time.time()
    limits = ANTHROPIC_RATE_LIMITS.get(model_name)
    if not limits:
        if context:
            context.log.warning(
                f"Unknown Anthropic model {model_name}, using default limits"
            )
        limits = {
            "input_tokens_per_minute": 50000,
            "output_tokens_per_minute": 50000,
            "requests_per_minute": 5000,
        }

    key = f"{provider}:{model_name}"
    tracking = _rate_limit_tracking[key]

    one_minute_ago = current_time - 60

    tracking["tokens"] = [t for t in tracking["tokens"] if t["time"] > one_minute_ago]
    tracking["requests"] = [r for r in tracking["requests"] if r > one_minute_ago]

    total_tokens_last_minute = sum(t["tokens"] for t in tracking["tokens"])

    if (
        total_tokens_last_minute + estimated_input_tokens
        > limits["input_tokens_per_minute"]
    ):
        oldest_token_time = min(
            (t["time"] for t in tracking["tokens"]), default=current_time
        )
        wait_time = 60 - (current_time - oldest_token_time) + 1
        if wait_time > 0:
            if context:
                context.log.warning(
                    f"Rate limit approaching for {model_name}. "
                    f"Used {total_tokens_last_minute}/{limits['input_tokens_per_minute']} tokens/min. "
                    f"Waiting {wait_time:.1f}s before proceeding."
                )
            time.sleep(wait_time)
            tracking["tokens"] = []
            tracking["requests"] = []

    tracking["tokens"].append({"time": current_time, "tokens": estimated_input_tokens})
    tracking["requests"].append(current_time)


class EconomicAnalysisConfig(dg.Config):
    """Configuration for economic analysis assets."""

    personality: str = Field(
        default="skeptical",
        description="Analytical personality: 'skeptical' (default, bearish), 'neutral' (balanced), or 'bullish' (optimistic)",
    )
    model_provider: Optional[str] = Field(
        default=None,
        description="LLM provider override: 'openai', 'anthropic', or 'gemini'. If not provided, uses resource default or LLM_PROVIDER env var.",
    )
    model_name: Optional[str] = Field(
        default=None,
        description="LLM model name override (e.g., 'gpt-4-turbo-preview', 'claude-3-5-haiku-20241022', 'gemini-2.0-flash-exp'). If not provided, uses resource default or MODEL_NAME env var.",
    )


class EconomyStateAnalysisSignature(dspy.Signature):
    """Analyze current economic indicators to determine the state of the economy."""

    economic_data: str = dspy.InputField(
        desc="CSV data containing latest economic indicators with current values and percentage changes over 3m, 6m, 1y periods"
    )

    commodity_data: str = dspy.InputField(
        desc="CSV data containing commodity price performance across energy, industrial/input, and agricultural commodities with returns, volatility, and trends over different time periods"
    )

    financial_conditions_index: str = dspy.InputField(
        desc="CSV data containing full historical Financial Conditions Index (FCI) values with date, FCI score, and component scores. FCI values above zero indicate expansionary conditions, below zero indicate contractionary conditions. Use the full history to identify trends, cycles, and current position relative to historical norms."
    )

    housing_data: str = dspy.InputField(
        desc="CSV data containing housing market indicators including inventory levels (with 3m, 6m, 1y changes) and mortgage rates with affordability metrics (median prices, monthly payments). Housing is a leading economic indicator - rising inventory suggests slowing demand, while affordability metrics indicate consumer purchasing power."
    )

    yield_curve_data: str = dspy.InputField(
        desc="CSV data containing Treasury yield curve data with key spreads (10Y-2Y, 10Y-3M) and curve shape classification (Steep/Normal/Flat/Inverted). Inverted yield curves (10Y-2Y < 0) historically precede recessions. Normal curves indicate healthy economic expectations."
    )

    economic_trends: str = dspy.InputField(
        desc="CSV data containing month-over-month changes for key economic indicators (GDP, CPI, unemployment, payrolls, Fed funds rate, industrial production, retail sales, housing starts). These trends show momentum and direction of economic activity."
    )

    personality: str = dspy.InputField(
        desc="Analytical personality: 'skeptical' (default, bearish, focuses on risks and downside), 'neutral' (balanced, objective), or 'bullish' (optimistic, focuses on opportunities and upside)"
    )

    analysis: str = dspy.OutputField(
        desc="""Comprehensive economic state analysis reflecting the specified personality perspective, including:
        1. Current Economic Cycle Position (Early/Expansion/Late/Recession with confidence level 0-1)
        2. Financial Conditions Index Analysis:
           - Current FCI value and its interpretation (expansionary if >0, contractionary if <0)
           - FCI trend over recent periods (3m, 6m, 1y) to assess whether conditions are improving or deteriorating
           - Historical context: current FCI position relative to historical range, percentiles, and past cycles
           - Component analysis: which financial indicators (equity, credit spreads, rates, housing, dollar) are driving FCI changes
           - FCI as a leading indicator: how current FCI levels and trends relate to economic expansion/contraction phases
           - Comparison of FCI trajectory with other economic indicators for consistency
        3. Key Economic Indicators Analysis:
           - GDP growth trends and outlook
           - Inflation levels and trajectory (CPI, PCE, core inflation)
           - Employment metrics (unemployment rate, job growth, labor force participation)
           - Interest rates and monetary policy stance
           - Consumer sentiment and spending indicators
           - Housing market indicators
           - Manufacturing and services PMI
        4. Commodity Market Analysis:
           - Energy commodity trends (oil, gas, coal) and implications for economic activity
           - Industrial/input commodity prices (metals, materials) and manufacturing signals
           - Agricultural commodity prices and food inflation pressures
           - Commodity price trends as leading indicators of economic activity
           - Supply chain and cost pressure signals from commodity markets
        5. Leading Indicators Assessment:
           - Yield curve analysis (normal, flat, inverted) - use yield_curve_data to assess curve shape and spreads (10Y-2Y, 10Y-3M). Inverted curves signal recession risk.
           - Credit spreads and financial conditions (integrate with FCI analysis)
           - Business confidence indicators
           - Leading economic index components
           - Commodity price momentum as economic signals
           - Housing market trends from housing_data - inventory levels, affordability, mortgage rates
           - Economic momentum from economic_trends - month-over-month changes in key indicators
        6. Economic Cycle Phase Characteristics:
           - Current phase identification with supporting evidence (use FCI to confirm expansion/contraction)
           - Phase duration and typical progression patterns
           - Key indicators suggesting phase transitions (FCI trends are particularly important here)
           - Commodity cycle alignment with economic cycle
           - FCI cycle alignment with economic cycle
        7. Risk Factors (skeptical: emphasize risks, neutral: balanced, bullish: acknowledge but minimize):
           - Economic imbalances or vulnerabilities
           - Potential inflection points (watch for FCI turning points)
           - External risks (geopolitical, trade, etc.)
           - Commodity price shocks and their economic impact
           - Financial conditions tightening or loosening (from FCI analysis)
        
        Personality Guidelines:
        - SKEPTICAL/BEARISH: Emphasize downside risks, vulnerabilities, potential recessions, negative indicators. Be cautious and highlight what could go wrong. Focus on defensive positioning. If FCI is declining or negative, emphasize contractionary risks.
        - NEUTRAL: Provide balanced, objective analysis weighing both positive and negative factors equally. Avoid extreme positions. Present FCI data objectively.
        - BULLISH/HOPEFUL: Emphasize opportunities, positive trends, resilience, and upside potential. Highlight strengths and growth prospects. Focus on expansionary positioning. If FCI is positive or improving, emphasize expansionary conditions.
        
        Focus on quantitative metrics, trends, and leading indicators including commodity markets and the Financial Conditions Index to provide a clear assessment of the current economic state from the specified perspective."""
    )


class EconomyStateModule(dspy.Module):
    """DSPy module for analyzing current economy state."""

    def __init__(self, personality: str = "skeptical"):
        super().__init__()
        self.personality = personality
        self.analyze_state = dspy.ChainOfThought(EconomyStateAnalysisSignature)

    def forward(
        self,
        economic_data: str,
        commodity_data: str,
        financial_conditions_index: str,
        housing_data: str = "",
        yield_curve_data: str = "",
        economic_trends: str = "",
        personality: str = None,
    ):
        personality_to_use = personality or self.personality
        return self.analyze_state(
            economic_data=economic_data,
            commodity_data=commodity_data,
            financial_conditions_index=financial_conditions_index,
            housing_data=housing_data or "No housing data available",
            yield_curve_data=yield_curve_data or "No yield curve data available",
            economic_trends=economic_trends or "No economic trends data available",
            personality=personality_to_use,
        )


class EconomicAnalysisResource(dg.ConfigurableResource):
    """Unified resource for economic analysis that consolidates useful parts from existing analyzers."""

    provider: str = Field(
        default="gemini",
        description="LLM provider: 'openai', 'gemini', or 'anthropic'. Can be set via LLM_PROVIDER env var.",
    )
    model_name: str = Field(
        default="gemini-3-pro-preview",
        description="LLM model name (e.g., 'gpt-4-turbo-preview', 'gemini-2.0-flash-exp', 'gemini-3-pro-preview', 'claude-3-opus-20240229')",
    )
    openai_api_key: Optional[str] = Field(
        default=None,
        description="OpenAI API key (required if provider='openai'). Can be set via OPENAI_API_KEY env var.",
    )
    gemini_api_key: Optional[str] = Field(
        default=None,
        description="Google Gemini API key (required if provider='gemini'). Can be set via GEMINI_API_KEY env var.",
    )
    anthropic_api_key: Optional[str] = Field(
        default=None,
        description="Anthropic API key (required if provider='anthropic'). Can be set via ANTHROPIC_API_KEY env var.",
    )
    use_optimized_models: bool = Field(
        default=True,
        description="Whether to use optimized models from GCS if available",
    )

    def setup_for_execution(
        self,
        context,
        provider_override: Optional[str] = None,
        model_name_override: Optional[str] = None,
    ) -> None:
        """Initialize DSPy when the resource is used.

        Args:
            context: Dagster execution context
            provider_override: Optional provider override (e.g., 'openai', 'anthropic', 'gemini')
            model_name_override: Optional model name override (e.g., 'gpt-4-turbo-preview', 'claude-3-5-haiku-20241022')
        """
        # Resolve provider: use override if provided, otherwise resolve from resource field
        if provider_override:
            provider = provider_override
        else:
            provider = object.__getattribute__(self, "provider")
            if isinstance(provider, dg.EnvVar):
                provider = provider.value or "gemini"
            elif not provider:
                provider = os.getenv("LLM_PROVIDER", "gemini")

        # Resolve model_name: use override if provided, otherwise resolve from resource field
        if model_name_override:
            model_name_val = model_name_override
        else:
            model_name_val = self.model_name
            if isinstance(model_name_val, dg.EnvVar):
                model_name_val = model_name_val.value or "gemini-3-pro-preview"
            elif not model_name_val:
                model_name_val = os.getenv("MODEL_NAME", "gemini-3-pro-preview")

        openai_key = self.openai_api_key
        if isinstance(openai_key, dg.EnvVar):
            openai_key = openai_key.value
        elif not openai_key:
            openai_key = os.getenv("OPENAI_API_KEY")

        gemini_key = self.gemini_api_key
        if isinstance(gemini_key, dg.EnvVar):
            gemini_key = gemini_key.value
        elif not gemini_key:
            gemini_key = os.getenv("GEMINI_API_KEY")

        anthropic_key = self.anthropic_api_key
        if isinstance(anthropic_key, dg.EnvVar):
            anthropic_key = anthropic_key.value
        elif not anthropic_key:
            anthropic_key = os.getenv("ANTHROPIC_API_KEY")

        api_key = None
        if provider == "openai":
            api_key = openai_key
            if not api_key:
                raise ValueError("openai_api_key is required when provider='openai'")
            model_str = f"openai/{model_name_val}"
        elif provider == "gemini":
            api_key = gemini_key
            if not api_key:
                raise ValueError("gemini_api_key is required when provider='gemini'")
            model_str = f"gemini/{model_name_val}"
        elif provider == "anthropic":
            api_key = anthropic_key
            if not api_key:
                raise ValueError(
                    "anthropic_api_key is required when provider='anthropic'"
                )
            model_str = f"anthropic/{model_name_val}"
        else:
            raise ValueError(
                f"Unknown provider: {provider}. Must be 'openai', 'gemini', or 'anthropic'"
            )

        self._provider = provider
        self._model_name = model_name_val

        is_gpt5_model = "gpt-5" in model_name_val.lower()
        if is_gpt5_model:
            context.log.info(
                f"Detected gpt-5 model ({model_name_val}). "
                f"Setting litellm.drop_params = True to handle unsupported temperature parameter."
            )
            import litellm

            litellm.drop_params = True

        try:
            if is_gpt5_model:
                lm = dspy.LM(model=model_str, api_key=api_key, max_tokens=8000)
                context.log.info(
                    "Configured LM for gpt-5 model with max_tokens=8000 to prevent truncation"
                )
            else:
                lm = dspy.LM(model=model_str, api_key=api_key)
            dspy.settings.configure(lm=lm)
            self._lm = lm
            if is_gpt5_model:
                context.log.info(
                    "Successfully configured LM for gpt-5 model with drop_params=True and max_tokens=8000"
                )
        except Exception as e:
            error_msg = str(e)
            if "temperature=0.0" in error_msg and "gpt-5" in error_msg.lower():
                context.log.warning(
                    "gpt-5 models don't support temperature=0.0. "
                    "Setting litellm.drop_params = True to handle unsupported parameters."
                )
                try:
                    import litellm

                    litellm.drop_params = True
                    if is_gpt5_model:
                        lm = dspy.LM(model=model_str, api_key=api_key, max_tokens=8000)
                        context.log.info(
                            "Configured LM for gpt-5 model with max_tokens=8000 in fallback"
                        )
                    else:
                        lm = dspy.LM(model=model_str, api_key=api_key)
                    dspy.settings.configure(lm=lm)
                    self._lm = lm
                    context.log.info("Successfully configured LM with drop_params=True")
                except Exception as retry_error:
                    context.log.error(
                        f"Failed to configure LM even with drop_params: {retry_error}"
                    )
                    raise ValueError(
                        f"Could not configure LLM for model {model_name_val}. "
                        f"gpt-5 models require temperature=1.0. Original error: {error_msg}"
                    ) from retry_error
            elif (
                "response_format" in error_msg.lower()
                or "structured output" in error_msg.lower()
            ):
                context.log.warning(
                    f"Model {model_name_val} may not support structured output format. "
                    f"Attempting to configure with fallback settings."
                )
                try:
                    import litellm

                    litellm.drop_params = True
                    lm = dspy.LM(model=model_str, api_key=api_key)
                    dspy.settings.configure(lm=lm)
                    self._lm = lm
                    context.log.info(
                        "Successfully configured LM with fallback settings"
                    )
                except Exception as retry_error:
                    context.log.error(
                        f"Failed to configure LM with fallback settings: {retry_error}"
                    )
                    raise ValueError(
                        f"Could not configure LLM for model {model_name_val}. "
                        f"Model may not support required features. Original error: {error_msg}"
                    ) from retry_error
            else:
                raise

        self._economy_state_analyzer = EconomyStateModule()
        self._optimized_modules_cache = {}

    def _get_provider(self) -> str:
        """Get the current LLM provider (resolved value, not the Field)."""
        return getattr(self, "_provider", getattr(self, "provider", "openai"))

    def _get_model_name(self) -> str:
        """Get the current model name (resolved value, not the Field)."""
        return getattr(
            self, "_model_name", getattr(self, "model_name", "gpt-4-turbo-preview")
        )

    @property
    def economy_state_analyzer(self):
        """Get economy state analyzer."""
        return self._economy_state_analyzer

    def load_optimized_module(
        self,
        module_name: str,
        md_resource: Optional["MotherDuckResource"] = None,
        gcs_resource: Optional[Any] = None,
        context: Optional[dg.AssetExecutionContext] = None,
        personality: Optional[str] = None,
    ) -> Optional[dspy.Module]:
        """
        Load optimized module from GCS if available.

        Args:
            module_name: Name of module to load ('economy_state', 'asset_class_relationship', 'investment_recommendations')
            md_resource: MotherDuck resource for querying model versions
            gcs_resource: GCS resource for downloading models
            context: Optional Dagster context for logging
            personality: Optional personality to filter by. If None, uses the best model regardless of personality.

        Returns:
            Optimized module if available, None otherwise
        """
        if not self.use_optimized_models or not md_resource or not gcs_resource:
            return None

        cache_key = (
            f"{module_name}_{personality}_optimized"
            if personality
            else f"{module_name}_optimized"
        )
        if cache_key in self._optimized_modules_cache:
            cached_module = self._optimized_modules_cache[cache_key]
            if personality and hasattr(cached_module, "personality"):
                if cached_module.personality != personality:
                    del self._optimized_modules_cache[cache_key]
                else:
                    return cached_module
            else:
                return cached_module

        log = context.log if context else None

        try:
            if personality:
                query = f"""
                SELECT version, gcs_path, metadata, personality
                FROM dspy_model_versions
                WHERE module_name = '{module_name}'
                    AND personality = '{personality}'
                    AND is_production = TRUE
                ORDER BY optimized_accuracy DESC, optimization_date DESC
                LIMIT 1
                """
            else:
                query = f"""
                SELECT version, gcs_path, metadata, personality
                FROM dspy_model_versions
                WHERE module_name = '{module_name}'
                    AND is_production = TRUE
                ORDER BY optimized_accuracy DESC, optimization_date DESC
                LIMIT 1
                """
            df = md_resource.execute_query(query, read_only=True)

            if df.is_empty():
                if log:
                    log.info(
                        f"No production optimized model found for {module_name}, using baseline"
                    )
                return None

            row = df.iter_rows(named=True).__next__()
            version = row["version"]
            model_personality = row.get("personality", personality)

            model_data = gcs_resource.download_model(
                module_name=module_name,
                version=version,
                context=context,
            )

            module = None
            module_state_b64 = model_data.get("module_state")

            if module_state_b64:
                try:
                    import base64
                    import io

                    module_bytes = base64.b64decode(module_state_b64.encode("utf-8"))

                    serialization_method = model_data.get(
                        "serialization_method", "pickle"
                    )
                    if serialization_method == "dspy.save" and hasattr(dspy, "load"):
                        buffer = io.BytesIO(module_bytes)
                        module = dspy.load(buffer)
                    else:
                        import pickle

                        module = pickle.loads(module_bytes)

                    if log:
                        log.info(
                            f"Successfully deserialized optimized {module_name} module"
                        )
                except Exception as e:
                    if log:
                        log.warning(
                            f"Could not deserialize module state: {e}, falling back to baseline"
                        )
                    module = None

            if module is None:
                if module_name == "economy_state":
                    module = EconomyStateModule()
                    if model_data.get("instructions") and hasattr(
                        module.analyze_state, "signature"
                    ):
                        module.analyze_state.signature.instructions = model_data[
                            "instructions"
                        ]
                elif module_name == "asset_class_relationship":
                    from macro_agents.defs.agents.asset_class_relationship_analyzer import (
                        AssetClassRelationshipModule,
                    )

                    module = AssetClassRelationshipModule()
                    if model_data.get("instructions") and hasattr(
                        module.analyze_relationships, "signature"
                    ):
                        module.analyze_relationships.signature.instructions = (
                            model_data["instructions"]
                        )
                elif module_name == "investment_recommendations":
                    from macro_agents.defs.agents.investment_recommendations import (
                        InvestmentRecommendationsModule,
                    )

                    module_personality = (
                        model_data.get("personality")
                        or model_personality
                        or "skeptical"
                    )
                    module = InvestmentRecommendationsModule(
                        personality=module_personality
                    )
                    if model_data.get("instructions") and hasattr(
                        module.generate_recommendations, "signature"
                    ):
                        module.generate_recommendations.signature.instructions = (
                            model_data["instructions"]
                        )
                else:
                    if log:
                        log.warning(f"Unknown module name: {module_name}")
                    return None

            self._optimized_modules_cache[cache_key] = module

            if log:
                log.info(
                    f"Loaded optimized {module_name} v{version} from GCS for production use"
                )

            return module

        except Exception as e:
            if log:
                log.warning(
                    f"Error loading optimized {module_name} module: {e}, using baseline"
                )
            return None

    def get_economic_data(
        self,
        md_resource: MotherDuckResource,
        cutoff_date: Optional[str] = None,
        max_series: Optional[int] = None,
        latest_month_only: bool = False,
        max_months_per_series: Optional[int] = 3,
    ) -> str:
        """Get latest economic data from FRED series.

        Args:
            md_resource: MotherDuck resource
            cutoff_date: Optional date string (YYYY-MM-DD) for backtesting.
                        If provided, uses snapshot tables.
            max_series: Optional limit on number of series to return (reduces token usage).
            latest_month_only: If True, only return data for the most recent month per series.
                              If False, returns recent months to show trends (controlled by max_months_per_series).
            max_months_per_series: Maximum months to return per series when latest_month_only=False (reduces token usage).
        """
        if cutoff_date:
            if latest_month_only:
                month_filter = f"AND month = (SELECT MAX(month) FROM fred_series_latest_aggregates_snapshot WHERE snapshot_date = '{cutoff_date}')"
                limit_clause = f"LIMIT {max_series}" if max_series else ""
            else:
                month_filter = f"AND month >= (SELECT MAX(month) - INTERVAL '{max_months_per_series - 1} months' FROM fred_series_latest_aggregates_snapshot WHERE snapshot_date = '{cutoff_date}')"
                limit_clause = (
                    f"LIMIT {max_series * max_months_per_series}" if max_series else ""
                )
            query = f"""
            SELECT 
                series_code,
                series_name,
                month,
                current_value,
                pct_change_3m,
                pct_change_6m,
                pct_change_1y,
                date_grain
            FROM fred_series_latest_aggregates_snapshot
            WHERE snapshot_date = '{cutoff_date}'
                AND current_value IS NOT NULL
                {month_filter}
            ORDER BY series_name, month DESC
            {limit_clause}
            """
        else:
            if latest_month_only:
                month_filter = (
                    "AND month = (SELECT MAX(month) FROM fred_series_latest_aggregates)"
                )
                limit_clause = f"LIMIT {max_series}" if max_series else ""
            else:
                month_filter = f"AND month >= (SELECT MAX(month) - INTERVAL '{max_months_per_series - 1} months' FROM fred_series_latest_aggregates)"
                limit_clause = (
                    f"LIMIT {max_series * max_months_per_series}" if max_series else ""
                )
            query = f"""
            SELECT 
                series_code,
                series_name,
                month,
                current_value,
                pct_change_3m,
                pct_change_6m,
                pct_change_1y,
                date_grain
            FROM fred_series_latest_aggregates
            WHERE current_value IS NOT NULL
                {month_filter}
            ORDER BY series_name, month DESC
            {limit_clause}
            """

        df = md_resource.execute_query(query)
        csv_buffer = io.BytesIO()
        df.write_csv(csv_buffer)
        return csv_buffer.getvalue().decode("utf-8")

    def get_market_data(
        self,
        md_resource: MotherDuckResource,
        cutoff_date: Optional[str] = None,
        max_assets: Optional[int] = 20,
        time_periods: Optional[list] = None,
    ) -> str:
        """Get latest market performance data including US sectors and major indices.

        Args:
            md_resource: MotherDuck resource
            cutoff_date: Optional date string (YYYY-MM-DD) for backtesting.
                        If provided, uses snapshot tables.
            max_assets: Maximum number of assets to return per category (reduces token usage).
            time_periods: List of time periods to include. Defaults to ['6_months'] to reduce data.
        """
        if time_periods is None:
            time_periods = ["6_months"]

        periods_str = "', '".join(time_periods)
        limit_clause = f"LIMIT {max_assets}" if max_assets else ""

        if cutoff_date:
            query = f"""
            SELECT 
                symbol,
                asset_type,
                time_period,
                exchange,
                name,
                period_start_date,
                period_end_date,
                trading_days,
                total_return_pct,
                avg_daily_return_pct,
                volatility_pct,
                win_rate_pct,
                total_price_change,
                avg_daily_price_change,
                worst_day_change,
                best_day_change,
                positive_days,
                negative_days,
                neutral_days,
                period_start_price,
                period_end_price,
                'sector' as market_category
            FROM us_sector_summary_snapshot
            WHERE snapshot_date = '{cutoff_date}'
                AND time_period IN ('{periods_str}')
            
            UNION ALL
            
            SELECT 
                symbol,
                asset_type,
                time_period,
                exchange,
                name,
                period_start_date,
                period_end_date,
                trading_days,
                total_return_pct,
                avg_daily_return_pct,
                volatility_pct,
                win_rate_pct,
                total_price_change,
                avg_daily_price_change,
                worst_day_change,
                best_day_change,
                positive_days,
                negative_days,
                neutral_days,
                period_start_price,
                period_end_price,
                'major_index' as market_category
            FROM major_indicies_summary
            WHERE time_period IN ('{periods_str}')
                AND period_end_date <= '{cutoff_date}'
            
            ORDER BY market_category, asset_type, time_period, total_return_pct DESC
            {limit_clause}
            """
        else:
            query = f"""
            SELECT 
                symbol,
                asset_type,
                time_period,
                exchange,
                name,
                period_start_date,
                period_end_date,
                trading_days,
                total_return_pct,
                avg_daily_return_pct,
                volatility_pct,
                win_rate_pct,
                total_price_change,
                avg_daily_price_change,
                worst_day_change,
                best_day_change,
                positive_days,
                negative_days,
                neutral_days,
                period_start_price,
                period_end_price,
                'sector' as market_category
            FROM us_sector_summary
            WHERE time_period IN ('{periods_str}')
            
            UNION ALL
            
            SELECT 
                symbol,
                asset_type,
                time_period,
                exchange,
                name,
                period_start_date,
                period_end_date,
                trading_days,
                total_return_pct,
                avg_daily_return_pct,
                volatility_pct,
                win_rate_pct,
                total_price_change,
                avg_daily_price_change,
                worst_day_change,
                best_day_change,
                positive_days,
                negative_days,
                neutral_days,
                period_start_price,
                period_end_price,
                'major_index' as market_category
            FROM major_indicies_summary
            WHERE time_period IN ('{periods_str}')
            
            ORDER BY market_category, asset_type, time_period, total_return_pct DESC
            {limit_clause}
            """

        df = md_resource.execute_query(query)
        csv_buffer = io.BytesIO()
        df.write_csv(csv_buffer)
        return csv_buffer.getvalue().decode("utf-8")

    def get_financial_conditions_index(
        self,
        md_resource: MotherDuckResource,
        cutoff_date: Optional[str] = None,
        max_months: Optional[int] = 12,
    ) -> str:
        """Get Financial Conditions Index data.

        Args:
            md_resource: MotherDuck resource
            cutoff_date: Optional date string (YYYY-MM-DD) for backtesting.
                        If provided, uses snapshot tables.
            max_months: Maximum number of months to return (reduces token usage). Defaults to 12.
        """
        limit_clause = f"LIMIT {max_months}" if max_months else ""
        if cutoff_date:
            query = f"""
            SELECT 
                date,
                FCI,
                equity_score,
                housing_score,
                "10yr_score" as treasury_10yr_score
            FROM financial_conditions_index
            WHERE date <= '{cutoff_date}'
                AND FCI IS NOT NULL
            ORDER BY date DESC
            {limit_clause}
            """
        else:
            query = f"""
            SELECT 
                date,
                FCI,
                equity_score,
                housing_score,
                "10yr_score" as treasury_10yr_score
            FROM financial_conditions_index
            WHERE FCI IS NOT NULL
            ORDER BY date DESC
            {limit_clause}
            """

        df = md_resource.execute_query(query, read_only=True)
        csv_buffer = io.BytesIO()
        df.write_csv(csv_buffer)
        return csv_buffer.getvalue().decode("utf-8")

    def get_commodity_data(
        self,
        md_resource: MotherDuckResource,
        cutoff_date: Optional[str] = None,
        max_commodities: Optional[int] = 15,
        time_periods: Optional[list] = None,
    ) -> str:
        """Get latest commodity performance data from all commodity summary tables.

        Args:
            md_resource: MotherDuck resource
            cutoff_date: Optional date string (YYYY-MM-DD) for backtesting.
                        If provided, uses snapshot tables.
            max_commodities: Maximum number of commodities per category (reduces token usage).
            time_periods: List of time periods to include. Defaults to ['6_months'] to reduce data.
        """
        if time_periods is None:
            time_periods = ["6_months"]

        periods_str = "', '".join(time_periods)
        limit_clause = f"LIMIT {max_commodities}" if max_commodities else ""

        if cutoff_date:
            query = f"""
            SELECT 
                commodity_name,
                commodity_unit,
                'energy' as commodity_category,
                time_period,
                period_start_date,
                period_end_date,
                trading_days,
                total_return_pct,
                avg_daily_return_pct,
                volatility_pct,
                win_rate_pct,
                total_price_change,
                avg_daily_price_change,
                worst_day_change,
                best_day_change,
                positive_days,
                negative_days,
                neutral_days,
                period_start_price,
                period_end_price
            FROM energy_commodities_summary_snapshot
            WHERE snapshot_date = '{cutoff_date}'
                AND time_period IN ('{periods_str}')
            
            UNION ALL
            
            SELECT 
                commodity_name,
                commodity_unit,
                'input' as commodity_category,
                time_period,
                period_start_date,
                period_end_date,
                trading_days,
                total_return_pct,
                avg_daily_return_pct,
                volatility_pct,
                win_rate_pct,
                total_price_change,
                avg_daily_price_change,
                worst_day_change,
                best_day_change,
                positive_days,
                negative_days,
                neutral_days,
                period_start_price,
                period_end_price
            FROM input_commodities_summary_snapshot
            WHERE snapshot_date = '{cutoff_date}'
                AND time_period IN ('{periods_str}')
            
            UNION ALL
            
            SELECT 
                commodity_name,
                commodity_unit,
                'agriculture' as commodity_category,
                time_period,
                period_start_date,
                period_end_date,
                trading_days,
                total_return_pct,
                avg_daily_return_pct,
                volatility_pct,
                win_rate_pct,
                total_price_change,
                avg_daily_price_change,
                worst_day_change,
                best_day_change,
                positive_days,
                negative_days,
                neutral_days,
                period_start_price,
                period_end_price
            FROM agriculture_commodities_summary_snapshot
            WHERE snapshot_date = '{cutoff_date}'
                AND time_period IN ('{periods_str}')
            
            ORDER BY commodity_category, commodity_name, time_period, total_return_pct DESC
            {limit_clause}
            """
        else:
            query = f"""
            SELECT 
                commodity_name,
                commodity_unit,
                'energy' as commodity_category,
                time_period,
                period_start_date,
                period_end_date,
                trading_days,
                total_return_pct,
                avg_daily_return_pct,
                volatility_pct,
                win_rate_pct,
                total_price_change,
                avg_daily_price_change,
                worst_day_change,
                best_day_change,
                positive_days,
                negative_days,
                neutral_days,
                period_start_price,
                period_end_price
            FROM energy_commodities_summary
            WHERE time_period IN ('{periods_str}')
            
            UNION ALL
            
            SELECT 
                commodity_name,
                commodity_unit,
                'input' as commodity_category,
                time_period,
                period_start_date,
                period_end_date,
                trading_days,
                total_return_pct,
                avg_daily_return_pct,
                volatility_pct,
                win_rate_pct,
                total_price_change,
                avg_daily_price_change,
                worst_day_change,
                best_day_change,
                positive_days,
                negative_days,
                neutral_days,
                period_start_price,
                period_end_price
            FROM input_commodities_summary
            WHERE time_period IN ('{periods_str}')
            
            UNION ALL
            
            SELECT 
                commodity_name,
                commodity_unit,
                'agriculture' as commodity_category,
                time_period,
                period_start_date,
                period_end_date,
                trading_days,
                total_return_pct,
                avg_daily_return_pct,
                volatility_pct,
                win_rate_pct,
                total_price_change,
                avg_daily_price_change,
                worst_day_change,
                best_day_change,
                positive_days,
                negative_days,
                neutral_days,
                period_start_price,
                period_end_price
            FROM agriculture_commodities_summary
            WHERE time_period IN ('{periods_str}')
            
            ORDER BY commodity_category, commodity_name, time_period, total_return_pct DESC
            {limit_clause}
            """

        df = md_resource.execute_query(query)
        csv_buffer = io.BytesIO()
        df.write_csv(csv_buffer)
        return csv_buffer.getvalue().decode("utf-8")

    def get_correlation_data(
        self,
        md_resource: MotherDuckResource,
        sample_size: int = 50,
        sampling_strategy: str = "top_correlations",
        cutoff_date: Optional[str] = None,
    ) -> str:
        """Get correlation data between economic indicators and asset returns.

        Args:
            md_resource: MotherDuck resource
            sample_size: Number of samples to return (reduced default to save tokens).
            sampling_strategy: Strategy for sampling
            cutoff_date: Optional date string (YYYY-MM-DD) for backtesting.
                        If provided, uses snapshot tables.
        """
        snapshot_table = "leading_econ_return_indicator_snapshot"

        if cutoff_date:
            if md_resource.table_exists(snapshot_table):
                table_name = snapshot_table
            else:
                table_name = "leading_econ_return_indicator"
        else:
            table_name = "leading_econ_return_indicator"

        if cutoff_date:
            if hasattr(md_resource, "query_sampled_data"):
                try:
                    correlation_data = md_resource.query_sampled_data(
                        table_name=table_name,
                        filters={"snapshot_date": cutoff_date}
                        if table_name == snapshot_table
                        else {},
                        sample_size=sample_size,
                        sampling_strategy=sampling_strategy,
                    )
                    return correlation_data
                except Exception:
                    return ""
            else:
                try:
                    if table_name == snapshot_table:
                        query = f"""
                        SELECT *
                        FROM {table_name}
                        WHERE snapshot_date = '{cutoff_date}'
                        ORDER BY GREATEST(
                            ABS(COALESCE(correlation_econ_vs_q1_returns, 0)),
                            ABS(COALESCE(correlation_econ_vs_q2_returns, 0)),
                            ABS(COALESCE(correlation_econ_vs_q3_returns, 0))
                        ) DESC
                        LIMIT {sample_size}
                        """
                    else:
                        query = f"""
                        SELECT *
                        FROM {table_name}
                        ORDER BY GREATEST(
                            ABS(COALESCE(correlation_econ_vs_q1_returns, 0)),
                            ABS(COALESCE(correlation_econ_vs_q2_returns, 0)),
                            ABS(COALESCE(correlation_econ_vs_q3_returns, 0))
                        ) DESC
                        LIMIT {sample_size}
                        """
                    df = md_resource.execute_query(query)
                    csv_buffer = io.StringIO()
                    df.write_csv(csv_buffer)
                    return csv_buffer.getvalue()
                except Exception:
                    return ""
        else:
            if hasattr(md_resource, "query_sampled_data"):
                correlation_data = md_resource.query_sampled_data(
                    table_name=table_name,
                    filters={},
                    sample_size=sample_size,
                    sampling_strategy=sampling_strategy,
                )
                return correlation_data
            else:
                query = f"""
                SELECT *
                FROM {table_name}
                ORDER BY GREATEST(
                    ABS(COALESCE(correlation_econ_vs_q1_returns, 0)),
                    ABS(COALESCE(correlation_econ_vs_q2_returns, 0)),
                    ABS(COALESCE(correlation_econ_vs_q3_returns, 0))
                ) DESC
                LIMIT {sample_size}
                """
                df = md_resource.execute_query(query)
                csv_buffer = io.StringIO()
                df.write_csv(csv_buffer)
                return csv_buffer.getvalue()

    def get_housing_data(
        self,
        md_resource: MotherDuckResource,
        cutoff_date: Optional[str] = None,
        latest_month_only: bool = False,
        max_months: Optional[int] = 6,
    ) -> str:
        """Get housing market data including inventory and mortgage rates.

        Args:
            md_resource: MotherDuck resource
            cutoff_date: Optional date string (YYYY-MM-DD) for backtesting.
                        If provided, filters data up to that date.
            latest_month_only: If True, only return data for the most recent month (reduces token usage).
                              If False, returns recent months to show trends (controlled by max_months).
            max_months: Maximum months to return when latest_month_only=False (reduces token usage).
        """
        if cutoff_date:
            if latest_month_only:
                month_filter = f"AND month = (SELECT MAX(month) FROM housing_inventory_latest_aggregates WHERE month <= '{cutoff_date}')"
            else:
                month_filter = f"AND month >= (SELECT MAX(month) - INTERVAL '{max_months - 1} months' FROM housing_inventory_latest_aggregates WHERE month <= '{cutoff_date}')"
        else:
            if latest_month_only:
                month_filter = "AND month = (SELECT MAX(month) FROM housing_inventory_latest_aggregates)"
            else:
                month_filter = f"AND month >= (SELECT MAX(month) - INTERVAL '{max_months - 1} months' FROM housing_inventory_latest_aggregates)"

        inventory_query = f"""
        SELECT 
            series_code,
            series_name,
            month,
            current_value,
            pct_change_3m,
            pct_change_6m,
            pct_change_1y,
            date_grain
        FROM housing_inventory_latest_aggregates
        WHERE current_value IS NOT NULL
            {month_filter}
        ORDER BY series_name, month DESC
        """

        # Get mortgage rates data (latest available)
        mortgage_query = """
        SELECT 
            date,
            mortgage_rate,
            median_price_no_down_payment,
            median_price_20_pct_down_payment,
            monthly_payment_no_down_payment,
            monthly_payment_20_pct_down_payment
        FROM housing_mortgage_rates
        WHERE date = (SELECT MAX(date) FROM housing_mortgage_rates)
        """

        if cutoff_date:
            inventory_query = f"""
            SELECT 
                series_code,
                series_name,
                month,
                current_value,
                pct_change_3m,
                pct_change_6m,
                pct_change_1y,
                date_grain
            FROM housing_inventory_latest_aggregates
            WHERE month <= '{cutoff_date}'
                AND current_value IS NOT NULL
            ORDER BY series_name, month DESC
            """
            mortgage_query = f"""
            SELECT 
                date,
                mortgage_rate,
                median_price_no_down_payment,
                median_price_20_pct_down_payment,
                monthly_payment_no_down_payment,
                monthly_payment_20_pct_down_payment
            FROM housing_mortgage_rates
            WHERE date <= '{cutoff_date}'
                AND date = (
                    SELECT MAX(date) 
                    FROM housing_mortgage_rates 
                    WHERE date <= '{cutoff_date}'
                )
            """

        inventory_df = md_resource.execute_query(inventory_query, read_only=True)
        mortgage_df = md_resource.execute_query(mortgage_query, read_only=True)

        # Combine into single CSV
        inventory_csv = ""
        if not inventory_df.is_empty():
            csv_buffer = io.BytesIO()
            inventory_df.write_csv(csv_buffer)
            inventory_csv = csv_buffer.getvalue().decode("utf-8")

        mortgage_csv = ""
        if not mortgage_df.is_empty():
            csv_buffer = io.BytesIO()
            mortgage_df.write_csv(csv_buffer)
            mortgage_csv = csv_buffer.getvalue().decode("utf-8")

        # Combine with separator
        if inventory_csv and mortgage_csv:
            return f"Housing Inventory Data:\n{inventory_csv}\n\nMortgage Rates Data:\n{mortgage_csv}"
        elif inventory_csv:
            return f"Housing Inventory Data:\n{inventory_csv}"
        elif mortgage_csv:
            return f"Mortgage Rates Data:\n{mortgage_csv}"
        else:
            return ""

    def get_yield_curve_data(
        self,
        md_resource: MotherDuckResource,
        cutoff_date: Optional[str] = None,
        max_months: Optional[int] = 12,
    ) -> str:
        """Get yield curve data and calculate spreads.

        Args:
            md_resource: MotherDuck resource
            cutoff_date: Optional date string (YYYY-MM-DD) for backtesting.
                        If provided, filters data up to that date.
            max_months: Maximum number of months to return (reduces token usage). Defaults to 12.
        """
        date_filter = f"AND date <= '{cutoff_date}'" if cutoff_date else ""
        limit_clause = f"LIMIT {max_months}" if max_months else ""

        query = f"""
        WITH pivoted_yields AS (
            SELECT 
                date,
                MAX(CASE WHEN yield_type = 'BC_1MONTH' THEN value END) as yield_1m,
                MAX(CASE WHEN yield_type = 'BC_3MONTH' THEN value END) as yield_3m,
                MAX(CASE WHEN yield_type = 'BC_6MONTH' THEN value END) as yield_6m,
                MAX(CASE WHEN yield_type = 'BC_1YEAR' THEN value END) as yield_1y,
                MAX(CASE WHEN yield_type = 'BC_2YEAR' THEN value END) as yield_2y,
                MAX(CASE WHEN yield_type = 'BC_3YEAR' THEN value END) as yield_3y,
                MAX(CASE WHEN yield_type = 'BC_5YEAR' THEN value END) as yield_5y,
                MAX(CASE WHEN yield_type = 'BC_7YEAR' THEN value END) as yield_7y,
                MAX(CASE WHEN yield_type = 'BC_10YEAR' THEN value END) as yield_10y,
                MAX(CASE WHEN yield_type = 'BC_20YEAR' THEN value END) as yield_20y,
                MAX(CASE WHEN yield_type = 'BC_30YEAR' THEN value END) as yield_30y
            FROM stg_treasury_yields
            WHERE date IS NOT NULL {date_filter}
            GROUP BY date
        ),
        latest_date AS (
            SELECT MAX(date) as max_date
            FROM pivoted_yields
        ),
        current_yields AS (
            SELECT *
            FROM pivoted_yields
            WHERE date = (SELECT max_date FROM latest_date)
        ),
        yield_spreads AS (
            SELECT
                date,
                yield_10y,
                yield_2y,
                yield_3m,
                yield_1y,
                yield_30y,
                -- Calculate key spreads
                yield_10y - yield_2y as spread_10y_2y,
                yield_10y - yield_3m as spread_10y_3m,
                yield_2y - yield_3m as spread_2y_3m,
                yield_30y - yield_2y as spread_30y_2y,
                -- Yield curve shape classification
                CASE
                    WHEN yield_10y - yield_2y > 0.5 THEN 'Steep'
                    WHEN yield_10y - yield_2y > 0 THEN 'Normal'
                    WHEN yield_10y - yield_2y > -0.5 THEN 'Flat'
                    ELSE 'Inverted'
                END as curve_shape,
                -- Inversion status
                CASE
                    WHEN yield_10y - yield_2y < 0 THEN 'Inverted'
                    WHEN yield_10y - yield_3m < 0 THEN 'Inverted (10Y-3M)'
                    ELSE 'Normal'
                END as inversion_status
            FROM current_yields
        )
        SELECT * FROM yield_spreads
        ORDER BY date DESC
        {limit_clause}
        """

        df = md_resource.execute_query(query, read_only=True)
        if df.is_empty():
            return ""
        csv_buffer = io.BytesIO()
        df.write_csv(csv_buffer)
        return csv_buffer.getvalue().decode("utf-8")

    def get_economic_trends(
        self,
        md_resource: MotherDuckResource,
        cutoff_date: Optional[str] = None,
        max_months: Optional[int] = 12,
    ) -> str:
        """Get month-over-month changes for key economic indicators using fred_monthly_diff.

        Args:
            md_resource: MotherDuck resource
            cutoff_date: Optional date string (YYYY-MM-DD) for backtesting.
                        If provided, filters data up to that date.
            max_months: Maximum number of months to return per indicator (reduces token usage). Defaults to 12.
        """
        # Key indicators to track trends for
        key_indicators = [
            "GDPC1",  # Real GDP
            "CPIAUCSL",  # CPI
            "UNRATE",  # Unemployment Rate
            "PAYEMS",  # Nonfarm Payrolls
            "FEDFUNDS",  # Fed Funds Rate
            "INDPRO",  # Industrial Production
            "RSXFS",  # Retail Sales
            "HOUST",  # Housing Starts
        ]

        indicators_list = "', '".join(key_indicators)
        date_filter = f"AND date <= '{cutoff_date}'" if cutoff_date else ""
        limit_clause = (
            f"LIMIT {max_months * len(key_indicators)}" if max_months else "LIMIT 100"
        )

        query = f"""
        SELECT 
            series_code,
            series_name,
            date,
            value,
            period_diff,
            data_source
        FROM fred_monthly_diff
        WHERE series_code IN ('{indicators_list}')
            {date_filter}
        ORDER BY series_name, date DESC
        {limit_clause}
        """

        df = md_resource.execute_query(query, read_only=True)
        if df.is_empty():
            return ""
        csv_buffer = io.BytesIO()
        df.write_csv(csv_buffer)
        return csv_buffer.getvalue().decode("utf-8")


def _get_token_usage(
    economic_analysis: EconomicAnalysisResource,
    initial_history_length: int,
    context: dg.AssetExecutionContext,
) -> Dict[str, Any]:
    """
    Extract token usage and cost from DSPy LM history.

    Returns dictionary with prompt_tokens, completion_tokens, total_tokens,
    and cost information (if available from API, otherwise calculated).
    """
    token_usage = {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
    }

    if not hasattr(economic_analysis, "_lm"):
        context.log.warning("LM object not found, cannot track token usage")
        return token_usage

    try:
        lm = economic_analysis._lm

        if hasattr(lm, "usage") and lm.usage:
            usage = lm.usage
            if isinstance(usage, dict):
                token_usage["prompt_tokens"] = usage.get("prompt_tokens", 0)
                token_usage["completion_tokens"] = usage.get("completion_tokens", 0)
                token_usage["total_tokens"] = usage.get("total_tokens", 0)
                if "cost" in usage or "total_cost" in usage or "cost_usd" in usage:
                    token_usage["total_cost_usd"] = usage.get(
                        "cost", usage.get("total_cost", usage.get("cost_usd", 0))
                    )
                if "prompt_cost" in usage or "prompt_cost_usd" in usage:
                    token_usage["prompt_cost_usd"] = usage.get(
                        "prompt_cost", usage.get("prompt_cost_usd", 0)
                    )
                if "completion_cost" in usage or "completion_cost_usd" in usage:
                    token_usage["completion_cost_usd"] = usage.get(
                        "completion_cost", usage.get("completion_cost_usd", 0)
                    )
            elif hasattr(usage, "prompt_tokens"):
                token_usage["prompt_tokens"] = getattr(usage, "prompt_tokens", 0)
                token_usage["completion_tokens"] = getattr(
                    usage, "completion_tokens", 0
                )
                token_usage["total_tokens"] = getattr(usage, "total_tokens", 0)
                if (
                    hasattr(usage, "cost")
                    or hasattr(usage, "total_cost")
                    or hasattr(usage, "cost_usd")
                ):
                    token_usage["total_cost_usd"] = getattr(
                        usage,
                        "cost",
                        getattr(usage, "total_cost", getattr(usage, "cost_usd", 0)),
                    )
                if hasattr(usage, "prompt_cost") or hasattr(usage, "prompt_cost_usd"):
                    token_usage["prompt_cost_usd"] = getattr(
                        usage, "prompt_cost", getattr(usage, "prompt_cost_usd", 0)
                    )
                if hasattr(usage, "completion_cost") or hasattr(
                    usage, "completion_cost_usd"
                ):
                    token_usage["completion_cost_usd"] = getattr(
                        usage,
                        "completion_cost",
                        getattr(usage, "completion_cost_usd", 0),
                    )

            if token_usage["total_tokens"] > 0:
                context.log.info(f"Token usage from lm.usage: {token_usage}")
                return token_usage

        if hasattr(lm, "get_token_count"):
            try:
                actual_counts = lm.get_token_count()
                if actual_counts:
                    if isinstance(actual_counts, dict):
                        token_usage.update(actual_counts)
                    else:
                        token_usage["total_tokens"] = actual_counts
                    if token_usage["total_tokens"] > 0:
                        context.log.info(
                            f"Token usage from get_token_count: {token_usage}"
                        )
                        return token_usage
            except Exception as e:
                context.log.debug(f"get_token_count failed: {e}")

        if hasattr(lm, "history"):
            history = lm.history
            new_entries = (
                history[initial_history_length:]
                if len(history) > initial_history_length
                else []
            )

            for entry in new_entries:
                if isinstance(entry, dict):
                    if "usage" in entry:
                        usage = entry["usage"]
                        if isinstance(usage, dict):
                            token_usage["prompt_tokens"] += usage.get(
                                "prompt_tokens", 0
                            )
                            token_usage["completion_tokens"] += usage.get(
                                "completion_tokens", 0
                            )
                            token_usage["total_tokens"] += usage.get("total_tokens", 0)
                            if (
                                "cost" in usage
                                or "total_cost" in usage
                                or "cost_usd" in usage
                            ):
                                token_usage["total_cost_usd"] = token_usage.get(
                                    "total_cost_usd", 0
                                ) + usage.get(
                                    "cost",
                                    usage.get("total_cost", usage.get("cost_usd", 0)),
                                )
                            if "prompt_cost" in usage or "prompt_cost_usd" in usage:
                                token_usage["prompt_cost_usd"] = token_usage.get(
                                    "prompt_cost_usd", 0
                                ) + usage.get(
                                    "prompt_cost", usage.get("prompt_cost_usd", 0)
                                )
                            if (
                                "completion_cost" in usage
                                or "completion_cost_usd" in usage
                            ):
                                token_usage["completion_cost_usd"] = token_usage.get(
                                    "completion_cost_usd", 0
                                ) + usage.get(
                                    "completion_cost",
                                    usage.get("completion_cost_usd", 0),
                                )
                    elif "prompt_tokens" in entry:
                        token_usage["prompt_tokens"] += entry.get("prompt_tokens", 0)
                    elif "completion_tokens" in entry:
                        token_usage["completion_tokens"] += entry.get(
                            "completion_tokens", 0
                        )
                elif hasattr(entry, "prompt_tokens"):
                    token_usage["prompt_tokens"] += getattr(entry, "prompt_tokens", 0)
                if hasattr(entry, "completion_tokens"):
                    token_usage["completion_tokens"] += getattr(
                        entry, "completion_tokens", 0
                    )
                elif isinstance(entry, dict) and "response" in entry:
                    try:
                        prompt_text = str(
                            entry.get("messages", entry.get("prompt", ""))
                        )
                        response_text = str(entry.get("response", ""))
                        token_usage["prompt_tokens"] += len(prompt_text) // 4
                        token_usage["completion_tokens"] += len(response_text) // 4
                    except Exception:
                        pass

            token_usage["total_tokens"] = (
                token_usage["prompt_tokens"] + token_usage["completion_tokens"]
            )

            if token_usage["total_tokens"] > 0:
                context.log.info(f"Token usage from history: {token_usage}")
                return token_usage

        if hasattr(lm, "client") and hasattr(lm.client, "usage"):
            try:
                client_usage = lm.client.usage
                if client_usage:
                    if isinstance(client_usage, dict):
                        token_usage.update(client_usage)
                    elif hasattr(client_usage, "prompt_tokens"):
                        token_usage["prompt_tokens"] = getattr(
                            client_usage, "prompt_tokens", 0
                        )
                        token_usage["completion_tokens"] = getattr(
                            client_usage, "completion_tokens", 0
                        )
                        token_usage["total_tokens"] = getattr(
                            client_usage, "total_tokens", 0
                        )
                    if token_usage["total_tokens"] > 0:
                        context.log.info(
                            f"Token usage from client.usage: {token_usage}"
                        )
                        return token_usage
            except Exception as e:
                context.log.debug(f"client.usage access failed: {e}")

        if token_usage["total_tokens"] == 0:
            context.log.warning(
                f"Could not extract token usage. History length: {len(history) if hasattr(lm, 'history') else 'N/A'}, "
                f"Initial length: {initial_history_length}, LM type: {type(lm)}"
            )

    except Exception as e:
        context.log.warning(f"Could not extract token usage: {e}", exc_info=True)

    return token_usage


def _calculate_cost(
    provider: str,
    model_name: str,
    prompt_tokens: int,
    completion_tokens: int,
) -> Dict[str, float]:
    """
    Calculate cost based on provider, model, and token usage.

    Pricing per 1M tokens (as of 2024-2025):
    - OpenAI: https://openai.com/api/pricing/
    - Anthropic: https://www.anthropic.com/pricing
    - Gemini: https://ai.google.dev/pricing

    Returns dictionary with prompt_cost, completion_cost, and total_cost in USD.
    """
    pricing = {
        "openai": {
            "gpt-4-turbo-preview": {"prompt": 10.0, "completion": 30.0},
            "gpt-4-turbo": {"prompt": 10.0, "completion": 30.0},
            "gpt-4": {"prompt": 30.0, "completion": 60.0},
            "gpt-4o": {"prompt": 5.0, "completion": 15.0},
            "gpt-4o-mini": {"prompt": 0.15, "completion": 0.6},
            "gpt-3.5-turbo": {"prompt": 0.5, "completion": 1.5},
            "default": {"prompt": 10.0, "completion": 30.0},
        },
        "anthropic": {
            "claude-3-5-opus-20241022": {"prompt": 15.0, "completion": 75.0},
            "claude-3-5-sonnet-20241022": {"prompt": 3.0, "completion": 15.0},
            "claude-3-5-haiku-20241022": {"prompt": 1.0, "completion": 5.0},
            "claude-3-opus-20240229": {"prompt": 15.0, "completion": 75.0},
            "claude-3-sonnet-20240229": {"prompt": 3.0, "completion": 15.0},
            "claude-3-haiku-20240307": {"prompt": 0.25, "completion": 1.25},
            "default": {"prompt": 3.0, "completion": 15.0},
        },
        "gemini": {
            "gemini-2.0-flash-exp": {"prompt": 0.075, "completion": 0.3},
            "gemini-1.5-pro": {"prompt": 1.25, "completion": 5.0},
            "gemini-1.5-flash": {"prompt": 0.075, "completion": 0.3},
            "gemini-3-pro-preview": {"prompt": 1.25, "completion": 5.0},
            "default": {"prompt": 0.075, "completion": 0.3},
        },
    }

    provider_pricing = pricing.get(provider.lower(), {})
    model_pricing = provider_pricing.get(
        model_name.lower(),
        provider_pricing.get("default", {"prompt": 1.0, "completion": 2.0}),
    )

    prompt_cost_per_million = model_pricing["prompt"]
    completion_cost_per_million = model_pricing["completion"]

    prompt_cost = (prompt_tokens / 1_000_000) * prompt_cost_per_million
    completion_cost = (completion_tokens / 1_000_000) * completion_cost_per_million
    total_cost = prompt_cost + completion_cost

    return {
        "prompt_cost_usd": round(prompt_cost, 6),
        "completion_cost_usd": round(completion_cost, 6),
        "total_cost_usd": round(total_cost, 6),
    }


def extract_economy_state_summary(analysis_content: str) -> Dict[str, Any]:
    """Extract key insights from economy state analysis for metadata."""
    summary = {}

    if not analysis_content:
        return summary

    cycle_match = re.search(
        r"(?:Current Economic Cycle Position|Cycle Position|Economic Cycle):\s*([^.\n]+)",
        analysis_content,
        re.IGNORECASE,
    )
    if cycle_match:
        summary["economic_cycle_position"] = cycle_match.group(1).strip()

    confidence_match = re.search(
        r"confidence[:\s]+([0-9.]+)", analysis_content, re.IGNORECASE
    )
    if confidence_match:
        try:
            summary["confidence_level"] = float(confidence_match.group(1))
        except ValueError:
            pass

    risk_section = re.search(
        r"(?:Risk Factors|Key Risks|Risks):\s*([^0-9]+?)(?:\d+\.|$)",
        analysis_content,
        re.IGNORECASE | re.DOTALL,
    )
    if risk_section:
        risks_text = risk_section.group(1)[:500]
        summary["key_risks_summary"] = risks_text.strip()

    inflation_match = re.search(
        r"(?:Inflation|CPI|PCE).*?(?:trend|trajectory|level):\s*([^.\n]+)",
        analysis_content,
        re.IGNORECASE,
    )
    if inflation_match:
        summary["inflation_trend"] = inflation_match.group(1).strip()

    gdp_match = re.search(
        r"(?:GDP|growth).*?(?:trend|outlook|growth):\s*([^.\n]+)",
        analysis_content,
        re.IGNORECASE,
    )
    if gdp_match:
        summary["gdp_outlook"] = gdp_match.group(1).strip()

    return summary


@dg.asset(
    kinds={"dspy", "duckdb"},
    group_name="economic_analysis",
    description="Analyze current economic indicators and commodity data to determine the state of the economy",
    deps=[
        dg.AssetKey(["fred_series_latest_aggregates"]),
        dg.AssetKey(["fred_monthly_diff"]),
        dg.AssetKey(["leading_econ_return_indicator"]),
        dg.AssetKey(["us_sector_summary"]),
        dg.AssetKey(["major_indicies_summary"]),
        dg.AssetKey(["energy_commodities_summary"]),
        dg.AssetKey(["input_commodities_summary"]),
        dg.AssetKey(["agriculture_commodities_summary"]),
        dg.AssetKey(["financial_conditions_index"]),
        dg.AssetKey(["housing_inventory_latest_aggregates"]),
        dg.AssetKey(["housing_mortgage_rates"]),
        dg.AssetKey(["stg_treasury_yields"]),
        dg.AssetKey(["auto_promote_best_models_to_production"]),
    ],
)
def analyze_economy_state(
    context: dg.AssetExecutionContext,
    config: EconomicAnalysisConfig,
    md: MotherDuckResource,
    economic_analysis: EconomicAnalysisResource,
    gcs: GCSResource,
) -> dg.MaterializeResult:
    """
    Asset that analyzes the current state of the economy based on economic indicators.

    This is Step 1 of the economic analysis pipeline. It focuses solely on analyzing
    economic indicators to determine the current economic state and cycle position.

    Returns:
        Dictionary with analysis metadata and results
    """
    context.log.info(
        f"Starting economy state analysis (personality: {config.personality})..."
    )

    economic_analysis.setup_for_execution(
        context,
        provider_override=config.model_provider,
        model_name_override=config.model_name,
    )

    context.log.info(
        "Gathering economic data (with token optimization, preserving trends)..."
    )
    economic_data = economic_analysis.get_economic_data(
        md, max_series=50, latest_month_only=False, max_months_per_series=3
    )

    context.log.info(
        "Gathering commodity data (with token optimization, preserving trends)..."
    )
    commodity_data = economic_analysis.get_commodity_data(
        md, max_commodities=15, time_periods=["6_months"]
    )

    context.log.info(
        "Gathering Financial Conditions Index data (with token optimization, preserving trends)..."
    )
    fci_data = economic_analysis.get_financial_conditions_index(md, max_months=12)

    context.log.info(
        "Gathering housing market data (with token optimization, preserving trends)..."
    )
    housing_data = economic_analysis.get_housing_data(
        md, latest_month_only=False, max_months=6
    )

    context.log.info(
        "Gathering yield curve data (with token optimization, preserving trends)..."
    )
    yield_curve_data = economic_analysis.get_yield_curve_data(md, max_months=12)

    context.log.info(
        "Gathering economic trends data (with token optimization, preserving trends)..."
    )
    economic_trends = economic_analysis.get_economic_trends(md, max_months=12)

    optimized_analyzer = None
    if economic_analysis.use_optimized_models:
        optimized_analyzer = economic_analysis.load_optimized_module(
            module_name="economy_state",
            md_resource=md,
            gcs_resource=gcs,
            context=context,
            personality=config.personality,
        )

    analyzer_to_use = (
        optimized_analyzer
        if optimized_analyzer
        else economic_analysis.economy_state_analyzer
    )

    initial_history_length = (
        len(economic_analysis._lm.history) if hasattr(economic_analysis, "_lm") else 0
    )

    provider = economic_analysis._get_provider()
    model_name = economic_analysis._get_model_name()

    combined_input = (
        economic_data
        + "\n"
        + commodity_data
        + "\n"
        + fci_data
        + "\n"
        + housing_data
        + "\n"
        + yield_curve_data
        + "\n"
        + economic_trends
    )
    estimated_tokens = _estimate_tokens(combined_input)
    context.log.info(
        f"Estimated input tokens: {estimated_tokens}. "
        f"Checking rate limits for {provider}/{model_name}..."
    )
    _check_rate_limit(provider, model_name, estimated_tokens, context)

    context.log.info(
        f"Running economy state analysis with economic, commodity, FCI, housing, yield curve, and trends data (personality: {config.personality})..."
    )
    try:
        analysis_result = analyzer_to_use(
            economic_data=economic_data,
            commodity_data=commodity_data,
            financial_conditions_index=fci_data,
            housing_data=housing_data,
            yield_curve_data=yield_curve_data,
            economic_trends=economic_trends,
            personality=config.personality,
        )
    except Exception as e:
        error_msg = str(e)
        if "temperature=0.0" in error_msg and "gpt-5" in error_msg.lower():
            context.log.error(
                f"gpt-5 model compatibility error: {error_msg}. "
                f"gpt-5 models only support temperature=1.0. "
                f"Consider using a different model or setting litellm.drop_params = True."
            )
            raise ValueError(
                f"gpt-5 model compatibility error: {error_msg}. "
                f"Please use a model that supports temperature=0.0 or configure litellm.drop_params = True."
            ) from e
        elif (
            "response_format" in error_msg.lower()
            or "structured output" in error_msg.lower()
            or "JSON mode" in error_msg.lower()
        ):
            context.log.error(
                f"Structured output format error: {error_msg}. "
                f"Model may not support required response format features."
            )
            raise ValueError(
                f"Model compatibility error: {error_msg}. "
                f"Please use a model that supports structured output format."
            ) from e
        else:
            raise

    token_usage = _get_token_usage(economic_analysis, initial_history_length, context)

    if "total_cost_usd" not in token_usage or token_usage.get("total_cost_usd", 0) == 0:
        provider = economic_analysis._get_provider()
        model_name = economic_analysis._get_model_name()
        cost_data = _calculate_cost(
            provider=provider,
            model_name=model_name,
            prompt_tokens=token_usage.get("prompt_tokens", 0),
            completion_tokens=token_usage.get("completion_tokens", 0),
        )
        token_usage.update(cost_data)

    analysis_timestamp = datetime.now()
    result = {
        "analysis_timestamp": analysis_timestamp.isoformat(),
        "analysis_date": analysis_timestamp.strftime("%Y-%m-%d"),
        "analysis_time": analysis_timestamp.strftime("%H:%M:%S"),
        "model_name": economic_analysis._get_model_name(),
        "personality": config.personality,
        "analysis_content": analysis_result.analysis,
        "data_sources": {
            "economic_data_table": "fred_series_latest_aggregates",
            "commodity_data_tables": [
                "energy_commodities_summary",
                "input_commodities_summary",
                "agriculture_commodities_summary",
            ],
            "financial_conditions_index_table": "financial_conditions_index",
            "housing_data_tables": [
                "housing_inventory_latest_aggregates",
                "housing_mortgage_rates",
            ],
            "yield_curve_table": "stg_treasury_yields",
            "economic_trends_table": "fred_monthly_diff",
            "market_data_tables": [
                "us_sector_summary",
                "major_indicies_summary",
            ],
        },
    }

    json_result = {
        "analysis_type": "economy_state",
        "analysis_content": result["analysis_content"],
        "analysis_timestamp": result["analysis_timestamp"],
        "analysis_date": result["analysis_date"],
        "analysis_time": result["analysis_time"],
        "model_name": result["model_name"],
        "personality": result["personality"],
        "data_sources": result["data_sources"],
        "dagster_run_id": context.run_id,
        "dagster_asset_key": str(context.asset_key),
    }

    context.log.info("Writing economy state analysis to database...")
    md.write_results_to_table(
        [json_result],
        output_table="economy_state_analysis",
        if_exists="append",
        context=context,
    )

    analysis_summary = extract_economy_state_summary(result["analysis_content"])

    result_metadata = {
        "analysis_completed": True,
        "analysis_timestamp": result["analysis_timestamp"],
        "model_name": result["model_name"],
        "personality": result["personality"],
        "output_table": "economy_state_analysis",
        "records_written": 1,
        "data_sources": result["data_sources"],
        "analysis_summary": analysis_summary,
        "analysis_preview": result["analysis_content"][:500]
        if result["analysis_content"]
        else "",
        "token_usage": token_usage,
        "provider": economic_analysis._get_provider(),
    }

    context.log.info(f"Economy state analysis complete: {result_metadata}")
    return dg.MaterializeResult(metadata=result_metadata)
