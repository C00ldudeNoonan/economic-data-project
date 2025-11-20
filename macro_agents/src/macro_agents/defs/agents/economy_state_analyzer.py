import dspy
from typing import Dict, Any, Optional
from datetime import datetime
import dagster as dg
from pydantic import Field
import re
import os

from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.resources.gcs import GCSResource


class EconomicAnalysisConfig(dg.Config):
    """Configuration for economic analysis assets."""

    personality: str = Field(
        default="skeptical",
        description="Analytical personality: 'skeptical' (default, bearish), 'neutral' (balanced), or 'bullish' (optimistic)",
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
           - Yield curve analysis (normal, flat, inverted)
           - Credit spreads and financial conditions (integrate with FCI analysis)
           - Business confidence indicators
           - Leading economic index components
           - Commodity price momentum as economic signals
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
        personality: str = None,
    ):
        personality_to_use = personality or self.personality
        return self.analyze_state(
            economic_data=economic_data,
            commodity_data=commodity_data,
            financial_conditions_index=financial_conditions_index,
            personality=personality_to_use,
        )


class EconomicAnalysisResource(dg.ConfigurableResource):
    """Unified resource for economic analysis that consolidates useful parts from existing analyzers."""

    provider: str = Field(
        default="openai",
        description="LLM provider: 'openai', 'gemini', or 'anthropic'. Can be set via LLM_PROVIDER env var.",
    )
    model_name: str = Field(
        default="gpt-4-turbo-preview",
        description="LLM model name (e.g., 'gpt-4-turbo-preview', 'gemini-2.0-flash-exp', 'claude-3-opus-20240229')",
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

    def setup_for_execution(self, context) -> None:
        """Initialize DSPy when the resource is used."""
        provider = object.__getattribute__(self, "provider")
        if isinstance(provider, dg.EnvVar):
            provider = provider.value or "openai"
        elif not provider:
            provider = os.getenv("LLM_PROVIDER", "openai")

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
            model_name_val = self.model_name
            if isinstance(model_name_val, dg.EnvVar):
                model_name_val = model_name_val.value
            model_str = f"openai/{model_name_val}"
        elif provider == "gemini":
            api_key = gemini_key
            if not api_key:
                raise ValueError("gemini_api_key is required when provider='gemini'")
            model_name_val = self.model_name
            if isinstance(model_name_val, dg.EnvVar):
                model_name_val = model_name_val.value
            model_str = f"gemini/{model_name_val}"
        elif provider == "anthropic":
            api_key = anthropic_key
            if not api_key:
                raise ValueError(
                    "anthropic_api_key is required when provider='anthropic'"
                )
            model_name_val = self.model_name
            if isinstance(model_name_val, dg.EnvVar):
                model_name_val = model_name_val.value
            model_str = f"anthropic/{model_name_val}"
        else:
            raise ValueError(
                f"Unknown provider: {provider}. Must be 'openai', 'gemini', or 'anthropic'"
            )

        self._provider = provider

        lm = dspy.LM(model=model_str, api_key=api_key)
        dspy.settings.configure(lm=lm)
        self._lm = lm  # Store LM reference for token tracking
        self._economy_state_analyzer = EconomyStateModule()
        self._optimized_modules_cache = {}

    def _get_provider(self) -> str:
        """Get the current LLM provider (resolved value, not the Field)."""
        return getattr(self, "_provider", getattr(self, "provider", "openai"))

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
        self, md_resource: MotherDuckResource, cutoff_date: Optional[str] = None
    ) -> str:
        """Get latest economic data from FRED series.

        Args:
            md_resource: MotherDuck resource
            cutoff_date: Optional date string (YYYY-MM-DD) for backtesting.
                        If provided, uses snapshot tables.
        """
        if cutoff_date:
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
            ORDER BY series_name, month DESC
            """
        else:
            query = """
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
            ORDER BY series_name, month DESC
            """

        df = md_resource.execute_query(query)
        csv_buffer = df.write_csv()
        return csv_buffer

    def get_market_data(
        self, md_resource: MotherDuckResource, cutoff_date: Optional[str] = None
    ) -> str:
        """Get latest market performance data.

        Args:
            md_resource: MotherDuck resource
            cutoff_date: Optional date string (YYYY-MM-DD) for backtesting.
                        If provided, uses snapshot tables.
        """
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
                period_end_price
            FROM us_sector_summary_snapshot
            WHERE snapshot_date = '{cutoff_date}'
                AND time_period IN ('12_weeks', '6_months', '1_year')
            ORDER BY asset_type, time_period, total_return_pct DESC
            """
        else:
            query = """
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
                period_end_price
            FROM us_sector_summary
            WHERE time_period IN ('12_weeks', '6_months', '1_year')
            ORDER BY asset_type, time_period, total_return_pct DESC
            """

        df = md_resource.execute_query(query)
        csv_buffer = df.write_csv()
        return csv_buffer

    def get_financial_conditions_index(
        self, md_resource: MotherDuckResource, cutoff_date: Optional[str] = None
    ) -> str:
        """Get full historical Financial Conditions Index data.

        Args:
            md_resource: MotherDuck resource
            cutoff_date: Optional date string (YYYY-MM-DD) for backtesting.
                        If provided, uses snapshot tables.
        """
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
            """
        else:
            query = """
            SELECT 
                date,
                FCI,
                equity_score,
                housing_score,
                "10yr_score" as treasury_10yr_score
            FROM financial_conditions_index
            WHERE FCI IS NOT NULL
            ORDER BY date DESC
            """

        df = md_resource.execute_query(query, read_only=True)

        df = md_resource.execute_query(query, read_only=True)
        csv_buffer = df.write_csv()
        return csv_buffer

    def get_commodity_data(
        self, md_resource: MotherDuckResource, cutoff_date: Optional[str] = None
    ) -> str:
        """Get latest commodity performance data from all commodity summary tables.

        Args:
            md_resource: MotherDuck resource
            cutoff_date: Optional date string (YYYY-MM-DD) for backtesting.
                        If provided, uses snapshot tables.
        """
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
                AND time_period IN ('12_weeks', '6_months', '1_year')
            
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
                AND time_period IN ('12_weeks', '6_months', '1_year')
            
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
                AND time_period IN ('12_weeks', '6_months', '1_year')
            
            ORDER BY commodity_category, commodity_name, time_period, total_return_pct DESC
            """
        else:
            query = """
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
            WHERE time_period IN ('12_weeks', '6_months', '1_year')
            
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
            WHERE time_period IN ('12_weeks', '6_months', '1_year')
            
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
            WHERE time_period IN ('12_weeks', '6_months', '1_year')
            
            ORDER BY commodity_category, commodity_name, time_period, total_return_pct DESC
            """

        df = md_resource.execute_query(query)
        csv_buffer = df.write_csv()
        return csv_buffer

    def get_correlation_data(
        self,
        md_resource: MotherDuckResource,
        sample_size: int = 100,
        sampling_strategy: str = "top_correlations",
        cutoff_date: Optional[str] = None,
    ) -> str:
        """Get correlation data between economic indicators and asset returns.

        Args:
            md_resource: MotherDuck resource
            sample_size: Number of samples to return
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
                    return df.write_csv()
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
                return df.write_csv()


def _get_token_usage(
    economic_analysis: EconomicAnalysisResource,
    initial_history_length: int,
    context: dg.AssetExecutionContext,
) -> Dict[str, Any]:
    """
    Extract token usage from DSPy LM history.

    Returns dictionary with prompt_tokens, completion_tokens, and total_tokens.
    """
    token_usage = {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
    }

    if not hasattr(economic_analysis, "_lm"):
        return token_usage

    try:
        lm = economic_analysis._lm
        history = lm.history

        new_entries = history[initial_history_length:]

        for entry in new_entries:
            if hasattr(entry, "prompt_tokens"):
                token_usage["prompt_tokens"] += entry.prompt_tokens
            if hasattr(entry, "completion_tokens"):
                token_usage["completion_tokens"] += entry.completion_tokens
            elif hasattr(entry, "response"):
                if hasattr(lm, "tokenizer"):
                    try:
                        prompt_text = str(entry.get("messages", ""))
                        response_text = str(entry.get("response", ""))
                        token_usage["prompt_tokens"] += len(prompt_text) // 4
                        token_usage["completion_tokens"] += len(response_text) // 4
                    except Exception:
                        pass

            token_usage["total_tokens"] = (
                token_usage["prompt_tokens"] + token_usage["completion_tokens"]
            )

        if hasattr(lm, "get_token_count"):
            try:
                actual_counts = lm.get_token_count()
                if actual_counts:
                    token_usage.update(actual_counts)
            except Exception:
                pass

        if hasattr(lm, "usage"):
            usage = lm.usage
            if usage:
                token_usage["prompt_tokens"] = usage.get(
                    "prompt_tokens", token_usage["prompt_tokens"]
                )
                token_usage["completion_tokens"] = usage.get(
                    "completion_tokens", token_usage["completion_tokens"]
                )
                token_usage["total_tokens"] = usage.get(
                    "total_tokens", token_usage["total_tokens"]
                )

    except Exception as e:
        context.log.warning(f"Could not extract token usage: {e}")

    return token_usage


def extract_economy_state_summary(analysis_content: str) -> Dict[str, Any]:
    """Extract key insights from economy state analysis for metadata."""
    summary = {}

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
        dg.AssetKey(["leading_econ_return_indicator"]),
        dg.AssetKey(["us_sector_summary"]),
        dg.AssetKey(["energy_commodities_summary"]),
        dg.AssetKey(["input_commodities_summary"]),
        dg.AssetKey(["agriculture_commodities_summary"]),
        dg.AssetKey(["financial_conditions_index"]),
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

    context.log.info("Gathering economic data...")
    economic_data = economic_analysis.get_economic_data(md)

    context.log.info("Gathering commodity data...")
    commodity_data = economic_analysis.get_commodity_data(md)

    context.log.info("Gathering Financial Conditions Index data...")
    fci_data = economic_analysis.get_financial_conditions_index(md)

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

    context.log.info(
        f"Running economy state analysis with economic, commodity, and FCI data (personality: {config.personality})..."
    )
    analysis_result = analyzer_to_use(
        economic_data=economic_data,
        commodity_data=commodity_data,
        financial_conditions_index=fci_data,
        personality=config.personality,
    )

    token_usage = _get_token_usage(economic_analysis, initial_history_length, context)

    analysis_timestamp = datetime.now()
    result = {
        "analysis_timestamp": analysis_timestamp.isoformat(),
        "analysis_date": analysis_timestamp.strftime("%Y-%m-%d"),
        "analysis_time": analysis_timestamp.strftime("%H:%M:%S"),
        "model_name": economic_analysis.model_name,
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
