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
                provider = provider.value or "openai"
            elif not provider:
                provider = os.getenv("LLM_PROVIDER", "openai")

        # Resolve model_name: use override if provided, otherwise resolve from resource field
        if model_name_override:
            model_name_val = model_name_override
        else:
            model_name_val = self.model_name
            if isinstance(model_name_val, dg.EnvVar):
                model_name_val = model_name_val.value

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
        """Get latest market performance data including US sectors and major indices.

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
                period_end_price,
                'sector' as market_category
            FROM us_sector_summary_snapshot
            WHERE snapshot_date = '{cutoff_date}'
                AND time_period IN ('12_weeks', '6_months', '1_year')
            
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
            WHERE time_period IN ('12_weeks', '6_months', '1_year')
                AND period_end_date <= '{cutoff_date}'
            
            ORDER BY market_category, asset_type, time_period, total_return_pct DESC
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
                period_end_price,
                'sector' as market_category
            FROM us_sector_summary
            WHERE time_period IN ('12_weeks', '6_months', '1_year')
            
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
            WHERE time_period IN ('12_weeks', '6_months', '1_year')
            
            ORDER BY market_category, asset_type, time_period, total_return_pct DESC
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

    def get_housing_data(
        self, md_resource: MotherDuckResource, cutoff_date: Optional[str] = None
    ) -> str:
        """Get housing market data including inventory and mortgage rates.

        Args:
            md_resource: MotherDuck resource
            cutoff_date: Optional date string (YYYY-MM-DD) for backtesting.
                        If provided, filters data up to that date.
        """
        # Get housing inventory data
        inventory_query = """
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
        inventory_csv = inventory_df.write_csv() if not inventory_df.is_empty() else ""
        mortgage_csv = mortgage_df.write_csv() if not mortgage_df.is_empty() else ""

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
        self, md_resource: MotherDuckResource, cutoff_date: Optional[str] = None
    ) -> str:
        """Get yield curve data and calculate spreads.

        Args:
            md_resource: MotherDuck resource
            cutoff_date: Optional date string (YYYY-MM-DD) for backtesting.
                        If provided, filters data up to that date.
        """
        date_filter = f"AND date <= '{cutoff_date}'" if cutoff_date else ""

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
        """

        df = md_resource.execute_query(query, read_only=True)
        if df.is_empty():
            return ""
        return df.write_csv()

    def get_economic_trends(
        self, md_resource: MotherDuckResource, cutoff_date: Optional[str] = None
    ) -> str:
        """Get month-over-month changes for key economic indicators using fred_monthly_diff.

        Args:
            md_resource: MotherDuck resource
            cutoff_date: Optional date string (YYYY-MM-DD) for backtesting.
                        If provided, filters data up to that date.
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
        LIMIT 100
        """

        df = md_resource.execute_query(query, read_only=True)
        if df.is_empty():
            return ""
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

    context.log.info("Gathering economic data...")
    economic_data = economic_analysis.get_economic_data(md)

    context.log.info("Gathering commodity data...")
    commodity_data = economic_analysis.get_commodity_data(md)

    context.log.info("Gathering Financial Conditions Index data...")
    fci_data = economic_analysis.get_financial_conditions_index(md)

    context.log.info("Gathering housing market data...")
    housing_data = economic_analysis.get_housing_data(md)

    context.log.info("Gathering yield curve data...")
    yield_curve_data = economic_analysis.get_yield_curve_data(md)

    context.log.info("Gathering economic trends data...")
    economic_trends = economic_analysis.get_economic_trends(md)

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
        f"Running economy state analysis with economic, commodity, FCI, housing, yield curve, and trends data (personality: {config.personality})..."
    )
    analysis_result = analyzer_to_use(
        economic_data=economic_data,
        commodity_data=commodity_data,
        financial_conditions_index=fci_data,
        housing_data=housing_data,
        yield_curve_data=yield_curve_data,
        economic_trends=economic_trends,
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
