import base64
import io
import os
import pickle
from typing import Any

import dagster as dg
import dspy
from pydantic import Field

from macro_agents.defs.analysis.data_points.data_point_analyzer import (
    DataPointAnalyzerModule,
)
from macro_agents.defs.analysis.economy_state import data_access
from macro_agents.defs.analysis.economy_state.modules import EconomyStateModule
from macro_agents.defs.resources.motherduck import MotherDuckResource


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
    openai_api_key: str | None = Field(
        default=None,
        description="OpenAI API key (required if provider='openai'). Can be set via OPENAI_API_KEY env var.",
    )
    gemini_api_key: str | None = Field(
        default=None,
        description="Google Gemini API key (required if provider='gemini'). Can be set via GEMINI_API_KEY env var.",
    )
    anthropic_api_key: str | None = Field(
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
        provider_override: str | None = None,
        model_name_override: str | None = None,
    ) -> None:
        """Initialize DSPy when the resource is used."""
        if provider_override:
            provider = provider_override
        else:
            provider = object.__getattribute__(self, "provider")
            if isinstance(provider, dg.EnvVar):
                provider = provider.get_value() or "gemini"
            elif not provider:
                provider = os.getenv("LLM_PROVIDER", "gemini")

        if model_name_override:
            model_name_val = model_name_override
        else:
            model_name_val = self.model_name
            if isinstance(model_name_val, dg.EnvVar):
                model_name_val = model_name_val.get_value() or "gemini-3-pro-preview"
            elif not model_name_val:
                model_name_val = os.getenv("MODEL_NAME", "gemini-3-pro-preview")

        openai_key = self.openai_api_key
        if isinstance(openai_key, dg.EnvVar):
            openai_key = openai_key.get_value()
        elif not openai_key:
            openai_key = os.getenv("OPENAI_API_KEY")

        gemini_key = self.gemini_api_key
        if isinstance(gemini_key, dg.EnvVar):
            gemini_key = gemini_key.get_value()
        elif not gemini_key:
            gemini_key = os.getenv("GEMINI_API_KEY")

        anthropic_key = self.anthropic_api_key
        if isinstance(anthropic_key, dg.EnvVar):
            anthropic_key = anthropic_key.get_value()
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
                "Unknown provider configured. Must be 'openai', 'gemini', or 'anthropic'"
            )

        self._provider = provider
        self._model_name = model_name_val

        is_gpt5_model = "gpt-5" in model_name_val.lower()
        if is_gpt5_model:
            context.log.info(
                "Detected gpt-5 model. "
                "Setting litellm.drop_params = True to handle unsupported temperature parameter."
            )
            # Inline import to avoid loading optional dependency unless needed.
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
                    # Inline import to avoid loading optional dependency unless needed.
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
                    context.log.error("Failed to configure LM even with drop_params")
                    raise ValueError(
                        "Could not configure LLM. gpt-5 models require temperature=1.0. "
                        f"Original error: {error_msg}"
                    ) from retry_error
            elif (
                "response_format" in error_msg.lower()
                or "structured output" in error_msg.lower()
            ):
                context.log.warning(
                    "Model may not support structured output format. "
                    "Attempting to configure with fallback settings."
                )
                try:
                    # Inline import to avoid loading optional dependency unless needed.
                    import litellm

                    litellm.drop_params = True
                    lm = dspy.LM(model=model_str, api_key=api_key)
                    dspy.settings.configure(lm=lm)
                    self._lm = lm
                    context.log.info(
                        "Successfully configured LM with fallback settings"
                    )
                except Exception as retry_error:
                    context.log.error("Failed to configure LM with fallback settings")
                    raise ValueError(
                        "Could not configure LLM. "
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
        md_resource: MotherDuckResource | None = None,
        gcs_resource: Any | None = None,
        context: dg.AssetExecutionContext | None = None,
        personality: str | None = None,
    ) -> dspy.Module | None:
        """Load optimized module from GCS if available."""
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
                    module_bytes = base64.b64decode(module_state_b64.encode("utf-8"))

                    serialization_method = model_data.get(
                        "serialization_method", "pickle"
                    )
                    if serialization_method == "dspy.save" and hasattr(dspy, "load"):
                        buffer = io.BytesIO(module_bytes)
                        module = dspy.load(buffer)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
                    else:
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
                    # Inline import to avoid circular dependency with EconomyState resources.
                    from macro_agents.defs.analysis.asset_relationships.asset_class_relationship_analyzer import (
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
                    # Inline import to avoid circular dependency with EconomyState resources.
                    from macro_agents.defs.analysis.investments.investment_recommendations import (
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
        cutoff_date: str | None = None,
        max_series: int | None = None,
        latest_month_only: bool = False,
        max_months_per_series: int | None = 3,
    ) -> str:
        return data_access.get_economic_data(
            md_resource,
            cutoff_date=cutoff_date,
            max_series=max_series,
            latest_month_only=latest_month_only,
            max_months_per_series=max_months_per_series,
        )

    def get_market_data(
        self,
        md_resource: MotherDuckResource,
        cutoff_date: str | None = None,
        max_assets: int | None = 20,
        time_periods: list[str] | None = None,
    ) -> str:
        return data_access.get_market_data(
            md_resource,
            cutoff_date=cutoff_date,
            max_assets=max_assets,
            time_periods=time_periods,
        )

    def get_financial_conditions_index(
        self,
        md_resource: MotherDuckResource,
        cutoff_date: str | None = None,
        max_months: int | None = 12,
    ) -> str:
        return data_access.get_financial_conditions_index(
            md_resource,
            cutoff_date=cutoff_date,
            max_months=max_months,
        )

    def get_commodity_data(
        self,
        md_resource: MotherDuckResource,
        cutoff_date: str | None = None,
        max_commodities: int | None = 15,
        time_periods: list[str] | None = None,
    ) -> str:
        return data_access.get_commodity_data(
            md_resource,
            cutoff_date=cutoff_date,
            max_commodities=max_commodities,
            time_periods=time_periods,
        )

    def get_correlation_data(
        self,
        md_resource: MotherDuckResource,
        sample_size: int = 50,
        sampling_strategy: str = "top_correlations",
        cutoff_date: str | None = None,
    ) -> str:
        return data_access.get_correlation_data(
            md_resource,
            sample_size=sample_size,
            sampling_strategy=sampling_strategy,
            cutoff_date=cutoff_date,
        )

    def get_housing_data(
        self,
        md_resource: MotherDuckResource,
        cutoff_date: str | None = None,
        latest_month_only: bool = False,
        max_months: int | None = 6,
    ) -> str:
        return data_access.get_housing_data(
            md_resource,
            cutoff_date=cutoff_date,
            latest_month_only=latest_month_only,
            max_months=max_months,
        )

    def get_yield_curve_data(
        self,
        md_resource: MotherDuckResource,
        cutoff_date: str | None = None,
        max_months: int | None = 12,
    ) -> str:
        return data_access.get_yield_curve_data(
            md_resource,
            cutoff_date=cutoff_date,
            max_months=max_months,
        )

    def get_economic_trends(
        self,
        md_resource: MotherDuckResource,
        cutoff_date: str | None = None,
        max_months: int | None = 12,
    ) -> str:
        return data_access.get_economic_trends(
            md_resource,
            cutoff_date=cutoff_date,
            max_months=max_months,
        )

    def get_data_point_analyzer(self):
        """
        Get initialized data point analyzer module for finding interesting data points.
        """
        if not hasattr(self, "_data_point_analyzer"):
            self._data_point_analyzer = DataPointAnalyzerModule()
        return self._data_point_analyzer
