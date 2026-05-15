"""Natural Language Query Resource - Dagster resource for NL to SQL conversion."""

import os

import dagster as dg
import dspy
from pydantic import Field

from macro_agents.defs.analysis.ai.nl_to_sql_module import (
    NaturalLanguageToSQLModule,
    SQLValidator,
)


class NaturalLanguageQueryResource(dg.ConfigurableResource):
    """Resource for converting natural language questions to SQL queries using DSPy."""

    provider: str = Field(
        default="gemini",
        description="LLM provider: 'openai', 'gemini', or 'anthropic'. Can be set via LLM_PROVIDER env var.",
    )

    model_name: str = Field(
        default="gemini-2.0-flash-exp",
        description="LLM model name (e.g., 'gpt-4-turbo-preview', 'gemini-2.0-flash-exp', 'claude-3-5-sonnet-20241022')",
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

    max_calls_per_run: int = Field(
        default=500,
        description=(
            "Hard cap on the number of LLM completions issued by this resource "
            "instance. Prevents runaway spend from a misconfigured backtest "
            "loop or sensor that fans out NL-to-SQL calls. Configure higher "
            "for production batch jobs; set via NL_QUERY_MAX_CALLS env var."
        ),
    )

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        """
        Initialize DSPy LM and NL-to-SQL module.

        Args:
            context: Dagster execution context for logging
        """
        # Resolve provider
        provider = object.__getattribute__(self, "provider")
        if isinstance(provider, dg.EnvVar):
            provider = provider.value or "gemini"
        elif not provider:
            provider = os.getenv("LLM_PROVIDER", "gemini")

        # Resolve model name
        model_name_val = self.model_name
        if isinstance(model_name_val, dg.EnvVar):
            model_name_val = model_name_val.get_value() or "gemini-2.0-flash-exp"
        elif not model_name_val:
            model_name_val = os.getenv("MODEL_NAME", "gemini-2.0-flash-exp")

        # Resolve API keys
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

        # Configure DSPy LM based on provider
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

        # Check for gpt-5 models which need special handling
        is_gpt5_model = "gpt-5" in model_name_val.lower()
        log = context.log
        if is_gpt5_model and log:
            log.info("Detected gpt-5 model. Setting litellm.drop_params = True.")
            import litellm

            litellm.drop_params = True

        # Initialize DSPy LM
        try:
            if is_gpt5_model:
                lm = dspy.LM(model=model_str, api_key=api_key, max_tokens=4000)
            else:
                lm = dspy.LM(model=model_str, api_key=api_key)

            dspy.settings.configure(lm=lm)
            self._lm = lm

            if log:
                log.info("Initialized NL Query Resource")
        except Exception:
            if log:
                log.error("Failed to initialize DSPy LM")
            raise

        # Initialize NL-to-SQL module and validator
        self._nl_module = NaturalLanguageToSQLModule()
        self._sql_validator = SQLValidator()
        self._call_count = 0

        if log:
            log.info("NL-to-SQL module and SQL validator initialized successfully")

    def query_to_sql(
        self,
        question: str,
        data_dict_json: str,
        conversation_history: str = "",
        context: dg.AssetExecutionContext | None = None,
    ) -> dict:
        """
        Convert natural language question to SQL query.

        Args:
            question: User's natural language question
            data_dict_json: JSON string of data dictionary (tables/columns metadata)
            conversation_history: Optional conversation history for context
            context: Optional Dagster context for logging

        Returns:
            Dictionary with keys:
                - sql_query: Generated SQL query string
                - explanation: Plain English explanation
                - relevant_tables: List of table names used
                - confidence: Confidence score 0.0-1.0
                - clarifying_questions: List of clarifying questions if needed
                - is_valid: Boolean indicating if SQL passed validation
                - validation_error: Error message if validation failed
        """
        if context:
            context.log.info(f"Processing question: {question}")

        # Enforce per-run LLM call cap to bound spend on runaway loops.
        current = getattr(self, "_call_count", 0)
        if current >= self.max_calls_per_run:
            raise RuntimeError(
                f"NL query LLM call cap reached ({self.max_calls_per_run}). "
                f"Increase max_calls_per_run if this is expected."
            )
        self._call_count = current + 1

        # Generate SQL using DSPy module
        result = self._nl_module.forward(
            question=question,
            data_dictionary=data_dict_json,
            conversation_history=conversation_history,
        )

        # Extract results
        sql_query = result.sql_query
        explanation = result.explanation
        relevant_tables_str = result.relevant_tables
        confidence = float(result.confidence) if result.confidence else 0.0
        clarifying_questions_str = result.clarifying_questions or ""

        # Parse comma-separated lists
        relevant_tables = [
            t.strip() for t in relevant_tables_str.split(",") if t.strip()
        ]
        clarifying_questions = [
            q.strip() for q in clarifying_questions_str.split(",") if q.strip()
        ]

        # Validate SQL
        is_valid, validation_error = self._sql_validator.validate_query(sql_query)

        if context:
            if is_valid:
                context.log.info(
                    f"Generated valid SQL query (confidence: {confidence})"
                )
            else:
                context.log.warning(
                    f"Generated SQL failed validation: {validation_error}"
                )

        # Add safety limits if valid
        if is_valid:
            sql_query = self._sql_validator.add_safety_limits(sql_query)

        return {
            "sql_query": sql_query,
            "explanation": explanation,
            "relevant_tables": relevant_tables,
            "confidence": confidence,
            "clarifying_questions": clarifying_questions,
            "is_valid": is_valid,
            "validation_error": validation_error if not is_valid else None,
        }


# Instantiate resource with environment variable configuration
nl_query_resource = NaturalLanguageQueryResource(
    provider=dg.EnvVar("LLM_PROVIDER"),
    model_name=dg.EnvVar("MODEL_NAME"),
    openai_api_key=dg.EnvVar("OPENAI_API_KEY"),
    gemini_api_key=dg.EnvVar("GEMINI_API_KEY"),
    anthropic_api_key=dg.EnvVar("ANTHROPIC_API_KEY"),
    max_calls_per_run=int(os.getenv("NL_QUERY_MAX_CALLS", "500")),
)
