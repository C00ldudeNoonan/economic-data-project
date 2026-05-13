import dagster as dg
from pydantic import Field


class EconomicAnalysisConfig(dg.Config):
    """Configuration for economic analysis assets."""

    personality: str = Field(
        default="skeptical",
        description="Analytical personality: 'skeptical' (default, bearish), 'neutral' (balanced), or 'bullish' (optimistic)",
    )
    model_provider: str | None = Field(
        default=None,
        description="LLM provider override: 'openai', 'anthropic', or 'gemini'. If not provided, uses resource default or LLM_PROVIDER env var.",
    )
    model_name: str | None = Field(
        default=None,
        description="LLM model name override (e.g., 'gpt-4-turbo-preview', 'claude-3-5-haiku-20241022', 'gemini-2.0-flash-exp'). If not provided, uses resource default or MODEL_NAME env var.",
    )
