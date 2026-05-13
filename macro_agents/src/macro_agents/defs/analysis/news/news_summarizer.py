"""DSPy module for news summarization with daily and weekly summaries."""

import os

import dagster as dg
import dspy
from pydantic import Field


class DailySummarySignature(dspy.Signature):
    """Generate daily summary of news/Fed content with sentiment analysis."""

    date: str = dspy.InputField(desc="Date of content (YYYY-MM-DD)")
    content_data: str = dspy.InputField(
        desc="CSV format data with columns: title, score, num_comments, subreddit (for Reddit) or content snippet (for FOMC)"
    )
    source_type: str = dspy.InputField(desc="Source type: 'reddit' or 'fomc'")

    summary: str = dspy.OutputField(
        desc="3-5 paragraph summary of key themes, sentiment, and notable items. Focus on economic and market sentiment."
    )
    key_themes: str = dspy.OutputField(
        desc="Comma-separated list of 3-5 major themes discussed"
    )
    sentiment: str = dspy.OutputField(
        desc="Overall market sentiment: 'bullish', 'bearish', or 'neutral'"
    )
    notable_items: str = dspy.OutputField(
        desc="2-3 most significant posts/topics with brief explanation"
    )


class WeeklySummarySignature(dspy.Signature):
    """Generate weekly cross-source analysis combining Reddit and FOMC data."""

    week_start: str = dspy.InputField(desc="Week start date (YYYY-MM-DD)")
    week_end: str = dspy.InputField(desc="Week end date (YYYY-MM-DD)")
    daily_summaries: str = dspy.InputField(
        desc="CSV of daily summaries with columns: date, source_type, summary, sentiment, key_themes"
    )

    summary: str = dspy.OutputField(
        desc="5-7 paragraph weekly overview synthesizing Reddit sentiment and FOMC policy signals. Connect market sentiment to policy context."
    )
    trends: str = dspy.OutputField(
        desc="Key trends across the week, both in market sentiment and policy direction"
    )
    sentiment_evolution: str = dspy.OutputField(
        desc="How sentiment changed throughout the week and why"
    )
    policy_context: str = dspy.OutputField(
        desc="FOMC/Fed policy context and how markets reacted (if applicable)"
    )


class NewsSummarizerModule(dspy.Module):
    """DSPy module for news summarization with chain-of-thought reasoning."""

    def __init__(self):
        super().__init__()
        self.daily_summarizer = dspy.ChainOfThought(DailySummarySignature)
        self.weekly_summarizer = dspy.ChainOfThought(WeeklySummarySignature)

    def summarize_daily(
        self, date: str, content_data: str, source_type: str
    ) -> dspy.Prediction:
        """
        Generate daily summary for Reddit or FOMC content.

        Args:
            date: Date of content (YYYY-MM-DD)
            content_data: CSV-formatted content data
            source_type: 'reddit' or 'fomc'

        Returns:
            DSPy Prediction with summary, key_themes, sentiment, notable_items
        """
        return self.daily_summarizer(
            date=date, content_data=content_data, source_type=source_type
        )

    def summarize_weekly(
        self, week_start: str, week_end: str, daily_summaries: str
    ) -> dspy.Prediction:
        """
        Generate weekly cross-source analysis.

        Args:
            week_start: Week start date (YYYY-MM-DD)
            week_end: Week end date (YYYY-MM-DD)
            daily_summaries: CSV of daily summaries

        Returns:
            DSPy Prediction with summary, trends, sentiment_evolution, policy_context
        """
        return self.weekly_summarizer(
            week_start=week_start, week_end=week_end, daily_summaries=daily_summaries
        )


class NewsSummarizerResource(dg.ConfigurableResource):
    """Dagster resource for news summarization using DSPy."""

    provider: str = Field(
        default="gemini",
        description="LLM provider: 'openai', 'gemini', or 'anthropic'",
    )
    model_name: str = Field(
        default="gemini-2.0-flash-exp",
        description="LLM model name (e.g., 'gpt-4-turbo-preview', 'gemini-2.0-flash-exp', 'claude-3-5-haiku-20241022')",
    )
    openai_api_key: str | None = Field(
        default=None, description="OpenAI API key (if using OpenAI)"
    )
    gemini_api_key: str | None = Field(
        default=None, description="Google Gemini API key (if using Gemini)"
    )
    anthropic_api_key: str | None = Field(
        default=None, description="Anthropic API key (if using Anthropic)"
    )

    def setup_for_execution(self, context) -> None:
        """Initialize DSPy with the configured LLM provider."""
        # Resolve provider
        provider = object.__getattribute__(self, "provider")
        if isinstance(provider, dg.EnvVar):
            provider = provider.value or "gemini"
        elif not provider:
            provider = os.getenv("LLM_PROVIDER", "gemini")

        # Resolve model name
        model_name = object.__getattribute__(self, "model_name")
        if isinstance(model_name, dg.EnvVar):
            model_name = model_name.value or "gemini-2.0-flash-exp"
        elif not model_name:
            model_name = os.getenv("MODEL_NAME", "gemini-2.0-flash-exp")

        context.log.info("Initializing NewsSummarizer")

        # Configure DSPy LM based on provider
        if provider == "openai":
            api_key = object.__getattribute__(self, "openai_api_key")
            if isinstance(api_key, dg.EnvVar):
                api_key = api_key.value
            if not api_key:
                api_key = os.getenv("OPENAI_API_KEY")

            lm = dspy.LM(model=f"openai/{model_name}", api_key=api_key)

        elif provider == "anthropic":
            api_key = object.__getattribute__(self, "anthropic_api_key")
            if isinstance(api_key, dg.EnvVar):
                api_key = api_key.value
            if not api_key:
                api_key = os.getenv("ANTHROPIC_API_KEY")

            lm = dspy.LM(model=f"anthropic/{model_name}", api_key=api_key)

        elif provider == "gemini":
            api_key = object.__getattribute__(self, "gemini_api_key")
            if isinstance(api_key, dg.EnvVar):
                api_key = api_key.value
            if not api_key:
                api_key = os.getenv("GEMINI_API_KEY")

            lm = dspy.LM(model=f"google/{model_name}", api_key=api_key)

        else:
            raise ValueError(
                "Unsupported provider configured. Choose from 'openai', 'anthropic', or 'gemini'"
            )

        # Configure DSPy
        dspy.configure(lm=lm)

        # Store module and config for later use
        self._module = NewsSummarizerModule()
        self._provider = provider
        self._model_name = model_name

        context.log.info("NewsSummarizer initialized")

    def summarize_daily(
        self, date: str, content_data: str, source_type: str
    ) -> dspy.Prediction:
        """Generate daily summary using the DSPy module."""
        if not hasattr(self, "_module"):
            raise RuntimeError(
                "NewsSummarizerResource not initialized. Call setup_for_execution first."
            )
        return self._module.summarize_daily(date, content_data, source_type)

    def summarize_weekly(
        self, week_start: str, week_end: str, daily_summaries: str
    ) -> dspy.Prediction:
        """Generate weekly summary using the DSPy module."""
        if not hasattr(self, "_module"):
            raise RuntimeError(
                "NewsSummarizerResource not initialized. Call setup_for_execution first."
            )
        return self._module.summarize_weekly(week_start, week_end, daily_summaries)
