"""DSPy module and Dagster resource for Fed sentiment analysis."""

import os

import dagster as dg
import dspy
from pydantic import Field

from macro_agents.defs.analysis.fed_sentiment.signatures import (
    FedSentimentSignature,
    FedTopicExtractionSignature,
)


class FedSentimentModule(dspy.Module):
    """DSPy module wrapping Fed sentiment and topic extraction signatures."""

    def __init__(self):
        super().__init__()
        self.sentiment_scorer = dspy.ChainOfThought(FedSentimentSignature)
        self.topic_extractor = dspy.ChainOfThought(FedTopicExtractionSignature)

    def score_sentiment(
        self, speaker: str, content: str, meeting_date: str
    ) -> dspy.Prediction:
        """Score hawkish/dovish sentiment for a transcript section."""
        return self.sentiment_scorer(
            speaker=speaker, content=content, meeting_date=meeting_date
        )

    def extract_topics(self, content: str, speaker: str) -> dspy.Prediction:
        """Extract economic topics from a transcript section."""
        return self.topic_extractor(content=content, speaker=speaker)


class FedSentimentResource(dg.ConfigurableResource):
    """Dagster resource for Fed communications sentiment analysis using DSPy.

    Follows the same provider/model pattern as NewsSummarizerResource.
    """

    provider: str = Field(
        default="gemini",
        description="LLM provider: 'openai', 'gemini', or 'anthropic'",
    )
    model_name: str = Field(
        default="gemini-2.0-flash-exp",
        description="LLM model name",
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
        provider = object.__getattribute__(self, "provider")
        if isinstance(provider, dg.EnvVar):
            provider = provider.get_value() or "gemini"
        elif not provider:
            provider = os.getenv("LLM_PROVIDER", "gemini")

        model_name = object.__getattribute__(self, "model_name")
        if isinstance(model_name, dg.EnvVar):
            model_name = model_name.get_value() or "gemini-2.0-flash-exp"
        elif not model_name:
            model_name = os.getenv("MODEL_NAME", "gemini-2.0-flash-exp")

        context.log.info(
            f"Initializing FedSentimentResource with {provider}/{model_name}"
        )

        if provider == "openai":
            api_key = self._resolve_key(self.openai_api_key, "OPENAI_API_KEY")
            lm = dspy.LM(model=f"openai/{model_name}", api_key=api_key)
        elif provider == "anthropic":
            api_key = self._resolve_key(self.anthropic_api_key, "ANTHROPIC_API_KEY")
            lm = dspy.LM(model=f"anthropic/{model_name}", api_key=api_key)
        elif provider == "gemini":
            api_key = self._resolve_key(self.gemini_api_key, "GEMINI_API_KEY")
            lm = dspy.LM(model=f"google/{model_name}", api_key=api_key)
        else:
            raise ValueError(
                f"Unsupported provider '{provider}'. Choose 'openai', 'anthropic', or 'gemini'."
            )

        dspy.configure(lm=lm)
        self._module = FedSentimentModule()
        self._provider = provider
        self._model_name = model_name

        context.log.info("FedSentimentResource initialized")

    def score_sentiment(
        self, speaker: str, content: str, meeting_date: str
    ) -> dspy.Prediction:
        """Score hawkish/dovish sentiment for a transcript section."""
        if not hasattr(self, "_module"):
            raise RuntimeError(
                "FedSentimentResource not initialized. Call setup_for_execution first."
            )
        return self._module.score_sentiment(speaker, content, meeting_date)

    def extract_topics(self, content: str, speaker: str) -> dspy.Prediction:
        """Extract economic topics from a transcript section."""
        if not hasattr(self, "_module"):
            raise RuntimeError(
                "FedSentimentResource not initialized. Call setup_for_execution first."
            )
        return self._module.extract_topics(content, speaker)

    @staticmethod
    def _resolve_key(key_field, env_var: str) -> str | None:
        """Resolve an API key from a field value or environment variable."""
        if isinstance(key_field, dg.EnvVar):
            return key_field.get_value()
        if key_field:
            return key_field
        return os.getenv(env_var)
