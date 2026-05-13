import dagster as dg

from macro_agents.defs.analysis.economy_state.economy_state_analyzer import (
    EconomicAnalysisResource,
)
from macro_agents.defs.analysis.fed_sentiment.resource import FedSentimentResource
from macro_agents.defs.analysis.news.news_summarizer import NewsSummarizerResource


analysis_resources = {
    "economic_analysis": EconomicAnalysisResource(
        provider="gemini",
        model_name="gemini-3-pro-preview",
        openai_api_key=dg.EnvVar("OPENAI_API_KEY"),
        gemini_api_key=dg.EnvVar("GEMINI_API_KEY"),
        anthropic_api_key=dg.EnvVar("ANTHROPIC_API_KEY"),
    ),
    "news_summarizer": NewsSummarizerResource(
        provider="gemini",
        model_name="gemini-2.0-flash-exp",
        gemini_api_key=dg.EnvVar("GEMINI_API_KEY"),
        openai_api_key=dg.EnvVar("OPENAI_API_KEY"),
        anthropic_api_key=dg.EnvVar("ANTHROPIC_API_KEY"),
    ),
    "fed_sentiment": FedSentimentResource(
        provider="gemini",
        model_name="gemini-2.0-flash-exp",
        gemini_api_key=dg.EnvVar("GEMINI_API_KEY"),
        openai_api_key=dg.EnvVar("OPENAI_API_KEY"),
        anthropic_api_key=dg.EnvVar("ANTHROPIC_API_KEY"),
    ),
}
