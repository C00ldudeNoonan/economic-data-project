from macro_agents.defs.analysis.ai.ai_models_fetcher import fetch_available_ai_models
from macro_agents.defs.analysis.asset_relationships.asset_class_relationship_analyzer import (
    analyze_asset_class_relationships,
)
from macro_agents.defs.analysis.data_points.interesting_data_points_assets import (
    detect_interesting_data_points_weekly,
)
from macro_agents.defs.analysis.economy_state.economy_state_analyzer import (
    analyze_economy_state,
)
from macro_agents.defs.analysis.economy_state.economy_state_charts import (
    generate_economy_state_charts,
)
from macro_agents.defs.analysis.investments.investment_recommendations import (
    generate_investment_recommendations,
)
from macro_agents.defs.analysis.investments.investment_recommendation_charts import (
    generate_investment_recommendation_charts,
)
from macro_agents.defs.analysis.narratives.economic_narrative_assets import (
    generate_economic_narratives,
    generate_indicator_forecasts,
)
from macro_agents.defs.analysis.fed_sentiment.assets import (
    fed_sentiment_rate_correlation,
    score_fed_sentiment_dictionary,
    score_fed_sentiment_llm,
)
from macro_agents.defs.analysis.news.news_summary_assets import (
    news_weekly_summary,
    reddit_daily_summary,
)


analysis_assets = [
    analyze_economy_state,
    generate_economy_state_charts,
    analyze_asset_class_relationships,
    generate_investment_recommendations,
    generate_investment_recommendation_charts,
    generate_economic_narratives,
    generate_indicator_forecasts,
    detect_interesting_data_points_weekly,
    fetch_available_ai_models,
    reddit_daily_summary,
    news_weekly_summary,
    score_fed_sentiment_dictionary,
    score_fed_sentiment_llm,
    fed_sentiment_rate_correlation,
]
