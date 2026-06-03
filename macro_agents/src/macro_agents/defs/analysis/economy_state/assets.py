from datetime import datetime

import dagster as dg

from macro_agents.defs.analysis.economy_state.config import EconomicAnalysisConfig
from macro_agents.defs.analysis.economy_state.rate_limits import (
    _check_rate_limit,
    _estimate_tokens,
)
from macro_agents.defs.analysis.economy_state.resource import EconomicAnalysisResource
from macro_agents.defs.analysis.economy_state.summary import (
    extract_economy_state_summary,
)
from macro_agents.defs.analysis.economy_state.token_usage import (
    _calculate_cost,
    _get_token_usage,
)
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


@dg.asset(
    kinds={"dspy", "duckdb"},
    group_name="economic_analysis",
    description="Analyze current economic indicators and commodity data to determine the state of the economy",
    deps=[
        dg.AssetKey(["agent_fred_series_latest_aggregates"]),
        dg.AssetKey(["agent_fred_monthly_diff"]),
        dg.AssetKey(["agent_leading_econ_return_indicator"]),
        dg.AssetKey(["agent_market_performance"]),
        dg.AssetKey(["agent_commodity_performance"]),
        dg.AssetKey(["agent_financial_conditions_index"]),
        dg.AssetKey(["agent_housing_inventory_latest_aggregates"]),
        dg.AssetKey(["agent_housing_mortgage_rates"]),
        dg.AssetKey(["agent_treasury_yield_curve_spreads"]),
        dg.AssetKey(["auto_promote_best_models_to_production"]),
    ],
)
def analyze_economy_state(
    context: dg.AssetExecutionContext,
    config: EconomicAnalysisConfig,
    bq: BigQueryWarehouseResource,
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
    fci_data = economic_analysis.get_financial_conditions_index(bq, max_months=12)

    context.log.info(
        "Gathering housing market data (with token optimization, preserving trends)..."
    )
    housing_data = economic_analysis.get_housing_data(
        md, latest_month_only=False, max_months=6
    )

    context.log.info(
        "Gathering yield curve data (with token optimization, preserving trends)..."
    )
    yield_curve_data = economic_analysis.get_yield_curve_data(bq, max_months=12)

    context.log.info(
        "Gathering economic trends data (with token optimization, preserving trends)..."
    )
    economic_trends = economic_analysis.get_economic_trends(bq, max_months=12)

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
                "gpt-5 models only support temperature=1.0. "
                "Consider using a different model or setting litellm.drop_params = True."
            )
            raise ValueError(
                f"gpt-5 model compatibility error: {error_msg}. "
                "Please use a model that supports temperature=0.0 or configure litellm.drop_params = True."
            ) from e
        if (
            "response_format" in error_msg.lower()
            or "structured output" in error_msg.lower()
            or "JSON mode" in error_msg.lower()
        ):
            context.log.error(
                f"Structured output format error: {error_msg}. "
                "Model may not support required response format features."
            )
            raise ValueError(
                f"Model compatibility error: {error_msg}. "
                "Please use a model that supports structured output format."
            ) from e
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
            "economic_data_table": "agent_fred_series_latest_aggregates",
            "commodity_data_tables": ["agent_commodity_performance"],
            "financial_conditions_index_table": "agent_financial_conditions_index",
            "housing_data_tables": [
                "agent_housing_inventory_latest_aggregates",
                "agent_housing_mortgage_rates",
            ],
            "yield_curve_table": "agent_treasury_yield_curve_spreads",
            "economic_trends_table": "agent_fred_monthly_diff",
            "market_data_tables": ["agent_market_performance"],
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
        "chart_manifest": None,
    }

    context.log.info("Writing economy state analysis to database...")
    bq.write_results_to_table(
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


@dg.asset(
    kinds={"dspy", "duckdb"},
    group_name="economic_analysis",
    description="Analyze economy state using specialized domain sub-agents (v2)",
    deps=[
        dg.AssetKey(["agent_fred_series_latest_aggregates"]),
        dg.AssetKey(["agent_fred_monthly_diff"]),
        dg.AssetKey(["agent_leading_econ_return_indicator"]),
        dg.AssetKey(["agent_market_performance"]),
        dg.AssetKey(["agent_commodity_performance"]),
        dg.AssetKey(["agent_financial_conditions_index"]),
        dg.AssetKey(["agent_treasury_yield_curve_spreads"]),
    ],
)
def analyze_economy_state_v2(
    context: dg.AssetExecutionContext,
    config: EconomicAnalysisConfig,
    bq: BigQueryWarehouseResource,
    economic_analysis: EconomicAnalysisResource,
) -> dg.MaterializeResult:
    """
    V2 Economy State Analysis using domain-specific sub-agents.

    This asset runs 5 specialized sub-agents sequentially:
    1. Labor Market Agent
    2. Financial Conditions Agent
    3. Commodities Agent
    4. Sector Agent
    5. Market Structure Agent

    Then aggregates their outputs into a comprehensive economic state analysis.

    Returns:
        MaterializeResult with analysis metadata
    """
    from macro_agents.defs.analysis.economy_state.domain_sub_agents import (
        LaborMarketModule,
        FinancialConditionsModule,
        CommoditiesModule,
        SectorModule,
        MarketStructureModule,
    )
    from macro_agents.defs.analysis.economy_state.economic_state_aggregator import (
        EconomicStateAggregatorModule,
        DomainAnalysisResult,
    )
    from macro_agents.defs.analysis.economy_state.domain_data_fetchers import (
        get_labor_market_data,
        get_labor_trends_data,
        get_financial_conditions_data,
        get_yield_curve_data,
        get_credit_data,
        get_energy_commodities_data,
        get_input_commodities_data,
        get_agriculture_commodities_data,
        get_sector_data,
        get_sector_correlation_data,
        get_major_indices_data,
        get_fixed_income_data,
        get_global_markets_data,
    )

    context.log.info(
        f"Starting V2 economy state analysis with sub-agents (personality: {config.personality})..."
    )

    economic_analysis.setup_for_execution(
        context,
        provider_override=config.model_provider,
        model_name_override=config.model_name,
    )

    provider = economic_analysis._get_provider()
    model_name = economic_analysis._get_model_name()
    personality = config.personality

    labor_agent = LaborMarketModule(personality=personality)
    financial_agent = FinancialConditionsModule(personality=personality)
    commodities_agent = CommoditiesModule(personality=personality)
    sector_agent = SectorModule(personality=personality)
    market_agent = MarketStructureModule(personality=personality)
    aggregator = EconomicStateAggregatorModule(personality=personality)

    domain_results = DomainAnalysisResult()
    total_token_usage: dict[str, int | float | str] = {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
    }

    context.log.info("Step 1/6: Running Labor Market Agent...")

    labor_data = get_labor_market_data(md)
    labor_trends = get_labor_trends_data(md)

    estimated_tokens = _estimate_tokens(labor_data + labor_trends)
    _check_rate_limit(provider, model_name, estimated_tokens, context)

    initial_history = (
        len(economic_analysis._lm.history) if hasattr(economic_analysis, "_lm") else 0
    )

    try:
        labor_result = labor_agent(
            labor_data=labor_data,
            employment_trends=labor_trends,
            personality=personality,
        )
        domain_results.labor_analysis = labor_result.labor_analysis
        context.log.info("Labor Market Agent complete.")
    except Exception as e:
        context.log.error(f"Labor Market Agent failed: {e}")
        domain_results.labor_analysis = f"Analysis failed: {e}"

    token_usage = _get_token_usage(economic_analysis, initial_history, context)
    for k in ["prompt_tokens", "completion_tokens", "total_tokens"]:
        total_token_usage[k] += token_usage.get(k, 0)

    context.log.info("Step 2/6: Running Financial Conditions Agent...")

    fci_data = get_financial_conditions_data(md)
    yield_curve = get_yield_curve_data(md)
    credit_data = get_credit_data(md)

    estimated_tokens = _estimate_tokens(fci_data + yield_curve + credit_data)
    _check_rate_limit(provider, model_name, estimated_tokens, context)

    initial_history = (
        len(economic_analysis._lm.history) if hasattr(economic_analysis, "_lm") else 0
    )

    try:
        financial_result = financial_agent(
            fci_data=fci_data,
            yield_curve_data=yield_curve,
            credit_data=credit_data,
            personality=personality,
        )
        domain_results.financial_analysis = financial_result.financial_analysis
        context.log.info("Financial Conditions Agent complete.")
    except Exception as e:
        context.log.error(f"Financial Conditions Agent failed: {e}")
        domain_results.financial_analysis = f"Analysis failed: {e}"

    token_usage = _get_token_usage(economic_analysis, initial_history, context)
    for k in ["prompt_tokens", "completion_tokens", "total_tokens"]:
        total_token_usage[k] += token_usage.get(k, 0)

    context.log.info("Step 3/6: Running Commodities Agent...")

    energy_data = get_energy_commodities_data(md)
    input_data = get_input_commodities_data(md)
    agriculture_data = get_agriculture_commodities_data(md)

    estimated_tokens = _estimate_tokens(energy_data + input_data + agriculture_data)
    _check_rate_limit(provider, model_name, estimated_tokens, context)

    initial_history = (
        len(economic_analysis._lm.history) if hasattr(economic_analysis, "_lm") else 0
    )

    try:
        commodities_result = commodities_agent(
            energy_data=energy_data,
            input_commodities_data=input_data,
            agriculture_data=agriculture_data,
            personality=personality,
        )
        domain_results.commodity_analysis = commodities_result.commodity_analysis
        context.log.info("Commodities Agent complete.")
    except Exception as e:
        context.log.error(f"Commodities Agent failed: {e}")
        domain_results.commodity_analysis = f"Analysis failed: {e}"

    token_usage = _get_token_usage(economic_analysis, initial_history, context)
    for k in ["prompt_tokens", "completion_tokens", "total_tokens"]:
        total_token_usage[k] += token_usage.get(k, 0)

    context.log.info("Step 4/6: Running Sector Agent...")

    sector_data = get_sector_data(md)
    correlation_data = get_sector_correlation_data(md)

    estimated_tokens = _estimate_tokens(sector_data + correlation_data)
    _check_rate_limit(provider, model_name, estimated_tokens, context)

    initial_history = (
        len(economic_analysis._lm.history) if hasattr(economic_analysis, "_lm") else 0
    )

    try:
        sector_result = sector_agent(
            sector_data=sector_data,
            correlation_data=correlation_data,
            personality=personality,
        )
        domain_results.sector_analysis = sector_result.sector_analysis
        context.log.info("Sector Agent complete.")
    except Exception as e:
        context.log.error(f"Sector Agent failed: {e}")
        domain_results.sector_analysis = f"Analysis failed: {e}"

    token_usage = _get_token_usage(economic_analysis, initial_history, context)
    for k in ["prompt_tokens", "completion_tokens", "total_tokens"]:
        total_token_usage[k] += token_usage.get(k, 0)

    context.log.info("Step 5/6: Running Market Structure Agent...")

    indices_data = get_major_indices_data(md)
    fixed_income = get_fixed_income_data(md)
    global_markets = get_global_markets_data(md)

    estimated_tokens = _estimate_tokens(indices_data + fixed_income + global_markets)
    _check_rate_limit(provider, model_name, estimated_tokens, context)

    initial_history = (
        len(economic_analysis._lm.history) if hasattr(economic_analysis, "_lm") else 0
    )

    try:
        market_result = market_agent(
            indices_data=indices_data,
            fixed_income_data=fixed_income,
            global_markets_data=global_markets,
            personality=personality,
        )
        domain_results.market_analysis = market_result.market_analysis
        context.log.info("Market Structure Agent complete.")
    except Exception as e:
        context.log.error(f"Market Structure Agent failed: {e}")
        domain_results.market_analysis = f"Analysis failed: {e}"

    token_usage = _get_token_usage(economic_analysis, initial_history, context)
    for k in ["prompt_tokens", "completion_tokens", "total_tokens"]:
        total_token_usage[k] += token_usage.get(k, 0)

    context.log.info("Step 6/6: Running Aggregator Module...")
    context.log.info(f"Domain analysis status: {domain_results.summary()}")

    combined_analyses = (
        domain_results.labor_analysis
        + domain_results.financial_analysis
        + domain_results.commodity_analysis
        + domain_results.sector_analysis
        + domain_results.market_analysis
    )
    estimated_tokens = _estimate_tokens(combined_analyses)
    _check_rate_limit(provider, model_name, estimated_tokens, context)

    initial_history = (
        len(economic_analysis._lm.history) if hasattr(economic_analysis, "_lm") else 0
    )

    try:
        aggregated_result = aggregator(
            labor_analysis=domain_results.labor_analysis,
            financial_analysis=domain_results.financial_analysis,
            commodity_analysis=domain_results.commodity_analysis,
            sector_analysis=domain_results.sector_analysis,
            market_analysis=domain_results.market_analysis,
            personality=personality,
        )
        final_analysis = aggregated_result.economic_state
        context.log.info("Aggregator complete.")
    except Exception as e:
        context.log.error(f"Aggregator failed: {e}")
        final_analysis = f"Aggregation failed: {e}"

    token_usage = _get_token_usage(economic_analysis, initial_history, context)
    for k in ["prompt_tokens", "completion_tokens", "total_tokens"]:
        total_token_usage[k] += token_usage.get(k, 0)

    cost_data = _calculate_cost(
        provider=provider,
        model_name=model_name,
        prompt_tokens=int(total_token_usage.get("prompt_tokens", 0)),
        completion_tokens=int(total_token_usage.get("completion_tokens", 0)),
    )
    total_token_usage.update(cost_data)

    analysis_timestamp = datetime.now()

    sub_agent_results = {
        "analysis_type": "economy_state_v2_sub_agents",
        "analysis_timestamp": analysis_timestamp.isoformat(),
        "analysis_date": analysis_timestamp.strftime("%Y-%m-%d"),
        "analysis_time": analysis_timestamp.strftime("%H:%M:%S"),
        "model_name": model_name,
        "personality": personality,
        "labor_analysis": domain_results.labor_analysis,
        "financial_analysis": domain_results.financial_analysis,
        "commodity_analysis": domain_results.commodity_analysis,
        "sector_analysis": domain_results.sector_analysis,
        "market_analysis": domain_results.market_analysis,
        "dagster_run_id": context.run_id,
    }

    context.log.info("Writing sub-agent results to database...")
    bq.write_results_to_table(
        [sub_agent_results],
        output_table="economy_state_v2_sub_agents",
        if_exists="append",
        context=context,
    )

    aggregated_json = {
        "analysis_type": "economy_state_v2",
        "analysis_content": final_analysis,
        "analysis_timestamp": analysis_timestamp.isoformat(),
        "analysis_date": analysis_timestamp.strftime("%Y-%m-%d"),
        "analysis_time": analysis_timestamp.strftime("%H:%M:%S"),
        "model_name": model_name,
        "personality": personality,
        "data_sources": {
            "sub_agents": [
                "labor_market",
                "financial_conditions",
                "commodities",
                "sectors",
                "market_structure",
            ],
        },
        "dagster_run_id": context.run_id,
        "dagster_asset_key": str(context.asset_key),
    }

    context.log.info("Writing aggregated analysis to database...")
    bq.write_results_to_table(
        [aggregated_json],
        output_table="economy_state_analysis",
        if_exists="append",
        context=context,
    )

    result_metadata = {
        "analysis_completed": True,
        "analysis_timestamp": analysis_timestamp.isoformat(),
        "model_name": model_name,
        "personality": personality,
        "sub_agents_run": 5,
        "sub_agents_successful": sum(
            [
                "failed" not in domain_results.labor_analysis.lower(),
                "failed" not in domain_results.financial_analysis.lower(),
                "failed" not in domain_results.commodity_analysis.lower(),
                "failed" not in domain_results.sector_analysis.lower(),
                "failed" not in domain_results.market_analysis.lower(),
            ]
        ),
        "output_tables": ["economy_state_v2_sub_agents", "economy_state_analysis"],
        "token_usage": total_token_usage,
        "provider": provider,
        "analysis_preview": final_analysis[:500] if final_analysis else "",
    }

    context.log.info(f"V2 Economy state analysis complete: {result_metadata}")
    return dg.MaterializeResult(metadata=result_metadata)
