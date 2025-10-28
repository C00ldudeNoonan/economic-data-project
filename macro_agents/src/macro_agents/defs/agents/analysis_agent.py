import duckdb
import polars as pl
import dspy
from typing import Optional, Dict, Any, List
import io
import json
from datetime import datetime
from pathlib import Path
import dagster as dg
from pydantic import Field

from macro_agents.defs.resources.motherduck import MotherDuckResource


class EconomicAnalysisSignature(dspy.Signature):
    """Analyze the relationship between economic indicator changes and subsequent stock/ETF returns using correlation analysis data."""

    correlation_data: str = dspy.InputField(
        desc="CSV data containing correlation analysis between economic indicators and stock returns"
    )

    analysis: str = dspy.OutputField(
        desc="""Comprehensive analysis including:
        1. Summary of Key Findings (5-10 strongest relationships, unexpected correlations, temporal patterns)
        2. Category-Specific Analysis (most predictive indicators by asset category)
        3. Investment Implications (reliable signals, timing considerations, risk factors)
        4. Data Quality Notes (observation counts, anomalies, additional analysis needs)
        
        Focus on correlations >0.3 absolute value, temporal changes Q1→Q2→Q3, Growth vs Inflation impacts, and return magnitude differences."""
    )


class EconomicAnalysisModule(dspy.Module):
    def __init__(self):
        super().__init__()
        self.analyze = dspy.ChainOfThought(EconomicAnalysisSignature)

    def forward(self, correlation_data: str):
        return self.analyze(correlation_data=correlation_data)


class EconomicAnalyzer(dg.ConfigurableResource):
    """Simplified economic analyzer that uses the enhanced MotherDuck resource."""

    model_name: str = Field(
        default="gpt-4-turbo-preview", description="LLM model to use for analysis"
    )
    openai_api_key: str = Field(description="OpenAI API key for DSPy")

    def setup_for_execution(self, context) -> None:
        """Initialize DSPy when the resource is used."""
        # Initialize DSPy
        lm = dspy.LM(model=self.model_name, api_key=self.openai_api_key)
        dspy.settings.configure(lm=lm)

        # Initialize analyzer
        self._analyzer = EconomicAnalysisModule()
        self._prompt_template = EconomicAnalysisSignature.__doc__

    @property
    def analyzer(self):
        """Get DSPy analyzer."""
        return self._analyzer

    @property
    def prompt_template(self):
        """Get prompt template."""
        return self._prompt_template

    def analyze_by_category(
        self,
        md_resource: MotherDuckResource,
        table_name: str = "leading_econ_return_indicator",
        category_column: str = "category",
        additional_filters: Optional[Dict[str, Any]] = None,
        sample_size: int = 50,
        sampling_strategy: str = "top_correlations",
        context: Optional[dg.AssetExecutionContext] = None,
    ) -> Dict[str, str]:
        """Iterate through each category and analyze separately with sampling."""
        # Get unique categories using the enhanced MotherDuck resource
        categories = md_resource.get_unique_categories(table_name, category_column)

        results = {}
        for category in categories:
            # Build filters
            filters = {category_column: category}
            if additional_filters:
                filters.update(additional_filters)

            if context:
                context.log.info(
                    f"Analyzing {category_column}: {category} (sampling {sample_size} rows with '{sampling_strategy}' strategy)..."
                )

            # Get sampled data using the enhanced MotherDuck resource
            correlation_data = md_resource.query_sampled_data(
                table_name=table_name,
                filters=filters,
                sample_size=sample_size,
                sampling_strategy=sampling_strategy,
            )

            # Run DSPy analysis
            result = self.analyzer(correlation_data=correlation_data)
            results[category] = result.analysis

        return results

    def format_results_as_json(
        self,
        category_results: Dict[str, str],
        sample_size: int,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Format category analysis results as a list of JSON objects."""
        json_results = []
        analysis_timestamp = datetime.now()

        for category, analysis in category_results.items():
            record = {
                "category": category,
                "analysis": analysis,
                "num_samples": sample_size,
                "model_name": self.model_name,
                "prompt_template": self.prompt_template,
                "analysis_timestamp": analysis_timestamp.isoformat(),
                "analysis_date": analysis_timestamp.strftime("%Y-%m-%d"),
                "analysis_time": analysis_timestamp.strftime("%H:%M:%S"),
            }

            # Add metadata if provided
            if metadata:
                record.update(metadata)

            json_results.append(record)

        return json_results


@dg.asset(
    kinds={"dspy", "duckdb"},
    group_name="analysis",
    description="Analyze economic indicators by sector using DSPy and write results to MotherDuck",
)
def sector_inflation_analysis(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    analyzer: EconomicAnalyzer,
) -> Dict[str, Any]:
    """
    Asset that performs economic analysis by sector and saves results to MotherDuck.

    Returns:
        Dictionary with analysis metadata and result counts
    """
    context.log.info("Starting sector inflation analysis...")

    # Configuration
    source_table = "leading_econ_return_indicator"
    output_table = "economic_analysis_results"
    sample_size = 50
    sampling_strategy = "top_correlations"

    # Run analysis by category
    context.log.info(f"Analyzing data from {source_table}...")
    category_results = analyzer.analyze_by_category(
        md_resource=md,
        table_name=source_table,
        category_column="category",
        additional_filters=None,  # Can be configured to filter by economic_category
        sample_size=sample_size,
        sampling_strategy=sampling_strategy,
        context=context,
    )

    # Format as JSON
    context.log.info("Formatting results as JSON...")
    json_results = analyzer.format_results_as_json(
        category_results,
        sample_size=sample_size,
        metadata={
            "analysis_type": "sector_inflation",
            "economic_category": "All",  # Add this line
            "source_table": source_table,
            "sampling_strategy": sampling_strategy,
            "dagster_run_id": context.run_id,
            "dagster_asset_key": str(context.asset_key),
        },
    )

    # Write to MotherDuck using the enhanced resource
    context.log.info(f"Writing results to {output_table}...")
    md.write_results_to_table(
        json_results, output_table=output_table, if_exists="append", context=context
    )

    # Return metadata
    result_metadata = {
        "num_categories_analyzed": len(category_results),
        "num_records_written": len(json_results),
        "output_table": output_table,
        "sample_size": sample_size,
        "model_name": analyzer.model_name,
        "analysis_timestamp": json_results[0]["analysis_timestamp"]
        if json_results
        else None,
    }

    context.log.info(f"Analysis complete: {result_metadata}")
    return result_metadata


@dg.asset(
    kinds={"dspy", "duckdb"},
    group_name="analysis",
    description="Analyze inflation-specific economic indicators by sector using DSPy",
)
def sector_inflation_specific_analysis(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    analyzer: EconomicAnalyzer,
) -> Dict[str, Any]:
    """
    Asset that performs economic analysis specifically for inflation indicators.

    Returns:
        Dictionary with analysis metadata and result counts
    """
    context.log.info("Starting sector inflation-specific analysis...")

    # Configuration
    source_table = "leading_econ_return_indicator"
    output_table = "economic_analysis_results"
    sample_size = 30
    sampling_strategy = "top_correlations"

    # Run analysis by category, filtered for inflation
    context.log.info(f"Analyzing inflation indicators from {source_table}...")
    category_results = analyzer.analyze_by_category(
        md_resource=md,
        table_name=source_table,
        category_column="category",
        additional_filters={"economic_category": "Inflation"},
        sample_size=sample_size,
        sampling_strategy=sampling_strategy,
        context=context,
    )

    # Format as JSON
    context.log.info("Formatting results as JSON...")
    json_results = analyzer.format_results_as_json(
        category_results,
        sample_size=sample_size,
        metadata={
            "analysis_type": "inflation_specific",
            "economic_category": "Inflation",
            "source_table": source_table,
            "sampling_strategy": sampling_strategy,
            "dagster_run_id": context.run_id,
            "dagster_asset_key": str(context.asset_key),
        },
    )

    # Write to MotherDuck using the enhanced resource
    context.log.info(f"Writing results to {output_table}...")
    md.write_results_to_table(
        json_results, output_table=output_table, if_exists="append", context=context
    )

    # Return metadata
    result_metadata = {
        "num_categories_analyzed": len(category_results),
        "num_records_written": len(json_results),
        "output_table": output_table,
        "sample_size": sample_size,
        "model_name": analyzer.model_name,
        "analysis_timestamp": json_results[0]["analysis_timestamp"]
        if json_results
        else None,
        "filter_applied": "economic_category = Inflation",
    }

    context.log.info(f"Analysis complete: {result_metadata}")
    return result_metadata
