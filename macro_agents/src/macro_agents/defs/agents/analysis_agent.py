import duckdb
import polars as pl
import dspy
from typing import Optional, Dict, Any, List
import io
import json
from datetime import datetime

from dagster import (
    asset,
    AssetExecutionContext,
    ConfigurableResource,
    Definitions,
    EnvVar,
)
from pydantic import Field


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


class DuckDBEconomicAnalyzer(ConfigurableResource):
    """Dagster resource for economic analysis using DuckDB and DSPy."""
    
    motherduck_token: str = Field(
        description="MotherDuck authentication token"
    )
    database_name: str = Field(
        default="econ_agent",
        description="MotherDuck database name"
    )
    model_name: str = Field(
        default="gpt-4-turbo-preview",
        description="LLM model to use for analysis"
    )
    openai_api_key: str = Field(
        description="OpenAI API key for DSPy"
    )
    
    def setup_for_execution(self, context) -> None:
        """Initialize connections when the resource is used."""
        # Setup DuckDB connection
        self._conn = duckdb.connect(
            f"md:{self.database_name}?motherduck_token={self.motherduck_token}"
        )
        
        # Initialize DSPy
        lm = dspy.LM(model=self.model_name, api_key=self.openai_api_key)
        dspy.settings.configure(lm=lm)
        
        # Initialize analyzer
        self._analyzer = EconomicAnalysisModule()
        self._prompt_template = EconomicAnalysisSignature.__doc__
    
    def teardown_after_execution(self, context) -> None:
        """Close connections when done."""
        if hasattr(self, '_conn'):
            self._conn.close()
    
    @property
    def conn(self):
        """Get DuckDB connection."""
        return self._conn
    
    @property
    def analyzer(self):
        """Get DSPy analyzer."""
        return self._analyzer
    
    @property
    def prompt_template(self):
        """Get prompt template."""
        return self._prompt_template
    
    def get_unique_categories(self, 
                            table_name: str = "leading_econ_return_indicator",
                            column: str = "category") -> List[str]:
        """Get unique values from a specified column."""
        query = f"SELECT DISTINCT {column} FROM {table_name} WHERE {column} IS NOT NULL ORDER BY {column}"
        result = self.conn.execute(query).pl()
        return result[column].to_list()
    
    def query_sampled_data(self,
                          table_name: str = "leading_econ_return_indicator",
                          filters: Optional[Dict[str, Any]] = None,
                          sample_size: int = 50,
                          sampling_strategy: str = "top_correlations") -> str:
        """Query sampled correlation data from DuckDB."""
        # Build base WHERE clause
        where_conditions = []
        if filters:
            for column, value in filters.items():
                if isinstance(value, str):
                    where_conditions.append(f"{column} = '{value}'")
                elif isinstance(value, list):
                    value_str = "', '".join(str(v) for v in value)
                    where_conditions.append(f"{column} IN ('{value_str}')")
                else:
                    where_conditions.append(f"{column} = {value}")
        
        where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Build query based on sampling strategy
        if sampling_strategy == "top_correlations":
            query = f"""
                SELECT * FROM {table_name}
                {where_clause}
                ORDER BY GREATEST(
                    ABS(COALESCE(correlation_econ_vs_q1_returns, 0)),
                    ABS(COALESCE(correlation_econ_vs_q2_returns, 0)),
                    ABS(COALESCE(correlation_econ_vs_q3_returns, 0))
                ) DESC
                LIMIT {sample_size}
            """
        elif sampling_strategy == "random":
            query = f"""
                SELECT * FROM {table_name}
                {where_clause}
                ORDER BY RANDOM()
                LIMIT {sample_size}
            """
        elif sampling_strategy == "mixed":
            half_size = sample_size // 2
            query = f"""
                (
                    SELECT * FROM {table_name}
                    {where_clause}
                    ORDER BY GREATEST(
                        ABS(COALESCE(correlation_econ_vs_q1_returns, 0)),
                        ABS(COALESCE(correlation_econ_vs_q2_returns, 0)),
                        ABS(COALESCE(correlation_econ_vs_q3_returns, 0))
                    ) DESC
                    LIMIT {half_size}
                )
                UNION ALL
                (
                    SELECT * FROM {table_name}
                    {where_clause}
                    ORDER BY RANDOM()
                    LIMIT {sample_size - half_size}
                )
            """
        else:
            raise ValueError(f"Unknown sampling strategy: {sampling_strategy}")
        
        # Execute query and convert to CSV
        df = self.conn.execute(query).pl()
        csv_buffer = io.StringIO()
        df.write_csv(csv_buffer)
        return csv_buffer.getvalue()
    
    def analyze_by_category(self, 
                          table_name: str = "leading_econ_return_indicator",
                          category_column: str = "category",
                          additional_filters: Optional[Dict[str, Any]] = None,
                          sample_size: int = 50,
                          sampling_strategy: str = "top_correlations",
                          context: Optional[AssetExecutionContext] = None) -> Dict[str, str]:
        """Iterate through each category and analyze separately with sampling."""
        # Get unique categories
        categories = self.get_unique_categories(table_name, category_column)
        
        results = {}
        for category in categories:
            # Build filters
            filters = {category_column: category}
            if additional_filters:
                filters.update(additional_filters)
            
            if context:
                context.log.info(f"Analyzing {category_column}: {category} (sampling {sample_size} rows with '{sampling_strategy}' strategy)...")
            
            # Get sampled data
            correlation_data = self.query_sampled_data(
                table_name=table_name,
                filters=filters,
                sample_size=sample_size,
                sampling_strategy=sampling_strategy
            )
            
            # Run DSPy analysis
            result = self.analyzer(correlation_data=correlation_data)
            results[category] = result.analysis
        
        return results
    
    def format_results_as_json(self, 
                              category_results: Dict[str, str],
                              sample_size: int,
                              metadata: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
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
                "analysis_time": analysis_timestamp.strftime("%H:%M:%S")
            }
            
            # Add metadata if provided
            if metadata:
                record.update(metadata)
            
            json_results.append(record)
        
        return json_results
    
    def write_results_to_table(self,
                              json_results: List[Dict[str, Any]],
                              output_table: str,
                              if_exists: str = "append",
                              context: Optional[AssetExecutionContext] = None) -> None:
        """Write JSON results to a MotherDuck table."""
        # Convert to Polars DataFrame
        df = pl.DataFrame(json_results)
        
        # Check if table exists
        try:
            self.conn.execute(f"SELECT 1 FROM {output_table} LIMIT 1")
            table_exists = True
        except:
            table_exists = False
        
        if table_exists and if_exists == "fail":
            raise ValueError(f"Table {output_table} already exists")
        
        # Write to table
        if if_exists == "replace" or not table_exists:
            # Create or replace table
            self.conn.execute(f"CREATE OR REPLACE TABLE {output_table} AS SELECT * FROM df")
            if context:
                context.log.info(f"Created table {output_table} with {len(df)} records")
        else:  # append
            # Insert into existing table
            self.conn.execute(f"INSERT INTO {output_table} SELECT * FROM df")
            if context:
                context.log.info(f"Appended {len(df)} records to {output_table}")


@asset(
    kinds={"dspy", "analysis"},
    description="Analyze economic indicators by sector using DSPy and write results to MotherDuck",
    compute_kind="dspy"
)
def sector_inflation_analysis(
    context: AssetExecutionContext,
    duckdb_analyzer: DuckDBEconomicAnalyzer
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
    category_results = duckdb_analyzer.analyze_by_category(
        table_name=source_table,
        category_column="category",
        additional_filters=None,  # Can be configured to filter by economic_category
        sample_size=sample_size,
        sampling_strategy=sampling_strategy,
        context=context
    )
    
    # Format as JSON
    context.log.info("Formatting results as JSON...")
    json_results = duckdb_analyzer.format_results_as_json(
        category_results,
        sample_size=sample_size,
        metadata={
            "analysis_type": "sector_inflation",
            "source_table": source_table,
            "sampling_strategy": sampling_strategy,
            "dagster_run_id": context.run_id,
            "dagster_asset_key": str(context.asset_key)
        }
    )
    
    # Write to MotherDuck
    context.log.info(f"Writing results to {output_table}...")
    duckdb_analyzer.write_results_to_table(
        json_results,
        output_table=output_table,
        if_exists="append",
        context=context
    )
    
    # Return metadata
    result_metadata = {
        "num_categories_analyzed": len(category_results),
        "num_records_written": len(json_results),
        "output_table": output_table,
        "sample_size": sample_size,
        "model_name": duckdb_analyzer.model_name,
        "analysis_timestamp": json_results[0]["analysis_timestamp"] if json_results else None
    }
    
    context.log.info(f"Analysis complete! Analyzed {result_metadata['num_categories_analyzed']} categories")
    
    return result_metadata


@asset(
    kinds={"dspy", "analysis"},
    description="Analyze inflation-specific economic indicators by sector using DSPy",
    compute_kind="dspy"
)
def sector_inflation_specific_analysis(
    context: AssetExecutionContext,
    duckdb_analyzer: DuckDBEconomicAnalyzer
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
    category_results = duckdb_analyzer.analyze_by_category(
        table_name=source_table,
        category_column="category",
        additional_filters={"economic_category": "Inflation"},
        sample_size=sample_size,
        sampling_strategy=sampling_strategy,
        context=context
    )
    
    # Format as JSON
    context.log.info("Formatting results as JSON...")
    json_results = duckdb_analyzer.format_results_as_json(
        category_results,
        sample_size=sample_size,
        metadata={
            "analysis_type": "inflation_specific",
            "economic_category": "Inflation",
            "source_table": source_table,
            "sampling_strategy": sampling_strategy,
            "dagster_run_id": context.run_id,
            "dagster_asset_key": str(context.asset_key)
        }
    )
    
    # Write to MotherDuck
    context.log.info(f"Writing results to {output_table}...")
    duckdb_analyzer.write_results_to_table(
        json_results,
        output_table=output_table,
        if_exists="append",
        context=context
    )
    
    # Return metadata
    result_metadata = {
        "num_categories_analyzed": len(category_results),
        "num_records_written": len(json_results),
        "output_table": output_table,
        "sample_size": sample_size,
        "model_name": duckdb_analyzer.model_name,
        "analysis_timestamp": json_results[0]["analysis_timestamp"] if json_results else None,
        "filter_applied": "economic_category = Inflation"
    }
    
    context.log.info(f"Analysis complete! Analyzed {result_metadata['num_categories_analyzed']} categories")
    
    return result_metadata


# Define the Dagster definitions
defs = Definitions(
    assets=[
        sector_inflation_analysis,
        sector_inflation_specific_analysis
    ],
    resources={
        "duckdb_analyzer": DuckDBEconomicAnalyzer(
            motherduck_token=EnvVar("MOTHERDUCK_TOKEN"),
            database_name="econ_agent",
            model_name=EnvVar.str("MODEL_NAME", default="gpt-4-turbo-preview"),
            openai_api_key=EnvVar("OPENAI_API_KEY")
        )
    }
)