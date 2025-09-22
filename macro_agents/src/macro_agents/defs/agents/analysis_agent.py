import dspy
import polars as pl
import dagster as dg
from typing import List, Dict, Any, Union
from macro_agents.defs.resources.motherduck import MotherDuckResource

def create_motherduck_tools(motherduck_resource: MotherDuckResource) -> List[callable]:
    """Create DSPy tools for MotherDuck database operations"""
    
    def read_from_motherduck(table_name: str, query: str = None) -> Dict[str, Any]:
        """
        Read data from a MotherDuck database table.
        
        Args:
            table_name: Name of the table to read from
            query: Optional SQL query to execute (if None, reads all data from table)
        
        Returns:
            Dictionary with status, data, and metadata
        """
        try:
            conn = motherduck_resource.get_connection(read_only=True)
            
            if query:
                # Execute custom query
                result = conn.execute(query)
                data = result.to_dicts()
            else:
                # Read all data from table
                data = motherduck_resource.read_data(table_name)
            
            conn.close()
            
            return {
                "status": "success",
                "data": data,
                "row_count": len(data),
                "table_name": table_name,
                "query_used": query or f"SELECT * FROM {table_name}"
            }
            
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to read from {table_name}: {str(e)}",
                "data": [],
                "row_count": 0
            }
    
    def upsert_to_motherduck(
        table_name: str, 
        data: List[Dict[str, Any]], 
        key_columns: List[str]
    ) -> Dict[str, Any]:
        """
        Upsert data to a MotherDuck database table.
        
        Args:
            table_name: Name of the table to upsert to
            data: List of dictionaries containing the data to upsert
            key_columns: List of column names that form the primary key for upsert logic
        
        Returns:
            Dictionary with status and metadata about the operation
        """
        try:
            if not data:
                return {
                    "status": "error",
                    "message": "No data provided for upsert",
                    "rows_affected": 0
                }
            
            # Convert list of dicts to Polars DataFrame
            df = pl.DataFrame(data)
            
            # Perform the upsert
            motherduck_resource.upsert_data(table_name, df, key_columns)
            
            return {
                "status": "success",
                "message": f"Successfully upserted {len(data)} rows to {table_name}",
                "rows_affected": len(data),
                "table_name": table_name,
                "key_columns": key_columns
            }
            
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to upsert to {table_name}: {str(e)}",
                "rows_affected": 0
            }
    
    def execute_sql_query(query: str) -> Dict[str, Any]:
        """
        Execute a custom SQL query against the MotherDuck database.
        
        Args:
            query: SQL query to execute
        
        Returns:
            Dictionary with query results and metadata
        """
        try:
            conn = motherduck_resource.get_connection(read_only=True)
            result = conn.execute(query)
            data = result.to_dicts()
            conn.close()
            
            return {
                "status": "success",
                "data": data,
                "row_count": len(data),
                "query": query
            }
            
        except Exception as e:
            return {
                "status": "error",
                "message": f"SQL query failed: {str(e)}",
                "data": [],
                "row_count": 0,
                "query": query
            }
    
    def get_table_schema(table_name: str) -> Dict[str, Any]:
        """
        Get the schema information for a table.
        
        Args:
            table_name: Name of the table to get schema for
        
        Returns:
            Dictionary with table schema information
        """
        try:
            conn = motherduck_resource.get_connection(read_only=True)
            
            # Get column information
            columns_query = f"DESCRIBE {table_name}"
            columns_result = conn.execute(columns_query)
            columns = columns_result.to_dicts()
            
            # Get row count
            count_query = f"SELECT COUNT(*) as row_count FROM {table_name}"
            count_result = conn.execute(count_query)
            row_count = count_result.fetchone()[0]
            
            conn.close()
            
            return {
                "status": "success",
                "table_name": table_name,
                "columns": columns,
                "row_count": row_count
            }
            
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to get schema for {table_name}: {str(e)}",
                "columns": [],
                "row_count": 0
            }
    
    # Return the tools as a list
    return [
        read_from_motherduck,
        upsert_to_motherduck,
        execute_sql_query,
        get_table_schema
    ]

# Example usage in a DSPy agent
class MotherDuckAnalysisAgent:
    """AI agent for analyzing data using MotherDuck database tools"""
    
    def __init__(self, motherduck_resource: MotherDuckResource):
        self.motherduck = motherduck_resource
        self.tools = create_motherduck_tools(motherduck_resource)
        self.agent = self._create_agent()
    
    def _create_agent(self):
        """Create the DSPy ReAct agent with MotherDuck tools"""
        
        # Define the signature for data analysis tasks
        class DataAnalysisSignature(dspy.Signature):
            """Analyze data from MotherDuck database to provide insights and recommendations"""
            
            user_request: str = dspy.InputField(
                desc="The data analysis request from the user"
            )
            trajectory: str = dspy.InputField(
                desc="Previous analysis steps and observations"
            )
            reasoning: str = dspy.OutputField(
                desc="Step-by-step reasoning for the analysis approach"
            )
            analysis_result: str = dspy.OutputField(
                desc="Summary of analysis results and key findings"
            )
        
        # Create the ReAct agent with MotherDuck tools
        agent = dspy.ReAct(DataAnalysisSignature, tools=self.tools, max_iters=10)
        return agent
    
    def analyze_data(self, user_request: str) -> Dict[str, Any]:
        """Main method to analyze data based on user request"""
        try:
            trajectory = ""
            result = self.agent(user_request=user_request, trajectory=trajectory)
            
            return {
                "status": "success",
                "reasoning": result.reasoning,
                "analysis_result": result.analysis_result,
                "trajectory": result.trajectory
            }
            
        except Exception as e:
            return {"status": "error", "message": f"Analysis failed: {str(e)}"}

@dg.asset(
    description="AI-powered data analysis using MotherDuck database tools with focus on leading economic indicators",
    compute_kind="dspy",
    deps=["leading_econ_return_indicator"] 
)
def motherduck_analysis_asset(
    context: dg.AssetExecutionContext, 
    md: MotherDuckResource
) -> Dict[str, Any]:
    """Analyze data using AI agent with MotherDuck tools, focusing on leading economic indicators"""
    
    # Create the analysis agent
    agent = MotherDuckAnalysisAgent(md)
    
    # Example analysis request focused on leading economic indicators
    analysis_request = """
    Please analyze the leading economic return indicators by:
    1. Reading the latest data from leading_econ_return_indicator table
    2. Analyzing correlation patterns between economic changes and future returns
    3. Identifying the strongest leading indicators for different asset classes
    4. Examining quintile performance to understand risk-return characteristics
    5. Providing insights on which economic indicators are most predictive of future market performance
    
    Use the available database tools to gather and analyze the data from the leading economic indicators.
    """
    
    # Run the analysis
    result = agent.analyze_data(analysis_request)
    
    context.log.info(f"Leading economic indicator analysis completed: {result.get('status')}")
    return result
