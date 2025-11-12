"""Database connection utilities for MotherDuck."""
import duckdb
from typing import Optional
from .config import settings


def get_motherduck_connection() -> duckdb.DuckDBPyConnection:
    """
    Create and return a connection to MotherDuck.
    
    Returns:
        duckdb.DuckDBPyConnection: Database connection object
    """
    # Build connection string
    if settings.motherduck_database:
        # Production: connect to specific database
        conn_str = f"md:{settings.motherduck_database}?motherduck_token={settings.motherduck_token}"
    else:
        # Default: connect to MotherDuck cloud
        conn_str = f"md:?motherduck_token={settings.motherduck_token}"
    
    try:
        conn = duckdb.connect(conn_str)
        
        # Set schema if specified
        if settings.motherduck_database and settings.motherduck_schema:
            try:
                conn.execute(f"USE {settings.motherduck_database}.{settings.motherduck_schema}")
            except Exception:
                # Schema might not exist or already in use
                pass
        
        return conn
    except Exception as e:
        raise ConnectionError(f"Failed to connect to MotherDuck: {str(e)}")


def execute_query(query: str, params: Optional[dict] = None) -> list:
    """
    Execute a SQL query and return results as a list of dictionaries.
    
    Args:
        query: SQL query string (use $param_name for named parameters)
        params: Optional query parameters dictionary
        
    Returns:
        list: Query results as list of dictionaries
    """
    conn = None
    try:
        conn = get_motherduck_connection()
        
        # Execute query with or without parameters
        if params:
            # DuckDB supports named parameters with $param_name syntax
            result = conn.execute(query, params).fetchall()
        else:
            result = conn.execute(query).fetchall()
        
        # Get column names from the result description
        columns = [desc[0] for desc in conn.description] if conn.description else []
        
        # Convert to list of dictionaries
        return [dict(zip(columns, row)) for row in result]
    finally:
        if conn:
            conn.close()

