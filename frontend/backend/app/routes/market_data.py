"""Market data API endpoints."""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from ..auth import verify_api_key
from ..database import execute_query


router = APIRouter(prefix="/api/market-data", tags=["market-data"])

# Category to table name mapping
CATEGORY_TABLES = {
    "currency": {
        "summary": "currency_summary",
        "analysis": "currency_analysis_return"
    },
    "global_markets": {
        "summary": "global_markets_summary",
        "analysis": "global_markets_analysis_return"
    },
    "major_indicies": {
        "summary": "major_indicies_summary",
        "analysis": "major_indicies_analysis_return"
    },
    "us_sector": {
        "summary": "us_sector_summary",
        "analysis": "us_sector_analysis_return"
    }
}


@router.get("/categories")
async def get_categories(api_key: str = Depends(verify_api_key)):
    """Get list of available market data categories."""
    return {
        "categories": list(CATEGORY_TABLES.keys())
    }


@router.get("/summary/{category}")
async def get_summary_data(
    category: str,
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    time_period: Optional[str] = Query(None, description="Filter by time period"),
    limit: Optional[int] = Query(100, ge=1, le=1000, description="Limit number of results"),
    api_key: str = Depends(verify_api_key)
):
    """
    Get summary market data for a specific category.
    
    Categories: currency, global_markets, major_indicies, us_sector
    """
    if category not in CATEGORY_TABLES:
        raise HTTPException(
            status_code=404,
            detail=f"Category '{category}' not found. Available categories: {list(CATEGORY_TABLES.keys())}"
        )
    
    table_name = CATEGORY_TABLES[category]["summary"]
    
    # Build query with optional filters
    query = f"SELECT * FROM {table_name} WHERE 1=1"
    params = {}
    
    if symbol:
        query += " AND symbol = $symbol"
        params["symbol"] = symbol
    
    if time_period:
        query += " AND time_period = $time_period"
        params["time_period"] = time_period
    
    query += " ORDER BY time_period, symbol LIMIT $limit"
    params["limit"] = limit
    
    try:
        results = execute_query(query, params if params else None)
        return {
            "category": category,
            "count": len(results),
            "data": results
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error querying database: {str(e)}"
        )


@router.get("/analysis/{category}")
async def get_analysis_data(
    category: str,
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    limit: Optional[int] = Query(100, ge=1, le=1000, description="Limit number of results"),
    api_key: str = Depends(verify_api_key)
):
    """
    Get analysis return data for a specific category (time-series data).
    
    Categories: currency, global_markets, major_indicies, us_sector
    """
    if category not in CATEGORY_TABLES:
        raise HTTPException(
            status_code=404,
            detail=f"Category '{category}' not found. Available categories: {list(CATEGORY_TABLES.keys())}"
        )
    
    table_name = CATEGORY_TABLES[category]["analysis"]
    
    # Build query with optional filters
    query = f"SELECT * FROM {table_name} WHERE 1=1"
    params = {}
    
    if symbol:
        query += " AND symbol = $symbol"
        params["symbol"] = symbol
    
    query += " ORDER BY month_date DESC, symbol LIMIT $limit"
    params["limit"] = limit
    
    try:
        results = execute_query(query, params if params else None)
        return {
            "category": category,
            "count": len(results),
            "data": results
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error querying database: {str(e)}"
        )


@router.get("/symbols/{category}")
async def get_symbols(
    category: str,
    api_key: str = Depends(verify_api_key)
):
    """Get list of unique symbols for a specific category."""
    if category not in CATEGORY_TABLES:
        raise HTTPException(
            status_code=404,
            detail=f"Category '{category}' not found. Available categories: {list(CATEGORY_TABLES.keys())}"
        )
    
    table_name = CATEGORY_TABLES[category]["summary"]
    
    query = f"SELECT DISTINCT symbol, name, exchange, asset_type FROM {table_name} ORDER BY symbol"
    
    try:
        results = execute_query(query)
        return {
            "category": category,
            "count": len(results),
            "symbols": results
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error querying database: {str(e)}"
        )

