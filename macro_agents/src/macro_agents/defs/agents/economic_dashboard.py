import polars as pl
from typing import Optional, Dict, Any, List
import json
from datetime import datetime
import dagster as dg

from macro_agents.defs.resources.motherduck import MotherDuckResource


@dg.asset(
    kinds={"analysis", "dashboard", "summary", "scheduled", "daily"},
    description="Daily comprehensive economic dashboard combining all analyses and recommendations",
    compute_kind="python",
    deps=["economic_cycle_analysis", "asset_allocation_recommendations", "integrated_economic_analysis"],
    tags={"schedule": "daily", "execution_time": "weekdays_6am_est", "analysis_type": "dashboard"},
)
def economic_dashboard(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
) -> Dict[str, Any]:
    """
    Asset that creates a comprehensive economic dashboard with all analyses and recommendations.
    
    Scheduled to run daily on weekdays at 6 AM EST.
    
    Returns:
        Dictionary with dashboard data and summary statistics
    """
    context.log.info("Building comprehensive economic dashboard...")
    
    # Add scheduling metadata
    context.log.info(f"Dashboard execution triggered at: {datetime.now()}")
    context.log.info("This is a daily scheduled dashboard update")
    
    # Get latest analysis from each table
    dashboard_data = {}
    
    # Get economic cycle analysis
    cycle_query = """
    SELECT analysis_content, analysis_timestamp
    FROM economic_cycle_analysis
    WHERE analysis_type = 'economic_cycle'
    ORDER BY analysis_timestamp DESC
    LIMIT 1
    """
    
    cycle_df = md.execute_query(cycle_query, read_only=True)
    if not cycle_df.is_empty():
        dashboard_data["economic_cycle"] = {
            "analysis": cycle_df[0, "analysis_content"],
            "timestamp": cycle_df[0, "analysis_timestamp"]
        }
    
    # Get market trend analysis
    trend_query = """
    SELECT analysis_content, analysis_timestamp
    FROM economic_cycle_analysis
    WHERE analysis_type = 'market_trends'
    ORDER BY analysis_timestamp DESC
    LIMIT 1
    """
    
    trend_df = md.execute_query(trend_query, read_only=True)
    if not trend_df.is_empty():
        dashboard_data["market_trends"] = {
            "analysis": trend_df[0, "analysis_content"],
            "timestamp": trend_df[0, "analysis_timestamp"]
        }
    
    # Get asset allocation recommendations
    allocation_query = """
    SELECT analysis_content, analysis_timestamp
    FROM asset_allocation_recommendations
    ORDER BY analysis_timestamp DESC
    LIMIT 1
    """
    
    allocation_df = md.execute_query(allocation_query, read_only=True)
    if not allocation_df.is_empty():
        dashboard_data["asset_allocation"] = {
            "recommendations": allocation_df[0, "analysis_content"],
            "timestamp": allocation_df[0, "analysis_timestamp"]
        }
    
    # Get integrated analysis
    integrated_query = """
    SELECT analysis_content, analysis_timestamp
    FROM integrated_economic_analysis
    ORDER BY analysis_timestamp DESC
    LIMIT 1
    """
    
    integrated_df = md.execute_query(integrated_query, read_only=True)
    if not integrated_df.is_empty():
        dashboard_data["integrated_analysis"] = {
            "analysis": integrated_df[0, "analysis_content"],
            "timestamp": integrated_df[0, "analysis_timestamp"]
        }
    
    # Get latest economic data summary
    economic_summary_query = """
    SELECT 
        COUNT(*) as total_indicators,
        COUNT(CASE WHEN pct_change_3m > 0 THEN 1 END) as positive_3m,
        COUNT(CASE WHEN pct_change_6m > 0 THEN 1 END) as positive_6m,
        COUNT(CASE WHEN pct_change_1y > 0 THEN 1 END) as positive_1y,
        AVG(pct_change_3m) as avg_change_3m,
        AVG(pct_change_6m) as avg_change_6m,
        AVG(pct_change_1y) as avg_change_1y
    FROM fred_series_latest_aggregates
    WHERE current_value IS NOT NULL
    """
    
    economic_summary_df = md.execute_query(economic_summary_query, read_only=True)
    if not economic_summary_df.is_empty():
        dashboard_data["economic_data_summary"] = economic_summary_df[0].to_dict()
    
    # Get market performance summary
    market_summary_query = """
    SELECT 
        asset_type,
        time_period,
        COUNT(*) as asset_count,
        AVG(total_return_pct) as avg_return,
        AVG(volatility_pct) as avg_volatility,
        AVG(win_rate_pct) as avg_win_rate
    FROM us_sector_summary
    WHERE time_period IN ('12_weeks', '6_months', '1_year')
    GROUP BY asset_type, time_period
    ORDER BY asset_type, time_period
    """
    
    market_summary_df = md.execute_query(market_summary_query, read_only=True)
    if not market_summary_df.is_empty():
        dashboard_data["market_performance_summary"] = market_summary_df.to_dicts()
    
    # Create dashboard summary
    dashboard_timestamp = datetime.now()
    dashboard_summary = {
        "dashboard_timestamp": dashboard_timestamp.isoformat(),
        "dashboard_date": dashboard_timestamp.strftime("%Y-%m-%d"),
        "dashboard_time": dashboard_timestamp.strftime("%H:%M:%S"),
        "analysis_components": list(dashboard_data.keys()),
        "data_freshness": {
            "economic_cycle": dashboard_data.get("economic_cycle", {}).get("timestamp"),
            "market_trends": dashboard_data.get("market_trends", {}).get("timestamp"),
            "asset_allocation": dashboard_data.get("asset_allocation", {}).get("timestamp"),
            "integrated_analysis": dashboard_data.get("integrated_analysis", {}).get("timestamp")
        },
        "schedule_type": "daily",
        "execution_date": dashboard_timestamp.strftime("%Y-%m-%d"),
    }
    
    # Combine all data
    full_dashboard = {
        "dashboard_summary": dashboard_summary,
        "analysis_data": dashboard_data
    }
    
    # Write dashboard to database
    context.log.info("Writing dashboard data to database...")
    md.write_results_to_table(
        [full_dashboard],
        output_table="economic_dashboard",
        if_exists="replace",  # Replace to get latest dashboard
        context=context
    )
    
    # Return summary
    result_metadata = {
        "dashboard_created": True,
        "dashboard_timestamp": dashboard_summary["dashboard_timestamp"],
        "analysis_components": len(dashboard_data),
        "output_table": "economic_dashboard",
        "records_written": 1,
        "schedule_type": "daily",
        "execution_date": dashboard_timestamp.strftime("%Y-%m-%d"),
    }
    
    context.log.info(f"Daily economic dashboard complete: {result_metadata}")
    return result_metadata


@dg.asset(
    kinds={"analysis", "monitoring", "scheduled", "daily"},
    description="Daily monitoring of key economic indicators and market metrics for early warning signals",
    compute_kind="python",
    tags={"schedule": "daily", "execution_time": "weekdays_6am_est", "analysis_type": "monitoring"},
)
def economic_monitoring_alerts(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
) -> Dict[str, Any]:
    """
    Asset that monitors key economic indicators and market metrics for early warning signals.
    
    Scheduled to run daily on weekdays at 6 AM EST.
    
    Returns:
        Dictionary with monitoring alerts and key metrics
    """
    context.log.info("Running daily economic monitoring analysis...")
    
    # Add scheduling metadata
    context.log.info(f"Monitoring execution triggered at: {datetime.now()}")
    context.log.info("This is a daily scheduled monitoring analysis")
    
    alerts = []
    key_metrics = {}
    
    # Monitor economic indicators for significant changes
    economic_alerts_query = """
    SELECT 
        series_name,
        current_value,
        pct_change_3m,
        pct_change_6m,
        pct_change_1y,
        CASE 
            WHEN ABS(pct_change_3m) > 0.1 THEN 'High 3M Change'
            WHEN ABS(pct_change_6m) > 0.2 THEN 'High 6M Change'
            WHEN ABS(pct_change_1y) > 0.3 THEN 'High 1Y Change'
            ELSE NULL
        END as alert_type
    FROM fred_series_latest_aggregates
    WHERE current_value IS NOT NULL
    AND (ABS(pct_change_3m) > 0.1 OR ABS(pct_change_6m) > 0.2 OR ABS(pct_change_1y) > 0.3)
    ORDER BY ABS(pct_change_3m) DESC
    """
    
    economic_alerts_df = md.execute_query(economic_alerts_query, read_only=True)
    if not economic_alerts_df.is_empty():
        for row in economic_alerts_df.iter_rows(named=True):
            if row["alert_type"]:
                alerts.append({
                    "type": "economic_indicator",
                    "alert_level": row["alert_type"],
                    "series_name": row["series_name"],
                    "current_value": row["current_value"],
                    "pct_change_3m": row["pct_change_3m"],
                    "pct_change_6m": row["pct_change_6m"],
                    "pct_change_1y": row["pct_change_1y"]
                })
    
    # Monitor market performance for extreme moves
    market_alerts_query = """
    SELECT 
        symbol,
        asset_type,
        time_period,
        total_return_pct,
        volatility_pct,
        win_rate_pct,
        CASE 
            WHEN total_return_pct > 20 THEN 'High Positive Return'
            WHEN total_return_pct < -20 THEN 'High Negative Return'
            WHEN volatility_pct > 30 THEN 'High Volatility'
            WHEN win_rate_pct < 30 THEN 'Low Win Rate'
            ELSE NULL
        END as alert_type
    FROM us_sector_summary
    WHERE time_period = '12_weeks'
    AND (total_return_pct > 20 OR total_return_pct < -20 OR volatility_pct > 30 OR win_rate_pct < 30)
    ORDER BY ABS(total_return_pct) DESC
    """
    
    market_alerts_df = md.execute_query(market_alerts_query, read_only=True)
    if not market_alerts_df.is_empty():
        for row in market_alerts_df.iter_rows(named=True):
            if row["alert_type"]:
                alerts.append({
                    "type": "market_performance",
                    "alert_level": row["alert_type"],
                    "symbol": row["symbol"],
                    "asset_type": row["asset_type"],
                    "time_period": row["time_period"],
                    "total_return_pct": row["total_return_pct"],
                    "volatility_pct": row["volatility_pct"],
                    "win_rate_pct": row["win_rate_pct"]
                })
    
    # Calculate key metrics
    key_metrics_query = """
    SELECT 
        'economic_indicators' as metric_type,
        COUNT(*) as total_count,
        COUNT(CASE WHEN pct_change_3m > 0 THEN 1 END) as positive_3m,
        COUNT(CASE WHEN pct_change_6m > 0 THEN 1 END) as positive_6m,
        AVG(pct_change_3m) as avg_change_3m,
        AVG(pct_change_6m) as avg_change_6m
    FROM fred_series_latest_aggregates
    WHERE current_value IS NOT NULL
    
    UNION ALL
    
    SELECT 
        'market_performance' as metric_type,
        COUNT(*) as total_count,
        COUNT(CASE WHEN total_return_pct > 0 THEN 1 END) as positive_3m,
        COUNT(CASE WHEN total_return_pct > 5 THEN 1 END) as positive_6m,
        AVG(total_return_pct) as avg_change_3m,
        AVG(volatility_pct) as avg_change_6m
    FROM us_sector_summary
    WHERE time_period = '12_weeks'
    """
    
    key_metrics_df = md.execute_query(key_metrics_query, read_only=True)
    if not key_metrics_df.is_empty():
        for row in key_metrics_df.iter_rows(named=True):
            key_metrics[row["metric_type"]] = {
                "total_count": row["total_count"],
                "positive_indicators": row["positive_3m"],
                "strong_indicators": row["positive_6m"],
                "average_change": row["avg_change_3m"],
                "average_volatility": row["avg_change_6m"]
            }
    
    # Create monitoring summary
    monitoring_timestamp = datetime.now()
    monitoring_summary = {
        "monitoring_timestamp": monitoring_timestamp.isoformat(),
        "total_alerts": len(alerts),
        "alert_breakdown": {
            "economic_indicators": len([a for a in alerts if a["type"] == "economic_indicator"]),
            "market_performance": len([a for a in alerts if a["type"] == "market_performance"])
        },
        "key_metrics": key_metrics,
        "alerts": alerts,
        "schedule_type": "daily",
        "execution_date": monitoring_timestamp.strftime("%Y-%m-%d"),
    }
    
    # Write monitoring data to database
    context.log.info("Writing monitoring alerts to database...")
    md.write_results_to_table(
        [monitoring_summary],
        output_table="economic_monitoring_alerts",
        if_exists="replace",
        context=context
    )
    
    # Return summary
    result_metadata = {
        "monitoring_completed": True,
        "monitoring_timestamp": monitoring_summary["monitoring_timestamp"],
        "total_alerts": len(alerts),
        "output_table": "economic_monitoring_alerts",
        "records_written": 1,
        "schedule_type": "daily",
        "execution_date": monitoring_timestamp.strftime("%Y-%m-%d"),
    }
    
    context.log.info(f"Daily economic monitoring complete: {result_metadata}")
    return result_metadata
