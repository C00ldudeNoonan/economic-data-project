"""
Scheduled execution configuration for economic analysis assets.
"""

import dagster as dg
from dagster import ScheduleDefinition, DefaultSensorStatus
from datetime import datetime, time
import pytz


# Monthly schedule for sector inflation analysis (1st of each month at 9 AM EST)
monthly_sector_analysis_schedule = ScheduleDefinition(
    name="monthly_sector_analysis_schedule",
    cron_schedule="0 9 1 * *",  # 9 AM EST on the 1st of every month
    execution_timezone="America/New_York",
    job_name="monthly_sector_analysis_job",
    description="Monthly sector inflation analysis on the 1st of each month at 9 AM EST"
)

# Weekly schedule for economic cycle analysis (Mondays at 8 AM EST)
weekly_cycle_analysis_schedule = ScheduleDefinition(
    name="weekly_cycle_analysis_schedule", 
    cron_schedule="0 8 * * 1",  # 8 AM EST every Monday
    execution_timezone="America/New_York",
    job_name="weekly_cycle_analysis_job",
    description="Weekly economic cycle analysis every Monday at 8 AM EST"
)

# Weekly schedule for asset allocation recommendations (Mondays at 9 AM EST)
weekly_allocation_schedule = ScheduleDefinition(
    name="weekly_allocation_schedule",
    cron_schedule="0 9 * * 1",  # 9 AM EST every Monday (1 hour after cycle analysis)
    execution_timezone="America/New_York", 
    job_name="weekly_allocation_job",
    description="Weekly asset allocation recommendations every Monday at 9 AM EST"
)

# Daily monitoring schedule (weekdays at 6 AM EST)
daily_monitoring_schedule = ScheduleDefinition(
    name="daily_monitoring_schedule",
    cron_schedule="0 6 * * 1-5",  # 6 AM EST Monday through Friday
    execution_timezone="America/New_York",
    job_name="daily_monitoring_job", 
    description="Daily economic monitoring and alerts on weekdays at 6 AM EST"
)

# Create sensor for conditional execution based on data freshness
def create_data_freshness_sensor():
    """Create a sensor that triggers analysis based on data freshness."""
    
    @dg.sensor(
        name="data_freshness_sensor",
        description="Triggers analysis when new economic data is available",
        default_status=DefaultSensorStatus.RUNNING
    )
    def data_freshness_sensor(context):
        """Sensor that checks for fresh economic data and triggers appropriate analysis."""
        
        # Check if we have fresh FRED data (this would need to be implemented based on your data pipeline)
        # For now, we'll use a simple time-based trigger
        current_time = datetime.now(pytz.timezone("America/New_York"))
        
        # Check if it's Monday morning (for weekly analysis)
        if current_time.weekday() == 0 and current_time.hour == 8:
            yield dg.RunRequest(
                run_key=f"weekly_cycle_analysis_{current_time.strftime('%Y%m%d_%H%M')}",
                tags={"trigger": "weekly_schedule", "analysis_type": "cycle"}
            )
            
        # Check if it's the 1st of the month (for monthly analysis)
        if current_time.day == 1 and current_time.hour == 9:
            yield dg.RunRequest(
                run_key=f"monthly_sector_analysis_{current_time.strftime('%Y%m%d_%H%M')}",
                tags={"trigger": "monthly_schedule", "analysis_type": "sector"}
            )
    
    return data_freshness_sensor


# Create job definitions for scheduled execution
def create_scheduled_jobs():
    """Create job definitions for scheduled execution."""
    
    # Monthly sector analysis job
    monthly_sector_job = dg.define_asset_job(
        name="monthly_sector_analysis_job",
        selection=["sector_inflation_analysis", "sector_inflation_specific_analysis"],
        description="Monthly sector inflation analysis job"
    )
    
    # Weekly cycle analysis job
    weekly_cycle_job = dg.define_asset_job(
        name="weekly_cycle_analysis_job", 
        selection=["economic_cycle_analysis"],
        description="Weekly economic cycle analysis job"
    )
    
    # Weekly allocation job (depends on cycle analysis)
    weekly_allocation_job = dg.define_asset_job(
        name="weekly_allocation_job",
        selection=["asset_allocation_recommendations", "custom_asset_allocation"],
        description="Weekly asset allocation recommendations job"
    )
    
    # Daily monitoring job
    daily_monitoring_job = dg.define_asset_job(
        name="daily_monitoring_job",
        selection=["economic_monitoring_alerts"],
        description="Daily economic monitoring job"
    )
    
    return {
        "monthly_sector_job": monthly_sector_job,
        "weekly_cycle_job": weekly_cycle_job, 
        "weekly_allocation_job": weekly_allocation_job,
        "daily_monitoring_job": daily_monitoring_job
    }


# Create all schedules
schedules = [
    monthly_sector_analysis_schedule,
    weekly_cycle_analysis_schedule,
    weekly_allocation_schedule,
    daily_monitoring_schedule
]

# Create sensors
sensors = [
    create_data_freshness_sensor()
]

# Create jobs
jobs = create_scheduled_jobs()
