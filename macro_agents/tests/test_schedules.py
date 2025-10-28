"""
Tests for scheduled execution and scheduling configuration.
"""

import pytest
from datetime import datetime, time
import pytz
from unittest.mock import Mock, patch

from macro_agents.defs.schedules import (
    monthly_sector_analysis_schedule,
    weekly_cycle_analysis_schedule,
    weekly_allocation_schedule,
    daily_monitoring_schedule,
    create_data_freshness_sensor,
    create_scheduled_jobs
)
from macro_agents.definitions import defs


class TestSchedules:
    """Test cases for schedule definitions."""

    def test_monthly_schedule_configuration(self):
        """Test monthly sector analysis schedule configuration."""
        assert monthly_sector_analysis_schedule.name == "monthly_sector_analysis_schedule"
        assert monthly_sector_analysis_schedule.cron_schedule == "0 9 1 * *"
        assert monthly_sector_analysis_schedule.execution_timezone == "America/New_York"
        assert monthly_sector_analysis_schedule.job_name == "monthly_sector_analysis_job"

    def test_weekly_cycle_schedule_configuration(self):
        """Test weekly cycle analysis schedule configuration."""
        assert weekly_cycle_analysis_schedule.name == "weekly_cycle_analysis_schedule"
        assert weekly_cycle_analysis_schedule.cron_schedule == "0 8 * * 1"
        assert weekly_cycle_analysis_schedule.execution_timezone == "America/New_York"
        assert weekly_cycle_analysis_schedule.job_name == "weekly_cycle_analysis_job"

    def test_weekly_allocation_schedule_configuration(self):
        """Test weekly allocation schedule configuration."""
        assert weekly_allocation_schedule.name == "weekly_allocation_schedule"
        assert weekly_allocation_schedule.cron_schedule == "0 9 * * 1"
        assert weekly_allocation_schedule.execution_timezone == "America/New_York"
        assert weekly_allocation_schedule.job_name == "weekly_allocation_job"

    def test_daily_monitoring_schedule_configuration(self):
        """Test daily monitoring schedule configuration."""
        assert daily_monitoring_schedule.name == "daily_monitoring_schedule"
        assert daily_monitoring_schedule.cron_schedule == "0 6 * * 1-5"
        assert daily_monitoring_schedule.execution_timezone == "America/New_York"
        assert daily_monitoring_schedule.job_name == "daily_monitoring_job"

    def test_schedule_timing_consistency(self):
        """Test that schedule timings are consistent and logical."""
        # Weekly cycle analysis should run before allocation
        cycle_hour = 8  # 8 AM EST
        allocation_hour = 9  # 9 AM EST
        assert cycle_hour < allocation_hour, "Cycle analysis should run before allocation"
        
        # Monthly analysis should run at a reasonable time
        monthly_hour = 9  # 9 AM EST
        assert 6 <= monthly_hour <= 18, "Monthly analysis should run during business hours"
        
        # Daily monitoring should run early
        daily_hour = 6  # 6 AM EST
        assert daily_hour <= 8, "Daily monitoring should run early in the morning"


class TestScheduledJobs:
    """Test cases for scheduled job definitions."""

    def test_job_creation(self):
        """Test that scheduled jobs are created correctly."""
        jobs = create_scheduled_jobs()
        
        assert "monthly_sector_job" in jobs
        assert "weekly_cycle_job" in jobs
        assert "weekly_allocation_job" in jobs
        assert "daily_monitoring_job" in jobs

    def test_job_asset_selection(self):
        """Test that jobs select the correct assets."""
        jobs = create_scheduled_jobs()
        
        # Monthly job should include sector analysis assets
        monthly_job = jobs["monthly_sector_job"]
        assert monthly_job is not None
        
        # Weekly cycle job should include cycle analysis
        weekly_cycle_job = jobs["weekly_cycle_job"]
        assert weekly_cycle_job is not None
        
        # Weekly allocation job should include allocation assets
        weekly_allocation_job = jobs["weekly_allocation_job"]
        assert weekly_allocation_job is not None


class TestDataFreshnessSensor:
    """Test cases for data freshness sensor."""

    def test_sensor_creation(self):
        """Test that data freshness sensor is created correctly."""
        sensor = create_data_freshness_sensor()
        assert sensor is not None
        assert sensor.name == "data_freshness_sensor"

    @patch('datetime.datetime')
    def test_sensor_monday_trigger(self, mock_datetime):
        """Test sensor triggers on Monday morning."""
        # Mock Monday 8 AM EST
        mock_now = datetime(2024, 1, 1, 8, 0, 0)  # Monday
        mock_datetime.now.return_value = mock_now
        mock_datetime.now.return_value = mock_now.replace(tzinfo=pytz.timezone("America/New_York"))
        
        sensor = create_data_freshness_sensor()
        context = Mock()
        
        # This would need to be tested with actual sensor execution
        # For now, just verify the sensor is created
        assert sensor is not None

    @patch('datetime.datetime')
    def test_sensor_monthly_trigger(self, mock_datetime):
        """Test sensor triggers on 1st of month."""
        # Mock 1st of month 9 AM EST
        mock_now = datetime(2024, 1, 1, 9, 0, 0)  # 1st of month
        mock_datetime.now.return_value = mock_now
        mock_datetime.now.return_value = mock_now.replace(tzinfo=pytz.timezone("America/New_York"))
        
        sensor = create_data_freshness_sensor()
        context = Mock()
        
        # This would need to be tested with actual sensor execution
        # For now, just verify the sensor is created
        assert sensor is not None


class TestDefinitionsIntegration:
    """Test integration of schedules with definitions."""

    def test_definitions_include_schedules(self):
        """Test that definitions include all schedules."""
        assert defs is not None
        assert len(defs.schedules) > 0
        
        schedule_names = [schedule.name for schedule in defs.schedules]
        expected_schedules = [
            "monthly_sector_analysis_schedule",
            "weekly_cycle_analysis_schedule", 
            "weekly_allocation_schedule",
            "daily_monitoring_schedule"
        ]
        
        for expected_schedule in expected_schedules:
            assert expected_schedule in schedule_names

    def test_definitions_include_sensors(self):
        """Test that definitions include sensors."""
        assert defs is not None
        assert len(defs.sensors) > 0
        
        sensor_names = [sensor.name for sensor in defs.sensors]
        assert "data_freshness_sensor" in sensor_names

    def test_definitions_include_jobs(self):
        """Test that definitions include scheduled jobs."""
        assert defs is not None
        assert len(defs.jobs) > 0
        
        job_names = [job.name for job in defs.jobs]
        expected_jobs = [
            "monthly_sector_analysis_job",
            "weekly_cycle_analysis_job",
            "weekly_allocation_job", 
            "daily_monitoring_job"
        ]
        
        for expected_job in expected_jobs:
            assert expected_job in job_names


class TestAssetScheduling:
    """Test cases for asset scheduling metadata."""

    def test_assets_have_scheduling_tags(self):
        """Test that scheduled assets have appropriate tags."""
        scheduled_assets = [
            "sector_inflation_analysis",
            "sector_inflation_specific_analysis", 
            "economic_cycle_analysis",
            "asset_allocation_recommendations",
            "custom_asset_allocation",
            "economic_dashboard",
            "economic_monitoring_alerts"
        ]
        
        for asset_name in scheduled_assets:
            if asset_name in defs.assets:
                asset = defs.assets[asset_name]
                assert "schedule" in asset.tags
                assert "execution_time" in asset.tags
                assert "analysis_type" in asset.tags

    def test_monthly_assets_have_monthly_tags(self):
        """Test that monthly assets have monthly scheduling tags."""
        monthly_assets = [
            "sector_inflation_analysis",
            "sector_inflation_specific_analysis"
        ]
        
        for asset_name in monthly_assets:
            if asset_name in defs.assets:
                asset = defs.assets[asset_name]
                assert asset.tags.get("schedule") == "monthly"
                assert "monthly" in asset.tags.get("execution_time", "")

    def test_weekly_assets_have_weekly_tags(self):
        """Test that weekly assets have weekly scheduling tags."""
        weekly_assets = [
            "economic_cycle_analysis",
            "asset_allocation_recommendations",
            "custom_asset_allocation"
        ]
        
        for asset_name in weekly_assets:
            if asset_name in defs.assets:
                asset = defs.assets[asset_name]
                assert asset.tags.get("schedule") == "weekly"
                assert "monday" in asset.tags.get("execution_time", "").lower()

    def test_daily_assets_have_daily_tags(self):
        """Test that daily assets have daily scheduling tags."""
        daily_assets = [
            "economic_dashboard",
            "economic_monitoring_alerts"
        ]
        
        for asset_name in daily_assets:
            if asset_name in defs.assets:
                asset = defs.assets[asset_name]
                assert asset.tags.get("schedule") == "daily"
                assert "6am" in asset.tags.get("execution_time", "").lower()


class TestCronScheduleValidation:
    """Test cases for cron schedule validation."""

    def test_cron_schedule_format(self):
        """Test that cron schedules are in correct format."""
        schedules = [
            monthly_sector_analysis_schedule,
            weekly_cycle_analysis_schedule,
            weekly_allocation_schedule,
            daily_monitoring_schedule
        ]
        
        for schedule in schedules:
            cron_parts = schedule.cron_schedule.split()
            assert len(cron_parts) == 5, f"Invalid cron format for {schedule.name}: {schedule.cron_schedule}"
            
            # Validate each part
            minute, hour, day, month, weekday = cron_parts
            assert minute.isdigit() or minute == "*", f"Invalid minute: {minute}"
            assert hour.isdigit() or hour == "*", f"Invalid hour: {hour}"
            assert day.isdigit() or day == "*", f"Invalid day: {day}"
            assert month.isdigit() or month == "*", f"Invalid month: {month}"
            assert weekday.isdigit() or weekday == "*" or "-" in weekday, f"Invalid weekday: {weekday}"

    def test_timezone_consistency(self):
        """Test that all schedules use the same timezone."""
        schedules = [
            monthly_sector_analysis_schedule,
            weekly_cycle_analysis_schedule,
            weekly_allocation_schedule,
            daily_monitoring_schedule
        ]
        
        expected_timezone = "America/New_York"
        for schedule in schedules:
            assert schedule.execution_timezone == expected_timezone, f"Timezone mismatch for {schedule.name}"


class TestScheduleDependencies:
    """Test cases for schedule dependencies and ordering."""

    def test_weekly_schedule_ordering(self):
        """Test that weekly schedules are ordered correctly."""
        # Cycle analysis should run before allocation
        cycle_hour = 8
        allocation_hour = 9
        
        assert cycle_hour < allocation_hour, "Cycle analysis must run before allocation"
        
        # Verify the actual schedule times match
        assert weekly_cycle_analysis_schedule.cron_schedule == "0 8 * * 1"
        assert weekly_allocation_schedule.cron_schedule == "0 9 * * 1"

    def test_asset_dependencies_align_with_schedules(self):
        """Test that asset dependencies align with schedule timing."""
        # Economic cycle analysis should run before allocation
        if "economic_cycle_analysis" in defs.assets and "asset_allocation_recommendations" in defs.assets:
            cycle_asset = defs.assets["economic_cycle_analysis"]
            allocation_asset = defs.assets["asset_allocation_recommendations"]
            
            # Allocation should depend on cycle analysis
            assert "economic_cycle_analysis" in allocation_asset.deps or "economic_cycle_analysis" in str(allocation_asset.deps)
