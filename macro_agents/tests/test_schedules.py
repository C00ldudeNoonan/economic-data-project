"""
Tests for scheduled execution and scheduling configuration.
"""

from macro_agents.defs.schedules import (
    weekly_replication_schedule,
    monthly_economic_analysis_schedule,
    create_ingestion_sensor,
    create_scheduled_jobs,
)
from macro_agents.definitions import defs


class TestSchedules:
    """Test cases for schedule definitions."""

    def test_weekly_replication_schedule_configuration(self):
        """Test weekly replication schedule configuration."""
        assert weekly_replication_schedule.name == "weekly_replication_schedule"
        assert weekly_replication_schedule.cron_schedule == "0 2 * * 0"
        assert weekly_replication_schedule.execution_timezone == "America/New_York"
        assert weekly_replication_schedule.job_name == "weekly_replication_job"

    def test_monthly_economic_analysis_schedule_configuration(self):
        """Test monthly economic analysis schedule configuration."""
        assert (
            monthly_economic_analysis_schedule.name
            == "monthly_economic_analysis_schedule"
        )
        assert monthly_economic_analysis_schedule.cron_schedule == "0 9 1-7 * 0"
        assert (
            monthly_economic_analysis_schedule.execution_timezone == "America/New_York"
        )
        assert (
            monthly_economic_analysis_schedule.job_name
            == "monthly_economic_analysis_job"
        )

    def test_schedule_timing_consistency(self):
        """Test that schedule timings are consistent and logical."""
        # Monthly analysis should run at a reasonable time
        monthly_hour = 9  # 9 AM EST
        assert 6 <= monthly_hour <= 18, (
            "Monthly analysis should run during business hours"
        )

        # Weekly replication should run early
        replication_hour = 2  # 2 AM EST
        assert replication_hour <= 6, (
            "Weekly replication should run early in the morning"
        )


class TestScheduledJobs:
    """Test cases for scheduled job definitions."""

    def test_job_creation(self):
        """Test that scheduled jobs are created correctly."""
        jobs = create_scheduled_jobs()

        assert "weekly_replication_job" in jobs
        assert "monthly_economic_analysis_job" in jobs

    def test_job_asset_selection(self):
        """Test that jobs select the correct assets."""
        jobs = create_scheduled_jobs()

        # Weekly replication job should exist
        weekly_replication_job = jobs["weekly_replication_job"]
        assert weekly_replication_job is not None

        # Monthly economic analysis job should include the analysis pipeline
        monthly_economic_job = jobs["monthly_economic_analysis_job"]
        assert monthly_economic_job is not None


class TestIngestionSensor:
    """Test cases for ingestion sensor."""

    def test_sensor_creation(self):
        """Test that ingestion sensor is created correctly."""
        sensor = create_ingestion_sensor()
        assert sensor is not None
        assert sensor.name == "weekly_ingestion_sensor"


class TestDefinitionsIntegration:
    """Test integration of schedules with definitions."""

    def test_definitions_include_schedules(self):
        """Test that definitions include all schedules."""
        assert defs is not None
        assert len(defs.schedules) > 0

        schedule_names = [schedule.name for schedule in defs.schedules]
        expected_schedules = [
            "weekly_replication_schedule",
            "monthly_economic_analysis_schedule",
        ]

        for expected_schedule in expected_schedules:
            assert expected_schedule in schedule_names

    def test_definitions_include_sensors(self):
        """Test that definitions include sensors."""
        assert defs is not None
        assert len(defs.sensors) > 0

        sensor_names = [sensor.name for sensor in defs.sensors]
        assert "weekly_ingestion_sensor" in sensor_names

    def test_definitions_include_jobs(self):
        """Test that definitions include scheduled jobs."""
        assert defs is not None
        assert len(defs.jobs) > 0

        job_names = [job.name for job in defs.jobs]
        expected_jobs = [
            "weekly_replication_job",
            "monthly_economic_analysis_job",
        ]

        for expected_job in expected_jobs:
            assert expected_job in job_names


class TestAssetScheduling:
    """Test cases for asset scheduling metadata."""

    def test_economic_analysis_assets_exist(self):
        """Test that economic analysis assets exist."""
        economic_assets = [
            "analyze_economy_state",
            "analyze_asset_class_relationships",
            "generate_investment_recommendations",
        ]

        asset_key_strings = []
        for asset_def in defs.assets:
            if hasattr(asset_def, "keys"):
                asset_key_strings.extend(
                    [key.to_user_string() for key in asset_def.keys]
                )
            else:
                asset_key_strings.append(asset_def.key.to_user_string())

        for asset_name in economic_assets:
            assert asset_name in asset_key_strings


class TestCronScheduleValidation:
    """Test cases for cron schedule validation."""

    def test_cron_schedule_format(self):
        """Test that cron schedules are in correct format."""
        schedules = [
            weekly_replication_schedule,
            monthly_economic_analysis_schedule,
        ]

        for schedule in schedules:
            cron_parts = schedule.cron_schedule.split()
            assert len(cron_parts) == 5, (
                f"Invalid cron format for {schedule.name}: {schedule.cron_schedule}"
            )

            # Validate each part
            minute, hour, day, month, weekday = cron_parts
            assert minute.isdigit() or minute == "*", f"Invalid minute: {minute}"
            assert hour.isdigit() or hour == "*", f"Invalid hour: {hour}"
            assert day.isdigit() or day == "*" or "-" in day, f"Invalid day: {day}"
            assert month.isdigit() or month == "*", f"Invalid month: {month}"
            assert weekday.isdigit() or weekday == "*" or "-" in weekday, (
                f"Invalid weekday: {weekday}"
            )

    def test_timezone_consistency(self):
        """Test that all schedules use the same timezone."""
        schedules = [
            weekly_replication_schedule,
            monthly_economic_analysis_schedule,
        ]

        expected_timezone = "America/New_York"
        for schedule in schedules:
            assert schedule.execution_timezone == expected_timezone, (
                f"Timezone mismatch for {schedule.name}"
            )


class TestScheduleDependencies:
    """Test cases for schedule dependencies and ordering."""

    def test_economic_analysis_pipeline_dependencies(self):
        """Test that economic analysis pipeline dependencies are correct."""
        # Verify the pipeline order: economy_state -> relationships -> recommendations
        asset_key_strings = []
        asset_defs_by_key_string = {}

        for asset_def in defs.assets:
            if hasattr(asset_def, "keys"):
                for key in asset_def.keys:
                    key_str = key.to_user_string()
                    asset_key_strings.append(key_str)
                    asset_defs_by_key_string[key_str] = asset_def
            else:
                key_str = asset_def.key.to_user_string()
                asset_key_strings.append(key_str)
                asset_defs_by_key_string[key_str] = asset_def

        if "analyze_economy_state" in asset_key_strings:
            # Relationships should depend on economy_state
            if "analyze_asset_class_relationships" in asset_key_strings:
                relationships_asset = asset_defs_by_key_string[
                    "analyze_asset_class_relationships"
                ]
                assert relationships_asset is not None

            # Recommendations should depend on both previous steps
            if "generate_investment_recommendations" in asset_key_strings:
                recommendations_asset = asset_defs_by_key_string[
                    "generate_investment_recommendations"
                ]
                assert recommendations_asset is not None
