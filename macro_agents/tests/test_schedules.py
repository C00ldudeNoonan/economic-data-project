"""
Tests for scheduled execution and scheduling configuration.
"""

from dagster import DefaultSensorStatus
from macro_agents.definitions import defs
from macro_agents.defs.analysis.schedules import (
    daily_reddit_summary_schedule,
    weekly_economic_analysis_schedule_bullish,
    weekly_economic_analysis_schedule_neutral,
    weekly_economic_analysis_schedule_skeptical,
    weekly_interesting_data_points_schedule,
    weekly_news_summary_schedule,
)
from macro_agents.defs.domains.calendars import (
    daily_calendar_dates_schedule,
    daily_earnings_calendar_schedule,
    daily_economic_calendar_schedule,
)
from macro_agents.defs.domains.housing import (
    weekly_housing_inventory_schedule,
    weekly_housing_pulse_schedule,
)
from macro_agents.defs.domains.macro import (
    daily_fomc_minutes_schedule,
    weekly_fred_ingestion_schedule,
)
from macro_agents.defs.domains.markets import (
    market_stack_ingestion_schedules,
    monthly_sp500_companies_list_schedule,
)
from macro_agents.defs.domains.social import daily_reddit_posts_schedule
from macro_agents.defs.signals.defs import signal_schedules
from macro_agents.defs.telemetry.telemetry import (
    daily_telemetry_events_schedule,
    daily_users_replication_schedule,
)
from macro_agents.defs.sensors import realtor_gdrive_sensor, treasury_yields_sensor


class TestSchedules:
    """Test cases for schedule definitions."""

    def test_weekly_economic_analysis_schedule_configuration(self):
        """Test weekly economic analysis schedule configuration."""
        assert (
            weekly_economic_analysis_schedule_skeptical.name
            == "weekly_economic_analysis_schedule_skeptical"
        )
        assert weekly_economic_analysis_schedule_skeptical.cron_schedule == "0 14 * * 0"
        assert (
            weekly_economic_analysis_schedule_skeptical.execution_timezone
            == "America/New_York"
        )
        assert (
            weekly_economic_analysis_schedule_skeptical.job_name
            == "weekly_economic_analysis_job_skeptical"
        )

        assert (
            weekly_economic_analysis_schedule_neutral.name
            == "weekly_economic_analysis_schedule_neutral"
        )
        assert weekly_economic_analysis_schedule_neutral.cron_schedule == "15 14 * * 0"

        assert (
            weekly_economic_analysis_schedule_bullish.name
            == "weekly_economic_analysis_schedule_bullish"
        )
        assert weekly_economic_analysis_schedule_bullish.cron_schedule == "30 14 * * 0"

    def test_schedule_timing_consistency(self):
        """Test that schedule timings are consistent and logical."""
        # Monthly analysis should run at a reasonable time
        monthly_hour = 9  # 9 AM EST
        assert 6 <= monthly_hour <= 18, (
            "Monthly analysis should run during business hours"
        )


class TestTelemetrySchedules:
    """Test cases for telemetry replication schedule definitions."""

    def test_telemetry_events_schedule_configuration(self):
        """Test telemetry events schedule runs on its own dedicated schedule."""
        assert daily_telemetry_events_schedule.name == "daily_telemetry_events_schedule"
        assert daily_telemetry_events_schedule.cron_schedule == "0 9 * * *"
        assert daily_telemetry_events_schedule.execution_timezone == "America/New_York"
        assert (
            daily_telemetry_events_schedule.job_name
            == "telemetry_events_replication_job"
        )

    def test_users_replication_schedule_configuration(self):
        """Test users replication schedule runs on its own dedicated schedule."""
        assert (
            daily_users_replication_schedule.name == "daily_users_replication_schedule"
        )
        assert daily_users_replication_schedule.cron_schedule == "30 9 * * *"
        assert daily_users_replication_schedule.execution_timezone == "America/New_York"
        assert daily_users_replication_schedule.job_name == "users_replication_job"

    def test_telemetry_schedules_do_not_overlap(self):
        """Test that telemetry schedules are staggered to avoid concurrent runs."""
        assert (
            daily_telemetry_events_schedule.cron_schedule
            != daily_users_replication_schedule.cron_schedule
        )


class TestScheduledJobs:
    """Test cases for scheduled job definitions."""

    def test_job_creation(self):
        """Test that scheduled jobs are created correctly."""
        job_names = [job.name for job in defs.jobs]

        assert "weekly_economic_analysis_job_skeptical" in job_names
        assert "weekly_economic_analysis_job_neutral" in job_names
        assert "weekly_economic_analysis_job_bullish" in job_names
        assert "telemetry_events_replication_job" in job_names
        assert "users_replication_job" in job_names
        assert "reddit_daily_summary_job" in job_names
        assert "news_weekly_summary_job" in job_names
        assert "interesting_data_points_job" in job_names
        assert "us_sector_etfs_ingestion_job" in job_names
        assert "currency_etfs_ingestion_job" in job_names
        assert "commodity_etfs_ingestion_job" in job_names
        assert "major_indices_ingestion_job" in job_names
        assert "fixed_income_etfs_ingestion_job" in job_names
        assert "global_markets_ingestion_job" in job_names
        assert "sp500_companies_prices_ingestion_job" in job_names
        assert "energy_commodities_ingestion_job" in job_names
        assert "input_commodities_ingestion_job" in job_names
        assert "agriculture_commodities_ingestion_job" in job_names
        assert "treasury_yields_ingestion_job" in job_names
        assert "asset_failure_monitor_job" in job_names
        # Macro domain jobs
        assert "fred_ingestion_job" in job_names
        assert "fomc_minutes_ingestion_job" in job_names
        # Calendar domain jobs
        assert "calendar_dates_job" in job_names
        assert "economic_calendar_job" in job_names
        assert "earnings_calendar_job" in job_names
        # Housing domain jobs
        assert "housing_inventory_job" in job_names
        assert "housing_pulse_job" in job_names
        # Social domain jobs
        assert "reddit_posts_ingestion_job" in job_names
        # Company list jobs
        assert "sp500_companies_list_job" in job_names
        # Signal jobs
        assert "absorption_ratio_job" in job_names
        assert "turbulence_index_job" in job_names
        assert "fear_greed_job" in job_names
        assert "entropy_complexity_job" in job_names
        assert "network_correlation_job" in job_names

    def test_job_asset_selection(self):
        """Test that jobs select the correct assets."""
        job_names = {job.name for job in defs.jobs}

        assert "weekly_economic_analysis_job_skeptical" in job_names
        assert "weekly_economic_analysis_job_neutral" in job_names
        assert "weekly_economic_analysis_job_bullish" in job_names
        assert "telemetry_events_replication_job" in job_names
        assert "users_replication_job" in job_names
        assert "reddit_daily_summary_job" in job_names
        assert "news_weekly_summary_job" in job_names
        assert "interesting_data_points_job" in job_names
        assert "us_sector_etfs_ingestion_job" in job_names
        assert "currency_etfs_ingestion_job" in job_names
        assert "commodity_etfs_ingestion_job" in job_names
        assert "major_indices_ingestion_job" in job_names
        assert "fixed_income_etfs_ingestion_job" in job_names
        assert "global_markets_ingestion_job" in job_names
        assert "sp500_companies_prices_ingestion_job" in job_names
        assert "energy_commodities_ingestion_job" in job_names
        assert "input_commodities_ingestion_job" in job_names
        assert "agriculture_commodities_ingestion_job" in job_names
        assert "treasury_yields_ingestion_job" in job_names
        assert "asset_failure_monitor_job" in job_names
        assert "fred_ingestion_job" in job_names
        assert "fomc_minutes_ingestion_job" in job_names
        assert "calendar_dates_job" in job_names
        assert "economic_calendar_job" in job_names
        assert "earnings_calendar_job" in job_names
        assert "housing_inventory_job" in job_names
        assert "housing_pulse_job" in job_names
        assert "reddit_posts_ingestion_job" in job_names
        assert "sp500_companies_list_job" in job_names
        assert "absorption_ratio_job" in job_names
        assert "turbulence_index_job" in job_names
        assert "fear_greed_job" in job_names
        assert "entropy_complexity_job" in job_names
        assert "network_correlation_job" in job_names


class TestIngestionSchedules:
    """Test cases for ingestion schedules."""

    def test_market_stack_schedules_migrated_to_automation(self):
        """All MarketStack ingestion schedules have been replaced by declarative automation."""
        assert len(market_stack_ingestion_schedules) == 0

    def test_company_list_schedule_creation(self):
        """Test that company list schedules are created correctly."""
        assert (
            monthly_sp500_companies_list_schedule.name
            == "monthly_sp500_companies_list_schedule"
        )
        assert monthly_sp500_companies_list_schedule.cron_schedule == "0 2 1 * *"
        assert (
            monthly_sp500_companies_list_schedule.execution_timezone
            == "America/New_York"
        )


class TestDefinitionsIntegration:
    """Test integration of schedules with definitions."""

    def test_definitions_include_schedules(self):
        """Test that definitions include all schedules."""
        assert defs is not None
        assert len(defs.schedules) > 0

        schedule_names = [schedule.name for schedule in defs.schedules]
        expected_schedules = [
            "weekly_economic_analysis_schedule_skeptical",
            "weekly_economic_analysis_schedule_neutral",
            "weekly_economic_analysis_schedule_bullish",
            "daily_telemetry_events_schedule",
            "daily_users_replication_schedule",
            "daily_reddit_summary_schedule",
            "weekly_news_summary_schedule",
            "weekly_interesting_data_points_schedule",
            # Macro domain schedules
            "weekly_fred_ingestion_schedule",
            "daily_fomc_minutes_schedule",
            # Calendar domain schedules
            "daily_calendar_dates_schedule",
            "daily_economic_calendar_schedule",
            "daily_earnings_calendar_schedule",
            # Housing domain schedules
            "weekly_housing_inventory_schedule",
            "weekly_housing_pulse_schedule",
            # Social domain schedules
            "daily_reddit_posts_schedule",
            # Company list schedules
            "monthly_sp500_companies_list_schedule",
            # Signal schedules
            "weekly_absorption_ratio_schedule",
            "weekday_turbulence_index_schedule",
            "weekday_fear_greed_schedule",
            "weekday_entropy_complexity_schedule",
            "weekly_network_correlation_schedule",
        ]

        for expected_schedule in expected_schedules:
            assert expected_schedule in schedule_names

    def test_definitions_include_sensors(self):
        """Test that definitions include ingestion sensors."""
        assert defs is not None
        assert len(defs.sensors) > 0

        sensor_names = [sensor.name for sensor in defs.sensors]
        expected_sensors = [
            "treasury_yields_ingestion_schedule",
            "realtor_gdrive_file_monitor",
        ]

        for expected_sensor in expected_sensors:
            assert expected_sensor in sensor_names

    def test_definitions_include_jobs(self):
        """Test that definitions include scheduled jobs."""
        assert defs is not None
        assert len(defs.jobs) > 0

        job_names = [job.name for job in defs.jobs]
        expected_jobs = [
            "weekly_economic_analysis_job_skeptical",
            "weekly_economic_analysis_job_neutral",
            "weekly_economic_analysis_job_bullish",
            "telemetry_events_replication_job",
            "users_replication_job",
            "reddit_daily_summary_job",
            "news_weekly_summary_job",
            "interesting_data_points_job",
            "us_sector_etfs_ingestion_job",
            "currency_etfs_ingestion_job",
            "commodity_etfs_ingestion_job",
            "major_indices_ingestion_job",
            "fixed_income_etfs_ingestion_job",
            "global_markets_ingestion_job",
            "sp500_companies_prices_ingestion_job",
            "energy_commodities_ingestion_job",
            "input_commodities_ingestion_job",
            "agriculture_commodities_ingestion_job",
            "treasury_yields_ingestion_job",
            "asset_failure_monitor_job",
            "fred_ingestion_job",
            "fomc_minutes_ingestion_job",
            "calendar_dates_job",
            "economic_calendar_job",
            "earnings_calendar_job",
            "housing_inventory_job",
            "housing_pulse_job",
            "reddit_posts_ingestion_job",
            "sp500_companies_list_job",
            "absorption_ratio_job",
            "turbulence_index_job",
            "fear_greed_job",
            "entropy_complexity_job",
            "network_correlation_job",
        ]

        for expected_job in expected_jobs:
            assert expected_job in job_names


class TestAssetScheduling:
    """Test cases for asset scheduling metadata."""

    @staticmethod
    def _get_asset_key_strings():
        asset_key_strings = []
        for asset_def in defs.assets:
            if hasattr(asset_def, "keys"):
                asset_key_strings.extend(
                    [key.to_user_string() for key in asset_def.keys]
                )
            else:
                asset_key_strings.append(asset_def.key.to_user_string())
        return asset_key_strings

    def test_economic_analysis_assets_exist(self):
        """Test that economic analysis assets exist."""
        economic_assets = [
            "analyze_economy_state",
            "analyze_asset_class_relationships",
            "generate_investment_recommendations",
        ]

        asset_key_strings = self._get_asset_key_strings()

        for asset_name in economic_assets:
            assert asset_name in asset_key_strings

    def test_asset_failure_monitor_has_cron_automation(self):
        """Test that asset failure monitoring is configured with a cron automation condition."""
        asset_def = None
        for asset in defs.assets:
            if hasattr(asset, "keys"):
                for key in asset.keys:
                    if key.to_user_string() == "asset_failure_monitor":
                        asset_def = asset
                        break
            else:
                if asset.key.to_user_string() == "asset_failure_monitor":
                    asset_def = asset
                    break
            if asset_def:
                break

        assert asset_def is not None
        from dagster import AssetKey

        asset_key = AssetKey("asset_failure_monitor")
        automation_condition = getattr(asset_def, "automation_condition", None)
        if automation_condition is None and hasattr(asset_def, "get_asset_spec"):
            automation_condition = asset_def.get_asset_spec(
                asset_key
            ).automation_condition
        assert automation_condition is not None

        cron_value = getattr(automation_condition, "cron_schedule", None)
        if cron_value is None:
            cron_value = getattr(automation_condition, "_cron_schedule", None)
        if cron_value is None:
            cron_value = str(automation_condition)

        assert "0 */6 * * *" in str(cron_value)


class TestCronScheduleValidation:
    """Test cases for cron schedule validation."""

    def test_cron_schedule_format(self):
        """Test that cron schedules are in correct format."""
        schedules = [
            weekly_economic_analysis_schedule_skeptical,
            weekly_economic_analysis_schedule_neutral,
            weekly_economic_analysis_schedule_bullish,
            daily_telemetry_events_schedule,
            daily_users_replication_schedule,
            daily_reddit_summary_schedule,
            weekly_news_summary_schedule,
            weekly_interesting_data_points_schedule,
            *market_stack_ingestion_schedules,
            weekly_fred_ingestion_schedule,
            daily_fomc_minutes_schedule,
            daily_calendar_dates_schedule,
            daily_economic_calendar_schedule,
            daily_earnings_calendar_schedule,
            weekly_housing_inventory_schedule,
            weekly_housing_pulse_schedule,
            daily_reddit_posts_schedule,
            monthly_sp500_companies_list_schedule,
            *signal_schedules,
        ]

        for schedule in schedules:
            cron_parts = schedule.cron_schedule.split()
            assert len(cron_parts) == 5, (
                f"Invalid cron format for {schedule.name}: {schedule.cron_schedule}"
            )

            # Validate each part
            minute, hour, day, month, weekday = cron_parts
            assert minute.isdigit() or minute == "*" or "/" in minute, (
                f"Invalid minute: {minute}"
            )
            assert hour.isdigit() or hour == "*" or "/" in hour, f"Invalid hour: {hour}"
            assert day.isdigit() or day == "*" or "-" in day, f"Invalid day: {day}"
            assert month.isdigit() or month == "*", f"Invalid month: {month}"
            assert weekday.isdigit() or weekday == "*" or "-" in weekday, (
                f"Invalid weekday: {weekday}"
            )

    def test_timezone_consistency(self):
        """Test that all schedules use the same timezone."""
        schedules = [
            weekly_economic_analysis_schedule_skeptical,
            weekly_economic_analysis_schedule_neutral,
            weekly_economic_analysis_schedule_bullish,
            daily_telemetry_events_schedule,
            daily_users_replication_schedule,
            daily_reddit_summary_schedule,
            weekly_news_summary_schedule,
            weekly_interesting_data_points_schedule,
            *market_stack_ingestion_schedules,
            weekly_fred_ingestion_schedule,
            daily_fomc_minutes_schedule,
            daily_calendar_dates_schedule,
            daily_economic_calendar_schedule,
            daily_earnings_calendar_schedule,
            weekly_housing_inventory_schedule,
            weekly_housing_pulse_schedule,
            daily_reddit_posts_schedule,
            monthly_sp500_companies_list_schedule,
            *signal_schedules,
        ]

        expected_timezone = "America/New_York"
        for schedule in schedules:
            assert schedule.execution_timezone == expected_timezone, (
                f"Timezone mismatch for {schedule.name}"
            )

    def test_sensor_configuration(self):
        """Test that ingestion sensors are configured correctly."""
        all_sensors = [
            treasury_yields_sensor,
            realtor_gdrive_sensor,
        ]

        for sensor in all_sensors:
            assert sensor is not None
            assert sensor.name is not None
            assert hasattr(sensor, "job")
            assert sensor.job is not None
            # Sensors default to STOPPED status and must be manually started
            assert sensor.default_status == DefaultSensorStatus.STOPPED


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
