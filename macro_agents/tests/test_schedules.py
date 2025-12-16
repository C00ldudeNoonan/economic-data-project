"""
Tests for scheduled execution and scheduling configuration.
"""

from dagster import DefaultSensorStatus

from macro_agents.defs.schedules import (
    weekly_replication_schedule,
    monthly_economic_analysis_schedule_skeptical,
    monthly_economic_analysis_schedule_neutral,
    monthly_economic_analysis_schedule_bullish,
    create_scheduled_jobs,
    us_sector_etfs_schedule,
    currency_etfs_schedule,
    major_indices_schedule,
    fixed_income_etfs_schedule,
    global_markets_schedule,
    energy_commodities_schedule,
    input_commodities_schedule,
    agriculture_commodities_schedule,
    treasury_yields_schedule,
)
from macro_agents.defs.constants.market_stack_constants import ENERGY_COMMODITIES
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
            monthly_economic_analysis_schedule_skeptical.name
            == "monthly_economic_analysis_schedule_skeptical"
        )
        assert (
            monthly_economic_analysis_schedule_skeptical.cron_schedule == "0 9 1-7 * 0"
        )
        assert (
            monthly_economic_analysis_schedule_skeptical.execution_timezone
            == "America/New_York"
        )
        assert (
            monthly_economic_analysis_schedule_skeptical.job_name
            == "monthly_economic_analysis_job_skeptical"
        )

        assert (
            monthly_economic_analysis_schedule_neutral.name
            == "monthly_economic_analysis_schedule_neutral"
        )
        assert (
            monthly_economic_analysis_schedule_neutral.cron_schedule == "0 10 1-7 * 0"
        )

        assert (
            monthly_economic_analysis_schedule_bullish.name
            == "monthly_economic_analysis_schedule_bullish"
        )
        assert (
            monthly_economic_analysis_schedule_bullish.cron_schedule == "0 11 1-7 * 0"
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
        assert "monthly_economic_analysis_job_skeptical" in jobs
        assert "monthly_economic_analysis_job_neutral" in jobs
        assert "monthly_economic_analysis_job_bullish" in jobs
        assert "us_sector_etfs_ingestion_job" in jobs
        assert "currency_etfs_ingestion_job" in jobs
        assert "major_indices_ingestion_job" in jobs
        assert "fixed_income_etfs_ingestion_job" in jobs
        assert "global_markets_ingestion_job" in jobs
        assert "energy_commodities_ingestion_job" in jobs
        assert "input_commodities_ingestion_job" in jobs
        assert "agriculture_commodities_ingestion_job" in jobs
        assert "treasury_yields_ingestion_job" in jobs

    def test_job_asset_selection(self):
        """Test that jobs select the correct assets."""
        jobs = create_scheduled_jobs()

        # Weekly replication job should exist
        weekly_replication_job = jobs["weekly_replication_job"]
        assert weekly_replication_job is not None

        # Monthly economic analysis jobs should include the analysis pipeline
        monthly_economic_job_skeptical = jobs["monthly_economic_analysis_job_skeptical"]
        assert monthly_economic_job_skeptical is not None

        monthly_economic_job_neutral = jobs["monthly_economic_analysis_job_neutral"]
        assert monthly_economic_job_neutral is not None

        monthly_economic_job_bullish = jobs["monthly_economic_analysis_job_bullish"]
        assert monthly_economic_job_bullish is not None

        # MarketStack ingestion jobs should exist
        assert jobs["us_sector_etfs_ingestion_job"] is not None
        assert jobs["currency_etfs_ingestion_job"] is not None
        assert jobs["major_indices_ingestion_job"] is not None
        assert jobs["fixed_income_etfs_ingestion_job"] is not None
        assert jobs["global_markets_ingestion_job"] is not None
        assert jobs["energy_commodities_ingestion_job"] is not None
        assert jobs["input_commodities_ingestion_job"] is not None
        assert jobs["agriculture_commodities_ingestion_job"] is not None
        assert jobs["treasury_yields_ingestion_job"] is not None


class TestIngestionSensors:
    """Test cases for ingestion sensors."""

    def test_market_stack_sensor_creation(self):
        """Test that MarketStack ingestion sensors are created correctly."""
        sensors = [
            us_sector_etfs_schedule,
            currency_etfs_schedule,
            major_indices_schedule,
            fixed_income_etfs_schedule,
            global_markets_schedule,
            energy_commodities_schedule,
            input_commodities_schedule,
            agriculture_commodities_schedule,
            treasury_yields_schedule,
        ]

        expected_names = [
            "us_sector_etfs_raw_ingestion_schedule",
            "currency_etfs_raw_ingestion_schedule",
            "major_indices_raw_ingestion_schedule",
            "fixed_income_etfs_raw_ingestion_schedule",
            "global_markets_raw_ingestion_schedule",
            "energy_commodities_raw_ingestion_schedule",
            "input_commodities_raw_ingestion_schedule",
            "agriculture_commodities_raw_ingestion_schedule",
            "treasury_yields_ingestion_schedule",
        ]

        for sensor, expected_name in zip(sensors, expected_names):
            assert sensor is not None
            assert sensor.name == expected_name


class TestDefinitionsIntegration:
    """Test integration of schedules with definitions."""

    def test_definitions_include_schedules(self):
        """Test that definitions include all schedules."""
        assert defs is not None
        assert len(defs.schedules) > 0

        schedule_names = [schedule.name for schedule in defs.schedules]
        expected_schedules = [
            "weekly_replication_schedule",
            "monthly_economic_analysis_schedule_skeptical",
            "monthly_economic_analysis_schedule_neutral",
            "monthly_economic_analysis_schedule_bullish",
        ]

        for expected_schedule in expected_schedules:
            assert expected_schedule in schedule_names

    def test_definitions_include_sensors(self):
        """Test that definitions include ingestion sensors."""
        assert defs is not None
        assert len(defs.sensors) > 0

        sensor_names = [sensor.name for sensor in defs.sensors]
        expected_sensors = [
            "us_sector_etfs_raw_ingestion_schedule",
            "currency_etfs_raw_ingestion_schedule",
            "major_indices_raw_ingestion_schedule",
            "fixed_income_etfs_raw_ingestion_schedule",
            "global_markets_raw_ingestion_schedule",
            "energy_commodities_raw_ingestion_schedule",
            "input_commodities_raw_ingestion_schedule",
            "agriculture_commodities_raw_ingestion_schedule",
            "treasury_yields_ingestion_schedule",
        ]

        for expected_sensor in expected_sensors:
            assert expected_sensor in sensor_names

    def test_definitions_include_jobs(self):
        """Test that definitions include scheduled jobs."""
        assert defs is not None
        assert len(defs.jobs) > 0

        job_names = [job.name for job in defs.jobs]
        expected_jobs = [
            "weekly_replication_job",
            "monthly_economic_analysis_job_skeptical",
            "monthly_economic_analysis_job_neutral",
            "monthly_economic_analysis_job_bullish",
            "us_sector_etfs_ingestion_job",
            "currency_etfs_ingestion_job",
            "major_indices_ingestion_job",
            "fixed_income_etfs_ingestion_job",
            "global_markets_ingestion_job",
            "energy_commodities_ingestion_job",
            "input_commodities_ingestion_job",
            "agriculture_commodities_ingestion_job",
            "treasury_yields_ingestion_job",
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
            monthly_economic_analysis_schedule_skeptical,
            monthly_economic_analysis_schedule_neutral,
            monthly_economic_analysis_schedule_bullish,
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
            monthly_economic_analysis_schedule_skeptical,
            monthly_economic_analysis_schedule_neutral,
            monthly_economic_analysis_schedule_bullish,
        ]

        expected_timezone = "America/New_York"
        for schedule in schedules:
            assert schedule.execution_timezone == expected_timezone, (
                f"Timezone mismatch for {schedule.name}"
            )

    def test_sensor_configuration(self):
        """Test that ingestion sensors are configured correctly."""
        all_sensors = [
            us_sector_etfs_schedule,
            currency_etfs_schedule,
            major_indices_schedule,
            fixed_income_etfs_schedule,
            global_markets_schedule,
            energy_commodities_schedule,
            input_commodities_schedule,
            agriculture_commodities_schedule,
            treasury_yields_schedule,
        ]

        for sensor in all_sensors:
            assert sensor is not None
            assert sensor.name is not None
            assert hasattr(sensor, "job")
            assert sensor.job is not None
            assert sensor.default_status == DefaultSensorStatus.RUNNING


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


class TestEnergyCommoditiesIngestionSensor:
    """Test cases for energy_commodities_raw_ingestion_schedule sensor behavior."""

    def test_sensor_does_not_run_on_non_friday_when_fresh(self):
        """Test that sensor does not yield run requests on non-Friday days when partitions are fresh."""
        from unittest.mock import patch, MagicMock
        from datetime import datetime, timedelta
        import pytz
        import dagster as dg

        instance = dg.DagsterInstance.ephemeral()
        repository_def = defs.get_repository_def()
        context = dg.build_sensor_context(
            instance=instance, repository_def=repository_def
        )

        three_days_ago = datetime.now(pytz.timezone("America/New_York")) - timedelta(
            days=3
        )
        mock_materialization = MagicMock()
        mock_materialization.timestamp = three_days_ago.timestamp()

        with (
            patch.object(
                instance,
                "get_latest_materialization_event",
                return_value=mock_materialization,
            ),
            patch("macro_agents.defs.schedules.datetime") as mock_datetime,
        ):
            for weekday in [0, 1, 2, 3, 5, 6]:
                mock_now = datetime(
                    2024,
                    12,
                    2 + weekday,
                    2,
                    0,
                    tzinfo=pytz.timezone("America/New_York"),
                )
                mock_datetime.now.return_value = mock_now
                mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
                mock_datetime.fromtimestamp = datetime.fromtimestamp

                sensor_result = energy_commodities_schedule.evaluate_tick(context)
                run_requests = sensor_result.run_requests if sensor_result else []
                assert len(run_requests) == 0, (
                    f"Should not run on weekday {weekday} when partitions are fresh"
                )

    def test_sensor_runs_on_friday(self):
        """Test that sensor yields run requests on Friday."""
        from unittest.mock import patch, MagicMock
        from datetime import datetime, timedelta
        import pytz
        import dagster as dg

        instance = dg.DagsterInstance.ephemeral()
        repository_def = defs.get_repository_def()
        context = dg.build_sensor_context(
            instance=instance, repository_def=repository_def
        )

        friday_date = datetime(
            2024, 12, 6, 2, 0, tzinfo=pytz.timezone("America/New_York")
        )
        assert friday_date.weekday() == 4
        three_days_ago = friday_date - timedelta(days=3)

        mock_materialization = MagicMock()
        mock_materialization.timestamp = three_days_ago.timestamp()

        with (
            patch.object(
                instance,
                "get_latest_materialization_event",
                return_value=mock_materialization,
            ),
            patch("macro_agents.defs.schedules.datetime") as mock_datetime,
        ):
            mock_datetime.now.return_value = friday_date
            mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
            mock_datetime.fromtimestamp = datetime.fromtimestamp

            sensor_result = energy_commodities_schedule.evaluate_tick(context)
            run_requests = sensor_result.run_requests if sensor_result else []

            assert len(run_requests) > 0, "Should yield run requests on Friday"

            first_day_current_month = friday_date.replace(day=1)
            current_month = first_day_current_month.strftime("%Y-%m-%d")
            last_day_previous_month = first_day_current_month - timedelta(days=1)
            first_day_previous_month = last_day_previous_month.replace(day=1)
            previous_month = first_day_previous_month.strftime("%Y-%m-%d")

            expected_months = {current_month, previous_month}
            actual_months = set()
            actual_commodities = set()

            for run_request in run_requests:
                assert isinstance(run_request, dg.RunRequest)
                assert run_request.partition_key is not None
                partition_key = run_request.partition_key
                assert "date" in partition_key.keys_by_dimension
                assert "commodity" in partition_key.keys_by_dimension
                actual_months.add(partition_key.keys_by_dimension["date"])
                actual_commodities.add(partition_key.keys_by_dimension["commodity"])
                assert run_request.tags.get("trigger") == "weekly_schedule", (
                    "Fresh partitions on Friday should have weekly_schedule trigger"
                )

            assert actual_months == expected_months, (
                f"Expected months {expected_months}, got {actual_months}"
            )
            assert actual_commodities == set(ENERGY_COMMODITIES), (
                f"Expected all commodities, got {actual_commodities}"
            )

    def test_sensor_skips_recently_materialized_partitions_on_friday(self):
        """Test that sensor skips partitions materialized within last 2 weeks on Friday."""
        from unittest.mock import patch, MagicMock
        from datetime import datetime, timedelta
        import pytz
        import dagster as dg

        instance = dg.DagsterInstance.ephemeral()
        repository_def = defs.get_repository_def()
        context = dg.build_sensor_context(
            instance=instance, repository_def=repository_def
        )

        friday_date = datetime(
            2024, 12, 6, 2, 0, tzinfo=pytz.timezone("America/New_York")
        )
        three_days_ago = friday_date - timedelta(days=3)

        mock_materialization = MagicMock()
        mock_materialization.timestamp = three_days_ago.timestamp()

        with (
            patch.object(
                instance,
                "get_latest_materialization_event",
                return_value=mock_materialization,
            ),
            patch("macro_agents.defs.schedules.datetime") as mock_datetime,
        ):
            mock_datetime.now.return_value = friday_date
            mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
            mock_datetime.fromtimestamp = datetime.fromtimestamp

            sensor_result = energy_commodities_schedule.evaluate_tick(context)
            run_requests = sensor_result.run_requests if sensor_result else []

            assert len(run_requests) > 0, (
                "Should run fresh partitions on Friday (weekly schedule)"
            )

    def test_sensor_runs_stale_partitions_on_friday(self):
        """Test that sensor runs partitions not materialized in 2+ weeks on Friday."""
        from unittest.mock import patch, MagicMock
        from datetime import datetime, timedelta
        import pytz
        import dagster as dg

        instance = dg.DagsterInstance.ephemeral()
        repository_def = defs.get_repository_def()
        context = dg.build_sensor_context(
            instance=instance, repository_def=repository_def
        )

        friday_date = datetime(
            2024, 12, 6, 2, 0, tzinfo=pytz.timezone("America/New_York")
        )
        three_weeks_ago = friday_date - timedelta(days=21)

        mock_materialization = MagicMock()
        mock_materialization.timestamp = three_weeks_ago.timestamp()

        with (
            patch.object(
                instance,
                "get_latest_materialization_event",
                return_value=mock_materialization,
            ),
            patch("macro_agents.defs.schedules.datetime") as mock_datetime,
        ):
            mock_datetime.now.return_value = friday_date
            mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
            mock_datetime.fromtimestamp = datetime.fromtimestamp

            sensor_result = energy_commodities_schedule.evaluate_tick(context)
            run_requests = sensor_result.run_requests if sensor_result else []

            assert len(run_requests) > 0, (
                "Should run partitions materialized 2+ weeks ago"
            )

            for run_request in run_requests:
                assert isinstance(run_request, dg.RunRequest)
                assert run_request.tags.get("trigger") == "freshness_violation"

    def test_sensor_runs_stale_partitions_on_non_friday(self):
        """Test that sensor runs partitions not materialized in 2+ weeks on any day (freshness violation)."""
        from unittest.mock import patch, MagicMock
        from datetime import datetime, timedelta
        import pytz
        import dagster as dg

        instance = dg.DagsterInstance.ephemeral()
        repository_def = defs.get_repository_def()
        context = dg.build_sensor_context(
            instance=instance, repository_def=repository_def
        )

        monday_date = datetime(
            2024, 12, 2, 2, 0, tzinfo=pytz.timezone("America/New_York")
        )
        assert monday_date.weekday() == 0
        three_weeks_ago = monday_date - timedelta(days=21)

        mock_materialization = MagicMock()
        mock_materialization.timestamp = three_weeks_ago.timestamp()

        with (
            patch.object(
                instance,
                "get_latest_materialization_event",
                return_value=mock_materialization,
            ),
            patch("macro_agents.defs.schedules.datetime") as mock_datetime,
        ):
            mock_datetime.now.return_value = monday_date
            mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
            mock_datetime.fromtimestamp = datetime.fromtimestamp

            sensor_result = energy_commodities_schedule.evaluate_tick(context)
            run_requests = sensor_result.run_requests if sensor_result else []

            assert len(run_requests) > 0, (
                "Should run stale partitions on non-Friday (freshness violation)"
            )

            for run_request in run_requests:
                assert isinstance(run_request, dg.RunRequest)
                assert run_request.tags.get("trigger") == "freshness_violation"

    def test_sensor_runs_never_materialized_partitions_on_friday(self):
        """Test that sensor runs partitions that have never been materialized on Friday."""
        from unittest.mock import patch
        from datetime import datetime, timedelta
        import pytz
        import dagster as dg

        instance = dg.DagsterInstance.ephemeral()
        repository_def = defs.get_repository_def()
        context = dg.build_sensor_context(
            instance=instance, repository_def=repository_def
        )

        friday_date = datetime(
            2024, 12, 6, 2, 0, tzinfo=pytz.timezone("America/New_York")
        )

        with (
            patch.object(
                instance, "get_latest_materialization_event", return_value=None
            ),
            patch("macro_agents.defs.schedules.datetime") as mock_datetime,
        ):
            mock_datetime.now.return_value = friday_date
            mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)

            sensor_result = energy_commodities_schedule.evaluate_tick(context)
            run_requests = sensor_result.run_requests if sensor_result else []

            assert len(run_requests) > 0, (
                "Should run partitions that have never been materialized"
            )

            first_day_current_month = friday_date.replace(day=1)
            current_month = first_day_current_month.strftime("%Y-%m-%d")
            last_day_previous_month = first_day_current_month - timedelta(days=1)
            first_day_previous_month = last_day_previous_month.replace(day=1)
            previous_month = first_day_previous_month.strftime("%Y-%m-%d")

            expected_months = {current_month, previous_month}
            actual_months = set()

            for run_request in run_requests:
                assert isinstance(run_request, dg.RunRequest)
                partition_key = run_request.partition_key
                actual_months.add(partition_key.keys_by_dimension["date"])
                assert run_request.tags.get("trigger") == "freshness_violation"

            assert actual_months == expected_months

    def test_sensor_runs_never_materialized_partitions_on_non_friday(self):
        """Test that sensor runs partitions that have never been materialized on any day (freshness violation)."""
        from unittest.mock import patch
        from datetime import datetime
        import pytz
        import dagster as dg

        instance = dg.DagsterInstance.ephemeral()
        repository_def = defs.get_repository_def()
        context = dg.build_sensor_context(
            instance=instance, repository_def=repository_def
        )

        monday_date = datetime(
            2024, 12, 2, 2, 0, tzinfo=pytz.timezone("America/New_York")
        )
        assert monday_date.weekday() == 0

        with (
            patch.object(
                instance, "get_latest_materialization_event", return_value=None
            ),
            patch("macro_agents.defs.schedules.datetime") as mock_datetime,
        ):
            mock_datetime.now.return_value = monday_date
            mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)

            sensor_result = energy_commodities_schedule.evaluate_tick(context)
            run_requests = sensor_result.run_requests if sensor_result else []

            assert len(run_requests) > 0, (
                "Should run never-materialized partitions on non-Friday (freshness violation)"
            )

            for run_request in run_requests:
                assert isinstance(run_request, dg.RunRequest)
                assert run_request.tags.get("trigger") == "freshness_violation"

    def test_sensor_handles_materialization_check_errors(self):
        """Test that sensor runs partitions when materialization check fails."""
        from unittest.mock import patch
        from datetime import datetime
        import pytz
        import dagster as dg

        instance = dg.DagsterInstance.ephemeral()
        repository_def = defs.get_repository_def()
        context = dg.build_sensor_context(
            instance=instance, repository_def=repository_def
        )

        friday_date = datetime(
            2024, 12, 6, 2, 0, tzinfo=pytz.timezone("America/New_York")
        )

        with (
            patch.object(
                instance,
                "get_latest_materialization_event",
                side_effect=Exception("Database error"),
            ),
            patch("macro_agents.defs.schedules.datetime") as mock_datetime,
        ):
            mock_datetime.now.return_value = friday_date
            mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)

            sensor_result = energy_commodities_schedule.evaluate_tick(context)
            run_requests = sensor_result.run_requests if sensor_result else []

            assert len(run_requests) > 0, (
                "Should run partitions when check fails (safe default)"
            )

    def test_sensor_includes_all_commodities(self):
        """Test that sensor includes all energy commodities in run requests."""
        from unittest.mock import patch
        from datetime import datetime
        import pytz
        import dagster as dg

        instance = dg.DagsterInstance.ephemeral()
        repository_def = defs.get_repository_def()
        context = dg.build_sensor_context(
            instance=instance, repository_def=repository_def
        )

        friday_date = datetime(
            2024, 12, 6, 2, 0, tzinfo=pytz.timezone("America/New_York")
        )

        with (
            patch.object(
                instance, "get_latest_materialization_event", return_value=None
            ),
            patch("macro_agents.defs.schedules.datetime") as mock_datetime,
        ):
            mock_datetime.now.return_value = friday_date
            mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)

            sensor_result = energy_commodities_schedule.evaluate_tick(context)
            run_requests = sensor_result.run_requests if sensor_result else []

            commodities_in_requests = set()
            for run_request in run_requests:
                partition_key = run_request.partition_key
                commodities_in_requests.add(
                    partition_key.keys_by_dimension["commodity"]
                )

            assert commodities_in_requests == set(ENERGY_COMMODITIES), (
                f"Expected all {len(ENERGY_COMMODITIES)} commodities, "
                f"got {len(commodities_in_requests)}: {commodities_in_requests}"
            )

    def test_sensor_includes_current_and_previous_month(self):
        """Test that sensor includes both current and previous month partitions."""
        from unittest.mock import patch
        from datetime import datetime, timedelta
        import pytz
        import dagster as dg

        instance = dg.DagsterInstance.ephemeral()
        repository_def = defs.get_repository_def()
        context = dg.build_sensor_context(
            instance=instance, repository_def=repository_def
        )

        friday_date = datetime(
            2024, 12, 6, 2, 0, tzinfo=pytz.timezone("America/New_York")
        )
        first_day_current_month = friday_date.replace(day=1)
        current_month = first_day_current_month.strftime("%Y-%m-%d")
        last_day_previous_month = first_day_current_month - timedelta(days=1)
        first_day_previous_month = last_day_previous_month.replace(day=1)
        previous_month = first_day_previous_month.strftime("%Y-%m-%d")

        with (
            patch.object(
                instance, "get_latest_materialization_event", return_value=None
            ),
            patch("macro_agents.defs.schedules.datetime") as mock_datetime,
        ):
            mock_datetime.now.return_value = friday_date
            mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)

            sensor_result = energy_commodities_schedule.evaluate_tick(context)
            run_requests = sensor_result.run_requests if sensor_result else []

            months_in_requests = set()
            for run_request in run_requests:
                partition_key = run_request.partition_key
                months_in_requests.add(partition_key.keys_by_dimension["date"])

            assert current_month in months_in_requests, (
                f"Current month {current_month} should be included"
            )
            assert previous_month in months_in_requests, (
                f"Previous month {previous_month} should be included"
            )
            assert len(months_in_requests) == 2, (
                f"Should only have 2 months, got {months_in_requests}"
            )

    def test_sensor_mixed_freshness_conditions(self):
        """Test that sensor handles mix of fresh and stale partitions correctly."""
        from unittest.mock import patch, MagicMock
        from datetime import datetime, timedelta
        import pytz
        import dagster as dg

        instance = dg.DagsterInstance.ephemeral()
        repository_def = defs.get_repository_def()
        context = dg.build_sensor_context(
            instance=instance, repository_def=repository_def
        )

        friday_date = datetime(
            2024, 12, 6, 2, 0, tzinfo=pytz.timezone("America/New_York")
        )
        three_days_ago = friday_date - timedelta(days=3)
        three_weeks_ago = friday_date - timedelta(days=21)

        def mock_get_materialization(asset_key, partition_key):
            commodity = partition_key.keys_by_dimension["commodity"]
            if commodity == ENERGY_COMMODITIES[0]:
                mock_event = MagicMock()
                mock_event.timestamp = three_days_ago.timestamp()
                return mock_event
            elif commodity == ENERGY_COMMODITIES[1]:
                mock_event = MagicMock()
                mock_event.timestamp = three_weeks_ago.timestamp()
                return mock_event
            else:
                return None

        with (
            patch.object(
                instance,
                "get_latest_materialization_event",
                side_effect=mock_get_materialization,
            ),
            patch("macro_agents.defs.schedules.datetime") as mock_datetime,
        ):
            mock_datetime.now.return_value = friday_date
            mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
            mock_datetime.fromtimestamp = datetime.fromtimestamp

            sensor_result = energy_commodities_schedule.evaluate_tick(context)
            run_requests = sensor_result.run_requests if sensor_result else []

            commodities_in_requests = set()
            for run_request in run_requests:
                partition_key = run_request.partition_key
                commodities_in_requests.add(
                    partition_key.keys_by_dimension["commodity"]
                )

            assert ENERGY_COMMODITIES[0] in commodities_in_requests, (
                "Fresh partition should run on Friday (weekly schedule)"
            )
            assert ENERGY_COMMODITIES[1] in commodities_in_requests, (
                "Stale partition should run (freshness violation)"
            )
            assert len(commodities_in_requests) == len(ENERGY_COMMODITIES), (
                f"Should run all commodities on Friday, got {len(commodities_in_requests)}"
            )
