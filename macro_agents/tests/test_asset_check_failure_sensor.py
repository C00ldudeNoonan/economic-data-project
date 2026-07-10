"""
Unit tests for asset check failure sensor.
"""

from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytz


class TestAssetCheckFailureSensor:
    """Test cases for asset check failure sensor logic."""

    def test_initialize_failure_tracking_table(self):
        """Test table initialization."""
        from macro_agents.defs.asset_failure_sensor import (
            _initialize_failure_tracking_table,
        )

        mock_md = Mock()
        mock_context = Mock()
        mock_context.log = Mock()

        _initialize_failure_tracking_table(mock_md, mock_context)

        mock_md.execute_query.assert_called_once()
        call_args = mock_md.execute_query.call_args
        assert "CREATE TABLE IF NOT EXISTS `asset_check_failures`" in call_args[0][0]
        assert "asset_key STRING NOT NULL" in call_args[0][0]
        assert "PRIMARY KEY" not in call_args[0][0]
        assert call_args.kwargs["read_only"] is False

    def test_get_failure_tracking_record_uses_named_parameters(self):
        """Test that failure lookup binds each key by name."""
        from macro_agents.defs.asset_failure_sensor import (
            _get_failure_tracking_record,
        )

        mock_md = Mock()
        mock_md.execute_query.return_value = MagicMock()
        mock_md.execute_query.return_value.__len__.return_value = 0

        result = _get_failure_tracking_record(
            mock_md,
            asset_key="test_asset",
            failure_type="asset_check",
            check_name="test_check",
        )

        assert result is None
        query = mock_md.execute_query.call_args.args[0]
        assert "asset_key = @asset_key" in query
        assert mock_md.execute_query.call_args.kwargs["params"] == {
            "asset_key": "test_asset",
            "failure_type": "asset_check",
            "check_name": "test_check",
        }

    def test_handle_first_failure(self):
        """Test handling the first failure of a check."""
        from macro_agents.defs.asset_failure_sensor import _handle_check_failure

        mock_md = Mock()
        mock_github = Mock()
        mock_context = Mock()
        mock_context.log = Mock()
        mock_execution_record = Mock()
        mock_execution_record.create_timestamp = datetime.now(pytz.UTC).timestamp()

        _handle_check_failure(
            md=mock_md,
            github=mock_github,
            asset_key="test_asset",
            check_name="test_check",
            current_record=None,
            timestamp=datetime.now(pytz.UTC),
            execution_record=mock_execution_record,
            context=mock_context,
        )

        mock_md.execute_query.assert_called_once()
        call_args = mock_md.execute_query.call_args
        assert "MERGE `asset_check_failures`" in call_args[0][0]
        assert call_args.kwargs["params"]["asset_key"] == "test_asset"
        assert call_args.kwargs["params"]["check_name"] == "test_check"
        assert isinstance(
            call_args.kwargs["params"]["last_failure_timestamp"], datetime
        )

    def test_handle_third_failure_creates_issue(self):
        """Test that the 3rd consecutive failure creates a GitHub issue."""
        from macro_agents.defs.asset_failure_sensor import _handle_check_failure

        mock_md = Mock()
        mock_github = Mock()
        mock_context = Mock()
        mock_context.log = Mock()
        mock_execution_record = Mock()
        mock_execution_record.create_timestamp = datetime.now(pytz.UTC).timestamp()
        mock_execution_record.event = Mock()
        mock_execution_record.event.metadata_entries = []

        current_record = {"consecutive_failures": 2, "github_issue_number": None}

        with patch(
            "macro_agents.defs.asset_failure_sensor._create_github_issue_for_failure"
        ) as mock_create:
            _handle_check_failure(
                md=mock_md,
                github=mock_github,
                asset_key="test_asset",
                check_name="test_check",
                current_record=current_record,
                timestamp=datetime.now(pytz.UTC),
                execution_record=mock_execution_record,
                context=mock_context,
            )

            mock_create.assert_called_once()

    def test_handle_success_resets_counter(self):
        """Test that success resets the failure counter."""
        from macro_agents.defs.asset_failure_sensor import _handle_check_success

        mock_md = Mock()
        mock_github = Mock()
        mock_context = Mock()
        mock_context.log = Mock()

        current_record = {"consecutive_failures": 5, "github_issue_number": 123}

        _handle_check_success(
            md=mock_md,
            github=mock_github,
            asset_key="test_asset",
            check_name="test_check",
            current_record=current_record,
            timestamp=datetime.now(pytz.UTC),
            context=mock_context,
        )

        mock_github.close_issue.assert_called_once()

        mock_md.execute_query.assert_called_once()
        call_args = mock_md.execute_query.call_args
        assert "UPDATE asset_check_failures" in call_args[0][0]
        assert "consecutive_failures = 0" in call_args[0][0]
        assert "@last_success_timestamp" in call_args[0][0]
        assert call_args.kwargs["params"]["asset_key"] == "test_asset"

    def test_handle_materialization_failure(self):
        """Test handling asset materialization failure."""
        from macro_agents.defs.asset_failure_sensor import (
            _handle_materialization_failure,
        )

        mock_md = Mock()
        mock_github = Mock()
        mock_context = Mock()
        mock_context.log = Mock()

        current_record = None

        _handle_materialization_failure(
            md=mock_md,
            github=mock_github,
            asset_key="test_asset",
            current_record=current_record,
            timestamp=datetime.now(pytz.UTC),
            context=mock_context,
        )

        mock_md.execute_query.assert_called_once()
        call_args = mock_md.execute_query.call_args
        assert "MERGE `asset_check_failures`" in call_args[0][0]
        assert "asset_materialization" in call_args[0][0]
        assert call_args.kwargs["params"]["asset_key"] == "test_asset"

    def test_handle_materialization_success(self):
        """Test handling successful asset materialization."""
        from macro_agents.defs.asset_failure_sensor import (
            _handle_materialization_success,
        )

        mock_md = Mock()
        mock_github = Mock()
        mock_context = Mock()
        mock_context.log = Mock()

        current_record = {"consecutive_failures": 3, "github_issue_number": 456}

        _handle_materialization_success(
            md=mock_md,
            github=mock_github,
            asset_key="test_asset",
            current_record=current_record,
            timestamp=datetime.now(pytz.UTC),
            context=mock_context,
        )

        mock_github.close_issue.assert_called_once()

        mock_md.execute_query.assert_called_once()
        call_args = mock_md.execute_query.call_args
        assert "UPDATE asset_check_failures" in call_args[0][0]
        assert "consecutive_failures = 0" in call_args[0][0]
        assert call_args.kwargs["params"]["asset_key"] == "test_asset"

    def test_asset_failure_monitor_materializes_with_metadata(self):
        """Test that asset failure monitor materializes with metadata."""
        from dagster import DagsterInstance, build_op_context
        from macro_agents.defs.asset_failure_sensor import asset_failure_monitor

        mock_md = Mock()
        mock_github = Mock()
        mock_github.setup_for_execution = Mock()

        instance = DagsterInstance.ephemeral()
        instance.event_log_storage.get_asset_check_summary_records = Mock(
            return_value={}
        )
        instance.get_latest_materialization_event = Mock(return_value=None)
        mock_context = build_op_context(
            resources={"bq": mock_md, "github": mock_github}, instance=instance
        )

        with (
            patch(
                "macro_agents.defs.asset_failure_sensor._initialize_failure_tracking_table"
            ) as mock_initialize,
            patch(
                "macro_agents.defs.asset_failure_sensor._get_all_asset_check_keys",
                return_value=[],
            ),
            patch(
                "macro_agents.defs.asset_failure_sensor._get_all_asset_keys",
                return_value=[],
            ),
        ):
            result = asset_failure_monitor(mock_context)

        mock_initialize.assert_called_once()
        assert result.metadata["status"] == "ok"
        assert result.metadata["asset_checks_evaluated"] == 0
        assert result.metadata["assets_evaluated"] == 0

    def test_asset_failure_monitor_handles_errors(self):
        """Test that asset failure monitor captures errors in metadata."""
        from dagster import DagsterInstance, build_op_context
        from macro_agents.defs.asset_failure_sensor import asset_failure_monitor

        mock_md = Mock()
        mock_github = Mock()
        mock_github.setup_for_execution = Mock()

        instance = DagsterInstance.ephemeral()
        mock_context = build_op_context(
            resources={"bq": mock_md, "github": mock_github}, instance=instance
        )

        with patch(
            "macro_agents.defs.asset_failure_sensor._initialize_failure_tracking_table",
            side_effect=RuntimeError("boom"),
        ):
            result = asset_failure_monitor(mock_context)

        assert result.metadata["status"] == "error"
        assert "boom" in result.metadata["error"]


class TestAssetFailureMonitorIntegration:
    """Integration coverage for asset failure monitor with real DuckDB."""

    def test_asset_failure_monitor_creates_tracking_table(self, tmp_path):
        from dagster import DagsterInstance, build_op_context
        from macro_agents.defs.asset_failure_sensor import asset_failure_monitor
        from tests.conftest import DuckDBWarehouseStub

        bq_resource = DuckDBWarehouseStub()

        mock_github = Mock()
        mock_github.setup_for_execution = Mock()

        instance = DagsterInstance.ephemeral()
        instance.event_log_storage.get_asset_check_summary_records = Mock(
            return_value={}
        )

        context = build_op_context(
            resources={"bq": bq_resource, "github": mock_github}, instance=instance
        )

        with (
            patch(
                "macro_agents.defs.asset_failure_sensor._get_all_asset_check_keys",
                return_value=[],
            ),
            patch(
                "macro_agents.defs.asset_failure_sensor._get_all_asset_keys",
                return_value=[],
            ),
        ):
            result = asset_failure_monitor(context)

        assert result.metadata["status"] == "ok"
        assert bq_resource.table_exists("asset_check_failures")
