"""
Unit tests for resources.
"""

import polars as pl
import tempfile
import os
import json
import pytest
from unittest.mock import Mock, patch
from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.resources.fred import FredResource
from macro_agents.defs.resources.market_stack import MarketStackResource
from macro_agents.defs.replication.sling import (
    SlingResourceWithCredentials,
    get_google_credentials_json,
)


class TestMotherDuckResource:
    """Test cases for MotherDuckResource."""

    def test_initialization_dev_environment(self):
        """Test resource initialization in dev environment."""
        resource = MotherDuckResource(
            md_token="test_token", environment="dev", local_path="test.duckdb"
        )

        assert resource.environment == "dev"
        assert resource.local_path == "test.duckdb"
        assert resource.db_connection == "test.duckdb"

    def test_initialization_prod_environment(self):
        """Test resource initialization in prod environment."""
        resource = MotherDuckResource(
            md_token="test_token", environment="prod", md_database="test_db"
        )

        assert resource.environment == "prod"
        assert resource.md_token == "test_token"
        assert resource.db_connection == "md:?motherduck_token=test_token"

    def test_table_exists(self):
        """Test table existence check."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
            resource = MotherDuckResource(
                md_token="test_token", environment="dev", local_path=tmp_file.name
            )

            # Test non-existent table
            assert not resource.table_exists("non_existent_table")

            # Create table and test
            test_df = pl.DataFrame({"id": [1, 2, 3]})
            resource.drop_create_duck_db_table("test_table", test_df)
            assert resource.table_exists("test_table")

            # Clean up
            os.unlink(tmp_file.name)


class TestFredResource:
    """Test cases for FredResource."""

    def test_initialization(self):
        """Test FredResource initialization."""
        resource = FredResource(api_key="test_key")
        assert resource.api_key == "test_key"


class TestMarketStackResource:
    """Test cases for MarketStackResource."""

    def test_initialization(self):
        """Test MarketStackResource initialization."""
        resource = MarketStackResource(api_key="test_key")
        assert resource.api_key == "test_key"


class TestSlingResourceWithCredentials:
    """Test cases for SlingResourceWithCredentials."""

    def test_get_google_credentials_json_from_string(self):
        """Test getting credentials from JSON string."""
        test_creds = {
            "type": "service_account",
            "project_id": "test-project",
            "private_key_id": "test-key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----\n",
            "client_email": "test@test-project.iam.gserviceaccount.com",
        }
        creds_json = json.dumps(test_creds)

        with patch.dict(
            os.environ, {"SLING_GOOGLE_APPLICATION_CREDENTIALS": creds_json}
        ):
            result = get_google_credentials_json()
            assert result == creds_json
            parsed = json.loads(result)
            assert parsed["project_id"] == "test-project"

    def test_get_google_credentials_json_from_file(self, tmp_path):
        """Test getting credentials from file path."""
        test_creds = {
            "type": "service_account",
            "project_id": "test-project",
            "private_key_id": "test-key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----\n",
            "client_email": "test@test-project.iam.gserviceaccount.com",
        }
        creds_file = tmp_path / "creds.json"
        creds_file.write_text(json.dumps(test_creds))

        with patch.dict(
            os.environ, {"SLING_GOOGLE_APPLICATION_CREDENTIALS": str(creds_file)}
        ):
            result = get_google_credentials_json()
            parsed = json.loads(result)
            assert parsed["project_id"] == "test-project"

    def test_get_google_credentials_json_missing_env_var(self):
        """Test error when environment variable is not set."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(
                ValueError, match="SLING_GOOGLE_APPLICATION_CREDENTIALS"
            ):
                get_google_credentials_json()

    def test_get_google_credentials_json_invalid_json(self):
        """Test error when JSON is invalid."""
        with patch.dict(
            os.environ, {"SLING_GOOGLE_APPLICATION_CREDENTIALS": "{invalid json"}
        ):
            with pytest.raises(ValueError, match="not valid JSON"):
                get_google_credentials_json()

    def test_sling_resource_setup_with_credentials(self):
        """Test SlingResourceWithCredentials setup with valid credentials."""
        test_creds = {
            "type": "service_account",
            "project_id": "test-project",
            "private_key_id": "test-key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----\n",
            "client_email": "test@test-project.iam.gserviceaccount.com",
        }
        creds_json = json.dumps(test_creds)

        mock_context = Mock()
        mock_context.log = Mock()
        mock_context.log.info = Mock()

        with patch.dict(
            os.environ, {"SLING_GOOGLE_APPLICATION_CREDENTIALS": creds_json}
        ):
            with (
                patch(
                    "macro_agents.defs.replication.sling.SlingConnectionResource"
                ) as mock_conn,
                patch(
                    "macro_agents.defs.replication.sling.SlingResource"
                ) as mock_sling,
            ):
                mock_bigquery_conn = Mock()
                mock_motherduck_conn = Mock()
                mock_conn.side_effect = [mock_bigquery_conn, mock_motherduck_conn]
                mock_sling_instance = Mock()
                mock_sling.return_value = mock_sling_instance

                resource = SlingResourceWithCredentials()
                resource.setup_for_execution(mock_context)

                assert hasattr(resource, "_sling_resource")
                assert mock_conn.call_count == 2

                bigquery_call = mock_conn.call_args_list[0]
                assert bigquery_call.kwargs["name"] == "BIGQUERY"
                assert bigquery_call.kwargs["type"] == "bigquery"
                assert bigquery_call.kwargs["key_body"] == creds_json

                mock_context.log.info.assert_any_call(
                    "BigQuery credentials loaded and validated as JSON"
                )
                mock_context.log.info.assert_any_call(
                    "Using service account for project: test-project"
                )
                mock_context.log.info.assert_any_call(
                    "BigQuery connection created successfully with key_body"
                )

    def test_sling_resource_replicate_calls_setup(self):
        """Test that replicate calls setup_for_execution if not already called."""
        test_creds = {
            "type": "service_account",
            "project_id": "test-project",
            "private_key_id": "test-key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----\n",
            "client_email": "test@test-project.iam.gserviceaccount.com",
        }
        creds_json = json.dumps(test_creds)

        mock_context = Mock()
        mock_context.log = Mock()
        mock_context.log.info = Mock()

        with patch.dict(
            os.environ, {"SLING_GOOGLE_APPLICATION_CREDENTIALS": creds_json}
        ):
            with (
                patch(
                    "macro_agents.defs.replication.sling.SlingConnectionResource"
                ) as mock_conn,
                patch(
                    "macro_agents.defs.replication.sling.SlingResource"
                ) as mock_sling,
            ):
                mock_bigquery_conn = Mock()
                mock_motherduck_conn = Mock()
                mock_conn.side_effect = [mock_bigquery_conn, mock_motherduck_conn]
                mock_sling_instance = Mock()
                mock_sling_instance.replicate.return_value = iter([])
                mock_sling.return_value = mock_sling_instance

                resource = SlingResourceWithCredentials()
                resource.replicate(mock_context)

                assert hasattr(resource, "_sling_resource")
                mock_sling_instance.replicate.assert_called_once_with(
                    context=mock_context
                )

    def test_sling_resource_setup_with_motherduck_credentials(self):
        """Test SlingResourceWithCredentials setup with MotherDuck credentials."""
        test_creds = {
            "type": "service_account",
            "project_id": "test-project",
            "private_key_id": "test-key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----\n",
            "client_email": "test@test-project.iam.gserviceaccount.com",
        }
        creds_json = json.dumps(test_creds)

        mock_context = Mock()
        mock_context.log = Mock()
        mock_context.log.info = Mock()

        env_vars = {
            "SLING_GOOGLE_APPLICATION_CREDENTIALS": creds_json,
            "MOTHERDUCK_TOKEN": "test-motherduck-token",
            "MOTHERDUCK_DATABASE": "test-database",
            "MOTHERDUCK_PROD_SCHEMA": "test-schema",
            "BIGQUERY_PROJECT_ID": "test-project-id",
            "BIGQUERY_LOCATION": "us-central1",
            "BIGQUERY_DATASET": "test-dataset",
        }

        with patch.dict(os.environ, env_vars):
            with (
                patch(
                    "macro_agents.defs.replication.sling.SlingConnectionResource"
                ) as mock_conn,
                patch(
                    "macro_agents.defs.replication.sling.SlingResource"
                ) as mock_sling,
            ):
                mock_bigquery_conn = Mock()
                mock_motherduck_conn = Mock()
                mock_conn.side_effect = [mock_bigquery_conn, mock_motherduck_conn]
                mock_sling_instance = Mock()
                mock_sling.return_value = mock_sling_instance

                resource = SlingResourceWithCredentials()
                resource.setup_for_execution(mock_context)

                assert hasattr(resource, "_sling_resource")
                assert mock_conn.call_count == 2

                motherduck_call = mock_conn.call_args_list[1]
                assert motherduck_call.kwargs["name"] == "MOTHERDUCK"
                assert motherduck_call.kwargs["type"] == "motherduck"

                bigquery_call = mock_conn.call_args_list[0]
                assert bigquery_call.kwargs["name"] == "BIGQUERY"
                assert bigquery_call.kwargs["type"] == "bigquery"
                assert bigquery_call.kwargs["key_body"] == creds_json

    def test_sling_resource_motherduck_connection_uses_env_vars(self):
        """Test that MotherDuck connection uses environment variables correctly."""
        test_creds = {
            "type": "service_account",
            "project_id": "test-project",
            "private_key_id": "test-key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----\n",
            "client_email": "test@test-project.iam.gserviceaccount.com",
        }
        creds_json = json.dumps(test_creds)

        mock_context = Mock()
        mock_context.log = Mock()
        mock_context.log.info = Mock()

        env_vars = {
            "SLING_GOOGLE_APPLICATION_CREDENTIALS": creds_json,
            "MOTHERDUCK_TOKEN": "test-motherduck-token-123",
            "MOTHERDUCK_DATABASE": "my-test-database",
            "MOTHERDUCK_PROD_SCHEMA": "production-schema",
            "BIGQUERY_PROJECT_ID": "test-project-id",
            "BIGQUERY_LOCATION": "us-central1",
            "BIGQUERY_DATASET": "test-dataset",
        }

        with patch.dict(os.environ, env_vars):
            with (
                patch(
                    "macro_agents.defs.replication.sling.SlingConnectionResource"
                ) as mock_conn,
                patch(
                    "macro_agents.defs.replication.sling.SlingResource"
                ) as mock_sling,
            ):
                mock_bigquery_conn = Mock()
                mock_motherduck_conn = Mock()
                mock_conn.side_effect = [mock_bigquery_conn, mock_motherduck_conn]
                mock_sling_instance = Mock()
                mock_sling.return_value = mock_sling_instance

                resource = SlingResourceWithCredentials()
                resource.setup_for_execution(mock_context)

                motherduck_call = mock_conn.call_args_list[1]
                assert motherduck_call.kwargs["name"] == "MOTHERDUCK"

                motherduck_kwargs = motherduck_call.kwargs
                assert "database" in motherduck_kwargs
                assert "motherduck_token" in motherduck_kwargs
                assert "schema" in motherduck_kwargs

    def test_sling_resource_both_connections_created(self):
        """Test that both BigQuery and MotherDuck connections are created."""
        test_creds = {
            "type": "service_account",
            "project_id": "test-project",
            "private_key_id": "test-key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----\n",
            "client_email": "test@test-project.iam.gserviceaccount.com",
        }
        creds_json = json.dumps(test_creds)

        mock_context = Mock()
        mock_context.log = Mock()
        mock_context.log.info = Mock()

        env_vars = {
            "SLING_GOOGLE_APPLICATION_CREDENTIALS": creds_json,
            "MOTHERDUCK_TOKEN": "test-token",
            "MOTHERDUCK_DATABASE": "test-db",
            "MOTHERDUCK_PROD_SCHEMA": "test-schema",
            "BIGQUERY_PROJECT_ID": "test-project",
            "BIGQUERY_LOCATION": "us-central1",
            "BIGQUERY_DATASET": "test-dataset",
        }

        with patch.dict(os.environ, env_vars):
            with (
                patch(
                    "macro_agents.defs.replication.sling.SlingConnectionResource"
                ) as mock_conn,
                patch(
                    "macro_agents.defs.replication.sling.SlingResource"
                ) as mock_sling,
            ):
                mock_bigquery_conn = Mock()
                mock_motherduck_conn = Mock()
                mock_conn.side_effect = [mock_bigquery_conn, mock_motherduck_conn]
                mock_sling_instance = Mock()
                mock_sling.return_value = mock_sling_instance

                resource = SlingResourceWithCredentials()
                resource.setup_for_execution(mock_context)

                assert mock_conn.call_count == 2

                connection_names = [
                    call.kwargs["name"] for call in mock_conn.call_args_list
                ]
                assert "BIGQUERY" in connection_names
                assert "MOTHERDUCK" in connection_names

                assert mock_sling.call_count == 1
                sling_call = mock_sling.call_args
                connections = sling_call.kwargs["connections"]
                assert len(connections) == 2
                assert mock_motherduck_conn in connections
                assert mock_bigquery_conn in connections
