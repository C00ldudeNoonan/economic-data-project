"""
Unit tests for GitHubResource.
"""

from unittest.mock import Mock, patch

import dagster as dg
from macro_agents.defs.resources.github import GitHubResource


class TestGitHubResource:
    """Test cases for GitHubResource."""

    def test_initialization(self):
        """Test resource initialization."""
        resource = GitHubResource(
            github_token="test_token", repo_owner="test_owner", repo_name="test_repo"
        )

        assert resource.github_token == "test_token"
        assert resource.repo_owner == "test_owner"
        assert resource.repo_name == "test_repo"

    def test_setup_for_execution(self):
        """Test setup with mocked GitHub client."""
        mock_context = Mock()
        mock_context.log = Mock()

        with patch("macro_agents.defs.resources.github.Github") as mock_github:
            mock_client = Mock()
            mock_repo = Mock()
            mock_github.return_value = mock_client
            mock_client.get_repo.return_value = mock_repo

            resource = GitHubResource(
                github_token="test_token",
                repo_owner="test_owner",
                repo_name="test_repo",
            )
            resource.setup_for_execution(mock_context)

            assert hasattr(resource, "_client")
            assert hasattr(resource, "_repo")
            mock_github.assert_called_once_with("test_token")
            mock_client.get_repo.assert_called_once_with("test_owner/test_repo")

    def test_create_issue(self):
        """Test creating a GitHub issue."""
        mock_context = Mock()
        mock_repo = Mock()
        mock_issue = Mock()
        mock_issue.number = 123
        mock_repo.create_issue.return_value = mock_issue

        resource = GitHubResource(
            github_token="test_token", repo_owner="test_owner", repo_name="test_repo"
        )
        resource._repo = mock_repo

        issue_number = resource.create_issue(
            title="Test Issue", body="Test body", labels=["bug"], context=mock_context
        )

        assert issue_number == 123
        mock_repo.create_issue.assert_called_once_with(
            title="Test Issue", body="Test body", labels=["bug"]
        )

    def test_close_issue(self):
        """Test closing a GitHub issue."""
        mock_context = Mock()
        mock_repo = Mock()
        mock_issue = Mock()
        mock_repo.get_issue.return_value = mock_issue

        resource = GitHubResource(
            github_token="test_token", repo_owner="test_owner", repo_name="test_repo"
        )
        resource._repo = mock_repo

        resource.close_issue(
            issue_number=123, comment="Closing comment", context=mock_context
        )

        mock_repo.get_issue.assert_called_once_with(123)
        mock_issue.create_comment.assert_called_once_with("Closing comment")
        mock_issue.edit.assert_called_once_with(state="closed")

    def test_add_comment(self):
        """Test adding a comment to an issue."""
        mock_context = Mock()
        mock_repo = Mock()
        mock_issue = Mock()
        mock_repo.get_issue.return_value = mock_issue

        resource = GitHubResource(
            github_token="test_token", repo_owner="test_owner", repo_name="test_repo"
        )
        resource._repo = mock_repo

        resource.add_comment(
            issue_number=123, comment="Test comment", context=mock_context
        )

        mock_issue.create_comment.assert_called_once_with("Test comment")

    def test_update_issue(self):
        """Test updating an existing issue."""
        mock_context = Mock()
        mock_repo = Mock()
        mock_issue = Mock()
        mock_repo.get_issue.return_value = mock_issue

        resource = GitHubResource(
            github_token="test_token", repo_owner="test_owner", repo_name="test_repo"
        )
        resource._repo = mock_repo

        resource.update_issue(
            issue_number=123,
            title="Updated Title",
            body="Updated body",
            state="closed",
            labels=["fixed"],
            context=mock_context,
        )

        mock_repo.get_issue.assert_called_once_with(123)
        assert mock_issue.edit.call_count == 3  # title, body, state
        mock_issue.set_labels.assert_called_once_with("fixed")

    def test_setup_for_execution_with_envvar(self, monkeypatch):
        """Test that setup_for_execution correctly resolves EnvVar fields."""
        monkeypatch.setenv("GITHUB_TOKEN", "test_token_from_env")
        monkeypatch.setenv("GITHUB_REPO_OWNER", "test_owner_from_env")
        monkeypatch.setenv("GITHUB_REPO_NAME", "test_repo_from_env")

        mock_context = Mock()
        mock_context.log = Mock()

        # Create resource with EnvVar (mimics how it's used in definitions.py)
        resource = GitHubResource(
            github_token=dg.EnvVar("GITHUB_TOKEN"),
            repo_owner=dg.EnvVar("GITHUB_REPO_OWNER"),
            repo_name=dg.EnvVar("GITHUB_REPO_NAME"),
        )

        # This should not raise an error
        with patch("macro_agents.defs.resources.github.Github") as mock_github:
            mock_client = Mock()
            mock_repo = Mock()
            mock_github.return_value = mock_client
            mock_client.get_repo.return_value = mock_repo

            resource.setup_for_execution(mock_context)

            # Verify the resource was initialized with values from environment
            assert hasattr(resource, "_client")
            assert hasattr(resource, "_repo")
            mock_github.assert_called_once_with("test_token_from_env")
            mock_client.get_repo.assert_called_once_with(
                "test_owner_from_env/test_repo_from_env"
            )
