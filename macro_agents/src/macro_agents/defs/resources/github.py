import os
from typing import TYPE_CHECKING

import dagster as dg
from github import Github, GithubException
from pydantic import Field

if TYPE_CHECKING:
    from github.Repository import Repository


class GitHubResource(dg.ConfigurableResource):
    """Resource for managing GitHub issues via the GitHub API."""

    github_token: str = Field(
        description="GitHub personal access token for authentication"
    )
    repo_owner: str = Field(
        description="GitHub repository owner (organization or user)"
    )
    repo_name: str = Field(description="GitHub repository name")

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        token = object.__getattribute__(self, "github_token")
        if isinstance(token, dg.EnvVar):
            token = os.getenv(token.env_var_name)
        if not token:
            token = os.getenv("GITHUB_TOKEN")

        owner = object.__getattribute__(self, "repo_owner")
        if isinstance(owner, dg.EnvVar):
            owner = os.getenv(owner.env_var_name)
        if not owner:
            owner = os.getenv("GITHUB_REPO_OWNER")

        name = object.__getattribute__(self, "repo_name")
        if isinstance(name, dg.EnvVar):
            name = os.getenv(name.env_var_name)
        if not name:
            name = os.getenv("GITHUB_REPO_NAME")

        log = context.log
        if not token:
            error_msg = "GITHUB_TOKEN environment variable is not set"
            if log:
                log.error(error_msg)
            raise ValueError(error_msg)

        if not owner:
            error_msg = "GITHUB_REPO_OWNER environment variable is not set"
            if log:
                log.error(error_msg)
            raise ValueError(error_msg)

        if not name:
            error_msg = "GITHUB_REPO_NAME environment variable is not set"
            if log:
                log.error(error_msg)
            raise ValueError(error_msg)

        try:
            self._client = Github(token)
            self._repo = self._client.get_repo(f"{owner}/{name}")
            if log:
                log.debug("GitHub client initialized")
        except Exception as e:
            error_msg = f"Failed to initialize GitHub client: {e!s}"
            if log:
                log.error(error_msg)
            raise RuntimeError(error_msg) from e

    @property
    def client(self) -> Github:
        if not hasattr(self, "_client"):
            raise RuntimeError(
                "GitHubResource not initialized. Call setup_for_execution first."
            )
        return self._client

    @property
    def repo(self) -> "Repository":
        if not hasattr(self, "_repo"):
            raise RuntimeError(
                "GitHubResource not initialized. Call setup_for_execution first."
            )
        return self._repo

    def create_issue(
        self,
        title: str,
        body: str,
        labels: list[str] | None = None,
        context: dg.AssetExecutionContext | None = None,
    ) -> int:
        """
        Create a new GitHub issue.

        Args:
            title: Issue title
            body: Issue description/body
            labels: Optional list of label names to apply
            context: Optional Dagster context for logging

        Returns:
            Issue number of the created issue
        """
        if not title or not title.strip():
            raise ValueError("Issue title cannot be empty")

        if not body or not body.strip():
            raise ValueError("Issue body cannot be empty")

        try:
            issue = self._repo.create_issue(title=title, body=body, labels=labels or [])

            if context:
                context.log.debug(f"Created GitHub issue #{issue.number}: {title}")

            return issue.number

        except GithubException as e:
            error_msg = f"Failed to create GitHub issue: {e!s}"
            if context:
                context.log.error(error_msg)
            raise RuntimeError(error_msg) from e

    def update_issue(
        self,
        issue_number: int,
        title: str | None = None,
        body: str | None = None,
        state: str | None = None,
        labels: list[str] | None = None,
        context: dg.AssetExecutionContext | None = None,
    ) -> None:
        """
        Update an existing GitHub issue.

        Args:
            issue_number: Issue number to update
            title: Optional new title
            body: Optional new body
            state: Optional state ('open' or 'closed')
            labels: Optional list of labels to set
            context: Optional Dagster context for logging
        """
        if issue_number <= 0:
            raise ValueError(f"Invalid issue number: {issue_number}")

        if state is not None and state not in ("open", "closed"):
            raise ValueError(f"Invalid state: {state}. Must be 'open' or 'closed'")

        try:
            issue = self._repo.get_issue(issue_number)

            if title is not None:
                issue.edit(title=title)
            if body is not None:
                issue.edit(body=body)
            if state is not None:
                issue.edit(state=state)
            if labels is not None:
                issue.set_labels(*labels)

            if context:
                context.log.debug(f"Updated GitHub issue #{issue_number}")

        except GithubException as e:
            error_msg = f"Failed to update GitHub issue #{issue_number}: {e!s}"
            if context:
                context.log.error(error_msg)
            raise RuntimeError(error_msg) from e

    def close_issue(
        self,
        issue_number: int,
        comment: str | None = None,
        context: dg.AssetExecutionContext | None = None,
    ) -> None:
        """
        Close a GitHub issue.

        Args:
            issue_number: Issue number to close
            comment: Optional closing comment
            context: Optional Dagster context for logging
        """
        if issue_number <= 0:
            raise ValueError(f"Invalid issue number: {issue_number}")

        try:
            issue = self._repo.get_issue(issue_number)

            if comment:
                issue.create_comment(comment)

            issue.edit(state="closed")

            if context:
                context.log.debug(f"Closed GitHub issue #{issue_number}")

        except GithubException as e:
            error_msg = f"Failed to close GitHub issue #{issue_number}: {e!s}"
            if context:
                context.log.error(error_msg)
            raise RuntimeError(error_msg) from e

    def add_comment(
        self,
        issue_number: int,
        comment: str,
        context: dg.AssetExecutionContext | None = None,
    ) -> None:
        """
        Add a comment to an existing GitHub issue.

        Args:
            issue_number: Issue number
            comment: Comment text
            context: Optional Dagster context for logging
        """
        if issue_number <= 0:
            raise ValueError(f"Invalid issue number: {issue_number}")

        if not comment or not comment.strip():
            raise ValueError("Comment cannot be empty")

        try:
            issue = self._repo.get_issue(issue_number)
            issue.create_comment(comment)

            if context:
                context.log.debug(f"Added comment to GitHub issue #{issue_number}")

        except GithubException as e:
            error_msg = f"Failed to add comment to issue #{issue_number}: {e!s}"
            if context:
                context.log.error(error_msg)
            raise RuntimeError(error_msg) from e


github_resource = GitHubResource(
    github_token=dg.EnvVar("GITHUB_TOKEN"),
    repo_owner=dg.EnvVar("GITHUB_REPO_OWNER"),
    repo_name=dg.EnvVar("GITHUB_REPO_NAME"),
)
