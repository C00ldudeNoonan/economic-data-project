"""Tests for the Gmail OAuth notifier resource."""

import json

import pytest

from macro_agents.defs.resources.gmail import GmailNotifierResource


def test_missing_token_file_raises(tmp_path):
    resource = GmailNotifierResource(
        token_path=str(tmp_path / "missing.json"),
        sender="me@example.com",
    )
    with pytest.raises(FileNotFoundError, match="OAuth token file not found"):
        resource._load_credentials()


def test_token_missing_required_fields_raises(tmp_path):
    token_path = tmp_path / "bad.json"
    token_path.write_text(json.dumps({"refresh_token": "abc"}))  # no client_id/secret
    resource = GmailNotifierResource(
        token_path=str(token_path), sender="me@example.com"
    )
    with pytest.raises(ValueError, match="missing required fields"):
        resource._load_credentials()


def test_token_with_required_fields_builds_credentials(tmp_path):
    token_path = tmp_path / "token.json"
    token_path.write_text(
        json.dumps(
            {
                "token": "access-token",
                "refresh_token": "refresh-token",
                "client_id": "client-id",
                "client_secret": "client-secret",
                "token_uri": "https://oauth2.googleapis.com/token",
                "scopes": ["https://www.googleapis.com/auth/gmail.send"],
            }
        )
    )
    resource = GmailNotifierResource(
        token_path=str(token_path), sender="me@example.com"
    )
    credentials = resource._load_credentials()
    assert credentials.refresh_token == "refresh-token"
    assert credentials.client_id == "client-id"
