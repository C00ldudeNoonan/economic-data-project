"""Utility for scrubbing API keys/tokens from URLs before they reach logs.

Several upstream APIs (FRED, MarketStack, Census) accept credentials as
query string parameters. When `requests.Response.raise_for_status()` raises,
the resulting `HTTPError` message embeds the full URL — including the secret —
and that message gets persisted to Dagster's event log indefinitely.
"""

import re

import requests

SECRET_QUERY_PARAMS = (
    "api_key",
    "apikey",
    "access_key",
    "access_token",
    "auth_token",
    "token",
    "secret",
    "key",
)

_SECRET_RE = re.compile(
    r"((?:" + "|".join(SECRET_QUERY_PARAMS) + r"))=([^&\s\"'<>]+)",
    re.IGNORECASE,
)


def redact_secrets(text: str) -> str:
    """Replace known secret query-param values with REDACTED."""
    return _SECRET_RE.sub(r"\1=REDACTED", text)


def raise_for_status_safe(response: requests.Response) -> None:
    """Like `response.raise_for_status()` but scrubs secrets from the error message."""
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise requests.HTTPError(
            redact_secrets(str(exc)), response=response, request=exc.request
        ) from None


def get_safe(url: str, **kwargs) -> requests.Response:
    """Like `requests.get` but scrubs secrets from any `RequestException` raised."""
    try:
        return requests.get(url, **kwargs)
    except requests.RequestException as exc:
        raise type(exc)(redact_secrets(str(exc))) from None
