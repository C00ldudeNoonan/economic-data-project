from unittest.mock import MagicMock

import pytest
import requests

from macro_agents.defs.resources._url_secrets import (
    raise_for_status_safe,
    redact_secrets,
)


def test_redact_secrets_strips_api_key():
    msg = "400 Client Error for url: https://x.com/foo?series_id=ABC&api_key=SECRET&file_type=json"
    out = redact_secrets(msg)
    assert "SECRET" not in out
    assert "api_key=REDACTED" in out
    assert "series_id=ABC" in out


def test_redact_secrets_handles_multiple_param_names():
    msg = "url=https://x.com/?access_key=AAA&token=BBB&key=CCC&apikey=DDD"
    out = redact_secrets(msg)
    for leaked in ("AAA", "BBB", "CCC", "DDD"):
        assert leaked not in out


def test_redact_secrets_is_idempotent():
    msg = "api_key=SECRET"
    assert redact_secrets(redact_secrets(msg)) == redact_secrets(msg)


def test_raise_for_status_safe_scrubs_http_error():
    response = MagicMock(spec=requests.Response)
    response.raise_for_status.side_effect = requests.HTTPError(
        "400 Client Error for url: https://api.stlouisfed.org/?api_key=LEAKED&series_id=X"
    )

    with pytest.raises(requests.HTTPError) as exc_info:
        raise_for_status_safe(response)

    assert "LEAKED" not in str(exc_info.value)
    assert "REDACTED" in str(exc_info.value)


def test_raise_for_status_safe_passes_when_ok():
    response = MagicMock(spec=requests.Response)
    response.raise_for_status.return_value = None
    raise_for_status_safe(response)
