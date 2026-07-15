"""Environment-derived resource defaults (dev/staging/prod isolation).

These defaults are what keep dev and staging runs from writing to prod
BigQuery datasets or the prod GCS bucket when no explicit override is set.
"""

import pytest

from macro_agents.defs.resources.bigquery_warehouse import default_raw_dataset
from macro_agents.defs.resources.gcs import GCSResource, default_gcs_bucket


@pytest.mark.parametrize(
    ("environment", "expected"),
    [
        ("prod", "economics_raw"),
        ("staging", "economics_raw_staging"),
        ("dev", "economics_raw_dev"),
        ("unrecognized", "economics_raw_dev"),
    ],
)
def test_default_raw_dataset_follows_environment(monkeypatch, environment, expected):
    monkeypatch.delenv("BIGQUERY_DATASET", raising=False)
    monkeypatch.setenv("ENVIRONMENT", environment)
    assert default_raw_dataset() == expected


def test_default_raw_dataset_defaults_to_dev(monkeypatch):
    monkeypatch.delenv("BIGQUERY_DATASET", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    assert default_raw_dataset() == "economics_raw_dev"


def test_default_raw_dataset_explicit_override_wins(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "prod")
    monkeypatch.setenv("BIGQUERY_DATASET", "custom_dataset")
    assert default_raw_dataset() == "custom_dataset"


@pytest.mark.parametrize(
    ("environment", "expected"),
    [
        ("prod", "econ-project-general-storage"),
        ("staging", "econ-project-general-storage-staging"),
        ("dev", "econ-project-general-storage-dev"),
        ("unrecognized", "econ-project-general-storage-dev"),
    ],
)
def test_default_gcs_bucket_follows_environment(monkeypatch, environment, expected):
    monkeypatch.delenv("GCS_BUCKET_NAME", raising=False)
    monkeypatch.setenv("ENVIRONMENT", environment)
    assert default_gcs_bucket() == expected


def test_default_gcs_bucket_defaults_to_dev(monkeypatch):
    monkeypatch.delenv("GCS_BUCKET_NAME", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    assert default_gcs_bucket() == "econ-project-general-storage-dev"


def test_default_gcs_bucket_explicit_override_wins(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "prod")
    monkeypatch.setenv("GCS_BUCKET_NAME", "pinned-bucket")
    assert default_gcs_bucket() == "pinned-bucket"


def test_gcs_resource_resolves_bucket_at_runtime(monkeypatch):
    monkeypatch.delenv("GCS_BUCKET_NAME", raising=False)
    monkeypatch.setenv("ENVIRONMENT", "staging")
    assert GCSResource().resolved_bucket_name == "econ-project-general-storage-staging"


def test_gcs_resource_configured_bucket_wins(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "dev")
    assert (
        GCSResource(bucket_name="explicit-bucket").resolved_bucket_name
        == "explicit-bucket"
    )
