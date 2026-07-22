"""The dbt-ml producer parses `dbt-ml build --json` (issue #92).

This is the project's one coupling point to dbt-ml's run_results.json
contract, so these tests pin the field names the producer reads. They caught
the `execution_time` -> `duration_seconds` drift after the v0.2.9 bump; a
future dbt-ml upgrade that renames a field again fails here instead of
silently emitting null metadata.
"""

import importlib.util
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import dagster as dg
import pytest

# Load the producer module directly by path rather than via
# `from macro_agents.defs.transformation import dbt_ml`. That package import
# runs transformation/__init__.py, which imports dbt.py and requires
# dbt_project/target/manifest.json at import time — so on a clean checkout it
# would abort collection before the offline-parse fixture can build the
# manifest. The producer only shells out to the dbt-ml CLI and has no
# macro_agents imports, so it loads standalone with no manifest needed.
_DBT_ML_PATH = (
    Path(__file__).parents[1] / "src/macro_agents/defs/transformation/dbt_ml.py"
)
_spec = importlib.util.spec_from_file_location(
    "dbt_ml_producer_under_test", _DBT_ML_PATH
)
dbt_ml = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dbt_ml)


def _run_results_payload(tables: tuple[str, ...] = dbt_ml.DBT_ML_TABLES) -> dict:
    """A representative run_results.json as dbt-ml v0.2.9 emits it."""
    return {
        "metadata": {
            "dbt_ml_version": "0.2.9",
            "invocation": "build",
            "status": "success",
            "target": {"adapter_type": "bigquery", "schema": "economics_documents_dev"},
        },
        "results": [
            {
                "model_name": table,
                "status": "success",
                "duration_seconds": 1.5,
                "rows_written": 100,
                "documents_processed": 10,
                "documents_skipped": 2,
                "errors": [],
                "test_failures": [],
                "relation": {
                    "fully_qualified": f"proj.economics_documents_dev.{table}"
                },
            }
            for table in tables
        ],
    }


def _materialize(
    monkeypatch: pytest.MonkeyPatch,
    payload: dict,
    returncode: int = 0,
    asset=dbt_ml.dbt_ml_document_extraction,
):
    def fake_run(*_args, **_kwargs):
        return subprocess.CompletedProcess(
            args=[], returncode=returncode, stdout=json.dumps(payload), stderr=""
        )

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(dbt_ml, "_resolve_project_dir", lambda: SimpleNamespace())
    monkeypatch.setattr(dbt_ml, "_dbt_ml_launcher", lambda _dir: ["dbt-ml"])
    monkeypatch.setattr(dbt_ml, "_subprocess_env", dict)
    context = dg.build_asset_context()
    return list(asset(context))


def test_producer_maps_json_contract_to_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    results = _materialize(monkeypatch, _run_results_payload())

    assert len(results) == len(dbt_ml.DBT_ML_TABLES)
    by_table = {r.asset_key.path[-1]: r for r in results}
    assert set(by_table) == set(dbt_ml.DBT_ML_TABLES)

    sec_registry = by_table["sec_document_registry"].metadata
    # elapsed_seconds must read duration_seconds, not the old execution_time
    assert sec_registry["elapsed_seconds"] == 1.5
    assert sec_registry["rows_written"] == 100
    assert sec_registry["documents_processed"] == 10
    assert sec_registry["documents_skipped"] == 2
    assert sec_registry["status"] == "success"
    assert sec_registry["warehouse"] == "bigquery"
    assert sec_registry["failed_documents"] == 0
    assert sec_registry["failed_tests"] == 0
    assert (
        sec_registry["relation"] == "proj.economics_documents_dev.sec_document_registry"
    )


def test_clustering_producer_maps_ml_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The clustering asset is separate from extraction and surfaces the three
    # classic-ML tables (issue #47) as their own Dagster materializations.
    payload = _run_results_payload(dbt_ml.DBT_ML_CLUSTERING_TABLES)
    results = _materialize(
        monkeypatch, payload, asset=dbt_ml.dbt_ml_document_clustering
    )

    by_table = {r.asset_key.path[-1]: r for r in results}
    assert set(by_table) == set(dbt_ml.DBT_ML_CLUSTERING_TABLES)
    assert by_table["sec_document_clusters"].metadata["rows_written"] == 100
    # clustering tables are keyed under the same source name as extraction
    assert results[0].asset_key.path[0] == dbt_ml.DBT_ML_SOURCE_NAME


def test_producer_raises_on_misconfigured_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(RuntimeError, match="misconfigured"):
        _materialize(monkeypatch, {}, returncode=2)


def test_producer_raises_on_build_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(RuntimeError, match="build failed"):
        _materialize(monkeypatch, {}, returncode=1)


def test_subprocess_env_drops_empty_gcs_bucket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # docker-compose injects ${GCS_BUCKET_NAME} as a present-but-empty var;
    # it must be dropped so dbt-ml's env_var default (per-target bucket) applies
    # instead of rendering gs:///.
    monkeypatch.setenv("GCS_BUCKET_NAME", "")
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "proj")
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    assert "GCS_BUCKET_NAME" not in dbt_ml._subprocess_env()


def test_subprocess_env_keeps_pinned_gcs_bucket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GCS_BUCKET_NAME", "pinned-bucket")
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "proj")
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    assert dbt_ml._subprocess_env()["GCS_BUCKET_NAME"] == "pinned-bucket"
