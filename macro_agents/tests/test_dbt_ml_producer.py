"""The dbt-ml producer parses `dbt-ml build --json` (issue #92).

This is the project's one coupling point to dbt-ml's run_results.json
contract, so these tests pin the field names the producer reads. They caught
the `execution_time` -> `duration_seconds` drift after the v0.2.9 bump; a
future dbt-ml upgrade that renames a field again fails here instead of
silently emitting null metadata.
"""

import json
import subprocess
from types import SimpleNamespace

import dagster as dg
import pytest

from macro_agents.defs.transformation import dbt_ml


def _run_results_payload() -> dict:
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
            for table in dbt_ml.DBT_ML_TABLES
        ],
    }


def _materialize(monkeypatch: pytest.MonkeyPatch, payload: dict, returncode: int = 0):
    def fake_run(*_args, **_kwargs):
        return subprocess.CompletedProcess(
            args=[], returncode=returncode, stdout=json.dumps(payload), stderr=""
        )

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(dbt_ml, "_resolve_project_dir", lambda: SimpleNamespace())
    monkeypatch.setattr(dbt_ml, "_dbt_ml_launcher", lambda _dir: ["dbt-ml"])
    monkeypatch.setattr(dbt_ml, "_subprocess_env", dict)
    context = dg.build_asset_context()
    return list(dbt_ml.dbt_ml_document_extraction(context))


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


def test_producer_raises_on_misconfigured_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(RuntimeError, match="misconfigured"):
        _materialize(monkeypatch, {}, returncode=2)


def test_producer_raises_on_build_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(RuntimeError, match="build failed"):
        _materialize(monkeypatch, {}, returncode=1)
