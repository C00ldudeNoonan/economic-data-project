"""Dagster producer assets for the dbt-ml document_extraction project.

dbt-ml materializes GCS documents into BigQuery tables that dbt models
consume as sources (dbt-ml docs/orchestration-dagster.md). This module runs
`dbt-ml build --json` as one multi_asset whose outputs carry the same asset
keys that `dbt-ml emit-dbt-sources --dagster-meta` pins on the emitted
source tables ([source_name, table_name]). Keys are declared literally
instead of read from the dbt manifest because this code location may load
dbt in Platform observe-only mode, where no local manifest exists.
"""

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import dagster as dg

from macro_agents.defs.transformation.dbt import environment

DBT_ML_SOURCE_NAME = "dbt_ml_document_extraction"
DBT_ML_TABLES = (
    "sec_document_registry",
    "sec_document_text",
    "sec_document_chunks",
    "fomc_document_registry",
    "fomc_document_chunks",
)

_DEFAULT_GCP_PROJECT = "econ-data-project-478800"


def _resolve_project_dir() -> Path:
    env_dir = os.getenv("DBT_ML_PROJECT_DIR")
    if env_dir:
        path = Path(env_dir).resolve()
        if not path.exists():
            raise FileNotFoundError(
                "DBT_ML_PROJECT_DIR environment variable points to a non-existent path."
            )
        return path

    repo_root = Path(__file__).resolve().parents[5]
    default_dir = repo_root / "document_extraction"
    if not (default_dir / "dbt_ml_project.yml").exists():
        raise FileNotFoundError(
            "Could not find the document_extraction dbt-ml project. "
            "Set DBT_ML_PROJECT_DIR if it lives elsewhere."
        )
    return default_dir


def _dbt_ml_launcher(project_dir: Path) -> list[str]:
    """Command prefix that provides the dbt-ml CLI.

    dbt-ml requires Python >= 3.12 while macro_agents pins 3.11, so the
    document_extraction project carries its own uv-managed environment.
    Fall back to a dbt-ml already on PATH when uv is unavailable.
    """
    if shutil.which("uv"):
        return ["uv", "run", "--project", str(project_dir), "dbt-ml"]
    if shutil.which("dbt-ml"):
        return ["dbt-ml"]
    raise RuntimeError(
        "Neither uv nor dbt-ml is on PATH; install uv or "
        "'dbt-ml[bigquery,gcs]' to run document extraction."
    )


def _subprocess_env() -> dict[str, str]:
    """Environment for the dbt-ml subprocess.

    GOOGLE_APPLICATION_CREDENTIALS may hold inline service-account JSON in
    this deployment (see BigQueryWarehouseResource); google-auth requires a
    file path, so inline JSON is materialized to a temp file. The GCS client
    cannot infer a project from user ADC, so GOOGLE_CLOUD_PROJECT gets a
    default.
    """
    env = os.environ.copy()
    credentials = env.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if credentials.startswith("{"):
        credentials_path = os.path.join(
            tempfile.gettempdir(), "gcp_service_account_credentials.json"
        )
        Path(credentials_path).write_text(credentials, encoding="utf-8")
        env["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    if not env.get("GOOGLE_CLOUD_PROJECT"):
        env["GOOGLE_CLOUD_PROJECT"] = _DEFAULT_GCP_PROJECT
    return env


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            key=dg.AssetKey([DBT_ML_SOURCE_NAME, table]),
            group_name="document_extraction",
            description=f"dbt-ml materialized table {table}",
            kinds={"bigquery"},
        )
        for table in DBT_ML_TABLES
    ],
)
def dbt_ml_document_extraction(context: dg.AssetExecutionContext):
    """Run the dbt-ml document_extraction project (extraction + tests)."""
    project_dir = _resolve_project_dir()
    proc = subprocess.run(
        [
            *_dbt_ml_launcher(project_dir),
            "--project-dir",
            str(project_dir),
            "--target",
            environment,
            "build",
            "--json",
        ],
        capture_output=True,
        text=True,
        env=_subprocess_env(),
    )
    # Exit codes (dbt-ml issue #87): 0 = ok, 1 = a model/document/test
    # failed, 2 = misconfigured project.
    if proc.returncode == 2:
        raise RuntimeError(f"dbt-ml project is misconfigured:\n{proc.stderr}")
    if proc.returncode != 0:
        context.log.error(proc.stdout or proc.stderr)
        raise RuntimeError("dbt-ml build failed")

    payload = json.loads(proc.stdout)
    by_model = {result["model_name"]: result for result in payload["results"]}
    target = payload["metadata"]["target"]

    for table in DBT_ML_TABLES:
        result = by_model.get(table, {})
        yield dg.MaterializeResult(
            asset_key=dg.AssetKey([DBT_ML_SOURCE_NAME, table]),
            metadata={
                "relation": result.get("relation", {}).get("fully_qualified"),
                "status": result.get("status"),
                "rows_written": result.get("rows_written", 0),
                "documents_processed": result.get("documents_processed", 0),
                "documents_skipped": result.get("documents_skipped", 0),
                "failed_documents": len(result.get("errors", [])),
                "failed_tests": len(result.get("test_failures", [])),
                "warehouse": target["adapter_type"],
                "elapsed_seconds": result.get("execution_time"),
            },
        )


dbt_ml_documents_job = dg.define_asset_job(
    name="dbt_ml_documents_job",
    tags={"dagster/priority": "5", "dagster/max_runtime": 3600},
    selection=dg.AssetSelection.assets(dbt_ml_document_extraction),
    description=(
        "Run the dbt-ml document_extraction project: SEC filing HTML from "
        "GCS into BigQuery registry and chunk tables."
    ),
)
