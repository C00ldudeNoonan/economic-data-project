"""Dagster producer assets for the dbt-ml document_extraction project.

dbt-ml materializes GCS documents into BigQuery tables that dbt models
consume as sources (dbt-ml docs/orchestration-dagster.md). This module runs
`dbt-ml build --json` and maps each produced table to a Dagster asset whose
key matches what `dbt-ml emit-dbt-sources --dagster-meta` pins on the emitted
source tables ([source_name, table_name]). Keys are declared literally
instead of read from the dbt manifest because this code location may load
dbt in Platform observe-only mode, where no local manifest exists.

Extraction and classic-ML clustering are two separate assets/jobs, selected
by dbt-ml tag: extraction builds everything except `tag:classic_ml`, and
clustering (issue #47) builds `tag:classic_ml` (tfidf -> clusters/topics).
Keeping them apart means a sparse-corpus clustering failure cannot fail the
extraction job, and the clustering outputs get their own asset lineage
instead of materializing invisibly inside the extraction build.
"""

import json
import os
import shutil
import subprocess
import tempfile
from collections.abc import Iterator
from pathlib import Path

import dagster as dg

# Resolve the dbt target locally rather than importing it from `dbt.py`: that
# module builds a DbtProject and requires dbt_project/target/manifest.json at
# import time, so importing it (transitively, e.g. under pytest collection)
# would force a dbt parse before this producer — which only shells out to the
# dbt-ml CLI — can even be loaded. This mirrors `dbt.environment`.
environment = os.getenv("DBT_TARGET", "dev")

DBT_ML_SOURCE_NAME = "dbt_ml_document_extraction"
DBT_ML_EXTRACTION_TABLES = (
    "sec_document_registry",
    "sec_document_text",
    "sec_document_chunks",
    "fomc_document_registry",
    "fomc_document_chunks",
)
DBT_ML_CLUSTERING_TABLES = (
    "sec_document_tfidf",
    "sec_document_clusters",
    "sec_document_topics",
)
# Kept for backwards compatibility with anything importing the old name.
DBT_ML_TABLES = DBT_ML_EXTRACTION_TABLES

_CLASSIC_ML_TAG = "classic_ml"
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
        "'dbt-ml[bigquery,gcs,html,pdf]' to run document extraction."
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
    # docker-compose injects GCS_BUCKET_NAME as ${GCS_BUCKET_NAME}, i.e. a
    # present-but-empty var when the host leaves it unset. dbt-ml's
    # env_var('GCS_BUCKET_NAME', <default>) only applies the default when the
    # var is entirely absent, so an empty value would render gs:///... . Drop
    # an empty (or whitespace) value so the profile falls back to the
    # per-target default bucket.
    if not env.get("GCS_BUCKET_NAME", "").strip():
        env.pop("GCS_BUCKET_NAME", None)
    return env


def _run_dbt_ml_build(
    context: dg.AssetExecutionContext,
    tables: tuple[str, ...],
    *,
    select: str | None = None,
    exclude: str | None = None,
) -> Iterator[dg.MaterializeResult]:
    """Run `dbt-ml build --json` for a tag selection and emit one
    MaterializeResult per table in `tables`."""
    project_dir = _resolve_project_dir()
    command = [
        *_dbt_ml_launcher(project_dir),
        "--project-dir",
        str(project_dir),
        "--target",
        environment,
        "build",
        "--json",
    ]
    if select is not None:
        command += ["--select", select]
    if exclude is not None:
        command += ["--exclude", exclude]

    proc = subprocess.run(
        command, capture_output=True, text=True, env=_subprocess_env()
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

    for table in tables:
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
                "elapsed_seconds": result.get("duration_seconds"),
            },
        )


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            key=dg.AssetKey([DBT_ML_SOURCE_NAME, table]),
            group_name="document_extraction",
            description=f"dbt-ml materialized table {table}",
            kinds={"bigquery"},
        )
        for table in DBT_ML_EXTRACTION_TABLES
    ],
)
def dbt_ml_document_extraction(context: dg.AssetExecutionContext):
    """Extract SEC/FOMC documents into BigQuery (everything except classic ML)."""
    yield from _run_dbt_ml_build(
        context, DBT_ML_EXTRACTION_TABLES, exclude=f"tag:{_CLASSIC_ML_TAG}"
    )


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            key=dg.AssetKey([DBT_ML_SOURCE_NAME, table]),
            group_name="document_clustering",
            description=f"dbt-ml classic-ML table {table} (issue #47)",
            kinds={"bigquery"},
            # Reads sec_document_text, produced by the extraction asset.
            deps=[dg.AssetKey([DBT_ML_SOURCE_NAME, "sec_document_text"])],
        )
        for table in DBT_ML_CLUSTERING_TABLES
    ],
)
def dbt_ml_document_clustering(context: dg.AssetExecutionContext):
    """Fit classic-ML TF-IDF features, k-means clusters, and NMF topics over
    the SEC corpus (issue #47). Separate from extraction so a sparse-corpus
    fit failure cannot fail the extraction job."""
    yield from _run_dbt_ml_build(
        context, DBT_ML_CLUSTERING_TABLES, select=f"tag:{_CLASSIC_ML_TAG}"
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

dbt_ml_clustering_job = dg.define_asset_job(
    name="dbt_ml_clustering_job",
    tags={"dagster/priority": "5", "dagster/max_runtime": 3600},
    selection=dg.AssetSelection.assets(dbt_ml_document_clustering),
    description=(
        "Fit classic-ML clustering/topics over the SEC document corpus "
        "(issue #47): TF-IDF features -> k-means clusters + NMF topics."
    ),
)
