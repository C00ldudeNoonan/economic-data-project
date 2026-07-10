import json
import os
from pathlib import Path
from types import ModuleType
from typing import Any

import dagster as dg
from pydantic import Field

# Optional GCS deps — keep imports lazy-safe so the resource module can be
# loaded in environments without google-cloud-storage installed.
storage: ModuleType | None = None
service_account: ModuleType | None = None

try:
    from google.cloud import storage as _storage_mod
    from google.oauth2 import service_account as _service_account_mod

    storage = _storage_mod
    service_account = _service_account_mod
except ImportError:
    pass


def default_gcs_bucket() -> str:
    """Environment-suffixed document storage bucket name.

    GCS_BUCKET_NAME overrides when set (deployment pins it explicitly).
    Otherwise mirrors default_raw_dataset()'s environment convention:
      prod    → econ-project-general-storage
      staging → econ-project-general-storage-staging
      dev     → econ-project-general-storage-dev
    Computed at call time so env overrides take effect without reimport.
    """
    explicit = os.getenv("GCS_BUCKET_NAME")
    if explicit:
        return explicit
    environment = os.getenv("ENVIRONMENT", "dev")
    suffix = {"prod": "", "staging": "-staging"}.get(environment, "-dev")
    return f"econ-project-general-storage{suffix}"


class GCSResource(dg.ConfigurableResource):
    """Resource for managing DSPy model storage in Google Cloud Storage."""

    bucket_name: str = Field(
        default="",
        description=(
            "GCS bucket name. Empty means resolve at runtime via "
            "default_gcs_bucket() (GCS_BUCKET_NAME env var, else the "
            "ENVIRONMENT-suffixed document bucket)."
        ),
    )
    credentials_path: str | None = Field(
        default=None,
        description="Path to Google service account credentials JSON file. Can be set via GOOGLE_APPLICATION_CREDENTIALS env var.",
    )
    local_cache_dir: str = Field(
        default=".dspy_models",
        description="Local directory for caching downloaded models",
    )

    @property
    def resolved_bucket_name(self) -> str:
        """Configured bucket, or the environment-derived default."""
        return self.bucket_name or default_gcs_bucket()

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        """Initialize GCS client."""
        if storage is None or service_account is None:
            raise ImportError(
                "google-cloud-storage is not installed. Install it with: pip install google-cloud-storage"
            )

        # Check for credentials path from env var if not set directly
        creds_path = self.credentials_path
        if not creds_path:
            creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        if not creds_path:
            raise ValueError(
                "credentials_path must be set or GOOGLE_APPLICATION_CREDENTIALS environment variable must be provided"
            )

        # Check if creds_path is a JSON string (starts with {) or a file path
        if creds_path.strip().startswith("{"):
            # It's a JSON string, parse it and use from_service_account_info
            try:
                creds_info = json.loads(creds_path)
                credentials = service_account.Credentials.from_service_account_info(
                    creds_info
                )
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"Failed to parse service account JSON from GOOGLE_APPLICATION_CREDENTIALS: {e}"
                )
        elif os.path.exists(creds_path):
            # It's a file path, use from_service_account_file
            credentials = service_account.Credentials.from_service_account_file(
                creds_path
            )
        else:
            raise FileNotFoundError(
                "Credentials not found. Expected either a valid file path or JSON string."
            )

        self._client = storage.Client(credentials=credentials)
        self._bucket = self._client.bucket(self.resolved_bucket_name)

        # Create local cache directory if it doesn't exist
        Path(self.local_cache_dir).mkdir(parents=True, exist_ok=True)

    @property
    def client(self):
        """Get GCS client."""
        return self._client

    @property
    def bucket(self):
        """Get GCS bucket."""
        return self._bucket

    def upload_model(
        self,
        module_name: str,
        version: str,
        model_data: dict[str, Any],
        context: dg.AssetExecutionContext | None = None,
    ) -> str:
        """
        Upload a DSPy model to GCS.

        Args:
            module_name: Name of the module (e.g., 'economy_state', 'investment_recommendations')
            version: Version string (e.g., '1.0.0' or timestamp-based)
            model_data: Dictionary containing model data (will be serialized to JSON)
            context: Optional Dagster context for logging

        Returns:
            GCS path where model was uploaded
        """
        log = context.log if context else None

        gcs_path = f"models/{module_name}/{version}/model.json"
        blob = self._bucket.blob(gcs_path)

        # Serialize model data to JSON
        model_json = json.dumps(model_data, indent=2)

        # Upload to GCS
        blob.upload_from_string(model_json, content_type="application/json")

        if log:
            log.info(f"Uploaded model {module_name} v{version} to GCS")

        return gcs_path

    def download_model(
        self,
        module_name: str,
        version: str,
        context: dg.AssetExecutionContext | None = None,
    ) -> dict[str, Any]:
        """
        Download a DSPy model from GCS.

        Args:
            module_name: Name of the module
            version: Version string
            context: Optional Dagster context for logging

        Returns:
            Dictionary containing model data
        """
        log = context.log if context else None

        gcs_path = f"models/{module_name}/{version}/model.json"
        local_cache_path = Path(self.local_cache_dir) / f"{module_name}_{version}.json"

        # Check local cache first
        if local_cache_path.exists():
            if log:
                log.info(f"Loading model {module_name} v{version} from local cache")
            with open(local_cache_path, "r") as f:
                return json.load(f)

        # Download from GCS
        blob = self._bucket.blob(gcs_path)

        if not blob.exists():
            raise FileNotFoundError(f"Model not found at GCS path {gcs_path}")

        model_json = blob.download_as_text()
        model_data = json.loads(model_json)

        # Cache locally
        with open(local_cache_path, "w") as f:
            json.dump(model_data, f, indent=2)

        if log:
            log.info(f"Downloaded model {module_name} v{version} from GCS")

        return model_data

    def list_versions(
        self,
        module_name: str,
        context: dg.AssetExecutionContext | None = None,
    ) -> list[str]:
        """
        List all versions of a module stored in GCS.

        Args:
            module_name: Name of the module
            context: Optional Dagster context for logging

        Returns:
            List of version strings
        """
        prefix = f"models/{module_name}/"
        blobs = self._bucket.list_blobs(prefix=prefix)

        versions = set()
        for blob in blobs:
            # Extract version from path: models/{module_name}/{version}/model.json
            parts = blob.name.split("/")
            if len(parts) >= 3 and parts[-1] == "model.json":
                versions.add(parts[2])

        return sorted(list(versions))

    def model_exists(
        self,
        module_name: str,
        version: str,
        context: dg.AssetExecutionContext | None = None,
    ) -> bool:
        """
        Check if a model version exists in GCS.

        Args:
            module_name: Name of the module
            version: Version string
            context: Optional Dagster context for logging

        Returns:
            True if model exists, False otherwise
        """
        gcs_path = f"models/{module_name}/{version}/model.json"
        blob = self._bucket.blob(gcs_path)
        return blob.exists()

    def upload_json(
        self,
        gcs_path: str,
        data: dict[str, Any],
        context: dg.AssetExecutionContext | None = None,
    ) -> str:
        """
        Upload a JSON file to GCS.

        Args:
            gcs_path: Path within the bucket (e.g., "fomc/2024/2024-01-31.json")
            data: Dictionary to serialize as JSON
            context: Optional Dagster context for logging

        Returns:
            Full GCS URI (gs://bucket/path)
        """
        log = context.log if context else None

        blob = self._bucket.blob(gcs_path)

        # Serialize to JSON
        json_data = json.dumps(data, indent=2, default=str)

        # Upload to GCS
        blob.upload_from_string(json_data, content_type="application/json")

        if log:
            log.info(f"Uploaded JSON to GCS path {gcs_path}")

        return f"gs://{self.resolved_bucket_name}/{gcs_path}"

    def upload_bytes(
        self,
        gcs_path: str,
        data: bytes,
        content_type: str,
        context: dg.AssetExecutionContext | None = None,
    ) -> str:
        """
        Upload raw bytes to GCS.

        Args:
            gcs_path: Path within the bucket
            data: Raw bytes to upload
            content_type: MIME type (e.g., "image/png")
            context: Optional Dagster context for logging

        Returns:
            Full GCS URI (gs://bucket/path)
        """
        log = context.log if context else None

        blob = self._bucket.blob(gcs_path)
        blob.upload_from_string(data, content_type=content_type)

        if log:
            log.info(f"Uploaded bytes to GCS path {gcs_path}")

        return f"gs://{self.resolved_bucket_name}/{gcs_path}"

    def download_json(
        self,
        gcs_path: str,
        context: dg.AssetExecutionContext | None = None,
    ) -> dict[str, Any]:
        """
        Download a JSON file from GCS.

        Args:
            gcs_path: Path within the bucket
            context: Optional Dagster context for logging

        Returns:
            Parsed JSON data as dictionary
        """
        log = context.log if context else None

        blob = self._bucket.blob(gcs_path)

        if not blob.exists():
            raise FileNotFoundError(f"File not found at GCS path {gcs_path}")

        json_data = blob.download_as_text()
        data = json.loads(json_data)

        if log:
            log.info(f"Downloaded JSON from GCS path {gcs_path}")

        return data

    def file_exists(
        self,
        gcs_path: str,
        context: dg.AssetExecutionContext | None = None,
    ) -> bool:
        """
        Check if a file exists in GCS.

        Args:
            gcs_path: Path within the bucket
            context: Optional Dagster context for logging

        Returns:
            True if file exists, False otherwise
        """
        blob = self._bucket.blob(gcs_path)
        return blob.exists()

    def list_files(
        self,
        prefix: str,
        context: dg.AssetExecutionContext | None = None,
    ) -> list[str]:
        """
        List all files with a given prefix in GCS.

        Args:
            prefix: Prefix to filter files (e.g., "fomc/2024/")
            context: Optional Dagster context for logging

        Returns:
            List of file paths within the bucket
        """
        blobs = self._bucket.list_blobs(prefix=prefix)
        return [blob.name for blob in blobs]
