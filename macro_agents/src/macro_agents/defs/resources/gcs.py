import json
import os
from typing import Optional, List, Dict, Any
from pathlib import Path
import dagster as dg
from pydantic import Field

try:
    from google.cloud import storage
    from google.oauth2 import service_account
except ImportError:
    storage = None
    service_account = None


class GCSResource(dg.ConfigurableResource):
    """Resource for managing DSPy model storage in Google Cloud Storage."""

    bucket_name: str = Field(description="GCS bucket name for storing models")
    credentials_path: Optional[str] = Field(
        default=None,
        description="Path to Google service account credentials JSON file. Can be set via GOOGLE_APPLICATION_CREDENTIALS env var.",
    )
    local_cache_dir: str = Field(
        default=".dspy_models",
        description="Local directory for caching downloaded models",
    )

    def setup_for_execution(self, context) -> None:
        """Initialize GCS client."""
        if storage is None:
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
                f"Credentials not found. Expected either a valid file path or JSON string, got: {creds_path[:100]}..."
            )

        self._client = storage.Client(credentials=credentials)
        self._bucket = self._client.bucket(self.bucket_name)

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
        model_data: Dict[str, Any],
        context: Optional[dg.AssetExecutionContext] = None,
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
            log.info(
                f"Uploaded model {module_name} v{version} to gs://{self.bucket_name}/{gcs_path}"
            )

        return gcs_path

    def download_model(
        self,
        module_name: str,
        version: str,
        context: Optional[dg.AssetExecutionContext] = None,
    ) -> Dict[str, Any]:
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
            raise FileNotFoundError(
                f"Model not found at gs://{self.bucket_name}/{gcs_path}"
            )

        model_json = blob.download_as_text()
        model_data = json.loads(model_json)

        # Cache locally
        with open(local_cache_path, "w") as f:
            json.dump(model_data, f, indent=2)

        if log:
            log.info(
                f"Downloaded model {module_name} v{version} from gs://{self.bucket_name}/{gcs_path}"
            )

        return model_data

    def list_versions(
        self,
        module_name: str,
        context: Optional[dg.AssetExecutionContext] = None,
    ) -> List[str]:
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
        context: Optional[dg.AssetExecutionContext] = None,
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
