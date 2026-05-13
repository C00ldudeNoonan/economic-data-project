"""Google Drive resource for monitoring and downloading files."""

import json
import io
import os
from pathlib import Path
from typing import Any

import dagster as dg
from pydantic import Field


from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload


class GoogleDriveResource(dg.ConfigurableResource):
    """Resource for monitoring and downloading files from Google Drive."""

    credentials_path: str | None = Field(
        default=None,
        description="Path to Google service account credentials JSON file. Can be set via GOOGLE_APPLICATION_CREDENTIALS env var.",
    )
    folder_id: str | None = Field(
        default=None,
        description="Google Drive folder ID to monitor. Can be set via REALTOR_GDRIVE_FOLDER_ID env var.",
    )

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        """Initialize Google Drive API client."""
        if build is None:
            raise ImportError(
                "google-api-python-client is not installed. Install it with: pip install google-api-python-client google-auth-httplib2"
            )

        creds_path = self.credentials_path
        if not creds_path:
            creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        if not creds_path:
            raise ValueError(
                "credentials_path must be set or GOOGLE_APPLICATION_CREDENTIALS environment variable must be provided"
            )

        # Parse credentials (JSON string or file path)
        if creds_path.strip().startswith("{"):
            try:
                creds_info = json.loads(creds_path)
                credentials = service_account.Credentials.from_service_account_info(
                    creds_info,
                    scopes=["https://www.googleapis.com/auth/drive.readonly"],
                )
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"Failed to parse service account JSON from GOOGLE_APPLICATION_CREDENTIALS: {e}"
                )
        elif os.path.exists(creds_path):
            credentials = service_account.Credentials.from_service_account_file(
                creds_path,
                scopes=["https://www.googleapis.com/auth/drive.readonly"],
            )
        else:
            raise FileNotFoundError(
                "Credentials not found. Expected either a valid file path or JSON string."
            )

        self._drive_service = build("drive", "v3", credentials=credentials)

        # Get folder_id from env var if not set
        if not self.folder_id:
            folder_id_from_env = os.getenv("REALTOR_GDRIVE_FOLDER_ID")
            if folder_id_from_env:
                object.__setattr__(self, "folder_id", folder_id_from_env)

        log = context.log if context else None
        if log:
            log.debug("Google Drive API client initialized successfully")

    @property
    def drive_service(self):
        """Get Google Drive service."""
        return self._drive_service

    def list_files_in_folder(
        self,
        folder_id: str | None = None,
        file_extension: str | None = None,
        context: dg.AssetExecutionContext | None = None,
    ) -> list[dict[str, Any]]:
        """
        List all files in a Google Drive folder.

        Args:
            folder_id: Folder ID to list files from. Uses self.folder_id if not provided.
            file_extension: Optional file extension filter (e.g., 'csv')
            context: Optional Dagster context for logging

        Returns:
            List of file metadata dictionaries with keys: id, name, modifiedTime, size
        """
        log = context.log if context else None
        target_folder_id = folder_id or self.folder_id

        if not target_folder_id:
            raise ValueError(
                "folder_id must be provided either as parameter, in config, or via REALTOR_GDRIVE_FOLDER_ID env var"
            )

        try:
            query = f"'{target_folder_id}' in parents and trashed=false"
            if file_extension:
                query += f" and name contains '.{file_extension}'"

            results = (
                self._drive_service.files()
                .list(
                    q=query,
                    spaces="drive",
                    fields="files(id, name, modifiedTime, size, mimeType)",
                    orderBy="modifiedTime desc",
                )
                .execute()
            )

            files = results.get("files", [])

            if log:
                log.info(
                    f"Found {len(files)} files in folder {target_folder_id}"
                    + (f" with extension .{file_extension}" if file_extension else "")
                )

            return files

        except HttpError as error:
            raise RuntimeError(
                f"An error occurred while listing files in Google Drive: {error}"
            )

    def list_files_in_subfolders(
        self,
        parent_folder_id: str | None = None,
        subfolder_names: list[str] | None = None,
        file_extension: str | None = None,
        context: dg.AssetExecutionContext | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """
        List files in specific subfolders of a parent folder.

        Args:
            parent_folder_id: Parent folder ID. Uses self.folder_id if not provided.
            subfolder_names: List of subfolder names to search (e.g., ['country', 'state', 'metro'])
            file_extension: Optional file extension filter (e.g., 'csv')
            context: Optional Dagster context for logging

        Returns:
            Dictionary mapping subfolder name to list of file metadata
        """
        log = context.log if context else None
        target_parent_folder_id = parent_folder_id or self.folder_id

        if not target_parent_folder_id:
            raise ValueError(
                "parent_folder_id must be provided either as parameter, in config, or via REALTOR_GDRIVE_FOLDER_ID env var"
            )

        if not subfolder_names:
            subfolder_names = ["country", "state", "metro", "county", "zip"]

        try:
            subfolders_query = f"'{target_parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
            subfolders_result = (
                self._drive_service.files()
                .list(
                    q=subfolders_query,
                    spaces="drive",
                    fields="files(id, name)",
                )
                .execute()
            )

            subfolders = {
                folder["name"]: folder["id"]
                for folder in subfolders_result.get("files", [])
                if folder["name"] in subfolder_names
            }

            if log:
                log.info(f"Found subfolders: {list(subfolders.keys())}")

            files_by_subfolder = {}
            for subfolder_name, subfolder_id in subfolders.items():
                files = self.list_files_in_folder(
                    folder_id=subfolder_id,
                    file_extension=file_extension,
                    context=context,
                )
                files_by_subfolder[subfolder_name] = files

            return files_by_subfolder

        except HttpError as error:
            raise RuntimeError(
                f"An error occurred while listing subfolders in Google Drive: {error}"
            )

    def download_file(
        self,
        file_id: str,
        destination_path: str,
        context: dg.AssetExecutionContext | None = None,
    ) -> str:
        """
        Download a file from Google Drive to local storage.

        Args:
            file_id: Google Drive file ID
            destination_path: Local path to save the file
            context: Optional Dagster context for logging

        Returns:
            Path to downloaded file
        """
        log = context.log if context else None

        try:
            # Create parent directory if it doesn't exist
            Path(destination_path).parent.mkdir(parents=True, exist_ok=True)

            # Download file
            request = self._drive_service.files().get_media(fileId=file_id)

            with open(destination_path, "wb") as f:
                downloader = MediaIoBaseDownload(f, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    if log and status:
                        log.info(f"Download {int(status.progress() * 100)}%")

            if log:
                log.info(f"Downloaded file {file_id} to {destination_path}")

            return destination_path

        except HttpError as error:
            raise RuntimeError(
                f"An error occurred while downloading file from Google Drive: {error}"
            )

    def get_file_content(
        self,
        file_id: str,
        context: dg.AssetExecutionContext | None = None,
    ) -> bytes:
        """
        Get file content from Google Drive as bytes without saving to disk.

        Args:
            file_id: Google Drive file ID
            context: Optional Dagster context for logging

        Returns:
            File content as bytes
        """
        log = context.log if context else None

        try:
            request = self._drive_service.files().get_media(fileId=file_id)
            file_content = io.BytesIO()

            downloader = MediaIoBaseDownload(file_content, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if log and status:
                    log.info(f"Download {int(status.progress() * 100)}%")

            if log:
                log.info(f"Retrieved file content for {file_id}")

            return file_content.getvalue()

        except HttpError as error:
            raise RuntimeError(
                f"An error occurred while getting file content from Google Drive: {error}"
            )

    def get_file_metadata(
        self,
        file_id: str,
        context: dg.AssetExecutionContext | None = None,
    ) -> dict[str, Any]:
        """
        Get metadata for a file in Google Drive.

        Args:
            file_id: Google Drive file ID
            context: Optional Dagster context for logging

        Returns:
            Dictionary with file metadata
        """
        try:
            file_metadata = (
                self._drive_service.files()
                .get(
                    fileId=file_id,
                    fields="id, name, modifiedTime, size, mimeType, createdTime",
                )
                .execute()
            )

            return file_metadata

        except HttpError as error:
            raise RuntimeError(
                f"An error occurred while getting file metadata from Google Drive: {error}"
            )

    def file_exists(
        self,
        file_id: str,
        context: dg.AssetExecutionContext | None = None,
    ) -> bool:
        """
        Check if a file exists in Google Drive.

        Args:
            file_id: Google Drive file ID
            context: Optional Dagster context for logging

        Returns:
            True if file exists, False otherwise
        """
        try:
            self._drive_service.files().get(fileId=file_id, fields="id").execute()
            return True
        except HttpError:
            return False

    def get_most_recent_file_in_folder(
        self,
        folder_id: str | None = None,
        file_extension: str | None = None,
        context: dg.AssetExecutionContext | None = None,
    ) -> dict[str, Any] | None:
        """
        Get the most recently modified file in a folder.

        Args:
            folder_id: Folder ID to search. Uses self.folder_id if not provided.
            file_extension: Optional file extension filter (e.g., 'csv')
            context: Optional Dagster context for logging

        Returns:
            File metadata dictionary or None if no files found
        """
        files = self.list_files_in_folder(
            folder_id=folder_id,
            file_extension=file_extension,
            context=context,
        )

        if not files:
            return None

        return files[0]
