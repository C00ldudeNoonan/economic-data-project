import json
import os
from datetime import datetime
from io import StringIO

import dagster as dg
import polars as pl
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pydantic import BaseModel, PrivateAttr


class DriveFile(BaseModel):
    id: str
    name: str
    createdTime: str
    modifiedTime: str


class GoogleDriveClient:
    """Handles the Google Drive client creation and API interactions."""

    def __init__(self, credentials_json: dict):
        self.credentials = service_account.Credentials.from_service_account_info(
            credentials_json, scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
        self.service = build("drive", "v3", credentials=self.credentials)

    def retrieve_files(self, folder_id: str):
        """Query for files in a Google Drive folder."""
        query = f"'{folder_id}' in parents and mimeType='text/csv'"
        return (
            self.service.files()
            .list(q=query, fields="files(id, name, createdTime, modifiedTime)")
            .execute()
        )

    def request_content(self, file_id: str):
        """Fetch file content from Google Drive using file_id."""
        request = self.service.files().get_media(fileId=file_id)
        return request.execute()

    def get_file_metadata(self, file_id: str, fields: str = "modifiedTime"):
        """Get metadata for a specific file."""
        return self.service.files().get(fileId=file_id, fields=fields).execute()


class GoogleDriveResource(dg.ConfigurableResource):
    """Resource configuration for Google Drive credentials."""

    json_data: str

    _client: GoogleDriveClient = PrivateAttr()

    def setup_for_execution(self, context: dg.InitResourceContext):
        """Initialize the Google Drive client using the credentials."""
        credentials_json = json.loads(self.json_data)
        self._client = GoogleDriveClient(credentials_json)

    def retrieve_files(self, folder_id: str):
        """Delegates to the client to retrieve files."""
        return self._client.retrieve_files(folder_id)

    def request_content(self, file_id: str):
        """Delegates to the client to fetch content."""
        return self._client.request_content(file_id)

    def get_file_metadata(self, file_id: str, fields: str = "modifiedTime"):
        """Delegates to the client to get file metadata."""
        return self._client.get_file_metadata(file_id, fields)


google_drive_resource = GoogleDriveResource(
    json_data=os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
)

google_drive_resource.setup_for_execution(dg.build_init_resource_context())
