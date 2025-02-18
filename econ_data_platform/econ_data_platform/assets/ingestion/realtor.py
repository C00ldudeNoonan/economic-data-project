import dagster as dg
from econ_data_platform.resources.motherduck import MotherDuckResource

import polars as pl
from io import StringIO
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dataclasses import dataclass
from collections.abc import Sequence

from datetime import datetime
import os


@dataclass
class DriveFile:
    id: str
    name: str
    createdTime: str
    modifiedTime: str


def realtor_asset_factory(file_definition: DriveFile) -> dg.Definitions:
    file_name, _ = os.path.splitext(file_definition.name)
    file_id = file_definition.id

    @dg.asset(
        name=file_name,
        group_name="ingestion",
        kinds={"polars", "duckdb", "google_drive"},
    )
    def read_csv_from_drive(
        context: dg.AssetExecutionContext, md: MotherDuckResource
    ) -> dg.MaterializeResult:
        """Read CSV directly from Google Drive file ID into a polars DataFrame"""
        context.log.info(f"Reading file {file_name} from Google Drive")
        request = service.files().get_media(fileId=file_id)
        content = request.execute()
        csv_string = content.decode("utf-8")
        df = pl.read_csv(StringIO(csv_string))
        md.drop_create_duck_db_table(file_name, df)

        return dg.MaterializeResult(
            metadata={
                "file_id": file_id,
                "file_name": file_name,
                "num_records": len(df),
            }
        )

    file_job = dg.define_asset_job(
        name=f"{file_name}_job", selection=[read_csv_from_drive]
    )

    @dg.sensor(
        name=f"{file_name}_sensor",
        job_name=f"{file_name}_job",
        minimum_interval_seconds=15,
    )
    def file_sensor(context):
        # Get current modification time from cursor
        last_mtime = float(context.cursor) if context.cursor else 0

        # Get file details from Drive
        file_metadata = (
            service.files().get(fileId=file_id, fields="modifiedTime").execute()
        )
        context.log.info(f"File metadata: {file_metadata}")

        current_mtime = datetime.strptime(
            file_metadata["modifiedTime"], "%Y-%m-%dT%H:%M:%S.%fZ"
        ).timestamp()
        context.log.info(f"Current mtime: {current_mtime}")

        # If file has been modified, trigger the job
        if current_mtime > last_mtime:
            context.update_cursor(str(current_mtime))
            # Create AssetKey for the file
            asset_key = dg.AssetKey(file_name)
            yield dg.RunRequest(run_key=str(current_mtime), asset_selection=[asset_key])

    return dg.Definitions(
        assets=[read_csv_from_drive],
        jobs=[file_job],
        sensors=[file_sensor],
    )


SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
credentials = service_account.Credentials.from_service_account_file(
    "creds.json", scopes=SCOPES
)
service = build("drive", "v3", credentials=credentials)
folder_id = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")

# Get files from folder
query = f"'{folder_id}' in parents and mimeType='text/csv'"
results = (
    service.files()
    .list(q=query, fields="files(id, name, createdTime, modifiedTime)")
    .execute()
)

realtor_definitions = [
    realtor_asset_factory(DriveFile(**file)) for file in results.get("files", [])
]
