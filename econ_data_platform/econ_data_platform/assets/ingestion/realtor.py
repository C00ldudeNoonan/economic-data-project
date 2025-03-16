import dagster as dg
from econ_data_platform.resources.motherduck import MotherDuckResource
from econ_data_platform.resources.google_drive import (
    GoogleDriveResource,
    google_drive_resource,
)

import polars as pl
from io import StringIO

from dataclasses import dataclass

from datetime import datetime
import os


@dataclass
class DriveFile:
    id: str
    name: str
    createdTime: str
    modifiedTime: str


def realtor_asset_factory(
    file_definition: DriveFile, google_drive: GoogleDriveResource
) -> dg.Definitions:
    file_name, _ = os.path.splitext(file_definition.name)
    file_id = file_definition.id

    @dg.asset(
        name=file_name,
        group_name="ingestion",
        kinds={"polars", "duckdb", "google_drive"},
    )
    def read_csv_from_drive(
        context: dg.AssetExecutionContext,
        md: MotherDuckResource,
        google_drive: GoogleDriveResource,
    ) -> dg.MaterializeResult:
        """Read CSV directly from Google Drive file ID into a polars DataFrame"""
        context.log.info(f"Reading file {file_name} from Google Drive")
        request = google_drive.request_content(file_id)
        csv_string = request.decode("utf-8")
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
    def file_sensor(context, google_drive: GoogleDriveResource):
        # Get current modification time from cursor
        last_mtime = float(context.cursor) if context.cursor else 0

        # Get file details from Drive
        file_metadata = google_drive.get_file_metadata(file_id)
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
        resources={"google_drive": google_drive},
    )


# Fetch files from the Google Drive folder using properly initialized _client
folder_id = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")
file_results = google_drive_resource.retrieve_files(folder_id).get("files", [])

# Create realtor definitions dynamically
realtor_definitions = [
    realtor_asset_factory(DriveFile(**file), google_drive_resource)
    for file in file_results
]
