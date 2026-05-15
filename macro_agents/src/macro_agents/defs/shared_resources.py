import dagster as dg
from metaxy.ext.dagster import MetaxyStoreFromConfigResource

from macro_agents.defs.resources.federal_reserve import FederalReserveResource
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.resources.google_sheets import GoogleSheetsResource
from macro_agents.defs.resources.motherduck import motherduck_resource
from macro_agents.defs.resources.nl_query_resource import nl_query_resource
from macro_agents.defs.resources.pdf import PDFResource
from macro_agents.defs.resources.sqlite_resource import sqlite_resource


defs = dg.Definitions(
    resources={
        "md": motherduck_resource,
        "metaxy_store": MetaxyStoreFromConfigResource(name="prod"),
        "sqlite": sqlite_resource,
        "gcs": GCSResource(
            bucket_name=dg.EnvVar("GCS_BUCKET_NAME"),
            credentials_path=dg.EnvVar("GOOGLE_APPLICATION_CREDENTIALS"),
        ),
        "google_sheets": GoogleSheetsResource(
            credentials_json=dg.EnvVar("GOOGLE_APPLICATION_CREDENTIALS"),
            spreadsheet_id=dg.EnvVar("DQ_SPREADSHEET_ID"),
        ),
        "nl_query": nl_query_resource,
        "fed": FederalReserveResource(),
        "pdf": PDFResource(),
    }
)
