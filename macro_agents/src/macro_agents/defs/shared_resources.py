import dagster as dg
from metaxy.ext.dagster import MetaxyStoreFromConfigResource

from macro_agents.defs.resources.federal_reserve import FederalReserveResource
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.resources.google_sheets import GoogleSheetsResource
from macro_agents.defs.resources.bigquery_warehouse import bigquery_warehouse_resource
from macro_agents.defs.resources.nl_query_resource import nl_query_resource
from macro_agents.defs.resources.pdf import PDFResource
from macro_agents.defs.resources.sqlite_resource import sqlite_resource


defs = dg.Definitions(
    resources={
        "bq": bigquery_warehouse_resource,
        "metaxy_store": MetaxyStoreFromConfigResource(name="prod"),
        "sqlite": sqlite_resource,
        # bucket_name intentionally unset: resolved at runtime via
        # default_gcs_bucket() (GCS_BUCKET_NAME when set, else the
        # ENVIRONMENT-suffixed document bucket) so dev/staging runs never
        # write to the prod bucket by default.
        "gcs": GCSResource(
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
