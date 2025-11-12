from pathlib import Path
from typing import Mapping, Any
import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource, sling_assets
from dagster_sling.asset_decorator import DagsterSlingTranslator

_replication_yaml_path = Path(__file__).parent / "replication.yaml"

# Use the same monthly partitions as market_stack ingestion assets
# This aligns replication with the ingestion partition logic
monthly_partitions = dg.MonthlyPartitionsDefinition(
    start_date="2012-01-01", end_offset=1
)

REPLICATION_DEPS = {
    ("target", "main", "stg_fred_series"): ["stg_fred_series"],
    ("target", "main", "stg_housing_inventory"): ["stg_housing_inventory"],
    ("target", "main", "stg_housing_pulse"): ["stg_housing_pulse"],
    ("target", "main", "stg_treasury_yields"): ["stg_treasury_yields"],
    ("target", "main", "stg_us_sectors"): ["stg_us_sectors"],
    ("target", "main", "stg_currency"): ["stg_currency"],
    ("target", "main", "stg_major_indices"): ["stg_major_indices"],
    ("target", "main", "stg_fixed_income"): ["stg_fixed_income"],
    ("target", "main", "stg_global_markets"): ["stg_global_markets"],
    ("target", "main", "stg_energy_commodities"): ["stg_energy_commodities"],
    ("target", "main", "stg_input_commodities"): ["stg_input_commodities"],
    ("target", "main", "stg_agriculture_commodities"): ["stg_agriculture_commodities"],
    ("target", "main", "stg_realtor_country_history"): ["stg_realtor_country_history"],
    ("target", "main", "stg_realtor_county_history"): ["stg_realtor_county_history"],
    ("target", "main", "stg_realtor_metro_history"): ["stg_realtor_metro_history"],
    ("target", "main", "stg_realtor_state_history"): ["stg_realtor_state_history"],
    ("target", "main", "stg_realtor_zip_history"): ["stg_realtor_zip_history"],
    ("target", "main", "fred_series_grain"): ["fred_series_grain"],
    ("target", "main", "fred_series_latest_aggregates"): [
        "fred_series_latest_aggregates"
    ],
    ("target", "main", "fred_monthly_diff"): ["fred_monthly_diff"],
    ("target", "main", "fred_quarterly_roc"): ["fred_quarterly_roc"],
    ("target", "main", "housing_inventory"): ["housing_inventory"],
    ("target", "main", "housing_inventory_latest_aggregates"): [
        "housing_inventory_latest_aggregates"
    ],
    ("target", "main", "housing_inventory_and_population"): [
        "housing_inventory_and_population"
    ],
    ("target", "main", "housing_mortgage_rates"): ["housing_mortgage_rates"],
    ("target", "main", "currency_summary"): ["currency_summary"],
    ("target", "main", "currency_analysis_return"): ["currency_analysis_return"],
    ("target", "main", "us_sector_summary"): ["us_sector_summary"],
    ("target", "main", "us_sector_analysis_return"): ["us_sector_analysis_return"],
    ("target", "main", "major_indicies_summary"): ["major_indicies_summary"],
    ("target", "main", "major_indicies_analysis_return"): [
        "major_indicies_analysis_return"
    ],
    ("target", "main", "fixed_income_analysis_return"): [
        "fixed_income_analysis_return"
    ],
    ("target", "main", "global_markets_summary"): ["global_markets_summary"],
    ("target", "main", "global_markets_analysis_return"): [
        "global_markets_analysis_return"
    ],
    ("target", "main", "energy_commodities_summary"): ["energy_commodities_summary"],
    ("target", "main", "energy_commodities_analysis_return"): [
        "energy_commodities_analysis_return"
    ],
    ("target", "main", "input_commodities_summary"): ["input_commodities_summary"],
    ("target", "main", "input_commodities_analysis_return"): [
        "input_commodities_analysis_return"
    ],
    ("target", "main", "agriculture_commodities_summary"): [
        "agriculture_commodities_summary"
    ],
    ("target", "main", "agriculture_commodities_analysis_return"): [
        "agriculture_commodities_analysis_return"
    ],
    ("target", "main", "base_historical_analysis"): ["base_historical_analysis"],
    ("target", "main", "market_economic_analysis"): ["market_economic_analysis"],
    ("target", "main", "leading_econ_return_indicator"): [
        "leading_econ_return_indicator"
    ],
    ("target", "main", "financial_conditions_index"): ["financial_conditions_index"],
}


class CustomDagsterSlingTranslator(DagsterSlingTranslator):
    """Custom translator to add upstream dependencies and group_name to Sling replication assets."""

    def get_asset_spec(self, stream_definition: Mapping[str, Any]) -> dg.AssetSpec:
        default_spec = super().get_asset_spec(stream_definition)
        asset_key_tuple = tuple(default_spec.key.path)

        # Add group_name to all replication assets
        updated_spec = default_spec.replace_attributes(group_name="replication")

        # Add upstream dependencies if defined
        if asset_key_tuple in REPLICATION_DEPS:
            source_keys = [
                dg.AssetKey(dep.split(".")) if "." in dep else dg.AssetKey([dep])
                for dep in REPLICATION_DEPS[asset_key_tuple]
            ]
            updated_spec = updated_spec.replace_attributes(deps=source_keys)

        return updated_spec


motherduck_connection = SlingConnectionResource(
    name="MOTHERDUCK",
    type="motherduck",
    database=dg.EnvVar("MOTHERDUCK_DATABASE"),
    motherduck_token=dg.EnvVar("MOTHERDUCK_TOKEN"),
    schema=dg.EnvVar("MOTHERDUCK_PROD_SCHEMA"),
)

bigquery_connection = SlingConnectionResource(
    name="BIGQUERY",
    type="bigquery",
    project=dg.EnvVar("BIGQUERY_PROJECT_ID"),
    location=dg.EnvVar("BIGQUERY_LOCATION"),
    credentials=dg.EnvVar("GOOGLE_APPLICATION_CREDENTIALS"),
    dataset=dg.EnvVar("BIGQUERY_DATASET"),
)


# Create SlingResource with both connections
sling_resource = SlingResource(
    connections=[
        motherduck_connection,
        bigquery_connection,
    ]
)


@sling_assets(
    replication_config=str(_replication_yaml_path),
    dagster_sling_translator=CustomDagsterSlingTranslator(),
    partitions_def=monthly_partitions,
)
def replication_assets(
    context,
    sling: SlingResource,
):
    """Sling assets for replicating data from MotherDuck to BigQuery.

    Partitioned by month to align with market_stack ingestion assets.
    Each partition represents a month, allowing for incremental replication
    and tracking of which months have been replicated.
    """
    partition_key = context.partition_key
    context.log.info(f"Processing replication for partition: {partition_key}")

    yield from sling.replicate(context=context)
    for row in sling.stream_raw_logs():
        context.log.info(row)
