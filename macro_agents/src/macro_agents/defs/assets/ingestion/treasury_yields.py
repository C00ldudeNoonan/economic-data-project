import dagster as dg
import polars as pl
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from macro_agents.defs.resources.motherduck import MotherDuckResource

# Create year partitions from 1990 to current year
current_year = datetime.now().year
year_partition = dg.StaticPartitionsDefinition(
    [str(year) for year in range(1990, current_year + 1)]
)


@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb", "web_scraping"},
    partitions_def=year_partition,
    description="Raw Treasury yield curve data scraped from Treasury.gov XML feed",
    automation_condition=dg.AutomationCondition.on_cron(
        "0 1 * * 1"
    ),  # Weekly on Monday at 1 AM
)
def treasury_yields_raw(
    context: dg.AssetExecutionContext, md: MotherDuckResource
) -> dg.MaterializeResult:
    """Scrape Treasury yield curve data for a specific year."""
    year = context.partition_key

    # Construct the URL with the year parameter
    url = f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xmlview?data=daily_treasury_yield_curve&field_tdr_date_value={year}"

    context.log.info(f"Fetching Treasury yield data for year {year} from: {url}")

    try:
        # Fetch the XML data
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Parse the XML with BeautifulSoup
        soup = BeautifulSoup(response.content, "xml")

        # Find all entry elements (each represents a daily record)
        entries = soup.find_all("entry")
        context.log.info(f"Found {len(entries)} entries for year {year}")

        if not entries:
            context.log.warning(f"No data found for year {year}")
            return dg.MaterializeResult(
                metadata={"year": year, "num_records": 0, "status": "no_data_found"}
            )

        # Extract data from each entry
        records = []
        for entry in entries:
            # Find the properties section
            properties = entry.find("m:properties")
            if not properties:
                continue

            # Get the date with namespace prefix
            date_elem = properties.find("d:NEW_DATE")
            if not date_elem:
                continue

            # Parse the ISO datetime and extract just the date part
            date_str = date_elem.get_text().strip()
            # Convert "2024-01-02T00:00:00" to just "2024-01-02"
            date_only = date_str.split("T")[0]

            # Define all possible yield elements with the d: namespace
            yield_elements = [
                "d:BC_1MONTH",
                "d:BC_2MONTH",
                "d:BC_3MONTH",
                "d:BC_4MONTH",
                "d:BC_6MONTH",
                "d:BC_1YEAR",
                "d:BC_2YEAR",
                "d:BC_3YEAR",
                "d:BC_5YEAR",
                "d:BC_7YEAR",
                "d:BC_10YEAR",
                "d:BC_20YEAR",
                "d:BC_30YEAR",
            ]

            for yield_type in yield_elements:
                yield_elem = properties.find(yield_type)
                if yield_elem and yield_elem.get_text().strip():
                    try:
                        yield_value = float(yield_elem.get_text().strip())
                        # Remove the 'd:' prefix for cleaner field names
                        clean_yield_type = yield_type.replace("d:", "")
                        records.append(
                            {
                                "date": date_only,
                                "yield_type": clean_yield_type,
                                "value": yield_value,
                                "year": int(year),
                            }
                        )
                    except ValueError as e:
                        context.log.warning(
                            f"Could not parse yield value '{yield_elem.get_text().strip()}': {e}"
                        )
                        continue

        if not records:
            context.log.warning(f"No valid yield data extracted for year {year}")
            return dg.MaterializeResult(
                metadata={"year": year, "num_records": 0, "status": "no_valid_data"}
            )

        # Create DataFrame
        df = pl.DataFrame(records)
        context.log.info(f"Created DataFrame with columns: {df.columns}")
        context.log.info(
            f"Sample dates before conversion: {df['date'].head().to_list()}"
        )

        # Convert date string to proper date format (YYYY-MM-DD format)
        df = df.with_columns(pl.col("date").str.strptime(pl.Date, format="%Y-%m-%d"))

        context.log.info(
            f"Sample dates after conversion: {df['date'].head().to_list()}"
        )
        context.log.info(f"Date column dtype: {df['date'].dtype}")

        # Sort by date and yield_type for consistency
        df = df.sort(["date", "yield_type"])

        context.log.info(f"Processed {len(df)} records for year {year}")
        context.log.info(f"Date range: {df['date'].min()} to {df['date'].max()}")

        # Debug: Show a few sample rows
        context.log.info(
            f"Sample data: {df.head(3).select(['date', 'yield_type', 'value']).to_dicts()}"
        )

        # Upsert data to MotherDuck using the existing upsert logic
        md.upsert_data("treasury_yields_raw", df, ["date", "yield_type"])

        return dg.MaterializeResult(
            metadata={
                "year": year,
                "num_records": len(df),
                "date_range": f"{df['date'].min()} to {df['date'].max()}",
                "yield_types": sorted(df["yield_type"].unique().to_list()),
                "unique_dates": len(df["date"].unique()),
                "entries_found": len(entries),
                "avg_records_per_date": round(len(df) / len(df["date"].unique()), 2)
                if len(df["date"].unique()) > 0
                else 0,
            }
        )

    except requests.RequestException as e:
        context.log.error(f"Failed to fetch data for year {year}: {str(e)}")
        raise
    except Exception as e:
        context.log.error(
            f"Failed to process Treasury yield data for year {year}: {str(e)}"
        )
        raise
