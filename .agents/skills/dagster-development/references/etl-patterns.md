# ETL Patterns Reference

## Pattern Summary

| Pattern | When to Use |
| ------- | ----------- |
| Cloud storage resources | Loading CSVs, Parquet, JSON from S3, GCS, Azure Blob (production) |
| Database resources | Direct database connections for data ingestion (preferred) |
| API resource + assets | Fetching data from REST APIs |
| dlt integration | Embedded ETL with schema inference and pagination |
| Sling integration | Database-to-database replication |
| Configurable ingestion | Dynamic file/endpoint selection at runtime |
| Local file storage | **Only for local development/testing** - use cloud storage or databases in production |

---

## File Import Pattern

**⚠️ Important**: Avoid saving files to local Dagster storage in production. Use cloud storage (S3, GCS, Azure Blob) or load directly into databases. Local file storage should only be used for local development/testing purposes.

### Cloud Storage Pattern (Production)

```python
import dagster as dg
from dagster_aws import S3Resource
import polars as pl
import io

class S3FileConfig(dg.Config):
    bucket: str
    key: str  # S3 object key (file path)

@dg.asset
def raw_data_table(
    context: dg.AssetExecutionContext,
    s3: S3Resource,
    database: DuckDBResource,
    config: S3FileConfig,
) -> None:
    """Load CSV file from S3 directly into database without local storage."""
    table_name = "raw_data"

    # Download file content directly into memory
    s3_obj = s3.get_object(Bucket=config.bucket, Key=config.key)
    file_content = s3_obj['Body'].read()

    # Load directly into Polars DataFrame
    df = pl.read_csv(io.BytesIO(file_content))

    # Write directly to database
    with database.get_connection() as conn:
        conn.register("temp_df", df)
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")
```

### Google Cloud Storage Pattern

```python
from dagster_gcp import GCSResource
import polars as pl
import io

@dg.asset
def load_from_gcs(
    context: dg.AssetExecutionContext,
    gcs: GCSResource,
    database: DuckDBResource,
) -> None:
    """Load file from GCS directly into database."""
    bucket = gcs.get_bucket("my-bucket")
    blob = bucket.blob("data/file.csv")

    # Read directly into memory
    file_content = blob.download_as_bytes()
    df = pl.read_csv(io.BytesIO(file_content))

    # Write to database
    with database.get_connection() as conn:
        conn.register("temp_df", df)
        conn.execute("CREATE OR REPLACE TABLE raw_data AS SELECT * FROM temp_df")
```

### Local File Pattern (Development Only)

```python
import dagster as dg
from pathlib import Path
import os

class IngestionFileConfig(dg.Config):
    path: str

@dg.asset
def import_file(config: IngestionFileConfig) -> str:
    """
    Resolve file path from config.

    ⚠️ Only use for local development. In production, use cloud storage resources.
    """
    # Check if we're in local dev environment
    if os.getenv("ENVIRONMENT") == "dev":
        file_path = Path(__file__).parent / f"../../../data/source/{config.path}"
        return str(file_path.resolve())
    else:
        raise ValueError(
            "Local file paths should not be used in production. "
            "Use S3Resource, GCSResource, or database connections instead."
        )
```

### Load File into Database (Direct)

```python
from dagster_duckdb import DuckDBResource
import polars as pl

@dg.asset(kinds={"duckdb"})
def raw_data_table(
    context: dg.AssetExecutionContext,
    database: DuckDBResource,
    file_content: bytes,  # From cloud storage or API
) -> None:
    """Load CSV file content directly into DuckDB table."""
    table_name = "raw_data"

    # Load directly from bytes into DataFrame
    df = pl.read_csv(io.BytesIO(file_content))

    with database.get_connection() as conn:
        # Register DataFrame and create table directly
        conn.register("temp_df", df)
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")
```

### Running with Config

**CLI:**
```bash
dg launch --assets import_file,raw_data_table \
  --config-json '{"ops": {"import_file": {"config": {"path": "2024-01-01.csv"}}}}'
```

**UI YAML:**
```yaml
ops:
  import_file:
    config:
      path: 2024-01-01.csv
```

---

## API Integration Pattern

### API Resource

```python
import dagster as dg
import requests

class NASAResource(dg.ConfigurableResource):
    api_key: str
    base_url: str = "https://api.nasa.gov"

    def get_near_earth_asteroids(self, start_date: str, end_date: str) -> list:
        url = f"{self.base_url}/neo/rest/v1/feed"
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "api_key": self.api_key,
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()["near_earth_objects"][start_date]
```

### API Asset

```python
@dg.asset
def asteroid_data(
    context: dg.AssetExecutionContext,
    nasa: NASAResource,
) -> list[dict]:
    """Fetch asteroid data from NASA API."""
    data = nasa.get_near_earth_asteroids("2024-01-01", "2024-01-07")
    context.log.info(f"Retrieved {len(data)} asteroids")
    return data
```

### Register API Resource

```python
@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "nasa": NASAResource(
                api_key=dg.EnvVar("NASA_API_KEY"),
            ),
        },
    )
```

---

## dlt Integration

dlt (data load tool) handles schema inference, pagination, and loading automatically.

### Basic dlt Pipeline

```python
import dlt

@dlt.source
def simple_source():
    @dlt.resource
    def load_dict():
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        yield data

    return load_dict

# Standalone execution
pipeline = dlt.pipeline(
    pipeline_name="simple_pipeline",
    destination="duckdb",
    dataset_name="mydata",
)
load_info = pipeline.run(simple_source())
```

### dlt with Dagster

```python
import dagster as dg
import dlt
from dagster_dlt import DagsterDltResource, dlt_assets

@dlt.source
def simple_source():
    @dlt.resource
    def load_dict():
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        yield data

    return load_dict

@dlt_assets(
    dlt_source=simple_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="simple_pipeline",
        dataset_name="simple",
        destination="duckdb",
        progress="log",
    ),
)
def dlt_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)
```

### dlt Resource Registration

```python
from dagster_dlt import DagsterDltResource

@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "dlt": DagsterDltResource(),
        },
    )
```

### dlt Benefits

- **Schema inference**: Automatically detects and creates table schemas
- **Type inference**: Maps Python types to database types
- **Pagination handling**: Built-in support for paginated APIs
- **Incremental loading**: Track state for incremental updates
- **Multiple destinations**: DuckDB, Snowflake, BigQuery, Postgres, etc.

---

## Sling Integration

Sling is declarative database-to-database replication.

### Sling Connection Resources

```python
from dagster_sling import SlingConnectionResource, SlingResource

# Source database
source = SlingConnectionResource(
    name="MY_POSTGRES",
    type="postgres",
    host="localhost",
    port=5432,
    database="source_db",
    user="user",
    password="password",
)

# Destination database
destination = SlingConnectionResource(
    name="MY_DUCKDB",
    type="duckdb",
    connection_string="duckdb:///data/staging/data.duckdb",
)

# Combine into SlingResource
sling = SlingResource(
    connections=[source, destination],
)
```

### Sling Replication Config

```yaml
# sling_replication.yaml
source: MY_POSTGRES
target: MY_DUCKDB

defaults:
  mode: full-refresh
  object: "{stream_schema}_{stream_table}"

streams:
  data.customers:
  data.products:
  data.orders:
```

### Sling Replication Modes

| Mode | Description |
| ---- | ----------- |
| `full-refresh` | Drop and recreate table each run |
| `incremental` | Append new records |
| `truncate` | Truncate table before loading |
| `snapshot` | Full refresh with point-in-time snapshot |

### Sling Assets

```python
from dagster_sling import SlingResource, sling_assets

replication_config = dg.file_relative_path(__file__, "sling_replication.yaml")

@sling_assets(replication_config=replication_config)
def postgres_sling_assets(context: dg.AssetExecutionContext, sling: SlingResource):
    yield from sling.replicate(context=context).fetch_column_metadata()
```

### Register Sling Resource

```python
@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "sling": sling,
        },
    )
```

---

## Choosing ETL Approach

| Scenario | Recommended Approach |
| -------- | -------------------- |
| Load files from cloud storage (S3, GCS) | Cloud storage resource (S3Resource, GCSResource) + direct database load |
| Load files from local storage | **Only for local dev** - use cloud storage in production |
| Custom REST API | API resource + assets (load directly into database) |
| Standard API (Stripe, GitHub) | dlt verified sources |
| Schema-on-read ingestion | dlt |
| Database replication | Sling |
| Complex transformations | Custom assets or dbt |
| File downloads from APIs | Load directly into memory (BytesIO) then to database, avoid local saves |

---

## Partitioned ETL

### Partitioned File Import

```python
daily_partition = dg.DailyPartitionsDefinition(start_date="2024-01-01")

@dg.asset(partitions_def=daily_partition)
def daily_import(context: dg.AssetExecutionContext) -> str:
    partition_date = context.partition_key  # "2024-01-01"
    file_path = f"data/source/{partition_date}.csv"
    return file_path

@dg.asset(partitions_def=daily_partition)
def daily_load(
    context: dg.AssetExecutionContext,
    database: DuckDBResource,
    daily_import: str,
) -> None:
    partition = context.partition_key

    with database.get_connection() as conn:
        # Delete existing partition data
        conn.execute(f"DELETE FROM raw_data WHERE date = '{partition}'")
        # Load new data
        conn.execute(f"COPY raw_data FROM '{daily_import}'")
```

### Partitioned API Fetch

```python
@dg.asset(partitions_def=daily_partition)
def daily_api_data(
    context: dg.AssetExecutionContext,
    api: MyAPIResource,
) -> list[dict]:
    partition_date = context.partition_key
    return api.fetch_data_for_date(partition_date)
```

---

## Data Validation

### Validate After Load

```python
@dg.asset(deps=["raw_data_table"])
def validated_data(database: DuckDBResource) -> dg.MaterializeResult:
    with database.get_connection() as conn:
        # Check row count
        result = conn.execute("SELECT COUNT(*) FROM raw_data").fetchone()
        row_count = result[0]

        # Check for nulls
        null_check = conn.execute(
            "SELECT COUNT(*) FROM raw_data WHERE value IS NULL"
        ).fetchone()
        null_count = null_check[0]

    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "null_values": null_count,
            "null_percentage": round(null_count / row_count * 100, 2) if row_count > 0 else 0,
        }
    )
```

### Asset Check for Data Quality

```python
@dg.asset_check(asset=raw_data_table)
def no_duplicate_records(database: DuckDBResource) -> dg.AssetCheckResult:
    with database.get_connection() as conn:
        result = conn.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT id) as duplicates
            FROM raw_data
        """).fetchone()
        duplicates = result[0]

    return dg.AssetCheckResult(
        passed=duplicates == 0,
        metadata={"duplicate_count": duplicates},
    )
```

---

## Error Handling

### Retry Pattern

```python
import time

class RobustAPIResource(dg.ConfigurableResource):
    api_key: str
    max_retries: int = 3

    def fetch_with_retry(self, endpoint: str) -> dict:
        for attempt in range(self.max_retries):
            try:
                response = requests.get(endpoint, headers={"Authorization": self.api_key})
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
```

### Graceful Failure

```python
@dg.asset
def api_data_with_fallback(
    context: dg.AssetExecutionContext,
    api: MyAPIResource,
) -> dg.MaterializeResult:
    try:
        data = api.fetch_data()
        return dg.MaterializeResult(
            metadata={"status": "success", "row_count": len(data)}
        )
    except Exception as e:
        context.log.error(f"API fetch failed: {e}")
        raise  # Let Dagster handle the failure
```

---

## Anti-Patterns to Avoid

| Anti-Pattern | Better Approach |
| ------------ | --------------- |
| Saving files to local storage in production | Use cloud storage (S3, GCS) or load directly into databases |
| Using `tempfile` or local file writes in assets | Load file content into memory (BytesIO) then directly to database |
| Hardcoded file paths | Use `Config` for dynamic paths, prefer cloud storage resources |
| No schema validation | Add asset checks for data quality |
| Ignoring pagination | Use dlt or implement proper pagination |
| Full refresh always | Consider incremental loading |
| No retry logic | Add retries with exponential backoff |
| Downloading files locally before processing | Stream directly from cloud storage or API into memory |

---

## References

- [dlt Documentation](https://dlthub.com/docs)
- [Sling Documentation](https://docs.slingdata.io/)
- [Dagster dlt Integration](https://docs.dagster.io/integrations/libraries/dlt)
- [Dagster Sling Integration](https://docs.dagster.io/integrations/libraries/sling)
- [ETL Pipeline Tutorial](https://docs.dagster.io/etl-pipeline-tutorial)
