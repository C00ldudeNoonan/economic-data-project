"""Telemetry data replication from SQLite to MotherDuck."""

from datetime import datetime, timezone
from decimal import Decimal

import dagster as dg
import polars as pl

from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.resources.sqlite_resource import SQLiteResource
from macro_agents.defs.telemetry.checks import telemetry_checks


@dg.asset(
    group_name="application_ingestion",
    kinds={"sqlite", "duckdb"},
    description="Replicate telemetry events from SQLite to MotherDuck with incremental updates",
)
def telemetry_events_raw(
    context: dg.AssetExecutionContext,
    sqlite: SQLiteResource,
    md: MotherDuckResource,
) -> dg.MaterializeResult:
    """
    Replicate telemetry_events table from SQLite to MotherDuck.

    Incremental strategy:
    - Query MotherDuck for max(id) to get last replicated ID
    - Fetch all events from SQLite with id > last_replicated_id
    - Insert new events into MotherDuck
    """

    def ensure_table_exists() -> None:
        """Create empty table if it doesn't exist."""
        # Ensure telemetry schema exists
        conn = md.get_connection()
        try:
            conn.execute("CREATE SCHEMA IF NOT EXISTS telemetry")
            conn.commit()
        finally:
            conn.close()

        empty_df = pl.DataFrame(
            schema={
                "id": pl.Int64,
                "event_type": pl.Utf8,
                "session_id": pl.Utf8,
                "user_id": pl.Int64,
                "timestamp_ms": pl.Int64,
                "properties": pl.Utf8,
                "created_at": pl.Utf8,  # SQLite TIMESTAMP as string
                "replicated_at": pl.Datetime,
            }
        )
        # Use upsert to create table structure without inserting data
        md.upsert_data(
            "telemetry.telemetry_events_raw", empty_df, ["id"], context=context
        )

    context.log.info("Starting telemetry replication from SQLite to MotherDuck")

    # Check if source table exists
    if not sqlite.table_exists("telemetry_events"):
        context.log.warning(
            "telemetry_events table doesn't exist in SQLite yet, creating empty table in MotherDuck"
        )
        ensure_table_exists()
        return dg.MaterializeResult(
            metadata={
                "num_events_replicated": 0,
                "source": "sqlite",
                "destination": "motherduck",
                "status": "source_table_not_found",
            }
        )

    # Get the last replicated ID from MotherDuck
    last_id: int | None = None
    try:
        # Check in telemetry schema
        result = md.execute_query(
            "SELECT MAX(id) as max_id FROM telemetry.telemetry_events_raw"
        )
        if len(result) > 0 and result["max_id"][0] is not None:
            last_id = int(result["max_id"][0])
            context.log.info(f"Last replicated ID from MotherDuck: {last_id}")
        else:
            context.log.info("No existing data in telemetry.telemetry_events_raw")
    except Exception as e:
        context.log.warning(
            f"Error checking last replicated ID: {e}, starting from beginning"
        )
        last_id = None

    # Build WHERE clause for incremental replication
    where = None
    if last_id is not None:
        where = ("id > ?", (last_id,))
        context.log.info(f"Fetching events with id > {last_id}")
    else:
        context.log.info("Fetching all events (first run)")

    # Read new events from SQLite
    try:
        df = sqlite.read_table_as_polars(
            table_name="telemetry_events",
            where=where,
            order_by=("id", "ASC"),
        )
    except Exception as e:
        context.log.error(f"Failed to read from SQLite: {e}")
        ensure_table_exists()
        return dg.MaterializeResult(
            metadata={
                "num_events_replicated": 0,
                "source": "sqlite",
                "destination": "motherduck",
                "status": "read_error",
                "error": str(e),
            }
        )

    # Check if any new events were found
    if df.is_empty():
        context.log.info("No new events to replicate")
        ensure_table_exists()
        return dg.MaterializeResult(
            metadata={
                "num_events_replicated": 0,
                "source": "sqlite",
                "destination": "motherduck",
                "status": "no_new_events",
                "last_replicated_id": last_id,
            }
        )

    # Add replication timestamp
    df = df.with_columns(pl.lit(datetime.now(timezone.utc)).alias("replicated_at"))

    context.log.info(f"Replicating {len(df)} new telemetry events to MotherDuck")

    # Insert into MotherDuck telemetry schema
    try:
        md.upsert_data("telemetry.telemetry_events_raw", df, ["id"], context=context)
    except Exception as e:
        context.log.error(f"Failed to insert into MotherDuck: {e}")
        return dg.MaterializeResult(
            metadata={
                "num_events_replicated": 0,
                "source": "sqlite",
                "destination": "motherduck",
                "status": "insert_error",
                "error": str(e),
            }
        )

    # Calculate statistics
    event_type_counts = df.group_by("event_type").agg(pl.count().alias("count"))
    event_types = {
        row["event_type"]: row["count"] for row in event_type_counts.to_dicts()
    }

    def _coerce_int(value: object, default: int = 0) -> int:
        if value is None:
            return default
        if isinstance(value, (int, float, Decimal)):
            return int(value)
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                return default
        return default

    min_timestamp = _coerce_int(df["timestamp_ms"].min())
    max_timestamp = _coerce_int(df["timestamp_ms"].max())
    min_id = _coerce_int(df["id"].min())
    max_id = _coerce_int(df["id"].max())

    unique_sessions = df["session_id"].n_unique()
    unique_users = df["user_id"].drop_nulls().n_unique()

    context.log.info(
        f"Successfully replicated {len(df)} events (IDs {min_id}-{max_id}), "
        f"{unique_sessions} sessions, {unique_users} users"
    )

    return dg.MaterializeResult(
        metadata={
            "num_events_replicated": len(df),
            "min_id": min_id,
            "max_id": max_id,
            "min_timestamp_ms": min_timestamp,
            "max_timestamp_ms": max_timestamp,
            "unique_sessions": unique_sessions,
            "unique_users": unique_users,
            "event_type_distribution": event_types,
            "source": "sqlite",
            "destination": "motherduck",
            "status": "success",
        }
    )


@dg.asset(
    group_name="application_ingestion",
    kinds={"sqlite", "duckdb"},
    description="Replicate users table from SQLite to MotherDuck (full refresh daily)",
)
def users_raw(
    context: dg.AssetExecutionContext,
    sqlite: SQLiteResource,
    md: MotherDuckResource,
) -> dg.MaterializeResult:
    """
    Replicate users table from SQLite to MotherDuck.

    Strategy: Full refresh daily (small table, privacy-sensitive)
    """

    def ensure_table_exists() -> None:
        """Create empty table if it doesn't exist."""
        # Ensure telemetry schema exists
        conn = md.get_connection()
        try:
            conn.execute("CREATE SCHEMA IF NOT EXISTS telemetry")
            conn.commit()
        finally:
            conn.close()

        empty_df = pl.DataFrame(
            schema={
                "id": pl.Int64,
                "email": pl.Utf8,
                "password_hash": pl.Utf8,
                "created_at": pl.Utf8,
                "updated_at": pl.Utf8,
                "replicated_at": pl.Datetime,
            }
        )
        md.drop_create_duck_db_table("telemetry.users_raw", empty_df)

    context.log.info("Starting users table replication from SQLite to MotherDuck")

    # Check if source table exists
    if not sqlite.table_exists("users"):
        context.log.warning(
            "users table doesn't exist in SQLite yet, creating empty table in MotherDuck"
        )
        ensure_table_exists()
        return dg.MaterializeResult(
            metadata={
                "num_users_replicated": 0,
                "source": "sqlite",
                "destination": "motherduck",
                "status": "source_table_not_found",
            }
        )

    # Read all users from SQLite
    try:
        df = sqlite.read_table_as_polars(table_name="users", order_by=("id", "ASC"))
    except Exception as e:
        context.log.error(f"Failed to read users from SQLite: {e}")
        ensure_table_exists()
        return dg.MaterializeResult(
            metadata={
                "num_users_replicated": 0,
                "source": "sqlite",
                "destination": "motherduck",
                "status": "read_error",
                "error": str(e),
            }
        )

    # Check if any users exist
    if df.is_empty():
        context.log.info("No users found in SQLite")
        ensure_table_exists()
        return dg.MaterializeResult(
            metadata={
                "num_users_replicated": 0,
                "source": "sqlite",
                "destination": "motherduck",
                "status": "no_users",
            }
        )

    # Add replication timestamp
    df = df.with_columns(pl.lit(datetime.now(timezone.utc)).alias("replicated_at"))

    context.log.info(f"Replicating {len(df)} users to MotherDuck")

    # Full refresh (drop and recreate) in telemetry schema
    try:
        md.drop_create_duck_db_table("telemetry.users_raw", df)
    except Exception as e:
        context.log.error(f"Failed to insert users into MotherDuck: {e}")
        return dg.MaterializeResult(
            metadata={
                "num_users_replicated": 0,
                "source": "sqlite",
                "destination": "motherduck",
                "status": "insert_error",
                "error": str(e),
            }
        )

    context.log.info(f"Successfully replicated {len(df)} users")

    return dg.MaterializeResult(
        metadata={
            "num_users_replicated": len(df),
            "source": "sqlite",
            "destination": "motherduck",
            "status": "success",
        }
    )


telemetry_events_replication_job = dg.define_asset_job(
    name="telemetry_events_replication_job",
    tags={"dagster/priority": "1", "dagster/max_runtime": 600},
    selection=dg.AssetSelection.assets(telemetry_events_raw),
    description="Dedicated job for incremental telemetry event replication from SQLite to MotherDuck",
)

users_replication_job = dg.define_asset_job(
    name="users_replication_job",
    tags={"dagster/priority": "1", "dagster/max_runtime": 600},
    selection=dg.AssetSelection.assets(users_raw),
    description="Dedicated job for full-refresh user replication from SQLite to MotherDuck",
)

daily_telemetry_events_schedule = dg.ScheduleDefinition(
    name="daily_telemetry_events_schedule",
    cron_schedule="0 9 * * *",
    execution_timezone="America/New_York",
    job=telemetry_events_replication_job,
    description="Daily telemetry events replication from SQLite to MotherDuck at 9 AM EST",
)

daily_users_replication_schedule = dg.ScheduleDefinition(
    name="daily_users_replication_schedule",
    cron_schedule="30 9 * * *",
    execution_timezone="America/New_York",
    job=users_replication_job,
    description="Daily users replication from SQLite to MotherDuck at 9:30 AM EST",
)


defs = dg.Definitions(
    assets=[telemetry_events_raw, users_raw],
    asset_checks=telemetry_checks,
    jobs=[telemetry_events_replication_job, users_replication_job],
    schedules=[daily_telemetry_events_schedule, daily_users_replication_schedule],
)
