"""Calendar and economic event ingestion assets."""

import calendar
from datetime import date, datetime, timedelta, timezone

import dagster as dg
import httpx
import polars as pl

from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource
from macro_agents.defs.resources.yahoo_finance import (
    YahooFinanceResource,
    yahoo_finance_resource,
)
from macro_agents.defs.domains.calendar_events import (
    ECONOMIC_CALENDAR_FEEDS,
    EARNINGS_NUMERIC_COLUMNS,
    EARNINGS_NUMERIC_COLUMN_TYPES,
    _to_float,
    classify_event_type,
    parse_numeric_value,
)
from macro_agents.defs.domains.calendar_holidays import (
    CALENDAR_END_DATE,
    CALENDAR_START_DATE,
    MONTH_ABBRS,
    MONTH_NAMES,
    WEEKDAY_ABBRS,
    WEEKDAY_NAMES,
    _build_us_federal_holidays,
)

CALENDAR_GROUP = "calendar_ingestion"


@dg.asset(
    group_name=CALENDAR_GROUP,
    kinds={"reference", "calendar"},
    description="Calendar date dimension from 1995-01-01 through 2030-12-31",
)
def calendar_dates(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """Build a reusable calendar/date dimension table for analytics."""
    context.log.info(
        f"Building calendar table from {CALENDAR_START_DATE} to {CALENDAR_END_DATE}"
    )

    today = date.today()
    current_week_start = today - timedelta(days=today.weekday())
    current_month_index = today.year * 12 + today.month
    current_quarter = (today.month - 1) // 3 + 1
    current_quarter_index = today.year * 4 + current_quarter

    us_holidays = _build_us_federal_holidays(
        CALENDAR_START_DATE.year,
        CALENDAR_END_DATE.year,
    )

    rows: list[dict[str, object]] = []
    current = CALENDAR_START_DATE
    while current <= CALENDAR_END_DATE:
        year = current.year
        month = current.month
        day = current.day
        quarter = (month - 1) // 3 + 1

        weekday_index = current.weekday()
        iso_year, iso_week, iso_weekday = current.isocalendar()

        week_start_date = current - timedelta(days=weekday_index)
        week_end_date = week_start_date + timedelta(days=6)

        month_start_date = date(year, month, 1)
        month_end_date = date(year, month, calendar.monthrange(year, month)[1])

        quarter_start_month = (quarter - 1) * 3 + 1
        quarter_end_month = quarter_start_month + 2
        quarter_start_date = date(year, quarter_start_month, 1)
        quarter_end_date = date(
            year, quarter_end_month, calendar.monthrange(year, quarter_end_month)[1]
        )

        year_start_date = date(year, 1, 1)
        year_end_date = date(year, 12, 31)

        holiday = us_holidays.get(current)
        holiday_name = holiday[0] if holiday else None
        is_holiday_observed = bool(holiday[1]) if holiday else False

        month_index = year * 12 + month
        quarter_index = year * 4 + quarter

        rows.append(
            {
                "date": current,
                "date_key": year * 10000 + month * 100 + day,
                "year": year,
                "quarter": quarter,
                "month": month,
                "month_name": MONTH_NAMES[month - 1],
                "month_abbr": MONTH_ABBRS[month - 1],
                "week_of_year": iso_week,
                "iso_year": iso_year,
                "day_of_year": current.timetuple().tm_yday,
                "day_of_month": day,
                "day_of_week": iso_weekday,
                "day_of_week_name": WEEKDAY_NAMES[weekday_index],
                "day_of_week_abbr": WEEKDAY_ABBRS[weekday_index],
                "is_weekend": weekday_index >= 5,
                "week_start_date": week_start_date,
                "week_end_date": week_end_date,
                "month_start_date": month_start_date,
                "month_end_date": month_end_date,
                "quarter_start_date": quarter_start_date,
                "quarter_end_date": quarter_end_date,
                "year_start_date": year_start_date,
                "year_end_date": year_end_date,
                "is_current_day": current == today,
                "is_current_week": week_start_date == current_week_start,
                "is_current_month": month_index == current_month_index,
                "is_current_quarter": quarter_index == current_quarter_index,
                "is_holiday": holiday is not None,
                "holiday_name": holiday_name,
                "is_holiday_observed": is_holiday_observed,
            }
        )
        current += timedelta(days=1)

    df = pl.DataFrame(rows)
    bq.upsert_data("calendar_dates", df, ["date"], context=context)

    return dg.MaterializeResult(
        metadata={
            "rows": len(df),
            "start_date": str(CALENDAR_START_DATE),
            "end_date": str(CALENDAR_END_DATE),
            "current_date": str(today),
            "first_10_rows": str(df.head(10)),
        }
    )


@dg.asset(
    group_name=CALENDAR_GROUP,
    kinds={"api", "economic_calendar"},
    description="Economic calendar events from Forex Factory JSON feeds",
)
def economic_calendar(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """Fetch the weekly economic calendar events and store them in DuckDB."""
    context.log.info("Fetching economic calendar data")

    events: list[dict[str, object]] = []
    fetched_at = datetime.now(timezone.utc)

    with httpx.Client(timeout=30.0) as client:
        for feed_name, url in ECONOMIC_CALENDAR_FEEDS.items():
            try:
                response = client.get(url)
                response.raise_for_status()
            except Exception as exc:
                context.log.warning(f"Failed to fetch {feed_name} calendar: {exc}")
                continue

            for item in response.json():
                if not item:
                    continue

                event_time = item.get("time")
                event_date = item.get("date")

                if event_date:
                    try:
                        event_datetime = datetime.strptime(
                            f"{event_date} {event_time}", "%Y-%m-%d %H:%M"
                        )
                        event_datetime = event_datetime.replace(tzinfo=timezone.utc)
                    except Exception:
                        try:
                            event_datetime = datetime.strptime(event_date, "%Y-%m-%d")
                            event_datetime = event_datetime.replace(tzinfo=timezone.utc)
                        except Exception:
                            event_datetime = None
                else:
                    event_datetime = None

                title = item.get("title", "")
                event_type = classify_event_type(title)

                events.append(
                    {
                        "event_id": item.get("id"),
                        "title": title,
                        "country": item.get("country"),
                        "date": event_date,
                        "time": event_time,
                        "timestamp": event_datetime,
                        "impact": item.get("impact"),
                        "forecast": parse_numeric_value(item.get("forecast")),
                        "previous": parse_numeric_value(item.get("previous")),
                        "actual": parse_numeric_value(item.get("actual")),
                        "event_type": event_type,
                        "fetched_at": fetched_at,
                        "source": feed_name,
                    }
                )

    df = pl.DataFrame(events)
    if df.is_empty():
        context.log.warning("No economic calendar events fetched")
        return dg.MaterializeResult(metadata={"rows": 0, "status": "no_data"})

    bq.upsert_data("economic_calendar", df, ["event_id"], context=context)

    return dg.MaterializeResult(
        metadata={
            "rows": len(df),
            "sources": df["source"].unique().to_list(),
            "first_10_rows": str(df.head(10)),
        }
    )


@dg.asset(
    group_name=CALENDAR_GROUP,
    kinds={"api", "earnings"},
    description="Earnings calendar data from Yahoo Finance",
)
def earnings_calendar(
    context: dg.AssetExecutionContext,
    yahoo_finance: YahooFinanceResource,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """
    Ingest earnings calendar data from Yahoo Finance.

    Fetches upcoming and recent earnings announcements.
    """
    context.log.debug("Starting earnings calendar ingestion from Yahoo Finance")

    today = date.today()
    from_date = today - timedelta(days=7)
    to_date = today + timedelta(days=28)

    try:
        df = yahoo_finance.get_earnings_range(from_date, to_date)
    except Exception as exc:
        context.log.error(f"Failed to fetch earnings calendar: {exc}")
        return dg.MaterializeResult(
            metadata={
                "num_earnings": 0,
                "error": str(exc),
            }
        )

    if df.is_empty():
        context.log.warning("No earnings data returned from Yahoo Finance")
        return dg.MaterializeResult(
            metadata={
                "num_earnings": 0,
                "date_range": f"{from_date} to {to_date}",
            }
        )

    fetched_at = datetime.now(timezone.utc).isoformat()
    processed_records = []
    for row in df.to_dicts():
        symbol = row.get("symbol", "")
        report_date = row.get("report_date", "")
        event_id = f"earnings_{report_date}_{symbol}"

        processed_records.append(
            {
                "event_id": event_id,
                "symbol": symbol,
                "company_name": row.get("company_name", ""),
                "report_date": report_date,
                "fiscal_date_ending": "",
                "eps_estimated": _to_float(row.get("eps_estimated")),
                "eps_actual": _to_float(row.get("eps_actual")),
                "revenue_estimated": None,
                "revenue_actual": None,
                "report_time": row.get("report_time", ""),
                "timing": row.get("timing", "unknown"),
                "updated_from_date": "",
                "event_type": "earnings",
                "source": "yahoo",
                "fetched_at": fetched_at,
            }
        )

    processed_df = pl.DataFrame(processed_records)
    # Pin numeric columns to Float64 so the staging table always matches the
    # FLOAT64 raw-table schema — even for a batch where every value is None,
    # which would otherwise infer a Null/String column and break the MERGE.
    processed_df = processed_df.with_columns(
        pl.col(col).cast(pl.Float64, strict=False) for col in EARNINGS_NUMERIC_COLUMNS
    )
    context.log.info(f"Fetched {len(processed_df)} earnings announcements")

    # Repair existing deployments: a target table first created by pre-fix code
    # can still hold these columns as STRING. The FLOAT64 staging columns above
    # would then fail the MERGE assignment into a STRING target, so normalize
    # the target's numeric columns first (SAFE_CAST; no-op if already FLOAT64
    # or the table doesn't yet exist).
    bq.normalize_column_types(
        "earnings_calendar", EARNINGS_NUMERIC_COLUMN_TYPES, context=context
    )
    bq.upsert_data("earnings_calendar", processed_df, ["event_id"], context=context)

    return dg.MaterializeResult(
        metadata={
            "num_earnings": len(processed_df),
            "date_range": f"{from_date} to {to_date}",
            "unique_symbols": processed_df["symbol"].n_unique(),
            "fetched_at": fetched_at,
        }
    )


@dg.asset_check(asset=calendar_dates)
def calendar_dates_quality_check(bq: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate the calendar dimension table is populated and recent."""
    if not bq.table_exists("calendar_dates"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "calendar_dates table does not exist"},
        )

    df = bq.execute_query(
        "SELECT MAX(date) AS max_date, COUNT(*) AS row_count FROM calendar_dates",
        read_only=True,
    )
    if df.is_empty():
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "calendar_dates table is empty"},
        )

    max_date = df["max_date"][0]
    row_count = df["row_count"][0]

    return dg.AssetCheckResult(
        passed=True,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "max_date": str(max_date),
            "row_count": int(row_count),
        },
    )


@dg.asset_check(asset=economic_calendar)
def economic_calendar_quality_check(
    bq: BigQueryWarehouseResource,
) -> dg.AssetCheckResult:
    """Ensure economic calendar has recent events."""
    if not bq.table_exists("economic_calendar"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "economic_calendar table does not exist"},
        )

    df = bq.execute_query(
        """
        SELECT MAX(date) AS max_date, COUNT(*) AS row_count
        FROM economic_calendar
        """,
        read_only=True,
    )
    if df.is_empty():
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "economic_calendar table is empty"},
        )

    max_date = df["max_date"][0]
    row_count = df["row_count"][0]

    return dg.AssetCheckResult(
        passed=True,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "max_date": str(max_date),
            "row_count": int(row_count),
        },
    )


@dg.asset_check(asset=earnings_calendar)
def earnings_calendar_quality_check(
    bq: BigQueryWarehouseResource,
) -> dg.AssetCheckResult:
    """Ensure earnings calendar table is populated."""
    if not bq.table_exists("earnings_calendar"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "earnings_calendar table does not exist"},
        )

    df = bq.execute_query(
        "SELECT COUNT(*) AS row_count FROM earnings_calendar",
        read_only=True,
    )
    if df.is_empty() or df["row_count"][0] == 0:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "earnings_calendar table is empty"},
        )

    return dg.AssetCheckResult(
        passed=True,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "row_count": int(df["row_count"][0]),
        },
    )


calendar_dates_job = dg.define_asset_job(
    name="calendar_dates_job",
    tags={
        "dagster/priority": "5",
        "dagster/max_runtime": 1800,
        "job_type": "daily_ingestion",
    },
    selection=dg.AssetSelection.assets("calendar_dates"),
    description="Calendar dates dimension table refresh - runs daily at 2 AM EST",
)

economic_calendar_job = dg.define_asset_job(
    name="economic_calendar_job",
    tags={
        "dagster/priority": "10",
        "dagster/max_runtime": 1800,
        "job_type": "daily_ingestion",
    },
    selection=dg.AssetSelection.assets("economic_calendar"),
    description="Economic calendar ingestion - runs daily at 2:30 AM EST",
)

earnings_calendar_job = dg.define_asset_job(
    name="earnings_calendar_job",
    tags={"dagster/priority": "10", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("earnings_calendar"),
    description="Earnings calendar ingestion - runs daily at 4 AM EST",
)

daily_calendar_dates_schedule = dg.ScheduleDefinition(
    name="daily_calendar_dates_schedule",
    cron_schedule="0 2 * * *",
    execution_timezone="America/New_York",
    job=calendar_dates_job,
    description="Daily calendar dates refresh at 2 AM EST",
)

daily_economic_calendar_schedule = dg.ScheduleDefinition(
    name="daily_economic_calendar_schedule",
    cron_schedule="30 2 * * *",
    execution_timezone="America/New_York",
    job=economic_calendar_job,
    description="Daily economic calendar ingestion at 2:30 AM EST",
)

daily_earnings_calendar_schedule = dg.ScheduleDefinition(
    name="daily_earnings_calendar_schedule",
    cron_schedule="0 4 * * *",
    execution_timezone="America/New_York",
    job=earnings_calendar_job,
    description="Daily earnings calendar ingestion at 4 AM EST",
)

defs = dg.Definitions(
    assets=[
        calendar_dates,
        economic_calendar,
        earnings_calendar,
    ],
    asset_checks=[
        calendar_dates_quality_check,
        economic_calendar_quality_check,
        earnings_calendar_quality_check,
    ],
    jobs=[calendar_dates_job, economic_calendar_job, earnings_calendar_job],
    schedules=[
        daily_calendar_dates_schedule,
        daily_economic_calendar_schedule,
        daily_earnings_calendar_schedule,
    ],
    resources={
        "yahoo_finance": yahoo_finance_resource,
    },
)
