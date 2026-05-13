"""SEC filing sensors for automated discovery and processing.

Monitors for unprocessed filings and triggers the document processing pipeline.
Detects new S&P 500 companies and triggers CIK enrichment + metadata ingestion.
"""

import json
from datetime import datetime, timezone

import dagster as dg

from macro_agents.defs.domains.sec.jobs import (
    sec_filing_processing_job,
    sec_filings_ingestion_job,
)
from macro_agents.defs.domains.sec.config import (
    NEW_COMPANIES_CHECK_SECONDS,
    UNPROCESSED_FILINGS_CHECK_SECONDS,
)
from macro_agents.defs.resources.motherduck import MotherDuckResource


@dg.sensor(
    name="sec_unprocessed_filings_sensor",
    description=(
        "Monitors for unprocessed SEC filings and triggers the document "
        "processing pipeline (download, text extraction, BI signals)"
    ),
    minimum_interval_seconds=UNPROCESSED_FILINGS_CHECK_SECONDS,
    job=sec_filing_processing_job,
)
def sec_unprocessed_filings_sensor(
    context: dg.SensorEvaluationContext,
    md: MotherDuckResource,
):
    """
    Check for unprocessed SEC filings and trigger processing.

    Queries sec_filings for rows where processed=FALSE and primary_document
    is available. If any are found, yields a RunRequest for the processing job.

    Cursor tracks the last check timestamp to avoid duplicate triggers within
    the same evaluation window.
    """
    cursor = context.cursor
    last_check: dict = {}

    if cursor:
        try:
            last_check = json.loads(cursor)
        except json.JSONDecodeError:
            context.log.warning("Invalid cursor JSON, starting fresh")

    last_run_key = last_check.get("last_run_key", "")

    conn = None
    try:
        conn = md.get_connection()

        result = conn.execute(
            """
            SELECT COUNT(*) as cnt
            FROM sec_filings
            WHERE processed = FALSE
            AND primary_document IS NOT NULL
            AND primary_document != ''
            """
        ).fetchone()
        unprocessed_count = result[0] if result else 0

    except Exception as e:
        context.log.warning(f"Could not query sec_filings table: {e}")
        unprocessed_count = 0

    finally:
        if conn:
            conn.close()

    now_iso = datetime.now(timezone.utc).isoformat()

    if unprocessed_count > 0:
        run_key = f"sec_processing_{now_iso}"

        # Skip if we already triggered for this exact key
        if run_key == last_run_key:
            context.log.debug("Already triggered for this evaluation, skipping")
            return

        context.log.info(
            f"Found {unprocessed_count} unprocessed filings, triggering processing job"
        )

        yield dg.RunRequest(
            run_key=run_key,
            tags={
                "trigger": "unprocessed_filings_detected",
                "unprocessed_count": str(unprocessed_count),
                "checked_at": now_iso,
            },
        )

        context.update_cursor(
            json.dumps(
                {
                    "last_check": now_iso,
                    "last_run_key": run_key,
                    "last_unprocessed_count": unprocessed_count,
                }
            )
        )
    else:
        context.log.debug("No unprocessed filings found")
        context.update_cursor(
            json.dumps(
                {
                    "last_check": now_iso,
                    "last_run_key": last_run_key,
                    "last_unprocessed_count": 0,
                }
            )
        )


@dg.sensor(
    name="sec_new_companies_sensor",
    description=(
        "Detects S&P 500 companies with CIK codes that have no SEC filings yet "
        "and triggers the ingestion pipeline to fetch their filing metadata"
    ),
    minimum_interval_seconds=NEW_COMPANIES_CHECK_SECONDS,
    job=sec_filings_ingestion_job,
)
def sec_new_companies_sensor(
    context: dg.SensorEvaluationContext,
    md: MotherDuckResource,
):
    """
    Detect new S&P 500 companies missing from sec_filings and trigger ingestion.

    Compares sec_company_cik against sec_filings to find companies that have
    CIK codes but no filing metadata yet. This catches newly added S&P 500
    members after sp500_cik_enriched has run.
    """
    cursor = context.cursor
    last_check: dict = {}

    if cursor:
        try:
            last_check = json.loads(cursor)
        except json.JSONDecodeError:
            context.log.warning("Invalid cursor JSON, starting fresh")

    last_run_key = last_check.get("last_run_key", "")

    conn = None
    try:
        conn = md.get_connection()

        # Check if sec_filings table exists; if not, all CIK companies need ingestion
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name = 'sec_filings'"
        ).fetchall()

        if not tables:
            # sec_filings doesn't exist yet — count all companies with CIKs
            result = conn.execute("SELECT COUNT(*) FROM sec_company_cik").fetchone()
            new_company_count = result[0] if result else 0
            if new_company_count > 0:
                context.log.info(
                    "sec_filings table not found, treating all "
                    f"{new_company_count} CIK companies as new"
                )
        else:
            result = conn.execute(
                """
                SELECT COUNT(*) as cnt
                FROM sec_company_cik c
                WHERE NOT EXISTS (
                    SELECT 1 FROM sec_filings f WHERE f.symbol = c.symbol
                )
                """
            ).fetchone()
            new_company_count = result[0] if result else 0

    except Exception as e:
        context.log.warning(f"Could not query for new companies: {e}")
        new_company_count = 0

    finally:
        if conn:
            conn.close()

    now_iso = datetime.now(timezone.utc).isoformat()

    if new_company_count > 0:
        run_key = f"sec_new_companies_{now_iso}"

        if run_key == last_run_key:
            context.log.debug("Already triggered for this evaluation, skipping")
            return

        context.log.info(
            f"Found {new_company_count} companies without filings, "
            "triggering ingestion job"
        )

        yield dg.RunRequest(
            run_key=run_key,
            tags={
                "trigger": "new_companies_detected",
                "new_company_count": str(new_company_count),
                "checked_at": now_iso,
            },
        )

        context.update_cursor(
            json.dumps(
                {
                    "last_check": now_iso,
                    "last_run_key": run_key,
                    "last_new_company_count": new_company_count,
                }
            )
        )
    else:
        context.log.debug("All companies have filings")
        context.update_cursor(
            json.dumps(
                {
                    "last_check": now_iso,
                    "last_run_key": last_run_key,
                    "last_new_company_count": 0,
                }
            )
        )
