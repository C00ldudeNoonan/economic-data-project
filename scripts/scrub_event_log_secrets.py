# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "psycopg2-binary>=2.9",
# ]
# ///
"""One-off scrubber for API keys leaked into Dagster's event_logs table.

When upstream HTTP requests failed (e.g. FRED 400 errors before #266 landed),
`requests.HTTPError` messages embedded the full URL — including query-string
secrets like `api_key=...` — which Dagster persisted into Postgres event_logs.

This script regexp_replaces those leaked values in place. Dry-run by default;
pass --apply to actually write.

Usage:
    # show counts only (default)
    uv run scripts/scrub_event_log_secrets.py

    # apply the scrub
    uv run scripts/scrub_event_log_secrets.py --apply

Env vars (read from Docker compose defaults if not set):
    DAGSTER_PG_HOST      (default: localhost)
    DAGSTER_PG_PORT      (default: 5432)
    DAGSTER_PG_DB        (default: dagster)
    DAGSTER_PG_USER      (default: dagster_user)
    DAGSTER_PG_PASSWORD  (required)
"""

import argparse
import os
import sys

import psycopg2

SECRET_PARAM_NAMES = (
    "api_key",
    "apikey",
    "access_key",
    "access_token",
    "auth_token",
)

# Matches `<param>=<value>` where value is a typical URL-safe API key/token.
# Anchored to URL query-string context with non-greedy value capture.
SECRET_PATTERN = (
    r"(" + "|".join(SECRET_PARAM_NAMES) + r")=([A-Za-z0-9_\-\.]+)"
)
REPLACEMENT = r"\1=REDACTED"


def connect():
    pw = os.environ.get("DAGSTER_PG_PASSWORD")
    if not pw:
        sys.exit("ERROR: DAGSTER_PG_PASSWORD env var is required")
    return psycopg2.connect(
        host=os.environ.get("DAGSTER_PG_HOST", "localhost"),
        port=int(os.environ.get("DAGSTER_PG_PORT", "5432")),
        dbname=os.environ.get("DAGSTER_PG_DB", "dagster"),
        user=os.environ.get("DAGSTER_PG_USER", "dagster_user"),
        password=pw,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true", help="Actually write changes (default: dry-run)"
    )
    args = parser.parse_args()

    conn = connect()
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute(
        "SELECT COUNT(*) FROM event_logs WHERE event ~* %s",
        (SECRET_PATTERN,),
    )
    affected = cur.fetchone()[0]
    print(f"Rows matching secret patterns: {affected}")

    if affected == 0:
        print("Nothing to scrub.")
        return

    cur.execute(
        """
        SELECT param, COUNT(*)
        FROM (
            SELECT (regexp_matches(event, %s, 'gi'))[1] AS param
            FROM event_logs
            WHERE event ~* %s
        ) t
        GROUP BY param
        ORDER BY 2 DESC
        """,
        (SECRET_PATTERN, SECRET_PATTERN),
    )
    print("By param name:")
    for name, count in cur.fetchall():
        print(f"  {name}: {count}")

    if not args.apply:
        print("\nDry-run only. Re-run with --apply to scrub.")
        return

    print("\nApplying scrub...")
    cur.execute(
        "UPDATE event_logs SET event = regexp_replace(event, %s, %s, 'gi') WHERE event ~* %s",
        (SECRET_PATTERN, REPLACEMENT, SECRET_PATTERN),
    )
    updated = cur.rowcount

    cur.execute(
        "SELECT COUNT(*) FROM event_logs WHERE event ~* %s",
        (SECRET_PATTERN,),
    )
    remaining = cur.fetchone()[0]

    if remaining > 0:
        conn.rollback()
        sys.exit(
            f"ERROR: {remaining} rows still match after scrub — rolled back. "
            "Investigate before retrying."
        )

    conn.commit()
    print(f"Updated {updated} rows. 0 leaked secrets remain.")


if __name__ == "__main__":
    main()
