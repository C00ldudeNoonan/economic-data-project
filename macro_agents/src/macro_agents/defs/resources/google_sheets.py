"""Google Sheets resource for data quality verification workflow."""

import json
import os

import dagster as dg
import gspread
from gspread.utils import ValueInputOption
import polars as pl
from google.oauth2 import service_account
from pydantic import Field


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class GoogleSheetsResource(dg.ConfigurableResource):
    """Resource for reading/writing Google Sheets for data quality verification.

    Uses service account credentials (JSON string or file path) via
    GOOGLE_APPLICATION_CREDENTIALS env var or the credentials_json field.
    """

    credentials_json: str | None = Field(
        default=None,
        description=(
            "Google service account credentials as JSON string or file path. "
            "Falls back to GOOGLE_APPLICATION_CREDENTIALS env var."
        ),
    )
    spreadsheet_id: str = Field(
        description="Google Sheets spreadsheet ID for data quality verification.",
    )

    def _get_client(self) -> gspread.Client:
        creds_raw = self.credentials_json or os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS", ""
        )
        if not creds_raw:
            raise ValueError(
                "credentials_json must be set or GOOGLE_APPLICATION_CREDENTIALS "
                "environment variable must be provided"
            )

        if creds_raw.strip().startswith("{"):
            creds_info = json.loads(creds_raw)
            credentials = service_account.Credentials.from_service_account_info(
                creds_info, scopes=SCOPES
            )
        elif os.path.exists(creds_raw):
            credentials = service_account.Credentials.from_service_account_file(
                creds_raw, scopes=SCOPES
            )
        else:
            raise ValueError(
                f"Credentials not found: '{creds_raw[:50]}...' is neither valid JSON "
                "nor an existing file path"
            )

        return gspread.authorize(credentials)

    def write_anomalies(
        self,
        anomalies_df: pl.DataFrame,
        worksheet_name: str = "anomalies",
    ) -> int:
        """Write anomaly rows to a Google Sheet for verification.

        Creates/clears the worksheet, writes headers + data rows, and adds
        GOOGLEFINANCE formula columns for price verification.

        Returns the number of rows written.
        """
        logger = dg.get_dagster_logger()
        client = self._get_client()
        spreadsheet = client.open_by_key(self.spreadsheet_id)

        # Get or create worksheet
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            worksheet.clear()
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=worksheet_name, rows=1000, cols=20
            )

        if anomalies_df.is_empty():
            worksheet.update([["No anomalies found"]], "A1")
            logger.info("No anomalies to write")
            return 0

        # Prepare headers
        headers = [
            "source_table",
            "symbol",
            "date",
            "check_type",
            "failure_reason",
            "flagged_close",
            "flagged_open",
            "flagged_high",
            "flagged_low",
            "gf_close",
            "gf_open",
            "gf_high",
            "gf_low",
            "verified",
        ]

        # Build data rows with GOOGLEFINANCE formulas
        rows = []
        for row in anomalies_df.iter_rows(named=True):
            date_str = str(row.get("date", ""))
            symbol = str(row.get("symbol", ""))

            # GOOGLEFINANCE formulas for verification
            # These will be evaluated by the spreadsheet when opened
            gf_close = (
                f'=IFERROR(GOOGLEFINANCE("{symbol}","close","{date_str}"),"")'
                if symbol and date_str
                else ""
            )
            gf_open = (
                f'=IFERROR(GOOGLEFINANCE("{symbol}","open","{date_str}"),"")'
                if symbol and date_str
                else ""
            )
            gf_high = (
                f'=IFERROR(GOOGLEFINANCE("{symbol}","high","{date_str}"),"")'
                if symbol and date_str
                else ""
            )
            gf_low = (
                f'=IFERROR(GOOGLEFINANCE("{symbol}","low","{date_str}"),"")'
                if symbol and date_str
                else ""
            )

            rows.append(
                [
                    str(row.get("source_table", "")),
                    symbol,
                    date_str,
                    str(row.get("check_type", "")),
                    str(row.get("failure_reason", "")),
                    row.get("close"),
                    row.get("open"),
                    row.get("high"),
                    row.get("low"),
                    gf_close,
                    gf_open,
                    gf_high,
                    gf_low,
                    "",  # verified column - to be filled manually
                ]
            )

        # Write headers + rows
        all_data = [headers] + rows
        worksheet.update(
            all_data, "A1", value_input_option=ValueInputOption.user_entered
        )

        logger.info(f"Wrote {len(rows)} anomaly rows to sheet '{worksheet_name}'")
        return len(rows)

    def read_verified_corrections(
        self,
        worksheet_name: str = "anomalies",
    ) -> pl.DataFrame:
        """Read back verified corrections from the Google Sheet.

        Returns rows where the 'verified' column is 'yes' (case-insensitive),
        with the GOOGLEFINANCE values as the corrected prices.
        """
        logger = dg.get_dagster_logger()
        client = self._get_client()
        spreadsheet = client.open_by_key(self.spreadsheet_id)

        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            logger.warning(f"Worksheet '{worksheet_name}' not found")
            return pl.DataFrame()

        records = worksheet.get_all_records()
        if not records:
            logger.info("No records found in sheet")
            return pl.DataFrame()

        df = pl.DataFrame(records)

        # Filter to verified rows only
        if "verified" not in df.columns:
            logger.warning("No 'verified' column found in sheet")
            return pl.DataFrame()

        verified = df.filter(pl.col("verified").str.to_lowercase().eq("yes"))

        logger.info(
            f"Found {len(verified)} verified corrections out of {len(df)} total rows"
        )
        return verified
