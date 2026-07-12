"""Economic-event feeds, event classification, and numeric parsing.

Pure helpers extracted from calendars.py so classification and value coercion
can be unit-tested independently of the ingestion assets.
"""

ECONOMIC_CALENDAR_FEEDS = {
    "this_week": "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
    "next_week": "https://nfs.faireconomy.media/ff_calendar_nextweek.json",
}

EVENT_TYPE_MAPPING = {
    "GDP": "growth",
    "Retail Sales": "growth",
    "Industrial Production": "growth",
    "Manufacturing": "growth",
    "PMI": "growth",
    "Trade Balance": "growth",
    "Current Account": "growth",
    "CPI": "inflation",
    "PPI": "inflation",
    "Inflation": "inflation",
    "Price Index": "inflation",
    "PCE": "inflation",
    "Employment": "employment",
    "Unemployment": "employment",
    "Jobless Claims": "employment",
    "NFP": "employment",
    "Non-Farm": "employment",
    "Jobs": "employment",
    "Payrolls": "employment",
    "Labor": "employment",
    "Claimant": "employment",
    "Interest Rate": "central_bank",
    "Rate Decision": "central_bank",
    "Monetary Policy": "central_bank",
    "FOMC": "central_bank",
    "Fed": "central_bank",
    "ECB": "central_bank",
    "BOE": "central_bank",
    "BOJ": "central_bank",
    "BOC": "central_bank",
    "RBA": "central_bank",
    "RBNZ": "central_bank",
    "SNB": "central_bank",
    "Bond": "bonds",
    "Auction": "bonds",
    "Treasury": "bonds",
    "Yield": "bonds",
    "Housing": "housing",
    "Home": "housing",
    "Building Permits": "housing",
    "Construction": "housing",
    "Existing Home": "housing",
    "New Home": "housing",
    "Pending Home": "housing",
    "Consumer Confidence": "consumer_survey",
    "Consumer Sentiment": "consumer_survey",
    "Michigan": "consumer_survey",
    "Business Confidence": "business_survey",
    "ifo": "business_survey",
    "ZEW": "business_survey",
    "ISM": "business_survey",
    "Tankan": "business_survey",
    "Speaks": "speech",
    "Speech": "speech",
    "Testimony": "speech",
    "Press Conference": "speech",
    "Chair": "speech",
    "Governor": "speech",
    "President": "speech",
}

# earnings_calendar numeric columns typed as FLOAT64 in BigQuery. Used both to
# pin the staging frame's dtypes and to normalize any drifted target columns
# (older tables created by pre-fix code can hold these as STRING).
EARNINGS_NUMERIC_COLUMNS = [
    "eps_estimated",
    "eps_actual",
    "revenue_estimated",
    "revenue_actual",
]
EARNINGS_NUMERIC_COLUMN_TYPES = {col: "FLOAT64" for col in EARNINGS_NUMERIC_COLUMNS}


def classify_event_type(title: str) -> str:
    """Classify event type based on title keywords."""
    title_upper = title.upper()
    for keyword, event_type in EVENT_TYPE_MAPPING.items():
        if keyword.upper() in title_upper:
            return event_type
    return "miscellaneous"


def parse_numeric_value(value: str | None) -> float | None:
    """Parse numeric value from string, handling percentages and suffixes."""
    if not value or value.strip() == "":
        return None

    cleaned = value.strip().replace("%", "").replace(",", "").replace(" ", "")
    if cleaned == "":
        return None

    suffix = cleaned[-1].upper()
    multipliers = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}
    multiplier = multipliers.get(suffix, 1)
    if multiplier != 1:
        cleaned = cleaned[:-1]

    try:
        return float(cleaned) * multiplier
    except ValueError:
        return None


def _to_float(value: str | float | int | None) -> float | None:
    """Coerce a source EPS value (str, float, or None) to float, or None.

    Yahoo's earnings feed returns EPS fields as strings, floats, or None
    depending on the row. The BigQuery ``earnings_calendar`` table types these
    columns as FLOAT64, so blank/None values must map to NULL and everything
    else must be a real float before the frame is built (otherwise Polars
    infers a String column and the MERGE is rejected).
    """
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
