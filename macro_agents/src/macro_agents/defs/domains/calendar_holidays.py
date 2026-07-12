"""US calendar-dimension constants and federal-holiday computation.

Pure date logic extracted from calendars.py so it can be unit-tested without
Dagster or a warehouse.
"""

import calendar
from datetime import date, timedelta

CALENDAR_START_DATE = date(1995, 1, 1)
CALENDAR_END_DATE = date(2030, 12, 31)

MONTH_NAMES = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]

MONTH_ABBRS = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]

WEEKDAY_NAMES = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]

WEEKDAY_ABBRS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date:
    """Return the date for the nth weekday of a month (weekday: 0=Mon)."""
    first_day = date(year, month, 1)
    days_until_weekday = (weekday - first_day.weekday()) % 7
    day = 1 + days_until_weekday + (n - 1) * 7
    return date(year, month, day)


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    """Return the date for the last weekday of a month (weekday: 0=Mon)."""
    last_day = date(year, month, calendar.monthrange(year, month)[1])
    days_back = (last_day.weekday() - weekday) % 7
    return last_day - timedelta(days=days_back)


def _observed_date(holiday_date: date) -> date:
    """Return the observed date when a holiday falls on a weekend."""
    if holiday_date.weekday() == 5:
        return holiday_date - timedelta(days=1)
    if holiday_date.weekday() == 6:
        return holiday_date + timedelta(days=1)
    return holiday_date


def _add_holiday(
    holidays: dict[date, tuple[str, bool]],
    holiday_date: date,
    name: str,
    is_observed: bool,
) -> None:
    if holiday_date in holidays:
        existing_name, existing_observed = holidays[holiday_date]
        combined_name = f"{existing_name} / {name}"
        holidays[holiday_date] = (combined_name, existing_observed or is_observed)
        return
    holidays[holiday_date] = (name, is_observed)


def _build_us_federal_holidays(
    start_year: int, end_year: int
) -> dict[date, tuple[str, bool]]:
    holidays: dict[date, tuple[str, bool]] = {}

    for year in range(start_year, end_year + 1):
        entries = [
            ("New Year's Day", date(year, 1, 1)),
            ("Martin Luther King Jr. Day", _nth_weekday_of_month(year, 1, 0, 3)),
            ("Presidents' Day", _nth_weekday_of_month(year, 2, 0, 3)),
            ("Memorial Day", _last_weekday_of_month(year, 5, 0)),
            ("Independence Day", date(year, 7, 4)),
            ("Labor Day", _nth_weekday_of_month(year, 9, 0, 1)),
            ("Columbus Day", _nth_weekday_of_month(year, 10, 0, 2)),
            ("Veterans Day", date(year, 11, 11)),
            ("Thanksgiving Day", _nth_weekday_of_month(year, 11, 3, 4)),
            ("Christmas Day", date(year, 12, 25)),
        ]

        if year >= 2021:
            entries.append(("Juneteenth", date(year, 6, 19)))

        for name, actual_date in entries:
            _add_holiday(holidays, actual_date, name, False)
            observed = _observed_date(actual_date)
            if observed != actual_date:
                _add_holiday(holidays, observed, name, True)

    return holidays
