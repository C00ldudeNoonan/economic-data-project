"""Tests for the pure calendar helpers extracted from calendars.py (issue #136).

Holiday computation and event/value parsing moved into calendar_holidays.py and
calendar_events.py; these lock in their behavior now that they're independently
importable.
"""

from datetime import date

from macro_agents.defs.domains.calendar_events import (
    classify_event_type,
    parse_numeric_value,
)
from macro_agents.defs.domains.calendar_holidays import (
    _build_us_federal_holidays,
    _last_weekday_of_month,
    _nth_weekday_of_month,
    _observed_date,
)


class TestHolidayDateMath:
    def test_nth_weekday_of_month(self):
        # 3rd Monday of Jan 2024 = MLK Day = 2024-01-15
        assert _nth_weekday_of_month(2024, 1, 0, 3) == date(2024, 1, 15)

    def test_last_weekday_of_month(self):
        # Last Monday of May 2024 = Memorial Day = 2024-05-27
        assert _last_weekday_of_month(2024, 5, 0) == date(2024, 5, 27)

    def test_observed_date_shifts_off_weekend(self):
        # 2021-07-04 is a Sunday -> observed Monday the 5th.
        assert _observed_date(date(2021, 7, 4)) == date(2021, 7, 5)
        # 2026-07-04 is a Saturday -> observed Friday the 3rd.
        assert _observed_date(date(2026, 7, 4)) == date(2026, 7, 3)
        # A weekday is unchanged.
        assert _observed_date(date(2024, 7, 4)) == date(2024, 7, 4)


class TestBuildFederalHolidays:
    def test_core_2024_holidays_present(self):
        holidays = _build_us_federal_holidays(2024, 2024)
        assert holidays[date(2024, 1, 1)][0] == "New Year's Day"
        assert holidays[date(2024, 7, 4)][0] == "Independence Day"
        assert holidays[date(2024, 12, 25)][0] == "Christmas Day"

    def test_juneteenth_only_from_2021(self):
        assert date(2020, 6, 19) not in _build_us_federal_holidays(2020, 2020)
        assert date(2021, 6, 19) in _build_us_federal_holidays(2021, 2021)

    def test_weekend_holiday_adds_observed_entry(self):
        # New Year's Day 2022 fell on a Saturday -> observed Fri 2021-12-31.
        holidays = _build_us_federal_holidays(2022, 2022)
        assert date(2022, 1, 1) in holidays
        observed = holidays[date(2021, 12, 31)]
        assert observed[1] is True  # is_observed flag


class TestEventClassification:
    def test_maps_known_keywords(self):
        assert classify_event_type("US CPI y/y") == "inflation"
        assert classify_event_type("Non-Farm Payrolls") == "employment"
        assert classify_event_type("FOMC Rate Decision") == "central_bank"

    def test_unknown_title_is_miscellaneous(self):
        assert classify_event_type("Widget Festival") == "miscellaneous"


class TestParseNumericValue:
    def test_plain_number(self):
        assert parse_numeric_value("3.5") == 3.5

    def test_percent_and_commas_stripped(self):
        assert parse_numeric_value("1,234.5%") == 1234.5

    def test_suffix_multipliers(self):
        assert parse_numeric_value("2K") == 2_000
        assert parse_numeric_value("1.5M") == 1_500_000
        assert parse_numeric_value("3B") == 3_000_000_000

    def test_blank_and_non_numeric_map_to_none(self):
        assert parse_numeric_value("") is None
        assert parse_numeric_value(None) is None
        assert parse_numeric_value("N/A") is None
