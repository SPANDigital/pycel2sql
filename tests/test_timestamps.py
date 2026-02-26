"""Timestamp and duration tests - ported from cel2sql_test.go."""

from pycel2sql import convert


class TestDuration:
    def test_second(self):
        assert convert('duration("10s")') == "INTERVAL 10 SECOND"

    def test_minute(self):
        assert convert('duration("1h1m")') == "INTERVAL 61 MINUTE"

    def test_hour(self):
        assert convert('duration("60m")') == "INTERVAL 1 HOUR"


class TestTimestamp:
    def test_timestamp_from_string(self):
        result = convert('timestamp("2021-09-01T18:00:00Z")')
        assert result == "CAST('2021-09-01T18:00:00Z' AS TIMESTAMP WITH TIME ZONE)"

    def test_timestamp_add(self):
        result = convert('duration("1h") + timestamp("2021-09-01T18:00:00Z")')
        assert result == "CAST('2021-09-01T18:00:00Z' AS TIMESTAMP WITH TIME ZONE) + INTERVAL 1 HOUR"

    def test_timestamp_sub(self):
        result = convert("created_at - interval(1, HOUR)")
        assert result == "created_at - INTERVAL 1 HOUR"


class TestTimestampExtract:
    def test_get_seconds(self):
        assert convert("created_at.getSeconds()") == "EXTRACT(SECOND FROM created_at)"

    def test_get_hours_with_timezone(self):
        result = convert('created_at.getHours("Asia/Tokyo")')
        assert result == "EXTRACT(HOUR FROM created_at AT TIME ZONE 'Asia/Tokyo')"

    def test_get_full_year(self):
        assert convert("birthday.getFullYear()") == "EXTRACT(YEAR FROM birthday)"

    def test_get_month(self):
        result = convert("scheduled_at.getMonth()")
        assert result == "EXTRACT(MONTH FROM scheduled_at) - 1"

    def test_get_day_of_month(self):
        result = convert("scheduled_at.getDayOfMonth()")
        assert result == "EXTRACT(DAY FROM scheduled_at) - 1"

    def test_get_minutes(self):
        assert convert("fixed_time.getMinutes()") == "EXTRACT(MINUTE FROM fixed_time)"


class TestInterval:
    def test_basic(self):
        assert convert("interval(1, MONTH)") == "INTERVAL 1 MONTH"


class TestDateTimeFunctions:
    def test_date(self):
        result = convert("birthday > date(2000, 1, 1) + 1")
        assert result == "birthday > DATE(2000, 1, 1) + 1"

    def test_time(self):
        result = convert('fixed_time == time("18:00:00")')
        assert result == "fixed_time = TIME('18:00:00')"

    def test_datetime(self):
        result = convert('scheduled_at != datetime(date("2021-09-01"), fixed_time)')
        assert result == "scheduled_at != DATETIME(DATE('2021-09-01'), fixed_time)"

    def test_date_add(self):
        result = convert('date("2021-09-01") + interval(1, DAY)')
        assert result == "DATE('2021-09-01') + INTERVAL 1 DAY"

    def test_date_sub(self):
        result = convert("current_date() - interval(1, DAY)")
        assert result == "CURRENT_DATE() - INTERVAL 1 DAY"

    def test_time_add(self):
        result = convert('time("09:00:00") + interval(1, MINUTE)')
        assert result == "TIME('09:00:00') + INTERVAL 1 MINUTE"

    def test_time_sub(self):
        result = convert('time("09:00:00") - interval(1, MINUTE)')
        assert result == "TIME('09:00:00') - INTERVAL 1 MINUTE"

    def test_datetime_add(self):
        result = convert('datetime("2021-09-01 18:00:00") + interval(1, MINUTE)')
        assert result == "DATETIME('2021-09-01 18:00:00') + INTERVAL 1 MINUTE"
