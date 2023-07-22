from datetime import datetime, date, time

import pytz

from pipelines.common.pipeline_util import get_prev_bus_day, TimePart


def test_middle_of_week():
    ref_date_utc = datetime.utcnow().replace(year=2018, month=1, day=11)
    prev_bus_day = get_prev_bus_day(ref_date_utc)
    assert prev_bus_day == date(2018, 1, 10)

def test_day_after_weekend():
    # a regular Monday, no holidays
    ref_date_utc = datetime.utcnow().replace(year=2018, month=1, day=8, hour=11)
    prev_bus_day = get_prev_bus_day(ref_date_utc)
    assert prev_bus_day == date(2018, 1, 5)

def test_day_on_weekend():
    # Sunday
    ref_date_utc = datetime.utcnow().replace(year=2018, month=1, day=7, hour=11)
    prev_bus_day = get_prev_bus_day(ref_date_utc)
    assert prev_bus_day == date(2018, 1, 5)
    # Saturday
    ref_date_utc = datetime.utcnow().replace(year=2018, month=1, day=6, hour=11)
    prev_bus_day = get_prev_bus_day(ref_date_utc)
    assert prev_bus_day == date(2018, 1, 5)

def test_day_after_holidays():
    # a day after Christmas (Tuesday, Xmas on Monday, so expect Friday)
    ref_date_utc = datetime.utcnow().replace(year=2017, month=12, day=26, hour=11)
    prev_bus_day = get_prev_bus_day(ref_date_utc)
    assert prev_bus_day == date(2017, 12, 22)
    # a day after Independence day (Wednesday)
    ref_date_utc = datetime.utcnow().replace(year=2017, month=7, day=5, hour=11)
    prev_bus_day = get_prev_bus_day(ref_date_utc)
    assert prev_bus_day == date(2017, 7, 3)

def test_day_after_monday_observed_as_holiday():
    # two days after Christmas 2016 (Monday the 26 markets were closed)
    ref_date_utc = datetime.utcnow().replace(year=2016, month=12, day=27, hour=11)
    prev_bus_day = get_prev_bus_day(ref_date_utc)
    assert prev_bus_day == date(2016, 12, 23)

def test_day_not_starting_on_midnight():
    # two days after Christmas 2016 (Monday the 26 markets were closed)
    ref_date_utc = datetime.combine(date(2018, 1, 11), time(1, 59))
    ref_date_utc = ref_date_utc.replace(tzinfo=pytz.timezone('US/Eastern')).astimezone(pytz.utc)
    prev_bus_day = get_prev_bus_day(ref_date_utc, time(2, 0))
    assert prev_bus_day == date(2018, 1, 9)

def test_time_should_be_preserved():
    ref_date_utc = datetime.combine(date(2018, 1, 11), time(12, 34))
    prev_bus_day = get_prev_bus_day(ref_date_utc, time_part=TimePart.Preserve)
    assert prev_bus_day.time() == ref_date_utc.time()

def test_datetime_should_be_normalized():
    ref_date_utc = datetime.combine(date(2018, 1, 11), time(12, 34))
    prev_bus_day = get_prev_bus_day(ref_date_utc, time_part=TimePart.Normalize)
    assert prev_bus_day.time() == time(0, 0)
