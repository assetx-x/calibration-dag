import errno
from functools import lru_cache
from pylru import lrudecorator

from dateutil import rrule
import datetime
import pytz
import pandas as pd
from numpy import int64
import os
from commonlib import transform_data

FIRST_POSSIBLE_TRADING_DATE = "1990-01-01"
LAST_POSSIBLE_TRADING_DATE = "2050-12-31"


class marketTimeline(object):
    @staticmethod
    def canonicalize_datetime(dt):
        # Strip out any HHMMSS or timezone info in the user's datetime, so that
        # all the datetimes we return will be 00:00:00 UTC.
        return datetime.datetime(dt.year, dt.month, dt.day, tzinfo=pytz.utc)

    @staticmethod
    def canonicalize_datetime_local(dt):
        # Strip out any HHMMSS or timezone info in the user's datetime, so that
        # all the datetimes we return will be 00:00:00 in the same timezone the dt is,
        # so time zone does not change to UTC.
        return datetime.datetime(dt.year, dt.month, dt.day)

    @staticmethod
    @lru_cache(maxsize=128)
    def get_all_non_trading_days(start=None, end=None, timezone_corr='utc'):
        # timezone_corr can be 'utc' or 'none'; 'none' means not to change timezone
        start = start or pd.Timestamp(FIRST_POSSIBLE_TRADING_DATE)
        end = end or pd.Timestamp(LAST_POSSIBLE_TRADING_DATE)
        non_trading_rules = []

        if timezone_corr == 'none':
            start = marketTimeline.canonicalize_datetime_local(start)
            end = marketTimeline.canonicalize_datetime_local(end)
        else:
            start = marketTimeline.canonicalize_datetime(start)
            end = marketTimeline.canonicalize_datetime(end)
        weekends = rrule.rrule(
            rrule.YEARLY,
            byweekday=(rrule.SA, rrule.SU),
            cache=True,
            dtstart=start,
            until=end,
        )
        non_trading_rules.append(weekends)

        new_years = rrule.rrule(
            rrule.MONTHLY, byyearday=1, cache=True, dtstart=start, until=end
        )
        non_trading_rules.append(new_years)

        new_years_sunday = rrule.rrule(
            rrule.MONTHLY,
            byyearday=2,
            byweekday=rrule.MO,
            cache=True,
            dtstart=start,
            until=end,
        )
        non_trading_rules.append(new_years_sunday)

        mlk_day = rrule.rrule(
            rrule.MONTHLY,
            bymonth=1,
            byweekday=(rrule.MO(+3)),
            cache=True,
            dtstart=datetime.datetime(1998, 1, 1, tzinfo=pytz.utc),
            until=end,
        )
        non_trading_rules.append(mlk_day)

        presidents_day = rrule.rrule(
            rrule.MONTHLY,
            bymonth=2,
            byweekday=(rrule.MO(3)),
            cache=True,
            dtstart=start,
            until=end,
        )
        non_trading_rules.append(presidents_day)

        good_friday = rrule.rrule(
            rrule.DAILY, byeaster=-2, cache=True, dtstart=start, until=end
        )
        non_trading_rules.append(good_friday)

        memorial_day = rrule.rrule(
            rrule.MONTHLY,
            bymonth=5,
            byweekday=(rrule.MO(-1)),
            cache=True,
            dtstart=start,
            until=end,
        )
        non_trading_rules.append(memorial_day)

        july_4th = rrule.rrule(
            rrule.MONTHLY, bymonth=7, bymonthday=4, cache=True, dtstart=start, until=end
        )
        non_trading_rules.append(july_4th)

        july_4th_sunday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=7,
            bymonthday=5,
            byweekday=rrule.MO,
            cache=True,
            dtstart=start,
            until=end,
        )
        non_trading_rules.append(july_4th_sunday)

        july_4th_saturday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=7,
            bymonthday=3,
            byweekday=rrule.FR,
            cache=True,
            dtstart=start,
            until=end,
        )
        non_trading_rules.append(july_4th_saturday)

        labor_day = rrule.rrule(
            rrule.MONTHLY,
            bymonth=9,
            byweekday=(rrule.MO(1)),
            cache=True,
            dtstart=start,
            until=end,
        )
        non_trading_rules.append(labor_day)

        thanksgiving = rrule.rrule(
            rrule.MONTHLY,
            bymonth=11,
            byweekday=(rrule.TH(4)),
            cache=True,
            dtstart=start,
            until=end,
        )
        non_trading_rules.append(thanksgiving)

        christmas = rrule.rrule(
            rrule.MONTHLY,
            bymonth=12,
            bymonthday=25,
            cache=True,
            dtstart=start,
            until=end,
        )
        non_trading_rules.append(christmas)

        christmas_sunday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=12,
            bymonthday=26,
            byweekday=rrule.MO,
            cache=True,
            dtstart=start,
            until=end,
        )
        non_trading_rules.append(christmas_sunday)

        # If Christmas is a Saturday then 24th, a Friday is observed.
        christmas_saturday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=12,
            bymonthday=24,
            byweekday=rrule.FR,
            cache=True,
            dtstart=start,
            until=end,
        )
        non_trading_rules.append(christmas_saturday)

        non_trading_ruleset = rrule.rruleset()

        for rule in non_trading_rules:
            non_trading_ruleset.rrule(rule)

        non_trading_days = non_trading_ruleset.between(start, end, inc=True)

        # Add September 11th closings
        # http://en.wikipedia.org/wiki/Aftermath_of_the_September_11_attacks
        # Due to the terrorist attacks, the stock market did not open on 9/11/2001
        # It did not open again until 9/17/2001.
        #
        #    September 2001
        # Su Mo Tu We Th Fr Sa
        #                    1
        #  2  3  4  5  6  7  8
        #  9 10 11 12 13 14 15
        # 16 17 18 19 20 21 22
        # 23 24 25 26 27 28 29
        # 30

        for day_num in range(11, 17):
            non_trading_days.append(
                datetime.datetime(2001, 9, day_num, tzinfo=pytz.utc)
            )

        # Add closings due to Hurricane Sandy in 2012
        # http://en.wikipedia.org/wiki/Hurricane_sandy
        #
        # The stock exchange was closed due to Hurricane Sandy's
        # impact on New York.
        # It closed on 10/29 and 10/30, reopening on 10/31
        #     October 2012
        # Su Mo Tu We Th Fr Sa
        #     1  2  3  4  5  6
        #  7  8  9 10 11 12 13
        # 14 15 16 17 18 19 20
        # 21 22 23 24 25 26 27
        # 28 29 30 31

        for day_num in range(29, 31):
            non_trading_days.append(
                datetime.datetime(2012, 10, day_num, tzinfo=pytz.utc)
            )

        # Misc closings from NYSE listing.
        # http://www.nyse.com/pdfs/closings.pdf
        #
        # National Days of Mourning
        # - President Richard Nixon
        non_trading_days.append(datetime.datetime(1994, 4, 27, tzinfo=pytz.utc))
        # - President Ronald W. Reagan - June 11, 2004
        non_trading_days.append(datetime.datetime(2004, 6, 11, tzinfo=pytz.utc))
        # - President Gerald R. Ford - Jan 2, 2007
        non_trading_days.append(datetime.datetime(2007, 1, 2, tzinfo=pytz.utc))
        # - President George H.W. Bush - Dec 5, 2018
        non_trading_days.append(datetime.datetime(2018, 12, 5, tzinfo=pytz.utc))
        # - Juneteenth Holiday - Jun 20, 2022
        non_trading_days.append(datetime.datetime(2022, 6, 20, tzinfo=pytz.utc))

        non_trading_days.sort()
        return pd.DatetimeIndex(non_trading_days)

    @staticmethod
    def get_non_trading_days(start, end, timezone_corr='utc'):
        base_dates = marketTimeline.get_all_non_trading_days()
        start_dt_ts = pd.Timestamp(start)
        end_dt_ts = pd.Timestamp(end)
        start_dt = start_dt_ts if start_dt_ts.tzinfo else start_dt_ts.tz_localize("UTC")
        end_dt = end_dt_ts if end_dt_ts.tzinfo else end_dt_ts.tz_localize("UTC")
        result = base_dates[(base_dates >= start_dt) & (base_dates <= end_dt)]
        if not start_dt_ts.tzinfo and not end_dt_ts.tzinfo:
            result = result.tz_localize(None).normalize()
        return result

    @staticmethod
    @lrudecorator(1000)
    def get_trading_days(start, end, as_long=False):
        # Requirement: start and end should be UTC times or simply days
        start = start
        end = end
        non_trading_days = marketTimeline.get_non_trading_days(start, end)
        trading_day = pd.tseries.offsets.CDay(holidays=non_trading_days)
        dates = pd.date_range(start=start, end=end, freq=trading_day)
        dates = (
            dates
            if not as_long
            else dates.tz_localize("UTC").normalize().asi8.astype(int64)
        )
        return dates

    @staticmethod
    @lrudecorator(100)
    def get_trading_day_using_offset(cob, offset_in_business_days):
        trading_days = marketTimeline.get_trading_days(
            FIRST_POSSIBLE_TRADING_DATE, LAST_POSSIBLE_TRADING_DATE, as_long=True
        )
        cob_long = cob.tz_localize(None).normalize().asm8.astype(int64)
        date_loc = trading_days.searchsorted(cob_long, "left")
        return pd.Timestamp(
            trading_days[
                date_loc
                + offset_in_business_days
                - (trading_days[date_loc] != cob_long) * (offset_in_business_days > 0)
            ].astype("M8[ns]")
        )

    @staticmethod
    @lrudecorator(100)
    def get_previous_trading_day(cob):
        return marketTimeline.get_trading_day_using_offset(cob, -1)

    @staticmethod
    @lrudecorator(100)
    def isTradingDay(dt):
        date = marketTimeline.canonicalize_datetime(dt).date()
        return date not in marketTimeline.get_non_trading_days(
            date, date + datetime.timedelta(days=1)
        )

    @staticmethod
    @lrudecorator(100)
    def get_next_trading_day_if_holiday(cob):
        return marketTimeline.get_trading_day_using_offset(cob, 0)

    @staticmethod
    @lrudecorator(100)
    def get_previous_trading_day_if_holiday(cob):
        is_td = marketTimeline.isTradingDay(cob)
        if is_td:
            return marketTimeline.get_trading_day_using_offset(cob, 0)
        else:
            return marketTimeline.get_trading_day_using_offset(cob, -1)


def pick_trading_week_dates(start_date, end_date, mode='w-mon'):
    weekly_days = pd.date_range(start_date, end_date, freq=mode)
    trading_dates = pd.Series(weekly_days).apply(
        marketTimeline.get_next_trading_day_if_holiday
    )
    dates = trading_dates[(trading_dates >= start_date) & (trading_dates <= end_date)]
    return dates


def pick_trading_month_dates(start_date, end_date, mode="bme"):
    trading_days = marketTimeline.get_trading_days(start_date, end_date).tz_localize(
        None
    )
    if mode == "bme":
        dates = pd.Series(trading_days).groupby(trading_days.to_period('M')).last()
    else:
        dates = pd.Series(trading_days).groupby(trading_days.to_period('M')).first()
    dates = dates[(dates >= start_date) & (dates <= end_date)]
    return dates


def transform_wrapper(ts, transform_dict):
    try:
        transform_type = transform_dict[ts.name]
    except:
        transform_type = 1  # Default is no transform
    return transform_data(ts, transform_type)


def create_directory_if_does_not_exists(dir_path):
    try:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
