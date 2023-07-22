from collections import OrderedDict
from datetime import timedelta, date, datetime, time
from math import ceil

import pandas as pd
import pytz
from dateutil.relativedelta import relativedelta

from commonlib.market_timeline import marketTimeline


def ensure_date(date):
    return pd.Timestamp(date).replace(hour=0, minute=0, second=0, tzinfo=None)


def week_of_month(date):
    first_day = date.replace(day=1)
    day_of_month = date.day
    adjusted_dom = day_of_month + first_day.weekday()

    if first_day.weekday() == 6 and day_of_month != first_day:
        return int(ceil(adjusted_dom / 7.0)) - 1
    else:
        return int(ceil(adjusted_dom / 7.0))


cboe_timezone = pytz.timezone("US/Central")


# TODO: use as_of_date to generate holiday schedule to make all the needed dates are included
holidays_start_date = pd.Timestamp.now().normalize() - relativedelta(years=2)
holidays_end_date = holidays_start_date + relativedelta(years=5)
known_cboe_holidays = marketTimeline.get_non_trading_days(holidays_start_date, holidays_end_date)
known_cboe_holidays = known_cboe_holidays[known_cboe_holidays.weekday < 5].tz_localize(None).normalize().tolist()


class TradingHours(object):
    __slots__ = ["start", "end"]

    def __init__(self, start, end):
        self.start = start
        self.end = end

    def slice_df(self, df):
        return df.between_time(self.start, self.end)


class TradingDay(object):
    __slots__ = ["date", "contract", "rth", "eth", "next_eth", "next_eth_contract"]

    def __init__(self, date, contract, rth, eth, next_eth, next_eth_contract):
        self.date = date
        self.contract = contract
        self.rth = rth
        self.eth = eth
        self.next_eth = next_eth
        self.next_eth_contract = next_eth_contract


class TradingTimeline(object):
    def __init__(self, lst=None):
        self._lst = lst or []

    def __iter__(self):
        return iter(self._lst)

    def __repr__(self):
        return "<%s contracts=%s start=%s end=%s>" % (
            self.__class__.__name__,
            self.contracts,
            self._lst[0].date.strftime("%Y%m%d"),
            self._lst[-1].date.strftime("%Y%m%d")
        )

    @property
    def contracts(self):
        """
        All contracts which contributes to timeline
        """
        return set([day.contract for day in self])

    def get_tick_dates(self, include_rth=True, include_eth=True, exclude_dates=None):
        """
        All dates for which ticks exist (including eth/rth)
        """
        tick_dates = OrderedDict()
        exclude_dates = set(map(ensure_date, exclude_dates or []))

        for day in self:
            if day.contract not in tick_dates:
                tick_dates[day.contract] = set()
            if include_eth and day.next_eth_contract and day.next_eth_contract not in tick_dates:
                tick_dates[day.next_eth_contract] = set()
            if include_eth and day.eth is not None:
                for eth in day.eth:
                    tick_dates[day.contract].add(ensure_date(eth.start))
                    tick_dates[day.contract].add(ensure_date(eth.end))
                if day.next_eth_contract:
                    tick_dates[day.next_eth_contract].add(ensure_date(day.next_eth.start))
                #tick_dates[day.contract].add(ensure_date(day.next_eth.end))
            if include_rth:
                tick_dates[day.contract].add(ensure_date(day.rth.start))
                tick_dates[day.contract].add(ensure_date(day.rth.end))

        for contract in tick_dates:
            tick_dates[contract] -= exclude_dates

        return tick_dates

    def exclude_dates(self, dates):
        dates = map(ensure_date, dates)
        return TradingTimeline(filter(lambda trading_day: trading_day.date not in dates, self))

    @staticmethod
    def get_trading_day(next_date, contract, with_eth=True, whole_day=True):
        """
        Constructs trading day for specified data
        Args:
            next_date: date for which TradingDay object is needed

        Returns:
            TradingDay object
        """
        next_date = ensure_date(next_date)
        if next_date in known_cboe_holidays:
            raise ValueError("%s is holiday" % next_date)

        rth = TradingHours(
            pd.Timestamp(
                cboe_timezone.localize(datetime.combine(next_date, time(8, 31)))
            ).astimezone(pytz.utc),
            pd.Timestamp(
                cboe_timezone.localize(datetime.combine(next_date, time(15, 15)))
            ).astimezone(pytz.utc),
        ) if not whole_day else TradingHours(  # RTH
            pd.Timestamp(
                cboe_timezone.localize(datetime.combine(next_date, time(2, 16)))
            ).astimezone(pytz.utc),
            pd.Timestamp(
                cboe_timezone.localize(datetime.combine(next_date, time(15, 15)))
            ).astimezone(pytz.utc),
        )

        prev_date = next_date - timedelta(days=1)
        if with_eth:
            eth_start_time = time(17, 1) \
                if prev_date in known_cboe_holidays or next_date.weekday() == 0 \
                else time(15, 31)
            eth = [TradingHours(
                pd.Timestamp(
                    cboe_timezone.localize(datetime.combine(prev_date, eth_start_time))
                ).astimezone(pytz.utc),
                pd.Timestamp(
                    cboe_timezone.localize(datetime.combine(next_date, time(8, 30)))
                ).astimezone(pytz.utc),
            )]
            if (prev_date in known_cboe_holidays and prev_date.weekday() == 0
                    and prev_date != ensure_date(date(2016, 12, 26))
                    and prev_date != ensure_date(date(2017, 1, 2))
                    and prev_date != ensure_date(date(2017, 12, 25))
                    and prev_date != ensure_date(date(2018, 1, 1))):
                eth.insert(0, TradingHours(
                    pd.Timestamp(
                        cboe_timezone.localize(datetime.combine(prev_date - timedelta(days=1), time(17, 1)))
                    ).astimezone(pytz.utc),
                    pd.Timestamp(
                        cboe_timezone.localize(datetime.combine(prev_date, time(10, 30)))
                    ).astimezone(pytz.utc),
                ))
            if (prev_date.month == 11 and week_of_month(prev_date) == 4 and prev_date.weekday() == 3):
                # Thanksgiving case
                eth.insert(0, TradingHours(
                    pd.Timestamp(
                        cboe_timezone.localize(datetime.combine(prev_date - timedelta(days=1), time(15, 31)))
                    ).astimezone(pytz.utc),
                    pd.Timestamp(
                        cboe_timezone.localize(datetime.combine(prev_date, time(10, 30)))
                    ).astimezone(pytz.utc),
                ))
        else:
            eth = None

        return TradingDay(
            next_date,
            contract,
            rth,
            eth,
            None,
            None
        )

    @classmethod
    def for_vix(cls, vix_index, start_date, end_date):
        """
        Constructs entire timeline for specified VX<vix_index> for valid regular days
        Args:
            vix_index: index of vix, i.e. VX<vix_index>
            start_date: first date
            end_date: last_date

        Returns:
            TradingTimeline object
        """
        start_date = ensure_date(start_date)
        end_date = ensure_date(end_date)
        trading_timeline = cls()

        vix_expiration_map = cls._get_vix_expiration_map(vix_index, start_date, end_date + timedelta(days=4))
        print("Map formed between {0} and {1} for index {2}. Map content is {3}"\
              .format(start_date, end_date + timedelta(days=4), vix_index, vix_expiration_map))

        for expiration, interval_dates in vix_expiration_map.iteritems():
            for next_date in interval_dates:
                if (
                    next_date.weekday() in (0, 1, 2, 3, 4) and
                    next_date not in known_cboe_holidays
                ):
                    trading_timeline._lst.append(cls.get_trading_day(next_date, expiration.strftime("VIX_%Y%m%d")))

        # Fill next_eth
        for trading_day, next_trading_day in zip(trading_timeline._lst, trading_timeline._lst[1:]):
            if next_trading_day:
                trading_day.next_eth = next_trading_day.eth[0]
                trading_day.next_eth_contract = next_trading_day.contract
        trading_timeline._lst = trading_timeline._lst[:-1]

        print("There are {0} dates to be processed before filtering from {1} to {2}"\
              .format(len(trading_timeline._lst), min(trading_timeline._lst, key=lambda x:x.date).date,
                      max(trading_timeline._lst, key=lambda x:x.date).date))
        trading_timeline._lst = [k for k in trading_timeline._lst if k.date<=pd.Timestamp(end_date)]
        print("There are {0} dates to be processed after filtering from {1} to {2}"\
                  .format(len(trading_timeline._lst), min(trading_timeline._lst, key=lambda x:x.date).date,
                          max(trading_timeline._lst, key=lambda x:x.date).date))
        return trading_timeline

    @classmethod
    def for_index(cls, contract, start_date, end_date, exclude_dates=None):
        start_date = ensure_date(start_date)
        end_date = ensure_date(end_date)
        exclude_dates = map(ensure_date, exclude_dates or [])
        trading_timeline = cls()

        next_date = start_date
        while next_date <= end_date:
            if (
                    next_date not in exclude_dates and
                    next_date.weekday() in (0, 1, 2, 3, 4) and
                    next_date not in known_cboe_holidays
                ):
                    trading_timeline._lst.append(cls.get_trading_day(next_date, contract, with_eth=False, whole_day=True))
            next_date += timedelta(days=1)

        return trading_timeline

    @classmethod
    def _get_vix_expiration_date_for_month(cls, curr_date):
        if curr_date.month == 12:
            third_friday_next_month = date(curr_date.year + 1, 1, 15)
        else:
            third_friday_next_month = date(curr_date.year, curr_date.month + 1, 15)
        one_day = timedelta(days=1)
        thirty_days = timedelta(days=30)
        while third_friday_next_month.weekday() != 4:
            # Using += results in a timedelta object
            third_friday_next_month = third_friday_next_month + one_day

        if ensure_date(third_friday_next_month) in known_cboe_holidays:
            third_friday_next_month = third_friday_next_month - one_day

        return pd.Timestamp(third_friday_next_month - thirty_days)

    @classmethod
    def _get_vix_expiration_date(cls, for_date, vix_index):
        for_date = ensure_date(for_date)
        month_date = for_date.replace(day=1)

        if cls._get_vix_expiration_date_for_month(month_date) > for_date:
            expiration_month_date = month_date + relativedelta(months=(vix_index - 1))
        else:
            expiration_month_date = month_date + relativedelta(months=vix_index)

        return cls._get_vix_expiration_date_for_month(expiration_month_date)

    @classmethod
    def _get_vix_expiration_map(cls, vix_index, start_date, end_date):
        """
        Construct continues intervals according to VIX expiration calendar
        Args:
            vix_index: index of vix, i.e. VX<vix_index>
            start_date: first interval's date
            end_date: last interval's date

        Returns:
            OrderedDict({<expiration_date1>: [<date1>, <date2>, ...], <expiration_date2>: [<date3>, ...]})
        """
        expiration_map = OrderedDict()
        start_date = ensure_date(start_date)
        end_date = ensure_date(end_date)

        curr_date = start_date
        while curr_date <= end_date:
            expiration_date = cls._get_vix_expiration_date(curr_date, vix_index)
            next_expiration_date = cls._get_vix_expiration_date(curr_date, 1)
            interval_end_date = min(next_expiration_date - timedelta(days=1), end_date)
            interval_dates = expiration_map[expiration_date] = []
            while curr_date <= interval_end_date:
                interval_dates.append(curr_date)
                curr_date += timedelta(days=1)

        return expiration_map
