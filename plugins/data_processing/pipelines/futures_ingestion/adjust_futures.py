from datetime import timedelta, datetime, time
from io import BytesIO

import pandas as pd
import pytz
import sqlalchemy as sa
from dateutil.relativedelta import relativedelta
from luigi import IntParameter, Parameter

from base import TaskWithStatus, credentials_conf
from pipelines.futures_ingestion.cboe_rules import TradingTimeline


class BaseAdjustTask(TaskWithStatus):
    ticker = None
    alias = None
    adjusted_fields = None
    daily_adjusted_fields = None
    lookback = IntParameter(default=22, significant=False)
    identifier_field = None

    def __init__(self, *args, **kwargs):
        super(BaseAdjustTask, self).__init__(*args, **kwargs)
        self.redshift_engine = sa.create_engine("redshift+psycopg2://%s:%s@%s:%s/%s" % (
            credentials_conf["redshift"]["username"],
            credentials_conf["redshift"]["password"],
            credentials_conf["redshift"]["host"],
            credentials_conf["redshift"]["port"],
            credentials_conf["redshift"]["database"]
        ))

    def get_trading_timeline(self):
        raise NotImplementedError()

    def collect_keys(self):
        assert self.identifier_field is not None

        adjusted_dates = {
            pd.Timestamp(row["date"])
            for row in self.redshift_engine.execute(
                """SELECT DISTINCT date(cob) as date FROM %s WHERE %s = %%s""" % (
                    self.input()["future_prices"].table,
                    self.identifier_field
                ),
                (str(self.alias), )
            )
        }
        trading_timeline = self.get_trading_timeline()
        trading_timeline = trading_timeline.exclude_dates(adjusted_dates)

        tick_dates = trading_timeline.get_tick_dates(include_rth=True, include_eth=True)
        print(tick_dates)
        where_clause = " OR ".join([
            "(%s = '%s' AND date(cob) IN %s)" % (
                "ticker", contract, "('" + "', '".join([d.strftime("%Y-%m-%d") for d in contract_tick_dates]) + "')"
            ) for (contract, contract_tick_dates) in tick_dates.iteritems()
        ])
        daily_where_clause = " OR ".join([
            "(%s = '%s' AND date(date) IN %s)" % (
                "ticker", contract, "('" + "', '".join([d.strftime("%Y-%m-%d") for d in contract_tick_dates]) + "')"
            ) for (contract, contract_tick_dates) in tick_dates.iteritems()
        ])

        with self.output()["adjustment_data"].open("w") as adjusted_output_fd:
            with self.output()["adjustment_daily_data"].open("w") as daily_output_fd:
                if not where_clause:
                    return

                df = pd.read_sql_query("SELECT * FROM %s WHERE %s ORDER BY cob" % (
                    self.input()["raw_future_prices"].table, where_clause
                ), self.redshift_engine, parse_dates=["cob"])
                daily_df = pd.read_sql_query("SELECT * FROM %s WHERE %s ORDER BY date" % (
                    self.input()["raw_daily_future_prices"].table, daily_where_clause
                ), self.redshift_engine, parse_dates=["date"])
                df.sort_values("cob", inplace=True)
                df.set_index("cob", inplace=True)
                df.index = df.index + pd.tseries.frequencies.to_offset("1T")
                daily_df.sort_values("date", inplace=True)
                daily_df.set_index("date", inplace=True)
                daily_df.index = daily_df.index + pd.tseries.frequencies.to_offset("0T")

                now = pd.Timestamp(datetime.now())
                daily_output_fd.write(",".join(self.daily_adjusted_fields) + "\n")
                adjusted_output_fd.write(",".join(self.adjusted_fields) + "\n")
                for trading_day in trading_timeline:
                    print("Adjusting %s" % trading_day.date)
                    adjusted_df, adjusted_daily_df = self.adjust_day(trading_day, df, daily_df)
                    adjusted_df["as_of_start"] = adjusted_df.apply(lambda row: now, axis=1)
                    adjusted_df["as_of_end"] = adjusted_df.apply(lambda row: None, axis=1)
                    adjusted_daily_df["as_of_start"] = adjusted_daily_df.apply(lambda row: now, axis=1)
                    adjusted_daily_df["as_of_end"] = adjusted_daily_df.apply(lambda row: None, axis=1)
                    adjusted_df = adjusted_df[self.adjusted_fields]
                    adjusted_daily_df = adjusted_daily_df[self.daily_adjusted_fields]
                    buf = BytesIO()
                    adjusted_df.to_csv(buf, header=False, index=False)
                    adjusted_output_fd.write(buf.getvalue())
                    buf = BytesIO()
                    adjusted_daily_df.to_csv(buf, header=False, index=False)
                    daily_output_fd.write(buf.getvalue())


class AdjustFutureTask(BaseAdjustTask):
    adjusted_fields = ["continous_contract", "underlying", "expiration", "cob", "open", "close", "high", "low",
                       "volume", "count", "vwap", "hasgaps", "as_of_start", "as_of_end"]
    daily_adjusted_fields = ["continous_contract", "underlying", "expiration", "date", "open", "close", "high", "low",
                             "volume", "settle", "next_open", "as_of_start", "as_of_end"]

    def adjust_day(self, trading_day, df, daily_df):
        daily_df = daily_df[daily_df["ticker"] == trading_day.contract].ix[trading_day.date]
        eth_df = df[df["ticker"] == trading_day.contract][trading_day.eth[0].start:trading_day.eth[-1].end]
        rth_df = df[df["ticker"] == trading_day.contract][trading_day.rth.start:trading_day.rth.end]
        next_eth = df[df["ticker"] == trading_day.next_eth_contract][trading_day.next_eth.start:trading_day.next_eth.end]

        print(trading_day.next_eth.start)
        print(trading_day.next_eth_contract)

        open = eth_df.iloc[0]["open"]
        settle = daily_df["close"]

        rth_df = rth_df[:trading_day.rth.end - timedelta(minutes=15)]
        rth_df = rth_df.set_value(rth_df.iloc[0].name, "open", open)
        rth_df = self._normalize_bar(rth_df, rth_df.iloc[0].name)
        close = rth_df.iloc[-1]["close"]
        rth_df = rth_df.set_value(rth_df.iloc[-1].name, "close", settle)
        rth_df = self._normalize_bar(rth_df, rth_df.iloc[-1].name)
        rth_df["continous_contract"] = rth_df.apply(lambda row: self.alias, axis=1)
        rth_df["underlying"] = rth_df.apply(lambda row: trading_day.contract.split("_")[0], axis=1)
        rth_df["expiration"] = rth_df.apply(lambda row: pd.Timestamp(datetime.strptime(
            trading_day.contract.split("_")[1],
            "%Y%m%d"
        )), axis=1)
        eastern_tz = pytz.timezone("US/Eastern")
        daily_df = pd.DataFrame.from_records([{
            "continous_contract": self.alias,
            "underlying": trading_day.contract.split("_")[0],
            "expiration": pd.Timestamp(datetime.strptime(trading_day.contract.split("_")[1], "%Y%m%d").date()),
            "date": pd.Timestamp(
                eastern_tz.localize(datetime.combine(trading_day.date.date(), time(16, 30))).astimezone(pytz.utc)
            ),
            "open": rth_df.iloc[0]["open"],
            "close": close,
            "high": max(rth_df["high"].max(), eth_df["high"].max()),
            "low": min(rth_df["low"].min(), eth_df["low"].min()),
            "volume": rth_df["volume"].sum() + eth_df["volume"].sum(),
            "settle": settle,
            "next_open": next_eth.iloc[0]["open"]
        }])
        print(float(daily_df["close"]))

        return rth_df.reset_index(), daily_df

    def _normalize_bar(self, df, index):
        df = df.set_value(index, "low", min(df["open"][index], df["low"][index], df["close"][index]))
        df = df.set_value(index, "high", max(df["open"][index], df["high"][index], df["close"][index]))

        return df


class AdjustIndexTask(BaseAdjustTask):
    adjusted_fields = ["cob", "open", "close", "high", "low", "as_of_start", "as_of_end", "ticker"]
    daily_adjusted_fields = ["symbol", "date", "open", "close", "high", "low", "as_of_start", "as_of_end"]
    ticker = Parameter()
    exchange = Parameter(significant=False)
    security_type = Parameter(significant=False)
    identifier_field = "ticker"

    @property
    def alias(self):
        return self.ticker

    def get_trading_timeline(self):
        start_date = self.date - relativedelta(months=self.lookback)
        end_date = self.date

        return TradingTimeline.for_index(self.ticker, start_date, end_date)

    def adjust_day(self, trading_day, df, daily_df):
        daily_df = daily_df[daily_df["ticker"] == trading_day.contract].ix[trading_day.date]
        rth_df = df[df["ticker"] == trading_day.contract][trading_day.rth.start:trading_day.rth.end]
        rth_df["ticker"] = rth_df.apply(lambda row: self.alias, axis=1)
        eastern_tz = pytz.timezone("US/Eastern")
        daily_df = pd.DataFrame.from_records([{
            "symbol": self.alias,
            "date":  pd.Timestamp(
                eastern_tz.localize(datetime.combine(trading_day.date.date(), time(16, 00))).astimezone(pytz.utc)
            ),
            "open": daily_df["open"],
            "close": daily_df["close"],
            "high": daily_df["high"],
            "low": daily_df["low"]
        }])

        return rth_df.reset_index(), daily_df


class AdjustVIXTask(AdjustFutureTask):
    ticker = "VIX"
    index = IntParameter(default=1)
    identifier_field = "continous_contract"

    @property
    def alias(self):
        return "VX" + str(self.index)

    def get_trading_timeline(self):
        start_date = self.date - relativedelta(months=self.lookback)
        end_date = self.date

        return TradingTimeline.for_vix(int(self.index), start_date, end_date)
