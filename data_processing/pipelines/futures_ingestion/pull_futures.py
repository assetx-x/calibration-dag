import pandas as pd
import sqlalchemy as sa
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from luigi import IntParameter, Parameter

from base import TaskWithStatus, credentials_conf
from pipelines.futures_ingestion.cboe_rules import TradingTimeline
from pipelines.futures_ingestion.ib_base import IBPullEngine, create_contract


class PullFutureTask(TaskWithStatus):
    ticker = None
    alias = None
    security_type = None
    exchange = None
    ib_config = None
    lookback = IntParameter(default=22)  # How far look back in months (IB controller limitations)
    ib_type = None
    pull_interval = None

    def __init__(self, *args, **kwargs):
        super(PullFutureTask, self).__init__(*args, **kwargs)
        self.pg_engine = sa.create_engine("postgresql+psycopg2://%s:%s@%s:%s/%s" % (
            credentials_conf["postgres"]["username"],
            credentials_conf["postgres"]["password"],
            credentials_conf["postgres"]["host"],
            credentials_conf["postgres"]["port"],
            credentials_conf["postgres"]["database"]
        ), echo=True)

    def get_trading_timeline(self):
        raise NotImplementedError()

    def collect_keys(self):
        assert self.ticker is not None
        assert self.alias is not None
        assert self.exchange is not None
        assert self.security_type is not None
        assert self.ib_config is not None
        assert self.ib_type is not None
        assert self.pull_interval is not None

        self.ib_pull_engine = IBPullEngine(self.output()["data"].path)

        trading_timeline = self.get_trading_timeline()
        dates_to_pull = trading_timeline.get_tick_dates(include_eth=True, include_rth=True)
        print("A priori the possible dates to pull include the following: {0}".format(dates_to_pull))

        for contract in dates_to_pull:
            dates_to_pull[contract] -= set([
                pd.Timestamp(row["date"]).replace(tzinfo=None)
                for row in self.pg_engine.execute(
                    """SELECT * FROM tickers_timeline WHERE ticker = %s AND type = %s""",
                    (contract, self.ib_type)
                )
            ])

        print("After checking table tickers_timeline the dates to pull include the following: {0}".format(dates_to_pull))
        intervals_to_pull = []
        for contract, contract_dates_to_pull in dates_to_pull.iteritems():
            # Split by weeks
            interested_dates = []
            contract_dates_to_pull = sorted(contract_dates_to_pull)
            while True:
                try:
                    next_date = contract_dates_to_pull.pop(0)
                except Exception:
                    next_date = None

                if interested_dates and (not next_date or next_date - interested_dates[0] >= timedelta(days=self.pull_interval)):
                    intervals_to_pull.append((
                        create_contract(
                            str(self.ticker),
                            str(self.security_type),
                            str(self.exchange),
                            "",
                            "USD",
                            datetime.strptime(contract.split("_")[-1], "%Y%m%d").date() if "_" in contract else None
                        ),
                        contract,
                        interested_dates,
                        self.ib_config
                    ))
                    interested_dates = []
                if not next_date:
                    break
                else:
                    interested_dates.append(next_date)
        intervals_to_pull = sorted(intervals_to_pull, key=lambda i: i[2][0])
        print("The data that will be acquired includes: {0}".format(map(str, intervals_to_pull)))

        # Perform actual pull
        for pulled_ticker, pulled_date, s3_location in self.ib_pull_engine.pull(intervals_to_pull):
            with self.pg_engine.begin() as connection:
                connection.execute(
                    """INSERT INTO tickers_timeline (ticker, date, type, s3_location) VALUES (%s, %s, %s, %s)""",
                    (pulled_ticker, pulled_date, self.ib_type, s3_location)
                )

    def on_finally(self):
        self.ib_pull_engine.dispose()


class PullVIXTask(PullFutureTask):
    index = IntParameter(default=1)
    ticker = "VIX"
    security_type = "FUT"
    exchange = "CFE"
    ib_config = {
        "whatToShow": "TRADES",
        "useRTH": 0
    }
    ib_type = "ib"
    pull_interval = 5

    @property
    def alias(self):
        return "VX" + str(self.index)

    def get_trading_timeline(self):
        start_date = self.date - relativedelta(months=self.lookback)
        end_date = self.date

        return TradingTimeline.for_vix(int(self.index), start_date, end_date)


class PullIndexTask(PullFutureTask):
    ticker = Parameter()
    exchange = Parameter()
    security_type = Parameter()
    ib_config = {
        "whatToShow": "TRADES",
        "useRTH": 1
    }
    ib_type = "ib"
    pull_interval = 5

    @property
    def alias(self):
        return self.ticker

    def get_trading_timeline(self):
        self.lookback = 22
        start_date = self.date - relativedelta(months=self.lookback)
        end_date = self.date

        return TradingTimeline.for_index(self.ticker, start_date, end_date)


class PullDailyVIXTask(PullFutureTask):
    index = IntParameter(default=1)
    ticker = "VIX"
    security_type = "FUT"
    exchange = "CFE"
    ib_config = {
        "whatToShow": "TRADES",
        "useRTH": 0,
        "durationStr": "1 M",
        "barSizeSetting": "1 day"
    }
    ib_type = "ib_daily"
    pull_interval = 20

    @property
    def alias(self):
        return "VX" + str(self.index)

    def get_trading_timeline(self):
        start_date = self.date - relativedelta(months=self.lookback)
        end_date = self.date

        print("Getting trading_timeline for VIX index {0} with start={1} and end={2}".format(self.index, start_date, end_date))
        trading_timeline = TradingTimeline.for_vix(int(self.index), start_date, end_date)
        # FIXME
        for day in trading_timeline:
            day.eth = None
            day.next_eth = None
        return trading_timeline


class PullDailyIndexTask(PullFutureTask):
    ticker = Parameter()
    exchange = Parameter()
    security_type = Parameter()
    ib_config = {
        "whatToShow": "TRADES",
        "useRTH": 1,
        "durationStr": "1 M",
        "barSizeSetting": "1 day"
    }
    ib_type = "ib_daily"
    pull_interval = 20

    @property
    def alias(self):
        return self.ticker

    def get_trading_timeline(self):
        self.lookback = 22
        start_date = self.date - relativedelta(months=self.lookback)
        end_date = self.date

        return TradingTimeline.for_index(self.ticker, start_date, end_date)
