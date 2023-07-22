from collections import defaultdict
from types import StringTypes
from uuid import uuid4

import pandas as pd
from datetime import time, timedelta, datetime

import pytz
from luigi import Task, IntParameter

from base import DCMTaskParams


class PrepareEarningsTask(DCMTaskParams, Task):
    look_forward = IntParameter(
        description="Months to look forward for future events", default=6)

    def run(self):
        self.logger.info("Loading ticker universe")
        with self.input()["universe"].open() as universe_fd:
            ticker_list = [row.strip() for row in universe_fd.read().split("\n")]

        self.logger.info("Loading earnings dates")
        with self.input()["earnings_data"].engine() as engine:
            df = pd.read_sql("""select ticker, quarter, earnings_datetime_utc FROM %s WHERE as_of_end is null""" % self.input()["earnings_data"].table, engine)

        date = pd.to_datetime(self.date)
        current_quarter = "%d_%d" % (date.year, date.quarter)

        self.logger.info("Adding 'id' column")
        df = df[df["ticker"].isin(ticker_list)]
        df["id"] = df.apply(lambda row: str(uuid4()), axis=1)

        df.rename(columns={
            "ticker": "Ticker",
            "quarter": "Quarter",
            "earnings_datetime_utc": "Timestamp"
        }, inplace=True)

        self.logger.info("Partitioning into past and current quarters")
        past_quarters_events = df[df["Quarter"] < current_quarter]
        current_quarter_events = df[df["Quarter"] == current_quarter]

        past_ticker_in_quarters_observed = defaultdict(lambda: defaultdict(int))
        current_ticker_in_quarters_observed = defaultdict(lambda: defaultdict(int))

        def observe_event(d, row):
            d[row["Quarter"]][row["Ticker"]] += 1

        self.logger.info("Calculating observed ticker counts")
        past_quarters_events.apply(lambda row: observe_event(past_ticker_in_quarters_observed, row), axis=1)
        current_quarter_events.apply(lambda row: observe_event(current_ticker_in_quarters_observed, row), axis=1)

        self.logger.info("Selecting past quarters events with count == 1")
        past_quarters_events = past_quarters_events[
            past_quarters_events.apply(
                lambda row: past_ticker_in_quarters_observed[row["Quarter"]][row["Ticker"]] == 1,
                axis=1
            )
        ]

        self.logger.info("Selecting current quarter events with count == 1")
        current_quarter_events = current_quarter_events[
            current_quarter_events.apply(
                lambda row: current_ticker_in_quarters_observed[row["Quarter"]][row["Ticker"]] == 1,
                axis=1
            )
        ]

        self.logger.info("Saving results to CSV")

        with self.output()["past_quarters_earnings"].open("w") as output_fd:
            past_quarters_events.to_csv(output_fd, index=False)

        with self.output()["current_quarter_earnings"].open("w") as output_fd:
            current_quarter_events.to_csv(output_fd, index=False)
