from datetime import datetime, timedelta, date

import sqlalchemy as sa
import pandas as pd

from luigi import Task, DateParameter
from luigi.contrib.redshift import RedshiftTarget
from luigi.contrib.s3 import S3Target

from base import DCMTaskParams


class GenerateQuarterEndEventsTask(DCMTaskParams, Task):
    min_date = DateParameter(
        description="Minimum date", default=date(2000, 1, 3))

    def run(self):
        self.logger.info("Loading ticker universe")
        with self.input()["ticker_list"].open() as ticker_list_fd:
            tickers = [line.strip() for line in ticker_list_fd.read().split("\n")]

        with self.input()["price_data"].engine() as price_data_engine:
            self.logger.info("Loading price data")
            query_result = price_data_engine.execute(
                sa
                    .select([sa.column("ticker"), sa.func.min(sa.column("cob")).label("min_date")])
                    .select_from(self.input()["price_data"].table)
                    .group_by(sa.column("ticker"))
            )

            self.logger.info("Matching tickers with dates")
            tickers_dates_map = {
                ticker: (
                    min(min_date.date(), self.min_date),
                    self.date
                )
                for ticker, min_date in query_result
                if min_date is not None
            }

        self.logger.info("Constructing quarter end event data frame")
        quarter_end_events_df = pd.DataFrame([
            (ticker, quarter_end_date, "%d-%d" % (quarter_end_date.year, quarter_end_date.quarter))
            for ticker in tickers
                if ticker in tickers_dates_map
            for quarter_end_date in (pd.bdate_range(
                tickers_dates_map[ticker][0],
                tickers_dates_map[ticker][1],
                freq='Q', tz='US/Eastern'
            ) + timedelta(hours=16))
        ], columns=["Ticker", "Timestamp", "Quarter"])
        quarter_end_events_df['Id'] = range(len(quarter_end_events_df))
        quarter_end_events_df = quarter_end_events_df.set_index('Id')

        self.logger.info("Writing results to CSV")
        with self.output().open("w") as output_fd:
            quarter_end_events_df.to_csv(output_fd)
