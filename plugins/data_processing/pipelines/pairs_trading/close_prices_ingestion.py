import pandas as pd
import sqlalchemy as sa

from base import TaskWithStatus, credentials_conf
from datetime import datetime


def get_sql_engine():
    redshift_engine = sa.create_engine(
        "redshift+psycopg2://%s:%s@%s:%s/%s" % (
            credentials_conf["redshift"]["username"],
            credentials_conf["redshift"]["password"],
            credentials_conf["redshift"]["host"],
            credentials_conf["redshift"]["port"],
            credentials_conf["redshift"]["database"]
        ), echo=True)

    return redshift_engine


class ClosePricesIngestionTask(TaskWithStatus):
    def collect_keys(self):
        with self.input()["universe_file"].open("r") as fd:
            tickers = pd.read_csv(fd, names=["ticker"], header=None)

        close_prices_df = get_close_prices(tickers)

        with self.output()["data"].open("w") as fd:
            close_prices_df.to_csv(fd)

        return {}


def get_close_prices(tickers,  start_date=datetime(2000, 1, 3), end_date=datetime.now()):
    """
    Downloads close prices data from daily_equity_prices table

    Args:
        tickers: pandas DF with tickers
        start_date: start date
        end_date: end date
    """
    tickers["missing"] = 0
    price_data = []

    for ticker in tickers["ticker"]:
        print("\n" + ticker)
        sql_str = (
            "SELECT date, close "
            "FROM daily_equity_prices "
            "WHERE ticker = '{ticker}' AND date BETWEEN '{start_date}' AND '{end_date}' "
            "ORDER BY date;"
        ).format(ticker=ticker, start_date=start_date, end_date=end_date)

        close_prices_df = pd.read_sql(sql_str, get_sql_engine(), index_col='date')
        tickers.loc[ticker, "missing"] = 0
        close_prices_df.name = ticker
        price_data.append(close_prices_df)
        tickers.loc[ticker, "missing"] = 1

    price_data = pd.concat(price_data, axis=1)
    price_data.fillna(method="ffill", inplace=True)

    return price_data

