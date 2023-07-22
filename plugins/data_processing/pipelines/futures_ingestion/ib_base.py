import random
from copy import copy
from io import BytesIO
from time import sleep, time

from datetime import datetime, timedelta

import boto3
import pandas as pd
import sqlalchemy as sa
from ib.ext.Contract import Contract
from ib.opt import Connection, message

from base import ExternalServicesMixin, credentials_conf
from pipelines.futures_ingestion.ib_connection_manager import IBConnectionManager


def create_contract(symbol, sec_type, exch, prim_exch="", curr="USD", expiration_date=None):
    """Create a Contract object defining what will
    be purchased, at which exchange and in which currency.

    symbol - The ticker symbol for the contract
    sec_type - The security type for the contract ('STK' is 'stock')
    exch - The exchange to carry out the contract on
    prim_exch - The primary exchange to carry out the contract on
    curr - The currency in which to purchase the contract"""
    contract = Contract()
    contract.m_symbol = symbol
    contract.m_secType = sec_type
    contract.m_exchange = exch
    contract.m_primaryExch = prim_exch
    contract.m_currency = curr
    contract.m_expiry = expiration_date and expiration_date.strftime("%Y%m%d")
    if sec_type == "FUT":
        contract.m_includeExpired = True
    return contract


class IBPullEngine(ExternalServicesMixin, object):
    client_id = 777

    def __init__(self, s3_path):
        super(IBPullEngine, self).__init__(s3_path)

        self.pg_engine = sa.create_engine("postgresql+psycopg2://%s:%s@%s:%s/%s" % (
            credentials_conf["postgres"]["username"],
            credentials_conf["postgres"]["password"],
            credentials_conf["postgres"]["host"],
            credentials_conf["postgres"]["port"],
            credentials_conf["postgres"]["database"]
        ), echo=True)
        self.s3 = boto3.client("s3")

        # Initialize tws connection
        self.ib_connection = IBConnectionManager.get_tws_connection(
            IBPullEngine.client_id,
            {
                message.historicalData: (self._on_futures_historical_data,),
                message.error: (self._on_error_data,)
            },
            []
        )
        IBPullEngine.client_id += 1
        # self.tws_connection = Connection.create(port=7496, clientId=self.CLIENT_ID) #host="172.16.1.10",
        # self.tws_connection.register(self._on_futures_historical_data, message.historicalData)
        # self.tws_connection.register(self._on_error_data, message.error)
        # self.tws_connection.connect()
        self.pull_finished = False
        self.pulled_data = []
        self.stored_files = []

        # Initialize s3 client
        self.s3_bucket = s3_path.split("s3://")[1].split("/", 1)[0]
        self.s3_key = s3_path.split("s3://")[1].split("/", 1)[1]

    def pull(self, intervals):
        print("Pulling intervals from ib: ", intervals)
        base_ib_settings = {
            "durationStr": "1 W",
            "barSizeSetting": "1 min",
            "useRTH": 0,
            "formatDate": 2,
        }
        for contract, ticker, interested_dates, additional_ib_settings in intervals:
            ib_settings = copy(base_ib_settings)
            ib_settings.update(additional_ib_settings)
            print(ib_settings)

            while True:
                with self.pg_engine.begin() as connection:
                    connection.execute("""LOCK ib_pull_log""")
                    result = [r for r in connection.execute("""SELECT count(ts) as c FROM ib_pull_log WHERE clock_timestamp() - ts <= INTERVAL '10 minutes'""")]
                    print(result[0]["c"])
                    if result[0]["c"] < 50:
                        connection.execute(
                            """INSERT INTO ib_pull_log (ts, request) VALUES (clock_timestamp(), %s)""",
                            ("(%s, %s)" % (ticker, str(interested_dates[-1])))
                        )
                        break
                    sleep(5)

            self.pull_finished = False
            self.pulled_data = []
            contract_descr = "cid=%s, sym=%s, secType=%s, exp=%s, K=%s, %s, %s, %s, %s, %s, %s" % (
                contract.m_conId,
                contract.m_symbol,
                contract.m_secType,
                contract.m_expiry,
                contract.m_strike,
                contract.m_right,
                contract.m_multiplier,
                contract.m_exchange,
                contract.m_primaryExch,
                contract.m_currency,
                contract.m_localSymbol)
            print("Requesting %s <%s>, %s" % (ticker, contract_descr, interested_dates))

            self._request_ib_historical_data(
                contract,
                (interested_dates[-1] + timedelta(days=1)).strftime("%Y%m%d 00:00:00 UTC"),
                ib_settings
            )

            seconds_elapsed = 0
            while not self.pull_finished and seconds_elapsed < 120:
                sleep(1)
                seconds_elapsed += 1

            if not self.pull_finished:
                raise Exception("Timeout error")

            print("Data collected")

            df = pd.DataFrame(
                self.pulled_data,
                columns=["Date", "Open", "High", "Low", "Close", "Volume", "Count", "VWAP", "HasGaps"],
            )
            df.set_index("Date", inplace=True)
            for next_date in interested_dates:
                print("Storing %s, %s" % (ticker, next_date))
                buf = BytesIO()
                day_df = df.loc[(df.index >= next_date) & (df.index < (next_date + timedelta(days=1)))]
                if day_df["Open"].sum() == 0:
                    print("day_df with empty prices:")
                    print(day_df)
                    raise RuntimeError("No prices for date: " + str(next_date))
                now_dt = datetime.now()
                now_ts = int(time())
                day_df["as_of"] = day_df.apply(lambda row: now_dt, axis=1)
                day_df["ticker"] = day_df.apply(lambda row: ticker, axis=1)
                day_df.to_csv(buf)
                s3_key = "%s%s/%s/%s_%s.csv" % (
                    self.s3_key, ticker, next_date.strftime("%Y/%m/%d"), ticker, now_ts
                )
                self.s3.put_object(
                    Bucket=self.s3_bucket,
                    Key=s3_key,
                    Body=buf.getvalue()
                )
                self.stored_files.append("s3://%s/%s" % (self.s3_bucket, s3_key))
                yield ticker, next_date, "s3://%s/%s" % (self.s3_bucket, s3_key)
                next_date += timedelta(days=1)

    def _request_ib_historical_data(self, contract, last_requested_bar, historical_data_features):
        self.ib_connection.tws_connection.reqHistoricalData(
            0, contract, last_requested_bar, historical_data_features['durationStr'],
            historical_data_features['barSizeSetting'], historical_data_features['whatToShow'],
            historical_data_features['useRTH'], historical_data_features['formatDate']
        )

    def _on_futures_historical_data(self, msg):
        if int(msg.high) > 0:
            #return
            try:
                date = pd.Timestamp(datetime.strptime(msg.date, "%Y%m%d"))
            except Exception:
                date = pd.Timestamp.utcfromtimestamp(float(msg.date))
            data = (
                date,
                msg.open,
                msg.high,
                msg.low,
                msg.close,
                msg.volume,
                msg.count,
                msg.WAP,
                msg.hasGaps
            )
            self.pulled_data.append(data)
        else:
            self.pull_finished = True

    def _on_error_data(self, msg):
        print(msg)

    def dispose(self):
        IBConnectionManager.dispose_tws_connection(self.ib_connection, False)
