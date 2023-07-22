import json
from uuid import uuid4

import boto3
import pandas as pd
import sqlalchemy as sa

from base import credentials_conf, TaskWithStatus


class MergeRawDataTask(TaskWithStatus):
    def __init__(self, *args, **kwargs):
        super(MergeRawDataTask, self).__init__(*args, **kwargs)
        self.pg_engine = sa.create_engine("postgresql+psycopg2://%s:%s@%s:%s/%s" % (
            credentials_conf["postgres"]["username"],
            credentials_conf["postgres"]["password"],
            credentials_conf["postgres"]["host"],
            credentials_conf["postgres"]["port"],
            credentials_conf["postgres"]["database"]
        ))
        self.redshift_engine = sa.create_engine("redshift+psycopg2://%s:%s@%s:%s/%s" % (
            credentials_conf["redshift"]["username"],
            credentials_conf["redshift"]["password"],
            credentials_conf["redshift"]["host"],
            credentials_conf["redshift"]["port"],
            credentials_conf["redshift"]["database"]
        ))

    def collect_keys(self):
        self._merge_minute_data()
        self._merge_daily_data()

    def _merge_minute_data(self):
        self._merge_futures_data("ib", "cob", "raw_future_prices", "manifest")

    def _merge_daily_data(self):
        self._merge_futures_data("ib_daily", "date", "raw_daily_future_prices", "daily_manifest")

    def _merge_futures_data(self, ib_type, date_column, px_table_key, manifest_type):
        px_table_name = self.output()[px_table_key].table
        if self._generate_manifest(ib_type, date_column, px_table_name, manifest_type):
            credentials = self._get_redshift_credentials()
            query_params = {
                "table": px_table_name,
                "temp_update_table": "update_futures_" + str(uuid4()).replace("-", ""),
                "date_column": date_column,
                "update_manifest": self.output()[manifest_type].path,
                "access_key": credentials["Credentials"]["AccessKeyId"],
                "secret_access_key": credentials["Credentials"]["SecretAccessKey"],
                "session_token": credentials["Credentials"]["SessionToken"],
            }
            self._execute_redshift_query(query_params)

    def _generate_manifest(self, ib_type, date_column, px_table_name, manifest_type):
        available_dates_query = "SELECT ticker, date, s3_location FROM tickers_timeline WHERE type = '%s'" % ib_type
        available_dates = [
            (pd.Timestamp(row["date"]), row["ticker"], row["s3_location"])
            for row in self.pg_engine.execute(available_dates_query)
        ]

        merged_date_query = "SELECT DISTINCT ticker, date(%s) as date FROM %s" % (date_column, px_table_name)
        merged_dates = {
            (pd.Timestamp(row["date"]), row["ticker"])
            for row in self.redshift_engine.execute(merged_date_query)
        }

        manifest_data = {
            "entries": [
                {"url": s3_location, "mandatory": True}
                for date, ticker, s3_location in available_dates
                if (date, ticker) not in merged_dates
            ],
        }

        if not manifest_data["entries"]:
            print("No data to merge")
            return False

        with self.output()[manifest_type].open("w") as manifest_fd:
            manifest_fd.write(json.dumps(manifest_data))
        return True

    @staticmethod
    def _get_redshift_credentials():
        print("Acquiring temporary credentials for redshift")
        sts = boto3.client("sts")
        try:
            credentials = sts.assume_role(
                RoleArn="arn:aws:iam::294659765478:role/slave",
                RoleSessionName="AdjustmentImportDaily"
            )
        except Exception:
            print("STS:AssumeRole failed, falling back to normal session token retrieval")
            credentials = sts.get_session_token()
        return credentials

    def _execute_redshift_query(self, query_params):
        with self.redshift_engine.begin() as connection:
            self._create_temp_table(connection, query_params)
            self._populate_temp_table(connection, query_params)
            self._merge_data_into_final_table(connection, query_params)

    @staticmethod
    def _create_temp_table(connection, query_params):
        # noinspection SpellCheckingInspection
        connection.execute(
            """
            CREATE TEMP TABLE {temp_update_table} (
                {date_column} datetime,
                "open" real,
                high real,
                low real,
                close real,
                volume real,
                count real,
                vwap real,
                hasgaps bool,
                as_of timestamp not null,
                ticker varchar(32) not null,
                primary key(ticker, {date_column}, as_of)
            )
            DISTSTYLE KEY
            DISTKEY (ticker)
            SORTKEY (ticker, {date_column}, as_of)
            """.format(**query_params)
        )

    @staticmethod
    def _populate_temp_table(connection, query_params):
        # noinspection SpellCheckingInspection
        connection.execute(
            """
            COPY {temp_update_table}
            FROM '{update_manifest}'
            WITH CREDENTIALS AS 'aws_access_key_id={access_key};aws_secret_access_key={secret_access_key};token={session_token}'
            dateformat 'auto'
            timeformat 'auto'
            ignoreheader 1
            delimiter ','
            acceptinvchars
            blanksasnull
            emptyasnull
            manifest
            csv
            """.format(**query_params)
        )

    @staticmethod
    def _merge_data_into_final_table(connection, query_params):
        # noinspection SpellCheckingInspection
        connection.execute(
            """
            INSERT INTO {table} (
                {date_column}, "open", high, low, close, volume, count, vwap, hasgaps, as_of, import_time, ticker
            )
            SELECT DISTINCT
                ut.{date_column}, ut."open", ut.high, ut.low, ut.close, ut.volume, ut.count, ut.vwap, ut.hasgaps, ut.as_of, getdate(), ut.ticker
            FROM
                {temp_update_table} ut
            FULL OUTER JOIN (SELECT DISTINCT ticker, date({date_column}) as date FROM {table}) dates
            ON
                dates.ticker = ut.ticker
                AND dates.date = date(ut.{date_column})
            WHERE
                dates.date is NULL
            """.format(**query_params)
        )
