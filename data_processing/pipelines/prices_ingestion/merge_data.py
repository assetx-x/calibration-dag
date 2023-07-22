import json
import boto3
from datetime import datetime

import pickle
from sqlalchemy import create_engine
from sqlalchemy import text as sa_text

from base import credentials_conf, TaskWithStatus
from pipelines.prices_ingestion.config import get_config
from pipelines.prices_ingestion.etl_workflow_aux_functions import build_s3_url


class MergeDataTask(TaskWithStatus):
    def collect_keys(self):
        self.log("Merging tickers...")
        pg_engine = create_engine("postgresql+psycopg2://%s:%s@%s:%s/%s" % (
            credentials_conf["postgres"]["username"],
            credentials_conf["postgres"]["password"],
            credentials_conf["postgres"]["host"],
            credentials_conf["postgres"]["port"],
            credentials_conf["postgres"]["database"]
        ), echo=True)

        # tickers = [r[0] for r in engine.execute("SELECT ticker FROM adjustment_merge_queue WHERE run_dt = %s", (self.date.date()))]
        # if not tickers:
        #     # No tickers in queue
        #     self.log("No tickers to merge")
        #     return
        # in_clause = "('" + "', '".join([("%s-price-ingestion-adjust-%s" % (self.date.date().strftime("%Y%m%d"), ticker)) for ticker in tickers]) + "')"
        # self.log(in_clause)
        # statuses = [
        #     pickle.loads(row[0]) for row in
        #     engine.execute("SELECT serialized_data FROM pipeline_Status WHERE stage_id IN %s AND step_name = 'DepositAdjustedDataIntoS3' AND status_type = 'Success'" % in_clause)
        # ]

        adjusted_prices_manifest_location = self._create_price_data_manifest(pg_engine)
        adjustment_factors_manifest_location = self._create_adjustment_factors_manifest(pg_engine)
        self.execute_redshift_query(adjusted_prices_manifest_location, adjustment_factors_manifest_location)
        pg_engine.execute("DELETE FROM adjustment_merge_queue WHERE run_dt = %s", (self.date.date()))

    def _create_price_data_manifest(self, engine):
        statuses = self._load_stage_results(engine, "DepositAdjustedDataIntoS3")
        return self._create_manifest("adjusted_prices", "S3_LOCATION_LABEL", statuses)

    def _create_adjustment_factors_manifest(self, engine):
        statuses = self._load_stage_results(engine, "DepositAdjustmentFactorsIntoS3")
        return self._create_manifest("adjustment_factors", "S3_ADJ_FACTORS_LOCATION_LABEL", statuses)

    def _load_stage_results(self, engine, step_name):
        results_query = """SELECT serialized_data FROM pipeline_status 
                           WHERE stage_id like '{run_date}-price-ingestion-adjust-%' 
                           AND step_name = '{step_name}' 
                           AND status_type = 'Success'
                        """.format(run_date=self.date.date().strftime("%Y%m%d"), step_name=step_name)
        stage_results = engine.execute(sa_text(results_query))
        return [pickle.loads(row[0]) for row in stage_results]

    @staticmethod
    def _create_manifest(manifest_id, location_label, statuses):
        manifest_bucket = get_config()["adjustments"]["data_bucket"]
        manifest_key = "{manifest_location}/{manifest_id}_manifest.json".format(
            manifest_location=get_config()["adjustments"]["manifest_location"],
            manifest_id=manifest_id)
        tickers_data_files = [
            status[get_config()["etl_equities_results_labels"][location_label]]
            for status in statuses
        ]
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=manifest_bucket,
            Key=manifest_key,
            Body=json.dumps({
                "entries": [
                    {"url": s3_location, "mandatory": True}
                    for s3_location in tickers_data_files
                    if s3_location
                ],
            }, indent=2, separators=(',', ': '))
        )
        return build_s3_url(manifest_bucket, manifest_key)

    def execute_redshift_query(self, adjusted_prices_manifest_location, adjustment_factors_manifest_location):
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

        as_of_end = datetime.now()
        format_params = {
            "adjusted_prices_manifest": adjusted_prices_manifest_location,
            "adjustment_factors_manifest": adjustment_factors_manifest_location,
            "access_key": credentials["Credentials"]["AccessKeyId"],
            "secret_access_key": credentials["Credentials"]["SecretAccessKey"],
            "session_token": credentials["Credentials"]["SessionToken"],
            "equity_price_table": self.output()["prices"].table,
            "adjustment_factors_table": self.output()["adjustment_factors"].table,
            "whitelist_table": self.output()["whitelist"].table,
            "daily_table": self.output()["daily_prices"].table,
            "temp_table_prefix": "_staging"
        }

        engine = create_engine("redshift+psycopg2://%s:%s@%s:%s/%s" % (
            credentials_conf["redshift"]["username"],
            credentials_conf["redshift"]["password"],
            credentials_conf["redshift"]["host"],
            credentials_conf["redshift"]["port"],
            credentials_conf["redshift"]["database"]
        ), echo=True)

        with engine.begin() as connection:
            self._update_equity_price_table(connection, format_params, as_of_end)
            self._update_whitelist_table(connection, format_params, as_of_end, self.date.date())
            self._update_daily_price_table(connection, format_params, as_of_end)
            self._update_adjustment_factors_table(connection, format_params)

    @staticmethod
    def _update_equity_price_table(connection, format_params, as_of_end):
        # noinspection SqlNoDataSourceInspection,SqlDialectInspection
        connection.execute(
            """
            CREATE TEMP TABLE update_import_luigi_{temp_table_prefix} (
                cob datetime,
                volume real,
                "open" real,
                close real,
                high real,
                low real,
                as_of_start datetime not null,
                as_of_end datetime default null,
                ticker varchar(10) not null,
                primary key(ticker, cob, as_of_start)
            )
            DISTSTYLE KEY
            DISTKEY (ticker)
            SORTKEY (ticker, cob, as_of_start, as_of_end)
            """.format(**format_params)
        )
        connection.execute(
            """
            COPY update_import_luigi_{temp_table_prefix}
            FROM '{adjusted_prices_manifest}'
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
            """.format(**format_params)
        )
        connection.execute(
            """
            UPDATE {equity_price_table}
            SET as_of_end = as_of.min_as_of_start
            FROM update_import_luigi_{temp_table_prefix} ut
            INNER JOIN (
                SELECT ticker, date(cob) as cob_day, min(as_of_start) as min_as_of_start
                FROM update_import_luigi_{temp_table_prefix}
                GROUP BY ticker, cob_day
            ) as_of
            ON
                ut.ticker = as_of.ticker
                AND date(ut.cob) = as_of.cob_day
            WHERE
                {equity_price_table}.as_of_end IS NULL
                AND {equity_price_table}.ticker = ut.ticker
                AND {equity_price_table}.cob = ut.cob
                AND {equity_price_table}.as_of_start < ut.as_of_start
            """.format(**format_params), (as_of_end,)
        )
        connection.execute(
            """
            INSERT INTO {equity_price_table} (
                cob, "open", high, low, close, volume, as_of_start, as_of_end, ticker
            )
            SELECT DISTINCT
                ut.cob, ut."open", ut.high, ut.low, ut.close, ut.volume, ut.as_of_start, cast(NULL as timestamp), ut.ticker
            FROM
                update_import_luigi_{temp_table_prefix} ut
            LEFT JOIN (SELECT ticker, max(as_of_start) as latest_start FROM {equity_price_table} GROUP BY ticker) starts
            ON starts.ticker = ut.ticker
            WHERE
                starts.latest_start IS NULL OR
                starts.latest_start < ut.as_of_start
            """.format(**format_params)
        )

    @staticmethod
    def _update_whitelist_table(connection, format_params, as_of_end, run_dt):
        connection.execute(
            """
            UPDATE {whitelist_table}
            SET as_of_end = %s
            WHERE date(run_dt) = %s AND as_of_end IS NULL
            """.format(**format_params), (as_of_end, run_dt)
        )
        connection.execute(
            """
            INSERT INTO {whitelist_table} (ticker, run_dt, as_of_start, as_of_end)
            SELECT DISTINCT ticker, date(cob), %s, cast(NULL AS timestamp) FROM {equity_price_table} WHERE date(cob) = %s AND as_of_end IS NULL
            """.format(**format_params), (as_of_end, run_dt)
        )

    @staticmethod
    def _update_daily_price_table(connection, format_params, as_of_end):
        connection.execute(
            """
            DELETE FROM {daily_table}
            """.format(**format_params)
        )

        connection.execute(
            """
            INSERT INTO {daily_table} WITH
                equity_data_with_minute_indicator AS (
                    SELECT ticker, cob, date(cob), datediff(minute, date(cob),
                       convert_timezone('UTC','US/Eastern',cob) - interval '9 hours, 30 minutes') AS minute_in_day,
                       close, "open", high, low, volume
                    FROM {equity_price_table}
                    WHERE as_of_end is null),
                open_quotes AS (
                    SELECT ticker, date, "open" FROM equity_data_with_minute_indicator WHERE minute_in_day = 1
                ),
                close_quotes AS (
                    SELECT ticker, date, "close" FROM equity_data_with_minute_indicator WHERE minute_in_day = 390
                ),
                misc_quotes AS (
                    SELECT ticker, date, sum("volume") as volume, max("high") as high, min("low") AS low
                    FROM equity_data_with_minute_indicator
                    GROUP BY ticker, date
                )
            SELECT o.ticker, convert_timezone('US/Eastern','UTC',o.date + interval '16 hours') as "date", o."open", c.close, m.high, m.low, m.volume, %s as as_of_start, cast(NULL AS timestamp) as as_of_end
            FROM open_quotes o
            LEFT JOIN close_quotes c
            ON o.ticker=c.ticker AND o.date=c.date
            LEFT JOIN misc_quotes m
            ON o.ticker=m.ticker AND o.date=m.date
            """.format(**format_params), (as_of_end,)
        )

    @staticmethod
    def _update_adjustment_factors_table(connection, format_params):
        connection.execute(
            """TRUNCATE TABLE {adjustment_factors_table}""".format(**format_params)
        )
        connection.execute(
            """
            COPY {adjustment_factors_table}
            FROM '{adjustment_factors_manifest}'
            WITH CREDENTIALS AS 'aws_access_key_id={access_key};aws_secret_access_key={secret_access_key};token={session_token}'
            dateformat 'auto'
            timeformat 'auto'
            ignoreheader 1
            acceptinvchars
            blanksasnull
            emptyasnull
            format csv
            encoding UTF8
            manifest
            """.format(**format_params)
        )
