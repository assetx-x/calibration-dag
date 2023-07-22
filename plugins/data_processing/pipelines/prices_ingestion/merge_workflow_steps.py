import json
import logging
import pandas as pd
from collections import namedtuple
from io import BytesIO, StringIO

from etl_workflow_steps import (BaseETLStep, BaseETLStage, EquitiesETLTaskParameters, StatusType,
                                compose_main_flow_and_engine_for_task)
from google.cloud import bigquery, storage
from pipelines.prices_ingestion.etl_workflow_aux_functions import get_redshift_engine, build_s3_url, store_data_in_s3
from pipelines.prices_ingestion.config import get_config


EquityDataForMerge = namedtuple("EquityDataForMerge", ["minute_data_file", "daily_data_file", "adjustment_factor_file"])
S3_LOCATION_LABEL = get_config()["etl_equities_results_labels"]["S3_LOCATION_LABEL"]

logger = logging.getLogger("merge_workflow")

class RedshiftMergeForAllEquityData(BaseETLStep):
    PROVIDES_FIELDS = []

    def __init__(self, equity_data_to_merge_list):
        self.equity_data_to_merge_list = None
        self.manifest_files = None
        BaseETLStep.__init__(self)
        if not hasattr(equity_data_to_merge_list, "__iter__") or isinstance(equity_data_to_merge_list, str):
            raise ValueError("Parameter equity_data_to_merge_list must be an iterable")
        self.equity_data_to_merge_list = list(equity_data_to_merge_list)
        incorrect_data = list(filter(lambda x:not isinstance(x, EquityDataForMerge), self.equity_data_to_merge_list))
        if incorrect_data:
            raise ValueError("All elements of parameter equity_data_to_merge_list must be of type EquityDataForMerge")

    def store_manifests_for_redshift_load(self):
        manifests = []
        for manifest_type in EquityDataForMerge._fields:
            manifest_content = {"entries": list(map(lambda x: {"url": x, "mandatory": True},
                                               [getattr(k, manifest_type) for k in self.equity_data_to_merge_list]))}
            manifest_buffer = StringIO(json.dumps(manifest_content))

            key_prefix = "{0}/{1}".format(get_config()["raw_data_pull"]["TickData"]["Price"]["data_location_prefix"],
                                          "manifests_for_redshift_staging")
            s3_manifest_key = store_data_in_s3(manifest_buffer, get_config()["raw_data_pull"]["base_data_bucket"],
                                               key_prefix, "manifest_{0}".format(manifest_type), self.task_params.run_dt,
                                               extension="json")

            manifests.append(build_s3_url(*s3_manifest_key))
        self.manifest_files = EquityDataForMerge(*manifests)

    def do_step_action(self, **kwargs):
        self.store_manifests_for_redshift_load()
        engine = get_redshift_engine()
        closing_date = pd.Timestamp.now("UTC").to_pydatetime()
        with engine.begin() as connection:
            self.__upsert_minute_adjusted_price_data(connection, closing_date)
            self.__upsert_daily_adjusted_price_data(connection, closing_date)
            self.__upsert_adjustment_factors_data(connection, closing_date)
            self._update_whitelist_table(connection, closing_date)
            logger.info("About to commit")
        engine.dispose()
        self.step_status[S3_LOCATION_LABEL] = self.manifest_files._asdict()
        return StatusType.Success

    def __upsert_minute_adjusted_price_data(self, connection, closing_date):
        cfg = get_config()
        query_params = {
            'equity_price_table': '{}'.format(
                cfg['redshift']['equity_price_table']
            ),
            'temp_table_id': '{}_staging'.format(
                cfg['redshift']['equity_price_table']
            )
        }

        # ------------------------------------------------------------------
        # Load up the JSON from storage
        # {
        #   "entries": [
        #     {
        #       "mandatory": true,
        #       "url": <url>
        #     },
        storage_client = storage.Client()
        mem_file = BytesIO()
        storage_client.download_blob_to_file(
            self.manifest_files.minute_data_file, mem_file
        )
        manifest = json.loads(mem_file.getvalue().decode('utf-8'))
        uris = [x['url'] for x in manifest['entries']]

        # ------------------------------------------------------------------
        # Fill a temporary table with the CSV
        #
        # Note: The time strings in the CSV files have time zones, but UTC

        job_config = bigquery.LoadJobConfig(
            schema=[
                    bigquery.SchemaField('cob', 'TIMESTAMP'), # <- Files have TZ
                    bigquery.SchemaField('volume', 'FLOAT'),
                    bigquery.SchemaField('open', 'FLOAT'),
                    bigquery.SchemaField('close', 'FLOAT'),
                    bigquery.SchemaField('high', 'FLOAT'),
                    bigquery.SchemaField('low', 'FLOAT'),
                    bigquery.SchemaField('as_of_start', 'TIMESTAMP'),
                    bigquery.SchemaField('as_of_end', 'TIMESTAMP'),
                    bigquery.SchemaField('ticker', 'STRING'),
                    bigquery.SchemaField('symbol_asof', 'STRING'),
                    bigquery.SchemaField('dcm_security_id', 'INTEGER'),
            ],
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            # only these rows in the temp table
            write_disposition='WRITE_TRUNCATE'
        )

        logger.info('Minute prices: copying data into {temp_table_id}'.format(**query_params))
        client = bigquery.Client()
        load_job = client.load_table_from_uri(
            uris, query_params['temp_table_id'], job_config=job_config
        )
        result = load_job.result()  # Waits for the job to complete.

        # ------------------------------------------------------------------

        logger.info("Minute prices: updating '{equity_price_table}' table using data in temporary table".format(**query_params))
        connection.execute(
            """
            UPDATE {equity_price_table} ept
            SET ept.as_of_end = cast(as_of.min_as_of_start as DATETIME)
            FROM {temp_table_id} ut
            INNER JOIN (
                SELECT ticker, date(cob) as cob_day, min(as_of_start) as min_as_of_start
                FROM {temp_table_id}
                GROUP BY ticker, cob_day
            ) as_of
            ON
                ut.ticker = as_of.ticker
                AND date(ut.cob) = as_of.cob_day
            WHERE
                ept.as_of_end IS NULL
                AND ept.ticker = ut.ticker
                AND ept.cob = cast(ut.cob as DATETIME)
                AND ept.as_of_start < cast(ut.as_of_start as DATETIME)
            """.format(**query_params)
        )

        logger.info("Minute prices: inserting new records into '{equity_price_table}' table".format(**query_params))
        connection.execute(
            """
            INSERT INTO {equity_price_table} (
                cob, open, high, low, close, volume, as_of_start, as_of_end, ticker,
                symbol_asof, dcm_security_id, farm_fingerprint
            )
            SELECT DISTINCT
                cast(ut.cob as DATETIME), ut.open, ut.high, ut.low, ut.close, ut.volume,
                cast(ut.as_of_start as DATETIME), cast(NULL as DATETIME),
                ut.ticker, ut.symbol_asof, ut.dcm_security_id, FARM_FINGERPRINT(ut.ticker)
            FROM
                {temp_table_id} ut
            LEFT JOIN (SELECT ticker, max(as_of_start) as latest_start FROM {equity_price_table} GROUP BY ticker) starts
            ON starts.ticker = ut.ticker
            WHERE
                starts.latest_start IS NULL OR
                starts.latest_start < cast(ut.as_of_start as DATETIME)
            """.format(**query_params)
        )

    # ideally we would like to have identical structure to the minute table and consolidate this function with the one
    # above. However many processes are now attached to the daily_equity_prices directly and through multiple points
    # of contact so the risk of consolidate it is higher than the benefit
    def __upsert_daily_adjusted_price_data(self, connection, closing_date):
        cfg = get_config()
        query_params = {
            'daily_equity_price_table': '{}'.format(
                cfg['redshift']['daily_price_table']
            ),
            'temp_table_id': '{}_staging'.format(
                cfg['redshift']['daily_price_table']
            )
        }

        # ------------------------------------------------------------------
        # Load up the JSON from storage
        # {
        #   "entries": [
        #     {
        #       "mandatory": true,
        #       "url": <url>
        #     },
        storage_client = storage.Client()
        mem_file = BytesIO()
        storage_client.download_blob_to_file(
            self.manifest_files.daily_data_file, mem_file
        )
        manifest = json.loads(mem_file.getvalue().decode('utf-8'))
        uris = [x['url'] for x in manifest['entries']]

        # ------------------------------------------------------------------
        # Fill a temporary table with the CSV

        # Note: Using TIMESTAMP instead of DATETIME since sometimes files seems to have TZ in UTC

        job_config = bigquery.LoadJobConfig(
            schema=[
                    bigquery.SchemaField('ticker', 'STRING'),
                    bigquery.SchemaField('date', 'TIMESTAMP'), # <- Files have TZ
                    bigquery.SchemaField('open', 'FLOAT'),
                    bigquery.SchemaField('close', 'FLOAT'),
                    bigquery.SchemaField('high', 'FLOAT'),
                    bigquery.SchemaField('low', 'FLOAT'),
                    bigquery.SchemaField('volume', 'FLOAT'),
                    bigquery.SchemaField('as_of_start', 'TIMESTAMP'),
                    bigquery.SchemaField('as_of_end', 'TIMESTAMP'),
                    bigquery.SchemaField('symbol_asof', 'STRING'),
                    bigquery.SchemaField('dcm_security_id', 'INTEGER'),
            ],
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            # only these rows in the temp table
            write_disposition='WRITE_TRUNCATE'
        )

        logger.info('Daily prices: copying data into {temp_table_id}'.format(**query_params))
        client = bigquery.Client()
        load_job = client.load_table_from_uri(
            uris, query_params['temp_table_id'], job_config=job_config
        )
        result = load_job.result()  # Waits for the job to complete.

        logger.info("Daily prices: updating '{daily_equity_price_table}' table using data in temporary table".format(**query_params))
        connection.execute(
            """
            UPDATE {daily_equity_price_table} dept
            SET dept.as_of_end = cast(as_of.min_as_of_start as DATETIME)
            FROM {temp_table_id} ut
            INNER JOIN (
                SELECT ticker, date(date) as cob_day, min(as_of_start) as min_as_of_start
                FROM {temp_table_id}
                GROUP BY ticker, cob_day
            ) as_of
            ON
                ut.ticker = as_of.ticker
                AND date(ut.date) = as_of.cob_day
            WHERE
                dept.as_of_end IS NULL
                AND dept.ticker = ut.ticker
                AND dept.date = cast(ut.date as DATETIME)
                AND dept.as_of_start < cast(ut.as_of_start as DATETIME)
            """.format(**query_params)
        )

        logger.info("Daily prices: inserting new records into '{daily_equity_price_table}' table".format(**query_params))
        connection.execute(
            """
            INSERT INTO {daily_equity_price_table} (
                ticker, date, open, close, high, low, volume, as_of_start, as_of_end, symbol_asof, dcm_security_id
            )
            SELECT DISTINCT
                ut.ticker, cast(ut.date as DATETIME), ut.open, ut.close, ut.high, ut.low, ut.volume,
                cast(ut.as_of_start as DATETIME), cast(NULL as DATETIME), ut.symbol_asof, ut.dcm_security_id
            FROM
                {temp_table_id} ut
            LEFT JOIN (SELECT ticker, max(as_of_start) as latest_start FROM {daily_equity_price_table} GROUP BY ticker) starts
            ON starts.ticker = ut.ticker
            WHERE
                starts.latest_start IS NULL OR
                starts.latest_start < cast(ut.as_of_start as DATETIME)
            """.format(**query_params)
        )

    def __upsert_adjustment_factors_data(self, connection, closing_date):
        cfg = get_config()
        query_params = {
            'adjustment_table': '{}'.format(
                cfg['redshift']['adjustment_factors_table']
            ),
            'temp_table_id': '{}_staging'.format(
                cfg['redshift']['adjustment_factors_table']
            )
        }

        # ------------------------------------------------------------------
        # Load up the JSON from storage
        # {
        #   "entries": [
        #     {
        #       "mandatory": true,
        #       "url": <url>
        #     },
        storage_client = storage.Client()
        mem_file = BytesIO()
        storage_client.download_blob_to_file(
            self.manifest_files.adjustment_factor_file, mem_file
        )
        manifest = json.loads(mem_file.getvalue().decode('utf-8'))
        uris = [x['url'] for x in manifest['entries']]

        # ------------------------------------------------------------------
        # Fill a temporary table with the CSV
        #
        # Note: The time strings in the CSV files have time zones, but UTC

        job_config = bigquery.LoadJobConfig(
            schema=[
                    bigquery.SchemaField('ticker', 'STRING'),
                    bigquery.SchemaField('cob', 'TIMESTAMP'), # <- Files have TZ
                    bigquery.SchemaField('split_factor', 'FLOAT'),
                    bigquery.SchemaField('split_div_factor', 'FLOAT'),
                    bigquery.SchemaField('as_of_start', 'TIMESTAMP'),
                    bigquery.SchemaField('as_of_end', 'TIMESTAMP'),
                    bigquery.SchemaField('symbol_asof', 'STRING'),
                    bigquery.SchemaField('dcm_security_id', 'INTEGER'),
            ],
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            # only these rows in the temp table
            write_disposition='WRITE_TRUNCATE'
        )

        logger.info("Adjustment factors: copying data into '{temp_table_id}' table".format(**query_params))
        client = bigquery.Client()
        load_job = client.load_table_from_uri(
            uris, query_params['temp_table_id'], job_config=job_config
        )
        result = load_job.result()  # Waits for the job to complete.

        # ------------------------------------------------------------------

        logger.info("Adjustment factors: updating '{adjustment_table}' table using data in temporary table".format(**query_params))
        connection.execute(
            """
            UPDATE {adjustment_table} adjt
            SET adjt.as_of_end = CAST(as_of.min_as_of_start as DATETIME)
            FROM {temp_table_id} ut
            INNER JOIN (
                SELECT ticker, date(cob) as cob_day, min(as_of_start) as min_as_of_start
                FROM {temp_table_id}
                GROUP BY ticker, cob_day
            ) as_of
            ON
                ut.ticker = as_of.ticker
                AND date(ut.cob) = as_of.cob_day
            WHERE
                adjt.as_of_end IS NULL
                AND adjt.ticker = ut.ticker
                AND adjt.cob = CAST(ut.cob as DATETIME)
                AND adjt.as_of_start < CAST(ut.as_of_start as DATETIME)
            """.format(**query_params)
        )

        logger.info("Adjustment factors: inserting new records into '{adjustment_table}' table".format(**query_params))
        connection.execute(
            """
            INSERT INTO {adjustment_table} (
                ticker, cob, split_factor, split_div_factor, as_of_start, as_of_end, symbol_asof, dcm_security_id
            )
            SELECT DISTINCT
                ut.ticker, CAST(ut.cob as DATETIME), ut.split_factor, ut.split_div_factor,
                CAST(ut.as_of_start as DATETIME), CAST(NULL as DATETIME),
                ut.symbol_asof, ut.dcm_security_id
            FROM
                {temp_table_id} ut
            LEFT JOIN (SELECT ticker, max(as_of_start) as latest_start FROM {adjustment_table} GROUP BY ticker) starts
            ON starts.ticker = ut.ticker
            WHERE
                starts.latest_start IS NULL OR
                starts.latest_start < CAST(ut.as_of_start as DATETIME)
            """.format(**query_params)
        )

    def _update_whitelist_table(self, connection, closing_date):
        cfg = get_config()
        query_params = {
            'equity_price_table': '{}'.format(
                cfg['redshift']['daily_price_table']
            ),
            'whitelist_table': '{}'.format(
                cfg['redshift']['whitelist_table']
            )
        }

        logger.info("Whitelist: updating '{whitelist_table}' table".format(**query_params))
        connection.execute(
            """
            UPDATE {whitelist_table}
            SET as_of_end = CAST(%s as DATETIME)
            WHERE date(run_dt) = %s AND as_of_end IS NULL
            """.format(**query_params), (closing_date, self.task_params.run_dt)
        )

        logger.info("Whitelist: inserting new records into '{whitelist_table}' table".format(**query_params))
        connection.execute(
            """
            INSERT INTO {whitelist_table} (ticker, run_dt, as_of_start, as_of_end)
            SELECT DISTINCT ticker, date(date), CAST(%s as DATETIME), CAST(NULL AS DATETIME)
            FROM {equity_price_table}
            WHERE date(date) = %s AND as_of_end IS NULL
            """.format(**query_params), (closing_date, self.task_params.run_dt)
        )


def _main():
    import os
    path_to_bucket = r'fluder/price_ingestion/adjusted_data/'

    # ticker = 'JPM'
    # start_dt = pd.Timestamp("2000-01-03")
    # end_dt = pd.Timestamp("2018-12-26")
    # run_dt = pd.Timestamp("2018-12-26")
    # as_of_dt = pd.Timestamp("2018-12-27 09:30:00")

    ticker = "EGO"  # has both CUSIP CHANGE and SPLIT on 12/28/2018 for the first time
    as_of_dt = pd.Timestamp("2019-02-10")
    run_dt = pd.Timestamp("2019-02-08")
    start_dt = pd.Timestamp("2018-12-26")
    end_dt = pd.Timestamp("2019-02-08")

    task_params = EquitiesETLTaskParameters(ticker=ticker, start_dt=start_dt, end_dt=end_dt,
                                            run_dt=run_dt, as_of_dt=as_of_dt)

    run_dt_str = run_dt.strftime("%Y/%m/%d")
    minute_prices_bucket = 'price_data_20170707/{0}/{1}/{0}_{2}.csv'.format(ticker, run_dt_str, '1549693953')
    daily_data_prices_bucket = 'daily_price_data/{0}/{1}/{0}_{2}.csv'.format(ticker, run_dt_str, '1549693954')
    adjustment_factors_bucket = 'adjustment_factors/{0}/{1}/{0}_{2}.csv'.format(ticker, run_dt_str, '1549693954')

    minute_data = build_s3_url('dcm-data-test', os.path.join(path_to_bucket, minute_prices_bucket))
    daily_data = build_s3_url('dcm-data-test', os.path.join(path_to_bucket, daily_data_prices_bucket))
    adj_data = build_s3_url('dcm-data-test', os.path.join(path_to_bucket, adjustment_factors_bucket))
    data_to_merge = [EquityDataForMerge(minute_data, daily_data, adj_data)]

    merge_step = RedshiftMergeForAllEquityData(data_to_merge)
    merge_flow = BaseETLStage("RedshiftMerge", "Stage", merge_step)
    engine, main_flow = compose_main_flow_and_engine_for_task("Data_merge_Sample", task_params, merge_flow)

    # run flow and get results
    engine.run(main_flow)
    results = engine.storage.fetch_all()
    step_results = filter(None, results["step_results"])
    print(step_results)


if __name__ == '__main__':
    _main()
