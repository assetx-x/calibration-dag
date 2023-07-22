import logging
import itertools
import json
import re
from io import StringIO
from datetime import datetime

import pandas as pd
from google.cloud import bigquery, storage
from sqlalchemy import create_engine
from io import BytesIO

from base import credentials_conf
from pipelines.earnings_ingestion.capiq import CapIQClient
from etl_workflow_steps import BaseETLStep, BaseETLStage, StatusType, EarningsCalendarTaskParams
from etl_workflow_steps import compose_main_flow_and_engine_for_task
from pipelines.prices_ingestion.config import get_config
from pipelines.prices_ingestion.etl_workflow_aux_functions import get_data_keys_between_dates
from utils import chunks
from pipelines.earnings_ingestion.yahoo import pull_yahoo_earnings
from numpy import int64


RAW_YAHOO_DATA_LABEL = get_config()["etl_earnings_calendar_results_labels"]["RAW_YAHOO_DATA_LABEL"]
CLEAN_YAHOO_DATA_LABEL = get_config()["etl_earnings_calendar_results_labels"]["CLEAN_YAHOO_DATA_LABEL"]
RAW_CAPIQ_DATA_LABEL = get_config()["etl_earnings_calendar_results_labels"]["RAW_CAPIQ_DATA_LABEL"]
CLEAN_CAPIQ_DATA_LABEL = get_config()["etl_earnings_calendar_results_labels"]["CLEAN_CAPIQ_DATA_LABEL"]
CONSOLIDATED_DATA_LABEL = get_config()["etl_earnings_calendar_results_labels"]["CONSOLIDATED_DATA_LABEL"]

logger = logging.getLogger('luigi-interface')

# Real task flow starts here
class YahooEarningsCalendarDataPuller(BaseETLStep):
    def do_step_action(self, **kwargs):
        start_date = self.task_params.start_dt
        end_date = self.task_params.end_dt
        run_date = self.task_params.run_dt
        forward_horizon = self.task_params.forward_horizon
        backward_horizon = self.task_params.backward_horizon

        storage_client = storage.Client()
        bucket = get_config()["raw_data_pull"]["base_data_bucket"]
        key_prefix = get_config()["raw_data_pull"]["Yahoo"]["EarningsCalendar"]["data_location_prefix"]
        filename_root = get_config()["raw_data_pull"]["Yahoo"]["EarningsCalendar"]["filename_root"]

        date_range = pd.bdate_range(start_date, end_date)
        for day in date_range:
            df = pull_yahoo_earnings(day - backward_horizon, day + forward_horizon)
            buf = StringIO()
            df.to_csv(buf, index=False)

            # pull_timestamp = str(
            #     pd.Timestamp.now().tz_localize('US/Eastern').tz_convert('UTC').asm8.astype(long) / 1000000000)
            pull_timestamp = pd.Timestamp.now(tz="UTC").asm8.astype(int64) / 1000000000
            segment_key = "{0}/{1}/{2}/{3}_{4}.csv".format(
                key_prefix,
                filename_root,
                day.strftime("%Y/%m/%d"),
                filename_root,
                pull_timestamp
            )

            storage_client.bucket(bucket).blob(segment_key).upload_from_string(buf.getvalue())

        return StatusType.Success


class CapIQEarningsCalendarDataPuller(BaseETLStep):
    def do_step_action(self, **kwargs):
        with CapIQClient(credentials_conf["capiq"]["username"], credentials_conf["capiq"]["password"]) as capiq:
            return self._pull_data(capiq)

    def _pull_data(self, capiq):
        start_date = self.task_params.start_dt
        end_date = self.task_params.end_dt
        forward_horizon = self.task_params.forward_horizon
        backward_horizon = self.task_params.backward_horizon

        storage_client = storage.Client()
        bucket = get_config()["raw_data_pull"]["base_data_bucket"]
        key_prefix = get_config()["raw_data_pull"]["CapIQ"]["EarningsCalendar"]["data_location_prefix"]
        filename_root = get_config()["raw_data_pull"]["CapIQ"]["EarningsCalendar"]["filename_root"]

        blob = storage_client.bucket(bucket).blob('chris/universe.txt')
        s = blob.download_as_string().decode('utf-8')
        tickers = [line.strip() for line in s.split('\n') if line.strip()]

        company_names = capiq.request({
            ticker: """=CIQ("%s", "IQ_COMPANY_NAME", 5000)""" % ticker
            for ticker in tickers
        })

        for ticker in company_names:
            company_names[ticker] = company_names[ticker].encode("utf8")

        date_range = pd.bdate_range(start_date, end_date)
        for day in date_range:
            ciq_items = []
            for ticker in tickers:
                ciq_items.append((ticker, """=CIQRANGE("%s", "IQ_EVENT_ID", "%s", "%s", "T55")""" % (
                    ticker,
                    (day - backward_horizon).strftime("%Y-%m-%d"),
                    (day + forward_horizon).strftime("%Y-%m-%d")
                )))
            response = {}
            chks = list(chunks(ciq_items, 500))
            for i, chunk in enumerate(chks):
                logger.info("[%d/%d] Requesting chunk from CapIQ", i, len(chks))
                response.update(capiq.request(dict(chunk), fields=("IQ_EVENT_DATE",)))

            rows = []
            for ticker, data in response.items():
                if not data:
                    continue
                for item in data:
                    if item["IQ_EVENT_DATE"] == "(Invalid Formula Name)":
                        continue
                    dt = datetime.strptime(item["IQ_EVENT_DATE"], "%Y-%m-%d %I:%M %p")
                    rows.append({"ticker": ticker, "datetime": dt, "date": dt.date(), "company_name": company_names[ticker]})

            df = pd.DataFrame(rows)
            buf = StringIO()
            df.to_csv(buf, index=False)

            # pull_timestamp = str(
            #     pd.Timestamp.now().tz_localize('US/Eastern').tz_convert('UTC').asm8.astype(long) / 1000000000)
            pull_timestamp = pd.Timestamp.now(tz="UTC").asm8.astype(int64) / 1000000000
            segment_key = "{0}/{1}/{2}/{3}_{4}.csv".format(
                key_prefix,
                filename_root,
                day.strftime("%Y/%m/%d"),
                filename_root,
                pull_timestamp
            )

            storage_client.bucket(bucket).blob(segment_key).upload_from_string(buf.getvalue())

        return StatusType.Success


class EarningsCalendarS3Retriever(BaseETLStep):
    def __init__(self):
        BaseETLStep.__init__(self)
        self.retrieved_data = None

    def _retrieve_data(self, start_date, end_date, storage_client, bucket, key_prefix, filename_root):
        logger.info("task_params.as_of_dt = %s", self.task_params.as_of_dt)
        keys_to_read = get_data_keys_between_dates(bucket, key_prefix, filename_root, start_date, end_date,
                                                   self.task_params.as_of_dt)
        if keys_to_read:
            all_data = []
            for key in keys_to_read:
                segment_key = key[1].name
                as_of_timestamp = segment_key.split('.csv')[0].split('_')[-1]

                blob = storage_client.bucket(bucket).blob(segment_key)
                fd = BytesIO(blob.download_as_string())

                re_read_segment = pd.read_csv(fd, index_col=None)
                re_read_segment['as_of_timestamp'] = as_of_timestamp
                all_data.append(re_read_segment)
            self.retrieved_data = pd.concat(all_data).reset_index(drop=True)


class YahooEarningsCalendarS3Retriever(EarningsCalendarS3Retriever):
    PROVIDES_FIELDS = [RAW_YAHOO_DATA_LABEL]

    def __init__(self):
        super(YahooEarningsCalendarS3Retriever, self).__init__()

    def do_step_action(self, **kwargs):
        start_date = self.task_params.start_dt
        end_date = self.task_params.end_dt
        storage_client = storage.Client()
        bucket = get_config()["raw_data_pull"]["base_data_bucket"]
        key_prefix = get_config()["raw_data_pull"]["Yahoo"]["EarningsCalendar"]["data_location_prefix"]
        filename_root = get_config()["raw_data_pull"]["Yahoo"]["EarningsCalendar"]["filename_root"]
        self._retrieve_data(start_date, end_date, storage_client, bucket, key_prefix, filename_root)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {RAW_YAHOO_DATA_LABEL: self.retrieved_data}


class CapIQEarningsCalendarS3Retriever(EarningsCalendarS3Retriever):
    PROVIDES_FIELDS = [RAW_CAPIQ_DATA_LABEL]

    def __init__(self):
        super(CapIQEarningsCalendarS3Retriever, self).__init__()

    def do_step_action(self, **kwargs):
        start_date = self.task_params.start_dt
        end_date = self.task_params.end_dt
        logger.info("task_params.start_dt = %s, task_params.end_dt = %s", start_date, end_date)
        logger.info("----------")
        storage_client = storage.Client()
        bucket = get_config()["raw_data_pull"]["base_data_bucket"]
        key_prefix = get_config()["raw_data_pull"]["CapIQ"]["EarningsCalendar"]["data_location_prefix"]
        filename_root = get_config()["raw_data_pull"]["CapIQ"]["EarningsCalendar"]["filename_root"]
        self._retrieve_data(start_date, end_date, storage_client, bucket, key_prefix, filename_root)
        logger.info("here!!!")
        return StatusType.Success

    def _get_additional_step_results(self):
        return {RAW_CAPIQ_DATA_LABEL: self.retrieved_data}


class EarningsCalendarStandaloneQA(BaseETLStep):
    def __init__(self):
        BaseETLStep.__init__(self)
        self.raw_earnings_df = None
        self.clean_earnings_df = None

    def __convert_date(self, dt):
        if isinstance(dt, pd.Timestamp):
            return dt
        else:
            try:
                return pd.Timestamp(dt)
            except:
                return None

    def __is_us_name(self, ticker):
        try:
            return(('.' not in ticker) and not bool(re.search(r'\d', ticker)))
        except:
            return False

    def __give_quarter_id(self, earnings_date):
        try:
            year = earnings_date.year
            if earnings_date.month<=3:
                quarter = 1
            elif earnings_date.month<=6:
                quarter = 2
            elif earnings_date.month<=9:
                quarter = 3
            else:
                quarter = 4
            return("{0}_{1}".format(year, quarter))
        except:
            return None

    def _remove_non_us_names(self, ticker_label="Ticker"):
        cond = self.raw_earnings_df[ticker_label].apply(self.__is_us_name)
        self.raw_earnings_df = self.raw_earnings_df[cond]

    def _convert_date_column(self, date_label='Date'):
        self.raw_earnings_df[date_label] = self.raw_earnings_df[date_label].apply(self.__convert_date)

    def _add_quarter_id(self, date_label='Date'):
        self.raw_earnings_df['Quarter'] = self.raw_earnings_df[date_label].apply(self.__give_quarter_id)

    def _update_with_most_recent_record(self, ticker_label='Ticker', date_label='Date'):
        self.raw_earnings_df = \
            self.raw_earnings_df.sort_values([ticker_label, date_label, 'as_of_timestamp'])\
                .drop_duplicates([ticker_label, date_label], keep='last').reset_index(drop=True)

    def _create_all_ticker_quarters(self, date_label='Date', ticker_label='Ticker'):
        tickers = list(set(self.raw_earnings_df[ticker_label]))
        all_dates = list(set(self.raw_earnings_df[date_label]))
        all_dates.sort()
        start_dt = all_dates[0]
        end_dt = all_dates[-1] - pd.tseries.frequencies.to_offset('1D') + pd.tseries.frequencies.to_offset('1Q')
        quarter_dates = list(pd.bdate_range(start_dt, end_dt, freq='Q'))
        quarters = list(map(self.__give_quarter_id, quarter_dates))
        all_ticker_quarter_df = pd.DataFrame([k for k in itertools.product(tickers, quarters)],
                                             columns=[ticker_label, 'Quarter'])
        return all_ticker_quarter_df

    def _return_expanded_earnings_with_all_quarters(self, all_ticker_quarter_df, ticker_label='Ticker', date_label='Date'):
        aggregation = {
            date_label: {
                "date_last": "last",
                "date_count": "count"
            }
        }
        grouped = self.raw_earnings_df.groupby([ticker_label, 'Quarter']).agg(aggregation)
        grouped.columns = grouped.columns.droplevel(0)
        grouped.reset_index(inplace=True)
        selected_earning_dates = pd.merge(grouped, self.raw_earnings_df, left_on=[ticker_label, 'date_last', 'Quarter'],
                                          right_on=['Ticker', 'Date', 'Quarter'], how="left")
        found_earnings_dates = pd.merge(all_ticker_quarter_df, selected_earning_dates, on=[ticker_label, "Quarter"], how="left")
        return found_earnings_dates.reset_index(drop=True)

    def _backfill_earnings_dates(self, found_earnings_dates, date_label='Date', ticker_label='Ticker'):
        data_fills = []
        for ticker, segment_earnings in found_earnings_dates.groupby(ticker_label):
            possible_dates_to_fill = segment_earnings["date_count"].fillna(method="bfill", limit=1)\
                [pd.isnull(segment_earnings["date_count"])]
            possible_dates_to_fill = possible_dates_to_fill[possible_dates_to_fill>1].index
            for earnings_point_index in possible_dates_to_fill:
                quarter_to_fill_from = segment_earnings.loc[segment_earnings.index[segment_earnings.index.get_loc(earnings_point_index)+1],
                                                            "Quarter"]
                data_to_fill_from = self.raw_earnings_df.query("{0}=='{1}' and Quarter=='{2}'"
                                                               .format(ticker_label, ticker, quarter_to_fill_from))
                selected_record_to_fill = data_to_fill_from.head(1)
                selected_record_to_fill.index = [earnings_point_index]
                data_fills.append(selected_record_to_fill)
        if len(data_fills):
            data_fills = pd.concat(data_fills)
            self.clean_earnings_df = found_earnings_dates.combine_first(data_fills)
        else:
            self.clean_earnings_df = found_earnings_dates
        self.clean_earnings_df.dropna(subset=[ticker_label, date_label], inplace=True)
        self.clean_earnings_df.reset_index(drop=True, inplace=True)

    def _full_cleanup_sequence(self, date_label='Date', ticker_label='Ticker'):
        self.raw_earnings_df.dropna(subset=[ticker_label, date_label], inplace=True)
        self._remove_non_us_names(ticker_label=ticker_label)
        self._convert_date_column(date_label=date_label)
        self._add_quarter_id(date_label=date_label)
        self._update_with_most_recent_record(ticker_label=ticker_label, date_label=date_label)
        all_ticker_quarter_df = self._create_all_ticker_quarters(date_label=date_label, ticker_label=ticker_label)
        found_earnings_dates = self._return_expanded_earnings_with_all_quarters(all_ticker_quarter_df)
        self._backfill_earnings_dates(found_earnings_dates, date_label=date_label, ticker_label=ticker_label)


class YahooEarningsCalendarStandaloneQA(EarningsCalendarStandaloneQA):
    REQUIRES_FIELDS = [RAW_YAHOO_DATA_LABEL]
    PROVIDES_FIELDS = [CLEAN_YAHOO_DATA_LABEL]

    def __init__(self):
        EarningsCalendarStandaloneQA.__init__(self)

    def do_step_action(self, **kwargs):
        self.raw_earnings_df = kwargs[RAW_YAHOO_DATA_LABEL]
        self._full_cleanup_sequence()
        return StatusType.Success

    def _get_additional_step_results(self):
        return {CLEAN_YAHOO_DATA_LABEL: self.clean_earnings_df}


class CapIQEarningsCalendarStandaloneQA(EarningsCalendarStandaloneQA):
    REQUIRES_FIELDS = [RAW_CAPIQ_DATA_LABEL]
    PROVIDES_FIELDS = [CLEAN_CAPIQ_DATA_LABEL]

    def __init__(self):
        EarningsCalendarStandaloneQA.__init__(self)

    def __strip_time_str(self, full_dt):
        try:
            return pd.Timestamp.strftime(pd.Timestamp(full_dt), '%H:%M')
        except:
            return None

    def _normalize_labels(self):
        self.raw_earnings_df.rename(columns={'date': 'Date', 'ticker': "Ticker", 'company_name': 'Company'},
                                    inplace=True)
        self.raw_earnings_df['Time'] = self.raw_earnings_df['datetime'].apply(self.__strip_time_str)

    def do_step_action(self, **kwargs):
        self.raw_earnings_df = kwargs[RAW_CAPIQ_DATA_LABEL]
        self._normalize_labels()
        self._full_cleanup_sequence()
        return StatusType.Success

    def _get_additional_step_results(self):
        return {CLEAN_CAPIQ_DATA_LABEL: self.clean_earnings_df}


class EarningsCalendarConsolidationQA(BaseETLStep):
    REQUIRES_FIELDS = [CLEAN_YAHOO_DATA_LABEL, CLEAN_CAPIQ_DATA_LABEL]
    PROVIDES_FIELDS = [CONSOLIDATED_DATA_LABEL]

    def __init__(self):
        BaseETLStep.__init__(self)
        self.consolidated_earnings_df = None

    def _convert_time_str(self, time_str):
        try:
            converted = pd.Timestamp(time_str)
            if pd.isnull(converted):
                return pd.Timestamp('08:00').time()
            else:
                return converted.time()
        except:
            if "after" in time_str.lower():
                return pd.Timestamp('16:30').time()
            else:
                return pd.Timestamp('08:00').time()

    def _create_earnings_datetime_utc(self, row):
        earnings_date = row['Date']
        try:
            earnings_time = pd.Timedelta(str(row['Earnings_Time']))
            earnings_datetime = (earnings_date + earnings_time).tz_localize('US/Eastern').tz_convert('UTC')
            return earnings_datetime
        except:
            return None

    def do_step_action(self, **kwargs):
        yahoo_data = kwargs[CLEAN_YAHOO_DATA_LABEL]
        capiq_data = kwargs[CLEAN_CAPIQ_DATA_LABEL]
        final_columns = ['Ticker', 'Quarter', 'Company', 'Date', 'Time']
        yahoo_data = yahoo_data[final_columns].set_index(['Ticker', 'Quarter'])
        capiq_data = capiq_data[final_columns].set_index(['Ticker', 'Quarter'])
        consolidated_earnings_df = yahoo_data.combine_first(capiq_data).reset_index()\
            .dropna(subset=['Ticker', 'Date']).reset_index(drop=True)
        consolidated_earnings_df["Earnings_Time"] = consolidated_earnings_df["Time"].apply(self._convert_time_str)
        consolidated_earnings_df["Earnings_Datetime_UTC"] = \
            consolidated_earnings_df[["Date", "Earnings_Time"]].apply(self._create_earnings_datetime_utc, axis=1)
        self.consolidated_earnings_df = consolidated_earnings_df
        return StatusType.Success

    def _get_additional_step_results(self):
        return {CONSOLIDATED_DATA_LABEL: self.consolidated_earnings_df}


class ConsolidatedEarningsS3Writer(BaseETLStep):
    REQUIRES_FIELDS = [CONSOLIDATED_DATA_LABEL]
    PROVIDES_FIELDS = ["manifest"]

    def __init__(self, **kwargs):
        super(ConsolidatedEarningsS3Writer, self).__init__(**kwargs)
        self.segment_manifest_key = None

    def do_step_action(self, **kwargs):
        storage_client = storage.Client()
        bucket = get_config()["raw_data_pull"]["base_data_bucket"]
        key_prefix = get_config()["consolidated_earnings"]["data_location_prefix"]
        filename_root = get_config()["consolidated_earnings"]["filename_root"]
        segment_for_day = kwargs[CONSOLIDATED_DATA_LABEL]

        segment_buffer = StringIO()
        segment_for_day.to_csv(segment_buffer, index=False)
        pull_date = pd.Timestamp.now('US/Eastern')
        pull_timestamp = pull_date.tz_convert('UTC').asm8.astype(int64)/1000000000
        segment_key = "{0}/{1}/{2}/{3}_{4}.csv".format(key_prefix, filename_root, pull_date.strftime("%Y/%m/%d"), filename_root, pull_timestamp)
        self.segment_manifest_key = segment_manifest_key = "{0}/{1}/{2}/{3}_{4}.json".format(key_prefix, filename_root, pull_date.strftime("%Y/%m/%d"),
                                                       filename_root, pull_timestamp)

        bo = storage_client.bucket(bucket)
        bo.blob(segment_key).upload_from_string(segment_buffer.getvalue())
        bo.blob(segment_manifest_key).upload_from_string(
            json.dumps({
                "entries": [
                    {"url": "gs://%s/%s" % (bucket, segment_key), "mandatory": True}
                ],
            })
        )
        return StatusType.Success

    def _get_additional_step_results(self):
        return {"manifest": self.segment_manifest_key}


class ConsolidatedEarningsRedshiftUpdate(BaseETLStep):
    REQUIRES_FIELDS = ["manifest"]
    def do_step_action(self, **kwargs):
        try:
            client = bigquery.Client()

            bucket = get_config()["raw_data_pull"]["base_data_bucket"]
            temp_table_id = get_config()["consolidated_earnings"]["temp_table_id"]
            uri = "gs://%s/%s" % (bucket, kwargs["manifest"].replace(".json", ".csv"))

            # ------------------------------------------------------------------
            # Fill a temporary table with the CSV
            job_config = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField('ticker', 'STRING'),
                    bigquery.SchemaField('quarter', 'STRING'),
                    bigquery.SchemaField('company', 'STRING'),
                    bigquery.SchemaField('date', 'DATE'),
                    bigquery.SchemaField('time', 'STRING'),
                    bigquery.SchemaField('earnings_time', 'STRING'),
                    bigquery.SchemaField('earnings_datetime_utc', 'TIMESTAMP'),
                ],
                skip_leading_rows=1,
                # The source format defaults to CSV, so the line below is optional.
                source_format=bigquery.SourceFormat.CSV,
                # only these rows in the temp table
                write_disposition='WRITE_TRUNCATE',
            )

            logger.info('Loading rows into {}'.format(temp_table_id))
            load_job = client.load_table_from_uri(
                uri, temp_table_id, job_config=job_config
            )
            result = load_job.result()  # Waits for the job to complete.
            logger.info('Loading rows completed')

            # ------------------------------------------------------------------
            # Merge with the earnings table

            dest_table_id = get_config()['consolidated_earnings']['dest_table_id']
            format_params = {
                'earnings_table': dest_table_id,
                'temp_table': temp_table_id,
                'utcnow': datetime.utcnow().isoformat()
            }

            job_config = bigquery.QueryJobConfig(
                priority=bigquery.QueryPriority.BATCH
            )

            sql = """
            UPDATE {earnings_table} dst
            SET as_of_end = '{utcnow}'
            FROM {temp_table} ut
            WHERE
                ut.ticker = dst.ticker
                AND date(ut.date) = date(dst.date)
                AND dst.as_of_end IS NULL
                AND ut.time != dst.time
            """.format(**format_params)

            logger.info('Updating as_of_end in {}'.format(dest_table_id))
            query_job = client.query(sql, job_config=job_config)
            result = query_job.result()  # Waits for the job to complete.
            logger.info('Updating as_of_end completed')

            sql = """
            INSERT INTO {earnings_table} (
                ticker, quarter, company, date, time, earnings_time, earnings_datetime_utc, as_of_start, as_of_end
            )
            SELECT DISTINCT
                ut.ticker, ut.quarter, ut.company, ut.date, ut.time,
                ut.earnings_time,
                cast(ut.earnings_datetime_utc as DATETIME),
                cast('{utcnow}' as DATETIME),
                cast(NULL as DATETIME)
            FROM
                {temp_table} ut
            LEFT JOIN {earnings_table} dst
            ON
                ut.ticker = dst.ticker
                AND date(ut.date) = date(dst.date)
                AND dst.as_of_end IS NULL
            WHERE
                dst.ticker IS NULL
            """.format(**format_params)

            logger.info('Inserting rows into {}'.format(dest_table_id))
            query_job = client.query(sql, job_config=job_config)
            result = query_job.result()  # Waits for the job to complete.
            logger.info('Inserting rows completed')

            return StatusType.Success
        except Exception as ex:
            logger.error(ex)

        return StatusType.Fail

def compose_earnings_calendar_workflow():
    yahoo_pull = YahooEarningsCalendarDataPuller()
    capiq_pull = CapIQEarningsCalendarDataPuller()
    data_pull = BaseETLStage("EarningsDataPull", "Sub-Stage", yahoo_pull, capiq_pull)

    yahoo_retriever = YahooEarningsCalendarS3Retriever()
    capiq_retriever = CapIQEarningsCalendarS3Retriever()
    data_retrieval = BaseETLStage("EarningsDataRetrieval", "Sub-Stage", yahoo_retriever, capiq_retriever)

    yahoo_qa = YahooEarningsCalendarStandaloneQA()
    capiq_qa = CapIQEarningsCalendarStandaloneQA()
    consolidated_qa = EarningsCalendarConsolidationQA()
    data_qa = BaseETLStage("DataQualityCheck", "Sub-Stage", yahoo_qa, capiq_qa, consolidated_qa)

    s3_writer = ConsolidatedEarningsS3Writer()
    redshift_updater = ConsolidatedEarningsRedshiftUpdate()
    data_update = BaseETLStage("DataUpdate", "Sub-Stage", s3_writer, redshift_updater)

    return [data_pull, data_retrieval, data_qa, data_update]


def compose_earnings_calendar_workflow_data_pull_only():
    yahoo_pull = YahooEarningsCalendarDataPuller()
    capiq_pull = CapIQEarningsCalendarDataPuller()
    data_pull_stage = BaseETLStage("EarningsDataPull", "Stage", yahoo_pull, capiq_pull)

    yahoo_retriever = YahooEarningsCalendarS3Retriever()
    capiq_retriever = CapIQEarningsCalendarS3Retriever()
    retrieval_stage = BaseETLStage("EarningsDataRetrieval", "Sub-Stage", yahoo_retriever, capiq_retriever)

    yahoo_qa = YahooEarningsCalendarStandaloneQA()
    capiq_qa = CapIQEarningsCalendarStandaloneQA()
    consolidated_qa = EarningsCalendarConsolidationQA()
    qa_substeps = BaseETLStage("DataConsolidation", "Sub-Stage", yahoo_qa, capiq_qa, consolidated_qa)

    full_qa_stage = BaseETLStage("QA", "Stage", retrieval_stage, qa_substeps)

    s3_writer = ConsolidatedEarningsS3Writer()
    #redshift_updater = ConsolidatedEarningsRedshiftUpdate()
    data_writing_stage = BaseETLStage("DataUpdate", "Stage", s3_writer)
    return [retrieval_stage, full_qa_stage, data_writing_stage]


def compose_earnings_calendar_workflow_data_consolidation_only():
    yahoo_pull = YahooEarningsCalendarDataPuller()
    capiq_pull = CapIQEarningsCalendarDataPuller()
    data_pull_stage = BaseETLStage("EarningsDataPull", "Stage", yahoo_pull, capiq_pull)

    yahoo_retriever = YahooEarningsCalendarS3Retriever()
    capiq_retriever = CapIQEarningsCalendarS3Retriever()
    retrieval_stage = BaseETLStage("EarningsDataRetrieval", "Sub-Stage", yahoo_retriever, capiq_retriever)

    yahoo_qa = YahooEarningsCalendarStandaloneQA()
    capiq_qa = CapIQEarningsCalendarStandaloneQA()
    consolidated_qa = EarningsCalendarConsolidationQA()
    qa_substeps = BaseETLStage("DataConsolidation", "Sub-Stage", yahoo_qa, capiq_qa, consolidated_qa)

    full_qa_stage = BaseETLStage("QA", "Stage", retrieval_stage, qa_substeps)

    s3_writer = ConsolidatedEarningsS3Writer()
    # redshift_updater = ConsolidatedEarningsRedshiftUpdate()
    data_writing_stage = BaseETLStage("DataUpdate", "Stage", s3_writer)
    return [full_qa_stage, data_writing_stage]


def pull_run(start_date, end_date):
    forward_horizon = \
        pd.tseries.frequencies.to_offset(get_config()["consolidated_earnings"]["forward_time_offset"])
    backward_horizon = \
        pd.tseries.frequencies.to_offset(get_config()["consolidated_earnings"]["backward_time_offset"])
    task_params = EarningsCalendarTaskParams(as_of_dt=pd.Timestamp("2018-01-03") ,
                                             run_dt=pd.Timestamp.now(), backward_horizon=backward_horizon,
                                             forward_horizon=forward_horizon,
                                             start_dt=start_date, end_dt=end_date)

    # composing tickdata task for all tickers
    earnings_stages = compose_earnings_calendar_workflow_data_pull_only()
    engine, main_flow = compose_main_flow_and_engine_for_task("Earnings_Calendar_Flow", task_params, *earnings_stages)

    # run flow and get results
    engine.run(main_flow)
    results = engine.storage.fetch_all()
    step_results = list(filter(None, results["step_results"]))
    for k in step_results:
        logger.info("step_results[i]['status_type'] = %s", k['status_type'])
    logger.info("results = %s", results)
    logger.info("Done!")


def consolidation_run(start_date, end_date):
    forward_horizon = \
        pd.tseries.frequencies.to_offset(get_config()["consolidated_earnings"]["forward_time_offset"])
    backward_horizon = \
        pd.tseries.frequencies.to_offset(get_config()["consolidated_earnings"]["backward_time_offset"])
    task_params = EarningsCalendarTaskParams(as_of_dt=pd.Timestamp("2018-01-03") ,
                                             run_dt=pd.Timestamp.now(), backward_horizon=backward_horizon,
                                             forward_horizon=forward_horizon,
                                             start_dt=start_date, end_dt=end_date)

    # composing tickdata task for all tickers
    earnings_stages = compose_earnings_calendar_workflow_data_consolidation_only()
    engine, main_flow = compose_main_flow_and_engine_for_task("Earnings_Calendar_Flow", task_params, *earnings_stages)

    # run flow and get results
    engine.run(main_flow)
    results = engine.storage.fetch_all()
    step_results = list(filter(None, results["step_results"]))
    for k in step_results:
        logger.info("step_results[i]['status_type'] = %s", k['status_type'])
    logger.info("results = %s", results)
    logger.info("Done!")
