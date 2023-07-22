import time
import json
import logging
import requests
import pandas as pd
import numpy as np
import taskflow.types.failure as tff

from os import path
from random import uniform
from zipfile import ZipFile
from io import StringIO, BytesIO
from datetime import datetime
from commonlib.config import get_config
from bs4 import BeautifulSoup
from collections import namedtuple
# from pyspark import SparkContext
from process_info import ProcessInfo

from google.cloud import bigquery, storage
from utils import translate_ticker_name
from pipelines.common.pipeline_util import get_postgres_engine, read_sql_with_retry
from etl_workflow_steps import (BaseETLStep, BaseETLStage, EquitiesETLTaskParameters, StatusType,
                                _STATUS_STEP_NAME_LABEL, StatusNew)
from pipelines.prices_ingestion.etl_workflow_aux_functions import store_data_in_s3, TICKMARKET_FILENAME_REGEX, get_redshift_engine
from pipelines.prices_ingestion.corporate_actions_holder import S3TickDataCorporateActionsHolder


DATA_LAST_ADJUSTMENT_DATES = get_config()["etl_equities_process_labels"]["DATA_LAST_ADJUSTMENT_DATES"]
DATA_LAST_CORPORATE_ACTIONS_DATES = get_config()["etl_equities_process_labels"]["DATA_LAST_CORPORATE_ACTIONS_DATES"]
DATA_LAST_CORPORATE_ACTIONS_TICKDATA_DATES = get_config()["etl_equities_process_labels"]["DATA_LAST_CORPORATE_ACTIONS_TICKDATA_DATES"]

LOCATION_S3_YAHOO_PRICE_LABEL = get_config()["etl_equities_process_labels"]["LOCATION_S3_YAHOO_PRICE_LABEL"]
LOCATION_S3_YAHOO_CORPORATE_ACTION_LABEL = \
    get_config()["etl_equities_process_labels"]["LOCATION_S3_YAHOO_CORPORATE_ACTION_LABEL"]

LOCATION_S3_TICKDATA_PRICE_LABEL = get_config()["etl_equities_process_labels"]["LOCATION_S3_TICKDATA_PRICE_LABEL"]
LOCATION_S3_TICKDATA_CA_FILE_LABEL = get_config()["etl_equities_process_labels"]["LOCATION_S3_TICKDATA_CA_FILE_LABEL"]
LOCATION_S3_TICKDATA_PRICE_MANIFEST_FILE_LABEL = \
    get_config()["etl_equities_process_labels"]["LOCATION_S3_TICKDATA_PRICE_MANIFEST_FILE_LABEL"]

S3_LOCATION_LABEL = get_config()["etl_equities_results_labels"]["S3_LOCATION_LABEL"]

BASE_PAGE_URL_TEMPLATE = "https://finance.yahoo.com/quote/{ticker}/history?p={ticker}"
BASE_INFORMATION_URL_TEMPLATE = ("https://query1.finance.yahoo.com/v7/finance/download/{ticker}?"
                                 "period1={start_dt_ts}&period2={end_dt_ts}&interval=1d&crumb={crumb}")

prices_pull_logger = logging.getLogger("prices_data_pull")
ca_pull_logger = logging.getLogger("ca_data_pull")


class LatestAdjustmentDates(BaseETLStep):
    PROVIDES_FIELDS = [DATA_LAST_ADJUSTMENT_DATES]

    def __init__(self):
        BaseETLStep.__init__(self)
        self.last_adjustment_dates = None

    def do_step_action(self, **kwargs):
        as_of_date = self.task_params.as_of_dt
        table_name = get_config()["redshift"]["equity_price_table"]

        if as_of_date.tzinfo:
            as_of_date = pd.Timestamp(as_of_date).tz_convert("UTC").tz_localize(None)

        sql_engine = get_redshift_engine()
        sql_command = (
            "SELECT ticker, MIN(as_of_start) AS min_as_of "
            "FROM {table_name} "
            "WHERE as_of_start <= '{asof}' AND (as_of_end >= '{asof}' OR as_of_end IS NULL) "
            "GROUP BY ticker").format(asof=as_of_date, table_name=table_name)

        df = read_sql_with_retry(sql_command, sql_engine)

        sql_engine.dispose()
        self.last_adjustment_dates = dict(zip(df["ticker"], df["min_as_of"]))
        return StatusType.Success

    def _get_additional_step_results(self):
        return {DATA_LAST_ADJUSTMENT_DATES: self.last_adjustment_dates}


class YahooPriceAndCorporateActionsDataPull(BaseETLStep):
    PROVIDES_FIELDS = [LOCATION_S3_YAHOO_PRICE_LABEL,
                       LOCATION_S3_YAHOO_CORPORATE_ACTION_LABEL,
                       DATA_LAST_CORPORATE_ACTIONS_DATES]

    def __init__(self):
        BaseETLStep.__init__(self, report_failure_as_warning=True)
        self.price_data_s3_key = None
        self.ca_data_s3_key = None
        self.last_ca_date = None

        self.crumb = None
        self.session = requests.session()
        # FIXME
        self.session.params.update({"f": "sohgpvk3b"})

    def __get_crumb(self, ticker):
        base_page = self.session.get(BASE_PAGE_URL_TEMPLATE.format(**{"ticker": ticker}))
        base_page_soup = BeautifulSoup(base_page.content, "html.parser")

        page_scripts = base_page_soup.find(attrs={"id": "atomic"}).find("body").findAll("script")
        script_with_crumbstore = [s for s in page_scripts if s.text.find("CrumbStore") >= 0][0].text

        crumb_loc = script_with_crumbstore.find("CrumbStore")
        crumb = script_with_crumbstore[crumb_loc-1:crumb_loc+50].split("}")[0].split(":")[2].split('"')[1]
        crumb = crumb.replace("\u002F", "/")  # TODO: what to do with the unicode characters in general???
        self.crumb = crumb

    def get_price_data(self, ticker, run_dt):
        price_data_url = get_config()["raw_data_pull"]["Yahoo"]["PriceAndCA"]["data_pull_URL"] + "&events=history"
        first_dt = get_config()["raw_data_pull"]["Yahoo"]["PriceAndCA"]["defaults"]["start_dt"]

        start_dt_ts = pd.Timestamp(first_dt).tz_localize("US/Eastern").asm8.astype(np.int64)//1000000000
        end_dt_ts = (pd.Timestamp(run_dt) + pd.tseries.frequencies.to_offset("1B")). \
            tz_localize("US/Eastern").asm8.astype(np.int64)//1000000000
        stock_price_url = price_data_url.format(**{"ticker": ticker,
                                                   "start_dt_ts": start_dt_ts,
                                                   "end_dt_ts": end_dt_ts,
                                                   "crumb": self.crumb})

        data_request = self.session.get(stock_price_url)
        price_data = data_request.content

        if not price_data:
            raise ValueError("Could not retrieve price data for ticker {0} on run date {1}. URL utilized was {2}"
                             .format(ticker, run_dt, stock_price_url))

        price_data = pd.read_csv(BytesIO(price_data), index_col=0, na_values="null").dropna(how="any")
        price_data.sort_index(ascending=False, inplace=True)
        price_data = price_data[["Open", "High", "Low", "Close", "Volume", "Adj Close"]]
        first_trade_date = price_data.index[-1]
        last_trade_date = price_data.index[0]
        result = StringIO()
        price_data.to_csv(result)

        return first_trade_date, last_trade_date, result

    def get_corporate_actions(self, ticker, run_dt, first_trade_date, last_trade_date):
        if not first_trade_date or not last_trade_date:
            raise ValueError("Parameters 'first_trade_date' and 'last_trade_date' must be not None. Received {0} "
                             "and {1} respectively".format(first_trade_date, last_trade_date))

        first_dt = get_config()["raw_data_pull"]["Yahoo"]["PriceAndCA"]["defaults"]["start_dt"]
        ca_results = {}
        ca_action_for_final_file = {"div": "DIVIDEND", "split": "SPLIT"}

        for ca_type in ["div", "split"]:
            ca_url_template = get_config()["raw_data_pull"]["Yahoo"]["PriceAndCA"]["data_pull_URL"] \
                + "&events={0}".format(ca_type)

            start_dt_ts = pd.Timestamp(first_dt).tz_localize("US/Eastern").asm8.astype(np.int64)//1000000000
            end_dt_ts = (pd.Timestamp(run_dt) + pd.tseries.frequencies.to_offset("1B")).\
                tz_localize("US/Eastern").asm8.astype(np.int64)//1000000000
            stock_price_url = ca_url_template.format(**{"ticker": ticker,
                                                        "start_dt_ts": start_dt_ts,
                                                        "end_dt_ts": end_dt_ts,
                                                        "crumb": self.crumb})

            data_request = self.session.get(stock_price_url)
            ca_data = data_request.content
            ca_data = pd.read_csv(BytesIO(ca_data))
            ca_data.columns = ["Date", "Value"]
            ca_data["Action"] = ca_action_for_final_file[ca_type]
            ca_results[ca_type] = ca_data

        all_ca_data = pd.concat(ca_results.values(), ignore_index=True).set_index("Date").sort_index(ascending=False)
        last_ca_date = all_ca_data.index.max()
        all_ca_data = all_ca_data.append(pd.DataFrame([[pd.np.NaN, "STARTDATE"], [pd.np.NaN, "ENDDATE"]],
                                                      columns=["Value", "Action"],
                                                      index=[first_trade_date, last_trade_date]))
        all_ca_data.index.name = "Date"
        all_ca_data["Ticker"] = ticker
        all_ca_data = all_ca_data[["Ticker", "Action", "Value"]]
        all_ca_data["Value"] = all_ca_data["Value"].astype(str)
        all_ca_data["Value"] = all_ca_data["Value"].str.replace("/", ":").combine_first(all_ca_data["Value"])

        final_result = StringIO()
        all_ca_data.to_csv(final_result)

        return last_ca_date, final_result

    def do_step_action(self, **kwargs):
        try:
            ticker = translate_ticker_name(self.task_params.ticker, "unified", "yahoo")
            run_dt = self.task_params.run_dt

            self.__get_crumb(ticker)

            first_trade_date, last_trade_date, price_data = self.get_price_data(ticker, run_dt)

            if pd.Timestamp(last_trade_date) < pd.to_datetime(run_dt.date()):
                raise RuntimeError("There was an issue with ticker {0}. Latest dt on pulled data is {1} while run_dt is {2}"
                                   .format(ticker, last_trade_date, run_dt))

            last_ca_date, ca_data = self.get_corporate_actions(ticker, run_dt, first_trade_date, last_trade_date)
            self.last_ca_date = {ticker: last_ca_date}

            price_data_s3_key = store_data_in_s3(
                price_data, get_config()["raw_data_pull"]["base_data_bucket"],
                get_config()["raw_data_pull"]["Yahoo"]["PriceAndCA"]["price_data_location_prefix"],
                ticker, run_dt)

            ca_data_s3_key = store_data_in_s3(
                ca_data, get_config()["raw_data_pull"]["base_data_bucket"],
                get_config()["raw_data_pull"]["Yahoo"]["PriceAndCA"]["ca_data_location_prefix"],
                ticker, run_dt)

            self.price_data_s3_key = price_data_s3_key
            self.ca_data_s3_key = ca_data_s3_key

            self.step_status[S3_LOCATION_LABEL] = {"price": "gs://{0}/{1}".format(*price_data_s3_key),
                                                   "CA": "gs://{0}/{1}".format(*ca_data_s3_key)}
        except:
            self.record_exception_info()
            return StatusType.Fail

        return StatusType.Success

    def _get_additional_step_results(self):
        return {LOCATION_S3_YAHOO_PRICE_LABEL: self.price_data_s3_key,
                LOCATION_S3_YAHOO_CORPORATE_ACTION_LABEL: self.ca_data_s3_key,
                DATA_LAST_CORPORATE_ACTIONS_DATES: self.last_ca_date}


class TickDataPriceDataPull(BaseETLStep):
    PROVIDES_FIELDS = [LOCATION_S3_TICKDATA_PRICE_LABEL, LOCATION_S3_TICKDATA_PRICE_MANIFEST_FILE_LABEL]
    DailyMinuteData = namedtuple("DailyMinuteData", ["ticker", "year", "month", "day", "csv_data"])

    def __init__(self):
        BaseETLStep.__init__(self)
        self.data_s3_key = None
        self.manifest_s3_key = None

    @staticmethod
    def __create_tickdata_url_and_authentication(start_date):
        tick_data_url = get_config()["raw_data_pull"]["TickData"]["Price"]["URL"]\
            .format(**{"start": start_date.strftime("%m/%d/%Y")})
        authentication = (get_config()["raw_data_pull"]["TickData"]["Price"]["username"],
                          get_config()["raw_data_pull"]["TickData"]["Price"]["password"])

        return tick_data_url, authentication

    @staticmethod
    def _daily_data_from_nested_zip(parent_zip_name, nested_zip_bytes, as_of=None):
        if not as_of:
            as_of = datetime.now()

        with ZipFile(BytesIO(nested_zip_bytes)) as f:
            asc_filename = "{0}.asc".format(path.splitext(parent_zip_name)[0])

            if asc_filename not in f.namelist():
                raise RuntimeError("Expected filename %s not found in nested TickMarket zip data, ignoring."
                                   "Found filenames were %s", asc_filename, f.namelist())

            match = TICKMARKET_FILENAME_REGEX.match(parent_zip_name)
            # We need to add the ticker, combine the date and time together, and add the as_of time
            # to the raw data before we ship it off to be saved
            raw_data = pd.read_csv(f.open(asc_filename),
                                   names=["day", "time", "open", "high", "low", "close", "volume"],
                                   header=None,
                                   parse_dates={"cob": ["day", "time"]})

            raw_data["ticker"] = match.group("ticker")
            raw_data["as_of"] = as_of
            csv_data = StringIO()
            raw_data[["open", "high", "low", "close", "volume", "ticker", "cob", "as_of"]].to_csv(csv_data,
                                                                                                  header=False,
                                                                                                  index=False)

            daily_data = TickDataPriceDataPull.DailyMinuteData(match.group("ticker"), match.group("year"),
                                                               match.group("month"), match.group("day"),
                                                               csv_data.getvalue())

            return daily_data

    def get_tickmarket_day(self):
        """
            Retrieve a day of minute level marketdata from TickData tickmarket API

            :return: A generator that yields a DailyMinuteData with the ticker, year, month, and day (as strings,
                     as seen in the source data) and the CSV data associated with it for the day's ticks for each
                     item found in the data
        """
        day = self.task_params.run_dt
        as_of = self.task_params.as_of_dt

        start_date = pd.to_datetime(day)
        tickmarket_url, auth = self.__create_tickdata_url_and_authentication(start_date)

        prices_pull_logger.info("Requesting minute prices for all tickers from URL %s", tickmarket_url)
        response = requests.get(tickmarket_url, auth=auth)
        response.raise_for_status()

        with ZipFile(BytesIO(response.content)) as top_zip:
            prices_pull_logger.info("TickData zipped price file received for %s, contains "
                                    "%d files", start_date, len(top_zip.namelist()))
            for ticker_zip in top_zip.namelist():
                # log.debug("Processing TickMarket nested zip data with name %s", ticker_zip)
                match = TICKMARKET_FILENAME_REGEX.match(ticker_zip)

                if not match:
                    pass
                    # log.error("Could not recognize nested filename in zipped daily TickData: %s, ignoring",
                    # ticker_zip)
                else:
                    daily_data = self._daily_data_from_nested_zip(ticker_zip, top_zip.read(ticker_zip), as_of)
                    # log.debug("Found data for %s : %s-%s-%s", daily_data.ticker, daily_data.year,
                    #              daily_data.month, daily_data.day)
                    yield daily_data

    def store_manifest_for_redshift_load(self):
        manifest_content = {"entries": list(map(lambda x: {"url": "gs://{0}/{1}".format(*x), "mandatory": True},
                                           self.data_s3_key))}
        manifest_buffer = StringIO(json.dumps(manifest_content))

        s3_manifest_key = store_data_in_s3(manifest_buffer, get_config()["raw_data_pull"]["base_data_bucket"],
                                           get_config()["raw_data_pull"]["TickData"]["Price"]["data_location_prefix"],
                                           ".manifest", self.task_params.run_dt, extension="json")

        self.step_status[S3_LOCATION_LABEL] = "gs://{0}/{1}".format(*s3_manifest_key)
        self.manifest_s3_key = s3_manifest_key

    def do_step_action(self, **kwargs):
        # TODO: can this be parallelized with eventlet to accelerate the upload time?
        run_dt = self.task_params.run_dt
        self.data_s3_key = []

        count = 1
        for daily_data in self.get_tickmarket_day():
            prices_pull_logger.info("%d Saving minute prices for ticker %s to S3. Content length = %d",
                                    count, daily_data.ticker, len(daily_data.csv_data))
            count += 1
            csv_buffer = StringIO()
            csv_buffer.write(daily_data.csv_data)

            data_s3_key = store_data_in_s3(csv_buffer, get_config()["raw_data_pull"]["base_data_bucket"],
                                           get_config()["raw_data_pull"]["TickData"]["Price"]["data_location_prefix"],
                                           daily_data.ticker, run_dt)

            self.data_s3_key.append(data_s3_key)
        self.store_manifest_for_redshift_load()

        return StatusType.Success

    def _get_additional_step_results(self):
        return {LOCATION_S3_TICKDATA_PRICE_LABEL: self.data_s3_key,
                LOCATION_S3_TICKDATA_PRICE_MANIFEST_FILE_LABEL: self.manifest_s3_key}


def setup_mock_s3():
    from moto import mock_s3
    m = mock_s3()
    m.start()
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket= get_config()["raw_data_pull"]["base_data_bucket"])
    return m


def extract_data_and_deposit_on_s3(zipdata, ticker, run_dt, asof_dt):
    try:
        start = str(pd.Timestamp.now("US/Eastern").tz_convert("UTC").tz_localize(None))
        asof_dt = asof_dt or pd.Timestamp.now()
        filename = "{0}_{1}".format(ticker, run_dt.strftime("%Y_%m_%d_X_O.zip"))
        with ZipFile(BytesIO(zipdata)) as zipfile:
            try:
                inner_zip_data = zipfile.read(filename)
            except KeyError:
                inner_zip_data = None
        result = (ticker, None)
        if inner_zip_data:
            with ZipFile(BytesIO(inner_zip_data)) as f:
                asc_filename = "{0}.asc".format(path.splitext(filename)[0])
                raw_data = pd.read_csv(f.open(asc_filename),
                                       names=["day", "time", "open", "high", "low", "close", "volume"],
                                       header=None,
                                       parse_dates={"cob": ["day", "time"]})
                raw_data["ticker"] = ticker
                raw_data["as_of"] = asof_dt
                csv_data = StringIO()
                raw_data[["open", "high", "low", "close", "volume", "ticker", "cob", "as_of"]].to_csv(csv_data,
                                                                                                      header=False,
                                                                                                      index=False)
                m = setup_mock_s3()
                data_s3_key = store_data_in_s3(csv_data, get_config()["raw_data_pull"]["base_data_bucket"],
                                               get_config()["raw_data_pull"]["TickData"]["Price"]["data_location_prefix"],
                                               ticker, run_dt)
                m.stop()
                result = (ticker, data_s3_key)
    except BaseException as e:
        result = (ticker, tff.Failure().to_dict())
    end = str(pd.Timestamp.now("US/Eastern").tz_convert("UTC").tz_localize(None))
    print("Processed {0}".format(ticker))
    pinfo = None #ProcessInfo.retrieve_all_process_info()
    return (result[0], result[1], start, end, pinfo)


class TickDataPriceDataPullSpark(BaseETLStep):
    PROVIDES_FIELDS = [LOCATION_S3_TICKDATA_PRICE_LABEL, LOCATION_S3_TICKDATA_PRICE_MANIFEST_FILE_LABEL,
                       "individual_tickdata_price_load_stepresults"]

    def __init__(self, tickers=None):
        BaseETLStep.__init__(self)
        self.data_s3_key = None
        self.manifest_s3_key = None
        self.tickers = tickers or []
        self.individual_tickdata_price_load_stepresults = None

    @staticmethod
    def __create_tickdata_url_and_authentication(start_date):
        tick_data_url = get_config()["raw_data_pull"]["TickData"]["Price"]["URL"]\
            .format(**{"start": start_date.strftime("%m/%d/%Y")})
        authentication = (get_config()["raw_data_pull"]["TickData"]["Price"]["username"],
                          get_config()["raw_data_pull"]["TickData"]["Price"]["password"])

        return tick_data_url, authentication

    def __collect_data_from_tickdata(self):
        tickmarket_url, auth = self.__create_tickdata_url_and_authentication(pd.to_datetime(self.task_params.run_dt))
        response = requests.get(tickmarket_url, auth=auth)
        response.raise_for_status()
        zipdata = ZipFile(BytesIO(response.content))
        self.tickers = self.tickers or \
            sorted([str(TICKMARKET_FILENAME_REGEX.match(k.filename).group("ticker")) for k in zipdata.filelist])
        return  response.content

    def retrieve_and_unpack_all_data(self):
        zipdata = self.__collect_data_from_tickdata()
        sc = SparkContext(master="local[{0}]".format(min(len(self.tickers), 24)), appName="tickdata_pull")
        try:
            zipdata_broadcast = sc.broadcast(zipdata)

            data_to_extract = [{"ticker":k, "run_dt":self.task_params.run_dt, "asof_dt":self.task_params.as_of_dt}
                               for k in self.tickers]
            data_to_extract_rdd = sc.parallelize(data_to_extract)
            results_rdd = data_to_extract_rdd.map(lambda x:extract_data_and_deposit_on_s3(zipdata_broadcast.value, **x))
            results = results_rdd.collect()
        finally:
            sc.stop()
        return results

    def store_manifest_for_redshift_load(self):
        m = setup_mock_s3()
        manifest_content = {"entries": map(lambda x: {"url": "gs://{0}/{1}".format(*x), "mandatory": True},
                                           self.data_s3_key)}
        manifest_buffer = StringIO(json.dumps(manifest_content))

        s3_manifest_key = store_data_in_s3(manifest_buffer, get_config()["raw_data_pull"]["base_data_bucket"],
                                           get_config()["raw_data_pull"]["TickData"]["Price"]["data_location_prefix"],
                                           ".manifest", self.task_params.run_dt, extension="json")

        self.step_status[S3_LOCATION_LABEL] = "gs://{0}/{1}".format(*s3_manifest_key)
        self.manifest_s3_key = s3_manifest_key
        m.stop()

    def form_individual_step_results(self, pull_results):
        individual_step_results = []
        current_time = str(pd.Timestamp.now("US/Eastern").tz_convert("UTC").tz_localize(None))
        for res in pull_results:
            new_step_res = StatusNew(self.step_status[_STATUS_STEP_NAME_LABEL])
            status = StatusType.Success if isinstance(res[1], tuple) else StatusType.Fail
            individual_task_params = type(self.task_params)(res[0], *self.task_params[1:])
            result_info = {"task_start": res[2], "task_end": res[3], "process_info":res[4]}
            if not isinstance(res[1], tuple):
                result_info["error"] = res[1]
            new_step_res.standard_set_step_status(status, individual_task_params, **result_info)
            individual_step_results.append(new_step_res)
        return individual_step_results

    def __generate_pull_results_for_global_failure(self):
        global_error = tff.Failure().to_dict()
        end_time = str(pd.Timestamp.now("US/Eastern").tz_convert("UTC").tz_localize(None))
        pinfo = ProcessInfo.retrieve_all_process_info()
        pull_results = [(k, global_error, self.start_step_time, end_time, pinfo) for k in self.tickers]
        return pull_results

    def do_step_action(self, **kwargs):
        try:
            pull_results = self.retrieve_and_unpack_all_data()
        except BaseException as e:
            pull_results = self.__generate_pull_results_for_global_failure()
        self.individual_tickdata_price_load_stepresults = self.form_individual_step_results(pull_results)
        self.data_s3_key = [k[1] for k in pull_results if isinstance(k[1], tuple)]
        self.store_manifest_for_redshift_load()
        return StatusType.Success if len(self.data_s3_key) else StatusType.Fail

    def _get_additional_step_results(self):
        return {LOCATION_S3_TICKDATA_PRICE_LABEL: self.data_s3_key,
                LOCATION_S3_TICKDATA_PRICE_MANIFEST_FILE_LABEL: self.manifest_s3_key,
                self.__class__.PROVIDES_FIELDS[2]:self.individual_tickdata_price_load_stepresults}


class TickDataCorporateActionsPull(BaseETLStep):
    PROVIDES_FIELDS = [LOCATION_S3_TICKDATA_CA_FILE_LABEL, DATA_LAST_CORPORATE_ACTIONS_TICKDATA_DATES]

    def __init__(self):
        BaseETLStep.__init__(self)
        self.ca_data_s3_key = None
        self.session = requests.session()
        self.last_ca_date = None

    @staticmethod
    def add_ticker_to_pending_security_changes_table(dcm_security_id, ticker, run_dt):
        pending_security_changes_table = get_config()["security_master"]["pending_security_changes_table"]
        sql_statement = "INSERT INTO {table_name} VALUES ({dcm_security_id}, '{ticker}', '{run_dt}')".format(
            table_name=pending_security_changes_table, dcm_security_id=dcm_security_id, ticker=ticker, run_dt=run_dt)

        sa_engine = get_postgres_engine()
        with sa_engine.begin() as connection:
            connection.execute(sql_statement)
        sa_engine.dispose()

    @staticmethod
    def find_security_changes(ca_data_normalized, run_dt, ticker):
        security_universe_table = get_config()["security_master"]["security_universe_table"]

        df_temp = ca_data_normalized[['COMPANY ID', 'SYMBOL']].reset_index().copy()
        footer_date = df_temp.loc[df_temp['ACTION'] == 'FOOTER', 'ACTION DATE'].iloc[0]
        dcm_security_id = df_temp['COMPANY ID'].iloc[0]

        # Check if security has been delisted [FOOTER ACTION DATE < run_date in ETLParameters]
        if footer_date < run_dt:
            ca_pull_logger.warn("Ticker {0}: FOOTER '{1}' < run_dt '{2}'. It might be delisted/acquired.".format(
                ticker, footer_date.date(), run_dt.date()))
            TickDataCorporateActionsPull.add_ticker_to_pending_security_changes_table(dcm_security_id, ticker, run_dt)

        # Read current security_universe table from Redshift for dcm_security_id
        # We need to extract 'latest_update' so to check what CA should be applied to update security_universe
        sql_engine = get_redshift_engine()
        query = "SELECT * FROM %s WHERE dcm_security_id=%d" % (security_universe_table, dcm_security_id)
        security_universe = read_sql_with_retry(query, sql_engine)

        if len(security_universe):
            latest_update_of_security = security_universe['latest_update'].iloc[0]
        else:
            # Need to get this latest update date timestamp reviewed
            latest_update_of_security = pd.Timestamp("2020-01-02 14:00:00")
        sql_engine.dispose()

        # Mask to search events that affect Security Master and have occured at run_dt
        ca_columns_affecting_sm = ['TICKER SYMBOL CHANGE', 'CUSIP CHANGE', 'CHANGE IN LISTING', 'NAME CHANGE']
        mask = (df_temp['ACTION'].isin(ca_columns_affecting_sm)) & (df_temp['ACTION DATE'] > latest_update_of_security)

        # Check if security have any CA on run_date that could affect Security Master
        if not df_temp.loc[mask].empty:
            TickDataCorporateActionsPull.add_ticker_to_pending_security_changes_table(dcm_security_id, ticker, run_dt)

    def do_step_action(self, **kwargs):
        ticker = translate_ticker_name(self.task_params.ticker, "unified", "tickdata")
        run_dt = self.task_params.run_dt

        ca_url_template = get_config()["raw_data_pull"]["TickData"]["CA"]["data_pull_URL"]
        url_replacement_dict = {"ticker": ticker}
        url_replacement_dict.update(get_config()["raw_data_pull"]["TickData"]["CA"]["defaults"])
        url_replacement_dict["end"] = run_dt.strftime("%m/%d/%Y")
        ca_url = ca_url_template.format(**url_replacement_dict)

        authentication = (get_config()["raw_data_pull"]["TickData"]["Price"]["username"],
                          get_config()["raw_data_pull"]["TickData"]["Price"]["password"])

        for pull_attempt in range(3, 0, -1):
            data_request = self.session.get(ca_url, auth=authentication, verify=False)
            if data_request.ok:
                break
            if pull_attempt == 1:
                raise RuntimeError("Error pulling corporate actions for ticker {0}. Error is {1}"
                                   .format(ticker, data_request.content))
            time.sleep(uniform(0.15, 0.3))  # random back-off between 150 and 300 ms

        ca_data = BytesIO(data_request.content)
        ca_data_normalized = S3TickDataCorporateActionsHolder.parse_tickdata_data(ca_data)[0]

        adjustment_cas = pd.concat([ca_data_normalized["RATIO"].dropna().reset_index().drop_duplicates()
                                   .rename(columns={"RATIO": "ADJUSTMENT FACTOR"}),
                                    ca_data_normalized["ADJUSTMENT FACTOR"].dropna().reset_index().drop_duplicates()])

        last_ca_date = adjustment_cas["ACTION DATE"].max().strftime("%Y-%m-%d") if len(adjustment_cas) \
            else ca_data_normalized.ix["HEADER", :].index[0].strftime("%Y-%m-%d")
        self.last_ca_date = {ticker: last_ca_date}

        ca_data = BytesIO(data_request.content)
        ca_data_s3_key = store_data_in_s3(
            ca_data, get_config()["raw_data_pull"]["base_data_bucket"],
            get_config()["raw_data_pull"]["TickData"]["CA"]["data_location_prefix"],
            ticker, run_dt)

        self.find_security_changes(ca_data_normalized, run_dt, ticker)
        self.step_status[S3_LOCATION_LABEL] = "gs://{0}/{1}".format(*ca_data_s3_key)
        self.ca_data_s3_key = ca_data_s3_key
        return StatusType.Success

    def _get_additional_step_results(self):
        return {LOCATION_S3_TICKDATA_CA_FILE_LABEL: self.ca_data_s3_key,
                DATA_LAST_CORPORATE_ACTIONS_TICKDATA_DATES: self.last_ca_date}


class TickDataPriceDataCopyIntoRedshift(BaseETLStep):
    REQUIRES_FIELDS = [LOCATION_S3_TICKDATA_PRICE_MANIFEST_FILE_LABEL]

    def __init__(self):
        BaseETLStep.__init__(self)
        self.loaded_into_redshift = None
        self.task_params = None

    def do_step_action(self, **kwargs):
        self.task_params = EquitiesETLTaskParameters(ticker="*", as_of_dt=self.task_params.as_of_dt,
                                                     run_dt=self.task_params.run_dt,
                                                     start_dt=self.task_params.start_dt,
                                                     end_dt=self.task_params.end_dt)

        bucket, manifest_key = kwargs[LOCATION_S3_TICKDATA_PRICE_MANIFEST_FILE_LABEL]
        # manifest_key = ("dcm-data-test", "chris/tickdata/%s/manifest.json" %
        # self.task_params.run_dt.strftime("%Y/%m/%d"))

        # ------------------------------------------------------------------
        # Load up the JSON from storage
        # {
        #   "entries": [
        #     {
        #       "mandatory": true,
        #       "url": <url>
        #     },
        storage_client = storage.Client()
        manifest = json.loads(
            storage_client.bucket(bucket).blob(manifest_key).download_as_string()
        )
        uris = [x['url'] for x in manifest['entries']]
        prices_pull_logger.info("Total gs uris to be loaded into bigquery: {}".format(len(uris)))

        # ------------------------------------------------------------------

        table_for_equities = get_config()["redshift"]["raw_tickdata_equity_price_table"]
        temp_table_id = '{}_staging'.format(table_for_equities)

        client = bigquery.Client()

        # ------------------------------------------------------------------
        # Fill a temporary table with the CSV

        job_config = bigquery.LoadJobConfig(
            # no COPY...LIKE, so have to specify
            schema=[
                    bigquery.SchemaField('open', 'FLOAT'),
                    bigquery.SchemaField('high', 'FLOAT'),
                    bigquery.SchemaField('low', 'FLOAT'),
                    bigquery.SchemaField('close', 'FLOAT'),
                    bigquery.SchemaField('volume', 'FLOAT'),
                    bigquery.SchemaField('ticker', 'STRING'),
                    bigquery.SchemaField('cob', 'DATETIME'),
                    bigquery.SchemaField('as_of', 'DATETIME'),
                    # missing in source CSV, but needed in destination table
                    bigquery.SchemaField('import_time', 'DATETIME'),
            ],
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            # commenting WRITE_TRUNCATE option to not lose previous chunk data
            # before loading new chunk data in the loop below for load_table_from_uri
            # write_disposition='WRITE_TRUNCATE',
            # allow the missing last column
            allow_jagged_rows=True
            )

        temp_table_id_uri = '{}.{}'.format(get_config()['gcp']['project_id'],
                                          temp_table_id)
        prices_pull_logger.info('Loading rows into {}'.format(temp_table_id))

        chunk_size = 10000
        all_uris_chunks = [uris[i:i+chunk_size] for i in range(0, len(uris), chunk_size)]
        for uris_chunk in all_uris_chunks:
            prices_pull_logger.info("Loading chunk of size: {}".format(len(uris_chunk)))

            load_job = client.load_table_from_uri(
                uris_chunk, temp_table_id_uri, job_config=job_config
            )
            result = load_job.result()  # Waits for the job to complete.
            prices_pull_logger.info("Load job result: {}".format(result.state))
        prices_pull_logger.info('Loading rows from gs to {} completed!'.format(temp_table_id))


        # ------------------------------------------------------------------
        # Only insert new r(ticker, cob, as_of)aw price data if we don't already have it

        sql = """
        INSERT INTO {table_name}
            SELECT DISTINCT s.open, s.high,  s.low, s.close, s.volume, s.ticker, s.cob,
                IFNULL(s.as_of, CURRENT_DATETIME()) AS as_of,
                IFNULL(s.import_time, CURRENT_DATETIME()) AS import_time,
                FARM_FINGERPRINT(s.ticker) as farm_fingerprint
            FROM {temp_table} s
            LEFT JOIN {table_name} r
                   ON (s.cob = r.cob AND s.ticker = r.ticker)
            WHERE r.cob IS NULL
        """.format(table_name=table_for_equities, temp_table=temp_table_id)

        job_config = bigquery.QueryJobConfig(
            priority=bigquery.QueryPriority.BATCH
        )

        prices_pull_logger.info('Updating {}'.format(table_for_equities))
        query_job = client.query(sql, job_config=job_config)
        result = query_job.result()  # Waits for the job to complete.
        prices_pull_logger.info('Updating completed')

        # --------------------------------------------------------------------------
        # Delete the temporary staging table, created while load_table_from_uri above
        prices_pull_logger.info('Deleting table {}'.format(temp_table_id_uri))
        client.delete_table(temp_table_id_uri, not_found_ok=True)
        prices_pull_logger.info('Deleted table {}!'.format(temp_table_id_uri))

        return StatusType.Success


def create_data_pull_stage():
    yahoo_price_and_ca_data_puller = YahooPriceAndCorporateActionsDataPull()
    tickdata_ca_data_puller = TickDataCorporateActionsPull()
    yahoo_pull_sub_stage = BaseETLStage("yahoo_pull", "Sub-Stage", yahoo_price_and_ca_data_puller)
    tickdata_ca_sub_stage = BaseETLStage("tickdata_ca_pull", "Sub-Stage", tickdata_ca_data_puller)
    price_ca_pull_stage = BaseETLStage("DataPull", "Stage", yahoo_pull_sub_stage, tickdata_ca_sub_stage)
    return price_ca_pull_stage


def create_full_tickdata_pull_stage():
    # latest_dates = LatestAdjustmentDates()
    tickdata_puller = TickDataPriceDataPull()
    redshift_copier = TickDataPriceDataCopyIntoRedshift()

    # tickdata_pull_sub_stage = BaseETLStage("tickdata_pull", "Sub-Stage", latest_dates, tickdata_puller, redshift_copier)
    tickdata_pull_sub_stage = BaseETLStage("tickdata_pull", "Sub-Stage", tickdata_puller, redshift_copier)
    tickdata_pull_stage = BaseETLStage("DataPull", "Stage", tickdata_pull_sub_stage)
    return tickdata_pull_stage


def _main():
    from etl_workflow_steps import compose_main_flow_and_engine_for_task

    # ticker = 'AAPL'
    # start_dt = pd.Timestamp('2000-01-03')
    # end_dt = pd.Timestamp('2019-03-08')
    # run_dt = end_dt
    # as_of_dt = pd.Timestamp('2019-03-12')

    # ticker = 'ITG'  # DELISTED
    # start_dt = pd.Timestamp('2000-01-03')
    # end_dt = pd.Timestamp('2019-03-11')
    # run_dt = end_dt
    # as_of_dt = pd.Timestamp('2019-03-13')

    # ticker = 'BRKB'  # CUSIP CHANGE
    # start_dt = pd.Timestamp('2000-01-03')
    # end_dt = pd.Timestamp('2010-01-21')
    # run_dt = end_dt
    # as_of_dt = pd.Timestamp('2019-03-12')

    # ticker = 'AMN'  # TICKER SYMBOL CHANGE
    # start_dt = pd.Timestamp('2000-01-03')
    # end_dt = pd.Timestamp('2016-10-27')
    # run_dt = end_dt
    # as_of_dt = pd.Timestamp('2019-03-12')

    ticker = 'AAPL'
    start_dt = pd.Timestamp('2000-01-03')
    end_dt = pd.Timestamp('2019-03-18')
    run_dt = pd.Timestamp('2019-03-18')
    as_of_dt = pd.Timestamp('2019-03-20')

    task_params = EquitiesETLTaskParameters(ticker=ticker, start_dt=start_dt, end_dt=end_dt,
                                            run_dt=run_dt, as_of_dt=as_of_dt)

    # Composing yahoo tasks for a single ticker
    data_pull = create_data_pull_stage()
    # engine, main_flow = compose_main_flow_and_engine_for_task("Yahoo_sample_flow", task_params, data_pull)

    # Composing tickdata task for all tickers
    # tickdata_pull = create_full_tickdata_pull_stage()
    engine, main_flow = compose_main_flow_and_engine_for_task("Tickdata_sample_flow", task_params, data_pull)

    # Run flow and get results
    engine.run(main_flow)
    results = engine.storage.fetch_all()
    step_results = filter(None, results["step_results"])

    for k in step_results:
        if isinstance(k, list):
            for l in k:
                print(l["status_type"])
        else:
            print(k["status_type"])
    print('Done!!!')


if __name__ == "__main__":
    _main()



