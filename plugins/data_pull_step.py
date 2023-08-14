from core_classes import GCPReader,download_yahoo_data,DataReaderClass
from market_timeline import marketTimeline
from abc import ABC,ABCMeta, abstractmethod
from google.cloud import storage
from enum import Enum
import pandas as pd
import io
import os
from pandas_datareader import data as web
import yfinance as yf
#import fix_yahoo_finance as fyf
import sys
from fredapi import Fred
import pandas
from google.cloud import bigquery
import gc
import numpy as np
from datetime import datetime
from core_classes import construct_required_path,construct_destination_path


parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
plugins_folder = os.path.join(parent_directory, "plugins")
data_processing_folder = os.path.join(plugins_folder, "data_processing")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(data_processing_folder,'dcm-prod.json')
os.environ['GCS_BUCKET'] = 'dcm-prod-ba2f-us-dcm-data-test'
JUMP_DATES_CSV = os.path.join(data_processing_folder,'intervals_for_jump.csv')
current_date = datetime.now().date()
RUN_DATE = '2023-06-28' #current_date.strftime('%Y-%m-%d')


class S3SecurityMasterReader(GCPReader):
    '''

    Downloads security master from GCP location

    '''

    PROVIDES_FIELDS = ["security_master"]
    def __init__(self, bucket, key):
        self.data = None
        GCPReader.__init__(self, bucket, key)

    def _post_process_pulled_data(self, data, **kwargs):
        data.drop(data.columns[self.index_col], axis=1, inplace=True)
        data["dcm_security_id"] = data["dcm_security_id"].astype(int)

        """if self.task_params.run_mode==TaskflowPipelineRunMode.Test:
            query = "select ticker from whitelist where run_dt in ('{0}') and as_of_end is null"\
                .format(self.task_params.universe_dt)
            universe_tickers = list(pd.read_sql(query, get_redshift_engine())["ticker"])
            data = data[data["ticker"].isin(universe_tickers)].reset_index(drop=True)"""
        return data


    @classmethod
    def get_default_config(cls):
        # import ipdb;
        # ipdb.set_trace(context=50)
        return {"bucket": "dcm-data-temp", "key": "jack/security_master.csv"}


class S3GANUniverseReader(GCPReader):
    def __init__(self, bucket, key):
        self.data = None
        GCPReader.__init__(self, bucket, key, )

    def _post_process_pulled_data(self, data, **kwargs):
        data["dcm_security_id"] = data["dcm_security_id"].astype(int)
        return data


class S3IndustryMappingReader(GCPReader):
    '''

    Downloads industry mapping from GCP location

    '''

    #REQUIRES_FIELDS = ["security_master"]
    #PROVIDES_FIELDS = ["industry_map"]
    def __init__(self, bucket, key):
        self.data = None
        GCPReader.__init__(self, bucket, key)

    def _post_process_pulled_data(self, data, **kwargs):
        data.drop(data.columns[self.index_col], axis=1, inplace=True)
        sec_master = kwargs["security_master"]
        data["dcm_security_id"] = data["dcm_security_id"].astype(int)
        data = data[data["dcm_security_id"].isin(list(sec_master["dcm_security_id"]))]\
            .reset_index(drop=True)
        return data


class S3EconTransformationReader(GCPReader):
    '''

    Downloads security master from GCP location

    '''

    # PROVIDES_FIELDS = ["econ_transformation"]
    def __init__(self, sector_etfs, bucket, key):
        self.data = None
        self.sector_etfs = sector_etfs
        GCPReader.__init__(self, bucket, key)

    def _pull_data(self, **kwargs):
        bucket = self.gcp_client.bucket(self.bucket)
        blob = bucket.blob(self.key)
        file_content = io.BytesIO(blob.download_as_string())
        data = pd.read_csv(file_content)
        transform_dict = {}
        for ticker in self.sector_etfs:
            close_col = "{0}_close".format(ticker)
            vol_col = "{0}_volume".format(ticker)
            transform_dict[close_col] = 5
            transform_dict[vol_col] = 5
        df = pd.DataFrame(list(transform_dict.values()), index=list(transform_dict.keys())).reset_index()
        df = df.rename(columns={"index": data.columns[0], 0: data.columns[1]})
        data = pd.concat([data, df], axis=0, ignore_index=True)

        return data

    def _post_process_pulled_data(self, data, **kwargs):
        return data

    def _get_return_types_for_permutations(self, data, **kwargs):
        pass



    @classmethod
    def get_default_config(cls):
        return {"sector_etfs": ["SPY", "MDY", "EWG", "EWH", "EWJ", "EWW", "EWS", "EWU"],
                "bucket": "dcm-data-temp",
                "key": "jack/econ_transform_definitions.csv"}


class YahooDailyPriceReader(DataReaderClass):
    # PROVIDES_FIELDS = ["yahoo_daily_price_data"]
    def __init__(self, sector_etfs, start_date, end_date):
        self.start_date = pd.Timestamp(start_date)
        self.end_date = pd.Timestamp(end_date)
        self.data = None
        self.sector_etfs = sector_etfs
        # DataReader.__init__(self, "date", "dcm_security_id", "US/Eastern", "UTC", False, False)

    def _prepare_to_pull_data(self, **kwargs):
        self.start_date = self.start_date  # if pd.notnull(self.start_date) else self.task_params.start_dt
        self.end_date = self.end_date  # if pd.notnull(self.end_date) else self.task_params.end_dt

    def _get_data_lineage(self):
        pass

    def _pull_data(self, **kwargs):
        etf_prices = download_yahoo_data(self.sector_etfs, self.start_date, self.end_date)
        return etf_prices

    def do_step_action(self, **kwargs):
        self.data = self._pull_data()
        return self.data


class S3RussellComponentReader(DataReaderClass):
    '''

    Downloads industry mapping from GCP location

    '''

    REQUIRES_FIELDS = ["security_master"]
    #PROVIDES_FIELDS = ["russell_components"]
    def __init__(self, bucket,key, **kwargs):
        self.data = None
        self.r1k = None
        self.r3k_key = kwargs['r3k_key']
        self.r1k_key = kwargs['r1k_key']
        DataReaderClass.__init__(self,bucket, key)

    def do_step_action(self, **kwargs):
        sec_master = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        s3_client = storage.Client()
        bucket = s3_client.bucket(self.bucket)
        blob = bucket.blob(self.r3k_key)
        file_content = io.BytesIO(blob.download_as_string())
        r3k = pd.read_csv(file_content, index_col=0)
        r3k.columns = r3k.columns.str.lower()
        r3k['date'] = pd.to_datetime(r3k['date'])
        r3k[['wt-r1', 'wt-r1g', 'wt-r1v']] = r3k[['wt-r1', 'wt-r1g', 'wt-r1v']].fillna(0)

        blob = bucket.blob(self.r1k_key)
        file_content = io.BytesIO(blob.download_as_string())
        r1k = pd.read_csv(file_content, index_col=0)
        r1k.columns = r1k.columns.str.lower()
        r1k['date'] = pd.to_datetime(r1k['date'])

        r1k["cusip"] = r1k["cusip"].astype(str)
        r3k["cusip"] = r3k["cusip"].astype(str)
        r1k["dcm_security_id"] = r1k["dcm_security_id"].astype(int)
        df = pd.merge(r3k, r1k, how='left', on=['date', 'cusip'])
        df = df.drop_duplicates()
        df = df[df["dcm_security_id"].isin(list(sec_master["dcm_security_id"]))].reset_index(drop=True)
        df = df.drop_duplicates(["date", "ticker"], keep="last").reset_index(drop=True)
        self.data = df
        return self.data

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self, **kwargs):
        pass

    def _pull_data(self, **kwargs):
        pass

    def _post_process_pulled_data(self, data, **kwargs):
        pass

class S3RawQuandlDataReader(GCPReader):
    '''

    Downloads raw quandl data from GCP location

    '''
    REQUIRES_FIELDS = ["security_master"]
    #PROVIDES_FIELDS = ["raw_quandl_data"]
    def __init__(self, bucket, key,index_col,start_date,end_date):
        self.data = None
        self.index_col = index_col or 0
        self._start_date = start_date
        self._end_date = end_date
        GCPReader.__init__(self, bucket, key)

    def _post_process_pulled_data(self, raw, **kwargs):
        raw = raw.reset_index(drop=True)
        sec_master = kwargs["security_master"]
        mapping = sec_master[['dcm_security_id', 'quandl_ticker']]
        mapping.columns = ['dcm_security_id', 'ticker']
        raw = pd.merge(raw, mapping, how='inner', on='ticker')
        raw.drop('ticker', axis=1, inplace=True)
        start_date_str = str(self._start_date)
        end_date_str = str(self._end_date)
        raw = raw[(raw['datekey']>=start_date_str) & (raw['datekey']<=end_date_str)]
        raw = raw[raw['dimension'].isin(['ARQ', 'ART'])].reset_index(drop=True)
        data = raw
        data["dcm_security_id"] = data["dcm_security_id"].astype(int)
        return data


class SQLMinuteToDailyEquityPrices(DataReaderClass):
    '''

    Creates daily bars from minute data (with dcm_security_id identifier)

    '''

    # PROVIDES_FIELDS = ["daily_price_data"]
    # REQUIRES_FIELDS = ["security_master"]

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

    def _prepare_to_pull_data(self, **kwargs):
        self.query_client = bigquery.Client()

    def _get_data_lineage(self, **kwargs):
        pass

    def do_step_action(self, security_master, **kwargs):
        base_query = """
        with equity_data_with_minute_indicator as
           (
           select distinct
            dcm_security_id,
            ticker, cob, date(cob) as date,
            datetime_diff(datetime_sub(marketdata.convert_timezone('UTC','US/Eastern',cob), interval (9 * 60 + 30) minute), date(cob), minute) as minute_in_day,
            close, open, high, low, volume
           from
            marketdata.equity_prices
           where
            as_of_end is null
            and date(cob)>=date('{0}')
            and date(cob)<=date('{1}')
            and dcm_security_id in {2}
           ),
           open_quotes as
           (
             select dcm_security_id, ticker, date, open from equity_data_with_minute_indicator where minute_in_day =1
           ),
           close_quotes as
           (
             select dcm_security_id, ticker, date, close from equity_data_with_minute_indicator where minute_in_day =390
           ),
           misc_quotes as
           (
             select dcm_security_id, ticker, date, sum(volume) as volume, max(high) as high, min(low) as low
             from equity_data_with_minute_indicator group by dcm_security_id, ticker, date
           )
            select o.dcm_security_id, o.ticker, cast(o.date as datetime) as date, o.open, c.close, m.high, m.low, m.volume
            from open_quotes o
            left join close_quotes c
            on o.dcm_security_id=c.dcm_security_id and o.ticker=c.ticker and o.date=c.date
            left join misc_quotes m
            on o.dcm_security_id=m.dcm_security_id and  o.ticker=m.ticker and o.date=m.date
            order by dcm_security_id, ticker, date
        """.format(self.start_date, self.end_date, tuple(security_master['dcm_security_id'].values))

        self._prepare_to_pull_data()
        data = self.query_client.query(base_query).to_dataframe()
        return data


class CalibrationDates(DataReaderClass):

    def __init__(self, cache_file=None, read_csv_kwargs=None, force_recalculation=False, save_intervals_to=None):
        self.data = None
        read_csv_kwargs = read_csv_kwargs or {}
        self.force_recalculation = force_recalculation
        self.save_intervals_to = save_intervals_to
        if not isinstance(read_csv_kwargs, dict):
            raise TypeError("CalibrationDates.__init__ - read_csv_kwargs must be a dictionary; "
                            "received a {0}".format(type(read_csv_kwargs)))
        if cache_file:
            cache_file = os.path.abspath(os.path.join(os.path.dirname(__file__), cache_file)) \
                if not os.path.isabs(cache_file) else cache_file
            if not os.path.exists(cache_file):
                raise RuntimeError("CalibrationDates.__init__ - file {0} does not exists".format(cache_file))
        self.read_csv_kwargs = read_csv_kwargs
        self.file_to_read = cache_file
        # CalibrationTaskflowTask.__init__(self)

    @classmethod
    def get_default_config(cls):
        cache_file = {"cache_file": "./configuration/interevals_as_of_20181218.csv",
                      "read_csv_kwargs": dict(index_col=0, parse_dates=["date"]),
                      "force_recalculation": False, "save_intervals_to": None}

    @abstractmethod
    def post_process_intervals(self, intervals):
        raise NotImplementedError("CalibrationDates.post_process_intervals must be overriden by derived class")

    @abstractmethod
    def calculate_intervals(self):
        raise NotImplementedError("CalibrationDates.calculate_intervals must be overriden by derived class")

    @abstractmethod
    def calculate_intervals_for_calibration(self):
        raise NotImplementedError("CalibrationDates.calculate_intervals_for_calibration must be overriden by "
                                  "derived class")

    def _prepare_to_pull_data(self, **kwargs):
        self.query_client = bigquery.Client()

    def _get_data_lineage(self, **kwargs):
        pass

    def _pull_data(self):
        self.step_status["DataLineage"] = self.file_to_read
        result = pd.read_csv(self.file_to_read, **self.read_csv_kwargs)
        for date_col in self.read_csv_kwargs.get("parse_dates", []):
            result[date_col] = pd.DatetimeIndex(result[date_col]).normalize()
        return result

    def do_step_action(self, **kwargs):
        # calibration_mode = self.task_params.run_mode is TaskflowPipelineRunMode.Calibration
        calibration_mode = True
        if calibration_mode:
            result = self.calculate_intervals_for_calibration()
        else:
            if self.force_recalculation or not self.file_to_read:
                result = self.calculate_intervals()
            else:
                result = self._pull_data()
        result = self.post_process_intervals(result)

        self.data = result
        return self.data


class CalibrationDatesJump(CalibrationDates):
    PROVIDES_FIELDS = ["intervals_data"]

    def __init__(self, cache_file="../configuration/intervals_for_jump.csv", target_dt=None, intervals_start_dt=None,
                 intervals_end_dt=None, holding_period_in_trading_days=1, force_recalculation=False,
                 save_intervals_to=None):
        target_dt = pd.Timestamp(target_dt) if target_dt else None
        intervals_start_dt = pd.Timestamp(intervals_start_dt) if intervals_start_dt else None
        intervals_end_dt = pd.Timestamp(intervals_end_dt) if intervals_end_dt else None
        self.data = None
        self.target_dt = target_dt
        self.intervals_start_dt = intervals_start_dt
        self.intervals_end_dt = intervals_end_dt
        self.holding_period_in_trading_days = holding_period_in_trading_days
        read_csv_kwargs = dict(index_col=0, parse_dates=["entry_date", "exit_date"])
        CalibrationDates.__init__(self, cache_file, read_csv_kwargs, force_recalculation, save_intervals_to)

    def calculate_intervals(self):
        self.intervals_start_dt = self.intervals_start_dt  # or self.task_params.start_dt.normalize()
        self.intervals_end_dt = self.intervals_end_dt  # or self.task_params.end_dt.normalize()
        entry_dates = list(marketTimeline.get_trading_days(self.intervals_start_dt, self.intervals_end_dt).normalize())
        exit_dates = map(lambda x: marketTimeline.get_trading_day_using_offset(x, self.holding_period_in_trading_days),
                         entry_dates)
        intervals = pd.concat([pd.Series(entry_dates), pd.Series(exit_dates)], axis=1, keys=["entry_date", "exit_date"])
        quarter_func = lambda x: str(x - pd.tseries.frequencies.QuarterEnd(normalize=True) \
                                     + pd.tseries.frequencies.to_offset('1D')).split(" ")[0]
        intervals['quarter_date'] = intervals['entry_date'].apply(quarter_func)
        return intervals

    def calculate_intervals_for_calibration(self):
        self.target_dt = self.target_dt  # or self.task_params.end_dt.normalize()
        try:
            quarter_end = str(self.target_dt - pd.tseries.frequencies.QuarterEnd(normalize=True) \
                              + pd.tseries.frequencies.to_offset('1D')).split(" ")[0]

        except Exception:
            quarter_end = str(self.target_dt - pd.tseries.offsets.QuarterEnd(normalize=True) \
                              + pd.tseries.offsets.Day()).split(" ")[0]
        entry_date = self.target_dt.normalize()
        data = [[entry_date, entry_date, quarter_end]]
        intervals = pd.DataFrame(data, columns=['entry_date', 'exit_date', 'quarter_date'])
        return intervals

    def post_process_intervals(self, intervals):
        intervals["this_quarter"] = intervals["entry_date"].apply(
            lambda x: str(x.year) + "-Q" + str((x.month - 1) / 3 + 1))
        return intervals

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}




############ AIRFLOW FUNCTIONS ############




CALIBRATIONDATEJUMP_PARAMS = {
    "params": {'intervals_start_dt':'2016-06-01','cache_file':JUMP_DATES_CSV,
                       'holding_period_in_trading_days':1,'force_recalculation':True,
                       'target_dt':"2018-11-30"},
    "start_date": RUN_DATE,
    'class': CalibrationDatesJump,
    'provided_data': {'interval_data':"gs://{}/alex/calibration_data/{}/DataPull/{}.csv"},
    'required_data':{}
        }


S3_SECURITY_MASTER_READER_PARAMS = {
    "params": {
                "bucket": os.environ['GCS_BUCKET'],
                "key": "alex/security_master_20230603.csv",
            },
    "start_date": RUN_DATE,
    'class': S3SecurityMasterReader,
    'provided_data': {'security_master':construct_destination_path('data_pull')},
    'required_data':{}

        }



S3_INDUSTRY_MAPPER_READER_PARAMS={
            "params": {
                "bucket": os.environ['GCS_BUCKET'],
                "key": "alex/industry_map.csv",
            },
            "start_date":RUN_DATE,
    'class': S3IndustryMappingReader,
    'provided_data': {'industry_mapper':construct_destination_path('data_pull')},
    'required_data':{'security_master':construct_required_path('data_pull','security_master')}
        }


S3_ECON_TRANSFORMATION_PARAMS = {"params": {"sector_etfs": ["SPY", "MDY", "EWG", "EWH", "EWJ", "EWW", "EWS", "EWU"],
                              "bucket": "dcm-prod-ba2f-us-dcm-data-test",
                              "key": "alex/econ_transform_definitions.csv"},
                   "start_date": RUN_DATE,
                    'class': S3EconTransformationReader,
    'provided_data': {'econ_transformation':construct_destination_path('data_pull')},
    'required_data':{'security_master':construct_required_path('data_pull','security_master')}
                   }



YAHOO_DAILY_PRICE_READER_PARAMS ={"params": {"sector_etfs": ["SPY", "MDY", "EWG", "EWH", "EWJ", "EWW", "EWS", "EWU"],
                              "start_date": "1997-01-01",
                              "end_date": RUN_DATE},
                   "start_date": RUN_DATE,
                           'class': YahooDailyPriceReader,
                           'provided_data': {'etf_prices': construct_destination_path('data_pull')},
                           'required_data': {}
                           }


S3_RUSSELL_COMPONENT_READER_PARAMS = {"params": {"bucket": "dcm-prod-ba2f-us-dcm-data-test", 'key': 'N/A',
                              "r3k_key": "alex/r3k.csv", "r1k_key": "alex/r1k.csv"},
                   "start_date": RUN_DATE,
                               'class': S3RussellComponentReader,
                               'provided_data': {'russell_components': construct_destination_path('data_pull')},
                               'required_data': {'security_master':construct_required_path('data_pull','security_master')}
                               }


S3_RAW_SQL_READER_PARAMS = {"params": {"bucket": "dcm-prod-ba2f-us-dcm-data-temp",
                              "key": "jack/SHARADAR_SF1.csv", 'index_col': False,
                                       "start_date" : "2000-01-03","end_date" : RUN_DATE
                                      },
                   "start_date": RUN_DATE,
                     'class': S3RawQuandlDataReader,
                     'provided_data': {'raw_quandl_data': construct_destination_path('data_pull')},
                     'required_data': {
                         'security_master': construct_required_path('data_pull','security_master')}
                     }


SQL_MINUTE_TO_DAILY_EQUITY_PRICES_PARAMS = {'params':{"start_date" : "2000-01-03","end_date" : "2023-06-28"},
                                            'class':SQLMinuteToDailyEquityPrices,
                                      'start_date':RUN_DATE,
                                        'provided_data': {'daily_price_data': construct_destination_path('data_pull')},
                                            'required_data': {'security_master': construct_required_path('data_pull','security_master')}

                                            }




















