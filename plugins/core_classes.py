from abc import ABC,ABCMeta, abstractmethod
from google.cloud import storage
from enum import Enum
import pandas as pd
import io
import os
import yfinance as yf
from market_timeline import marketTimeline

EXEMPTIONS = []

class StatusType(Enum):
    Not_Tested = 0
    Success = 1
    Warn = 2
    Fail = 3
    Not_Tested_Warn = 10

class ReturnTypeForPermutation(Enum):
    LogReturn=1
    AbsReturn=2
    Level=3
    LogOU_VIX = 4
    LogOU_VXST = 5
    LogOU_VX1 = 6
    LogOU_VX2 = 7


class GCPInstance(ABC):
    """@abstractclassmethod
    def get_data():
        NotImplementedError()"""

    @abstractmethod
    def get_bucket(self):
        NotImplementedError()

def grab_csv_from_gcp(bucket, data):
    return pd.read_csv(
        os.path.join('gs://{}'.format(bucket), data),
        # index_col=[0]
    )

def instantiate_gcp_bucket(bucket):
    storage_client = storage.Client()
    # bucket_name = bucket_name
    bucket = storage_client.get_bucket(bucket)
    return storage_client, bucket

class GCPInstanceSingleton(GCPInstance):
    __instance = None

    def __init__(self, bucket_name):
        if GCPInstanceSingleton.__instance != None:
            raise Exception('Singleton cannot be instantiated twice')
        else:
            self.bucket_name = bucket_name
            GCPInstanceSingleton.__instance = self

    def get_bucket(self):
        _, bucket = instantiate_gcp_bucket(self.bucket_name)

        return bucket

    def get_data(self, path):
        # _,bucket = instantiate_gcp_bucket(self.bucket_name)
        csv_data = grab_csv_from_gcp(self.bucket_name, path)
        return csv_data


def download_yahoo_data(ticker_list, start_dt, end_dt):
    df_1 = []
    for ticker in ticker_list:
        close_col = "{0}_close".format(ticker)
        vol_col = "{0}_volume".format(ticker)
        try:
            df = yf.download(ticker, start_dt, end_dt,progress=False)
            df = df.fillna(method="ffill")[["Adj Close","Volume"]].rename(columns={"Adj Close": close_col, "Volume": vol_col})
            df = df[~df.index.duplicated(keep='last')]
            df_1.append(df)
        except:
            print("Download failure for {0}".format(ticker))

    etf_prices = pd.concat(df_1, axis=1)
    return etf_prices


class DataReaderClass(ABC):

    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key = key

    @abstractmethod
    def _prepare_to_pull_data(self, **kwargs):
        raise NotImplementedError()

    # @abstractmethod
    # def _pull_data(self,**kwargs):
    #    raise NotImplementedError()

    # @abstractmethod
    # def _post_process_pulled_data(self,data,**kwargs):
    #    raise NotImplementedError()

    @abstractmethod
    def _get_data_lineage(self):
        raise NotImplementedError()

    # @abstractmethod
    # def do_step_action(self, data, **kwargs):
    #    raise NotImplementedError()


class GCPReader(DataReaderClass):

    def __init__(self, bucket, key):
        super().__init__(bucket, key)
        self.data = None
        if isinstance(key, str):
            self.files_to_read = list(map(lambda x: "gs://{0}/{1}".format(bucket, x), [key]))
        else:
            self.files_to_read = list(map(lambda x: "gs://{0}/{1}".format(bucket, x), key))
        self.s3_client = None
        self.index_col = 0

    def _prepare_to_pull_data(self, **kwargs):
        self.gcp_client = storage.Client()

    def _get_data_lineage(self):
        return self.files_to_read

    def _pull_data(self, **kwargs):
        bucket = self.gcp_client.bucket(self.bucket)
        blob = bucket.blob(self.key)
        file_content = io.BytesIO(blob.download_as_string())
        data = pd.read_csv(file_content)
        return data

    def do_step_action(self, **kwargs):
        # calibration_logger.info("Executing {0}._prepare_to_pull_data".format(self.__class__.__name__))
        self._prepare_to_pull_data(**kwargs)
        # calibration_logger.info("Executing {0}._pull_data".format(self.__class__.__name__))
        result = self._pull_data(**kwargs)
        # calibration_logger.info("Executing {0}._post_process_pulled_data".format(self.__class__.__name__))
        self.data = self._post_process_pulled_data(result, **kwargs)
        # self.step_status["DataLineage"] = self._get_data_lineage()
        # return StatusType.Success
        return self.data


def construct_required_path(step,file_name):
    return "gs://{}/calibration_data/testing_universe" + "/{}/".format(step) + "{}.csv".format(file_name)

def construct_destination_path(step):
    return "gs://{}/calibration_data/testing_universe" +"/{}/".format(step) +"{}.csv"



def construct_required_path_earnings(step,file_name):
    return "gs://{}/calibration_data/earnings/tenere_portfolio" + "/{}/".format(step) + "{}.csv".format(file_name)

def construct_destination_path_earnings(step):
    return "gs://{}/calibration_data/earnings/tenere_portfolio" +"/{}/".format(step) +"{}.csv"

def pick_trading_quarterly_dates(start_date, end_date, mode='BQ'):
    weekly_days = pd.date_range(start_date, end_date, freq=mode)
    trading_dates = pd.Series(weekly_days).apply(marketTimeline.get_next_trading_day_if_holiday)
    dates = trading_dates[(trading_dates>=start_date) & (trading_dates<=end_date)]
    return dates

def pick_trading_month_dates(start_date, end_date, mode="bme"):
    trading_days = marketTimeline.get_trading_days(start_date, end_date).tz_localize(None)
    if mode=="bme":
        dates = pd.Series(trading_days).groupby(trading_days.to_period('M')).last()
    else:
        dates = pd.Series(trading_days).groupby(trading_days.to_period('M')).first()
    dates = dates[(dates>=start_date) & (dates<=end_date)]
    return dates