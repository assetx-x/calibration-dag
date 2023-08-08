from abc import ABC,ABCMeta, abstractmethod
from google.cloud import storage
from enum import Enum
import pandas as pd
import io
import os
import yfinance as yf

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


######## AIRFLOW WRAPPER

def airflow_wrapper(**kwargs):
    params = kwargs['params']

    # Read all required data into step_action_args dictionary
    try:
        step_action_args = {k: pd.read_csv(v, index_col=0) for k, v in kwargs['required_data'].items()}
    except Exception as e:
        print(f"Error reading data: {e}")
        step_action_args = {}

    # Execute do_step_action method
    data_outputs = kwargs['class'](**params).do_step_action(**step_action_args)

    # If the method doesn't return a dictionary (for classes returning just a single DataFrame)
    # convert it into a dictionary for consistency
    if not isinstance(data_outputs, dict):
        data_outputs = {list(kwargs['provided_data'].keys())[0]: data_outputs}

    # Save each output data to its respective path on GCS
    for data_key, data_value in data_outputs.items():
        if data_key in kwargs['provided_data']:
            gcs_path = kwargs['provided_data'][data_key].format(os.environ['GCS_BUCKET'], kwargs['start_date'], data_key)

            data_value.to_csv(gcs_path)

