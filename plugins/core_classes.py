import tempfile
from abc import ABC, abstractmethod
from google.cloud import storage
from enum import Enum
import pandas as pd
import io
import os
import yfinance as yf
from market_timeline import marketTimeline
from joblib import dump, load

EXEMPTIONS = []


class StatusType(Enum):
    Not_Tested = 0
    Success = 1
    Warn = 2
    Fail = 3
    Not_Tested_Warn = 10


class ReturnTypeForPermutation(Enum):
    LogReturn = 1
    AbsReturn = 2
    Level = 3
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
            df = yf.download(ticker, start_dt, end_dt, progress=False)
            df = df.fillna(method="ffill")[["Adj Close", "Volume"]].rename(
                columns={"Adj Close": close_col, "Volume": vol_col}
            )
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

    @abstractmethod
    def _get_data_lineage(self):
        raise NotImplementedError()


class GCPReader(DataReaderClass):

    def __init__(self, bucket, key):
        super().__init__(bucket, key)
        self.data = None
        if isinstance(key, str):
            self.files_to_read = list(
                map(lambda x: "gs://{0}/{1}".format(bucket, x), [key])
            )
        else:
            self.files_to_read = list(
                map(lambda x: "gs://{0}/{1}".format(bucket, x), key)
            )
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


def construct_required_path(step, file_name):
    return (
        "gs://{}/calibration_data/live"
        + "/{}/".format(step)
        + "{}.csv".format(file_name)
    )


def construct_destination_path(step):
    return "gs://{}/calibration_data/live" + "/{}/".format(step) + "{}.csv"


def pick_trading_quarterly_dates(start_date, end_date, mode='BQ'):
    weekly_days = pd.date_range(start_date, end_date, freq=mode)
    trading_dates = pd.Series(weekly_days).apply(
        marketTimeline.get_next_trading_day_if_holiday
    )
    dates = trading_dates[(trading_dates >= start_date) & (trading_dates <= end_date)]
    return dates


def pick_trading_month_dates(start_date, end_date, mode="bme"):
    trading_days = marketTimeline.get_trading_days(start_date, end_date).tz_localize(
        None
    )
    if mode == "bme":
        dates = pd.Series(trading_days).groupby(trading_days.to_period('M')).last()
    else:
        dates = pd.Series(trading_days).groupby(trading_days.to_period('M')).first()
    dates = dates[(dates >= start_date) & (dates <= end_date)]
    return dates


def convert_date_to_string(date):
    return date.strftime('%Y-%m-%d')


def create_directory_if_does_not_exists(dir_path):
    try:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


class DataFormatter(object):

    def __init__(self, class_, class_parameters, provided_data, required_data):

        self.class_ = class_
        self.class_parameters = class_parameters
        self.provided_data = provided_data
        self.required_data = required_data

    def construct_data(self):
        return {
            'class': self.class_,
            'params': self.class_parameters,
            # 'start_date':self.start_date,
            'provided_data': self._provided_data_construction(),
            'required_data': self._required_data_construction(),
        }

    def __call__(self):
        # When an instance is called, return the result of construct_data
        return self.construct_data()

    def _required_data_construction(self):

        if len(self.required_data) == 0:
            return {}
        elif len(self.required_data) == 1:
            directory = list(self.required_data.keys())[0]
            return {
                k: construct_required_path(directory, k)
                for k in self.required_data[directory]
            }

        else:
            path_dictionary = {}
            directories = list(self.required_data.keys())
            for directory in directories:
                path_dictionary.update(
                    {
                        k: construct_required_path(directory, k)
                        for k in self.required_data[directory]
                    }
                )
            return path_dictionary

    def _provided_data_construction(self):
        directory = list(self.provided_data.keys())[0]
        return {
            k: construct_destination_path(directory)
            for k in self.provided_data[directory]
        }


class ShapProcessor:
    @staticmethod
    def get_shap_values(shap_dictionary, X_cols):
        shap_values_df_dict = {}
        for explainers in shap_dictionary.keys():
            try:
                shap_values = shap_dictionary[explainers].shap_values(X_cols, check_additivity=False)
            except Exception:
                shap_values = shap_dictionary[explainers].shap_values(X_cols)
            shap_values_df = pd.DataFrame(shap_values, columns=X_cols.columns, index=X_cols.index)
            shap_values_df_dict[explainers] = shap_values_df
        return shap_values_df_dict

    @staticmethod
    def create_explainers(models, x_econ):
        explainers = {
            'gbm': shap.TreeExplainer(model=models['gbm']),
            'rf': shap.TreeExplainer(model=models['rf']),
            'lasso': shap.LinearExplainer(models['lasso'], x_econ),
            'ols': shap.LinearExplainer(models['ols'], x_econ),
            'enet': shap.LinearExplainer(models['enet'], x_econ),
            'et': shap.TreeExplainer(model=models['et'], data=x_econ)
        }
        return explainers


class ModelLoader:
    def __init__(self, base_dir, leaf_path, bucket):
        self.base_dir = base_dir
        self.leaf_path = leaf_path
        self.bucket = bucket
        self.s3_client = storage.Client()

    def load_models(self, model_list):
        models = {}
        key = '{0}/{1}'.format(self.base_dir, self.leaf_path)
        print(key)
        for model in model_list:
            print(model)
            print(model_list)
            model_location = key.format(model)
            with tempfile.TemporaryFile() as fp:
                bucket = self.s3_client.bucket(self.bucket)
                print(model_location)
                blob = bucket.blob(model_location)
                self.s3_client.download_blob_to_file(blob, fp)
                fp.seek(0)
                models[model] = load(fp)
        return models

    def load_single_model(self, model):
        key = '{0}/{1}'.format(self.base_dir, self.leaf_path)
        model_location = key.format(model)
        with tempfile.TemporaryFile() as fp:
            bucket = self.s3_client.bucket(self.bucket)
            blob = bucket.blob(model_location)
            self.s3_client.download_blob_to_file(blob, fp)
            fp.seek(0)
        return load(fp)


class EconDataCreator:
    def __init__(self, base_dir, model_loader):
        self.base_dir = base_dir
        self.model_loader = model_loader

    def create_econ_data(self, df, X_cols, y_col, quarter_cut):
        model_list = ['rf', 'enet', 'ols', 'lasso', 'et', 'gbm']
        models = self.model_loader.load_models(model_list)

        df = df[df['date'] >= quarter_cut]
        y_econ, x_econ = df[y_col], df[X_cols]
        x_econ.set_index(['date', 'ticker'], inplace=True)

        explainers = ShapProcessor.create_explainers(models, x_econ)
        complete_econ_shap_dictionary = ShapProcessor.get_shap_values(explainers, x_econ)

        return complete_econ_shap_dictionary, x_econ