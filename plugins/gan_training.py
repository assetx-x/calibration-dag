from core_classes import GCPReader,download_yahoo_data,DataReaderClass
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
from core_classes import construct_required_path,construct_destination_path,convert_date_to_string


parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
plugins_folder = os.path.join(parent_directory, "plugins")
data_processing_folder = os.path.join(plugins_folder, "data_processing")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(data_processing_folder,'dcm-prod.json')
os.environ['GCS_BUCKET'] = 'dcm-prod-ba2f-us-dcm-data-test'
JUMP_DATES_CSV = os.path.join(data_processing_folder,'intervals_for_jump.csv')
current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')

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






class TrainGANModel(DataReaderClass):

    PROVIDES_FIELDS = ["gan_model_save_info"]
    REQUIRES_FIELDS = ["gan_data_info"]

    '''
    GAN_asset_pricing_multi_batch_long_history.ipynb
    DataGeneratorMultiBatchFast
    AssetPricingGAN
    '''

    def __init__(self, insample_cut_date, epochs, epochs_discriminator,
                 gan_iterations, retrain):
        self.data = None
        self.python_bin = os.environ.get("PY3_EXEC", None)
        self.util_script_location = "processes/data_processing/calibrations/quantamental_ml/gan_py3/gan_util_functions.py"
        self.insample_cut_date = insample_cut_date
        self.epochs = epochs
        self.epochs_discriminator = epochs_discriminator
        self.gan_iterations = gan_iterations
        self.retrain = retrain

    def do_step_action(self, **kwargs):

        # TODO: Skipping the actual model training data since it has been set to false

        relative_path = 'dcm-prod-ba2f-us-dcm-data-test/calibration_data/live/save_gan_inputs/save_gan_inputs'

        file_location = os.path.join(relative_path, "saved_weights.h5")

        results = pd.DataFrame(columns=["relative_path", "success", "insample_cut_date"])
        results.loc[0, "success"] = 1
        results.loc[0, "relative_path"] = relative_path
        results.loc[0, "insample_cut_date"] = self.insample_cut_date
        results["success"] = results["success"].astype(int) # Not sure why this is necessary
        self.data = results



train_gan_params = { 'insample_cut_date': "2018-03-29",
                    'epochs': 100,
                    'epochs_discriminator': 30,
                    'gan_iterations': 2,
                    'retrain': False,
                }



TrainGANModel_params = {'params':train_gan_params,
                                   'class':TrainGANModel,
                                       'start_date':RUN_DATE,
                                'provided_data': {'gan_model_save_info': construct_destination_path('gan_training'),
                                                 },
                                'required_data': {}}

