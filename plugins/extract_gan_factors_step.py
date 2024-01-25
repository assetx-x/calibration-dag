from airflow.decorators import task

from core_classes import DataReaderClass
from plugins.asset_pricing_model import extract_factors
import pandas as pd
import os
from datetime import datetime
from core_classes import (
    construct_destination_path,
)

parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
plugins_folder = os.path.join(parent_directory, "plugins")
data_processing_folder = os.path.join(plugins_folder, "data_processing")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(
    data_processing_folder, 'dcm-prod.json'
)
os.environ['GCS_BUCKET'] = 'dcm-prod-ba2f-us-dcm-data-test'
JUMP_DATES_CSV = os.path.join(data_processing_folder, 'intervals_for_jump.csv')
current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')


class ExtractGANFactors(DataReaderClass):
    def __init__(
        self,
        insample_cut_date,
        epochs,
        epochs_discriminator,
        gan_iterations,
        retrain,
        bucket,
        key,
    ):
        super().__init__(bucket, key)
        self.insample_cut_date = insample_cut_date
        self.epochs = epochs
        self.epochs_discriminator = epochs_discriminator
        self.gan_iterations = gan_iterations
        self.retrain = retrain
        self._data = None

    @property
    def data(self):
        print(f'[>] Extracting data from {self.key}')
        return self._data

    @data.setter
    def data(self, val):
        print(f'[>] Setting data to {val}!')
        self._data = val

    @task.external_python(
        task_id="external_python",
        python='/home/dcmadmin/calibration_ml_training/bin/python',
    )
    def do_step_action(self, **kwargs):
        full_path = 'gs://dcm-prod-ba2f-us-dcm-data-test/calibration_data/live/save_gan_inputs/save_gan_inputs'
        factor_file = os.path.join(full_path, "all_factors.h5")

        # Call the extract_factors function directly
        extract_factors(full_path, self.insample_cut_date)
        print('finished extracting factors')
        all_factors = pd.read_hdf(factor_file)
        self.data = all_factors
        return {'gan_factors': self.data}


extract_gan_factors_params = {
    'insample_cut_date': "2023-02-03",
    'epochs': 150,
    'epochs_discriminator': 50,
    'gan_iterations': 2,
    'retrain': False,
}

ExtractGANFactors_params = {
    'params': extract_gan_factors_params,
    'class': ExtractGANFactors,
    'start_date': RUN_DATE,
    'provided_data': {
        'gan_factors': construct_destination_path('generate_gan_results'),
    },
    'required_data': {},
}
