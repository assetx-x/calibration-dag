from airflow.decorators import task

from core_classes import DataReaderClass
from plugins.asset_pricing_model import SDFExtraction
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


@task.external_python(
    task_id="external_python",
    python='/home/dcmadmin/calibration_ml_training/bin/python',
)
def _do_step_action(insample_cut_date):

    full_path = 'gs://dcm-prod-ba2f-us-dcm-data-test/calibration_data/live/save_gan_inputs/save_gan_inputs'
    factor_file = os.path.join(full_path, "all_factors.h5")

    sdf_network = SDFExtraction(full_path, insample_cut_date)
    sdf_network.generate_all_factors()
    sdf_network.save_factors()

    print('finished extracting factors')
    all_factors = pd.read_hdf(factor_file)
    return {'gan_factors': all_factors}


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

    def do_step_action(self, **kwargs):
        data = _do_step_action(self)
        self.data = data.get('gan_factors')
        return data

    def _prepare_to_pull_data(self):
        print(f'[>] Preparing to pull data mock')

    def _get_data_lineage(self):
        print(f'[>] Getting data lineage mock')


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
