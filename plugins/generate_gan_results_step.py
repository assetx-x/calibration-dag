from asset_pricing_model import extract_factors
import pandas as pd
import os
from datetime import datetime

# parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
# plugins_folder = os.path.join(parent_directory, "plugins")
# data_processing_folder = os.path.join(plugins_folder, "data_processing")
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(
#     data_processing_folder, 'dcm-prod.json'
# )
os.environ['GCS_BUCKET'] = 'dcm-prod-ba2f-us-dcm-data-test'
JUMP_DATES_CSV = os.path.join('dapta_processing', 'intervals_for_jump.csv')
current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')


#@task.external_python(
#    task_id="external_python",
#    python='/home/dcmadmin/calibration_ml_training/bin/python',)

class ExtractGANFactors:

    def __init__(self, insample_cut_date, epochs, epochs_discriminator,gan_iterations,
                 retrain):
        self.insample_cut_date = insample_cut_date
        self.epochs = epochs
        self.epochs_discriminator = epochs_discriminator
        self.gan_iterations = gan_iterations
        self.retrain = retrain

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

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
    'data_dir': 'gs://dcm-prod-ba2f-us-dcm-data-test/calibration_data/local/save_gan_inputs',
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
        'gan_factors': "gs://{}/calibration_data/live" + "/{}/".format('generate_gan_result') + "{}.csv"
    },
    'required_data': {},
}

if __name__ == "__main__":
    data_dir = ExtractGANFactors_params['params']['data_dir']
    insample_cut_date = ExtractGANFactors_params['params']['insample_cut_date']
    epochs = ExtractGANFactors_params['params']['epochs']
    epochs_discriminator = ExtractGANFactors_params['params']['epochs_discriminator']
    gan_iterations = ExtractGANFactors_params['params']['gan_iterations']
    mode = ExtractGANFactors_params['params']['retrain']

    print("******************************************")
    print(data_dir)
    print("******************************************")

    saved_weight_path = os.path.join(data_dir, "saved_weights.h5")

    mode=1
    if mode == 0:
        print('Retrain Mode!!!!!!!!')
        #train_gan_model(data_dir, insample_cut_date, epochs, epochs_discriminator, gan_iterations)
    elif mode == 1:
        print('Rolling Mode!!!!')
        extract_factors(data_dir, insample_cut_date)
    else:
        print("Only supported modes are 0 for training and 1 for factor extraction")
        raise Exception("Only supported modes are 0 for training and 1 for factor extraction")

    print("Arguments loaded")
