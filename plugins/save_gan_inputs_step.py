from core_classes import GCPReader, download_yahoo_data, DataReaderClass
import pandas as pd
import numpy as np
from datetime import datetime
from core_classes import StatusType, create_directory_if_does_not_exists
from market_timeline import pick_trading_week_dates, pick_trading_month_dates
from commonlib import talib_STOCHRSI, MA, talib_PPO, talib_TRIX
import talib
import os

current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')
from core_classes import construct_required_path, construct_destination_path

from dotenv import load_dotenv


class GenerateDataGAN(DataReaderClass):
    PROVIDES_FIELDS = ["company_data", "gan_data_info"]
    REQUIRES_FIELDS = [
        "normalized_data_full_population",
        "econ_data_final",
        "future_returns_bme",
        "active_matrix",
    ]

    '''
    GAN_results_generation_multi_batch_long_history_1_ts_201910_update.ipynb
    Last Section. It is unstructured in the notebook
    '''

    def __init__(self, data_dir):
        self.data = None
        self.gan_data_info = None
        self.data_dir = data_dir

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def melt_merge(self, future_return_data, df):

        future_returns = pd.melt(
            future_return_data.reset_index().rename(columns={"index": "date"}),
            id_vars=["date"],
            value_name="future_return_bme",
        )
        future_returns.rename(columns={"variable": "ticker"}, inplace=True)

        # ensure columns are correct format
        future_returns['date'] = pd.to_datetime(future_returns['date'])
        df['date'] = pd.to_datetime(df['date'])
        future_returns['ticker'] = future_returns['ticker'].astype(str)
        df['ticker'] = df['ticker'].astype(str)

        merged = pd.merge(future_returns, df, how="left", on=["date", "ticker"])
        merged = merged.sort_values(["date", "ticker"])
        return merged

    def do_step_action(self, **kwargs):
        df = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        econ_data = kwargs[self.__class__.REQUIRES_FIELDS[1]].copy(deep=True)
        future_return_data = kwargs[self.__class__.REQUIRES_FIELDS[2]].copy(deep=True)
        active_matrix = kwargs[self.__class__.REQUIRES_FIELDS[3]].copy(deep=True)

        company_data = self.melt_merge(
            future_return_data, df
        )  # Merging in this direction ensures that it conforms to the rest of the GAN data

        base_dir = (
            'gs://dcm-prod-ba2f-us-dcm-data-test/calibration_data/save_gan_inputs'
        )

        # full_path = get_local_dated_dir(base_dir, self.task_params.end_dt, self.data_dir)
        full_path = base_dir
        create_directory_if_does_not_exists(full_path)
        company_data.to_hdf(
            os.path.join(full_path, "company_data.h5"), key="df", format="table"
        )
        econ_data.to_hdf(
            os.path.join(full_path, "econ_data_final.h5"), key="df", format="table"
        )
        future_return_data.to_hdf(
            os.path.join(full_path, "future_returns.h5"), key="df", format="fixed"
        )
        active_matrix.to_hdf(
            os.path.join(full_path, "active_matrix.h5"), key="df", format="fixed"
        )
        print("****************************************************")
        print("****************************************************")
        print("GAN Data full path: {0}".format(full_path))
        self.data = company_data
        self.gan_data_info = pd.DataFrame([self.data_dir], columns=["relative_path"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {
            self.__class__.PROVIDES_FIELDS[0]: self.data,
            self.__class__.PROVIDES_FIELDS[1]: self.gan_data_info,
        }


class GenerateDataGANWeekly(GenerateDataGAN):
    PROVIDES_FIELDS = ["company_data", "company_data_weekly", "gan_data_info"]
    REQUIRES_FIELDS = [
        "normalized_data_full_population",
        "normalized_data_full_population_weekly",
        "econ_data_final",
        "econ_data_final_weekly",
        "future_returns_bme",
        "future_returns_weekly",
        "active_matrix",
        "active_matrix_weekly",
    ]

    def __init__(self, data_dir):
        self.weekly_data = None
        GenerateDataGAN.__init__(self, data_dir)

    def do_step_action(self, **kwargs):
        monthly_df = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        weekly_df = kwargs[self.__class__.REQUIRES_FIELDS[1]].copy(deep=True)
        econ_data = kwargs[self.__class__.REQUIRES_FIELDS[2]].copy(deep=True)
        econ_data_weekly = kwargs[self.__class__.REQUIRES_FIELDS[3]].copy(deep=True)
        future_return_data = kwargs[self.__class__.REQUIRES_FIELDS[4]].copy(deep=True)
        future_return_weekly = kwargs[self.__class__.REQUIRES_FIELDS[5]].copy(deep=True)
        active_matrix = kwargs[self.__class__.REQUIRES_FIELDS[6]].copy(deep=True)
        active_matrix_weekly = kwargs[self.__class__.REQUIRES_FIELDS[7]].copy(deep=True)

        company_data_monthly = self.melt_merge(future_return_data, monthly_df)
        company_data_weekly = self.melt_merge(future_return_weekly, weekly_df)

        base_dir = 'gs://dcm-prod-ba2f-us-dcm-data-test/calibration_data/live/save_gan_inputs/save_gan_inputs'

        # full_path = get_local_dated_dir(base_dir, self.task_params.end_dt, self.data_dir)
        full_path = base_dir
        # create_directory_if_does_not_exists(full_path)
        company_data_monthly.to_csv(os.path.join(full_path, "company_data.csv"))
        company_data_weekly.to_csv(os.path.join(full_path, "company_data_weekly.csv"))
        econ_data.to_csv(os.path.join(full_path, "econ_data_final.csv"))
        econ_data_weekly.to_csv(os.path.join(full_path, "econ_data_final_weekly.csv"))
        future_return_data.to_csv(os.path.join(full_path, "future_returns.csv"))
        future_return_weekly.to_csv(
            os.path.join(full_path, "future_returns_weekly.csv")
        )
        active_matrix.to_csv(os.path.join(full_path, "active_matrix.csv"))
        active_matrix_weekly.to_csv(os.path.join(full_path, "active_matrix_weekly.csv"))

        print("****************************************************")
        print("****************************************************")
        print("GAN Data full path: {0}".format(full_path))
        self.data = company_data_monthly
        self.weekly_data = company_data_weekly
        self.gan_data_info = pd.DataFrame([self.data_dir], columns=["relative_path"])
        return {
            'company_data': self.data,
            'company_data_weekly': self.weekly_data,
            'gan_data_info': self.gan_data_info,
        }
        # return self.data,self.weekly_data,self.gan_data_info

    def _get_additional_step_results(self):
        return {
            self.__class__.PROVIDES_FIELDS[0]: self.data,
            self.__class__.PROVIDES_FIELDS[1]: self.weekly_data,
            self.__class__.PROVIDES_FIELDS[2]: self.gan_data_info,
        }


GDGW_params2 = {
    'data_dir': construct_destination_path('save_gan_inputs')
    .format(os.environ['GCS_BUCKET'], 'holding')
    .split('/holding')[0]
}

GenerateDataGANWeekly_params = {
    'params': GDGW_params2,
    'class': GenerateDataGANWeekly,
    'start_date': RUN_DATE,
    'provided_data': {
        'company_data': construct_destination_path('save_gan_inputs'),
        'company_data_weekly': construct_destination_path('save_gan_inputs'),
        'gan_data_info': construct_destination_path('save_gan_inputs'),
    },
    'required_data': {
        'normalized_data_full_population': construct_required_path(
            'standarization', 'normalized_data_full_population'
        ),
        'normalized_data_full_population_weekly': construct_required_path(
            'standarization', 'normalized_data_full_population_weekly'
        ),
        'econ_data_final': construct_required_path('merge_econ', 'econ_data_final'),
        'econ_data_final_weekly': construct_required_path(
            'merge_econ', 'econ_data_final_weekly'
        ),
        'future_returns_bme': construct_required_path(
            'additional_gan_features', 'future_returns_bme'
        ),
        'future_returns_weekly': construct_required_path(
            'additional_gan_features', 'future_returns_weekly'
        ),
        'active_matrix': construct_required_path('active_matrix', 'active_matrix'),
        'active_matrix_weekly': construct_required_path(
            'active_matrix', 'active_matrix_weekly'
        ),
    },
}
