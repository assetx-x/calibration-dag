import pandas as pd
from core_classes import GCPReader,download_yahoo_data,DataReaderClass
import pandas as pd
import numpy as np
from datetime import datetime
import os
from core_classes import StatusType
from google.cloud import storage

current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')
from core_classes import construct_required_path,construct_destination_path

class ConsolidateGANResults(DataReaderClass):
    PROVIDES_FIELDS = ["company_data_rf"]
    REQUIRES_FIELDS = ["company_data", "gan_factors"]

    '''
    GAN_results_generation_multi_batch_long_history_1_ts_201910_update.ipynb
    Last Section. It is unstructured in the notebook
    '''

    def __init__(self):
        self.data = None

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def create_gan_target_columns(self, df):
        df["future_return_RF"] = df["future_return_bme"] * df["f_t"]  # TODO: Should be f_t+1 (fix at next full recal)?
        df["future_return_RF_100"] = df["future_return_RF"] * 100.0

        col = "future_return_RF_100"
        new_col = "future_return_RF_100_std"
        lb = df.groupby('date')[col].transform(pd.Series.quantile, 0.005)
        lb.fillna(-999999999999999, inplace=True)
        ub = df.groupby('date')[col].transform(pd.Series.quantile, 0.995)
        ub.fillna(999999999999999, inplace=True)
        df[new_col] = df[col].clip(lb, ub)
        # df[new_col] = df[col]
        # df.loc[df[col]<=lb, new_col] = lb
        # df.loc[df[col]>=ub, new_col] = ub
        df[new_col] = df.groupby('date')[col].transform(lambda x: (x - x.mean()) / x.std())
        df[new_col] = df[new_col].fillna(0.0)
        return df

    def _get_gan_factors(self):
        bucket_name = 'dcm-prod-ba2f-us-dcm-data-test'
        source_blob_name = 'calibration_data/live/save_gan_inputs/save_gan_inputs/all_factors.h5'
        destination_file_name = os.path.join(os.getcwd(), 'all_factors.h5')

        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        # Download the file to a local destination
        print('downloading weights from blob storage')
        blob.download_to_filename(destination_file_name)

        data = pd.read_hdf(os.path.join(os.getcwd(), 'all_factors.h5'))
        # remove the temporary holding location
        os.remove(destination_file_name)
        return data

    def do_step_action(self, **kwargs):
        df = kwargs['company_data'].reset_index(drop=True)
        gan_factors = self._get_gan_factors().drop("f_t+1", axis=1).reset_index()

        # Need to join by gan_factors since we lose 1 time step from the lstm lag
        true_start = min(df["date"].min(), gan_factors["date"].min())
        df = df[df["date"] >= true_start]
        gan_factors = gan_factors[gan_factors["date"] >= true_start]

        df = pd.merge(df, gan_factors, how="left", on=["date"]).set_index(["date", "ticker"]).sort_index().reset_index()
        df = self.create_gan_target_columns(df)
        df = df.fillna(0.0)
        self.data = df.reset_index(drop=True)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}

    @classmethod
    def get_default_config(cls):
        return {}


class ConsolidateGANResultsWeekly(ConsolidateGANResults):
    PROVIDES_FIELDS = ["company_data_rf", "company_data_rf_weekly"]
    REQUIRES_FIELDS = ["company_data", "company_data_weekly", "gan_factors"]

    def __init__(self):
        self.weekly_data = None
        ConsolidateGANResults.__init__(self)

    def do_step_action(self, **kwargs):
        monthly_df = kwargs["company_data"].reset_index(drop=True)
        weekly_df = kwargs["company_data_weekly"].reset_index(drop=True)
        gan_factors = self._get_gan_factors().drop("f_t+1", axis=1).reset_index()
        monthly_df['date'] = monthly_df['date'].apply(pd.Timestamp)
        weekly_df['date'] = weekly_df['date'].apply(pd.Timestamp)

        # Need to join by gan_factors since we lose 1 time step from the lstm lag
        true_start = min(monthly_df["date"].min(), gan_factors["date"].min())

        monthly_df = monthly_df[monthly_df["date"] >= true_start]
        weekly_df = weekly_df[weekly_df["date"] >= true_start]
        gan_factors = gan_factors[gan_factors["date"] >= true_start]

        monthly_df = pd.merge(monthly_df, gan_factors, how="left", on=["date"]).set_index(
            ["date", "ticker"]).sort_index().reset_index()
        monthly_df = self.create_gan_target_columns(monthly_df)
        self.data = monthly_df.fillna(0.0).reset_index(drop=True)

        weekly_df = pd.merge_asof(weekly_df, gan_factors, on="date", direction="backward")
        weekly_df.set_index(["date", "ticker"]).sort_index().reset_index()
        weekly_df = self.create_gan_target_columns(weekly_df)
        cols_to_ffill = ['f_t', 'hidden_state_0', 'hidden_state_1', 'hidden_state_2', 'hidden_state_3']
        weekly_df.loc[:, cols_to_ffill] = weekly_df.loc[:, cols_to_ffill].ffill()
        self.weekly_data = weekly_df.fillna(0.0).reset_index(drop=True)
        return {'company_data_rf': self.data, 'company_data_rf_weekly': self.weekly_data}

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data,
                self.__class__.PROVIDES_FIELDS[1]: self.weekly_data}


class AddFoldIdToGANResultData(DataReaderClass):
    PROVIDES_FIELDS = ["normalized_data_full_population_with_foldId"]
    REQUIRES_FIELDS = ["company_data_rf"]

    def __init__(self, cut_dates):
        self.cut_dates = sorted(
            [pd.Timestamp(date) for date in cut_dates] + [pd.Timestamp("1900-12-31"), pd.Timestamp("2100-12-31")])
        self.data = None

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        df = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        df["fold_id"] = pd.cut(df["date"], self.cut_dates, labels=pd.np.arange(len(self.cut_dates) - 1))
        df["fold_id"] = pd.Series(df["fold_id"].cat.codes).astype(int)
        self.data = df.set_index(["date", "ticker"]).reset_index()
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}

    @classmethod
    def get_default_config(cls):
        return {"cut_dates": ["2003-12-31", "2007-09-28", "2011-06-30", "2014-12-31", "2017-11-30"]}


class AddFoldIdToGANResultDataWeekly(AddFoldIdToGANResultData):
    PROVIDES_FIELDS = ["normalized_data_full_population_with_foldId",
                       "normalized_data_full_population_with_foldId_weekly"]
    REQUIRES_FIELDS = ["company_data_rf", "company_data_rf_weekly"]

    def __init__(self, cut_dates):
        self.weekly_data = None
        AddFoldIdToGANResultData.__init__(self, cut_dates)

    def do_step_action(self, **kwargs):
        monthly_df = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        weekly_df = kwargs[self.__class__.REQUIRES_FIELDS[1]].copy(deep=True)
        monthly_df['date'] = monthly_df['date'].apply(pd.Timestamp)
        weekly_df['date'] = weekly_df['date'].apply(pd.Timestamp)

        monthly_df["fold_id"] = pd.cut(monthly_df["date"], self.cut_dates, labels=np.arange(len(self.cut_dates) - 1))
        monthly_df["fold_id"] = pd.Series(monthly_df["fold_id"].cat.codes).astype(int)
        self.data = monthly_df.set_index(["date", "ticker"]).reset_index()

        weekly_df["fold_id"] = pd.cut(weekly_df["date"], self.cut_dates, labels=np.arange(len(self.cut_dates) - 1))
        weekly_df["fold_id"] = pd.Series(weekly_df["fold_id"].cat.codes).astype(int)
        self.weekly_data = weekly_df.set_index(["date", "ticker"]).reset_index()
        return {'normalized_data_full_population_with_foldId': self.data,
                'normalized_data_full_population_with_foldId_weekly': self.weekly_data}

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data,
                self.__class__.PROVIDES_FIELDS[1]: self.weekly_data}


ConsolidateGANResultsWeekly_params = {'params': {},
                                      'class': ConsolidateGANResultsWeekly,
                                      'start_date': RUN_DATE,
                                      'provided_data': {
                                          'company_data_rf': construct_destination_path('merge_gan_results'),
                                          'company_data_rf_weekly': construct_destination_path('merge_gan_results')
                                          },
                                      'required_data': {
                                          'company_data': construct_required_path('save_gan_inputs/save_gan_inputs',
                                                                                  'company_data'),
                                          'company_data_weekly': construct_required_path(
                                              'save_gan_inputs/save_gan_inputs', 'company_data_weekly'),

                                          }}


cut_dates_params =  {
        'cut_dates' : ["2003-10-31", "2007-08-31", "2011-06-30", "2015-04-30", "2019-02-28", "2023-02-03"]
    }

AddFoldIdToGANResultDataWeekly_params = {'params': cut_dates_params,
                                         'class': AddFoldIdToGANResultDataWeekly,
                                         'start_date': RUN_DATE,
                                         'provided_data': {
                                             'normalized_data_full_population_with_foldId': construct_destination_path(
                                                 'merge_gan_results'),
                                             'normalized_data_full_population_with_foldId_weekly': construct_destination_path(
                                                 'merge_gan_results')
                                             },
                                         'required_data': {
                                             'company_data_rf': construct_required_path('merge_gan_results',
                                                                                        'company_data_rf'),
                                             'company_data_rf_weekly': construct_required_path('merge_gan_results',
                                                                                               'company_data_rf_weekly'),

                                             }}




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