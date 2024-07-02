import pandas as pd
from market_timeline import marketTimeline
import pandas as pd
import numpy as np
from datetime import datetime
import os
from abc import ABC, abstractmethod
from google.cloud import storage
#import statsmodels.api as sm
from google.cloud import storage
import gcsfs
from collections import defaultdict
import tempfile
from joblib import load
import io
import joblib
import shap

current_date = datetime.now().date()

growth_factors = [
    "revenue_growth",
    "grossmargin",
    "netmargin_indgrp",
    "netmargin",
    "capex_yield",
    "log_mktcap",
    "revenue_yield",
]
quality_factors = [
    "currentratio",
    "fcf_yield",
    "debt2equity_indgrp",
    "debt2equity",
    "amihud",
    "current_to_total_assets",
    "retearn_to_total_assets",
]
macro_factors = [
    "DCOILWTICO",
    "T5YFFM",
    "IPDCONGD",
    "DEXUSUK",
    "RPI",
    "USTRADE",
    "RETAILx",
    "AAAFFM",
    "GS10",
    "HWI",
    "GS5",
    "CUSR0000SAC",
    "CPFF",
    "CPITRNSL",
    "T10YFFM",
    "T1YFFM",
    "WPSID62",
    "CUSR0000SA0L2",
]
momentum_fast_factors = [
    "STOCHRSI_FASTK_63_15_10",
    "STOCHRSI_FASTD_14_5_3",
    "volatility_126_indgrp",
    "volatility_126",
    "volatility_21",
    "VIXCLS",
]
momentum_slow_factors = [
    "ret_10B",
    "macd_diff_indgrp",
    "momentum",
    "ret_126B",
    "ret_1B",
    "WILLR_14",
    "macd_diff_ConsumerStaples",
    "macd_diff",
    "bm_Financials",
    "bm_Utilities",
    "bm_indgrp",
    "bm",
]
trend_following_factors = [
    "PPO_21_126",
    "ADX_14_MA_3",
    "PPO_12_26",
    "PPO_21_126_ConsumerDiscretionary",
    "PPO_3_14",
    "rel_to_high",
    "PPO_21_126_Industrials",
    "PPO_21_126_Energy",
    "PPO_21_126_InformationTechnology",
    "PPO_21_126_indgrp",
    "PPO_12_26_indgrp",
]
value_factors = [
    "pe1",
    "ebitdamargin",
    "investments_yield",
    "ebitda_to_ev_indgrp",
    "ebitda_to_ev",
    "pe_indgrp",
    "divyield_indgrp",
    "divyield_Industrials",
    "divyield",
    "gp2assets",
    "payoutratio",
    "divyield_ConsumerStaples",
]
other_factors = [
    "std_turn",
    "EWJ_volume",
    "ret_63B_indgrp",
    "overnight_return",
    "EWG_close",
    "log_avg_dollar_volume",
    "ret_252B",
    "log_dollar_volume",
    "SPY_beta",
    "maxret",
    "SPY_close",
    "ret_63B",
    "ret_2B",
]

MAPPING_DICTIONARY = {
    "Growth": growth_factors,
    "Quality": quality_factors,
    "Macro": macro_factors,
    "Momentum Fast": momentum_fast_factors,
    "Momentum Slow": momentum_slow_factors,
    "Trend Following": trend_following_factors,
    "Value": value_factors,
    "Other Factors": other_factors,
}

def factor_grouping(feature_importance, sub_factor_map):
    shap_df_21D_full = feature_importance
    shap_df_21D = feature_importance.iloc[-1]
    raw_data = feature_importance
    shap_variables = shap_df_21D[
        abs(shap_df_21D).sort_values(ascending=False).index.tolist()
    ]

    sub_data_dictionary = sub_factor_map

    subfactor_dictionary_21 = {}
    subfactor_dictionary_5 = {}

    longitudinal_subfactor_exposure = pd.DataFrame([])

    longitudinal_subfactor_exposure_raw = pd.DataFrame([])

    for shap_group in sub_data_dictionary:
        subfactor_dictionary_21 = {}
        subfactor_dictionary_5 = {}
        overlapping_features = [
            i
            for i in sub_data_dictionary[shap_group]
            if i in shap_df_21D.index.tolist()
        ]

        subgroup_long = abs(
            shap_df_21D_full.reindex(overlapping_features, axis="columns")
        ).sum(axis=1)

        subgroup_long_raw = shap_df_21D_full.reindex(
            overlapping_features, axis="columns"
        ).sum(axis=1)

        longitudinal_subfactor_exposure[shap_group] = subgroup_long.values
        longitudinal_subfactor_exposure_raw[shap_group] = subgroup_long_raw.values

        longitudinal_subfactor_exposure_raw.index = feature_importance.index
        longitudinal_subfactor_exposure.index = feature_importance.index

    return longitudinal_subfactor_exposure, longitudinal_subfactor_exposure_raw

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
            model_data = fp.read()
        model_file = io.BytesIO(model_data)
        return joblib.load(model_file)

def set_models(model, timestamp):
    return {key: model[timestamp][key] for key in model[timestamp]}
def normalize_weights_from_model(meta_model_weights):
    weight_sum = sum(meta_model_weights.values())
    normalized_weights = {k: v / weight_sum for k, v in meta_model_weights.items()}
    return normalized_weights


def dynamic_concat(complete_econ_shap_dictionary, rolling_value_shaps, x_econ, ensemble_weights):
    # Initialize an empty list to store the results
    results = []

    # Iterate over each split in rolling_value_shaps
    for i, shap in enumerate(rolling_value_shaps):
        # Apply the unfold_subcomponent function and collect the result
        result = unfold_subcomponent(complete_econ_shap_dictionary, shap, x_econ, ensemble_weights)
        results.append(result)

    # Concatenate all results together
    concatenated_results = pd.concat(results)

    return concatenated_results

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

def set_rolling_models(x_rolled_recent, models_recent, ensemble_weights):
    explainers = ShapProcessor.create_explainers(models_recent, x_rolled_recent)
    expected_values = {key: explainer.expected_value for key, explainer in explainers.items()}

    for key, value in expected_values.items():
        if isinstance(value, list):
            expected_values[key] = value[1]

    weighted_expected_value = sum(expected_values[key] * ensemble_weights[key] for key in ensemble_weights)
    rolling_shaps = ShapProcessor.get_shap_values(explainers, x_rolled_recent)

    return rolling_shaps, weighted_expected_value


def unfold_subcomponent(econ_models, rolling_models, x_econ, ensemble_weights):
    weighted_shap_econ = (econ_models['enet'] * ensemble_weights['enet']) + \
                         (econ_models['et'] * ensemble_weights['et']) + (econ_models['gbm'] * ensemble_weights['gbm']) + \
                         (econ_models['lasso'] * ensemble_weights['lasso']) + (
                                     econ_models['ols'] * ensemble_weights['ols']) + \
                         (econ_models['rf'] * ensemble_weights['rf'])
    meaned_shaps_econ = (econ_models['rf'] + econ_models['et'] + econ_models['gbm'] + weighted_shap_econ) / 4

    x_dropped = [value for value in rolling_models['ols'].columns.tolist() if
                 value in meaned_shaps_econ.columns.tolist()]
    mean_shaps_econ_dropped_duplicates = meaned_shaps_econ.drop(x_dropped, axis=1)
    mean_shaps_econ_dropped_duplicates = mean_shaps_econ_dropped_duplicates.reindex(rolling_models['ols'].index).fillna(
        0)

    # ols
    all_feature_ols = rolling_models['ols'].join(mean_shaps_econ_dropped_duplicates)

    # rf
    all_feature_rf = rolling_models['rf'].join(mean_shaps_econ_dropped_duplicates)

    # gbm
    all_feature_gbm = rolling_models['gbm'].join(mean_shaps_econ_dropped_duplicates)

    # enet
    all_feature_enet = rolling_models['enet'].join(mean_shaps_econ_dropped_duplicates)

    # lasso
    all_feature_lasso = rolling_models['lasso'].join(mean_shaps_econ_dropped_duplicates)

    # et
    all_feature_et = rolling_models['et'].join(mean_shaps_econ_dropped_duplicates)

    # Final Calculation

    final_shap = (rolling_models['enet'] * ensemble_weights['enet']) + \
                 (rolling_models['et'] * ensemble_weights['et']) + (rolling_models['gbm'] * ensemble_weights['gbm']) + \
                 (rolling_models['lasso'] * ensemble_weights['lasso']) + (
                             rolling_models['ols'] * ensemble_weights['ols']) + \
                 (rolling_models['rf'] * ensemble_weights['rf'])

    econ_models['et'].set_index(x_econ.index, inplace=True)
    econ_models['rf'].set_index(x_econ.index, inplace=True)
    econ_models['lasso'].set_index(x_econ.index, inplace=True)
    econ_models['ols'].set_index(x_econ.index, inplace=True)
    econ_models['gbm'].set_index(x_econ.index, inplace=True)
    econ_models['enet'].set_index(x_econ.index, inplace=True)

    et_shap_df_reindexed = econ_models['et'].reindex(rolling_models['et'].index).fillna(0)
    lasso_shap_df_reindexed = econ_models['lasso'].reindex(rolling_models['lasso'].index).fillna(0)
    enet_shap_df_reindexed = econ_models['enet'].reindex(rolling_models['enet'].index).fillna(0)
    ols_shap_df_reindexed = econ_models['ols'].reindex(rolling_models['ols'].index).fillna(0)
    gbm_shap_df_reindexed = econ_models['gbm'].reindex(rolling_models['gbm'].index).fillna(0)
    rf_shap_df_reindexed = econ_models['rf'].reindex(rolling_models['rf'].index).fillna(0)

    rf_shap_econ_multiplied = final_shap['rf'][-1] * rf_shap_df_reindexed
    et_shap_econ_multiplied = final_shap['et'][-1] * et_shap_df_reindexed
    gbm_shap_econ_multiplied = final_shap['gbm'][-1] * gbm_shap_df_reindexed
    ensemble_multiplied = final_shap['ensemble'][-1] * (rf_shap_econ_multiplied + et_shap_econ_multiplied +
                                                        gbm_shap_econ_multiplied)

    unraveled_full_econ = (rf_shap_econ_multiplied + et_shap_econ_multiplied +
                           gbm_shap_econ_multiplied + ensemble_multiplied)

    final_shap_dropped_ensemble = final_shap.drop(['rf', 'ensemble', 'gbm', 'et'], axis=1)
    x_dropped = [value for value in rolling_models['ols'].columns.tolist() if
                 value in unraveled_full_econ.columns.tolist()]
    final_shap_dropped_duplicates = final_shap_dropped_ensemble.drop(x_dropped, axis=1)
    final_unraveled = final_shap_dropped_duplicates.join(unraveled_full_econ)
    return final_unraveled



############## AIRFLOW RELATED


def construct_required_path(step, file_name):
    return (
        "gs://{}/calibration_data/live"
        + "/{}/".format(step)
        + "{}.csv".format(file_name)
    )


def construct_destination_path(step):
    return "gs://{}/calibration_data/live" + "/{}/".format(step) + "{}.csv"


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

def read_csv_in_chunks(gcs_path, batch_size=10000, project_id='dcm-prod-ba2f'):
    """
    Reads a CSV file from Google Cloud Storage in chunks.
    Parameters:
    - gcs_path (str): The path to the CSV file on GCS.
    - batch_size (int, optional): The number of rows per chunk. Default is 10,000.
    - project_id (str, optional): The GCP project id. Default is 'dcm-prod-ba2f'.
    Yields:
    - pd.DataFrame: The data chunk as a DataFrame.
    """
    # Create GCS file system object
    fs = gcsfs.GCSFileSystem(project=project_id)

    # Open the GCS file for reading
    with fs.open(gcs_path, 'r') as f:
        # Yield chunks from the CSV
        for chunk in pd.read_csv(f, chunksize=batch_size, index_col=0):
            yield chunk


def airflow_wrapper(**kwargs):
    params = kwargs['params']

    # Read all required data into step_action_args dictionary
    step_action_args = {
        k: pd.read_csv(v.format(os.environ['GCS_BUCKET']), index_col=0)
        for k, v in kwargs['required_data'].items()
    }
    #print(f'Executing step action with args {step_action_args}')

    # Execute do_step_action method
    data_outputs = kwargs['class'](**params).do_step_action(**step_action_args)

    print('Your Params are')
    print(params)

    # If the method doesn't return a dictionary (for classes returning just a single DataFrame)
    # convert it into a dictionary for consistency
    if not isinstance(data_outputs, dict):
        data_outputs = {list(kwargs['provided_data'].keys())[0]: data_outputs}

    # Save each output data to its respective path on GCS
    for data_key, data_value in data_outputs.items():
        if data_key in kwargs['provided_data']:
            gcs_path = kwargs['provided_data'][data_key].format(
                os.environ['GCS_BUCKET'], data_key
            )
            print('Here is the path')
            print(gcs_path)
            data_value.to_csv(gcs_path)


class RollingComponentCalculation(object):

    def __init__(self, y_col_rolling, X_cols_rolling, ensemble_weights, rolling_models_path, truncating_date):
        self.y_col_rolling = y_col_rolling
        self.X_cols_rolling = X_cols_rolling
        self.ensemble_weights = ensemble_weights
        self.model_loader = ModelLoader(
            base_dir=rolling_models_path['BASE_KEY'],
            leaf_path=rolling_models_path['leaf_path'],
            bucket=rolling_models_path['bucket']
        )
        self.truncating_date = truncating_date

    def _prep_internal_data(self, df):
        y, x = df[self.y_col_rolling], df[self.X_cols_rolling]
        x['date'] = x['date'].apply(pd.Timestamp)
        x.set_index(['date', 'ticker'], inplace=True)
        return x, y

    def neutral_or_small_cap_calculation(self, df, model_key='results_g'):

        if model_key not in ['results_g', 'results_v']:
            raise ValueError('Model can only be of type results_v or results_g')

        results_model = self.model_loader.load_single_model(model_key)

        x, y = self._prep_internal_data(df)
        recent_key_dict = {k: pd.Timestamp(k).strftime('%Y-%m-%d') for k in results_model.keys()}
        all_shap_values = []
        date_keys = sorted(recent_key_dict.keys())

        # MANUAL EDIT 2015-01-01
        x_recent = x[(x.index.get_level_values(0) >= self.truncating_date)]
        print('x_rolled_recent_g shape:', x_recent.shape)

        for i, key in enumerate(date_keys):
            if i == 0:
                start_date = x_recent.index.get_level_values(0).min()
            else:

                # Determine the start and end date for the current slice
                start_date = recent_key_dict[key]

            if i + 1 < len(date_keys):  # if there's a next date in the list
                end_date = recent_key_dict[date_keys[i + 1]]
            else:
                end_date = x.index.get_level_values(0).max() + pd.Timedelta(1,
                                                                            'D')  # next day after the last date in the df

            print('start_date=', start_date, 'end_date=', end_date)
            # Slice the x_rolled_recent dataframe for the current date range

            x_slice = x_recent[(x_recent.index.get_level_values(0) >= start_date) &
                               (x_recent.index.get_level_values(0) < end_date)]

            # Get the models for the current date key
            models_recent = set_models(results_model, key)

            # Compute shap values for the sliced data normalize_weights_from_model(growth_meta_weights)
            rolling_shaps, weighted_expected_value = set_rolling_models(x_slice, models_recent,
                                                                        self.ensemble_weights)

            all_shap_values.append(rolling_shaps)
        return all_shap_values, weighted_expected_value

    def large_cap_calculation(self, df, model_key='results_lg'):
        if model_key not in ['results_lg', 'results_lv']:
            raise ValueError('Model can only be of type results_lv or results_lg')

        results_model = self.model_loader.load_single_model(model_key)
        x, y = self._prep_internal_data(df)
        recent_key_dict = {k: pd.Timestamp(k).strftime('%Y-%m-%d') for k in results_model.keys()}
        all_shap_values = []
        date_keys = sorted(recent_key_dict.keys())

        # MANUAL EDIT 2015-01-01
        x_recent = x[(x.index.get_level_values(0) >= self.truncating_date)]
        print('x_rolled_recent_g shape:', x_recent.shape)
        print(date_keys)

        for i, (key_growth, value_growth) in enumerate(recent_key_dict.items()):
            # for i, key in enumerate(date_keys):
            if i == 0:
                start_date = x_recent.index.get_level_values(0).min()

            else:

                # Determine the start and end date for the current slice
                start_date = value_growth

            if i + 1 < len(date_keys):  # if there's a next date in the list
                end_date = recent_key_dict[date_keys[i + 1]]
            else:
                end_date = x.index.get_level_values(0).max() + pd.Timedelta(1,
                                                                            'D')  # next day after the last date in the df
            print('Growth Dates', 'start_date=', start_date, 'end_date=', end_date)

            # Slice the x_rolled_recent dataframe for the current date range

            x_slice = x_recent[(x_recent.index.get_level_values(0) >= start_date) &
                               (x_recent.index.get_level_values(0) < end_date)]

            # Get the models for the current date key
            models_recent = set_models(results_model, key_growth)

            # Compute shap values for the sliced data
            rolling_shaps, weighted_expected_value = set_rolling_models(x_slice, models_recent,
                                                                        self.ensemble_weights)

            all_shap_values.append(rolling_shaps)

        return all_shap_values, weighted_expected_value

    def calculation(self, df, key):
        if key not in ['results_g', 'results_v', 'results_lg', 'results_lv']:
            raise ValueError('feature impact can only be calculated for neutral and large_cap')

        if key in ['results_g', 'results_v']:
            return self.neutral_or_small_cap_calculation(df, key)
        else:
            return self.large_cap_calculation(df, key)


class RollingModelUnraveling(object):
    PROVIDES_FIELDS = [
        'factor_exposure_monthly_growth',
        'factor_exposure_monthly_value',
        'factor_exposure_monthly_lcgrowth',
        'factor_exposure_monthly_lcvalue',
    ]
    REQUIRES_FIELDS = [
        'econ_rf',
        'econ_gbm',
        'econ_lasso',
        'econ_ols',
        'econ_et',
        'econ_enet',
        'x_econ'
    ]

    def __init__(self, y_col_rolling, X_cols_rolling, ensemble_weights,
                 rolling_models_path, mapping_dictionary, truncating_date):
        self.y_col_rolling = y_col_rolling
        self.X_cols_rolling = X_cols_rolling
        self.ensemble_weights = ensemble_weights
        self.rolling_models_path = rolling_models_path
        self.mapping_dictionary = mapping_dictionary
        self.truncating_date = truncating_date

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self, **kwargs):
        pass

    def _set_econ_shap_df(self, **kwargs):
        econ_models = ['rf',
                       'gbm',
                       'lasso',
                       'ols',
                       'et',
                       'enet']

        # econ_mapper = dict(zip(econ_models,REQUIRES_FIELDS))

        return {k: kwargs[self.__class__.REQUIRES_FIELDS[i]].copy(deep=True).set_index(['date', 'ticker']) for i, k in
                enumerate(econ_models)}

    def _apply_timestamp(self, **kwargs):
        print(self.__class__.REQUIRES_FIELDS)
        for i in range(len(self.__class__.REQUIRES_FIELDS)):
            kwargs[self.__class__.REQUIRES_FIELDS[i]].reset_index(inplace=True)
            kwargs[self.__class__.REQUIRES_FIELDS[i]]['date'] = kwargs[self.__class__.REQUIRES_FIELDS[i]]['date'].apply(
                pd.Timestamp)

    def do_step_action(self, **kwargs):
        self._apply_timestamp(**kwargs)
        complete_econ_shap_dictionary = self._set_econ_shap_df(**kwargs)
        security_master = kwargs['security_master']
        security_master_dict = security_master.set_index(['dcm_security_id'])['ticker'].to_dict()
        x_econ = kwargs[self.__class__.REQUIRES_FIELDS[6]].copy(deep=True).set_index(['date', 'ticker'])

        r1k_filtered_model_map = ['growth', 'value', 'largecap_value', 'largecap_growth']
        r1k_filtered_dfs = [kwargs['r1k_neutral_normal_models_with_foldId_growth'],
                            kwargs['r1k_neutral_normal_models_with_foldId_value'],
                            kwargs['r1k_neutral_normal_models_with_foldId_largecap_value'],
                            kwargs['r1k_neutral_normal_models_with_foldId_largecap_growth'],
                            ]
        r1k_model_keys = ['results_g', 'results_v', 'results_lv', 'results_lg']

        rmc = RollingComponentCalculation(self.y_col_rolling,
                                          self.X_cols_rolling,
                                          self.ensemble_weights,
                                          self.rolling_models_path,
                                          truncating_date=self.truncating_date)
        self.final_results = {}

        for model, df, key in zip(r1k_filtered_model_map, r1k_filtered_dfs, r1k_model_keys):
            all_shap_values, weighted_expected_value = rmc.calculation(df, key)
            unraveled_shap = dynamic_concat(complete_econ_shap_dictionary,
                                            all_shap_values,
                                            x_econ,
                                            ensemble_weights)
            unraveled_shap = unraveled_shap[(unraveled_shap.index.get_level_values(0) > '2024-02-01')]
            unraveled_shap.reset_index(inplace=True)
            unraveled_shap['ticker'] = unraveled_shap['ticker'].replace(security_master_dict)
            unraveled_shap.set_index(['date', 'ticker'], inplace=True)
            grouped_exposure, grouped_raw = factor_grouping(unraveled_shap, self.mapping_dictionary)

            self.final_results[model] = grouped_exposure

        return self._get_additional_step_results()

    def _get_additional_step_results(self):

        return {
            "factor_exposure_monthly_growth": self.final_results['growth'],
            "factor_exposure_monthly_value": self.final_results['value'],
            "factor_exposure_monthly_lcgrowth": self.final_results['largecap_growth'],
            "factor_exposure_monthly_lcvalue": self.final_results['largecap_value'],

        }





rolling_models_data_path ={'BASE_KEY':'calibration_data/live/saved_rolling_models_gan',
'bucket':'dcm-prod-ba2f-us-dcm-data-test',
'leaf_path':'{}.joblib'}

y_col_rolling = ["future_ret_21B_std"]
X_cols_rolling = ['date', 'ticker','PPO_12_26_indgrp', 'et', 'IPDCONGD', 'ebitda_to_ev_indgrp', 'revenue_yield', 'T5YFFM', 'rf', 'ret_1B',
             'overnight_return', 'grossmargin', 'netmargin_indgrp', 'maxret', 'DCOILWTICO', 'currentratio',
             'ensemble', 'EWG_close', 'STOCHRSI_FASTK_63_15_10', 'bm', 'PPO_21_126', 'current_to_total_assets',
             'ret_63B', 'revenue_growth', 'PPO_21_126_Industrials', 'ret_2B', 'PPO_21_126_Energy', 'ebitdamargin',
             'debt2equity_indgrp', 'CPFF', 'ret_126B', 'log_dollar_volume', 'debt2equity', 'investments_yield',
             'fcf_yield', 'volatility_126_indgrp', 'macd_diff_indgrp', 'STOCHRSI_FASTD_14_5_3', 'PPO_21_126_indgrp',
             'amihud', 'VIXCLS', 'ebitda_to_ev', 'pe1', 'macd_diff_ConsumerStaples', 'T1YFFM', 'volatility_21',
             'gbm', 'netmargin', 'ret_10B', 'pe_indgrp', 'divyield', 'WILLR_14', 'HWI', 'CUSR0000SAC',
             'divyield_indgrp', 'ret_63B_indgrp', 'payoutratio', 'EWJ_volume', 'PPO_12_26', 'PPO_3_14',
             'retearn_to_total_assets', 'std_turn', 'log_avg_dollar_volume', 'ret_252B', 'DEXUSUK', 'rel_to_high',
             'ADX_14_MA_3', 'macd_diff', 'capex_yield', 'bm_indgrp', 'PPO_21_126_InformationTechnology', 'gp2assets',
             ]


ensemble_weights = {'enet': 0.03333333333333333,'et': 0.3,'gbm': 0.2,'lasso': 0.03333333333333333,
                            'ols': 0.03333333333333333,'rf': 0.4}


rolling_params = {'y_col_rolling':y_col_rolling,
                  'X_cols_rolling':X_cols_rolling,
                  'ensemble_weights':ensemble_weights,
                  'rolling_models_path':rolling_models_data_path,
                  'truncating_date':'2024-01-01',
                  'mapping_dictionary':MAPPING_DICTIONARY
}

unraveling_rolling_model_dataformatter = DataFormatter(
    class_=RollingModelUnraveling,
    class_parameters=rolling_params,
    provided_data={
        'FactorIntepretation': [
            'factor_exposure_monthly_growth',
            'factor_exposure_monthly_value',
            'factor_exposure_monthly_lcgrowth',
            'factor_exposure_monthly_lcvalue',
        ]
    },
    required_data={
        'EconInterpretation': [
            'econ_rf',
            'econ_gbm',
            'econ_lasso',
            'econ_ols',
            'econ_et',
            'econ_enet',
            'x_econ'
        ],
        'AddFinalFoldId': [
            'r1k_neutral_normal_models_with_foldId_growth',
            'r1k_neutral_normal_models_with_foldId_value',
            'r1k_neutral_normal_models_with_foldId_largecap_value',
            'r1k_neutral_normal_models_with_foldId_largecap_growth',
        ],
        'DataPull': [
            'security_master',
        ]
    },
)

if __name__ == "__main__":
    os.environ['GCS_BUCKET'] = 'dcm-prod-ba2f-us-dcm-data-test'
    filter_r1k_weekly_data = unraveling_rolling_model_dataformatter()
    airflow_wrapper(**filter_r1k_weekly_data)