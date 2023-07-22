import numpy as np
SEED = 20190213
np.random.seed(SEED)
import gc
import pandas as pd

from commonlib.util_functions import create_directory_if_does_not_exists
from etl_workflow_steps import StatusType, BaseETLStage, compose_main_flow_and_engine_for_task
from calibrations.common.calibration_logging import calibration_logger

from calibrations.common.config_functions import get_config
from calibrations.core.calibration_taskflow_task import CalibrationTaskflowTask

from trading_calendars import get_calendar
import statsmodels.api as sm
import sqlalchemy as sa
from calibrations.data_pullers.data_pull import get_redshift_engine
import os
from google.cloud import storage
import io
import pandas as pd
from commonlib.commonStatTools.simple_ml_module import train_ml_model
from sklearn.ensemble.partial_dependence import partial_dependence, plot_partial_dependence
from sklearn.model_selection import GroupKFold, GridSearchCV, cross_val_score
from sklearn.linear_model import LinearRegression, LassoCV, RidgeCV, ElasticNetCV
from sklearn.linear_model.coordinate_descent import _alpha_grid
from sklearn.ensemble import RandomForestRegressor, ExtraTreesRegressor, GradientBoostingRegressor
from sklearn.externals.joblib import dump, load
import s3io
from commonlib.util_functions import nonnull_lb, nonnull_ub, pick_trading_month_dates, \
     pick_trading_week_dates, safe_log
from calibrations.feature_builders.digitizer import Digitization
from scipy.stats import norm as normal
from collections import defaultdict

from calibrations.core.calibration_taskflow_task import CalibrationTaskflowTask
from etl_workflow_steps import StatusType
from calibrations.common.calibration_logging import calibration_logger

from commonlib.market_timeline import marketTimeline

import tempfile
from time import time

from io import StringIO
from pipelines.prices_ingestion.etl_workflow_aux_functions import build_s3_url
import subprocess

from calibrations.quantamental_ml.tii_model_generation_py2 import *
from calibrations.common.calibrations_utils import get_local_dated_dir
from calibrations.common.taskflow_params import TaskflowPipelineRunMode
from numbers import Number

from commonlib.security_master_utils.special_mappings import special_dcm_to_ib_symbol_map

DEFAULT_MARKETCAP = 2000000000.0
NORMALIZATION_CONSTANT = 0.67448975019608171
_SMALL_EPSILON = np.finfo(np.float64).eps

INTUITION_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../../"))
TEST_DATA_DIR = INTUITION_DIR.replace("dcm-intuition", "test_data")
UNIT_TEST_GAN_BASE = "processes/data_processing/calibrations/quantamental_ml/gan_data"
UNIT_TEST_MODEL_BASE = "processes/data_processing/calibrations/quantamental_ml/models"

FILTER_MODES = ["growth", "value", "largecap_growth", "largecap_value"]


def mad(arr):
    med = np.lib.nanfunctions._nanmedian1d(arr)

    return np.lib.nanfunctions._nanmedian1d(np.abs(arr - med))/NORMALIZATION_CONSTANT


class DummyMergedData(CalibrationTaskflowTask):
    '''

    Merges the data elements for the Quantamental ML calibration pipeline

    '''

    PROVIDES_FIELDS = ["merged_data"]


    def __init__(self, file_name_location, key):
        self.key = key
        self.file_name_location = file_name_location
        self.data = None
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        store = pd.HDFStore(self.file_name_location)
        monthly_merged_data = store[self.key]
        store.close()
        self.data = monthly_merged_data
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}


    @classmethod
    def get_default_config(cls):
        return {"file_name_location" : "D:/DCM/TIAA weight/pipeline_tests/DerivedSimplePriceFeatureProcessing.h5",
                "key" : "/pandas/merged_data"}

class DummyMergedEconData(CalibrationTaskflowTask):
    '''

    Merges the data elements for the Quantamental ML calibration pipeline

    '''

    PROVIDES_FIELDS = ["merged_data_econ_industry"]


    def __init__(self, file_name_location, key):
        self.key = key
        self.file_name_location = file_name_location
        self.data = None
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        store = pd.HDFStore(self.file_name_location)
        monthly_merged_data = store[self.key]
        store.close()
        self.data = monthly_merged_data
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}


    @classmethod
    def get_default_config(cls):
        return {"file_name_location" : "D:/DCM/TIAA weight/pipeline_tests/DerivedSimplePriceFeatureProcessing.h5",
                "key" : "/pandas/merged_data"}


class DummyFinalModelData(CalibrationTaskflowTask):
    '''

    Merges the data elements for the Quantamental ML calibration pipeline

    '''

    PROVIDES_FIELDS = ["signal_v", "signal_g"]


    def __init__(self, file_name_location, key):
        self.key = key
        self.file_name_location = file_name_location
        self.data = None
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        store = pd.HDFStore(self.file_name_location)
        monthly_merged_data = store[self.key]
        store.close()
        self.data = monthly_merged_data
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}


    @classmethod
    def get_default_config(cls):
        return {"file_name_location" : "D:/DCM/TIAA weight/pipeline_tests/DerivedSimplePriceFeatureProcessing.h5",
                "key" : "/pandas/merged_data"}



class QuantamentalMerge(CalibrationTaskflowTask):
    '''

    Merges the data elements for the Quantamental ML calibration pipeline

    '''

    PROVIDES_FIELDS = ["merged_data"]
    REQUIRES_FIELDS = ["daily_price_data", "fundamental_features", "target_returns", "talib_stochrsi_indicator_data",
                       "volatility_data", "talib_willr_indicator_data", "talib_adx_indicator_data",
                       "talib_ppo_indicator_data", "beta_data", "macd_indicator_data", "correlation_data",
                       "dollar_volume_data", "overnight_return_data", "past_return_data", "talib_stoch_indicator_data",
                       "talib_stochf_indicator_data", "talib_trix_indicator_data", "talib_ultosc_indicator_data",
                       "security_master"]

    def __init__(self, apply_log_vol=True, start_date=None, end_date=None):
        self.data = None
        self.apply_log_vol = apply_log_vol
        self.start_date = pd.Timestamp(start_date) if start_date else None
        self.end_date = pd.Timestamp(end_date) if end_date else None
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        gc.collect()
        try:
            lcl = globals()
            for k in self.REQUIRES_FIELDS:
                lcl[k] = kwargs[k]

            merged_data = overnight_return_data.copy(deep=True)
            merged_data["date"] = pd.DatetimeIndex(merged_data["date"]).normalize()
            self.start_date = self.start_date or merged_data["date"].min()
            self.end_date = self.end_date or merged_data["date"].max()
            merged_data = merged_data[(merged_data["date"]>=self.start_date) & (merged_data["date"]<=self.end_date)]
            for df in [talib_stochrsi_indicator_data, volatility_data, talib_willr_indicator_data,
                       talib_adx_indicator_data, talib_ppo_indicator_data, beta_data, macd_indicator_data,
                       correlation_data, dollar_volume_data, past_return_data,
                       talib_stoch_indicator_data, talib_stochf_indicator_data, talib_trix_indicator_data,
                       talib_ultosc_indicator_data, fundamental_features, target_returns]:
                current_df = df.copy(deep=True)
                if isinstance(current_df.index, pd.core.index.MultiIndex):
                    current_df = current_df.reset_index()
                current_df["date"] = pd.DatetimeIndex(current_df["date"]).normalize()
                merged_data["ticker"] = merged_data["ticker"].astype(int)
                current_df["ticker"] = current_df["ticker"].astype(int)
                merged_data = pd.merge(merged_data, current_df, how="left", on=["date", "ticker"])

            merged_data = merged_data.drop(["8554_correl_rank"], axis=1)
            merged_data = merged_data.drop(["8554_beta_rank"], axis=1)

            if self.apply_log_vol:
                for col in [c for c in merged_data.columns if str(c).startswith("volatility_")]:
                    merged_data[col] = pd.np.log(merged_data[col])
            merged_data["log_avg_dollar_volume"] = pd.np.log(merged_data["avg_dollar_volume"])

            merged_data["ticker"] = merged_data["ticker"].astype(int)
            merged_data["momentum"] = merged_data["ret_252B"] - merged_data["ret_21B"]
            index_cols = [c for c in merged_data.columns if str(c).startswith("index")]
            good_cols = list(set(merged_data.columns) - set(index_cols))
            merged_data = merged_data[good_cols]

            merged_data.columns = map(lambda x:x.replace("8554", "SPY"), merged_data.columns)
            merged_data["SPY_beta"] = merged_data["SPY_beta"].astype(float)
            #TODO: added by Jack 2020/04/30 for Live mode. Live mode should use the latest available data without date shift
            if self.task_params.run_mode==TaskflowPipelineRunMode.Calibration:
                merged_data["date"] = merged_data["date"].apply(lambda x:marketTimeline.get_trading_day_using_offset(x, 1))
            self.data = merged_data
        finally:
            lcl = globals()
            for k in self.REQUIRES_FIELDS:
                lcl.pop(k,None)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}


    @classmethod
    def get_default_config(cls):
        return {"apply_log_vol": True, "start_date": "2018-01-01", "end_date": "2019-06-15"}



class QuantamentalMergeEconIndustry(CalibrationTaskflowTask):
    '''

    Merges the data elements for the Quantamental ML calibration pipeline

    '''

    PROVIDES_FIELDS = ["merged_data_econ_industry", "econ_data_final"]
    REQUIRES_FIELDS = ["monthly_merged_data_single_names", "security_master", "industry_average",
                       "sector_average", "transformed_econ_data"]

    def __init__(self, industry_cols, security_master_cols, sector_cols, key_sectors, econ_cols, start_date, end_date,
                 normalize_econ=True):
        self.data = None
        self.econ_data_final = None
        self.sector_cols = sector_cols
        self.key_sectors = key_sectors
        self.start_date =  pd.Timestamp(start_date) if start_date else None
        self.end_date = pd.Timestamp(end_date) if end_date else None
        self.industry_cols = industry_cols
        self.econ_cols = econ_cols
        self.security_master_cols = security_master_cols
        self.normalize_econ = normalize_econ
        CalibrationTaskflowTask.__init__(self)

    def _remove_bad_cols_from_econ_data(self, econ_data):
        econ_data = econ_data.drop("CLAIMSx", axis=1) # bad column
        econ_data = econ_data.drop("S&P 500", axis=1) # bad column
        econ_data = econ_data.drop("AWOTMAN", axis=1)
        econ_data = econ_data.set_index("date")
        econ_data[econ_data == pd.np.inf] = 0.0
        econ_data[econ_data == -pd.np.inf] = 0.0
        econ_data = econ_data.reset_index()
        econ_data = econ_data.fillna(0.0)
        return econ_data

    def _generate_econ_data(self, transformed_econ_data, sector_average, max_end_date):
        econ_data = transformed_econ_data[(transformed_econ_data["date"]>=self.start_date) &
                                          (transformed_econ_data["date"]<=max_end_date)].reset_index(drop=True)
        econ_data = econ_data.sort_values(["date"])

        sector_averages_summary = []
        for sector in self.key_sectors:
            short_name = sector.replace(" ", "")
            df = sector_average[sector_average["Sector"]==sector].drop("Sector", axis=1).set_index("date")[self.sector_cols]

            df.columns = list(map(lambda x:"{0}_{1}".format(x, short_name), df.columns))
            sector_averages_summary.append(df)

        sector_averages_with_renamed_cols = pd.concat(sector_averages_summary, axis=1)
        econ_data = pd.merge(econ_data, sector_averages_with_renamed_cols.reset_index(), how="left", on=["date"])
        econ_data = self._remove_bad_cols_from_econ_data(econ_data)
        return econ_data

    def _normalize_econ_data(self, econ_data):
        econ_data = econ_data[["date"] + self.econ_cols] # Select key features
        if self.normalize_econ:
            for col in self.econ_cols:
                econ_data[col] = econ_data[col].transform(lambda x: (x - x.mean()) / x.std())
        return econ_data

    def _merge_data(self, monthly_merged_data, security_master, industry_average, econ_data):
        merged_data = pd.merge(monthly_merged_data.rename(columns= {"ticker" : "dcm_security_id"}),
                               security_master[self.security_master_cols],
                               how="left", on=["dcm_security_id"])
        merged_data = pd.merge(merged_data, industry_average[self.industry_cols].rename(columns={col: col+"_indgrp"
                                                                                                 for col in industry_average.columns}).reset_index(),
                               how="left", on=["date", "IndustryGroup"])
        merged_data["dcm_security_id"] = merged_data["dcm_security_id"].astype(int)
        merged_data = merged_data.rename(columns={"dcm_security_id" : "ticker"})

        merged_data = pd.merge(merged_data, econ_data, how="left", on=["date"] )
        merged_data = merged_data.set_index(["date","ticker"])
        merged_data[merged_data==pd.np.inf] = pd.np.nan
        merged_data[merged_data==-pd.np.inf] = pd.np.nan
        return merged_data

    def do_step_action(self, **kwargs):
        monthly_merged_data = kwargs["monthly_merged_data_single_names"].copy(deep=True)
        security_master = kwargs["security_master"]
        industry_average = kwargs["industry_average"]
        sector_data = kwargs["sector_average"]
        sector_average = sector_data.reset_index()
        transformed_econ_data = kwargs["transformed_econ_data"]

        self.start_date = self.start_date if pd.notnull(self.start_date) else self.task_params.start_dt
        self.end_date = self.end_date if pd.notnull(self.end_date) else self.task_params.end_dt

        max_end_date = min(monthly_merged_data["date"].max(), self.end_date)
        econ_data = self._generate_econ_data(transformed_econ_data, sector_average, max_end_date)
        self.econ_data_final = econ_data.copy() # Has all features (for gan)
        econ_data = self._normalize_econ_data(econ_data)

        merged_data = self._merge_data(monthly_merged_data, security_master, industry_average, econ_data)
        self.data = merged_data.reset_index()
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.econ_data_final}

    @classmethod
    def get_default_config(cls):
        return {"industry_cols" : ["volatility_126", "PPO_12_26", "PPO_21_126", "netmargin",
                                   "macd_diff", "pe", "debt2equity", "bm", "ret_63B",
                                   "ebitda_to_ev", "divyield"],
                "security_master_cols" : ["dcm_security_id", "Sector", "IndustryGroup"],
                "sector_cols" : ["volatility_126", "PPO_21_126", "macd_diff", "divyield", "bm"],
                "key_sectors" : ["Energy", "Information Technology", "Financials", "Utilities",
                                 "Consumer Discretionary", "Industrials", "Consumer Staples"],
                "econ_cols" : ['RETAILx', 'USTRADE', 'SPY_close', 'bm_Financials'],
                "start_date" : "1997-12-15",
                "end_date" : "2019-09-20",
                "normalize_econ" : True}


class QuantamentalMergeEconIndustryWeekly(QuantamentalMergeEconIndustry):
    '''

    Merges the data elements on both monthly/weekly timeline for the Quantamental ML calibration pipeline

    '''

    PROVIDES_FIELDS = ["merged_data_econ_industry", "merged_data_econ_industry_weekly", "econ_data_final", "econ_data_final_weekly"]
    REQUIRES_FIELDS = ["monthly_merged_data_single_names", "weekly_merged_data_single_names", "security_master",
                       "industry_average", "sector_average", "transformed_econ_data",
                       "industry_average_weekly", "sector_average_weekly", "transformed_econ_data_weekly"]

    def __init__(self, industry_cols, security_master_cols, sector_cols, key_sectors, econ_cols, start_date, end_date,
                 normalize_econ=True):
        self.weekly_data = None
        self.econ_data_final_weekly = None
        QuantamentalMergeEconIndustry.__init__(self, industry_cols, security_master_cols, sector_cols,
                                               key_sectors, econ_cols, start_date, end_date, normalize_econ)

    def do_step_action(self, **kwargs):
        monthly_merged_data = kwargs["monthly_merged_data_single_names"].copy(deep=True)
        weekly_merged_data = kwargs["weekly_merged_data_single_names"].copy(deep=True)
        security_master = kwargs["security_master"]
        industry_average = kwargs["industry_average"]
        sector_average = kwargs["sector_average"].reset_index()
        transformed_econ_data = kwargs["transformed_econ_data"]
        industry_average_weekly = kwargs["industry_average_weekly"]
        sector_average_weekly = kwargs["sector_average_weekly"].reset_index()
        transformed_econ_data_weekly = kwargs["transformed_econ_data_weekly"]

        self.start_date = self.start_date if pd.notnull(self.start_date) else self.task_params.start_dt
        self.end_date = self.end_date if pd.notnull(self.end_date) else self.task_params.end_dt

        max_end_date = min(monthly_merged_data["date"].max(), self.end_date)
        econ_data = self._generate_econ_data(transformed_econ_data, sector_average, max_end_date)
        self.econ_data_final = econ_data.copy() # Has all features (for gan)
        econ_data = self._normalize_econ_data(econ_data)
        monthly_data = self._merge_data(monthly_merged_data, security_master, industry_average, econ_data)
        self.data = monthly_data.reset_index()

        max_end_date = min(weekly_merged_data["date"].max(), self.end_date)
        econ_data_weekly = self._generate_econ_data(transformed_econ_data_weekly, sector_average_weekly, max_end_date)
        self.econ_data_final_weekly = econ_data_weekly.copy()
        econ_data_weekly = self._normalize_econ_data(econ_data_weekly)
        weekly_data = self._merge_data(weekly_merged_data, security_master, industry_average_weekly, econ_data_weekly)
        self.weekly_data = weekly_data.reset_index()

        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.weekly_data,
                self.__class__.PROVIDES_FIELDS[2] : self.econ_data_final,
                self.__class__.PROVIDES_FIELDS[3] : self.econ_data_final_weekly}


class QuantamentalMergeSignals(CalibrationTaskflowTask):
    '''

    Merges the data elements for the Quantamental ML calibration pipeline

    '''

    PROVIDES_FIELDS = ["data_full_population_signal"]
    REQUIRES_FIELDS = ["normalized_data_full_population", "intermediate_signals"]

    def __init__(self, drop_column):
        self.data = None
        self.drop_column = drop_column
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        all_data = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        all_signal_ranks = kwargs[self.__class__.REQUIRES_FIELDS[1]].copy(deep=True)
        merged_data = pd.merge(all_data, all_signal_ranks.drop(self.drop_column, axis=1), how="left", on=["date", "ticker"])
        self.data = merged_data
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"drop_column": "future_ret_21B"}


class QuantamentalMergeSignalsWeekly(QuantamentalMergeSignals):

    PROVIDES_FIELDS = ["data_full_population_signal", "data_full_population_signal_weekly"]
    REQUIRES_FIELDS = ["normalized_data_full_population", "normalized_data_full_population_weekly",
                       "intermediate_signals", "intermediate_signals_weekly"]

    def __init__(self, drop_column):
        self.weekly_data = None
        QuantamentalMergeSignals.__init__(self, drop_column)

    def do_step_action(self, **kwargs):
        all_data = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        all_data_weekly = kwargs[self.__class__.REQUIRES_FIELDS[1]].copy(deep=True)
        all_signal_ranks = kwargs[self.__class__.REQUIRES_FIELDS[2]].copy(deep=True)
        all_signal_ranks_weekly = kwargs[self.__class__.REQUIRES_FIELDS[3]].copy(deep=True)
        self.data = pd.merge(all_data, all_signal_ranks.drop(self.drop_column, axis=1), how="left", on=["date", "ticker"])
        self.weekly_data = pd.merge(all_data_weekly, all_signal_ranks_weekly.drop(self.drop_column, axis=1),
                                    how="left", on=["date", "ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.weekly_data}


class ConsolidatePriceESGData(CalibrationTaskflowTask):
    '''

    Consolidates ESG and Traditional Data

    '''

    PROVIDES_FIELDS = ["tvlid_raw", "tvlid_norm", "tvlid_sector_avg", "tvlid_industry_avg", "feature_list"]
    REQUIRES_FIELDS = ["esg_data", "merged_data", "sasb_mapping", "daily_price_data", "security_master"]

    def __init__(self, start_date, end_date, mode, nan_thresh, esg_nan_thresh):
        self.data = None
        self.tvlid_sector_avg = None
        self.tvlid_industry_avg = None
        self.tvlid_norm= None
        self.feature_list = None

        self.start_date = pd.Timestamp(start_date)
        self.end_date = pd.Timestamp(end_date)
        self.mode = mode or "bms"
        self.nan_thresh = nan_thresh or 0.2
        self.esg_nan_thresh = esg_nan_thresh or 0.25
        CalibrationTaskflowTask.__init__(self)

    def sector_grouping(self, sector):
        if sector in ['Consumer Staples', 'Consumer Discretionary']:
            return 1
        elif sector in ['Utilities', 'Energy']:
            return 2
        elif sector in ['Information Technology', 'Communication Services']:
            return 3
        elif sector in ['Health Care']:
            return 4
        elif sector in ['Industrials']:
            return 5
        elif sector in ['Materials']:
            return 6
        elif sector in ['Financials']:
            return 7
        else:
            return 0

    def generate_sector_industry_averages(self, raw_values):
        parent_cats = ["E", "S", "G"]
        add_cols = []
        for c in ["Pulse", "Insight", "Momentum"]:
            cat_list = list(map(lambda x:"{0}_{1}".format(x, c), parent_cats))
            add_cols += cat_list

        all_numeric_cols = list(set(raw_values.columns) - set(["date", "ticker", "dcm_security_id", "company_name", "Sector", "IndustryGroup"]))
        sector_avg = raw_values[["date", "Sector"] + all_numeric_cols].groupby(["date", "Sector"]).mean()
        self.tvlid_sector_avg = sector_avg
        industry_avg = raw_values[["date", "IndustryGroup"] + all_numeric_cols].groupby(["date", "IndustryGroup"]).mean()
        self.tvlid_industry_avg = industry_avg

    def generate_normalized_data(self, raw_values):
        df = raw_values.copy()
        future_cols = [col for col in raw_values.columns if col.startswith("future_")]
        all_numeric_cols = list(set(raw_values.columns) - set(["date", "ticker", "dcm_security_id", "company_name", "Sector", "IndustryGroup"]))
        X_cols = list(set(all_numeric_cols) - set(future_cols + ["fq", "close_price", "company_name" + "SectorGroup"]))
        y_col = list(future_cols)

        for col in X_cols:
            print(col)
            lb = df.groupby('date')[col].transform(pd.Series.quantile, 0.005)
            lb = lb.apply(nonnull_lb)
            ub = df.groupby('date')[col].transform(pd.Series.quantile, 0.995)
            ub = ub.apply(nonnull_ub)
            df[col] = df[col].clip(lb, ub)
            df[col] = df.groupby('date')[col].transform(lambda x: (x - x.mean()) / x.std())
            df[col] = df[col].fillna(0)
        for col in y_col:
            print(col)
            new_col = "{0}_std".format(col)
            lb = df.groupby('date')[col].transform(pd.Series.quantile, 0.005)
            lb = lb.apply(nonnull_lb)
            ub = df.groupby('date')[col].transform(pd.Series.quantile, 0.995)
            ub = ub.apply(nonnull_ub)
            df[new_col] = df[col].clip(lb, ub)
            df[new_col] = df.groupby('date')[col].transform(lambda x: (x - x.mean()) / x.std())
            df[new_col] = df[new_col].fillna(0)

        self.tvlid_norm = df

    def generate_feature_list(self, raw_values):
        tvlid_cols = ['AllCategories_CategoryVolumeTTM', 'Materiality_CategoryVolumeTTM', 'AllCategories_ArticleVolumeTTM',
                      'Materiality_ArticleVolumeTTM', 'AllCategories_Pulse', 'Materiality_Pulse', 'AccessAndAffordability_Pulse',
                      'AirQuality_Pulse', 'BusinessEthics_Pulse', 'BusinessModelResilience_Pulse', 'CompetitiveBehavior_Pulse',
                      'CriticalIncidentRiskManagement_Pulse', 'CustomerPrivacy_Pulse', 'CustomerWelfare_Pulse', 'DataSecurity_Pulse',
                      'EcologicalImpacts_Pulse', 'EmployeeEngagementDiversityAndInclusion_Pulse', 'EmployeeHealthAndSafety_Pulse',
                      'EnergyManagement_Pulse', 'GHGEmissions_Pulse', 'HumanRightsAndCommunityRelations_Pulse', 'LaborPractices_Pulse',
                      'ManagementOfTheLegalAndRegulatoryEnvironment_Pulse', 'MaterialsSourcingAndEfficiency_Pulse',
                      'PhysicalImpactsOfClimateChange_Pulse', 'ProductDesignAndLifecycleManagement_Pulse', 'ProductQualityAndSafety_Pulse',
                      'SellingPracticesAndProductLabeling_Pulse', 'SupplyChainManagement_Pulse', 'SystemicRiskManagement_Pulse',
                      'WasteAndHazardousMaterialsManagement_Pulse', 'WaterAndWastewaterManagement_Pulse', 'AllCategories_Insight',
                      'Materiality_Insight', 'AccessAndAffordability_Insight', 'AirQuality_Insight', 'BusinessEthics_Insight',
                      'BusinessModelResilience_Insight', 'CompetitiveBehavior_Insight', 'CriticalIncidentRiskManagement_Insight',
                      'CustomerPrivacy_Insight', 'CustomerWelfare_Insight', 'DataSecurity_Insight', 'EcologicalImpacts_Insight',
                      'EmployeeEngagementDiversityAndInclusion_Insight', 'EmployeeHealthAndSafety_Insight', 'EnergyManagement_Insight',
                      'GHGEmissions_Insight', 'HumanRightsAndCommunityRelations_Insight', 'LaborPractices_Insight',
                      'ManagementOfTheLegalAndRegulatoryEnvironment_Insight', 'MaterialsSourcingAndEfficiency_Insight',
                      'PhysicalImpactsOfClimateChange_Insight', 'ProductDesignAndLifecycleManagement_Insight',
                      'ProductQualityAndSafety_Insight', 'SellingPracticesAndProductLabeling_Insight', 'SupplyChainManagement_Insight',
                      'SystemicRiskManagement_Insight', 'WasteAndHazardousMaterialsManagement_Insight', 'WaterAndWastewaterManagement_Insight',
                      'AllCategories_Momentum', 'Materiality_Momentum', 'AccessAndAffordability_Momentum', 'AirQuality_Momentum',
                      'BusinessEthics_Momentum', 'BusinessModelResilience_Momentum', 'CompetitiveBehavior_Momentum',
                      'CriticalIncidentRiskManagement_Momentum', 'CustomerPrivacy_Momentum', 'CustomerWelfare_Momentum', 'DataSecurity_Momentum',
                      'EcologicalImpacts_Momentum', 'EmployeeEngagementDiversityAndInclusion_Momentum', 'EmployeeHealthAndSafety_Momentum',
                      'EnergyManagement_Momentum', 'GHGEmissions_Momentum', 'HumanRightsAndCommunityRelations_Momentum',
                      'LaborPractices_Momentum', 'ManagementOfTheLegalAndRegulatoryEnvironment_Momentum',
                      'MaterialsSourcingAndEfficiency_Momentum', 'PhysicalImpactsOfClimateChange_Momentum',
                      'ProductDesignAndLifecycleManagement_Momentum', 'ProductQualityAndSafety_Momentum',
                      'SellingPracticesAndProductLabeling_Momentum', 'SupplyChainManagement_Momentum', 'SystemicRiskManagement_Momentum',
                      'WasteAndHazardousMaterialsManagement_Momentum', 'WaterAndWastewaterManagement_Momentum']
        vol_cols = ['AccessAndAffordability_CategoryVolumeTTM', 'AirQuality_CategoryVolumeTTM', 'BusinessEthics_CategoryVolumeTTM',
                    'BusinessModelResilience_CategoryVolumeTTM', 'CompetitiveBehavior_CategoryVolumeTTM', 'CriticalIncidentRiskManagement_CategoryVolumeTTM',
                    'CustomerPrivacy_CategoryVolumeTTM', 'CustomerWelfare_CategoryVolumeTTM', 'DataSecurity_CategoryVolumeTTM',
                    'EcologicalImpacts_CategoryVolumeTTM', 'EmployeeEngagementDiversityAndInclusion_CategoryVolumeTTM',
                    'EmployeeHealthAndSafety_CategoryVolumeTTM', 'EnergyManagement_CategoryVolumeTTM', 'GHGEmissions_CategoryVolumeTTM',
                    'HumanRightsAndCommunityRelations_CategoryVolumeTTM', 'LaborPractices_CategoryVolumeTTM',
                    'ManagementOfTheLegalAndRegulatoryEnvironment_CategoryVolumeTTM', 'MaterialsSourcingAndEfficiency_CategoryVolumeTTM',
                    'PhysicalImpactsOfClimateChange_CategoryVolumeTTM', 'ProductDesignAndLifecycleManagement_CategoryVolumeTTM',
                    'ProductQualityAndSafety_CategoryVolumeTTM', 'SellingPracticesAndProductLabeling_CategoryVolumeTTM',
                    'SupplyChainManagement_CategoryVolumeTTM', 'SystemicRiskManagement_CategoryVolumeTTM', 'WasteAndHazardousMaterialsManagement_CategoryVolumeTTM',
                    'WaterAndWastewaterManagement_CategoryVolumeTTM']

        parent_cats = ["E", "S", "G"]
        add_cols = []
        for c in ["Pulse", "Insight", "Momentum"]:
            cat_list = list(map(lambda x:"{0}_{1}".format(x, c), parent_cats))
            add_cols += cat_list
        esg_cols = tvlid_cols + vol_cols + add_cols
        sectors = list(raw_values["Sector"].unique())

        thresh = self.esg_nan_thresh

        bad_cols = []
        n_obs = len(raw_values)
        for col in esg_cols:
            nan_pct = 1.*pd.isnull(raw_values[col]).sum()/n_obs
            if nan_pct>=nan_thresh:
                print(col)
                bad_cols.append(col)

        root_bad_cols = set(list(map(lambda x:x.split("_")[0], bad_cols)))
        all_bad_esg_cols = []
        for col in root_bad_cols:
            print(col)
            for suffix in ["Pulse", "Insight", "Momentum", "CategoryVolumeTTM"]:
                all_bad_esg_cols.append("{0}_{1}".format(col, suffix))

        exclusion_cols = list(pd.read_csv(os.path.join(final_dir, "exclusion.csv"))["column"])
        all_numeric_cols = list(set(raw_values.columns) - set(["date", "ticker", "dcm_security_id", "company_name", "Sector", "IndustryGroup"]))
        future_cols = [col for col in raw_values.columns if col.startswith("future_")]
        X_cols = list(set(all_numeric_cols) - set(future_cols + ["fq", "close_price", "company_name"] + all_bad_esg_cols + exclusion_cols))
        y_col = list(future_cols)

        thresh=self.nan_thresh
        bad_cols_per_sector_group = {}
        for sector_group in list(raw_values["SectorGroup"].unique()):
            current = []
            print(sector_group)
            cond = (raw_values["SectorGroup"]==sector_group)
            nobs = len(raw_values[cond])
            for col in X_cols:
                if 1.0*pd.isnull(raw_values.loc[cond, col]).sum()/nobs>=thresh:
                    current.append(col)
            print(current)
            print(len(current))
            bad_cols_per_sector_group[sector_group] = current

        good_cols_per_sector_group = {}
        for sector_group in list(raw_values["SectorGroup"].unique()):
            current = []
            print(sector_group)
            cond = (raw_values["SectorGroup"]==sector_group)
            nobs = len(raw_values[cond])
            current = list(set(X_cols) - set(bad_cols_per_sector_group[sector_group] + ["SectorGroup"]))
            print(len(current))
            good_cols_per_sector_group[sector_group] = current

        self.feature_list = pd.DataFrame.from_dict(good_cols_per_sector_group, orient="index").T


    def do_step_action(self, **kwargs):
        volume =  kwargs[self.__class__.REQUIRES_FIELDS[1]]
        volume["date"] = volume["date"].apply(lambda x:pd.Timestamp(x+pd.tseries.frequencies.to_offset("1D")))
        good_ids = list(volume["dcm_security_id"].unique())
        dcm_features = kwargs[self.__class__.REQUIRES_FIELDS[1]]
        dcm_features["date"] = dcm_features["date"].apply(pd.Timestamp)
        sasb_mapping =  kwargs[self.__class__.REQUIRES_FIELDS[2]]
        prices = kwargs[self.__class__.REQUIRES_FIELDS[3]]
        sec_master =  kwargs[self.__class__.REQUIRES_FIELDS[4]]

        cond = (dcm_features["date"]>=self.start_date) & (dcm_features["date"]<=self.end_date) & \
            pd.notnull(dcm_features["ret_10B"]) & dcm_features["ticker"].isin(good_ids)
        dcm_features = dcm_features[cond].reset_index(drop=True)

        feature_cols = list(dcm_features.columns)
        feature_cols.remove("date")
        feature_cols.remove("ticker")
        bad_cols = []
        nan_thresh = self.nan_thresh
        n_obs = len(dcm_features)
        for col in feature_cols:
            nan_pct = 1.*pd.isnull(dcm_features[col]).sum()/n_obs
            if nan_pct>=nan_thresh:
                bad_cols.append(col)
        good_cols = list(set(dcm_features.columns) - set(bad_cols))
        dcm_features = dcm_features[good_cols]

        dcm_features = dcm_features.set_index(["date", "ticker"])
        dcm_features.reset_index(inplace=True)
        dcm_features = pd.merge(dcm_features, volume.rename(columns={"dcm_security_id": "ticker"}), how="left",
                                on=["date", "ticker"])

        dcm_features.sort_values(["date", "ticker"], inplace=True)

        vol_cols = list(volume.columns)
        vol_cols.remove("date")
        vol_cols.remove("dcm_security_id")

        dcm_features[vol_cols] = dcm_features[["ticker"] + vol_cols].groupby("ticker").transform(lambda x:x.fillna(method="ffill"))
        dcm_features = dcm_features[pd.np.logical_not(dcm_features["Sector"].isin(["Real Estate"]))].reset_index(drop=True)

        average_volumes = dcm_features.groupby("ticker").mean()[['AllCategories_CategoryVolumeTTM', 'Materiality_CategoryVolumeTTM', 'AllCategories_ArticleVolumeTTM','Materiality_ArticleVolumeTTM']]
        average_volumes = average_volumes.sort_values('Materiality_CategoryVolumeTTM', ascending=False)
        good_names = pd.DataFrame(list(average_volumes.head(100).index), columns=["dcm_security_id"])
        good_names = pd.merge(good_names, sec_master[["dcm_security_id", "ticker", "company_name", "Sector", "IndustryGroup"]], how="left", on=["dcm_security_id"])

        if self.mode in ["bme", "bms"]:
            chosen_days = list(pick_trading_month_dates(self.start_date, self.end_date, self.mode))
            df = dcm_features[dcm_features["date"].isin(chosen_days)].reset_index(drop=True)
        else:
            df = dcm_features[(dcm_features["date"]>=self.start_date) & (dcm_features["date"]>=self.end_date)]

        category_dict = dict(zip(sasb_mapping["sasb_cat"], sasb_mapping["esg_cat"]))
        E_roots = list(sasb_mapping[sasb_mapping["esg_cat"]=="E"]["sasb_cat"])
        S_roots = list(sasb_mapping[sasb_mapping["esg_cat"]=="S"]["sasb_cat"])
        G_roots = list(sasb_mapping[sasb_mapping["esg_cat"]=="G"]["sasb_cat"])
        E_roots_l = list(map(lambda x:x.lower(), E_roots))
        S_roots_l = list(map(lambda x:x.lower(), S_roots))
        G_roots_l = list(map(lambda x:x.lower(), G_roots))
        esg_column_dict = {"E": E_roots_l, "S": S_roots_l, "G": G_roots_l}

        # Volume score aggregations
        good_cols = [col for col in df.columns if ((col.split("_")[0].lower() in E_roots_l) and col.endswith("CategoryVolumeTTM"))]
        suffix = "CategoryVolumeTTM"
        for cat in ["E", "S", "G"]:
            print(cat)
            root_list = esg_column_dict[cat]
            good_cols = [col for col in df.columns if ((col.split("_")[0].lower() in root_list) and col.endswith(suffix))]
            col_name = "{0}_{1}".format(cat, suffix)
            print(col_name)
            print(good_cols)
            df[col_name] = df[good_cols].sum(axis=1)

        # Pulse score aggregations
        suffix = "Pulse"
        for cat in ["E", "S", "G"]:
            print(cat)
            root_list = esg_column_dict[cat]
            good_cols = [col for col in df.columns if ((col.split("_")[0].lower() in root_list) and col.endswith(suffix))]
            col_name = "{0}_{1}".format(cat, suffix)
            print(col_name)
            print(good_cols)
            df[col_name] = df[good_cols].mean(axis=1)

        # Insight score aggregations
        suffix = "Insight"
        for cat in ["E", "S", "G"]:
            print(cat)
            root_list = esg_column_dict[cat]
            good_cols = [col for col in df.columns if ((col.split("_")[0].lower() in root_list) and col.endswith(suffix))]
            col_name = "{0}_{1}".format(cat, suffix)
            print(col_name)
            print(good_cols)
            df[col_name] = df[good_cols].mean(axis=1)

        # Momentum score aggregations
        suffix = "Momentum"
        for cat in ["E", "S", "G"]:
            print(cat)
            root_list = esg_column_dict[cat]
            good_cols = [col for col in df.columns if ((col.split("_")[0].lower() in root_list) and col.endswith(suffix))]
            col_name = "{0}_{1}".format(cat, suffix)
            print(col_name)
            print(good_cols)
            df[col_name] = df[good_cols].mean(axis=1)

        volume_suffixes = ["ArticleVolumeTTM", "CategoryVolumeTTM"]
        volume_columns = [col for col in df.columns if (col.endswith("ArticleVolumeTTM") | col.endswith("CategoryVolumeTTM"))]
        prices["date"] = prices["date"].apply(pd.Timestamp)
        prices.sort_values(["date", "ticker"], inplace=True)
        df = pd.merge(df, prices[["date", "ticker", "close", "volume"]].rename(columns={"close": "close_price"}), how="left", on=["date", "ticker"])

        ref_col = "avg_dollar_volume"
        df = df[pd.notnull(df[ref_col])].reset_index(drop=True)

        df["log_avg_dollar_volume"] = pd.np.log(df[ref_col])
        for col in volume_columns:
            new_col = "{0}_norm".format(col)
            print(new_col)
            df[new_col] = df[col].apply(safe_log) - pd.np.log(df["log_avg_dollar_volume"])

        df = df.rename(columns={"ticker": "dcm_security_id"})
        df = pd.merge(df, sec_master[["dcm_security_id", "ticker", "company_name"]], how="left", on=["dcm_security_id"])

        for concept in ["Pulse", "Momentum", "Insight"]:
            print(concept)
            column = 'AllCategories_{0}'.format(concept)
            growth_column = 'ESG_{0}_growth'.format(concept)
            future_column = 'future_ESG_{0}_growth'.format(concept)
            esg = df.pivot_table(index='date', columns='dcm_security_id', values=column, dropna=False).sort_index()
            esg_growth = esg.pct_change(1, fill_method=None)
            esg_growth = esg_growth.stack().reset_index().rename(columns={0: growth_column})
            future_esg_growth = esg.pct_change(1, fill_method=None).shift(-1)
            future_esg_growth = future_esg_growth.stack().reset_index().rename(columns={0: future_column})
            df = pd.merge(df, esg_growth, how="left", on=["date", "dcm_security_id"])
            df = pd.merge(df, future_esg_growth, how="left", on=["date", "dcm_security_id"])

        df["SectorGroup"] = df["Sector"].apply(lambda x:self.sector_grouping(x))

        normalize_cols = ['investments', 'ncff', 'revenue', 'ncfi']
        denom = "assets"
        for col in normalize_cols:
            new_col = "{0}_yield".format(col)
            df[new_col] = df[col]/df[denom]
        self.data = df

        self.generate_sector_industry_averages(df)
        self.generate_normalized_data(df)
        self.generate_feature_list(df)

        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.tvlid_norm,
                self.__class__.PROVIDES_FIELDS[2] : self.tvlid_sector_avg,
                self.__class__.PROVIDES_FIELDS[3] : self.tvlid_industry_avg,
                self.__class__.PROVIDES_FIELDS[4] : self.feature_list}

    @classmethod
    def get_default_config(cls):
        return {"start_date": "2018-01-01", "end_date": "2019-05-15", "mode": "bms", "nan_thresh": 0.2,
                "esg_nan_thresh": 0.25}


class FilterMonthlyDatesFullPopulation(CalibrationTaskflowTask):
    '''

    Filters data for month start or month end observations

    '''

    PROVIDES_FIELDS = ["monthly_merged_data"]
    REQUIRES_FIELDS = ["merged_data"]
    def __init__(self, start_date, end_date, mode):
        self.data = None
        self.start_date = pd.Timestamp(start_date) if start_date else None
        self.end_date = pd.Timestamp(end_date) if end_date else None
        self.mode = mode or "bme"
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        df = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        self.start_date = self.start_date if pd.notnull(self.start_date) else self.task_params.start_dt
        self.end_date = self.end_date if pd.notnull(self.end_date) else self.task_params.end_dt

        max_end_date = min(self.end_date, df["date"].max())
        if self.mode in ["bme", "bms"]:
            chosen_days = list(pick_trading_month_dates(self.start_date, max_end_date, self.mode))
            filtered_data = df[df["date"].isin(chosen_days)].reset_index(drop=True)
        else:
            filtered_data = df[(df["date"]>=self.start_date) & (df["date"]<=max_end_date)]
        #filtered_data["log_avg_dollar_volume"] = pd.np.log(filtered_data["avg_dollar_volume"])
        self.data = filtered_data
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"start_date": "1997-06-30", "end_date": "2019-05-15", "mode": "bme"}


class FilterMonthlyDatesFullPopulationWeekly(FilterMonthlyDatesFullPopulation):
    '''

    Filters data for month start or month end, as well as weekly observations

    '''

    PROVIDES_FIELDS = ["monthly_merged_data", "weekly_merged_data"]
    REQUIRES_FIELDS = ["merged_data"]

    def __init__(self, start_date, end_date, monthly_mode, weekly_mode):
        self.weekly_data = None
        self.weekly_mode = weekly_mode or "w-mon"
        FilterMonthlyDatesFullPopulation.__init__(self, start_date, end_date, monthly_mode)

    def do_step_action(self, **kwargs):
        FilterMonthlyDatesFullPopulation.do_step_action(self, **kwargs)
        df = kwargs[self.__class__.REQUIRES_FIELDS[0]]

        max_end_date = min(self.end_date, df["date"].max())
        chosen_days = list(pick_trading_week_dates(self.start_date, max_end_date, self.weekly_mode))
        filtered_data = df[df["date"].isin(chosen_days)].reset_index(drop=True)
        self.weekly_data = filtered_data
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.weekly_data}

    @classmethod
    def get_default_config(cls):
        return {"start_date": "1997-06-30", "end_date": "2019-05-15", "monthly_mode": "bme", "weekly_mode": "w-mon"}


class CreateMonthlyDataSingleNames(CalibrationTaskflowTask):

    PROVIDES_FIELDS = ["monthly_merged_data_single_names"]
    REQUIRES_FIELDS = ["monthly_merged_data","security_master"]

    def __init__(self):
        self.data = None
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        security_master = kwargs[self.REQUIRES_FIELDS[1]]
        monthly_merged_data = kwargs[self.REQUIRES_FIELDS[0]]
        singlename_tickers = list(security_master.dropna(subset=["IndustryGroup"])["dcm_security_id"].unique())
        res = monthly_merged_data[monthly_merged_data["ticker"].isin(singlename_tickers)].reset_index(drop=True)
        self.data = res
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {}


class CreateMonthlyDataSingleNamesWeekly(CreateMonthlyDataSingleNames):

    PROVIDES_FIELDS = ["monthly_merged_data_single_names", "weekly_merged_data_single_names"]
    REQUIRES_FIELDS = ["monthly_merged_data", "weekly_merged_data", "security_master"]

    def __init__(self):
        self.weekly_data = None
        CreateMonthlyDataSingleNames.__init__(self)

    def do_step_action(self, **kwargs):
        security_master = kwargs["security_master"]
        monthly_merged_data = kwargs["monthly_merged_data"]
        weekly_merged_data = kwargs["weekly_merged_data"]
        singlename_tickers = list(security_master.dropna(subset=["IndustryGroup"])["dcm_security_id"].unique())

        self.data = monthly_merged_data[monthly_merged_data["ticker"].isin(singlename_tickers)].reset_index(drop=True)
        self.weekly_data = weekly_merged_data[weekly_merged_data["ticker"].isin(singlename_tickers)].reset_index(drop=True)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.weekly_data}



class FilterRussell1000(CalibrationTaskflowTask):

    '''

    Filters data for month start or month end observations

    '''

    PROVIDES_FIELDS = ["russell_data"]
    REQUIRES_FIELDS = ["monthly_merged_data", "russell_components"]
    def __init__(self, mode):
        self.data = None
        self.mode = mode or "full"
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        monthly = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        df = kwargs[self.__class__.REQUIRES_FIELDS[1]].copy(deep=True)

        temp = df.pivot(index='date', columns='cusip', values='wt-r1')
        dates = temp.index.union(monthly['date'].unique())
        temp = temp.reindex(dates).fillna(method='ffill', limit=5)
        temp = temp.stack().reset_index()
        temp.columns = ['date', 'cusip', 'wt-r1']
        df = pd.merge(temp[['date', 'cusip']], df, how='left', on=['date', 'cusip'])
        df.sort_values(['cusip', 'date'], inplace=True)
        df = df.groupby('cusip', as_index=False).fillna(method='ffill')

        monthly.rename(columns={'ticker': 'dcm_security_id'}, inplace=True)
        univ = df[['date', 'dcm_security_id', 'wt-r1', 'wt-r1v', 'wt-r1g']]
        univ = pd.merge(monthly, univ, how='left', on=['date', 'dcm_security_id'])
        univ['dcm_security_id'] = univ['dcm_security_id'].astype(int)
        univ.rename(columns={"dcm_security_id": "ticker"}, inplace=True)
        for col in ['wt-r1', 'wt-r1g', 'wt-r1v']:
            univ[col] = univ[col].astype(float)
        univ[['wt-r1', 'wt-r1g', 'wt-r1v']] = univ.groupby('ticker', as_index=False).fillna(method='ffill')[['wt-r1', 'wt-r1g', 'wt-r1v']]
        if self.mode=="growth":
            univ = univ[univ['wt-r1g'] > 0].reset_index(drop=True)
        elif self.mode=="value":
            univ = univ[univ['wt-r1v'] > 0].reset_index(drop=True)
        else:
            univ = univ[univ['wt-r1'] > 0].reset_index(drop=True)
        univ["ticker"] = univ["ticker"].astype(int)
        self.data = univ
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"mode": "full"}


class AddFilterFeatures(CalibrationTaskflowTask):
    '''
    
    TODO (20220110): Adds features that are used for downstream filtering of population
    In particular we need to add back the raw marketcap from monthly_merged_data_single_names as
    data_full_population_signal has normalized marketcap only. Column name may have to be relabeled 
    as e.g. _raw to distinguish from previous feature
    In the future other raw and derived features can go here
    
    '''
    
    PROVIDES_FIELDS = ["data_full_population_signal_enhanced"]
    REQUIRES_FIELDS = ["data_full_population_signal", "monthly_merged_data_single_names"]
    
    def __init__(self, **kwargs):
        pass
    
    def do_step_action(self, **kwargs):
        pass
    
    @classmethod
    def get_default_config(cls):
        return {}
    

class FilterRussell1000Augmented(CalibrationTaskflowTask):
    '''

    Filters data for month start or month end observations

    '''

    PROVIDES_FIELDS = ["r1k_models"]
    REQUIRES_FIELDS = ["data_full_population_signal", "russell_components", "quandl_daily", "raw_price_data"]

    def __init__(self, start_date, end_date, filter_price_marketcap=False, price_limit=7.0,
                 marketcap_limit=300000000.0, largecap_quantile=0.25):
        self.r1k_models = None
        self.start_date = pd.Timestamp(start_date) if start_date else None
        self.end_date = pd.Timestamp(end_date) if end_date else None
        self.filter_price_marketcap = filter_price_marketcap
        self.price_limit = price_limit
        self.marketcap_limit = marketcap_limit
        self.largecap_quantile = largecap_quantile
        CalibrationTaskflowTask.__init__(self)

    def _get_whole_univ(self, data, russell_df, marketcap):
        temp = russell_df.pivot(index='date', columns='cusip', values='wt-r1')
        dates = temp.index.union(data['date'].unique())
        temp = temp.reindex(dates).fillna(method='ffill', limit=5)
        temp = temp.stack().reset_index()
        temp.columns = ['date', 'cusip', 'wt-r1']
        russell_df = pd.merge(temp[['date', 'cusip']], russell_df, how='left', on=['date', 'cusip'])
        russell_df.sort_values(['cusip', 'date'], inplace=True)
        russell_df = russell_df.groupby('cusip', as_index=False).fillna(method='ffill')
        
        data.rename(columns={'ticker': 'dcm_security_id'}, inplace=True)
        univ = russell_df[['date', 'dcm_security_id', 'wt-r1', 'wt-r1v', 'wt-r1g']]
        univ = pd.merge(data, univ, how='left', on=['date', 'dcm_security_id'])
        univ['dcm_security_id'] = univ['dcm_security_id'].astype(int)
        univ.rename(columns={"dcm_security_id": "ticker"}, inplace=True)

        for col in ['wt-r1', 'wt-r1g', 'wt-r1v']:
            univ[col] = univ[col].astype(float)
        #univ[['wt-r1', 'wt-r1g', 'wt-r1v']] = univ.groupby('ticker', as_index=False).fillna(method='ffill')[['wt-r1', 'wt-r1g', 'wt-r1v']]
        univ[['wt-r1', 'wt-r1g', 'wt-r1v']] = univ.groupby('ticker', as_index=False).fillna(value=0.0)[['wt-r1', 'wt-r1g', 'wt-r1v']]

        univ = pd.merge(univ, marketcap, how="left", on=["date", "ticker"])
        univ["marketcap"] = univ["marketcap"].fillna(DEFAULT_MARKETCAP)
        return univ

    def _filter_russell_components(self, univ, mode):
        # To pick the top 40th percentile (0.4), we need to filter the items greater than (1 - 0.4 = 0.6) i.e. 60th percentile
        marketcap_filter = (univ["marketcap"] > univ.groupby("date")["marketcap"].transform("quantile", (1-self.largecap_quantile)))

        if mode=="growth":
            univ = univ[univ['wt-r1g'] > 0].reset_index(drop=True)
        elif mode=="value":
            univ = univ[univ['wt-r1v'] > 0].reset_index(drop=True)
        elif mode=="largecap_growth":
            univ = univ[(univ['wt-r1g'] > 0) & marketcap_filter].reset_index(drop=True)
        elif mode=="largecap_value":
            univ = univ[(univ['wt-r1v'] > 0) & marketcap_filter].reset_index(drop=True)
        else:
            univ = univ[univ['wt-r1'] > 0].reset_index(drop=True)
        univ["ticker"] = univ["ticker"].astype(int)
        return univ

    def _filter_price_marketcap(self, data_to_filter, raw_prices, marketcap):
        original_columns = list(data_to_filter.columns)
        aux_data = pd.merge(data_to_filter, raw_prices, how="left", on=["ticker", "date"])
        aux_data = pd.merge(aux_data, marketcap, how="left", on=["ticker", "date"])
        aux_data = aux_data.sort_values(["ticker", "date"])
        aux_data["marketcap"] = aux_data["marketcap"].fillna(DEFAULT_MARKETCAP)

        cond = (aux_data["marketcap"]>=self.marketcap_limit) & (aux_data["raw_close_price"]>=self.price_limit)
        aux_data = aux_data[cond].reset_index(drop=True)
        data_to_filter = aux_data[original_columns]
        return data_to_filter

    def do_step_action(self, **kwargs):
        russell_components = kwargs[self.__class__.REQUIRES_FIELDS[1]].copy(deep=True)
        data_to_filter = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        marketcap = kwargs[self.__class__.REQUIRES_FIELDS[2]][["date", "dcm_security_id", "marketcap"]]\
            .rename(columns={"dcm_security_id": "ticker"})
        raw_prices = kwargs[self.__class__.REQUIRES_FIELDS[3]].copy(deep=True)
        raw_prices["ticker"] = raw_prices["ticker"].fillna(-1).astype(int)

        if self.task_params.run_mode==TaskflowPipelineRunMode.Calibration:
            raw_prices["date"] = raw_prices["date"].apply(lambda x:marketTimeline.get_trading_day_using_offset(x, 1))
            marketcap["date"] = marketcap["date"].apply(lambda x:marketTimeline.get_trading_day_using_offset(x, 1))        

        if self.filter_price_marketcap:
            data_to_filter = self._filter_price_marketcap(data_to_filter, raw_prices, marketcap)

        self.start_date = self.start_date if pd.notnull(self.start_date) else self.task_params.start_dt
        self.end_date = self.end_date if pd.notnull(self.end_date) else self.task_params.end_dt

        univ = self._get_whole_univ(data_to_filter, russell_components, marketcap)
        r1k_dict = {}
        for mode in FILTER_MODES:
            filtered_df = self._filter_russell_components(univ, mode)
            r1k_dict[mode] = filtered_df[(filtered_df["date"]>=self.start_date) & (filtered_df["date"]<=self.end_date)]

        self.r1k_models = pd.DataFrame.from_dict(r1k_dict, orient='index')
        return StatusType.Success

    def _get_additional_step_results(self):
        return {"r1k_models" : self.r1k_models}

    @classmethod
    def get_default_config(cls):
        return {"start_date": "2009-03-15", "end_date": None, "filter_price_marketcap": True, "price_limit": 7.0,
                "marketcap_limit": 300000000.0, "largecap_quantile": 0.25} # end_date: 2019-08-30


class FilterRussell1000AugmentedWeekly(FilterRussell1000Augmented):

    PROVIDES_FIELDS = ["r1k_models", "r1k_models_sc_weekly", "r1k_models_lc_weekly"]
    REQUIRES_FIELDS = ["data_full_population_signal", "data_full_population_signal_weekly",
                       "russell_components", "quandl_daily", "raw_price_data"]

    def __init__(self, start_date, end_date, filter_price_marketcap=False, price_limit=7.0,
                 marketcap_limit=300000000.0, largecap_quantile=0.25):
        self.r1k_models_sc_weekly = None
        self.r1k_models_lc_weekly = None
        FilterRussell1000Augmented.__init__(self, start_date, end_date, filter_price_marketcap,
                                            price_limit, marketcap_limit, largecap_quantile)

    def do_step_action(self, **kwargs):
        data_to_filter_monthly = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        data_to_filter_weekly = kwargs[self.__class__.REQUIRES_FIELDS[1]].copy(deep=True)
        russell_components = kwargs[self.__class__.REQUIRES_FIELDS[2]].copy(deep=True)
        marketcap = kwargs[self.__class__.REQUIRES_FIELDS[3]][["date", "dcm_security_id", "marketcap"]]\
            .rename(columns={"dcm_security_id": "ticker"})
        raw_prices = kwargs[self.__class__.REQUIRES_FIELDS[4]].copy(deep=True)
        raw_prices["ticker"] = raw_prices["ticker"].fillna(-1).astype(int)

        if self.task_params.run_mode==TaskflowPipelineRunMode.Calibration:
            raw_prices["date"] = raw_prices["date"].apply(lambda x:marketTimeline.get_trading_day_using_offset(x, 1))
            marketcap["date"] = marketcap["date"].apply(lambda x:marketTimeline.get_trading_day_using_offset(x, 1))

        if self.filter_price_marketcap:
            data_to_filter_monthly = self._filter_price_marketcap(data_to_filter_monthly, raw_prices, marketcap)
            data_to_filter_weekly = self._filter_price_marketcap(data_to_filter_weekly, raw_prices, marketcap)

        self.start_date = self.start_date if pd.notnull(self.start_date) else self.task_params.start_dt
        self.end_date = self.end_date if pd.notnull(self.end_date) else self.task_params.end_dt

        univ_monthly = self._get_whole_univ(data_to_filter_monthly, russell_components, marketcap)
        univ_weekly = self._get_whole_univ(data_to_filter_weekly, russell_components, marketcap)

        r1k_dict_monthly = {}
        r1k_dict_sc_weekly = {}
        r1k_dict_lc_weekly = {}
        for mode in FILTER_MODES:
            filtered_df_monthly = self._filter_russell_components(univ_monthly, mode)
            filtered_df_weekly = self._filter_russell_components(univ_weekly, mode)
            r1k_dict_monthly[mode] = filtered_df_monthly[(filtered_df_monthly["date"]>=self.start_date) & (filtered_df_monthly["date"]<=self.end_date)]

            filtered_weekly_mode_df = filtered_df_weekly[(filtered_df_weekly["date"]>=self.start_date) & (filtered_df_weekly["date"]<=self.end_date)]
            if "largecap" in mode:
                r1k_dict_lc_weekly[mode] = filtered_weekly_mode_df
            else:
                r1k_dict_sc_weekly[mode] = filtered_weekly_mode_df

        self.r1k_models = pd.DataFrame.from_dict(r1k_dict_monthly, orient='index')
        self.r1k_models_sc_weekly = pd.DataFrame.from_dict(r1k_dict_sc_weekly, orient='index')
        self.r1k_models_lc_weekly = pd.DataFrame.from_dict(r1k_dict_lc_weekly, orient='index')
        return StatusType.Success

    def _get_additional_step_results(self):
        return {"r1k_models" : self.r1k_models,
                "r1k_models_sc_weekly" : self.r1k_models_sc_weekly,
                "r1k_models_lc_weekly" : self.r1k_models_lc_weekly}


def ols_res(x, y):
    x = sm.add_constant(x)
    fit = sm.OLS(y, x).fit()
    return fit.resid

def neutralize_data(df, factors, exclusion_list):
    df = df.copy(deep=True)
    df = df.drop(['wt-r1', 'wt-r1g', 'wt-r1v'], axis=1)
    missing_cols = [k for k in exclusion_list if k not in df.columns]
    missing_factors = [k for k in factors if k not in df.columns]
    print("Missing columns refered to in exclusion list: {0}".format(missing_cols))
    print("Missing columns refered to in factor list: {0}".format(missing_factors))
    exclusion_list = [k for k in exclusion_list if k not in missing_cols]
    factors = [k for k in factors if k not in missing_factors]
    future_cols = df.columns[df.columns.str.startswith('future_')].tolist()
    x_cols = factors
    y_cols = df.columns.difference(future_cols + x_cols + exclusion_list + ['date', 'ticker', 'Sector', 'IndustryGroup']).tolist()
    df.sort_values(['date', 'ticker'], inplace=True)
    df = df.reset_index(drop=True).set_index(['date', 'ticker'])
    df.drop_duplicates(inplace=True)
    df[y_cols + x_cols] = df[y_cols + x_cols].apply(pd.to_numeric, errors='ignore')
    for col in x_cols + y_cols:
        df[col] = df[col].fillna(df.groupby('date')[col].transform('median'))
    for dt in df.index.levels[0].unique():
        for y in y_cols:
            df.loc[dt, y] = ols_res(df.loc[dt, x_cols], df.loc[dt, y]).values
    df = df.reset_index()
    return df



class FactorNeutralization(CalibrationTaskflowTask):
    '''

    Neutralizes common risk factors through regression residualization

    '''

    PROVIDES_FIELDS = ["neutralized_data"]
    REQUIRES_FIELDS = ["russell_data"]
    def __init__(self, factors=None):
        self.data = None
        self.factors = factors or ['SPY_beta', 'log_mktcap', 'ret_5B', 'ret_21B', 'volatility_63', 'volatility_126',
                                   'momentum']
        CalibrationTaskflowTask.__init__(self)


    def do_step_action(self, **kwargs):
        df = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        self.data = neutralize_data(df, self.factors)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"factors": ['SPY_beta', 'log_mktcap', 'ret_5B', 'ret_21B', 'volatility_63', 'volatility_126',
                            'momentum']}


class FactorNeutralizationForStacking(CalibrationTaskflowTask):
    '''

    Neutralizes common risk factors through regression residualization

    '''

    REQUIRES_FIELDS = ["r1k_models"]
    PROVIDES_FIELDS = ["r1k_resid_models"]

    def __init__(self, factors, exclusion_list):
        self.r1k_resid_models = None
        self.factors = factors or ['SPY_beta', 'log_mktcap', 'ret_5B', 'ret_21B', 'volatility_63', 'volatility_126',
                                   'momentum']
        self.exclusion_list = exclusion_list or ['assetturnover', 'wt-r1', 'wt-r1g', 'wt-r1v', 'zerodays', 'SPY_correl',
                                                 'revenue_growth']
        CalibrationTaskflowTask.__init__(self)


    def do_step_action(self, **kwargs):
        # Strangely dict -> dataframe -> dict conversion seems to introduce a nested dictionary with a 0 key.
        r1k_models = kwargs["r1k_models"].copy(deep=True).to_dict(orient='dict')[0]

        assert set(r1k_models)==set(FILTER_MODES), "FactorNeutralizationForStacking - r1k_models doesn't seem to contain all \
        expected modes. It contains- {0}".format(set(r1k_models))

        r1k_resid_dict = {}
        for mode in r1k_models:
            print("Neutralizing r1k data for {0} model".format(mode))
            r1k_resid_dict[mode] = neutralize_data(r1k_models[mode], self.factors, self.exclusion_list)

        self.r1k_resid_models = pd.DataFrame.from_dict(r1k_resid_dict, orient='index')
        return StatusType.Success

    def _get_additional_step_results(self):
        return {"r1k_resid_models": self.r1k_resid_models}

    @classmethod
    def get_default_config(cls):
        return {"factors": ['SPY_beta', 'log_mktcap', 'ret_5B', 'ret_21B', 'volatility_63', 'volatility_126',
                            'momentum'],
                "exclusion_list": ['fq', 'divyield_Industrials', 'PPO_21_126_ConsumerDiscretionary', 'DNDGRG3M086SBEA','EXUSUKx',
                                   'GS10', 'IPDCONGD', 'T5YFFM', 'USTRADE', 'CUSR0000SA0L2', 'RETAILx', 'bm_Financials', 'OILPRICEx',
                                   'T10YFFM', 'CPITRNSL', 'CP3Mx', 'CUSR0000SAC', 'EWJ_volume', 'SPY_close', 'VXOCLSx',
                                   'PPO_21_126_InformationTechnology', 'WPSID62', 'GS5', 'COMPAPFFx', 'CUSR0000SA0L5', 'T1YFFM',
                                   'PPO_21_126_Energy', 'bm_Utilities', 'PPO_21_126_Financials', 'HWI', 'RPI', 'PPO_21_126_Industrials',
                                   'divyield_ConsumerStaples', 'EWG_close', 'macd_diff_ConsumerStaples', 'AAAFFM', 'fold_id', 'f_t', 'f_t+1',
                                   'hidden_state_0', 'hidden_state_1', 'hidden_state_2', 'hidden_state_3', 'future_return_RF',
                                   'ols', 'lasso', 'enet', 'et', 'rf', 'gbm', 'ensemble', 'ols_const', 'lasso_const', 'enet_const',
                                   'et_const', 'rf_const', 'gbm_const', 'ensemble_const', 'total_ensemble', 'ffn']}


class FactorNeutralizationForStackingWeekly(FactorNeutralizationForStacking):

    REQUIRES_FIELDS = ["r1k_models", "r1k_models_sc_weekly", "r1k_models_lc_weekly"]
    PROVIDES_FIELDS = ["r1k_resid_models", "r1k_resid_sc_weekly", "r1k_resid_lc_weekly"]

    def __init__(self, factors, exclusion_list):
        self.r1k_resid_sc_weekly = None
        self.r1k_resid_lc_weekly = None
        FactorNeutralizationForStacking.__init__(self, factors, exclusion_list)

    def do_step_action(self, **kwargs):
        # Strangely dict -> dataframe -> dict conversion seems to introduce a nested dictionary with a 0 key.
        r1k_models_monthly = kwargs["r1k_models"].copy(deep=True).to_dict(orient='dict')[0]
        r1k_models_sc_weekly = kwargs["r1k_models_sc_weekly"].copy(deep=True).to_dict(orient='dict')[0]
        r1k_models_lc_weekly = kwargs["r1k_models_lc_weekly"].copy(deep=True).to_dict(orient='dict')[0]

        assert set(r1k_models_monthly)==set(FILTER_MODES), "FactorNeutralizationForStacking - r1k_models_monthly doesn't seem to \
        contain all expected modes. It contains- {0}".format(set(r1k_models_monthly))
        assert set(r1k_models_sc_weekly).union(set(r1k_models_lc_weekly))==set(FILTER_MODES), "FactorNeutralizationForStacking - \
        r1k_models_weekly doesn't seem to contain all expected modes. \
        It contains- {0}".format(set(r1k_models_sc_weekly).union(r1k_models_lc_weekly))

        r1k_resid_dict_monthly = {}
        r1k_resid_sc_dict_weekly = {}
        r1k_resid_lc_dict_weekly = {}
        for mode in r1k_models_monthly:
            print("Neutralizing r1k data for {0} model".format(mode))
            r1k_resid_dict_monthly[mode] = neutralize_data(r1k_models_monthly[mode], self.factors, self.exclusion_list)
            if "largecap" in mode:
                r1k_resid_lc_dict_weekly[mode] = neutralize_data(r1k_models_lc_weekly[mode], self.factors, self.exclusion_list)
            else:
                r1k_resid_sc_dict_weekly[mode] = neutralize_data(r1k_models_sc_weekly[mode], self.factors, self.exclusion_list)

        self.r1k_resid_models = pd.DataFrame.from_dict(r1k_resid_dict_monthly, orient='index')
        self.r1k_resid_sc_weekly = pd.DataFrame.from_dict(r1k_resid_sc_dict_weekly, orient='index')
        self.r1k_resid_lc_weekly = pd.DataFrame.from_dict(r1k_resid_lc_dict_weekly, orient='index')
        return StatusType.Success

    def _get_additional_step_results(self):
        return {"r1k_resid_models": self.r1k_resid_models,
                "r1k_resid_sc_weekly": self.r1k_resid_sc_weekly,
                "r1k_resid_lc_weekly": self.r1k_resid_lc_weekly}


def factor_standarization(df, X_cols, y_col, exclude_from_standardization):
    df = df.copy(deep=True).set_index(["date", "ticker"])
    not_X_col = [k for k in X_cols if k not in df.columns]
    print("Missing X columns: {0}".format(not_X_col))
    not_y_col = [k for k in y_col if k not in df.columns]
    print("Missing Y columns: {0}".format(not_y_col))
    X_cols = [k for k in X_cols if k not in not_X_col]
    y_col = [k for k in y_col if k not in not_y_col]
    exclude_from_standardization_df = df[list(set(df.columns).intersection(set(exclude_from_standardization)))]
    df = df[y_col + X_cols]

    lb = df.groupby('date').transform(pd.Series.quantile, 0.005)
    lb.fillna(-999999999999999, inplace=True)
    ub = df.groupby('date').transform(pd.Series.quantile, 0.995)
    ub.fillna(999999999999999, inplace=True)
    new_df = df.clip(lb, ub)
    df = new_df
    #df[df<=lb]=lb
    #df[df>=ub]=ub
    #new_df = df.copy()
    new_df = new_df.groupby("date").transform(lambda x: (x - x.mean()) / x.std())
    new_df.fillna(0, inplace=True)
    renaming = {k:"{0}_std".format(k) for k in y_col}
    new_df.rename(columns=renaming, inplace=True)
    df = df[y_col]
    new_df = pd.concat([new_df, df, exclude_from_standardization_df], axis=1)
    new_df = new_df.reset_index()
    new_df["ticker"] = new_df["ticker"].astype(int)
    new_df["Sector"] = new_df["Sector"].astype(str)
    new_df["IndustryGroup"] = new_df["IndustryGroup"].astype(str)
    return new_df

class FactorStandardization(CalibrationTaskflowTask):
    '''

    Cross-sectional standardization of features

    '''

    PROVIDES_FIELDS = ["normalized_data"]
    REQUIRES_FIELDS = ["russell_data"]

    def __init__(self, all_features, exclude_from_standardization, target_columns, suffixes_to_exclude):
        self.data = None
        self.suffixes_to_remove = suffixes_to_exclude or []
        self.all_features = all_features or ['divyield', 'WILLR_14_MA_3', 'macd', 'bm', 'retvol', 'netmargin']
        self.exclude_from_standardization = exclude_from_standardization or []
        self.target_columns = target_columns or ['future_ret_21B']
        CalibrationTaskflowTask.__init__(self)

    def _select_features(self, df):
        if self.all_features is True:
            exclude_from_standardization = self.exclude_from_standardization
            for suffix in self.suffixes_to_remove:
                cols_to_exclude = [c for c in df.columns if c.endswith(suffix)]
                exclude_from_standardization.extend(cols_to_exclude)
            all_features = list(df.columns.difference(self.target_columns + exclude_from_standardization
                                                      + ["date", "ticker"]))
        else:
            all_features = self.all_features
        return all_features

    def do_step_action(self, **kwargs):
        df = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        all_features = self._select_features(df)
        self.data = factor_standarization(df, all_features, self.target_columns, self.exclude_from_standardization)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"all_features": ['divyield', 'WILLR_14_MA_3', 'macd', 'bm', 'retvol', 'netmargin'],
                "exclude_from_standardization": [], "target_columns": ["future_ret_21B"],
                "suffixes_to_exclude": ["_std"]}

class FactorStandardizationFullPopulation(FactorStandardization):
    '''

    Cross-sectional standardization of features

    '''

    PROVIDES_FIELDS = ["normalized_data_full_population"]
    REQUIRES_FIELDS = ["merged_data_econ_industry"]

    @classmethod
    def get_full_class_requires(cls, include_parent_classes=True, include_base_etl_requires=True):
        reqs = cls.__bases__[0].__bases__[0].get_full_class_requires(include_parent_classes, include_base_etl_requires)
        reqs = reqs + list(cls.REQUIRES_FIELDS)
        return reqs

    @classmethod
    def get_full_class_provides(cls, include_parent_classes=True, include_base_etl_provides=True):
        provs = cls.__bases__[0].__bases__[0].get_full_class_provides(include_parent_classes, include_base_etl_provides)
        provs = set(list(provs) + list(cls.PROVIDES_FIELDS))
        return provs


class FactorStandardizationFullPopulationWeekly(FactorStandardization):

    PROVIDES_FIELDS = ["normalized_data_full_population", "normalized_data_full_population_weekly"]
    REQUIRES_FIELDS = ["merged_data_econ_industry", "merged_data_econ_industry_weekly"]

    def __init__(self, all_features, exclude_from_standardization, target_columns, suffixes_to_exclude):
        self.weekly_data = None
        FactorStandardization.__init__(self, all_features, exclude_from_standardization, target_columns, suffixes_to_exclude)

    def do_step_action(self, **kwargs):
        monthly_df = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        weekly_df = kwargs[self.__class__.REQUIRES_FIELDS[1]]
        all_features = self._select_features(weekly_df) # should be same with both monthly and weekly df
        self.data = factor_standarization(monthly_df, all_features, self.target_columns, self.exclude_from_standardization)
        self.weekly_data = factor_standarization(weekly_df, all_features, self.target_columns, self.exclude_from_standardization)
        return StatusType.Success

    @classmethod
    def get_full_class_requires(cls, include_parent_classes=True, include_base_etl_requires=True):
        reqs = cls.__bases__[0].__bases__[0].get_full_class_requires(include_parent_classes, include_base_etl_requires)
        reqs = reqs + list(cls.REQUIRES_FIELDS)
        return reqs

    @classmethod
    def get_full_class_provides(cls, include_parent_classes=True, include_base_etl_provides=True):
        provs = cls.__bases__[0].__bases__[0].get_full_class_provides(include_parent_classes, include_base_etl_provides)
        provs = set(list(provs) + list(cls.PROVIDES_FIELDS))
        return provs

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.weekly_data}



class FactorStandardizationNeutralized(FactorStandardization):
    '''

    Cross-sectional standardization of features on factor-neutralized data

    '''

    PROVIDES_FIELDS = ["normalized_neutralized_data"]
    REQUIRES_FIELDS = ["neutralized_data"]

    @classmethod
    def get_full_class_requires(cls, include_parent_classes=True, include_base_etl_requires=True):
        reqs = cls.__bases__[0].__bases__[0].get_full_class_requires(include_parent_classes, include_base_etl_requires)
        reqs = reqs + list(cls.REQUIRES_FIELDS)
        return reqs

    @classmethod
    def get_full_class_provides(cls, include_parent_classes=True, include_base_etl_provides=True):
        provs = cls.__bases__[0].__bases__[0].get_full_class_provides(include_parent_classes, include_base_etl_provides)
        provs = set(list(provs) + list(cls.PROVIDES_FIELDS))
        return provs


class FactorStandardizationNeutralizedForStacking(CalibrationTaskflowTask):
    '''

    Cross-sectional standardization of features on factor-neutralized data

    '''

    REQUIRES_FIELDS =  ["r1k_resid_models"]
    PROVIDES_FIELDS =  ["r1k_neutral_normal_models"]

    def __init__(self, all_features, exclude_from_standardization, target_columns, suffixes_to_exclude):
        self.r1k_neutral_normal_models = None
        self.suffixes_to_remove = suffixes_to_exclude or []
        self.all_features = all_features or ['divyield', 'WILLR_14_MA_3', 'macd', 'bm', 'retvol', 'netmargin']
        self.exclude_from_standardization = exclude_from_standardization or []
        self.target_columns = target_columns or ['future_ret_21B']
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        r1k_resid_models = kwargs["r1k_resid_models"].copy(deep=True).to_dict(orient='dict')[0]

        assert set(r1k_resid_models)==set(FILTER_MODES), "FactorStandardizationNeutralizedForStacking - r1k_resid_models \
        doesn't seem to contain all expected modes. It contains - {0}".format(set(r1k_resid_models))

        #TODO: fix the logic flow when self.all_features is not True
        if self.all_features is True:
            for suffix in self.suffixes_to_remove:
                cols_to_drop = [c for c in r1k_resid_models["value"].columns if c.endswith(suffix)]

            for mode in r1k_resid_models:
                r1k_resid_models[mode] = r1k_resid_models[mode][list(set(r1k_resid_models[mode].columns).difference(set(cols_to_drop)))]

            all_features = list(r1k_resid_models["value"].columns.difference(self.target_columns + self.exclude_from_standardization
                                                               + ["date", "ticker"]))

        r1k_neutral_normal_dict = {}
        for mode in r1k_resid_models:
            print("Standardizing r1k_resid data for {0} model".format(mode))
            r1k_neutral_normal_dict[mode] = factor_standarization(r1k_resid_models[mode], all_features, self.target_columns,
                                                                  self.exclude_from_standardization)

        self.r1k_neutral_normal_models = pd.DataFrame.from_dict(r1k_neutral_normal_dict, orient='index')
        return StatusType.Success

    def _get_additional_step_results(self):
        return {"r1k_neutral_normal_models" : self.r1k_neutral_normal_models}

    @classmethod
    def get_default_config(cls):
        return {"all_features": True,
                "exclude_from_standardization": ['RETAILx', 'VXOCLSx', 'USTRADE', 'SPY_close', 'bm_Financials', 'T5YFFM',
                                                 'CPITRNSL', 'OILPRICEx', 'T1YFFM', 'HWI', 'CUSR0000SA0L2', 'CUSR0000SA0L5',
                                                 'EWJ_volume', 'DNDGRG3M086SBEA', 'AAAFFM', 'RPI', 'macd_diff_ConsumerStaples',
                                                 'fq', 'EXUSUKx', 'COMPAPFFx', 'PPO_21_126_Industrials', 'PPO_21_126_Financials',
                                                 'fold_id', 'CP3Mx', 'divyield_ConsumerStaples', 'T10YFFM', 'GS10',
                                                 'bm_Utilities', 'EWG_close', 'Sector', 'CUSR0000SAC', 'GS5', 'divyield_Industrials',
                                                 'WPSID62', 'IPDCONGD', 'PPO_21_126_InformationTechnology', 'PPO_21_126_Energy',
                                                 'PPO_21_126_ConsumerDiscretionary', 'IndustryGroup'],
                "target_columns": ['future_ret_1B', 'future_asset_growth_qoq', 'future_ret_21B', 'future_revenue_growth_qoq',
                                   'future_ret_5B', 'future_ret_10B', 'future_ret_42B'],
                "suffixes_to_exclude": ["_std"]}


class FactorStandardizationNeutralizedForStackingWeekly(FactorStandardizationNeutralizedForStacking):

    REQUIRES_FIELDS =  ["r1k_resid_models", "r1k_resid_sc_weekly", "r1k_resid_lc_weekly"]
    PROVIDES_FIELDS =  ["r1k_neutral_normal_models", "r1k_neutral_normal_sc_weekly", "r1k_neutral_normal_lc_weekly"]

    def __init__(self, all_features, exclude_from_standardization, target_columns, suffixes_to_exclude):
        self.r1k_neutral_normal_sc_weekly = None
        self.r1k_neutral_normal_lc_weekly = None
        FactorStandardizationNeutralizedForStacking.__init__(self, all_features, exclude_from_standardization,
                                                             target_columns, suffixes_to_exclude)

    def do_step_action(self, **kwargs):
        r1k_resid_models_monthly = kwargs["r1k_resid_models"].copy(deep=True).to_dict(orient='dict')[0]
        r1k_resid_sc_weekly = kwargs["r1k_resid_sc_weekly"].copy(deep=True).to_dict(orient='dict')[0]
        r1k_resid_lc_weekly = kwargs["r1k_resid_lc_weekly"].copy(deep=True).to_dict(orient='dict')[0]

        assert set(r1k_resid_models_monthly)==set(FILTER_MODES), "FactorStandardizationNeutralizedForStacking - r1k_resid_models_monthly \
        doesn't seem to contain all expected modes. It contains - {0}".format(set(r1k_resid_models_monthly))
        assert set(r1k_resid_sc_weekly).union(set(r1k_resid_lc_weekly))==set(FILTER_MODES), "FactorStandardizationNeutralizedForStacking \
        - r1k_resid_models_weekly doesn't seem to contain all expected modes. \
        It contains - {0}".format(set(r1k_resid_sc_weekly).union(set(r1k_resid_lc_weekly)))

        #TODO: fix the logic flow when self.all_features is not True
        if self.all_features is True:
            for suffix in self.suffixes_to_remove:
                cols_to_drop = [c for c in r1k_resid_models_monthly["value"].columns if c.endswith(suffix)]

            for mode in r1k_resid_models_monthly:
                r1k_resid_models_monthly[mode] = r1k_resid_models_monthly[mode][list(set(r1k_resid_models_monthly[mode].columns).difference(set(cols_to_drop)))]
                if "largecap" in mode:
                    r1k_resid_lc_weekly[mode] = r1k_resid_lc_weekly[mode][list(set(r1k_resid_lc_weekly[mode].columns).difference(set(cols_to_drop)))]
                else:
                    r1k_resid_sc_weekly[mode] = r1k_resid_sc_weekly[mode][list(set(r1k_resid_sc_weekly[mode].columns).difference(set(cols_to_drop)))]

            all_features = list(r1k_resid_models_monthly["value"].columns.difference(self.target_columns + \
                                                                                     self.exclude_from_standardization \
                                                                                     + ["date", "ticker"]))

        r1k_neutral_dict_monthly = {}
        r1k_neutral_sc_dict_weekly = {}
        r1k_neutral_lc_dict_weekly = {}
        for mode in r1k_resid_models_monthly:
            print("Standardizing r1k_resid data for {0} model".format(mode))
            r1k_neutral_dict_monthly[mode] = factor_standarization(r1k_resid_models_monthly[mode], all_features, self.target_columns,
                                                                   self.exclude_from_standardization)
            if "largecap" in mode:
                r1k_neutral_lc_dict_weekly[mode] = factor_standarization(r1k_resid_lc_weekly[mode], all_features, self.target_columns,
                                                                         self.exclude_from_standardization)
            else:
                r1k_neutral_sc_dict_weekly[mode] = factor_standarization(r1k_resid_sc_weekly[mode], all_features, self.target_columns,
                                                                         self.exclude_from_standardization)

        self.r1k_neutral_normal_models = pd.DataFrame.from_dict(r1k_neutral_dict_monthly, orient='index')
        self.r1k_neutral_normal_sc_weekly = pd.DataFrame.from_dict(r1k_neutral_sc_dict_weekly, orient='index')
        self.r1k_neutral_normal_lc_weekly = pd.DataFrame.from_dict(r1k_neutral_lc_dict_weekly, orient='index')
        return StatusType.Success

    def _get_additional_step_results(self):
        return {"r1k_neutral_normal_models" : self.r1k_neutral_normal_models,
                "r1k_neutral_normal_sc_weekly" : self.r1k_neutral_normal_sc_weekly,
                "r1k_neutral_normal_lc_weekly" : self.r1k_neutral_normal_lc_weekly}


class RollingModelEstimation(CalibrationTaskflowTask):
    '''

    Cross-sectional standardization of features on factor-neutralized data

    '''

    PROVIDES_FIELDS =  ["signals", "rolling_model_info"]
    REQUIRES_FIELDS =  ["r1k_neutral_normal_models_with_foldId"]

    def __init__(self, date_combinations, ensemble_weights, bucket, key_base, local_save_dir,
                 training_modes, model_codes, target_cols, return_cols):
        self.signals = None
        self.rolling_model_info = None
        self.signal_consolidated = None
        self.results = defaultdict(dict)
        self.date_combinations = date_combinations
        self.ensemble_weights = ensemble_weights
        self.local_save_dir = local_save_dir
        self.bucket = bucket
        self.key_base = key_base
        self.training_modes = training_modes
        self.model_codes = model_codes
        self.target_cols = target_cols
        self.return_cols = return_cols
        CalibrationTaskflowTask.__init__(self)

    def _get_results_filename(self, model):
        if model == "value":
            results_file = 'results_v.joblib'
        elif model == "growth":
            results_file = 'results_g.joblib'
        elif model == "largecap_value":
            results_file = 'results_lv.joblib'
        elif model == "largecap_growth":
            results_file = 'results_lg.joblib'
        else:
            raise ValueError("Got unexpected type of model to fetch the results_*.joblib filename - {0}".format(model))
        return results_file

    def rolling_estimation_common(self, df_v, df_g):
        model = self.value_model_code
        results_v = {}
        for (year, month) in self.date_combinations:
            current, key = run_for_date(df_v, out_dir, year, month, model, True, True, False)
            all_algos = {}
            for k in current.keys():
                all_algos[k] = current[k]["estimator"]
            results_v[key] = all_algos

        model = self.growth_model_code
        results_g = {}
        for (year, month) in self.date_combinations:
            current, key = run_for_date(df_g, out_dir, year, month, model, True, True, False)
            all_algos = {}
            for k in current.keys():
                all_algos[k] = current[k]["estimator"]
            results_g[key] = all_algos

        create_directory_if_does_not_exists(full_path)
        dump(results_v, os.path.join(full_path, "results_v.joblib"))
        dump(results_g, os.path.join(full_path, "results_g.joblib"))

    def train_rolling_models(self, model_name, model_df):
        mode = self.task_params.run_mode
        if mode==TaskflowPipelineRunMode.Test:
            full_path = os.path.abspath(os.path.join(os.path.join(TEST_DATA_DIR, UNIT_TEST_MODEL_BASE), self.local_save_dir))
            self.results[model_name] = load(os.path.join(full_path, self._get_results_filename(model_name)))

        else:
            full_path = get_local_dated_dir(os.environ["MODEL_DIR"], self.task_params.end_dt, self.local_save_dir)
            print("***********************************************")
            print("***********************************************")
            print(full_path)

            out_dir = ""
            model_code = self.model_codes[model_name]
            target_col = self.target_cols[str(model_code)]
            for (year, month) in self.date_combinations:
                print("Final model training for: ", model_name, ", for date_combination: ", (year, month))
                current, key = run_for_date(model_df, out_dir, year, month, target_col, model_code, True, True, False)
                all_algos = {}
                for k in current.keys():
                    all_algos[k] = current[k]["estimator"]
                self.results[model_name][key] = all_algos

            create_directory_if_does_not_exists(full_path)
            dump(self.results[model_name], os.path.join(full_path, self._get_results_filename(model_name)))

        # DISCUSS: This will be same for every model trained, and will be overwritten every time after the training.
        self.rolling_model_info = pd.DataFrame([self.local_save_dir], columns=["relative_path"])

    def train_rolling_models_append(self, model_name, model_df):
        self.load_model(model_name)
        self.train_rolling_models(model_name, model_df)

    def temp_load_models(self): # For testing only
        current_dir = r"C:\DCM\temp\pipeline_tests\snapshots_201910\models_v\20171229"
        models = ['enet', 'gbm', 'rf', 'ols', 'et', 'lasso']
        self.results_v = {}
        self.results_g = {}

        for d in ["20171229", "20180329", "20180629", "20180928", "20181231", "20190329", "20190628"]:
            self.results_v[d] = {}
            for k in models:
                self.results_v[d][k] = load(os.path.join(current_dir, "{0}_neutral_r1_v_iso.joblib".format(k)))

        current_dir = r"C:\DCM\temp\pipeline_tests\snapshots_201910\models_g\20171229"
        for d in ["20171229", "20180329", "20180629", "20180928", "20181231", "20190329", "20190628"]:
            self.results_g[d] = {}
            for k in models:
                self.results_g[d][k] = load(os.path.join(current_dir, "{0}_neutral_r1_v_iso.joblib".format(k)))

    def load_model(self, model_name):
        self.s3_client = storage.Client()

        key = '{0}/{1}'.format(self.key_base, self._get_results_filename(model_name))
        with tempfile.TemporaryFile() as fp:
            bucket = self.s3_client.bucket(self.bucket)
            blob = bucket.blob(key)
            self.s3_client.download_blob_to_file(blob, fp)
            fp.seek(0)
            self.results[model_name] = load(fp)

    def prediction(self, algo, X):
        pred = pd.Series(algo.predict(X), index=X.index)
        return pred

    def generate_ensemble_prediction(self, df, algos, target_variable_name, return_column_name, feature_cols):
        ensemble_weights = self.ensemble_weights
        df = df.set_index(["date", "ticker"])
        X = df[feature_cols].fillna(0.0)
        ret = df[return_column_name].reset_index().fillna(0.0)
        pred = pd.concat((self.prediction(algos[k], X) for k in algos.keys()), axis=1, keys=algos.keys())
        ensemble_pred = (pred[list(algos.keys())[0]]*0.0)
        for k in algos.keys():
            ensemble_pred += ensemble_weights[k]*pred[k]
        ensemble_pred = ensemble_pred.to_frame()
        ensemble_pred.columns = ["ensemble"]
        rank = pred.groupby("date").rank(pct=True)
        rank["ensemble"] = ensemble_pred.groupby("date").rank(pct=True)
        signal_ranks = pd.merge(rank.reset_index(), ret[['date', 'ticker', return_column_name]], how='left', on=['date', 'ticker'])
        return signal_ranks

    def generate_ensemble_prediction_rolling(self, df, algos, target_variable_name, return_column_name, feature_cols, ensemble_weights=None):
        all_signals = {}
        for dt in list(algos.keys()):
            #print("Predictions for : \t {0}".format(dt))
            signal_ranks = self.generate_ensemble_prediction(df, algos[dt], target_variable_name, return_column_name, feature_cols)
            all_signals[dt] = signal_ranks
        return all_signals

    def concatenate_predictions(self, algos, all_signals):
        dates = list(algos.keys())
        dates.sort()
        real_dates = list(map(pd.Timestamp, dates))
        real_dates.append(pd.Timestamp("2040-12-31"))
        #print(real_dates)
        signal_ranks = []
        for ind in range(len(dates)):
            key = dates[ind]
            #print(key)
            current_signal = all_signals[key]
            cond = (current_signal["date"]<real_dates[ind+1])
            if ind>0:
                cond = cond & (current_signal["date"]>=real_dates[ind])
            current_signal = current_signal[cond]
            #print(current_signal["date"].unique())
            signal_ranks.append(current_signal)

        signal_ranks = pd.concat(signal_ranks, ignore_index=True)
        return signal_ranks

    def do_step_action(self, **kwargs):
        r1k_dfs_dict = kwargs["r1k_neutral_normal_models_with_foldId"].copy(deep=True).to_dict(orient='dict')[0]

        assert set(r1k_dfs_dict)==set(FILTER_MODES), "RollingModelEstimation - r1k_neutral_normal_models_with_foldId dict\
        doesn't seem to contain all expected modes. It contains- {0}".format(set(r1k_dfs_dict))

        signals_dict = {}
        for model_name in r1k_dfs_dict:
            if self.training_modes[model_name] == 'load_from_s3':
                self.load_model(model_name)
            elif self.training_modes[model_name] == 'full_train':
                self.train_rolling_models(model_name, r1k_dfs_dict[model_name])
            elif self.training_modes[model_name] == 'append':
                self.train_rolling_models_append(model_name, r1k_dfs_dict[model_name])
            else:
                raise ValueError("RollingModelEstimation - training_modes should be one of - 'load_from_s3', 'full_train', or 'append'")

            model_code = self.model_codes[model_name]
            target_variable_name = self.target_cols[str(model_code)] # get_y_col(model_code)
            return_column_name = self.return_cols[str(model_code)] # get_return_col(model_code)
            feature_cols = get_X_cols(model_code)
            all_signals = self.generate_ensemble_prediction_rolling(r1k_dfs_dict[model_name], self.results[model_name], \
                                                                    target_variable_name, return_column_name, feature_cols)
            signals_dict[model_name] = self.concatenate_predictions(self.results[model_name], all_signals)

        self.signals = pd.DataFrame.from_dict(signals_dict, orient='index')
        return StatusType.Success

    def _get_additional_step_results(self):
        return {"signals" : self.signals,
                "rolling_model_info" : self.rolling_model_info}

    @classmethod
    def get_default_config(cls):
        return {"date_combinations": [[2017, 12], [2018, 3], [2018, 6], [2018, 9], [2018, 12], [2019, 3], [2019, 6]],
                "ensemble_weights": {'enet': 0.03333333333333333, 'et': 0.3, 'gbm': 0.2,
                                     'lasso': 0.03333333333333333, 'ols': 0.03333333333333333, 'rf': 0.4},
                "local_save_dir": "rolling_models",
                "bucket": "dcm-data-temp", "key_base": "saved_rolling_models",
                "training_modes": {"value": "load_from_s3", "growth": "load_from_s3", "largecap": "load_from_s3"},
                "model_codes": {"value": 1, "growth": 2, "largecap_value": 6, "largecap_growth": 7},
                "target_cols": {"1": "future_ret_21B_std", "2": "future_ret_21B_std", "6": "future_ret_21B_std", "7": "future_ret_21B_std"},
                "return_cols": {"1": "future_ret_21B", "2": "future_ret_21B", "6": "future_ret_21B", "7": "future_ret_21B"}
                }


class RollingModelEstimationWeekly(RollingModelEstimation):

    PROVIDES_FIELDS =  ["signals", "signals_weekly", "rolling_model_info"]
    REQUIRES_FIELDS =  ["r1k_neutral_normal_models_with_foldId", "r1k_sc_with_foldId_weekly", "r1k_lc_with_foldId_weekly"]

    def __init__(self, date_combinations, ensemble_weights, bucket, key_base, local_save_dir,
                 training_modes, model_codes, target_cols, return_cols):
        self.signals_weekly = None
        RollingModelEstimation.__init__(self, date_combinations, ensemble_weights, bucket, key_base, local_save_dir,
                                        training_modes, model_codes, target_cols, return_cols)

    def do_step_action(self, **kwargs):
        r1k_dfs_dict_monthly = kwargs["r1k_neutral_normal_models_with_foldId"].copy(deep=True).to_dict(orient='dict')[0]
        r1k_dfs_dict_weekly = kwargs["r1k_sc_with_foldId_weekly"].copy(deep=True).to_dict(orient='dict')[0]
        r1k_dfs_dict_weekly.update(kwargs["r1k_lc_with_foldId_weekly"].copy(deep=True).to_dict(orient='dict')[0])

        assert set(r1k_dfs_dict_monthly)==set(FILTER_MODES), "RollingModelEstimation - r1k_dfs_dict_monthly dict\
        doesn't seem to contain all expected modes. It contains- {0}".format(set(r1k_dfs_dict_monthly))
        assert set(r1k_dfs_dict_weekly)==set(FILTER_MODES), "RollingModelEstimation - r1k_dfs_dict_weekly dict\
        doesn't seem to contain all expected modes. It contains- {0}".format(set(r1k_dfs_dict_weekly))

        signals_dict_monthly = {}
        signals_dict_weekly = {}
        for model_name in r1k_dfs_dict_monthly:
            if self.training_modes[model_name] == 'load_from_s3':
                self.load_model(model_name)
            elif self.training_modes[model_name] == 'full_train':
                self.train_rolling_models(model_name, r1k_dfs_dict_weekly[model_name])
            elif self.training_modes[model_name] == 'append':
                self.train_rolling_models_append(model_name, r1k_dfs_dict_weekly[model_name])
            else:
                raise ValueError("RollingModelEstimation - training_modes should be one of - 'load_from_s3', 'full_train', or 'append'")

            model_code = self.model_codes[model_name]
            target_variable_name = self.target_cols[str(model_code)] # get_y_col(model_code)
            return_column_name = self.return_cols[str(model_code)] # get_return_col(model_code)
            feature_cols = get_X_cols(model_code)

            all_signals_monthly = self.generate_ensemble_prediction_rolling(r1k_dfs_dict_monthly[model_name], self.results[model_name], \
                                                                            target_variable_name, return_column_name, feature_cols)
            signals_dict_monthly[model_name] = self.concatenate_predictions(self.results[model_name], all_signals_monthly)

            all_signals_weekly = self.generate_ensemble_prediction_rolling(r1k_dfs_dict_weekly[model_name], self.results[model_name], \
                                                                           target_variable_name, return_column_name, feature_cols)
            signals_dict_weekly[model_name] = self.concatenate_predictions(self.results[model_name], all_signals_weekly)

        self.signals = pd.DataFrame.from_dict(signals_dict_monthly, orient='index')
        self.signals_weekly = pd.DataFrame.from_dict(signals_dict_weekly, orient='index')
        return StatusType.Success

    def _get_additional_step_results(self):
        return {"signals" : self.signals,
                "signals_weekly" : self.signals_weekly,
                "rolling_model_info" : self.rolling_model_info}


class ConvertSignalsToInstructionFile(CalibrationTaskflowTask):


    PROVIDES_FIELDS =  ["all_instructions"]
    REQUIRES_FIELDS =  ["signals", "security_master"]

    def __init__(self, number_of_quantiles, actorID, rebalancing_frequency, rebalancing_time, convert_to_ib=False):
        self.number_of_quantiles = number_of_quantiles
        self.actorID = actorID
        self.rebalancing_frequency = rebalancing_frequency
        self.rebalancing_time = rebalancing_time
        self.data = None
        self.all_instructions = None
        self.convert_to_ib = convert_to_ib
        CalibrationTaskflowTask.__init__(self)

    def generate_long_table(self, signal, sec_master):
        final_model = "ensemble"
        long_table = signal[["date", "ticker", final_model]].rename(columns={final_model: "signal_pctg_rank", "ticker": "dcm_security_id"})
        long_table = pd.merge(long_table, sec_master[["dcm_security_id", "cusip", "ticker"]], how="left", on=["dcm_security_id"])
        return long_table
    
    def convert_basket_to_ib_symbol(self, basket):
        new_basket = list(map(lambda x:special_dcm_to_ib_symbol_map.get(x, x), basket))
        return new_basket

    def _generate_instructions(self, signals_dict, security_master):
        holdings_dict = {}
        for model_name in signals_dict:
            holdings_dict[model_name] = self.generate_long_table(signals_dict[model_name], security_master)

        parameters =  {"Long" : {"hedge_ratio": 0, "leverage_ratio_per_trade": 1.0},
                       "Short" : {"hedge_ratio": 5000.0, "leverage_ratio_per_trade": 1.0},
                       "Long_Short" : {"hedge_ratio": 1.0, "leverage_ratio_per_trade": 1.0}}
        hedge_ratio = parameters["Long_Short"]["hedge_ratio"],
        leverage_ratio_per_trade = parameters["Long_Short"]["leverage_ratio_per_trade"]

        model_abrv_mapping = {"growth": 'g', "value": 'v', "largecap_growth": 'lg', "largecap_value": 'lv'}

        instructions = {}
        for quantile in self.number_of_quantiles:
            instructions[quantile] = {}
            instructions[quantile]["instructions_consolidated"] = pd.DataFrame()
            generate_data = {}
            for model_name, holdings_data in holdings_dict.items():
                model_actor_name = model_name.capitalize()
                actorID = "{0}_{1}_Long_Short".format(self.actorID, model_actor_name)
                holdings = holdings_data.copy(deep=True)
                holdings["qt"] = holdings.groupby('date')["signal_pctg_rank"].apply(pd.qcut, quantile, labels=False,
                                                                                    duplicates='drop')
                long_holdings = holdings[holdings["qt"]==quantile-1]
                long_holdings = long_holdings.groupby('date')['ticker'].apply(lambda x: list(np.unique(x)))

                short_holdings = holdings[holdings["qt"]==0]
                short_holdings = short_holdings.groupby('date')['ticker'].apply(lambda x: list(np.unique(x)))

                # Need to translate symbols
                if self.convert_to_ib:
                    long_holdings = long_holdings.apply(self.convert_basket_to_ib_symbol)
                    short_holdings = short_holdings.apply(self.convert_basket_to_ib_symbol)

                sample_input_data = pd.concat([long_holdings,short_holdings], axis=1,
                                              keys=["long_basket","short_basket"]).reset_index()

                all_df =[]
                for k,row in sample_input_data.iterrows():
                    df= {}
                    df["date"] = row["date"]
                    df["actorID"] = actorID
                    df["role"] = 2
                    df["ranking"] = 1
                    df["long_basket"] = row["long_basket"]
                    df["short_basket"] = row["short_basket"]
                    df["hedge_ratio"] = hedge_ratio
                    df["stop_limits_pct"] = (-99.99, 999.9)
                    df["basket_selloff_date"] = "2025-01-01"
                    df["rebalancing_frequency"]= self.rebalancing_frequency
                    df["rebalancing_time"] = self.rebalancing_time
                    df["leverage_ratio_per_trade"] = leverage_ratio_per_trade

                    df["total_funding"] = pd.np.NaN
                    df["fundingRequestsAmount"] = pd.np.NaN
                    df["funding_buffer"] = pd.np.NaN
                    df["override_filter"] = pd.np.NaN
                    df["instructions_overrides"] = pd.np.NaN
                    all_df.append(df)
                res = pd.DataFrame(all_df)
                res = res[["date","actorID","role","ranking","long_basket","short_basket","hedge_ratio","stop_limits_pct",
                           "basket_selloff_date","rebalancing_frequency","rebalancing_time","leverage_ratio_per_trade",
                           "total_funding","fundingRequestsAmount","funding_buffer","override_filter","instructions_overrides"]]
                generate_data[model_name] = res

                model_abrv = model_abrv_mapping[model_name]
                long_short_key = "instructions_" + model_abrv + "_ls"
                long_key = "instructions_" + model_abrv + "_l"
                short_key = "instructions_" + model_abrv + "_s"

                instructions[quantile][long_short_key] = generate_data[model_name]

                instructions[quantile][long_key] = instructions[quantile][long_short_key].copy()
                instructions[quantile][long_key]["hedge_ratio"] = parameters["Long"]["hedge_ratio"]
                instructions[quantile][long_key]["leverage_ratio_per_trade"] = parameters["Long"]["leverage_ratio_per_trade"]
                instructions[quantile][long_key]["actorID"] = "{0}_{1}_Long".format(self.actorID, model_actor_name)

                instructions[quantile][short_key] = instructions[quantile][long_short_key].copy()
                instructions[quantile][short_key]["hedge_ratio"] = parameters["Short"]["hedge_ratio"]
                instructions[quantile][short_key]["leverage_ratio_per_trade"] = parameters["Short"]["leverage_ratio_per_trade"]
                instructions[quantile][short_key]["actorID"] = "{0}_{1}_Short".format(self.actorID, model_actor_name)

                instructions[quantile]["instructions_consolidated"] = pd.concat([instructions[quantile]["instructions_consolidated"],
                                                                                 instructions[quantile][long_short_key],
                                                                                 instructions[quantile][long_key],
                                                                                 instructions[quantile][short_key]],
                                                                                ignore_index=True)
        return instructions

    def do_step_action(self, **kwargs):
        signals_dict = kwargs["signals"].copy(deep=True).to_dict(orient='dict')[0]
        security_master = kwargs["security_master"]

        instructions_dict = self._generate_instructions(signals_dict, security_master)
        self.all_instructions = pd.DataFrame.from_dict(instructions_dict, orient='index')
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.all_instructions}

    @classmethod
    def get_default_config(cls):
        return {"number_of_quantiles" : [10], "actorID" : "Quantamental_Trader_GAN", "rebalancing_frequency" : "BMS",
                "rebalancing_time": "09:31:00", "convert_to_ib": True}


class ConvertSignalsToInstructionFileWeekly(ConvertSignalsToInstructionFile):

    PROVIDES_FIELDS =  ["all_instructions", "all_instructions_weekly"]
    REQUIRES_FIELDS =  ["signals", "signals_weekly", "security_master"]

    def __init__(self, number_of_quantiles, actorID, rebalancing_frequency, rebalancing_time, convert_to_ib=False):
        self.all_instructions_weekly = None
        ConvertSignalsToInstructionFile.__init__(self, number_of_quantiles, actorID,
                                                 rebalancing_frequency, rebalancing_time, convert_to_ib)

    def do_step_action(self, **kwargs):
        signals_dict_monthly = kwargs["signals"].copy(deep=True).to_dict(orient='dict')[0]
        signals_dict_weekly = kwargs["signals_weekly"].copy(deep=True).to_dict(orient='dict')[0]
        security_master = kwargs["security_master"]

        monthly_instructions_dict = self._generate_instructions(signals_dict_monthly, security_master)
        weekly_instructions_dict = self._generate_instructions(signals_dict_weekly, security_master)
        self.all_instructions = pd.DataFrame.from_dict(monthly_instructions_dict, orient='index')
        self.all_instructions_weekly = pd.DataFrame.from_dict(weekly_instructions_dict, orient='index')
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.all_instructions,
                self.__class__.PROVIDES_FIELDS[1] : self.all_instructions_weekly}


class DesignateCVFolds(CalibrationTaskflowTask):
    '''

    Add id's for cross validation

    '''

    PROVIDES_FIELDS = ["normalized_data_with_folds"]
    REQUIRES_FIELDS = ["normalized_data"]
    def __init__(self, n_folds):
        self.data = None
        self.n_folds = n_folds or 6
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        df = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)#.reset_index()
        dates = list(df["date"].unique())
        dates.sort()
        n_per_fold = int(pd.np.ceil(1.*len(dates)/self.n_folds))
        cut_points = [dates[r] for r in range(0, len(dates), n_per_fold)]
        cut_points = map(pd.Timestamp, cut_points + [pd.Timestamp("2050-12-31")])
        for ind in range(self.n_folds):
            cond = (df["date"]>=cut_points[ind]) & (df["date"]<cut_points[ind+1])
            df.loc[cond, "fold_id"] = ind
        self.data = df
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"n_folds": 6}

class TrainLinearModels(CalibrationTaskflowTask):
    '''

    Add id's for cross validation

    '''

    PROVIDES_FIELDS = ["linear_model_locations", "linear_model_cv_scores"]
    REQUIRES_FIELDS = ["normalized_data_with_folds"]
    SEED = 20190213
    def __init__(self, all_features, target_column, bucket, key_prefix, n_jobs=10, use_shap=True):
        self.data = None
        self.cv_scores = None
        self.x_cols = all_features
        self.y_col = target_column
        self.n_jobs = n_jobs or 10
        self.use_shap = use_shap
        self.bucket = bucket
        self.key_prefix = key_prefix
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        df = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        oos_id = df["fold_id"].max()
        df = df[df["fold_id"]<oos_id].reset_index(drop=True)
        df = df.set_index(["date", "ticker"]).sort_index()
        y = df[self.y_col].fillna(0)
        X = df[self.x_cols]
        n, p = X.shape
        folds = df["fold_id"]

        algos = {'ols': LinearRegression(), 'lasso': LassoCV(random_state=self.SEED),
                 'enet': ElasticNetCV(random_state=self.SEED)}
        grids = {'ols': None, 'lasso': None, 'enet': None}
        models = ['ols', 'lasso', 'enet']
        results = {k: train_ml_model(folds, X, y, algos[k], grids[k], self.n_jobs) for k in models}
        cv_scores = pd.concat([results[k]["cv_score"] for k in models], axis=1, keys=models)
        self.cv_scores = cv_scores
        model_locations = pd.DataFrame(index=models, columns=["bucket", "key"])
        for k in models:
            key = "{0}_{1}.joblib".format(self.key_prefix, k)
            with s3io.open('s3://{0}/{1}'.format(self.bucket, key), mode='w',
                           aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                           aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]) as s3_file:
                dump(results[k]["estimator"], s3_file)
            model_locations.loc[k, "bucket"] = self.bucket
            model_locations.loc[k, "key"] = key
        self.data = model_locations
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.cv_scores}

    @classmethod
    def get_default_config(cls):
        return {"n_jobs": 10,
                "all_features": ['divyield', 'WILLR_14_MA_3', 'macd', 'bm', 'retvol', 'netmargin'],
                "target_column": "future_ret_21B_std",
                "use_shap": True,
                "bucket": "dcm-data-temp",
                "key_prefix": "jack/"}

class QuantamentalDigitization(Digitization):
    '''

    Adds digitization of feature columns provided a digitization description (quantiles, groupby, and column names)
    for fundamentals calibration

    '''
    REQUIRES_FIELDS = ["merged_data", "normalized_neutralized_data"]
    PROVIDES_FIELDS = ["final_data"]

    def __init__(self, digitization_description):
        Digitization.__init__(self, digitization_description)

    def do_step_action(self, **kwargs):
        gc.collect()
        merged_data = kwargs[self.__class__.REQUIRES_FIELDS[1]]
        res = [merged_data]
        for digitization_type in self.digitization_description:
            calibration_logger.info("Running digitization schema: {0}".format(digitization_type))
            digitization_params = self.digitization_description[digitization_type]
            data_groups = merged_data.groupby(digitization_params["group_by"])[digitization_params["columns"]]
            shift = 1.0 if digitization_type=="tech_indicators_per_date" else 0.0
            cutpoints_in_sigmas = np.asarray(map(normal.ppf, digitization_params["quantiles"]))
            results = []
            for key, data in data_groups:
                digitized_cols = [self.add_digitized_columns_given_input_filter(data[x], cut_point_list = cutpoints_in_sigmas,
                                                                                suffix = digitization_params["suffix"], category_value_shift=shift,
                                                                                qs_are_quantiles=False) for x in digitization_params["columns"]]
                grp_result = pd.concat(digitized_cols, axis=1)
                grp_result+=shift-1.0
                assert grp_result.index.equals(data.index)
                results.append(grp_result)
            digitized_res = pd.concat(results, ignore_index=False)
            assert len(merged_data) == len(digitized_res)
            res.append(digitized_res) # = pd.merge(merged_data, digitized_res, left_index=True, right_index=True)
        self.data = pd.concat(res, axis=1)
        return StatusType.Success

    @classmethod
    def get_default_config(cls):
        return {"digitization_description": {"per_sector_date": \
                                             {"columns": ["ret_20B", "ret_60B", "ret_125B", "ret_252B", "indicator"],
                                              "quantiles": [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
                 "group_by": ["sector", "entry_date"],
                 "suffix": "_sector_date"}}}


class QuantamentalDigitizationRussell(Digitization):
    '''

    Adds digitization of feature columns provided a digitization description (quantiles, groupby, and column names)
    for fundamentals calibration

    '''
    REQUIRES_FIELDS = ["merged_data", "russell_data"]
    PROVIDES_FIELDS = ["final_data"]

    def __init__(self, digitization_description):
        Digitization.__init__(self, digitization_description)

    def do_step_action(self, **kwargs):
        gc.collect()
        merged_data = kwargs[self.__class__.REQUIRES_FIELDS[1]]
        res = [merged_data]
        for digitization_type in self.digitization_description:
            calibration_logger.info("Running digitization schema: {0}".format(digitization_type))
            digitization_params = self.digitization_description[digitization_type]
            data_groups = merged_data.groupby(digitization_params["group_by"])[digitization_params["columns"]]
            shift = 1.0 if digitization_type=="tech_indicators_per_date" else 0.0
            cutpoints_in_sigmas = np.asarray(map(normal.ppf, digitization_params["quantiles"]))
            results = []
            for key, data in data_groups:
                digitized_cols = [self.add_digitized_columns_given_input_filter(data[x], cut_point_list = cutpoints_in_sigmas,
                                                                                suffix = digitization_params["suffix"], category_value_shift=shift,
                                                                                qs_are_quantiles=False) for x in digitization_params["columns"]]
                grp_result = pd.concat(digitized_cols, axis=1)
                grp_result+=shift-1.0
                assert grp_result.index.equals(data.index)
                results.append(grp_result)
            digitized_res = pd.concat(results, ignore_index=False)
            assert len(merged_data) == len(digitized_res)
            res.append(digitized_res) # = pd.merge(merged_data, digitized_res, left_index=True, right_index=True)
        self.data = pd.concat(res, axis=1)
        return StatusType.Success

    @classmethod
    def get_default_config(cls):
        return {"digitization_description": {"per_sector_date": \
                                             {"columns": ["ret_20B", "ret_60B", "ret_125B", "ret_252B", "indicator"],
                                              "quantiles": [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
                 "group_by": ["sector", "entry_date"],
                 "suffix": "_sector_date"}}}


def feature_importance(X, y, algo, grid, folds, n_jobs=6):

    print(algo.__class__)

    n_folds = len(folds.unique())
    gkf = GroupKFold(n_splits=n_folds)
    splits = list(gkf.split(X, y, folds))

    tic = time()

    if grid is not None:
        cv = GridSearchCV(algo, grid, cv=splits, n_jobs=n_jobs, return_train_score=True)
        cv.fit(X, y)
        algo = cv.best_estimator_
        print(cv.best_params_)
        score = pd.DataFrame(cv.cv_results_).loc[cv.best_index_]
        score = score[score.index.str.startswith('split') & score.index.str.endswith('test_score')]

    else:
        if 'cv' in algo.get_params().keys():
            algo.set_params(cv=splits)
        algo.fit(X.values, y.values)
        score = pd.Series([algo.score(X.iloc[s[1]], y.iloc[s[1]]) for s in splits])

    toc = time()
    fit_time = (toc - tic) / 60.0

    #tic = time()
    #perm = PermutationImportance(algo).fit(X, y)
    #perm_vimp = pd.Series(perm.feature_importances_, index=X.columns).sort_values(ascending=False)
    #toc = time()
    #perm_time = (toc - tic) / 60.0

    #tic = time()

    #if use_shap:
        #if any(['leaf' in a for a in dir(algo)]):
            #explainer = shap.TreeExplainer(algo)
        #elif hasattr(algo, 'coef_') and hasattr(algo, 'intercept_'):
            #explainer = shap.LinearExplainer(algo, X, feature_dependence='correlation')
        #else:
            #explainer = shap.KernelExplainer(algo.predict, X)

        #if explainer is not None:
            #shap_values = pd.DataFrame(explainer.shap_values(X), columns=X.columns, index=X.index)
            #shap_vimp = shap_values.abs().mean().sort_values(ascending=False)
        #else:
            #shap_vimp = perm_vimp
    #else:
        #shap_vimp = perm_vimp

    #toc = time()
    #shap_time = (toc - tic) / 60.0

    #vimp = pd.concat([perm_vimp, shap_vimp], axis=1, keys=['PERM', 'SHAP'])
    lapsed = {'fit': fit_time}
    print(lapsed)

    return {
        'estimator': algo,
        'cv_score': score,
        'lapsed': lapsed
    }

class TrainIntermediateModels(CalibrationTaskflowTask):
    '''

    Add id's for cross validation

    '''

    PROVIDES_FIELDS = ["intermediate_signals", "econ_model_info"]
    REQUIRES_FIELDS = ["normalized_data_full_population_with_foldId"]
    #SEED = 20190213
    def __init__(self, y_col, X_cols, return_col, ensemble_weights, bucket, key_base, local_save_dir, load_from_s3=True):
        self.data = None
        self.econ_model_info = None
        self.y_col = y_col
        self.X_cols = X_cols
        self.return_col = return_col
        self.ensemble_weights = ensemble_weights
        self.load_from_s3 = load_from_s3
        self.bucket = bucket
        self.key_base = key_base
        self.local_save_dir = local_save_dir
        self.s3_client = None
        self.seed = 20190213
        if os.name=="nt":        
            self.n_jobs = 30
        else:
            self.n_jobs = 64
        CalibrationTaskflowTask.__init__(self)

    def _train_models(self, df):
        models = [
            'ols',
            'lasso',
            'enet',
            'rf',
            'et',
            'gbm'
        ]

        mode = self.task_params.run_mode
        if mode==TaskflowPipelineRunMode.Test:
            full_path = os.path.abspath(os.path.join(os.path.join(TEST_DATA_DIR, UNIT_TEST_MODEL_BASE), self.local_save_dir))
            results = {}
            for k in models:
                f = os.path.join(full_path, '%s_econ_no_gan.joblib' % k)
                results[k] = load(f)
            self.econ_model_info = pd.DataFrame([self.local_save_dir], columns=["relative_path"])
            print(results)
            return results

        full_path = get_local_dated_dir(os.environ["MODEL_DIR"], self.task_params.end_dt, self.local_save_dir)
        print("***********************************************")
        print("***********************************************")
        print(full_path)
        oos_id = df["fold_id"].max()
        # Cut training set
        df = df[df["fold_id"]<oos_id].reset_index(drop=True)
        df = df.set_index(["date", "ticker"]).sort_index()

        y = df[self.y_col].fillna(0)
        X = df[self.X_cols]

        algos = {
            'ols': LinearRegression(),
            'lasso': LassoCV(random_state=self.seed),
            'enet': ElasticNetCV(random_state=self.seed),
            'rf': RandomForestRegressor(n_estimators=500, max_depth=5, min_samples_leaf=0.001, random_state=self.seed, n_jobs=self.n_jobs),
            'et': ExtraTreesRegressor(n_estimators=500, max_depth=5, min_samples_leaf=0.001, random_state=self.seed, n_jobs=self.n_jobs),
            'gbm': GradientBoostingRegressor(n_estimators=200, min_samples_leaf=0.001, random_state=self.seed)
        }

        # CV only for lasso and enet
        grids = {
            'ols': None,
            'lasso': None,
            'enet': None,
            'rf': {'max_features': [6], 'max_depth': [6]},
            'et': {'max_features': [6], 'max_depth': [10]},
            'gbm': {'n_estimators': [400], 'max_depth': [7], 'learning_rate': [0.001]}
        }

        folds = df["fold_id"]
        results = {k: feature_importance(X, y, algos[k], grids[k], folds, self.n_jobs)["estimator"] for k in models}
        print(results)

        create_directory_if_does_not_exists(full_path)
        for k in results.keys():
            dump(results[k], os.path.join(full_path, '%s_econ_no_gan.joblib' % k))
        self.econ_model_info = pd.DataFrame([self.local_save_dir], columns=["relative_path"])
        return results

    def _load_models(self):
        self.s3_client = storage.Client()
        models = [
            'ols',
            'lasso',
            'enet',
            'rf',
            'et',
            'gbm'
        ]
        results = {}
        for model in models:
            key = '{0}/{1}_econ_no_gan.joblib'.format(self.key_base, model)
            with tempfile.TemporaryFile() as fp:
                bucket = self.s3_client.bucket(self.bucket)
                blob = bucket.blob(key)
                self.s3_client.download_blob_to_file(blob, fp)
                fp.seek(0)
                results[model] = load(fp)
        return results

    def prediction(self, algo, X):
        pred = pd.Series(algo.predict(X), index=X.index)
        return pred

    def generate_ensemble_prediction(self, df, algos, target_variable_name, return_column_name, feature_cols):
        ensemble_weights = self.ensemble_weights
        df = df.set_index(["date", "ticker"])
        X = df[feature_cols].fillna(0.0)
        ret = df[return_column_name].reset_index().fillna(0.0)
        pred = pd.concat((self.prediction(algos[k], X) for k in algos.keys()), axis=1, keys=algos.keys())
        ensemble_pred = (pred[list(algos.keys())[0]]*0.0)
        for k in algos.keys():
            ensemble_pred += ensemble_weights[k]*pred[k]
        ensemble_pred = ensemble_pred.to_frame()
        ensemble_pred.columns = ["ensemble"]
        rank = pred.groupby("date").rank(pct=True)
        rank["ensemble"] = ensemble_pred.groupby("date").rank(pct=True)
        signal_ranks = pd.merge(rank.reset_index(), ret[['date', 'ticker', return_column_name]], how='left', on=['date', 'ticker'])
        return signal_ranks

    def do_step_action(self, **kwargs):
        df = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        if self.load_from_s3:
            algos = self._load_models()
        else:
            algos = self._train_models(df)
        target_variable_name = self.y_col
        return_column_name = self.return_col
        feature_cols = self.X_cols
        signal_ranks = self.generate_ensemble_prediction(df, algos, target_variable_name, return_column_name, feature_cols)
        self.data = signal_ranks
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.econ_model_info}

    @classmethod
    def get_default_config(cls):
        return {"y_col": "future_ret_21B_std",
                "X_cols": ['EWG_close', 'HWI', 'IPDCONGD', 'EXUSUKx', 'COMPAPFFx', 'GS5', 'CUSR0000SAC', 'T5YFFM', 'PPO_21_126_InformationTechnology', 'macd_diff_ConsumerStaples', 'PPO_21_126_Industrials', 'VXOCLSx', 'PPO_21_126_Energy', 'T1YFFM', 'WPSID62', 'CUSR0000SA0L2', 'EWJ_volume', 'PPO_21_126_ConsumerDiscretionary', 'OILPRICEx', 'GS10', 'RPI', 'CPITRNSL', 'divyield_ConsumerStaples', 'bm_Financials', 'USTRADE', 'T10YFFM', 'divyield_Industrials', 'AAAFFM', 'RETAILx', 'bm_Utilities', 'SPY_close', 'log_mktcap', 'volatility_126', 'momentum', 'bm', 'PPO_12_26', 'SPY_beta', 'log_dollar_volume', 'fcf_yield'],
                "return_col": "future_ret_21B",
                "ensemble_weights": {'enet': 0.03333333333333333, 'et': 0.3, 'gbm': 0.2,
                                     'lasso': 0.03333333333333333, 'ols': 0.03333333333333333, 'rf': 0.4},
                "load_from_s3" : False,
                "bucket" : "dcm-data-temp",
                "key_base" : "saved_econ_models", "local_save_dir": "econ_models"}


class TrainIntermediateModelsWeekly(TrainIntermediateModels):

    PROVIDES_FIELDS = ["intermediate_signals", "intermediate_signals_weekly", "econ_model_info"]
    REQUIRES_FIELDS = ["normalized_data_full_population_with_foldId", "normalized_data_full_population_with_foldId_weekly"]

    def __init__(self, y_col, X_cols, return_col, ensemble_weights, bucket, key_base, local_save_dir, load_from_s3=True):
        self.weekly_data = None
        TrainIntermediateModels.__init__(self, y_col, X_cols, return_col, ensemble_weights, bucket, key_base, local_save_dir, load_from_s3)

    def do_step_action(self, **kwargs):
        monthly_df = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        weekly_df = kwargs[self.__class__.REQUIRES_FIELDS[1]]
        if self.load_from_s3:
            algos = self._load_models()
        else:
            algos = self._train_models(monthly_df)
        target_variable_name = self.y_col
        return_column_name = self.return_col
        feature_cols = self.X_cols
        monthly_signal_ranks = self.generate_ensemble_prediction(monthly_df, algos, target_variable_name, return_column_name, feature_cols)
        weekly_signal_ranks = self.generate_ensemble_prediction(weekly_df, algos, target_variable_name, return_column_name, feature_cols)
        self.data = monthly_signal_ranks
        self.weekly_data = weekly_signal_ranks
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.weekly_data,
                self.__class__.PROVIDES_FIELDS[2] : self.econ_model_info}


class AddFoldIdToNormalziedData(CalibrationTaskflowTask):

    PROVIDES_FIELDS = ["normalized_data_full_population_with_foldId"]
    REQUIRES_FIELDS = ["normalized_data_full_population"]

    def __init__(self, cut_dates):
        self.cut_dates = sorted([pd.Timestamp(date) for date in cut_dates] + [pd.Timestamp("1900-12-31"),pd.Timestamp("2100-12-31")])
        self.data = None
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        df = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        df["fold_id"] = pd.cut(df["date"], self.cut_dates, labels = pd.np.arange(len(self.cut_dates)-1))
        df["fold_id"] = pd.Series(df["fold_id"].cat.codes).astype(int)
        self.data = df.set_index(["date", "ticker"]).reset_index()
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"cut_dates" : ["2003-12-31", "2007-09-28", "2011-06-30", "2014-12-31", "2017-11-30"]}


class AddFoldIdToNormalizedDataPortfolio(CalibrationTaskflowTask):

    PROVIDES_FIELDS = ["r1k_neutral_normal_models_with_foldId"]
    REQUIRES_FIELDS = ["r1k_neutral_normal_models"]

    def __init__(self, cut_dates):
        self.cut_dates = sorted([pd.Timestamp(date) for date in cut_dates] + [pd.Timestamp("1900-12-31"),pd.Timestamp("2100-12-31")])
        self.r1k_neutral_normal_models_with_foldId = None
        CalibrationTaskflowTask.__init__(self)

    def _add_foldId(self, df):
        df["fold_id"] = pd.cut(df["date"], self.cut_dates, labels = pd.np.arange(len(self.cut_dates)-1), right =False)
        df["fold_id"] = pd.Series(df["fold_id"].cat.codes).astype(int)
        df = df.set_index(["date", "ticker"]).reset_index()
        return df

    def do_step_action(self, **kwargs):
        r1k_neutral_normal_models = kwargs["r1k_neutral_normal_models"].copy(deep=True).to_dict(orient='dict')[0]

        assert set(r1k_neutral_normal_models)==set(FILTER_MODES), "AddFoldIdToNormalizedData - r1k_neutral_normal_models \
        doesn't seem to contain all expected modes. It contains- {0}".format(set(r1k_neutral_normal_models))

        r1k_foldId_dict = {}
        for mode in r1k_neutral_normal_models:
            print("Adding foldId to r1k data for {0} model".format(mode))
            r1k_foldId_dict[mode] = self._add_foldId(r1k_neutral_normal_models[mode])

        self.r1k_neutral_normal_models_with_foldId = pd.DataFrame.from_dict(r1k_foldId_dict, orient='index')
        return StatusType.Success

    def _get_additional_step_results(self):
        return {"r1k_neutral_normal_models_with_foldId" : self.r1k_neutral_normal_models_with_foldId}

    @classmethod
    def get_default_config(cls):
        return {"cut_dates" : ["2010-12-31", "2012-09-28", "2014-06-30", "2016-03-31", "2017-12-29"]}


class AddFoldIdToNormalizedDataPortfolioWeekly(AddFoldIdToNormalizedDataPortfolio):

    PROVIDES_FIELDS = ["r1k_neutral_normal_models_with_foldId", "r1k_sc_with_foldId_weekly", "r1k_lc_with_foldId_weekly"]
    REQUIRES_FIELDS = ["r1k_neutral_normal_models", "r1k_neutral_normal_sc_weekly", "r1k_neutral_normal_lc_weekly"]

    def __init__(self, cut_dates):
        self.r1k_sc_with_foldId_weekly = None
        self.r1k_lc_with_foldId_weekly = None
        AddFoldIdToNormalizedDataPortfolio.__init__(self, cut_dates)

    def do_step_action(self, **kwargs):
        r1k_neutral_models_monthly = kwargs["r1k_neutral_normal_models"].copy(deep=True).to_dict(orient='dict')[0]
        r1k_neutral_sc_weekly = kwargs["r1k_neutral_normal_sc_weekly"].copy(deep=True).to_dict(orient='dict')[0]
        r1k_neutral_lc_weekly = kwargs["r1k_neutral_normal_lc_weekly"].copy(deep=True).to_dict(orient='dict')[0]

        assert set(r1k_neutral_models_monthly)==set(FILTER_MODES), "AddFoldIdToNormalizedDataWeekly - r1k_neutral_models_monthly \
        doesn't seem to contain all expected modes. It contains- {0}".format(set(r1k_neutral_models_monthly))
        assert set(r1k_neutral_sc_weekly).union(set(r1k_neutral_lc_weekly))==set(FILTER_MODES), "AddFoldIdToNormalizedDataWeekly - \
        r1k_neutral_models_weekly doesn't seem to contain all expected modes. \
        It contains- {0}".format(set(r1k_neutral_sc_weekly).union(set(r1k_neutral_lc_weekly)))

        r1k_foldId_dict_monthly = {}
        r1k_foldId_sc_dict_weekly = {}
        r1k_foldId_lc_dict_weekly = {}
        for mode in r1k_neutral_models_monthly:
            print("Adding foldId to r1k data for {0} model".format(mode))
            r1k_foldId_dict_monthly[mode] = self._add_foldId(r1k_neutral_models_monthly[mode])
            if "largecap" in mode:
                r1k_foldId_lc_dict_weekly[mode] = self._add_foldId(r1k_neutral_lc_weekly[mode])
            else:
                r1k_foldId_sc_dict_weekly[mode] = self._add_foldId(r1k_neutral_sc_weekly[mode])

        self.r1k_neutral_normal_models_with_foldId = pd.DataFrame.from_dict(r1k_foldId_dict_monthly, orient='index')
        self.r1k_sc_with_foldId_weekly = pd.DataFrame.from_dict(r1k_foldId_sc_dict_weekly, orient='index')
        self.r1k_lc_with_foldId_weekly = pd.DataFrame.from_dict(r1k_foldId_lc_dict_weekly, orient='index')
        return StatusType.Success

    def _get_additional_step_results(self):
        return {"r1k_neutral_normal_models_with_foldId" : self.r1k_neutral_normal_models_with_foldId,
                "r1k_sc_with_foldId_weekly" : self.r1k_sc_with_foldId_weekly,
                "r1k_lc_with_foldId_weekly" : self.r1k_lc_with_foldId_weekly}


class DepositInstructionsS3(CalibrationTaskflowTask):

    PROVIDES_FIELDS = ["s3_file_locations"]
    REQUIRES_FIELDS = ["all_instructions"]

    def __init__(self, bucket, key_base, prefix, deposit_on_gcs, local_save_dir, suffix=""):
        self.data = None
        self.bucket = bucket
        self.key_base = key_base
        self.s3_file_locations = None
        self.prefix = prefix or "quantamental"
        self.suffix = suffix
        self.deposit_on_gcs = deposit_on_gcs
        self.local_save_dir = local_save_dir
        CalibrationTaskflowTask.__init__(self)

    def hedge_ratio_handler(self, hr):
        if isinstance(hr, str):
            hr = eval(hr)

        #if isinstance(hr, (int, float)): # No more long
        if isinstance(hr, Number):     
            new_hr = hr
        else:
            new_hr = hr[0]
        return new_hr
    
    def upload_to_gcs(self, storage_client, bucket_name, name, csv_buffer):
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(name)
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')

    def _deposit_instructions(self, instructions_dict):
        storage_client = storage.Client()

        qt_ins_keys = ['_'.join([str(qt_key), ins_key]) for qt_key in instructions_dict.keys() for ins_key in instructions_dict[qt_key]]
        self.s3_file_locations = pd.DataFrame(index=qt_ins_keys, columns=["s3_location"])

        model_name_mapping = {'g': "growth", 'v': "value", 'lg': "largecap_growth", 'lv': "largecap_value"}

        for qt_key in instructions_dict:
            qt_ins_dict = instructions_dict[qt_key]
            last_date = qt_ins_dict["instructions_consolidated"]["date"].max()

            for key in qt_ins_dict:
                qt_ins_dict[key]["hedge_ratio"] = qt_ins_dict[key]["hedge_ratio"].apply(self.hedge_ratio_handler)

            # Processing for calibration mode
            if self.task_params.run_mode==TaskflowPipelineRunMode.Calibration:
                for key in qt_ins_dict:
                    qt_ins_dict[key] = qt_ins_dict[key][qt_ins_dict[key]["date"]==last_date].reset_index(drop=True)
                    qt_ins_dict[key]["date"] = self.task_params.run_dt
                last_date = self.task_params.run_dt

            date_str = str(last_date).split(" ")[0].replace("-", "")
            # use the quantile number as default suffix + add self.suffix after that if also provided in this step
            file_suffix = str(qt_key) +'_'+ self.suffix if self.suffix else str(qt_key)

            for key in qt_ins_dict:
                if key == "instructions_consolidated":
                    continue
                key_parts = key.split('_')
                full_key = "{0}/{1}_{2}_{3}_{4}_{5}.csv".format(self.key_base, date_str, self.prefix, model_name_mapping[key_parts[1]],
                                                                key_parts[2], file_suffix)
                if self.deposit_on_gcs:
                    csv_buffer = StringIO()
                    qt_ins_dict[key].to_csv(csv_buffer, quotechar="'", index=False)
                    self.upload_to_gcs(storage_client, self.bucket, full_key, csv_buffer)
                else:
                    qt_ins_dict[key].to_csv(os.path.join(self.local_save_dir, full_key), quotechar="'", index=False)
                self.s3_file_locations.loc['_'.join([str(qt_key), key]), "s3_location"] = "s3://{0}/{1}".format(self.bucket, full_key)

            full_key = "{0}/{1}_{2}_consolidated_{3}.csv".format(self.key_base, date_str, self.prefix, file_suffix)
            if self.deposit_on_gcs:
                csv_buffer = StringIO()
                qt_ins_dict["instructions_consolidated"].to_csv(csv_buffer, quotechar="'", index=False)
                self.upload_to_gcs(storage_client, self.bucket, full_key, csv_buffer)
            else:
                qt_ins_dict["instructions_consolidated"].to_csv(os.path.join(self.local_save_dir, full_key), quotechar="'", index=False)
            self.s3_file_locations.loc['_'.join([str(qt_key), "consolidated"]), "s3_location"] = \
                "s3://{0}/{1}".format(self.bucket, full_key)

    def do_step_action(self, **kwargs):
        all_instructions = kwargs["all_instructions"].copy()
        instructions_dict = all_instructions.to_dict(orient='index')
        self._deposit_instructions(instructions_dict)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {"s3_file_locations" : self.s3_file_locations}

    @classmethod
    def get_default_config(cls):
        return {"bucket" : "dcm-data-temp", "key_base": "live_calibrations/quantamental_ml/gan", "prefix": "gan", "suffix": ""}


class DepositInstructionsS3Weekly(DepositInstructionsS3):

    PROVIDES_FIELDS = ["s3_file_locations"]
    REQUIRES_FIELDS = ["all_instructions", "all_instructions_weekly"]

    def __init__(self, bucket, key_base, prefix, suffix=""):
        DepositInstructionsS3.__init__(self, bucket, key_base, prefix, suffix)

    def do_step_action(self, **kwargs):
        all_instructions = kwargs["all_instructions"].copy().to_dict(orient='index')
        all_instructions_weekly = kwargs["all_instructions_weekly"].copy().to_dict(orient='index')
        self._deposit_instructions(all_instructions_weekly)
        return StatusType.Success


    ## Jack: I put everything here for clarity. Items should be moved to appropriate places (e.g. data_puller, feature_builder etc)

class GenerateDataGAN(CalibrationTaskflowTask):

    PROVIDES_FIELDS = ["company_data", "gan_data_info"]
    REQUIRES_FIELDS = ["normalized_data_full_population", "econ_data_final", "future_returns_bme", "active_matrix"]

    '''
    GAN_results_generation_multi_batch_long_history_1_ts_201910_update.ipynb
    Last Section. It is unstructured in the notebook
    '''

    def __init__(self, data_dir):
        self.data = None
        self.gan_data_info = None
        self.data_dir = data_dir
        CalibrationTaskflowTask.__init__(self)

    def melt_merge(self, future_return_data, df):
        future_returns = pd.melt(future_return_data.reset_index().rename(columns={"index": "date"}), id_vars=["date"],
                                 value_name="future_return_bme")
        future_returns["ticker"] = future_returns["ticker"].astype(int)
        merged = pd.merge(future_returns, df, how="left", on=["date", "ticker"])
        merged = merged.sort_values(["date", "ticker"])
        return merged

    def do_step_action(self, **kwargs):
        df = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        econ_data = kwargs[self.__class__.REQUIRES_FIELDS[1]].copy(deep=True)
        future_return_data = kwargs[self.__class__.REQUIRES_FIELDS[2]].copy(deep=True)
        active_matrix = kwargs[self.__class__.REQUIRES_FIELDS[3]].copy(deep=True)

        company_data = self.melt_merge(future_return_data, df) # Merging in this direction ensures that it conforms to the rest of the GAN data

        base_dir = os.environ["GAN_DIR"]
        if self.task_params.run_mode==TaskflowPipelineRunMode.Test:
            # Override for unit testing
            full_path = os.path.abspath(os.path.join(os.path.join(TEST_DATA_DIR, UNIT_TEST_GAN_BASE), self.data_dir))
        else:
            full_path = get_local_dated_dir(base_dir, self.task_params.end_dt, self.data_dir)
            create_directory_if_does_not_exists(full_path)
            company_data.to_hdf(os.path.join(full_path, "company_data.h5"), key="df", format="table")
            econ_data.to_hdf(os.path.join(full_path, "econ_data_final.h5"), key="df", format="table")
            future_return_data.to_hdf(os.path.join(full_path, "future_returns.h5"), key="df", format="fixed")
            active_matrix.to_hdf(os.path.join(full_path, "active_matrix.h5"), key="df", format="fixed")
        print("****************************************************")
        print("****************************************************")
        print("GAN Data full path: {0}".format(full_path))
        self.data = company_data
        self.gan_data_info = pd.DataFrame([self.data_dir], columns=["relative_path"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.gan_data_info}

    @classmethod
    def get_default_config(cls):
        return {"data_dir": "calibration_data/gan_data"}


class GenerateDataGANWeekly(GenerateDataGAN):

    PROVIDES_FIELDS = ["company_data", "company_data_weekly", "gan_data_info"]
    REQUIRES_FIELDS = ["normalized_data_full_population", "normalized_data_full_population_weekly", "econ_data_final",
                       "econ_data_final_weekly", "future_returns_bme", "future_returns_weekly", "active_matrix", "active_matrix_weekly"]

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

        base_dir = os.environ["GAN_DIR"]
        if self.task_params.run_mode==TaskflowPipelineRunMode.Test:
            # Override for unit testing
            full_path = os.path.abspath(os.path.join(os.path.join(TEST_DATA_DIR, UNIT_TEST_GAN_BASE), self.data_dir))
        else:
            full_path = get_local_dated_dir(base_dir, self.task_params.end_dt, self.data_dir)
            create_directory_if_does_not_exists(full_path)
            company_data_monthly.to_hdf(os.path.join(full_path, "company_data.h5"), key="df", format="table")
            company_data_weekly.to_hdf(os.path.join(full_path, "company_data_weekly.h5"), key="df", format="table")
            econ_data.to_hdf(os.path.join(full_path, "econ_data_final.h5"), key="df", format="table")
            econ_data_weekly.to_hdf(os.path.join(full_path, "econ_data_final_weekly.h5"), key="df", format="table")
            future_return_data.to_hdf(os.path.join(full_path, "future_returns.h5"), key="df", format="fixed")
            future_return_weekly.to_hdf(os.path.join(full_path, "future_returns_weekly.h5"), key="df", format="fixed")
            active_matrix.to_hdf(os.path.join(full_path, "active_matrix.h5"), key="df", format="fixed")
            active_matrix_weekly.to_hdf(os.path.join(full_path, "active_matrix_weekly.h5"), key="df", format="fixed")

        print("****************************************************")
        print("****************************************************")
        print("GAN Data full path: {0}".format(full_path))
        self.data = company_data_monthly
        self.weekly_data = company_data_weekly
        self.gan_data_info = pd.DataFrame([self.data_dir], columns=["relative_path"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.weekly_data,
                self.__class__.PROVIDES_FIELDS[2] : self.gan_data_info}

    @classmethod
    def get_default_config(cls):
        return {"data_dir": "calibration_data/gan_data"}


class TrainGANModel(CalibrationTaskflowTask):

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
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        util_script_location = os.path.join(INTUITION_DIR,
                                            "processes/data_processing/calibrations/quantamental_ml/gan_py3/gan_util_functions.py")
        self.util_script_location = os.path.abspath(util_script_location)
        path_df = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        retval = None
        relative_path = path_df.loc[0, "relative_path"]

        base_dir = os.environ["GAN_DIR"]
        if self.task_params.run_mode==TaskflowPipelineRunMode.Test:
            # Override for unit testing
            full_path = os.path.abspath(os.path.join(os.path.join(TEST_DATA_DIR, UNIT_TEST_GAN_BASE), relative_path))
        else:
            full_path = get_local_dated_dir(base_dir, self.task_params.end_dt, relative_path)
        print("****************************************************")
        print("full_path: {0}".format(full_path))
        if self.retrain:
            if not self.python_bin:
                raise ValueError("TrainGANModel.do_step_action - the step is set to retrain the model, but there isn't "
                                 "any specification of python3 binary using environment variable 'PY3_EXEC'. Please set"
                                 " the environment variable")
            command = [self.python_bin, self.util_script_location, full_path, self.insample_cut_date, str(self.epochs),
                       str(self.epochs_discriminator), str(self.gan_iterations), '0']
            command = " ".join(command)
            p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
            for line in iter(p.stdout.readline, ''):
                print(line.rstrip())
            retval = p.wait()
        else:
            file_location = os.path.join(full_path, "saved_weights.h5")
            if os.path.exists(file_location):
                print("********** Saved file exists at {0}".format(file_location))
                retval = 0
            else:
                print("Model file does not exist at {0} !!!!!!!!".format(file_location))

        if retval==0:
            results = pd.DataFrame(columns=["relative_path", "success", "insample_cut_date"])
            results.loc[0, "success"] = 1
            results.loc[0, "relative_path"] = relative_path
            results.loc[0, "insample_cut_date"] = self.insample_cut_date
            results["success"] = results["success"].astype(int) # Not sure why this is necessary
            self.data = results
            return StatusType.Success
        else:
            return StatusType.Fail

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"insample_cut_date": "2017-12-29", "epochs": 20, "epochs_discriminator": 10, "gan_iterations": 2,
                "retrain": True}

class ExtractGANFactors(CalibrationTaskflowTask):

    PROVIDES_FIELDS = ["gan_factors"]
    REQUIRES_FIELDS = ["gan_model_save_info"]

    '''
    GAN_asset_pricing_multi_batch_long_history.ipynb
    DataGeneratorMultiBatchFast
    AssetPricingGAN
    '''

    def __init__(self):
        self.data = None
        self.python_bin = os.environ.get("PY3_EXEC", None)
        self.util_script_location = "processes/data_processing/calibrations/quantamental_ml/gan_py3/gan_util_functions.py"
        self.insample_cut_date = None
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        if not self.python_bin:
            raise ValueError("ExtractGANFactors.do_step_action - cannot extract the GAN factors without access to a "
                             "python 3 envrionment to run the GAN. Please set up the environment variable 'PY3_EXEC'")
        util_script_location = os.path.join(INTUITION_DIR,
                                            "processes/data_processing/calibrations/quantamental_ml/gan_py3/gan_util_functions.py")
        self.util_script_location = os.path.abspath(util_script_location)
        retval = None
        model_info = kwargs[self.__class__.REQUIRES_FIELDS[0]]

        relative_path = model_info.loc[0, "relative_path"]

        base_dir = os.environ["GAN_DIR"]
        if self.task_params.run_mode==TaskflowPipelineRunMode.Test:
            # Override for unit testing
            full_path = os.path.abspath(os.path.join(os.path.join(TEST_DATA_DIR, UNIT_TEST_GAN_BASE), relative_path))
        else:
            full_path = get_local_dated_dir(base_dir, self.task_params.end_dt, relative_path)
        self.insample_cut_date = model_info.loc[0, "insample_cut_date"]

        file_location = os.path.join(full_path, "saved_weights.h5")
        if os.path.exists(file_location):
            command = [self.python_bin, self.util_script_location, full_path, self.insample_cut_date, '150',
                       '50', '2', '1']
            command = " ".join(command)
            p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
            for line in iter(p.stdout.readline, ''):
                if not line:
                    break
                print(line.rstrip())
            retval = p.wait()
        else:
            file_location = os.path.join(full_path, "saved_weights.h5")
            print("Model file does not exist at {0} !!!!!!!!".format(file_location))

        if retval==0:
            factor_file = os.path.join(full_path, "all_factors.h5")
            all_factors = pd.read_hdf(os.path.join(factor_file))
            self.data = all_factors
            return StatusType.Success
        else:
            return StatusType.Fail

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {}

class ConsolidateGANResults(CalibrationTaskflowTask):

    PROVIDES_FIELDS = ["company_data_rf"]
    REQUIRES_FIELDS = ["company_data", "gan_factors"]

    '''
    GAN_results_generation_multi_batch_long_history_1_ts_201910_update.ipynb
    Last Section. It is unstructured in the notebook
    '''

    def __init__(self):
        self.data = None
        CalibrationTaskflowTask.__init__(self)

    def create_gan_target_columns(self, df):
        df["future_return_RF"] = df["future_return_bme"]*df["f_t"] # TODO: Should be f_t+1 (fix at next full recal)?
        df["future_return_RF_100"] = df["future_return_RF"]*100.0

        col = "future_return_RF_100"
        new_col = "future_return_RF_100_std"
        lb = df.groupby('date')[col].transform(pd.Series.quantile, 0.005)
        lb.fillna(-999999999999999, inplace=True)
        ub = df.groupby('date')[col].transform(pd.Series.quantile, 0.995)
        ub.fillna(999999999999999, inplace=True)
        df[new_col] = df[col].clip(lb, ub)
        #df[new_col] = df[col]
        #df.loc[df[col]<=lb, new_col] = lb
        #df.loc[df[col]>=ub, new_col] = ub
        df[new_col] = df.groupby('date')[col].transform(lambda x: (x - x.mean()) / x.std())
        df[new_col] = df[new_col].fillna(0.0)
        return df

    def do_step_action(self, **kwargs):
        df = kwargs[self.__class__.REQUIRES_FIELDS[0]].reset_index(drop=True)
        gan_factors = kwargs[self.__class__.REQUIRES_FIELDS[1]].drop("f_t+1", axis=1).reset_index()

        # Need to join by gan_factors since we lose 1 time step from the lstm lag
        true_start = min(df["date"].min(), gan_factors["date"].min())
        df = df[df["date"]>=true_start]
        gan_factors = gan_factors[gan_factors["date"]>=true_start]

        df = pd.merge(df, gan_factors, how="left", on=["date"]).set_index(["date", "ticker"]).sort_index().reset_index()
        df = self.create_gan_target_columns(df)
        df = df.fillna(0.0)
        self.data = df.reset_index(drop=True)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

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
        gan_factors = kwargs["gan_factors"].drop("f_t+1", axis=1).reset_index()

        # Need to join by gan_factors since we lose 1 time step from the lstm lag
        true_start = min(monthly_df["date"].min(), gan_factors["date"].min())

        monthly_df = monthly_df[monthly_df["date"]>=true_start]
        weekly_df = weekly_df[weekly_df["date"]>=true_start]
        gan_factors = gan_factors[gan_factors["date"]>=true_start]

        monthly_df = pd.merge(monthly_df, gan_factors, how="left", on=["date"]).set_index(["date", "ticker"]).sort_index().reset_index()
        monthly_df = self.create_gan_target_columns(monthly_df)
        self.data = monthly_df.fillna(0.0).reset_index(drop=True)

        weekly_df = pd.merge_asof(weekly_df, gan_factors, on="date", direction="backward")
        weekly_df.set_index(["date", "ticker"]).sort_index().reset_index()
        weekly_df = self.create_gan_target_columns(weekly_df)
        cols_to_ffill = ['f_t', 'hidden_state_0', 'hidden_state_1', 'hidden_state_2', 'hidden_state_3']
        weekly_df.loc[:, cols_to_ffill] = weekly_df.loc[:, cols_to_ffill].ffill()
        self.weekly_data = weekly_df.fillna(0.0).reset_index(drop=True)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.weekly_data}


class AddFoldIdToGANResultData(CalibrationTaskflowTask):

    PROVIDES_FIELDS = ["normalized_data_full_population_with_foldId"]
    REQUIRES_FIELDS = ["company_data_rf"]

    def __init__(self, cut_dates):
        self.cut_dates = sorted([pd.Timestamp(date) for date in cut_dates] + [pd.Timestamp("1900-12-31"),pd.Timestamp("2100-12-31")])
        self.data = None
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        df = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        df["fold_id"] = pd.cut(df["date"], self.cut_dates, labels = pd.np.arange(len(self.cut_dates)-1))
        df["fold_id"] = pd.Series(df["fold_id"].cat.codes).astype(int)
        self.data = df.set_index(["date", "ticker"]).reset_index()
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"cut_dates" : ["2003-12-31", "2007-09-28", "2011-06-30", "2014-12-31", "2017-11-30"]}


class AddFoldIdToGANResultDataWeekly(AddFoldIdToGANResultData):

    PROVIDES_FIELDS = ["normalized_data_full_population_with_foldId", "normalized_data_full_population_with_foldId_weekly"]
    REQUIRES_FIELDS = ["company_data_rf", "company_data_rf_weekly"]

    def __init__(self, cut_dates):
        self.weekly_data = None
        AddFoldIdToGANResultData.__init__(self, cut_dates)

    def do_step_action(self, **kwargs):
        monthly_df = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        weekly_df = kwargs[self.__class__.REQUIRES_FIELDS[1]].copy(deep=True)

        monthly_df["fold_id"] = pd.cut(monthly_df["date"], self.cut_dates, labels = pd.np.arange(len(self.cut_dates)-1))
        monthly_df["fold_id"] = pd.Series(monthly_df["fold_id"].cat.codes).astype(int)
        self.data = monthly_df.set_index(["date", "ticker"]).reset_index()

        weekly_df["fold_id"] = pd.cut(weekly_df["date"], self.cut_dates, labels = pd.np.arange(len(self.cut_dates)-1))
        weekly_df["fold_id"] = pd.Series(weekly_df["fold_id"].cat.codes).astype(int)
        self.weekly_data = weekly_df.set_index(["date", "ticker"]).reset_index()
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.weekly_data}
