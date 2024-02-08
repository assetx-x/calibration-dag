import pandas as pd
from core_classes import GCPReader,download_yahoo_data,DataReaderClass
from market_timeline import marketTimeline
import pandas as pd
import numpy as np
from datetime import datetime
import os
from core_classes import StatusType
from google.cloud import storage
import statsmodels.api as sm

current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')
from core_classes import construct_required_path,construct_destination_path

FILTER_MODES = ["growth", "value", "largecap_growth", "largecap_value"]


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




class FactorStandardizationNeutralizedForStacking(DataReaderClass):
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

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

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
        return {"r1k_neutral_normal_models" : self.r1k_neutral_normal_models,
                "r1k_neutral_normal_sc_weekly" : self.r1k_neutral_normal_sc_weekly,
                "r1k_neutral_normal_lc_weekly" : self.r1k_neutral_normal_lc_weekly}

    def _get_additional_step_results(self):
        return {"r1k_neutral_normal_models" : self.r1k_neutral_normal_models,
                "r1k_neutral_normal_sc_weekly" : self.r1k_neutral_normal_sc_weekly,
                "r1k_neutral_normal_lc_weekly" : self.r1k_neutral_normal_lc_weekly}




params = {'all_features': True,
        'exclude_from_standardization': ["fq", "divyield_Industrials", "PPO_21_126_ConsumerDiscretionary", "DNDGRG3M086SBEA", "DEXUSUK", "GS10", "IPDCONGD", "T5YFFM",
                                        "USTRADE", "CUSR0000SA0L2", "RETAILx", "bm_Financials", "DCOILWTICO", "T10YFFM", "CPITRNSL", "CP3Mx", "CUSR0000SAC", "EWJ_volume",
                                        "SPY_close", "VIXCLS", "PPO_21_126_InformationTechnology", "WPSID62", "GS5", "CPFF", "CUSR0000SA0L5", "T1YFFM", "PPO_21_126_Energy",
                                        "bm_Utilities", "PPO_21_126_Financials", "HWI", "RPI", "PPO_21_126_Industrials", "divyield_ConsumerStaples", "EWG_close", "macd_diff_ConsumerStaples",
                                        "AAAFFM", "fold_id", "Sector", "IndustryGroup"],
        'target_columns': ["future_asset_growth_qoq", "future_ret_10B", "future_ret_1B", "future_ret_21B",
                           "future_ret_42B", "future_ret_5B", "future_revenue_growth_qoq"],
        'suffixes_to_exclude': ["_std"]
    }


FactorStandardizationNeutralizedForStackingWeekly_params = {'params': params,
                                           'class':FactorStandardizationNeutralizedForStackingWeekly,
                                           'start_date': RUN_DATE,
                                           'provided_data': {
                                             'r1k_neutral_normal_models': construct_destination_path(
                                                 'residualized_standardized'),
                                             'r1k_neutral_normal_sc_weekly': construct_destination_path(
                                                 'residualized_standardized'),
                                               'r1k_neutral_normal_lc_weekly': construct_destination_path(
                                                 'residualized_standardized')
                                             },

                                           'required_data':{'r1k_resid_models':
                                                                construct_required_path('residualization',
                                                                                        'r1k_resid_models'),
                                                            'r1k_resid_sc_weekly':
                                                                construct_required_path('residualization',
                                                                                        'r1k_resid_sc_weekly'),

                                                            'r1k_resid_lc_weekly': construct_required_path('residualization',
                                                                                                          'r1k_resid_lc_weekly'),
                                                            } }



