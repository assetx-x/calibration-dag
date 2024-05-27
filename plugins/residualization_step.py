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



class FactorNeutralizationForStacking(DataReaderClass):
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

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

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

    @staticmethod
    def _dictionary_format(**kwargs):
        return {k:v for k,v in kwargs.items()}

    def do_step_action(self, **kwargs):
        # Strangely dict -> dataframe -> dict conversion seems to introduce a nested dictionary with a 0 key.

        r1k_models_monthly = self._dictionary_format(growth=kwargs["r1k_models_growth"],
                                        value=kwargs["r1k_models_value"],
                                        largecap_growth=kwargs["r1k_models_largecap_growth"],
                                        largecap_value=kwargs["r1k_models_largecap_value"]
                                        )

        r1k_models_sc_weekly = self._dictionary_format(growth=kwargs["r1k_models_sc_weekly_growth"],
                                value=kwargs["r1k_models_sc_weekly_value"],
                                )

        r1k_models_lc_weekly = self._dictionary_format(largecap_growth=kwargs["r1k_models_lc_weekly_largecap_growth"],
                                largecap_value=kwargs["r1k_models_lc_weekly_largecap_value"],
                                )


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


        self.r1k_resid_models = pd.DataFrame(list(r1k_resid_dict_monthly.items()), columns=['Key', 0]).set_index('Key')
        self.r1k_resid_sc_weekly = pd.DataFrame(list(r1k_resid_sc_dict_weekly.items()), columns=['Key', 0]).set_index('Key')
        self.r1k_resid_lc_weekly = pd.DataFrame(list(r1k_resid_lc_dict_weekly.items()), columns=['Key', 0]).set_index('Key')




        return self._get_additional_step_results()



    def _get_additional_step_results(self):
        return {"r1k_resid_models_growth": self.r1k_resid_models.loc['growth'][0],
                "r1k_resid_models_value": self.r1k_resid_models.loc['value'][0],
                "r1k_resid_models_largecap_value": self.r1k_resid_models.loc['largecap_growth'][0],
                "r1k_resid_models_largecap_growth": self.r1k_resid_models.loc['largecap_value'][0],
                "r1k_resid_sc_weekly_growth": self.r1k_resid_sc_weekly.loc['growth'][0],
                "r1k_resid_sc_weekly_value": self.r1k_resid_sc_weekly.loc['value'][0],
                "r1k_resid_lc_weekly_largecap_growth": self.r1k_resid_lc_weekly.loc['largecap_growth'][0],
                "r1k_resid_lc_weekly_largecap_value": self.r1k_resid_lc_weekly.loc['largecap_value'][0],
                }



params = {'factors':["SPY_beta", "log_mktcap", "ret_5B", "ret_21B", "volatility_63", "volatility_126", "momentum"],
          'exclusion_list':["fq", "divyield_Industrials", "PPO_21_126_ConsumerDiscretionary", "DNDGRG3M086SBEA", "DEXUSUK",
                          "GS10", "IPDCONGD", "T5YFFM", "USTRADE", "CUSR0000SA0L2", "RETAILx", "bm_Financials", "DCOILWTICO",
                          "T10YFFM", "CPITRNSL", "CP3Mx", "CUSR0000SAC", "EWJ_volume", "SPY_close", "VIXCLS",
                          "PPO_21_126_InformationTechnology",  "WPSID62", "GS5", "CPFF", "CUSR0000SA0L5",
                          "T1YFFM", "PPO_21_126_Energy", "bm_Utilities", "PPO_21_126_Financials", "HWI", "RPI",
                          "PPO_21_126_Industrials",  "divyield_ConsumerStaples", "EWG_close", "macd_diff_ConsumerStaples",
                          "AAAFFM", "ols", "lasso", "enet", "et", "rf", "gbm", "ensemble"]}


FactorNeutralizationForStackingWeekly_params = {'params':params,
                                                "start_date": RUN_DATE,
                                                'class': FactorNeutralizationForStackingWeekly,
                                                'provided_data': {'r1k_resid_models':
                                                                      construct_destination_path('residualization'),
                                                                  'r1k_resid_sc_weekly':
                                                                      construct_destination_path('residualization'),
                                                                  'r1k_resid_lc_weekly':
                                                                      construct_destination_path('residualization'),
                                                                  },
                                                'required_data':{'r1k_models':
                                                                     construct_required_path('population_split','r1k_models'),
                                                                 'r1k_models_sc_weekly':
                                                                     construct_required_path('population_split','r1k_models_sc_weekly'),
                                                                 'r1k_models_lc_weekly':
                                                                     construct_required_path('population_split','r1k_models_lc_weekly'),
                                                                 }
                                                }


