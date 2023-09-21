from core_classes import GCPReader,download_yahoo_data,DataReaderClass
import pandas as pd
import numpy as np
from datetime import datetime
import yfinance as yf
from core_classes import StatusType
current_date = datetime.datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')

from core_classes import construct_required_path_earnings as construct_required_path
from core_classes import construct_destination_path_earnings as construct_destination_path


def factor_standarization(df, X_cols, y_col, exclude_from_standardization):
    df = df.copy(deep=True).set_index(["date", "ticker"])
    df = df.drop(['fq', 'Sector', 'IndustryGroup'],axis=1)
    not_X_col = [k for k in X_cols if k not in df.columns]
    print("Missing X columns: {0}".format(not_X_col))
    not_y_col = [k for k in y_col if k not in df.columns]
    print("Missing Y columns: {0}".format(not_y_col))
    X_cols = [k for k in X_cols if k not in not_X_col]
    y_col = [k for k in y_col if k not in not_y_col]
    exclude_from_standardization_df = df[list(set(df.columns).intersection(set(exclude_from_standardization)))]
    df = df[y_col + X_cols]
    #df = df[X_cols]

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
    new_df = pd.concat([new_df, df, exclude_from_standardization_df], axis=1)
    new_df = new_df.reset_index()
    #new_df["ticker"] = new_df["ticker"].astype(int)
    #new_df["Sector"] = new_df["Sector"].astype(str)
    #new_df["IndustryGroup"] = new_df["IndustryGroup"].astype(str)
    return new_df


class FactorStandardization(DataReaderClass):
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

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

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
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}


class FactorStandardizationFullPopulationWeekly(FactorStandardization):
    PROVIDES_FIELDS = ["normalized_data_full_population_quarterly"]
    REQUIRES_FIELDS = ["merged_earnings_data"]

    def __init__(self, all_features, exclude_from_standardization, target_columns, suffixes_to_exclude):
        self.weekly_data = None
        FactorStandardization.__init__(self, all_features, exclude_from_standardization, target_columns,
                                       suffixes_to_exclude)

    def do_step_action(self, **kwargs):
        weekly_df = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        all_features = self._select_features(weekly_df)  # should be same with both monthly and weekly df
        self.weekly_data = factor_standarization(weekly_df, all_features, self.target_columns,
                                                 self.exclude_from_standardization)
        return {'normalized_data_full_population_quarterly': self.weekly_data}

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



FSFPW_params = {'all_features':True,
                'exclude_from_standardization':["fq", "divyield_Industrials", "PPO_21_126_ConsumerDiscretionary", "DNDGRG3M086SBEA", "EXUSUKx", "GS10", "IPDCONGD", "T5YFFM",
                                        "USTRADE", "CUSR0000SA0L2", "RETAILx", "bm_Financials", "OILPRICEx", "T10YFFM", "CPITRNSL", "CP3Mx", "CUSR0000SAC", "EWJ_volume",
                                        "SPY_close", "VXOCLSx", "PPO_21_126_InformationTechnology", "WPSID62", "GS5", "COMPAPFFx", "CUSR0000SA0L5", "T1YFFM", "PPO_21_126_Energy",
                                        "bm_Utilities", "PPO_21_126_Financials", "HWI", "RPI", "PPO_21_126_Industrials", "divyield_ConsumerStaples", "EWG_close", "macd_diff_ConsumerStaples",
                                        "AAAFFM", "fold_id"],
                'target_columns':["future_earnings_1Q"],
                'suffixes_to_exclude':["_std"]
    }

FactorStandardizationFullPopulationWeekly_params = {'params':FSFPW_params,
 'class':FactorStandardizationFullPopulationWeekly,
                                       'start_date':RUN_DATE,
                                'provided_data': {'normalized_data_full_population_quarterly': construct_destination_path('standarization')},
                                'required_data': {
                                                  'merged_earnings_data': construct_required_path('merged_earnings_data',
                                                                                                                 'merged_earnings_data')
                                }}