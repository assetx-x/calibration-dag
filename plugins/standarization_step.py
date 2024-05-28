from core_classes import GCPReader, download_yahoo_data, DataReaderClass
import pandas as pd
import numpy as np
from datetime import datetime
from core_classes import StatusType
from market_timeline import pick_trading_week_dates, pick_trading_month_dates
from commonlib import factor_standarization
import talib

current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')
from core_classes import construct_required_path, construct_destination_path


class FactorStandardization(DataReaderClass):
    '''

    Cross-sectional standardization of features

    '''

    PROVIDES_FIELDS = ["normalized_data"]
    REQUIRES_FIELDS = ["russell_data"]

    def __init__(
        self,
        all_features,
        exclude_from_standardization,
        target_columns,
        suffixes_to_exclude,
    ):
        self.data = None
        self.suffixes_to_remove = suffixes_to_exclude or []
        self.all_features = all_features or [
            'divyield',
            'WILLR_14_MA_3',
            'macd',
            'bm',
            'retvol',
            'netmargin',
        ]
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
            all_features = list(
                df.columns.difference(
                    self.target_columns
                    + exclude_from_standardization
                    + ["date", "ticker"]
                )
            )
        else:
            all_features = self.all_features
        return all_features

    def do_step_action(self, **kwargs):
        df = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        all_features = self._select_features(df)
        self.data = factor_standarization(
            df, all_features, self.target_columns, self.exclude_from_standardization
        )
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}


class FactorStandardizationFullPopulationWeekly(FactorStandardization):

    PROVIDES_FIELDS = [
        "normalized_data_full_population",
        "normalized_data_full_population_weekly",
    ]
    REQUIRES_FIELDS = ["merged_data_econ_industry", "merged_data_econ_industry_weekly"]

    def __init__(
        self,
        all_features,
        exclude_from_standardization,
        target_columns,
        suffixes_to_exclude,
    ):
        self.weekly_data = None
        FactorStandardization.__init__(
            self,
            all_features,
            exclude_from_standardization,
            target_columns,
            suffixes_to_exclude,
        )

    def do_step_action(self, **kwargs):
        monthly_df = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        weekly_df = kwargs[self.__class__.REQUIRES_FIELDS[1]]
        all_features = self._select_features(
            weekly_df
        )  # should be same with both monthly and weekly df
        self.data = factor_standarization(
            monthly_df,
            all_features,
            self.target_columns,
            self.exclude_from_standardization,
        )
        self.weekly_data = factor_standarization(
            weekly_df,
            all_features,
            self.target_columns,
            self.exclude_from_standardization,
        )

        self.data.drop_duplicates(subset=['date', 'ticker'], inplace=True)
        self.weekly_data.drop_duplicates(subset=['date', 'ticker'], inplace=True)

        return self.data, self.weekly_data

    @classmethod
    def get_full_class_requires(
        cls, include_parent_classes=True, include_base_etl_requires=True
    ):
        reqs = (
            cls.__bases__[0]
            .__bases__[0]
            .get_full_class_requires(include_parent_classes, include_base_etl_requires)
        )
        reqs = reqs + list(cls.REQUIRES_FIELDS)
        return reqs

    @classmethod
    def get_full_class_provides(
        cls, include_parent_classes=True, include_base_etl_provides=True
    ):
        provs = (
            cls.__bases__[0]
            .__bases__[0]
            .get_full_class_provides(include_parent_classes, include_base_etl_provides)
        )
        provs = set(list(provs) + list(cls.PROVIDES_FIELDS))
        return provs


class FactorStandardizationFullPopulationWeekly(FactorStandardization):

    PROVIDES_FIELDS = [
        "normalized_data_full_population",
        "normalized_data_full_population_weekly",
    ]
    REQUIRES_FIELDS = ["merged_data_econ_industry", "merged_data_econ_industry_weekly"]

    def __init__(
        self,
        all_features,
        exclude_from_standardization,
        target_columns,
        suffixes_to_exclude,
    ):
        self.weekly_data = None
        FactorStandardization.__init__(
            self,
            all_features,
            exclude_from_standardization,
            target_columns,
            suffixes_to_exclude,
        )

    def do_step_action(self, **kwargs):
        monthly_df = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        weekly_df = kwargs[self.__class__.REQUIRES_FIELDS[1]]
        all_features = self._select_features(
            weekly_df
        )  # should be same with both monthly and weekly df
        self.data = factor_standarization(
            monthly_df,
            all_features,
            self.target_columns,
            self.exclude_from_standardization,
        )
        self.weekly_data = factor_standarization(
            weekly_df,
            all_features,
            self.target_columns,
            self.exclude_from_standardization,
        )
        return {
            'normalized_data_full_population': self.data,
            'normalized_data_full_population_weekly': self.weekly_data,
        }

    @classmethod
    def get_full_class_requires(
        cls, include_parent_classes=True, include_base_etl_requires=True
    ):
        reqs = (
            cls.__bases__[0]
            .__bases__[0]
            .get_full_class_requires(include_parent_classes, include_base_etl_requires)
        )
        reqs = reqs + list(cls.REQUIRES_FIELDS)
        return reqs

    @classmethod
    def get_full_class_provides(
        cls, include_parent_classes=True, include_base_etl_provides=True
    ):
        provs = (
            cls.__bases__[0]
            .__bases__[0]
            .get_full_class_provides(include_parent_classes, include_base_etl_provides)
        )
        provs = set(list(provs) + list(cls.PROVIDES_FIELDS))
        return provs


FSFPW_params = {
    'all_features': True,
    'exclude_from_standardization': [
        "fq",
        "divyield_Industrials",
        "PPO_21_126_ConsumerDiscretionary",
        "DNDGRG3M086SBEA",
        "DEXUSUK",
        "GS10",
        "IPDCONGD",
        "T5YFFM",
        "USTRADE",
        "CUSR0000SA0L2",
        "RETAILx",
        "bm_Financials",
        "DCOILWTICO",
        "T10YFFM",
        "CPITRNSL",
        "CP3Mx",
        "CUSR0000SAC",
        "EWJ_volume",
        "SPY_close",
        "VIXCLS",
        "PPO_21_126_InformationTechnology",
        "WPSID62",
        "GS5",
        "CPFF",
        "CUSR0000SA0L5",
        "T1YFFM",
        "PPO_21_126_Energy",
        "bm_Utilities",
        "PPO_21_126_Financials",
        "HWI",
        "RPI",
        "PPO_21_126_Industrials",
        "divyield_ConsumerStaples",
        "EWG_close",
        "macd_diff_ConsumerStaples",
        "AAAFFM",
        "fold_id",
        "Sector",
        "IndustryGroup",
    ],
    'target_columns': [
        "future_asset_growth_qoq",
        "future_ret_10B",
        "future_ret_1B",
        "future_ret_21B",
        "future_ret_42B",
        "future_ret_5B",
        "future_revenue_growth_qoq",
    ],
    'suffixes_to_exclude': ["_std"],
}


FactorStandardizationFullPopulationWeekly_params = {
    'params': FSFPW_params,
    'class': FactorStandardizationFullPopulationWeekly,
    'start_date': RUN_DATE,
    'provided_data': {
        'normalized_data_full_population': construct_destination_path('standarization'),
        'normalized_data_full_population_weekly': construct_destination_path(
            'standarization'
        ),
    },
    'required_data': {
        'merged_data_econ_industry': construct_required_path(
            'merge_econ', 'merged_data_econ_industry'
        ),
        'merged_data_econ_industry_weekly': construct_required_path(
            'merge_econ', 'merged_data_econ_industry_weekly'
        ),
    },
}
