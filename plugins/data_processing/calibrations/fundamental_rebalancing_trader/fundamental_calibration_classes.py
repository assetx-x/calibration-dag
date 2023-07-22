import gc
import json

import pandas as pd

from commonlib.util_functions import create_directory_if_does_not_exists
from etl_workflow_steps import StatusType, BaseETLStage, compose_main_flow_and_engine_for_task
from calibrations.common.config_functions import get_config, set_config
from calibrations.feature_builders.digitizer import Digitization

from calibrations.common.calibrations_utils import store_pandas_object
from calibrations.views_in_market_trading.market_views_calibration_classes import BaseAssetSelector
from calibrations.common.snapshot import snapshot_taskflow_engine_results, read_taskflow_engine_snapshot
from calibrations.common.calibration_logging import calibration_logger

from calibrations.core.calibration_taskflow_task import CalibrationTaskflowTask, CalibrationFileTaskflowTask

RANDOM_STATE = 12345

class FundamentalsMerge(CalibrationTaskflowTask):
    '''

    Merges all data elements from fundamentals pipeline into a single dataframe

    '''
    PROVIDES_FIELDS = ["merged_data"]
    REQUIRES_FIELDS = ["daily_price_data", "names_performance_over_intervals",
                       "overnight_return_data", "quarterly_return_data", "stochastic_indicator_data",
                       "correlation_data", "technical_indicator_data", "sector_industry_mapping", "etf_information",
                       "ravenpack_indicator_data", "ravenpack_overnight_volume_data", "fundamental_data",
                       "derived_fundamental_data", "dollar_volume_data", "spy_indicator_peer_rank",
                       "indicator_comparisons", "past_return_data"]

    def __init__(self, slippage_pct=None, addtive_tc=None, ranking_benchmarks=None, sector_benchmarks=None,
                 other_benchmarks=None):
        self.data = None
        self.slippage_pct = slippage_pct if slippage_pct else 0.001
        self.addtive_tc = addtive_tc if addtive_tc else 0.01
        self.ranking_benchmarks = ranking_benchmarks
        self.sector_benchmarks = sector_benchmarks
        self.other_benchmarks = other_benchmarks
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        gc.collect()
        try:
            lcl = globals()
            for k in self.REQUIRES_FIELDS:
                lcl[k] = kwargs[k]

            new_sector_industry_mapping = sector_industry_mapping.rename(columns = {"etf" : "benchmark_etf"})

            merged_data = names_performance_over_intervals

            new_overnight_return_data = overnight_return_data.copy(deep=True)
            new_overnight_return_data["date"] = pd.DatetimeIndex(new_overnight_return_data["date"]).normalize()
            merged_data = pd.merge(merged_data,new_overnight_return_data, how="left",
                                   left_on=["entry_date","ticker"], right_on=["date","ticker"]).drop(["date"],axis=1)

            correlation_data_subset = correlation_data.copy(deep=True).reset_index()
            correlation_data_subset["date"] = pd.DatetimeIndex(correlation_data_subset["date"]).normalize()

            correlation_data_subset = correlation_data_subset[correlation_data_subset["date"].isin(names_performance_over_intervals["entry_date"].unique())]

            technical_indicator_subset = technical_indicator_data.copy(deep=True).reset_index()
            technical_indicator_subset["date"] = pd.DatetimeIndex(technical_indicator_subset["date"]).normalize()

            technical_indicator_subset = technical_indicator_subset[technical_indicator_subset["date"].isin(names_performance_over_intervals["entry_date"].unique())]

            correlation_indicator_data = pd.merge(correlation_data_subset,technical_indicator_subset, how="left" ,
                                                   on= ["date","ticker"])
            merged_data = pd.merge(merged_data,correlation_indicator_data,
                                   how="left",  left_on=["entry_date","ticker"],
                                   right_on=["date","ticker"]).drop(["date"],axis=1)

            stochastic_indicator_subset = stochastic_indicator_data.copy(deeop=True).reset_index()
            stochastic_indicator_subset["date"] = pd.DatetimeIndex(stochastic_indicator_subset["date"]).normalize()
            stochastic_indicator_subset = stochastic_indicator_subset[stochastic_indicator_subset["date"].isin(names_performance_over_intervals["entry_date"].unique())]

            merged_data = pd.merge(merged_data, stochastic_indicator_subset,
                                   how="left",  left_on=["entry_date","ticker"],
                                   right_on=["date","ticker"]).drop(["date"],axis=1)

            new_past_return_data = past_return_data.copy(deep=True)
            new_past_return_data["date"] = pd.DatetimeIndex(new_past_return_data["date"]).normalize()
            merged_data = pd.merge(merged_data, new_past_return_data, how="left", left_on=["entry_date", "ticker"],
                                   right_on=["date", "ticker"]).drop(["date"], axis=1)

            for df in [new_sector_industry_mapping, etf_information]:
                merged_data = pd.merge(merged_data, df, how="left", on=["ticker"])

            for df in [ravenpack_indicator_data, ravenpack_overnight_volume_data]:
                merged_data = pd.merge(merged_data, df.rename(columns = {"date" : "entry_date"}).drop(["rp_entity_id"],axis=1),
                                       how ="left", on=["entry_date", "ticker"])

            fund_cols = [col for col in fundamental_data.columns if '_date' in col]
            joint = pd.concat([derived_fundamental_data, fundamental_data[fund_cols]], axis=1)

            for df in [quarterly_return_data, joint]:
                merged_data = pd.merge(merged_data, df, how="left",
                                       on = ["ticker", "this_quarter"]).drop(["date"],axis=1)

            merged_data["sector"].fillna(value = "None", inplace= True)

            # Add indicator_benchmark
            merged_data = pd.merge(merged_data,
                                   merged_data[['entry_date', 'ticker', 'indicator']].rename(columns={'entry_date': 'date'}),
                                   how='left', left_on=['entry_date', 'benchmark_etf'], right_on=['date', 'ticker'],
                                   suffixes=("", "_benchmark")).drop(['date', 'ticker_benchmark'], axis=1)

            # Add here (Check column name)
            all_benchmarks = list(set(self.ranking_benchmarks).union(set(self.sector_benchmarks))\
                                  .union(set(self.other_benchmarks)))
            all_dfs = []
            for etf in all_benchmarks:
                cond = (merged_data['ticker']==etf)
                current_df = merged_data[cond][['entry_date', 'indicator', 'indicator_rank']]
                current_df = current_df.set_index('entry_date', drop=True)
                current_df.rename(columns={'indicator': "{0}_indicator".format(etf),
                                                   'indicator_rank': "{0}_indicator_rank".format(etf)}, inplace=True)
                if current_df.shape[1]==2:
                    all_dfs.append(current_df)
            all_dfs = pd.concat(all_dfs, axis=1)
            all_dfs['entry_date'] = all_dfs.index
            merged_data = pd.merge(merged_data, all_dfs, how="left", on=["entry_date"])
            merged_data = pd.merge(merged_data,
                                   spy_indicator_peer_rank.rename(columns = {"SPY" : "spy_indicator_peer_rank",
                                                                             "date" : "entry_date"}),
                                   how ="left", on=["entry_date"])

            merged_data = pd.merge(merged_data, indicator_comparisons.rename(columns = {"date" : "entry_date"}),
                                   how ="left", on=["entry_date"])

            tlt = merged_data[merged_data["ticker"]=="TLT"][["entry_date", "SPY_correl", "SPY_correl_rank"]]
            merged_data = pd.merge(merged_data,
                                   tlt.rename(columns = {"SPY_correl" : "SPY_TLT_correl",
                                                         "SPY_correl_rank": "SPY_TLT_correl_rank"}),
                                   how ="left", on=["entry_date"])

            merged_data = pd.merge(merged_data, dollar_volume_data.rename(columns = {"date" : "entry_date"}),
                                   how ="left", on=["entry_date", "ticker"])
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
        return {"slippage_pct": 0.001, "addtive_tc": 0.01,
                "ranking_benchmarks": ['DIA', 'DVY', 'IJH', 'IJJ', 'IJK', 'IJR', 'IVE', 'IVV', 'IVW', 'IWB',
                                       'IWD', 'IWM', 'IWN', 'IWO', 'IWR', 'IWV', 'MDY', 'RSP', 'SCHA', 'SPY',
                                       'VTI', 'VTV', 'VV'],
                "sector_benchmarks": ["XLI", "XLV", "XLF", "XLK", "XLY", "XLE", "VDC", "XLB", "XLU"],
                "other_benchmarks": ["TLT", "RSP", "EEM", "ACWI"]}

class FundamentalsMergeQuandl(CalibrationTaskflowTask):
    '''

    Merges all data elements from fundamentals pipeline (with quandl data) into a single dataframe

    '''
    PROVIDES_FIELDS = ["merged_data"]
    REQUIRES_FIELDS = ["daily_price_data", "names_performance_over_intervals_with_raw_prices",
                       "overnight_return_data", "quarterly_return_data", "stochastic_indicator_data",
                       "stochastic_ewma_indicator_data", "correlation_data", "technical_indicator_data",
                       "sector_industry_mapping", "etf_information", "ravenpack_indicator_data",
                       "ravenpack_overnight_volume_data", "fundamental_data_quandl",
                       "derived_fundamental_data", "dollar_volume_data", "spy_indicator_peer_rank",
                       "indicator_comparisons", "past_return_data", "tsi_indicator_data",
                       "macd_indicator_data", "volatility_data"]

    def __init__(self, slippage_pct=None, addtive_tc=None, ranking_benchmarks=None, sector_benchmarks=None,
                 other_benchmarks=None):
        self.data = None
        self.slippage_pct = slippage_pct if slippage_pct else 0.001
        self.addtive_tc = addtive_tc if addtive_tc else 0.01
        self.ranking_benchmarks = ranking_benchmarks
        self.sector_benchmarks = sector_benchmarks
        self.other_benchmarks = other_benchmarks
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        gc.collect()
        try:
            lcl = globals()
            for k in self.REQUIRES_FIELDS:
                lcl[k] = kwargs[k]

            new_sector_industry_mapping = sector_industry_mapping.copy(deep=True).rename(columns = {"etf" : "benchmark_etf"})

            merged_data = names_performance_over_intervals_with_raw_prices

            new_overnight_return_data = overnight_return_data.copy(deep=True)
            new_overnight_return_data["date"] = pd.DatetimeIndex(new_overnight_return_data["date"]).normalize()
            merged_data = pd.merge(merged_data,new_overnight_return_data, how="left",
                                   left_on=["entry_date","ticker"], right_on=["date","ticker"]).drop(["date"],axis=1)

            correlation_data_subset = correlation_data.copy(deep=True).reset_index()
            correlation_data_subset["date"] = pd.DatetimeIndex(correlation_data_subset["date"]).normalize()

            correlation_data_subset = correlation_data_subset[correlation_data_subset["date"].isin(names_performance_over_intervals_with_raw_prices["entry_date"].unique())]

            technical_indicator_subset = technical_indicator_data.copy(deep=True).reset_index()
            technical_indicator_subset["date"] = pd.DatetimeIndex(technical_indicator_subset["date"]).normalize()

            technical_indicator_subset = technical_indicator_subset[technical_indicator_subset["date"].isin(names_performance_over_intervals_with_raw_prices["entry_date"].unique())]

            correlation_indicator_data = pd.merge(correlation_data_subset,technical_indicator_subset, how="left" ,
                                                   on= ["date","ticker"])
            merged_data = pd.merge(merged_data,correlation_indicator_data,
                                   how="left",  left_on=["entry_date","ticker"],
                                   right_on=["date","ticker"]).drop(["date"],axis=1)

            stochastic_indicator_subset = stochastic_indicator_data.copy(deep=True).reset_index()
            stochastic_indicator_subset["date"] = pd.DatetimeIndex(stochastic_indicator_subset["date"]).normalize()
            stochastic_indicator_subset = stochastic_indicator_subset[stochastic_indicator_subset["date"].isin(names_performance_over_intervals_with_raw_prices["entry_date"].unique())]

            merged_data = pd.merge(merged_data, stochastic_indicator_subset,
                                   how="left",  left_on=["entry_date","ticker"],
                                   right_on=["date","ticker"]).drop(["date"],axis=1)

            stochastic_ewma_indicator_subset = stochastic_ewma_indicator_data.copy(deep=True).reset_index()
            stochastic_ewma_indicator_subset["date"] = pd.DatetimeIndex(stochastic_ewma_indicator_subset["date"]).normalize()
            stochastic_ewma_indicator_subset = stochastic_ewma_indicator_subset[stochastic_ewma_indicator_subset["date"].isin(names_performance_over_intervals_with_raw_prices["entry_date"].unique())]

            merged_data = pd.merge(merged_data, stochastic_ewma_indicator_subset,
                                   how="left",  left_on=["entry_date","ticker"],
                                   right_on=["date","ticker"]).drop(["date"],axis=1)

            tsi_indicator_subset = tsi_indicator_data.copy(deep=True).reset_index()
            tsi_indicator_subset["date"] = pd.DatetimeIndex(tsi_indicator_subset["date"]).normalize()
            tsi_indicator_subset = tsi_indicator_subset[tsi_indicator_subset["date"].isin(names_performance_over_intervals_with_raw_prices["entry_date"].unique())]

            merged_data = pd.merge(merged_data, tsi_indicator_subset,
                                   how="left",  left_on=["entry_date","ticker"],
                                   right_on=["date","ticker"]).drop(["date"],axis=1)

            macd_indicator_subset = macd_indicator_data.copy(deep=True).reset_index()
            macd_indicator_subset["date"] = pd.DatetimeIndex(macd_indicator_subset["date"]).normalize()
            macd_indicator_subset = macd_indicator_subset[macd_indicator_subset["date"].isin(names_performance_over_intervals_with_raw_prices["entry_date"].unique())]

            merged_data = pd.merge(merged_data, macd_indicator_subset,
                                   how="left",  left_on=["entry_date","ticker"],
                                   right_on=["date","ticker"]).drop(["date"],axis=1)

            volatility_subset = volatility_data.copy(deep=True).reset_index()
            volatility_subset["date"] = pd.DatetimeIndex(volatility_subset["date"]).normalize()
            volatility_subset = volatility_subset[volatility_subset["date"].isin(names_performance_over_intervals_with_raw_prices["entry_date"].unique())]

            merged_data = pd.merge(merged_data, volatility_subset,
                                   how="left",  left_on=["entry_date","ticker"],
                                   right_on=["date","ticker"]).drop(["date"],axis=1)

            # Introduce diffs
            #ret_20B, ret_60B, ret_125B, ret_252B
            new_past_return_data = past_return_data.copy(deep=True)
            new_past_return_data["ret_20B_diff_ret_60B"] = past_return_data["ret_20B"] - past_return_data["ret_60B"]
            new_past_return_data["ret_20B_diff_ret_125B"] = past_return_data["ret_20B"] - past_return_data["ret_125B"]
            new_past_return_data["ret_20B_diff_ret_252B"] = past_return_data["ret_20B"] - past_return_data["ret_252B"]
            new_past_return_data["ret_60B_diff_ret_125B"] = past_return_data["ret_60B"] - past_return_data["ret_125B"]
            new_past_return_data["ret_60B_diff_ret_252B"] = past_return_data["ret_60B"] - past_return_data["ret_252B"]
            new_past_return_data["date"] = pd.DatetimeIndex(new_past_return_data["date"]).normalize()
            merged_data = pd.merge(merged_data, new_past_return_data, how="left", left_on=["entry_date", "ticker"],
                                   right_on=["date", "ticker"]).drop(["date"], axis=1)

            for df in [new_sector_industry_mapping, etf_information]:
                merged_data = pd.merge(merged_data, df, how="left", on=["ticker"])

            for df in [ravenpack_indicator_data, ravenpack_overnight_volume_data]:
                merged_data = pd.merge(merged_data, df.rename(columns = {"date" : "entry_date"}).drop(["rp_entity_id"],axis=1),
                                       how ="left", on=["entry_date", "ticker"])

            #fund_cols = [col for col in fundamental_data.columns if '_date' in col]
            #joint = pd.concat([derived_fundamental_data, fundamental_data[fund_cols]], axis=1)
            #joint = derived_fundamental_data
            #joint["date"] = joint["calendardate"]

            for df in [quarterly_return_data]:
                merged_data = pd.merge(merged_data, df, how="left",
                                       on = ["ticker", "this_quarter"]).drop(["date"],axis=1)


            merged_data = pd.merge(merged_data, derived_fundamental_data, how="left", on=["ticker", "entry_date"])\
                .drop(["cob"], axis=1)
            merged_data["sector"] = merged_data["sector"].fillna(value = "None")

            # Add indicator_benchmark
            merged_data = pd.merge(merged_data,
                                   merged_data[['entry_date', 'ticker', 'indicator']].rename(columns={'entry_date': 'date'}),
                                   how='left', left_on=['entry_date', 'benchmark_etf'], right_on=['date', 'ticker'],
                                   suffixes=("", "_benchmark")).drop(['date', 'ticker_benchmark'], axis=1)

            # Add here (Check column name)
            all_benchmarks = list(set(self.ranking_benchmarks).union(set(self.sector_benchmarks))\
                                  .union(set(self.other_benchmarks)))
            all_benchmarks.sort()

            all_dfs = []
            for etf in all_benchmarks:
                cond = (merged_data['ticker']==etf)
                current_df = merged_data[cond][['entry_date', 'indicator', 'indicator_rank']]
                current_df = current_df.set_index('entry_date', drop=True)
                current_df.rename(columns={'indicator': "{0}_indicator".format(etf),
                                           'indicator_rank': "{0}_indicator_rank".format(etf)}, inplace=True)
                if current_df.shape[1]==2:
                    all_dfs.append(current_df)
            all_dfs = pd.concat(all_dfs, axis=1)
            all_dfs['entry_date'] = all_dfs.index
            merged_data = pd.merge(merged_data, all_dfs, how="left", on=["entry_date"])
            merged_data = pd.merge(merged_data,
                                   spy_indicator_peer_rank.rename(columns = {"SPY" : "spy_indicator_peer_rank",
                                                                             "date" : "entry_date"}),
                                   how ="left", on=["entry_date"])

            merged_data = pd.merge(merged_data, indicator_comparisons.rename(columns = {"date" : "entry_date"}),
                                   how ="left", on=["entry_date"])

            tlt = merged_data[merged_data["ticker"]=="TLT"][["entry_date", "SPY_correl", "SPY_correl_rank"]]
            merged_data = pd.merge(merged_data,
                                   tlt.rename(columns = {"SPY_correl" : "SPY_TLT_correl",
                                                         "SPY_correl_rank": "SPY_TLT_correl_rank"}),
                                   how ="left", on=["entry_date"])

            merged_data = pd.merge(merged_data, dollar_volume_data.rename(columns = {"date" : "entry_date"}),
                                   how ="left", on=["entry_date", "ticker"])
            good_cols = [c for c in merged_data.columns if not c.startswith("index")]
            merged_data = merged_data[good_cols]
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
        return {"slippage_pct": 0.001, "addtive_tc": 0.01,
                "ranking_benchmarks": ['DIA', 'DVY', 'IJH', 'IJJ', 'IJK', 'IJR', 'IVE', 'IVV', 'IVW', 'IWB',
                                       'IWD', 'IWM', 'IWN', 'IWO', 'IWR', 'IWV', 'MDY', 'RSP', 'SCHA', 'SPY',
                                       'VTI', 'VTV', 'VV'],
                "sector_benchmarks": ["XLI", "XLV", "XLF", "XLK", "XLY", "XLE", "VDC", "XLB", "XLU"],
                "other_benchmarks": ["TLT", "RSP", "EEM", "ACWI"]}



class FundamentalsDigitization(Digitization):
    '''

    Adds digitization of feature columns provided a digitization description (quantiles, groupby, and column names)
    for fundamentals calibration

    '''
    REQUIRES_FIELDS = ["merged_data"]
    PROVIDES_FIELDS = ["final_data"]

    def __init__(self, digitization_description):
        Digitization.__init__(self, digitization_description)

    @classmethod
    def get_default_config(cls):
        return {"digitization_description": {"per_sector_date": \
                {"columns": ["ret_20B", "ret_60B", "ret_125B", "ret_252B", "indicator"],
                 "quantiles": [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
                 "group_by": ["sector", "entry_date"],
                 "suffix": "_sector_date"}}}

class FundamentalsDigitizationQuandl(Digitization):
    '''

    Adds digitization of feature columns provided a digitization description (quantiles, groupby, and column names)
    for fundamentals calibration (with quandl data)

    '''
    REQUIRES_FIELDS = ["merged_data"]
    PROVIDES_FIELDS = ["final_data"]

    @classmethod
    def __get_sample_digitization_description(cls):
        return {
          "per_date": {
            "columns": ["ret_20B", "ret_60B", "stoch_k"],
            "quantiles": [ 0.0, 0.1, 0.2, 0.3],
            "group_by": ["entry_date"],
            "suffix": "_date"
          }
        }

    @classmethod
    def get_default_config(cls):
        return {"digitization_description": {"per_sector_date": \
                {"columns": ["ret_20B", "ret_60B", "ret_125B", "ret_252B", "indicator"],
                 "quantiles": [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
                 "group_by": ["sector", "entry_date"],
                 "suffix": "_sector_date"}}}

class FundamentalsAssetSelectorAndWeighting(BaseAssetSelector):
    '''

    Creates the long short baskets for fundamental calibration based on feature rules and ranking specification

    '''

    REQUIRES_FIELDS = ["final_data", "rule_applications", "intervals_data"]
    PROVIDES_FIELDS = ["selections_rule_date"]

    RULE_DEPENDENCIES = None

    def __init__(self, rule_dependencies, sub_rule_params, regime=1, exclude_tickers=None):
        self.data = None
        self.exclude_tickers = exclude_tickers or ['BIV', 'BSV', 'CHAD', 'IEF', 'IEI', 'LABD', 'QID', 'SDS', 'SH',
                                                   'SHY', 'SPXU', 'SQQQ', 'TIP', 'TLH', 'TLT', 'TVIX', 'TWM', 'TZA',
                                                   'UVXY', 'VIXY', 'VXX']
        BaseAssetSelector.__init__(self, "fundamental_assets", regime)
        self.sub_rule_params = sub_rule_params
        self.__class__.RULE_DEPENDENCIES = rule_dependencies

    def calculate_weights(self, matched_names, matched_rules_set, non_matched_rules_set):
        sub_rule_params_dict = self.sub_rule_params
        assets_dict = {}
        for rule in list(matched_rules_set):
            rule_group, sub_rule = rule.split(".")
            rule_matches = matched_names.loc[rule, :].reset_index()
            if len(rule_matches):
                sub_rule_params = sub_rule_params_dict[rule_group][sub_rule]
                sort_columns = sub_rule_params["sort_columns"]
                ascending = sub_rule_params["ascending"]
                max_num = sub_rule_params["max_num"]
                # Filtering tickers that should be excluded
                rule_matches = rule_matches.loc[~rule_matches.ticker.isin(self.exclude_tickers), :]
                filtered_tickers = \
                    list(rule_matches.sort_values(sort_columns, ascending=ascending)['ticker'].drop_duplicates()[:max_num])
                filtered_tickers.sort()
                assets_dict[rule] = filtered_tickers
            else:
                assets_dict[rule] = []
        return assets_dict

    def do_step_action(self, **kwargs):
        intervals_data = kwargs["intervals_data"]
        rule_applications = kwargs["rule_applications"]
        final_data = kwargs["final_data"]
        individual_results = []
        dates_to_process = intervals_data["entry_date"].sort_values().tolist()
        for n, dt in enumerate(dates_to_process):
            if not (n%self.report_every_n_iterations):
                calibration_logger.info("Processing {0} on date {1} ({2} out of {3})".format(self.__class__.__name__, dt,
                                                                                            n+1, len(dates_to_process)))
            single_date_data = self._get_data_for_entry_date(rule_applications, dt)
            individual_results.append(self.calculate_weights(*single_date_data))
        if (n%self.report_every_n_iterations):
            calibration_logger.info("Processing {0} on date {1} ({2} out of {3})".format(self.__class__.__name__, dt, n+1,
                                                                                   len(dates_to_process)))
        self.data = pd.Series(data=individual_results, index=dates_to_process, name=self.weights_name)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"rule_dependencies": ["long_rules.long_rule_combined", "short_rules.short_rule_combined"],
                "sub_rule_params": { \
                    "long_rules.long_rule_combined": {"sort_columns": ["ret_60B"], "ascending": [True], "max_num": 60},
                    "short_rules.short_rule_combined": {"sort_columns": ["ret_60B"], "ascending": [True], "max_num": 60}},
                "regime": 1,
                "exclude_tickers": ['BIV', 'BSV', 'CHAD', 'IEF', 'IEI', 'LABD', 'QID', 'SDS', 'SH', 'SHY', 'SPXU',
                                    'SQQQ', 'TIP', 'TLH', 'TLT', 'TVIX', 'TWM', 'TZA', 'UVXY', 'VIXY', 'VXX']}


class FundamentalsCalibration(CalibrationFileTaskflowTask):
    '''

    Creates the final fundamental trader calibration file based on basket selections and auxiliary properties
    such as hedge_ratio, rebalancing frequency etc. Deposits the file in a local or s3 location

    '''

    REQUIRES_FIELDS = ["selections_rule_date", "intervals_data"]
    PROVIDES_FIELDS = ["calibration"]
    TRADER_FIELDS = ["date", "actorID", "role", "ranking", "long_basket", "short_basket", "hedge_ratio",
                     "stop_limits_pct", "basket_selloff_date", "rebalancing_frequency", "rebalancing_time",
                     "leverage_ratio_per_trade", "total_funding", "fundingRequestsAmount", "funding_buffer",
                     "override_filter", "instructions_overrides"]

    def __init__(self, traders, supervisor, include_supervisor, output_dir, file_suffix,
                 ignore_calibration_timeline=False, calibration_date_offset=None, calibration_anchor_date=None):
        self.traders = traders
        self.supervisor = supervisor
        self.include_supervisor = include_supervisor or False
        self.data = None
        self.calibration_date_offset = calibration_date_offset
        self.calibration_anchor_date = calibration_anchor_date
        CalibrationFileTaskflowTask.__init__(self, output_dir, file_suffix or "fundamental_rebalancing_trader.csv",
                                             ignore_calibration_timeline)

    def _create_calibration_for_one_trader(self, trader, selections_rule_date):
        cal_df = pd.DataFrame(columns=self.__class__.TRADER_FIELDS)
        trader_config = self.traders[trader]

        cal_df['date'] = selections_rule_date.index
        cal_df['actorID'] = trader
        cal_df['role'] = 2
        cal_df['long_basket'] = selections_rule_date.apply(lambda x:x.get(trader_config["long_rules"], [])).values
        cal_df['short_basket'] = selections_rule_date.apply(lambda x:x.get(trader_config["short_rules"], [])).values

        for key, row in cal_df.iterrows():
            long_basket = row['long_basket']
            short_basket = row['short_basket']
            long_basket.sort()
            short_basket.sort()
            intersection_names = set(long_basket).intersection(set(short_basket))
            cal_df.loc[cal_df.index == key, 'long_basket'] = \
                str([elem for elem in long_basket if elem not in list(intersection_names)]).replace("'", '"') \
                    if intersection_names else str(long_basket).replace("'", '"')
            cal_df.loc[cal_df.index == key, 'short_basket'] = \
                str([elem for elem in short_basket if elem not in list(intersection_names)]).replace("'", '"') \
                    if intersection_names else str(short_basket).replace("'", '"')

        skip_fields = set(["date", "actorID", "role", "long_basket", "short_basket", "total_funding",
                           "fundingRequestsAmount", "funding_buffer", "override_filter", "instructions_overrides"])

        direct_concepts = list(set(self.__class__.TRADER_FIELDS) - skip_fields)
        for concept in direct_concepts:
            cal_df[concept] = trader_config[concept]

        return cal_df


    def do_step_action(self, **kwargs):
        selections_rule_date = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        intervals_data = kwargs[self.__class__.REQUIRES_FIELDS[1]]
        all_calibrations = [self._create_calibration_for_one_trader(trader, selections_rule_date) \
                            for trader in self.traders.keys()]
        if self.include_supervisor:
            cal_df = pd.DataFrame(columns=self.__class__.TRADER_FIELDS)
            sup_config = self.supervisor
            cal_df["date"] = selections_rule_date.index
            cal_df["actorID"] = "SUP"
            cal_df["role"] = 1
            cal_df["total_funding"] = sup_config["total_funding"]
            cal_df["fundingRequestsAmount"] = sup_config["fundingRequestsAmount"]
            cal_df["funding_buffer"] = sup_config["funding_buffer"]
            all_calibrations.append(cal_df)
        all_calibrations = pd.concat(all_calibrations, ignore_index=True)

        self.data = all_calibrations
        #TODO: change later
        output_df = self.data
        filename = self.get_file_targets()[0]
        store_pandas_object(output_df, filename, "Fundamentals Calibration")
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    def get_calibration_dates(self, task_params):
        return self.get_calibration_dates_with_offset_anchor(task_params, self.calibration_date_offset,
                                                             self.calibration_anchor_date)

    @classmethod
    def get_default_config(cls):
        return {
            "traders": json.dumps({ \
                "Fundamentals_FCFTrader_1": { \
                    "ranking": 1,
                    "long_rules": "long_rules.long_rule_combined", "short_rules": "short_rules.short_rule_combined",
                    "hedge_ratio": 0.5, "stop_limits_pct": "(-0.4, 100.0)", "basket_selloff_date": "2019-12-15",
                    "rebalancing_frequency": "BMS", "rebalancing_time": "09:31:00", "leverage_ratio_per_trade": 1.5}}),
            "supervisor": json.dumps({"total_funding": 9000000.0, "fundingRequestsAmount": 6350000.0, "funding_buffer": 250000.0}),
            "include_supervisor": True, "output_dir": "./calibration_files/", "file_suffix": "fundamental_rebalancing_trader.csv",
            "ignore_calibration_timeline": True, "calibration_date_offset": "BMS", "calibration_anchor_date": None
        }


