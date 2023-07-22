import gc
import pandas as pd

from commonlib.util_functions import create_directory_if_does_not_exists
from etl_workflow_steps import StatusType, BaseETLStage, compose_main_flow_and_engine_for_task
from calibrations.common.config_functions import get_config
from calibrations.data_pullers.data_pull \
    import (SQLReaderPriceData, SQLReaderETFInformation, SQLReaderDailyIndexData, SQLReaderRavenpackMomentumIndicator,
            SQLReaderRavenpackOvernightVolume, SQLReaderSectorBenchmarkMap,
            SQLReaderEarningsDates, SQLReaderFundamentalDataQuandl, SQLReaderAdjustmentFactors)
from calibrations.feature_builders.digitizer import Digitization

from calibrations.common.calibration_logging import calibration_logger

from calibrations.common.config_functions import get_config
from calibrations.core.calibration_taskflow_task import CalibrationTaskflowTask

RANDOM_STATE = 12345


class JumpMerge(CalibrationTaskflowTask):
    '''

    Merges the data elements for the jump calibration pipeline

    '''

    PROVIDES_FIELDS = ["merged_data"]
    REQUIRES_FIELDS = ["daily_price_data", "names_performance_over_intervals_with_raw_prices",
                       "overnight_return_data", "quarterly_return_data", "stochastic_indicator_data",
                       "correlation_data", "technical_indicator_data",
                       "sector_industry_mapping", "etf_information", "ravenpack_indicator_data",
                       "ravenpack_overnight_volume_data", "derived_fundamental_data", "dollar_volume_data",
                       "past_return_data", "future_return_data", "jump_indicator_data"]

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

            merged_data = names_performance_over_intervals_with_raw_prices.copy(deep=True)
            new_overnight_return_data = overnight_return_data.copy(deep=True)
            new_overnight_return_data["date"] = pd.DatetimeIndex(new_overnight_return_data["date"]).normalize()
            merged_data = pd.merge(merged_data, new_overnight_return_data, how="left",
                                   left_on=["entry_date","ticker"], right_on=["date","ticker"]).drop(["date"],axis=1)

            correlation_data_subset = correlation_data.copy(deep=True).reset_index()
            correlation_data_subset["date"] = pd.DatetimeIndex(correlation_data_subset["date"]).normalize()

            correlation_data_subset = correlation_data_subset[correlation_data_subset["date"].isin(names_performance_over_intervals_with_raw_prices["entry_date"].unique())]

            technical_indicator_subset = technical_indicator_data.copy(deep=True).reset_index()
            technical_indicator_subset["date"] = pd.DatetimeIndex(technical_indicator_subset["date"]).normalize()

            technical_indicator_subset = technical_indicator_subset[technical_indicator_subset["date"].isin(names_performance_over_intervals_with_raw_prices["entry_date"].unique())]

            stochastic_indicator_subset = stochastic_indicator_data.copy(deep=True).reset_index()
            stochastic_indicator_subset["date"] = pd.DatetimeIndex(stochastic_indicator_subset["date"]).normalize()
            stochastic_indicator_subset = stochastic_indicator_subset[stochastic_indicator_subset["date"].isin(names_performance_over_intervals_with_raw_prices["entry_date"].unique())]

            merged_data = pd.merge(merged_data, stochastic_indicator_subset,
                                   how="left",  left_on=["entry_date","ticker"],
                                   right_on=["date","ticker"]).drop(["date"],axis=1)

            jump_indicator_subset = jump_indicator_data.copy(deep=True).reset_index()
            jump_indicator_subset["date"] = pd.DatetimeIndex(jump_indicator_subset["date"]).normalize()
            jump_indicator_subset = jump_indicator_subset[jump_indicator_subset["date"].isin(names_performance_over_intervals_with_raw_prices["entry_date"].unique())]

            merged_data = pd.merge(merged_data, jump_indicator_subset, how="left",  left_on=["entry_date","ticker"],
                                   right_on=["date","ticker"]).drop(["date"],axis=1)

            correlation_indicator_data = pd.merge(correlation_data_subset,technical_indicator_subset, how="left" ,
                                                  on= ["date","ticker"])
            merged_data = pd.merge(merged_data,correlation_indicator_data,
                                   how="left",  left_on=["entry_date","ticker"],
                                   right_on=["date","ticker"]).drop(["date"],axis=1)

            new_past_return_data = past_return_data.copy(deep=True)
            new_past_return_data["date"] = pd.DatetimeIndex(new_past_return_data["date"]).normalize()
            merged_data = pd.merge(merged_data, new_past_return_data, how="left", left_on=["entry_date", "ticker"],
                                   right_on=["date", "ticker"]).drop(["date"], axis=1)

            new_future_return_data = future_return_data.copy(deep=True)
            new_future_return_data["date"] = pd.DatetimeIndex(new_future_return_data["date"]).normalize()
            merged_data = pd.merge(merged_data, new_future_return_data, how="left", left_on=["entry_date", "ticker"],
                                   right_on=["date", "ticker"]).drop(["date"], axis=1)

            etf_information_unique = etf_information.drop_duplicates().reset_index(drop=True)
            for df in [new_sector_industry_mapping, etf_information_unique]:
                merged_data = pd.merge(merged_data, df, how="left", on=["ticker"])

            for df in [ravenpack_indicator_data, ravenpack_overnight_volume_data]:
                merged_data = pd.merge(merged_data, df.rename(columns = {"date" : "entry_date"}).drop(["rp_entity_id"],axis=1),
                                       how ="left", on=["entry_date", "ticker"])

            merged_data.rename(columns={"volume": "news_volume"}, inplace=True)
            for df in [quarterly_return_data]:
                merged_data = pd.merge(merged_data, df, how="left",
                                       on = ["ticker", "this_quarter"]).drop(["date"],axis=1)


            merged_data = pd.merge(merged_data, derived_fundamental_data, how="left", on=["ticker", "entry_date"])
            merged_data["sector"].fillna(value = "None", inplace= True)

            # Add indicator_benchmark
            merged_data = pd.merge(merged_data,
                                   merged_data[['entry_date', 'ticker', 'indicator']].rename(columns={'entry_date': 'date'}),
                                   how='left', left_on=['entry_date', 'benchmark_etf'], right_on=['date', 'ticker'],
                                   suffixes=("", "_benchmark")).drop(['date', 'ticker_benchmark'], axis=1)

            merged_data = pd.merge(merged_data,
                                   merged_data[['entry_date', 'ticker', 'overnight_return']].rename(columns={'entry_date': 'date'}),
                                   how='left', left_on=['entry_date', 'benchmark_etf'], right_on=['date', 'ticker'],
                                   suffixes=("", "_benchmark")).drop(['date', 'ticker_benchmark'], axis=1)

            # Add here (Check column name)
            all_benchmarks = list(set(self.ranking_benchmarks).union(set(self.sector_benchmarks))\
                                  .union(set(self.other_benchmarks)))
            all_dfs = []
            for etf in ["SPY"]:
                cond = (merged_data['ticker']==etf)
                current_df = merged_data[cond][['entry_date', 'indicator']]
                current_df = current_df.set_index('entry_date', drop=True)
                current_df.rename(columns={'indicator': "{0}_indicator".format(etf)}, inplace=True)
                all_dfs.append(current_df)
            all_dfs = pd.concat(all_dfs, axis=1)
            all_dfs['entry_date'] = all_dfs.index
            merged_data = pd.merge(merged_data, all_dfs, how="left", on=["entry_date"])

            all_dfs = []
            for etf in all_benchmarks:
                cond = (merged_data['ticker']==etf)
                current_df = merged_data[cond][['entry_date', 'overnight_return']]
                current_df = current_df.set_index('entry_date', drop=True)
                current_df.rename(columns={'overnight_return': "{0}_overnight_return".format(etf)}, inplace=True)
                all_dfs.append(current_df)
            all_dfs = pd.concat(all_dfs, axis=1)
            all_dfs['entry_date'] = all_dfs.index
            merged_data = pd.merge(merged_data, all_dfs, how="left", on=["entry_date"])

            merged_data["overnight_return_over_SPY"] = merged_data["overnight_return"] \
                - merged_data["SPY_overnight_return"]
            merged_data["overnight_return_over_benchmark"] = merged_data["overnight_return"] \
                - merged_data["overnight_return_benchmark"]

            remove_cols = list(all_dfs.columns)
            remove_cols.remove("SPY_overnight_return")
            remove_cols.remove("entry_date")
            remove_cols.append("split_factor")
            remove_cols.append("split_div_factor")
            remove_cols.append("avg_price")

            merged_data = pd.merge(merged_data, dollar_volume_data.rename(columns = {"date" : "entry_date"}),
                                   how ="left", on=["entry_date", "ticker"])

            final_cols = list(set(merged_data.columns) - set(remove_cols))
            merged_data = merged_data.drop_duplicates().reset_index(drop=True)
            self.data = merged_data[final_cols]
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

class JumpDigitization(Digitization):
    '''

    Digitizes the features from the jump calibration pipeline according to provided digitization description

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


