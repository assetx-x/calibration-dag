import gc
import os

import numpy as np
import pandas as pd
import pyhocon
from collections import OrderedDict
import json
import tempfile
import talib

from pandas.tseries.offsets import BDay
from etl_workflow_steps import StatusType
from commonlib.commonStatTools.technical_indicators.offline import (price_oscillator, stoch_k, MA, tsi, rsi, macd,
                                                                    compute_WVF, lee_jump_detection, fracDiff_FFD,
                                                                    talib_PPO, talib_TRIX, talib_STOCHRSI,
                                                                    talib_STOCH, talib_STOCHF, talib_ULTOSC,
                                                                    winsorize)

from commonlib.market_timeline import marketTimeline
from commonlib.data_requirements import LookbackField, DataRequirementsCollection
from commonlib.timeline_permutation import BusinessDatePermutatator, MarketDataPermutator
from commonlib.feature_builders.special_features import ETFPeerRankingFeatureBuilder
from commonlib.util_functions import load_obj_from_module, load_module_by_full_path, get_max_of_pandas_frequency, \
     pick_trading_month_dates, pick_trading_week_dates
from commonlib.commonStatTools.clustering_tools import CalculateBeta, PrincipalComponents, KMeansCluster, \
     GraphicalLassoPartiallyDependentGroups, get_beta, get_pca, get_kmeans, get_glasso_groups
from calibrations.core.calibration_taskflow_task import CalibrationTaskflowTask

from calibrations.common.config_functions import get_config
from quick_simulator_market_builder import expand_daily_to_minute_data, DRDataTypes
from calibrations.common.taskflow_params import BasicTaskParameters, TaskflowPipelineRunMode

SMALL = 1e-6
def transform_data(ts, transform_type=1):
    if transform_type==2:
        transformed = ts.diff()
    elif transform_type==3:
        transformed = ts.diff().diff()
    elif transform_type==4:
        transformed = pd.np.log(ts)
    elif transform_type==5:
        transformed = pd.np.log(ts).diff()
    elif transform_type==6:
        transformed = pd.np.log(ts).diff().diff()
    elif transform_type==7:
        transformed = ts.pct_change().diff()
    else:
        transformed = ts # Default is no transform
    return transformed

def transform_wrapper(ts, transform_dict):
    try:
        transform_type = transform_dict[ts.name]
    except:
        transform_type = 1 # Default is no transform
    return transform_data(ts, transform_type)


class TimelinePermutator(CalibrationTaskflowTask):
    '''

    Permutes the data timeline for bootstrapping

    '''

    PROVIDES_FIELDS = ["data_permutator"]

    def __init__(self, permutation_enabled=False, timeline_segmenter_function_names=None, random_seed=None,
                 permute_day_at_lowest_segment=True):
        self.timeline_segmenter_function_names = timeline_segmenter_function_names or []
        self.random_seed = random_seed
        self.permutation_enabled = permutation_enabled
        self.permute_day_at_lowest_segment = permute_day_at_lowest_segment
        self.permutator=None
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        if self.permutation_enabled:
            date_st = self.task_params.start_dt
            date_end = self.task_params.end_dt
            timeline_to_permute = marketTimeline.get_trading_days(date_st, date_end)
            time_segmenters = OrderedDict()
            for ts_name in self.timeline_segmenter_function_names:
                time_segmenters[ts_name] = globals()[ts_name]
            bd_permutator = BusinessDatePermutatator(timeline_to_permute, time_segmenters, self.random_seed)
            self.permutator = MarketDataPermutator(bd_permutator)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]:pd.DataFrame({"permutator":[self.permutator]})}

    @classmethod
    def get_default_config(cls):
        return {"permutation_enabled": False, "timeline_segmenter_function_names": None, "random_seed": 12345,
                "permute_day_at_lowest_segment": True}

class CalculateOvernightReturn(CalibrationTaskflowTask):
    '''

    Calculates overnight returns from daily_price_data

    '''


    PROVIDES_FIELDS = ["overnight_return_data"]
    REQUIRES_FIELDS = ["daily_price_data"]
    def __init__(self):
        self.data = None
        CalibrationTaskflowTask.__init__(self)


    def do_step_action(self, **kwargs):
        # TODO: Check whether indiscriminantly rounding is a good idea
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].sort_values(["ticker", "date"]).round(3)
        return_func = lambda g: (g["open"].combine_first(g["close"].fillna(method="ffill").shift())\
                                 /g["close"].fillna(method="ffill").shift()-1.0).to_frame(name="overnight_return")
        apply_func = lambda x:return_func(x.set_index("date"))
        self.data = daily_prices.groupby("ticker").apply(apply_func).reset_index()\
            [["date","ticker","overnight_return"]]
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}


class CalculateXIVReturn(CalibrationTaskflowTask):
    '''

    Calculates the returns of XIV based on daily price data for a given lookback period

    '''

    PROVIDES_FIELDS = ["xiv_return_data"]
    REQUIRES_FIELDS = ["daily_price_data"]
    def __init__(self, lookback_periods=7, price_concept="close"):
        self.data = None
        self.lookback_periods = lookback_periods
        self.price_concept = price_concept
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        daily_prices = daily_prices[daily_prices["ticker"]=="XIV"].set_index("date", drop=True)[self.price_concept]
        returns = (daily_prices.shift(1)/daily_prices.shift(1+self.lookback_periods) - 1.).reset_index()
        returns["date"] = pd.DatetimeIndex(returns["date"]).normalize()
        returns.rename(columns={self.price_concept: "XIV_returns"}, inplace=True)
        self.data = returns
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"lookback_periods": 7, "price_concept": "close"}

class CalculateVXXReturn(CalibrationTaskflowTask):
    '''

    Calculates the returns of VXX based on daily price data for a given lookback period

    '''

    PROVIDES_FIELDS = ["vxx_return_data"]
    REQUIRES_FIELDS = ["daily_price_data"]
    def __init__(self, lookback_periods=7, price_concept="close"):
        self.data = None
        self.lookback_periods = lookback_periods
        self.price_concept = price_concept
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices["date"] = pd.DatetimeIndex(daily_prices["date"]).normalize()

        daily_prices = daily_prices[daily_prices["ticker"]=="VXX"].set_index("date", drop=True)[self.price_concept]
        returns = (daily_prices.shift(1)/daily_prices.shift(1+self.lookback_periods) - 1.).reset_index()
        returns["date"] = pd.DatetimeIndex(returns["date"]).normalize()
        returns.rename(columns={self.price_concept: "VXX_returns"}, inplace=True)
        self.data = returns
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"lookback_periods": 7, "price_concept": "close"}

class CalculateDollarVolume(CalibrationTaskflowTask):
    '''

    Calculates the average daily dollar volume per ticker over a given lookback period

    '''

    PROVIDES_FIELDS = ["dollar_volume_data"]
    REQUIRES_FIELDS = ["daily_price_data"]
    def __init__(self, lookback_periods=10):
        self.data = None
        self.lookback_periods = lookback_periods
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)

        daily_prices["date"] = pd.DatetimeIndex(daily_prices["date"]).normalize()

        daily_prices["dollar_volume"] = daily_prices.eval("volume*(open+close)/2.0")
        daily_prices["average_price"] = daily_prices.eval("(open+close)/2.0")
        dollar_volume_table = pd.pivot_table(daily_prices, values="dollar_volume", index="date", columns="ticker",
                                             dropna=False).sort_index().fillna(method="ffill")
        avg_dollar_volumes = dollar_volume_table.rolling(self.lookback_periods).mean()
        avg_prices_table = pd.pivot_table(daily_prices, values="average_price",
                                          index="date", columns="ticker", dropna=False).sort_index().fillna(method="ffill")
        avg_prices = avg_prices_table.rolling(self.lookback_periods).mean()

        avg_dollar_volumes = avg_dollar_volumes.shift(1)
        avg_dollar_volumes.index = avg_dollar_volumes.index.normalize()
        avg_prices = avg_prices.shift(1)
        avg_prices.index = avg_prices.index.normalize()
        df_1 = pd.melt(avg_dollar_volumes.reset_index(), id_vars=['date'], var_name='ticker',
                       value_name='avg_dollar_volume')
        df_2 = pd.melt(avg_prices.reset_index(), id_vars=['date'], var_name='ticker',
                       value_name='avg_price')
        self.data = pd.merge(df_2, df_1, how='left', on=['ticker', 'date'])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"lookback_periods": 10}

class CalcualteQuarterlyReturn(CalibrationTaskflowTask):
    '''

    Calculates the quarterly return per ticker and date for the previous 1, 2, 3, 4 quarters

    '''
    PROVIDES_FIELDS = ["quarterly_return_data"]
    REQUIRES_FIELDS = ["daily_price_data"]
    def __init__(self):
        self.data = None
        CalibrationTaskflowTask.__init__(self)

    def calculate_past_performance_for_group(self, g, column = "close"):
        g_grouped_by_quarter = g.reset_index().groupby("this_quarter").last().\
            reset_index().set_index(["this_quarter"]).shift()
        for shift_amount in [1,2,3,4]:
            g_grouped_by_quarter["q{0}".format(str(shift_amount))] = g_grouped_by_quarter[column]/g_grouped_by_quarter[column].shift(shift_amount)-1.0
        #due ot the shift the first row is all nans
        return g_grouped_by_quarter.reset_index().set_index("date").iloc[1:]

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices["this_quarter"] = pd.Series(pd.DatetimeIndex(daily_prices["date"]).year.astype("str"), index=daily_prices.index) \
            + "-Q" + ((pd.DatetimeIndex(daily_prices["date"]).month-1)//3+1).astype(str)
        end_of_quarters_result = daily_prices.set_index("date")
        res = end_of_quarters_result.groupby("ticker").apply(self.calculate_past_performance_for_group)
        self.data = res.drop(["ticker"],axis=1).reset_index()[["date","this_quarter","ticker","q1","q2","q3","q4"]]
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}


class CalculatePastReturn(CalibrationTaskflowTask):
    '''

    Calculates the past returns per ticker from daily prices for a list of lookback periods

    '''

    PROVIDES_FIELDS = ["past_return_data"]
    REQUIRES_FIELDS = ["daily_price_data", 'daily_index_data']
    def __init__(self, column, lookback_list):
        self.data = None
        self.column = column or "close"
        self.lookback_list = lookback_list or [20,60,125,252]
        CalibrationTaskflowTask.__init__(self)

    def calculate_past_performance_for_group(self, g):
        column = self.column
        g_copy = g.copy(deep=True)
        if column=="open":
            day_shift = 0
        else:
            day_shift = 1
        for shift_amount in self.lookback_list:
            g_copy["ret_{0}B".format(str(shift_amount))] = g[column].shift(day_shift)/g[column].shift(day_shift).shift(shift_amount)-1.0
        return g_copy
    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_index = kwargs[self.__class__.REQUIRES_FIELDS[1]].copy(deep=True)

        daily_prices = daily_prices.set_index("date").sort_index()
        daily_index = daily_index.sort_values(['symbol', 'date', 'as_of_start'])
        daily_index = daily_index.groupby(['symbol', 'date'], as_index=False).last()

        daily_index = daily_index.drop(['as_of_end', 'as_of_start'], axis=1).rename(columns={'symbol': 'ticker'}).set_index("date").sort_index()
        daily_prices = pd.concat([daily_prices, daily_index])
        daily_prices = daily_prices.fillna(method="ffill")

        res = daily_prices.groupby("ticker").apply(self.calculate_past_performance_for_group)
        added_cols = list(map(lambda x: "ret_{0}B".format(str(x)), self.lookback_list))
        self.data = res.drop(["ticker"],axis=1).reset_index()[["date","ticker"] + added_cols]
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"column": "close","lookback_list": [20,60,125,252]}

class CalculatePastReturnEquity(CalibrationTaskflowTask):
    '''

    Calculates the past returns per ticker from daily prices for a list of lookback periods

    '''

    PROVIDES_FIELDS = ["past_return_data"]
    REQUIRES_FIELDS = ["daily_price_data"]
    def __init__(self, column, lookback_list):
        self.data = None
        self.column = column or "close"
        self.lookback_list = lookback_list or [20,60,125,252]
        CalibrationTaskflowTask.__init__(self)

    def calculate_past_performance_for_group(self, g):
        column = self.column
        g_copy = g.copy(deep=True)
        if column=="open":
            day_shift = 0
        else:
            day_shift = 1
        for shift_amount in self.lookback_list:
            g_copy["ret_{0}B".format(str(shift_amount))] = g[column].shift(day_shift)/g[column].shift(day_shift).shift(shift_amount)-1.0
        return g_copy

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices = daily_prices.set_index("date").sort_index()

        res = daily_prices.groupby("ticker").apply(self.calculate_past_performance_for_group)
        added_cols = list(map(lambda x: "ret_{0}B".format(str(x)), self.lookback_list))
        self.data = res.drop(["ticker"],axis=1).reset_index()[["date","ticker"] + added_cols]
        self.data = self.data.fillna(0.0)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"column": "close","lookback_list": [20,60,125,252]}


class GetLastAvailablePrice(CalibrationTaskflowTask):
    '''

    Provides the last available price per ticker date from daily prices

    '''

    PROVIDES_FIELDS = ["last_available_price"]
    REQUIRES_FIELDS = ["daily_price_data"]
    def __init__(self, column):
        self.data = None
        self.column = column or "close"
        CalibrationTaskflowTask.__init__(self)

    def get_last_available_price_for_group(self, g):
        column = self.column
        g_copy = g.copy(deep=True)
        if column=="open":
            day_shift = 0
        else:
            day_shift = 1
        #for shift_amount in self.lookback_list:
        g_copy["last_available_{0}".format(column)] = g[column].shift(day_shift)
        return g_copy

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].set_index("date").sort_index()
        res = daily_prices.groupby("ticker").apply(self.get_last_available_price_for_group)
        added_cols = ["last_available_{0}".format(self.column)]
        self.data = res.drop(["ticker"],axis=1).reset_index()[["date","ticker"] + added_cols]
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"column": "close"}

class CalculateFutureReturn(CalibrationTaskflowTask):
    '''

    Calculates the future returns to be used as targets for analysis from daily prices provided a list of lookforwards

    '''

    PROVIDES_FIELDS = ["future_return_data"]
    REQUIRES_FIELDS = ["daily_price_data"]
    def __init__(self, column, lookback_list):
        self.data = None
        self.column = column or "close"
        self.lookback_list = lookback_list or [1, 2, 5, 10, 21, 42, 63]
        CalibrationTaskflowTask.__init__(self)

    def calculate_future_performance_for_group(self, g, column = "close"):
        column = self.column
        g_copy = g.copy(deep=True)
        for shift_amount in self.lookback_list:
            g_copy["future_ret_{0}B".format(str(shift_amount))] = g[column].shift(-shift_amount)/g[column]-1.0
        return g_copy
    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].set_index("date").sort_index()
        daily_prices = daily_prices.fillna(method="ffill")
        res = daily_prices.groupby("ticker").apply(self.calculate_future_performance_for_group)
        added_cols = list(map(lambda x: "future_ret_{0}B".format(str(x)), self.lookback_list))
        self.data = res.drop(["ticker"],axis=1).reset_index()[["date","ticker"] + added_cols]
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"column": "close", "lookback_list": [20,60,125,252]}

class CalcualteCorrelation(CalibrationTaskflowTask):
    '''

    Calculates the return correlation between tickers and benchmarks given a lookback

    '''

    PROVIDES_FIELDS = ["correlation_data"]
    REQUIRES_FIELDS = ["daily_price_data"]
    def __init__(self, benchmark_names, correlation_lookback, price_column):
        self.data = None
        self.benchmark_names = benchmark_names or [8554] #["SPY"]
        self.correlation_lookback = correlation_lookback or 63
        self.price_column = price_column or "close"
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        pivot_data = daily_prices[["date", "ticker", self.price_column]] \
            .pivot_table(index="date", values = self.price_column, columns="ticker", dropna=False).sort_index()
        return_data = pd.np.log(pivot_data).diff()
        return_data = return_data.fillna(0.0)
        rolling_return_data = return_data.rolling(self.correlation_lookback, self.correlation_lookback)
        return_data_mean = rolling_return_data.mean()
        return_data_std = rolling_return_data.std()
        return_data = (return_data - return_data_mean) / return_data_std
        benchmark_df = return_data[self.benchmark_names]
        rolling_return_data = return_data.rolling(self.correlation_lookback, self.correlation_lookback)
        correlations = {"{0}_correl".format(i): rolling_return_data.corr(benchmark_df.loc[:, i])
                        for i in benchmark_df.columns}

        correlations = {benchmark_corr : correlations[benchmark_corr].shift() for benchmark_corr in correlations}
        rank_correlations = {"{0}_rank".format(i): correlations[i].rank(axis=1) for i in correlations}
        correlations.update(rank_correlations)
        all_correlation_data = []
        for concept in correlations:
            this_data = pd.melt(correlations[concept].reset_index(), var_name="ticker",
                                id_vars="date",value_name=concept).set_index(["date","ticker"])
            all_correlation_data.append(this_data)
        result = pd.concat(all_correlation_data, axis=1)
        self.data =  result.sort_index().reset_index()
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"benchmark_names": ["SPY"], "correlation_lookback": 63, "price_column": "close"}

class CalculateTechnicalIndicator(CalibrationTaskflowTask):
    '''

    Calculates technical indicator values based on price data. Indicator function name and parameters should be provided.

    '''

    PROVIDES_FIELDS = ["technical_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator, technical_indicator_params, price_column, calculate_indicator_ranking=True):
        self.data = None
        self.technical_indicator = globals()[technical_indicator]
        self.technical_indicator_params = technical_indicator_params
        self.calculate_indicator_ranking = calculate_indicator_ranking
        self.price_column = price_column or "close"
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        shift_value = 0  if self.price_column == "open" else 1

        indicator_func = lambda x: self.technical_indicator(x[self.price_column].sort_index(),
                                                            **self.technical_indicator_params).shift(shift_value)\
            .to_frame(name="indicator")
        apply_func = lambda x:indicator_func(x.set_index("date"))
        indicator_signal = daily_prices[["date","ticker", self.price_column]].groupby("ticker").apply(apply_func)\
            .reset_index()

        if self.calculate_indicator_ranking:
            indicator_signal["indicator_rank"] = indicator_signal.groupby(["date"])["indicator"].apply(lambda x : x.rank())
        self.data = indicator_signal#.set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator": "price_oscillator", "technical_indicator_params": {"nslow": 26, "nfast": 12},
                "price_column": "close", "calculate_indicator_ranking": True}

class CalculateSPYPeerRank(CalibrationTaskflowTask):
    '''

    Calculates the relative ranking of SPY's technical indicator w.r.t. a list of peer tickers

    '''

    PROVIDES_FIELDS = ["spy_indicator_peer_rank"]
    REQUIRES_FIELDS = ["technical_indicator_data"]

    def __init__(self, peer_tickers):
        self.data = None
        self.peer_tickers = peer_tickers or ['DIA', 'DVY', 'IJH', 'IJJ', 'IJK', 'IJR', 'IVE', 'IVV', 'IVW', 'IWB',
                                             'IWD', 'IWM', 'IWN', 'IWO', 'IWR', 'IWV', 'MDY', 'RSP', 'SCHA', 'SPY',
                                             'VTI', 'VTV', 'VV']
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        technical_indicator_data = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        technical_indicator_data_formatted = pd.pivot_table(technical_indicator_data.reset_index(),
                                                            index="date", columns="ticker", values = "indicator", dropna=False)

        effective_names = list(set(self.peer_tickers).intersection(set(technical_indicator_data_formatted.columns)))
        ranker = ETFPeerRankingFeatureBuilder(effective_names)
        data = ranker(technical_indicator_data_formatted)["SPY"].reset_index()
        data["date"] = pd.DatetimeIndex(data["date"]).normalize()
        self.data = data
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"peer_tickers": ['DIA', 'DVY', 'IJH', 'IJJ', 'IJK', 'IJR', 'IVE', 'IVV', 'IVW', 'IWB',
                                 'IWD', 'IWM', 'IWN', 'IWO', 'IWR', 'IWV', 'MDY', 'RSP', 'SCHA', 'SPY',
                                 'VTI', 'VTV', 'VV']}

class CalculateTechnicalIndicatorComparisons(CalibrationTaskflowTask):
    '''

    Returns the difference in technical indicator values provided a dictionary of ticker pairs

    '''

    PROVIDES_FIELDS = ["indicator_comparisons"]
    REQUIRES_FIELDS = ["technical_indicator_data"]

    def __init__(self, pairs_to_compare):
        self.data = None
        self.pairs_to_compare = dict(pairs_to_compare) or {'DVY_VCIT': ['DVY', 'VCIT'], 'IWO_IJR': ['IWO', 'IJR']}
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        technical_indicator_data = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        technical_indicator_data.reset_index(inplace= True)
        all_pairs = {}
        for pair in self.pairs_to_compare:
            first_indicator = technical_indicator_data[technical_indicator_data["ticker"]==self.pairs_to_compare[pair][0]].set_index("date")
            second_indicator = technical_indicator_data[technical_indicator_data["ticker"]==self.pairs_to_compare[pair][1]].set_index("date")
            all_pairs[pair +"_indicator_diff"] = first_indicator["indicator"]-second_indicator["indicator"]
        data = pd.DataFrame(all_pairs).reset_index()
        data["date"] = pd.DatetimeIndex(data["date"]).normalize()
        self.data = data
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"pairs_to_compare": {'DVY_VCIT': ['DVY', 'VCIT'], 'IWO_IJR': ['IWO', 'IJR']}}



class CalculateEarningsDatesBlocks(CalibrationTaskflowTask):
    '''

    Provides a mask that indicates the existance of an earnings event for a specified before and after interval

    '''

    PROVIDES_FIELDS = ["earnings_block_dates"]
    REQUIRES_FIELDS = ["earnings_dates", "daily_price_data"]

    def __init__(self, block_previous, block_after):
        self.data = None
        self.earnings_per_ticker = None
        self.block_previous = block_previous
        self.block_after = block_after
        CalibrationTaskflowTask.__init__(self)

    def __calculate_earnings_block_mask(self, price_data_timeline, group_name):
        if group_name not in self.earnings_per_ticker.keys:
            return pd.Series(0,index=price_data_timeline.index)
        earnings = self.earnings_per_ticker.get_group(group_name)
        start_date_block_check = pd.np.greater_equal.outer(price_data_timeline, earnings["date_lookback"])
        end_date_block_check = pd.np.less_equal.outer(price_data_timeline, earnings["date_lookfwd"])
        full_check = start_date_block_check & end_date_block_check
        return pd.Series(pd.np.sum(full_check,axis=1), index=price_data_timeline.index)

    def do_step_action(self, **kwargs):
        earnings_dates = kwargs["earnings_dates"]
        daily_price_data = kwargs["daily_price_data"]

        earnings_dates = earnings_dates.copy()
        earnings_dates["date_lookback"] = (pd.DatetimeIndex(earnings_dates["earnings_datetime_utc"])
                                                           - pd.tseries.frequencies.to_offset(self.block_previous)).normalize()
        earnings_dates["date_lookfwd"] = (pd.DatetimeIndex(earnings_dates["earnings_datetime_utc"])
                                                          + pd.tseries.frequencies.to_offset(self.block_after)).normalize()
        self.earnings_per_ticker = earnings_dates.groupby("ticker")[["date_lookback", "date_lookfwd"]]
        results = []
        for grp_name, data in daily_price_data.groupby("ticker")["date"]:
            results.append(self.__calculate_earnings_block_mask(data, grp_name))
        results = pd.concat(results)
        results.name = "earnings_block_{0}_{1}".format(self.block_previous, self.block_after)
        self.data = pd.concat([daily_price_data[["ticker","date"]].sort_index(), results.sort_index()], axis=1)
        self.data["date"] = pd.DatetimeIndex(self.data["date"]).normalize()
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"block_previous": "3B", "block_after": "5B"}

class CalculatePerformanceReturnOnIntervals(CalibrationTaskflowTask):
    '''

    calculates the long and short return with tc (if specified) for holding a ticker over regime intervals

    '''

    PROVIDES_FIELDS = ["names_performance_over_intervals"]
    REQUIRES_FIELDS = ["daily_price_data", "intervals_data"]

    def __init__(self, slippage_pct = None, addtive_tc = None):
        self.data = None
        self.slippage_pct = slippage_pct if slippage_pct else 0.001
        self.addtive_tc = addtive_tc if addtive_tc else 0.01
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_price_data = kwargs["daily_price_data"].copy(deep=True)
        intervals_data = kwargs["intervals_data"].copy(deep=True)

        daily_price_data["date"] = pd.DatetimeIndex(daily_price_data["date"]).normalize()
        merged_data = pd.merge(intervals_data, daily_price_data[["date","ticker","open"]],
                                       how="left", left_on=["entry_date"],
                                       right_on =["date"]).drop(['date'], axis=1).rename(columns={'open':'entry_price'})
        merged_data = pd.merge(merged_data, daily_price_data[["date","ticker","open"]],
                                       how="left", left_on=["exit_date", "ticker"],
                                       right_on =["date","ticker"]).drop(['date'], axis=1).rename(columns={'open':'exit_price'})
        merged_data["return_long"] = (merged_data["exit_price"]*(1-self.slippage_pct)- self.addtive_tc)/(merged_data["entry_price"]*(1+self.slippage_pct)+ self.addtive_tc)-1.0
        merged_data["return_short"] = 1.0- (merged_data["exit_price"]*(1+self.slippage_pct)+ self.addtive_tc)/(merged_data["entry_price"]*(1-self.slippage_pct)-self.addtive_tc)

        self.data = merged_data
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"slippage_pct": 0.001, "addtive_tc": 0.01}

class CalculateRawEntryPriceOnIntervals(CalibrationTaskflowTask):
    '''

    Calculates raw entry price (unadjusted using the adjustment factors) at the beginning of intervals

    '''

    PROVIDES_FIELDS = ["names_performance_over_intervals_with_raw_prices"]
    REQUIRES_FIELDS = ["adjustment_factor_data", "daily_price_data", "names_performance_over_intervals"]

    def __init__(self, use_previous_close_as_entry=False):
        self.data = None
        self.use_previous_close_as_entry = use_previous_close_as_entry
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        adjustment_factor_data = kwargs["adjustment_factor_data"].copy(deep=True)
        names_performance_over_intervals = kwargs["names_performance_over_intervals"]
        daily_price_data = kwargs["daily_price_data"]
        adjustment_factor_data["date"] = pd.DatetimeIndex(adjustment_factor_data["date"]).normalize()
        if not self.use_previous_close_as_entry:
            adjustment_factor_data.rename(columns={"date": "entry_date"}, inplace=True)
            merged_data = pd.merge(names_performance_over_intervals, adjustment_factor_data, how="left", on=["entry_date", "ticker"])
            merged_data["raw_entry_price"] = merged_data["entry_price"]*merged_data["split_div_factor"]
            self.data = merged_data
        else:
            data_to_use = daily_price_data[["close", "ticker", "date"]]
            merged_data = pd.merge(data_to_use, adjustment_factor_data, how="left", on=["date", "ticker"])
            merged_data["raw_entry_price"] = merged_data["close"]*merged_data["split_div_factor"]
            merged_data = merged_data[["ticker", "date", "raw_entry_price"]]
            merged_shifted_data = merged_data.sort_values(["ticker", "date"]).set_index("date").groupby("ticker")\
                .apply(lambda x:x.shift(1)).reset_index().rename(columns={"date": "entry_date"})
            final_data = pd.merge(names_performance_over_intervals, merged_shifted_data, how="left",
                                  on=["entry_date", "ticker"])
            self.data = final_data
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"use_previous_close_as_entry": False}

class CalculateRawPrices(CalibrationTaskflowTask):
    '''

    Calculates raw entry price (unadjusted using the adjustment factors) at the beginning of intervals

    '''

    PROVIDES_FIELDS = ["raw_price_data"]
    REQUIRES_FIELDS = ["adjustment_factor_data", "daily_price_data"]

    def __init__(self):
        self.data = None
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        adjustment_factor_data = kwargs["adjustment_factor_data"].drop("ticker", axis=1).rename(columns={"dcm_security_id": "ticker"})
        adjustment_factor_data = adjustment_factor_data[pd.notnull(adjustment_factor_data["ticker"])].reset_index(drop=True)
        adjustment_factor_data["ticker"] = adjustment_factor_data["ticker"].astype(int)
        daily_price_data = kwargs["daily_price_data"]
        adjustment_factor_data["date"] = pd.DatetimeIndex(adjustment_factor_data["date"]).normalize()
        data_to_use = daily_price_data[["close", "ticker", "date"]]
        merged_data = pd.merge(data_to_use, adjustment_factor_data, how="left", on=["date", "ticker"])
        merged_data = merged_data.sort_values(["ticker", "date"])
        merged_data["split_div_factor"] = merged_data["split_div_factor"].fillna(1.0)
        merged_data["raw_close_price"] = merged_data["close"]*merged_data["split_div_factor"]
        merged_data = merged_data[["ticker", "date", "raw_close_price"]]
        merged_shifted_data = merged_data.sort_values(["ticker", "date"]).set_index("date").groupby("ticker")\
            .apply(lambda x:x.shift(1)).reset_index()
        merged_shifted_data = merged_shifted_data.dropna(subset=["ticker"])
        merged_shifted_data["ticker"] = merged_shifted_data["ticker"].astype(int)
        self.data = merged_shifted_data
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {}

class CreateLiquidTradeList(CalibrationTaskflowTask):
    '''

    Creates a list of tradeable tickers filtered by an average daily volume threshold and exclusion list

    '''

    PROVIDES_FIELDS = ["liquidity_list"]
    REQUIRES_FIELDS = ["dollar_volume_data", "calibration_dates"]

    # TODO: add exclusion list
    def __init__(self, adv_threshold, price_threshold, exclusion_list):
        self.data = None
        self.adv_threshold = adv_threshold or 20000000.0
        self.price_threshold = price_threshold or 8.0
        self.exclusion_list = exclusion_list or None
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        dollar_volume_data = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        dates_to_compute = list(set(kwargs[self.__class__.REQUIRES_FIELDS[1]]['date']))
        dates_to_compute.sort()
        liquidity_list = pd.DataFrame(index=dates_to_compute, columns=['tradeable_tickers'])
        for dt in dates_to_compute:
            try:
                adv = dollar_volume_data[dollar_volume_data['date']==dt][["ticker", "avg_dollar_volume"]].dropna()
                price = dollar_volume_data[dollar_volume_data['date']==dt][["ticker", "avg_price"]].dropna()
            except:
                continue
            if ((not len(adv)) or (not len(price))):
                liquidity_list.loc[dt, 'tradeable_tickers'] = []
                continue
            adv_filtered = set(adv[adv["avg_dollar_volume"]>=self.adv_threshold]["ticker"])
            price_filtered = set(price[price["avg_price"]>=self.price_threshold]["ticker"])
            liquid_names = list(adv_filtered.intersection(price_filtered))
            if self.exclusion_list:
                liquid_names = list(set(liquid_names) - set(self.exclusion_list))
            liquid_names.sort()
            liquidity_list.loc[dt, 'tradeable_tickers'] = liquid_names
        self.data = liquidity_list.shift().reset_index().rename(columns={'index': 'date'})
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"adv_threshold": 20000000.0, "price_threshold": 8.0, "exclusion_list": None}

class CalculateVolatility(CalibrationTaskflowTask):
    '''

    Calculates annualized return volatility based on daily prices given a lookback window

    '''

    PROVIDES_FIELDS = ["volatility_data"]
    REQUIRES_FIELDS = ["daily_price_data"]
    def __init__(self, volatility_lookback, price_column):
        self.data = None
        self.volatility_lookback = volatility_lookback or 63
        self.price_column = price_column or "close"
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        # TODO: currently this step does not work. Change to return space. Also, need to pivot data
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        shift_value = 0  if self.price_column == "open" else 1
        column_name = "volatility_{0}".format(self.volatility_lookback)
        vol_func = lambda g: (pd.np.log(g[self.price_column]).diff().rolling(self.volatility_lookback).std())\
            .to_frame(name=column_name)
        apply_func = lambda x:vol_func(x.set_index("date").sort_index().shift(shift_value))
        self.data = daily_prices.groupby("ticker").apply(apply_func).reset_index()[["date", "ticker", column_name]]
        self.data[column_name] = self.data[column_name]*np.sqrt(252)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"volatility_lookback": 63, "price_column": "close"}

class ComputeBeta(CalibrationTaskflowTask):
    '''

    Calculates the regression beta of daily returns against a list of benchmarks

    '''

    PROVIDES_FIELDS = ["beta_data"]
    #REQUIRES_FIELDS = ["daily_price_data", "calibration_dates", "daily_index_data"]
    REQUIRES_FIELDS = ["daily_price_data", "calibration_dates", "daily_index_data"]
    def __init__(self, benchmark_names, beta_lookback, offset_unit, price_column, dropna_pctg, use_robust, epsilon,
                 alpha, fit_intercept):
        self.data = None
        self.benchmark_names = benchmark_names or [8554] #["SPY"] # This is overridden for now
        self.beta_lookback = beta_lookback or 63
        self.beta_offset = "{0}{1}".format(self.beta_lookback, offset_unit)
        self.price_column = price_column or "close"
        self.dropna_pctg = dropna_pctg or 0.15
        #TODO: incorporate the robust method at a later date, currently it changes the output format
        if use_robust:
            raise ValueError("Currently robust regrssion is not supported. use_robust must be False.")
        self.use_robust = use_robust or False
        self.epsilon = epsilon or 1.35
        self.alpha = alpha or 0.0001
        self.fit_intercept = fit_intercept or False
        self.required_records = int(self.beta_lookback*(1-self.dropna_pctg))
        self.dates_to_compute = None
        CalibrationTaskflowTask.__init__(self)

    def _regression_for_all_dates(self, indep_var, depend_vars):
        betas = pd.DataFrame(index=indep_var.index, columns=depend_vars.columns)
        for dt in self.dates_to_compute:
            start_date = dt - pd.tseries.frequencies.to_offset(self.beta_offset)
            X = indep_var.ix[start_date:dt]
            y_mat = depend_vars.ix[start_date:dt].dropna(thresh=self.required_records, axis=1)
            result_df = get_beta(X, y_mat, lookback=None, take_return=False, return_type='log',
                                 fit_intercept=self.fit_intercept, use_robust_method=self.use_robust, epsilon=self.epsilon,
                                 regularization=self.alpha).drop('intercept', axis=1).T
            betas.loc[dt, result_df.columns] = result_df.loc[result_df.index[0], result_df.columns]
        return betas

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        intervals = kwargs[self.__class__.REQUIRES_FIELDS[1]]
        if "date" in intervals:
            self.dates_to_compute = list(set(intervals["date"]))
        else:
            self.dates_to_compute = list(set(intervals["entry_date"]))
        #self.dates_to_compute = list(set(kwargs[self.__class__.REQUIRES_FIELDS[1]]['date']))
        self.dates_to_compute.sort()
        pivot_data = daily_prices[["date", "ticker", self.price_column]] \
            .pivot_table(index="date", values = self.price_column, columns="ticker", dropna=False).sort_index()
        return_data = pd.np.log(pivot_data).diff()
        return_data.index = return_data.index.normalize()
        benchmark_df = return_data[self.benchmark_names]
        if self.use_robust:
            betas = {"{0}_beta".format(i): (self._regression_for_all_dates(benchmark_df.loc[:, i], return_data))
                     for i in benchmark_df.columns}
        else:
            denom = return_data.rolling(min_periods=self.beta_lookback, window=self.beta_lookback, center=False).var()
            betas = {"{0}_beta".format(i): (return_data.rolling(window=self.beta_lookback)\
                                            .cov(other=benchmark_df.loc[:, i], pairwise=self.beta_lookback)/denom)
                     for i in benchmark_df.columns}
        betas = {"{0}".format(benchmark) : betas[benchmark].shift() for benchmark in betas}
        rank_betas = {"{0}_rank".format(i): betas[i].rank(axis=1) for i in betas}
        betas.update(rank_betas)
        all_beta_data = []
        for concept in betas:
            this_data = pd.melt(betas[concept].reset_index(), var_name="ticker",
                                id_vars="date",value_name=concept).set_index(["date", "ticker"])
            all_beta_data.append(this_data)
        result = pd.concat(all_beta_data, axis=1)
        self.data = result.sort_index(axis=1)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"benchmark_names": ["SPY", "VWO", "TLT", "GSG", "GLD"], "beta_lookback": 63,
                "offset_unit": "B", "price_column": "close", "dropna_pctg": 0.15,
                "use_robust": False, "epsilon": 1.35, "alpha": 0.0001, "fit_intercept": False}

class ComputeBetaQuantamental(CalibrationTaskflowTask):
    '''

    Calculates the regression beta of daily returns against a list of benchmarks

    '''

    PROVIDES_FIELDS = ["beta_data"]
    REQUIRES_FIELDS = ["daily_price_data", "intervals_data"]
    def __init__(self, benchmark_names, beta_lookback, offset_unit, price_column, dropna_pctg, use_robust, epsilon,
                 alpha, fit_intercept):
        self.data = None
        self.benchmark_names = benchmark_names or [8554] #["SPY"] # This is overridden for now
        self.beta_lookback = beta_lookback or 63
        self.beta_offset = "{0}{1}".format(self.beta_lookback, offset_unit)
        self.price_column = price_column or "close"
        self.dropna_pctg = dropna_pctg or 0.15
        #TODO: incorporate the robust method at a later date, currently it changes the output format
        if use_robust:
            raise ValueError("Currently robust regrssion is not supported. use_robust must be False.")
        self.use_robust = use_robust or False
        self.epsilon = epsilon or 1.35
        self.alpha = alpha or 0.0001
        self.fit_intercept = fit_intercept or False
        self.required_records = int(self.beta_lookback*(1-self.dropna_pctg))
        self.dates_to_compute = None
        CalibrationTaskflowTask.__init__(self)

    def _regression_for_all_dates(self, indep_var, depend_vars):
        betas = pd.DataFrame(index=indep_var.index, columns=depend_vars.columns)
        for dt in self.dates_to_compute:
            start_date = dt - pd.tseries.frequencies.to_offset(self.beta_offset)
            X = indep_var.ix[start_date:dt]
            y_mat = depend_vars.ix[start_date:dt].dropna(thresh=self.required_records, axis=1)
            result_df = get_beta(X, y_mat, lookback=None, take_return=False, return_type='log',
                                 fit_intercept=self.fit_intercept, use_robust_method=self.use_robust, epsilon=self.epsilon,
                                 regularization=self.alpha).drop('intercept', axis=1).T
            betas.loc[dt, result_df.columns] = result_df.loc[result_df.index[0], result_df.columns]
        return betas

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        intervals = kwargs[self.__class__.REQUIRES_FIELDS[1]]
        if "date" in intervals:
            self.dates_to_compute = list(set(intervals["date"]))
        else:
            self.dates_to_compute = list(set(intervals["entry_date"]))
        #self.dates_to_compute = list(set(kwargs[self.__class__.REQUIRES_FIELDS[1]]['date']))
        self.dates_to_compute.sort()
        pivot_data = daily_prices[["date", "ticker", self.price_column]] \
            .pivot_table(index="date", values = self.price_column, columns="ticker", dropna=False).sort_index()
        return_data = pd.np.log(pivot_data).diff()
        return_data.index = return_data.index.normalize()
        benchmark_df = return_data[self.benchmark_names]
        if self.use_robust:
            betas = {"{0}_beta".format(i): (self._regression_for_all_dates(benchmark_df.loc[:, i], return_data))
                     for i in benchmark_df.columns}
        else:
            denom = return_data.rolling(min_periods=self.beta_lookback, window=self.beta_lookback, center=False).var()
            betas = {"{0}_beta".format(i): (return_data.rolling(window=self.beta_lookback)\
                                            .cov(other=benchmark_df.loc[:, i], pairwise=self.beta_lookback)/denom)
                     for i in benchmark_df.columns}
        betas = {"{0}".format(benchmark) : betas[benchmark].shift() for benchmark in betas}
        rank_betas = {"{0}_rank".format(i): betas[i].rank(axis=1) for i in betas}
        betas.update(rank_betas)
        all_beta_data = []
        for concept in betas:
            this_data = pd.melt(betas[concept].reset_index(), var_name="ticker",
                                id_vars="date",value_name=concept).set_index(["date", "ticker"])
            all_beta_data.append(this_data)
        result = pd.concat(all_beta_data, axis=1)
        result = result.sort_index(axis=1)
        self.data = result.reset_index()
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"benchmark_names": ["SPY", "VWO", "TLT", "GSG", "GLD"], "beta_lookback": 63,
                "offset_unit": "B", "price_column": "close", "dropna_pctg": 0.15,
                "use_robust": False, "epsilon": 1.35, "alpha": 0.0001, "fit_intercept": False}


class ComputeIdioVol(CalibrationTaskflowTask):
    '''

    Calculates the regression beta of daily returns against a list of benchmarks as well as the idio vols

    '''

    PROVIDES_FIELDS = ["beta_data", "idiovol_data"]
    REQUIRES_FIELDS = ["daily_price_data", "intervals_data"]
    def __init__(self, benchmark_names, beta_lookback, offset_unit, price_column, dropna_pctg, fit_intercept):
        self.data = None
        self.idiovol_data = None
        self.benchmark_names = benchmark_names or ["SPY"]
        self.beta_lookback = beta_lookback or 63
        self.beta_offset = "{0}{1}".format(self.beta_lookback, offset_unit)
        self.price_column = price_column or "close"
        self.dropna_pctg = dropna_pctg or 0.15
        self.fit_intercept = fit_intercept or False
        self.required_records = int(self.beta_lookback*(1-self.dropna_pctg))
        self.dates_to_compute = None
        CalibrationTaskflowTask.__init__(self)

    def _regression_for_all_dates(self, indep_var, depend_vars):
        betas = pd.DataFrame(index=indep_var.index, columns=depend_vars.columns)
        idio_vols = pd.DataFrame(index=indep_var.index, columns=depend_vars.columns)
        for dt in self.dates_to_compute:
            start_date = dt - pd.tseries.frequencies.to_offset(self.beta_offset)
            X = indep_var.ix[start_date:dt]
            y_mat = depend_vars.ix[start_date:dt].dropna(thresh=self.required_records, axis=1)
            y_mat = y_mat.fillna(value=0.0)
            result_df = get_beta(X, y_mat, lookback=None, take_return=False, return_type='log',
                                 fit_intercept=self.fit_intercept, use_robust_method=False, epsilon=1.35,
                                 regularization=0.0001).drop('intercept', axis=1).T
            betas.loc[dt, result_df.columns] = result_df.loc[result_df.index[0], result_df.columns]
            residuals = y_mat.values - np.matmul(X.to_frame().values, result_df.values)
            residuals = pd.DataFrame(residuals, columns=y_mat.columns)
            idiovol = residuals.std()
            idio_vols.loc[dt, idiovol.index] = idiovol
        return betas, idio_vols

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        self.dates_to_compute = list(set(kwargs[self.__class__.REQUIRES_FIELDS[1]]['entry_date']))
        self.dates_to_compute.sort()
        pivot_data = daily_prices[["date", "ticker", self.price_column]] \
            .pivot_table(index="date", values = self.price_column, columns="ticker", dropna=False).sort_index()
        return_data = pd.np.log(pivot_data).diff()
        return_data.index = return_data.index.normalize()
        benchmark_df = return_data[self.benchmark_names]
        betas = {"{0}_beta".format(i): (self._regression_for_all_dates(benchmark_df.loc[:, i], return_data))
                 for i in benchmark_df.columns}
        betas = {"{0}".format(benchmark) : (betas[benchmark][0].shift(), betas[benchmark][1].shift()) for benchmark in betas}
        all_beta_data = []
        all_idiovol_data = []
        for concept in betas:
            this_data = pd.melt(betas[concept][0].reset_index(), var_name="ticker",
                                id_vars="date",value_name=concept).set_index(["date", "ticker"])
            all_beta_data.append(this_data)
            this_data = pd.melt(betas[concept][1].reset_index(), var_name="ticker",
                                id_vars="date",value_name=concept).set_index(["date", "ticker"])
            all_idiovol_data.append(this_data)

        result = pd.concat(all_beta_data, axis=1)
        idiovol_data = pd.concat(all_idiovol_data, axis=1)
        self.data = result.sort_index(axis=1)
        idiovol_data.columns = list(map(lambda x:x.replace("beta", "idiovol"), idiovol_data.columns))
        self.idiovol_data = idiovol_data.sort_index(axis=1).reset_index()
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.idiovol_data}

    @classmethod
    def get_default_config(cls):
        return {"benchmark_names": ["SPY"], "beta_lookback": 63, "offset_unit": "B", "price_column": "close",
                "dropna_pctg": 0.15, "fit_intercept": False}

class CalculatePCACoeff(CalibrationTaskflowTask):
    '''

    Performs PCA and returns the daily return loadings of each individual ticker to the principal components

    '''

    PROVIDES_FIELDS = ["pca_data"]
    REQUIRES_FIELDS = ["daily_price_data", "calibration_dates", "etf_information"]
    def __init__(self, n_components, lookback, offset_unit, price_column, dropna_pctg, method, normalize):
        self.data = None
        self.n_components = n_components or 5
        self.lookback = lookback or 126
        self.offset_unit = offset_unit or "B"
        self.offset = "{0}{1}".format(self.lookback, self.offset_unit)
        self.price_column = price_column or "close"
        self.dropna_pctg = dropna_pctg or 0.15
        self.normalize = normalize or True
        self.required_records = int(self.lookback*(1-self.dropna_pctg))
        self.method = method or 'auto'
        self.dates_to_compute = None
        self.stock_list = None
        CalibrationTaskflowTask.__init__(self)

    def _pca_coeff(self, all_returns):
        column_names = list(map(lambda x:"PC_{0}_beta".format(x), range(1, self.n_components+1)))
        betas = {k:pd.DataFrame(index=all_returns.index, columns=all_returns.columns) for k in column_names}
        for dt in self.dates_to_compute:
            start_date = dt - pd.tseries.frequencies.to_offset(self.offset)
            current_data = all_returns.loc[start_date:dt]
            return_data = current_data.dropna(thresh=self.required_records, axis=1).fillna(value=0.0)
            stock_returns = return_data[return_data.columns[return_data.columns.isin(self.stock_list)]]
            _, coeffs = get_pca(stock_returns, return_data, n_components=self.n_components, method=self.method,
                                take_return=False, lookback=None, normalize=True, demean=True)
            for pc in betas.keys():
                betas[pc].loc[dt, list(coeffs.index)] = coeffs[pc]
        return betas

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        self.dates_to_compute = list(set(kwargs[self.__class__.REQUIRES_FIELDS[1]]['date']))
        self.dates_to_compute.sort()
        pivot_data = daily_prices[["date", "ticker", self.price_column]] \
            .pivot_table(index="date", values = self.price_column, columns="ticker", dropna=False).sort_index()
        return_data = pd.np.log(pivot_data).diff()
        return_data.index = return_data.index.normalize()
        etf_information = kwargs[self.__class__.REQUIRES_FIELDS[2]]
        self.stock_list = list(etf_information[np.logical_not(etf_information['is_etf'])]['ticker'])
        betas = self._pca_coeff(return_data)
        betas = {"{0}".format(benchmark) : betas[benchmark].shift() for benchmark in betas}
        rank_betas = {"{0}_rank".format(i): betas[i].rank(axis=1) for i in betas}
        betas.update(rank_betas)
        all_beta_data = []
        for concept in betas:
            this_data = pd.melt(betas[concept].reset_index(), var_name="ticker",
                                id_vars="date",value_name=concept).set_index(["date", "ticker"])
            all_beta_data.append(this_data)
        result = pd.concat(all_beta_data, axis=1)
        self.data = result.sort_index(axis=1)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"n_components": 5, "lookback": 252, "offset_unit": "B", "price_column": "close",
                "normalize": True, "dropna_pctg": 0.15, "method": "randomized"}

class CalculateStochasticIndicator(CalibrationTaskflowTask):
    '''

    Calculates the Stochastic Oscillator (K, D) for tickers based on daily prices

    '''
    PROVIDES_FIELDS = ["stochastic_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator, technical_indicator_params, smoothing_period, smoothing_period_2,
                 price_column):
        self.data = None
        self.technical_indicator = globals()[technical_indicator]
        self.technical_indicator_params = technical_indicator_params
        self.smoothing_period = smoothing_period
        self.smoothing_period_2 = smoothing_period_2
        self.price_column = price_column or "close"
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        shift_value = 0  if self.price_column == "open" else 1

        indicator_func = lambda x: self.technical_indicator(x[self.price_column].sort_index(),
                                                            **self.technical_indicator_params).shift(shift_value)\
            .to_frame(name="stoch_k")
        apply_func = lambda x:indicator_func(x.set_index("date"))
        indicator_signal = daily_prices[["date","ticker", self.price_column]].groupby("ticker").apply(apply_func)\
            .reset_index()
        indicator_signal["stoch_d"] = indicator_signal.groupby(["ticker"])["stoch_k"]\
            .apply(lambda x : MA(x, self.smoothing_period, type='simple'))
        indicator_signal["slow_stoch_d"] = indicator_signal.groupby(["ticker"])["stoch_d"]\
            .apply(lambda x : MA(x, self.smoothing_period_2, type='simple'))
        indicator_signal["stoch_k_d"] = indicator_signal["stoch_k"] - indicator_signal["stoch_d"]
        self.data = indicator_signal.set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator": "stoch_k", "technical_indicator_params": {"offset": 14},
                "smoothing_period": 3, "smoothing_period_2": 3, "price_column": "close"}


class CalculateStochasticIndicatorEWMA(CalibrationTaskflowTask):
    '''

    Calculates the EWMA smoothed versions of the Stochastic Oscillator based on daily prices

    '''

    PROVIDES_FIELDS = ["stochastic_ewma_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator, technical_indicator_params, smoothing_period, smoothing_period_2,
                 price_column):
        self.data = None
        self.technical_indicator = globals()[technical_indicator]
        self.technical_indicator_params = technical_indicator_params
        self.smoothing_period = smoothing_period
        self.smoothing_period_2 = smoothing_period_2
        self.price_column = price_column or "close"
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        shift_value = 0  if self.price_column == "open" else 1

        indicator_func = lambda x: self.technical_indicator(x[self.price_column].sort_index(),
                                                            **self.technical_indicator_params).shift(shift_value)\
            .to_frame(name="stoch_k_ewma")
        apply_func = lambda x:indicator_func(x.set_index("date"))
        indicator_signal = daily_prices[["date","ticker", self.price_column]].groupby("ticker").apply(apply_func)\
            .reset_index()
        indicator_signal["stoch_d_ewma"] = indicator_signal.groupby(["ticker"])["stoch_k_ewma"]\
            .apply(lambda x : MA(x, self.smoothing_period, type='ewma'))
        indicator_signal["slow_stoch_d_ewma"] = indicator_signal.groupby(["ticker"])["stoch_d_ewma"]\
            .apply(lambda x : MA(x, self.smoothing_period_2, type='ewma'))
        indicator_signal["stoch_k_d_ewma"] = indicator_signal["stoch_k_ewma"] - indicator_signal["stoch_d_ewma"]
        self.data = indicator_signal.set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator": "stoch_k", "technical_indicator_params": {"offset": 14},
                "smoothing_period": 3, "smoothing_period_2": 3, "price_column": "high"}


class CalculateRSI(CalibrationTaskflowTask):
    '''

    Calculates the RSI indicator based on daily prices

    '''

    PROVIDES_FIELDS = ["rsi_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator, technical_indicator_params, smoothing_period, price_column):
        self.data = None
        self.technical_indicator = globals()[technical_indicator]
        self.technical_indicator_params = technical_indicator_params
        self.smoothing_period = smoothing_period
        self.price_column = price_column or "close"
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        shift_value = 0  if self.price_column == "open" else 1

        indicator_func = lambda x: self.technical_indicator(x[self.price_column].sort_index(),
                                                            **self.technical_indicator_params).shift(shift_value)\
            .to_frame(name="rsi")
        apply_func = lambda x:indicator_func(x.set_index("date"))
        indicator_signal = daily_prices[["date","ticker", self.price_column]].groupby("ticker").apply(apply_func)\
            .reset_index()
        indicator_signal["rsi_ma"] = indicator_signal.groupby(["ticker"])["rsi"]\
            .apply(lambda x : MA(x, self.smoothing_period, type='simple'))
        self.data = indicator_signal.set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator": "rsi", "technical_indicator_params": {"window": 14},
                "smoothing_period": 3, "price_column": "close"}

class CalculateMA(CalibrationTaskflowTask):
    '''

    Calculates moving average of price/level data based on daily data

    '''

    PROVIDES_FIELDS = ["moving_average_data"]
    REQUIRES_FIELDS = ["derived_cboe_index_data"]

    def __init__(self, technical_indicator, technical_indicator_params, price_column):
        self.data = None
        self.technical_indicator = MA
        self.technical_indicator_params = technical_indicator_params
        self.price_column = price_column or "close"
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        shift_value = 0  if self.price_column == "open" else 1

        price = daily_prices#.pivot(index='date', columns='ticker', values=self.price_column).sort_index().shift(shift_value)

        indicator_signal = price.apply(self.technical_indicator, **self.technical_indicator_params)
        indicator_signal = indicator_signal.stack().reset_index()
        indicator_signal.columns = ['date', 'ticker', 'moving_average']

        self.data = indicator_signal.set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator": MA, "technical_indicator_params": "technical_indicator_params",
                "price_column": "close"}

class CalculateTaLibRSI(CalibrationTaskflowTask):
    '''

    Calculates the talib version of RSI indicator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_rsi_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator, technical_indicator_params, smoothing_period, price_column):
        self.data = None
        self.technical_indicator = talib.RSI
        self.technical_indicator_params = technical_indicator_params or {"timeperiod": 14}
        self.smoothing_period = smoothing_period
        self.price_column = price_column or "close"
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 0  if self.price_column == "open" else 1

        price = daily_prices.pivot_table(index='date', columns='ticker', values=self.price_column)\
            .sort_index().shift(shift_value)

        column_name = 'RSI_{0}'.format(self.technical_indicator_params["timeperiod"])
        ma_column_name = '{0}_MA_{1}'.format(column_name, self.smoothing_period)
        indicator_signal = price.apply(self.technical_indicator, **self.technical_indicator_params)
        indicator_signal = indicator_signal.stack().reset_index().rename(columns={0: column_name})

        indicator_signal[ma_column_name] = indicator_signal.groupby(["ticker"])[column_name]\
            .apply(lambda x : MA(x, self.smoothing_period, type='simple'))
        self.data = indicator_signal#.set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator": talib.RSI, "technical_indicator_params": {"timeperiod": 14},
                "smoothing_period": 3, "price_column": "close"}

class CalculateTaLibADX(CalibrationTaskflowTask):
    '''

    Calculates the talib version of ADX indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_adx_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator, technical_indicator_params, smoothing_period):
        self.data = None
        self.technical_indicator = talib.ADX
        self.technical_indicator_params = technical_indicator_params or {"timeperiod": 14}
        self.smoothing_period = smoothing_period or 3
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 1

        # TODO: Check whether indiscriminantly rounding is a good idea
        high = daily_prices.pivot_table(index='date', columns='ticker', values='high', dropna=False).sort_index().shift(shift_value).round(2)
        low = daily_prices.pivot_table(index='date', columns='ticker', values='low', dropna=False).sort_index().shift(shift_value).round(2)
        close = daily_prices.pivot_table(index='date', columns='ticker', values='close', dropna=False).sort_index().shift(shift_value).round(2)

        column_name = 'ADX_{0}'.format(self.technical_indicator_params["timeperiod"])
        ma_column_name = '{0}_MA_{1}'.format(column_name, self.smoothing_period)

        indicator_generator = (self.technical_indicator(high[c], low[c], close[c], **self.technical_indicator_params) for c in high.columns)
        indicator_signal = pd.concat(indicator_generator, axis=1, keys=high.columns)
        indicator_signal = indicator_signal.stack().reset_index().rename(columns={0: column_name})

        indicator_signal[ma_column_name] = indicator_signal.groupby(["ticker"])[column_name]\
            .apply(lambda x : MA(x, self.smoothing_period, type='simple'))
        self.data = indicator_signal#.set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator": talib.ADX, "technical_indicator_params": {"timeperiod": 14},
                "smoothing_period": 3}

class CalculateFracDiff(CalibrationTaskflowTask):
    '''

    Calculates the factional differencing returns on price/level data

    '''

    PROVIDES_FIELDS = ["frac_diff_data"]
    REQUIRES_FIELDS = ["daily_index_data"]

    def __init__(self, technical_indicator, technical_indicator_params, price_column):
        self.data = None
        self.technical_indicator = fracDiff_FFD
        self.technical_indicator_params = technical_indicator_params or {"differencing":0.6, "threshold":0.001}
        self.price_column = price_column or "close"
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_index = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        shift_value = 0  if self.price_column == "open" else 1

        daily_index = daily_index.sort_values(['symbol', 'date', 'as_of_start'])
        daily_index = daily_index.groupby(['symbol', 'date'], as_index=False).last()
        price = daily_index.pivot_table(index='date', columns='symbol', values=self.price_column, dropna=False)\
            .sort_index().shift(shift_value)

        indicator_generator = (self.technical_indicator(pd.np.log(price[c]), **self.technical_indicator_params) for c in price.columns)
        indicator_signal = pd.concat(indicator_generator, axis=1)
        indicator_signal = indicator_signal.stack().reset_index()
        column_name = 'FRAC_DIFF_{0}'.format(self.technical_indicator_params["differencing"])
        indicator_signal.columns = ['date', 'ticker', column_name]

        self.data = indicator_signal#.set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator": "frac_diff", "technical_indicator_params": {"differencing":0.6, "threshold":0.001},
                "price_column": "close"}

class CalculateTSI(CalibrationTaskflowTask):
    '''

    Calculates the tsi indicator on daily prices

    '''

    PROVIDES_FIELDS = ["tsi_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator, technical_indicator_params, smoothing_period, price_column):
        self.data = None
        self.technical_indicator = globals()[technical_indicator]
        self.technical_indicator_params = technical_indicator_params
        self.smoothing_period = smoothing_period
        self.price_column = price_column or "close"
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        shift_value = 0  if self.price_column == "open" else 1

        indicator_func = lambda x: self.technical_indicator(x[self.price_column].sort_index(),
                                                            **self.technical_indicator_params).shift(shift_value)\
            .to_frame(name="tsi")
        apply_func = lambda x:indicator_func(x.set_index("date"))
        indicator_signal = daily_prices[["date","ticker", self.price_column]].groupby("ticker").apply(apply_func)\
            .reset_index()
        indicator_signal["tsi_ma"] = indicator_signal.groupby(["ticker"])["tsi"]\
            .apply(lambda x : MA(x, self.smoothing_period, type='simple'))
        self.data = indicator_signal.set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator": "tsi", "technical_indicator_params": {"nslow":25, "nfast":13, "offset":1},
                "smoothing_period": 3, "price_column": "close"}

class CalculateMACD(CalibrationTaskflowTask):
    '''

    Calculates MACD indicator (indicator value, smoothed centerline, and difference) on daily prices

    '''

    PROVIDES_FIELDS = ["macd_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator, technical_indicator_params, smoothing_period, price_column):
        self.data = None
        self.technical_indicator = globals()[technical_indicator]
        self.technical_indicator_params = technical_indicator_params
        self.smoothing_period = smoothing_period
        self.price_column = price_column or "close"
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        shift_value = 0  if self.price_column == "open" else 1

        indicator_func = lambda x: self.technical_indicator(x[self.price_column].sort_index(),
                                                            **self.technical_indicator_params).shift(shift_value)\
            .to_frame(name="macd")
        apply_func = lambda x:indicator_func(x.set_index("date"))
        indicator_signal = daily_prices[["date","ticker", self.price_column]].groupby("ticker").apply(apply_func)\
            .reset_index()
        indicator_signal["macd_centerline"] = indicator_signal.groupby(["ticker"])["macd"]\
            .apply(lambda x : MA(x, self.smoothing_period, type='simple'))
        indicator_signal["macd_diff"] = indicator_signal["macd"] - indicator_signal["macd_centerline"]
        self.data = indicator_signal#.set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator": "macd", "technical_indicator_params": {"nslow":26, "nfast":12},
                "smoothing_period": 3, "price_column": "close"}

#TODO: check and incorporate into the interval prints instead
class CalculateSpecialIndexRatios(CalibrationTaskflowTask):
    '''

    Calculates VIX index term ratios from daily indices

    '''
    PROVIDES_FIELDS = ["index_ratio_data"]
    REQUIRES_FIELDS = ["daily_index_data"]

    def __init__(self, price_column):
        self.data = None
        self.price_column = price_column or "close"
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        shift_value = 0  if self.price_column == "open" else 1

        vxst = daily_prices[daily_prices["symbol"]=="VXST"][["date", self.price_column]].set_index("date").sort_index().shift(shift_value)[self.price_column]
        vix = daily_prices[daily_prices["symbol"]=="VIX"][["date", self.price_column]].set_index("date").sort_index().shift(shift_value)[self.price_column]
        vxv = daily_prices[daily_prices["symbol"]=="VXV"][["date", self.price_column]].set_index("date").sort_index().shift(shift_value)[self.price_column]
        vxmt = daily_prices[daily_prices["symbol"]=="VXMT"][["date", self.price_column]].set_index("date").sort_index().shift(shift_value)[self.price_column]

        for ts in [vxst, vix, vxv, vxmt]:
            ts.index = ts.index.normalize()

        vix_vxst = vix.div(vxst, fill_value=np.nan)
        vxv_vxst = vxv.div(vxst, fill_value=np.nan)
        vxmt_vxst = vxmt.div(vxst, fill_value=np.nan)
        vxv_vix = vxv.div(vix, fill_value=np.nan)
        vxmt_vix = vxmt.div(vix, fill_value=np.nan)
        vxmt_vxv = vxmt.div(vxv, fill_value=np.nan)

        data = pd.concat([vxst, vix, vxv, vxmt, vix_vxst, vxv_vxst, vxmt_vxst, vxv_vix, vxmt_vix, vxmt_vxv],
                         axis=1,
                         keys=["VXST", "VIX", "VXV", "VXMT", "VIX_VXST_ratio", "VXV_VXST_ratio", "VXMT_VXST_ratio",
                               "VXV_VIX_ratio", "VXMT_VIX_ratio", "VXMT_VXV_ratio"])

        self.data = data
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"price_column": "close"}

class CalculateLeeJumpIndicator(CalibrationTaskflowTask):
    '''

    Calculates the lee jump indicator based on overnight returns

    '''

    PROVIDES_FIELDS = ["jump_indicator_data"]
    REQUIRES_FIELDS = ["overnight_return_data"]

    def __init__(self, technical_indicator, technical_indicator_params):
        self.data = None
        self.technical_indicator = globals()[technical_indicator]
        self.technical_indicator_params = technical_indicator_params
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        overnight_return = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        shift_value = 0

        indicator_func = lambda x: self.technical_indicator(x["overnight_return"].sort_index(),
                                                            **self.technical_indicator_params).shift(shift_value)\
            .to_frame(name="lee_jump_indicator")
        apply_func = lambda x:indicator_func(x.set_index("date"))
        indicator_signal = overnight_return[["date","ticker", "overnight_return"]].groupby("ticker").apply(apply_func)\
            .reset_index()
        self.data = indicator_signal.set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator": "lee_jump_detection",
                "technical_indicator_params": {"delta_t":0.003968253968253968, "window":70, "g":1.2,
                                                   "jump_return_criteria":"either", "alpha":0.05}}

class CalculateDerivedCBOEIndexData(CalibrationTaskflowTask):
    '''

    Performs shift operations on custom CBOE index data to align dates

    '''
    PROVIDES_FIELDS = ["derived_cboe_index_data"]
    REQUIRES_FIELDS = ["daily_index_data"]

    def __init__(self, price_column):
        self.data = None
        self.price_column = price_column or "close"
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        #cboe_index_data = kwargs[self.__class__.REQUIRES_FIELDS[0]][["date", "symbol", self.price_column]]
        #cboe_index_data[self.price_column] = cboe_index_data[self.price_column].astype(float)
        daily_index_data = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        #cboe_index_data["date"] = pd.DatetimeIndex(cboe_index_data["date"]).normalize()
        daily_index_data["date"] = pd.DatetimeIndex(daily_index_data["date"]).normalize()
        #all_data = pd.concat([daily_index_data, cboe_index_data], ignore_index=True)
        shift_value = 0  if self.price_column == "open" else 1
        data = pd.pivot_table(daily_index_data, index="date", values = self.price_column, columns="symbol", dropna=False)\
            .sort_index()
        data = data.shift(shift_value)
        self.data = data
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"price_column": "close"}

class TransposeFundamentalsQuandl(CalibrationTaskflowTask):
    '''

    Provides date-aligned transposed wide table from the raw quandl long table data

    '''

    PROVIDES_FIELDS = ["derived_fundamental_data"]
    REQUIRES_FIELDS = ["fundamental_data_quandl", "intervals_data"]

    def __init__(self, concept_filter=None):
        self.concept_filter = concept_filter
        self.data = None
        CalibrationTaskflowTask.__init__(self)

    def _create_date_lookup(self, df, interval_dt_index):
        padded_index = [interval_dt_index[0]-pd.tseries.frequencies.to_offset("252B")]
        padded_index.extend(interval_dt_index)
        dt_index = pd.DatetimeIndex(padded_index)
        df = df.copy(deep=True)
        df["cob"] = df["as_of_start"]
        unique_dates = df.drop_duplicates().drop("ticker", axis=1)
        if ((len(unique_dates["as_of_start"].unique())!=len(unique_dates))
            or (len(unique_dates["cob"].unique())!=len(unique_dates))):
            raise ValueError("Jacccckkkk!!!! don't let me be!!!!")
        #TODO: do we need a limit on the ffill?? different target dates would imply different limits though
        mapping = self.__resample_series(unique_dates.set_index("as_of_start"),
                                         dt_index).fillna(method="ffill")
        return mapping.iloc[1:].dropna()

    def __resample_series(self, pandas_obj, resampling_index):
        how = "last"
        closed = "right"
        label = "right"
        grouping_col = pd.cut(pandas_obj.index.asi8.astype(long), resampling_index.asi8.astype(long), closed=="right",
                              labels=(resampling_index[1:] if label=="right" else resampling_index[0:-1])).get_values()
        result = pandas_obj.groupby(grouping_col).agg(how).reindex(resampling_index)
        result.index = result.index.astype(long)
        result.index = result.index.astype("M8[ns]").tz_localize("UTC").tz_convert(resampling_index.tzinfo)
        return result

    def do_step_action(self, **kwargs):
        long_table = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        intervals_data = kwargs[self.__class__.REQUIRES_FIELDS[1]]
        interval_dt_index = pd.DatetimeIndex(intervals_data["entry_date"])
        #TODO: this query assumes all data is published at same time. Need to generalize it
        if self.concept_filter:
            long_table = long_table[long_table["concept"].isin(self.concept_filter)]
        long_table_for_calendar_lookup = long_table[~(long_table["concept"].str.split("_").str[-1]=="ary")]
        calendardate_lookup = long_table_for_calendar_lookup[["ticker", "as_of_start", "cob"]].groupby("ticker")\
            .apply(lambda x:self._create_date_lookup(x, interval_dt_index))

        long_table = pd.merge(calendardate_lookup.reset_index().rename(columns={"cob":"as_of_start"}),
                              long_table, on=["ticker", "as_of_start"]).rename(columns={"level_1": "entry_date"})
        derived_fundamentals_temp = long_table.drop(["source", "as_of_start", "as_of_end"], axis=1)
        derived_fundamentals = pd.pivot_table(derived_fundamentals_temp, index=["ticker", "entry_date", "cob"],
                                              values='value', columns='concept', dropna=False)
        # Normalize here
        derived_fundamentals["marketcap"] = derived_fundamentals["marketcap"]/1000000.0
        derived_fundamentals = derived_fundamentals.reset_index()
        del derived_fundamentals_temp; gc.collect()
        self.data = derived_fundamentals
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"concept_filter": ["marketcap"]}

class CalculateDerivedFundamentals(CalibrationTaskflowTask):
    '''

    Calculates derived quantities (e.g. normalized yields and ratios) based on capiq fundamental data

    '''

    PROVIDES_FIELDS = ["derived_fundamental_data"]
    REQUIRES_FIELDS = ["fundamental_data"]

    def __init__(self):
        self.data = None
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        raw_fundamentals = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        raw_metrics = ["enterprise_value", "operating_cash_cq", "capex_cq", "sale_intangible_cq", "grossprofit_cq",
                       "dividend_yield", "total_dividend_cq", "pe_forward", "repurchase_commonshares_cq",
                       "netdebt_issued_cq", "dividend_pershare", "eps_diluted_cq", "total_shares_outstanding_filing",
                       "total_shares_outstanding_bs", "roi_cq", "totalasset_turnover_cq", "marketcap",
                       "total_currentassets", "total_currentliab", "total_liabilities", "longtermdebt", "netdebt",
                       "totaldebt", "total_equity", "total_assets", "diluted_sharesoutstanding_ltm",
                       "diluted_earningspershare_ltm", "revenue_ltm", "grossprofit_ltm", "netincome_ltm", "ebitda_ltm",
                       "ebit_ltm", "revenue_cq", "ebitda_cq", "capex_ltm", "grossprofit_margin_ltm",
                       "ebitda_margin_ltm", "ebit_margin_ltm", "netincome_margin_ltm", "return_on_assets_ltm",
                       "return_on_equity_ltm", "return_on_capital_ltm", "return_on_investment_capital",
                       "enterprisevalue_to_ebitda_ltm", "price_to_earningspershare_ltm", "marketcap_to_revenue_ltm",
                       "enterprisevalue_to_revenue_ltm", "price_to_cashflowpershare_ltm", "estimates_eps_growth_1yr_cq",
                       "date", "ticker", "this_quarter"]
        derived_fundamentals = raw_fundamentals[raw_metrics]
        derived_fundamentals["capex_cq"] = derived_fundamentals["capex_cq"].fillna(0.0)
        derived_fundamentals["sale_intangible_cq"] = derived_fundamentals["sale_intangible_cq"].fillna(0.0)
        derived_fundamentals["total_dividend_cq"] = derived_fundamentals["total_dividend_cq"].fillna(0.0)
        derived_fundamentals["repurchase_commonshares_cq"] = derived_fundamentals["repurchase_commonshares_cq"].fillna(0.0)
        derived_fundamentals["netdebt_issued_cq"] = derived_fundamentals["netdebt_issued_cq"].fillna(0.0)

        derived_fundamentals["free_cash_flow"] = \
            derived_fundamentals["operating_cash_cq"] + derived_fundamentals["capex_cq"] + \
            derived_fundamentals["sale_intangible_cq"]
        derived_fundamentals["free_cash_flow_yield"] = \
            derived_fundamentals["free_cash_flow"]/derived_fundamentals["marketcap"]
        derived_fundamentals["free_cash_flow_to_assets"] = \
            derived_fundamentals["free_cash_flow"]/derived_fundamentals["total_assets"]

        derived_fundamentals["cash_return"] = \
            derived_fundamentals["free_cash_flow"]/derived_fundamentals["enterprise_value"]
        derived_fundamentals["shareholder_yield"] = \
            (derived_fundamentals["total_dividend_cq"] +\
             derived_fundamentals["repurchase_commonshares_cq"] + \
             derived_fundamentals["netdebt_issued_cq"])/derived_fundamentals["marketcap"]
        derived_fundamentals["ebitda_to_ev"] = \
            derived_fundamentals["ebitda_cq"]/derived_fundamentals["enterprise_value"]
        derived_fundamentals["gross_profit_to_assets"] = \
            derived_fundamentals["grossprofit_cq"]/derived_fundamentals["total_assets"]
        derived_fundamentals["earnings_yield_ltm"] = 1.0/derived_fundamentals["price_to_earningspershare_ltm"]

        key_fundamentals = ["marketcap", "enterprise_value", "free_cash_flow", "free_cash_flow_yield", "cash_return",
                            "shareholder_yield", "ebitda_to_ev", "pe_forward", "ebitda_margin_ltm", "ebit_margin_ltm",
                            "netincome_margin_ltm", "return_on_assets_ltm", "return_on_equity_ltm",
                            "return_on_capital_ltm", "return_on_investment_capital", "roi_cq",
                            "totalasset_turnover_cq", "gross_profit_to_assets", "dividend_yield",
                            "grossprofit_margin_ltm", "earnings_yield_ltm", "estimates_eps_growth_1yr_cq",
                            "operating_cash_cq", "capex_cq", "sale_intangible_cq",
                            "total_dividend_cq", "repurchase_commonshares_cq", "netdebt_issued_cq",
                            "total_shares_outstanding_filing", "total_shares_outstanding_bs",
                            "ebitda_cq", "grossprofit_cq", "total_assets", "free_cash_flow_to_assets"]
        key_fundamentals.sort()
        key_fundamentals[:0] = ['date', 'ticker', 'this_quarter']
        self.data = derived_fundamentals[key_fundamentals].reset_index(drop=True)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

class CalculateDerivedFundamentalsQuandl(CalibrationTaskflowTask):
    '''
    Provides date-aligned transposed wide table from the raw quandl long table data along with derived quantities
    (e.g. normalized yields and ratios)

    '''
    PROVIDES_FIELDS = ["derived_fundamental_data"]
    REQUIRES_FIELDS = ["fundamental_data_quandl", "intervals_data", "daily_price_data", "adjustment_factor_data"]

    def __init__(self, concept_filter=None):
        self.concept_filter = concept_filter
        self.data = None
        CalibrationTaskflowTask.__init__(self)

    def _create_date_lookup(self, df, interval_dt_index):
        padded_index = [interval_dt_index[0]-pd.tseries.frequencies.to_offset("252B")]
        padded_index.extend(interval_dt_index)
        dt_index = pd.DatetimeIndex(padded_index)
        df = df.copy(deep=True)
        df["cob"] = df["as_of_start"]
        unique_dates = df.drop_duplicates().drop("ticker", axis=1)
        if ((len(unique_dates["as_of_start"].unique())!=len(unique_dates))
            or (len(unique_dates["cob"].unique())!=len(unique_dates))):
            raise ValueError("Jacccckkkk!!!! don't let me be!!!!")
        #TODO: do we need a limit on the ffill?? different target dates would imply different limits though
        mapping = self.__resample_series(unique_dates.set_index("as_of_start"),
                                         dt_index).fillna(method="ffill")
        return mapping.iloc[1:].dropna()

    def __resample_series(self, pandas_obj, resampling_index):
        how = "last"
        closed = "right"
        label = "right"
        grouping_col = pd.cut(pandas_obj.index.asi8.astype(np.int64), resampling_index.asi8.astype(np.int64), closed=="right",
                              labels=(resampling_index[1:] if label=="right" else resampling_index[0:-1])).get_values()
        result = pandas_obj.groupby(grouping_col).agg(how).reindex(resampling_index)
        result.index = result.index.astype(np.int64)
        result.index = result.index.astype("M8[ns]").tz_localize("UTC").tz_convert(resampling_index.tzinfo)
        return result

    def do_step_action(self, **kwargs):
        long_table = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        intervals_data = kwargs[self.__class__.REQUIRES_FIELDS[1]]
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[2]]
        adjustment_factors = kwargs[self.__class__.REQUIRES_FIELDS[3]]
        interval_dt_index = pd.DatetimeIndex(intervals_data["entry_date"])
        #TODO: this query assumes all data is published at same time. Need to generalize it
        long_table_for_calendar_lookup = long_table[~(long_table["concept"].str.split("_").str[-1]=="ary")]
        calendardate_lookup = long_table_for_calendar_lookup[["ticker", "as_of_start", "cob"]].groupby("ticker")\
            .apply(lambda x:self._create_date_lookup(x, interval_dt_index))

        long_table = pd.merge(calendardate_lookup.reset_index().rename(columns={"cob":"as_of_start"}),
                              long_table, on=["ticker", "as_of_start"]).rename(columns={"level_1": "entry_date"})
        derived_fundamentals_temp = long_table.drop(["source", "as_of_start", "as_of_end"], axis=1)
        derived_fundamentals = pd.pivot_table(derived_fundamentals_temp, index=["ticker", "entry_date", "cob"],
                                              values='value', columns='concept')
        derived_fundamentals = derived_fundamentals.reset_index()
        del derived_fundamentals_temp; gc.collect()

        pivot_data = pd.pivot_table(daily_prices[["date", "ticker", "close"]],
                                    index="date", values = "close", columns="ticker", dropna=False).sort_index()

        pivot_data = pivot_data.shift()
        pivot_data = pivot_data.reset_index().rename(columns={"date": "entry_date"})
        price_data = pd.melt(pivot_data.reset_index(), id_vars=['entry_date'], var_name="ticker", value_name="close")
        price_data["entry_date"] = pd.DatetimeIndex(price_data["entry_date"]).normalize()

        combined = pd.merge(derived_fundamentals[["entry_date", "ticker", "free_cash_flow_pershare_art"]],
                            price_data, how="left", on=["entry_date", "ticker"])
        combined["fcf_yield"] = combined["free_cash_flow_pershare_art"]/combined["close"]
        derived_fundamentals["fcf_yield"] = combined["fcf_yield"]
        derived_fundamentals["adjusted_price_basis_fundamental"] = combined["close"]

        pivot_data = pd.pivot_table(adjustment_factors[["date", "ticker", "split_factor"]],
                                    index="date", values = "split_factor", columns="ticker", dropna=False).sort_index()
        pivot_data = pivot_data.reset_index().rename(columns={"date": "entry_date"})
        split_data = pd.melt(pivot_data.reset_index(), id_vars=['entry_date'], var_name="ticker", value_name="split_factor")
        split_data["entry_date"] = pd.DatetimeIndex(split_data["entry_date"]).normalize()
        combined = pd.merge(combined, split_data, how="left", on=["entry_date", "ticker"])
        combined["fcfps_art_unadjusted"] = combined["free_cash_flow_pershare_art"]*combined["split_factor"]
        derived_fundamentals["fcfps_art_unadjusted"] = combined["fcfps_art_unadjusted"]

        pivot_data = pd.pivot_table(adjustment_factors[["date", "ticker", "split_div_factor"]],
                                    index="date", values = "split_div_factor", columns="ticker", dropna=False).sort_index()
        pivot_data = pivot_data.shift()
        pivot_data = pivot_data.reset_index().rename(columns={"date": "entry_date"})
        split_data = pd.melt(pivot_data.reset_index(), id_vars=['entry_date'], var_name="ticker", value_name="split_div_factor")
        split_data["entry_date"] = pd.DatetimeIndex(split_data["entry_date"]).normalize()
        combined = pd.merge(combined, split_data, how="left", on=["entry_date", "ticker"])
        combined["raw_price_basis_fundamental"] = combined["close"]*combined["split_div_factor"]
        derived_fundamentals["raw_price_basis_fundamental"] = combined["raw_price_basis_fundamental"]
        derived_fundamentals["fcfy_test"] = combined["fcfps_art_unadjusted"]/combined["raw_price_basis_fundamental"]

        derived_fundamentals["capex_arq"] = derived_fundamentals["capex_arq"].fillna(0.0)
        derived_fundamentals["dividend_cash_arq"] = derived_fundamentals["dividend_cash_arq"].fillna(0.0)
        derived_fundamentals["share_repurchase_arq"] = derived_fundamentals["share_repurchase_arq"].fillna(0.0)
        derived_fundamentals["debt_issuance_arq"] = derived_fundamentals["debt_issuance_arq"].fillna(0.0)
        derived_fundamentals["capex_art"] = derived_fundamentals["capex_art"].fillna(0.0)
        derived_fundamentals["dividend_cash_art"] = derived_fundamentals["dividend_cash_art"].fillna(0.0)
        derived_fundamentals["share_repurchase_art"] = derived_fundamentals["share_repurchase_art"].fillna(0.0)
        derived_fundamentals["debt_issuance_art"] = derived_fundamentals["debt_issuance_art"].fillna(0.0)

        derived_fundamentals["free_cash_flow_yield_arq"] = \
            derived_fundamentals["free_cash_flow_arq"]/derived_fundamentals["marketcap"]
        derived_fundamentals["free_cash_flow_to_assets_arq"] = \
            derived_fundamentals["free_cash_flow_arq"]/derived_fundamentals["total_assets_arq"]
        derived_fundamentals["free_cash_flow_yield_art"] = \
            derived_fundamentals["free_cash_flow_art"]/derived_fundamentals["marketcap"]
        derived_fundamentals["free_cash_flow_to_assets_art"] = \
            derived_fundamentals["free_cash_flow_art"]/derived_fundamentals["total_assets_art"]

        derived_fundamentals["cash_return_arq"] = \
            derived_fundamentals["free_cash_flow_arq"]/derived_fundamentals["enterprise_value_arq"]
        derived_fundamentals["shareholder_yield_arq"] = \
            (derived_fundamentals["dividend_cash_arq"] +\
             derived_fundamentals["share_repurchase_arq"] + \
             derived_fundamentals["debt_issuance_arq"])/derived_fundamentals["marketcap"]
        derived_fundamentals["cash_return_art"] = \
            derived_fundamentals["free_cash_flow_art"]/derived_fundamentals["enterprise_value_art"]
        derived_fundamentals["shareholder_yield_art"] = \
            (derived_fundamentals["dividend_cash_art"] +\
             derived_fundamentals["share_repurchase_art"] + \
             derived_fundamentals["debt_issuance_art"])/derived_fundamentals["marketcap"]

        derived_fundamentals["ebitda_to_ev_arq"] = \
            derived_fundamentals["ebitda_arq"]/derived_fundamentals["enterprise_value_arq"]
        derived_fundamentals["gross_profit_to_assets_arq"] = \
            derived_fundamentals["gross_profit_arq"]/derived_fundamentals["total_assets_arq"]
        derived_fundamentals["debt_to_assets_arq"] = \
            derived_fundamentals["total_debt_arq"]/derived_fundamentals["total_assets_arq"]
        derived_fundamentals["ebitda_to_ev_art"] = \
            derived_fundamentals["ebitda_art"]/derived_fundamentals["enterprise_value_art"]
        derived_fundamentals["gross_profit_to_assets_art"] = \
            derived_fundamentals["gross_profit_art"]/derived_fundamentals["total_assets_art"]
        derived_fundamentals["debt_to_assets_art"] = \
            derived_fundamentals["total_debt_art"]/derived_fundamentals["total_assets_art"]

        derived_fundamentals["revenue_yield_arq"] = derived_fundamentals["revenue_arq"]/derived_fundamentals["marketcap"]
        derived_fundamentals["total_asset_yield_arq"] = \
            derived_fundamentals["total_assets_arq"]/derived_fundamentals["marketcap"]
        derived_fundamentals["total_debt_yield_arq"] = \
            derived_fundamentals["total_debt_arq"]/derived_fundamentals["marketcap"]
        derived_fundamentals["debt_issuance_yield_arq"] = \
            derived_fundamentals["debt_issuance_arq"]/derived_fundamentals["marketcap"]
        derived_fundamentals["share_repurchase_yield_arq"] = \
            derived_fundamentals["share_repurchase_arq"]/derived_fundamentals["marketcap"]
        derived_fundamentals["net_cash_flow_yield_arq"] = \
            derived_fundamentals["net_cash_flow_arq"]/derived_fundamentals["marketcap"]
        derived_fundamentals["net_operating_cash_flow_yield_arq"] = \
            derived_fundamentals["net_operating_cash_flow_arq"]/derived_fundamentals["marketcap"]

        derived_fundamentals["revenue_yield_art"] = derived_fundamentals["revenue_art"]/derived_fundamentals["marketcap"]
        derived_fundamentals["total_asset_yield_art"] = \
            derived_fundamentals["total_assets_art"]/derived_fundamentals["marketcap"]
        derived_fundamentals["total_debt_yield_art"] = \
            derived_fundamentals["total_debt_art"]/derived_fundamentals["marketcap"]
        derived_fundamentals["debt_issuance_yield_art"] = \
            derived_fundamentals["debt_issuance_art"]/derived_fundamentals["marketcap"]
        derived_fundamentals["share_repurchase_yield_art"] = \
            derived_fundamentals["share_repurchase_art"]/derived_fundamentals["marketcap"]
        derived_fundamentals["net_cash_flow_yield_art"] = \
            derived_fundamentals["net_cash_flow_art"]/derived_fundamentals["marketcap"]
        derived_fundamentals["net_operating_cash_flow_yield_art"] = \
            derived_fundamentals["net_operating_cash_flow_art"]/derived_fundamentals["marketcap"]


        # New additions
        derived_fundamentals["inventory_ratio_art"] = \
            derived_fundamentals["inventory_art"]/derived_fundamentals["revenue_art"]
        derived_fundamentals["inventory_ratio_arq"] = \
            derived_fundamentals["inventory_arq"]/derived_fundamentals["revenue_arq"]
        derived_fundamentals["gearing_ratio_art"] = \
            derived_fundamentals["total_assets_art"]/derived_fundamentals["shareholder_equity_ary"]
        derived_fundamentals["gearing_ratio_arq"] = \
            derived_fundamentals["total_assets_arq"]/derived_fundamentals["shareholder_equity_arq"]
        derived_fundamentals["fcf_to_tangible_assets_art"] = \
            derived_fundamentals["free_cash_flow_art"]/derived_fundamentals["tangible_asset_art"]
        derived_fundamentals["fcf_to_tangible_assets_arq"] = \
            derived_fundamentals["free_cash_flow_arq"]/derived_fundamentals["tangible_asset_arq"]

        roic_zscore = derived_fundamentals.groupby("cob")["roic_art"].apply(lambda x:(x-np.nanmean(x))/np.nanstd(x))
        roe_zscore = derived_fundamentals.groupby("cob")["return_on_equity_art"].apply(lambda x:(x-np.nanmean(x))/np.nanstd(x))
        pb_zscore = derived_fundamentals.groupby("cob")["price_to_book_arq"].apply(lambda x:(x-np.nanmean(x))/np.nanstd(x))
        gpa_zscore = derived_fundamentals.groupby("cob")["gross_profit_to_assets_arq"].apply(lambda x:(x-np.nanmean(x))/np.nanstd(x))
        fcfy_zscore = derived_fundamentals.groupby("cob")["free_cash_flow_yield_art"].apply(lambda x:(x-np.nanmean(x))/np.nanstd(x))
        cash_zscore = derived_fundamentals.groupby("cob")["cash_return_art"].apply(lambda x:(x-np.nanmean(x))/np.nanstd(x))
        sharey_zscore = derived_fundamentals.groupby("cob")["shareholder_yield_art"].apply(lambda x:(x-np.nanmean(x))/np.nanstd(x))

        derived_fundamentals["roe_roic_zcombined"] = roic_zscore + roe_zscore
        derived_fundamentals["pb_roe_art_zcombined"] = pb_zscore + roe_zscore
        derived_fundamentals["pb_gpa_zcombined"] = pb_zscore + gpa_zscore
        derived_fundamentals["fcf_cash_yield_zcombined"] = fcfy_zscore + cash_zscore
        derived_fundamentals["fcf_cash_shareholder_yield_zcombined"] = fcfy_zscore + cash_zscore + sharey_zscore
        derived_fundamentals["fcf_gpa_zcombined"] = fcfy_zscore + gpa_zscore
        derived_fundamentals["shareholder_yield_gpa_zcombined"] = sharey_zscore + gpa_zscore
        derived_fundamentals["cash_return_gpa_zcombined"] = cash_zscore + gpa_zscore

        if self.concept_filter:
            self.concept_filter.extend(["ticker", "entry_date"])
            derived_fundamentals = derived_fundamentals[self.concept_filter]
        self.data = derived_fundamentals
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"concept_filter": None}

class RavenpackNewsMomentumDaily(CalibrationTaskflowTask):
    '''

    Converts intraday news momentum to daily

    '''

    PROVIDES_FIELDS = ["ravenpack_news_momentum_daily"]
    REQUIRES_FIELDS = ["ravenpack_news_momentum", "daily_price_data"]
    def __init__(self, cutoff_time = "15:00"):
        self.data = None
        self.cutoff_time = cutoff_time
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        news = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[1]]
        news['date'] = news['time_interval_est'].dt.normalize().dt.tz_localize(None)
        news.loc[news['time_interval_est'].dt.time >= pd.Timestamp(self.cutoff_time).time(), 'date'] += BDay()
        news.loc[news['date'].dt.dayofweek == 6, 'date'] += BDay()
        news.loc[news['date'].dt.dayofweek == 5, 'date'] += BDay()
        news['ess'] -= 50
        self.data = news.groupby(['date', 'ticker', 'rp_entity_id', 'rp_group'], as_index=False).agg({'ret': 'sum', 'ess': 'mean'})
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"cutoff_time": "15:00"}

class CalculateTaLibPPO(CalibrationTaskflowTask):
    '''

    Calculates the talib version of PPO indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_ppo_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator_params, price_column, invert_sign):
        self.data = None
        self.technical_indicator = talib_PPO
        self.technical_indicator_params = technical_indicator_params or {"fastperiod": 12, "slowperiod": 26,
                                                                         "matype": 0}
        self.price_column = price_column or "close"
        self.invert_sign = invert_sign or True # talib sign convention is the opposite of everywhere else
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 0  if self.price_column == "open" else 1

        sign = -1.0 if self.invert_sign else 1.0
        indicator_func = lambda x: sign*self.technical_indicator(x[self.price_column].sort_index().fillna(method='ffill', limit=3),
                                                                 **self.technical_indicator_params).shift(shift_value)\
            .to_frame(name="PPO_{0}_{1}".format(self.technical_indicator_params["fastperiod"],
                                                self.technical_indicator_params["slowperiod"]))
        apply_func = lambda x:indicator_func(x.set_index("date"))
        indicator_signal = daily_prices[["date","ticker", self.price_column]].groupby("ticker").apply(apply_func)\
            .reset_index()
        self.data = indicator_signal#.set_index(["date","ticker"])
        return StatusType.Success


    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator_params": {"fastperiod": 12, "slowperiod": 26, "matype": 0},
                "price_column": "close", "invert_sign": True}


class CalculateTaLibTRIX(CalibrationTaskflowTask):
    '''

    Calculates the talib version of TRIX indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_trix_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator_params, price_column):
        self.data = None
        self.technical_indicator = talib_TRIX
        self.technical_indicator_params = technical_indicator_params or {"timeperiod": 30}
        self.price_column = price_column or "close"
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 0  if self.price_column == "open" else 1

        indicator_func = lambda x: self.technical_indicator(x[self.price_column].sort_index().fillna(method='ffill', limit=3),
                                                            **self.technical_indicator_params).shift(shift_value)\
            .to_frame(name="TRIX_{0}".format(self.technical_indicator_params["timeperiod"]))
        apply_func = lambda x:indicator_func(x.set_index("date"))
        indicator_signal = daily_prices[["date","ticker", self.price_column]].groupby("ticker").apply(apply_func)\
            .reset_index()
        self.data = indicator_signal#.set_index(["date","ticker"])
        return StatusType.Success


    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator_params": {"timeperiod": 30}, "price_column": "close"}


class CalculateTaLibSTOCHRSI(CalibrationTaskflowTask):
    '''

    Calculates the talib version of STOCHRSI indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_stochrsi_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator_params, price_column):
        self.data = None
        self.technical_indicator = talib_STOCHRSI
        self.technical_indicator_params = technical_indicator_params or {"timeperiod": 14, "fastk_period": 5,
                                                                         "fastd_period": 3, "fastd_matype": 0}
        self.price_column = price_column or "close"
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 0  if self.price_column == "open" else 1

        # TODO: Check whether indiscriminantly rounding is a good idea
        price = daily_prices.pivot_table(index='date', columns='ticker', values=self.price_column, dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)

        indicator_generator = (self.technical_indicator(price[c], **self.technical_indicator_params) for c in price.columns)
        slow_fast_indicators = zip(*[k for k in indicator_generator])
        slow_fast_stoch_kd = list(map(lambda x:pd.concat(x, axis=1, keys=price.columns), slow_fast_indicators))
        fastk, fastd = slow_fast_stoch_kd
        fastk = fastk.stack().reset_index()\
            .rename(columns={0: 'STOCHRSI_FASTK_{0}_{1}_{2}'.format(self.technical_indicator_params['timeperiod'],
                                                                    self.technical_indicator_params['fastk_period'],
                                                                    self.technical_indicator_params['fastd_period'])})\
            .set_index(["date", "ticker"])
        fastd = fastd.stack().reset_index()\
            .rename(columns={0: 'STOCHRSI_FASTD_{0}_{1}_{2}'.format(self.technical_indicator_params['timeperiod'],
                                                                    self.technical_indicator_params['fastk_period'],
                                                                    self.technical_indicator_params['fastd_period'])})\
            .set_index(["date", "ticker"])

        self.data = pd.concat([fastk, fastd], axis=1).reset_index()
        return StatusType.Success


    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator_params": {"timeperiod": 14, "fastk_period": 5, "fastd_period": 3,
                                               "fastd_matype": 0}, "price_column": "close"}


class CalculateTaLibSTOCH(CalibrationTaskflowTask):
    '''

    Calculates the talib version of STOCH indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_stoch_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator_params):
        self.data = None
        self.technical_indicator = talib.STOCH
        self.technical_indicator_params = technical_indicator_params or \
            {"fastk_period": 5, "slowk_period": 3, "slowk_matype": 0, "slowd_period": 3, "slowd_matype": 0}
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 1

        # TODO: Check whether indiscriminantly rounding is a good idea
        high = daily_prices.pivot_table(index='date', columns='ticker', values='high', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        low = daily_prices.pivot_table(index='date', columns='ticker', values='low', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        close = daily_prices.pivot_table(index='date', columns='ticker', values='close', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)

        indicator_generator = (self.technical_indicator(high[c], low[c], close[c], **self.technical_indicator_params) for c in high.columns)
        slow_fast_indicators = zip(*[k for k in indicator_generator])
        slow_fast_stoch_kd = list(map(lambda x:pd.concat(x, axis=1, keys=high.columns), slow_fast_indicators))
        slowk, slowd = slow_fast_stoch_kd
        slowk = slowk.stack().reset_index()\
            .rename(columns={0: 'SLOWK_{0}_{1}_{2}'.format(self.technical_indicator_params['fastk_period'],
                                                           self.technical_indicator_params['slowd_period'],
                                                           self.technical_indicator_params['slowk_period'])})\
            .set_index(["date", "ticker"])
        slowd = slowd.stack().reset_index()\
            .rename(columns={0: 'SLOWD_{0}_{1}_{2}'.format(self.technical_indicator_params['fastk_period'],
                                                           self.technical_indicator_params['slowd_period'],
                                                           self.technical_indicator_params['slowk_period'])})\
            .set_index(["date", "ticker"])

        self.data = pd.concat([slowk, slowd], axis=1).reset_index()
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator_params": {"fastk_period": 5, "slowk_period": 3, "slowk_matype": 0, "slowd_period": 3, "slowd_matype": 0}}

class CalculateTaLibSTOCHF(CalibrationTaskflowTask):
    '''

    Calculates the talib version of STOCHF indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_stochf_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator_params):
        self.data = None
        self.technical_indicator = talib.STOCHF
        self.technical_indicator_params = technical_indicator_params or \
            {"fastk_period": 5, "fastd_period": 3, "fastd_matype": 0}
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 1

        # TODO: Check whether indiscriminantly rounding is a good idea
        high = daily_prices.pivot_table(index='date', columns='ticker', values='high', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        low = daily_prices.pivot_table(index='date', columns='ticker', values='low', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        close = daily_prices.pivot_table(index='date', columns='ticker', values='close', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)

        indicator_generator = (self.technical_indicator(high[c], low[c], close[c], **self.technical_indicator_params) for c in high.columns)
        slow_fast_indicators = zip(*[k for k in indicator_generator])
        slow_fast_stoch_kd = list(map(lambda x:pd.concat(x, axis=1, keys=high.columns), slow_fast_indicators))
        fastk, fastd = slow_fast_stoch_kd
        fastk = fastk.stack().reset_index()\
            .rename(columns={0: 'FASTK_{0}_{1}'.format(self.technical_indicator_params['fastk_period'],
                                                       self.technical_indicator_params['fastd_period'])})\
            .set_index(["date", "ticker"])
        fastd = fastd.stack().reset_index()\
            .rename(columns={0: 'FASTD_{0}_{1}'.format(self.technical_indicator_params['fastk_period'],
                                                       self.technical_indicator_params['fastd_period'])})\
            .set_index(["date", "ticker"])

        self.data = pd.concat([fastk, fastd], axis=1).reset_index()
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator_params": {"fastk_period": 5, "fastd_period": 3, "fastd_matype": 0}}

class CalculateTaLibWILLR(CalibrationTaskflowTask):
    '''

    Calculates the talib version of WILLR indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_willr_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator_params, smoothing_period):
        self.data = None
        self.technical_indicator = talib.WILLR
        self.technical_indicator_params = technical_indicator_params or {"timeperiod": 14}
        self.smoothing_period = smoothing_period or 3
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 1

        # TODO: Check whether indiscriminantly rounding is a good idea
        high = daily_prices.pivot_table(index='date', columns='ticker', values='high', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        low = daily_prices.pivot_table(index='date', columns='ticker', values='low', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        close = daily_prices.pivot_table(index='date', columns='ticker', values='close', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)

        column_name = 'WILLR_{0}'.format(self.technical_indicator_params["timeperiod"])
        ma_column_name = '{0}_MA_{1}'.format(column_name, self.smoothing_period)

        indicator_generator = (self.technical_indicator(high[c], low[c], close[c], **self.technical_indicator_params) for c in high.columns)
        indicator_signal = pd.concat(indicator_generator, axis=1, keys=high.columns)
        indicator_signal = indicator_signal.stack().reset_index().rename(columns={0: column_name})

        indicator_signal[ma_column_name] = indicator_signal.groupby(["ticker"])[column_name]\
            .apply(lambda x : MA(x, self.smoothing_period, type='simple'))
        self.data = indicator_signal#.set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator_params":{"timeperiod": 14}, "smoothing_period": 3}

class CalculateTaLibCCI(CalibrationTaskflowTask):
    '''

    Calculates the talib version of CCI indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_cci_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator_params, smoothing_period):
        self.data = None
        self.technical_indicator = talib.CCI
        self.technical_indicator_params = technical_indicator_params or {"timeperiod": 14}
        self.smoothing_period = smoothing_period or 3
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 1

        # TODO: Check whether indiscriminantly rounding is a good idea
        high = daily_prices.pivot_table(index='date', columns='ticker', values='high', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        low = daily_prices.pivot_table(index='date', columns='ticker', values='low', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        close = daily_prices.pivot_table(index='date', columns='ticker', values='close', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)

        column_name = 'CCI_{0}'.format(self.technical_indicator_params["timeperiod"])
        ma_column_name = '{0}_MA_{1}'.format(column_name, self.smoothing_period)

        indicator_generator = (self.technical_indicator(high[c], low[c], close[c], **self.technical_indicator_params) for c in high.columns)
        indicator_signal = pd.concat(indicator_generator, axis=1, keys=high.columns)
        indicator_signal = indicator_signal.stack().reset_index().rename(columns={0: column_name})

        indicator_signal[ma_column_name] = indicator_signal.groupby(["ticker"])[column_name]\
            .apply(lambda x : MA(x, self.smoothing_period, type='simple'))
        self.data = indicator_signal#.set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator_params":{"timeperiod": 14}, "smoothing_period": 3}

class CalculateTaLibMFI(CalibrationTaskflowTask):
    '''

    Calculates the talib version of MFI indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_mfi_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator_params, smoothing_period):
        self.data = None
        self.technical_indicator = talib.MFI
        self.technical_indicator_params = technical_indicator_params or {"timeperiod": 14}
        self.smoothing_period = smoothing_period or 3
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 1

        # TODO: Check whether indiscriminantly rounding is a good idea
        high = daily_prices.pivot_table(index='date', columns='ticker', values='high', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        low = daily_prices.pivot_table(index='date', columns='ticker', values='low', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        close = daily_prices.pivot_table(index='date', columns='ticker', values='close', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        volume = daily_prices.pivot_table(index='date', columns='ticker', values='volume', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value)

        column_name = 'MFI_{0}'.format(self.technical_indicator_params["timeperiod"])
        ma_column_name = '{0}_MA_{1}'.format(column_name, self.smoothing_period)

        indicator_generator = (self.technical_indicator(high[c], low[c], close[c], volume[c],
                                                        **self.technical_indicator_params) for c in high.columns)
        indicator_signal = pd.concat(indicator_generator, axis=1, keys=high.columns)
        indicator_signal = indicator_signal.stack().reset_index().rename(columns={0: column_name})

        indicator_signal[ma_column_name] = indicator_signal.groupby(["ticker"])[column_name]\
            .apply(lambda x : MA(x, self.smoothing_period, type='simple'))
        self.data = indicator_signal#.set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator_params":{"timeperiod": 14}, "smoothing_period": 3}

class CalculateTaLibCMO(CalibrationTaskflowTask):
    '''

    Calculates the talib version of CMO indicator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_cmo_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator_params, smoothing_period, price_column):
        self.data = None
        self.technical_indicator = talib.CMO
        self.technical_indicator_params = technical_indicator_params or {"timeperiod": 14}
        self.smoothing_period = smoothing_period or 3
        self.price_column = price_column or "close"
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 0  if self.price_column == "open" else 1

        price = daily_prices.pivot_table(index='date', columns='ticker', values=self.price_column, dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value)

        column_name = 'CMO_{0}'.format(self.technical_indicator_params["timeperiod"])
        ma_column_name = '{0}_MA_{1}'.format(column_name, self.smoothing_period)
        indicator_signal = price.apply(self.technical_indicator, **self.technical_indicator_params)
        indicator_signal = indicator_signal.stack().reset_index().rename(columns={0: column_name})

        indicator_signal[ma_column_name] = indicator_signal.groupby(["ticker"])[column_name]\
            .apply(lambda x : MA(x, self.smoothing_period, type='simple'))
        self.data = indicator_signal#.set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator_params": {"timeperiod": 14},
                "smoothing_period": 3, "price_column": "close"}

class CalculateTaLibAROONOSC(CalibrationTaskflowTask):
    '''

    Calculates the talib version of AROONOSC indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_aroonosc_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator_params, smoothing_period):
        self.data = None
        self.technical_indicator = talib.AROONOSC
        self.technical_indicator_params = technical_indicator_params or {"timeperiod": 14}
        self.smoothing_period = smoothing_period or 3
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 1

        # TODO: Check whether indiscriminantly rounding is a good idea
        high = daily_prices.pivot_table(index='date', columns='ticker', values='high', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        low = daily_prices.pivot_table(index='date', columns='ticker', values='low', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)

        column_name = 'AROONOSC_{0}'.format(self.technical_indicator_params["timeperiod"])
        ma_column_name = '{0}_MA_{1}'.format(column_name, self.smoothing_period)

        indicator_generator = (self.technical_indicator(high[c], low[c], **self.technical_indicator_params) for c in high.columns)
        indicator_signal = pd.concat(indicator_generator, axis=1, keys=high.columns)
        indicator_signal = indicator_signal.stack().reset_index().rename(columns={0: column_name})

        indicator_signal[ma_column_name] = indicator_signal.groupby(["ticker"])[column_name]\
            .apply(lambda x : MA(x, self.smoothing_period, type='simple'))
        self.data = indicator_signal#.set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator_params":{"timeperiod": 14}, "smoothing_period": 3}

class CalculateTaLibMACD(CalibrationTaskflowTask):
    '''

    Calculates the talib version of MACD indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_macd_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator_params, price_column):
        self.data = None
        self.technical_indicator = talib.MACD
        self.technical_indicator_params = technical_indicator_params or \
            {"fastperiod": 12, "slowperiod": 26, "signalperiod": 9}
        self.price_column = price_column
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 0  if self.price_column == "open" else 1

        # TODO: Check whether indiscriminantly rounding is a good idea
        price = daily_prices.pivot_table(index='date', columns='ticker', values=self.price_column, dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)

        indicator_generator = (self.technical_indicator(price[c], **self.technical_indicator_params) for c in price.columns)
        slow_fast_indicators = zip(*[k for k in indicator_generator])
        slow_fast_stoch_kd = list(map(lambda x:pd.concat(x, axis=1, keys=price.columns), slow_fast_indicators))
        macd, macdsignal, macdhist = slow_fast_stoch_kd
        macd = macd.stack().reset_index()\
            .rename(columns={0: 'MACD_{0}_{1}_{2}'.format(self.technical_indicator_params['fastperiod'],
                                                          self.technical_indicator_params['slowperiod'],
                                                          self.technical_indicator_params['signalperiod'])})\
            .set_index(["date", "ticker"])
        macdsignal = macdsignal.stack().reset_index()\
            .rename(columns={0: 'MACDSIGNAL_{0}_{1}_{2}'.format(self.technical_indicator_params['fastperiod'],
                                                                self.technical_indicator_params['slowperiod'],
                                                                self.technical_indicator_params['signalperiod'])})\
            .set_index(["date", "ticker"])
        macdhist = macdhist.stack().reset_index()\
            .rename(columns={0: 'MACDHIST_{0}_{1}_{2}'.format(self.technical_indicator_params['fastperiod'],
                                                              self.technical_indicator_params['slowperiod'],
                                                              self.technical_indicator_params['signalperiod'])})\
            .set_index(["date", "ticker"])


        self.data = pd.concat([macd, macdsignal, macdhist], axis=1).reset_index()
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator_params": {"fastperiod": 12, "slowperiod": 26, "signalperiod": 9},
                "price_column": "close"}


class CalculateTaLibULTOSC(CalibrationTaskflowTask):
    '''

    Calculates the talib version of ADX indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_ultosc_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator_params, smoothing_period):
        self.data = None
        self.technical_indicator = talib.ULTOSC
        self.technical_indicator_params = technical_indicator_params or \
            {"timeperiod1": 7, "timeperiod2": 14, "timeperiod3": 28}
        self.smoothing_period = smoothing_period or 3
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 1

        # TODO: Check whether indiscriminantly rounding is a good idea
        high = daily_prices.pivot_table(index='date', columns='ticker', values='high', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        low = daily_prices.pivot_table(index='date', columns='ticker', values='low', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        close = daily_prices.pivot_table(index='date', columns='ticker', values='close', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)

        column_name = 'ULTOSC_{0}_{1}_{2}'.format(self.technical_indicator_params["timeperiod1"],
                                                  self.technical_indicator_params["timeperiod2"],
                                                  self.technical_indicator_params["timeperiod3"])
        ma_column_name = '{0}_MA_{1}'.format(column_name, self.smoothing_period)

        indicator_generator = (self.technical_indicator(high[c], low[c], close[c], **self.technical_indicator_params) for c in high.columns)
        indicator_signal = pd.concat(indicator_generator, axis=1, keys=high.columns)
        indicator_signal = indicator_signal.stack().reset_index().rename(columns={0: column_name})

        indicator_signal[ma_column_name] = indicator_signal.groupby(["ticker"])[column_name]\
            .apply(lambda x : MA(x, self.smoothing_period, type='simple'))
        self.data = indicator_signal#.set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator_params": {"timeperiod1": 7, "timeperiod2": 14, "timeperiod3": 28},
                "smoothing_period": 3}


class CalculateTaLibADOSC(CalibrationTaskflowTask):
    '''

    Calculates the talib version of ADOSC indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_adosc_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator_params, smoothing_period, normalize):
        self.data = None
        self.technical_indicator = talib.ADOSC
        self.technical_indicator_params = technical_indicator_params or {"fastperiod": 3, "slowperiod": 10}
        self.smoothing_period = smoothing_period or 3
        self.normalize = normalize or True
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 1

        # TODO: Check whether indiscriminantly rounding is a good idea
        high = daily_prices.pivot_table(index='date', columns='ticker', values='high', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        low = daily_prices.pivot_table(index='date', columns='ticker', values='low', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        close = daily_prices.pivot_table(index='date', columns='ticker', values='close', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        volume = daily_prices.pivot_table(index='date', columns='ticker', values='volume', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value)

        column_name = 'ADOSC_{0}_{1}'.format(self.technical_indicator_params["fastperiod"],
                                             self.technical_indicator_params["slowperiod"])
        ma_column_name = '{0}_MA_{1}'.format(column_name, self.smoothing_period)

        indicator_generator = (self.technical_indicator(high[c], low[c], close[c], volume[c],
                                                        **self.technical_indicator_params) for c in high.columns)
        indicator_signal = pd.concat(indicator_generator, axis=1, keys=high.columns)
        indicator_signal = indicator_signal.stack().reset_index().rename(columns={0: column_name})

        if self.normalize:
            indicator_generator = (talib.AD(high[c], low[c], close[c], volume[c]) for c in high.columns)
            denominator_signal = pd.concat(indicator_generator, axis=1, keys=high.columns)
            denominator_signal = denominator_signal.stack().reset_index().rename(columns={0: column_name})
            denominator_signal = indicator_signal.groupby(["ticker"])[column_name]\
                .apply(lambda x : MA(x, self.technical_indicator_params["fastperiod"], type='simple'))
            indicator_signal[column_name] = indicator_signal[column_name]/denominator_signal

        indicator_signal[ma_column_name] = indicator_signal.groupby(["ticker"])[column_name]\
            .apply(lambda x : MA(x, self.smoothing_period, type='simple'))


        self.data = indicator_signal#.set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator_params": {"fastperiod": 3, "slowperiod": 10}, "smoothing_period": 3,
                "normalize": True}

class CalculateTaLibOBV(CalibrationTaskflowTask):
    '''

    Calculates the talib version of OBV indicator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_obv_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, smoothing_period):
        self.data = None
        self.technical_indicator = talib.OBV
        self.smoothing_period = smoothing_period or 3
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 1

        # TODO: Check whether indiscriminantly rounding is a good idea
        close = daily_prices.pivot_table(index='date', columns='ticker', values='close', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        volume = daily_prices.pivot_table(index='date', columns='ticker', values='volume', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value)

        column_name = 'OBV'
        ma_column_name = '{0}_MA_{1}'.format(column_name, self.smoothing_period)
        normalized_column_name = 'OBV_normalized_{0}'.format(self.smoothing_period)

        indicator_generator = (self.technical_indicator(close[c], volume[c]) for c in close.columns)
        indicator_signal = pd.concat(indicator_generator, axis=1, keys=close.columns)
        indicator_signal = indicator_signal.stack().reset_index().rename(columns={0: column_name})

        indicator_signal[ma_column_name] = indicator_signal.groupby(["ticker"])[column_name]\
            .apply(lambda x : MA(np.abs(x), self.smoothing_period, type='simple'))
        indicator_signal = indicator_signal.set_index(["date", "ticker"])
        indicator_signal[normalized_column_name] = indicator_signal[column_name]/indicator_signal[ma_column_name]
        self.data = indicator_signal.reset_index()
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"smoothing_period": 3}

class CalculateTaLibNATR(CalibrationTaskflowTask):
    '''

    Calculates the talib version of NATR indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_natr_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator_params):
        self.data = None
        self.technical_indicator = talib.NATR
        self.technical_indicator_params = technical_indicator_params or {"timeperiod": 14}
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 1

        # TODO: Check whether indiscriminantly rounding is a good idea
        high = daily_prices.pivot_table(index='date', columns='ticker', values='high', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        low = daily_prices.pivot_table(index='date', columns='ticker', values='low', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        close = daily_prices.pivot_table(index='date', columns='ticker', values='close', dropna=False)\
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)

        column_name = 'NATR_{0}'.format(self.technical_indicator_params["timeperiod"])

        indicator_generator = (self.technical_indicator(high[c], low[c], close[c], **self.technical_indicator_params) for c in high.columns)
        indicator_signal = pd.concat(indicator_generator, axis=1, keys=high.columns)
        indicator_signal = indicator_signal.stack().reset_index().rename(columns={0: column_name})

        self.data = indicator_signal#.set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"technical_indicator_params": {"timeperiod": 14}}

class QuandlDataCleanup(CalibrationTaskflowTask):
    '''
    Cleans Quandl Data and resamples to daily, monthly, and quarterly frequencies
    '''

    PROVIDES_FIELDS = ["quandl_daily", "quandl_quarterly"]
    REQUIRES_FIELDS = ["daily_price_data", "raw_quandl_data"]
    def __init__(self):
        self.data = None
        self.monthly = None
        self.quarterly = None
        CalibrationTaskflowTask.__init__(self)

    def _resolve_duplicates(self, raw):
        raw['fq'] = pd.to_datetime(raw['calendardate'])
        raw['filing_date'] = pd.to_datetime(raw['datekey'])
        raw['date'] = (raw['filing_date'].dt.to_period('M') + 1).dt.to_timestamp(freq='B')
        raw.drop(['calendardate', 'datekey', 'reportperiod', 'lastupdated'], axis=1, inplace=True)
        raw = raw[['dcm_security_id', 'date', 'filing_date', 'fq'] + \
                  raw.columns.drop(['dcm_security_id', 'date', 'filing_date', 'fq']).tolist()]

        raw.sort_values(['dcm_security_id', 'filing_date', 'fq', 'dimension'], inplace=True)
        raw.drop_duplicates(subset=['dcm_security_id', 'filing_date', 'fq'], keep='first', inplace=True)
        raw.drop_duplicates(subset=['dcm_security_id', 'date'], keep='last', inplace=True)
        return raw

    def _get_close_volume_filing_periods(self, raw, price, dates):
        close = price.pivot_table(index='date', columns='dcm_security_id', values='close', dropna=False)
        close = close.reindex(dates).sort_index()
        close = close.ffill(limit=5)
        close = close.stack().reset_index().rename(columns={0: 'close'})
        volume = price.pivot_table(index='date', columns='dcm_security_id', values='volume', dropna=False)
        volume = volume.reindex(dates).sort_index()
        volume = volume.ffill(limit=5)
        volume = volume.stack().reset_index().rename(columns={0: 'volume'})
        filing_periods = raw.groupby(['dcm_security_id', 'dimension']).size().unstack().idxmax(axis=1)
        filing_periods = pd.Series(filing_periods.index, index=filing_periods)
        return close, volume, filing_periods

    def _process_quarterly(self, raw, arq):
        quarterly = arq.reset_index()
        quarterly.sort_values(['dcm_security_id', 'fq', 'filing_date'], inplace=True)
        self.quarterly = quarterly.groupby(['dcm_security_id', 'fq']).last().reset_index()

    def _process_monthly(self, raw, close, arq, art, months):
        arq = arq.unstack().reindex(months)
        art = art.unstack().reindex(months)
        #arq.ffill(limit=3, inplace=True)
        #art.ffill(limit=12, inplace=True)
        arq.ffill(limit=4, inplace=True)
        art.ffill(limit=13, inplace=True)
        monthly = pd.concat([arq.stack().reset_index(), art.stack().reset_index()], ignore_index=True)
        monthly = pd.merge(monthly, close, how='left', on=['date', 'dcm_security_id'])
        monthly['marketcap'] = monthly['sharesbas'] * monthly['close'] * monthly['sharefactor']
        self.monthly = monthly.sort_values(['dcm_security_id', 'date'])

    def _process_daily(self, raw, close, volume, filing_periods, dates):
        arq_temp = raw[raw['dcm_security_id'].isin(filing_periods['ARQ'])].drop(['dimension', 'date'], axis=1)\
            .rename(columns={'filing_date': 'date'})
        arq = arq_temp.set_index(['date', 'dcm_security_id'])
        
        dummy_date = pd.Timestamp("2040-12-31")
        dummy_security_id = 9999999
        if "ART" in filing_periods:
            art = raw[raw['dcm_security_id'].isin(filing_periods['ART'])].drop(['dimension', 'date'], axis=1)\
                .rename(columns={'filing_date': 'date'}).set_index(['date', 'dcm_security_id'])            
        else:
            art = pd.DataFrame(columns=arq_temp.columns)
            art.loc[0, "dcm_security_id"] = dummy_security_id
            for col in ["date", "fq"]:
                art.loc[0, col] = dummy_date
            art.set_index(["date", "dcm_security_id"], inplace=True)
        
        arq = arq.unstack().reindex(dates).shift(1)
        art = art.unstack().reindex(dates).shift(1)
        #arq.ffill(limit=91, inplace=True)
        #art.ffill(limit=365, inplace=True)
        arq.ffill(limit=126, inplace=True)
        art.ffill(limit=400, inplace=True)
        daily = pd.concat([arq.stack().reset_index(), art.stack().reset_index()], ignore_index=True)
        daily = pd.merge(daily, close, how='left', on=['date', 'dcm_security_id'])
        daily = pd.merge(daily, volume, how='left', on=['date', 'dcm_security_id'])
        daily['marketcap'] = daily['sharesbas'] * daily['close'] * daily['sharefactor']
        self.data = daily.sort_values(['dcm_security_id', 'date'])

    def do_step_action(self, **kwargs):
        price = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        price.rename(columns={"ticker": "dcm_security_id"}, inplace=True)
        raw = kwargs[self.__class__.REQUIRES_FIELDS[1]].copy(deep=True)

        raw = self._resolve_duplicates(raw)
        dates = pd.date_range(raw['date'].min(), max(raw['date'].max(), pd.datetime.today()), freq='B', name='date')
        months = raw['date'].drop_duplicates().sort_values()
        close, volume, filing_periods = self._get_close_volume_filing_periods(raw, price, dates)

        arq_temp = raw[raw['dcm_security_id'].isin(filing_periods['ARQ'])].drop('dimension', axis=1)
        arq = arq_temp.set_index(['date', 'dcm_security_id'])
        dummy_date = pd.Timestamp("2040-12-31")
        dummy_security_id = 9999999
        if "ART" in filing_periods:
            art = raw[raw['dcm_security_id'].isin(filing_periods['ART'])].drop('dimension', axis=1)\
                .set_index(['date', 'dcm_security_id'])
        else:
            art = pd.DataFrame(columns=arq_temp.columns)
            art.loc[0, "dcm_security_id"] = dummy_security_id
            for col in ["date", "filing_date", "fq"]:
                art.loc[0, col] = dummy_date
            art.set_index(["date", "dcm_security_id"], inplace=True)
        self._process_quarterly(raw, arq)
        self._process_monthly(raw, close, arq, art, months)
        self._process_daily(raw, close, volume, filing_periods, dates)
        
        self.data = self.data[self.data["dcm_security_id"]<dummy_security_id].reset_index(drop=True)
        self.monthly = self.monthly[self.monthly["dcm_security_id"]<dummy_security_id].reset_index(drop=True)
        self.quarterly = self.quarterly[self.quarterly["dcm_security_id"]<dummy_security_id].reset_index(drop=True)
        
        self.data["dcm_security_id"] = self.data["dcm_security_id"].astype(int)
        self.monthly["dcm_security_id"] = self.monthly["dcm_security_id"].astype(int)
        self.quarterly["dcm_security_id"] = self.quarterly["dcm_security_id"].astype(int)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.quarterly}

    @classmethod
    def get_default_config(cls):
        return {}

class CalculateDerivedQuandlFeatures(CalibrationTaskflowTask):
    '''
    Creates derived fundamental features from the clean quandl data sets
    '''

    PROVIDES_FIELDS = ["fundamental_features"]
    REQUIRES_FIELDS = ["industry_map", "daily_price_data", "quandl_daily", "quandl_quarterly"]
    def __init__(self):
        self.data = None
        CalibrationTaskflowTask.__init__(self)

    def _construct_simple_features(self, sector, daily):
        daily = pd.merge(daily, sector[['dcm_security_id', 'GICS_Sector', 'GICS_IndustryGroup']], how='left', on='dcm_security_id')
        daily['GICS_Sector'] = daily['GICS_Sector'].fillna('n/a')
        daily['GICS_IndustryGroup'] = daily['GICS_IndustryGroup'].fillna('n/a')
        daily['log_mktcap'] = pd.np.log(daily['marketcap'])
        daily['log_dollar_volume'] = pd.np.log(daily['close'] * daily['volume'])
        daily['bm'] = 1.0 / daily['pb']
        daily['bm_ia_sector'] = daily['bm'] - daily.groupby(['date', 'GICS_Sector'])['bm'].transform('mean')
        daily['bm_ia_indgrp'] = daily['bm'] - daily.groupby(['date', 'GICS_IndustryGroup'])['bm'].transform('mean')
        daily['debt2equity'] = daily['debt'] / daily['equity']
        daily['fcf_yield'] = daily['fcf'] / daily['marketcap']
        daily['gp2assets'] = daily['gp'] / daily['assets']
        daily['turnover'] = daily['volume'] / daily['shareswa']

        # 2019/08/23 These are added
        daily['capex_yield'] = daily['capex'] / daily['marketcap']
        daily['current_to_total_assets'] = daily['assetsc']/daily['assets']
        daily['inventory_to_revenue'] = daily['inventory']/daily['revenue']
        daily['ncf_yield'] = daily['ncf']/daily['marketcap']
        daily['ncfdiv_yield'] = daily['ncfdiv']/daily['marketcap']
        daily['ncfcommon_yield'] = daily['ncfcommon']/daily['marketcap']
        daily['ncfdebt_yield'] = daily['ncfdebt']/daily['marketcap']
        daily['retearn_to_liabilitiesc'] =  daily['retearn']/daily['liabilitiesc']
        daily['retearn_to_total_assets'] = daily['retearn']/daily['assets']
        daily['rnd_to_revenue'] = daily['rnd']/daily['revenue']

        for col in ['investments', 'ncff', 'revenue', 'ncfi']:
            new_col = "{0}_yield".format(col)
            daily[new_col] = daily[col]/daily["assets"]
        return daily

    def _construct_quarterly_pivot_data(self, daily, quarterly):
        df_dict = dict()
        for key in  ["assets", "revenue", "capex", "inventory", "shareswa"]:
            df_dict[key] = quarterly.pivot_table(index='fq', columns='dcm_security_id', values=key,
                                                 dropna=False).sort_index()
        
        for key in ["assets", "revenue", "shareswa"]:
            df_dict[key+"_growth"]  = df_dict[key].pct_change(4, fill_method=None)


        df_dict["delta_capex"] = df_dict["capex"].diff(4) / df_dict["assets"].rolling(4, min_periods=1).mean()
        df_dict["delta_inventory"] = df_dict["inventory"].diff(4) / df_dict["assets"].rolling(4, min_periods=1).mean()
        
       
        for key in  ["assets_growth", "revenue_growth", "shareswa_growth", "delta_capex", "delta_inventory"]:
            df_dict[key] = df_dict[key].stack().reset_index().rename(columns={0: key})
        
       
        turnover = daily.pivot_table(index='date', columns='dcm_security_id', values='turnover', dropna=False).sort_index()
        std_turn = turnover.rolling(21).std().shift(1)
        std_turn = std_turn.stack().reset_index().rename(columns={0: 'std_turn'})

        # This is just for demo
        
        for key in ["revenue", "assets"]:
            df_dict[key + "_growth_qoq"] = df_dict[key].pct_change(1, fill_method=None)
            df_dict[key + "_growth_qoq"] = df_dict[key + "_growth_qoq"].stack().reset_index().rename(columns={0: 
                                                                                                              key + "_growth_qoq"})
            df_dict["future_" + key + "_growth_qoq"] = df_dict[key].pct_change(1, fill_method=None).shift()
            df_dict["future_" + key + "_growth_qoq"] = df_dict["future_" + key + "_growth_qoq"].stack().reset_index().rename(columns={0: 
                                                                                                                       "future_" + key + "_growth_qoq"})            
            
        while df_dict:
            key, df = df_dict.popitem()
            if key in  ["assets_growth", "revenue_growth", "shareswa_growth", "delta_capex",
                        "delta_inventory", "revenue_growth_qoq", "future_revenue_growth_qoq", 
                        "assets_growth_qoq", "future_assets_growth_qoq"]:
                daily = pd.merge(daily, df, how='left', on=['dcm_security_id', 'fq'])
            gc.collect()
        daily = pd.merge(daily, std_turn, how='left', on=['dcm_security_id', 'date'])
        daily = daily.rename(columns={"assets_growth": "asset_growth", "assets_growth_qoq": "asset_growth_qoq",
                                      "future_assets_growth_qoq": "future_asset_growth_qoq"})
        return daily

    def _construct_liquidity_ratio_features(self, price, quandl):
        close = price.pivot_table(index='date', columns='dcm_security_id', values='close', dropna=False).sort_index()
        ret = close.pct_change()
        retvol = ret.rolling(21).std()
        maxret = ret.rolling(21).max()
        volume = price.pivot_table(index='date', columns='dcm_security_id', values='volume', dropna=False).sort_index()
        dolvol = close * volume
        high = price.pivot_table(index='date', columns='dcm_security_id', values='close', dropna=False).sort_index()
        high52wk = high.rolling(252).max()
        rel_to_high = close.div(high52wk)
        zerodays = (volume == 0).rolling(21).sum()
        amihud = (ret.abs() / dolvol).rolling(21).mean()

        quandl['ebitda_to_ev'] = quandl['ebitda'] / quandl['ev']
        quandl['sharey'] = (quandl['ncfdiv'] + quandl['ncfcommon'] + quandl['ncfdebt']) / quandl['marketcap']
        quandl['shares'] = quandl['sharesbas'] * quandl['sharefactor']
        shares = quandl.pivot_table(index='date', columns='dcm_security_id', values='shares', dropna=False).sort_index()
        chcsho = shares.pct_change(252)

        features = ['retvol', 'maxret', 'dolvol', 'rel_to_high', 'amihud', 'zerodays', 'chcsho']
        objects = [retvol, maxret, dolvol, rel_to_high, amihud, zerodays, chcsho]
        df = pd.concat((f.shift(1) for f in objects), keys=features, names=['feature'], axis=1).reset_index()
        df = df.set_index('date').stack().reset_index()
        df.dropna(how='all', subset=features, inplace=True)

        quandl = quandl[['date', 'dcm_security_id', 'ebitda_to_ev', 'ps', 'netmargin', 'sharey']]
        quandl["date"] = quandl["date"].apply(pd.Timestamp)
        df['amihud'] = -pd.np.log(df['amihud'] + 1e-12) # normalize the skew

        df["dcm_security_id"] =  df["dcm_security_id"].astype(int)
        merged = pd.merge(df, quandl, how='left', on=['date', 'dcm_security_id'])
        merged["dcm_security_id"] = merged["dcm_security_id"].astype(int)
        return merged

    def do_step_action(self, **kwargs):
        sector = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        price = kwargs[self.__class__.REQUIRES_FIELDS[1]].copy(deep=True)
        price.rename(columns={"ticker": "dcm_security_id"}, inplace=True)
        quandl = kwargs[self.__class__.REQUIRES_FIELDS[2]].copy(deep=True)
        quarterly = kwargs[self.__class__.REQUIRES_FIELDS[3]].copy(deep=True)

        daily = self._construct_simple_features(sector, quandl)
        daily = self._construct_quarterly_pivot_data(daily, quarterly)
        cols = ['log_mktcap', 'bm', 'bm_ia_sector', 'bm_ia_indgrp', 'assetturnover', 'gp2assets', 'divyield',
                'shareswa_growth', 'pe', 'log_dollar_volume', 'debt2equity', 'delta_inventory', 'delta_capex',
                'fcf_yield', 'asset_growth', 'revenue_growth', 'std_turn', 'investments_yield', 'ncff_yield',
                'revenue_yield', 'ncfi_yield']
        # Added 2019/08/23
        cols = cols + ["revenue_growth_qoq", "future_revenue_growth_qoq", "asset_growth_qoq", "future_asset_growth_qoq"]
        cols = cols + \
            ['assets', 'assetsc', 'capex', 'currentratio', 'ebitdamargin', 'ebitda', 'eps', 'epsdil',
             'equityusd', 'ev', 'fcf', 'gp', 'grossmargin', 'invcap', 'inventory', 'investments', 'ncf',
             'ncfcommon', 'ncfdebt', 'ncfdiv', 'ncff', 'ncfi', 'ncfinv', 'payoutratio',
             'pb', 'pe1', 'ps1', 'retearn', 'revenue', 'rnd', 'roa', 'roe', 'roic', 'ros', 'sps',
             'ncfdiv_yield', 'ncfdebt_yield', 'inventory_to_revenue', 'retearn_to_liabilitiesc',
             'current_to_total_assets', 'capex_yield', 'ncfcommon_yield', 'ncf_yield', 'retearn_to_total_assets',
             'rnd_to_revenue']

        cols = list(set(cols))
        cols.sort()

        daily = daily[['date', 'dcm_security_id', 'fq'] + cols]
        daily["dcm_security_id"] = daily["dcm_security_id"].astype(int)
        extra_features = self._construct_liquidity_ratio_features(price, quandl)
        daily = pd.merge(daily, extra_features, how="left", on=["date", "dcm_security_id"])\
            .rename(columns={"dcm_security_id": "ticker"})
        self.data = daily
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {}

class CalculateTargetReturns(CalibrationTaskflowTask):
    '''

    Calculates the talib version of OBV indicator based on daily prices

    '''

    PROVIDES_FIELDS = ["target_returns"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, return_column, periods, winsorize_alpha):
        self.data = None
        self.return_column = return_column or "close"
        self.periods = periods or [1, 5, 10, 21]
        self.winsorize_alpha = winsorize_alpha or 0.01
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        price = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        target = price.pivot_table(index='date', columns='ticker', values=self.return_column, dropna=False)
        target = target.sort_index().fillna(method='ffill', limit=3)
        ret = pd.concat((target.pct_change(d).shift(-d).stack() for d in self.periods),
                        axis=1, keys=['future_ret_%dB' % d for d in self.periods])
        if self.winsorize_alpha:
            ret = winsorize(ret, ret.columns, ['date'], self.winsorize_alpha)
        self.data = ret.reset_index()
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"return_column": "close", "periods": [1, 5, 10, 21], "winsorize_alpha": 0.01}



class TransformEconomicData(CalibrationTaskflowTask):

    PROVIDES_FIELDS = ["transformed_econ_data"]
    REQUIRES_FIELDS = ["econ_data", "yahoo_daily_price_rolling","econ_transformation"]

    def __init__(self, start_date, end_date, mode, shift_increment="month"):
        self.data = None
        self.start_date = pd.Timestamp(start_date) if start_date else None
        self.end_date = pd.Timestamp(end_date) if end_date else None
        self.mode = mode
        self.shift_increment = shift_increment
        CalibrationTaskflowTask.__init__(self)

    def _transform_data(self, etf_rolling, econ_data, transform_dict, trading_days, date_shift):
        if self.shift_increment=="day": # Shifts 1-day
            etf_rolling = etf_rolling.shift(date_shift).ix[trading_days].fillna(method="ffill")
            econ_data = econ_data.shift(date_shift).ix[trading_days].fillna(method="ffill")
        else: # Shifts 1-month
            etf_rolling = etf_rolling.ix[trading_days].fillna(method="ffill").shift(date_shift)
            econ_data = econ_data.ix[trading_days].fillna(method="ffill").shift(date_shift)

        etf_rolling = etf_rolling.apply(lambda x:transform_wrapper(x, transform_dict), axis=0)
        econ_data = econ_data.apply(lambda x:transform_wrapper(x, transform_dict), axis=0)
        etf_rolling = etf_rolling.reset_index().rename(columns={"Date": "date"})
        econ_data = econ_data.reset_index()
        econ_data["date"] = pd.DatetimeIndex(econ_data["date"]).normalize()
        etf_rolling["date"] = pd.DatetimeIndex(etf_rolling["date"]).normalize()
        econ_data = pd.merge(econ_data, etf_rolling, how="left", on=["date"])
        dates = pd.DatetimeIndex(etf_rolling["date"])
        date_intersection = dates.intersection(pd.DatetimeIndex(econ_data["date"]))
        econ_data = econ_data[econ_data["date"].isin(date_intersection)].sort_values("date").reset_index(drop=True)
        return econ_data[2:]

    def do_step_action(self, **kwargs):
        etf_rolling = kwargs["yahoo_daily_price_rolling"]
        econ_data = kwargs["econ_data"]
        transform_dict_org = kwargs["econ_transformation"]
        transform_dict = transform_dict_org.set_index("Unnamed: 0")["transform"].to_dict()

        self.start_date = self.start_date if pd.notnull(self.start_date) else self.task_params.start_dt
        self.end_date = self.end_date if pd.notnull(self.end_date) else self.task_params.end_dt
        #trading_days = self._pick_trading_month_dates()
        max_end_date = min(min(etf_rolling.index.max(), econ_data.index.max()), self.end_date)
        trading_days = list(pick_trading_month_dates(self.start_date, max_end_date, self.mode))

        if self.task_params.run_mode==TaskflowPipelineRunMode.Calibration:
            date_shift = 0
        else:
            date_shift = 1

        self.data = self._transform_data(etf_rolling, econ_data, transform_dict, trading_days, date_shift)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"mode" : "bme",
                "start_date" : "1997-01-01",
		"end_date" : "2019-10-05",
                "shift_increment": "month"}


class TransformEconomicDataWeekly(TransformEconomicData):

    PROVIDES_FIELDS = ["transformed_econ_data", "transformed_econ_data_weekly"]
    REQUIRES_FIELDS = ["econ_data", "yahoo_daily_price_rolling","econ_transformation"]

    def __init__(self, start_date, end_date, monthly_mode, weekly_mode, shift_increment="month"):
        self.weekly_data = None
        self.weekly_mode = weekly_mode
        TransformEconomicData.__init__(self, start_date, end_date, monthly_mode, shift_increment)

    def do_step_action(self, **kwargs):
        etf_rolling = kwargs["yahoo_daily_price_rolling"]
        econ_data = kwargs["econ_data"]
        transform_dict_org = kwargs["econ_transformation"]
        transform_dict = transform_dict_org.set_index("Unnamed: 0")["transform"].to_dict()

        self.start_date = self.start_date if pd.notnull(self.start_date) else self.task_params.start_dt
        self.end_date = self.end_date if pd.notnull(self.end_date) else self.task_params.end_dt
        if self.task_params.run_mode==TaskflowPipelineRunMode.Calibration:
            date_shift = 0
        else:
            date_shift = 1

        max_end_date = min(min(etf_rolling.index.max(), econ_data.index.max()), self.end_date)
        monthly_trading_days = list(pick_trading_month_dates(self.start_date, max_end_date, self.mode))
        self.data = self._transform_data(etf_rolling, econ_data, transform_dict, monthly_trading_days, date_shift)

        weekly_trading_days = list(pick_trading_week_dates(self.start_date, max_end_date, self.weekly_mode))
        weekly_dated_df = pd.DataFrame({'date':weekly_trading_days})
        self.weekly_data = pd.merge_asof(weekly_dated_df, self.data, on='date', direction='backward')
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.weekly_data}

    @classmethod
    def get_default_config(cls):
        return {"monthly_mode" : "bme",
                "weekly_mode" : "w-mon",
                "start_date" : "1997-01-01",
                "end_date" : "2019-10-05",
                "shift_increment": "month"}


class CreateIndustryAverage(CalibrationTaskflowTask):

    PROVIDES_FIELDS = ["industry_average", "sector_average"]
    REQUIRES_FIELDS = ["monthly_merged_data_single_names", "security_master"]

    def __init__(self, industry_cols, sector_cols):
        self.industry_cols = industry_cols
        self.sector_cols = sector_cols
        self.industry_averages = None
        self.sector_averages = None
        CalibrationTaskflowTask.__init__(self)

    def _average_data(self, mapping, merged_data, averaging_col):
        #singlename_tickers = list(mapping.dropna(subset=[averaging_col])["dcm_security_id"].unique())
        #data_to_average = data_to_average[data_to_average["ticker"].isin(singlename_tickers)].reset_index(drop=True)
        if averaging_col == "Sector":
            columns_needed_for_averaging = self.sector_cols
        else:
            columns_needed_for_averaging = self.industry_cols
        columns_needed_for_averaging = columns_needed_for_averaging+["ticker","date"]
        data_to_average = merged_data[columns_needed_for_averaging]
        data_to_average = pd.merge(data_to_average, mapping[["dcm_security_id", averaging_col]], how="left",
                                   left_on=["ticker"], right_on=["dcm_security_id"])
        data_to_average[averaging_col] = data_to_average[averaging_col].astype(str)
        avg_data = data_to_average.groupby(["date", averaging_col]).apply(lambda x:x.mean(skipna=True)).drop(["ticker","dcm_security_id"], axis=1)
        for col in avg_data:
            print(col)
            avg_data[col] = avg_data[col].astype(float)

        #print(avg_data.tail(10)[["netmargin", "ADX_14_MA_3", "ret_5B", "future_ret_5B"]])
        return avg_data


    def do_step_action(self, **kwargs):
        industry_mapping = kwargs[self.REQUIRES_FIELDS[1]]
        monthly_merged_data = kwargs[self.REQUIRES_FIELDS[0]]
        self.industry_averages = self._average_data(industry_mapping, monthly_merged_data, "IndustryGroup")
        self.sector_averages = self._average_data(industry_mapping, monthly_merged_data, "Sector")
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.industry_averages,
                self.__class__.PROVIDES_FIELDS[1] : self.sector_averages}

    @classmethod
    def get_default_config(cls):
        return {"industry_cols" : ["volatility_126", "PPO_12_26", "PPO_21_126", "netmargin", "macd_diff", "pe",
                                   "debt2equity", "bm", "ret_63B", "ebitda_to_ev", "divyield"],
                "sector_cols" : ["volatility_126", "PPO_21_126", "macd_diff", "divyield", "bm"]}


class CreateIndustryAverageWeekly(CreateIndustryAverage):

    PROVIDES_FIELDS = ["industry_average", "sector_average", "industry_average_weekly", "sector_average_weekly"]
    REQUIRES_FIELDS = ["monthly_merged_data_single_names", "weekly_merged_data_single_names", "security_master"]

    def __init__(self, industry_cols, sector_cols):
        self.industry_averages_weekly = None
        self.sector_averages_weekly = None
        CreateIndustryAverage.__init__(self, industry_cols, sector_cols)

    def do_step_action(self, **kwargs):
        monthly_merged_data = kwargs[self.REQUIRES_FIELDS[0]]
        weekly_merged_data = kwargs[self.REQUIRES_FIELDS[1]]
        industry_mapping = kwargs[self.REQUIRES_FIELDS[2]]
        self.industry_averages = self._average_data(industry_mapping, monthly_merged_data, "IndustryGroup")
        self.sector_averages = self._average_data(industry_mapping, monthly_merged_data, "Sector")
        self.industry_averages_weekly = self._average_data(industry_mapping, weekly_merged_data, "IndustryGroup")
        self.sector_averages_weekly = self._average_data(industry_mapping, weekly_merged_data, "Sector")
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.industry_averages,
                self.__class__.PROVIDES_FIELDS[1] : self.sector_averages,
                self.__class__.PROVIDES_FIELDS[2] : self.industry_averages_weekly,
                self.__class__.PROVIDES_FIELDS[3] : self.sector_averages_weekly}


class CreateYahooDailyPriceRolling(CalibrationTaskflowTask):
    REQUIRES_FIELDS = ["yahoo_daily_price_data"]
    PROVIDES_FIELDS = ["yahoo_daily_price_rolling"]

    def __init__(self, rolling_interval):
        self.rolling_interval = rolling_interval
        self.data = None
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        yahoo_daily_price = kwargs["yahoo_daily_price_data"]
        self.data = yahoo_daily_price.rolling(self.rolling_interval).mean()
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"rolling_interval" : 5}

class GenerateActiveMatrix(CalibrationTaskflowTask):
    
    PROVIDES_FIELDS = ["active_matrix"]
    REQUIRES_FIELDS = ["current_gan_universe", "monthly_merged_data_single_names"]
    
    '''
    Consolidate_data_industry_average_201910.ipynb
    Generate Active Matrix section
    '''    
    
    def __init__(self, start_date, end_date, ref_col):
        self.data = None
        self.start_date = pd.Timestamp(start_date) if start_date else None
        self.end_date = pd.Timestamp(end_date) if end_date else None
        self.ref_col = ref_col
        CalibrationTaskflowTask.__init__(self)

    def _generate_active_matrix(self, singlename_tickers, data):
        dates = pd.DatetimeIndex(list(data["date"].unique()))
        dates.sort_values()
        indicator = data[data["date"].isin(dates) & data["ticker"].isin(singlename_tickers)][["date", "ticker", self.ref_col]]
        indicator["is_active"] = (pd.notnull(indicator[self.ref_col]) & abs(indicator[self.ref_col])>1e-6).astype(int)    

        result = pd.pivot_table(indicator, values="is_active", index=["date"], columns=["ticker"], fill_value=0)
        result = result.sort_index()
        residual_tickers = list(set(singlename_tickers) - set(result.columns))
        for rt in residual_tickers:
            result[rt] = 0

        result = result.loc[self.start_date:self.end_date, singlename_tickers]
        result = result.fillna(0)
        result = result.astype(int)
        return result

    def do_step_action(self, **kwargs):
        gan_universe = kwargs[self.REQUIRES_FIELDS[0]]
        data = kwargs[self.REQUIRES_FIELDS[1]]

        self.start_date = self.start_date if pd.notnull(self.start_date) else self.task_params.start_dt
        self.end_date = self.end_date if pd.notnull(self.end_date) else self.task_params.end_dt
        singlename_tickers = list(gan_universe["dcm_security_id"].unique())
        singlename_tickers.sort()

        self.data = self._generate_active_matrix(singlename_tickers, data)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"start_date": "2000-01-01", "end_date": "2019-12-16", "ref_col": "ret_5B"}


class GenerateActiveMatrixWeekly(GenerateActiveMatrix):

    PROVIDES_FIELDS = ["active_matrix", "active_matrix_weekly"]
    REQUIRES_FIELDS = ["current_gan_universe", "monthly_merged_data_single_names", "weekly_merged_data_single_names"]

    def __init__(self, start_date, end_date, ref_col):
        self.weekly_data = None
        GenerateActiveMatrix.__init__(self, start_date, end_date, ref_col)

    def do_step_action(self, **kwargs):
        gan_universe = kwargs[self.REQUIRES_FIELDS[0]]
        monthly_data = kwargs[self.REQUIRES_FIELDS[1]]
        weekly_data = kwargs[self.REQUIRES_FIELDS[2]]

        self.start_date = self.start_date if pd.notnull(self.start_date) else self.task_params.start_dt
        self.end_date = self.end_date if pd.notnull(self.end_date) else self.task_params.end_dt
        singlename_tickers = list(gan_universe["dcm_security_id"].unique())
        singlename_tickers.sort()

        self.data = self._generate_active_matrix(singlename_tickers, monthly_data)
        self.weekly_data = self._generate_active_matrix(singlename_tickers, weekly_data)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.weekly_data}


class GenerateBMEReturns(CalibrationTaskflowTask):
    
    PROVIDES_FIELDS = ["past_returns_bme", "future_returns_bme"]
    REQUIRES_FIELDS = ["active_matrix", "daily_price_data"]

    '''
    Consolidate_data_industry_average_201910.ipynb
    Generate Target BME Returns section
    '''
    
    def __init__(self):
        self.data = None
        self.future_data = None
        CalibrationTaskflowTask.__init__(self)

    def _generate_raw_returns(self, daily_price_data, gan_universe, trading_freq):
        pivot_data = pd.pivot_table(daily_price_data[["date", "ticker", "close"]], index="date", values = "close", \
                                    columns="ticker", dropna=False).sort_index()
        pivot_data = pivot_data.fillna(method="ffill")
        pivot_data.index = pd.DatetimeIndex(list(map(pd.Timestamp, pivot_data.index))).normalize()
        
        start_date = pivot_data.index.min()
        end_date = pivot_data.index.max()
        if trading_freq == "monthly":
            trading_dates = pick_trading_month_dates(start_date, end_date, "bme")
        elif trading_freq == "weekly":
            trading_dates = pick_trading_week_dates(start_date, end_date, "w-mon")
        else:
            raise ValueError(f"Unsupported trading frequency passed to generate raw returns data - {trading_freq}")
        trading_days = pd.DatetimeIndex(list(trading_dates))
        pivot_data = pivot_data.ix[trading_days]
        raw_returns = pivot_data.pct_change()
        
        for dt in raw_returns.index:
            lb = raw_returns.loc[dt, :].quantile(0.005)
            lb = lb if pd.notnull(lb) else -0.999999999
            ub = raw_returns.loc[dt, :].quantile(0.995)
            ub = ub if pd.notnull(ub) else 2.0
            raw_returns.loc[dt, :] = raw_returns.loc[dt, :].clip(lb, ub)
        
        raw_returns = raw_returns.fillna(0.0)
        residual_tickers = list(set(gan_universe) - set(raw_returns.columns))
        for rt in residual_tickers:
            raw_returns[rt] = 0.0
        return raw_returns

    def do_step_action(self, **kwargs):
        active_matrix = kwargs[self.REQUIRES_FIELDS[0]]
        daily_price_data = kwargs[self.REQUIRES_FIELDS[1]]

        gan_universe = list(active_matrix.columns)
        raw_returns = self._generate_raw_returns(daily_price_data, gan_universe, "monthly")
        future_returns = raw_returns.shift(-1)
        future_returns = future_returns.fillna(0.0)

        self.data = raw_returns.loc[active_matrix.index, gan_universe]
        self.future_data = future_returns.loc[active_matrix.index, gan_universe]
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.future_data}

    @classmethod
    def get_default_config(cls):
        return {}


class GenerateBMEReturnsWeekly(GenerateBMEReturns):

    PROVIDES_FIELDS = ["past_returns_bme", "future_returns_bme", "future_returns_weekly"]
    REQUIRES_FIELDS = ["active_matrix", "active_matrix_weekly", "daily_price_data"]

    def __init__(self):
        self.future_data_weekly = None
        GenerateBMEReturns.__init__(self)

    def do_step_action(self, **kwargs):
        active_matrix = kwargs[self.REQUIRES_FIELDS[0]]
        active_matrix_weekly = kwargs[self.REQUIRES_FIELDS[1]]
        daily_price_data = kwargs[self.REQUIRES_FIELDS[2]]
        gan_universe = list(active_matrix.columns)

        raw_returns = self._generate_raw_returns(daily_price_data, gan_universe, "monthly")
        future_returns = raw_returns.shift(-1).fillna(0.0)
        self.data = raw_returns.loc[active_matrix.index, gan_universe]
        self.future_data = future_returns.loc[active_matrix.index, gan_universe]

        raw_returns_weekly = self._generate_raw_returns(daily_price_data, gan_universe, "weekly")
        future_returns_weekly = raw_returns_weekly.shift(-1).fillna(0.0)
        self.future_data_weekly = future_returns_weekly.loc[active_matrix_weekly.index, gan_universe]
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.future_data,
                self.__class__.PROVIDES_FIELDS[2] : self.future_data_weekly}
