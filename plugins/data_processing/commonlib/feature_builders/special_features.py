import pandas as pd
from fractions import Fraction

from commonlib.commonStatTools.technical_indicators.offline import *

try:
    from pandas.lib import checknull as pd_checknull
except ImportError:
    from pandas._libs.missing import checknull as pd_checknull

DEFAULT_ETF_PEER_RANKING_ETF_LIST =  list(set(['IVE', 'IWD', 'DIA', 'VTV','IVV', 'IWB', 'IWV', 'VV', 'RSP','IVW','SPY',
                                      'DVY','IJJ', 'IJH', 'IWR', 'MDY','IJK','IWN','IJR','IWM','SCHA','IWO']))

DEFAULT_TECHNICAL_INDICATOR_FEATURE_NAME = "price_oscillator"
DEFAULT_TECHNICAL_INDICATOR_FEATURE_PARAMS = {"nslow": 26, "nfast": 12}
DEFAULT_TECHNICAL_INDICATORS_ETF_LIST = DEFAULT_ETF_PEER_RANKING_ETF_LIST + ["TLT", "VCIT"]

DEFAULT_ADX_FEATURE_PARAMS = {"timeperiod": 14}
DEFAULT_RSI_FEATURE_PARAMS = {"timeperiod": 14}
DEFAULT_FRAC_DIFF_FEATURE_PARAMS = {"differencing": 0.6, "threshold": 1e-3}
DEFAULT_MA_FEATURE_PARAMS = {"window_size": 5, "type":"simple", "adjust":True}

DEFAULT_LOOKBACK_FOR_CORRELATION = 63

def ivts_feature_builder(numerator_series, denominator_series, medfilt_lookback_in_days, look_back_for_mean_in_days):
    iv_ts = numerator_series/denominator_series
    ivts_medianfiltered = pd.rolling_min(iv_ts, medfilt_lookback_in_days, medfilt_lookback_in_days).replace({0.0:1.0})
    average_iv_ts = pd.rolling_mean(iv_ts, look_back_for_mean_in_days, look_back_for_mean_in_days)
    return (iv_ts, ivts_medianfiltered, average_iv_ts)

class ETFPeerRankingFeatureBuilder(object):
    def __init__(self, benchmark_peer_list=None):
        self.benchmark_peer_list = benchmark_peer_list or DEFAULT_ETF_PEER_RANKING_ETF_LIST

    def get_peer_etf_list(self):
        return self.benchmark_peer_list

    def rank_values(self, x):
        rnk = x.rank().astype(float)
        max_rnk = rnk.max()
        if not pd_checknull(max_rnk):
            max_rnk = int(max_rnk)
            rnk = rnk.map(lambda x:Fraction(int(x), max_rnk) if not pd_checknull(x) else pd.np.NaN)
        return rnk

    def __call__(self, indicators_df):
        indicator_rankings = indicators_df[self.benchmark_peer_list].apply(self.rank_values,axis=1)
        return indicator_rankings

class TechnicalIndicatorFeatureBuilder(object):
    def __init__(self, technical_indicator_name=None, technical_indicator_params=None, technical_indicators_tickers=None):
        self.technical_indicator_name = technical_indicator_name or DEFAULT_TECHNICAL_INDICATOR_FEATURE_NAME
        self.technical_indicator_params = technical_indicator_params or DEFAULT_TECHNICAL_INDICATOR_FEATURE_PARAMS
        self.technical_indicators_tickers = technical_indicators_tickers or DEFAULT_TECHNICAL_INDICATORS_ETF_LIST

    def __acquire_technical_indicator(self):
        indicator_func = globals()[self.technical_indicator_name]
        indicator = lambda x:indicator_func(x, **self.technical_indicator_params)
        return indicator

    def get_indicator_etf_list(self):
        return self.technical_indicators_tickers

    def __call__(self, prices_df):
        indicator_func = self.__acquire_technical_indicator()
        all_indicators = pd.DataFrame(index=prices_df.index)
        for ticker in self.technical_indicators_tickers:
            all_indicators[ticker] = indicator_func(prices_df[ticker])
        return all_indicators

class ADXIndicatorFeatureBuilder(TechnicalIndicatorFeatureBuilder):
    def __init__(self, technical_indicator_params=None, technical_indicators_tickers=None):
        super(ADXIndicatorFeatureBuilder, self).__init__("talib_ADX", technical_indicator_params, technical_indicators_tickers)

    def __acquire_technical_indicator(self):
        indicator_func = globals()[self.technical_indicator_name]
        indicator = lambda x, y, z:indicator_func(x, y, z, **self.technical_indicator_params)
        return indicator

    def __call__(self, high_df, low_df, close_df):
        indicator_func = self.__acquire_technical_indicator()
        all_indicators = pd.DataFrame(index=close_df.index)
        for ticker in self.technical_indicators_tickers:
            all_indicators[ticker] = indicator_func(high_df[ticker], low_df[ticker], close_df[ticker])
        return all_indicators


def get_return_correlations(prices_df, reference_names, correlation_lookback=None, include_correlation_ranking=False,
                            melt_data=False):
    correlation_lookback = correlation_lookback or DEFAULT_LOOKBACK_FOR_CORRELATION
    return_data = pd.np.log(prices_df.sort_index(ascending=True)).diff()
    return_data_mean = pd.stats.moments.rolling_mean(return_data, correlation_lookback, correlation_lookback)
    return_data_std = pd.stats.moments.rolling_std(return_data, correlation_lookback, correlation_lookback)
    return_data = (return_data - return_data_mean) / return_data_std
    benchmark_df = return_data[reference_names]
    correlations = {"{0}_correl".format(i): pd.stats.moments.rolling_corr(return_data, benchmark_df.loc[:, i],
                                                                          correlation_lookback,
                                                                          correlation_lookback).shift()
                    for i in benchmark_df.columns}
    if include_correlation_ranking:
        rank_correlations = {"{0}_rank".format(i): correlations[i].rank(axis=1) for i in correlations}
        correlations.update(rank_correlations)
    if melt_data:
        all_correlation_data = []
        for concept in correlations:
            this_data = pd.melt(correlations[concept].reset_index(), var_name="ticker",
                                id_vars="date",value_name=concept).set_index(["date","ticker"])
            all_correlation_data.append(this_data)
        result = pd.concat(all_correlation_data, axis=1)
    else:
        result = correlations
    return result