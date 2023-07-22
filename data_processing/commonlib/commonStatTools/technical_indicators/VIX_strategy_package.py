from collections import deque

#from online_indicators_cython_copy import *
import offline

import numpy as np
import inspect
from functools import partial
import json
import pandas as pd
import multiprocessing
from scipy.signal import medfilt
import itertools
import time

from multiprocessing import Pool, TimeoutError



def prepare_dataframe_for_performance_analysis(df, holdings_columns, price_column = None,
                                               returns_columns = None, date_column= None, date_convention ="end",
                                               holdings_convention = "start", pricing_convention = "end"):
    if price_column and returns_columns:
        raise ValueError("Only on of price_column or returns_column should be provided")
    if holdings_convention not in ["start", "s", "e", "end"]:
        raise
    if holdings_convention not in ["start", "s", "e", "end"]:
        raise

    data = df.copy()
    if date_column:
        data.set_index(date_column, inplace = True)
    data.index = pd.DatetimeIndex(data.index)
    data = data[[price_column or returns_columns, holdings_columns]]
    data.columns = ["returns", "holdings"]
    data.sort_index(inplace=True)
    if price_column:
        data["returns"] = data["returns"].pct_change()
    holding_shift = 0 if holdings_convention[0]==date_convention[0] else (1 if date_convention[0]=="e" else -1)
    pricing_shift = 0 if pricing_convention[0]==date_convention[0] else (1 if date_convention[0]=="e" else -1)
    data["returns"] = data["returns"].shift(pricing_shift)
    data["holdings"] = data["holdings"].shift(holding_shift)
    return data

def hit_ratio(input_array):
    number_of_pos = len(input_array[input_array>0])
    number_of_neg = len(input_array[input_array<=0])
    return np.float(number_of_pos)/(number_of_pos+number_of_neg)




def get_performance_from_cython(perf, xiv_with_ivts, holdings_columns, price_column = None, returns_columns = None,
                                date_column= None, date_convention ="end", holdings_convention = "start", pricing_convention = "end"):
    res = prepare_dataframe_for_performance_analysis(xiv_with_ivts, holdings_columns, price_column, returns_columns,
                                                     date_column, date_convention, holdings_convention, pricing_convention).dropna()
    values = perf.calculate_measure(np.asanyarray(res['holdings'].values), np.asanyarray(res['returns'].values),
                                    True);
    return values,res


def get_xiv_with_ivts(df, median_parameter, mean_parameter):
    xiv_with_ivts = df.copy()
    xiv_with_ivts = xiv_with_ivts.sort_values(by=['Date'])

    xiv_with_ivts['Yesterday_ivts_median'] = pd.expanding_apply(xiv_with_ivts['Yesterday_ivts'], lambda x:medfilt(x, median_parameter)[-1])
    #xiv_with_ivts['Yesterday_ivts_median'] = pd.rolling_apply(xiv_with_ivts['Yesterday_ivts'],3,np.min)
    #xiv_with_ivts['Yesterday_ivts_median'] = pd.rolling_apply(xiv_with_ivts['Yesterday_ivts'],median_parameter,np.median)
    xiv_with_ivts['Yesterday_ivts_mean_of_median'] = pd.rolling_apply(xiv_with_ivts['Yesterday_ivts'],mean_parameter,np.mean)
    xiv_with_ivts['Yesterday_ivts_diff_with_mean'] = xiv_with_ivts['Yesterday_ivts_median'] - xiv_with_ivts['Yesterday_ivts_mean_of_median']
    xiv_with_ivts = xiv_with_ivts.sort_values(by=['Date'], ascending=False)
    return xiv_with_ivts

def get_results_for_a_specific_param_for_parralel_run(data_org, params, perf, price_col, combination_operator ,
                                                      signal_name = "signal",
                                                      how_to_combine = None):
    data = data_org.copy()
    all_constraints = []
    for param in params:
        #if param != "mean_parameter":
        for this_param in params[param]:
            all_constraints.append('({0}{1}{2})'.format(param,this_param[0],this_param[1]))
    if how_to_combine:
        combination_query = how_to_combine.format(*all_constraints)
    else:
        combination_operator = " " + combination_operator + " "
        combination_query = combination_operator.join(all_constraints)
    sub_select = data.query(combination_query).index
    data[signal_name] = pd.Series(1, index=sub_select).reindex(data.index).fillna(0).sort_index()
    good_values,res, df = get_detail_for_a_subset(perf,data,signal_name,price_col)
    res = res.reset_index().rename(columns = {'index':'Date'})
    return (good_values,res,df)

def get_detail_for_a_subset(perf, xiv_with_ivts, signal_col, price_col ):
    subset_values,res = get_performance_from_cython(perf, xiv_with_ivts[["Date",signal_col,price_col]], signal_col, price_column = price_col,
                                          returns_columns = None, date_column= "Date", date_convention ="end",
                                          holdings_convention = "start", pricing_convention = "end")
    subset = xiv_with_ivts[(xiv_with_ivts[signal_col] - 1.0).abs()<1E-8]
    return subset_values,res, xiv_with_ivts

def f(x, this_eps_spy=0.01, this_thresh=1.19):
    return 1.0 if ((x[0]<this_eps_spy) and (x[1]<this_thresh)) else 0.0

def f_prime(x, this_eps_spy=0.01, this_thresh=1.19):
    return 1.0 if ((x[0]>=this_eps_spy) or (x[1] >=this_thresh)) else 0.0




def get_measures():
    fee_amount = 0.0
    filter_to_calculate_measure = [1.0]
    measure_1 = GenericPerformanceIndicatorPerSegments(fee_amount, TechnicalStrategyPerformance(fee_amount), np.mean,
                                                       filter_to_calculate_measure)
    measure_2 = TechnicalStrategyHitRatio(fee_amount)
    measure_3 = TechnicalStrategyPerformance(fee_amount)
    measure_4 = GenericPerformanceIndicatorPerSegments(fee_amount, TechnicalStrategyHitRatio(fee_amount), np.mean,
                                                       filter_to_calculate_measure)

    measure_5 = GenericPerformanceIndicatorPerSegments(fee_amount, TechnicalStrategyPerformance(fee_amount), np.max,
                                                       filter_to_calculate_measure)

    measure_11 = GenericPerformanceIndicatorPerSegments(fee_amount, TechnicalStrategyPerformance(fee_amount), np.min,
                                                       filter_to_calculate_measure)

    measure_9 = GenericPerformanceIndicatorPerSegments(fee_amount, TechnicalStrategyPerformance(fee_amount), np.std,
                                                       filter_to_calculate_measure)

    measure_10 = GenericPerformanceIndicatorPerSegments(fee_amount, TechnicalStrategyPerformance(fee_amount), len,
                                                        filter_to_calculate_measure)

    measure_6 = GenericPerformanceIndicatorPerSegments(fee_amount, TechnicalStrategyPerformance(fee_amount), hit_ratio,
                                                       filter_to_calculate_measure)

    measure_7 = GenericPerformanceIndicatorPerSegments(fee_amount, TechnicalStrategyTimeLenght(fee_amount), np.mean,
                                                       filter_to_calculate_measure)

    measure_8 = GenericPerformanceIndicatorPerSegments(fee_amount, TechnicalStrategyTimeLenght(fee_amount), np.sum,
                                                       filter_to_calculate_measure)

    measure_dict = {"mean_segment_perf": measure_1, "hit_ratio": measure_2, "perf": measure_3,
                    "hit_ratio_per_segment": measure_4, "max_segment_perf": measure_5, "min_segment_perf": measure_11,
                    "actual_hit_ratio": measure_6, "average_time_length" : measure_7, "total_days_in_market" : measure_8,
                    "std_segment_perf":measure_9, "number_of_segmetns" : measure_10}

    perf = PerformanceSummary(measure_dict)
    return perf



class PerformanceForDataOld(object):
    def __init__(self, data, args_list, ticker, mean_param, perf, median_param = 5):
        self.data = data
        self.args_list = args_list
        self.results = []
        self.ticker = ticker
        self.median_param = median_param
        self.mean_param = mean_param
        self.xiv_with_ivts = get_xiv_with_ivts(self.data, self.median_param, self.mean_param)
        self.function_to_apply = f if self.ticker == "SPY" else f_prime
        self.perf = perf

    def calculate(self):
        for (eps_spy,k) in self.args_list:
            self.xiv_with_ivts["SPY_signal"] = self.xiv_with_ivts[['Yesterday_ivts_diff_with_mean',
                                                                   'Yesterday_ivts_mean_of_median']].apply(self.function_to_apply,
                                                                                                 this_eps_spy = eps_spy,
                                                                                                 this_thresh = k,
                                                                                                 axis=1)
            if self.xiv_with_ivts["SPY_signal"].sum()>0:
                values,res, df = get_detail_for_a_subset(self.perf, self.xiv_with_ivts, "SPY_signal", self.ticker + " Open" )
                values["mean_parameter"] = self.mean_param
                values["eps_spy"] = eps_spy
                values["thresh"] = k
                values["ticker"] = self.ticker
                self.results.append(values)
        return self.results

    def get_output_as_dataframe(self):
        return pd.DataFrame(self.results)



class PerformanceForData(object):

    def __init__(self, data, df_params_xiv, ticker, perf, how_to_combine, combination_operator):
        self.data = data
        self.results = []
        self.ticker = ticker
        self.df_params_xiv = df_params_xiv
        self.perf = perf
        self.how_to_combine = how_to_combine
        self.combination_operator = combination_operator

    def calculate(self):
        values, res, df = get_results_for_a_specific_param_for_parralel_run(self.data, self.df_params_xiv, self.perf,
                                                                            self.ticker + " Open", self.combination_operator,
                                                                            signal_name = "signal",  how_to_combine = self.how_to_combine)

        if df["signal"].sum()>0:
            for param in self.df_params_xiv:
                for param_value in self.df_params_xiv[param]:
                    values[param + "_" + param_value[0]] = param_value[1]
            values["ticker"] = self.ticker
            self.results.append(values)

        return self.results




def calc(obj):
    res = obj.calculate()
    print("Done")
    return res