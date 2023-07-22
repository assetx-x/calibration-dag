import pandas as pd
import numpy as np
from collections import OrderedDict
from statsmodels.stats import proportion, weightstats
from scipy.stats import bayes_mvs

def calculate_periods(signal_values_original):
    signal_values = pd.np.sign(signal_values_original)
    periods_starts = signal_values.index[signal_values.diff().fillna(value=0) != 0].insert(0, signal_values.index[0])
    return_summary = signal_values[periods_starts].to_frame(name="signal_value")
    return_summary.index.name = "entry_time"
    return_summary.reset_index(inplace=True)
    return_summary["exit_time"] = return_summary["entry_time"].shift(-1).fillna(signal_values.index[-1])
    return_summary["signal_value"] = signal_values_original.reindex(return_summary["entry_time"]).values
    return_summary["signal_asset"] = return_summary["signal_value"].apply(lambda x:"Long" if x>0
                                                                          else ("Short" if x<0 else ""))
    return return_summary

def calculate_return(df, additive_tc=0.01, multiplicative_tc = 0.001):
    df["Long_Return"] = (df["Exit_Price"]*(1-multiplicative_tc)-additive_tc)/(df["Entry_Price"]*(1+multiplicative_tc)+additive_tc)-1.0
    df["Short_Return"] = 1-(df["Exit_Price"]*(1+multiplicative_tc)+additive_tc)/(df["Entry_Price"]*(1-multiplicative_tc)-additive_tc)
    return df

def get_number_of_tickers_in_each_interval(subset, return_col, grouping_by="entry_date"):
    if isinstance(grouping_by, str):
        grouping_by = [grouping_by]
    res = subset.dropna(subset=[return_col]).groupby(grouping_by).apply(len)
    count = len(res)
    return {"num_count" : count,
            "num_mean" : (count and res.mean()) or pd.np.NaN,
            "num_median" : (count and res.median()) or pd.np.NaN,
            "num_max" : (count and res.max()) or pd.np.NaN,
            "num_min" : (count and res.min()) or pd.np.NaN}

def _calculate_averages(subset, return_col, tickers_summary_how, groupbed_by):
    res = subset.dropna(subset = [return_col]).groupby(groupbed_by)[return_col].agg(tickers_summary_how).reset_index()
    #TODO: this is not generic enough, assumes a specific form of tickers_summary_how
    dict_to_return = {"weighted_average" : res["total"].sum()/res["count"].sum() if res["count"].sum() else pd.np.NaN,
                      "simple_average" : res["mean"].mean()}
    return dict_to_return

def get_stat(series, thresh, include_ci = True):
    mean_val = series.mean()
    std_val = series.std(ddof=1)
    median_val = series.median()
    max_val = series.max()
    min_val = series.min()
    num_val = len(series)
    hit_val = 100*pd.np.float(len(series[series>thresh]))/(len(series)+0.00001)
    hit_val_ci = include_ci and len(series)>=1 and proportion.proportion_confint((series>thresh).sum(), len(series), method="jeffrey")
    mean_val_ci = include_ci and len(series)>=2 and bayes_mvs(series)[0].minmax
    return {"mean" : mean_val,
            "median" :median_val,
             "max" : max_val,
             "min" :min_val,
             "count" :num_val,
             "hit_ratio": hit_val,
             "hit_ratio_interval": hit_val_ci,
             "mean_interval": mean_val_ci}

def get_summary_of_a_subset(subset, return_col, tickers_summary_how=None, grouped_by=None):
    grouped_by = grouped_by or ["entry_date","exit_date","interval"]
    tickers_summary_how = tickers_summary_how or {"count" : len, "mean" : pd.np.mean, "median": pd.np.median,
                                                  "total" : pd.np.sum, "max" : pd.np.max, "min": pd.np.min,
                                                  "hit_ratio" : lambda x: len(x[x>0])/pd.np.float(len(x)) }
    stat_dict = {}
    per_interval_perf = get_stat(subset[return_col].dropna(),0.0) if len(subset)>0 else {}
    stat_dict.update(per_interval_perf)

    #number_of_tickers_dict = get_number_of_tickers_in_each_interval(subset, return_col)
    #stat_dict.update(number_of_tickers_dict)
    #average_dict = _calculate_averages(subset, return_col, tickers_summary_how, grouped_by) if len(subset)>0 else {}
    #stat_dict.update(average_dict)
    return stat_dict

def pretty_print(data_dict):
    #for k in ["count","mean","median","hit_ratio","min","max","hit_ratio_interval","mean_interval"]:
    for k in ["count","mean","median","hit_ratio","min","max"]:
        print(k +" : " + str(data_dict[k]))
    print("---------------------------------------")
    for k in ["num_count","num_mean","num_median","num_min","num_max"]:
        print(k +" : " + str(data_dict[k]))
    print("---------------------------------------")
    for k in ["weighted_average","simple_average"]:
        print(k +" : " + str(data_dict[k]))

