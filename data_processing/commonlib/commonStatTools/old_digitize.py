#!/usr/bin/env python
# coding:utf-8

import numpy as np
import pandas as pd
from scipy.stats import norm as normal
from bisect import bisect_left, bisect_right
from statsmodels.robust.scale import mad

def cut_modified(x, q, use_mad_for_std=False):
    try:
        quantiles_in_sigmas = np.asarray(map(normal.ppf, q))
        x_clean = x.dropna()
        mean = np.mean(x_clean)
        std = np.std(x_clean) if not use_mad_for_std else mad(x_clean)
        bins = mean + quantiles_in_sigmas * std
        bins = np.sort(np.append(bins, (x_clean.min() - 1E-6, x_clean.max() + 1E-6)))
        # bins = np.sort(np.append(bins, (0.0, 1.0)))
        return pd.cut(x, bins, labels=range(len(bins) - 1))
    except ValueError as e:
        # print len(x_clean)
        # print "{0} group had an exception {1}".format(x.name, e)
        return [pd.np.NaN] * len(x)

def add_digitized_columns_given_input_filter(df_orig, columns_list, cut_point_list, based_on_filter,
                                             quantile_cut_point_format=True,
                                             digitized_columns_names=[],
                                             use_mad_for_std=False):
    df = df_orig.copy()
    filter_name = '*'.join(['_Digital'] + based_on_filter)
    if not digitized_columns_names:
        digitized_columns_names = map(lambda x: x + filter_name, columns_list)
    print(digitized_columns_names)
    if not based_on_filter:
        if quantile_cut_point_format:
            for k, col in enumerate(columns_list):
                df[digitized_columns_names[k]] = cut_modified(df[col], cut_point_list, use_mad_for_std)
        else:
            for k, col in enumerate(columns_list):
                df[digitized_columns_names[k]] = pd.cut(df[col], cut_point_list, labels=range(len(cut_point_list) - 1))

    else:
        df_groups = df.groupby(based_on_filter)
        if quantile_cut_point_format:
            for k, col in enumerate(columns_list):
                # df[digitized_columns_names[k]] = df_groups[col].transform(lambda x: pd.qcut(x,cut_point_list,
                # labels =digital_vals))
                df[digitized_columns_names[k]] = df_groups[col].transform(lambda x: cut_modified(x, cut_point_list, use_mad_for_std))
        else:
            for k, col in enumerate(columns_list):
                df[digitized_columns_names[k]] = df_groups[col].transform(lambda x: pd.cut(x, cut_point_list,
                                                                                           labels=range(
                                                                                               len(cut_point_list) - 1)))
    return df, digitized_columns_names

