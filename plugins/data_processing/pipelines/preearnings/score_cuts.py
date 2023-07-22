from __future__ import division

import csv
import json
from itertools import combinations, chain, product, ifilter

import pandas as pd
import numpy as np
from luigi import IntParameter, ListParameter
from scipy import stats
from scipy.stats import norm

from base import DCMTaskParams, PySparkTask


class ScoreCutsTask(DCMTaskParams, PySparkTask):
    executor_memory = "15G"
    max_size = IntParameter(description="Max size of cut combination", default=3)
    fundamentals = ListParameter(description="List of fundamentals")
    offsets = ListParameter(description="Offsets", default=[])
    udfs = ListParameter(description="Udfs", default=[])
    sectors = []

    def run_spark(self):
        with self.input()["sector_mapping"].engine() as engine:
            select_str = "SELECT DISTINCT(sector) FROM sector_industry_mapping;"
            data = pd.read_sql(select_str, engine)
            self.sectors = list(data["sector"])

        fundamentals = list(self.fundamentals)
        offsets = list(self.offsets)
        udfs = list(self.udfs)
        max_size = int(self.max_size)
        sectors = list(self.sectors)

        combinations = list(generate_combinations(fundamentals=fundamentals, offsets=offsets, udfs=udfs,
                                                  sectors=sectors, max_size=max_size))

        with self.input()["digitized_result"].engine() as engine:
            data = pd.read_sql(self.input()["digitized_result"].table, engine)

        data = self.spark_context.broadcast(data)
        def mapper(iterator):
            data_df = data.value

            for combination in iterator:
                print(combination)
                filter, fundamentals = combination
                sample_sum_stats, sample_pivot = _get_pivot(data_df, filter, [f for f in fundamentals], "result")
                score = score_combination(sample_pivot, sample_sum_stats, repr(combination))

                yield combination, score
        scored_combinations = self.spark_context.parallelize(list(combinations), 512).mapPartitions(mapper).collect()

        with self.output().open("w") as output_fd:
            self.save_scored_combinations(scored_combinations, output_fd)

    def save_scored_combinations(self, combinations, fd):
        writer = csv.writer(fd, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        for (filter, fundamentals), score in combinations:
            print((filter, fundamentals))
            writer.writerow([json.dumps(filter) + "~" + "~".join(
                [f for f in fundamentals]), str(score)])


class Filter(dict):

    def apply_to_data(self, data):
        for field, rule in self.items():
            if type(rule) in (list, tuple):
                data = data[data[field].isin(rule)]
            else:
                data = data[data[field] == rule]

        return data


def mad(data, c=norm.ppf(3/4.0), axis=0, center=np.median):
    data = np.asarray(data)
    data = data[~np.isnan(data)]
    if callable(center):
        center = np.apply_over_axes(center, data, axis)
    return np.median((np.fabs(data-center))/c, axis=axis)


def filter_outliers(data, mad_thresh=5):
    med = np.median(data)
    m = mad(data)
    current_dev = np.abs(data - med)/m

    return data[current_dev < mad_thresh]


def medcouple_sk(data):
    if data.ndim == 1:
        data = data.reshape((len(data), 1))

    data = np.sort(data)

    score = data - np.median(data)
    score_u = score[score >= 0.0]
    score_l = score[score <= 0.0]

    denom = score_u - score_l
    denom[np.logical_and(score_l == 0.0, score_u == 0.0)] = np.inf
    numer = score_u + score_l

    return np.median(numer/denom)


def sentiment_compressor(score, center, slope):
    score = score * 1.0
    const_n = slope - center
    const_d = slope + center
    return 0.5*(np.log((const_n+score)/(const_d-score)))


def area_under(mu_1, sig_1, mu_2, sig_2):
    diff_var = sig_1**2 - sig_2**2
    c = (mu_2 * sig_1**2 - sig_2 * (
        mu_1 * sig_2 + sig_1 * np.sqrt(
            (mu_1-mu_2)**2 + 2 * diff_var * np.log(sig_1 / sig_2)
        ))) / diff_var
    auc = 1 - norm.cdf((c-mu_1)/sig_1) + norm.cdf((c-mu_2)/sig_2)
    return auc


def segment_score(df, total_stats, cuts=10, obs_thresh=30):
    df = df[df['count'] >= obs_thresh].sort_values(by=['mean'])
    df = df.reset_index()
    df = df[['mean', 'std', 'count']]
    if len(df) > 2 * cuts:
        df = pd.concat((df[:cuts], df[-cuts:]))
    df = df.reset_index()

    ss_b = df['count']*(df['mean'] - total_stats['mean'])**2
    ss_w = (df['count']-1.0)*df['std']**2

    N = df['count'].sum()
    k = len(df)

    f_stat = ss_b.sum()*(N-k)/ss_w.sum()/(k-1.0)
    p = stats.f.cdf(f_stat, k-1, N-k)
    p = max(min((p, 0.9999)), 0.0001)
    return np.log(p/(1.0-p))


def segment_score_transform(df, total_stats, cuts=10, obs_thresh=30,
                            transform=False):
    df.dropna(inplace=True)
    df = df[df['count'] >= obs_thresh].sort_values(by=['mean'])
    df = df.reset_index()
    df = df[['mean', 'std', 'count']]
    if len(df) > 2 * cuts:
        df = pd.concat((df[:cuts], df[-cuts:]))
    df = df.reset_index()

    if transform:
        center = total_stats['mean']
        rng = df['mean'].max() - df['mean'].min()
        transformed_means = df['mean'].apply(
            lambda x: sentiment_compressor(x, center, rng/1.25))
    else:
        transformed_means = df['mean']

    # transformed_means.plot()
    # plt.show()
    ss_b = df['count']*(transformed_means - total_stats['mean'])**2
    ss_w = (df['count']-1.0)*df['std']**2

    N = df['count'].sum()
    k = len(df)

    f_stat = ss_b.sum()*(N-k)/ss_w.sum()/(k-1.0)
    p = stats.f.cdf(f_stat, k-1, N-k)
    p = max(min((p, 0.99999)), 0.00001)
    return np.log(p/(1.0-p))


def segment_auc(df, total_stats, cuts=10, obs_thresh=30, transform=False,
                ref_thresh=0.9):
    df.dropna(inplace=True)
    df = df[df['count'] >= obs_thresh].sort_values(by=['mean'])

    if len(df) < 2:
        return -10000.0

    df = df.reset_index(drop=True)
    df = df[['mean', 'std', 'count']]
    if len(df) > 2 * cuts:
        df = pd.concat((df[:cuts], df[-cuts:]))
    df = df.reset_index(drop=True)

    if transform:
        center = total_stats['mean']
        rng = df['mean'].max() - df['mean'].min()
        transformed_means = df['mean'].apply(
            lambda x: sentiment_compressor(x, center, rng/1.25))
    else:
        transformed_means = df['mean']

    ss_b = df['count'] * (transformed_means - total_stats['mean']) ** 2
    ss_w = (df['count']-1.0)*df['std']**2

    N = df['count'].sum()
    k = len(df)

    f_stat = ss_b.sum()*(N-k)/(ss_w.sum()*(k-1.0))
    p = stats.f.cdf(f_stat, k-1, N-k)

    if p < ref_thresh:
        return -10000.0

    num_rows = len(df)
    bad_df = df.ix[:num_rows/2]
    good_df = df.ix[num_rows/2:]

    total_bad = 1.0*bad_df['count'].sum()
    total_good = 1.0*good_df['count'].sum()
    bad_mu = (bad_df['mean']*bad_df['count']).sum()/total_bad
    bad_var = (bad_df['count']*bad_df['std']**2).sum()/total_bad
    bad_sig = np.sqrt(bad_var)
    good_mu = (good_df['mean']*good_df['count']).sum()/total_good
    good_var = (good_df['count']*good_df['std']**2).sum()/total_good
    good_sig = np.sqrt(good_var)

    sig = np.sqrt(bad_var/total_bad + good_var/total_good)
    numer = (bad_var/total_bad + good_var/total_good)**2
    denom = (bad_var/total_bad)**2/(total_bad-1.0) +\
        (good_var/total_good)**2/(total_good-1.0)
    dof = numer/denom

    auc = area_under(bad_mu, bad_sig, good_mu, good_sig)
    t_stat = (good_mu - bad_mu)/sig
    p = stats.t.cdf(t_stat, dof)
    p = max(min((p, 0.99999)), 0.00001)

    if p < ref_thresh:
        return -10000.0
    auc = max(min((auc, 0.99999)), 0.00001)
    p = 1.0 - auc
    return np.log(p/(1.0 - p))


def segment_fraction(df, total_stats, fraction=0.1, obs_thresh=30,
                     transform=False, ref_thresh=0.9, name=None):
    df.dropna(inplace=True)
    df = df[df['count'] >= obs_thresh].sort_values(by=['mean'])

    if len(df) < 2:
        return -10000.0

    df = df.reset_index(drop=True)
    df = df[['mean', 'std', 'count']]

    q = fraction/2.
    counts = df['count']
    selected_bins_lower = counts[(counts.cumsum() / counts.sum()).apply(
        lambda x:True if x <= q else False)].index
    selected_bins_upper = counts[(counts.cumsum() / counts.sum()).apply(
        lambda x:True if x >= 1.0-q else False)].index
    selected_bins = selected_bins_lower.union(selected_bins_upper)

    df = df.ix[selected_bins]
    bad_df = df.ix[selected_bins_lower]
    good_df = df.ix[selected_bins_upper]

    if transform:
        center = total_stats['mean']
        rng = df['mean'].max() - df['mean'].min()
        transformed_means = df['mean'].apply(
            lambda x: sentiment_compressor(x, center, rng/1.25))
    else:
        transformed_means = df['mean']

    ss_b = df['count']*(transformed_means - total_stats['mean'])**2
    ss_w = (df['count']-1.0)*df['std']**2

    N = df['count'].sum()
    k = len(df)

    f_stat = ss_b.sum()*(N-k)/(ss_w.sum()*(k-1.0))
    p = stats.f.cdf(f_stat, k-1, N-k)

    if p < ref_thresh:
        return -10000.0

    try:
        total_bad = 1.0*bad_df['count'].sum()
        total_good = 1.0*good_df['count'].sum()
        bad_mu = (bad_df['mean']*bad_df['count']).sum()/total_bad
        bad_var = (bad_df['count']*bad_df['std']**2).sum()/total_bad
        bad_sig = np.sqrt(bad_var)
        good_mu = (good_df['mean']*good_df['count']).sum()/total_good
        good_var = (good_df['count']*good_df['std']**2).sum()/total_good
        good_sig = np.sqrt(good_var)

        sig = np.sqrt(bad_var/total_bad + good_var/total_good)
        numer = (bad_var/total_bad + good_var/total_good)**2
        denom = (bad_var/total_bad)**2/(total_bad-1.0) +\
            (good_var/total_good)**2/(total_good-1.0)
        dof = numer/denom

        auc = area_under(bad_mu, bad_sig, good_mu, good_sig)
        t_stat = (good_mu - bad_mu)/sig
        p = stats.t.cdf(t_stat, dof)
        p = max(min((p, 0.99999)), 0.00001)

        if p < ref_thresh:
            return -10000.0
        auc = max(min((auc, 0.99999)), 0.00001)
        p = 1.0 - auc
    except:
        print("error witht the following cut:", name)
        return -10000.0
    return np.log(p/(1.0 - p))


def score_combination(tb, total_stats, name):
    long_format_df = tb.reset_index(col_level=1)
    long_format_df.columns = long_format_df.columns.droplevel(0)

    # return segment_auc(long_format_df, total_stats, cuts=10, obs_thresh=10,
    # ref_thresh=0.95)
    try:
        score = segment_fraction(long_format_df, total_stats, fraction=0.2,
                                 obs_thresh=10, ref_thresh=0.95, name=name)
    except:
        score = -10000
    return score


def generate_combinations(fundamentals, offsets, udfs, sectors, max_size,
                          required_fundamentals=None):
    required_fundamentals = required_fundamentals or []
    assert len(required_fundamentals) < max_size

    if required_fundamentals:
        fundamentals = set(fundamentals) - set(required_fundamentals)

    fundamentals_combinations = chain(
        *[combinations(fundamentals, size) for size in
          xrange(1, max_size - len(required_fundamentals) + 1)]
    )

    if required_fundamentals:
        fundamentals_combinations = product(
            required_fundamentals,
            fundamentals_combinations
        )

    filters = [
        Filter({"offset": offset, "udf_name": udf, "sector": sector})
        for offset in offsets
        for udf in udfs
        for sector in sectors
    ]

    return product(filters, ifilter(lambda combination: len(combination) > 1, fundamentals_combinations))


def _get_pivot(data, filter, pivot_columns, target_col):
    functions_to_add_in_pivot = {
        "mean": pd.np.mean,
        "std": pd.np.std,
        "median": pd.np.median,
        "count": len,
        "max": max,
        "min": min,
    }

    filtered_data = filter.apply_to_data(data)

    sample_sum_stats = filtered_data[target_col].describe()
    sample_pivot = (
        filtered_data
        .groupby(list(pivot_columns))
        .agg({target_col: functions_to_add_in_pivot})
    )

    return sample_sum_stats, sample_pivot

