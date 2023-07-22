import pandas as pd
import numpy as np
import os, sys, glob
from sklearn.decomposition import PCA, RandomizedPCA
from sklearn.covariance import GraphLasso, GraphLassoCV, shrunk_covariance
from sklearn.cluster import KMeans, affinity_propagation
from scipy.stats import zscore
import statsmodels.api as sma
import warnings
from math import isnan

from commonlib.market_timeline import marketTimeline
from commonlib.subjects import *

from pandas.tseries.frequencies import to_offset
#from commonlib.commonStatTools.grassman_average import TGA
#from commonlib.commonStatTools.robust_pca import inexact_augmented_lagrange_multiplier, go_dec

DATA_DIR = r"D:\DCM\temp\etf_experiment"

config = {
    "num_comp": 5,
    "price_column": "close",
    "lookback": "126B",
    "SPY_beta_bound": 1.5
}

cal_config = {

}

def load_data(data_location, target_fields=['daily_price_data', 'merged_data']):
    results = {}
    with pd.HDFStore(data_location, "r") as store:
        for el in target_fields:
            print(el)
            results[el] = store["/pandas/{0}".format(el)]
    print("done")
    return results

def pca_coeff_for_day(daily_price_data, stock_list, num_comp, date=None, lookback="63B", price_column="close",
                      normalize=True, dropna_pctg=0.2):
    pivot_data = daily_price_data[["date", "ticker", price_column]] \
        .pivot_table(index="date", values = price_column, columns="ticker").sort_index()
    return_data = pd.np.log(pivot_data).diff()
    return_data = return_data.ix[start_date:end_date]
    required_records = int(len(return_data)*(1-dropna_pctg))
    return_data = return_data.dropna(thresh=required_records, axis=1).fillna(value=0.0)
    if normalize:
        return_data = (return_data - return_data.mean())/return_data.std()
    stock_returns = return_data[return_data.columns[return_data.columns.isin(stock_list)]]

    factors = RandomizedPCA(n_components=num_comp, iterated_power=4, random_state=12345).fit_transform(stock_returns)
    factors = zscore(factors)
    model = sma.OLS(return_data.fillna(0.0).values, sma.add_constant(factors)).fit()
    betas = pd.DataFrame(model.params[1:,:].T, columns=map(lambda x:"PC_{0}".format(x), range(1, num_comp+1)),
                         index=return_data.columns)
    return betas

def robust_pca_coeff_for_day(daily_price_data, stock_list, num_comp, date=None, lookback="63B", price_column="close",
                             normalize=True, dropna_pctg=0.2):
    pivot_data = daily_price_data[["date", "ticker", price_column]] \
        .pivot_table(index="date", values = price_column, columns="ticker").sort_index()
    return_data = pd.np.log(pivot_data).diff()
    return_data = return_data.ix[start_date:end_date]
    required_records = int(len(return_data)*(1-dropna_pctg))
    return_data = return_data.dropna(thresh=required_records, axis=1).fillna(value=0.0)
    if normalize:
        return_data = (return_data - return_data.mean())/return_data.std()
    stock_returns = return_data[return_data.columns[return_data.columns.isin(stock_list)]]

    factors = RandomizedPCA(n_components=num_comp, iterated_power=4, random_state=12345).fit_transform(stock_returns)
    factors = zscore(factors)
    model = sma.OLS(return_data.fillna(0.0).values, sma.add_constant(factors)).fit()
    betas = pd.DataFrame(model.params[1:,:].T, columns=map(lambda x:"PC_{0}".format(x), range(1, num_comp+1)),
                         index=return_data.columns)
    return betas


def collect_stats_for_date(results, date, lookback, num_comp=5):
    daily_price_data = results["daily_price_data"]
    etf_information = results["etf_information"]
    start_date = date - pd.tseries.frequencies.to_offset(lookback)
    trading_days = marketTimeline.get_trading_days(start_date, date)
    end_date = trading_days[-1].normalize() # Real last trading date according to market timeline
    stock_list = list(etf_information[np.logical_not(etf_information['is_etf'])]['ticker'])
    pc_loadings = robust_pca_coeff_for_day(daily_price_data, stock_list, num_comp, end_date, lookback)
    beta_data = results['beta_data'].reset_index()
    beta_data = beta_data[beta_data['date']==end_date].set_index('ticker', drop=True)\
        [["SPY_beta", "VWO_beta", "TLT_beta", "GSG_beta", "GLD_beta"]].dropna()
    beta_data.dropna(inplace=True)
    beta_data = beta_data[abs(beta_data['SPY_beta'])<=config['SPY_beta_bound']]

    combined_features = pd.concat([pc_loadings, beta_data], axis=1).dropna()
    return combined_features

def kmeans_clustering(combined_features, n_clusters=11):
    data = combined_features.values
    kmeans = KMeans(n_clusters=n_clusters, random_state=12345).fit(data)
    cluster_ids = list(kmeans.labels_)
    combined_features['cluster_id'] = cluster_ids
    return combined_features

def graphical_lasso(combined_features, results, date, lookback='63B', price_column='close', normalize=True,
                    shrinkage=0.1, max_iter=500, tol=5e-4):
    start_date = date - pd.tseries.frequencies.to_offset(lookback)
    trading_days = marketTimeline.get_trading_days(start_date, date)
    end_date = trading_days[-1].normalize() # Real last trading date according to market timeline
    daily_price_data = results['daily_price_data']
    pivot_data = daily_price_data[["date", "ticker", price_column]] \
        .pivot_table(index="date", values = price_column, columns="ticker").sort_index()
    return_data = pd.np.log(pivot_data).diff()
    return_data = return_data.ix[start_date:end_date]
    return_data = return_data[return_data.columns[return_data.columns.isin(combined_features.index)]]
    if normalize:
        return_data = (return_data - return_data.mean())/return_data.std()

    clusters = list(combined_features['cluster_id'].unique())
    clusters.sort()
    precision_matrices = {}
    covariance_matrices = {}

    combined_features['sub_cluster_id'] = -1
    for c in clusters:
        current_cluster = list(combined_features[combined_features['cluster_id']==c].index)
        if shrinkage:
            corr = shrunk_covariance(return_data[current_cluster].corr(), shrinkage=shrinkage)
        else:
            corr = return_data[current_cluster].corr()
        model = GraphLassoCV(max_iter=max_iter, tol=tol).fit(corr)
        precision = pd.DataFrame(model.get_precision(), index=current_cluster, columns=current_cluster)
        precision_matrices[c] = precision
        cov = pd.DataFrame(model.covariance_, index=current_cluster, columns=current_cluster)
        cov[abs(cov)<=0.0001] = 0
        covariance_matrices[c] = cov
        _, labels = affinity_propagation(cov)
        sub_clusters = pd.Series(labels, current_cluster)
        combined_features.loc[current_cluster, 'sub_cluster_id'] = sub_clusters
    return precision_matrices, covariance_matrices, combined_features

def filter_row(row, precision_matrix, max_num):
    num = len(row[0])
    if num<=max_num:
        basket = row[0]
    else:
        current_pmat = abs(precision_matrix[row['index']])
        current_pmat.sort(ascending=False)
        basket = set(current_pmat[:max_num].index)
    return basket

def filter_precision_matrix(precision_matrices, min_num=2, max_num=12):
    filtered_baskets = {}
    for cluster in precision_matrices.keys():
        precision_matrix = precision_matrices[cluster]
        condition_matrix = precision_matrix.apply(lambda x:map(lambda x:abs(x)>0.0001, x), axis=1)
        good_names = condition_matrix.apply(lambda x:set(condition_matrix.columns[x.values]), axis=1)
        baskets = good_names[good_names.apply(lambda x:len(x)>=min_num)]
        final_baskets = baskets.reset_index().apply(lambda x:filter_row(x, precision_matrix, max_num), axis=1)
        if len(final_baskets):
            baskets = list(final_baskets.values)
            unique_baskets = [list(x) for x in set(tuple(x) for x in baskets)]
            filtered_baskets[cluster] = unique_baskets
        else:
            filtered_baskets[cluster] = []
    return filtered_baskets

def collect_rolling_stats(engine_results, lookback, start_date=None, end_date=None):
    entire_test_range = marketTimeline.get_trading_days(start_date, end_date)
    print("done")

def combine_features(results, start_date=None, end_date=None):
    beta_data = results['beta_data'].reset_index()
    pca_data = results['pca_data'].reset_index()
    combined_features = pd.merge(beta_data, pca_data, how='left', on=['date', 'ticker'])
    combined_features = combined_features[(combined_features['date']>=start_date) & \
                                          (combined_features['date']<=end_date)]
    non_rank_columns = map(lambda x:'_rank' not in x, list(combined_features.columns))
    return combined_features[combined_features.columns[non_rank_columns]]

def graphical_lasso_aux(combined_features, return_data, shrinkage=0.1, max_iter=200, tol=5e-3,
                        use_cross_validation=False):
    clusters = list(combined_features['cluster_id'].unique())
    clusters.sort()
    precision_matrices = {}
    covariance_matrices = {}
    combined_features['sub_cluster_id'] = -1
    for c in clusters:
        current_cluster = list(combined_features[combined_features['cluster_id']==c].index)
        if shrinkage:
            corr = shrunk_covariance(return_data[current_cluster].corr(), shrinkage=shrinkage)
        else:
            corr = return_data[current_cluster].corr()
        try:
            if use_cross_validation:
                model = GraphLassoCV(max_iter=max_iter, tol=tol).fit(corr)
            else:
                model = GraphLasso(max_iter=max_iter, tol=tol).fit(corr)
            precision = pd.DataFrame(model.get_precision(), index=current_cluster, columns=current_cluster)
            precision[abs(precision)<=0.0001] = 0
            precision_matrices[c] = precision
            cov = pd.DataFrame(model.covariance_, index=current_cluster, columns=current_cluster)
            cov[abs(cov)<=0.0001] = 0
            covariance_matrices[c] = cov
            _, labels = affinity_propagation(cov)
            sub_clusters = pd.Series(labels, current_cluster)
            combined_features.loc[current_cluster, 'sub_cluster_id'] = sub_clusters
        except:
            dummy = pd.DataFrame(corr, index=current_cluster, columns=current_cluster)
            precision_matrices[c] = dummy
            covariance_matrices[c] = dummy
    return precision_matrices, covariance_matrices, combined_features

def generate_stats_one_day(features, return_data, kmeans_num_cluster=11, glasso_shrinkage=0.1):
    features = kmeans_clustering(features, n_clusters=kmeans_num_cluster)
    precision_matrices, covariance_matrices, combined_features \
        = graphical_lasso_aux(features, return_data, glasso_shrinkage)
    return precision_matrices, covariance_matrices, combined_features


def generate_candidate_baskets(precision_matrices, covariance_matrices, combined_features, min_num=5, max_num=12):
    filtered_baskets = filter_precision_matrix(precision_matrices, 5, 12)
    return filtered_baskets

def generate_clusters(results, dates_to_calibrate, price_column="close", glasso_lookback="63B", dropna_pctg=0.15,
                      kmeans_num_cluster=8, glasso_shrinkage=0.1, min_num=5, max_num=12):
    daily_price_data = results['daily_price_data']
    pivot_data = daily_price_data[["date", "ticker", price_column]] \
        .pivot_table(index="date", values = price_column, columns="ticker").sort_index()
    all_returns = pd.np.log(pivot_data).diff()

    beta_data = results['beta_data'].reset_index()
    pca_data = results['pca_data'].reset_index()
    combined_features = pd.merge(beta_data, pca_data, how='left', on=['date', 'ticker'])
    #combined_features = combined_features[(combined_features['date']>=start_date) & \
    #                                      (combined_features['date']<=end_date)]
    non_rank_columns = map(lambda x:'_rank' not in x, list(combined_features.columns))
    all_features = combined_features[combined_features.columns[non_rank_columns]]

    all_baskets = pd.DataFrame(index=dates_to_calibrate, columns=['baskets'])
    for dt in dates_to_calibrate:
        start_dt = dt - pd.tseries.frequencies.to_offset(glasso_lookback)
        return_data = all_returns.ix[start_dt:dt]
        required_records = int(len(return_data)*(1-dropna_pctg))
        return_data = return_data.dropna(thresh=required_records, axis=1).fillna(value=0.0)
        features = all_features[all_features['date']==dt].drop('date', axis=1).dropna().set_index('ticker')
        good_names = list(set(return_data.columns).intersection(features.index))
        features = features.ix[good_names]
        return_data = return_data[return_data.columns[return_data.columns.isin(good_names)]]
        p, v, f = generate_stats_one_day(features, return_data, kmeans_num_cluster, glasso_shrinkage)
        baskets = generate_candidate_baskets(p, v, f)
        flattened = [val for sublist in baskets.values() for val in sublist]
        all_baskets.loc[dt, 'baskets'] = flattened
        print(dt)
    return all_baskets

def process_one_basket(dt, basket, price_data, current_oscillator, current_stochastic, trader_id='trader',
                       proportion=.3333333, entry_thresh=-40, exit_thresh=20, timelimit='21B', scale_by_leg_size=True):

    num_per_leg = int(len(basket)*proportion)
    #num_per_leg = int(np.round(len(basket)*proportion))
    starting_momentum = current_oscillator.ix[:dt].iloc[-1]
    starting_momentum.sort()
    # long low momentum
    # short high momentum
    longs = list(starting_momentum[:num_per_leg].index)
    shorts = list(starting_momentum[-num_per_leg:].index)
    if scale_by_leg_size:
        leg_size = 1.0/len(longs)
    else:
        leg_size = 1.0
    # This is an approximation, we are trying to monitor the richness/cheapness of
    # Stochastics gives you more dynamic entry/exits. Can change later
    spread_momentum = current_oscillator[longs].mean(axis=1) - current_oscillator[shorts].mean(axis=1)
    spread_stochastic = current_stochastic[longs].mean(axis=1) - current_stochastic[shorts].mean(axis=1)
    # Trading period, previous history was included in case history were to be used (e.g. re-compute stats on real
    # prices)
    spread_stochastic_for_entry = spread_stochastic.ix[dt:dt+pd.tseries.frequencies.to_offset("20B")]
    entry_dt = spread_stochastic_for_entry[spread_stochastic_for_entry<entry_thresh].index
    if len(entry_dt):
        entry_dt = entry_dt[0]
        default_exit_dt = entry_dt + pd.tseries.frequencies.to_offset(timelimit)
        spread_stochastic_for_exit = spread_stochastic.ix[entry_dt:]
        exit_dt = spread_stochastic_for_exit[spread_stochastic_for_exit>exit_thresh].index
        exit_dt = min(default_exit_dt, exit_dt[0]) if len(exit_dt) else default_exit_dt
        trade_segment = spread_stochastic_for_exit[:exit_dt]

        # Form results dataframe
        results = pd.DataFrame(columns=['date', 'ticker', 'normalized_dollars', 'purpose', 'trader_id'])
        dates = [entry_dt]*(len(longs)+len(shorts))
        dates.extend([exit_dt]*(len(longs)+len(shorts)))
        results['date'] = dates
        tickers = list(longs)
        tickers.extend(shorts)
        tickers = tickers*2
        results['ticker'] = tickers
        open_dollars = [leg_size]*len(longs)
        open_dollars.extend([-leg_size]*len(shorts))
        close_dollars = [-leg_size]*len(longs)
        close_dollars.extend([leg_size]*len(shorts))
        dollars = list(open_dollars)
        dollars.extend(close_dollars)
        results['normalized_dollars'] = dollars
        purpose = ['open']*(len(longs)+len(shorts))
        purpose.extend(['close']*(len(longs)+len(shorts)))
        results['purpose'] = purpose
        results['trader_id'] = trader_id
        return results
    else:
        # No opportunities
        return pd.DataFrame()

def process_one_basket_overnight(dt, basket, price_data, current_oscillator, current_stochastic, current_overnight,
                                 trader_id='trader', proportion=.3333333, entry_thresh=-40, exit_thresh=20,
                                 timelimit='21B', scale_by_leg_size=True, overnight_limit=0.02):
    num_per_leg = int(len(basket)*proportion)
    #num_per_leg = int(np.round(len(basket)*proportion))
    starting_momentum = current_oscillator.ix[:dt].iloc[-1].copy()
    starting_momentum.sort_values(inplace=True)
    # long low momentum
    # short high momentum
    longs = list(starting_momentum[:num_per_leg].index)
    shorts = list(starting_momentum[-num_per_leg:].index)
    if scale_by_leg_size:
        leg_size = 1.0/len(longs)
    else:
        leg_size = 1.0
    # This is an approximation, we are trying to monitor the richness/cheapness of
    # Stochastics gives you more dynamic entry/exits. Can change later
    spread_momentum = current_oscillator[longs].mean(axis=1) - current_oscillator[shorts].mean(axis=1)
    spread_stochastic = current_stochastic[longs].mean(axis=1) - current_stochastic[shorts].mean(axis=1)
    overnight_signal = current_overnight[shorts].max(axis=1)
    # Trading period, previous history was included in case history were to be used (e.g. re-compute stats on real
    # prices)
    spread_stochastic_for_entry = spread_stochastic.ix[dt:dt+pd.tseries.frequencies.to_offset("20B")]
    overnight_signal_for_entry = overnight_signal.ix[dt:dt+pd.tseries.frequencies.to_offset("20B")]
    entry_condition = (spread_stochastic_for_entry<entry_thresh) & (overnight_signal_for_entry<overnight_limit)
    entry_dt = spread_stochastic_for_entry[entry_condition].index

    if len(entry_dt):
        entry_dt = entry_dt[0]
        default_exit_dt = entry_dt + pd.tseries.frequencies.to_offset(timelimit)
        spread_stochastic_for_exit = spread_stochastic.ix[entry_dt:]
        exit_dt = spread_stochastic_for_exit[spread_stochastic_for_exit>exit_thresh].index
        exit_dt = min(default_exit_dt, exit_dt[0]) if len(exit_dt) else default_exit_dt
        trade_segment = spread_stochastic_for_exit[:exit_dt]

        # Form results dataframe
        results = pd.DataFrame(columns=['date', 'ticker', 'normalized_dollars', 'purpose', 'trader_id'])
        dates = [entry_dt]*(len(longs)+len(shorts))
        dates.extend([exit_dt]*(len(longs)+len(shorts)))
        results['date'] = dates
        tickers = list(longs)
        tickers.extend(shorts)
        tickers = tickers*2
        results['ticker'] = tickers
        open_dollars = [leg_size]*len(longs)
        open_dollars.extend([-leg_size]*len(shorts))
        close_dollars = [-leg_size]*len(longs)
        close_dollars.extend([leg_size]*len(shorts))
        dollars = list(open_dollars)
        dollars.extend(close_dollars)
        results['normalized_dollars'] = dollars
        purpose = ['open']*(len(longs)+len(shorts))
        purpose.extend(['close']*(len(longs)+len(shorts)))
        results['purpose'] = purpose
        results['trader_id'] = trader_id
        return results
    else:
        # No opportunities
        return pd.DataFrame()

def generate_decisions(all_baskets, results, proportion=.3333, entry_thresh=-50, exit_thresh=10, timelimit="21B",
                       overnight_limit=None):
    daily_price_data = results['daily_price_data']
    pivot_data = daily_price_data[["date", "ticker", "close"]] \
        .pivot_table(index="date", values = "close", columns="ticker").sort_index()
    all_returns = pd.np.log(pivot_data).diff().fillna(value=0.0)

    technical_indicator_data = results['technical_indicator_data'].reset_index()
    oscillator_data = technical_indicator_data[["date", "ticker", "indicator"]] \
        .pivot_table(index="date", values = "indicator", columns="ticker").sort_index()

    stochastics = results['stochastic_indicator_data'].reset_index()
    stochastic_data = stochastics[["date", "ticker", "stoch_d"]] \
        .pivot_table(index="date", values = "stoch_d", columns="ticker").sort_index()

    overnight_returns = results['overnight_return_data'].reset_index()
    overnight_data = overnight_returns[["date", "ticker", "overnight_return"]] \
        .pivot_table(index="date", values = "overnight_return", columns="ticker").sort_index()

    strength_index = results["strength_index"]["strength_index"]
    # Check index here
    oscillator_data.index = pd.DatetimeIndex(oscillator_data.index).normalize()
    stochastic_data.index = pd.DatetimeIndex(stochastic_data.index).normalize()
    overnight_data.index = pd.DatetimeIndex(overnight_data.index).normalize()

    counts = pd.DataFrame(index=all_baskets.index, columns=['all_baskets'])
    stats = pd.DataFrame(index=all_baskets.index, columns=['count', 'count_pos', 'mean', 'median', 'hit',
                                                           'ave_holding'])
    observations =[]
    for dt in all_baskets.index:
        print("{0} calibration ************".format(dt.date()))
        current_baskets = all_baskets.ix[dt].ix[0]
        if ((isinstance(current_baskets, float) and isnan(current_baskets)) or (not len(current_baskets))):
            counts.loc[dt, 'all_baskets'] = 0
            continue
        counts.loc[dt, 'all_baskets'] = len(current_baskets)
        current_trader_count = 0
        current_entry_thresh = entry_thresh
        current_exit_thresh = exit_thresh
        current_strength = strength_index.ix[:dt].iloc[-1]
        #if ((dt>=pd.Timestamp('2013-01-01')) and (dt<=pd.Timestamp('2013-06-01'))):
        if current_strength<0.12:
            current_entry_thresh = -75
            current_exit_thresh = -45
        for basket in current_baskets:
            start_dt = dt - pd.tseries.frequencies.to_offset('50B')
            end_dt = dt + pd.tseries.frequencies.to_offset('50B')
            price_data = pivot_data[start_dt:end_dt][basket]
            current_oscillator = oscillator_data.ix[start_dt:end_dt][basket]
            current_stochastic = stochastic_data.ix[start_dt:end_dt][basket]
            current_overnight = overnight_data.ix[start_dt:end_dt][basket]
            current_trader_count += 1
            trader_id = "trader_{0}_{1}".format(str(dt.date()).replace('-', ''), current_trader_count)
            if overnight_limit:
                current_obs = process_one_basket_overnight(dt, basket, price_data, current_oscillator,
                                                           current_stochastic, current_overnight, trader_id,
                                                           proportion, current_entry_thresh, current_exit_thresh, timelimit,
                                                           overnight_limit=overnight_limit)
            else:
                current_obs = process_one_basket(dt, basket, price_data, current_oscillator, current_stochastic, trader_id,
                                                 proportion, current_entry_thresh, current_exit_thresh, timelimit)
            if len(current_obs):
                print(basket)
                observations.append(current_obs)

    all_trades = pd.concat(observations).reset_index(drop=True)
    return all_trades


def generate_full_transaction_dataframe_test(decision_dataframe, results):
    #transactions_df = group_transactions[["security_id", "executed_dt_transaction", "execution_amount",
    #                                      "execution_price", "commision"]]
    daily_price_data = results['daily_price_data']
    pivot_data = daily_price_data[["date", "ticker", "close"]] \
        .pivot_table(index="date", values = "close", columns="ticker").sort_index()

    transactions = pd.DataFrame(columns=["date", "security_id", "executed_dt_transaction", "execution_amount",
                                         "execution_price", "commision"])
    transactions["date"] = decision_dataframe["date"]
    transactions["security_id"] = decision_dataframe["ticker"].apply(Ticker)
    transactions["executed_dt_transaction"] = decision_dataframe["date"]
    transactions["execution_amount"] = decision_dataframe["normalized_dollars"]*200
    transactions["execution_price"] = 120 + 2.1*pd.np.random.randn(len(decision_dataframe))
    transactions["commision"] = 0.0
    return transactions

def generate_full_transaction_dataframe(decision_dataframe, results, dollar_amount=100000.0):
    #transactions_df = group_transactions[["security_id", "executed_dt_transaction", "execution_amount",
    #                                      "execution_price", "commision"]]
    daily_price_data = results['daily_price_data']
    pivot_data = daily_price_data[["date", "ticker", "close"]] \
        .pivot_table(index="date", values = "close", columns="ticker").sort_index()

    decision_dataframe['date'] = pd.DatetimeIndex(decision_dataframe['date']).normalize()
    daily_price_data["date"] = pd.DatetimeIndex(daily_price_data["date"]).normalize()
    intermediate = pd.merge(decision_dataframe, daily_price_data, how='left', on=['ticker', 'date'])
    opens = intermediate[intermediate['purpose']=='open'].reset_index()
    closes = intermediate[intermediate['purpose']=='close'].reset_index()
    opens["execution_amount"] = np.round(dollar_amount/opens['open'])*opens["normalized_dollars"]
    opens["execution_amount"] = opens["execution_amount"].apply(lambda x:int(x) if pd.notnull(x) else 0)
    closes["execution_amount"] = -opens["execution_amount"]
    intermediate = pd.concat([opens, closes])
    intermediate = intermediate.sort_values('index').reset_index(drop=True)

    transactions = pd.DataFrame(columns=["date", "security_id", "executed_dt_transaction", "execution_amount",
                                         "execution_price", "commision"])
    transactions["date"] = intermediate["date"]
    transactions["security_id"] = intermediate["ticker"].apply(Ticker)
    transactions["executed_dt_transaction"] = \
        (pd.DatetimeIndex(intermediate['date']) + pd.Timedelta(hours=9, minutes=31))\
        .tz_localize('US/Eastern').tz_convert('UTC')
    transactions["execution_amount"] = intermediate["execution_amount"]
    transactions["execution_price"] = intermediate['open']
    transactions["commision"] = 0.0
    return transactions

def generate_full_transaction_dataframe_equally_weighted(decision_dataframe, results, dollar_amount=200000.0):
    #transactions_df = group_transactions[["security_id", "executed_dt_transaction", "execution_amount",
    #                                      "execution_price", "commision"]]
    daily_price_data = results['daily_price_data']
    pivot_data = daily_price_data[["date", "ticker", "close"]] \
        .pivot_table(index="date", values = "close", columns="ticker").sort_index()

    decision_dataframe['date'] = pd.DatetimeIndex(decision_dataframe['date']).normalize()
    daily_price_data["date"] = pd.DatetimeIndex(daily_price_data["date"]).normalize()
    intermediate = pd.merge(decision_dataframe, daily_price_data, how='left', on=['ticker', 'date'])
    opens = intermediate[intermediate['purpose']=='open'].reset_index()
    closes = intermediate[intermediate['purpose']=='close'].reset_index()
    opens["execution_amount"] = np.round(dollar_amount/opens['open'])*opens["normalized_dollars"]
    opens["execution_amount"] = opens["execution_amount"].apply(lambda x:int(x) if pd.notnull(x) else 0)
    closes["execution_amount"] = -opens["execution_amount"]
    intermediate = pd.concat([opens, closes])
    intermediate = intermediate.sort_values('index').reset_index(drop=True)

    transactions = pd.DataFrame(columns=["date", "security_id", "executed_dt_transaction", "execution_amount",
                                         "execution_price", "commision"])
    transactions["date"] = intermediate["date"]
    transactions["security_id"] = intermediate["ticker"].apply(Ticker)
    transactions["executed_dt_transaction"] = \
        (pd.DatetimeIndex(intermediate['date']) + pd.Timedelta(hours=9, minutes=31))\
        .tz_localize('US/Eastern').tz_convert('UTC')
    transactions["execution_amount"] = intermediate["execution_amount"]
    transactions["execution_price"] = intermediate['open']
    transactions["commision"] = 0.0
    return transactions

def generate_correlations_for_mvp(results, start_date, end_date, out_file, dropna_pctg=0.15, normalize=False,
                                  vol_lookback='63B', price_column='close', beta_column='SPY_beta'):
    daily_price_data = results['daily_price_data']
    beta_data = results['beta_data'].reset_index()
    etf_information = results["etf_information"]
    stock_list = list(etf_information[np.logical_not(etf_information['is_etf'])]['ticker'])
    etf_list = list(set(etf_information['ticker']) - set(stock_list))
    etf_list.sort()
    pivot_data = daily_price_data[["date", "ticker", price_column]] \
        .pivot_table(index="date", values = price_column, columns="ticker").sort_index()
    pivot_data = pivot_data.ix[start_date-pd.tseries.frequencies.to_offset('1B'):end_date]
    required_records = int(len(pivot_data)*(1-dropna_pctg))
    pivot_data = pivot_data.dropna(thresh=required_records, axis=1).fillna(method='ffill')
    pivot_data = pivot_data[pivot_data.columns[pivot_data.columns.isin(etf_list)]]
    return_data = pd.np.log(pivot_data).diff()
    return_data = return_data.ix[start_date:end_date]
    if normalize:
        return_data = (return_data - return_data.mean())/return_data.std()
    etf_returns = return_data[return_data.columns[return_data.columns.isin(etf_list)]]
    return_correlation_data = etf_returns.corr()
    price_correlation_data = pivot_data.corr()
    beta_pivot = beta_data[["date", "ticker", beta_column]] \
        .pivot_table(index="date", values = beta_column, columns="ticker").sort_index()
    spy_beta_data = beta_pivot.iloc[-1]
    spy_beta_data = spy_beta_data[spy_beta_data.index[spy_beta_data.index.isin(etf_list)]]
    vol_data = etf_returns.ix[end_date-pd.tseries.frequencies.to_offset(vol_lookback):].std()*np.sqrt(252)

    with pd.HDFStore(out_file, "w") as store:
        store["price_data"] = pivot_data
        store["return_data"] = etf_returns
        store["vol_data"] = vol_data
        store["return_corr"] = return_correlation_data
        store["price_corr"] = price_correlation_data
        store["spy_beta_data"] = spy_beta_data

    print("done")


def generate_calibration_file(all_trades, out_file, entry_threshold=0, exit_threshold=10, time_limit="21B",
                              stop_limits_pct='(-0.06, 10.0)', valid_until_offset="20B", drop_duplicates=False):
    unique_traders = list(all_trades['trader_id'].unique())
    n_traders = len(unique_traders)
    print("{0} total traders".format(n_traders))
    columns = ["date", "actorID", "first_daily_check_up_time", "valid_until_date", "role",
               "ranking", "long_basket", "short_basket", "stoch_d_params", "entry_threshold",
               "exit_threshold", "time_limit", "indicator_check_frequency", "stop_limits_pct",
               "total_funding", "fundingRequestsAmount", "funding_buffer", "override_filter",
               "instructions_overrides"]
    cal_df = pd.DataFrame(index=range(n_traders), columns=columns)
    cal_df["first_daily_check_up_time"] = "09:30:00"
    cal_df["role"] = 2
    cal_df["ranking"] = range(n_traders)
    cal_df["stoch_d_params"] = '{"offset": 14, "window": 3}'
    cal_df["entry_threshold"] = entry_threshold
    cal_df["exit_threshold"] = exit_threshold
    cal_df["time_limit"] = time_limit
    cal_df["indicator_check_frequency"] = "00:30:00"
    cal_df["stop_limits_pct"] = stop_limits_pct
    cal_df['total_funding'] = 100000000.0
    cal_df["fundingRequestsAmount"] = 210000.0
    cal_df["funding_buffer"] = 100000.0

    pos = 0
    for trader_id in unique_traders:
        filter_cond = (all_trades['trader_id']==trader_id) & (all_trades['purpose']=='open')
        date = trader_id.split('_')[1].split('_')[0]
        date = pd.Timestamp(date).date()

        long_basket = list(all_trades[filter_cond & (np.sign(all_trades["normalized_dollars"])>0)]["ticker"])
        short_basket = list(all_trades[filter_cond & (np.sign(all_trades["normalized_dollars"])<0)]["ticker"])

        cal_df.loc[pos, "long_basket"] = str(map(Ticker, long_basket))
        cal_df.loc[pos, "short_basket"] = str(map(Ticker, short_basket))
        cal_df.loc[pos, "date"] = date
        cal_df.loc[pos, "actorID"] = trader_id
        cal_df.loc[pos, "valid_until_date"] = str((date+pd.tseries.frequencies.to_offset(valid_until_offset)).date())

        pos += 1
        print(trader_id)

    if drop_duplicates:
        cal_df = cal_df.drop_duplicates(['date', 'long_basket', 'short_basket']).reset_index(drop=True)
    cal_df.to_csv(out_file, quotechar="'", index=False)
    print(out_file)
    #str((current_time+pd.tseries.frequencies.to_offset('1M')).date())



if __name__=="__main__":
    # Testing for pipeline results
    ENGINE_DIR = r"D:\DCM\engine_results\pairs_trading"

    # This is the baseline standard and calibration that changes
    #lasso_data_location = os.path.join(ENGINE_DIR, "lasso_data_baseline_01.h5")

    lasso_data_location = os.path.join(ENGINE_DIR, "lasso_data_baseline_01_dates_protected.h5")
    #lasso_data_location = os.path.join(ENGINE_DIR, "lasso_data_baseline_dynamic_dates_shrinkage_30.h5")
    indicator_data_location = os.path.join(ENGINE_DIR, "derived_data_overnight.h5")
    strength_data_location = os.path.join(ENGINE_DIR, "strength_index.csv")
    target_fields = ['kmeans_cluster_data', 'glasso_group_data', 'daily_price_data',
                     'calibration_dates']
    results = load_data(lasso_data_location, target_fields)
    indicator_results = load_data(indicator_data_location, ['technical_indicator_data', 'stochastic_indicator_data',
                                                            'overnight_return_data', 'volatility_data'])
    results.update(indicator_results)
    strength_index = pd.read_csv(strength_data_location, parse_dates=['date'], index_col=['date'])
    results.update({"strength_index": strength_index})

    # For data test (delete later)
    new_data_results = load_data(os.path.join(ENGINE_DIR, "raw_data_20171109.h5"), ["daily_price_data"])
    results.update({"daily_price_data": new_data_results["daily_price_data"]})

    all_baskets = results['glasso_group_data'].dropna().set_index('date')
    all_baskets.sort_index(inplace=True)
    #all_baskets = all_baskets.ix[pd.Timestamp('2015-09-15'):]
    start_date = pd.Timestamp('2011-01-05')
    end_date = pd.Timestamp('2016-12-31')
    #all_baskets = all_baskets.loc[dates_to_calibrate]

    timelimit = "21B"
    # Build trades
    #all_trades = generate_decisions(all_baskets, results, entry_thresh=-50, exit_thresh=10, timelimit="21B")
    all_trades = generate_decisions(all_baskets, results, entry_thresh=-50, exit_thresh=10, timelimit=timelimit,
                                    overnight_limit=0.02)
    transactions = generate_full_transaction_dataframe(all_trades, results)
    with pd.HDFStore(os.path.join(ENGINE_DIR, "all_trades_21B_n50_10_overnight_02_strength_index_block_compare.h5"), "w") as store:
        store['all_trades'] = all_trades
        store['transactions'] = transactions
        print("a")

    print("aaaaaaaa")
    with pd.HDFStore(os.path.join(ENGINE_DIR, "all_trades_21B_n50_10_overnight_02_strength_index_block_compare.h5"), "a") as store:
        all_trades = store['all_trades']
        transactions = store['transactions']


    out_file = os.path.join(ENGINE_DIR, "test_cal_21B_new_strength_n50_10_overnight_02_strength_index_block_compare.csv")
    generate_calibration_file(all_trades, out_file, stop_limits_pct='(-0.10, 10.0)', entry_threshold=-50,
                              exit_threshold=10, time_limit=timelimit)

    print("done")


    ##############################################################################

    '''
    with pd.HDFStore(os.path.join(ENGINE_DIR, "all_trades_lasso_011_scaled.h5")) as store:
        all_trades = store['all_trades']

    out_file = os.path.join(ENGINE_DIR, "test_cal_50_10_stoploss.csv")
    generate_calibration_file(all_trades, out_file)

    lasso_data_location = os.path.join(ENGINE_DIR, "lasso_data.h5")
    indicator_data_location = os.path.join(ENGINE_DIR, "derived_data.h5")
    target_fields = ['kmeans_cluster_data', 'glasso_group_data', 'daily_price_data',
                     'calibration_dates']
    results = load_data(lasso_data_location, target_fields)
    indicator_results = load_data(indicator_data_location, ['technical_indicator_data', 'stochastic_indicator_data'])
    results.update(indicator_results)
    all_baskets = results['glasso_group_data'].set_index('date')
    start_date = pd.Timestamp('2011-01-05')
    end_date = pd.Timestamp('2016-12-31')
    dates_to_calibrate = list(pd.DataFrame(index=marketTimeline.get_trading_days(start_date, end_date))\
                              .groupby(pd.TimeGrouper('4W-WED')).first().index)
    all_baskets = all_baskets.loc[dates_to_calibrate]
    all_trades = generate_decisions(all_baskets, results, exit_thresh=10)

    with pd.HDFStore(os.path.join(ENGINE_DIR, "all_trades_lasso_01_scaled.h5")) as store:
        store['all_trades'] = all_trades

    with pd.HDFStore(os.path.join(ENGINE_DIR, "all_trades_lasso_01_scaled.h5")) as store:
        all_trades = store['all_trades']

    transaction_df = generate_full_transaction_dataframe(all_trades, results)
    with pd.HDFStore(os.path.join(DATA_DIR, "transaction_df_01_scaled.h5")) as store:
        store["transaction_df"] = transaction_df

    #############################################################################################
    # Old testing
    data_location = os.path.join(DATA_DIR, "pairs_trading.h5")
    target_fields = ['dollar_volume_data', 'earnings_block_dates', 'volatility_data', 'regime_data', 'beta_data',
                     'correlation_data', 'daily_price_data', 'etf_information', 'sector_industry_mapping',
                     'daily_index_data', 'overnight_return_data', 'technical_indicator_data',
                     'stochastic_indicator_data', 'pca_data']


    data_location = os.path.join(DATA_DIR, "mvp_data.h5")
    #target_fields = ['beta_data', 'correlation_data', 'daily_price_data', 'etf_information']
    target_fields = ['beta_data', 'correlation_data', 'daily_price_data', 'etf_information', 'technical_indicator_data',
                     'stochastic_indicator_data']
    #results = load_data(data_location, target_fields)
    results = load_data(r"D:\DCM\engine_results\pairs_trading\derived_data.h5", target_fields)
    start_date = pd.Timestamp('2016-01-01')
    end_date = pd.Timestamp('2017-10-18')
    out_file = os.path.join(DATA_DIR, "etf_statistics.h5")
    #generate_correlations_for_mvp(results, start_date, end_date, out_file)

    #features = combine_features(results, start_date, end_date)
    dt = pd.Timestamp('2016-12-28')
    dates_to_calibrate = [pd.Timestamp('2011-03-22'), pd.Timestamp('2011-05-11'), pd.Timestamp('2011-05-25')]
    dates_to_calibrate = list(pd.DataFrame(index=marketTimeline.get_trading_days(start_date, end_date))\
                              .groupby(pd.TimeGrouper('M')).nth(0).index)
    #all_baskets = generate_clusters(results, dates_to_calibrate, min_num=5, max_num=16)
    #with pd.HDFStore(os.path.join(DATA_DIR, "basket_calibrations_5_16.h5"), "a") as store:
        #store["all_baskets"] = all_baskets

    with pd.HDFStore(os.path.join(DATA_DIR, "basket_calibrations_5_24.h5"), "r") as store:
        all_baskets = store["all_baskets"]

    all_trades = generate_decisions(all_baskets, results, entry_thresh=-65, exit_thresh=0)
    #with pd.HDFStore(r"D:\DCM\temp\etf_experiment\all_trades_final_excl_5_24_65_0.h5") as store:
        #store['all_trades'] = all_trades
    with pd.HDFStore(r"D:\DCM\temp\etf_experiment\all_trades_final_excl_5_24_65_0.h5", "r") as store:
        all_trades = store['all_trades']

    transaction_df = generate_full_transaction_dataframe(all_trades, results)
    with pd.HDFStore(os.path.join(DATA_DIR, "transaction_df_excl_5_24_65_0.h5")) as store:
        store["transaction_df"] = transaction_df

    print("dones")

    # Scrapbook

    daily_price_data = results["daily_price_data"]
    etf_information = results["etf_information"]
    end_date = pd.Timestamp('2015-08-24')
    start_date = end_date - pd.tseries.frequencies.to_offset("126B")
    stock_list = list(etf_information[np.logical_not(etf_information['is_etf'])]['ticker'])
    num_comp = 5
    #pca_coeff_for_day(daily_price_data, stock_list, num_comp, start_date, end_date)

    lookback = config["lookback"]
    date = end_date
    combined_features = collect_stats_for_date(results, date, lookback)
    combined_features = kmeans_clustering(combined_features, 11)
    precision_matrices, covariance_matrices, combined_features \
        = graphical_lasso(combined_features, results, date, lookback)
    filter_precision_matrix(precision_matrices)
    print("done")
    '''