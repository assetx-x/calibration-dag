import numpy as np
SEED = 20190213
np.random.seed(SEED)
from sklearn.ensemble.partial_dependence import partial_dependence, plot_partial_dependence
from sklearn.model_selection import GroupKFold, GridSearchCV, cross_val_score
from sklearn.linear_model import LinearRegression, LassoCV, RidgeCV, ElasticNetCV
from sklearn.linear_model.coordinate_descent import _alpha_grid
from sklearn.ensemble import RandomForestRegressor, ExtraTreesRegressor, GradientBoostingRegressor
from time import time
import pandas as pd
import os
from multiprocessing import Pool
from loky import get_reusable_executor
from enum import Enum
import errno

from sklearn.externals.joblib import dump, load
seed = 20190213

base_cols = ['PPO_12_26_indgrp', 'et', 'IPDCONGD', 'ebitda_to_ev_indgrp', 'revenue_yield', 'T5YFFM', 'rf', 'ret_1B',
             'overnight_return', 'grossmargin', 'netmargin_indgrp', 'maxret', 'OILPRICEx', 'currentratio',
             'ensemble', 'EWG_close', 'STOCHRSI_FASTK_63_15_10', 'bm', 'PPO_21_126', 'current_to_total_assets',
             'ret_63B', 'revenue_growth', 'PPO_21_126_Industrials', 'ret_2B', 'PPO_21_126_Energy', 'ebitdamargin',
             'debt2equity_indgrp', 'COMPAPFFx', 'ret_126B', 'log_dollar_volume', 'debt2equity', 'investments_yield',
             'fcf_yield', 'volatility_126_indgrp', 'macd_diff_indgrp', 'STOCHRSI_FASTD_14_5_3', 'PPO_21_126_indgrp',
             'amihud', 'VXOCLSx', 'ebitda_to_ev', 'pe1', 'macd_diff_ConsumerStaples', 'T1YFFM', 'volatility_21',
             'gbm', 'netmargin', 'ret_10B', 'pe_indgrp', 'divyield', 'WILLR_14', 'HWI', 'CUSR0000SAC',
             'divyield_indgrp', 'ret_63B_indgrp', 'payoutratio', 'EWJ_volume', 'PPO_12_26', 'PPO_3_14',
             'retearn_to_total_assets', 'std_turn', 'log_avg_dollar_volume', 'ret_252B', 'EXUSUKx', 'rel_to_high',
             'ADX_14_MA_3', 'macd_diff', 'capex_yield', 'bm_indgrp', 'PPO_21_126_InformationTechnology', 'gp2assets']

class ModelType(Enum):
    ensemble_value = 1
    ensemble_growth = 2
    gan_value = 3
    gan_growth = 4

# Logic is redundant because features and targets may evolve with time
def get_X_cols(model=1):
    if model==1:
        return base_cols
    elif model==2:
        return base_cols
    elif model==3:
        return base_cols + ['f_t', 'hidden_state_0', 'hidden_state_1', 'hidden_state_2', 'hidden_state_3']
    elif model==4:
        return base_cols + ['f_t', 'hidden_state_0', 'hidden_state_1', 'hidden_state_2', 'hidden_state_3']
    elif model in (6, 7):
        return base_cols
    else:
        return base_cols

def get_y_col(model=1):
    if model==1:
        return "future_ret_21B_std"
    elif model==2:
        return "future_ret_21B_std"
    elif model==3:
        return "future_ret_21B_std"   # "future_return_RF_100_std"
    elif model==4:
        return "future_ret_21B_std"   # "future_return_RF_100_std"
    elif model in (6, 7):
        return "future_ret_21B_std"   # "future_return_RF_100_std"
    else:
        return "future_ret_21B_std"

def get_return_col(model=1):
    if model==1:
        return "future_ret_21B"
    elif model==2:
        return "future_ret_21B"
    elif model==3:
        return "future_return_bme"
    elif model==4:
        return "future_return_bme"
    elif model in (6, 7):
        return "future_ret_21B"
    else:
        return "future_ret_21B"

def create_directory_if_does_not_exists(dir_path):
    try:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

def cut_insample_data(df, end_year, end_month, y_col, model=1):
    dates = pd.DatetimeIndex(df["date"].unique()).sort_values()
    monthly_final_date = dates[(dates.year==end_year) & (dates.month==end_month)][0]
    monthly_final_date_loc = dates.get_loc(monthly_final_date) + 1

    monthly_initial_date_loc = monthly_final_date_loc - 106
    in_sample_dates = dates[monthly_initial_date_loc:monthly_final_date_loc]

    fold_ids = pd.DataFrame({"date":in_sample_dates, "fold_id":pd.cut(in_sample_dates, 5).codes})

    new_df = df[df["date"].isin(in_sample_dates)].reset_index(drop=True)
    new_df.drop("fold_id", axis=1, errors="ignore", inplace=True)
    new_df = pd.merge(new_df, fold_ids, how="left", on=["date"])
    new_df = new_df.set_index(["date", "ticker"]).sort_index()

    #y_col = get_y_col(model)
    X_cols = get_X_cols(model)

    y = new_df[y_col].fillna(0)
    X = new_df[X_cols].fillna(0)
    n, p = X.shape

    return new_df["fold_id"], X, y, n, p, monthly_final_date

def get_algos():
    algos = {
        'ols': LinearRegression(),
        'lasso': LassoCV(random_state=seed),
        'enet': ElasticNetCV(random_state=seed),
        'rf': RandomForestRegressor(n_estimators=500, min_samples_leaf=0.001, random_state=seed, n_jobs=17,
                                    max_features=17, max_depth=10),
        'et': ExtraTreesRegressor(n_estimators=500, min_samples_leaf=0.001, random_state=seed, n_jobs=17,
                                  max_features=17, max_depth=10),
        'gbm': GradientBoostingRegressor(min_samples_leaf=0.001, random_state=seed, n_estimators=700,
                                         learning_rate=0.005, max_depth=6)
    }
    return algos


def feature_importance(folds, X, y, algo, grid, output_dir, algo_name, dump_file=False):
    print(algo.__class__)

    n_folds = len(folds.unique())
    gkf = GroupKFold(n_splits=n_folds)
    splits = list(gkf.split(X, y, folds))

    tic = time()

    if grid is not None:
        cv = GridSearchCV(algo, grid, cv=splits, n_jobs=n_jobs, return_train_score=True)
        cv.fit(X, y)
        algo = cv.best_estimator_
        print(cv.best_params_)
        score = pd.DataFrame(cv.cv_results_).loc[cv.best_index_]
        score = score[score.index.str.startswith('split') & score.index.str.endswith('test_score')]

    else:
        if 'cv' in algo.get_params().keys():
            algo.set_params(cv=splits)
        algo.fit(X.values, y.values)
        score = pd.Series([algo.score(X.iloc[s[1]], y.iloc[s[1]]) for s in splits])

    toc = time()
    fit_time = (toc - tic) / 60.0

    lapsed = {'fit': fit_time}
    print(lapsed)

    if dump_file:
        path_file = os.path.join(output_dir, '%s_neutral_r1_v_iso.joblib' % algo_name)
        dump(algo, path_file)
        estimator = path_file
    else:
        estimator = algo

    return {
        'estimator': estimator,
        'cv_score': score,
        'lapsed': lapsed
    }

def run_feature_importance_parallel(x):
    return feature_importance(*x)

def run_models(folds, X, y, algos, grids, models, output_dir, parallel, dump_file):
    if parallel:
        params = [[folds, X, y, algos[k], grids[k], output_dir, k] for k in models]
        p = Pool(6)
        res = p.map(run_feature_importance_parallel, params)
        p.terminate()
        p.join()
        results = dict(zip(models, res))
    else:
        results = {k: feature_importance(folds, X, y, algos[k], grids[k], output_dir, k, dump_file) for k in models}
    return results

def run_models_loky(folds, X, y, algos, grids, models, output_dir, parallel, dump_file):
    if parallel:
        params = [[folds, X, y, algos[k], grids[k], output_dir, k] for k in models]
        p = get_reusable_executor(max_workers=4, timeout=2)
        res = p.map(run_feature_importance_parallel, params)
        p.shutdown()
        results = dict(zip(models, res))
    else:
        results = {k: feature_importance(folds, X, y, algos[k], grids[k], output_dir, k, dump_file) for k in models}
    return results

def run_for_date(df, output_dir, end_year, end_month, target_col, model=1, run_all=True, run_parallel=False, dump_file=False):
    folds, X, y, n, p, monthly_final_date = cut_insample_data(df, end_year, end_month, target_col, model)
    date_key = monthly_final_date.strftime("%Y%m%d")
    out_dir = os.path.join(output_dir, date_key)
    if dump_file:
        create_directory_if_does_not_exists(out_dir)

    grids = {
        'ols': None,
        'lasso': None,
        'enet': None,
        'rf': None,
        'et': None,
        'gbm': None
    }
    algos = get_algos()

    models = ['ols', 'lasso', 'enet']
    if run_all:
        models = models + ['et', 'rf', 'gbm']
    results = run_models_loky(folds, X, y, algos, grids, models, out_dir, run_parallel, dump_file)
    if dump_file:
        results = {k:{l:(results[k][l] if l!="estimator" else load(results[k][l])) for l in results[k]} for k in results}
    return results, date_key
