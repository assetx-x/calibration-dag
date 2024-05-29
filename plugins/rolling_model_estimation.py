from enum import Enum

from core_classes import DataReaderClass
import pandas as pd
from datetime import datetime
import os
from core_classes import StatusType
from google.cloud import storage
import gcsfs
from collections import defaultdict

current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')
from joblib import dump, load
from sklearn.model_selection import GroupKFold, GridSearchCV, cross_val_score
from sklearn.linear_model import LinearRegression, LassoCV, RidgeCV, ElasticNetCV
from sklearn.ensemble import (
    RandomForestRegressor,
    ExtraTreesRegressor,
    GradientBoostingRegressor,
)
from time import time
from multiprocessing import Pool
import errno
import pyhocon
import tempfile
from core_classes import DataFormatter


os.environ['GCS_BUCKET'] = 'dcm-prod-ba2f-us-dcm-data-test'
os.environ['MODEL_DIR'] = ''

seed = 20190213
FILTER_MODES = ["growth", "value", "largecap_growth", "largecap_value"]


seed = 20190213

base_cols = [
    'PPO_12_26_indgrp',
    'et',
    'IPDCONGD',
    'ebitda_to_ev_indgrp',
    'revenue_yield',
    'T5YFFM',
    'rf',
    'ret_1B',
    'overnight_return',
    'grossmargin',
    'netmargin_indgrp',
    'maxret',
    'DCOILWTICO',
    'currentratio',
    'ensemble',
    'EWG_close',
    'STOCHRSI_FASTK_63_15_10',
    'bm',
    'PPO_21_126',
    'current_to_total_assets',
    'ret_63B',
    'revenue_growth',
    'PPO_21_126_Industrials',
    'ret_2B',
    'PPO_21_126_Energy',
    'ebitdamargin',
    'debt2equity_indgrp',
    'CPFF',
    'ret_126B',
    'log_dollar_volume',
    'debt2equity',
    'investments_yield',
    'fcf_yield',
    'volatility_126_indgrp',
    'macd_diff_indgrp',
    'STOCHRSI_FASTD_14_5_3',
    'PPO_21_126_indgrp',
    'amihud',
    'VIXCLS',
    'ebitda_to_ev',
    'pe1',
    'macd_diff_ConsumerStaples',
    'T1YFFM',
    'volatility_21',
    'gbm',
    'netmargin',
    'ret_10B',
    'pe_indgrp',
    'divyield',
    'WILLR_14',
    'HWI',
    'CUSR0000SAC',
    'divyield_indgrp',
    'ret_63B_indgrp',
    'payoutratio',
    'EWJ_volume',
    'PPO_12_26',
    'PPO_3_14',
    'retearn_to_total_assets',
    'std_turn',
    'log_avg_dollar_volume',
    'ret_252B',
    'DEXUSUK',
    'rel_to_high',
    'ADX_14_MA_3',
    'macd_diff',
    'capex_yield',
    'bm_indgrp',
    'PPO_21_126_InformationTechnology',
    'gp2assets',
]


class ModelType(Enum):
    ensemble_value = 1
    ensemble_growth = 2
    gan_value = 3
    gan_growth = 4


def read_csv_in_chunks(gcs_path, batch_size=10000, project_id='dcm-prod-ba2f'):
    """
    Reads a CSV file from Google Cloud Storage in chunks.
    Parameters:
    - gcs_path (str): The path to the CSV file on GCS.
    - batch_size (int, optional): The number of rows per chunk. Default is 10,000.
    - project_id (str, optional): The GCP project id. Default is 'dcm-prod-ba2f'.
    Yields:
    - pd.DataFrame: The data chunk as a DataFrame.
    """
    # Create GCS file system object
    fs = gcsfs.GCSFileSystem(project=project_id)

    # Open the GCS file for reading
    with fs.open(gcs_path, 'r') as f:
        # Yield chunks from the CSV
        for chunk in pd.read_csv(f, chunksize=batch_size, index_col=0):
            yield chunk


def airflow_wrapper(**kwargs):
    params = kwargs['params']

    # Read all required data into step_action_args dictionary
    step_action_args = {
        k: pd.read_csv(v.format(os.environ['GCS_BUCKET']), index_col=0)
        for k, v in kwargs['required_data'].items()
    }
    print(f'Executing step action with args {step_action_args}')

    # Execute do_step_action method
    data_outputs = kwargs['class'](**params).do_step_action(**step_action_args)

    # If the method doesn't return a dictionary (for classes returning just a single DataFrame)
    # convert it into a dictionary for consistency
    if not isinstance(data_outputs, dict):
        data_outputs = {list(kwargs['provided_data'].keys())[0]: data_outputs}

    # Save each output data to its respective path on GCS
    for data_key, data_value in data_outputs.items():
        if data_key in kwargs['provided_data']:
            gcs_path = kwargs['provided_data'][data_key].format(
                os.environ['GCS_BUCKET'], data_key
            )
            print(gcs_path)
            data_value.to_csv(gcs_path)


# Logic is redundant because features and targets may evolve with time
def get_X_cols(model=1):
    if model == 1:
        return base_cols
    elif model == 2:
        return base_cols
    elif model == 3:
        return base_cols + [
            'f_t',
            'hidden_state_0',
            'hidden_state_1',
            'hidden_state_2',
            'hidden_state_3',
        ]
    elif model == 4:
        return base_cols + [
            'f_t',
            'hidden_state_0',
            'hidden_state_1',
            'hidden_state_2',
            'hidden_state_3',
        ]
    elif model in (6, 7):
        return base_cols
    else:
        return base_cols


def get_y_col(model=1):
    if model == 1:
        return "future_ret_21B_std"
    elif model == 2:
        return "future_ret_21B_std"
    elif model == 3:
        return "future_ret_21B_std"  # "future_return_RF_100_std"
    elif model == 4:
        return "future_ret_21B_std"  # "future_return_RF_100_std"
    elif model in (6, 7):
        return "future_ret_21B_std"  # "future_return_RF_100_std"
    else:
        return "future_ret_21B_std"


def get_return_col(model=1):
    if model == 1:
        return "future_ret_21B"
    elif model == 2:
        return "future_ret_21B"
    elif model == 3:
        return "future_return_bme"
    elif model == 4:
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
    monthly_final_date = dates[(dates.year == end_year) & (dates.month == end_month)][0]
    monthly_final_date_loc = dates.get_loc(monthly_final_date) + 1

    monthly_initial_date_loc = monthly_final_date_loc - 106
    in_sample_dates = dates[monthly_initial_date_loc:monthly_final_date_loc]

    fold_ids = pd.DataFrame(
        {"date": in_sample_dates, "fold_id": pd.cut(in_sample_dates, 5).codes}
    )

    new_df = df[df["date"].isin(in_sample_dates)].reset_index(drop=True)
    new_df.drop("fold_id", axis=1, errors="ignore", inplace=True)
    new_df = pd.merge(new_df, fold_ids, how="left", on=["date"])
    new_df = new_df.set_index(["date", "ticker"]).sort_index()

    # y_col = get_y_col(model)
    X_cols = get_X_cols(model)

    y = new_df[y_col].fillna(0)
    X = new_df[X_cols].fillna(0)
    n, p = X.shape

    return new_df["fold_id"], X, y, n, p, monthly_final_date


def get_local_dated_dir(base_path, end_dt, relative_path):
    hocon_str = """
        {{
          date = {0}
          dir = {1}/${{date}}/{2}
        }}
        """.format(
        end_dt.strftime("%Y%m%d"), base_path, relative_path
    )
    directory = os.path.realpath(pyhocon.ConfigFactory.parse_string(hocon_str)["dir"])
    return directory


def get_algos():
    algos = {
        'ols': LinearRegression(),
        'lasso': LassoCV(random_state=seed),
        'enet': ElasticNetCV(random_state=seed),
        'rf': RandomForestRegressor(
            n_estimators=500,
            min_samples_leaf=0.001,
            random_state=seed,
            n_jobs=17,
            max_features=17,
            max_depth=10,
        ),
        'et': ExtraTreesRegressor(
            n_estimators=500,
            min_samples_leaf=0.001,
            random_state=seed,
            n_jobs=17,
            max_features=17,
            max_depth=10,
        ),
        'gbm': GradientBoostingRegressor(
            min_samples_leaf=0.001,
            random_state=seed,
            n_estimators=700,
            learning_rate=0.005,
            max_depth=6,
        ),
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
        score = score[
            score.index.str.startswith('split') & score.index.str.endswith('test_score')
        ]

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

    return {'estimator': estimator, 'cv_score': score, 'lapsed': lapsed}


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
        results = {
            k: feature_importance(
                folds, X, y, algos[k], grids[k], output_dir, k, dump_file
            )
            for k in models
        }
    return results


def run_models_loky(folds, X, y, algos, grids, models, output_dir, parallel, dump_file):
    if parallel:
        params = [[folds, X, y, algos[k], grids[k], output_dir, k] for k in models]
        p = get_reusable_executor(max_workers=4, timeout=2)
        res = p.map(run_feature_importance_parallel, params)
        p.shutdown()
        results = dict(zip(models, res))
    else:
        results = {
            k: feature_importance(
                folds, X, y, algos[k], grids[k], output_dir, k, dump_file
            )
            for k in models
        }
    return results


def run_for_date(
    df,
    output_dir,
    end_year,
    end_month,
    target_col,
    model=1,
    run_all=True,
    run_parallel=False,
    dump_file=False,
):

    folds, X, y, n, p, monthly_final_date = cut_insample_data(
        df, end_year, end_month, target_col, model
    )
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
        'gbm': None,
    }
    algos = get_algos()

    models = ['ols', 'lasso', 'enet']
    if run_all:
        models = models + ['et', 'rf', 'gbm']
    results = run_models_loky(
        folds, X, y, algos, grids, models, out_dir, run_parallel, dump_file
    )
    if dump_file:
        results = {
            k: {
                l: (results[k][l] if l != "estimator" else load(results[k][l]))
                for l in results[k]
            }
            for k in results
        }
    return results, date_key


class RollingModelEstimation(DataReaderClass):
    '''

    Cross-sectional standardization of features on factor-neutralized data

    '''

    PROVIDES_FIELDS = ["signals", "rolling_model_info"]
    REQUIRES_FIELDS = ["r1k_neutral_normal_models_with_foldId"]

    def __init__(
        self,
        date_combinations,
        ensemble_weights,
        bucket,
        key_base,
        local_save_dir,
        training_modes,
        model_codes,
        target_cols,
        return_cols,
    ):
        self.signals = None
        self.rolling_model_info = None
        self.signal_consolidated = None
        self.results = defaultdict(dict)
        self.date_combinations = date_combinations
        self.ensemble_weights = ensemble_weights
        self.local_save_dir = local_save_dir
        self.bucket = bucket
        self.key_base = key_base
        self.training_modes = training_modes
        self.model_codes = model_codes
        self.target_cols = target_cols
        self.return_cols = return_cols

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def _get_results_filename(self, model):
        if model == "value":
            results_file = 'results_v.joblib'
        elif model == "growth":
            results_file = 'results_g.joblib'
        elif model == "largecap_value":
            results_file = 'results_lv.joblib'
        elif model == "largecap_growth":
            results_file = 'results_lg.joblib'
        else:
            raise ValueError(
                "Got unexpected type of model to fetch the results_*.joblib filename - {0}".format(
                    model
                )
            )
        return results_file

    def rolling_estimation_common(self, df_v, df_g):
        model = self.value_model_code
        results_v = {}
        for year, month in self.date_combinations:
            current, key = run_for_date(
                df_v, out_dir, year, month, model, True, True, False
            )
            all_algos = {}
            for k in current.keys():
                all_algos[k] = current[k]["estimator"]
            results_v[key] = all_algos

        model = self.growth_model_code
        results_g = {}
        for year, month in self.date_combinations:
            current, key = run_for_date(
                df_g, out_dir, year, month, model, True, True, False
            )
            all_algos = {}
            for k in current.keys():
                all_algos[k] = current[k]["estimator"]
            results_g[key] = all_algos

        create_directory_if_does_not_exists(full_path)
        dump(results_v, os.path.join(full_path, "results_v.joblib"))
        dump(results_g, os.path.join(full_path, "results_g.joblib"))

    def train_rolling_models(self, model_name, model_df):

        full_path = get_local_dated_dir(
            os.environ["MODEL_DIR"], self.task_params.end_dt, self.local_save_dir
        )
        print("***********************************************")
        print("***********************************************")
        print(full_path)

        out_dir = ""
        model_code = self.model_codes[model_name]
        target_col = self.target_cols[str(model_code)]

        for year, month in self.date_combinations:
            print(
                "Final model training for: ",
                model_name,
                ", for date_combination: ",
                (year, month),
            )

            current, key = run_for_date(
                model_df,
                out_dir,
                year,
                month,
                target_col,
                model_code,
                True,
                True,
                False,
            )
            all_algos = {}
            for k in current.keys():
                all_algos[k] = current[k]["estimator"]
            self.results[model_name][key] = all_algos

        create_directory_if_does_not_exists(full_path)

        dump(
            self.results[model_name],
            os.path.join(full_path, self._get_results_filename(model_name)),
        )

        # DISCUSS: This will be same for every model trained, and will be overwritten every time after the training.
        self.rolling_model_info = pd.DataFrame(
            [self.local_save_dir], columns=["relative_path"]
        )

    def train_rolling_models_append(self, model_name, model_df):
        self.load_model(model_name)
        self.train_rolling_models(model_name, model_df)

    def temp_load_models(self):  # For testing only
        current_dir = r"C:\DCM\temp\pipeline_tests\snapshots_201910\models_v\20171229"
        models = ['enet', 'gbm', 'rf', 'ols', 'et', 'lasso']
        self.results_v = {}
        self.results_g = {}

        for d in [
            "20171229",
            "20180329",
            "20180629",
            "20180928",
            "20181231",
            "20190329",
            "20190628",
        ]:
            self.results_v[d] = {}
            for k in models:
                self.results_v[d][k] = load(
                    os.path.join(current_dir, "{0}_neutral_r1_v_iso.joblib".format(k))
                )

        current_dir = r"C:\DCM\temp\pipeline_tests\snapshots_201910\models_g\20171229"
        for d in [
            "20171229",
            "20180329",
            "20180629",
            "20180928",
            "20181231",
            "20190329",
            "20190628",
        ]:
            self.results_g[d] = {}
            for k in models:
                self.results_g[d][k] = load(
                    os.path.join(current_dir, "{0}_neutral_r1_v_iso.joblib".format(k))
                )

    def load_model(self, model_name):
        self.s3_client = storage.Client()

        key = '{0}/{1}'.format(self.key_base, self._get_results_filename(model_name))
        with tempfile.TemporaryFile() as fp:
            bucket = self.s3_client.bucket(self.bucket)
            blob = bucket.blob(key)
            self.s3_client.download_blob_to_file(blob, fp)
            fp.seek(0)
            self.results[model_name] = load(fp)

    def prediction(self, algo, X):
        pred = pd.Series(algo.predict(X), index=X.index)
        return pred

    def generate_ensemble_prediction(
        self, df, algos, target_variable_name, return_column_name, feature_cols
    ):
        ensemble_weights = self.ensemble_weights
        df['date'] = df['date'].apply(pd.Timestamp)
        df = df.set_index(["date", "ticker"])
        X = df[feature_cols].fillna(0.0)
        ret = df[return_column_name].reset_index().fillna(0.0)
        pred = pd.concat(
            (self.prediction(algos[k], X) for k in algos.keys()),
            axis=1,
            keys=algos.keys(),
        )
        ensemble_pred = pred[list(algos.keys())[0]] * 0.0
        for k in algos.keys():
            ensemble_pred += ensemble_weights[k] * pred[k]
        ensemble_pred = ensemble_pred.to_frame()
        ensemble_pred.columns = ["ensemble"]
        rank = pred.groupby("date").rank(pct=True)
        rank["ensemble"] = ensemble_pred.groupby("date").rank(pct=True)
        signal_ranks = pd.merge(
            rank.reset_index(),
            ret[['date', 'ticker', return_column_name]],
            how='left',
            on=['date', 'ticker'],
        )
        return signal_ranks

    def generate_ensemble_prediction_rolling(
        self,
        df,
        algos,
        target_variable_name,
        return_column_name,
        feature_cols,
        ensemble_weights=None,
    ):
        all_signals = {}
        for dt in list(algos.keys()):
            # print("Predictions for : \t {0}".format(dt))
            signal_ranks = self.generate_ensemble_prediction(
                df, algos[dt], target_variable_name, return_column_name, feature_cols
            )
            all_signals[dt] = signal_ranks
        return all_signals

    def concatenate_predictions(self, algos, all_signals):
        dates = list(algos.keys())
        dates.sort()
        real_dates = list(map(pd.Timestamp, dates))
        real_dates.append(pd.Timestamp("2040-12-31"))
        # print(real_dates)
        signal_ranks = []
        for ind in range(len(dates)):
            key = dates[ind]
            # print(key)
            current_signal = all_signals[key]
            cond = current_signal["date"] < real_dates[ind + 1]
            if ind > 0:
                cond = cond & (current_signal["date"] >= real_dates[ind])
            current_signal = current_signal[cond]
            # print(current_signal["date"].unique())
            signal_ranks.append(current_signal)

        signal_ranks = pd.concat(signal_ranks, ignore_index=True)
        return signal_ranks

    def do_step_action(self, **kwargs):
        r1k_dfs_dict = (
            kwargs["r1k_neutral_normal_models_with_foldId"]
            .copy(deep=True)
            .to_dict(orient='dict')[0]
        )

        assert set(r1k_dfs_dict) == set(
            FILTER_MODES
        ), "RollingModelEstimation - r1k_neutral_normal_models_with_foldId dict\
        doesn't seem to contain all expected modes. It contains- {0}".format(
            set(r1k_dfs_dict)
        )

        signals_dict = {}
        for model_name in r1k_dfs_dict:
            if self.training_modes[model_name] == 'load_from_s3':
                self.load_model(model_name)
            elif self.training_modes[model_name] == 'full_train':
                self.train_rolling_models(model_name, r1k_dfs_dict[model_name])
            elif self.training_modes[model_name] == 'append':
                self.train_rolling_models_append(model_name, r1k_dfs_dict[model_name])
            else:
                raise ValueError(
                    "RollingModelEstimation - training_modes should be one of - 'load_from_s3', 'full_train', or 'append'"
                )

            model_code = self.model_codes[model_name]
            target_variable_name = self.target_cols[
                str(model_code)
            ]  # get_y_col(model_code)
            return_column_name = self.return_cols[
                str(model_code)
            ]  # get_return_col(model_code)
            feature_cols = get_X_cols(model_code)
            all_signals = self.generate_ensemble_prediction_rolling(
                r1k_dfs_dict[model_name],
                self.results[model_name],
                target_variable_name,
                return_column_name,
                feature_cols,
            )
            signals_dict[model_name] = self.concatenate_predictions(
                self.results[model_name], all_signals
            )

        self.signals = pd.DataFrame(
            list(signals_dict.items()), columns=['Key', 0]
        ).set_index('Key')

        return StatusType.Success

    def _get_additional_step_results(self):
        return {"signals": self.signals, "rolling_model_info": self.rolling_model_info}

    @classmethod
    def get_default_config(cls):
        return {
            "date_combinations": [
                [2017, 12],
                [2018, 3],
                [2018, 6],
                [2018, 9],
                [2018, 12],
                [2019, 3],
                [2019, 6],
            ],
            "ensemble_weights": {
                'enet': 0.03333333333333333,
                'et': 0.3,
                'gbm': 0.2,
                'lasso': 0.03333333333333333,
                'ols': 0.03333333333333333,
                'rf': 0.4,
            },
            "local_save_dir": "rolling_models",
            "bucket": "dcm-data-temp",
            "key_base": "saved_rolling_models",
            "training_modes": {
                "value": "load_from_s3",
                "growth": "load_from_s3",
                "largecap": "load_from_s3",
            },
            "model_codes": {
                "value": 1,
                "growth": 2,
                "largecap_value": 6,
                "largecap_growth": 7,
            },
            "target_cols": {
                "1": "future_ret_21B_std",
                "2": "future_ret_21B_std",
                "6": "future_ret_21B_std",
                "7": "future_ret_21B_std",
            },
            "return_cols": {
                "1": "future_ret_21B",
                "2": "future_ret_21B",
                "6": "future_ret_21B",
                "7": "future_ret_21B",
            },
        }


class RollingModelEstimationWeekly(RollingModelEstimation):

    PROVIDES_FIELDS = ["signals", "signals_weekly", "rolling_model_info"]
    REQUIRES_FIELDS = [
        "r1k_neutral_normal_models_with_foldId",
        "r1k_sc_with_foldId_weekly",
        "r1k_lc_with_foldId_weekly",
    ]

    def __init__(
        self,
        date_combinations,
        ensemble_weights,
        bucket,
        key_base,
        local_save_dir,
        training_modes,
        model_codes,
        target_cols,
        return_cols,
    ):
        self.signals_weekly = None
        RollingModelEstimation.__init__(
            self,
            date_combinations,
            ensemble_weights,
            bucket,
            key_base,
            local_save_dir,
            training_modes,
            model_codes,
            target_cols,
            return_cols,
        )

    @staticmethod
    def _dictionary_format(**kwargs):
        population_splits = {k: v for k, v in kwargs.items()}
        for keys in population_splits.keys():
            population_splits[keys]['date'] = population_splits[keys]['date'].apply(
                pd.Timestamp
            )

        return population_splits

    def do_step_action(self, **kwargs):

        r1k_dfs_dict_monthly = self._dictionary_format(
            growth=kwargs["r1k_neutral_normal_models_with_foldId_growth"],
            value=kwargs["r1k_neutral_normal_models_with_foldId_value"],
            largecap_growth=kwargs[
                "r1k_neutral_normal_models_with_foldId_largecap_growth"
            ],
            largecap_value=kwargs[
                "r1k_neutral_normal_models_with_foldId_largecap_value"
            ],
        )
        r1k_dfs_dict_weekly = self._dictionary_format(
            growth=kwargs["r1k_sc_with_foldId_weekly_growth"],
            value=kwargs["r1k_sc_with_foldId_weekly_value"],
        )

        r1k_weekly_lc = self._dictionary_format(
            largecap_growth=kwargs["r1k_lc_with_foldId_weekly_largecap_growth"],
            largecap_value=kwargs["r1k_lc_with_foldId_weekly_largecap_value"],
        )

        r1k_dfs_dict_weekly.update(r1k_weekly_lc)

        assert set(r1k_dfs_dict_monthly) == set(
            FILTER_MODES
        ), "RollingModelEstimation - r1k_dfs_dict_monthly dict\
        doesn't seem to contain all expected modes. It contains- {0}".format(
            set(r1k_dfs_dict_monthly)
        )
        assert set(r1k_dfs_dict_weekly) == set(
            FILTER_MODES
        ), "RollingModelEstimation - r1k_dfs_dict_weekly dict\
        doesn't seem to contain all expected modes. It contains- {0}".format(
            set(r1k_dfs_dict_weekly)
        )

        signals_dict_monthly = {}
        signals_dict_weekly = {}
        for model_name in r1k_dfs_dict_monthly:
            if self.training_modes[model_name] == 'load_from_s3':
                self.load_model(model_name)
            elif self.training_modes[model_name] == 'full_train':
                self.train_rolling_models(model_name, r1k_dfs_dict_weekly[model_name])
            elif self.training_modes[model_name] == 'append':
                self.train_rolling_models_append(
                    model_name, r1k_dfs_dict_weekly[model_name]
                )
            else:
                raise ValueError(
                    "RollingModelEstimation - training_modes should be one of - 'load_from_s3', 'full_train', or 'append'"
                )

            model_code = self.model_codes[model_name]
            target_variable_name = self.target_cols[
                str(model_code)
            ]  # get_y_col(model_code)
            return_column_name = self.return_cols[
                str(model_code)
            ]  # get_return_col(model_code)
            feature_cols = get_X_cols(model_code)

            all_signals_monthly = self.generate_ensemble_prediction_rolling(
                r1k_dfs_dict_monthly[model_name],
                self.results[model_name],
                target_variable_name,
                return_column_name,
                feature_cols,
            )
            signals_dict_monthly[model_name] = self.concatenate_predictions(
                self.results[model_name], all_signals_monthly
            )

            all_signals_weekly = self.generate_ensemble_prediction_rolling(
                r1k_dfs_dict_weekly[model_name],
                self.results[model_name],
                target_variable_name,
                return_column_name,
                feature_cols,
            )
            signals_dict_weekly[model_name] = self.concatenate_predictions(
                self.results[model_name], all_signals_weekly
            )

        self.signals = self.signals = pd.DataFrame(
            list(signals_dict_monthly.items()), columns=['Key', 0]
        ).set_index('Key')

        self.signals_weekly = pd.DataFrame(
            list(signals_dict_weekly.items()), columns=['Key', 0]
        ).set_index('Key')

        return self._get_additional_step_results()

    def _get_additional_step_results(self):

        return {
            "signals_growth": self.signals.loc['growth'][0],
            'signals_value': self.signals.loc['value'][0],
            'signals_largecap_value': self.signals.loc['largecap_value'][0],
            'signals_largecap_growth': self.signals.loc['largecap_growth'][0],
            "signals_weekly_growth": self.signals_weekly.loc['growth'][0],
            "signals_weekly_value": self.signals_weekly.loc['value'][0],
            "signals_weekly_largecap_growth": self.signals_weekly.loc[
                'largecap_growth'
            ][0],
            "signals_weekly_largecap_value": self.signals_weekly.loc['largecap_value'][
                0
            ],
        }


rolling_model_estimator_params = {
    'date_combinations': [[2023, 9]],
    'ensemble_weights': {
        "enet": 0.03333333333333333,
        "et": 0.3,
        "gbm": 0.2,
        "lasso": 0.03333333333333333,
        "ols": 0.03333333333333333,
        "rf": 0.4,
    },
    'training_modes': {
        "value": "load_from_s3",
        "growth": "load_from_s3",
        "largecap_value": "load_from_s3",
        "largecap_growth": "load_from_s3",
    },  # load_from_s3, append, full_train
    'bucket': "dcm-prod-ba2f-us-dcm-data-test",
    'key_base': "calibration_data/live/saved_rolling_models_gan",
    'local_save_dir': "rolling_models_gan",
    'model_codes': {"value": 1, "growth": 2, "largecap_value": 6, "largecap_growth": 7},
    'target_cols': {
        "1": "future_ret_5B_std",
        "2": "future_ret_5B_std",
        "6": "future_ret_5B_std",
        "7": "future_ret_5B_std",
    },
    'return_cols': {
        "1": "future_ret_5B",
        "2": "future_ret_5B",
        "6": "future_ret_5B",
        "7": "future_ret_5B",
    },
}

# PROVIDES_FIELDS =  ["signals", "signals_weekly", "rolling_model_info"]
# REQUIRES_FIELDS =  ["r1k_neutral_normal_models_with_foldId", "r1k_sc_with_foldId_weekly", "r1k_lc_with_foldId_weekly"]

rolling_model_est = DataFormatter(
    class_=RollingModelEstimationWeekly,
    class_parameters=rolling_model_estimator_params,
    provided_data={
        'FinalModelTraining': [
            'r1k_neutral_normal_models_with_foldId_growth',
            'r1k_neutral_normal_models_with_foldId_value',
            'r1k_neutral_normal_models_with_foldId_largecap_value',
            'r1k_neutral_normal_models_with_foldId_largecap_growth',
            'r1k_sc_with_foldId_weekly_growth',
            'r1k_sc_with_foldId_weekly_value',
            'r1k_lc_with_foldId_weekly_largecap_growth',
            'r1k_lc_with_foldId_weekly_largecap_value',
        ]
    },
    required_data={
        'AddFinalFoldId': [
            'r1k_neutral_normal_models_with_foldId_growth',
            'r1k_neutral_normal_models_with_foldId_value',
            'r1k_neutral_normal_models_with_foldId_largecap_value',
            'r1k_neutral_normal_models_with_foldId_largecap_growth',
            'r1k_sc_with_foldId_weekly_growth',
            'r1k_sc_with_foldId_weekly_value',
            'r1k_lc_with_foldId_weekly_largecap_growth',
            'r1k_lc_with_foldId_weekly_largecap_value',
        ]
    },
)

if __name__ == "__main__":
    rolling_model_data = rolling_model_est()
    airflow_wrapper(**rolling_model_data)
