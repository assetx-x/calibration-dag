from core_classes import DataReaderClass
import pandas as pd
from time import time
import tempfile
from datetime import datetime
import os
from google.cloud import storage
import gcsfs
from joblib import dump, load

from sklearn.model_selection import GroupKFold, GridSearchCV, cross_val_score
from sklearn.linear_model import LinearRegression, LassoCV, RidgeCV, ElasticNetCV
from sklearn.ensemble import (
    RandomForestRegressor,
    ExtraTreesRegressor,
    GradientBoostingRegressor,
)
from time import time
import tempfile
import pyhocon


current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')
from core_classes import construct_required_path, construct_destination_path


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


def feature_importance(X, y, algo, grid, folds, n_jobs=6):

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

    return {'estimator': algo, 'cv_score': score, 'lapsed': lapsed}


def train_ml_model(folds, X, y, algo, grid, n_jobs=11):
    from sklearn.model_selection import GroupKFold, GridSearchCV, cross_val_score
    from sklearn.linear_model import LinearRegression, LassoCV, RidgeCV, ElasticNetCV
    from sklearn.ensemble import (
        RandomForestRegressor,
        ExtraTreesRegressor,
        GradientBoostingRegressor,
    )
    from sklearn.externals.joblib import dump, load

    print(algo.__class__)

    n_folds = folds.nunique()
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

    return {'estimator': algo, 'cv_score': score, 'lapsed': lapsed}


class TrainIntermediateModels(DataReaderClass):
    '''

    Add id's for cross validation

    '''

    PROVIDES_FIELDS = ["intermediate_signals", "econ_model_info"]
    REQUIRES_FIELDS = ["normalized_data_full_population_with_foldId"]

    # SEED = 20190213
    def __init__(
        self,
        y_col,
        X_cols,
        return_col,
        ensemble_weights,
        bucket,
        key_base,
        source_path,
        local_save_dir,
        load_from_s3,
    ):
        self.data = None
        self.econ_model_info = None
        self.y_col = y_col
        self.X_cols = X_cols
        self.return_col = return_col
        self.ensemble_weights = ensemble_weights
        self.load_from_s3 = load_from_s3
        self.bucket = bucket
        self.key_base = key_base
        self.source_path = source_path
        self.local_save_dir = local_save_dir
        self.s3_client = None
        self.seed = 20190213
        if os.name == "nt":
            self.n_jobs = 30
        else:
            self.n_jobs = 64

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def _train_models(self, df):
        models = ['ols', 'lasso', 'enet', 'rf', 'et', 'gbm']
        oos_id = df["fold_id"].max()
        df = df[df["fold_id"] < oos_id].reset_index(drop=True)
        df = df.set_index(["date", "ticker"]).sort_index()

        y = df[self.y_col].fillna(0)
        X = df[self.X_cols]

        # Define your algorithms here, as you did before
        algos = {
            'ols': LinearRegression(),
            'lasso': LassoCV(random_state=self.seed),
            'enet': ElasticNetCV(random_state=self.seed),
            'rf': RandomForestRegressor(
                n_estimators=500,
                max_depth=5,
                min_samples_leaf=0.001,
                random_state=self.seed,
                n_jobs=self.n_jobs,
            ),
            'et': ExtraTreesRegressor(
                n_estimators=500,
                max_depth=5,
                min_samples_leaf=0.001,
                random_state=self.seed,
                n_jobs=self.n_jobs,
            ),
            'gbm': GradientBoostingRegressor(
                n_estimators=200, min_samples_leaf=0.001, random_state=self.seed
            ),
        }

        # CV only for lasso and enet
        grids = {
            'ols': None,
            'lasso': None,
            'enet': None,
            'rf': {'max_features': [6], 'max_depth': [6]},
            'et': {'max_features': [6], 'max_depth': [10]},
            'gbm': {'n_estimators': [400], 'max_depth': [7], 'learning_rate': [0.001]},
        }  # Same as before
        folds = df["fold_id"]

        results = {
            k: feature_importance(X, y, algos[k], grids[k], folds, self.n_jobs)[
                "estimator"
            ]
            for k in models
        }
        print(results)

        # Initialize GCS client
        client = storage.Client()
        bucket = client.bucket(self.bucket)
        print('bucket: {}'.format(self.bucket))
        for k, model in results.items():
            local_filename = f'{k}_econ_no_gan.joblib'
            # Save model locally
            print('saving file locally : {}'.format(local_filename))
            dump(model, local_filename)

            # Define the GCS path
            print(
                'saving to path : {}'.format(
                    os.path.join(self.source_path, local_filename)
                )
            )
            blob = bucket.blob(os.path.join(self.source_path, local_filename))
            # Upload to GCS
            blob.upload_from_filename(local_filename)
            print(f"Uploaded {local_filename} to GCS")

            # Delete local file
            os.remove(local_filename)

        self.econ_model_info = pd.DataFrame([self.key_base], columns=["relative_path"])
        return results

    def _load_models(self):
        self.s3_client = storage.Client()
        models = ['ols', 'lasso', 'enet', 'rf', 'et', 'gbm']
        results = {}
        for model in models:
            key = '{0}/{1}_econ_no_gan.joblib'.format(self.key_base, model)
            with tempfile.TemporaryFile() as fp:
                bucket = self.s3_client.bucket(self.bucket)
                blob = bucket.blob(key)
                self.s3_client.download_blob_to_file(blob, fp)
                fp.seek(0)
                results[model] = load(fp)
        return results

    def prediction(self, algo, X):
        pred = pd.Series(algo.predict(X), index=X.index)
        return pred

    def generate_ensemble_prediction(
        self, df, algos, target_variable_name, return_column_name, feature_cols
    ):
        ensemble_weights = self.ensemble_weights
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

    def do_step_action(self, **kwargs):
        df = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        if self.load_from_s3:
            algos = self._load_models()
        else:
            algos = self._train_models(df)
        target_variable_name = self.y_col
        return_column_name = self.return_col
        feature_cols = self.X_cols
        signal_ranks = self.generate_ensemble_prediction(
            df, algos, target_variable_name, return_column_name, feature_cols
        )
        self.data = signal_ranks
        return StatusType.Success

    def _get_additional_step_results(self):
        return {
            self.__class__.PROVIDES_FIELDS[0]: self.data,
            self.__class__.PROVIDES_FIELDS[1]: self.econ_model_info,
        }

    @classmethod
    def get_default_config(cls):
        return {
            "y_col": "future_ret_21B_std",
            "X_cols": [
                'EWG_close',
                'HWI',
                'IPDCONGD',
                'EXUSUKx',
                'COMPAPFFx',
                'GS5',
                'CUSR0000SAC',
                'T5YFFM',
                'PPO_21_126_InformationTechnology',
                'macd_diff_ConsumerStaples',
                'PPO_21_126_Industrials',
                'VXOCLSx',
                'PPO_21_126_Energy',
                'T1YFFM',
                'WPSID62',
                'CUSR0000SA0L2',
                'EWJ_volume',
                'PPO_21_126_ConsumerDiscretionary',
                'OILPRICEx',
                'GS10',
                'RPI',
                'CPITRNSL',
                'divyield_ConsumerStaples',
                'bm_Financials',
                'USTRADE',
                'T10YFFM',
                'divyield_Industrials',
                'AAAFFM',
                'RETAILx',
                'bm_Utilities',
                'SPY_close',
                'log_mktcap',
                'volatility_126',
                'momentum',
                'bm',
                'PPO_12_26',
                'SPY_beta',
                'log_dollar_volume',
                'fcf_yield',
            ],
            "return_col": "future_ret_21B",
            "ensemble_weights": {
                'enet': 0.03333333333333333,
                'et': 0.3,
                'gbm': 0.2,
                'lasso': 0.03333333333333333,
                'ols': 0.03333333333333333,
                'rf': 0.4,
            },
            "load_from_s3": False,
            "bucket": "dcm-data-temp",
            "key_base": "saved_econ_models",
            "local_save_dir": "econ_models",
        }


class TrainIntermediateModelsWeekly(TrainIntermediateModels):
    PROVIDES_FIELDS = [
        "intermediate_signals",
        "intermediate_signals_weekly",
        "econ_model_info",
    ]
    REQUIRES_FIELDS = [
        "normalized_data_full_population_with_foldId",
        "normalized_data_full_population_with_foldId_weekly",
    ]

    def __init__(
        self,
        y_col,
        X_cols,
        return_col,
        ensemble_weights,
        bucket,
        key_base,
        source_path,  # This parameter needs to be passed to the superclass
        local_save_dir,
        load_from_s3,
    ):
        self.weekly_data = None
        TrainIntermediateModels.__init__(
            self,
            y_col,
            X_cols,
            return_col,
            ensemble_weights,
            bucket,
            key_base,
            source_path,  # Add this line to pass source_path correctly
            local_save_dir,
            load_from_s3,
        )

    def do_step_action(self, **kwargs):
        monthly_df = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        weekly_df = kwargs[self.__class__.REQUIRES_FIELDS[1]]
        if self.load_from_s3:
            print('loading models')
            algos = self._load_models()
            print('models loaded')
        else:
            print('training models')
            algos = self._train_models(monthly_df)
        target_variable_name = self.y_col
        return_column_name = self.return_col
        feature_cols = self.X_cols
        monthly_signal_ranks = self.generate_ensemble_prediction(
            monthly_df, algos, target_variable_name, return_column_name, feature_cols
        )
        weekly_signal_ranks = self.generate_ensemble_prediction(
            weekly_df, algos, target_variable_name, return_column_name, feature_cols
        )
        self.data = monthly_signal_ranks
        self.weekly_data = weekly_signal_ranks

        return {
            'intermediate_signals': self.data,
            'intermediate_signals_weekly': self.weekly_data,
        }

    def _get_additional_step_results(self):
        return {
            self.__class__.PROVIDES_FIELDS[0]: self.data,
            self.__class__.PROVIDES_FIELDS[1]: self.weekly_data,
            self.__class__.PROVIDES_FIELDS[2]: self.econ_model_info,
        }


TMW_params = {
    'y_col': "future_return_RF_100_std",
    'X_cols': [
        "EWG_close",
        "HWI",
        "IPDCONGD",
        "DEXUSUK",
        "CPFF",
        "GS5",
        "CUSR0000SAC",
        "T5YFFM",
        "PPO_21_126_InformationTechnology",
        "macd_diff_ConsumerStaples",
        "PPO_21_126_Industrials",
        "VIXCLS",
        "PPO_21_126_Energy",
        "T1YFFM",
        "WPSID62",
        "CUSR0000SA0L2",
        "EWJ_volume",
        "PPO_21_126_ConsumerDiscretionary",
        "DCOILWTICO",
        "GS10",
        "RPI",
        "CPITRNSL",
        "divyield_ConsumerStaples",
        "bm_Financials",
        "USTRADE",
        "T10YFFM",
        "divyield_Industrials",
        "AAAFFM",
        "RETAILx",
        "bm_Utilities",
        "SPY_close",
        "log_mktcap",
        "volatility_126",
        "momentum",
        "bm",
        "PPO_12_26",
        "SPY_beta",
        "log_dollar_volume",
        "fcf_yield",
    ],
    'return_col': "future_ret_21B",
    'ensemble_weights': {
        "enet": 0.03333333333333333,
        "et": 0.3,
        "gbm": 0.2,
        "lasso": 0.03333333333333333,
        "ols": 0.03333333333333333,
        "rf": 0.4,
    },
    'load_from_s3': False,
    'bucket': 'dcm-prod-ba2f-us-dcm-data-test',
    'key_base': "gs://dcm-prod-ba2f-us-dcm-data-test/calibration_data/live/saved_econ_models_gan",
    'source_path': 'calibration_data/live/saved_econ_models_gan',
    'local_save_dir': "econ_models_gan",
}

TrainIntermediateModelsWeekly_params = {
    'params': TMW_params,
    'class': TrainIntermediateModelsWeekly,
    'start_date': RUN_DATE,
    'provided_data': {
        'intermediate_signals': construct_destination_path(
            'intermediate_model_training'
        ),
        'intermediate_signals_weekly': construct_destination_path(
            'intermediate_model_training'
        ),
    },
    'required_data': {
        'normalized_data_full_population_with_foldId': construct_required_path(
            'merge_gan_results', 'normalized_data_full_population_with_foldId'
        ),
        'normalized_data_full_population_with_foldId_weekly': construct_required_path(
            'merge_gan_results', 'normalized_data_full_population_with_foldId'
        ),
    },
}
