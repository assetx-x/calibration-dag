import pandas as pd
from market_timeline import marketTimeline
import pandas as pd
import numpy as np
from datetime import datetime
import os
from abc import ABC, abstractmethod
from google.cloud import storage
#import statsmodels.api as sm
from google.cloud import storage
import gcsfs
from collections import defaultdict

current_date = datetime.now().date()


FILTER_MODES = ["growth", "value", "largecap_growth", "largecap_value"]
DEFAULT_MARKETCAP = 2000000000.0
NORMALIZATION_CONSTANT = 0.67448975019608171
_SMALL_EPSILON = np.finfo(np.float64).eps




def construct_required_path(step, file_name):
    return (
        "gs://{}/calibration_data/live"
        + "/{}/".format(step)
        + "{}.csv".format(file_name)
    )


def construct_destination_path(step):
    return "gs://{}/calibration_data/live" + "/{}/".format(step) + "{}.csv"


class DataFormatter(object):

    def __init__(self, class_, class_parameters, provided_data, required_data):

        self.class_ = class_
        self.class_parameters = class_parameters
        self.provided_data = provided_data
        self.required_data = required_data

    def construct_data(self):
        return {
            'class': self.class_,
            'params': self.class_parameters,
            # 'start_date':self.start_date,
            'provided_data': self._provided_data_construction(),
            'required_data': self._required_data_construction(),
        }

    def __call__(self):
        # When an instance is called, return the result of construct_data
        return self.construct_data()

    def _required_data_construction(self):

        if len(self.required_data) == 0:
            return {}
        elif len(self.required_data) == 1:
            directory = list(self.required_data.keys())[0]
            return {
                k: construct_required_path(directory, k)
                for k in self.required_data[directory]
            }

        else:
            path_dictionary = {}
            directories = list(self.required_data.keys())
            for directory in directories:
                path_dictionary.update(
                    {
                        k: construct_required_path(directory, k)
                        for k in self.required_data[directory]
                    }
                )
            return path_dictionary

    def _provided_data_construction(self):
        directory = list(self.provided_data.keys())[0]
        return {
            k: construct_destination_path(directory)
            for k in self.provided_data[directory]
        }


class DataReaderClass(ABC):

    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key = key

    @abstractmethod
    def _prepare_to_pull_data(self, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def _get_data_lineage(self):
        raise NotImplementedError()

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
    #print(f'Executing step action with args {step_action_args}')

    # Execute do_step_action method
    data_outputs = kwargs['class'](**params).do_step_action(**step_action_args)

    print('Your Params are')
    print(params)

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
            print('Here is the path')
            print(gcs_path)
            data_value.to_csv(gcs_path)

def offset_with_prices(data):
    # Step 1: Extract unique dates
    unique_dates = pd.to_datetime(data["date"].unique())

    # Step 2: Compute the offset for these unique dates
    unique_dates_with_offset = {date: marketTimeline.get_trading_day_using_offset(pd.Timestamp(date), 1) for date in unique_dates}

    # Step 3: Create a mapping from the original dates to the offset dates
    date_offset_mapping = pd.Series(unique_dates_with_offset)

    return date_offset_mapping

class FilterRussell1000Augmented(DataReaderClass):
    '''

    Filters data for month start or month end observations

    '''

    PROVIDES_FIELDS = ["r1k_models"]
    REQUIRES_FIELDS = [
        "data_full_population_signal",
        "russell_components",
        "quandl_daily",
        "raw_price_data",
    ]

    def __init__(
        self,
        start_date,
        end_date,
        filter_price_marketcap=False,
        price_limit=7.0,
        marketcap_limit=300000000.0,
        largecap_quantile=0.25,
    ):
        self.r1k_models = None
        self.start_date = pd.Timestamp(start_date) if start_date else None
        self.end_date = pd.Timestamp(end_date) if end_date else None
        self.filter_price_marketcap = filter_price_marketcap
        self.price_limit = price_limit
        self.marketcap_limit = marketcap_limit
        self.largecap_quantile = largecap_quantile

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def _get_whole_univ(self, data, russell_df, marketcap):
        temp = russell_df.pivot(index='date', columns='cusip', values='wt-r1')
        dates = temp.index.union(data['date'].unique())
        temp = temp.reindex(dates).fillna(method='ffill', limit=5)
        temp = temp.stack().reset_index()
        temp.columns = ['date', 'cusip', 'wt-r1']
        russell_df = pd.merge(
            temp[['date', 'cusip']], russell_df, how='left', on=['date', 'cusip']
        )
        russell_df.sort_values(['cusip', 'date'], inplace=True)
        russell_df = russell_df.groupby('cusip', as_index=False).fillna(method='ffill')

        data.rename(columns={'ticker': 'dcm_security_id'}, inplace=True)
        univ = russell_df[['date', 'dcm_security_id', 'wt-r1', 'wt-r1v', 'wt-r1g']]
        univ['dcm_security_id'] = univ['dcm_security_id'].astype(int)
        univ = pd.merge(data, univ, how='left', on=['date', 'dcm_security_id'])
        univ['dcm_security_id'] = univ['dcm_security_id'].astype(int)
        univ.rename(columns={"dcm_security_id": "ticker"}, inplace=True)

        for col in ['wt-r1', 'wt-r1g', 'wt-r1v']:
            univ[col] = univ[col].astype(float)
        # univ[['wt-r1', 'wt-r1g', 'wt-r1v']] = univ.groupby('ticker', as_index=False).fillna(method='ffill')[['wt-r1', 'wt-r1g', 'wt-r1v']]
        univ[['wt-r1', 'wt-r1g', 'wt-r1v']] = univ.groupby(
            'ticker', as_index=False
        ).fillna(value=0.0)[['wt-r1', 'wt-r1g', 'wt-r1v']]

        univ = pd.merge(univ, marketcap, how="left", on=["date", "ticker"])
        univ["marketcap"] = univ["marketcap"].fillna(DEFAULT_MARKETCAP)
        return univ

    def _filter_russell_components(self, univ, mode):
        # To pick the top 40th percentile (0.4), we need to filter the items greater than (1 - 0.4 = 0.6) i.e. 60th percentile
        marketcap_filter = univ["marketcap"] > univ.groupby("date")[
            "marketcap"
        ].transform("quantile", (1 - self.largecap_quantile))

        # ALEX EDIT
        if mode == "growth":
            univ_part1 = univ[univ['wt-r1g'] > 0].reset_index(drop=True)
            existing_tickers = univ_part1['ticker'].unique().tolist()
            future_univ = univ[
                (univ.date > '2023-03-06') & (univ.ticker.isin(existing_tickers))
            ]
            univ = pd.concat([univ_part1, future_univ], ignore_index=True)
            # univ = univ_part1.append(future_univ)

        elif mode == "value":
            univ_part1 = univ[univ['wt-r1v'] > 0].reset_index(drop=True)
            existing_tickers = univ_part1['ticker'].unique().tolist()
            future_univ = univ[
                (univ.date > '2023-03-06') & (univ.ticker.isin(existing_tickers))
            ]
            univ = pd.concat([univ_part1, future_univ], ignore_index=True)
            # univ = univ_part1.append(future_univ)

        elif mode == "largecap_growth":
            univ_part1 = univ[(univ['wt-r1g'] > 0) & marketcap_filter].reset_index(
                drop=True
            )
            existing_tickers = univ_part1['ticker'].unique().tolist()
            future_univ = univ[
                (univ.date > '2023-03-06') & (univ.ticker.isin(existing_tickers))
            ]
            univ = pd.concat([univ_part1, future_univ], ignore_index=True)
            # univ = univ_part1.append(future_univ)

        elif mode == "largecap_value":
            univ_part1 = univ[(univ['wt-r1v'] > 0) & marketcap_filter].reset_index(
                drop=True
            )
            existing_tickers = univ_part1['ticker'].unique().tolist()
            future_univ = univ[
                (univ.date > '2023-03-06') & (univ.ticker.isin(existing_tickers))
            ]
            univ = pd.concat([univ_part1, future_univ], ignore_index=True)
            # univ = univ_part1.append(future_univ)
        else:
            univ_part1 = univ[univ['wt-r1'] > 0].reset_index(drop=True)
            existing_tickers = univ_part1['ticker'].unique().tolist()
            future_univ = univ[
                (univ.date > '2023-03-06') & (univ.ticker.isin(existing_tickers))
            ]
            univ = pd.concat([univ_part1, future_univ], ignore_index=True)
            # univ = univ_part1.append(future_univ)

        univ["ticker"] = univ["ticker"].astype(int)
        univ.drop_duplicates(subset=['date', 'ticker'], inplace=True)
        return univ

    def _filter_price_marketcap(self, data_to_filter, raw_prices, marketcap):
        original_columns = list(data_to_filter.columns)
        aux_data = pd.merge(
            data_to_filter, raw_prices, how="left", on=["ticker", "date"]
        )
        aux_data = pd.merge(aux_data, marketcap, how="left", on=["ticker", "date"])
        aux_data = aux_data.sort_values(["ticker", "date"])
        aux_data["marketcap"] = aux_data["marketcap"].fillna(DEFAULT_MARKETCAP)

        cond = (aux_data["marketcap"] >= self.marketcap_limit) & (
            aux_data["raw_close_price"] >= self.price_limit
        )
        aux_data = aux_data[cond].reset_index(drop=True)
        data_to_filter = aux_data[original_columns]
        return data_to_filter

    def do_step_action(self, **kwargs):
        russell_components = kwargs[self.__class__.REQUIRES_FIELDS[1]].copy(deep=True)
        data_to_filter = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        marketcap = kwargs[self.__class__.REQUIRES_FIELDS[2]][
            ["date", "dcm_security_id", "marketcap"]
        ].rename(columns={"dcm_security_id": "ticker"})
        raw_prices = kwargs[self.__class__.REQUIRES_FIELDS[3]].copy(deep=True)
        raw_prices["ticker"] = raw_prices["ticker"].fillna(-1).astype(int)
        raw_prices_day_map = offset_with_prices(raw_prices)
        marketcap_day_map = offset_with_prices(marketcap)
        raw_prices["date"] = raw_prices['date'].map(raw_prices_day_map)
        marketcap["date"] = marketcap['date'].map(marketcap_day_map)

        if self.filter_price_marketcap:
            data_to_filter = self._filter_price_marketcap(
                data_to_filter, raw_prices, marketcap
            )

        self.start_date = (
            self.start_date
            if pd.notnull(self.start_date)
            else self.task_params.start_dt
        )
        self.end_date = (
            self.end_date if pd.notnull(self.end_date) else self.task_params.end_dt
        )

        univ = self._get_whole_univ(data_to_filter, russell_components, marketcap)
        r1k_dict = {}
        for mode in FILTER_MODES:
            filtered_df = self._filter_russell_components(univ, mode)
            r1k_dict[mode] = filtered_df[
                (filtered_df["date"] >= self.start_date)
                & (filtered_df["date"] <= self.end_date)
            ]

        self.r1k_models = pd.DataFrame.from_dict(r1k_dict, orient='index')
        return self.r1k_models

    def _get_additional_step_results(self):
        return {"r1k_models": self.r1k_models}

    @classmethod
    def get_default_config(cls):
        return {
            "start_date": "2009-03-15",
            "end_date": None,
            "filter_price_marketcap": True,
            "price_limit": 7.0,
            "marketcap_limit": 300000000.0,
            "largecap_quantile": 0.25,
        }  # end_date: 2019-08-30


class FilterRussell1000AugmentedWeekly(FilterRussell1000Augmented):
    PROVIDES_FIELDS = ["r1k_models", "r1k_models_sc_weekly", "r1k_models_lc_weekly"]
    REQUIRES_FIELDS = [
        "data_full_population_signal",
        "data_full_population_signal_weekly",
        "russell_components",
        "quandl_daily",
        "raw_price_data",
    ]

    def __init__(
        self,
        start_date,
        end_date,
        filter_price_marketcap=False,
        price_limit=7.0,
        marketcap_limit=300000000.0,
        largecap_quantile=0.25,
    ):
        self.r1k_models_sc_weekly = None
        self.r1k_models_lc_weekly = None
        FilterRussell1000Augmented.__init__(
            self,
            start_date,
            end_date,
            filter_price_marketcap,
            price_limit,
            marketcap_limit,
            largecap_quantile,
        )

    def do_step_action(self, **kwargs):

        data_to_filter_monthly = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(
            deep=True
        )
        data_to_filter_weekly = kwargs[self.__class__.REQUIRES_FIELDS[1]].copy(
            deep=True
        )
        russell_components = kwargs[self.__class__.REQUIRES_FIELDS[2]].copy(deep=True)
        marketcap = kwargs[self.__class__.REQUIRES_FIELDS[3]][
            ["date", "dcm_security_id", "marketcap"]
        ].rename(columns={"dcm_security_id": "ticker"})
        raw_prices = kwargs[self.__class__.REQUIRES_FIELDS[4]].copy(deep=True)
        raw_prices["ticker"] = raw_prices["ticker"].fillna(-1).astype(int)
        for dataframe in [raw_prices, data_to_filter_monthly,data_to_filter_weekly,marketcap]:

            dataframe['date'] = dataframe['date'].map(
                {i: pd.Timestamp(i) for i in dataframe.date.unique()})

            dataframe['date'] = dataframe['date'].map(
                offset_with_prices(dataframe)
            )

            print('shape: {}'.format(dataframe.shape))
            print('marketcap :{}'.format(self.marketcap_limit))


        if self.filter_price_marketcap:
            data_to_filter_monthly = self._filter_price_marketcap(
                data_to_filter_monthly, raw_prices, marketcap
            )
            data_to_filter_weekly = self._filter_price_marketcap(
                data_to_filter_weekly, raw_prices, marketcap
            )

        self.start_date = self.start_date
        self.end_date = self.end_date

        print('start_date', self.start_date)
        print('end_date', self.end_date)

        univ_monthly = self._get_whole_univ(
            data_to_filter_monthly, russell_components, marketcap
        )
        univ_weekly = self._get_whole_univ(
            data_to_filter_weekly, russell_components, marketcap
        )

        print('univ_monthly: {}'.format(univ_monthly.shape))

        r1k_dict_monthly = {}
        r1k_dict_sc_weekly = {}
        r1k_dict_lc_weekly = {}
        for mode in FILTER_MODES:
            filtered_df_monthly = self._filter_russell_components(univ_monthly, mode)
            filtered_df_weekly = self._filter_russell_components(univ_weekly, mode)
            r1k_dict_monthly[mode] = filtered_df_monthly[
                (filtered_df_monthly["date"] >= pd.Timestamp(self.start_date))
                & (filtered_df_monthly["date"] <= pd.Timestamp(self.end_date))
            ]

            filtered_weekly_mode_df = filtered_df_weekly[
                (filtered_df_weekly["date"] >= pd.Timestamp(self.start_date))
                & (filtered_df_weekly["date"] <= pd.Timestamp(self.end_date))
            ]
            if "largecap" in mode:
                r1k_dict_lc_weekly[mode] = filtered_weekly_mode_df
            else:
                r1k_dict_sc_weekly[mode] = filtered_weekly_mode_df

        self.r1k_models = pd.DataFrame(
            list(r1k_dict_monthly.items()), columns=['Key', 0]
        ).set_index('Key')
        self.r1k_models_sc_weekly = pd.DataFrame(
            list(r1k_dict_sc_weekly.items()), columns=['Key', 0]
        ).set_index('Key')
        self.r1k_models_lc_weekly = pd.DataFrame(
            list(r1k_dict_lc_weekly.items()), columns=['Key', 0]
        ).set_index('Key')

        return self._get_additional_step_results()

    def _get_additional_step_results(self):

        return {
            "r1k_models_growth": self.r1k_models.loc['growth'][0],
            "r1k_models_value": self.r1k_models.loc['value'][0],
            "r1k_models_largecap_value": self.r1k_models.loc['largecap_growth'][0],
            "r1k_models_largecap_growth": self.r1k_models.loc['largecap_value'][0],
            "r1k_models_sc_weekly_growth": self.r1k_models_sc_weekly.loc['growth'][0],
            "r1k_models_sc_weekly_value": self.r1k_models_sc_weekly.loc['value'][0],
            "r1k_models_lc_weekly_largecap_growth": self.r1k_models_lc_weekly.loc[
                'largecap_growth'
            ][0],
            "r1k_models_lc_weekly_largecap_value": self.r1k_models_lc_weekly.loc[
                'largecap_value'
            ][0],
        }


def ols_res(x, y):
    x = sm.add_constant(x)
    fit = sm.OLS(y, x).fit()
    return fit.resid


def neutralize_data(df, factors, exclusion_list):
    df = df.copy(deep=True)
    df = df.drop(['wt-r1', 'wt-r1g', 'wt-r1v'], axis=1)
    missing_cols = [k for k in exclusion_list if k not in df.columns]
    missing_factors = [k for k in factors if k not in df.columns]
    print("Missing columns refered to in exclusion list: {0}".format(missing_cols))
    print("Missing columns refered to in factor list: {0}".format(missing_factors))
    exclusion_list = [k for k in exclusion_list if k not in missing_cols]
    factors = [k for k in factors if k not in missing_factors]
    future_cols = df.columns[df.columns.str.startswith('future_')].tolist()
    x_cols = factors
    y_cols = df.columns.difference(
        future_cols
        + x_cols
        + exclusion_list
        + ['date', 'ticker', 'Sector', 'IndustryGroup']
    ).tolist()
    df.sort_values(['date', 'ticker'], inplace=True)
    df = df.reset_index(drop=True).set_index(['date', 'ticker'])
    df.drop_duplicates(inplace=True)
    df[y_cols + x_cols] = df[y_cols + x_cols].apply(pd.to_numeric, errors='ignore')
    for col in x_cols + y_cols:
        df[col] = df[col].fillna(df.groupby('date')[col].transform('median'))
    for dt in df.index.levels[0].unique():
        for y in y_cols:
            df.loc[dt, y] = ols_res(df.loc[dt, x_cols], df.loc[dt, y]).values
    df = df.reset_index()
    return df




# Set specific date
#RUN_DATE = current_date.strftime('%Y-%m-%d')
RUN_DATE = '2024-06-21'

filter_r1k_weekly_params = {
    'start_date': "2009-03-15",
    'end_date': RUN_DATE,
    'filter_price_marketcap': True,
    'price_limit': 6.0,
    'marketcap_limit': 800000000.0,
    'largecap_quantile': 0.25,
}


filter_r1k_weekly = DataFormatter(
    class_=FilterRussell1000AugmentedWeekly,
    class_parameters=filter_r1k_weekly_params,
    provided_data={
        'PopulationSplit': [
            'r1k_models_growth',
            'r1k_models_value',
            'r1k_models_largecap_value',
            'r1k_models_largecap_growth',
            'r1k_models_sc_weekly_growth',
            'r1k_models_sc_weekly_value',
            'r1k_models_lc_weekly_largecap_growth',
            'r1k_models_lc_weekly_largecap_value',
        ]
    },
    required_data={
        'GetAdjustmentFactors': ['adjustment_factor_data'],
        'MergeSignal': [
            'data_full_population_signal',
            'data_full_population_signal_weekly',
        ],
        'DataPull': ['russell_components'],
        'FundamentalCleanup': ['quandl_daily'],
        'GetRawPrices': ['raw_price_data'],
    },
)

if __name__ == "__main__":
    os.environ['GCS_BUCKET'] = 'dcm-prod-ba2f-us-dcm-data-test'
    RUN_DATE = '2024-06-22'
    filter_r1k_weekly_data = filter_r1k_weekly()
    airflow_wrapper(**filter_r1k_weekly_data)