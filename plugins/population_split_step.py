import pandas as pd
from core_classes import GCPReader,download_yahoo_data,DataReaderClass
from market_timeline import marketTimeline
import pandas as pd
import numpy as np
from datetime import datetime
import os
from core_classes import StatusType
from google.cloud import storage
import statsmodels.api as sm
current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')
from core_classes import construct_required_path,construct_destination_path

FILTER_MODES = ["growth", "value", "largecap_growth", "largecap_value"]
DEFAULT_MARKETCAP = 2000000000.0
NORMALIZATION_CONSTANT = 0.67448975019608171
_SMALL_EPSILON = np.finfo(np.float64).eps

class FilterRussell1000Augmented(DataReaderClass):
    '''

    Filters data for month start or month end observations

    '''

    PROVIDES_FIELDS = ["r1k_models"]
    REQUIRES_FIELDS = ["data_full_population_signal", "russell_components", "quandl_daily", "raw_price_data"]

    def __init__(self, start_date, end_date, filter_price_marketcap=False, price_limit=7.0,
                 marketcap_limit=300000000.0, largecap_quantile=0.25):
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
        russell_df = pd.merge(temp[['date', 'cusip']], russell_df, how='left', on=['date', 'cusip'])
        russell_df.sort_values(['cusip', 'date'], inplace=True)
        russell_df = russell_df.groupby('cusip', as_index=False).fillna(method='ffill')

        data.rename(columns={'ticker': 'dcm_security_id'}, inplace=True)
        univ = russell_df[['date', 'dcm_security_id', 'wt-r1', 'wt-r1v', 'wt-r1g']]
        univ = pd.merge(data, univ, how='left', on=['date', 'dcm_security_id'])
        univ['dcm_security_id'] = univ['dcm_security_id'].astype(int)
        univ.rename(columns={"dcm_security_id": "ticker"}, inplace=True)

        for col in ['wt-r1', 'wt-r1g', 'wt-r1v']:
            univ[col] = univ[col].astype(float)
        # univ[['wt-r1', 'wt-r1g', 'wt-r1v']] = univ.groupby('ticker', as_index=False).fillna(method='ffill')[['wt-r1', 'wt-r1g', 'wt-r1v']]
        univ[['wt-r1', 'wt-r1g', 'wt-r1v']] = univ.groupby('ticker', as_index=False).fillna(value=0.0)[
            ['wt-r1', 'wt-r1g', 'wt-r1v']]

        univ = pd.merge(univ, marketcap, how="left", on=["date", "ticker"])
        univ["marketcap"] = univ["marketcap"].fillna(DEFAULT_MARKETCAP)
        return univ

    def _filter_russell_components(self, univ, mode):
        # To pick the top 40th percentile (0.4), we need to filter the items greater than (1 - 0.4 = 0.6) i.e. 60th percentile
        marketcap_filter = (univ["marketcap"] > univ.groupby("date")["marketcap"].transform("quantile", (
                    1 - self.largecap_quantile)))

        # ALEX EDIT
        if mode == "growth":
            univ_part1 = univ[univ['wt-r1g'] > 0].reset_index(drop=True)
            existing_tickers = univ_part1['ticker'].unique().tolist()
            future_univ = univ[(univ.date > '2023-03-06') & (univ.ticker.isin(existing_tickers))]
            univ = univ_part1.append(future_univ)

        elif mode == "value":
            univ_part1 = univ[univ['wt-r1v'] > 0].reset_index(drop=True)
            existing_tickers = univ_part1['ticker'].unique().tolist()
            future_univ = univ[(univ.date > '2023-03-06') & (univ.ticker.isin(existing_tickers))]
            univ = univ_part1.append(future_univ)

        elif mode == "largecap_growth":
            univ_part1 = univ[(univ['wt-r1g'] > 0) & marketcap_filter].reset_index(drop=True)
            existing_tickers = univ_part1['ticker'].unique().tolist()
            future_univ = univ[(univ.date > '2023-03-06') & (univ.ticker.isin(existing_tickers))]
            univ = univ_part1.append(future_univ)

        elif mode == "largecap_value":
            univ_part1 = univ[(univ['wt-r1v'] > 0) & marketcap_filter].reset_index(drop=True)
            existing_tickers = univ_part1['ticker'].unique().tolist()
            future_univ = univ[(univ.date > '2023-03-06') & (univ.ticker.isin(existing_tickers))]
            univ = univ_part1.append(future_univ)
        else:
            univ_part1 = univ[univ['wt-r1'] > 0].reset_index(drop=True)
            existing_tickers = univ_part1['ticker'].unique().tolist()
            future_univ = univ[(univ.date > '2023-03-06') & (univ.ticker.isin(existing_tickers))]
            univ = univ_part1.append(future_univ)

        univ["ticker"] = univ["ticker"].astype(int)
        univ.drop_duplicates(subset=['date', 'ticker'], inplace=True)
        return univ

    def _filter_price_marketcap(self, data_to_filter, raw_prices, marketcap):
        original_columns = list(data_to_filter.columns)
        aux_data = pd.merge(data_to_filter, raw_prices, how="left", on=["ticker", "date"])
        aux_data = pd.merge(aux_data, marketcap, how="left", on=["ticker", "date"])
        aux_data = aux_data.sort_values(["ticker", "date"])
        aux_data["marketcap"] = aux_data["marketcap"].fillna(DEFAULT_MARKETCAP)

        cond = (aux_data["marketcap"] >= self.marketcap_limit) & (aux_data["raw_close_price"] >= self.price_limit)
        aux_data = aux_data[cond].reset_index(drop=True)
        data_to_filter = aux_data[original_columns]
        return data_to_filter

    def do_step_action(self, **kwargs):
        russell_components = kwargs[self.__class__.REQUIRES_FIELDS[1]].copy(deep=True)
        data_to_filter = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        marketcap = kwargs[self.__class__.REQUIRES_FIELDS[2]][["date", "dcm_security_id", "marketcap"]] \
            .rename(columns={"dcm_security_id": "ticker"})
        raw_prices = kwargs[self.__class__.REQUIRES_FIELDS[3]].copy(deep=True)
        raw_prices["ticker"] = raw_prices["ticker"].fillna(-1).astype(int)

        raw_prices["date"] = raw_prices["date"].apply(lambda x: marketTimeline.get_trading_day_using_offset(x, 1))
        marketcap["date"] = marketcap["date"].apply(lambda x: marketTimeline.get_trading_day_using_offset(x, 1))

        if self.filter_price_marketcap:
            data_to_filter = self._filter_price_marketcap(data_to_filter, raw_prices, marketcap)

        self.start_date = self.start_date if pd.notnull(self.start_date) else self.task_params.start_dt
        self.end_date = self.end_date if pd.notnull(self.end_date) else self.task_params.end_dt

        univ = self._get_whole_univ(data_to_filter, russell_components, marketcap)
        r1k_dict = {}
        for mode in FILTER_MODES:
            filtered_df = self._filter_russell_components(univ, mode)
            r1k_dict[mode] = filtered_df[
                (filtered_df["date"] >= self.start_date) & (filtered_df["date"] <= self.end_date)]

        self.r1k_models = pd.DataFrame.from_dict(r1k_dict, orient='index')
        return StatusType.Success

    def _get_additional_step_results(self):
        return {"r1k_models": self.r1k_models}

    @classmethod
    def get_default_config(cls):
        return {"start_date": "2009-03-15", "end_date": None, "filter_price_marketcap": True, "price_limit": 7.0,
                "marketcap_limit": 300000000.0, "largecap_quantile": 0.25}  # end_date: 2019-08-30


class FilterRussell1000AugmentedWeekly(FilterRussell1000Augmented):
    PROVIDES_FIELDS = ["r1k_models", "r1k_models_sc_weekly", "r1k_models_lc_weekly"]
    REQUIRES_FIELDS = ["data_full_population_signal", "data_full_population_signal_weekly",
                       "russell_components", "quandl_daily", "raw_price_data"]

    def __init__(self, start_date, end_date, filter_price_marketcap=False, price_limit=7.0,
                 marketcap_limit=300000000.0, largecap_quantile=0.25):
        self.r1k_models_sc_weekly = None
        self.r1k_models_lc_weekly = None
        FilterRussell1000Augmented.__init__(self, start_date, end_date, filter_price_marketcap,
                                            price_limit, marketcap_limit, largecap_quantile)

    def do_step_action(self, **kwargs):

        data_to_filter_monthly = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        data_to_filter_weekly = kwargs[self.__class__.REQUIRES_FIELDS[1]].copy(deep=True)
        russell_components = kwargs[self.__class__.REQUIRES_FIELDS[2]].copy(deep=True)
        marketcap = kwargs[self.__class__.REQUIRES_FIELDS[3]][["date", "dcm_security_id", "marketcap"]] \
            .rename(columns={"dcm_security_id": "ticker"})
        raw_prices = kwargs[self.__class__.REQUIRES_FIELDS[4]].copy(deep=True)
        raw_prices["ticker"] = raw_prices["ticker"].fillna(-1).astype(int)


        raw_prices["date"] = raw_prices["date"].apply(lambda x: marketTimeline.get_trading_day_using_offset(x, 1))
        marketcap["date"] = marketcap["date"].apply(lambda x: marketTimeline.get_trading_day_using_offset(x, 1))

        if self.filter_price_marketcap:
            data_to_filter_monthly = self._filter_price_marketcap(data_to_filter_monthly, raw_prices, marketcap)
            data_to_filter_weekly = self._filter_price_marketcap(data_to_filter_weekly, raw_prices, marketcap)

        self.start_date = self.start_date
        self.end_date = self.end_date

        univ_monthly = self._get_whole_univ(data_to_filter_monthly, russell_components, marketcap)
        univ_weekly = self._get_whole_univ(data_to_filter_weekly, russell_components, marketcap)

        r1k_dict_monthly = {}
        r1k_dict_sc_weekly = {}
        r1k_dict_lc_weekly = {}
        for mode in FILTER_MODES:
            filtered_df_monthly = self._filter_russell_components(univ_monthly, mode)
            filtered_df_weekly = self._filter_russell_components(univ_weekly, mode)
            r1k_dict_monthly[mode] = filtered_df_monthly[
                (filtered_df_monthly["date"] >= self.start_date) & (filtered_df_monthly["date"] <= self.end_date)]

            filtered_weekly_mode_df = filtered_df_weekly[
                (filtered_df_weekly["date"] >= self.start_date) & (filtered_df_weekly["date"] <= self.end_date)]
            if "largecap" in mode:
                r1k_dict_lc_weekly[mode] = filtered_weekly_mode_df
            else:
                r1k_dict_sc_weekly[mode] = filtered_weekly_mode_df

        self.r1k_models = pd.DataFrame.from_dict(r1k_dict_monthly, orient='index')
        self.r1k_models_sc_weekly = pd.DataFrame.from_dict(r1k_dict_sc_weekly, orient='index')
        self.r1k_models_lc_weekly = pd.DataFrame.from_dict(r1k_dict_lc_weekly, orient='index')

        return {"r1k_models": self.r1k_models,
                "r1k_models_sc_weekly": self.r1k_models_sc_weekly,
                "r1k_models_lc_weekly": self.r1k_models_lc_weekly}

    def _get_additional_step_results(self):
        return {"r1k_models": self.r1k_models,
                "r1k_models_sc_weekly": self.r1k_models_sc_weekly,
                "r1k_models_lc_weekly": self.r1k_models_lc_weekly}


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
        future_cols + x_cols + exclusion_list + ['date', 'ticker', 'Sector', 'IndustryGroup']).tolist()
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



params = {
        'start_date': "2009-03-15",
        'end_date': RUN_DATE,
        'filter_price_marketcap': True,
        'price_limit': 6.0,
        'marketcap_limit': 800000000.0,
        'largecap_quantile': 0.25,
    }


FilterRussell1000AugmentedWeekly_params = {'params': params,
                                           'class':FilterRussell1000AugmentedWeekly,
                                           'start_date': RUN_DATE,
                                           'provided_data': {
                                             'r1k_models': construct_destination_path(
                                                 'population_split'),
                                             'r1k_models_sc_weekly': construct_destination_path(
                                                 'population_split'),
                                               'r1k_models_lc_weekly': construct_destination_path(
                                                 'population_split')
                                             },

                                           'required_data':{'data_full_population_signal':
                                                                construct_required_path('merge_signal',
                                                                                        'data_full_population_signal'),
                                                            'data_full_population_signal_weekly':
                                                                construct_required_path('merge_signal',
                                                                                        'data_full_population_signal_weekly'),

                                                            'russell_components': construct_required_path('data_pull',
                                                                                                          'russell_components'),
                                                            'quandl_daily': construct_required_path('data_pull',
                                                                                                    'quandl_daily'),

                                                            'raw_price_data': construct_required_path('get_raw_prices',
                                                                                                      'raw_price_data')
                                                            }
                                           }




