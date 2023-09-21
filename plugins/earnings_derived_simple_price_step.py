from core_classes import GCPReader,download_yahoo_data,DataReaderClass
import pandas as pd
import numpy as np
from datetime import datetime
from core_classes import StatusType
from commonlib import talib_STOCHRSI, MA,talib_PPO, talib_TRIX,macd,winsorize
import talib
current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')
from core_classes import construct_required_path_earnings as construct_required_path
from core_classes import construct_destination_path_earnings as construct_destination_path

class ComputeBetaQuantamental(DataReaderClass):
    '''

    Calculates the regression beta of daily returns against a list of benchmarks

    '''

    PROVIDES_FIELDS = ["beta_data"]
    REQUIRES_FIELDS = ["daily_price_data", "intervals_data"]

    def __init__(self, benchmark_names, beta_lookback, offset_unit, price_column, dropna_pctg, use_robust, epsilon,
                 alpha, fit_intercept):
        self.data = None
        self.benchmark_names = benchmark_names or [8554]  # ["SPY"] # This is overridden for now
        self.beta_lookback = beta_lookback or 63
        self.beta_offset = "{0}{1}".format(self.beta_lookback, offset_unit)
        self.price_column = price_column or "close"
        self.dropna_pctg = dropna_pctg or 0.15
        # TODO: incorporate the robust method at a later date, currently it changes the output format
        if use_robust:
            raise ValueError("Currently robust regrssion is not supported. use_robust must be False.")
        self.use_robust = use_robust or False
        self.epsilon = epsilon or 1.35
        self.alpha = alpha or 0.0001
        self.fit_intercept = fit_intercept or False
        self.required_records = int(self.beta_lookback * (1 - self.dropna_pctg))
        self.dates_to_compute = None
        # CalibrationTaskflowTask.__init__(self)

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def _regression_for_all_dates(self, indep_var, depend_vars):
        betas = pd.DataFrame(index=indep_var.index, columns=depend_vars.columns)
        for dt in self.dates_to_compute:
            start_date = dt - pd.tseries.frequencies.to_offset(self.beta_offset)
            X = indep_var.ix[start_date:dt]
            y_mat = depend_vars.loc[start_date:dt].dropna(thresh=self.required_records, axis=1)
            result_df = get_beta(X, y_mat, lookback=None, take_return=False, return_type='log',
                                 fit_intercept=self.fit_intercept, use_robust_method=self.use_robust,
                                 epsilon=self.epsilon,
                                 regularization=self.alpha).drop('intercept', axis=1).T
            betas.loc[dt, result_df.columns] = result_df.loc[result_df.index[0], result_df.columns]
        return betas

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        intervals = kwargs[self.__class__.REQUIRES_FIELDS[1]]
        daily_prices['date'] = daily_prices['date'].apply(pd.Timestamp)
        if "date" in intervals:
            self.dates_to_compute = list(set(intervals["date"]))
        else:
            self.dates_to_compute = list(set(intervals["entry_date"]))
        # self.dates_to_compute = list(set(kwargs[self.__class__.REQUIRES_FIELDS[1]]['date']))
        self.dates_to_compute.sort()
        pivot_data = daily_prices[["date", "ticker", self.price_column]] \
            .pivot_table(index="date", values=self.price_column, columns="ticker", dropna=False).sort_index()
        return_data = np.log(pivot_data).diff()
        return_data.index = return_data.index.normalize()
        benchmark_df = return_data[self.benchmark_names]
        if self.use_robust:
            betas = {"{0}_beta".format(i): (self._regression_for_all_dates(benchmark_df.loc[:, i], return_data))
                     for i in benchmark_df.columns}
        else:
            denom = return_data.rolling(min_periods=self.beta_lookback, window=self.beta_lookback, center=False).var()
            betas = {"{0}_beta".format(i): (return_data.rolling(window=self.beta_lookback) \
                                            .cov(other=benchmark_df.loc[:, i], pairwise=self.beta_lookback) / denom)
                     for i in benchmark_df.columns}
        betas = {"{0}".format(benchmark): betas[benchmark].shift() for benchmark in betas}
        rank_betas = {"{0}_rank".format(i): betas[i].rank(axis=1) for i in betas}
        betas.update(rank_betas)
        all_beta_data = []
        for concept in betas:
            this_data = pd.melt(betas[concept].reset_index(), var_name="ticker",
                                id_vars="date", value_name=concept).set_index(["date", "ticker"])
            all_beta_data.append(this_data)
        result = pd.concat(all_beta_data, axis=1)
        result = result.sort_index(axis=1)
        self.data = result.reset_index()
        return self.data



class CalculateOvernightReturn(DataReaderClass):
    '''

    Calculates overnight returns from daily_price_data

    '''

    PROVIDES_FIELDS = ["overnight_return_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self):
        self.data = None

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        # TODO: Check whether indiscriminantly rounding is a good idea
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].sort_values(["ticker", "date"]).round(3)
        return_func = lambda g: (g["open"].combine_first(g["close"].fillna(method="ffill").shift()) \
                                 / g["close"].fillna(method="ffill").shift() - 1.0).to_frame(name="overnight_return")
        apply_func = lambda x: return_func(x.set_index("date"))
        self.data = daily_prices.groupby("ticker").apply(apply_func).reset_index() \
            [["date", "ticker", "overnight_return"]]
        return self.data

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}




CBQ_params = {"benchmark_names": ['SPY'], "beta_lookback": 63,
                "offset_unit": "B", "price_column": "close", "dropna_pctg": 0.15,
                "use_robust": False, "epsilon": 1.35, "alpha": 0.0001, "fit_intercept": False}
ComputeBetaQuantamental_params = {'params':CBQ_params,
                                  'class':ComputeBetaQuantamental,'start_date':RUN_DATE,
                                  'provided_data': {'beta_data': construct_destination_path('derived_simple')},
                                            'required_data': {'daily_price_data': construct_required_path('data_pull','daily_price_data'),
                                                             'intervals_data':construct_required_path('data_pull','interval_data')}}


####################

CalculateOvernightReturn_params = {'params':{},
                                   'class':CalculateOvernightReturn,'start_date':RUN_DATE,
                                'provided_data': {'overnight_return_data': construct_destination_path('derived_simple')},
                                'required_data': {'daily_price_data': construct_required_path('data_pull','daily_price_data')}}


####################
