from core_classes import GCPReader,download_yahoo_data,DataReaderClass
import pandas as pd
import numpy as np
from datetime import datetime
from core_classes import StatusType
from commonlib import talib_STOCHRSI, MA,talib_PPO, talib_TRIX
import talib
current_date = datetime.now().date()
RUN_DATE = '2023-06-28' #current_date.strftime('%Y-%m-%d')
from core_classes import construct_required_path,construct_destination_path


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


class CalculateMACD(DataReaderClass):
    '''

    Calculates MACD indicator (indicator value, smoothed centerline, and difference) on daily prices

    '''

    PROVIDES_FIELDS = ["macd_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator, technical_indicator_params, smoothing_period, price_column):
        self.data = None
        self.technical_indicator = globals()[technical_indicator]
        self.technical_indicator_params = technical_indicator_params
        self.smoothing_period = smoothing_period
        self.price_column = price_column or "close"

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        shift_value = 0 if self.price_column == "open" else 1

        indicator_func = lambda x: self.technical_indicator(x[self.price_column].sort_index(),
                                                            **self.technical_indicator_params).shift(shift_value) \
            .to_frame(name="macd")
        apply_func = lambda x: indicator_func(x.set_index("date"))
        indicator_signal = daily_prices[["date", "ticker", self.price_column]].groupby("ticker").apply(apply_func) \
            .reset_index()
        indicator_signal["macd_centerline"] = indicator_signal.groupby(["ticker"])["macd"] \
            .apply(lambda x: MA(x, self.smoothing_period, type='simple')).reset_index(level=0, drop=True)
        indicator_signal["macd_diff"] = indicator_signal["macd"] - indicator_signal["macd_centerline"]
        self.data = indicator_signal  # .set_index(["date","ticker"])
        return self.data

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}


class CalcualteCorrelation(DataReaderClass):
    '''

    Calculates the return correlation between tickers and benchmarks given a lookback

    '''

    PROVIDES_FIELDS = ["correlation_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, benchmark_names, correlation_lookback, price_column):
        self.data = None
        self.benchmark_names = benchmark_names or [8554]  # ["SPY"]
        self.correlation_lookback = correlation_lookback or 63
        self.price_column = price_column or "close"
        # CalibrationTaskflowTask.__init__(self)

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        pivot_data = daily_prices[["date", "ticker", self.price_column]] \
            .pivot_table(index="date", values=self.price_column, columns="ticker", dropna=False).sort_index()
        return_data = np.log(pivot_data).diff()
        return_data = return_data.fillna(0.0)
        rolling_return_data = return_data.rolling(self.correlation_lookback, self.correlation_lookback)
        return_data_mean = rolling_return_data.mean()
        return_data_std = rolling_return_data.std()
        return_data = (return_data - return_data_mean) / return_data_std
        benchmark_df = return_data[self.benchmark_names]
        rolling_return_data = return_data.rolling(self.correlation_lookback, self.correlation_lookback)
        correlations = {"{0}_correl".format(i): rolling_return_data.corr(benchmark_df.loc[:, i])
                        for i in benchmark_df.columns}

        correlations = {benchmark_corr: correlations[benchmark_corr].shift() for benchmark_corr in correlations}
        rank_correlations = {"{0}_rank".format(i): correlations[i].rank(axis=1) for i in correlations}
        correlations.update(rank_correlations)
        all_correlation_data = []
        for concept in correlations:
            this_data = pd.melt(correlations[concept].reset_index(), var_name="ticker",
                                id_vars="date", value_name=concept).set_index(["date", "ticker"])
            all_correlation_data.append(this_data)
        result = pd.concat(all_correlation_data, axis=1)
        self.data = result.sort_index().reset_index()
        return self.data

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}

    @classmethod
    def get_default_config(cls):
        return {"benchmark_names": ["SPY"], "correlation_lookback": 63, "price_column": "close"}


class CalculateDollarVolume(DataReaderClass):
    '''

    Calculates the average daily dollar volume per ticker over a given lookback period

    '''

    PROVIDES_FIELDS = ["dollar_volume_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, lookback_periods=10):
        self.data = None
        self.lookback_periods = lookback_periods

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].apply(pd.Timestamp)
        daily_prices["date"] = pd.DatetimeIndex(daily_prices["date"]).normalize()

        daily_prices["dollar_volume"] = daily_prices.eval("volume*(open+close)/2.0")
        daily_prices["average_price"] = daily_prices.eval("(open+close)/2.0")
        dollar_volume_table = pd.pivot_table(daily_prices, values="dollar_volume", index="date", columns="ticker",
                                             dropna=False).sort_index().fillna(method="ffill")
        avg_dollar_volumes = dollar_volume_table.rolling(self.lookback_periods).mean()
        avg_prices_table = pd.pivot_table(daily_prices, values="average_price",
                                          index="date", columns="ticker", dropna=False).sort_index().fillna(
            method="ffill")
        avg_prices = avg_prices_table.rolling(self.lookback_periods).mean()

        avg_dollar_volumes = avg_dollar_volumes.shift(1)
        avg_dollar_volumes.index = avg_dollar_volumes.index.normalize()
        avg_prices = avg_prices.shift(1)
        avg_prices.index = avg_prices.index.normalize()
        df_1 = pd.melt(avg_dollar_volumes.reset_index(), id_vars=['date'], var_name='ticker',
                       value_name='avg_dollar_volume')
        df_2 = pd.melt(avg_prices.reset_index(), id_vars=['date'], var_name='ticker',
                       value_name='avg_price')
        self.data = pd.merge(df_2, df_1, how='left', on=['ticker', 'date'])
        return self.data

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}

    @classmethod
    def get_default_config(cls):
        return {"lookback_periods": 10}


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


class CalculatePastReturnEquity(DataReaderClass):
    '''

    Calculates the past returns per ticker from daily prices for a list of lookback periods

    '''

    PROVIDES_FIELDS = ["past_return_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, column, lookback_list):
        self.data = None
        self.column = column or "close"
        self.lookback_list = lookback_list or [20, 60, 125, 252]

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def calculate_past_performance_for_group(self, g):
        column = self.column
        g_copy = g.copy(deep=True)
        if column == "open":
            day_shift = 0
        else:
            day_shift = 1
        for shift_amount in self.lookback_list:
            g_copy["ret_{0}B".format(str(shift_amount))] = g[column].shift(day_shift) / g[column].shift(
                day_shift).shift(shift_amount) - 1.0
        return g_copy

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices = daily_prices.set_index("date").sort_index()

        res = daily_prices.groupby("ticker").apply(self.calculate_past_performance_for_group)
        added_cols = list(map(lambda x: "ret_{0}B".format(str(x)), self.lookback_list))
        self.data = res.drop(["ticker"], axis=1).reset_index()[["date", "ticker"] + added_cols]
        self.data = self.data.fillna(0.0)
        return self.data

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}

    @classmethod
    def get_default_config(cls):
        return {"column": "close", "lookback_list": [1, 2, 5, 10, 21, 63, 126, 252]}


class CalculateTaLibSTOCH(DataReaderClass):
    '''

    Calculates the talib version of STOCH indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_stoch_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator_params):
        self.data = None
        self.technical_indicator = talib.STOCH
        self.technical_indicator_params = technical_indicator_params or \
                                          {"fastk_period": 5, "slowk_period": 3, "slowk_matype": 0, "slowd_period": 3,
                                           "slowd_matype": 0}

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].apply(pd.Timestamp)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 1

        # TODO: Check whether indiscriminantly rounding is a good idea
        high = daily_prices.pivot_table(index='date', columns='ticker', values='high', dropna=False) \
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        low = daily_prices.pivot_table(index='date', columns='ticker', values='low', dropna=False) \
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        close = daily_prices.pivot_table(index='date', columns='ticker', values='close', dropna=False) \
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)

        indicator_generator = (self.technical_indicator(high[c], low[c], close[c], **self.technical_indicator_params)
                               for c in high.columns)
        slow_fast_indicators = zip(*[k for k in indicator_generator])
        slow_fast_stoch_kd = list(map(lambda x: pd.concat(x, axis=1, keys=high.columns), slow_fast_indicators))
        slowk, slowd = slow_fast_stoch_kd
        slowk = slowk.stack().reset_index() \
            .rename(columns={0: 'SLOWK_{0}_{1}_{2}'.format(self.technical_indicator_params['fastk_period'],
                                                           self.technical_indicator_params['slowd_period'],
                                                           self.technical_indicator_params['slowk_period'])}) \
            .set_index(["date", "ticker"])
        slowd = slowd.stack().reset_index() \
            .rename(columns={0: 'SLOWD_{0}_{1}_{2}'.format(self.technical_indicator_params['fastk_period'],
                                                           self.technical_indicator_params['slowd_period'],
                                                           self.technical_indicator_params['slowk_period'])}) \
            .set_index(["date", "ticker"])

        self.data = pd.concat([slowk, slowd], axis=1).reset_index()
        return self.data


class CalculateTaLibSTOCHF(DataReaderClass):
    '''

    Calculates the talib version of STOCHF indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_stochf_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator_params):
        self.data = None
        self.technical_indicator = talib.STOCHF
        self.technical_indicator_params = technical_indicator_params or \
                                          {"fastk_period": 5, "fastd_period": 3, "fastd_matype": 0}

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].apply(pd.Timestamp)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 1

        # TODO: Check whether indiscriminantly rounding is a good idea
        high = daily_prices.pivot_table(index='date', columns='ticker', values='high', dropna=False) \
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        low = daily_prices.pivot_table(index='date', columns='ticker', values='low', dropna=False) \
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        close = daily_prices.pivot_table(index='date', columns='ticker', values='close', dropna=False) \
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)

        indicator_generator = (self.technical_indicator(high[c], low[c], close[c], **self.technical_indicator_params)
                               for c in high.columns)
        slow_fast_indicators = zip(*[k for k in indicator_generator])
        slow_fast_stoch_kd = list(map(lambda x: pd.concat(x, axis=1, keys=high.columns), slow_fast_indicators))
        fastk, fastd = slow_fast_stoch_kd
        fastk = fastk.stack().reset_index() \
            .rename(columns={0: 'FASTK_{0}_{1}'.format(self.technical_indicator_params['fastk_period'],
                                                       self.technical_indicator_params['fastd_period'])}) \
            .set_index(["date", "ticker"])
        fastd = fastd.stack().reset_index() \
            .rename(columns={0: 'FASTD_{0}_{1}'.format(self.technical_indicator_params['fastk_period'],
                                                       self.technical_indicator_params['fastd_period'])}) \
            .set_index(["date", "ticker"])

        self.data = pd.concat([fastk, fastd], axis=1).reset_index()
        return self.data

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}


class CalculateTaLibTRIX(DataReaderClass):
    '''

    Calculates the talib version of TRIX indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_trix_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator_params, price_column):
        self.data = None
        self.technical_indicator = talib_TRIX
        self.technical_indicator_params = technical_indicator_params or {"timeperiod": 30}
        self.price_column = price_column or "close"

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].apply(pd.Timestamp)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 0 if self.price_column == "open" else 1

        indicator_func = lambda x: self.technical_indicator(
            x[self.price_column].sort_index().fillna(method='ffill', limit=3),
            **self.technical_indicator_params).shift(shift_value) \
            .to_frame(name="TRIX_{0}".format(self.technical_indicator_params["timeperiod"]))
        apply_func = lambda x: indicator_func(x.set_index("date"))
        indicator_signal = daily_prices[["date", "ticker", self.price_column]].groupby("ticker").apply(apply_func) \
            .reset_index()
        self.data = indicator_signal  # .set_index(["date","ticker"])
        return self.data

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}


class CalculateTaLibULTOSC(DataReaderClass):
    '''

    Calculates the talib version of ADX indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_ultosc_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator_params, smoothing_period):
        self.data = None
        self.technical_indicator = talib.ULTOSC
        self.technical_indicator_params = technical_indicator_params or \
                                          {"timeperiod1": 7, "timeperiod2": 14, "timeperiod3": 28}
        self.smoothing_period = smoothing_period or 3

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].apply(pd.Timestamp)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 1

        # TODO: Check whether indiscriminantly rounding is a good idea
        high = daily_prices.pivot_table(index='date', columns='ticker', values='high', dropna=False) \
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        low = daily_prices.pivot_table(index='date', columns='ticker', values='low', dropna=False) \
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)
        close = daily_prices.pivot_table(index='date', columns='ticker', values='close', dropna=False) \
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)

        column_name = 'ULTOSC_{0}_{1}_{2}'.format(self.technical_indicator_params["timeperiod1"],
                                                  self.technical_indicator_params["timeperiod2"],
                                                  self.technical_indicator_params["timeperiod3"])
        ma_column_name = '{0}_MA_{1}'.format(column_name, self.smoothing_period)

        indicator_generator = (self.technical_indicator(high[c], low[c], close[c], **self.technical_indicator_params)
                               for c in high.columns)
        indicator_signal = pd.concat(indicator_generator, axis=1, keys=high.columns)
        indicator_signal = indicator_signal.stack().reset_index().rename(columns={0: column_name})

        indicator_signal[ma_column_name] = indicator_signal.groupby(["ticker"])[column_name] \
            .apply(lambda x: MA(x, self.smoothing_period, type='simple')).reset_index(level=0, drop=True)
        self.data = indicator_signal  # .set_index(["date","ticker"])
        return self.data

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}


########## AIRFLOW FUNCTIONS #########


CBQ_params = {"benchmark_names": ['SPY'], "beta_lookback": 63,
                "offset_unit": "B", "price_column": "close", "dropna_pctg": 0.15,
                "use_robust": False, "epsilon": 1.35, "alpha": 0.0001, "fit_intercept": False}
ComputeBetaQuantamental_params = {'params':CBQ_params,
                                  'class':ComputeBetaQuantamental,'start_date':RUN_DATE,
                                  'provided_data': {'beta_data': construct_destination_path('derived_simple')},
                                            'required_data': {'daily_price_data': construct_required_path('data_pull','daily_price_data'),
                                                             'intervals_data':construct_required_path('data_pull','interval_data')}}

###################

CMACD_params ={"technical_indicator": "macd", "technical_indicator_params": {"nslow":26, "nfast":12},
                "smoothing_period": 3, "price_column": "close"}

CalculateMACD_params ={'params':CMACD_params,
                                  'class':CalculateMACD,'start_date':RUN_DATE,
                                  'provided_data': {'macd_indicator_data': construct_destination_path('derived_simple')},
                                            'required_data': {'daily_price_data': construct_required_path('data_pull','daily_price_data')}}


#############

CC_params ={"benchmark_names": ["SPY"], "correlation_lookback": 63, "price_column": "close"}


CalcualteCorrelation_params ={'params':CC_params,
                                  'class':CalcualteCorrelation,'start_date':RUN_DATE,
                                  'provided_data': {'correlation_data': construct_destination_path('derived_simple')},
                                            'required_data': {'daily_price_data': construct_required_path('data_pull','daily_price_data')}}



#########

CalculateDollarVolume_params = {'params':{"lookback_periods": 21},'class':CalculateDollarVolume,'start_date':RUN_DATE,
                                'provided_data': {'dollar_volume_data': construct_destination_path('derived_simple')},
                                'required_data': {'daily_price_data': construct_required_path('data_pull','daily_price_data')}}



####################

CalculateOvernightReturn_params = {'params':{},
                                   'class':CalculateOvernightReturn,'start_date':RUN_DATE,
                                'provided_data': {'overnight_return_data': construct_destination_path('derived_simple')},
                                'required_data': {'daily_price_data': construct_required_path('data_pull','daily_price_data')}}


####################


CalculatePastReturnEquity_params = {'params':{"column": "close",
                                                 "lookback_list": [1, 2, 5, 10, 21, 63, 126, 252]},
                                   'class':CalculatePastReturnEquity,'start_date':RUN_DATE,
                                'provided_data': {'past_return_data': construct_destination_path('derived_simple')},
                                'required_data': {'daily_price_data': construct_required_path('data_pull','daily_price_data')}}




#####################


configs_CalculateTaLibSTOCH =  {"fastk_period": 5, "slowk_period": 3, "slowk_matype": 0, "slowd_period": 3, "slowd_matype": 0}
CalculateTaLibSTOCH_params = {'params':{'technical_indicator_params':configs_CalculateTaLibSTOCH},
                                   'class':CalculateTaLibSTOCH,'start_date':RUN_DATE,
                                'provided_data': {'talib_stoch_indicator_data': construct_destination_path('derived_simple')},
                                'required_data': {'daily_price_data': construct_required_path('data_pull','daily_price_data')}}


#######################


CalculateTaLibSTOCHF_configs={"fastk_period": 5, "fastd_period": 3, "fastd_matype": 0}
CalculateTaLibSTOCHF_params = {'params':{'technical_indicator_params':CalculateTaLibSTOCHF_configs},
                                   'class':CalculateTaLibSTOCHF,'start_date':RUN_DATE,
                                'provided_data': {'talib_stochf_indicator_data': construct_destination_path('derived_simple')},
                                'required_data': {'daily_price_data': construct_required_path('data_pull','daily_price_data')}}



##################

talib_trix_indicator_data ={"timeperiod": 30}

CalculateTaLibTRIX_params = {'params':{'technical_indicator_params':
                                                  talib_trix_indicator_data,
                                                 'price_column':'close'},
                                   'class':CalculateTaLibTRIX,'start_date':RUN_DATE,
                                'provided_data': {'talib_trix_indicator_data': construct_destination_path('derived_simple')},
                                'required_data': {'daily_price_data': construct_required_path('data_pull','daily_price_data')}}


##################


CalculateTaLibULTOSC_params = {'params':{'technical_indicator_params':{"timeperiod1": 7, "timeperiod2": 14, "timeperiod3": 28},'smoothing_period':3},
                                   'class':CalculateTaLibULTOSC,'start_date':RUN_DATE,
                                'provided_data': {'talib_ultosc_indicator_data': construct_destination_path('derived_simple')},
                                'required_data': {'daily_price_data': construct_required_path('data_pull','daily_price_data')}}

