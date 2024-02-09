from core_classes import GCPReader,download_yahoo_data,DataReaderClass
import pandas as pd
import numpy as np
from datetime import datetime
from core_classes import StatusType
from commonlib import talib_STOCHRSI, MA,talib_PPO
import talib
current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')
from core_classes import construct_required_path,construct_destination_path


class CalculateTaLibSTOCHRSI(DataReaderClass):
    '''

    Calculates the talib version of STOCHRSI indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_stochrsi_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator_params, price_column):
        self.data = None
        self.technical_indicator = talib_STOCHRSI
        self.technical_indicator_params = technical_indicator_params or {"timeperiod": 14, "fastk_period": 5,
                                                                         "fastd_period": 3, "fastd_matype": 0}
        self.price_column = price_column or "close"
        # CalibrationTaskflowTask.__init__(self)

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].apply(pd.Timestamp)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 0 if self.price_column == "open" else 1

        # TODO: Check whether indiscriminantly rounding is a good idea
        price = daily_prices.pivot_table(index='date', columns='ticker', values=self.price_column, dropna=False) \
            .sort_index().fillna(method='ffill', limit=3).shift(shift_value).round(2)

        indicator_generator = (self.technical_indicator(price[c], **self.technical_indicator_params) for c in
                               price.columns)
        slow_fast_indicators = zip(*[k for k in indicator_generator])
        slow_fast_stoch_kd = list(map(lambda x: pd.concat(x, axis=1, keys=price.columns), slow_fast_indicators))
        fastk, fastd = slow_fast_stoch_kd
        fastk = fastk.stack().reset_index() \
            .rename(columns={0: 'STOCHRSI_FASTK_{0}_{1}_{2}'.format(self.technical_indicator_params['timeperiod'],
                                                                    self.technical_indicator_params['fastk_period'],
                                                                    self.technical_indicator_params['fastd_period'])}) \
            .set_index(["date", "ticker"])
        fastd = fastd.stack().reset_index() \
            .rename(columns={0: 'STOCHRSI_FASTD_{0}_{1}_{2}'.format(self.technical_indicator_params['timeperiod'],
                                                                    self.technical_indicator_params['fastk_period'],
                                                                    self.technical_indicator_params['fastd_period'])}) \
            .set_index(["date", "ticker"])

        self.data = pd.concat([fastk, fastd], axis=1).reset_index()
        return StatusType.Success


class CalculateTaLibSTOCHRSIMultiParam(DataReaderClass):
    '''

    Calculates the talib version of ADX with multiple parameter configurations

    '''

    PROVIDES_FIELDS = ["talib_stochrsi_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, configs):
        self.data = None
        self.configs = configs or [{"technical_indicator_params": {"timeperiod": 14, "fastk_period": 5,
                                                                   "fastd_period": 3, "fastd_matype": 0},
                                    "price_column": "close"}]
        # CalibrationTaskflowTask.__init__(self)

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}

    def do_step_action(self, **kwargs):
        all_results = []
        for config in self.configs:
            indicator = CalculateTaLibSTOCHRSI(**config)
            indicator.do_step_action(**kwargs)
            result = list(indicator._get_additional_step_results().values())[0].set_index(["date", "ticker"])
            all_results.append(result)
        all_results = pd.concat(all_results, axis=1)
        self.data = all_results.sort_index().reset_index()
        return self.data


class CalculateVolatility(DataReaderClass):
    '''

    Calculates annualized return volatility based on daily prices given a lookback window

    '''

    PROVIDES_FIELDS = ["volatility_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, volatility_lookback, price_column):
        self.data = None
        self.volatility_lookback = volatility_lookback or 63
        self.price_column = price_column or "close"
        # CalibrationTaskflowTask.__init__(self)

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        # TODO: currently this step does not work. Change to return space. Also, need to pivot data
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        shift_value = 0 if self.price_column == "open" else 1
        column_name = "volatility_{0}".format(self.volatility_lookback)
        vol_func = lambda g: (np.log(g[self.price_column]).diff().rolling(self.volatility_lookback).std()) \
            .to_frame(name=column_name)
        apply_func = lambda x: vol_func(x.set_index("date").sort_index().shift(shift_value))
        self.data = daily_prices.groupby("ticker").apply(apply_func).reset_index()[["date", "ticker", column_name]]
        self.data[column_name] = self.data[column_name] * np.sqrt(252)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}


class CalculateVolatilityMultiParam(DataReaderClass):
    '''

    Calculates the talib version of ADX with multiple parameter configurations

    '''

    PROVIDES_FIELDS = ["volatility_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, configs):
        self.data = None
        self.configs = configs or [{"volatility_lookback": 63, "price_column": "close"}]
        # CalibrationTaskflowTask.__init__(self)

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        all_results = []
        for config in self.configs:
            indicator = CalculateVolatility(**config)
            indicator.do_step_action(**kwargs)
            result = list(indicator._get_additional_step_results().values())[0].set_index(["date", "ticker"])
            all_results.append(result)
        all_results = pd.concat(all_results, axis=1)
        self.data = all_results.sort_index().reset_index()
        return self.data

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}


class CalculateTaLibWILLR(DataReaderClass):
    '''

    Calculates the talib version of WILLR indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_willr_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator_params, smoothing_period):
        self.data = None
        self.technical_indicator = talib.WILLR
        self.technical_indicator_params = technical_indicator_params or {"timeperiod": 14}
        self.smoothing_period = smoothing_period or 3
        # CalibrationTaskflowTask.__init__(self)

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

        column_name = 'WILLR_{0}'.format(self.technical_indicator_params["timeperiod"])
        ma_column_name = '{0}_MA_{1}'.format(column_name, self.smoothing_period)

        indicator_generator = (self.technical_indicator(high[c], low[c], close[c], **self.technical_indicator_params)
                               for c in high.columns)
        indicator_signal = pd.concat(indicator_generator, axis=1, keys=high.columns)
        indicator_signal = indicator_signal.stack().reset_index().rename(columns={0: column_name})

        # TODO : Investigate allignment later
        indicator_signal[ma_column_name] = indicator_signal.groupby(["ticker"])[column_name] \
            .apply(lambda x: MA(x, self.smoothing_period, type='simple')).reset_index(level=0, drop=True)
        self.data = indicator_signal  # .set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}


class CalculateTaLibWILLRMultiParam(DataReaderClass):
    '''

    Calculates the talib version of ADX with multiple parameter configurations

    '''

    PROVIDES_FIELDS = ["talib_willr_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, configs):
        self.data = None
        self.configs = configs or [{"technical_indicator_params": {"timeperiod": 14}, "smoothing_period": 3}]
        # CalibrationTaskflowTask.__init__(self)

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        all_results = []
        for config in self.configs:
            indicator = CalculateTaLibWILLR(**config)
            indicator.do_step_action(**kwargs)
            result = list(indicator._get_additional_step_results().values())[0].set_index(["date", "ticker"])
            all_results.append(result)
        all_results = pd.concat(all_results, axis=1)
        self.data = all_results.sort_index().reset_index()
        return self.data

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}


class CalculateTaLibPPO(DataReaderClass):
    '''

    Calculates the talib version of PPO indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_ppo_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator_params, price_column, invert_sign):
        self.data = None
        self.technical_indicator = talib_PPO
        self.technical_indicator_params = technical_indicator_params or {"fastperiod": 12, "slowperiod": 26,
                                                                         "matype": 0}
        self.price_column = price_column or "close"
        self.invert_sign = invert_sign or True  # talib sign convention is the opposite of everywhere else
        # CalibrationTaskflowTask.__init__(self)

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        daily_prices['date'] = daily_prices['date'].apply(pd.Timestamp)
        daily_prices['date'] = daily_prices['date'].dt.normalize()
        shift_value = 0 if self.price_column == "open" else 1

        sign = -1.0 if self.invert_sign else 1.0
        indicator_func = lambda x: sign * self.technical_indicator(
            x[self.price_column].sort_index().fillna(method='ffill', limit=3),
            **self.technical_indicator_params).shift(shift_value) \
            .to_frame(name="PPO_{0}_{1}".format(self.technical_indicator_params["fastperiod"],
                                                self.technical_indicator_params["slowperiod"]))
        apply_func = lambda x: indicator_func(x.set_index("date"))
        indicator_signal = daily_prices[["date", "ticker", self.price_column]].groupby("ticker").apply(apply_func) \
            .reset_index()
        self.data = indicator_signal  # .set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}


class CalculateTaLibPPOMultiParam(DataReaderClass):
    '''

    Calculates the talib version of PPO indiciator with multiple parameter configurations

    '''

    PROVIDES_FIELDS = ["talib_ppo_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, configs):
        self.data = None
        self.technical_indicator = talib_PPO
        self.configs = configs or [{"technical_indicator_params": [{"fastperiod": 12, "slowperiod": 26, "matype": 0}],
                                    "price_column": "close", "invert_sign": True}]
        # CalibrationTaskflowTask.__init__(self)

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        all_results = []
        for config in self.configs:
            indicator = CalculateTaLibPPO(**config)
            indicator.do_step_action(**kwargs)
            result = list(indicator._get_additional_step_results().values())[0].set_index(["date", "ticker"])
            all_results.append(result)
        all_results = pd.concat(all_results, axis=1)
        self.data = all_results.sort_index().reset_index()
        return self.data

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}


class CalculateTaLibADX(DataReaderClass):
    '''

    Calculates the talib version of ADX indiciator based on daily prices

    '''

    PROVIDES_FIELDS = ["talib_adx_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, technical_indicator, technical_indicator_params, smoothing_period):
        self.data = None
        self.technical_indicator = talib.ADX
        self.technical_indicator_params = technical_indicator_params or {"timeperiod": 14}
        self.smoothing_period = smoothing_period or 3
        # CalibrationTaskflowTask.__init__(self)

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
        high = daily_prices.pivot_table(index='date', columns='ticker', values='high', dropna=False).sort_index().shift(
            shift_value).round(2)
        low = daily_prices.pivot_table(index='date', columns='ticker', values='low', dropna=False).sort_index().shift(
            shift_value).round(2)
        close = daily_prices.pivot_table(index='date', columns='ticker', values='close',
                                         dropna=False).sort_index().shift(shift_value).round(2)

        column_name = 'ADX_{0}'.format(self.technical_indicator_params["timeperiod"])
        ma_column_name = '{0}_MA_{1}'.format(column_name, self.smoothing_period)

        indicator_generator = (self.technical_indicator(high[c], low[c], close[c], **self.technical_indicator_params)
                               for c in high.columns)
        indicator_signal = pd.concat(indicator_generator, axis=1, keys=high.columns)
        indicator_signal = indicator_signal.stack().reset_index().rename(columns={0: column_name})

        indicator_signal[ma_column_name] = indicator_signal.groupby(["ticker"])[column_name] \
            .apply(lambda x: MA(x, self.smoothing_period, type='simple')).reset_index(level=0, drop=True)
        self.data = indicator_signal  # .set_index(["date","ticker"])
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}


class CalculateTaLibADXMultiParam(DataReaderClass):
    '''

    Calculates the talib version of ADX with multiple parameter configurations

    '''

    PROVIDES_FIELDS = ["talib_adx_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, configs):
        self.data = None
        self.configs = configs or [{"technical_indicator": talib.ADX, "technical_indicator_params": {"timeperiod": 14},
                                    "smoothing_period": 3}]
        # CalibrationTaskflowTask.__init__(self)

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        all_results = []
        for config in self.configs:
            indicator = CalculateTaLibADX(**config)
            indicator.do_step_action(**kwargs)
            result = list(indicator._get_additional_step_results().values())[0].set_index(["date", "ticker"])
            all_results.append(result)
        all_results = pd.concat(all_results, axis=1)
        self.data = all_results.sort_index().reset_index()
        return self.data

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}

############# AIRFLOW ############

CalculateTaLibSTOCHRSIMultiParam_figs = [{"technical_indicator_params": {"timeperiod": 14, "fastk_period": 5,
                                                                   "fastd_period": 3, "fastd_matype": 0},
                                    "price_column": "close"},
        {"technical_indicator_params": {"timeperiod": 30, "fastk_period": 10,
                                                                   "fastd_period": 5, "fastd_matype": 0},
                                    "price_column": "close"},
        {"technical_indicator_params": {"timeperiod": 63, "fastk_period": 15,
                                                                   "fastd_period": 10, "fastd_matype": 0},
                                    "price_column": "close"},
       ]


CalculateTaLibSTOCHRSIMultiParam_PARAMS = {'params':{'configs':CalculateTaLibSTOCHRSIMultiParam_figs},
                                           'class':CalculateTaLibSTOCHRSIMultiParam,
                                           'start_date':RUN_DATE,
                                           'provided_data': {'talib_stochrsi_indicator_data':construct_destination_path('derived_technical')},
                                            'required_data': {'daily_price_data': construct_required_path('data_pull','daily_price_data')}}

######


CalculateVolatilityMultiParam_configs = [{"volatility_lookback": 63, "price_column": "close"},
                                        {"volatility_lookback": 21, "price_column": "close"},
                                        {"volatility_lookback": 126, "price_column": "close"}]

CalculateVolatilityMultiParam_params = {'params':{'configs':CalculateVolatilityMultiParam_configs},
                                           'class':CalculateVolatilityMultiParam,
                                           'start_date':RUN_DATE,
                                           'provided_data': {'volatility_data': construct_destination_path('derived_technical')},
                                            'required_data': {'daily_price_data': construct_required_path('data_pull','daily_price_data')}}

#######

CalculateTaLibWILLRMultiParam_configs = [{"technical_indicator_params": {"timeperiod": 5}, "smoothing_period": 3},
                                         {"technical_indicator_params": {"timeperiod": 14}, "smoothing_period": 3},
                                         {"technical_indicator_params": {"timeperiod": 63}, "smoothing_period": 3},
                                        ]

CalculateTaLibWILLRMultiParam_params = {'params':{'configs':CalculateTaLibWILLRMultiParam_configs},
                                           'class':CalculateTaLibWILLRMultiParam,
                                           'start_date':RUN_DATE,
                                           'provided_data': {'talib_willr_indicator_data': construct_destination_path('derived_technical')},
                                            'required_data': {'daily_price_data': construct_required_path('data_pull','daily_price_data')}}




##############

CalculateTaLibPPOMultiParam_configs = [{"technical_indicator_params": {"fastperiod": 12, "slowperiod": 26, "matype": 0},
                                    "price_column": "close", "invert_sign": True},
                                      {"technical_indicator_params": {"fastperiod": 3, "slowperiod": 14, "matype": 0},
                                    "price_column": "close", "invert_sign": True},
                                      {"technical_indicator_params": {"fastperiod": 21, "slowperiod": 126, "matype": 0},
                                    "price_column": "close", "invert_sign": True},
                                     ]


CalculateTaLibPPOMultiParam_params = {'params':{'configs':CalculateTaLibPPOMultiParam_configs},
                                           'class':CalculateTaLibPPOMultiParam,
                                           'start_date':RUN_DATE,
                                           'provided_data': {'talib_ppo_indicator_data': construct_destination_path('derived_technical')},
                                            'required_data': {'daily_price_data': construct_required_path('data_pull','daily_price_data')}}


##########


CalculateTaLibADXMultiParam_configs = [{"technical_indicator": talib.ADX, "technical_indicator_params": {"timeperiod": 5},
                                     "smoothing_period": 3},
                              {"technical_indicator": talib.ADX, "technical_indicator_params": {"timeperiod": 14},
                                     "smoothing_period": 3},
                               {"technical_indicator": talib.ADX, "technical_indicator_params": {"timeperiod": 63},
                                     "smoothing_period": 3}
                              ]

CalculateTaLibADXMultiParam_params = {'params':{'configs':CalculateTaLibADXMultiParam_configs},
                                           'class':CalculateTaLibADXMultiParam,
                                           'start_date':RUN_DATE,
                                           'provided_data': {'talib_adx_indicator_data':construct_destination_path('derived_technical')},
                                            'required_data': {'daily_price_data': construct_required_path('data_pull','daily_price_data')}}
