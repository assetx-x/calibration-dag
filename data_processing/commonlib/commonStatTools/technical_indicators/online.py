from collections import deque
#from .. ..cointegration.util_functions import bind_parameters_in_standalone_function
#from . import offline


import sys, os
#sys.path.append(os.path.dirname(os.path.abspath(os.path.join(__file__, "..", "..")))) #TODO: check import

from commonlib.util_functions import bind_parameters_in_standalone_function
from commonlib.intuition_base_objects  import DCMIntuitionObject
from commonlib.commonStatTools.technical_indicators import offline

import numpy as np
import pandas as pd

_SMALL_EPSILON = offline._SMALL_EPSILON
# TODO (Jack): Investigate further later
_PRECISION = offline._PRECISION

ONLINE_TO_OFFLINE_MAPPING = {
    "OnlineSMA": "SMA",
    "OnlineEWMA": "EWMA",
    "OnlineRSI": "rsi",
    "OnlineMACD": "macd",
    "OnlineKAMA": "KaufmanMA",
    "OnlineOscillator": "price_oscillator",
    "OnlineROC": "rate_of_change",
    "OnlineDisparity": "disparity",
    "OnlineMomentum": "momentum",
    "OnlineStochasticK": "stoch_k",
    "OnlineStochasticD": "stoch_d",
    "OnlineSlowStochasticD": "slow_stoch_d",
    "OnlineStochasticKD": "stoch_k_d",
    "OnlineTSI": "tsi",
    "OnlineRealizedBipowerVariance": "realized_bipower_var",
    "OnlineTruncatedPowerVariance": "truncated_power_var",
    "OnlineLeeJumpDetection": "lee_jump_detection"
}

class RingBuffer(DCMIntuitionObject, deque):
    EXCLUDED_ATTRS_FOR_STATE = ["maxlen"]
    def __init__(self, size):
        super(RingBuffer, self).__init__((np.nan,) * size, maxlen=size)

    def __getinitargs__(self):
        return (self.maxlen,)


class OnlineTechnicalIndicator(DCMIntuitionObject):

    EXCLUDED_ATTRS_FOR_STATE = ["stock", "cache_results", "update_method", "get_metric_method"]

    def __init__(self, stock, cache_results=False, stock_history=None, indicator_parameters=None):
        self.stock = stock
        self.n_observations = None
        self.last_observation_dt = None
        self.last_observation_value = None
        self.cache_results = cache_results

        if cache_results:
            if stock_history is None:
                raise RuntimeError("OnlineTechnicalIndicator.__init__ - if the parameter 'cache_results' is enabled then "
                                   "the parameters 'stock_history' must be provided")

            if indicator_parameters is None:
                raise RuntimeError("OnlineTechnicalIndicator.__init__ - if the parameter 'cache_results' is enabled then "
                                   "the parameters 'stock_history' must be provided")

            if self.__class__.__name__ not in ONLINE_TO_OFFLINE_MAPPING:
                raise TypeError("OnlineTechnicalIndicator.__init__ - cannot find the offline version of technical "
                                "indicator {0} on mapping. Please verify".format(self.__class__.__name__))
            offline_func_name = ONLINE_TO_OFFLINE_MAPPING[self.__class__.__name__]
            offline_func = bind_parameters_in_standalone_function(offline, offline_func_name, indicator_parameters,
                                                                  ["ts"])
            self.indicator_values = offline_func(stock_history.fillna(method="ffill"))

        self.update_method = self.update_online if not cache_results else self.update_offline
        self.get_metric_method = self.get_metric_online if not cache_results else self.get_metric_offline
        self.reset()

    #TODO: enforce use of timestamps for indicators; for now optional to keep track of dates

    def __getinitargs__(self):
        return (self.stock, self.cache_results)

    def update_online(self, x, dt=None):
        self.n_observations += 1
        self.last_observation_dt = dt
        self.last_observation_value = x
        return self.get_metric()

    def update_offline(self, x, dt=None):
        self.n_observations += 1
        self.last_observation_dt = dt
        self.last_observation_value = x
        if dt and self.indicator_values.index[self.n_observations-1] != dt:
            raise RuntimeError("the update method invoked with dt={0} mismatch the expected cache value".format(dt))
        return self.get_metric()

    def update(self, x, dt=None):
        #TODO (Jack): maybe in the future we want an indication of how many consecutive values are nan
        x = x if not np.isnan(x) else self.last_observation_value
        return self.update_method(x, dt)

    def get_cached_results(self, start_date, end_date):
        if not self.cache_results:
            return None
        else:
            return self.indicator_values[start_date:end_date]

    def reset(self):
        self.n_observations = 0
        self.last_observation_dt = None
        self.last_observation_value = None

    def get_metric_online(self):
        pass

    def get_metric_offline(self):
        return self.indicator_values.values[self.n_observations-1]

    def get_metric(self):
        return self.get_metric_method()

    def return_n_observations(self):
        return self.n_observations

    def get_last_observation_dt(self):
        return self.last_observation_dt

    def get_last_observation_value(self):
        return self.last_observation_value

class OnlineRSI(OnlineTechnicalIndicator):
    EXCLUDED_ATTRS_FOR_STATE = ["window"]

    def __init__(self, stock, window, cache_results=False, stock_history=None):
        self.window = window
        self.values = RingBuffer(self.window)
        self.current_day = 0
        self.historical_up_value = np.nan
        self.historical_down_value = np.nan
        OnlineTechnicalIndicator.__init__(self, stock, cache_results, stock_history, {"window": window})

    def __getinitargs__(self):
        return (self.stock, self.window, self.cache_results)

    def update_online(self, x, dt=None):
        # TODO (Jack): investigate later
        x = np.around(x, decimals=_PRECISION)
        self.current_day += 1
        self.values.append(x)
        if self.current_day == self.window:
            deltas = (np.nan, ) + tuple(self.values[i] - self.values[i - 1] for i in range(1, self.window))
            self.historical_up_value = sum(filter(lambda value: value > 0, deltas))
            self.historical_down_value = -sum(filter(lambda value: value < 0, deltas))
            self.values.clear()
        elif self.current_day > self.window:
            current_delta = x - self.last_observation_value
            self.historical_up_value *= (self.window - 1.) / self.window
            self.historical_down_value *= (self.window - 1.) / self.window
            if current_delta > 0:
                self.historical_up_value += current_delta
            elif current_delta < 0:
                self.historical_down_value -= current_delta

        return super(self.__class__, self).update_online(x, dt)

    def reset(self):
        self.current_day = 0
        self.historical_up_value = np.nan
        self.historical_down_value = np.nan
        self.values = RingBuffer(self.window)
        return super(self.__class__, self).reset()

    def get_metric_online(self):
        m = np.float32(
            100. - 100. / (1 + (self.historical_up_value / (self.historical_down_value + _SMALL_EPSILON)))
        )
        return m


class OnlineSMA(OnlineTechnicalIndicator):
    EXCLUDED_ATTRS_FOR_STATE = ["window"]

    def __init__(self, stock, window, cache_results=False, stock_history=None):
        self.window = window

        self.values = RingBuffer(self.window)
        self.current_result = np.nan

        super(OnlineSMA, self).__init__(stock, cache_results, stock_history, {"window":window})

    def __getinitargs__(self):
        return (self.stock, self.window, self.cache_results)

    def update_online(self, x, dt=None):
        self.values.append(x if not np.isnan(x) else self.values[-1])
        self.current_result = sum(self.values) / self.window if np.nan not in self.values else np.nan
        return super(self.__class__, self).update_online(x, dt)

    def reset(self):
        self.values = RingBuffer(self.window)
        self.current_result = np.nan
        return super(self.__class__, self).reset()

    def get_metric_online(self):
        return self.current_result


class OnlineEWMA(OnlineTechnicalIndicator):
    def __init__(self, stock, window, adjust=True, ignore_na=False, cache_results=False, stock_history=None):
        self.window = window
        self.adjust = adjust
        self.ignore_na = ignore_na
        self.alpha = 2. / (window + 1.)
        self.old_wt_factor = 1. - self.alpha
        self.new_wt = 1. if adjust else self.alpha

        self.old_wt = 1.
        self.count = 0
        self.current_result = np.nan

        super(OnlineEWMA, self).__init__(stock, cache_results, stock_history,
                                         {"window":window, "adjust":adjust, "ignore_na":ignore_na})

    def update_online(self, x, dt=None):
        # This is the same implementation rewritten from pandas sources in online manner
        # https://github.com/pydata/pandas/blob/master/pandas/algos.pyx#L1007

        value = x
        is_observation = not np.isnan(x)
        if is_observation:
            self.count += 1

        if not np.isnan(self.current_result):
            if is_observation or (not self.ignore_na):
                self.old_wt *= self.old_wt_factor
                if is_observation:
                    if self.current_result != value:  # avoid numerical errors on constant series
                        self.current_result = \
                            ((self.old_wt * self.current_result) + (self.new_wt * value)) / (self.old_wt + self.new_wt)
                    if self.adjust:
                        self.old_wt += self.new_wt
                    else:
                        self.old_wt = 1.
        elif is_observation:
            self.current_result = value

        return super(self.__class__, self).update_online(x, dt)

    def reset(self):
        self.old_wt = 1.
        self.count = 0
        self.current_result = np.nan
        return super(self.__class__, self).reset()

    def get_metric_online(self):
        return self.current_result if self.count >= self.window else np.nan


class OnlineKAMA(OnlineTechnicalIndicator):
    def __init__(self, stock, offset=10, lambda1=2, lambda2=30, cache_results=False, stock_history=None):
        self.offset = offset
        self.lambda1 = lambda1
        self.lambda2 = lambda2

        self.values = RingBuffer(self.offset + 1)
        self.count = 0
        self.current_result = np.nan

        super(OnlineKAMA, self).__init__(stock, cache_results, stock_history,
                                         {"offset":offset, "lambda1":lambda1, "lambda2":lambda2})

    def update_online(self, x, dt=None):
        self.values.append(x if not np.isnan(x) else self.values[-1])

        if self.count > self.offset:
            pdelta = (abs(self.values[i] - self.values[i - 1]) for i in xrange(1, self.offset + 1))
            direction = self.values[-1] - self.values[0]
            volatility = sum(pdelta)
            er = direction / (volatility + _SMALL_EPSILON)
            sc = (er * ((2. / (self.lambda1 + 1.)) - (2. / (self.lambda2 + 1.))) + (2. / (self.lambda2 + 1.))) ** 2
            self.current_result += sc * (self.values[-1] - self.current_result)

        elif self.count == self.offset:
            self.current_result = x
        self.count += 1
        return super(self.__class__, self).update_online(x, dt)

    def reset(self):
        self.values = RingBuffer(self.offset + 1)
        self.count = 0
        self.current_result = np.nan
        return super(self.__class__, self).reset()

    def get_metric_online(self):
        return self.current_result


class OnlineMACD(OnlineTechnicalIndicator):
    def __init__(self, stock, nslow=26, nfast=12, cache_results=False, stock_history=None):
        self.nslow = nslow
        self.nfast = nfast

        self.slow_ma = OnlineEWMA("slow_ma_%s" % stock, self.nslow)
        self.fast_ma = OnlineEWMA("fast_ma_%s" % stock, self.nfast)
        self.current_result = np.nan

        super(OnlineMACD, self).__init__(stock, cache_results, stock_history,
                                         {"nslow":nslow, "nfast":nfast})

    def update_online(self, x, dt=None):
        fast_ma_value = self.fast_ma.update_online(x)
        slow_ma_value = self.slow_ma.update_online(x)
        self.current_result = fast_ma_value - slow_ma_value
        return super(self.__class__, self).update_online(x, dt)

    def reset(self):
        self.slow_ma = OnlineEWMA("slow_ma_%s" % self.stock, self.nslow)
        self.fast_ma = OnlineEWMA("fast_ma_%s" % self.stock, self.nfast)
        self.current_result = np.nan
        return super(self.__class__, self).reset()

    def get_metric_online(self):
        return self.current_result


class OnlineOscillator(OnlineTechnicalIndicator):
    def __init__(self, stock, nslow=26, nfast=12, cache_results=False, stock_history=None):
        self.nslow = nslow
        self.nfast = nfast

        self.slow_ma = OnlineEWMA(stock, nslow)
        self.fast_ma = OnlineEWMA(stock, nfast)

        super(OnlineOscillator, self).__init__(stock, cache_results, stock_history, {"nslow":nslow, "nfast":nfast})

    def update_online(self, x, dt=None):
        self.slow_ma.update_online(x)
        self.fast_ma.update_online(x)
        return super(self.__class__, self).update_online(x, dt)

    def reset(self):
        self.slow_ma.reset()
        self.fast_ma.reset()
        return super(self.__class__, self).reset()

    def get_metric_online(self):
        fast_ma_value = self.fast_ma.get_metric_online()
        slow_ma_value = self.slow_ma.get_metric_online()

        return (fast_ma_value - slow_ma_value) / (fast_ma_value + _SMALL_EPSILON)


class OnlineROC(OnlineTechnicalIndicator):
    def __init__(self, stock, offset=10, cache_results=False, stock_history=None):
        self.offset = offset

        self.values = RingBuffer(self.offset + 1)
        self.current_result = np.nan

        super(OnlineROC, self).__init__(stock, cache_results, stock_history, {"offset":offset})

    def update_online(self, x, dt=None):
        self.values.append(x if not np.isnan(x) else self.values[-1])
        current_value = x
        shifted_value = self.values[0]
        self.current_result = ((current_value - shifted_value) / (shifted_value + _SMALL_EPSILON)) * 100.

        return super(self.__class__, self).update_online(x, dt)

    def reset(self):
        self.values = RingBuffer(self.offset + 1)
        self.current_result = np.nan
        return super(self.__class__, self).reset()

    def get_metric_online(self):
        return self.current_result if not np.isnan(self.current_result) else 0.


class OnlineMomentum(OnlineTechnicalIndicator):
    def __init__(self, stock, offset=10, cache_results=False, stock_history=None):
        self.offset = offset

        self.values = RingBuffer(self.offset + 1)
        self.current_result = np.nan

        super(OnlineMomentum, self).__init__(stock, cache_results, stock_history, {"offset":offset})

    def update_online(self, x, dt=None):
        self.values.append(x if not np.isnan(x) else self.values[-1])
        self.current_result = self.values[-1] - self.values[0]

        return super(self.__class__, self).update_online(x, dt)

    def reset(self):
        self.values = RingBuffer(self.offset + 1)
        self.current_result = np.nan
        return super(self.__class__, self).reset()

    def get_metric_online(self):
        return self.current_result


class OnlineDisparity(OnlineTechnicalIndicator):
    def __init__(self, stock, offset=10, cache_results=False, stock_history=None):
        self.offset = offset

        self.prev_value = np.nan
        self.ma = OnlineEWMA(stock, self.offset)
        self.current_result = np.nan

        super(OnlineDisparity, self).__init__(stock, cache_results, stock_history, {"offset":offset})

    def update_online(self, x, dt=None):
        value = x if not np.isnan(x) else self.prev_value
        ma_value = self.ma.update_online(value)
        self.current_result = value / (ma_value + _SMALL_EPSILON)
        self.prev_value = value
        return super(self.__class__, self).update_online(x, dt)

    def reset(self):
        self.ma = OnlineEWMA(self.stock, self.offset)
        self.current_result = np.nan
        return super(self.__class__, self).reset()

    def get_metric_online(self):
        return self.current_result


class OnlineStochasticK(OnlineTechnicalIndicator):
    def __init__(self, stock, offset=14, cache_results=False, stock_history=None):
        self.offset = offset

        self.values = RingBuffer(self.offset)
        self.current_result = np.nan

        super(OnlineStochasticK, self).__init__(stock, cache_results, stock_history, {"offset":offset})

    def update_online(self, x, dt=None):
        self.values.append(x if not np.isnan(x) else self.values[-1])

        if np.nan in self.values:
            self.current_result = np.nan
            return super(OnlineStochasticK, self).update_online(x)

        low = min(self.values)
        high = max(self.values)
        self.current_result = ((x - low) / (high - low + _SMALL_EPSILON)) * 100
        return super(self.__class__, self).update_online(x, dt)

    def reset(self):
        self.values = RingBuffer(self.offset)
        self.current_result = np.nan
        return super(self.__class__, self).reset()

    def get_metric_online(self):
        return self.current_result


class OnlineStochasticD(OnlineTechnicalIndicator):
    def __init__(self, stock, offset=14, window=3, cache_results=False, stock_history=None):
        self.offset = offset
        self.window = window

        self.stochastic_k = OnlineStochasticK(stock, self.offset)
        self.stochastic_k_ma = OnlineEWMA("stock_k_%s" % stock, self.window)

        super(OnlineStochasticD, self).__init__(stock, cache_results, stock_history, {"offset":offset, "window":window})

    def update_online(self, x, dt=None):
        self.stochastic_k_ma.update_online(
            self.stochastic_k.update_online(x)
        )
        return super(self.__class__, self).update_online(x, dt)

    def reset(self):
        self.stochastic_k = OnlineStochasticK(self.stock, self.offset)
        self.stochastic_k_ma = OnlineEWMA("stock_k_%s" % self.stock, self.window)
        return super(self.__class__, self).reset()

    def get_metric_online(self):
        return self.stochastic_k_ma.get_metric_online()


class OnlineSlowStochasticD(OnlineTechnicalIndicator):
    def __init__(self, stock, offset=14, window1=3, window2=3, cache_results=False, stock_history=None):
        self.offset = offset
        self.window1 = window1
        self.window2 = window2

        self.stochastic_d = OnlineStochasticD(stock, self.offset, self.window1)
        self.stochastic_d_ma = OnlineEWMA("stock_d_%s" % stock, self.window2)

        super(OnlineSlowStochasticD, self).__init__(stock, cache_results, stock_history,
                                                    {"offset":offset, "window1":window1, "window2":window2})

    def update_online(self, x, dt=None):
        self.stochastic_d_ma.update_online(
            self.stochastic_d.update_online(x)
        )

        return super(self.__class__, self).update_online(x, dt)

    def reset(self):
        self.stochastic_d = OnlineStochasticD(self.stock, self.offset, self.window1)
        self.stochastic_d_ma = OnlineEWMA("stock_k_%s" % self.stock, self.window2)
        return super(self.__class__, self).reset()

    def get_metric_online(self):
        return self.stochastic_d_ma.get_metric_online()



class OnlineStochasticKD(OnlineTechnicalIndicator):
    def __init__(self, stock, offset, window, cache_results=False, stock_history=None):
        self.stochastic_k = OnlineStochasticK(stock, offset)
        self.stochastic_d = OnlineStochasticD(stock, offset, window)
        super(OnlineStochasticKD, self).__init__(stock, cache_results, stock_history,
                                                 {"offset":offset, "window":window})


    def update_online(self, x, dt=None):
        self.stochastic_k.update_online(x, dt)
        self.stochastic_d.update_online(x, dt)
        return super(self.__class__, self).update_online(x, dt)


    def reset(self):
        self.stochastic_k.reset()
        self.stochastic_d.reset()
        return super(self.__class__, self).reset()


    def get_metric_online(self):
        return self.stochastic_k.get_metric_online() - self.stochastic_d.get_metric_online()



class OnlineTSI(OnlineTechnicalIndicator):
    def __init__(self, stock, nslow=25, nfast=13, offset=1, cache_results=False, stock_history=None):
        self.nslow = nslow
        self.nfast = nfast
        self.offset = offset

        self.momentum = OnlineMomentum(stock, self.offset)
        self.slow_ma = OnlineEWMA("slow_ma_%s" % stock, self.nslow)
        self.fast_ma = OnlineEWMA("fast_ma_%s" % stock, self.nfast)
        self.slow_abs_ma = OnlineEWMA("slow_abs_ma_%s" % stock, self.nslow)
        self.fast_abs_ma = OnlineEWMA("fast_abs_ma_%s" % stock, self.nfast)
        self.current_result = np.nan

        super(OnlineTSI, self).__init__(stock, cache_results, stock_history,
                                        {"nslow":nslow, "nfast":nfast, "offset":offset})

    def update_online(self, x, dt=None):
        momentum_value = self.momentum.update_online(x)

        ma_value = self.fast_ma.update_online(
            self.slow_ma.update_online(
                momentum_value
            )
        )

        ma_abs_value = self.fast_abs_ma.update_online(
            self.slow_abs_ma.update_online(
                abs(momentum_value)
            )
        )
        self.current_result = (ma_value / (ma_abs_value + _SMALL_EPSILON)) * 100.
        return super(self.__class__, self).update_online(x, dt)

    def reset(self):
        self.momentum = OnlineMomentum(self.stock, self.offset)
        self.slow_ma = OnlineEWMA("slow_ma_%s" % self.stock, self.nslow)
        self.fast_ma = OnlineEWMA("fast_ma_%s" % self.stock, self.nfast)
        self.slow_abs_ma = OnlineEWMA("slow_abs_ma_%s" % self.stock, self.nslow)
        self.fast_abs_ma = OnlineEWMA("fast_abs_ma_%s" % self.stock, self.nfast)
        self.current_result = np.nan
        return super(self.__class__, self).reset()

    def get_metric_online(self):
        return self.current_result

class OnlineRealizedBipowerVariance(OnlineTechnicalIndicator):

    EXCLUDED_ATTRS_FOR_STATE = ["window", "delta_t"]

    def __init__(self, stock, delta_t, window, cache_results=False, stock_history=None):
        self.metric_denominator = offline.BIPOWER_VAR_DENOM*delta_t
        self.variance = OnlineSMA(stock, window-2)
        self.window = window
        self.delta_t = delta_t
        super(OnlineRealizedBipowerVariance, self).__init__(stock, cache_results, stock_history,
                                                            {"delta_t":delta_t, "window":window})

    def __getinitargs__(self):
        return (self.stock, self.delta_t, self.window, self.cache_results)

    def update_online(self, x, dt=None):
        new_value = abs(self.last_observation_value)*abs(x)
        self.variance.update_online(new_value)
        return super(self.__class__, self).update_online(x, dt)

    def reset(self):
        super(self.__class__, self).reset()
        self.variance.reset()
        self.last_observation_value = pd.np.nan # 0.0

    def get_metric_online(self):
        return self.variance.get_metric_online()/self.metric_denominator

class OnlineTruncatedPowerVariance(OnlineTechnicalIndicator):

    EXCLUDED_ATTRS_FOR_STATE = ["window", "delta_t", "g"]

    def __init__(self, stock, delta_t, window, g, cache_results=False, stock_history=None):
        self.window = window
        self.delta_t = delta_t
        self.g = g
        self.threshold = g*np.power(delta_t, offline.TRUNCATED_POWER_VAR_OMEGA)
        self.variance = OnlineSMA(stock, window)
        super(OnlineTruncatedPowerVariance, self).__init__(stock, cache_results, stock_history,
                                                           {"delta_t":delta_t, "window":window, "g":g})

    def __getinitargs__(self):
        return (self.stock, self.delta_t, self.window, self.g, self.cache_results)

    def update_online(self, x, dt=None):
        self.variance.update_online(x**2*int(x<=self.threshold))
        return super(self.__class__, self).update_online(x, dt)

    def reset(self):
        super(self.__class__, self).reset()
        self.variance.reset()
        self.last_observation_value = pd.np.nan # 0.0

    def get_metric_online(self):
        return self.variance.get_metric_online()/self.delta_t

class OnlineLeeJumpDetection(OnlineTechnicalIndicator):

    EXCLUDED_ATTRS_FOR_STATE = ["delta_t", "window", "g", "jump_return_criteria", "alpha", "jump_detection_function"]

    def __init__(self, stock, delta_t, window, g, jump_return_criteria="either", alpha=0.05,
                 cache_results=False, stock_history=None, n_max=252, variance_decimals=6, stat_decimals=3):

        if jump_return_criteria not in ["realized_bipower", "truncated_power", "either", "both"]:
            raise RuntimeError("lee_jump_detection method must be one of 'realized_bipower', 'truncated_power', "
                               "'either' or 'both'")

        self.var_rbv = OnlineRealizedBipowerVariance(stock, delta_t, window)
        self.var_tpv = OnlineTruncatedPowerVariance(stock, delta_t, window, g)

        self.window = window

        self.delta_t = delta_t
        self.g = g
        self.jump_return_criteria = jump_return_criteria
        self.alpha = alpha

        self.delta_t_sqrt = np.sqrt(delta_t)
        self.q = -np.log(-np.log(1.0-alpha))

        self.n_max = n_max
        self.variance_decimals = variance_decimals
        self.stat_decimals = stat_decimals

        jump_detection_function_dict = {
            "realized_bipower": lambda x,y: x,
            "truncated_power": lambda x,y: y,
            "either": lambda x,y: np.sign((x+y)/2.0),
            "both": lambda x,y: np.int((x+y)/2.0)
        }
        self.jump_detection_function = jump_detection_function_dict[jump_return_criteria]

        super(OnlineLeeJumpDetection, self).__init__(stock, cache_results, stock_history,
                                                     {"delta_t":delta_t, "window":window, "g":g,
                                                      "jump_return_criteria":jump_return_criteria, "alpha": alpha})

    def __getinitargs__(self):
        return (self.stock, self.delta_t, self.window, self.g, self.jump_return_criteria, self.alpha,
                self.cache_results)

    def __compute_lee_test_limits(self):
        log_n = np.log(self.n_max)
        log_n_factor = (2.0*log_n)**0.5
        C = log_n_factor - (np.log(np.pi) + np.log(log_n))/(2.0*(log_n_factor))
        S = 1.0/log_n_factor
        self.statistic_limits = (-self.q*S-C, self.q*S+C)

    def update_online(self, x, dt=None):
        if abs(x) < 0.0000001:
            x = 0
        self.var_rbv.update_online(x, dt)
        self.var_tpv.update_online(x, dt)
        self.__compute_lee_test_limits()
        return super(self.__class__, self).update_online(x, dt)

    def reset(self):
        super(self.__class__, self).reset()
        self.var_rbv.reset()
        self.var_tpv.reset()
        self.statistic_limits = None

    def get_metric_online(self):
        rbv_stat = self.last_observation_value / np.sqrt(pd.np.round(self.var_rbv.get_metric_online(),
                                                                     self.variance_decimals)) / self.delta_t_sqrt
        tpv_stat = self.last_observation_value / np.sqrt(pd.np.round(self.var_tpv.get_metric_online(),
                                                                     self.variance_decimals)) / self.delta_t_sqrt
        rbv_stat = pd.np.round(rbv_stat, self.stat_decimals)
        tpv_stat = pd.np.round(tpv_stat, self.stat_decimals)

        rbv_ind = -1*int(rbv_stat<self.statistic_limits[0]) + int(rbv_stat>self.statistic_limits[1])
        tpv_ind = -1*int(tpv_stat<self.statistic_limits[0]) + int(tpv_stat>self.statistic_limits[1])
        return self.jump_detection_function(rbv_ind, tpv_ind)

