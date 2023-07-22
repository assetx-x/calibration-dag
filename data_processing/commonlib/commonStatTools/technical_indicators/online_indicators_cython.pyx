#!python
#cython: embedsignature=True, boundscheck=False, wraparound=False

import numpy as np

try:
    from pandas.lib import checknull as pd_checknull
except ImportError:
    from pandas._libs.missing import checknull as pd_checknull

from pandas import Timestamp, Series
from offline import _SMALL_EPSILON as offline_small_eps, _PRECISION as offline_precision
import offline
from collections import OrderedDict
from numpy.core._methods import umr_sum

cimport numpy as np
cdef _SMALL_EPSILON = offline_small_eps
cdef _PRECISION = offline_precision
cdef list REGION_CODES = [1, 0.5, -0.5, -1]

cdef np.float64_t MINS_ANNUALIZE_SHARPE = 313.49641146271517
cdef np.float64_t PERIOD_MDD_THRESHOLD = 0.03
cdef np.float64_t PERIOD_MDD_OFFSET = 0.07
cdef np.float64_t MDD_MULTIPLE = 1.2
cdef np.float64_t MIN_RETURN_THRESHOLD = 0.03

cdef dict ONLINE_TO_OFFLINE_MAPPING = {
    "OnlineSMA": "SMA",
    "OnlineEWMA": "EWMA",
    "OnlineRSI": "rsi_alt",
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

cdef class CythonModuleBase:
    def __reduce__(self):
        #return (self.__class__, self.__getinitargs__(), self.__getstate__())
        return (self.__class__, self.__getinitargs__())

    cdef tuple __getinitargs__(self):
        raise NotImplementedError()

    cdef dict __getstate__(self):
        return {}

    cdef __setstate__(self, dict state):
        pass

cdef class CythonOnlineTechnicalIndicatorBase(CythonModuleBase):
    cdef:
        str stock
        unsigned long n_observations
        np.int64_t last_observation_dt
        np.float64_t last_observation_value
        str name

    def __init__(self, str stock):
        self.stock = stock
        self.reset()
        self.__build_name()

    cpdef np.float64_t update(self, np.float64_t x, np.int64_t dt):
        x = x if not np.isnan(x) else self.last_observation_value
        self.n_observations += 1
        self.last_observation_dt = dt
        self.last_observation_value = x
        return self.get_metric_online()

    cpdef reset(self):
        self.n_observations = 0
        self.last_observation_dt = -1
        self.last_observation_value = np.NaN

    cpdef np.float64_t get_metric(self):
        raise NotImplementedError()

    cpdef return_n_observations(self):
        return self.n_observations

    cpdef get_last_observation_dt(self):
        return self.last_observation_dt

    cpdef get_last_observation_value(self):
        return self.last_observation_value

    cpdef dict get_indicator_parameters(self):
        raise NotImplementedError()

    cpdef str get_stock(self):
        return self.stock

    cpdef str get_name(self):
        return self.name

    cpdef __build_name(self):
        cdef list all_names = []
        cdef dict params = self.get_indicator_parameters()
        for k in sorted(params.keys()):
            all_names.append("{0}={1}".format(k,params[k]))
        self.name = "{0}({1}; {2})".format(self.__class__.__name__, self.stock,",".join(all_names))

    cdef tuple __getinitargs__(self):
        return (self.stock,)

    cdef dict __getstate__(self):
        return {0: self.n_observations, 1:self.last_observation_dt, 2:self.last_observation_value}

    cdef __setstate__(self, dict state):
        self.n_observations = state[0]
        self.last_observation_dt = state[1]
        self.last_observation_value = state[2]

cdef class CachedCythonTechnicalIndicator(CythonOnlineTechnicalIndicatorBase):
    cdef:
        np.ndarray indicator_values
        np.ndarray indicator_dates
        dict params

    def __init__(self, str name, dict params, str stock, indicator_cached_values):
        self.indicator_values =  indicator_cached_values.values
        self.indicator_dates = indicator_cached_values.index.view('int64')
        self.params = params
        CythonOnlineTechnicalIndicator.__init__(self, stock)
        self.name = name


    cpdef np.float64_t update(self, np.float64_t x, np.int64_t dt):
        x = x if not pd_checknull(x) else self.last_observation_value
        self.n_observations += 1
        self.last_observation_dt = dt
        self.last_observation_value = x
        if dt and self.indicator_dates[self.n_observations-1] != dt:
            raise RuntimeError("the update method invoked with dt={0} mismatch the expected cache value".format(dt))
        return self.indicator_values[self.n_observations-1]

    cpdef np.float64_t get_metric(self):
        return self.indicator_values[self.n_observations-1]

    cpdef dict get_indicator_parameters(self):
        return self.params

    cpdef np.ndarray get_cached_values(self):
        return self.indicator_values

    cpdef np.ndarray get_cached_dates(self):
        return self.indicator_dates

    cdef tuple __getinitargs__(self):
        return (self.name, self.params, self.stock, Series(self.indicator_values, index=self.indicator_dates))


cdef class CythonOnlineTechnicalIndicator(CythonOnlineTechnicalIndicatorBase):
    cpdef CachedCythonTechnicalIndicator get_cached_indicator(self, stock_history):
        offline_func = getattr(offline, ONLINE_TO_OFFLINE_MAPPING[self.__class__.__name__], None)
        if offline_func is None:
            raise RuntimeError()
        params = self.get_indicator_parameters()
        values = offline_func(stock_history, **params)
        return CachedCythonTechnicalIndicator(self.get_name(), params, self.stock, values)

cdef class OnlineRSI(CythonOnlineTechnicalIndicator):
    cdef:
        unsigned long window
        np.float64_t historical_up_value, historical_down_value

    def __init__(self,  str stock, unsigned long window):
        self.window = window
        CythonOnlineTechnicalIndicator.__init__(self, stock)

    cpdef dict get_indicator_parameters(self):
        return {"window": self.window}

    cpdef np.float64_t update_online(self, np.float64_t x, np.int64_t dt):
        if self.n_observations == 0:
            return np.NaN
        x = np.around(x, decimals=_PRECISION)
        cdef np.float64_t delta = x - self.last_observation_value

        if self.n_observations >= self.window:
            self.historical_up_value *= (self.window - 1.0) / self.window
            self.historical_down_value *= (self.window - 1.0) / self.window

        if delta>0:
            self.historical_up_value += delta
        elif delta<0:
            self.historical_down_value -= delta

        return CythonOnlineTechnicalIndicator.update_online(self, x, dt)

    cpdef reset(self):
        self.historical_up_value = 0
        self.historical_down_value = 0
        return CythonOnlineTechnicalIndicator.reset(self)

    cpdef np.float64_t get_metric_online(self):
        cdef np.float64_t m = np.float32(
            100. - 100. / (1 + (self.historical_up_value / (self.historical_down_value + _SMALL_EPSILON)))
        )
        return m

    cdef tuple __getinitargs__(self):
        return (self.stock, self.window)

cdef class OnlineSMA(CythonOnlineTechnicalIndicator):
    cdef:
        unsigned long window
        np.float64_t current_result

    def __init__(self, str stock, unsigned long window):
        self.window = window
        self.current_result = np.nan
        CythonOnlineTechnicalIndicator.__init__(self, stock)

    cpdef dict get_indicator_parameters(self):
        return {"window": self.window}

    cdef tuple __getinitargs__(self):
        return (self.stock, self.window)

cdef class OnlineEWMA(CythonOnlineTechnicalIndicator):
    cdef:
        unsigned long window

    def __init__(self, str stock, unsigned long window):
        self.window = window
        CythonOnlineTechnicalIndicator.__init__(self, stock)

    cpdef dict get_indicator_parameters(self):
        return {"window": self.window}

    cdef tuple __getinitargs__(self):
        return (self.stock, self.window)

cdef class OnlineKAMA(CythonOnlineTechnicalIndicator):
    cdef:
        unsigned long offset
        unsigned long lambda1
        unsigned long lambda2

    def __init__(self, str stock, unsigned long offset=10, unsigned long lambda1=2, unsigned long lambda2=30):
        self.offset = offset
        self.lambda1 = lambda1
        self.lambda2 = lambda2
        CythonOnlineTechnicalIndicator.__init__(self, stock)

    cpdef dict get_indicator_parameters(self):
        return {"offset": self.offset,
                "lambda1" : self.lambda1,
                "lambda2" : self.lambda2}

    cdef tuple __getinitargs__(self):
        return (self.stock, self.offset, self.lambda1, self.lambda2)

cdef class OnlineMACD(CythonOnlineTechnicalIndicator):
    cdef:
        unsigned long nslow
        unsigned long nfast

    def __init__(self, str stock, unsigned long nslow=26, unsigned long nfast=12):
        self.nslow = nslow
        self.nfast = nfast
        CythonOnlineTechnicalIndicator.__init__(self, stock)

    cpdef dict get_indicator_parameters(self):
        return {"nslow": self.nslow,
                "nfast" : self.nfast}

    cdef tuple __getinitargs__(self):
        return (self.stock, self.nslow, self.nfast)

cdef class OnlineOscillator(CythonOnlineTechnicalIndicator):
    cdef:
        unsigned long nslow
        unsigned long nfast

    def __init__(self, str stock, unsigned long nslow=26, unsigned long nfast=12):
        self.nslow = nslow
        self.nfast = nfast
        CythonOnlineTechnicalIndicator.__init__(self, stock)

    cpdef dict get_indicator_parameters(self):
        return {"nslow": self.nslow,
                "nfast" : self.nfast}

    cdef tuple __getinitargs__(self):
        return (self.stock, self.nslow, self.nfast)

cdef class OnlineROC(CythonOnlineTechnicalIndicator):
    cdef:
        unsigned long offset

    def __init__(self, str stock, unsigned long offset=10):
        self.offset = offset
        CythonOnlineTechnicalIndicator.__init__(self, stock)

    cpdef dict get_indicator_parameters(self):
        return {"offset": self.offset}

    cdef tuple __getinitargs__(self):
        return (self.stock, self.offset)

cdef class OnlineMomentum(CythonOnlineTechnicalIndicator):
    cdef:
        unsigned long offset

    def __init__(self, str stock, unsigned long offset=10):
        self.offset = offset
        CythonOnlineTechnicalIndicator.__init__(self, stock)

    cpdef dict get_indicator_parameters(self):
        return {"offset": self.offset}

    cdef tuple __getinitargs__(self):
        return (self.stock, self.offset)

cdef class OnlineDisparity(CythonOnlineTechnicalIndicator):
    cdef:
        unsigned long offset

    def __init__(self, str stock, unsigned long offset=10):
        self.offset = offset
        CythonOnlineTechnicalIndicator.__init__(self, stock)

    cpdef dict get_indicator_parameters(self):
        return {"offset": self.offset}

    cdef tuple __getinitargs__(self):
        return (self.stock, self.offset)

cdef class OnlineStochasticK(CythonOnlineTechnicalIndicator):
    cdef:
        unsigned long offset

    def __init__(self, str stock, unsigned long offset=14):
        self.offset = offset
        CythonOnlineTechnicalIndicator.__init__(self, stock)

    cpdef dict get_indicator_parameters(self):
        return {"offset": self.offset}

    cdef tuple __getinitargs__(self):
        return (self.stock, self.offset)

cdef class OnlineStochasticD(CythonOnlineTechnicalIndicator):
    cdef:
        unsigned long offset
        unsigned long window

    def __init__(self, str stock, unsigned long offset=14, unsigned long window=14):
        self.offset = offset
        self.window = window
        CythonOnlineTechnicalIndicator.__init__(self, stock)

    cpdef dict get_indicator_parameters(self):
        return {"offset": self.offset,
                "window": self.window}

    cdef tuple __getinitargs__(self):
        return (self.stock, self.offset, self.window)

cdef class OnlineSlowStochasticD(CythonOnlineTechnicalIndicator):
    cdef:
        unsigned long offset
        unsigned long window1
        unsigned long window2

    def __init__(self, str stock, unsigned long offset=14, unsigned long window1=3, unsigned long window2=3):
        self.offset = offset
        self.window1 = window1
        self.window2 = window2
        CythonOnlineTechnicalIndicator.__init__(self, stock)

    cpdef dict get_indicator_parameters(self):
        return {"offset": self.offset,
                "window1": self.window1,
                "window2": self.window2}

    cdef tuple __getinitargs__(self):
        return (self.stock, self.offset, self.window1, self.window2)

cdef class OnlineStochasticKD(CythonOnlineTechnicalIndicator):
    cdef:
        unsigned long offset
        unsigned long window

    def __init__(self, str stock, unsigned long offset=14, unsigned long window=14):
        self.offset = offset
        self.window= window
        CythonOnlineTechnicalIndicator.__init__(self, stock)
        #self.stochastic_k = OnlineStochasticK(stock, offset)
        #self.stochastic_d = OnlineStochasticD(stock, offset, window)

    cpdef dict get_indicator_parameters(self):
        return {"offset": self.offset,
                "window": self.window}

    cdef tuple __getinitargs__(self):
        return (self.stock, self.offset, self.window)

cdef class OnlineTSI(CythonOnlineTechnicalIndicator):
    cdef:
        unsigned long offset
        unsigned long nslow
        unsigned long nfast

    def __init__(self, str stock, unsigned long nslow=25, unsigned long nfast=13, unsigned long offset=1):
        self.nslow = nslow
        self.nfast = nfast
        self.offset = offset
        CythonOnlineTechnicalIndicator.__init__(self, stock)

    cpdef dict get_indicator_parameters(self):
        return {"offset": self.offset,
                "nslow": self.nslow,
                "nfast" : self.nfast}

    cdef tuple __getinitargs__(self):
        return (self.stock, self.nslow, self.nfast, self.offset)

cdef class CythonOnlineTechnicalIndicatorDigitizerBase(CythonModuleBase):
    cdef str name

    def __init__(self, str name):
        self.name = name

    cpdef np.float64_t get_digital_value(self, np.float64_t measure):
        raise NotImplementedError()

    cpdef str get_name(self):
        return self.name

    cpdef dict get_digitizer_parameters(self):
        raise NotImplementedError()

    cpdef set_parameters_to_optimize(self, dict parameters):
        raise NotImplementedError()

    cpdef dict get_parameters_to_optimize(self):
        raise NotImplementedError()

    cdef tuple __getinitargs__(self):
        return (self.name)

cdef class GenericTechnicalDigitizer(CythonOnlineTechnicalIndicatorDigitizerBase):
    cdef np.float64_t lower_threshold
    cdef np.float64_t upper_threshold
    cdef np.float64_t exit_position
    cdef np.float64_t orientation
    cdef np.float64_t exit_line

    def __init__(self, str name, np.float64_t lower_threshold, np.float64_t upper_threshold, np.float64_t exit_position,
                 np.float64_t orientation):
        self.lower_threshold = lower_threshold
        self.upper_threshold = upper_threshold
        self.exit_position = exit_position
        self.orientation = orientation
        self.exit_line = self.upper_threshold*self.exit_position + self.lower_threshold*(1.0-self.exit_position)
        CythonOnlineTechnicalIndicatorDigitizerBase.__init__(self, "standard" if orientation>0 else "reverse")

    cpdef np.float64_t get_digital_value(self, np.float64_t measure):
        cdef np.float64_t signal = measure if pd_checknull(measure) else (REGION_CODES[0] if measure>self.upper_threshold else REGION_CODES[1]) \
                if measure>self.exit_line else (REGION_CODES[2] if measure>self.lower_threshold else REGION_CODES[3])
        return signal*self.orientation

    cpdef dict get_digitizer_parameters(self):
        return {"lower_threshold": self.lower_threshold, "upper_threshold": self.upper_threshold,
                "exit_position": self.exit_position, "orientation": self.orientation}

    cpdef dict get_parameters_to_optimize(self):
        return {"lower_threshold": self.lower_threshold, "upper_threshold": self.upper_threshold,
                "exit_position": self.exit_position}

    cpdef set_parameters_to_optimize(self, dict parameters):
        self.lower_threshold = parameters.pop("lower_threshold")
        self.upper_threshold = parameters.pop("upper_threshold")
        self.exit_position = parameters.pop("exit_position")
        self.exit_line = self.upper_threshold*self.exit_position + self.lower_threshold*(1.0-self.exit_position)

    cdef tuple __getinitargs__(self):
        return (self.name, self.lower_threshold, self.upper_threshold, self.exit_position, self.orientation)

cdef class TechnicalTradingSignalBase(CythonModuleBase):
    '''
    A class that combines an online technical indicator (with appropriate parameters) and a function that converts the
    signal into a trading recommendation (1 == buy, 0 == do nothing, -1 == sell)
    '''
    cdef str name
    def __init__(self):
        self.__build_name()

    cpdef np.float64_t update(self, np.float64_t x, np.int64_t dt):
        '''
        Method that received a single value and pass it to the underlying online technical indicator
        '''
        raise NotImplementedError()

    cpdef reset(self):
        raise NotImplementedError()

    cpdef np.float64_t get_digitization(self):
        raise NotImplementedError()

    cpdef dict get_recommendation(self, bint with_details= False):
        raise NotImplementedError()

    cpdef str get_name(self):
        return self.name

    cpdef __build_name(self):
        raise NotImplementedError()

    cdef tuple __getinitargs__(self):
        return ()

cdef class SimpleTechnicalTradingSignal(TechnicalTradingSignalBase):
    '''
    A class that combines an online technical indicator (with appropriate parameters) and a function that converts the
    signal into a trading recommendation (1 == buy, 0 == do nothing, -1 == sell)
    '''
    cdef:
        CythonOnlineTechnicalIndicatorBase _indicator
        CythonOnlineTechnicalIndicatorDigitizerBase _digitizer

    def __init__(self, CythonOnlineTechnicalIndicatorBase indicator,
                 CythonOnlineTechnicalIndicatorDigitizerBase digitizer):
        self._digitizer = digitizer
        self._indicator = indicator
        TechnicalTradingSignalBase.__init__(self)

    @property
    def digitizer(self):
        return self._digitizer

    @property
    def indicator(self):
        return self._indicator

    cpdef np.float64_t update(self, np.float64_t x, np.int64_t dt):
        '''
        Method that received a single value and pass it to the underlying online technical indicator
        '''
        return self._indicator.update(x, dt)

    cpdef np.float64_t get_metric(self):
        '''
        Convenience function to access the udnerlying technical indicator value
        '''
        return self._indicator.get_metric()

    cpdef reset(self):
        return self._indicator.reset()

    cpdef np.float64_t get_digitization(self):
        return self._digitizer.get_digital_value(self._indicator.get_metric())

    cpdef dict get_recommendation(self, bint with_details= False):
        return {self.get_name(): self._digitizer.get_digital_value(self._indicator.get_metric())}

    cpdef __build_name(self):
        self.name = "{0}_{1}".format(self._indicator.get_name(), self._digitizer.get_name())

    cdef tuple __getinitargs__(self):
        return (self._indicator, self._digitizer)

cdef class CythonTechnicalTradingSignalVotingSchemeBase(CythonModuleBase):
    cpdef np.float64_t combine_signals(self, dict technical_trading_signal_recommendations):
        raise NotImplementedError()

    cpdef bint verify_combination_scheme_is_complete(self, list technical_indicators):
        raise NotImplementedError()

    cpdef set_parameters_to_optimize(self, dict parameters):
        raise NotImplementedError()

    cpdef dict get_parameters_to_optimize(self):
        raise NotImplementedError()



cdef class CythonSuperMajoritySimpleVotingScheme(CythonTechnicalTradingSignalVotingSchemeBase):
    cdef:
        list techn_indicator_names
        dict indicator_weights
        np.int64_t lower_threshold
        np.int64_t upper_threshold

    def __init__(self, list techn_indicator_names):
        self.techn_indicator_names = techn_indicator_names
        cdef Py_ssize_t n_tech_indicators
        n_tech_indicators = len(techn_indicator_names)
        self.indicator_weights = {k:1.0 for k in techn_indicator_names}
        self.lower_threshold = -int(2.0*n_tech_indicators/3.0)
        self.upper_threshold = int(2.0*n_tech_indicators/3.0)

    cpdef bint verify_combination_scheme_is_complete(self, list technical_indicators):
        return all([k.get_name() in self.indicator_weights for k in technical_indicators])

    cpdef np.float64_t combine_signals(self, dict technical_trading_signal_recommendations):
        total_tally = sum([technical_trading_signal_recommendations[k]*self.indicator_weights[k]
                           for k in technical_trading_signal_recommendations.keys()])
        return (-1 if total_tally < self.lower_threshold else 0 if total_tally < self.upper_threshold else 1)

    cpdef set_parameters_to_optimize(self, dict parameters):
        self.lower_threshold = parameters.pop("lower_threshold")
        self.upper_threshold = parameters.pop("upper_threshold")
        self.indicator_weights = parameters

    cpdef dict get_parameters_to_optimize(self):
        cdef dict params = {}
        params.update(self.indicator_weights)
        params["lower_threshold"] = self.lower_threshold
        params["upper_threshold"] = self.upper_threshold
        return params

    cdef tuple __getinitargs__(self):
        return (self.techn_indicator_names)

cdef class CythonWeightedAverageVotingScheme(CythonTechnicalTradingSignalVotingSchemeBase):
    cdef:
        #dict indicator_names_to_pos
        GenericTechnicalDigitizer group_digitizer
        dict indicator_weights
        list techn_indicator_names

    def __init__(self, list techn_indicator_names, np.ndarray[np.float64_t, ndim = 1] indicator_weights,
                 np.float64_t lower_threshold, np.float64_t upper_threshold,
                 np.float64_t exit_position, np.int64_t orientation):
        self.techn_indicator_names = techn_indicator_names
        #for count, indicator_name in enumerate(techn_indicator_names):
        #    self.indicator_names_to_pos[indicator_name] = count
        self.indicator_weights = dict(zip(techn_indicator_names, indicator_weights))
        self.group_digitizer = GenericTechnicalDigitizer("", lower_threshold, upper_threshold, exit_position, orientation)


    cpdef bint verify_combination_scheme_is_complete(self, list technical_indicators):
        return all([k.get_name() in self.indicator_weights for k in technical_indicators])

    cpdef np.float64_t combine_signals(self, dict technical_trading_signal_recommendations):
        cdef np.float64_t tally = 0.0
        for k in self.indicator_weights:
            if not pd_checknull(technical_trading_signal_recommendations[k]):
                tally += technical_trading_signal_recommendations[k]*self.indicator_weights[k]
        return self.group_digitizer.get_digital_value(tally)

    cpdef get_digitizer(self):
        return self.group_digitizer

    cpdef set_parameters_to_optimize(self, dict parameters):
        self.group_digitizer.set_parameters_to_optimize(parameters)
        self.indicator_weights = parameters

    cpdef dict get_parameters_to_optimize(self):
        cdef dict params = {}
        params.update(self.indicator_weights)
        params.update(self.group_digitizer.get_parameters_to_optimize())
        return params

    cdef tuple __getinitargs__(self):
        cdef dict params = self.group_digitizer.get_digitizer_parameters()
        return (self.techn_indicator_names, self.indicator_weights, params["lower_threshold"],
                params["upper_threshold"], params["exit_position"], params["orientation"])

cdef class TechnicalTradingSignalCohort(TechnicalTradingSignalBase):
    '''
    A class representing a tehcnical indicator that is composed from sub-technical indicator and a voting scheme
    '''
    cdef:
        str stock
        str cohort_name
        list _technical_signals
        CythonTechnicalTradingSignalVotingSchemeBase _voting_scheme

    def __init__(self, str stock, str cohort_name, CythonTechnicalTradingSignalVotingSchemeBase voting_scheme,
                 list technical_signals, bint verify_stock):
        if verify_stock:
            for technical_signal in technical_signals:
                if technical_signal.indicator.get_stock() != stock:
                    raise RuntimeError()
        if not voting_scheme.verify_combination_scheme_is_complete(technical_signals):
            raise RuntimeError()
        self.stock = stock
        self.cohort_name  = cohort_name
        self._voting_scheme = voting_scheme
        self._technical_signals = technical_signals
        TechnicalTradingSignalBase.__init__(self)

    @property
    def technical_signals(self):
        return self._technical_signals

    @property
    def voting_scheme(self):
        return self._voting_scheme


    cpdef np.float64_t update(self, np.float64_t x, np.int64_t dt):
        '''
        Updates all technical indicators
        '''
        for technical_signal in self._technical_signals:
            technical_signal.update(x, dt)
        return np.NaN


    cpdef reset(self):
        for technical_signal in self._technical_signals:
            technical_signal.reset()

    cpdef np.float64_t get_digitization(self):
        cdef dict signals
        signals = {}
        for technical_signal in self.technical_signals:
            signals.update(technical_signal.get_recommendation())
        return self._voting_scheme.combine_signals(signals)


    cpdef dict get_recommendation(self, bint with_details= False):
        cdef dict signals
        cdef np.float64_t cohort_recommendation
        cdef dict result
        signals = {}
        for technical_signal in self.technical_signals:
            signals.update(technical_signal.get_recommendation())
        cohort_recommendation = self._voting_scheme.combine_signals(signals)
        result = {self.cohort_name:cohort_recommendation}
        if with_details:
            result.update(signals)
        return result

    cpdef __build_name(self):
        self.name = self.cohort_name

    cdef tuple __getinitargs__(self):
        return (self.stock, self.cohort_name, self._voting_scheme, self.technical_signals, True)

cdef class TransactionUntilityFunction(CythonModuleBase):
    cdef :
        np.float64_t fee_amount

    def __init__(self, np.float64_t fee_amount):
        self.fee_amount = fee_amount

    cpdef dict get_indexers_for_holding_signal_segments(self, np.ndarray holding_signal):
        cdef:
            np.ndarray[np.int64_t, ndim=1] positions = np.arange(0,len(holding_signal)+1).astype(np.int64)
            np.ndarray[np.int64_t, ndim=1] transition_points = positions[np.insert(np.insert(np.diff(holding_signal)!=0,0,True),-1,True)]
            dict indexers

        indexers = dict(enumerate([(holding_signal[transition_points[i]],
                                    slice(transition_points[i], transition_points[i+1]))
                                    for i in range(len(transition_points)-1)]))
        return indexers

    cpdef np.ndarray convert_technical_signal_to_holdings(self, indicator_value_input):
        cdef:
            np.ndarray[np.float64_t, ndim=1] indicator_value = indicator_value_input
            Py_ssize_t N = len(indicator_value)
            Py_ssize_t i
            np.ndarray[np.float64_t, ndim=1] holding_signal = np.array([np.nan]*N)
            np.ndarray[np.float64_t, ndim=1] indicator_value_sign = np.sign(indicator_value)

        signal_enter_mask = (np.abs(indicator_value) - 1.0) < 1E-8
        holding_signal[signal_enter_mask] = indicator_value[signal_enter_mask]

        signal_close_mask = np.abs(indicator_value) < 1E-8
        holding_signal[signal_close_mask] = 0.0
        if pd_checknull(holding_signal[0]):
            holding_signal[0] = 0
        for i in range(1,N):
            if pd_checknull(holding_signal[i]):
                holding_signal[i] = 0 if (indicator_value_sign[i] != indicator_value_sign[i-1]) else holding_signal[i-1]
        return holding_signal

    cpdef np.ndarray calculate_pnl_after_fee(self, np.ndarray indicator_value_input,
                                            np.ndarray returns_value_input, bint input_is_holding=False):
        cdef:
            np.ndarray[np.float64_t, ndim=1] holding_signal = indicator_value_input if input_is_holding\
                else self.convert_technical_signal_to_holdings(indicator_value_input)
            Py_ssize_t N = len(holding_signal)
            np.ndarray[np.float64_t, ndim=1] returns_value = returns_value_input
            np.ndarray[np.float64_t, ndim=1] holding_signal_change = np.array([np.nan]*N)
            np.ndarray[np.float64_t, ndim=1] pnl_with_fees

        holding_signal_change = np.insert(np.diff(holding_signal), 0, 0)
        pnl = holding_signal*returns_value
        pnl_with_fees = pnl - np.abs(holding_signal_change)*self.fee_amount
        return pnl_with_fees

    cpdef np.float64_t calculate_measure(self, np.ndarray indicator_value,
                                        np.ndarray returns_value, bint input_is_holding=False):
        raise NotImplementedError()

    cdef tuple __getinitargs__(self):
        return (self.fee_amount, )



cdef class GenericPerformanceIndicatorPerSegments(TransactionUntilityFunction):
    cdef:
        TransactionUntilityFunction function_to_apply
        list filter_holdings
        aggregator

    def __init__(self, np.float64_t fee_amount, TransactionUntilityFunction segment_func, aggregator,
                 list filter_holdings=None):
        TransactionUntilityFunction.__init__(self, fee_amount)
        self.filter_holdings = [] if not filter_holdings else filter_holdings
        self.function_to_apply = segment_func
        self.aggregator = aggregator

    cpdef np.float64_t calculate_measure(self, np.ndarray indicator_value_input,
                                        np.ndarray returns, bint input_is_holding=False):
        cdef:
            np.ndarray[np.float64_t, ndim=1] returns_value = returns
            np.ndarray[np.float64_t, ndim=1] holding_signal = indicator_value_input.astype(float) if input_is_holding\
                else self.convert_technical_signal_to_holdings(indicator_value_input)
            np.ndarray[np.float64_t, ndim=1] segment_pnl
            np.ndarray holding
            dict segments_indexer = self.get_indexers_for_holding_signal_segments(holding_signal)
            list segment_results = []
            dict filtered_segments
            np.float64_t measure
        filtered_segments = {k:segments_indexer[k] for k in segments_indexer if segments_indexer[k][0]
                             in self.filter_holdings} if self.filter_holdings else segments_indexer

        #print filtered_segments
        for k in filtered_segments:
            #print k, filtered_segments[k]
            segment_pnl = returns_value[filtered_segments[k][1]]
            segment_pnl[0] -= self.fee_amount
            segment_pnl[-1] -= self.fee_amount
            holding = np.asarray([filtered_segments[k][0]]* (filtered_segments[k][1].stop - filtered_segments[k][1].start) )
            measure = self.function_to_apply.calculate_measure(holding, segment_pnl, True)
            segment_results.append(measure)
        #print np.nan_to_num(np.asarray(segment_results))
        measure = self.aggregator(np.nan_to_num(np.asarray(segment_results)))
        return measure

    cdef tuple __getinitargs__(self):
        return (self.fee_amount, self.function_to_apply, self.aggregator, self.filter_holdings)

cdef class PerformanceSummary(CythonModuleBase):
    cdef:
        dict performance_measures

    def __init__(self, dict performance_measures=None):
        cdef:
            list valid_measures
        self.performance_measures = {} if not performance_measures else performance_measures
        invalid_measures = {k:self.performance_measures[k] for k in self.performance_measures
                            if not isinstance(self.performance_measures[k], TransactionUntilityFunction)}
        if invalid_measures:
            raise ValueError("Received dictionary with objects that are not TransactionUntilityFunction: {0}"
                             .format(invalid_measures))

    cpdef dict calculate_measure(self, np.ndarray indicator_value,
                                 np.ndarray returns_value, bint input_is_holding=False):
        cdef:
            dict measures
        measures = {k:self.performance_measures[k].calculate_measure(indicator_value, returns_value, input_is_holding)
                    for k in self.performance_measures}
        return measures

    cdef tuple __getinitargs__(self):
        return (self.performance_measures, )

cdef class TechnicalStrategyPerformance(TransactionUntilityFunction):
    def __init__(self, np.float64_t fee_amount):
        TransactionUntilityFunction.__init__(self, fee_amount)

    cpdef np.float64_t calculate_measure(self, np.ndarray indicator_value,
                                        np.ndarray returns_value, bint input_is_holding=False):
        cdef np.ndarray pnl
        pnl = self.calculate_pnl_after_fee(indicator_value, returns_value, input_is_holding)
        #return np.exp(umr_sum(pnl, keepdims=False)) -1.0
        return np.exp(umr_sum(np.log(pnl+1), keepdims=False)) -1.0

    cdef tuple __getinitargs__(self):
        return (self.fee_amount, )

cdef class TechnicalStrategyHitRatio(TransactionUntilityFunction):
    def __init__(self, np.float64_t fee_amount):
        TransactionUntilityFunction.__init__(self, fee_amount)

    cpdef np.float64_t calculate_measure(self, np.ndarray indicator_value,
                                        np.ndarray returns_value, bint input_is_holding=False):
        cdef:
            np.ndarray pnl
            np.float64_t positive, negative
        pnl = self.calculate_pnl_after_fee(indicator_value, returns_value, input_is_holding)
        positive = umr_sum((pnl>0.0).astype(int), keepdims=False)
        negative = umr_sum((pnl<0.0).astype(int), keepdims=False)
        return positive / (positive + negative) if (positive + negative) > 0 else np.NaN

    cdef tuple __getinitargs__(self):
        return (self.fee_amount, )

cdef class TechnicalStrategyTimeLenght(TransactionUntilityFunction):
    def __init__(self, np.float64_t fee_amount):
        TransactionUntilityFunction.__init__(self, fee_amount)

    cpdef np.float64_t calculate_measure(self, np.ndarray indicator_value,
                                        np.ndarray returns_value, bint input_is_holding=False):
        return len(indicator_value)

    cdef tuple __getinitargs__(self):
        return (self.fee_amount, )


cdef class TechnicalStrategyOutPerformance(TransactionUntilityFunction):
    def __init__(self, np.float64_t fee_amount):
        TransactionUntilityFunction.__init__(self, fee_amount)

    cpdef np.float64_t calculate_measure(self, np.ndarray indicator_value,
                                        np.ndarray returns_value, bint input_is_holding=False):
        cdef np.ndarray pnl
        pnl = self.calculate_pnl_after_fee(indicator_value, returns_value, input_is_holding)
        return np.exp(np.sum(pnl)) - np.exp(np.sum(returns_value))

    cdef tuple __getinitargs__(self):
        return (self.fee_amount, )

cdef class TechnicalStrategySharpe(TransactionUntilityFunction):
    def __init__(self, np.float64_t fee_amount):
        TransactionUntilityFunction.__init__(self, fee_amount)

    cpdef np.float64_t calculate_measure(self, np.ndarray indicator_value,
                                        np.ndarray returns_value, bint input_is_holding=False):
        cdef np.ndarray pnl
        cdef np.float64_t mu
        cdef np.float64_t sig
        pnl = self.calculate_pnl_after_fee(indicator_value, returns_value, input_is_holding)
        mu = np.mean(pnl)
        #print mu
        sig = np.std(pnl)
        #print sig
        return (1000000 if (sig<1E-11 or np.isnan(sig)) else MINS_ANNUALIZE_SHARPE*mu/sig)

    cdef tuple __getinitargs__(self):
        return (self.fee_amount, )

cdef class TechnicalStrategySortino(TransactionUntilityFunction):
    def __init__(self, np.float64_t fee_amount):
        TransactionUntilityFunction.__init__(self, fee_amount)

    cpdef np.float64_t calculate_measure(self, np.ndarray indicator_value,
                                        np.ndarray returns_value, bint input_is_holding=False):
        cdef np.ndarray pnl
        cdef np.float64_t mu
        cdef np.float64_t sig
        pnl = self.calculate_pnl_after_fee(indicator_value, returns_value, input_is_holding)
        mu = np.mean(pnl)
        sig = np.std(pnl[pnl<0])
        return (1000000 if (sig<1E-11 or np.isnan(sig)) else MINS_ANNUALIZE_SHARPE*mu/sig)

    cdef tuple __getinitargs__(self):
        return (self.fee_amount, )

cdef class TechnicalStrategyCalmar(TransactionUntilityFunction):
    def __init__(self, np.float64_t fee_amount):
        TransactionUntilityFunction.__init__(self, fee_amount)

    cpdef np.float64_t calculate_measure(self, np.ndarray indicator_value,
                                        np.ndarray returns_value, bint input_is_holding=False):
        cdef np.ndarray pnl
        cdef np.ndarray cum_ret
        cdef np.int64_t argmax_index
        cdef np.int64_t argmax_index_cum_ret
        cdef np.float64_t mu
        cdef np.float64_t sig
        cdef np.float64_t offset = 0.1
        cdef np.float64_t threshold = 0.05
        cdef np.float64_t MDD
        cdef np.float64_t numer
        cdef np.float64_t denom
        cdef np.float64_t m_calmar

        pnl = self.calculate_pnl_after_fee(indicator_value, returns_value, input_is_holding)
        #print pnl
        cum_ret = np.exp(np.cumsum(pnl)) # dropna part
        #print cum_ret
        argmax_index = (np.maximum.accumulate(cum_ret) - cum_ret).argmax()
        #print argmax_index
        if argmax_index == 0:
            return 1000000
        argmax_index_cum_ret = np.argmax(cum_ret[:argmax_index])
        MDD = MDD_MULTIPLE*(cum_ret[argmax_index_cum_ret] - cum_ret[argmax_index])/cum_ret[argmax_index_cum_ret]
        numer = cum_ret[len(cum_ret)-1] - 1.
        denom = (PERIOD_MDD_OFFSET + max(MDD, PERIOD_MDD_THRESHOLD)) if numer>=0 else (PERIOD_MDD_OFFSET + PERIOD_MDD_THRESHOLD)
        m_calmar = numer/denom
        return m_calmar if not np.isinf(m_calmar) else 1000000

    cdef tuple __getinitargs__(self):
        return (self.fee_amount, )

cdef class ParticleOptimizationFunction(CythonModuleBase):
    cpdef np.ndarray get_parameter_values_to_optimize(self):
        raise NotImplementedError()

    cpdef set_optimized_parameter_values(self, np.ndarray values):
        raise NotImplementedError()

    cpdef np.float64_t evaluate(self):
        raise NotImplementedError()

    cpdef list get_names_of_parameter_values_to_optimize(self):
        raise NotImplementedError()

cdef class testParticleOptimizationFunction(ParticleOptimizationFunction):
    cdef public np.ndarray S, t, params
    def __init__(self):
        cdef long N = 1000
        cdef np.ndarray t = np.linspace(-2, 2, N)
        self.t = t
        self.S = 4*t*np.cos(4*np.pi*t**2) * np.exp(-3*t**2) + (np.random.random(N)-0.5)*0.000005
        self.params = np.array([1.0]*4)

    cpdef np.ndarray get_parameter_values_to_optimize(self):
        return self.params

    cpdef set_optimized_parameter_values(self, np.ndarray[np.float64_t, ndim=1] values):
        self.params = values

    cpdef np.float64_t evaluate(self):
        cdef np.ndarray[np.float64_t, ndim=1] a = self.params
        cdef np.ndarray t = self.t
        cdef np.ndarray est = a[0]*t*np.sin(a[1]*np.pi*t**2 +a[2])*np.exp(-a[3]*t**2)
        return np.sum(np.abs(self.S-est))

    cdef tuple __getinitargs__(self):
        return ()

cdef class SimpleTechnicalTradingSignalOptimizationFunction(ParticleOptimizationFunction):
    cdef:
        CythonOnlineTechnicalIndicatorDigitizerBase digitizer
        public list param_names
        np.ndarray params
        np.ndarray returns
        np.ndarray underlying_signal
        np.ndarray digitized_signal
        Py_ssize_t n_signals
        Py_ssize_t n_params
        TransactionUntilityFunction util_func

    def __init__(self, CythonOnlineTechnicalIndicatorDigitizerBase digitizer, np.ndarray[np.float64_t, ndim=1] returns,
                 np.ndarray[np.float64_t, ndim=1] underlying_signal, TransactionUntilityFunction util_func):
        self.digitizer = digitizer
        self.param_names = sorted(self.digitizer.get_parameters_to_optimize().keys())
        self.n_params = len(self.param_names)
        self.params = np.array([np.nan]*self.n_params)
        self.util_func = util_func
        self.returns = np.around(returns, _PRECISION)
        self.underlying_signal = np.around(underlying_signal, _PRECISION)
        self.n_signals = len(underlying_signal)
        self.digitized_signal = np.array([0.0]*self.n_signals)
        #print self.param_names

    cpdef np.ndarray get_parameter_values_to_optimize(self):
        cdef dict params = self.digitizer.get_parameters_to_optimize()
        cdef Py_ssize_t i
        cdef np.ndarray[np.float64_t, ndim=1] params_view = self.params
        for i in range(self.n_params):
            params_view[i] = params[self.param_names[i]]
        return self.params

    cpdef set_optimized_parameter_values(self, np.ndarray[np.float64_t, ndim=1] values):
        cdef dict params = {}
        cdef Py_ssize_t i
        for i in range(self.n_params):
            params[self.param_names[i]] = values[i]
        self.digitizer.set_parameters_to_optimize(params)

    cpdef np.float64_t evaluate(self):
        cdef Py_ssize_t i
        cdef np.ndarray[np.float64_t, ndim=1] digitized_signal_views = self.digitized_signal
        cdef np.ndarray[np.float64_t, ndim=1] underlying_signal_views = self.underlying_signal
        for i in range(self.n_signals):
            digitized_signal_views[i] = self.digitizer.get_digital_value(underlying_signal_views[i])
        cdef np.float64_t result = self.util_func.calculate_measure(digitized_signal_views, self.returns)
        return result

    cpdef np.ndarray get_optimal_digitized_signals(self):
        return self.digitized_signal

    cpdef np.ndarray get_underlying_signals(self):
        return self.underlying_signal

    cdef tuple __getinitargs__(self):
        return (self.digitizer, self.returns, self.underlying_signal, self.util_func)

    cpdef list get_names_of_parameter_values_to_optimize(self):
        return self.param_names

cdef class VotingSchemeOptimizationFunction(ParticleOptimizationFunction):
    cdef:
        CythonTechnicalTradingSignalVotingSchemeBase voting_scheme
        public list params_names
        np.ndarray params
        np.ndarray returns
        np.ndarray combined_signal
        list underlying_digitization # list of dict, one for each day
        Py_ssize_t n_dates
        Py_ssize_t n_params
        TransactionUntilityFunction util_func

    def __init__(self, CythonTechnicalTradingSignalVotingSchemeBase voting_scheme, np.ndarray[np.float64_t, ndim=1] returns,
                 list underlying_digitization, TransactionUntilityFunction util_func):
        self.voting_scheme = voting_scheme
        self.params_names = sorted(voting_scheme.get_parameters_to_optimize().keys())
        self.n_params = len(self.params_names)
        self.params = np.array([np.nan]*self.n_params)
        self.util_func = util_func
        self.returns = returns
        self.underlying_digitization = underlying_digitization
        self.n_dates = len(underlying_digitization)
        self.combined_signal = np.array([0.0]*self.n_dates)
        #print self.params_names

    cpdef np.ndarray get_parameter_values_to_optimize(self):
        cdef dict params = self.voting_scheme.get_parameters_to_optimize()
        cdef Py_ssize_t i
        cdef np.ndarray[np.float64_t, ndim=1] params_view = self.params
        for i in range(self.n_params):
            params_view[i] = params[self.params_names[i]]
        return self.params

    cpdef set_optimized_parameter_values(self, np.ndarray[np.float64_t, ndim=1] values):
        cdef dict params = {}
        cdef Py_ssize_t i
        for i in range(self.n_params):
            params[self.params_names[i]] = values[i]
        self.voting_scheme.set_parameters_to_optimize(params)

    cpdef np.float64_t evaluate(self):
        cdef Py_ssize_t i
        #print self.combined_signal
        cdef np.ndarray[np.float64_t, ndim=1] temp = self.combined_signal
        for i in range(self.n_dates):
            temp[i] = self.voting_scheme.combine_signals(self.underlying_digitization[i])
        #print self.combined_signal
        cdef np.float64_t result = self.util_func.calculate_measure(temp, self.returns)
        return result

    cdef tuple __getinitargs__(self):
        return (self.voting_scheme, self.returns, self.underlying_digitization, self.util_func)

    cpdef np.ndarray get_optimal_digitized_signals(self):
        return self.combined_signal

    cpdef list get_names_of_parameter_values_to_optimize(self):
        return self.params_names

cdef class Particle(CythonModuleBase):
    '''
    Class for common properties of the individual particles in the swarm.
    Individual positions, velocity, fitness, running best positions, best fitness,
    fit functions, and auxiliary variables for the fit function

    '''
    cdef:
        Py_ssize_t dimension
        np.ndarray initial_position
        np.ndarray position
        np.ndarray best_position
        np.ndarray velocity
        np.float64_t fitness
        public np.float64_t best_fit
        ParticleOptimizationFunction fit_func
        np.ndarray particle_lb
        np.ndarray particle_ub
        str optimization_func_string

    def __init__(self, ParticleOptimizationFunction fit_func, np.ndarray[np.float64_t, ndim=1] particle_lb,
                 np.ndarray[np.float64_t, ndim=1] particle_ub, str optimization_func_string = "min",
                 np.ndarray[np.float64_t, ndim=1] initial_position=None):
        self.dimension = len(fit_func.get_parameter_values_to_optimize())
        self.initial_position = initial_position
        if initial_position is not None and len(self.initial_position)!=self.dimension:
            raise RuntimeError("length of initial_position must be the same as number of parameters to optimize")
        self.fit_func = fit_func
        self.particle_lb = particle_lb
        self.particle_ub = particle_ub
        self.optimization_func_string = optimization_func_string

        self.initialize_particle_parameters()

    cpdef initialize_particle_parameters(self):
        cdef np.ndarray particle_range = self.particle_ub - self.particle_lb
        self.position = np.random.random(self.dimension)*particle_range + self.particle_lb \
            if self.initial_position is None else np.array(self.initial_position)
        self.position = np.around(self.position, decimals=_PRECISION)
        self.fit_func.set_optimized_parameter_values(self.position)
        self.velocity = np.asarray([-1., 1.][np.random.random()>0.5]*np.random.random(self.dimension)*particle_range)
        self.fitness = self.fit_func.evaluate()
        self.best_fit = self.fitness
        self.best_position = np.array(self.position)

    cpdef update_best_position(self):
        cdef np.ndarray[np.float64_t, ndim=1] pos_view = self.position
        cdef np.ndarray[np.float64_t, ndim=1] best_view = self.best_position
        cdef Py_ssize_t i
        if (((self.optimization_func_string == "min")  and (self.fitness < self.best_fit)) or
            ((self.optimization_func_string == "max")  and (self.fitness > self.best_fit))):
            self.best_fit = self.fitness
            for i in range(self.dimension):
                best_view[i] = pos_view[i]

    cpdef update_position(self, np.ndarray global_best_position, np.float64_t weight_w,
                          np.float64_t weight_c_1, np.float64_t weight_c_2):

        cdef np.ndarray[np.float64_t, ndim=1] rand = np.random.random(2)

        cdef np.ndarray[np.float64_t, ndim=1] local_momentum = self.best_position - self.position
        cdef np.ndarray[np.float64_t, ndim=1] global_momentum = global_best_position - self.position

        # velocity is a randomized blend of locally and globally optimal directions as well as damping
        self.velocity = weight_w*self.velocity \
            + weight_c_1*rand[0]*local_momentum \
            + weight_c_2*rand[1]*global_momentum
        self.position += self.velocity

        # If the particle position goes out of bounds, we clamp it (dimension by dimension) to
        # defined boundaries
        self.position = np.around(np.maximum(np.minimum(self.position, self.particle_ub), self.particle_lb), decimals=_PRECISION)
        self.fit_func.set_optimized_parameter_values(self.position)

        # Fitness function is is evaluated for each particle
        self.fitness = self.fit_func.evaluate()
        self.update_best_position()

    cpdef tuple get_best_parameters(self):
        return self.best_position, self.best_fit

    cdef tuple __getinitargs__(self):
        return (self.fit_func, self.particle_lb, self.particle_ub, self.optimization_func_string, self.initial_position)

cdef class ParticleSwarmOptimization(CythonModuleBase):
    '''
    Implementation of classic and linearly accelerated particle swarm optimization
    '''

    cdef:
        unsigned long num_particles
        unsigned long max_iterations
        str target_optimization_func_string
        np.ndarray particle_lb
        np.ndarray particle_ub
        bint acceleration
        unsigned long num_dimensions
        target_optimization_func
        np.float64_t termin_tol
        unsigned long termin_iter
        np.ndarray global_best_position
        np.float64_t global_best_fit
        np.ndarray particles
        np.ndarray particle_fits
        ParticleOptimizationFunction fit_func
        np.ndarray weight_vector
        np.ndarray positions_to_include

        np.float64_t weight_w
        np.float64_t weight_c_1
        np.float64_t weight_c_2

        np.float64_t weight_w_i
        np.float64_t weight_w_f
        np.float64_t weight_c_1_i
        np.float64_t weight_c_1_f
        np.float64_t weight_c_2_i
        np.float64_t weight_c_2_f

    def __init__(self, unsigned long num_particles, unsigned long max_iterations, unsigned long num_dimensions,
                 np.ndarray[np.float64_t, ndim=1] weight_vector,
                 np.ndarray[np.float64_t, ndim=1] particle_lb, np.ndarray[np.float64_t, ndim=1] particle_ub,
                 ParticleOptimizationFunction fit_func, bint acceleration=False,
                 str target_optimization_func_string="min", np.float64_t termin_tol=1e-6,
                 unsigned long termin_iter=100, np.ndarray[np.float64_t, ndim=2] positions_to_include=None):
        self.num_particles = num_particles
        self.max_iterations = max_iterations
        self.target_optimization_func_string = target_optimization_func_string

        # check if special positions are provided
        self.positions_to_include = positions_to_include
        if self.positions_to_include is not None:
            if self.positions_to_include.shape[1] != num_dimensions:
                raise RuntimeError("positions_to_include must have same dimensionality on columns as num_dimensions")
            if self.positions_to_include.shape[0] > num_particles:
                raise RuntimeError("lenght of positions_to_include must be less or equal than num_particles")


        # Lower and upper bounds for the particle positions
        self.particle_lb = particle_lb
        self.particle_ub = particle_ub

        # If acceleration=False, it is plain vanilla particle swarm optimization.
        # If True we decrease the damping factor and local optima weights linearly over time
        # while increasing the global optima weights over time. In this case must specify the
        # initial and final weights
        self.acceleration = acceleration
        self.num_dimensions = num_dimensions
        # This flag indicates whether we are looking for the minimum or maximum fit

        if target_optimization_func_string == "min":
            self.target_optimization_func = np.argmin
        elif target_optimization_func_string == "max":
            self.target_optimization_func = np.argmax
        else:
            raise RuntimeError("not known optimization func string {0}".format(target_optimization_func_string))

        # Early termination condition. If the fit function plateaus (by delta termin_tol) for
        # more termin_iter iterations we invoke early termination
        self.termin_tol = termin_tol
        self.termin_iter = termin_iter

        self.global_best_position = np.array([np.nan]*num_dimensions)
        self.global_best_fit = np.nan

        # List of particles (of the Particle class)
        #self.particles = []
        # Fit function to be minimized (or maximized must be specified) with auxiliary variables
        self.fit_func = fit_func
        #self.aux_var = aux_var

        # The accelerated version requires the initial and terminal values to be interpolated
        if acceleration:
            if len(weight_vector)!=6:
                raise("Should provide weights w_i, c_1_i, c_2_i, w_f, c_1_f, c_2_f")
        else:
            if len(weight_vector)!=3:
                raise("Should provide weights w, c_1, c_2")
        self.weight_vector = weight_vector
        self.particle_fits = np.array([0.0]*self.num_particles)

    cpdef _initialize_weights(self, np.ndarray[np.float64_t, ndim=1] weight_vector):
        self.weight_w = weight_vector[0]
        self.weight_c_1 = weight_vector[1]
        self.weight_c_2 = weight_vector[2]

        if self.acceleration:
            self.weight_w_i = weight_vector[0]
            self.weight_w_f = weight_vector[3]
            self.weight_c_1_i = weight_vector[1]
            self.weight_c_1_f = weight_vector[4]
            self.weight_c_2_i = weight_vector[2]
            self.weight_c_2_f = weight_vector[5]

    cpdef obtain_global_optimal_parameters(self):
        cdef np.ndarray[np.float64_t, ndim=1] temp_fit = self.particle_fits
        cdef np.ndarray[object, ndim=1] temp_part = self.particles
        cdef Py_ssize_t i
        for i in range(self.num_particles):
            temp_fit[i] = temp_part[i].best_fit
        temp_fit = np.around(temp_fit, decimals=_PRECISION)
        best_particle_index = self.target_optimization_func(temp_fit)
        self.global_best_position, self.global_best_fit = temp_part[best_particle_index].get_best_parameters()

    cpdef _initialize_particles(self):
        particle_list = []
        cdef Py_ssize_t n
        cdef Py_ssize_t n_special_pos = len(self.positions_to_include) if self.positions_to_include is not None else 0
        cdef np.ndarray[np.float64_t, ndim=2] special_pos
        if n_special_pos!=0:
            special_pos = self.positions_to_include
            for n in range(n_special_pos):
                p = Particle(self.fit_func, self.particle_lb, self.particle_ub,
                             self.target_optimization_func_string, special_pos[n])
                particle_list.append(p)
        for n in range(self.num_particles - n_special_pos):
            p = Particle(self.fit_func, self.particle_lb, self.particle_ub,
                         self.target_optimization_func_string)
            particle_list.append(p)
        self.particles = np.array(particle_list, dtype=np.object_)
        self.obtain_global_optimal_parameters()

    cpdef _update_weights(self, np.float64_t current_iter):
        # Only invoked if acceleration is on, otherwise weights are static
        cdef np.float64_t increment = current_iter/self.max_iterations
        self.weight_w = (self.weight_w_f - self.weight_w_i) * increment + self.weight_w_i
        self.weight_c_1 = (self.weight_c_1_f - self.weight_c_1_i) * increment + self.weight_c_1_i
        self.weight_c_2 = (self.weight_c_2_f - self.weight_c_2_i) * increment + self.weight_c_2_i

    cpdef _update(self):
        cdef Py_ssize_t i
        cdef np.ndarray[object, ndim=1] temp_part = self.particles
        for i in range(self.num_particles):
            temp_part[i].update_position(self.global_best_position, self.weight_w, self.weight_c_1, self.weight_c_2)
        self.obtain_global_optimal_parameters()

    cpdef _results(self):
        # returns the current best positions and fit values
        return self.global_best_position, self.global_best_fit

    cpdef optimize(self):
        cdef Py_ssize_t ind
        cdef np.ndarray[np.float64_t, ndim=1] pos
        cdef unsigned long int plateau_count = 0
        cdef np.float64_t prev_best, current_best, fit_delta

        print "Utility before optimizing parameters: {0}. Parameters={1}".format(self.fit_func.evaluate(),
                                                                                 self.fit_func.get_parameter_values_to_optimize())
        self.__reset_optimization_parameters()
        # Loops through successive particle migrations for specified maximum number of iterations
        print("Iteration 0 of {} : current global best = {}; best parameters = {}".format(self.max_iterations,
                                                                                          self.global_best_fit,
                                                                                          self.global_best_position))
        for ind in range(self.max_iterations):

            # store previous values to check early termination
            pos, prev_best = self._results()
            self._update()
            pos, current_best = self._results()

            # successive difference in fit
            fit_delta = np.abs(prev_best-current_best)
            if fit_delta<self.termin_tol:
                # early termination if the fit function has plateaued for specified number of iterations
                # termin_iter should be sufficiently large to give the particles a chance to randomly escape stagnation
                plateau_count += 1
                if plateau_count>=self.termin_iter:
                    print("***** Early termination due to fit function plateau ")
                    break
            else:
                plateau_count = 0

            if self.acceleration:
                self._update_weights(<np.float64_t>ind+1.0)

            print("Iteration {} of {} : current global best = {}; best parameters = {}".format(ind+1,
                                                                                   self.max_iterations,
                                                                                   self.global_best_fit,
                                                                                   self.global_best_position))
        self.fit_func.set_optimized_parameter_values(self.global_best_position)
        print "Utility after optimizing parameters: {0}. Parameters={1}".format(self.fit_func.evaluate(),
                                                                                self.fit_func.get_parameter_values_to_optimize())


        return self._results()

    cpdef __reset_optimization_parameters(self):
        t1 = Timestamp.now()
        print(t1)
        self._initialize_particles()
        t2 = Timestamp.now()
        print(t2)
        print(t2-t1)
        self._initialize_weights(self.weight_vector)


    cpdef ParticleOptimizationFunction get_optimization_function(self):
        return self.fit_func

    cdef tuple __getinitargs__(self):
        return (self.num_particles, self.max_iterations, self.num_dimensions, self.weight_vector, self.particle_lb,
                self.particle_ub, self.fit_func, self.acceleration, self.target_optimization_func_string,
                self.termin_tol, self.termin_iter)
