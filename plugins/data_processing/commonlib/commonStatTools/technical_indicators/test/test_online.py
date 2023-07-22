from functools import partial

import numpy as np
import pandas as pd
import sys
import os

import _pickle as cPickle
import pytest

from hypothesis import given, note
from hypothesis.extra.numpy import arrays
from hypothesis.strategies import floats, integers, booleans, sampled_from
from numpy.testing import assert_allclose

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from online import (
    OnlineRSI,
    OnlineSMA,
    OnlineEWMA,
    OnlineKAMA,
    OnlineMACD,
    OnlineOscillator,
    OnlineROC,
    OnlineMomentum,
    OnlineDisparity,
    OnlineStochasticK,
    OnlineStochasticD,
    OnlineSlowStochasticD,
    OnlineTSI,
    OnlineRealizedBipowerVariance,
    OnlineTruncatedPowerVariance,
    OnlineLeeJumpDetection
)

from offline import (
    rsi,
    MA,
    KaufmanMA,
    macd,
    price_oscillator,
    rate_of_change,
    momentum,
    disparity,
    stoch_k,
    stoch_d,
    slow_stoch_d,
    tsi,
    realized_bipower_var,
    truncated_power_var,
    lee_jump_detection
)

NELEMENTS_FOR_PRICE_TESTING = 100
NELEMENTS_FOR_RETURN_TESTING = 300
ALLOWED_TOLERANCE = 1e-5
DEFAULT_PRICE_STRATEGY = arrays(np.float64, NELEMENTS_FOR_PRICE_TESTING, elements=floats(min_value=0.1, max_value=1000.0))
DEFAULT_RETURNS_STRATEGY = arrays(np.float64, NELEMENTS_FOR_RETURN_TESTING, elements=floats(min_value=-0.2, max_value=0.2))
DEFAULT_WINDOW_VARIANCE_AND_JUMPS = integers(min_value=20, max_value=100)
DEFAULT_WINDOW_SIZE_STRATEGY = integers(min_value=2, max_value=64)

DEFAULT_DELTA_T_VARIANCE_AND_JUMPS = 1.0/252
DEFAULT_G = 1.2

pd.set_option('display.max_rows', 500)
pd.set_option('display.precision', 50)


def compare_with_offline(input_values, online_indicator_class, offline_indicator_func, *args, **kwargs):
    #input_values = np.around(input_values, decimals=5)
    input_values = pd.Series(input_values)
    online_indicator = online_indicator_class("SYM", *args, **kwargs)

    online_results = pd.Series(map(online_indicator.update, input_values))
    offline_results = offline_indicator_func(input_values, *args, **kwargs)

    note("online_results=%r" % online_results)
    note("offline_results=%r" % offline_results)

    assert_allclose(online_results, offline_results, rtol=1e-10, atol=ALLOWED_TOLERANCE, equal_nan=True, verbose=True)

def check_serialization(input_values, online_indicator_class, point_to_cut, *args, **kwargs):
    input_values = pd.Series(input_values)
    online_indicator = online_indicator_class("SYM", *args, **kwargs)
    full_online_results = pd.Series(map(online_indicator.update, input_values))

    online_indicator_serialization = online_indicator_class("SYM", *args, **kwargs)
    deserialized_indicator = cPickle.loads(cPickle.dumps(online_indicator_serialization))
    online_results_serialization_part1 = pd.Series(map(deserialized_indicator.update, input_values[0:point_to_cut]))
    online_results_serialization_part2 = pd.Series(map(deserialized_indicator.update, input_values[point_to_cut:]))
    full_serialization_results = pd.concat([online_results_serialization_part1, online_results_serialization_part2], ignore_index=True)

    note("online_results=%r" % full_serialization_results)
    note("online_results_part1=%r" % online_results_serialization_part1)
    note("online_results_part2=%r" % online_results_serialization_part2)

    assert_allclose(full_online_results, full_serialization_results, rtol=1E-6, atol=ALLOWED_TOLERANCE, equal_nan=True, verbose=True)



@given(
    input_values=DEFAULT_PRICE_STRATEGY,
    window=DEFAULT_WINDOW_SIZE_STRATEGY,
)
def test_online_sma(input_values, window):
    compare_with_offline(input_values, OnlineSMA, partial(MA, type="simple"), window)


@given(
    input_values=DEFAULT_PRICE_STRATEGY,
    window=DEFAULT_WINDOW_SIZE_STRATEGY,
    adjust = booleans()
)
def test_online_ewma(input_values, window, adjust):
    compare_with_offline(input_values, OnlineEWMA, partial(MA, type="ewma"), window, adjust=adjust)

@pytest.mark.skip()
@given(
    input_values=DEFAULT_PRICE_STRATEGY,
    window=DEFAULT_WINDOW_SIZE_STRATEGY
)
def test_online_rsi(input_values, window):
    compare_with_offline(input_values, OnlineRSI, rsi, window)


@pytest.mark.skip()
@given(
    input_values=DEFAULT_PRICE_STRATEGY,
    offset=DEFAULT_WINDOW_SIZE_STRATEGY,
    lambda1=DEFAULT_WINDOW_SIZE_STRATEGY,
    lambda2=DEFAULT_WINDOW_SIZE_STRATEGY
)
def test_online_kama(input_values, offset, lambda1, lambda2):
    compare_with_offline(input_values, OnlineKAMA, KaufmanMA, offset, lambda1, lambda2)


@given(
    input_values=DEFAULT_PRICE_STRATEGY,
    nslow=DEFAULT_WINDOW_SIZE_STRATEGY,
    nfast=DEFAULT_WINDOW_SIZE_STRATEGY
)
def test_online_macd(input_values, nslow, nfast):
    compare_with_offline(input_values, OnlineMACD, macd, nslow, nfast)


@given(
    input_values=DEFAULT_PRICE_STRATEGY
)
def test_online_oscillator(input_values):
    compare_with_offline(input_values, OnlineOscillator, price_oscillator)


@given(
    input_values=DEFAULT_PRICE_STRATEGY,
    offset=DEFAULT_WINDOW_SIZE_STRATEGY
)
def test_online_roc(input_values, offset):
    compare_with_offline(input_values, OnlineROC, rate_of_change, offset)


@given(
    input_values=DEFAULT_PRICE_STRATEGY,
    offset=DEFAULT_WINDOW_SIZE_STRATEGY
)
def test_online_momentum(input_values, offset):
    compare_with_offline(input_values, OnlineMomentum, momentum, offset)


@given(
    input_values=DEFAULT_PRICE_STRATEGY,
    offset=DEFAULT_WINDOW_SIZE_STRATEGY
)
def test_online_disparity(input_values, offset):
    compare_with_offline(input_values, OnlineDisparity, disparity, offset)


@given(
    input_values=DEFAULT_PRICE_STRATEGY,
    offset=DEFAULT_WINDOW_SIZE_STRATEGY
)
def test_online_stoch_k(input_values, offset):
    compare_with_offline(input_values, OnlineStochasticK, stoch_k, offset)


@given(
    input_values=DEFAULT_PRICE_STRATEGY,
    offset=DEFAULT_WINDOW_SIZE_STRATEGY,
    window=DEFAULT_WINDOW_SIZE_STRATEGY)
def test_online_stoch_d(input_values, offset, window):
    compare_with_offline(input_values, OnlineStochasticD, stoch_d, offset, window)


@given(
    input_values=DEFAULT_PRICE_STRATEGY,
    offset=DEFAULT_WINDOW_SIZE_STRATEGY,
    window1=DEFAULT_WINDOW_SIZE_STRATEGY,
    window2=DEFAULT_WINDOW_SIZE_STRATEGY
)
def test_online_slow_stoch_d(input_values, offset, window1, window2):
    compare_with_offline(input_values, OnlineSlowStochasticD, slow_stoch_d, offset, window1, window2)


@given(
    input_values=DEFAULT_PRICE_STRATEGY,
    nslow=DEFAULT_WINDOW_SIZE_STRATEGY,
    nfast=DEFAULT_WINDOW_SIZE_STRATEGY,
    offset=DEFAULT_WINDOW_SIZE_STRATEGY
)
def test_online_tsi(input_values, nslow, nfast, offset):
    compare_with_offline(input_values, OnlineTSI, tsi, nfast, nslow, offset)




@given(
    input_values=DEFAULT_RETURNS_STRATEGY,
    window=DEFAULT_WINDOW_VARIANCE_AND_JUMPS
)

def test_online_realized_bipower_var(input_values, window):
    compare_with_offline(input_values, OnlineRealizedBipowerVariance,  realized_bipower_var,
                         DEFAULT_DELTA_T_VARIANCE_AND_JUMPS, window)


@given(
    input_values=DEFAULT_RETURNS_STRATEGY,
    window=DEFAULT_WINDOW_VARIANCE_AND_JUMPS
)
def test_online_truncated_power_var(input_values, window):
    compare_with_offline(input_values, OnlineTruncatedPowerVariance,  truncated_power_var,
                         DEFAULT_DELTA_T_VARIANCE_AND_JUMPS, window, DEFAULT_G)

@given(
    input_values=DEFAULT_RETURNS_STRATEGY,
    window=DEFAULT_WINDOW_VARIANCE_AND_JUMPS,
    jump_return_criteria=sampled_from(["realized_bipower", "truncated_power", "either", "both"]),
    alpha=floats(min_value=0.01, max_value=0.1, allow_nan=False, allow_infinity=False)
)
def test_lee_jump_detection(input_values, window, jump_return_criteria, alpha):
    compare_with_offline(input_values, OnlineLeeJumpDetection,  lee_jump_detection,
                         DEFAULT_DELTA_T_VARIANCE_AND_JUMPS, window, DEFAULT_G, jump_return_criteria, alpha)




@given(
    input_values=DEFAULT_PRICE_STRATEGY,
    window=DEFAULT_WINDOW_SIZE_STRATEGY,
    point_to_cut = integers(1, NELEMENTS_FOR_RETURN_TESTING-1)
)
def test_serialization_rsi(input_values, window, point_to_cut):
    check_serialization(input_values, OnlineRSI, point_to_cut, window)


@given(
    input_values=DEFAULT_RETURNS_STRATEGY,
    window=DEFAULT_WINDOW_VARIANCE_AND_JUMPS,
    jump_return_criteria=sampled_from(["realized_bipower", "truncated_power", "either", "both"]),
    alpha=floats(min_value=0.01, max_value=0.1, allow_nan=False, allow_infinity=False),
    point_to_cut = integers(1, NELEMENTS_FOR_RETURN_TESTING-1)
)
def test_serialization_lee_jump_detection(input_values, window, jump_return_criteria, alpha, point_to_cut):
    check_serialization(input_values, OnlineLeeJumpDetection, point_to_cut,
                        delta_t=DEFAULT_DELTA_T_VARIANCE_AND_JUMPS, window=window, g=DEFAULT_G,
                        jump_return_criteria=jump_return_criteria, alpha=alpha)


