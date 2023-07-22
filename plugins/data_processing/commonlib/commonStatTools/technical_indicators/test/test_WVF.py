import pytest
import pandas as pd
import sys, os
import re
import numpy.testing as npt

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from commonlib.commonStatTools.technical_indicators.offline import compute_WVF

input_dir = os.path.join(os.path.dirname(__file__), "input_data")

Filenames = [
    ({"input_data"   : "test_WFV_VXX.csv",
      "file_expected": "expected_WVF_lookback_20_days.csv"}),
    ({"input_data"   : "test_WFV_VXX.csv",
      "file_expected": "expected_WVF_lookback_40_days.csv"}),
    ({"input_data": "test_WFV_VXX_missing.csv",
      "file_expected": "expected_WVF_lookback_20_days_missing.csv"})
]

Config_data = [
    (Filenames[0]["file_expected"], Filenames[0]["input_data"], 21),
    (Filenames[1]["file_expected"], Filenames[1]["input_data"], 41),
    (Filenames[2]["file_expected"], Filenames[2]["input_data"], 21)
]

@pytest.fixture
def series_data(input_data):
    ts = pd.read_csv(os.path.join(input_dir, input_data), parse_dates=["cob"])
    ts.set_index("cob", inplace=True)
    return ts

@pytest.fixture
def expected_data(file_expected):
    expected = pd.read_csv(os.path.join(input_dir, file_expected), parse_dates=["cob"])
    expected.set_index("cob", inplace=True)
    return expected.iloc[:,0]

@pytest.mark.parametrize("file_expected, input_data, window_size", Config_data)
def test_WVF(file_expected, input_data, window_size):
    ts = series_data(input_data)
    actual = compute_WVF(ts, window_size)
    expected = expected_data(file_expected)
    npt.assert_array_almost_equal(actual, expected, 4)

def test_WVF_online():
    pass
