import pytest
import pandas as pd
import sys, os
import numpy.testing as npt
import re

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from commonlib.commonStatTools.technical_indicators.offline import MA

input_dir = os.path.join(os.path.dirname(__file__), "input_data")
NameRE = re.compile(r"^test_(\w+)\.\w+")
this_test = NameRE.match(os.path.basename(__file__)).group(1)

Filenames = [
    ({"input_data": "test1.csv",
      "file_expected": "actual1.csv"}),
    ({"input_data": "test1.csv",
      "file_expected": "actual2.csv"}),
    ({"input_data": "test2.csv",
      "file_expected": "actual21.csv"}),
    ({"input_data": "test1.csv",
      "file_expected": "actual22.csv"}),
    ({"input_data": "test2missing.csv",
      "file_expected": "actual23.csv"}),
]


Config_data = [
    (Filenames[0]["file_expected"], Filenames[0]["input_data"], 5, "simple"),
    (Filenames[1]["file_expected"], Filenames[1]["input_data"], 3, "simple"),
    (Filenames[2]["file_expected"], Filenames[2]["input_data"], 20, None),
    (Filenames[3]["file_expected"], Filenames[3]["input_data"], 10, "ewma"),
    (Filenames[4]["file_expected"], Filenames[4]["input_data"], 14, None),
]



@pytest.fixture
def series_data(input_data):
    ts = pd.read_csv(os.path.join(input_dir, input_data), parse_dates=["Date"])
    ts.set_index(pd.to_datetime(ts["Date"]), inplace=True)
    ts.sort_index(ascending=True, inplace=True)
    ts.drop(["Date"], axis=1, inplace=True)
    return ts


@pytest.fixture
def expected_data(file_expected):
    expected = pd.read_csv(os.path.join(input_dir, file_expected), parse_dates=["Date"])
    expected.set_index(pd.to_datetime(expected["Date"]), inplace=True)
    expected.sort_index(ascending=True, inplace=True)
    expected.drop(["Date"], axis=1, inplace=True)
    return expected



@pytest.mark.parametrize("file_expected, input_data, window_size, method", Config_data)
def test_moving_average(file_expected, input_data, window_size, method):
    """
    Args
    ----
       file_expected: str
          Name of file with the expected result
       input_data: str
          Name of file with the input data
       window : int
          Minimum number of observations in window required to have a value.
    """
    ts = series_data(input_data)
    actual = MA(ts, window_size, type=method)
    if __name__ == "__main__":
        outfile = os.path.join(input_dir, file_expected)
        print("Saving expected data results to {} ...".format(outfile))
        print(actual)
        actual.to_csv(outfile, header=True)
    expected = expected_data(file_expected)
    npt.assert_array_almost_equal(actual, expected, 5)


if __name__ == "__main__":
    for file_expected, input_data, window_size, method in Config_data:
        test_moving_average(file_expected, input_data, window_size, method)
