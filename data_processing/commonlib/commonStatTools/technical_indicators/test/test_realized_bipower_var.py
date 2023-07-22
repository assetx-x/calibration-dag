import pytest
import pandas as pd
import sys, os
import re
import numpy.testing as npt

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from commonlib.commonStatTools.technical_indicators.offline import realized_bipower_var

input_dir = os.path.join(os.path.dirname(__file__), "input_data")
NameRE = re.compile(r"^test_(\w+)\.\w+")
this_test = NameRE.match(os.path.basename(__file__)).group(1)


Filenames = [
    ({"input_data": "test_var_jump.csv",
      "file_expected": "{}1.csv".format(this_test)}),
    ({"input_data": "test_var_jump.csv",
     "file_expected":  "{}2.csv".format(this_test)})
]

delta_t_daily = 1.0/252.0

Config_data = [
    (Filenames[0]["file_expected"], Filenames[0]["input_data"], delta_t_daily, 200),
    (Filenames[1]["file_expected"], Filenames[1]["input_data"], delta_t_daily, 100),
]


@pytest.fixture
def series_data(input_data):
    ts = pd.read_csv(os.path.join(input_dir, input_data), parse_dates=["Date"])
    ts.set_index("Date", inplace=True)
    ts.sort_index(ascending=True, inplace=True)
    return ts


@pytest.fixture
def expected_data(file_expected):
    expected = pd.read_csv(os.path.join(input_dir, file_expected), parse_dates=["Date"])
    expected.set_index(pd.to_datetime(expected["Date"]), inplace=True)
    expected.sort_index(ascending=True, inplace=True)
    expected.drop(["Date"], axis=1, inplace=True)
    return expected


@pytest.mark.parametrize("file_expected, input_data, delta_t, window", Config_data)
def test_realized_bipower_var(file_expected, input_data, delta_t, window):
    """
    Args
    ----
       file_expected: str
          Name of file with the expected result
       input_data: str
          Name of file with the input data
    """
    ts = series_data(input_data)
    actual = realized_bipower_var(ts, delta_t, window)
    if __name__ == "__main__":
        outfile = os.path.join(input_dir, file_expected)
        print("Saving expected data results to {} ...".format(outfile))
        print(actual)
        actual.to_csv(outfile, header=True)
    expected = expected_data(file_expected)
    npt.assert_array_almost_equal(actual, expected, 5)

if __name__ == "__main__":
    for file_expected, input_data, delta_t, window_size in Config_data:
        test_realized_bipower_var(file_expected, input_data, delta_t, window_size)
