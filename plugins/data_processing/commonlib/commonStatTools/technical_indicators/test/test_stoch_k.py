import pytest
import pandas as pd
import sys, os
import re
import numpy.testing as npt

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from commonlib.commonStatTools.technical_indicators.offline import stoch_k


input_dir = os.path.join(os.path.dirname(__file__), "input_data")
NameRE = re.compile(r"^test_(\w+)\.\w+")
this_test = NameRE.match(os.path.basename(__file__)).group(1)


Filenames = [
    ({"input_data": "test1.csv",
      "file_expected": "{}1.csv".format(this_test)}),
    ({"input_data": "test2missing.csv",
      "file_expected": "{}2.csv".format(this_test)}),
    ({"input_data": "test2missing.csv",
      "file_expected": "{}3.csv".format(this_test)}),
]


Config_data = [
    (Filenames[0]["file_expected"], Filenames[0]["input_data"], 14),
    (Filenames[1]["file_expected"], Filenames[1]["input_data"], 5),
    (Filenames[2]["file_expected"], Filenames[2]["input_data"], 20),
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


@pytest.mark.parametrize("file_expected, input_data, off_value", Config_data)
def test_stoch_k(file_expected, input_data, off_value):
    """
    Args
    ----
       file_expected: str
          Name of file with the expected result
       input_data: str
          Name of file with the input data
    """
    ts = series_data(input_data)
    actual = stoch_k(ts, offset=off_value)
    if __name__ == "__main__":
        outfile = os.path.join(input_dir, file_expected)
        print("Saving expected data results to {} ...".format(outfile))
        print(actual)
        actual.to_csv(outfile, header=True)
    expected = expected_data(file_expected)
    npt.assert_array_almost_equal(actual, expected, 5)


if __name__ == "__main__":
    for file_expected, input_data, off_value in Config_data:
        test_stoch_k(file_expected, input_data, off_value)
