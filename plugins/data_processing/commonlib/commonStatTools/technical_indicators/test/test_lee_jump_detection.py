import pytest
import pandas as pd
import sys, os
import re
import numpy.testing as npt

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from commonlib.commonStatTools.technical_indicators.offline import lee_jump_detection

input_dir = os.path.join(os.path.dirname(__file__), "input_data")
NameRE = re.compile(r"^test_(\w+)\.\w+")
this_test = NameRE.match(os.path.basename(__file__)).group(1)


Filenames = [
    ({"input_data": "test_var_jump.csv",
      "file_expected": "{}1.csv".format(this_test)}),
    ({"input_data": "test_var_jump.csv",
     "file_expected":  "{}2.csv".format(this_test)}),
    ({"input_data": "test_var_jump.csv",
     "file_expected":  "{}3.csv".format(this_test)}),
    ({"input_data": "test_var_jump.csv",
     "file_expected":  "{}4.csv".format(this_test)})
]

delta_t_daily = 1.0/252.0

Config_data = [
    (Filenames[0]["file_expected"], Filenames[0]["input_data"], delta_t_daily, 200, 1.15, "realized_bipower"),
    (Filenames[1]["file_expected"], Filenames[1]["input_data"], delta_t_daily, 200, 1.15, "truncated_power"),
    (Filenames[2]["file_expected"], Filenames[2]["input_data"], delta_t_daily, 200, 1.15, "either"),
    (Filenames[3]["file_expected"], Filenames[3]["input_data"], delta_t_daily, 200, 1.15, "both"),
]


@pytest.fixture
def series_data(input_data):
    ts = pd.read_csv(os.path.join(input_dir, input_data), parse_dates=["Date"])
    ts.set_index("Date", inplace=True)
    ts.sort_index(ascending=True, inplace=True)
    return ts.iloc[:,0]


@pytest.fixture
def expected_data(file_expected):
    expected = pd.read_csv(os.path.join(input_dir, file_expected), parse_dates=["Date"])
    expected.set_index(pd.to_datetime(expected["Date"]), inplace=True)
    expected.sort_index(ascending=True, inplace=True)
    expected.drop(["Date"], axis=1, inplace=True)
    return expected.iloc[:,0]


@pytest.mark.parametrize("file_expected, input_data, delta_t, window, g, jump_return_criteria", Config_data)
def test_lee_jump_detection(file_expected, input_data, delta_t, window, g, jump_return_criteria):
    """
    Args
    ----
       file_expected: str
          Name of file with the expected result
       input_data: str
          Name of file with the input data
    """
    ts = series_data(input_data)
    actual = lee_jump_detection(ts, delta_t, window, g, jump_return_criteria)
    if __name__ == "__main__":
        outfile = os.path.join(input_dir, file_expected)
        print("Saving expected data results to {} ...".format(outfile))
        print(actual)
        actual.to_csv(outfile, header=True)
    expected = expected_data(file_expected)
    npt.assert_array_almost_equal(actual, expected, 5)

def test_consistency_between_jump_return_criteria():
    ts = series_data(Filenames[0]["input_data"])
    results = {}
    for jump_return_criteria in ["realized_bipower", "truncated_power", "either", "both"]:
        jump_data = lee_jump_detection(ts, delta_t_daily, 800, 1.15, jump_return_criteria)
        results[jump_return_criteria] = jump_data[jump_data!=0].index
    set_1 = set(results["realized_bipower"]).union(set(results["truncated_power"]))
    set_2 = set(results["realized_bipower"]).intersection(set(results["truncated_power"]))
    assert set_1.symmetric_difference(set_2)
    assert len(set(results["either"]).symmetric_difference(set(results["realized_bipower"])
                                                           .union(set(results["truncated_power"])))) == 0
    assert len(set(results["both"]).symmetric_difference(set(results["realized_bipower"])
                                                         .intersection(set(results["truncated_power"])))) == 0


if __name__ == "__main__":
    for file_expected, input_data, delta_t, window_size, g, jump_return_criteria in Config_data:
        test_lee_jump_detection(file_expected, input_data, delta_t, window_size, g, jump_return_criteria)
    test_consistency_between_jump_return_criteria()
