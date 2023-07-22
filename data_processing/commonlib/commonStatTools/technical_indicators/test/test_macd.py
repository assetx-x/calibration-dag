#!/usr/bin/env python
# coding:utf-8
import os
import sys
import re
import pandas as pd
import numpy.testing as npt
import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from commonlib.commonStatTools.technical_indicators.offline import macd

input_dir = os.path.join(os.path.dirname(__file__), "input_data")
# So "test_macd.py" turns into "macd"...
NameRE = re.compile(r"^test_(\w+)\.\w+")
this_test = NameRE.match(os.path.basename(__file__)).group(1)
expected_data_filename = os.path.join(input_dir, "{}.csv".format(this_test))

@pytest.fixture()
def price_data():
    ts = pd.read_csv(os.path.join(input_dir, "test2.csv"), parse_dates=['Date'], index_col='Date')
    ts.sort_index(ascending=True, inplace=True)
    return ts

@pytest.fixture()
def expected_data():
    ts = pd.read_csv(expected_data_filename,
                       parse_dates=['Date'], index_col='Date')
    ts.sort_index(ascending=True, inplace=True)
    return ts

def test_macd(price_data, expected_data):
    results = macd(price_data["Adj Close"])
    npt.assert_array_almost_equal(results, expected_data[this_test], 5)

if __name__ == '__main__':
    results = globals()[this_test](price_data()["Adj Close"])
    results.name = this_test
    print("Saving expected data results to {} ...".format(expected_data_filename))
    print(results)
    results.to_csv(expected_data_filename, header=True)