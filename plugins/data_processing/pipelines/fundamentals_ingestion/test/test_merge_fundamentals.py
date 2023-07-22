from datetime import datetime, date

import pandas as pd
import pytest
from numpy import nan
from pandas.util.testing import assert_frame_equal

from pipelines.fundamentals_ingestion.merge_fundamentals import MergeFundamentalsTask


@pytest.fixture
def merge_task():
    return MergeFundamentalsTask(date=datetime(2017, 7, 21))


# noinspection PyProtectedMember,PyShadowingNames
def test_get_records_to_analyze_should_merge_left(merge_task):
    existing_data = pd.DataFrame(
        [("m1", "t1", date(2017, 7, 1), 3.14, datetime(2017, 7, 1), None, "3.14"),  # expect to stay as is
         ("m2", "t2", date(2017, 7, 1), 3.15, datetime(2017, 7, 1), None, "3.15"),  # expect values to change
         ("m3", "t3", date(2017, 4, 1), 3.16, datetime(2017, 7, 1), None, "3.16"),  # expect new empty values
         ("m4", "t4", date(2017, 4, 1), 3.16, datetime(2017, 7, 1), None, "3.16")],  # expect to go away - no match
        columns=["metric", "ticker", "date", "value", "as_of_start", "as_of_end", "raw_value"])
    input_data = pd.DataFrame(
        [("m1", "t1", date(2017, 7, 1), 3.14, datetime(2017, 7, 21), None, "3.14"),  # expect to match existing
         ("m2", "t2", date(2017, 7, 1), 3.25, datetime(2017, 7, 21), None, "3.25"),  # expect to update existing
         ("m3", "t3", date(2017, 4, 1), None, datetime(2017, 7, 21), None, None),  # expect to update existing
         ("m7", "t7", date(2017, 1, 1), 3.18, datetime(2017, 7, 21), None, "3.18")],  # expect to go away - no match
        columns=["metric", "ticker", "date", "value", "as_of_start", "as_of_end", "raw_value"])

    expected = pd.DataFrame(
        [("m1", "t1", date(2017, 7, 1), 3.14, dt(2017, 7, 1), None, "3.14", 3.14, dt(2017, 7, 21), None, "3.14"),
         ("m2", "t2", date(2017, 7, 1), 3.15, dt(2017, 7, 1), None, "3.15", 3.25, dt(2017, 7, 21), None, "3.25"),
         ("m3", "t3", date(2017, 4, 1), 3.16, dt(2017, 7, 1), None, "3.16", None, dt(2017, 7, 21), None, None)],
        columns=["metric", "ticker", "date", "value_old", "as_of_start_old", "as_of_end_old", "raw_value_old",
                 "value_new", "as_of_start_new", "as_of_end_new", "raw_value_new"])

    actual = merge_task._get_records_to_analyze(input_data, existing_data)

    assert_frame_equal(expected, actual)


# noinspection PyProtectedMember,PyShadowingNames
def test_get_records_to_update(merge_task):
    records_to_analyze = pd.DataFrame(
        # skip, value didn't change
        [("m1", "t1", date(2017, 7, 1), 3.14, dt(2017, 7, 1), None, "3.14", 3.14, dt(2017, 7, 21), None, "3.14"),
         # update, value has changed
         ("m2", "t2", date(2017, 7, 1), 3.15, dt(2017, 7, 1), None, "3.15", 3.25, dt(2017, 7, 21), None, "3.25"),
         # update, value increased by more than epsilon
         ("m3", "t3", date(2017, 7, 1), 3.1489, dt(2017, 7, 1), None, "3.14", 3.150, dt(2017, 7, 21), None, "3.15"),
         # skip, value has changed by less than epsilon
         ("m4", "t4", date(2017, 7, 1), 3.160001, dt(2017, 7, 1), None, "3.16", 3.16, dt(2017, 7, 21), None, "3.16"),
         # update, value decreased by more than epsilon
         ("m5", "t5", date(2017, 7, 1), 3.17, dt(2017, 7, 1), None, "3.17", 3.165, dt(2017, 7, 21), None, "3.165"),
         # skip, the new value is None
         ("m6", "t6", date(2017, 7, 1), 3.18, dt(2017, 7, 1), None, "3.18", None, dt(2017, 7, 21), None, None),
         # skip, the new value is nan
         ("m7", "t7", date(2017, 7, 1), 3.19, dt(2017, 7, 1), None, "3.19", nan, dt(2017, 7, 21), None, None),
         # update, the old value was nan
         ("m8", "t8", date(2017, 4, 1), None, dt(2017, 7, 1), None, None, 3.20, dt(2017, 7, 21), None, "3.20")],
        columns=["metric", "ticker", "date", "value_old", "as_of_start_old", "as_of_end_old", "raw_value_old",
                 "value_new", "as_of_start_new", "as_of_end_new", "raw_value_new"])

    expected = pd.DataFrame(
        [("m2", "t2", date(2017, 7, 1), 3.15, dt(2017, 7, 1), None, "3.15", 3.25, dt(2017, 7, 21), None, "3.25"),
         ("m3", "t3", date(2017, 7, 1), 3.1489, dt(2017, 7, 1), None, "3.14", 3.150, dt(2017, 7, 21), None, "3.15"),
         ("m5", "t5", date(2017, 7, 1), 3.17, dt(2017, 7, 1), None, "3.17", 3.165, dt(2017, 7, 21), None, "3.165"),
         ("m8", "t8", date(2017, 4, 1), None, dt(2017, 7, 1), None, None, 3.20, dt(2017, 7, 21), None, "3.20")],
        columns=["metric", "ticker", "date", "value_old", "as_of_start_old", "as_of_end_old", "raw_value_old",
                 "value_new", "as_of_start_new", "as_of_end_new", "raw_value_new"])

    actual = merge_task._get_records_to_update(records_to_analyze).reset_index(drop=True)

    assert_frame_equal(actual, expected)


# noinspection PyProtectedMember,PyShadowingNames
def test_get_records_to_retry(merge_task):
    records_to_analyze = pd.DataFrame(
        # skip, was missing, still missing
        [("m1", "t1", date(2017, 7, 1), None, dt(2017, 7, 1), None, None, None, dt(2017, 7, 21), None,None),
         # skip, was missing, now got a values
         ("m2", "t2", date(2017, 7, 1), None, dt(2017, 7, 1), None, None, 3.25, dt(2017, 7, 21), None, "3.25"),
         # skip, had a value, still have one
         ("m3", "t3", date(2017, 7, 1), 3.14, dt(2017, 7, 1), None, "3.14", 3.15, dt(2017, 7, 21), None, "3.15"),
         # retry, had a value, not anymore
         ("m4", "t4", date(2017, 7, 1), 3.16, dt(2017, 7, 1), None, "3.16", nan, dt(2017, 7, 21), None, None)],
        columns=["metric", "ticker", "date", "value_old", "as_of_start_old", "as_of_end_old", "raw_value_old",
                 "value_new", "as_of_start_new", "as_of_end_new", "raw_value_new"])

    expected = pd.DataFrame(
        [("m4", "t4", date(2017, 7, 1), 3.16, dt(2017, 7, 1), None, "3.16", nan, dt(2017, 7, 21), None, None)],
        columns=["metric", "ticker", "date", "value_old", "as_of_start_old", "as_of_end_old", "raw_value_old",
                 "value_new", "as_of_start_new", "as_of_end_new", "raw_value_new"])

    actual = merge_task._get_records_to_retry(records_to_analyze).reset_index(drop=True)

    assert_frame_equal(actual, expected)


# noinspection PyProtectedMember,PyShadowingNames
def test_get_records_to_insert(merge_task):
    existing_data = pd.DataFrame(
        [("m1", "t1", date(2017, 4, 1), 3.14, datetime(2017, 7, 1), None, "3.14"),
         ("m2", "t2", date(2017, 4, 1), 3.15, datetime(2017, 7, 1), None, "3.15"),
         ("m3", "t3", date(2017, 1, 1), 3.16, datetime(2017, 7, 1), None, "3.16"),
         ("m4", "t4", date(2017, 1, 1), 3.16, datetime(2017, 7, 1), None, "3.16")],
        columns=["metric", "ticker", "date", "value", "as_of_start", "as_of_end", "raw_value"])
    input_data = pd.DataFrame(
        [("m1", "t1", date(2017, 7, 1), 3.14, datetime(2017, 7, 21), None, "3.14"),  # new date
         ("m2", "t2", date(2017, 4, 1), 3.16, datetime(2017, 7, 21), None, "3.16"),  # skip, updated value
         ("m3", "t4", date(2017, 1, 1), 3.17, datetime(2017, 7, 21), None, "3.17"),  # new ticker for the metric
         ("m4", "t1", date(2017, 1, 1), 3.18, datetime(2017, 7, 21), None, "3.18")],  # new metric for the ticker
        columns=["metric", "ticker", "date", "value", "as_of_start", "as_of_end", "raw_value"])

    expected = pd.DataFrame(
        [("m0", "t0", date(2017, 7, 1), 1.1, dt(2017, 1, 1), "", "1.1", 3.14, dt(2017, 7, 21), None, "3.14"),
         ("m1", "t1", date(2017, 7, 1), nan, nan, nan, None, 3.14, dt(2017, 7, 21), None, "3.14"),
         ("m3", "t4", date(2017, 1, 1), nan, nan, nan, None, 3.17, dt(2017, 7, 21), None, "3.17"),
         ("m4", "t1", date(2017, 1, 1), nan, nan, nan, None, 3.18, dt(2017, 7, 21), None, "3.18")],
        columns=["metric", "ticker", "date", "value_old", "as_of_start_old", "as_of_end_old", "raw_value_old",
                 "value_new", "as_of_start_new", "as_of_end_new", "raw_value_new"])
    # added element 0 as an easy way to force column data types
    # need to get rid of it now
    expected = expected.iloc[1:].reset_index(drop=True)

    actual = merge_task._get_records_to_insert(input_data, existing_data).reset_index(drop=True)

    assert_frame_equal(expected, actual)


def dt(year, month, day):
    return datetime(year, month, day)
