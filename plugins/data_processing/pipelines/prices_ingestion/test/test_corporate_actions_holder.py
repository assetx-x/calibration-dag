import os
import pytest
import pandas as pd

from pandas.testing import assert_frame_equal
from pipelines.prices_ingestion.corporate_actions_holder import S3TickDataCorporateActionsHolder

path_to_test_dir = os.path.join(os.path.dirname(__file__), "testing_aux_files", "s3TickDataNormalizedCA")


# -------------------------------------------------------------------
# Test verify_no_duplicates_in_dividends()
# -------------------------------------------------------------------

test_data = [
    (os.path.join(path_to_test_dir, "JPM_two_dividends_on_same_day.csv"), [("2018-10-04", 0.8, 0.993047),
                                                                           ("2019-01-03", 1.6, 0.983955)]),
    (os.path.join(path_to_test_dir, "JPM_two_dividends_and_other_ca_on_same_day.csv"), [("2018-10-04", 1.57, 0.98505),
                                                                                        ("2019-01-03", 1.6, 0.983955)]),
    (os.path.join(path_to_test_dir, "CWENA_missing_adj_factor_in_dividends.csv"), [("2018-05-31", 0.309, 0.982718),
                                                                                   ("2018-08-31", 0.320, 0.984135),
                                                                                   ("2018-11-30", 0.331, pd.np.nan)]),
]


@pytest.mark.parametrize("input_file, expected_dividends", test_data)
def test_verify_no_duplicates_in_dividends(input_file, expected_dividends):
    ca_data_normalized = pd.read_csv(input_file, index_col=["ACTION", "ACTION DATE"]).sort_index()
    test_ca_data_deduplicated = S3TickDataCorporateActionsHolder.verify_no_duplicates_ca(ca_data_normalized)
    mask = (test_ca_data_deduplicated.index.get_level_values("ACTION") == "DIVIDEND")
    test_dividends = test_ca_data_deduplicated.loc[mask, ["GROSS AMOUNT", "ADJUSTMENT FACTOR"]] \
        .reset_index().drop("ACTION", axis=1)
    expected_dividends = pd.DataFrame(expected_dividends, columns=["ACTION DATE", "GROSS AMOUNT", "ADJUSTMENT FACTOR"])
    assert_frame_equal(test_dividends, expected_dividends)


# -------------------------------------------------------------------
# Test verify_no_duplicates_in_splits()
# -------------------------------------------------------------------

test_data = [
    (os.path.join(path_to_test_dir, "EGO_two_splits_on_same_day.csv"), [("2018-12-31", 5.0)]),
    (os.path.join(path_to_test_dir, "IRET_two_splits_and_other_ca_on_same_day.csv"), [("2018-12-28", 10.0)]),
]


@pytest.mark.parametrize("input_file, expected_splits", test_data)
def test_verify_no_duplicates_in_splits(input_file, expected_splits):
    ca_data_normalized = pd.read_csv(input_file, index_col=["ACTION", "ACTION DATE"]).sort_index()
    test_ca_data_deduplicated = S3TickDataCorporateActionsHolder.verify_no_duplicates_ca(ca_data_normalized)
    mask = (test_ca_data_deduplicated.index.get_level_values("ACTION") == "SPLIT")
    test_splits = test_ca_data_deduplicated.loc[mask, "RATIO"].reset_index().drop("ACTION", axis=1)
    expected_splits = pd.DataFrame(expected_splits, columns=["ACTION DATE", "RATIO"])
    assert_frame_equal(test_splits, expected_splits)
