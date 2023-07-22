import os
import boto3
import pytest
import pandas as pd

from io import StringIO
from pipelines.prices_ingestion import get_config
from etl_workflow_steps import EquitiesETLTaskParameters
from conftest import DummyDataHolder, random_price_data, patched_config, \
    setup_s3, create_s3_bucket_and_put_object
from pipelines.prices_ingestion.data_retrieval_workflow_steps import TICKDATA_CORPORATE_ACTION_LABEL
from pipelines.prices_ingestion.adjustment_workflow_steps import DepositAdjustmentFactorsIntoS3, \
    S3_ADJ_FACTORS_LOCATION_LABEL, DATA_TICKDATA_PRICE_HANDLER_LABEL

path_to_test_dir = os.path.join(os.path.dirname(__file__), "testing_aux_files")


# -------------------------------------------------------------------
# DepositAdjustmentFactorsIntoS3
# -------------------------------------------------------------------

test_data = [
    ("EGO", {
        "start_date": pd.Timestamp("2018-12-17"),
        "end_date": pd.Timestamp("2018-12-28"),
        "run_date": pd.Timestamp("2018-12-28"),
        "as_of_date": pd.Timestamp("2018-12-31 09:00:00"),
        "ca_file": os.path.join(path_to_test_dir, "s3TickDataNormalizedCA", "EGO_two_splits_on_same_day.csv")})
]


@pytest.mark.parametrize("ticker, data_config", test_data)
def test_DepositAdjustmentFactorsIntoS3(ticker, data_config, patched_config, setup_s3, mock):
    start_date = data_config["start_date"]
    end_date = data_config["end_date"]
    run_date = data_config["run_date"]
    as_of_date = data_config["as_of_date"]

    # Creating mocked s3 test bucket
    bucket_id = get_config()["raw_data_pull"]["base_data_bucket"]
    mock_storage = setup_s3
    mock_storage.setup_bucket(bucket_id)
    mock.patch("pipelines.prices_ingestion.etl_workflow_aux_functions.storage.Client", return_value=mock_storage)

    # Creating artificial split_adjustment_factor, full_adjustment_factor pandas Series and reading CA file
    date1 = (run_date - pd.Timedelta("4 days"))
    date2 = (run_date - pd.Timedelta("10 days"))
    full_adjustment_factor = pd.Series([2.17, 2.42], name="Adjustment Factor", index=[date1, date2])
    split_adjustment_factor = pd.Series([1.17, 1.42], name="Adjustment Factor", index=[date1, date2])
    ca_data_normalized = pd.read_csv(data_config["ca_file"], index_col=["ACTION", "ACTION DATE"],
                                     parse_dates=["ACTION DATE"]).sort_index()

    # Mocking CorporateActionsHolder
    ca_data = lambda: None
    ca_data.get_ca_data_normalized = lambda: ca_data_normalized
    ca_data.get_full_adjustment_factor = lambda: full_adjustment_factor
    ca_data.get_split_adjustment_factor = lambda: split_adjustment_factor

    # Mocking EquityDataHolder
    equity_prices = random_price_data(start_date, end_date)  # artificial equity prices data for EquityDataHolder
    price_dh = DummyDataHolder(ticker, start_date=start_date, end_date=end_date, as_of_date=as_of_date, df=equity_prices)
    data_holder = lambda: None
    data_holder.get_data_holder = lambda: price_dh

    # Call Function
    data_deposit = DepositAdjustmentFactorsIntoS3()
    data_deposit.task_params = EquitiesETLTaskParameters(ticker=ticker, as_of_dt=as_of_date, run_dt=run_date,
                                                         start_dt=start_date, end_dt=end_date)
    kwargs = {TICKDATA_CORPORATE_ACTION_LABEL: ca_data, DATA_TICKDATA_PRICE_HANDLER_LABEL: data_holder}
    data_deposit.do_step_action(**kwargs)

    # Test results
    result_url = data_deposit.step_status[S3_ADJ_FACTORS_LOCATION_LABEL]
    bucket, key = result_url[5:].split("/", 1)
    mock_storage.assert_exists(bucket, key)
    test_adj_factor_df = pd.read_csv(StringIO(mock_storage.bucket(bucket).blob(key).body))

    # Only dates of corporate actions are passed as inputs, but the result is expected to have time set to 4 pm
    date1 = (date1 + pd.Timedelta("16 hours")).tz_localize(tz="US/Eastern").tz_convert("UTC")
    date2 = (date2 + pd.Timedelta("16 hours")).tz_localize(tz="US/Eastern").tz_convert("UTC")

    assert bucket == bucket_id
    assert key.startswith("adjustment/factors/location_test/%s" % ticker)
    assert (test_adj_factor_df["cob"] == [str(date1), str(date2)]).all()
    assert (test_adj_factor_df["split_factor"] == split_adjustment_factor.reset_index(drop=True)).all()
    assert (test_adj_factor_df["split_div_factor"] == full_adjustment_factor.reset_index(drop=True)).all()
