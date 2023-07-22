import os
import pytest
import pandas as pd
import numpy.testing as npt

from datetime import date, time
from pandas.testing import assert_frame_equal
from pipelines.prices_ingestion.config import VolumeLabel, EquityPriceLabels, get_config
from pipelines.prices_ingestion.corporate_actions_holder import S3YahooCorporateActionsHolder
from pipelines.prices_ingestion.equity_data_holders import RedshiftRawTickDataDataHolder, S3YahooEquityDataHolder
from conftest import DummyDataHolder, read_file, create_s3_bucket_and_put_object, prepare_aws_s3_key, patched_config, \
    setup_s3, setup_redshift
from pipelines.prices_ingestion.equity_data_operations import MinuteDataAggregator, VerifyMinuteOutlierForPrice, \
    VerifyMinuteOutlierForVolume, DerivedEquityDataHolder, VerifyTimeline, CompleteMinutes, \
    SplitAndDividendEquityAdjuster, S3YahooPriceAdjuster

path_to_test_dir = os.path.join(os.path.dirname(__file__), "testing_aux_files")


# -------------------------------------------------------------------
# MinuteDataAgregator
# -------------------------------------------------------------------

test_data = [
    ("FB", {
        "start_date": pd.Timestamp("2003-03-26 00:00"),
        "end_date": pd.Timestamp("2016-06-14 23:59:00"),
        "as_of_date": pd.Timestamp("2016-06-15 23:59:00"),
        "input_files_and_keys": [
            (os.path.join(path_to_test_dir, "MinuteDataAggregator", "make_adjustment", "FB_CA_1465948740.csv"),
             "raw_data_pull.TickData.CA.data_location_prefix"),
        ],
        "output_local_file": os.path.join(path_to_test_dir, "MinuteDataAggregator", "make_adjustment",
                                          "FB_expected_aggregated.csv"),
        "raw_equity_price_file": os.path.join(path_to_test_dir, "MinuteDataAggregator", "make_adjustment",
                                              "FB_tickdata_minute.csv"),
        "is_data_adjusted": False}),
    ("XYZ", {
        "start_date": pd.Timestamp("2016-04-19 00:00"),
        "end_date": pd.Timestamp("2016-04-20 23:59:00"),
        "as_of_date": pd.Timestamp("2016-04-21 23:59:00"),
        "input_files_and_keys": [
            (os.path.join(path_to_test_dir, "MinuteDataAggregator", "make_adjustment", "XYZ_CA_1461196740.csv"),
             "raw_data_pull.TickData.CA.data_location_prefix"),
        ],
        "output_local_file": os.path.join(path_to_test_dir, "MinuteDataAggregator", "make_adjustment",
                                          "daily_data_output.csv"),
        "raw_equity_price_file": os.path.join(path_to_test_dir, "MinuteDataAggregator", "make_adjustment",
                                              "input_minute_data.csv"),
        "is_data_adjusted": False})
]

def mock_prepare_single_symbol_sql_query(raw_table, ticker, *timestamps):
    utc_start, utc_end, utc_as_of = timestamps

    """
    !!!DO NOT CHANGE THIS CODE WITHOUT ENSURING THAT THE QA PROCESS IS ALSO CHANGED ACCORDINGLY!!!
    """
    query = """
    SELECT rep.open, rep.high, rep.low, rep.close, rep.volume, rep.cob
    FROM {raw_table} rep
    JOIN (SELECT ticker,cob,max(as_of) as as_of
    FROM {raw_table}
    WHERE ticker='{ticker}' and
          as_of <= '{as_of_date}' and date(cob) BETWEEN '{start_date}' and '{end_date}'
    GROUP BY ticker,cob) l ON
      rep.ticker = l.ticker AND
      rep.cob = l.cob AND
      rep.as_of = l.as_of
    WHERE rep.ticker='{ticker}' and
    rep.as_of <= '{as_of_date}' and date(rep.cob) BETWEEN '{start_date}' and '{end_date}';
    """.format(ticker=ticker,
               raw_table=raw_table,
               start_date=utc_start.date(),
               end_date=utc_end.date(),
               as_of_date=utc_as_of,
               table=raw_table)
    return query

@pytest.fixture
def mock_query_creator(mocker, request):
    mocker.patch('pipelines.prices_ingestion.equity_data_holders.RedshiftRawTickDataDataHolder._prepare_single_symbol_sql_query', side_effect=mock_prepare_single_symbol_sql_query)
    request.addfinalizer(mocker.resetall)

@pytest.mark.parametrize("ticker, data_config", test_data)
def test_MinuteDataAgregator(ticker, data_config, setup_s3, setup_redshift, patched_config, mock, mock_query_creator):
    """
    This test is for MinuteDataAggregator class in price_data_extraction_and_adjustment_tickdata.
    This class aggregates minute equity data and returns a data holder with aggregated data in the same
    data structure for a data holder. It will not verify if the minutes are within 9:31am to 4pm time frame.
    """
    s3_resource = setup_s3  # get s3_resource
    engine = setup_redshift  # get sqlite engine

    bucket_id = get_config()["raw_data_pull"]["base_data_bucket"]
    raw_tickdata_table = get_config()["redshift"]["raw_tickdata_equity_price_table"]

    # Create and populate mocked S3 bucket with corporate actions file
    for path_to_input, data_location_prefix in data_config["input_files_and_keys"]:
        s3_key = prepare_aws_s3_key(ticker, get_config()[data_location_prefix], data_config["end_date"])
        if "TickData.CA" in data_location_prefix:
            body = read_file(path_to_input, header=None, sep="^")
        else:
            body = read_file(path_to_input, header=None, sep=",")
        create_s3_bucket_and_put_object(s3_resource, bucket_id, s3_key, body)

    # Create and populate mocked Redshift table with raw equity prices
    actual_data = pd.read_csv(data_config["raw_equity_price_file"], parse_dates=["cob", "as_of"]).dropna()
    actual_data.to_sql(raw_tickdata_table, engine, index=False)
    mock.patch("pipelines.prices_ingestion.equity_data_holders.RedshiftEquityDataHolderBaseClass.get_engine",
               return_value=engine)
    mock.patch("pipelines.prices_ingestion.equity_data_holders.storage.Client", return_value=s3_resource)

    # Call Function
    minute_data_dh = RedshiftRawTickDataDataHolder(ticker,
                                                   start_date=data_config["start_date"],
                                                   end_date=data_config["end_date"],
                                                   as_of_date=data_config["as_of_date"])
    adjuster = MinuteDataAggregator()
    aggregated_dh = adjuster(minute_data_dh)

    expected_data = pd.read_csv(data_config["output_local_file"], parse_dates=["Date"], index_col=["Date"])
    expected_data.index = expected_data.index.tz_localize("US/Eastern")
    expected_data = expected_data[EquityPriceLabels]

    assert aggregated_dh.is_daily_data() is True
    assert aggregated_dh.is_data_adjusted() == data_config["is_data_adjusted"]
    assert aggregated_dh.get_ticker() == ticker
    assert aggregated_dh.get_data().shape == expected_data.shape
    assert list(expected_data.columns) == EquityPriceLabels
    assert_frame_equal(expected_data, aggregated_dh.get_data(), True)


# -------------------------------------------------------------------
# VerifyMinuteOutlierForPrice
# -------------------------------------------------------------------

test_data = [
    ("X", {
        "case_name": "negative_high",
        "start_date": pd.Timestamp("2001-01-20"),
        "end_date": pd.Timestamp("2016-08-17"),
        "as_of_date": pd.Timestamp("2016-08-18 09:00:00"),
        "input_files_and_keys": [
            (os.path.join(path_to_test_dir, "VerifyMinuteOutlierForPrice", "X_CA_1471507200.csv"),
             "raw_data_pull.TickData.CA.data_location_prefix"),
            (os.path.join(path_to_test_dir, "VerifyMinuteOutlierForPrice", "yahoo_ca.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.ca_data_location_prefix"),
            (os.path.join(path_to_test_dir, "VerifyMinuteOutlierForPrice", "AA_yahoo.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.price_data_location_prefix")
        ],
        "output_local_file": None,
        "raw_equity_price_file": os.path.join(path_to_test_dir, "VerifyMinuteOutlierForPrice",
                                              "AA_minute_data_negative.csv"),
        "expect_exception": True,
        "exception_type": ValueError,
        "debug": False}),
    ("X", {
        "case_name": "low_outlier",
        "start_date": pd.Timestamp("2001-01-20"),
        "end_date": pd.Timestamp("2016-08-17"),
        "as_of_date": pd.Timestamp("2016-08-18 09:00:00"),
        "input_files_and_keys": [
            (os.path.join(path_to_test_dir, "VerifyMinuteOutlierForPrice", "X_CA_1471507200.csv"),
             "raw_data_pull.TickData.CA.data_location_prefix"),
            (os.path.join(path_to_test_dir, "VerifyMinuteOutlierForPrice", "yahoo_ca.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.ca_data_location_prefix"),
            (os.path.join(path_to_test_dir, "VerifyMinuteOutlierForPrice", "AA_yahoo.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.price_data_location_prefix")
        ],
        "output_local_file": os.path.join(path_to_test_dir, "VerifyMinuteOutlierForPrice",
                                          "AA_minute_data_outlier_expected.csv"),
        "raw_equity_price_file": os.path.join(path_to_test_dir, "VerifyMinuteOutlierForPrice",
                                              "AA_minute_data_outlier_lower.csv"),
        "expect_exception": False,
        "exception_type": None,
        "debug": False}),
    ("X", {
        "case_name": "high_outlier",
        "start_date": pd.Timestamp("2001-01-20"),
        "end_date": pd.Timestamp("2016-08-17"),
        "as_of_date": pd.Timestamp("2016-08-18 09:00:00"),
        "input_files_and_keys": [
            (os.path.join(path_to_test_dir, "VerifyMinuteOutlierForPrice", "X_CA_1471507200.csv"),
             "raw_data_pull.TickData.CA.data_location_prefix"),
            (os.path.join(path_to_test_dir, "VerifyMinuteOutlierForPrice", "yahoo_ca.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.ca_data_location_prefix"),
            (os.path.join(path_to_test_dir, "VerifyMinuteOutlierForPrice", "AA_yahoo.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.price_data_location_prefix")
        ],
        "output_local_file": os.path.join(path_to_test_dir, "VerifyMinuteOutlierForPrice",
                                          "AA_minute_data_outlier_expected2.csv"),
        "raw_equity_price_file": os.path.join(path_to_test_dir, "VerifyMinuteOutlierForPrice",
                                              "AA_minute_data_outlier_higher.csv"),
        "expect_exception": False,
        "exception_type": None,
        "debug": True})
]


@pytest.mark.parametrize("ticker, data_config", test_data, ids=[test[1]["case_name"] for test in test_data])
def test_VerifyMinuteOutlierForPrice(ticker, data_config, setup_s3, setup_redshift, patched_config, mock, mock_query_creator):
    """
    Testing should include:

    What happens when there are:
    1. Negative values
    2. Really crazy prices, relatively for:
        a. High,
        b. Low
        c. Open
        d. Close
    3. Check that the difference between yahoo and removed of outliers is not greater than the threshold
    this test is based on get_config()["adjustments"]["verification_parameters"]["minute_outlier"]\
            ["outlier_price_limit"], so if that changes this test may fail
    """

    s3_resource = setup_s3  # get s3_resource
    engine = setup_redshift  # get sqlite engine

    if data_config["debug"]:
        pass

    bucket_id = get_config()["raw_data_pull"]["base_data_bucket"]
    raw_tickdata_table = get_config()["redshift"]["raw_tickdata_equity_price_table"]

    start_date = data_config["start_date"]
    end_date = data_config["end_date"]
    as_of_date = data_config["as_of_date"]

    # Create and populate mocked S3 bucket with corporate actions file
    for path_to_input, data_location_prefix in data_config["input_files_and_keys"]:
        s3_key = prepare_aws_s3_key(ticker, get_config()[data_location_prefix], end_date)
        if "TickData.CA" in data_location_prefix:
            body = read_file(path_to_input, header=None, sep="^")
        else:
            body = read_file(path_to_input, header=None, sep=",")
        create_s3_bucket_and_put_object(s3_resource, bucket_id, s3_key, body)

    # Create and populate mocked Redshift table with raw equity prices
    actual_data = pd.read_csv(data_config["raw_equity_price_file"], parse_dates=["cob", "as_of"]).dropna()
    actual_data.to_sql(raw_tickdata_table, engine, index=False)
    mock.patch("pipelines.prices_ingestion.equity_data_holders.RedshiftEquityDataHolderBaseClass.get_engine",
               return_value=engine)
    mock.patch("pipelines.prices_ingestion.corporate_actions_holder.storage.Client", return_value=s3_resource)
    mock.patch("pipelines.prices_ingestion.equity_data_holders.storage.Client", return_value=s3_resource)

    # Call Function
    if data_config["expect_exception"]:
        with pytest.raises(data_config["exception_type"]):
            minute_data_dh = RedshiftRawTickDataDataHolder(ticker, start_date, end_date, as_of_date)
            ca = S3YahooCorporateActionsHolder(ticker, as_of_date=as_of_date, run_date=end_date)
            daily_reference_dh = S3YahooEquityDataHolder(ticker, ca, start_date, end_date, as_of_date)
            minute_data_dh.equity_data = minute_data_dh.equity_data.tz_convert("UTC")
            price_outlier_checker = VerifyMinuteOutlierForPrice(daily_reference_dh, do_operation_over_data_flag=True)

            # Get results
            outlier_adjusted = price_outlier_checker(minute_data_dh)
    else:
        minute_data_dh = RedshiftRawTickDataDataHolder(ticker, start_date, end_date, as_of_date)
        ca = S3YahooCorporateActionsHolder(ticker, as_of_date=as_of_date, run_date=end_date)
        daily_reference_dh = S3YahooEquityDataHolder(ticker, ca, start_date, end_date, as_of_date)
        minute_data_dh.equity_data = minute_data_dh.equity_data.tz_convert("UTC")
        price_outlier_checker = VerifyMinuteOutlierForPrice(daily_reference_dh, do_operation_over_data_flag=True)

        # Get results
        outlier_adjusted = price_outlier_checker(minute_data_dh)
        test_adjusted_prices = outlier_adjusted.get_data()

        # Check results
        assert outlier_adjusted.get_ticker() == ticker
        assert outlier_adjusted.is_daily_data() is False
        assert outlier_adjusted.is_data_adjusted() is False
        assert minute_data_dh.get_data().ix[0].name.tz_convert("UTC").date() == min(test_adjusted_prices.index.date)
        assert minute_data_dh.get_data().ix[-1].name.tz_convert("UTC").date() == max(test_adjusted_prices.index.date)
        assert min(test_adjusted_prices[VolumeLabel]) >= 0
        assert all(test_adjusted_prices.loc[test_adjusted_prices[VolumeLabel] != 0].notnull().all())

        # Compare adjusted prices
        expected_adjusted_prices = pd.read_csv(data_config["output_local_file"], index_col=["Date"], parse_dates=["Date"])
        expected_adjusted_prices.index = expected_adjusted_prices.index.tz_localize("UTC")
        assert_frame_equal(test_adjusted_prices, expected_adjusted_prices)


# -------------------------------------------------------------------
# VerifyOutlierVol
# -------------------------------------------------------------------

test_data = [
    ("NKE", {
        "start_date": pd.Timestamp("2003-03-26"),
        "end_date": pd.Timestamp("2016-06-14"),
        "as_of_date": pd.Timestamp("2016-06-15 23:59:00"),
        "expected_diff_index": [pd.DatetimeIndex([date(2012, 4, 3)], name="Date")],
        "outlier_vol_to_fix": [pd.Series(1080964.18, index=pd.DatetimeIndex([date(2012, 4, 3)], name="Date"))],
        "R": 2020775,
        "threshold_vol_reference": 0.015}),
]


@pytest.mark.parametrize("ticker, data_config", test_data, ids=["vol_test_1"])
def test_identify_outlier_for_vol(ticker, data_config, patched_config, mocker):
    start_date = data_config["start_date"]
    end_date = data_config["end_date"]
    as_of_date = data_config["as_of_date"]

    # Mocking TickData EquityDataHolder
    equity_input = pd.read_csv(os.path.join(path_to_test_dir, "MinuteOutlierRemoval", "identify_outlier_for_vol",
                                            "input_NKE_with_price_volume_outlier.csv"), index_col=0, parse_dates=[0])
    equity_input.index = equity_input.index.tz_localize("UTC")
    equity_data_holder = DerivedEquityDataHolder(ticker, equity_data=equity_input,
                                                 daily_frequency_flag=False,
                                                 adjusted_data_flag=False,
                                                 as_of_date=as_of_date)

    # Mocking Yahoo EquityDataHolder
    equity_yahoo_input = pd.read_csv(os.path.join(path_to_test_dir, "MinuteOutlierRemoval", "identify_outlier_for_vol",
                                                  "input_NKE_yahoo_data_unadjusted.csv"), index_col=0, parse_dates=[0])
    equity_yahoo_input.index = equity_yahoo_input.index.tz_localize("UTC")
    yahoo_data_holder = DerivedEquityDataHolder(ticker, equity_data=equity_yahoo_input,
                                                daily_frequency_flag=True,
                                                adjusted_data_flag=False,
                                                as_of_date=as_of_date)

    # Preparing mocks for rolling_mean and rolling_std to be used in equity_data_operations ->
    # -> VerifyMinuteOutlierForVolume -> identify_outlier_for_vol()
    mean_diff_mock_data = pd.Series.from_csv(os.path.join(path_to_test_dir, "MinuteOutlierRemoval",
                                                          "identify_outlier_for_vol", "mean_diff.csv"))
    mean_diff_mock_data.index = mean_diff_mock_data.index.tz_localize("UTC")
    stdev_diff_mock_data = pd.Series.from_csv(os.path.join(path_to_test_dir, "MinuteOutlierRemoval",
                                                           "identify_outlier_for_vol", "stdev_diff.csv"))
    stdev_diff_mock_data.index = stdev_diff_mock_data.index.tz_localize("UTC")

    # Mocking rolling_mean and rolling_std
    mocker.patch.object(pd.core.window.Rolling, "mean", return_value=mean_diff_mock_data)
    mocker.patch.object(pd.core.window.Rolling, "std",  return_value=stdev_diff_mock_data)

    # Aggregating minutes using the minute aggregator [MinuteDataAggregator is tested separately]
    daily_data = MinuteDataAggregator()(equity_data_holder)
    adjuster = VerifyMinuteOutlierForVolume(yahoo_data_holder)
    adjuster.identify_outlier_for_vol(daily_data.get_data())

    # Get test results and compare with the expected values
    test_output = adjuster.verification_output["Vol_outliers_dt"]
    npt.assert_array_equal(test_output.index, data_config["expected_diff_index"][0])
    npt.assert_array_almost_equal(test_output.values, data_config["outlier_vol_to_fix"][0].values, decimal=2)


@pytest.mark.parametrize("ticker, data_config", test_data, ids=["vol_test_2"])
def test_solve_linear_system(ticker, data_config, patched_config):
    start_date = data_config["start_date"]
    end_date = data_config["end_date"]
    as_of_date = data_config["as_of_date"]

    # Mocking Yahoo EquityDataHolder
    equity_yahoo_input = pd.read_csv(os.path.join(path_to_test_dir, "MinuteOutlierRemoval", "identify_outlier_for_vol",
                                                  "input_NKE_yahoo_data_unadjusted.csv"), index_col=0, parse_dates=[0])
    yahoo_data_holder = DerivedEquityDataHolder(ticker, equity_yahoo_input, True, False, as_of_date)
    adjuster = VerifyMinuteOutlierForVolume(ticker, yahoo_data_holder)

    # Read minute points with outlier data and compute required adjustments by solving liner system
    outlier_minute_points = pd.Series.from_csv(os.path.join(path_to_test_dir, "MinuteOutlierRemoval",
                                                            "identify_outlier_for_vol", "minutes_with_issues.csv"))
    x = outlier_minute_points.values
    n = len(x)
    identity_matrix = pd.np.identity(n)
    ones_matrix = pd.np.ones((n, n))
    matrix_inv = pd.np.linalg.inv((identity_matrix - data_config["threshold_vol_reference"] * ones_matrix))
    adjustments = pd.Series(pd.np.dot(matrix_inv, (x - data_config["threshold_vol_reference"] * data_config["R"]
                                                   * pd.np.ones(n))), index=outlier_minute_points.index)
    outlier_minute_points -= adjustments

    # Compare test results with the expected data
    expected_outlier_minute_points = adjuster._VerifyMinuteOutlierForVolume__solve_linear_system(
        outlier_minute_points, data_config["R"], data_config["threshold_vol_reference"])
    npt.assert_array_equal(outlier_minute_points, expected_outlier_minute_points)


test_data = [
    ("NKE", {
        "case_name": "",
        "start_date": pd.Timestamp("2003-03-26"),
        "end_date": pd.Timestamp("2016-06-14"),
        "as_of_date": pd.Timestamp("2016-06-15 23:59:00"),
        "input_local_data": os.path.join(path_to_test_dir, "VerifyOutlierVol", "input_NKE_normalized.csv"),
        "output_local_file": os.path.join(path_to_test_dir, "VerifyOutlierVol", "NKE_outlier_removed.csv"),
        "yahoo_csv": os.path.join(path_to_test_dir, "VerifyOutlierVol", "yahoo_NKE.csv"),
        "is_data_adjusted": False}),
]


@pytest.mark.parametrize("ticker, data_config", test_data, ids=["vol_test_3"])
def test_VerifyMinuteOutlierForVolume(ticker, data_config, patched_config):
    start_date = data_config["start_date"]
    end_date = data_config["end_date"]
    as_of_date = data_config["as_of_date"]

    # Mocking TickData EquityDataHolder
    input_data = pd.read_csv(data_config["input_local_data"], parse_dates=["Date"], index_col=["Date"])
    input_data.index = input_data.index.tz_localize("US/Eastern").tz_convert("UTC")
    normalized_data = DummyDataHolder(ticker, start_date, end_date, as_of_date, input_data[EquityPriceLabels])

    # Mocking Yahoo EquityDataHolder
    yahoo_file = pd.read_csv(data_config["yahoo_csv"], parse_dates=["Date"], index_col=["Date"])
    yahoo_dh = DummyDataHolder(ticker, start_date, end_date, as_of_date, yahoo_file[EquityPriceLabels])

    # Get results
    fixer_vol = VerifyMinuteOutlierForVolume(yahoo_dh)
    fixed_data = fixer_vol(normalized_data)

    # Check results
    assert fixed_data.get_ticker() == ticker
    assert list(fixed_data.get_data().columns) == EquityPriceLabels
    assert fixed_data.is_data_adjusted() == data_config["is_data_adjusted"]
    assert fixed_data.is_daily_data() is False

    # Reading expected adjusted prices
    expected_adjusted_prices = pd.read_csv(data_config["output_local_file"], parse_dates=["Date"],
                                           index_col=["Date"],)[EquityPriceLabels]
    expected_adjusted_prices.index = expected_adjusted_prices.index.tz_localize("UTC")
    assert_frame_equal(fixed_data.get_data(), expected_adjusted_prices)


# -------------------------------------------------------------------
# VerifyTimeline
# -------------------------------------------------------------------

test_data = [
    ("FB", {
        "case_name": "",
        "start_date": pd.Timestamp("2001-01-20"),
        "end_date": pd.Timestamp("2016-08-17"),
        "as_of_date": pd.Timestamp("2016-08-18 23:59:00"),
        "input_files_and_keys": [
            (os.path.join(path_to_test_dir, "VerifyTimeline", "FB_CA_1465977600.csv"),
             "raw_data_pull.TickData.CA.data_location_prefix"),
            (os.path.join(path_to_test_dir, "VerifyTimeline", "yahoo_ca_on_minute.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.ca_data_location_prefix"),
            (os.path.join(path_to_test_dir, "VerifyTimeline", "yahoo_data_with_zero_after_cuttof.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.price_data_location_prefix")
        ],
        "output_local_file": None,
        "raw_equity_price_file": os.path.join(path_to_test_dir, "VerifyTimeline", "Minute_data_on_no_trade_day.csv"),
        "expect_exception": True,
        "exception_type": ValueError,
        "debug": False}),
    ("FBB", {
        "case_name": "",
        "start_date": pd.Timestamp("2001-01-20"),
        "end_date": pd.Timestamp("2016-08-17"),
        "as_of_date": pd.Timestamp("2016-08-18 23:59:00"),
        "input_files_and_keys": [
            (os.path.join(path_to_test_dir, "VerifyTimeline", "FBB_CA_1465977600.csv"),
             "raw_data_pull.TickData.CA.data_location_prefix"),
            (os.path.join(path_to_test_dir, "VerifyTimeline", "yahoo_ca_on_minute.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.ca_data_location_prefix"),
            (os.path.join(path_to_test_dir, "VerifyTimeline", "yahoo_data_not_like_market_timeline.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.price_data_location_prefix")
        ],
        "output_local_file": None,
        "raw_equity_price_file": os.path.join(path_to_test_dir, "VerifyTimeline",
                                              "Minute_missing_before_missing_on_zeros.csv"),
        "expect_exception": True,
        "exception_type": ValueError,
        "debug": False}),
    ("FBB", {
        "case_name": "",
        "start_date": pd.Timestamp("2001-01-20"),
        "end_date": pd.Timestamp("2016-08-17"),
        "as_of_date": pd.Timestamp("2016-08-18 23:59:00"),
        "input_files_and_keys": [
            (os.path.join(path_to_test_dir, "VerifyTimeline", "FBB_CA_1465977600.csv"),
             "raw_data_pull.TickData.CA.data_location_prefix"),
            (os.path.join(path_to_test_dir, "VerifyTimeline", "yahoo_ca_on_minute.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.ca_data_location_prefix"),
            (os.path.join(path_to_test_dir, "VerifyTimeline", "yahoo_data_with_data_after_timeline.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.price_data_location_prefix")
        ],
        "output_local_file": None,
        "raw_equity_price_file": os.path.join(path_to_test_dir, "VerifyTimeline",
                                              "Minute_missing_before_missing_on_zeros.csv"),
        "expect_exception": True,
        "exception_type": ValueError,
        "debug": False}),
    ("X", {
        "case_name": "",
        "start_date": pd.Timestamp("2001-01-20"),
        "end_date": pd.Timestamp("2016-08-17"),
        "as_of_date": pd.Timestamp("2016-08-18 23:59:00"),
        "input_files_and_keys": [
            (os.path.join(path_to_test_dir, "VerifyTimeline", "X_CA_1465977600.csv"),
             "raw_data_pull.TickData.CA.data_location_prefix"),
            (os.path.join(path_to_test_dir, "VerifyTimeline", "yahoo_ca_before_minute.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.ca_data_location_prefix"),
            (os.path.join(path_to_test_dir, "VerifyTimeline", "yahoo_data_with_zero_after_cuttof.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.price_data_location_prefix")
        ],
        "output_local_file": None,
        "raw_equity_price_file": os.path.join(path_to_test_dir, "VerifyTimeline",
                                              "Minute_missing_before_missing_on_zeros2.csv"),
        "expect_exception": True,
        "exception_type": ValueError,
        "debug": False}),
    ("X", {
        "case_name": "",
        "start_date": pd.Timestamp("2001-01-20"),
        "end_date": pd.Timestamp("2016-08-17"),
        "as_of_date": pd.Timestamp("2016-08-18 23:59:00"),
        "input_files_and_keys": [
            (os.path.join(path_to_test_dir, "VerifyTimeline", "X_CA_1465977600.csv"),
             "raw_data_pull.TickData.CA.data_location_prefix"),
            (os.path.join(path_to_test_dir, "VerifyTimeline", "yahoo_ca_on_minute.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.ca_data_location_prefix"),
            (os.path.join(path_to_test_dir, "VerifyTimeline", "yahoo_data_with_zero_after_cuttof.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.price_data_location_prefix")
        ],
        "output_local_file": os.path.join(path_to_test_dir, "VerifyTimeline", "FB_good_expected_results.csv"),
        "raw_equity_price_file": os.path.join(path_to_test_dir, "VerifyTimeline",
                                              "Minute_missing_before_missing_on_zeros2.csv"),
        "expect_exception": True,
        "exception_type": ValueError,
        "debug": False}),
    ("GASL", {
        "case_name": "",
        "start_date": pd.Timestamp("2001-01-20"),
        "end_date": pd.Timestamp("2016-04-15"),
        "as_of_date": pd.Timestamp("2016-04-16 23:59:00"),
        "input_files_and_keys": [
            (os.path.join(path_to_test_dir, "VerifyTimeline", "GASL_CA_1460793600.csv"),
             "raw_data_pull.TickData.CA.data_location_prefix"),
            (os.path.join(path_to_test_dir, "VerifyTimeline", "CA_GASL_1460847600.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.ca_data_location_prefix"),
            (os.path.join(path_to_test_dir, "VerifyTimeline", "yahoo_GASL_1460847600.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.price_data_location_prefix")
        ],
        "output_local_file": os.path.join(path_to_test_dir, "VerifyTimeline", "GASL_expected_results.csv"),
        "raw_equity_price_file": os.path.join(path_to_test_dir, "VerifyTimeline", "GASL_tickdata_minute.csv"),
        "expect_exception": False,
        "exception_type": None,
        "debug": True,
        "CA_dt_error": 0,
        "Relevant_dt_with_holes": 0,
        "Non_relevant_dt_with_holes": 0}),
    ("FB", {
        "case_name": "",
        "start_date": pd.Timestamp("2003-03-26"),
        "end_date": pd.Timestamp("2016-06-14"),
        "as_of_date": pd.Timestamp("2016-06-15 23:59:00"),
        "input_files_and_keys": [
            (os.path.join(path_to_test_dir, "VerifyTimeline", "FB_CA_1465977600.csv"),
             "raw_data_pull.TickData.CA.data_location_prefix"),
            (os.path.join(path_to_test_dir, "VerifyTimeline", "FB_1468015319.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.ca_data_location_prefix"),
            (os.path.join(path_to_test_dir, "VerifyTimeline", "yahoo_FB.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.price_data_location_prefix")
        ],
        "output_local_file": os.path.join(path_to_test_dir, "VerifyTimeline", "FB_expected_results.csv"),
        "raw_equity_price_file": os.path.join(path_to_test_dir, "VerifyTimeline", "FB_tickdata_minute.csv"),
        "expect_exception": False,
        "exception_type": None,
        "debug": True,
        "CA_dt_error": 0,
        "Relevant_dt_with_holes": 0,
        "Non_relevant_dt_with_holes": 0})
]


@pytest.mark.parametrize("ticker, data_config", test_data)
def test_VerifyTimeline(ticker, data_config, setup_s3, setup_redshift, patched_config, mock, mock_query_creator):
    """
    Testing should include:

    1. When there is missing data, with non-zero in reference, and beyond date we care about, raises correct error,
    before date we care about, it should not raise an error

    2. When CA's are more recent than data start_date, the data should be cut off to start from CA date
    (Check the exact date). When CA's start is more previous than data start_date, the rules of test 1 should handle it

    THIS TEST ONLY TESTS THE DEFAULT PARAMETERS FOR THE OBJECT, and may fail if those parameters are changed
    def __init__(self, reference_data_holder, do_operation_over_data_flag=True, full_checks_flag=True,
                 cut_off_date_for_missing_data=date(2004,1,1)

    There are 6 factors to keep in mind:

    Cut of date: Using default of date(2004,1,1)
    do-operation flag: Using default of True
    full check flag: Using default of true

    Inputs: Daily data, Minute data, CA data

    Therefore there are three cases which must be tested, one which is a combo of 4 other cases
    1. 0 volume date for yahoo exist after the cutoff, where CA start date is earlier that minute start date,
        and the minute data does have missing data before the cut-off date, as well as after the cut-off date, but the
        missing dates after the cut-off dates are only on the non-zero dates. Even in this situation the output should
        look the same as the input
    2. CA start date is further in time than minute data, the return should contain minute data on/after CA start date
    3. Minute data is missing dates that are non Non zero dates, an adjustment exception should be raised.
    4. Minute data is found on dates that are not trading dates, an adjustment exception should be raised
    5. Daily data is found on dates that are not trading dates, an adjustment exception should be raised
    """
    s3_resource = setup_s3  # get s3_resource
    engine = setup_redshift  # get sqlite engine

    if data_config["debug"]:
        pass

    bucket_id = get_config()["raw_data_pull"]["base_data_bucket"]
    raw_tickdata_table = get_config()["redshift"]["raw_tickdata_equity_price_table"]

    start_date = data_config["start_date"]
    end_date = data_config["end_date"]
    as_of_date = data_config["as_of_date"]

    # Create and populate mocked S3 bucket with corporate actions file
    for path_to_input, data_location_prefix in data_config["input_files_and_keys"]:
        s3_key = prepare_aws_s3_key(ticker, get_config()[data_location_prefix], end_date)
        if "TickData.CA" in data_location_prefix:
            body = read_file(path_to_input, header=None, sep="^")
        else:
            body = read_file(path_to_input, header=None, sep=",")
        create_s3_bucket_and_put_object(s3_resource, bucket_id, s3_key, body)

    # Create and populate mocked Redshift table with raw equity prices
    actual_data = pd.read_csv(data_config["raw_equity_price_file"], parse_dates=["cob", "as_of"]).dropna()
    actual_data.to_sql(raw_tickdata_table, engine, index=False)
    mock.patch("pipelines.prices_ingestion.equity_data_holders.RedshiftEquityDataHolderBaseClass.get_engine",
               return_value=engine)

    mock.patch("pipelines.prices_ingestion.equity_data_holders.storage.Client", return_value=s3_resource)
    mock.patch("pipelines.prices_ingestion.corporate_actions_holder.storage.Client", return_value=s3_resource)

    # Call Function
    minute_data_dh = RedshiftRawTickDataDataHolder(ticker, start_date, end_date, as_of_date)
    ca = S3YahooCorporateActionsHolder(ticker, as_of_date=as_of_date, run_date=end_date)
    daily_reference_dh = S3YahooEquityDataHolder(ticker, ca, start_date, end_date, as_of_date)
    timeline_checker = VerifyTimeline(daily_reference_dh, ca, date(2004, 1, 1), start_date, end_date)

    if data_config["expect_exception"]:
        pytest.raises(ValueError)
    else:
        timeline_adjusted = timeline_checker(minute_data_dh)

        # Get expected output from local file
        df_expected = pd.read_csv(data_config["output_local_file"], parse_dates=["Date"], index_col=["Date"])
        df_expected.index = df_expected.index.tz_localize("UTC").tz_convert("US/Eastern")

        # Check results
        assert timeline_adjusted.is_daily_data() is False
        assert timeline_adjusted.is_data_adjusted() is False
        assert timeline_adjusted.get_ticker() == ticker
        assert timeline_adjusted.get_data().index.tzinfo.zone == "US/Eastern"
        assert ("CA_dt_error" in timeline_checker.verification_output) is False
        assert len(list(timeline_checker.verification_output["Dates_with_holes"]["Relevant_dt_with_holes"])) == \
               data_config["Relevant_dt_with_holes"]
        assert len(list(timeline_checker.verification_output["Dates_with_holes"]["Relevant_dt_with_holes"])) == \
               data_config["Non_relevant_dt_with_holes"]
        assert_frame_equal(timeline_adjusted.get_data(), df_expected)


# -------------------------------------------------------------------
# CompleteMinutes
# -------------------------------------------------------------------

test_data = [
    ("X", {
        "case_name": "",
        "start_date": pd.Timestamp("2001-01-20"),
        "end_date": pd.Timestamp("2016-08-17"),
        "as_of_date": pd.Timestamp("2016-08-18 23:59:00"),
        "input_files_and_keys": [
            (os.path.join(path_to_test_dir, "CompleteMinutes", "X_CA_1465977600.csv"),
             "raw_data_pull.TickData.CA.data_location_prefix"),
            (os.path.join(path_to_test_dir, "CompleteMinutes", "yahoo_ca.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.ca_data_location_prefix"),
            (os.path.join(path_to_test_dir, "CompleteMinutes", "yahoo_reference.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.price_data_location_prefix")
        ],
        "output_local_file": os.path.join(path_to_test_dir, "CompleteMinutes", "AA_expected_output.csv"),
        "raw_equity_price_file": os.path.join(path_to_test_dir, "CompleteMinutes", "AA_minute_data.csv"),
        "expect_exception": False,
        "exception_type": None,
        "debug": False}),
]


@pytest.mark.parametrize("ticker, data_config", test_data)
def test_CompleteMinutes(ticker, data_config, setup_s3, setup_redshift, patched_config, mock,mock_query_creator):
    """
    Testing should include:
    - start time and end time of output are correct
    - output has 390 minutes per day between the market open and close times, i.e. no minutes should be missing,
      or found afterwards
    - tzinfo should be utc
    - volume should never be nan
    - prices should only be nan if volume is zero

    It is only meant to handle cases where the inputs have the same start and end dates.
    Still it is currently being tested, where CA have a later end date, and dailyPrice has an earlier start date
    The time that the data is stamped on s3 is irrelevant, only needed for grabbing the data in the dataholders
    """
    s3_resource = setup_s3  # get s3_resource
    engine = setup_redshift  # get sqlite engine

    if data_config["debug"]:
        pass

    bucket_id = get_config()["raw_data_pull"]["base_data_bucket"]
    raw_tickdata_table = get_config()["redshift"]["raw_tickdata_equity_price_table"]

    start_date = data_config["start_date"]
    end_date = data_config["end_date"]
    as_of_date = data_config["as_of_date"]

    # Create and populate mocked S3 bucket with corporate actions file
    for path_to_input, data_location_prefix in data_config["input_files_and_keys"]:
        s3_key = prepare_aws_s3_key(ticker, get_config()[data_location_prefix], end_date)
        if "TickData.CA" in data_location_prefix:
            body = read_file(path_to_input, header=None, sep="^")
        else:
            body = read_file(path_to_input, header=None, sep=",")
        create_s3_bucket_and_put_object(s3_resource, bucket_id, s3_key, body)

    # Create and populate mocked Redshift table with raw equity prices
    actual_data = pd.read_csv(data_config["raw_equity_price_file"], parse_dates=["cob", "as_of"]).dropna()
    actual_data.to_sql(raw_tickdata_table, engine, index=False)
    mock.patch("pipelines.prices_ingestion.equity_data_holders.RedshiftEquityDataHolderBaseClass.get_engine",
               return_value=engine)

    mock.patch("pipelines.prices_ingestion.equity_data_holders.storage.Client", return_value=s3_resource)
    mock.patch("pipelines.prices_ingestion.corporate_actions_holder.storage.Client", return_value=s3_resource)

    # Call Function
    minute_data_dh = RedshiftRawTickDataDataHolder(ticker, start_date, end_date, as_of_date)
    ca = S3YahooCorporateActionsHolder(ticker, as_of_date=as_of_date, run_date=end_date)
    daily_reference_dh = S3YahooEquityDataHolder(ticker, ca, start_date, end_date, as_of_date)
    minute_completer = CompleteMinutes(start_date=start_date, end_date=end_date)

    if data_config["expect_exception"]:
        pytest.raises(data_config["exception_type"], minute_completer, minute_data_dh)
    else:
        # Get results
        minutes_adjusted = minute_completer(minute_data_dh)
        actual = minutes_adjusted.get_data()
        time_index = actual.index
        dates_of_index = time_index.date
        s = pd.Series(dates_of_index)

        # Confirm the START and END dates match minute data
        assert minute_data_dh.get_data().ix[0].name.tz_convert("UTC").date() == min(time_index.date)
        assert minute_data_dh.get_data().ix[-1].name.tz_convert("UTC").date() == max(time_index.date)

        # Check rest of the results
        assert minutes_adjusted.get_ticker() == ticker
        assert minutes_adjusted.is_daily_data() is False
        assert minutes_adjusted.is_data_adjusted() is False
        assert time_index.tzinfo == pd.Timestamp.now("UTC").tzinfo
        assert all(s.groupby(by=s).size() == 390)
        assert min(time_index.time) == time(14, 31)
        assert max(time_index.time) == time(21, 0)
        assert min(actual[VolumeLabel]) >= 0
        assert all(actual.loc[actual[VolumeLabel] != 0].notnull().all())

        # Test against output for forward and backwards fill for first-minute
        # Get expected output from local file
        df_expected = pd.read_csv(data_config["output_local_file"], index_col=["Date"], parse_dates=["Date"])
        df_expected.index = df_expected.index.tz_localize("UTC")
        assert_frame_equal(minutes_adjusted.get_data(), df_expected)


# -------------------------------------------------------------------
# SplitAndDividendEquityAdjuster
# -------------------------------------------------------------------

test_data = [
    ("NKE", {
        "start_date": pd.Timestamp("2012-12-21"),
        "end_date": pd.Timestamp("2012-12-31"),
        "as_of_date": pd.Timestamp("2013-01-03 23:59:00"),
        "input_adjustment_factors": {
            "full_adjustment": os.path.join(path_to_test_dir, "SplitAndDividendEquityAdjuster", "make_adjustment",
                                            "input_yahoo_full_adjustment_factor.csv"),
            "split_adjustment": os.path.join(path_to_test_dir, "SplitAndDividendEquityAdjuster", "make_adjustment",
                                             "input_yahoo_split_adjustment_factor.csv")},
        "input_equity_prices": {
            "adjusted": os.path.join(path_to_test_dir, "SplitAndDividendEquityAdjuster", "make_adjustment",
                                     "input_NKE_tickdata_adjusted.csv"),
            "unadjusted": os.path.join(path_to_test_dir, "SplitAndDividendEquityAdjuster", "make_adjustment",
                                       "input_NKE_tickdata_unadjusted.csv")},
    })
]


def read_equity_data(filename):
    equity_data = pd.read_csv(filename, parse_dates=[0], index_col=0).sort_index()
    equity_data.index = equity_data.index.tz_localize("UTC")
    return equity_data


@pytest.mark.parametrize("ticker, data_config", test_data)
@pytest.mark.parametrize("from_unadjusted_to_adjusted", [True, False], ids=["adjust", "unadjust"])
def test_make_adjustment(ticker, data_config, from_unadjusted_to_adjusted):
    yahoo_full_adjustment_factor = pd.Series.from_csv(data_config["input_adjustment_factors"]["full_adjustment"])
    yahoo_split_adjustment_factor = pd.Series.from_csv(data_config["input_adjustment_factors"]["split_adjustment"])
    input_key, output_key = ("unadjusted", "adjusted") if from_unadjusted_to_adjusted else ("adjusted", "unadjusted")
    input_equity_prices = read_equity_data(data_config["input_equity_prices"][input_key])
    dh = DummyDataHolder(ticker=ticker, start_date=data_config["start_date"],
                         end_date=data_config["end_date"],
                         as_of_date=data_config["as_of_date"],
                         df=input_equity_prices)
    # Call Function
    adjuster = SplitAndDividendEquityAdjuster(full_multiplicative_ratio=yahoo_full_adjustment_factor,
                                              split_multiplicative_ratio=yahoo_split_adjustment_factor,
                                              from_unadjusted_to_adjusted=from_unadjusted_to_adjusted)
    adjusted_equity_prices = adjuster._make_adjustment(dh)

    # Get expected output from local file
    expected_output = read_equity_data(data_config["input_equity_prices"][output_key])
    expected_output[VolumeLabel] = expected_output[VolumeLabel].astype(float)
    expected_output = expected_output[EquityPriceLabels]
    expected_output.index.name = "DateTime"
    assert_frame_equal(adjusted_equity_prices, expected_output)


# -------------------------------------------------------------------
# S3YahooPriceAdjuster
# -------------------------------------------------------------------

test_data = [
    ("NKE", {
        "start_date": pd.Timestamp("2012-12-21"),
        "end_date": pd.Timestamp("2012-12-31"),
        "as_of_date": pd.Timestamp("2013-01-03 23:59:00"),
        "input_files_and_keys": [
            (os.path.join(path_to_test_dir, "SplitAndDividendEquityAdjuster", "make_adjustment", "NKE_CA_1357200000.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.ca_data_location_prefix"),
            (os.path.join(path_to_test_dir, "SplitAndDividendEquityAdjuster", "make_adjustment", "NKE_yahoo.csv"),
             "raw_data_pull.Yahoo.PriceAndCA.price_data_location_prefix")
        ],
    })
]


@pytest.mark.parametrize("ticker, data_config", test_data)
def test_S3YahooPriceAdjuster(ticker, data_config, setup_s3, patched_config, mocker):
    start_date = data_config["start_date"]
    end_date = data_config["end_date"]
    as_of_date = data_config["as_of_date"]

    s3_resource = setup_s3  # get s3_resource
    bucket_id = get_config()["raw_data_pull"]["base_data_bucket"]

    # Create and populate mocked S3 bucket with corporate actions file
    for path_to_input, data_location_prefix in data_config["input_files_and_keys"]:
        s3_key = prepare_aws_s3_key(ticker, get_config()[data_location_prefix], end_date)
        if "TickData.CA" in data_location_prefix:
            body = read_file(path_to_input, header=None, sep="^")
        else:
            body = read_file(path_to_input, header=None, sep=",")
        create_s3_bucket_and_put_object(s3_resource, bucket_id, s3_key, body)

    mocker.patch("pipelines.prices_ingestion.corporate_actions_holder.storage.Client", return_value=s3_resource)

    yahoo_ca_dh = S3YahooCorporateActionsHolder(ticker, as_of_date=as_of_date, run_date=end_date)
    yahoo_data_holder = S3YahooEquityDataHolder(ticker, corporate_actions_holder=yahoo_ca_dh,
                                                start_date=start_date, end_date=end_date,
                                                as_of_date=as_of_date)

    get_full_adj_factor = mocker.patch.object(S3YahooEquityDataHolder, "get_full_multiplicative_adjustment_factor")
    get_split_adj_factor = mocker.patch.object(S3YahooEquityDataHolder, "get_split_multiplicative_adjustment_factor")

    adjuster = S3YahooPriceAdjuster(yahoo_data_holder)
    get_full_adj_factor.assert_called_once_with()
    get_split_adj_factor.assert_called_once_with()
