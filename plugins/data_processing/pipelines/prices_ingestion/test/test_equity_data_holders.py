import os
import pytest
import pandas as pd

from pandas.testing import assert_frame_equal, assert_series_equal
from pipelines.prices_ingestion.etl_workflow_aux_functions import AB_RE
from pipelines.prices_ingestion.corporate_actions_holder import S3YahooCorporateActionsHolder
from pipelines.prices_ingestion.config import get_config, OpenLabel, HighLabel, LowLabel, CloseLabel, VolumeLabel, \
    DateLabel
from pipelines.prices_ingestion.equity_data_holders import RedshiftEquityDataHolder, S3TickDataEquityDataHolder, \
    S3YahooEquityDataHolder
from conftest import DummyDataHolder, DefaultStartTime, DefaultEndTime, random_price_data, read_file, \
    prepare_aws_s3_key, create_s3_bucket_and_put_object, setup_s3, setup_redshift, patched_config

path_to_test_dir = os.path.join(os.path.dirname(__file__), "testing_aux_files")


# -------------------------------------------------------------------
# Test of cut_data_between_adjustment_dates()
# -------------------------------------------------------------------

def test_cut_data_between_adjustment_dates(patched_config):
    reference_rows = random_price_data(pd.Timestamp(DefaultStartTime) - pd.Timedelta("5 days"),
                                       pd.Timestamp(DefaultEndTime) + pd.Timedelta("5 days"))

    dh = DummyDataHolder("NKE", DefaultStartTime, DefaultEndTime, "2016-04-18 23:00:00", reference_rows)
    assert dh.get_data()[dh.get_data().index < DefaultStartTime.tz_localize("UTC")].empty
    assert len(dh.get_data()) > 0

    # EquityDataHolder.cut_data_between_adjustment_dates supports this optional argument, but there isn"t a gate
    # for it in the default constructor, so I just have to force the issue here.  It works, but this test could
    # otherwise be a little more focused

    dh.cut_data_between_adjustment_dates(cut_end_date=pd.Timestamp("2016-04-06 14:10:00", tz="UTC"))
    assert dh.get_data().index.min() == pd.Timestamp("2016-04-02 03:31:00+0000", tz="UTC")
    assert dh.get_data().index.max() == pd.Timestamp("2016-04-06 10:00:00+0000", tz="UTC")

    dh.cut_data_between_adjustment_dates(DefaultEndTime.tz_localize("UTC"))
    assert dh.get_data()[dh.get_data().index > DefaultEndTime.tz_localize("UTC")].empty


# -------------------------------------------------------------------
# S3TickDataEquityDataHolder
# -------------------------------------------------------------------

test_data = [
    ("NKE", {"filetest": os.path.join(path_to_test_dir, "s3TickDataDH", "tickdata1.csv"),
             "start_date": pd.Timestamp("2008-01-02"),
             "end_date": pd.Timestamp("2016-04-16"),
             "as_of_date": pd.Timestamp("2016-04-16 23:00:00")}),
    ("BRK-B", {"filetest": os.path.join(path_to_test_dir, "s3TickDataDH", "tickdata2.csv"),
               "start_date": pd.Timestamp("2008-01-02"),
               "end_date": pd.Timestamp("2011-12-18"),
               "as_of_date": pd.Timestamp("2011-12-18")})
]


@pytest.mark.parametrize("ticker, data_config", test_data)
def test_S3TickDataEquityDataHolder(ticker, data_config, patched_config, setup_s3, mock):
    """ Verifies S3TickDataEquityDataHolder correctly identifies the bucket based on end_date input and
    returns the most updated file in that bucket for the given security.

    Args:
    -----
       ticker: str
          Identity id of security
       data_config: dic
          A dictionary containing configuration settings
    """

    s3_resource = setup_s3  # get s3_resource

    bucket_id = get_config()["raw_data_pull"]["base_data_bucket"]
    price_location_prefix = get_config()["raw_data_pull"]["TickData"]["Price"]["data_location_prefix"]

    # Create and populate mocked S3 bucket with corporate actions file
    s3_key = prepare_aws_s3_key(ticker, price_location_prefix, data_config["end_date"])
    body = read_file(data_config["filetest"], header=None, sep=",", parse_dates=[6])
    create_s3_bucket_and_put_object(s3_resource, bucket_id, s3_key, body)

    mock.patch("pipelines.prices_ingestion.equity_data_holders.storage.Client", return_value=s3_resource)

    # Creating S3TickDataEquityDataHolder
    tickdata_dh = S3TickDataEquityDataHolder(ticker,
                                             start_date=data_config["start_date"],
                                             end_date=data_config["end_date"],
                                             as_of_date=data_config["as_of_date"])

    data_actual = pd.read_csv(data_config["filetest"], parse_dates=[6], header=None)
    data_actual = data_actual[data_actual.columns[[0, 1, 2, 3, 4, 6]]]
    data_actual.columns = [OpenLabel, HighLabel, LowLabel, CloseLabel, VolumeLabel, DateLabel]
    data_actual = data_actual.set_index(DateLabel).sort_index()
    data_actual.index = data_actual.index.tz_localize("US/Eastern")

    assert tickdata_dh.get_ticker() == AB_RE.sub("\\1", ticker)
    assert tickdata_dh.bucket_id == bucket_id
    assert tickdata_dh.data_location_prefix == price_location_prefix
    assert tickdata_dh.adjustment_unix_timestamp == data_config["as_of_date"]
    assert tickdata_dh.get_data().index.tzinfo.zone == "US/Eastern"
    assert tickdata_dh.get_data().shape == data_actual.shape
    assert set(tickdata_dh.get_data().columns.values) == {OpenLabel, HighLabel, LowLabel, CloseLabel, VolumeLabel}
    assert tickdata_dh.is_data_adjusted() is False
    assert tickdata_dh.daily_frequency_flag is False
    assert_frame_equal(tickdata_dh.equity_data, data_actual)


# -------------------------------------------------------------------
# S3TickDataEquityDataHolder_outcome
# -------------------------------------------------------------------

filenames = (
    {"test1": os.path.join(path_to_test_dir, "s3TickDataDH", "dummy1.csv"),
     "test2": os.path.join(path_to_test_dir, "s3TickDataDH", "dummy2.csv"),
     "actual": {"Day1": [os.path.join(path_to_test_dir, "s3TickDataDH", "dummy2.csv"),
                         os.path.join(path_to_test_dir, "s3TickDataDH", "dummy4.csv")],
                "Day2": [os.path.join(path_to_test_dir, "s3TickDataDH", "dummy3.csv")]},
     "test3": os.path.join(path_to_test_dir, "s3TickDataDH", "dummy3.csv"),
     "test4": os.path.join(path_to_test_dir, "s3TickDataDH", "dummy4.csv"),
     "actual2": {"Day1": [os.path.join(path_to_test_dir, "s3TickDataDH", "dummy1.csv"),
                          os.path.join(path_to_test_dir, "s3TickDataDH", "dummy4.csv")],
                 "Day2": [os.path.join(path_to_test_dir, "s3TickDataDH", "dummy3.csv")]}}
)

test_data = [
    ("AAPL", "Day1", {
        "Day1": {
            "filetest": filenames,
            "start_date": pd.Timestamp("2011-07-01"),
            "end_date": pd.Timestamp("2016-04-16"),
            "as_of_date": pd.Timestamp("2016-04-16 23:00:00"),
            "as_of_date1": pd.Timestamp("2016-04-16 19:00:00"),
            "as_of_date2": pd.Timestamp("2016-04-16 21:00:00")},
        "Day2": {
            "filetest": filenames,
            "start_date": pd.Timestamp("2000-01-01"),
            "end_date": pd.Timestamp("2011-12-18"),
            "as_of_date": pd.Timestamp("2011-12-18  10:00:00"),
            "as_of_date1": pd.Timestamp("2011-12-18 09:00:00"),
            "as_of_date2": pd.Timestamp("2011-12-18 12:00:00")}}),
    ("AAPL", "Day2", {
        "Day1": {
            "filetest": filenames,
            "start_date": pd.Timestamp("2011-07-01"),
            "end_date": pd.Timestamp("2016-04-16"),
            "as_of_date": pd.Timestamp("2016-04-16 23:00:00"),
            "as_of_date1": pd.Timestamp("2016-04-16 19:00:00"),
            "as_of_date2": pd.Timestamp("2016-04-16 21:00:00")},
        "Day2": {
            "filetest": filenames,
            "start_date": pd.Timestamp("2000-01-01"),
            "end_date": pd.Timestamp("2011-12-18"),
            "as_of_date": pd.Timestamp("2011-12-18  10:00:00"),
            "as_of_date1": pd.Timestamp("2011-12-18 09:00:00"),
            "as_of_date2": pd.Timestamp("2011-12-18 12:00:00")}})
 ]


@pytest.mark.parametrize("ticker, test_day, data_config", test_data)
def test_S3TickDataEquityDataHolder_outcome(ticker, test_day, data_config, patched_config, setup_s3, mock):
    """ Verifies S3TickDataEquityDataHolder correctly identifies the most recent file and concatenates different days
    based on as_of_date input for the given security.

    Args:
    -----
       ticker: str
          Identity id of security
       data_config: dic
          A dictionary containing configuration settings
       test_day: str
          A value to indicate what is the correct output for testing based on day of request input.
    """

    s3_resource = setup_s3  # get s3_resource

    bucket_id = get_config()["raw_data_pull"]["base_data_bucket"]
    price_location_prefix = get_config()["raw_data_pull"]["TickData"]["Price"]["data_location_prefix"]

    for day in data_config:
        if day == "Day1":
            for i, test in enumerate(["test1", "test2"]):
                s3_key = prepare_aws_s3_key(ticker, price_location_prefix, data_config[day]["as_of_date%s" % str(i+1)])
                body = read_file(data_config[day]["filetest"][test], header=None, sep=",")
                create_s3_bucket_and_put_object(s3_resource, bucket_id, s3_key, body)
        elif day == "Day2":
            for i, test in enumerate(["test3", "test4"]):
                s3_key = prepare_aws_s3_key(ticker, price_location_prefix, data_config[day]["as_of_date%s" % str(i+1)])
                body = read_file(data_config[day]["filetest"][test], header=None, sep=",")
                create_s3_bucket_and_put_object(s3_resource, bucket_id, s3_key, body)

    mock.patch("pipelines.prices_ingestion.equity_data_holders.storage.Client", return_value=s3_resource)

    tickdata_dh = S3TickDataEquityDataHolder(ticker,
                                             start_date=data_config[test_day]["start_date"],
                                             end_date=data_config[test_day]["end_date"],
                                             as_of_date=data_config[test_day]["as_of_date"])

    if len(data_config[test_day]["filetest"]["actual"][test_day]) == 1:
        data_actual = pd.read_csv(data_config[test_day]["filetest"]["actual"][test_day][0], parse_dates=[6], header=None)
    else:
        data_actual1 = pd.read_csv(data_config[test_day]["filetest"]["actual"][test_day][0], parse_dates=[6], header=None)
        data_actual2 = pd.read_csv(data_config[test_day]["filetest"]["actual"][test_day][1], parse_dates=[6], header=None)
        data_actual = pd.concat([data_actual1, data_actual2])

    data_actual = data_actual[data_actual.columns[[0, 1, 2, 3, 4, 6]]]
    data_actual.columns = [OpenLabel, HighLabel, LowLabel, CloseLabel, VolumeLabel, DateLabel]
    data_actual = data_actual.set_index(DateLabel).sort_index()
    data_actual.index = data_actual.index.tz_localize("US/Eastern")

    assert tickdata_dh.get_ticker() == ticker
    assert tickdata_dh.bucket_id == bucket_id
    assert tickdata_dh.data_location_prefix == price_location_prefix
    assert tickdata_dh.get_data().index.tzinfo.zone == "US/Eastern"
    assert set(tickdata_dh.get_data().columns.values) == {OpenLabel, HighLabel, LowLabel, CloseLabel, VolumeLabel}
    assert tickdata_dh.get_data().shape == data_actual.shape
    assert tickdata_dh.is_data_adjusted() is False
    assert tickdata_dh.daily_frequency_flag is False
    assert_frame_equal(data_actual, tickdata_dh.equity_data)


# -------------------------------------------------------------------
# RedshiftEquityDataHolder
# -------------------------------------------------------------------

test_data = [
    ("XOXO", {
        "start_date": pd.Timestamp("2001-08-30 13:00:00"),
        "end_date": pd.Timestamp("2001-07-08 20:43:00"),
        "as_of_date": pd.Timestamp("2016-07-08 23:59:00"),
        "equity_price_file": os.path.join(path_to_test_dir, "RedshiftDH", "fake_redshift_data.csv")})
]


@pytest.mark.parametrize("ticker, data_config", test_data)
def test_RedshiftEquityDataHolder(ticker, data_config, patched_config, setup_redshift, mock):
    engine = setup_redshift  # get sqlite engine
    equity_prices_table = get_config()["redshift"]["equity_price_table"]

    # Create and populate mocked Redshift table with raw equity prices
    actual_data = pd.read_csv(data_config["equity_price_file"], parse_dates=["cob"])
    actual_data.to_sql(equity_prices_table, engine, index=False)
    mock.patch("pipelines.prices_ingestion.equity_data_holders.RedshiftEquityDataHolderBaseClass.get_engine",
               return_value=engine)

    # Test of RedshiftEquityDataHolder
    redshift_dh = RedshiftEquityDataHolder(ticker=ticker,
                                           start_date=data_config["start_date"],
                                           end_date=data_config["end_date"],
                                           as_of_date=data_config["as_of_date"])

    # Expected equity prices
    expected_equity_prices = actual_data.set_index("cob", drop=True).sort_index()
    expected_equity_prices = expected_equity_prices.loc[(expected_equity_prices["ticker"] == ticker) &
                                                        (expected_equity_prices["as_of_end"].isnull())]
    expected_equity_prices = expected_equity_prices[["open", "high", "low", "close", "volume"]]
    expected_equity_prices.rename(columns=RedshiftEquityDataHolder.DICT_COLUMNS, inplace=True)
    expected_equity_prices.index.name = DateLabel

    assert redshift_dh.get_ticker() == ticker
    assert redshift_dh.as_of_date == data_config["as_of_date"]
    assert redshift_dh.start_date == data_config["start_date"]
    assert redshift_dh.end_date == data_config["end_date"]
    assert redshift_dh.is_daily_data() is False
    assert redshift_dh.is_data_adjusted() is True
    assert redshift_dh.get_data_lineage() == equity_prices_table
    assert_frame_equal(redshift_dh.get_data(), expected_equity_prices)


# -------------------------------------------------------------------
# S3YahooEquityDataHolder
# -------------------------------------------------------------------

input_files_and_keys = [
    [os.path.join(path_to_test_dir, "s3YahooDH", "yahooDiv2.csv"),
     "raw_data_pull.Yahoo.PriceAndCA.ca_data_location_prefix"],
    [os.path.join(path_to_test_dir, "s3YahooDH", "yahoo_price2.csv"),
     "raw_data_pull.Yahoo.PriceAndCA.price_data_location_prefix"],
    [os.path.join(path_to_test_dir, "s3YahooDH", "test1.csv"),
     "raw_data_pull.Yahoo.PriceAndCA.price_data_location_prefix", pd.Timestamp("2012-06-01")],
    [os.path.join(path_to_test_dir, "s3YahooDH", "test2.csv"),
     "raw_data_pull.Yahoo.PriceAndCA.price_data_location_prefix", pd.Timestamp("2011-01-01")],
    [os.path.join(path_to_test_dir, "s3YahooDH", "test2.csv"),
     "raw_data_pull.Yahoo.PriceAndCA.price_data_location_prefix", pd.Timestamp("2016-03-16 20:30:00")],
    [os.path.join(path_to_test_dir, "s3YahooDH", "testDiv1.csv"),
     "raw_data_pull.Yahoo.PriceAndCA.ca_data_location_prefix", pd.Timestamp("2016-03-16 20:30:00")]
]

output_files = {
    "yahoo_unadj": os.path.join(path_to_test_dir, "s3YahooDH", "output_prices2.csv"),
    "full_adjustment": os.path.join(path_to_test_dir, "s3YahooDH", "yahoo_price2.csv"),
    "split_adjustment": os.path.join(path_to_test_dir, "SplitAndDividendEquityAdjuster",
                                     "make_adjustment", "split_out.csv"),
}

test_data = [
    ("NKE", {
        "start_date": pd.Timestamp("2000-01-01"),
        "end_date": pd.Timestamp("2015-08-04 23:59:00"),
        "as_of_date": pd.Timestamp("2016-04-16 23:00:00"),
        "input_files_and_keys": input_files_and_keys,
        "output_local_file": output_files}),
    ("JWA", {
        "start_date": pd.Timestamp("2015-12-31"),
        "end_date": pd.Timestamp("2016-02-16"),
        "as_of_date": pd.Timestamp("2016-02-18 21:00:00"),
        "input_files_and_keys": input_files_and_keys,
        "output_local_file": output_files})
]


@pytest.mark.parametrize("ticker, data_config", test_data)
def test_S3YahooEquityDataHolder(ticker, data_config, setup_s3, patched_config, mock):
    """ Verifies S3YahooEquityDataHolder correctly identifies the most recent file based on as_of_date input
    for the given security.

    Args:
    -----
       ticker: str
          Identity id of security
       data_config: dic
          A dictionary containing configuration settings
    """

    s3_resource = setup_s3  # get s3_resource
    bucket_id = get_config()["raw_data_pull"]["base_data_bucket"]

    start_date = data_config["start_date"]
    end_date = data_config["end_date"]
    as_of_date = data_config["as_of_date"]

    # Create and populate mocked S3 bucket with corporate actions file
    for record in data_config["input_files_and_keys"]:
        # If end_date is not provided in input_files_and_keys -> use end_date = data_config["end_date"]
        if len(record) == 3:
            path_to_input, data_location_prefix, end_dt = record
        else:
            (path_to_input, data_location_prefix), end_dt = record, end_date
        s3_key = prepare_aws_s3_key(ticker, get_config()[data_location_prefix], end_dt)
        body = read_file(path_to_input, header=None, sep="^" if "TickData.CA" in data_location_prefix else ",")
        create_s3_bucket_and_put_object(s3_resource, bucket_id, s3_key, body)

    mock.patch("pipelines.prices_ingestion.equity_data_holders.storage.Client", return_value=s3_resource)
    mock.patch("pipelines.prices_ingestion.corporate_actions_holder.storage.Client", return_value=s3_resource)

    # Call Function
    yahoo_ca_dh = S3YahooCorporateActionsHolder(ticker, run_date=end_date, as_of_date=as_of_date)
    yahoo_dh = S3YahooEquityDataHolder(ticker, corporate_actions_holder=yahoo_ca_dh,
                                       start_date=start_date, end_date=end_date,
                                       as_of_date=as_of_date)

    assert yahoo_dh.bucket_id == bucket_id
    assert yahoo_dh.get_ticker() == ticker
    assert yahoo_dh.adjustment_unix_timestamp >= end_date
    assert yahoo_dh.data_location_prefix == get_config()["raw_data_pull.Yahoo.PriceAndCA.price_data_location_prefix"]
    assert set(yahoo_dh.get_data().columns.values) == {OpenLabel, HighLabel, LowLabel, CloseLabel, VolumeLabel}
    assert yahoo_dh.daily_frequency_flag is True
    assert yahoo_dh.is_data_adjusted() is False


@pytest.mark.parametrize("ticker, data_config", test_data)
def test_S3YahooDH_frames(ticker, data_config, setup_s3, patched_config, mock):
    """ Compares the expected with the actual outcome from S3YahooEquityDataHolder.

        Args:
        -----
           ticker: str
              Identity id of security
           data_config: dic
              A dictionary containing configuration settings
    """

    s3_resource = setup_s3  # get s3_resource
    bucket_id = get_config()["raw_data_pull"]["base_data_bucket"]

    start_date = data_config["start_date"]
    end_date = data_config["end_date"]
    as_of_date = data_config["as_of_date"]

    # Create and populate mocked S3 bucket with corporate actions file
    for record in data_config["input_files_and_keys"]:
        # If end_date is not provided in input_files_and_keys -> use end_date = data_config["end_date"]
        if len(record) == 3:
            path_to_input, data_location_prefix, end_dt = record
        else:
            (path_to_input, data_location_prefix), end_dt = record, end_date
        s3_key = prepare_aws_s3_key(ticker, get_config()[data_location_prefix], end_dt)
        body = read_file(path_to_input, header=None, sep="^" if "TickData.CA" in data_location_prefix else ",")
        create_s3_bucket_and_put_object(s3_resource, bucket_id, s3_key, body)

    mock.patch("pipelines.prices_ingestion.equity_data_holders.storage.Client", return_value=s3_resource)

    # Call Function
    yahoo_ca_dh = S3YahooCorporateActionsHolder(ticker, run_date=end_date, as_of_date=as_of_date)
    yahoo_dh = S3YahooEquityDataHolder(ticker, corporate_actions_holder=yahoo_ca_dh,
                                       start_date=start_date, end_date=end_date,
                                       as_of_date=as_of_date)

    test_unadj_price = yahoo_dh.get_data()
    test_full_adj = yahoo_dh.get_full_multiplicative_adjustment_factor()
    test_split_adj = yahoo_dh.get_split_multiplicative_adjustment_factor()

    # Reading expected unadjusted yahoo price data and compare them with the observed test
    expected_unadj_price = pd.read_csv(data_config["output_local_file"]["yahoo_unadj"],
                                       parse_dates=["Date"], index_col=["Date"]).sort_index()
    expected_unadj_price = expected_unadj_price[(expected_unadj_price.index >= data_config["start_date"]) &
                                                (expected_unadj_price.index <= data_config["end_date"])]
    assert_frame_equal(test_unadj_price, expected_unadj_price)

    # Reading expected yahoo full adjustment coefficients and compare them with the observed test
    expected_full_adj = pd.read_csv(data_config["output_local_file"]["full_adjustment"], parse_dates=["Date"],
                                    index_col=["Date"]).sort_index()
    expected_full_adj["Full_adj"] = expected_full_adj["Close"] / expected_full_adj["Adj Close"]
    expected_full_adj = expected_full_adj.loc[(expected_full_adj.index >= data_config["start_date"]) &
                                              (expected_full_adj.index <= data_config["end_date"]), "Full_adj"]
    expected_full_adj.name = None
    assert_series_equal(test_full_adj, expected_full_adj)
    
    # Reading expected yahoo split adjustment coefficients and compare them with the observed test
    expected_split_adj = pd.read_csv(data_config["output_local_file"]["split_adjustment"],
                                     parse_dates=[0], index_col=[0], names=["Date", "Value"])
    expected_split_adj = expected_split_adj.loc[(expected_split_adj.index >= data_config["start_date"]) &
                                                (expected_split_adj.index <= data_config["end_date"]), "Value"]
    assert_series_equal(test_split_adj, expected_split_adj)
