import os
import re
import pytest
import numpy as np
import pandas as pd

from etl_workflow_steps import StatusType
from pandas.testing import assert_frame_equal
from etl_workflow_steps import EquitiesETLTaskParameters
from pipelines.prices_ingestion.config import DateLabel, get_config
from pipelines.prices_ingestion.equity_data_holders import RedshiftRawTickDataDataHolder
from pipelines.prices_ingestion.data_retrieval_workflow_steps import TickDataFullHistoryDataRetriever
from conftest import read_file, create_s3_bucket_and_put_object, prepare_aws_s3_key, patched_config, \
    setup_s3, setup_redshift

path_to_test_dir = os.path.join(os.path.dirname(__file__), "testing_aux_files", "TickDataFullHistoryDataRetriever")


# -------------------------------------------------------------------
# Auxiliary functions
# -------------------------------------------------------------------

def get_expected_raw_equity_prices(actual_data, expected_symbol_history):
    expected_columns = ["open", "high", "low", "close", "volume", "cob"]
    expected_raw_equity_prices = []
    for ticker, df in actual_data.groupby("ticker"):
        start_dt, end_dt = expected_symbol_history[ticker]
        norm_cob = df["cob"].apply(lambda x: x.normalize())
        expected_raw_equity_prices.append(df.loc[(norm_cob >= start_dt) & (norm_cob <= end_dt), expected_columns])
    expected_raw_equity_prices = pd.concat(expected_raw_equity_prices, axis=0).set_index("cob").sort_index()
    expected_raw_equity_prices.rename(columns=RedshiftRawTickDataDataHolder.DICT_COLUMNS, inplace=True)
    expected_raw_equity_prices.index.name = DateLabel
    expected_raw_equity_prices.index = expected_raw_equity_prices.index.tz_localize("US/Eastern")
    return expected_raw_equity_prices


# -------------------------------------------------------------------
# TickDataFullHistoryDataRetriever
# -------------------------------------------------------------------

test_data = [
    ("JPM", {
        "start_dt": pd.Timestamp("2000-01-03"),
        "end_dt": pd.Timestamp("2019-01-07"),
        "run_dt": pd.Timestamp("2019-01-07"),
        "as_of_dt": pd.Timestamp("2019-01-08 09:00:00"),
        "ca_file": os.path.join(path_to_test_dir, "JPM_CA_1546927321.csv"),
        "raw_equity_price_file": os.path.join(path_to_test_dir, "JPM_raw_equity_prices_data.csv"),
        "expected_ticker_symbol_history": {"CMB": [pd.Timestamp("2000-01-03"), pd.Timestamp("2000-12-29")],
                                           "JPM": [pd.Timestamp("2001-01-02"), pd.Timestamp("2019-01-07")]}}),
    ("HSBC", {
        "start_dt": pd.Timestamp("2000-01-03"),
        "end_dt": pd.Timestamp("2019-01-07"),
        "run_dt": pd.Timestamp("2019-01-07"),
        "as_of_dt": pd.Timestamp("2019-01-08 09:00:00"),
        "ca_file": os.path.join(path_to_test_dir, "HSBC_CA_1546927700.csv"),
        "raw_equity_price_file": os.path.join(path_to_test_dir, "HSBC_raw_equity_prices_data.csv"),
        "expected_ticker_symbol_history": {"HBC": [pd.Timestamp("2000-01-03"), pd.Timestamp("2013-11-14")],
                                           "HSBC": [pd.Timestamp("2013-11-15"), pd.Timestamp("2019-01-07")]}}),
    ("AMN", {
        "start_dt": pd.Timestamp("2000-01-03"),
        "end_dt": pd.Timestamp("2019-01-07"),
        "run_dt": pd.Timestamp("2019-01-07"),
        "as_of_dt": pd.Timestamp("2019-01-08 09:00:00"),
        "ca_file": os.path.join(path_to_test_dir, "AMN_CA_1546928943.csv"),
        "raw_equity_price_file": os.path.join(path_to_test_dir, "AMN_raw_equity_prices_data.csv"),
        "expected_ticker_symbol_history": {"AHS": [pd.Timestamp("2001-11-13"), pd.Timestamp("2016-10-26")],
                                           "AMN": [pd.Timestamp("2016-10-27"), pd.Timestamp("2019-01-07")]}}),
    ("S", {
        "start_dt": pd.Timestamp("2000-01-03"),
        "end_dt": pd.Timestamp("2019-01-07"),
        "run_dt": pd.Timestamp("2019-01-07"),
        "as_of_dt": pd.Timestamp("2019-01-08 09:00:00"),
        "ca_file": os.path.join(path_to_test_dir, "S_CA_1546926428.csv"),
        "raw_equity_price_file": os.path.join(path_to_test_dir, "S_raw_equity_prices_data.csv"),
        "expected_ticker_symbol_history": {"SWD": [pd.Timestamp("2013-07-08"), pd.Timestamp("2013-07-11")],
                                           "S": [pd.Timestamp("2013-07-12"), pd.Timestamp("2019-01-07")]}})
]

def mock_prepare_multi_symbols_sql_query(raw_table, ticker_history_df, *timestamps):
    utc_start, utc_end, utc_as_of = timestamps

    query_header = """
        SELECT rep.open, rep.high, rep.low, rep.close, rep.volume, rep.cob
        FROM {raw_table} rep
        JOIN
        (SELECT ticker,cob,max(as_of) as as_of FROM {raw_table}
        """.format(raw_table=raw_table)

    query_body = ""
    for index, value in ticker_history_df.iterrows():
        symbol = value['Symbol']
        start_date = value['as_of_start'] if utc_start < value['as_of_start'] else utc_start
        end_date = value['as_of_end'] if value['as_of_end'] is not pd.NaT else pd.Timestamp.today('UTC').normalize()
        end_date = end_date if utc_end > end_date else utc_end

        if index == 0:
            query_body += """
                WHERE (ticker='{symbol}' and as_of <= '{as_of_date}' and date(cob) BETWEEN '{start_date}' and '{end_date}')
                """.format(symbol=symbol, as_of_date=utc_as_of, start_date=start_date.date(), end_date=end_date.date())
        else:
            query_body += """
                OR (ticker='{symbol}' and as_of <= '{as_of_date}' and date(cob) BETWEEN '{start_date}' and '{end_date}')
                """.format(symbol=symbol, as_of_date=utc_as_of, start_date=start_date.date(), end_date=end_date.date())

    query_footer = """
        GROUP BY ticker,cob) l
        ON
          rep.ticker =l.ticker AND
          rep.cob = l.cob AND
          rep.as_of = l.as_of
        WHERE rep.as_of <= '{as_of_date}' and date(rep.cob) BETWEEN '{start_date}' and '{end_date}';
        """.format(start_date=utc_start.date(),
                   end_date=utc_end.date(),
                   as_of_date=utc_as_of,
                   table=raw_table)

    query_composed = query_header + query_body + query_footer
    query = '\n'.join([line.strip() for line in query_composed.split('\n') if line.strip() != ''])
    return query

@pytest.fixture
def mock_query_creator(mocker, request):
    mocker.patch('pipelines.prices_ingestion.equity_data_holders.RedshiftRawTickDataDataHolder._prepare_multi_symbols_sql_query', side_effect=mock_prepare_multi_symbols_sql_query)
    request.addfinalizer(mocker.resetall)

@pytest.mark.parametrize("ticker, data_config", test_data)
def test_TickDataFullHistoryDataRetriever(ticker, data_config, patched_config, setup_s3, setup_redshift, mock, mock_query_creator):
    s3_resource = setup_s3  # get s3_resource
    engine = setup_redshift  # get sqlite engine

    bucket_id = get_config()["raw_data_pull"]["base_data_bucket"]
    ca_location_prefix = get_config()["raw_data_pull"]["TickData"]["CA"]["data_location_prefix"]
    raw_tickdata_table = get_config()["redshift"]["raw_tickdata_equity_price_table"]

    # Create and populate mocked S3 bucket with corporate actions file
    s3_key = prepare_aws_s3_key(ticker, ca_location_prefix, data_config["end_dt"])
    body = read_file(path_to_file=data_config["ca_file"], header=None, sep="^")
    create_s3_bucket_and_put_object(s3_resource, bucket_id, s3_key, body)

    # Create and populate mocked Redshift table with raw equity prices
    actual_data = pd.read_csv(data_config["raw_equity_price_file"], parse_dates=["cob", "as_of", "import_time"]).dropna()
    actual_data.to_sql(raw_tickdata_table, engine, index=False)

    mock.patch("pipelines.prices_ingestion.equity_data_holders.RedshiftEquityDataHolderBaseClass.get_engine",
               return_value=engine)
    mock.patch("pipelines.prices_ingestion.data_retrieval_workflow_steps.translate_ticker_name",
               return_value=ticker.replace('[^a-zA-Z]', ''))

    mock.patch("pipelines.prices_ingestion.etl_workflow_aux_functions.storage.Client", return_value=s3_resource)
    mock.patch("pipelines.prices_ingestion.corporate_actions_holder.storage.Client", return_value=s3_resource)

    # Test of TickDataFullHistoryDataRetriever Step
    tickdata_price_retriever = TickDataFullHistoryDataRetriever()
    task_params = EquitiesETLTaskParameters(ticker=ticker, as_of_dt=data_config["as_of_dt"],
                                            run_dt=data_config["run_dt"], start_dt=data_config["start_dt"],
                                            end_dt=data_config["end_dt"])
    tickdata_price_retriever.task_params = task_params
    results = tickdata_price_retriever.do_step_action()
    assert results == StatusType.Success

    # Ticker symbol change history and raw equity prices from TickDataFullHistoryDataRetriever
    test_raw_equity_prices = tickdata_price_retriever.tickdata_price_data.get_data_holder().get_data()
    test_query = tickdata_price_retriever.tickdata_price_data.get_data_holder().get_data_lineage()
    test_symbol_history = re.findall(r"ticker=\'(.+?)\'.+BETWEEN \'(.+?)\' and \'(.+?)\'", test_query)
    test_symbol_history = {record[0]: list(map(lambda x: pd.Timestamp(x), record[1:])) for record in test_symbol_history}

    # Expected ticker symbol change history and raw equity prices table
    expected_symbol_history = data_config["expected_ticker_symbol_history"]
    expected_raw_equity_prices = get_expected_raw_equity_prices(actual_data, expected_symbol_history)

    assert_frame_equal(test_raw_equity_prices, expected_raw_equity_prices)
    np.testing.assert_equal(test_symbol_history, expected_symbol_history)
