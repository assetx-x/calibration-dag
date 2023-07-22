import os
import pytest
import pandas as pd

from datetime import datetime
from status_objects import StatusType
from etl_workflow_steps import EquitiesETLTaskParameters, _BASE_FLOW_GLOBAL_PARAMETERS_LABEL
from conftest import DummyDataHolder, DummyCorporateActionsHolder
from pipelines.prices_ingestion.data_retrieval_workflow_steps import DATA_YAHOO_PRICE_LABEL, \
    DATA_YAHOO_CORPORATE_ACTION_LABEL, DATA_TICKDATA_PRICE_HANDLER_LABEL, DATA_TICKDATA_ONEDAY_PRICE_LABEL, \
    DataHolderHandlerForAdjustment
from pipelines.prices_ingestion.qa_workflow_steps import TickDataSanityCheckQA, YahooDailyPriceDataSanityCheckQA, \
    TickDataCorporateActionsPreviousDayStartDateComparisonQA, TickDataCorporateActionsPreviousDayContentComparisonQA, \
    TickDataCorporateActionsFirstDayStartDateComparisonQA, TickDataCorporateActionsFirstDayContentComparisonQA, \
    TickDataCorporateActionsRunDateQA, TickdataFullHistorySanityCheckQA, YahooDailyPriceVerifyTimeLine, \
    YahooDailyPricesDataPreviousDayComparisonQA, YahooPriceAndCorporateActionsDateComparison
from pipelines.prices_ingestion.config import DateLabel, OpenLabel, HighLabel, LowLabel, CloseLabel, VolumeLabel, \
    get_config

TICKDATA_CORPORATE_ACTION_LABEL = \
    get_config()["etl_equities_process_labels"]["TICKDATA_CORPORATE_ACTION_LABEL"]
TICKDATA_CORPORATE_ACTION_PREVIOUS_DATE_LABEL = \
    get_config()["etl_equities_process_labels"]["TICKDATA_CORPORATE_ACTION_PREVIOUS_DATE_LABEL"]
TICKDATA_CORPORATE_ACTION_FIRST_DATE_LABEL = \
    get_config()["etl_equities_process_labels"]["TICKDATA_CORPORATE_ACTION_FIRST_DATE_LABEL"]
DATA_YAHOO_PRICE_PREVIOUS_DATE_LABEL = \
    get_config()["etl_equities_process_labels"]["DATA_YAHOO_PRICE_PREVIOUS_DATE_LABEL"]


# -------------------------------------------------------------------
# Auxiliary functions [for TickData]
# -------------------------------------------------------------------

def get_fake_data_sub_path(filename, sub_path, data_type=None, set_ca_index=True):
    filename = (os.path.join(os.path.dirname(__file__), r"testing_aux_files", sub_path, filename))
    if data_type is None:
        return pd.read_csv(filename, delimiter=",", names=["Day", "Time", OpenLabel, HighLabel, LowLabel, CloseLabel,
                                                           VolumeLabel])
    elif data_type == "s3tickdataMinute":
        return pd.read_csv(filename, delimiter=",", names=["Day", "Time", OpenLabel, HighLabel, LowLabel,
                                                           CloseLabel, VolumeLabel],
                           parse_dates={DateLabel : [0, 1]}, index_col=DateLabel)
    elif data_type == "s3YahooDaily":
        return pd.read_csv(filename, delimiter=",", parse_dates=[DateLabel], index_col=DateLabel)
    elif data_type == "s3YahooCa":
        if set_ca_index:
            df = pd.read_csv(filename, parse_dates=["Date"])
            for col in ["EX_DATE", "EFFECTIVE DATE", "DATE"]:  # needed for DIVIDEND & SPINOFF corporate actions
                df[col] = df["Date"]
            # TODO: to check that this data is used by Yahoo CA data holder and not a TickData
            mapper = {"STARTDATE": "HEADER", "ENDDATE": "FOOTER"}
            df["Action"] = df["Action"].apply(lambda x: mapper[x] if x in mapper else x)
            df.set_index("Date", inplace=True)
            df.columns = df.columns.str.upper()
            return df.sort_values(["DATE", "ACTION"])
        else:
            return pd.read_csv(filename, parse_dates=["Date"])


def get_fake_vt_data(filename, data_type, set_ca_index=True):
    return get_fake_data_sub_path(filename=filename, sub_path="validationToolset", 
                                  data_type=data_type, set_ca_index=set_ca_index)


# -------------------------------------------------------------------
# TickdataSanityCheckQA
# -------------------------------------------------------------------

test_data = \
    [
        ("AAPL", "assumed_good_AAPL_1468030892.csv", "assumed_good_ref_AAPL_1468015555.csv",
         datetime(2016, 7, 8), StatusType.Success),
        ("LMT", "assumed_good_LMT_1468031199.csv", "assumed_good_ref_LMT_146801596.csv",
         datetime(2016, 7, 8), StatusType.Success),
        ("MCD", "assumed_good_MCD_1468031265.csv", "assumed_good_ref_MCD_1468015988.csv",
         datetime(2016, 7, 8), StatusType.Success),
        ("JPM", "assumed_good_JPM_1468030761.csv", "assumed_good_ref_JPM_1468015931.csv",
         datetime(2016, 7, 8), StatusType.Success),
        ("WMT", "assumed_good_WMT_1468031238.csv", "assumed_good_ref_WMT_1468016287.csv",
         datetime(2016, 7, 8), StatusType.Success),
        ("BAC", "assumed_good_BAC_1468031290.csv", "assumed_good_ref_BAC_1468015626.csv",
         datetime(2016, 7, 8), StatusType.Success)
    ]


@pytest.mark.parametrize("ticker, df_csv, ref_csv, date, expected_type", test_data)
def test_TickdataSanityCheckQA(ticker, df_csv, ref_csv, date, expected_type):
    df = get_fake_vt_data(df_csv, data_type="s3tickdataMinute")
    df_ref = get_fake_vt_data(ref_csv, data_type="s3YahooDaily")
    df_ref["Close"] = df_ref["Adj Close"]
    df_ref.drop("Adj Close", axis=1, inplace=True)

    tick_dhh = DataHolderHandlerForAdjustment(DummyDataHolder(ticker, start_date=date, end_date=date,
                                                              as_of_date=date, df=df))
    yahoo_dh = DummyDataHolder(ticker, start_date=df_ref.index[0], end_date=df_ref.index[-1],
                               as_of_date=date, df=df_ref)
    kwargs = {DATA_TICKDATA_ONEDAY_PRICE_LABEL: tick_dhh, DATA_YAHOO_PRICE_LABEL: yahoo_dh}
    qa = TickDataSanityCheckQA()
    actual = qa.do_step_action(**kwargs)
    assert actual is expected_type


# -------------------------------------------------------------------
# YahooDailyPriceDataSanityCheckQA
# -------------------------------------------------------------------

test_data = \
    [
        ("BAC", "assumed_good_ref_BAC_1468015626.csv", datetime(2016, 7, 8), StatusType.Success),
        ("WMT", "assumed_good_ref_WMT_1468016287.csv", datetime(2016, 7, 8), StatusType.Success),
        ("LMT", "assumed_good_ref_LMT_146801596.csv", datetime(2016, 7, 8), StatusType.Success),
        ("MCD", "assumed_good_ref_MCD_1468015988.csv", datetime(2016, 7, 8), StatusType.Success),
        ("JPM", "assumed_good_ref_JPM_1468015931.csv", datetime(2016, 7, 8), StatusType.Success),
        ("AAPL", "assumed_good_ref_AAPL_1468015555.csv", datetime(2016, 7, 8), StatusType.Success)
    ]


@pytest.mark.parametrize("ticker, df_csv, date, expected_type", test_data)
def test_YahooDailyPriceDataSanityCheckQA(ticker, df_csv, date, expected_type):
    df = get_fake_vt_data(df_csv, data_type="s3YahooDaily").sort_index()
    df["Close"] = df["Adj Close"]
    df.drop("Adj Close", axis=1, inplace=True)

    yahoo_dh = DummyDataHolder(ticker, start_date=df.index[0], end_date=df.index[-1], as_of_date=date, df=df)
    kwargs = {DATA_YAHOO_PRICE_LABEL: yahoo_dh}
    qa = YahooDailyPriceDataSanityCheckQA()
    actual = qa.do_step_action(**kwargs)
    assert actual is expected_type


# -------------------------------------------------------------------
# CorporateActions_qa
# -------------------------------------------------------------------

test_data = \
    [
        ("A", "assume_good_ca_A_1465142248.csv", "assume_good_prev_ca_A_1464992646.csv",
         "assume_good_origin_ca_A.csv", datetime(2016, 6, 3), StatusType.Success)
    ]


@pytest.mark.parametrize("ticker, df_csv, pre_ca_ref_csv, origin_ca_ref_csv, date, expected_type", test_data)
def test_CorporateActions_qa(ticker, df_csv, pre_ca_ref_csv, origin_ca_ref_csv, date, expected_type):
    df = get_fake_vt_data(df_csv, data_type="s3YahooCa", set_ca_index=True)
    prev_ref_ca_df = get_fake_vt_data(pre_ca_ref_csv, data_type="s3YahooCa", set_ca_index=True)
    original_ref_ca_df = get_fake_vt_data(origin_ca_ref_csv, data_type="s3YahooCa", set_ca_index=True)

    dh = DummyCorporateActionsHolder(ticker, df=df, run_date=date, as_of_date=date)
    prev_ref_ca_dh = DummyCorporateActionsHolder(ticker, df=prev_ref_ca_df, run_date=date, as_of_date=date)
    original_ref_ca_dh = DummyCorporateActionsHolder(ticker, df=original_ref_ca_df, run_date=date, as_of_date=date)

    qa = TickDataCorporateActionsPreviousDayStartDateComparisonQA()
    kwargs = {TICKDATA_CORPORATE_ACTION_LABEL: dh, TICKDATA_CORPORATE_ACTION_PREVIOUS_DATE_LABEL: prev_ref_ca_dh}
    actual = qa.do_step_action(**kwargs)
    assert actual is expected_type

    qa = TickDataCorporateActionsFirstDayStartDateComparisonQA()
    kwargs = {TICKDATA_CORPORATE_ACTION_LABEL: dh, TICKDATA_CORPORATE_ACTION_FIRST_DATE_LABEL: original_ref_ca_dh}
    actual = qa.do_step_action(**kwargs)
    assert actual is expected_type

    # TODO: add these tests at later stage
    # qa = TickDataCorporateActionsPreviousDayContentComparisonQA()
    # actual = qa.do_step_action(**kwargs)
    # assert actual is expected_type

    # qa = TickDataCorporateActionsFirstDayContentComparisonQA()
    # actual = qa.do_step_action(**kwargs)
    # assert actual is expected_type

    qa = TickDataCorporateActionsRunDateQA()
    task_params = EquitiesETLTaskParameters(ticker=ticker, as_of_dt=date, run_dt=date, start_dt=date, end_dt=date)
    qa.set_task_params(**{_BASE_FLOW_GLOBAL_PARAMETERS_LABEL:task_params})
    kwargs = {TICKDATA_CORPORATE_ACTION_LABEL: dh}
    actual = qa.do_step_action(**kwargs)
    assert actual is expected_type


# -------------------------------------------------------------------
# TickdataFullHistorySanityCheckQA
# -------------------------------------------------------------------

test_data = \
    [
        ("AA", "AA_expected_output.csv", "AA_yahoo.csv", datetime(2004, 1, 6), StatusType.Success)
    ]


@pytest.mark.parametrize("ticker, df_csv, ref_csv, date, expected_type", test_data)
def test_TickdataFullHistorySanityCheckQA(ticker, df_csv, ref_csv, date, expected_type):
    df = get_fake_data_sub_path(df_csv, sub_path="CompleteMinutes", data_type="s3YahooDaily")
    df_ref = get_fake_data_sub_path(ref_csv, sub_path="CompleteMinutes", data_type="s3YahooDaily")
    df_ref["Close"] = df_ref["Adj Close"]
    df_ref.drop("Adj Close", axis=1, inplace=True)
    df_ref = df_ref.ix[date:]

    tick_dhh = DataHolderHandlerForAdjustment(DummyDataHolder(ticker, start_date=date, end_date=date,
                                                              as_of_date=date, df=df))
    yahoo_dh = DummyDataHolder(ticker, start_date=df_ref.index[0], end_date=df_ref.index[-1],
                               as_of_date=date, df=df_ref)
    kwargs = {DATA_TICKDATA_PRICE_HANDLER_LABEL: tick_dhh, DATA_YAHOO_PRICE_LABEL: yahoo_dh}
    qa = TickdataFullHistorySanityCheckQA()
    actual = qa.do_step_action(**kwargs)
    assert actual is expected_type


# -------------------------------------------------------------------
# YahooDailyPriceVerifyTimeLine
# -------------------------------------------------------------------

test_data = \
    [
        ("BAC", "assumed_good_ref_BAC_1468015626.csv", datetime(2016, 7, 8), StatusType.Success)
    ]


@pytest.mark.parametrize("ticker, df_csv, date, expected_type", test_data, ids=[])
def test_YahooDailyPriceVerifyTimeLine(ticker, df_csv, date, expected_type):
    df = get_fake_vt_data(df_csv, data_type="s3YahooDaily").sort_index()
    df["Close"] = df["Adj Close"]
    df.drop("Adj Close", axis=1, inplace=True)
    df = df[df.index >= get_config()["adjustments"]["first_relevant_date_for_all_tests"]]

    yahoo_dh = DummyDataHolder(ticker, start_date=df.index[0], end_date=df.index[-1], as_of_date=date, df=df)
    kwargs = {DATA_YAHOO_PRICE_LABEL: yahoo_dh}
    qa = YahooDailyPriceVerifyTimeLine()
    actual = qa.do_step_action(**kwargs)
    assert actual is expected_type


# -------------------------------------------------------------------
# YahooDailyPricesDataPreviousDayComparisonQA
# -------------------------------------------------------------------

test_data = \
    [
        ("BAC", "assumed_good_ref_BAC_1468015626.csv", "assume_good_ref_prev_BAC_1467929223.csv",
         datetime(2016, 7, 8), StatusType.Success),
        ("WMT", "assumed_good_ref_WMT_1468016287.csv", "assume_good_ref_prev_WMT_1467929852.csv",
         datetime(2016, 7, 8), StatusType.Warn)
    ]


@pytest.mark.parametrize("ticker, df_csv, pre_ref_csv, date, expected_type", test_data)
def test_YahooDailyPricesDataPreviousDayComparisonQA(ticker, df_csv, pre_ref_csv, date, expected_type):
    df = get_fake_vt_data(df_csv, data_type="s3YahooDaily").sort_index()
    df["Close"] = df["Adj Close"]
    df.drop("Adj Close", axis=1, inplace=True)

    yahoo_dh = DummyDataHolder(ticker, start_date=df.index[0], end_date=df.index[-1], as_of_date=date, df=df)
    pre_ref_df = get_fake_vt_data(pre_ref_csv, data_type="s3YahooDaily").sort_index()
    pre_ref_df["Close"] = pre_ref_df["Adj Close"]
    pre_ref_df.drop("Adj Close", axis=1, inplace=True)
    pre_yahoo_dh = DummyDataHolder(ticker, start_date=pre_ref_df.index[0], end_date=pre_ref_df.index[-1],
                                   as_of_date=date, df=pre_ref_df)

    kwargs = {DATA_YAHOO_PRICE_LABEL: yahoo_dh, DATA_YAHOO_PRICE_PREVIOUS_DATE_LABEL: pre_yahoo_dh}
    qa = YahooDailyPricesDataPreviousDayComparisonQA()
    task_params = EquitiesETLTaskParameters(ticker=ticker, as_of_dt=date, run_dt=date, start_dt=date, end_dt=date)
    qa.set_task_params(**{_BASE_FLOW_GLOBAL_PARAMETERS_LABEL:task_params})
    actual = qa.do_step_action(**kwargs)
    assert actual is expected_type


# -------------------------------------------------------------------
# YahooPriceandCorporateActionsDateComparison
# -------------------------------------------------------------------

test_data = \
    [
        ("BAC", "assumed_good_ref_BAC_1468015626.csv", "assume_good_ca_BAC_1468015242.csv",
         datetime(2016, 7, 8), StatusType.Success),
        ("WMT", "assumed_good_ref_WMT_1468016287.csv", "assume_good_ca_WMT_1468015538.csv",
         datetime(2016, 7, 8), StatusType.Success)
    ]


@pytest.mark.parametrize("ticker, df_csv, ca_csv, date, expected_type", test_data)
def test_YahooPriceandCorporateActionsDateComparison(ticker, df_csv, ca_csv, date, expected_type):
    df = get_fake_vt_data(df_csv, data_type="s3YahooDaily").sort_index()
    df["Close"] = df["Adj Close"]
    df.drop("Adj Close", axis=1, inplace=True)
    yahoo_dh = DummyDataHolder(ticker, start_date=df.index[0], end_date=df.index[-1], as_of_date=date, df=df)

    df = get_fake_vt_data(ca_csv, data_type="s3YahooCa", set_ca_index=True)
    ca_dh = DummyCorporateActionsHolder(ticker, df=df, run_date=date, as_of_date=date)

    kwargs = {DATA_YAHOO_PRICE_LABEL: yahoo_dh, DATA_YAHOO_CORPORATE_ACTION_LABEL: ca_dh}
    qa = YahooPriceAndCorporateActionsDateComparison()
    task_params = EquitiesETLTaskParameters(ticker=ticker, as_of_dt=date, run_dt=date, start_dt=date, end_dt=date)
    qa.set_task_params(**{_BASE_FLOW_GLOBAL_PARAMETERS_LABEL:task_params})
    actual = qa.do_step_action(**kwargs)
    assert actual is expected_type
