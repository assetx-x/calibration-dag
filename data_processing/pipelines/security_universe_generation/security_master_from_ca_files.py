from __future__ import print_function

import sys
import traceback
import pandas as pd

from os import path

from fuzzywuzzy import process
#from pyspark import SparkContext
from commonlib.market_timeline import marketTimeline
from pipeline_util import convert_cusip_to_nine_digits
from commonlib.util_functions import create_directory_if_does_not_exists
from commonlib.security_master_utils.identifier_utils import convert_cusip_to_isin
from pipelines.prices_ingestion.etl_workflow_aux_functions import get_redshift_engine
from pipelines.prices_ingestion.corporate_actions_holder import S3TickDataCorporateActionsHolder


EXCHANGES_INFO = pd.read_csv('https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv', encoding='ISO-8859-1', header=0)
STANDARD_FIELDS = {"Currency" : "USD", "Country" : "US", "EquityType": "CommonStock", "SecurityType": "Equity"}


def exchange_to_mic(exchange_name):
    us_exchanges = EXCHANGES_INFO[(EXCHANGES_INFO["ISO COUNTRY CODE (ISO 3166)"] == "US")][
        "NAME-INSTITUTION DESCRIPTION"]
    exchange_mic = EXCHANGES_INFO.loc[
        process.extract(exchange_name, us_exchanges.to_dict(), limit=1)[0][-1], "OPERATING MIC"]
    return exchange_mic


def extract_info_from_ticker(run_dt, symbol):
    try:
        tick_ca = S3TickDataCorporateActionsHolder(symbol, run_date=run_dt, as_of_date=pd.Timestamp.now("UTC"),
                                                   start_dt=pd.Timestamp.today("UTC"), end_dt=pd.Timestamp.today("UTC"))
        ca_df = tick_ca.get_ca_information("FOOTER")[["SYMBOL", "EXCHANGE", "NAME", "CUSIP"]].T.iloc[:, 0]
        ca_dict = ca_df.to_dict()
        ca_dict.update(STANDARD_FIELDS)
        ca_dict["Ticker"] = ca_dict["SYMBOL"]
        return ca_dict
    except BaseException as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        formatted_tb = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print("Ticker {} encountered error {}. Traceback: {}".format(symbol, e, formatted_tb))


def acquire_corporate_action(run_dt, tickers):
    sc = SparkContext("local[10]", "test")
    tickers_rdd = sc.parallelize(tickers)
    info = tickers_rdd.map(lambda x: extract_info_from_ticker(run_dt, x))
    all_ca_data = info.collect()
    sc.stop()
    return all_ca_data


def build_master_security(all_ca_data):
    desired_columns = ["Symbol", "SecurityType", "Ticker", "Description", "ExchangeMIC", "Currency", "Country",
                       "EquityType", "Cusip", "ISIN"]
    if all_ca_data:
        all_ca_data_df = pd.DataFrame(filter(lambda x:x is not None, all_ca_data))
        all_ca_data_df["CUSIP"] = all_ca_data_df["CUSIP"].apply(convert_cusip_to_nine_digits)
        all_ca_data_df["ISIN"] = all_ca_data_df["CUSIP"].apply(convert_cusip_to_isin)
        all_ca_data_df["ExchangeMIC"] = all_ca_data_df["EXCHANGE"].apply(exchange_to_mic)
        all_ca_data_df.rename(columns={"SYMBOL": "Symbol", "NAME":"Description", "CUSIP":"Cusip"}, inplace=True)
        all_ca_data_df.drop("EXCHANGE", axis=1, inplace=True)
        return all_ca_data_df[desired_columns]
    else:
        return pd.DataFrame(columns=desired_columns)


def save_security_master_data(security_master_df, output_path):
    output_dir = path.dirname(output_path)
    if not path.exists(output_dir):
        create_directory_if_does_not_exists(output_dir)
    print("Output will be created in {0}".format(output_path))
    security_master_df.to_csv(output_path, index=False)


def get_ticker_list(run_dt):
    with get_redshift_engine().begin() as engine:
        query_result = engine.execute("select ticker from whitelist where as_of_end is null and run_dt = %s;", run_dt)
        tickers = [k.values()[0] for k in query_result]
        return tickers


def _main():
    output_path = "." if len(sys.argv)<2 else sys.argv[1]
    output_path = path.realpath(output_path if path.isfile(output_path) else path.join(output_path, "master_security.csv"))
    run_dt = marketTimeline.get_trading_day_using_offset(pd.Timestamp.today("US/Eastern"), -1)
    tickers = get_ticker_list(run_dt)
    all_ca_data = acquire_corporate_action(run_dt, tickers)
    security_master_df = build_master_security(all_ca_data)
    save_security_master_data(security_master_df, output_path)


if __name__ == '__main__':
    _main()
