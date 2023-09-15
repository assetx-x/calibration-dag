import os

import yfinance as yf
import pandas as pd
import warnings
from google.cloud import bigquery
from datetime import datetime, timedelta
warnings.filterwarnings("ignore")

iso_format = '%Y-%m-%dT%H:%M:%S'

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/iklo/calibration-DAG/plugins/data_processing/dcm-prod.json'
os.environ['GCS_BUCKET'] = 'dcm-prod-ba2f-us-dcm-data-test'

client = bigquery.Client()
table_name = 'dcm-prod-ba2f.marketdata.daily_equity_prices'


def get_security_id_from_ticker_mapper(ticker):
    if isinstance(ticker, str):
        ticker = [ticker]
    elif not isinstance(ticker, list):
        raise ValueError(
            "Tickers can only be of data type str or can be put into a list if multiple Tickers apply (i.e [AAPL,"
            "GOOGL,MSFT])"
        )

    sm_query = f"select ticker, dcm_security_id from marketdata.security_master_20230603 where ticker in ({ticker})"
    security_master_tickers = client.query(sm_query).to_dataframe()
    security_master_tickers.set_index("ticker", inplace=True)

    return security_master_tickers


def delta_date_to_isoformat(date):
    pd_date = pd.to_datetime(date)
    return pd_date.isoformat()


def get_tickers_to_carry_forward(date):
    if isinstance(date, str):
        date = datetime.strptime(date, iso_format)
        date = date.date()
    sm_query = f"select distinct ticker, dcm_security_id from marketdata.daily_equity_prices where date(date)='{date}'"
    return client.query(sm_query).to_dataframe()


def get_tickers_and_security_ids(date):
    sm_query = f"select distinct ticker from marketdata.daily_equity_prices where date='{date}'"
    security_master_tickers = (
        client.query(sm_query).to_dataframe()
    )
    return security_master_tickers


def download_tickers(tickers, start, debug=False, n=None):
    print('Downloading from Yahoo Finance')
    if debug or n is not None:
        tickers = tickers[:n]
        print(f'Downloading only {n} tickers: {tickers}')
    tickers_data = yf.download(tickers, start=start)
    tickers_data.columns = tickers_data.columns.swaplevel(0, 1)
    tickers_data.index = tickers_data.index.map(lambda x: x.isoformat())
    return tickers_data


def execute_query(head, query):
    try:
        q = head + ', '.join(query)
        query_job = client.query(q)
        query_job.result()
    except Exception as e:
        print('[!] Error:', type(e), e)


def main():
    queries = []

    def create_insert_query(tkr, r, sec_id):
        aos = (datetime.strptime(r['date'], iso_format) + timedelta(days=1)).isoformat()
        aoe = delta_date_to_isoformat(aos)
        return f"('{tkr}','{r['date']}', {r['open']}, {r['close']}, {r['high']}, {r['low']}, {r['volume']}, '{aos}', '{aoe}', '{tkr}', {sec_id})"

    tickers_data = get_tickers_to_carry_forward('2023-07-18T00:00:00')
    available_tickers = tickers_data['ticker'].tolist()
    all_ticker_data = download_tickers(available_tickers, start='2023-07-19')

    for ticker in available_tickers:
        try:
            ticker_data = all_ticker_data.get(ticker)
            if ticker_data is None:
                raise KeyError(f'No data for {ticker}')
            ticker_data = ticker_data.reset_index()
            ticker_data.columns = ticker_data.columns.str.lower()
            sec_id_value = int(tickers_data.loc[tickers_data['ticker'] == ticker, 'dcm_security_id'].values[0])
        except KeyError as e:
            print(e, type(e))
            continue

        for _, row in ticker_data.iterrows():
            insert_query = create_insert_query(ticker, row, sec_id_value)
            print(f'Ticker: {ticker}, Date: {row["date"]}, Query: {insert_query}')
            queries.append(insert_query)

        if queries:
            q_head = f"INSERT INTO {table_name} (ticker, date, open, close, high, low, volume, as_of_start, as_of_end, symbol_asof, dcm_security_id) VALUES "
            execute_query(q_head, queries)
            queries = []


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
