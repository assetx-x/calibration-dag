import logging
import os
import time
import sys

import requests
import pandas as pd
import warnings
import zipfile

from dotenv import load_dotenv
from google.cloud import bigquery
from datetime import datetime, date

warnings.filterwarnings("ignore")

load_dotenv()

if sys.platform in ['darwin', 'linux']:
    """
    To log in into GCP locally use the following command:
    $ gcloud auth application-default login
    and follow the instructions, then the json will be created automatically
    """
    home_path = os.getenv('HOME')
    credentials_path = os.path.join(home_path, '.config/gcloud/application_default_credentials.json')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    logging.info(f'Credentials set to {os.environ["GOOGLE_APPLICATION_CREDENTIALS"]}')
else:
    raise ValueError('Only Linux is supported')

os.environ['GCS_BUCKET'] = 'dcm-prod-ba2f-us-dcm-data-test'

client = bigquery.Client()
table_name = 'dcm-prod-ba2f.marketdata.daily_equity_prices'

iso_format = '%Y-%m-%dT%H:%M:%S'

weekday_dict = {
    0: 'Monday',
    1: 'Tuesday',
    2: 'Wednesday',
    3: 'Thursday',
    4: 'Friday',
    5: 'Saturday',
    6: 'Sunday'
}


def get_security_id_from_ticker_mapper(ticker):
    if isinstance(ticker, str):
        ticker = [ticker]
    elif not isinstance(ticker, list):
        raise ValueError(
            "Tickers can only be of data type str or can be put into a list if multiple Tickers apply (i.e [AAPL,"
            "GOOGL,MSFT])"
        )
    ticker = ','.join([f"'{t}'" for t in ticker])
    sm_query = f"select ticker, dcm_security_id from marketdata.security_master_20230603 where ticker in ({ticker})"
    security_master_tickers = client.query(sm_query).to_dataframe()
    security_master_tickers.set_index("ticker", inplace=True)

    return security_master_tickers


def delta_date_to_isoformat(d):
    pd_date = pd.to_datetime(d)
    return pd_date.isoformat()


def get_tickers_to_carry_forward(d):
    if isinstance(d, str):
        d = datetime.strptime(d, iso_format)
        d = d.date()
    sm_query = f"select distinct ticker, dcm_security_id from marketdata.daily_equity_prices where date(date)='{d}'"
    return client.query(sm_query).to_dataframe()


def get_tickers_and_security_ids(d):
    sm_query = f"select distinct ticker from marketdata.daily_equity_prices where date='{d}'"
    security_master_tickers = (
        client.query(sm_query).to_dataframe()
    )
    return security_master_tickers


def get_last_date_in_table():
    return (
        client.query(f"select max(date) as max_date from marketdata.daily_equity_prices").to_dataframe()
    )


def download_tickers_list_by_date(start: str, end: str = None):
    nasdaq_api_key = os.getenv('NASDAQ_DATA_LINK_API_KEY')
    api_url = 'https://data.nasdaq.com/api/v3/datatables/SHARADAR/SEP.json'
    filename_zip = f'data_{start}.zip'.replace(" ", "")
    if not os.path.exists(filename_zip):
        args = {
            'date.gte': start,
            'date.lte': end,
            'qopts.columns': 'ticker,date,open,close,high,low,volume',
            'qopts.export': 'true',
            'api_key': nasdaq_api_key,
        }
        r = requests.get(api_url, params=args)
        if r.status_code != 200:
            logging.info(f'[!] Error: {r.status_code}')
            return None
        file_status = r.json().get('datatable_bulk_download').get('file').get('status')
        while file_status.lower() == 'creating':
            logging.info(f'[*] Waiting for file to be created for {start}. Status: "{file_status}"')
            r = requests.get(api_url, params=args)
            file_status = r.json().get('datatable_bulk_download').get('file').get('status')
            time.sleep(45)
        file_to_download_link = r.json().get('datatable_bulk_download').get('file').get('link')
        if file_to_download_link is None:
            logging.info(f'[!] No download link for {start}: \n{r.json()}')
            return None
        file_content = requests.get(file_to_download_link, allow_redirects=True, timeout=10).content
        if file_content == b'':
            logging.info(f'[!] No bin content for {start}')
            return None
        with open(filename_zip, 'wb') as f:
            f.write(file_content)
    else:
        logging.info(f'[*] File {filename_zip} already exists')

    with zipfile.ZipFile(filename_zip, 'r') as zip_ref:
        logging.info(f'[*] Extracting {filename_zip}')
        zip_ref.extractall()
        csv_files = zip_ref.namelist()
    if len(csv_files) == 0:
        logging.info(f'[!] No csv file for {start}')
        return None
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        if 'No Data' in str(df.head()):
            logging.info(f'    No data for {start}')
            os.remove(csv_file)
            continue
        yield df
        os.remove(csv_file)
        logging.info(f'[*] Removed {csv_file}')


def execute_query(head: str, queries: list, batch_size=1_000):
    try:
        logging.info(f'[*] Total rows to execute: {len(queries)}')
        for i in range(0, len(queries), batch_size):
            batch = queries[i:i + batch_size]
            q = head + ', '.join(batch)
            query_job = client.query(q)
            result = query_job.result()
            logging.info(f'[*] Executed rows {len(batch)}.')
    except Exception as e:
        logging.error('[!] Error:', type(e), e)


def is_updated(*args):
    args = list(args)
    for ix, d in enumerate(args):
        if isinstance(d, datetime):
            args[ix] = d.date()
    r = date.today() <= max(args)
    return r


def main():
    queries = []
    last_date_df = get_last_date_in_table()
    last_date = last_date_df['max_date'].iloc[0].date()

    def create_insert_query(r):
        tkr = r['ticker']
        sec_id = r['dcm_security_id']
        aoe = (datetime.strptime(r['date'], iso_format)).isoformat()
        aos = delta_date_to_isoformat(aoe)
        return f"('{tkr}','{r['date']}', {r['open']}, {r['close']}, {r['high']}, {r['low']}, {r['volume']}, '{aos}', '{aoe}', '{tkr}', {sec_id})"

    start_date = last_date
    end_date = datetime.today()
    if is_updated(last_date, end_date):
        logging.info(f'[*] Already updated to {end_date}')
        return
    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    logging.info(f'[*] Getting data from {start_date} to {end_date}')
    for ticker_df in download_tickers_list_by_date(start_date, end_date):
        logging.info(f'[*] Processing {ticker_df.shape} rows')
        ticker_df = ticker_df[ticker_df['ticker'].notna()]
        tickers_list = ticker_df['ticker'].unique().tolist()
        sec_id_df = get_security_id_from_ticker_mapper(tickers_list)
        ticker_df = ticker_df.merge(sec_id_df, left_on='ticker', right_index=True)
        ticker_df['date'] = ticker_df['date'].map(lambda x: datetime.strptime(x, '%Y-%m-%d').isoformat())

        for _, row in ticker_df.iterrows():
            insert_query = create_insert_query(row)
            queries.append(insert_query)

        if queries:
            logging.info(f'[*] Executing {len(queries)} rows into: {table_name}')
            q_head = (f"INSERT INTO {table_name} (ticker, date, open, close, high, low, volume, as_of_start, "
                      f"as_of_end, symbol_asof, dcm_security_id) VALUES")
            execute_query(q_head, queries)  # , batch_size=500)
            queries = []
            logging.info(f'[*] Queries len now {len(queries)}')


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.error('Interrupted')
