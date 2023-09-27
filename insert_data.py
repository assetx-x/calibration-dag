import os
import time
import sys

import requests
import yfinance as yf
import pandas as pd
import warnings
import zipfile

from dotenv import load_dotenv
from google.cloud import bigquery
from datetime import datetime, timedelta


warnings.filterwarnings("ignore")

load_dotenv()


iso_format = '%Y-%m-%dT%H:%M:%S'

if sys.platform in ['darwin', 'linux']:
    home_path = os.getenv('HOME')
    credentials_path = os.path.join(home_path, '.config/gcloud/application_default_credentials.json')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    print(f'Credentials set to {os.environ["GOOGLE_APPLICATION_CREDENTIALS"]}')
else:
    raise ValueError('Only Linux is supported')

os.environ['GCS_BUCKET'] = 'dcm-prod-ba2f-us-dcm-data-test'

client = bigquery.Client()
# table_name = 'dcm-prod-ba2f.marketdata.daily_equity_prices'
table_name = 'dcm-prod-ba2f.marketdata.daily_equity_prices_nasdaq'

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


def download_tickers_yfinance(tickers, start, debug=False, n=None):
    print('Downloading from Yahoo Finance')
    if debug or n is not None:
        tickers = tickers[:n]
        print(f'Downloading only {n} tickers: {tickers}')
    tickers_data = yf.download(tickers, start=start)
    tickers_data.columns = tickers_data.columns.swaplevel(0, 1)
    tickers_data.index = tickers_data.index.map(lambda x: x.isoformat())
    return tickers_data


def download_tickers_list_by_date(start: str, end: str = None):
    nasdaq_api_key = os.getenv('NASDAQ_DATA_LINK_API_KEY')
    api_url = 'https://data.nasdaq.com/api/v3/datatables/SHARADAR/SEP.json'
    start = datetime.strptime(start, '%Y-%m-%d')
    if not end:
        end = datetime.today()
        print(f'[*] No end date provided. Using today: {end}')
    else:
        end = datetime.strptime(end, '%Y-%m-%d')
    dates = [start + timedelta(days=x) for x in range(0, (end - start).days)]
    dates = [x.strftime('%Y-%m-%d') for x in dates]
    dates.sort()
    filename_zip = f'data_{start}.zip'
    if not os.path.exists(filename_zip):
        print(f'[*] Downloading data from Nasdaq for {len(dates)} days. From {dates[0]} to {dates[-1]}')
        args = {
            'date.gte': start,
            'date.lte': end,
            'qopts.columns': 'ticker,date,open,close,high,low,volume',
            'qopts.export': 'true',
            'api_key': nasdaq_api_key,
        }
        r = requests.get(api_url, params=args)
        if r.status_code != 200:
            print(f'[!] Error: {r.status_code}')
            return None
        file_status = r.json().get('datatable_bulk_download').get('file').get('status')
        while file_status.lower() == 'creating':
            print(f'[*] Waiting for file to be created for {start}. Status: "{file_status}"')
            r = requests.get(api_url, params=args)
            file_status = r.json().get('datatable_bulk_download').get('file').get('status')
            time.sleep(1)
        file_to_download_link = r.json().get('datatable_bulk_download').get('file').get('link')
        if file_to_download_link is None:
            print(f'[!] No download link for {start}: \n{r.json()}')
            return None
        file_content = requests.get(file_to_download_link, allow_redirects=True, timeout=10).content
        if file_content == b'':
            print(f'[!] No bin content for {start}')
            return None
        with open(filename_zip, 'wb') as f:
            f.write(file_content)

    with zipfile.ZipFile(filename_zip, 'r') as zip_ref:
        zip_ref.extractall()
        csv_files = zip_ref.namelist()
    if len(csv_files) == 0:
        print(f'[!] No csv file for {start}')
        return None
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        if 'No Data' in str(df.head()):
            print(f'    No data for {start}')
            os.remove(csv_file)
            continue
        yield df
        os.remove(csv_file)
        print(f'[*] Removed {csv_file}')
    os.remove(filename_zip)
    print(f'[*] Removed {filename_zip}')


def download_tickers_by_date(start: str, end: str = None):
    nasdaq_api_key = os.getenv('NASDAQ_DATA_LINK_API_KEY')
    api_url = 'https://data.nasdaq.com/api/v3/datatables/SHARADAR/SEP.json'
    start = datetime.strptime(start, '%Y-%m-%d')
    if not end:
        end = datetime.today()
        print(f'[*] No end date provided. Using today: {end}')
    else:
        end = datetime.strptime(end, '%Y-%m-%d')
    dates = [start + timedelta(days=x) for x in range(0, (end - start).days)]
    dates = [x.strftime('%Y-%m-%d') for x in dates]
    dates.sort()
    print(f'[*] Downloading data from Nasdaq for {len(dates)} days. From {dates[0]} to {dates[-1]}')
    for d in dates:
        filename_zip = f'data_{d}.zip'
        if not os.path.exists(filename_zip):
            day_of_week = datetime.strptime(d, '%Y-%m-%d').weekday()
            day_of_week = weekday_dict.get(day_of_week)
            if day_of_week in ['Saturday', 'Sunday']:
                print(f'[*] Skipping {d}: {day_of_week}')
                continue
            print(f'[*] Downloading data for {d}: {day_of_week}')
            args = {
                'date': d,
                'qopts.columns': 'ticker,date,open,close,high,low,volume',
                'qopts.export': 'true',
                'api_key': nasdaq_api_key
            }
            r = requests.get(api_url, params=args)
            if r.status_code != 200:
                print(f'[!] Error: {r.status_code}')
                continue
            file_status = r.json().get('datatable_bulk_download').get('file').get('status')
            while file_status.lower() == 'creating':
                print(f'[*] Waiting for file to be created for {d}. Status: "{file_status}"')
                r = requests.get(api_url, params=args)
                file_status = r.json().get('datatable_bulk_download').get('file').get('status')
                time.sleep(1)
            file_to_download_link = r.json().get('datatable_bulk_download').get('file').get('link')
            if file_to_download_link is None:
                print(f'[!] No download link for {d}: \n{r.json()}')
                continue
            file_content = requests.get(file_to_download_link, allow_redirects=True, timeout=10).content
            if file_content == b'':
                print(f'[!] No bin content for {d}')
                continue
            with open(filename_zip, 'wb') as f:
                f.write(file_content)
        with zipfile.ZipFile(filename_zip, 'r') as zip_ref:
            zip_ref.extractall()
            csv_files = zip_ref.namelist()
        if len(csv_files) == 0:
            print(f'[!] No csv file for {d}')
            continue
        for csv_file in csv_files:
            df = pd.read_csv(csv_file)
            if 'No Data' in str(df.head()):
                print(f'    No data for {d}')
                os.remove(csv_file)
                continue
            yield df
            os.remove(csv_file)
            print(f'[*] Removed {csv_file}')
        os.remove(filename_zip)
        print(f'[*] Removed {filename_zip}')


def execute_query(head: str, queries: list, batch_size=1_000):
    try:
        print(f'[*] Total rows to execute: {len(queries)}')
        for i in range(0, len(queries), batch_size):
            batch = queries[i:i + batch_size]
            q = head + ', '.join(batch)
            query_job = client.query(q)
            result = query_job.result()
            print(f'[*] Executed rows {len(batch)}')
    except Exception as e:
        print('[!] Error:', type(e), e)


def main():
    queries = []

    def create_insert_query(r):
        tkr = r['ticker']
        sec_id = r['dcm_security_id']
        aoe = (datetime.strptime(r['date'], iso_format)).isoformat()
        aos = delta_date_to_isoformat(aoe)
        return f"('{tkr}','{r['date']}', {r['open']}, {r['close']}, {r['high']}, {r['low']}, {r['volume']}, '{aos}', '{aoe}', '{tkr}', {sec_id})"

    # yesterday = (datetime.today() - timedelta(days=1)).isoformat()
    start_date = '2000-01-03'
    # end_date = '2023-07-14'
    for ticker_df in download_tickers_list_by_date(start_date):
        ticker_df = ticker_df[ticker_df['ticker'].notna()]
        tickers_list = ticker_df['ticker'].unique().tolist()
        sec_id_df = get_security_id_from_ticker_mapper(tickers_list)
        ticker_df = ticker_df.merge(sec_id_df, left_on='ticker', right_index=True)
        ticker_df['date'] = ticker_df['date'].map(lambda x: datetime.strptime(x, '%Y-%m-%d').isoformat())

        for _, row in ticker_df.iterrows():
            insert_query = create_insert_query(row)
            queries.append(insert_query)

            if len(queries) >= 10_000_000:
                q_head = (f"INSERT INTO {table_name} (ticker, date, open, close, high, low, volume, as_of_start, "
                          f"as_of_end, symbol_asof, dcm_security_id) VALUES")
                execute_query(q_head, queries)  # , batch_size=500)
                queries = []


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
