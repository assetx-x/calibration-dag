import os
import time
import sys

import requests
import pandas as pd
import warnings
import zipfile

from dotenv import load_dotenv
from google.cloud import bigquery, storage
from datetime import datetime, date

from tqdm import tqdm

warnings.filterwarnings("ignore")

# load_dotenv()

# if sys.platform in ['darwin', 'linux']:
#     """
#     To log in into GCP locally use the following command:
#     $ gcloud auth application-default login
#     and follow the instructions, then the json will be created automatically
#     """
#     home_path = os.getenv('HOME')
#     credentials_path = os.path.join(
#         home_path, '.config/gcloud/application_default_credentials.json'
#     )
#     os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
#     print(f'Credentials set to {os.environ["GOOGLE_APPLICATION_CREDENTIALS"]}')
# else:
#     raise ValueError('Only Linux is supported')

# os.environ['GCS_BUCKET'] = 'dcm-prod-ba2f-us-dcm-data-test'
# os.environ['GCS_BUCKET'] = 'assetx-equity-data'

GCS_BUCKET = 'assetx-equity-data'

client = bigquery.Client(project='ax-prod-393101')
storage_client = storage.Client(project='ax-prod-393101')
table_name = 'ax-prod-393101.marketdata.daily_equity_prices'

iso_format = '%Y-%m-%dT%H:%M:%S'

weekday_dict = {
    0: 'Monday',
    1: 'Tuesday',
    2: 'Wednesday',
    3: 'Thursday',
    4: 'Friday',
    5: 'Saturday',
    6: 'Sunday',
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
    sm_query = (
        f"select distinct ticker from marketdata.daily_equity_prices where date='{d}'"
    )
    security_master_tickers = client.query(sm_query).to_dataframe()
    return security_master_tickers


def get_last_date_in_table():
    return client.query(
        f"select max(date) as max_date from marketdata.daily_equity_prices"
    ).to_dataframe()


def download_tickers_list_by_date(start: str, end: str = None):
    nasdaq_api_key = os.getenv('NASDAQ_DATA_LINK_API_KEY', 'tzfgtC1umXNxmDLcUZ-5')
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
            print(f'[!] Error: {r.status_code}. {r.content}')
            return None
        file_status = r.json().get('datatable_bulk_download').get('file').get('status')
        while file_status.lower() == 'creating':
            print(
                f'[*] Waiting for file to be created for {start}. Status: "{file_status}"'
            )
            r = requests.get(api_url, params=args)
            file_status = (
                r.json().get('datatable_bulk_download').get('file').get('status')
            )
            time.sleep(45)
        file_to_download_link = (
            r.json().get('datatable_bulk_download').get('file').get('link')
        )
        if file_to_download_link is None:
            print(f'[!] No download link for {start}: \n{r.json()}')
            return None
        file_content = requests.get(
            file_to_download_link, allow_redirects=True, timeout=10
        ).content
        if file_content == b'':
            print(f'[!] No bin content for {start}')
            return None
        with open(filename_zip, 'wb') as f:
            f.write(file_content)
    else:
        print(f'[*] File {filename_zip} already exists')

    with zipfile.ZipFile(filename_zip, 'r') as zip_ref:
        print(f'[*] Extracting {filename_zip}')
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


def execute_query(head: str, queries: list, batch_size=1_000):
    try:
        print(f'[*] Total rows to execute: {len(queries)}')
        for i in tqdm(range(0, len(queries), batch_size)):
            batch = queries[i : i + batch_size]
            q = head + ', '.join(batch)
            query_job = client.query(q)
            result = query_job.result()
        print(f'[*] Executed rows {len(queries)}.')
    except Exception as e:
        print('[!] Error:', type(e), e)


def is_updated(*args):
    args = list(args)
    for ix, d in enumerate(args):
        if isinstance(d, datetime):
            args[ix] = d.date()
    r = date.today() <= max(args)
    return r


def main():

    # check if have database access
    try:
        client.query('SELECT 1').result()
        print('[*] Connected to BigQuery')
    except Exception as e:
        print(f'[!] Error: {e}')
        return

    try:
        storage_client.get_bucket(GCS_BUCKET)
        print(f'[*] Connected to GCS bucket: {GCS_BUCKET}')
    except Exception as e:
        print(f'[!] Error: {e}')
        storage_client.create_bucket(GCS_BUCKET)
        print(f'[*] Bucket {GCS_BUCKET} created')

    queries = []
    last_date_df = get_last_date_in_table()
    last_date = last_date_df['max_date'].iloc[0].date()

    def create_insert_query(r):
        tkr = r['ticker']
        sec_id = r['dcm_security_id']
        aoe = (datetime.strptime(r['date'], iso_format)).isoformat()
        aos = delta_date_to_isoformat(aoe)
        return f"('{tkr}','{r['date']}', {r['open']}, {r['close']}, {r['high']}, {r['low']}, {r['volume']}, '{aos}', '{aoe}', '{tkr}', {sec_id})"

    end_date = datetime.today()
    date_diff = (end_date.date() - last_date).days
    if not date_diff > 1:
        print(f'[*] Nothing to update. Last date in table: {last_date}')
        return
    last_date = last_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    print(f'[*] Getting data from {last_date} to {end_date}')
    for ticker_df in download_tickers_list_by_date(last_date, end_date):
        print(f'[*] Processing {ticker_df.shape} rows')
        ticker_df = ticker_df[ticker_df['ticker'].notna()]
        tickers_list = ticker_df['ticker'].unique().tolist()
        sec_id_df = get_security_id_from_ticker_mapper(tickers_list)
        ticker_df = ticker_df.merge(sec_id_df, left_on='ticker', right_index=True)
        ticker_df['date'] = ticker_df['date'].map(
            lambda x: datetime.strptime(x, '%Y-%m-%d').isoformat()
        )

        for _, row in ticker_df.iterrows():
            insert_query = create_insert_query(row)
            queries.append(insert_query)

        if queries:
            print(f'[*] Executing {len(queries)} rows into: {table_name}')
            q_head = (
                f"INSERT INTO {table_name} (ticker, date, open, close, high, low, volume, as_of_start, "
                f"as_of_end, symbol_asof, dcm_security_id) VALUES"
            )
            execute_query(q_head, queries)  # , batch_size=500)
            queries = []
            print(f'[*] Queries len now {len(queries)}')


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
