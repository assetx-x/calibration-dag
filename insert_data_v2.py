import os
import time
from datetime import datetime
from typing import List, Optional, Generator
from dataclasses import dataclass

import pandas as pd
import requests
import zipfile
from google.cloud import bigquery, storage
from pydantic import BaseModel, Field
from tqdm import tqdm


class Config(BaseModel):
    gcs_bucket: str = Field(..., env='GCS_BUCKET')
    nasdaq_api_key: str = Field(..., env='NASDAQ_DATA_LINK_API_KEY')
    project_id: str = Field(..., env='GCP_PROJECT_ID')
    bigquery_table: str = Field(..., env='BIGQUERY_TABLE')
    iso_format: str = '%Y-%m-%dT%H:%M:%S'


@dataclass
class GCPClient:
    config: Config

    def __post_init__(self):
        self.bigquery_client = bigquery.Client(project=self.config.project_id)
        self.storage_client = storage.Client(project=self.config.project_id)
        self.gcs_bucket = self.config.gcs_bucket

    def check_bigquery_connection(self):
        """Check the connection to BigQuery."""
        try:
            self.bigquery_client.query('SELECT 1').result()
            print('[*] Connected to BigQuery')
        except Exception as e:
            print(f'[!] Error connecting to BigQuery: {e}')
            raise

    def check_or_create_bucket(self):
        """Check if the GCS bucket exists, and create it if it does not."""
        try:
            self.storage_client.get_bucket(self.gcs_bucket)
            print(f'[*] Connected to GCS bucket: {self.gcs_bucket}')
        except Exception:
            self.storage_client.create_bucket(self.gcs_bucket)
            print(f'[*] Bucket {self.gcs_bucket} created')


@dataclass
class SecurityService:
    gcp_client: GCPClient

    def get_security_id_from_ticker_mapper(self, tickers: List[str]) -> pd.DataFrame:
        """Fetch security IDs for given tickers from the security master table."""
        tickers_str = ','.join([f"'{t}'" for t in tickers])
        query = f"SELECT ticker, dcm_security_id FROM marketdata.security_master_20230603 WHERE ticker IN ({tickers_str})"
        result_df = self.gcp_client.bigquery_client.query(query).to_dataframe()
        return result_df.set_index("ticker")

    def get_tickers_to_carry_forward(self, target_date: str) -> pd.DataFrame:
        """Fetch distinct tickers and security IDs for a given date."""
        query = f"SELECT DISTINCT ticker, dcm_security_id FROM marketdata.daily_equity_prices WHERE date(date)='{target_date}'"
        return self.gcp_client.bigquery_client.query(query).to_dataframe()

    def get_last_date_in_table(self, table_name: str) -> datetime:
        """Fetch the last date from the daily equity prices table."""
        query = f"SELECT MAX(date) AS max_date FROM {table_name}"
        result_df = self.gcp_client.bigquery_client.query(query).to_dataframe()
        return result_df.iloc[0]['max_date']


@dataclass
class NasdaqDataDownloader:
    config: Config

    def download_tickers_list_by_date(self, start: str, end: Optional[str] = None) -> Generator[
        pd.DataFrame, None, None]:
        """Download and extract tickers data from NASDAQ by date range."""
        filename_zip = f'data_{start}.zip'.replace(" ", "")

        if not os.path.exists(filename_zip):
            self._download_zip_file(start, end, filename_zip)

        with zipfile.ZipFile(filename_zip, 'r') as zip_ref:
            zip_ref.extractall()
            csv_files = zip_ref.namelist()

        for csv_file in csv_files:
            df = pd.read_csv(csv_file)
            if 'No Data' not in str(df.head()):
                yield df
            os.remove(csv_file)

    def _download_zip_file(self, start: str, end: Optional[str], filename_zip: str):
        """Download the ZIP file containing tickers data."""
        params = {
            'date.gte': start,
            'date.lte': end,
            'qopts.columns': 'ticker,date,open,close,high,low,volume',
            'qopts.export': 'true',
            'api_key': self.config.nasdaq_api_key,
        }

        while True:
            response = requests.get(self.config.api_url, params=params)
            if response.status_code != 200:
                raise ConnectionError(f'Error: {response.status_code}. {response.content}')

            file_status = response.json().get('datatable_bulk_download', {}).get('file', {}).get('status', '').lower()
            if file_status == 'completed':
                break
            elif file_status != 'creating':
                raise ValueError(f'Unexpected file status: {file_status}')
            time.sleep(45)

        download_link = response.json().get('datatable_bulk_download', {}).get('file', {}).get('link')
        if not download_link:
            raise ValueError(f'No download link available for {start}')

        with open(filename_zip, 'wb') as f:
            file_content = requests.get(download_link, allow_redirects=True, timeout=10).content
            f.write(file_content)


@dataclass
class QueryExecutor:
    gcp_client: GCPClient
    table_name: str

    def execute_queries(self, head: str, queries: List[str], batch_size: int = 1_000):
        """Execute a batch of queries on BigQuery."""
        if not queries:
            print(f'[*] No queries to execute.')
            return

        try:
            for i in tqdm(range(0, len(queries), batch_size)):
                batch = queries[i:i + batch_size]
                query = f"{head} {','.join(batch)}"
                query_job = self.gcp_client.bigquery_client.query(query)
                query_job.result()
            print(f'[*] Executed {len(queries)} rows into {self.table_name}.')
        except Exception as e:
            raise RuntimeError(f'Error executing queries: {e}')


@dataclass
class EquityDataProcessor:
    security_service: SecurityService
    query_executor: QueryExecutor
    config: Config

    def process_equity_data(self, start_date: str, end_date: str):
        """Process equity data between start_date and end_date, and insert it into the BigQuery table."""
        last_date = self.security_service.get_last_date_in_table(self.query_executor.table_name)
        if (datetime.today().date() - last_date.date()).days <= 1:
            print(f'[*] Nothing to update. Last date in table: {last_date}')
            return

        queries = []
        for ticker_df in NasdaqDataDownloader(self.config).download_tickers_list_by_date(start_date, end_date):
            sec_id_df = self._merge_with_security_ids(ticker_df)
            queries.extend(self._create_insert_queries(sec_id_df))

        if queries:
            head = f"INSERT INTO {self.query_executor.table_name} (ticker, date, open, close, high, low, volume, as_of_start, as_of_end, symbol_asof, dcm_security_id) VALUES"
            self.query_executor.execute_queries(head, queries)

    def _merge_with_security_ids(self, ticker_df: pd.DataFrame) -> pd.DataFrame:
        """Merge ticker data with their corresponding security IDs."""
        tickers_list = ticker_df['ticker'].unique().tolist()
        sec_id_df = self.security_service.get_security_id_from_ticker_mapper(tickers_list)
        merged_df = ticker_df.merge(sec_id_df, left_on='ticker', right_index=True)
        merged_df['date'] = merged_df['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').isoformat())
        return merged_df

    def _create_insert_queries(self, merged_df: pd.DataFrame) -> List[str]:
        """Create SQL insert queries from the merged data frame."""
        queries = []
        for _, row in merged_df.iterrows():
            aoe = datetime.strptime(row['date'], self.config.iso_format).isoformat()
            query = (
                f"('{row['ticker']}', '{row['date']}', {row['open']}, {row['close']}, "
                f"{row['high']}, {row['low']}, {row['volume']}, '{aoe}', '{aoe}', '{row['ticker']}', {row['dcm_security_id']})"
            )
            queries.append(query)
        return queries


def main():
    config = Config()
    gcp_client = GCPClient(config)
    security_service = SecurityService(gcp_client)
    query_executor = QueryExecutor(gcp_client, config.bigquery_table)
    equity_data_processor = EquityDataProcessor(security_service, query_executor, config)

    gcp_client.check_bigquery_connection()
    gcp_client.check_or_create_bucket()

    equity_data_processor.process_equity_data(config.iso_format, datetime.today().strftime('%Y-%m-%d'))


if __name__ == '__main__':
    main()
