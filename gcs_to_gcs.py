import glob
import os
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from pprint import pprint

from google.cloud import storage, bigquery
from google.cloud.bigquery import DatasetReference
from google.oauth2.service_account import Credentials


def process(source_bucket, source_file, destination_bucket):
    start_time = time.time()
    try:
        source_blob = source_bucket.blob(source_file.name)
        destination_blob = destination_bucket.blob(source_file.name)
        destination_blob.content_type = source_blob.content_type
        destination_blob.upload_from_string(
            source_blob.download_as_text(), num_retries=10, timeout=120
        )

        print(
            {
                'source': source_blob.name,
                'destination': destination_blob.name,
                'time': time.time() - start_time,
            }
        )
    except Exception as e:
        print(
            {
                'error': f"{e}",
                'source': source_file.name,
                'time': time.time() - start_time,
            }
        )


def from_gcs_to_gcs():

    source_credentials = Credentials.from_service_account_file('./dcm_config.json')
    source_storage_client = storage.Client(
        credentials=source_credentials, project='dcm-prod-ba2f'
    )
    source_bucket = source_storage_client.bucket('table-transfer-eaf')

    destination_credentials = Credentials.from_service_account_file('./ax_config.json')
    destination_storage_client = storage.Client(
        credentials=destination_credentials, project='ax-prod-393101'
    )
    destination_bucket = destination_storage_client.bucket('table-eaf')

    with ThreadPoolExecutor(max_workers=1) as executor:
        for i, source_file in enumerate(list(source_bucket.list_blobs())):
            executor.submit(process, source_bucket, source_file, destination_bucket)


def export_bigquery_table_to_gcs():

    source_credentials = Credentials.from_service_account_file('./dcm_config.json')
    source_client = bigquery.Client(
        project='dcm-prod-ba2f', credentials=source_credentials
    )
    source_table_ref = DatasetReference('dcm-prod-ba2f', 'marketdata').table(
        'equity_adjustment_factors'
    )

    destination_credentials = Credentials.from_service_account_file('./ax_config.json')
    destination_storage_client = storage.Client(credentials=destination_credentials)
    destination_bucket = destination_storage_client.bucket('table-eaf')
    destination_gs = f"gs://{destination_bucket.name}/eaf-*.csv"

    extract_job = source_client.extract_table(
        source_table_ref,
        destination_gs,
        location="US",
    )

    extract_job.result()

    print(f"Table export completed: {source_table_ref.path}")


if __name__ == '__main__':
    from_gcs_to_gcs()
    # export_bigquery_table_to_gcs()
