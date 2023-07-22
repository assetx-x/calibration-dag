import os
import json
import tempfile
import requests
from io import BytesIO
from zipfile import ZipFile
from google.cloud import storage
from urllib.request import urlopen


SHARADAR_WEB_PAGE = "https://www.quandl.com/api/v3/datatables/SHARADAR/SF1?qopts.export=true&api_key"
API_KEY = "tzfgtC1umXNxmDLcUZ-5"

gs_bucket = "{}-us-dcm-data-temp".format(os.environ["GOOGLE_PROJECT_ID"])
gs_blob_path = os.environ["GCS_SHARADAR_BLOB"]


def upload_sharadar_file_to_gcs():
    sharadar_page = requests.get(f"{SHARADAR_WEB_PAGE}={API_KEY}")
    assert sharadar_page.status_code == 200

    page_content = json.loads(sharadar_page.content)
    sharadar_zip_url = page_content['datatable_bulk_download']['file']['link']

    temp_dir = tempfile.gettempdir()
    sharadar_filename = "SHARADAR_SF1.csv"
    local_path = f"{temp_dir}/{sharadar_filename}"

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(gs_bucket)

    print("\nDownloading SHARADAR zip file from ", sharadar_zip_url)
    with urlopen(sharadar_zip_url) as zip_resp:
        with ZipFile(BytesIO(zip_resp.read())) as zfile:
            file_names = zfile.namelist()
            assert len(file_names) == 1
            extracted_filename = file_names[0]
            zfile.extractall(temp_dir)
            os.rename(f"{temp_dir}/{extracted_filename}", local_path)
            print("\nZip file downloaded and extracted to ", local_path)

    blob = bucket.blob("{}/{}".format(gs_blob_path, sharadar_filename))
    blob.upload_from_filename(local_path)
    print("\nUploaded {} to {}/{}".format(sharadar_filename, gs_bucket, gs_blob_path))

    if os.path.isfile(local_path):
        os.remove(local_path)


if __name__=='__main__':
    upload_sharadar_file_to_gcs()