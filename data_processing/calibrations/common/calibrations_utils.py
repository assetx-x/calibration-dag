"""
Utilities used for calibrations processes of DCM
"""

from io import StringIO
import os.path

from google.cloud import storage
import pandas as pd
import pyhocon

from calibrations.common.calibration_logging import calibration_logger
from calibrations.common.config_functions import get_config
from commonlib.util_functions import create_directory_if_does_not_exists
from commonlib.util_classes import OMSSystems, InstructionsFileURILocator
from actor_instructions import InstructionParser

def get_local_dated_dir(base_path, end_dt, relative_path):
    hocon_str = """
        {{
          date = {0}
          dir = {1}/${{date}}/{2}
        }}
        """.format(end_dt.strftime("%Y%m%d"), base_path, relative_path)
    directory = os.path.realpath(pyhocon.ConfigFactory.parse_string(hocon_str)["dir"])
    return directory

def store_pandas_object(pandas_obj, filepath, pandas_object_name=None, **kwargs):
    """
    Function that stores a pandas Series or Dataframes into local or remote locations (based on GCS)
    """
    if not isinstance(pandas_obj, pd.core.base.PandasObject):
        raise ValueError("store_pandas_object can only be used to store pandas objects. received {0}"
                         .format(type(pandas_obj)))
    pandas_object_name = pandas_object_name or ""
    kwargs.setdefault("quotechar", "'",)
    kwargs.setdefault("index", False)
    parsed_path = pd.io.common.parse_url(filepath)
    if parsed_path.scheme == "gs":
        csv_buffer = StringIO()
        pandas_obj.to_csv(csv_buffer, **kwargs)
        csv_buffer.seek(0)
        s3_client = storage.Client()
        bucket, key = parsed_path[1:3]
        key = key[1:]
        calibration_logger.info("Storing pandas object {0} into Bucket {1}, under key {2}"
                               .format(pandas_object_name, bucket, key))
        gcs_bucket = s3_client.bucket(bucket)
        blob = gcs_bucket.blob(key)
        blob.upload_from_file(csv_buffer)
    else:
        create_directory_if_does_not_exists(os.path.dirname(filepath))
        calibration_logger.info("Storing pandas object {0} into location {1}"
                               .format(pandas_object_name, filepath))
        pandas_obj.to_csv(filepath, **kwargs)

def create_empty_folder(dirpath):
    parsed_path = pd.io.common.parse_url(dirpath)
    if parsed_path.scheme == "gs":
        s3_client = storage.Client()
        bucket, key = parsed_path[1:3]
        key = key[1:]
        calibration_logger.info("Creating empty directory into Bucket {0}, under key {1}"
                               .format(bucket, key))
        gcs_bucket = s3_client.bucket(bucket)
        blob = gcs_bucket.blob(key)
        blob.upload_from_string('', content_type='application/x-www-form-urlencoded;charset=UTF-8')
    else:
        create_directory_if_does_not_exists(dirpath)


def get_all_files_in_location(configuration_location):

    s3_client = storage.Client()   
    parsed_path = pd.io.common.parse_url(configuration_location)
    bucket, key = parsed_path[1:3]
    key = key[1:]
    s3_file_location_info = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
    s3_file_location_info = list(self.s3_client.list_blobs(bucket, prefix=key))
    #TODO:Ali Make the sorting is correct
    list_of_calibration_files = sorted(["gs://{0}/{1}".format(bucket,file_location.name) 
                                                for file_location in s3_file_location_info], reverse = True)
    return list_of_calibration_files
    
if __name__=="__main__":
    direc = get_local_dated_dir("${HOMEPATH}", pd.Timestamp("2020-01-07"), "calibration_data/gan_training_data-first")
    print(direc)
