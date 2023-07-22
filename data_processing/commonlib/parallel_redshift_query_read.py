from commonlib.config import get_config

import pyhocon
import pandas as pd
import json
from google.cloud import storage
import jmespath
from multiprocessing import Pool, TimeoutError
import traceback
import os
from bz2file import BZ2File
from gzip import GzipFile
from sqlalchemy import create_engine
import tempfile
from uuid import uuid4
from copy import deepcopy
from io import BytesIO
from functools import reduce

from datetime import datetime, date
from sqlalchemy.orm.query import Query

from commonlib.util_functions_db import get_engine_and_session

import copyreg as copy_reg

import types
import errno

from urllib.parse import urlparse
import fnmatch

DEFAULT_S3_LOCATION = "s3://dcm-test/data-pipelines"

def _reduce_method(m):
    # since this reduction function is registered for MethodTypes,
    # m is always expeced to have the __self__ attribute.
    return getattr, (m.__self__, m.__func__.__name__)
copy_reg.pickle(types.MethodType, _reduce_method)

def read_data_segment(single_reader):
    return single_reader.read_data()

def render_query(statement, bind=None):
    """
    Generate an SQL expression string with bound parameters rendered inline
    for the given SQLAlchemy statement.
    WARNING: This method of escaping is insecure, incomplete, and for debugging
    purposes only. Executing SQL statements with inline-rendered user values is
    extremely insecure.
    Based on http://stackoverflow.com/questions/5631078/sqlalchemy-print-the-actual-query
    """
    if isinstance(statement, Query):
        if bind is None:
            bind = statement.session.get_bind(statement._mapper_zero_or_none())
        statement = statement.statement
    elif bind is None:
        bind = statement.bind

    class LiteralCompiler(bind.dialect.statement_compiler):

        def visit_bindparam(self, bindparam, within_columns_clause=False,
                            literal_binds=False, **kwargs):
            return self.render_literal_value(bindparam.value, bindparam.type)

        def render_literal_value(self, value, type_):
            if isinstance(value, int):
                return str(value)
            elif isinstance(value, (date, datetime)):
                return "'%s'" % value
            return super(LiteralCompiler, self).render_literal_value(value, type_)

    return LiteralCompiler(bind.dialect, statement).process(statement)

class SinglePartS3Reader(object):
    def __init__(self, s3_bucket, s3_location, local_h5_directory, credentials_conf, read_csv_kwargs=None):
        self.bucket = s3_bucket
        self.location = s3_location if s3_location[0]!="/" else s3_location[1:]
        self.local_h5_directory = local_h5_directory
        self.local_target_h5_file = os.path.join(self.local_h5_directory, "{0}.h5".format(self.location.split("/")[-1]))
        self.credentials = credentials_conf
        self.read_csv_kwargs = read_csv_kwargs or {}

    def read_data(self):
        try:
            compression_dict = {"gz": lambda x:GzipFile(fileobj=x,mode="r"),
                                "bz2": lambda x:BZ2File(x,"r")}
            s3_client = storage.Client()
            bucket = s3_client.bucket(self.bucket)
            blob = bucket.blob(self.location)
            target_file = BytesIO(blob.download_as_string())
            decompressor = [compression_dict[k] for k in compression_dict if self.location.endswith(k)]
            decompressor = (decompressor and decompressor[0]) or (lambda x:x)
            target_file = decompressor(target_file)
            data_segment = pd.read_csv(target_file, **self.read_csv_kwargs)
            with pd.HDFStore(self.local_target_h5_file, "w", complevel=9, complib="blosc") as store:
                store["{0}/{1}".format(self.bucket, self.location)] = data_segment
        except BaseException as e:
            traceback.print_exc()
            return {self.local_target_h5_file:False}
        return {self.local_target_h5_file:True}

class MultiPartS3Reader(object):
    def __init__(self, s3_manifest_file, local_target_dir, credentials_conf, parallel_processes=5,
                 read_csv_kwargs=None, use_cached_data_if_available=False, additional_data_for_local_manifest=None,
                 cleanup_s3_when_finished=False, cleanup_localtarget_when_finished=True):
        self.credentials_conf = credentials_conf
        self.target_manifest_file = s3_manifest_file
        self.local_target_dir = local_target_dir
        self.read_csv_kwargs = read_csv_kwargs or {}
        self.parallel_processes = parallel_processes
        self.use_cached_data_if_available = use_cached_data_if_available
        self.local_manifest_location = os.path.join(self.local_target_dir, "manifest")
        self.additional_data_for_local_manifest = additional_data_for_local_manifest or {}
        self.cleanup_s3_when_finished = cleanup_s3_when_finished
        self.cleanup_localtarget_when_finished = cleanup_localtarget_when_finished

    def set_additional_info_for_manifest_file(self, additional_data_for_local_manifest):
        self.additional_data_for_local_manifest = additional_data_for_local_manifest or {}

    def __read_manifest_file(self):
        manifest_bucket, manifest_key = pd.io.common.parse_url(self.target_manifest_file)[1:3]
        manifest_key = manifest_key[1:]
        s3_client = storage.Client()
        bucket = s3_client.bucket(manifest_bucket)
        blob = bucket.blob(manifest_key)
        manifest_file = BytesIO(blob.download_as_string())
        manifest_content = json.load(manifest_file)
        files_to_read = map(lambda x:pd.io.common.parse_url(x)[1:3],
                            jmespath.search("entries[*].url", manifest_content))
        return files_to_read

    def __download_data(self):
        files_to_read = self.__read_manifest_file()
        single_part_readers = map(lambda x:SinglePartS3Reader(*x, local_h5_directory=self.local_target_dir,
                                                              credentials_conf=self.credentials_conf,
                                                              read_csv_kwargs=self.read_csv_kwargs), files_to_read)
        pool = Pool(processes=self.parallel_processes)
        single_reads_operations = pool.map_async(read_data_segment, single_part_readers)
        pool.close()
        pool.join()
        pool.terminate()
        single_reads_operations = reduce(lambda x,y:x.update(y) or x, single_reads_operations.get(), {})
        failed_segments = [k for k in single_reads_operations if not single_reads_operations[k]]
        if failed_segments:
            raise RuntimeError("There was a problem writing the segments on MultiPartS3Reader. Failed segments are: {0}"
                               .format(failed_segments))
        local_manifest = dict(entries=[{"url":k} for k in single_reads_operations])
        local_manifest.update(self.additional_data_for_local_manifest)
        with open(self.local_manifest_location, "w") as f:
            json.dump(local_manifest, f, indent=4)

    def __collect_h5_data(self):
        with open(self.local_manifest_location, "r") as f:
            local_manifest = json.load(f)
        data_segments = []
        for filename in jmespath.search("entries[*].url", local_manifest):
            with pd.HDFStore(filename,"r") as store:
                segment = store.get(store.keys()[0])
            data_segments.append(segment)
        data_segments = list(filter(len, data_segments))
        result = pd.DataFrame(dict([(k,pd.np.concatenate([d[k] for d in data_segments]))
                                    for k in data_segments[0].columns]))
        return result

    def is_data_available_from_cache(self):
        if self.use_cached_data_if_available:
            return os.path.exists(self.local_manifest_location)
        return False

    def __cleanup_s3_location(self):
        try:
            manifest_bucket, manifest_key = pd.io.common.parse_url(self.target_manifest_file)[1:3]
            manifest_key = manifest_key[1:]
            s3_client = storage.Client()
            bucket = s3_client.bucket(manifest_bucket)
            blob = bucket.blob(manifest_key)
            manifest_file = BytesIO(blob.download_as_string())
            manifest_content = json.load(manifest_file)
            files_to_delete = map(lambda x:pd.io.common.parse_url(x)[1:3],
                                  jmespath.search("entries[*].url", manifest_content))
            for k in files_to_delete:
                file_bucket, file_key = k
                file_key = file_key[1:]
                bucket = s3_client.bucket(file_bucket)
                blob = bucket.blob(file_key)
                blob.delete()
            bucket = s3_client.bucket(manifest_bucket)
            blob = bucket.blob(manifest_key)
            blob.delete()
        except BaseException as e:
            pass

    def __cleanup_localtarget(self):
        with open(self.local_manifest_location, "r") as f:
            local_manifest = json.load(f)
        for filename in jmespath.search("entries[*].url", local_manifest):
            os.remove(filename)
        os.remove(self.local_manifest_location)
        try:
            os.rmdir(os.path.dirname(self.local_manifest_location))
        except BaseException:
            pass

    def cleanup(self):
        if self.cleanup_s3_when_finished:
            self.__cleanup_s3_location()
        if self.cleanup_localtarget_when_finished:
            self.__cleanup_localtarget()

    def read(self):
        if not os.path.exists(self.local_manifest_location) \
           or (not self.use_cached_data_if_available):
            self.__download_data()
        result = self.__collect_h5_data()
        return result

    def set_read_csv_kwargs(self, read_csv_kwargs):
        self.read_csv_kwargs = read_csv_kwargs or {}

class ParallelQueryRedshiftDownload(object):
    def __init__(self, query, credentials_conf=None, local_target_dir=None, s3_temp_location=None,
                 parallel_processes=20, read_csv_kwargs=None, use_cached_data_if_available=False,
                 cleanup_s3_when_finished=True, cleanup_localtarget_when_finished=True):
        credentials_conf = credentials_conf or get_config()["ParallelQueryRedshiftDownload"]["credentials"]
        reader_time = pd.Timestamp.now().strftime("%Y%m%d")
        self.query_uuid = uuid4().hex
        self.query = query
        self.local_target_dir = local_target_dir or os.path.join(tempfile.gettempdir(), reader_time, self.query_uuid)
        self.s3_temp_location = s3_temp_location \
            or "/".join([get_config()["ParallelQueryRedshiftDownload"]["s3_default_location"],
                         reader_time, self.query_uuid]) + "/"
        self.credentials_conf = credentials_conf
        self.parallel_processes = parallel_processes
        self.read_csv_kwargs = read_csv_kwargs or {}
        self.use_cached_data_if_available = use_cached_data_if_available
        self.engine = None
        self.cleanup_s3_when_finished = cleanup_s3_when_finished
        self.cleanup_localtarget_when_finished = cleanup_localtarget_when_finished

        try:
            if not os.path.exists(self.local_target_dir):
                os.makedirs(self.local_target_dir)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

    def __execute_query_and_get_result_types(self):
        small_query = self.query + " LIMIT 100"
        small_results = self.engine.execute(small_query)
        results_descs = small_results.cursor.description
        sample_data = pd.DataFrame.from_records(small_results.fetchall(), columns=[k.name for k in results_descs])
        dtypes = sample_data.dtypes.replace({pd.np.dtype("int64"): pd.np.dtype("float64")})
        return dtypes

    def __create_engine(self):
        engine, _ = get_engine_and_session(verbose=True)
        return engine

    def __unload_data(self):
        gcs_location = os.path.join(self.s3_temp_location, "data.*.csv.gz")
        unload_query = """
        EXPORT DATA
        OPTIONS (
            compression="GZIP",
            format="CSV",
            overwrite=true,
            header=false,
            uri="{s3_location}"
        ) AS
        {query}
        """.format(query=self.query, s3_location=gcs_location)
        unload_query_result = self.engine.execute(unload_query)
        self.create_manifest(gcs_location)

    def __create_kwargs(self, data_types):
        data_types_as_dict = data_types.to_dict()
        datetime_cols = [k for k in data_types_as_dict if data_types_as_dict[k].name.find("time")>=0]
        data_types_as_dict = {k:data_types_as_dict[k] for k in data_types_as_dict if k not in datetime_cols}
        kwargs = dict(header=None, names=data_types.index.tolist(), dtype=data_types_as_dict)
        if datetime_cols:
            kwargs["parse_dates"] = datetime_cols
        kwargs["escapechar"] = "\\"
        return kwargs

    def execute(self):
        additional_manifest_content = {}
        segments_reader = MultiPartS3Reader("{0}manifest".format(self.s3_temp_location), self.local_target_dir,
                                            self.credentials_conf, self.parallel_processes, None,
                                            self.use_cached_data_if_available, additional_manifest_content,
                                            self.cleanup_s3_when_finished, self.cleanup_localtarget_when_finished)
        if not segments_reader.is_data_available_from_cache():
            self.engine = self.__create_engine()
            self.query = self.query if isinstance(self.query, str) else render_query(self.query, self.engine)
            additional_manifest_content = {"query":self.query, "run_dt": str(pd.Timestamp.now()),
                                           "s3_data_location":self.s3_temp_location}
            segments_reader.set_additional_info_for_manifest_file(additional_manifest_content)
            data_types = self.__execute_query_and_get_result_types()
            csv_read_kwargs = self.__create_kwargs(data_types)
            final_csv_kwargs = deepcopy(self.read_csv_kwargs)
            final_csv_kwargs.update(csv_read_kwargs)
            segments_reader.set_read_csv_kwargs(final_csv_kwargs)
            self.__unload_data()
            self.engine.dispose()
        
        data = segments_reader.read()
        segments_reader.cleanup()
        return data

    def create_manifest(self, gcs_location):
        # TODO: throw error if files already match and exist
        parsed = urlparse(gcs_location)
        path = parsed.path[1:]
        dir = os.path.dirname(path)
        bucket_name = parsed.netloc
        manifest_blob_name = os.path.join(dir, "manifest")
        prefix = path.split("*")[0]
        gcs = storage.Client()
        bucket = gcs.bucket(bucket_name)
        blobs = [f"gs://{bucket_name}/{blob.name}" for blob in bucket.list_blobs(prefix=prefix) if fnmatch.fnmatch(blob.name, path)]
        manifest_content = {"entries": [{"url": blob} for blob in blobs]}
        manifest_blob = bucket.blob(manifest_blob_name)
        manifest_blob.upload_from_string(json.dumps(manifest_content, indent=2), content_type="application/json")

if __name__=="__main__":
    credentials = """
    s3 {
        access_key = ${?AWS_ACCESS_KEY_ID}
        secret_key = ${?AWS_SECRET_ACCESS_KEY}
    }

    redshift {
        host = marketdata-test.cq0v4ljf3cuw.us-east-1.redshift.amazonaws.com
        username = ${?REDSHIFT_USER}
        password = ${?REDSHIFT_PWD}
        database = dev
        port = 5439
        s3_temp_dir = dcm-data-temp
    }
    """
    credentials_conf = pyhocon.ConfigFactory.parse_string(credentials)

    query = """
    SELECT * FROM daily_equity_prices where ticker='FB'
    """
    query_fast_reader = ParallelQueryRedshiftDownload(query, r"C:\DCM\temp\test",
                                                      "s3://dcm-test/napoleon/data_backup/daily_equity_prices/",
                                                      credentials_conf, use_cached_data_if_available=True)
    query_data = query_fast_reader.execute()
    print("Done")
