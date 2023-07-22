import os
from abc import abstractmethod

import pandas as pd

from status_objects import StatusType
from calibrations.common.calibrations_utils import store_pandas_object
from calibrations.core.calibration_taskflow_task import CalibrationTaskflowTask
from calibrations.common.taskflow_params import TaskflowPipelineRunMode
from commonlib.market_timeline import marketTimeline
from commonlib.parallel_redshift_query_read import ParallelQueryRedshiftDownload
from calibrations.common.config_functions import get_config
from calibrations.common.calibration_logging import calibration_logger
#from commonlib.util_functions import get_snowflake_engine

from google.cloud import storage
import io

class CalibrationDates(CalibrationTaskflowTask):

    def __init__(self, cache_file=None, read_csv_kwargs=None, force_recalculation=False, save_intervals_to=None):
        self.data = None
        read_csv_kwargs = read_csv_kwargs or {}
        self.force_recalculation = force_recalculation
        self.save_intervals_to = save_intervals_to
        if not isinstance(read_csv_kwargs, dict):
            raise TypeError("CalibrationDates.__init__ - read_csv_kwargs must be a dictionary; "
                            "received a {0}".format(type(read_csv_kwargs)))
        if cache_file:
            cache_file = os.path.abspath(os.path.join(os.path.dirname(__file__), cache_file)) \
                if not os.path.isabs(cache_file) else cache_file
            if not os.path.exists(cache_file):
                raise RuntimeError("CalibrationDates.__init__ - file {0} does not exists".format(cache_file))
        self.read_csv_kwargs = read_csv_kwargs
        self.file_to_read = cache_file
        CalibrationTaskflowTask.__init__(self)

    @classmethod
    def get_default_config(cls):
        cache_file = {"cache_file": "./configuration/interevals_as_of_20181218.csv",
                      "read_csv_kwargs": dict(index_col=0, parse_dates=["date"]),
                      "force_recalculation": False, "save_intervals_to": None}

    @abstractmethod
    def post_process_intervals(self, intervals):
        raise NotImplementedError("CalibrationDates.post_process_intervals must be overriden by derived class")

    @abstractmethod
    def calculate_intervals(self):
        raise NotImplementedError("CalibrationDates.calculate_intervals must be overriden by derived class")

    @abstractmethod
    def calculate_intervals_for_calibration(self):
        raise NotImplementedError("CalibrationDates.calculate_intervals_for_calibration must be overriden by "
                                  "derived class")

    def _pull_data(self):
        self.step_status["DataLineage"] = self.file_to_read
        result = pd.read_csv(self.file_to_read, **self.read_csv_kwargs)
        for date_col in self.read_csv_kwargs.get("parse_dates", []):
            result[date_col] = pd.DatetimeIndex(result[date_col]).normalize()
        return result

    def do_step_action(self, **kwargs):
        calibration_mode = self.task_params.run_mode is TaskflowPipelineRunMode.Calibration
        if calibration_mode:
            result = self.calculate_intervals_for_calibration()
        else:
            if self.force_recalculation or not self.file_to_read:
                result = self.calculate_intervals()
            else:
                result = self._pull_data()
        result = self.post_process_intervals(result)
        if self.save_intervals_to and not calibration_mode:
            store_pandas_object(result, self.save_intervals_to, "Intervals for {0}".format(self.__class__.__name__),
                                index=True, quotechar='"')
        self.data = result
        return StatusType.Success

class DataReader(CalibrationTaskflowTask):
    '''

    Generic parent class for sql and csv readers. Handles date, identifier columns and permutation.

    '''
    REQUIRES_FIELDS = ["data_permutator"]
    def __init__(self, datetime_column, identifier_column, permutation_timezone, datetime_column_timezone,
                 attach_additional_empty_data_day=False, permute_retrieved_data=True):
        provides_fields = self.get_full_class_provides()
        if not provides_fields or len(provides_fields)==0:
            raise RuntimeError("The Data Reader class {0} does not have a defined PROVIDES_FIELDS class attribute. "
                               .format(self.__class__.__name__))
        # Interval information now returns two fields
        #if len(provides_fields)>1:
        #    raise RuntimeError("The Data Reader class {0} must defined a single return element under PROVIDES_FIELDS"
        #                       .format(self.__class__.__name__))
        self.datetime_column = datetime_column
        self.identifier_column = identifier_column
        self.permutation_timezone = permutation_timezone
        self.datetime_column_timezone = datetime_column_timezone
        self.attach_additional_empty_data_day = attach_additional_empty_data_day
        self.permute_retrieved_data = permute_retrieved_data
        self.data = None
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        calibration_logger.info("Executing {0}._prepare_to_pull_data".format(self.__class__.__name__))
        self._prepare_to_pull_data(**kwargs)
        calibration_logger.info("Executing {0}._pull_data".format(self.__class__.__name__))
        result = self._pull_data(**kwargs)
        calibration_logger.info("Executing {0}._post_process_pulled_data".format(self.__class__.__name__))
        self.data = self._post_process_pulled_data(result, **kwargs)
        self.step_status["DataLineage"] = self._get_data_lineage()
        return StatusType.Success

    @abstractmethod
    def _get_data_lineage(self):
        raise NotImplementedError("The method DataReader._get_data_lineage must be overriden in derived class")

    @abstractmethod
    def _prepare_to_pull_data(self, **kwargs):
        raise NotImplementedError("The method DataReader._prepare_to_pull_data must be overriden in derived class")

    @abstractmethod
    def _pull_data(self, **kwargs):
        raise NotImplementedError("The method DataReader._pull_data must be overriden in derived class")

    @abstractmethod
    def _get_return_types_for_permutations(self, data, **kwargs):
        raise NotImplementedError("The method DataReader._get_return_types_for_permutations must be overriden in "
                                  "derived class")


    def _post_process_pulled_data(self, data, **kwargs):
        permutator = (kwargs["data_permutator"].iloc[0,0] if len(kwargs["data_permutator"])>0 else None)
        permuted_data = self.permute_data(permutator, data, **kwargs) \
            if permutator is not None and self.permute_retrieved_data else data
        final_data = self._attach_extra_date(permuted_data, self.datetime_column, self.identifier_column,
                                             self.datetime_column_timezone, self.permutation_timezone)\
            if self.attach_additional_empty_data_day else permuted_data
        columns_to_sort_by = list(filter(lambda x:x in final_data.columns, [self.identifier_column, self.datetime_column]))
        if columns_to_sort_by:
            final_data.sort_values(columns_to_sort_by, inplace=True)
        return final_data

    def permute_data(self, mkt_data_permutator, data, **kwargs):
        return_types_for_permutation = self._get_return_types_for_permutations(data, **kwargs)
        calibration_logger.info("Permuting historical data with specification {0} on datetime column {1} (tz={4}), grouped by {2} "
               "on timezone {3}".format(return_types_for_permutation, self.datetime_column, self.identifier_column,
                                        self.permutation_timezone, self.datetime_column_timezone))
        permuted_mkt_data = mkt_data_permutator.permute_data(data, return_types_for_permutation, self.datetime_column,
                                                             self.permutation_timezone, [self.identifier_column],
                                                             self.datetime_column_timezone)
        return permuted_mkt_data

    def _attach_extra_date(self, df, date_column, identifier_column, data_timezone, calendar_timezone="US/Eastern"):
        date_data = pd.DatetimeIndex(df[date_column] if date_column is not None else df.index)
        date_col_has_timezone = date_data.tzinfo is not None
        date_data = date_data.tz_localize(data_timezone) if not date_col_has_timezone else date_data
        date_data.tz_convert(calendar_timezone)
        df["__date_col"] = date_data
        max_pulled_date = date_data.normalize().max()
        data_on_last_date = df[date_data.normalize()==max_pulled_date]
        max_time_per_identifier = data_on_last_date.groupby(identifier_column).agg({"__date_col":"max"}).reset_index()
        df.drop("__date_col", axis=1, inplace=True)

        max_dates = pd.DatetimeIndex(max_time_per_identifier["__date_col"].astype("M8[ns]"))\
            .tz_localize("UTC").tz_convert(calendar_timezone)
        next_trading_date = marketTimeline.get_trading_day_using_offset(max_pulled_date,1)
        calibration_logger.info("Attaching an extra date {1} to data on step {0}".format(self.__class__.__name__, next_trading_date))
        empty_block_dates = next_trading_date + pd.TimedeltaIndex(max_dates.values - max_dates.normalize().values)
        empty_block_dates = empty_block_dates.tz_localize(calendar_timezone).tz_convert(data_timezone)
        empty_block_dates = empty_block_dates.tz_localize(None) if not date_col_has_timezone else empty_block_dates
        new_block = pd.DataFrame(index=range(len(empty_block_dates)))
        for k in df.columns:
            new_block[k] = pd.np.NaN if k not in [date_column, identifier_column] \
                else (max_time_per_identifier[identifier_column] if k==identifier_column else empty_block_dates)
        result_with_extra_day = pd.concat([df, new_block], ignore_index=True)
        return result_with_extra_day

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]:self.data}

    @classmethod
    @abstractmethod
    def get_data_requirement_type(cls):
        raise NotImplementedError("The method DataReader.get_data_requirement_type must be overriden in "
                                  "derived class")

class SQLReader(DataReader):
    '''

    Generic sql reader parent class. Supports parallel redshift queries.

    '''
    ENGINE = "Redshift"

    def __init__(self, base_query, datetime_column, identifier_column, permutation_timezone, datetime_column_timezone,
                 attach_additional_empty_data_day=False, permute_data=True, local_target_dir=None,
                 s3_temp_location=None, parallel_processes=None, read_csv_kwargs=None,
                 use_cached_data_if_available=None, cleanup_s3_when_finished=None,
                 cleanup_localtarget_when_finished=None):
        self.base_query = base_query
        self.final_query = None
        self.credentials_conf = get_config()["credentials"]

        persistent_data_caching_flag = get_config()["SQLReader"].get("persistent_data_caching", False)

        self.local_target_dir = local_target_dir or \
            ( persistent_data_caching_flag
             and get_config()["SQLReader"].get("local_cache_base_dir", None))
        if self.local_target_dir:
            self.local_target_dir = os.path.join(self.local_target_dir, self.__class__.__name__)
        self.s3_temp_location = s3_temp_location or \
            (persistent_data_caching_flag
             and get_config()["SQLReader"].get("s3_cache_base_dir", None))
        if self.s3_temp_location:
            self.s3_temp_location = os.path.join(self.s3_temp_location, self.__class__.__name__)
            if not self.s3_temp_location.endswith("/"):
                self.s3_temp_location+= "/"
        self.parallel_processes = parallel_processes or get_config()["SQLReader"].get("parallel_processes",5)
        self.read_csv_kwargs = read_csv_kwargs
        self.use_cached_data_if_available = use_cached_data_if_available if use_cached_data_if_available is not None \
            else (persistent_data_caching_flag\
            and not get_config()["SQLReader"].get("force_data_pull", True))
        self.cleanup_s3_when_finished = cleanup_s3_when_finished if cleanup_s3_when_finished is not None else\
            get_config()["SQLReader"].get("cleanup_remote_dir", True)
        self.cleanup_localtarget_when_finished = cleanup_localtarget_when_finished \
            if cleanup_localtarget_when_finished is not None\
            else not persistent_data_caching_flag
        self.data = None
        DataReader.__init__(self, datetime_column, identifier_column, permutation_timezone,
                            datetime_column_timezone, attach_additional_empty_data_day, permute_data)

    def _get_data_lineage(self):
        return self.final_query

    def _prepare_to_pull_data(self, **kwargs):
        self.final_query = self.compose_query(self.base_query, **kwargs)

    def _pull_data(self, **kwargs):
        if self.ENGINE=="Redshift":
            data_reader = ParallelQueryRedshiftDownload(self.final_query, self.credentials_conf, self.local_target_dir, self.s3_temp_location,
                                                        self.parallel_processes, self.read_csv_kwargs,
                                                        self.use_cached_data_if_available, self.cleanup_s3_when_finished,
                                                        self.cleanup_localtarget_when_finished)
            result = data_reader.execute()
            self.step_status["LocalDrive_Location"] = data_reader.local_target_dir
            self.step_status["S3_Location"] = data_reader.s3_temp_location
        #elif self.ENGINE=="Snowflake":
        #    engine = get_snowflake_engine()
        #    result = pd.read_sql(self.final_query, engine)
        return result

    @abstractmethod
    def compose_query(self, base_query, **kwargs):
        raise NotImplementedError()

    def _get_return_types_for_permutations(self, data, **kwargs):
        raise RuntimeError("{0}._get_return_types_for_permutations cannot be called with "
                           "FundamentalData. Logic has to be developed".format(self.__class__))

class CSVReader(DataReader):
    '''

    Generic csv reader parent class that takes care of date and identifier columns, pattern based reading
    and timeline permutations

    '''
    def __init__(self, files_pattern, datetime_column, identifier_column, permutation_timezone,
                 datetime_column_timezone, attach_additional_empty_data_day=False, permute_retrieved_data=True,
                 read_csv_kwargs=None):
        self.files_pattern = files_pattern
        self.files_to_read = glob(self.files_pattern)
        self.read_csv_kwargs = read_csv_kwargs or {}
        self.data = None
        DataReader.__init__(self, datetime_column, identifier_column, permutation_timezone, datetime_column_timezone,
                            attach_additional_empty_data_day, permute_retrieved_data)

    def _get_data_lineage(self):
        return self.files_to_read

    def _prepare_to_pull_data(self, **kwargs):
        pass

    def _pull_data(self, **kwargs):
        data_segments = []
        self.step_status["DataLineage"] = []
        for filename in self.files_to_read:
            this_file_data = pd.read_csv(filename, **self.read_csv_kwargs)
            data_segments.append(this_file_data)
            self.step_status["DataLineage"].append(filename)
        result = pd.concat(data_segments)
        for date_col in self.read_csv_kwargs.get("parse_dates", []):
            result[date_col] = pd.DatetimeIndex(result[date_col]).normalize()
        return result

    @classmethod
    def get_default_config(cls):
        return {"files_pattern": "*.csv", "datetime_column": "date", "identifier_column": "ticker",
                "permutation_timezone": "US/Eastern", "datetime_column_timezone": "US/Eastern",
                "attach_additional_empty_data_day": True, "permute_retrieved_data": False,
                "read_csv_kwargs": None}

class H5Reader(DataReader):
    '''

    Generic H5 reader parent class that takes care of date and identifier columns, pattern based reading
    and timeline permutations

    '''
    def __init__(self, files_pattern, datetime_column, identifier_column, permutation_timezone,
                 datetime_column_timezone, h5_data_key, attach_additional_empty_data_day=False,
                 permute_retrieved_data=True):
        self.files_pattern = files_pattern
        self.h5_data_key = h5_data_key
        self.files_to_read = glob(self.files_pattern)
        self.data = None
        DataReader.__init__(self, datetime_column, identifier_column, permutation_timezone, datetime_column_timezone,
                            attach_additional_empty_data_day, permute_retrieved_data)

    def _get_data_lineage(self):
        return self.files_to_read

    def _prepare_to_pull_data(self, **kwargs):
        pass

    def _pull_data(self, **kwargs):
        data_segments = []
        self.step_status["DataLineage"] = []
        for filename in self.files_to_read:
            with pd.HDFStore(filename, "r") as store:
                this_file_data = store[self.h5_data_key]
            data_segments.append(this_file_data)
            self.step_status["DataLineage"].append(filename)
        result = pd.concat(data_segments)
        return result

    @classmethod
    def get_default_config(cls):
        return {"files_pattern": "*.h5", "datetime_column": "date", "identifier_column": "ticker",
                "permutation_timezone": "US/Eastern", "datetime_column_timezone": "US/Eastern", "h5_data_key": "data",
                "attach_additional_empty_data_day": True, "permute_retrieved_data": False}


class S3Reader(DataReader):
    '''

    Generic AWS s3 reader class

    '''

    def __init__(self, bucket, key, index_col=0):
        self.data = None
        self.bucket = bucket
        self.key = key
        if isinstance(key, str):
            self.files_to_read = list(map(lambda x:"gs://{0}/{1}".format(bucket, x), [key]))
        else:
            self.files_to_read = list(map(lambda x:"gs://{0}/{1}".format(bucket, x), key))
        self.s3_client = None
        self.index_col = index_col or 0
        DataReader.__init__(self, "date", "dcm_security_id", "US/Eastern", "UTC", False, False)

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    def _get_data_lineage(self):
        return self.files_to_read

    def _prepare_to_pull_data(self, **kwargs):
        self.s3_client = storage.Client()

    def _pull_data(self, **kwargs):
        bucket = self.s3_client.bucket(self.bucket)
        blob = bucket.blob(self.key)
        file_content = io.BytesIO(blob.download_as_string())
        data = pd.read_csv(file_content)
        return data
