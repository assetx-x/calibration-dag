import re
import os
import datetime
import numpy as np
import pandas as pd
import logging.config

from time import sleep
from operator import eq
from io import StringIO
from bisect import bisect_left
from pylru import lrudecorator
from sqlalchemy import create_engine
from status_objects import StatusType
from google.cloud import storage
from commonlib.market_timeline import marketTimeline
from pipelines.prices_ingestion.config import get_config


logging.config.dictConfig(get_config().get_config("logging"))
data_adjustment_logger = logging.getLogger("adjustments_logger")
log = logging.getLogger('root')

AB_RE = re.compile(r"[-\.]([AB])$")
TICKMARKET_FILENAME_REGEX = re.compile(r'^(?P<ticker>\w+)_(?P<year>[\d]{4})_(?P<month>0?[1-9]|1[012])_'
                                       '(?P<day>0[1-9]|[12][0-9]|3[01])_X_O.zip')
DATE_REGEX = re.compile("^[\d]{4}\/(0?[1-9]|1[012])\/(0?[1-9]|[12][0-9]|3[01])\/[a-zA-Z\.\-\_]+\_[\d]+$")


def with_attempts(attempts=5):
    def decorator(func):
        def func_wrapper(*args, **kwargs):
            attempts_left = attempts
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception:
                    attempts_left -= 1
                    if attempts_left > 0:
                        sleep(5)
                        print("Attempt: %s" % attempts_left)
                        continue
                    else:
                        raise
                break
        return func_wrapper
    return decorator


@lrudecorator(size=1)
def get_redshift_engine():
    cfg = get_config()
    sa_engine = create_engine(
        'bigquery://{project_id}/{default_dataset}'.format(**cfg['gcp']))
    return sa_engine


def get_s3_client():
    session = boto3.session.Session(region_name='eu-central-1')
    s3_client = boto3.client("s3", aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                             aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                             config=boto3.session.Config(signature_version='s3v4'))
    return s3_client


def build_s3_url(bucket_id, s3_key):
    return "gs://{}/{}".format(bucket_id, s3_key)


def get_bitemporal_s3_key(data_location_prefix, identifier, run_dt, extension):
    timestamp = pd.Timestamp.now(tz="UTC").asm8.astype(np.int64) / 1000000000
    filename = "{}_{}.{}".format(identifier, timestamp, extension)
    s3_key = "{0}/{1}/{2}/{3}".format(data_location_prefix, identifier, run_dt.strftime("%Y/%m/%d"), filename)
    return s3_key


def get_bitemporal_s3_path(bucket_id, data_location_prefix, identifier, run_dt, extension):
    s3_key = get_bitemporal_s3_key(data_location_prefix, identifier, run_dt, extension)
    return build_s3_url(bucket_id, s3_key)


def split_s3_url(s3_url, return_protocol=False):
    protocol, _, s3_bucket, s3_key = s3_url.split('/', 3)
    if return_protocol:
        return protocol, s3_bucket, s3_key
    return s3_bucket, s3_key


def save_to_s3(data, s3_url=None, s3_bucket=None, s3_key=None):
    if s3_url is not None:
        if s3_bucket is not None or s3_key is not None:
            raise RuntimeError("If s3_url is passed, both s3_bucket and s3_key must be None")
        s3_bucket, s3_key = split_s3_url(s3_url)
    elif s3_bucket is None or s3_key is None:
        raise RuntimeError("If s3_url is not passed, both s3_bucket and s3_key must not be None")
    data_buffer = StringIO(data) if isinstance(data, str) else data
    storage_client = storage.Client()
    storage_client.bucket(s3_bucket).blob(s3_key).upload_from_string(data_buffer.getvalue())


@with_attempts()
def store_data_in_s3(data_buffer, bucket_id, data_location_prefix, identifier, run_dt, extension="csv"):
    s3_key = get_bitemporal_s3_key(data_location_prefix, identifier, run_dt, extension)
    storage_client = storage.Client()
    storage_client.bucket(bucket_id).blob(s3_key).upload_from_string(data_buffer.getvalue())
    return bucket_id, s3_key


@with_attempts()
def get_data_keys_between_dates(bucket_id, data_location_prefix, identifier, start_date, end_date, as_of_dt,
                                override_for_searching_keys=None):
    #TODO -BV - TO REVIEW
    date_pattern = re.compile("^[\d]{4}\/(0?[1-9]|1[012])\/(0?[1-9]|[12][0-9]|3[01])\/[a-zA-Z0-9\.\-\_]+[\d]+$")
    as_of_dt_pattern = re.compile(r"^.+_(\d+)\..+")

    storage_client = storage.Client()

    data_prefix_to_search = "{0}/{1}/".format(data_location_prefix, identifier)
    if not override_for_searching_keys:
        segments_for_file = storage_client.list_blobs(bucket_id, prefix=data_prefix_to_search)
    else:
        segments_for_file = storage_client.list_blobs(bucket_id, prefix=override_for_searching_keys)
    segments_per_date = {}
    cob_dt_func = lambda x: x.name.replace(data_prefix_to_search, "")[0:11]

    def as_of_dt_func(z):
        result = as_of_dt_pattern.match(z.name)
        if result:
            return np.int64(result.group(1))
        else:
            print("No as_of date could be found in {}".format(z.name))
            return None

    start_dt_normalized =  start_date.normalize()
    end_dt_normalized = end_date.normalize()
    as_of_dt_timestamp = as_of_dt.asm8.astype(np.int64) / 1000000000

    for k in segments_for_file:
        segment_cob_dt = cob_dt_func(k)
        as_of_dt_key = as_of_dt_func(k)

        # print "as_of_dt_key : {} k: {}".format(as_of_dt_key, k)
        if not date_pattern.match(k.name.replace(data_prefix_to_search, "")[0:-4]) \
           or pd.Timestamp(segment_cob_dt) < start_dt_normalized \
           or pd.Timestamp(segment_cob_dt) > end_dt_normalized \
           or as_of_dt_key > as_of_dt_timestamp:
            continue

        if segment_cob_dt in segments_per_date:
            segments_per_date[segment_cob_dt] = max([segments_per_date[segment_cob_dt], k], key=as_of_dt_func)
        else:
            segments_per_date[segment_cob_dt] = k
    if not segments_per_date:
        return None
    return segments_per_date.items()


def get_earliest_possible_data_key(bucket_id, data_location_prefix, identifier, start_date, end_date, as_of_dt):
    all_data_keys = get_data_keys_between_dates(bucket_id, data_location_prefix, identifier, start_date, end_date,
                                                as_of_dt)
    first_key = min([k for k in all_data_keys], key=lambda x: pd.Timestamp(x[0]))
    return pd.Timestamp(first_key[0])


def get_latest_possible_data_key(bucket_id, data_location_prefix, identifier, start_date, end_date, as_of_dt):
    all_data_keys = get_data_keys_between_dates(bucket_id, data_location_prefix, identifier, start_date, end_date,
                                                as_of_dt)
    latest_key = max([k for k in all_data_keys], key=lambda x: pd.Timestamp(x[0]))
    return pd.Timestamp(latest_key[0])


@lrudecorator(size=5)
def get_previous_business_day(dt):
    start_date = dt-pd.tseries.offsets.BDay(4)
    possible_dates = marketTimeline.get_trading_days(start_date, dt)

    i = bisect_left(possible_dates, dt)
    if not i:
        raise ValueError
    previous_dt = possible_dates[i-1]
    return previous_dt


def compare_start_date_of_data(df, ref, first_date_floor=None, df_date_column=None, ref_date_column=None,
                               comparison_operator=None):
    # NG - What to return in case of empty or None dfs here - True/False?
    # Or should we raise an error, or empty/none dfs are not expected here?
    if df is None or df.empty or ref is None or ref.empty:
        return True
    comparison_operator = eq if not comparison_operator else comparison_operator
    df_start_dt = df.index.min().date() if df_date_column is None else pd.DatetimeIndex(df[df_date_column]).min().date()
    ref_start_dt = ref.index.min().date() if ref_date_column is None else pd.DatetimeIndex(ref[ref_date_column]).min().date()
    if first_date_floor:
        first_dt = pd.Timestamp(first_date_floor).date()
        df_start_dt = max(first_dt, df_start_dt)
        ref_start_dt = ref_start_dt if ref_start_dt is pd.NaT else max(first_dt, ref_start_dt)
    return ref_start_dt is pd.NaT or comparison_operator(df_start_dt, ref_start_dt)


def compare_end_date_of_data(df, ref, df_date_column=None, ref_date_column= None,time_delta=None, last_dt_ceil=None):
    #TODO: This logic seems iffy, behavior is different between time_delta None and an actual value
    df_end_date = get_start_or_end_date(df, "end_date", df_date_column)
    ref_end_date = get_start_or_end_date(ref, "end_date", ref_date_column)
    if last_dt_ceil:
        last_dt = pd.Timestamp(last_dt_ceil).date()
        df_end_date = min(last_dt, df_end_date)
        ref_end_date = min(ref_end_date, last_dt)
    details = {"df_end_date":df_end_date, "ref_end_date": ref_end_date, "time_delta": time_delta}
    if time_delta is None:
        if df_end_date != ref_end_date:
            return (StatusType.Fail, details)
        return (StatusType.Success, details)
    elif not isinstance(time_delta, pd.Timedelta):
        raise ValueError("compare_end_date_of_data - parameter time_delta must be of type pd.Timedelta, received {0}"
                         .format(type(time_delta)))
    else:
        actual_delta = abs(pd.Timestamp(df_end_date) - pd.Timestamp(ref_end_date))
        if actual_delta > time_delta:
            return (StatusType.Warn, details)
        else:
            return (StatusType.Success, details)


def get_start_or_end_date(df, date_type, date_column=None):
    if date_type in ["start_date" ,"end_date"]:
        fnc = max if date_type == "end_date" else min
    else:
        raise ValueError("Unknown datetype {} ".format(date_type))
    if date_column:
        return fnc(df[date_column]).date()
    return fnc(df.index).date()


def verify_against_date(df, date, date_type = "end_date", date_column=None , normalize=True,
                        comparison_operator=None, warning_comparison_operator=None):
    comparison_operator = eq if not comparison_operator else comparison_operator
    comparison_date = pd.Timestamp(date)
    df_date = get_start_or_end_date(df, date_type, date_column)
    if normalize:
        comparison_date = comparison_date.normalize()
        df_date = pd.Timestamp(df_date).normalize()
    result_of_comparison =  comparison_operator(df_date, comparison_date)
    details = {"df_date": df_date, "expected_dt": comparison_date, "comparison_op": comparison_operator.__name__}
    if not result_of_comparison:
        return (-1, details)
    elif warning_comparison_operator:
        result_of_warning_operator_comp = warning_comparison_operator(df_date, comparison_date)
        details.update({"warning_op": warning_comparison_operator.__name__})
        if result_of_warning_operator_comp:
            return (1, details)
        else:
            return (0, details)
    return (1, details)


def convert_localize_timestamps(timestamp):
    """The following code should be reviewed, I am have doubts that this is what was intended when asking to
    make sure the dates were in UTC, also this is what I understood to be requested-Jason"""
    if timestamp.tzinfo:
        timestamp = timestamp.tz_convert("UTC").tz_localize(None)
    else:
        timestamp = timestamp.tz_localize("UTC")
    return timestamp


def convert_pandas_timestamp_to_bigquery_datetime(x):
    # The difference between BigQuery DATETIME and TIMESTAMP is time zone
    #    DATETIME = no time zone
    #    TIMESTAMP = with time zone
    # If the timestamp is UTC then DATETIME == TIMESTAMP
    # The assertion makes sure that the timestamp to convert is UTC
    assert(x.utcoffset().total_seconds() == 0)
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#datetime_type
    return x.strftime('%Y-%m-%dT%H:%M:%S.%f')

def convert_to_float_when_possible(x):
    try:
        return float(x)
    except:
        return x


def verify_no_missing_ca_events(ca_data_recent, ca_data_old):
    index_cols = ['ACTION DATE', 'ACTION']
    ca_recent = ca_data_recent.copy().reset_index()  # most recent CA file
    ca_old = ca_data_old.copy().reset_index()  # first day CA file

    # Exclude all records from the latest CA file that occurred after footer time of the old CA file
    footer_time_in_subset = ca_old.loc[ca_old['ACTION'] == 'FOOTER', 'ACTION DATE'].values[0]
    ca_recent = ca_recent.loc[ca_recent['ACTION DATE'] <= footer_time_in_subset].sort_values(index_cols)
    ca_old = ca_old.loc[ca_old['ACTION'] != 'FOOTER'].sort_values(index_cols)  # remove footer from the old CA file

    # Convert index_cols to tuples
    index_ca_recent = [tuple(x) for x in ca_recent[index_cols].values]
    index_ca_old = [tuple(x) for x in ca_old[index_cols].values]

    return len(set(index_ca_old).difference(set(index_ca_recent))) == 0 and len(index_ca_recent) >= len(index_ca_old)


def check_df2_subset_of_df1_v2(df2, df1, df1_date_column=None, df2_date_column=None, first_date_floor=None,
                               filter_cirteria=None, filter_column=None, numeric_column=None):
    df1 = df1.copy()
    df2 = df2.copy()
    if filter_cirteria:
        df1 =  df1[df1[filter_column].isin(filter_cirteria)]
        df2 =  df2[df2[filter_column].isin(filter_cirteria)]
    if first_date_floor:
        first_date_floor = pd.Timestamp(first_date_floor).date()
        df1 = df1[df1.index.date >= first_date_floor] if df1_date_column is None \
            else df1[df1[df1_date_column] >= first_date_floor]
        df2 = df2[df2.index.date >= first_date_floor] if df2_date_column is None \
            else df2[df2[df2_date_column] >= first_date_floor]
    if numeric_column:
        df1[numeric_column] = df1[numeric_column].apply(convert_to_float_when_possible)
        df2[numeric_column] = df2[numeric_column].apply(convert_to_float_when_possible)
    df1_date_column = df1_date_column or df1.index.name
    df1.reset_index(inplace=True)
    df2_date_column = df2_date_column or df2.index.name
    df2.reset_index(inplace=True)
    result=True
    if set(df2.columns).symmetric_difference(set(df1.columns)):
        result=False
    else:
        diff_dates = set(df2[df2_date_column]).difference(set(
            pd.merge(df2, df1, on=df2.columns.tolist(), how="inner")[df2_date_column]))
        if diff_dates:
            diff_dates = pd.DatetimeIndex(diff_dates)
            df1_different = df1[pd.DatetimeIndex(df1[df1_date_column]).isin(diff_dates)].set_index(df1_date_column)
            df2_different = df2[pd.DatetimeIndex(df2[df2_date_column]).isin(diff_dates)].set_index(df2_date_column)
            if len(df1)!=len(df2):
                result=False
            else:
                df1_numeric = df1_different._get_numeric_data()
                df2_numeric = df2_different._get_numeric_data()
                df1_non_numeric = df1_different.drop(df1_numeric.columns, axis=1)
                df2_non_numeric = df2_different.drop(df1_numeric.columns, axis=1)
                result = pd.np.allclose(df1_numeric, df2_numeric) and (df1_non_numeric==df2_non_numeric).all().all()
    return result


@lrudecorator(size=100)
def create_timeline(begin_date, end_date, start_time=datetime.time(9, 31), end_time=datetime.time(16, 0),
                    calculate_full_timeline=True):
    """ Creates a timeline to use as reference to set events.

    Args
    ----
       begin_date: datetime.date
          An initial date to start the timeline
       end_date: datetime.date
          An ending date to start the timeline
       start_time: datetime.time
          A start time to the timeline
       end_time: datetime.time
          An ending time to the timeline

    Returns
    -------
       A pandas.tseries with intra-day minutes(9:30am- 4pm) in 'UTC'.
    """
    date_timeline = marketTimeline.get_trading_days(pd.Timestamp(begin_date).date(),
                                                    pd.Timestamp(end_date).date()).values
    timeindex = None
    if calculate_full_timeline:
        timedelta_timeline = pd.timedelta_range(str(start_time), str(end_time), freq="1T").values
        datetime_timeline = pd.np.sort(pd.np.add.outer(date_timeline, timedelta_timeline).flatten())
        timeindex = pd.DatetimeIndex(datetime_timeline).tz_localize("US/Eastern").tz_convert("UTC")
    return timeindex, date_timeline


def verify_expected_time_line(df, timeline, first_date_floor=None, df_date_column= None):
    if first_date_floor:
        timeline = timeline[timeline>=first_date_floor]
        df = df[df.index.date >= first_date_floor] if df_date_column is None else df[df[df_date_column] >= first_date_floor]
    df_date = df.index.values if df_date_column is None else df[df_date_column].values
    as_expected =  set(timeline.values) - set(df_date)
    return as_expected


if __name__ == "__main__":
    ticker = 'HSBC'
    bucket_id = get_config()["raw_data_pull"]["base_data_bucket"]
    data_location_prefix = get_config()["raw_data_pull"]["TickData"]["CA"]["data_location_prefix"]

    as_of_dt = pd.Timestamp('2018-12-12 09:00:00')
    start_date = marketTimeline.get_trading_day_using_offset(as_of_dt, -1)
    end_date = marketTimeline.get_trading_day_using_offset(as_of_dt, 1)
    all_data_keys = get_data_keys_between_dates(bucket_id, data_location_prefix, ticker, start_date, end_date, as_of_dt)
    latest_data_key = get_latest_possible_data_key(bucket_id, data_location_prefix, ticker, start_date, end_date, as_of_dt)
    print("all_data_keys: ", all_data_keys)
    print("latest_data_key: ", latest_data_key)
    print("Finished!")
