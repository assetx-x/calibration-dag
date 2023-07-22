import os
import json
import boto3
import pandas as pd
import warnings

from datetime import datetime
from io import StringIO
from pyspark import SparkContext
warnings.filterwarnings("ignore")


def get_s3_client():
    session = boto3.session.Session(region_name='eu-central-1')
    s3_client = boto3.client("s3", aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                             aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                             config=boto3.session.Config(signature_version='s3v4'))
    return s3_client


def get_all_s3_keys(s3_client, s3_bucket, prefix):
    """Get a list of all keys in an S3 bucket."""
    keys = []
    kwargs = {'Bucket': s3_bucket, 'Prefix': prefix}
    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            keys.append(obj['Key'])
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break
    return keys


def take_timestamp(x):
    head, _ = os.path.splitext(x)
    return (head.split('_')[1])


def get_s3_keys_requiring_format_adjustment(keys):
    dirnames = list(set(map(lambda x: os.path.dirname(x), keys)))
    keys_wo_adjustment = []
    for dirname in dirnames:
        keys_in_dir = [key for key in keys if dirname in key]
        if not any(map(lambda x: '_new_format' in x, keys_in_dir)):
            if len(keys_in_dir) > 1:
                file_names = map(lambda x: os.path.basename(x), keys_in_dir)
                latest_file = sorted(file_names, key=take_timestamp)[-1]
                latest_key = [key for key in keys_in_dir if latest_file in os.path.basename(key)]
                keys_wo_adjustment.append(latest_key)
            else:
                keys_wo_adjustment.append(keys_in_dir)
    return sorted(reduce(lambda x, y: x + y, keys_wo_adjustment)) if keys_wo_adjustment else []


def adjustment_is_needed(csv_string):
    first_row = pd.read_csv(StringIO(csv_string), nrows=1, header=None, sep=",")
    n_cols = len(first_row.columns) if not first_row.dropna().empty else 0
    if n_cols == 7:
        return True
    return False


def adjust_tickdataprices_to_new_format_spark(s3_bucket, key, ticker, as_of):
    s3_client = get_s3_client()
    body = s3_client.get_object(Bucket=s3_bucket, Key=key)['Body']
    csv_string = body.read().decode('utf-8')
    adjustment_needed = len(csv_string) and adjustment_is_needed(csv_string)
    if adjustment_needed:
        raw_data = pd.read_csv(
            StringIO(csv_string), names=["day", "time", "open", "high", "low", "close", "volume"],
            header=None, sep=",", parse_dates={"cob": ["day", "time"]})

        raw_data["ticker"] = ticker
        raw_data["as_of"] = as_of
        raw_data = raw_data[["open", "high", "low", "close", "volume", "ticker", "cob", "as_of"]]

        csv_data = StringIO()
        raw_data.to_csv(csv_data, header=False, index=False)
        new_key = key.split('.csv')[0] + '_new_format.csv'
        s3_client.put_object(Bucket=s3_bucket, Key=new_key, Body=csv_data.getvalue())
        return new_key
    return


# =======================================================================================
# Global settings
# =======================================================================================
s3_bucket = "dcm-data-test"
subdir = 'chris/tickdata/'
delimiter = '/'
as_of = datetime.now()
run_dt = pd.Timestamp.now().normalize()
s3_client = get_s3_client()


# =======================================================================================
# Load all tickers that need adjustement from old to new format (575 names)
# =======================================================================================
key = "sergii/S3_Redshift_tickdata_analysis/"
filename = 's3_tickdata_tickers_to_be_converted_to_new_format.dat'

body = s3_client.get_object(Bucket=s3_bucket, Key="".join([key, filename]))['Body']
list_of_tickers_need_format_adjustment = json.loads(body.read().decode('utf-8'))
list_of_tickers_need_format_adjustment = \
    map(lambda x: x.encode('ascii', 'ignore'), list_of_tickers_need_format_adjustment)

print('Number of tickers that need adjustment of format: %d' % len(list(list_of_tickers_need_format_adjustment)))


# =======================================================================================
# Adjusting raw equity prices csv files to new format [Spark version]
# =======================================================================================
run_format_adjustment = True

if run_format_adjustment:
    sc = SparkContext(master="local[{0}]".format(min(len(list(list_of_tickers_need_format_adjustment)), 24)),
                      appName="tickdata_format_adjustment")
    try:
        data_to_extract = []
        for ticker in list_of_tickers_need_format_adjustment:
            prefix = subdir + ticker + '/'
            keys = get_all_s3_keys(s3_client, s3_bucket, prefix)
            s3_keys_for_adjustment = get_s3_keys_requiring_format_adjustment(keys)
            for key in s3_keys_for_adjustment:
                data_to_extract.append({"ticker": ticker, "s3_bucket": s3_bucket, "key": key, "as_of": as_of})
        # adjust_tickdataprices_to_new_format_spark() method will check each file on whether adjustment is needed
        print('Number of files that potentially need adjustment: %d' % (len(data_to_extract)))
        data_to_extract_rdd = sc.parallelize(data_to_extract)
        results_rdd = data_to_extract_rdd.map(lambda x: adjust_tickdataprices_to_new_format_spark(**x))
        results = results_rdd.collect()
        print('Number of adjusted files: %d' % len(results))
    finally:
        sc.stop()

print('DONE!!!')