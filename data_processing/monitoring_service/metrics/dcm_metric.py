from __future__ import division

import logging
import os
import re
from io import BytesIO

import pandas as pd
import sqlalchemy as sa
from enum import Enum
from google.cloud import storage
from pyhocon import ConfigFactory

_config_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "config"))
metrics_config = ConfigFactory.parse_file(os.path.join(_config_dir, "metrics.conf"))

_logger = logging.getLogger("monitoring.metrics")


class Goodness(Enum):
    NONE = 0
    GOOD = 1
    WARNING = 2
    ERROR = 3


def get_postgres_engine():
    return sa.create_engine(
        "postgresql+psycopg2://%s:%s@%s:%s/%s" % (
            metrics_config["postgres"]["username"],
            metrics_config["postgres"]["password"],
            metrics_config["postgres"]["host"],
            metrics_config["postgres"]["port"],
            metrics_config["postgres"]["database"]
        ))


def get_redshift_engine():
    cfg = metrics_config
    sa_engine = sa.create_engine(
        'bigquery://{project_id}/{default_dataset}'.format(**cfg['gcp']))
    return sa_engine


def get_data_keys_between_dates(bucket_id, data_location_prefix, identifier, start_date, end_date, as_of_dt,
                                override_for_searching_keys=None):
    date_pattern = re.compile(r"^[\d]{4}/(0?[1-9]|1[012])/(0?[1-9]|[12][0-9]|3[01])/[a-zA-Z.\-_]+_[\d]+$")
    as_of_dt_pattern = re.compile(r"^.+_(\d+)\..+")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_id)
    data_prefix_to_search = "{0}/{1}/".format(data_location_prefix, identifier)

    if not override_for_searching_keys:
        segments_for_file = bucket.list_blobs(prefix=data_prefix_to_search)
    else:
        segments_for_file = bucket.list_blobs(prefix=override_for_searching_keys)
    segments_per_date = {}
    cob_dt_func = lambda x: x.name.replace(data_prefix_to_search, "")[0:11]

    def as_of_dt_func(z):
        result = as_of_dt_pattern.match(z.name)
        if result:
            return long(result.group(1))
        else:
            return None

    start_dt_normalized = start_date.normalize()
    end_dt_normalized = end_date.normalize()
    as_of_dt_timestamp = as_of_dt.asm8.astype(long) / 1000000000
    for k in segments_for_file:
        segment_cob_dt = cob_dt_func(k)
        as_of_dt_key = as_of_dt_func(k)

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


def read_s3_file(s3_bucket, s3_key):
    storage_client = storage.Client()
    bucket = storage_client.bucket(s3_bucket)
    blob = bucket.blob(s3_key)
    file_content = BytesIO(blob.download_as_string())
    return file_content


class Metric(object):
    """ Parent class for all metrics """

    def __init__(self, metric_name, publishers, timezone):
        self._name = metric_name
        if hasattr(publishers, "__iter__"):
            self._publishers = publishers
        else:
            self._publishers = [] if not publishers else [publishers]
        self._value = None
        self._updated_time = None
        self.timezone = timezone
        self._goodness = Goodness.NONE

    def __call__(self):
        return self.publish()

    def _calculate_metric(self):
        raise NotImplementedError()

    def get_name(self):
        return self._name

    def get_updated_time(self):
        return self._updated_time

    def get_publishers(self):
        return self._publishers

    def get_value(self):
        if self._value is None:
            self.update()

        return self._value

    def get_formatted_value(self):
        return {
            "metric": self._name,
            "updated_at": self._updated_time,
            "value": self.get_value()
        }

    def update(self):
        _logger.info("Updating %s metric value", self.__class__.__name__)
        self._value = self._calculate_metric()
        self._updated_time = pd.Timestamp.now(tz=self.timezone)
        _logger.info("Metric %s value has been updated", self.__class__.__name__)
        return self._value

    def add_publishers(self, publishers):
        if hasattr(publishers, "__iter__"):
            self._publishers += publishers
        else:
            self._publishers.append(publishers)

    def publish(self, update=True):
        if update:
            self.update()
        _logger.info("Publishing metric %s", self.__class__.__name__)
        for publisher in self._publishers:
            publisher.publish(self.get_formatted_value(), goodness=self._goodness)
        _logger.info("Metric %s has been published", self.__class__.__name__)

    def filter(self, filter_tokens):
        target_tokens = re.sub(r"([A-Z])", r",\1", self.__class__.__name__)
        target_tokens = target_tokens + ',' + self.metric_aliases()
        target_tokens = target_tokens.lower()
        return any(map(lambda s: s.lower() in target_tokens, filter_tokens))

    def metric_aliases(self):
        return ""


class MetricGroup(Metric):
    def __init__(self, name, metrics=None, *args, **kwargs):
        super(MetricGroup, self).__init__(name, **kwargs)
        if hasattr(metrics, "__iter__"):
            self._metrics = metrics
        else:
            self._metrics = [] if not metrics else [metrics]

    def _calculate_metric(self):
        return [metric.get_formatted_value() for metric in self._metrics]

    def get_metrics(self):
        return self._metrics

    def add_metrics(self, metrics):
        if hasattr(metrics, "__iter__"):
            self._metrics += metrics
        else:
            self._metrics.append(metrics)
