import pytest
import operator
import numpy as np
import pandas as pd
import sqlalchemy as sa

from datetime import time
from io import StringIO
from pandas.util.testing import randn
from commonlib.config import get_config
from commonlib.market_timeline import marketTimeline
from pyhocon.exceptions import ConfigMissingException

from pipelines.prices_ingestion.etl_workflow_aux_functions import AB_RE
from pipelines.prices_ingestion.equity_data_holders import EquityDataHolder
from pipelines.prices_ingestion.corporate_actions_holder import CorporateActionsHolder
from pipelines.prices_ingestion.config import OpenLabel, HighLabel, LowLabel, CloseLabel, VolumeLabel

DefaultStartTime = pd.to_datetime("2016-04-01 10:00:00-04:00")
DefaultEndTime = pd.to_datetime("2016-04-01 10:10:00-04:00")


# -------------------------------------------------------------------
# Auxiliary classes
# -------------------------------------------------------------------

class DummyDataHolder(EquityDataHolder):
    def __init__(self, ticker, start_date, end_date, as_of_date, df, **kwargs):
        """
        Constructs a data holder for test data.

        :param df: A time-indexed dataframe with data_extraction.config.EquityPriceLabels columns
        """
        self.df = df
        self.data_lineage = None
        EquityDataHolder.__init__(self, ticker, start_date, end_date, as_of_date, **kwargs)

    def read_data(self):
        self.equity_data = self.df

    def set_internal_flags(self):
        self.adjusted_data_flag = False
        self.daily_frequency_flag = False
        self.dairy_frequency_flag = "moo"

    def _do_additional_initialization(self, **kwargs):
        print("_do_additional_initialization")

    def get_data_lineage(self):
        return self.data_lineage


class DummyCorporateActionsHolder(CorporateActionsHolder):
    def __init__(self, ticker, df, run_date=None, as_of_date=None):
        CorporateActionsHolder.__init__(self)
        self.data_lineage = None
        self.ticker = ticker
        self.run_date = run_date if run_date else get_config()["adjustments"]["data_end"]
        self.as_of_date = as_of_date if as_of_date else get_config()["adjustments"]["as_of_date"]
        self.df = df
        self.ca_data = self.__read_yahoo_corporate_actions()
        self.data_columns = ["CHANGE IN LISTING", "CUSIP CHANGE", "DIVIDEND", "FOOTER", "HEADER", "NAME CHANGE",
                             "SPINOFF", "SPLIT", "TICKER SYMBOL CHANGE"]
        self.ca_data_normalized = self.ca_data.set_index(["ACTION", "DATE"])

    def __read_yahoo_corporate_actions(self):
        return self.df

    def get_ca_data(self):
        return self.ca_data

    def get_data_lineage(self):
        return self.data_lineage

    def get_ca_information(self, label):
        result = self.ca_data.loc[self.ca_data["ACTION"] == label].dropna(how="all", axis=1) \
            if label in self.ca_data["ACTION"].unique() else (None if label not in self.data_columns else
                                                              pd.DataFrame(columns=list(self.ca_data.columns)))
        return result

    def get_ca_data_normalized(self):
        return self.ca_data_normalized


class ConfigMocker(object):
    def __init__(self):
        self.modify_patches = []
        self.add_patches = []

    def patch(self, config_object, key, value):
        try:
            patch = config_object, key, config_object[key]
            self.modify_patches.append(patch)
            config_object.put(key, value)
        except ConfigMissingException:
            patch = config_object, key
            self.add_patches.append(patch)
            config_object.put(key, value, append=True)

    def revert(self):
        for config_object, key, value in self.modify_patches:
            config_object.put(key, value)
        for config_object, key in self.add_patches:
            # This is needed since ConfigTree.pop() method does not works properly in pyhocon version 0.25.3
            parsed_key = config_object._parse_key(key)
            path, key_elem = parsed_key[:-1], parsed_key[-1]
            del reduce(operator.getitem, path, config_object)[key_elem]

# -------------------------------------------------------------------
# Google Auxiliary classes
# -------------------------------------------------------------------

class MockBlob(object):
    def __init__(self, name):
        self.name = name
        self.body = ''

    @property
    def key(self):
        raise NotImplementedError('use name not key')

    @property
    def size(self):
        return len(self.body)

    def download_as_string(self):
        return self.body.encode("utf-8")

    def upload_from_string(self, data):
        self.body = data

class MockBucket(object):
    def __init__(self, name):
        self._blobs = {}
        self.name = name

    def blob(self, blob_name):
        if blob_name not in self._blobs:
            mock_blob = MockBlob(blob_name)
            self._blobs[blob_name] = mock_blob
        return self._blobs[blob_name]


class MockStorageClient(object):
    def __init__(self):
        self._buckets = {}

    def bucket(self, bucket_name, user_project=None):
        assert(bucket_name in self._buckets)
        return self._buckets[bucket_name]

    def list_blobs(
        self,
        bucket_or_name,
        max_results=None,
        page_token=None,
        prefix=None,
        delimiter=None,
        start_offset=None,
        end_offset=None,
        include_trailing_delimiter=None,
        versions=None,
        projection="noAcl",
        fields=None,
        timeout=30,
        retry=30,
    ):
        mocket = self._buckets.get(bucket_or_name, None)
        assert(mocket)
        if prefix is None:
            return mocket._blobs.values()

        return [v for k,v in mocket._blobs.items() if k.startswith(prefix)]

    def assert_exists(self, bucket_id, blob_name):
        mocket = self.bucket(bucket_id)
        assert(blob_name in mocket._blobs)

    def setup_bucket(self, bucket_id):
        mocket = MockBucket(bucket_id)
        self._buckets[bucket_id] = mocket

    def create_s3_bucket_and_put_object(self, bucket_id, name, body):
        mocket = self._buckets.get(bucket_id, None)
        if mocket is None:
            mocket = MockBucket(bucket_id)
            self._buckets[bucket_id] = mocket

        mock_blob = mocket._blobs.get(name, None)
        if mock_blob is None:
            mock_blob = MockBlob(name)
            mocket._blobs[name] = mock_blob

        mock_blob.body = body


# -------------------------------------------------------------------
# Fixtures
# -------------------------------------------------------------------

@pytest.fixture(scope="session")
def config_mocker():
    config = ConfigMocker()
    yield config
    config.revert()


@pytest.fixture(scope="session")
def patched_config(config_mocker):
    cnf = get_config()
    config_mocker.patch(cnf, "raw_data_pull.base_data_bucket", "dcm-sandbox-300220-unit-test-bucket")
    config_mocker.patch(cnf, "raw_data_pull.TickData.Price.data_location_prefix", "chris/tickdata_test")
    config_mocker.patch(cnf, "raw_data_pull.TickData.CA.data_location_prefix", "chris/tickdata_ca_test")
    config_mocker.patch(cnf, "redshift.raw_tickdata_equity_price_table", "raw_equity_prices_test")
    config_mocker.patch(cnf, "redshift.equity_price_table", "equity_prices_test")
    config_mocker.patch(cnf, "adjustments.adjustment_factors", "adjustment/factors/location_test")
    config_mocker.patch(cnf, "raw_data_pull.Yahoo.PriceAndCA.price_data_location_prefix", "chris/yahoo_data/price_data_test")
    config_mocker.patch(cnf, "raw_data_pull.Yahoo.PriceAndCA.ca_data_location_prefix", "chris/yahoo_data/corporate_actions_test")

    # Setting-up additional variables used by S3YahooEquityDataHolder class constructor
    # config.patch(get_config(), "adjustments.data_start", str(DefaultStartTime))  # by default is "2000-01-03"
    # config.patch(get_config(), "adjustments.data_end", str(DefaultEndTime))  # uses os.env["ADJUSTED_DATA_END"]
    # config.patch(get_config(), "adjustments.as_of_date", str(DefaultEndTime))  # uses os.env["ADJUSTMENT_AS_OF_DATE"]
    return config_mocker


@pytest.fixture(scope="session")
def setup_s3():
    s3_resource = MockStorageClient()
    return s3_resource


@pytest.fixture(scope="module")
def setup_redshift():
    engine = sa.create_engine("sqlite:///", echo=False)
    yield engine
    engine.dispose()


# -------------------------------------------------------------------
# Auxiliary functions [for TickData]
# -------------------------------------------------------------------

def read_file(path_to_file, header, sep, **kwargs):
    data = pd.read_csv(path_to_file, header=header, sep=sep, **kwargs)
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, header=header, index=False)
    return csv_buffer.getvalue()


def prepare_aws_s3_key(ticker, prefix, data_end):
    timestamp_filename = data_end.tz_localize("UTC").asm8.astype(np.int64) / 1000000000
    aws_key = prefix + "/" + AB_RE.sub("\\1", ticker) + "/" + data_end.strftime("%Y") + "/" + \
              data_end.strftime("%m") + "/" + data_end.strftime("%d") + "/" + AB_RE.sub("\\1", ticker) + "_" + \
              str(timestamp_filename) + ".csv"
    return aws_key


def create_s3_bucket_and_put_object(storage_client, bucket_id, s3_key, body):
    storage_client.create_s3_bucket_and_put_object(bucket_id, s3_key, body)

def create_trading_hours_timeline(begin_date,end_date, freq = "1T",start_time = time(9,31), end_time=time(16,0)):
    date_timeline = marketTimeline.get_trading_days(begin_date, end_date).values
    timedelta_timeline = pd.timedelta_range(str(start_time), str(end_time), freq=freq).values
    datetime_timeline = pd.np.sort(pd.np.add.outer(date_timeline, timedelta_timeline).flatten())
    timeindex = pd.DatetimeIndex(datetime_timeline).tz_localize("US/Eastern").tz_convert("UTC")
    return timeindex


def random_price_data(start, end, freq="1T"):
    dates = create_trading_hours_timeline(start,end, freq)
    df = pd.DataFrame(randn(len(dates), 3), index=dates,
                      columns=[HighLabel, LowLabel, VolumeLabel]).abs()
    df += 0.5  # Make sure we meet minimum price threshold
    df *= 100  # And let's make things a little bigger
    df[VolumeLabel] = df[VolumeLabel].astype(int)

    # Make sure our highs and lows are what they say they are
    maxes = pd.np.maximum(df[HighLabel], df[LowLabel])
    mins = pd.np.minimum(df[HighLabel], df[LowLabel])

    df[HighLabel] = maxes
    df[LowLabel] = mins  # Generate some properly-scoped close prices...
    df[CloseLabel] = df.apply(lambda x: np.random.uniform(low=x[LowLabel], high=x[HighLabel]), axis=1)
    df[OpenLabel] = df[CloseLabel].shift(1) # Make sure open/close prices match
    df.fillna(abs(randn()+ 0.5 * 100), inplace=True)

    # Readjust anything that got out of range in open/close price adjustments
    df[HighLabel] = pd.np.maximum(pd.np.maximum(df[OpenLabel], df[CloseLabel]), df[HighLabel])
    df[LowLabel] = pd.np.minimum(pd.np.minimum(df[OpenLabel], df[CloseLabel]), df[LowLabel])
    return df
