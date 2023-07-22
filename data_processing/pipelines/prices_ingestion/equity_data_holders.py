import logging
import eventlet
import pandas as pd

from io import BytesIO
from functools import partial
from google.cloud import storage
from commonlib.util_functions import retry
from status_objects import Status, StatusType, STATUS_TICKER_KEY
from pipelines.prices_ingestion.config import (DateLabel, OpenLabel, HighLabel, LowLabel, CloseLabel,
                                               VolumeLabel, EquityPriceLabels, get_config)
from pipelines.prices_ingestion.etl_workflow_aux_functions import (get_redshift_engine, get_data_keys_between_dates, convert_localize_timestamps, convert_pandas_timestamp_to_bigquery_datetime,
                                        AB_RE, DATE_REGEX)
from pipelines.prices_ingestion.corporate_actions_holder import (EquityETLBaseClass, S3TickDataCorporateActionsHolder,
                                      S3YahooCorporateActionsHolder, S3TickDataCorporateActionsHandler)
from pipelines.common.pipeline_util import read_sql_with_retry


import time
logger = logging.getLogger("equity_data_holders")


def get_raw_daily_bar_from_tickdata_s3(ticker, date, asof_date):
    try:
        raw_data = S3TickDataEquityDataHolder(ticker, date, date, asof_date).get_data()
        raw_data = raw_data.ix[raw_data.index.indexer_between_time("9:31:00", "16:00:00"),:]
        extra_data = raw_data.groupby(lambda t:t.date).agg({OpenLabel: "first", CloseLabel:"last", HighLabel:max,
                                                            LowLabel:min, VolumeLabel:sum})
        extra_data.index = pd.DatetimeIndex(extra_data.index)
    except RuntimeError:
        extra_data = pd.DataFrame(columns=[OpenLabel, CloseLabel, HighLabel, LowLabel, VolumeLabel])
    return extra_data


class AdjustmentException(Exception):
    def __init__(self, status):
        self.status = status
        self.message = self.status[status.MESSAGE_LABEL]
        if not Status.QA_NAME_LABEL in self.status.keys():
            self.status[Status.QA_NAME_LABEL] = "Unknown"
        if not DEFAULT_STATUS_LOCATION_INFO_KEY in self.status.keys():
            self.status[DEFAULT_STATUS_LOCATION_INFO_KEY] = DEFAULT_STATUS_LOCATION_INFO_VALUE


class AdjustmentQAException(AdjustmentException):
    def __init__(self, status):
        AdjustmentException.__init__(self, status)


class AdjustmentOperationException(AdjustmentException):
    def __init__(self, status):
                AdjustmentException.__init__(self, status)


class EquityDataHolder(EquityETLBaseClass):
    def __init__(self, ticker, start_date=None, end_date=None, as_of_date=None, **kwargs):
        self.ticker = ticker
        self.data_lineage = None
        self.start_date = start_date if start_date else get_config()["adjustments"]["data_start"]
        self.end_date = end_date if end_date else get_config()["adjustments"]["data_end"]
        self.as_of_date = as_of_date if as_of_date else get_config()["adjustments"]["as_of_date"]
        self.equity_data = None
        self.adjusted_data_flag = None
        self.daily_frequency_flag = None
        self.read_data(**kwargs)
        self.set_internal_flags()
        self._do_additional_initialization(**kwargs)
        self.cut_data_between_adjustment_dates()
        self.round_data()
        self.verify_all_data_filled()
        self.verify_hi_greater_lo()
        if self.adjusted_data_flag:
            self.verify_open_is_close()
        self.verify_open_close_in_range()
        self.verify_price_non_negative_zero()

    def read_data(self, **kwargs):
        """ This function reads the actual data and place them into equity_data. Columns will be checked in
        # verify_all_data_filled. """
        raise RuntimeError("The function read_data needs to be overriden in derived class")

    def set_internal_flags(self):
        """ This function is used to initialize the flags adjusted_data_flag and daily_frequency_flag. """
        raise RuntimeError("The function set_internal_flag needs to be overriden in derived class")

    def _do_additional_initialization(self, **kwargs):
        """ Custom intialization method. """
        raise RuntimeError("The function _do_additional_initialization needs to be overriden in derived class")

    def verify_all_data_filled(self):
        if not isinstance(self.equity_data, pd.DataFrame):
            raise RuntimeError("The field equity_data is not a DataFrame")
        if set(self.equity_data.columns).symmetric_difference(set(EquityPriceLabels)):
            raise RuntimeError("The dataframe EquityData does not have the right columns."
                               "Expected {} got {}".format(EquityPriceLabels, self.equity_data.columns))
        if self.adjusted_data_flag is None or self.daily_frequency_flag is None:
            raise RuntimeError("The flags adjusted_data_flag and daily_frequency_flag are not properly set")
        self.equity_data.index.name = DateLabel

    def verify_hi_greater_lo(self):
        """ Verify high price data is greater than low price data. """
        if any(self.equity_data[HighLabel] < self.equity_data[LowLabel]):
            # data_adjustment_logger.info("At validation hight greater than low in '%s' for ticker '%s' the column "  \
            #                             "values were exhanged.", self.__class__, self.ticker)
            hi_temp = pd.np.maximum(self.equity_data[LowLabel], self.equity_data[HighLabel])
            lo_temp = pd.np.minimum(self.equity_data[HighLabel], self.equity_data[LowLabel])
            self.equity_data[HighLabel] = hi_temp
            self.equity_data[LowLabel] = lo_temp
        # data_adjustment_logger.info('Validation passed: verify_hi_greater_lo for %s, ticker %s',
        #                             self.__class__, self.ticker)

    def get_data_lineage(self):
        return self.data_lineage

    def verify_open_is_close(self):
        """ Verify that the open price is the close price from the previous period. """
        self.equity_data["Close_prev"] = self.equity_data[CloseLabel].shift()
        change_ratio = self.equity_data[OpenLabel] / self.equity_data["Close_prev"]
        min_tolerance_change_ratio = get_config()["adjustments"]["verification_parameters"] \
            ["equity_data_holder_tests"]["min_tolerance_change_ratio"]
        max_tolerance_change_ratio = get_config()["adjustments"]["verification_parameters"] \
            ["equity_data_holder_tests"]["max_tolerance_change_ratio"]
        if not change_ratio.dropna().empty and (change_ratio.any() < min_tolerance_change_ratio or change_ratio.any() > max_tolerance_change_ratio):
            # data_adjustment_logger.error("There is a problem with open and close price data ticker" + \
            #                              " {0} for {1} ".format(self.ticker, self.__class__))
            raise ValueError("There is a problem with open and close price data with ticker {0}. Verify open price "
                             "matches with close price of the previous period for {1}.".format(self.ticker,
                                                                                               self.__class__))
        self.equity_data.drop(["Close_prev"], axis=1, inplace=True)
        # data_adjustment_logger.info('Validation passed: verify_open_is_close for %s, ticker %s',
        #                             self.__class__, self.ticker)

    def verify_open_close_in_range(self):
        """ Verify that the open and close prices are within the low and high price range. """
        if any((self.equity_data[OpenLabel] < self.equity_data[LowLabel]) |
               (self.equity_data[OpenLabel] > self.equity_data[HighLabel])):
            self.equity_data[OpenLabel] = pd.np.minimum(pd.np.maximum(self.equity_data[OpenLabel],
                                                                      self.equity_data[LowLabel]),
                                                        self.equity_data[HighLabel])
        if any((self.equity_data[CloseLabel] < self.equity_data[LowLabel]) |
               (self.equity_data[CloseLabel] > self.equity_data[HighLabel])):
            self.equity_data[CloseLabel] = pd.np.minimum(pd.np.maximum(self.equity_data[CloseLabel],
                                                                       self.equity_data[LowLabel]),
                                                         self.equity_data[HighLabel])
        # data_adjustment_logger.info('Validation passed: verify_open_close_in_range for %s, ticker %s',
        #              self.__class__, self.ticker)

    def verify_price_non_negative_zero(self):
        """ Verifies equity data for negative or min_price_check_amount prices."""
        # Check for no negative prices
        negative_check = (self.equity_data.min() < 0).any()

        # Checks that prices are not less that min_price_check_amount
        min_price_check_amount = get_config()["adjustments"]["verification_parameters"]["equity_data_holder_tests"] \
            ["min_price_check_amount"]
        zero_prices_check = (self.equity_data.drop(VolumeLabel, axis=1).min() <= min_price_check_amount).any()

        if negative_check or zero_prices_check:
            # data_adjustment_logger.error("There is a problem with ticker {0} due to negative or zero"
            #                " prices. Please verify {1}".format(self.ticker, self.__class__))
            raise ValueError("There is a problem with ticker {0} due to negative or zero prices." \
                             " Please verify {1}".format(self.ticker, self.__class__))
        # data_adjustment_logger.info('Validation passed: verify_price_non_negative_zero for %s, ticker %s',
        #              self.__class__, self.ticker)

    def cut_data_between_adjustment_dates(self, cut_end_date=None):
        if self.equity_data.empty:
            return
        _equity_data_index = self.equity_data.index \
            if self.equity_data.index.tzinfo else self.equity_data.index.tz_localize("UTC")
        self.equity_data = self.equity_data[_equity_data_index >= pd.Timestamp(self.start_date).tz_localize("UTC")]
        if cut_end_date:
            self.equity_data = self.equity_data[self.equity_data.index <= cut_end_date]

    def round_data(self, decimals=4):
        self.equity_data = pd.np.around(self.equity_data, decimals)

    def is_data_adjusted(self):
        return self.adjusted_data_flag

    def is_daily_data(self):
        return self.daily_frequency_flag

    def get_data(self):
        return self.equity_data

    def get_ticker(self):
        return self.ticker


class S3EquityDataHolder(EquityDataHolder):
    EVENTLET_INITIALIZED = False

    def __init__(self, ticker, data_location_prefix, start_date=None, end_date=None, as_of_date=None,
                 override_s3_source=None):
        if override_s3_source:
            s3_location = override_s3_source.replace("gs://","")
            self.bucket_id = s3_location.split("/")[0]
            self.data_location_prefix = s3_location.replace("{0}/".format(self.bucket_id),"")
            self.data_location_prefix = self.data_location_prefix[0:self.data_location_prefix.find(ticker)-1]
            self.override_s3_source = s3_location.replace("{0}/".format(self.bucket_id),"")
        else:
            self.bucket_id = get_config()["raw_data_pull"]["base_data_bucket"]
            self.data_location_prefix = data_location_prefix
            self.override_s3_source = None
        self.start_date = start_date if start_date else get_config()["adjustments"]["data_start"]
        self.end_date = end_date if end_date else get_config()["adjustments"]["data_end"]
        self.as_of_date = as_of_date if as_of_date else get_config()["adjustments"]["as_of_date"]
        self.adjustment_unix_timestamp = pd.Timestamp(self.as_of_date)
        self.date_pattern = DATE_REGEX
        EquityDataHolder.__init__(self, ticker, self.start_date, self.end_date, self.as_of_date)

    def _get_key_iterator_for_data_segments(self, data_location_prefix = None):
        data_location_prefix = self.data_location_prefix if not data_location_prefix else data_location_prefix
        return get_data_keys_between_dates(self.bucket_id, data_location_prefix, self.ticker,
                                           pd.Timestamp(self.start_date),
                                           pd.Timestamp(self.end_date),
                                           self.adjustment_unix_timestamp,
                                           override_for_searching_keys=self.override_s3_source)

    def _get_single_segment(self, dt_handler_key, s3_client, bucket_id, **kwargs):
        dt, s3_key_handler = dt_handler_key
        if s3_key_handler.size == 0:
            raise RuntimeError("S3EquityDataHolder._get_single_segment - File {0} has zero size"
                               .format(s3_key_handler.key))
        # data_adjustment_logger.debug("About to read data segment %s", s3_key_handler.key)
        a_file = BytesIO(s3_client.bucket(bucket_id).blob(s3_key_handler.name).download_as_string())
        res = pd.read_csv(a_file, **kwargs)
        # data_adjustment_logger.debug("Read data segment %s", s3_key_handler.key)
        return res

    def _read_raw_data_segments(self, data_segments_iterator, **kwargs):
        if not data_segments_iterator:
            raise RuntimeError("There are no segments to read for ticker {0}, as_of_date={1}"
                               .format(self.ticker, self.adjustment_unix_timestamp))
        if not S3EquityDataHolder.EVENTLET_INITIALIZED:
            eventlet.patcher.monkey_patch(all=get_config()["eventlet_patcher_flag"])
            S3EquityDataHolder.EVENTLET_INITIALIZED = True
        storage_client = storage.Client()
        reader_func = partial(self._get_single_segment, s3_client=storage_client, bucket_id=self.bucket_id, **kwargs)
        segment_data = []

        green_pool = eventlet.GreenPool(size = 50)
        for raw_data in green_pool.imap(reader_func, data_segments_iterator):
            segment_data.append(raw_data)

        all_segment_data = pd.concat(segment_data)
        return all_segment_data


class S3TickDataEquityDataHolder(S3EquityDataHolder):
    def __init__(self, ticker, start_date=None, end_date=None, as_of_date=None):
        self.start_date = start_date if start_date else get_config()["adjustments"]["data_start"]
        self.end_date = end_date if end_date else get_config()["adjustments"]["data_end"]
        self.as_of_date = as_of_date if as_of_date else get_config()["adjustments"]["as_of_date"]
        dlp = get_config()["raw_data_pull"]["TickData"]["Price"]["data_location_prefix"]
        S3EquityDataHolder.__init__(self, AB_RE.sub("\\1", ticker), dlp, self.start_date, self.end_date,
                                    self.as_of_date)

    def read_data(self):
        data_segments = self._get_key_iterator_for_data_segments()
        self.equity_data = self._read_raw_data_segments(data_segments, parse_dates=[6], header=None)
        self.equity_data = self.equity_data[self.equity_data.columns[[0,1,2,3,4,6]]]
        self.equity_data.columns = [OpenLabel, HighLabel, LowLabel, CloseLabel, VolumeLabel, DateLabel]
        self.equity_data.set_index(DateLabel, inplace=True)
        self.equity_data.sort_index(inplace=True)
        self.equity_data.index = self.equity_data.index.tz_localize("US/Eastern")

    def set_internal_flags(self):
        self.adjusted_data_flag = False
        self.daily_frequency_flag = False

    def _do_additional_initialization(self, **kwargs):
        pass


class S3TickDataOneDayEquityDataHolder(S3EquityDataHolder):
    def __init__(self, ticker, start_date=None, end_date=None, as_of_date=None):
        self.start_date = start_date if start_date else get_config()["adjustments"]["data_start"]
        self.end_date = end_date if end_date else get_config()["adjustments"]["data_end"]
        self.as_of_date = as_of_date if as_of_date else get_config()["adjustments"]["as_of_date"]
        self.ticker = ticker
        self.read_data()

    def __read_tick_data_prices_for_one_day(self):
        bucket_id = get_config()["raw_data_pull"]["base_data_bucket"]
        data_location_prefix = get_config()["raw_data_pull"]["TickData"]["Price"]["data_location_prefix"]
        key_iterator = get_data_keys_between_dates(bucket_id, data_location_prefix, self.ticker,
                                                   pd.Timestamp(self.end_date), pd.Timestamp(self.end_date),
                                                   pd.Timestamp(self.as_of_date))
        if not key_iterator:
            raise RuntimeError("ERROR! could not find suitable data to read as of {0} for ticker {1} on bucket {2}"
                               ", location {3} on date {4}".format(self.as_of_date, self.ticker,
                                                       bucket_id, data_location_prefix, self.end_date))
        dt, s3_key_handler = max([k for k in key_iterator], key=lambda x:x[0])
        if s3_key_handler.size == 0:
            raise RuntimeError("S3YahooCorporateActionsHolder.__read_yahoo_corporate_actions - File {0} has zero size"
                               .format(s3_key_handler.key))

        storage_client = storage.Client()
        price_file = BytesIO(storage_client.bucket(bucket_id).blob(s3_key_handler.name).download_as_string())
        res = pd.read_csv(price_file, parse_dates=[6,7])

        self.data_lineage = "gs://{0}/{1}".format(bucket_id, s3_key_handler.key)
        return res

    def read_data(self):
        self.equity_data = self.__read_tick_data_prices_for_one_day()

        self.equity_data.columns = [OpenLabel, HighLabel, LowLabel, CloseLabel, VolumeLabel, "ticker",
                                    "Date", "as_of"]
        self.equity_data.set_index(DateLabel, inplace=True)
        self.equity_data.sort_index(inplace=True)
        self.equity_data.index = self.equity_data.index.tz_localize("US/Eastern")

    def set_internal_flags(self):
        self.adjusted_data_flag = False
        self.daily_frequency_flag = False

    def _do_additional_initialization(self, **kwargs):
        pass


class S3YahooEquityDataHolder(S3EquityDataHolder):
    DICT_COLUMNS = {
        "Open": OpenLabel, "High": HighLabel, "Low": LowLabel,
        "Close": CloseLabel, "Volume": VolumeLabel, "Date": DateLabel
    }

    def __init__(self, ticker, corporate_actions_holder, start_date=None, end_date=None, as_of_date=None,
                 override_with_lineage=None):
        self.ticker = ticker
        self.start_date = start_date if start_date else get_config()["adjustments"]["data_start"]
        self.end_date = end_date if end_date else get_config()["adjustments"]["data_end"]
        self.as_of_date = as_of_date if as_of_date else get_config()["adjustments"]["as_of_date"]
        dlp = get_config()["raw_data_pull"]["Yahoo"]["PriceAndCA"]["price_data_location_prefix"]
        self.adjusted_close = None
        if not isinstance(corporate_actions_holder, S3YahooCorporateActionsHolder):
            raise TypeError("S3YahooEquityDataHolder.__init__ - the parameter corporate_actions_holder must be of type "
                            "S3YahooCorporateActionsHolder. Received object of type {0}"
                            .format(type(corporate_actions_holder)))
        self.corporate_actions_holder = corporate_actions_holder
        S3EquityDataHolder.__init__(self, ticker, dlp, self.start_date, self.end_date, self.as_of_date,
                                    override_with_lineage)
        self.cut_data_between_adjustment_dates(cut_end_date=self.end_date)

    def __get_adjustment_factor_from_corporate_actions(self):
        corp_actions = self.corporate_actions_holder.get_ca_data().copy()
        corp_actions["Value"] = corp_actions["Value"].astype(str)
        price_data = self.equity_data.copy()

        splits = corp_actions[corp_actions["Action"]=="SPLIT"].copy(deep=True)
        splits["Value"] = splits["Value"].str.split(":").apply(lambda x:float(x[0])/float(x[1]))
        price_data["SPLITS"] = splits["Value"]
        price_data["SPLITS"] = price_data["SPLITS"].shift(-1).fillna(value=1.0)

        dividends = corp_actions[corp_actions["Action"]=="DIVIDEND"].copy(deep=True)
        dividends["Value"] = dividends["Value"].astype(float)
        price_data["DIVIDENDS"] = dividends["Value"]
        price_data["DIVIDENDS"] = price_data["DIVIDENDS"].shift(-1)

        price_data = price_data.sort_index(ascending=False)
        split_cumulative_factor = ((1.0 / price_data["SPLITS"]).cumprod())
        price_data["CLOSE_ADJ_SPLIT_ONLY"] = price_data[CloseLabel] * split_cumulative_factor
        dividend_cumulative_factor = (1.0 - (price_data["DIVIDENDS"] / price_data["CLOSE_ADJ_SPLIT_ONLY"]))\
            .fillna(value=1.0).cumprod()
        price_data["CLOSE_ADJ_SPLIT_AND_DIV"] = price_data["CLOSE_ADJ_SPLIT_ONLY"] * dividend_cumulative_factor
        adj_factor = price_data[CloseLabel] / price_data["CLOSE_ADJ_SPLIT_AND_DIV"]
        adj_factor = adj_factor.reindex(self.equity_data.index)

        return adj_factor

    def read_data(self):
        key_iterator = self._get_key_iterator_for_data_segments()
        if not key_iterator:
            raise RuntimeError("ERROR! could not find suitable data to read as of {0} for ticker {1} on bucket {2}"
                               ", location {3}".format(self.as_of_date, self.ticker, self.bucket_id,
                                                       self.data_location_prefix))
        most_recent_date = max([k for k in key_iterator], key=lambda x:x[0])
        self.equity_data = self._read_raw_data_segments(iter([most_recent_date]), parse_dates=["Date"])
        self.data_lineage ="gs://{0}/{1}".format(self.bucket_id, most_recent_date[1].name)
        self.equity_data.rename(columns=self.DICT_COLUMNS, inplace=True)
        self.equity_data.set_index(DateLabel, inplace=True)
        self.equity_data.index.name = DateLabel
        self.equity_data.sort_index(inplace=True)

        yahoo_data_format_change = get_config()["adjustments"]["s3_yahoo_price_data_read"]\
            ["date_yahoo_changed_file_format"]
        if pd.Timestamp(most_recent_date[0])>pd.Timestamp(yahoo_data_format_change):
            raw_adj_factor_for_data = (self.equity_data[CloseLabel] / self.equity_data["Adj Close"])
            self.equity_data[OpenLabel] *= raw_adj_factor_for_data
            self.equity_data[HighLabel] *= raw_adj_factor_for_data
            self.equity_data[LowLabel] *= raw_adj_factor_for_data
            self.equity_data.drop(["Adj Close"], inplace=True, axis=1)

            commonly_missing_dates_in_yahoo = ["2016-06-29"]
            additional_data = [get_raw_daily_bar_from_tickdata_s3(self.ticker, k, self.as_of_date)
                               for k in commonly_missing_dates_in_yahoo if k not in self.equity_data.index]
            if additional_data:
                self.equity_data = pd.concat([self.equity_data] + additional_data)
                self.equity_data.sort_index(inplace=True)
            self.full_multiplicative_adjustment_factor = self.__get_adjustment_factor_from_corporate_actions()
        else:
            self.full_multiplicative_adjustment_factor = self.equity_data[CloseLabel] / self.equity_data["Adj Close"]
            self.equity_data.drop(["Adj Close"], inplace=True, axis=1)

    def get_full_multiplicative_adjustment_factor(self):
        return self.full_multiplicative_adjustment_factor

    def get_split_multiplicative_adjustment_factor(self):
        return self.split_multiplicative_adjustment_factor

    def set_internal_flags(self):
        self.adjusted_data_flag = False
        self.daily_frequency_flag = True

    def set_adjusted_close(self):
        if self.equity_data.empty or not self.equity_data.columns.__contains__("Adj Close"):
            status_obj = Status(StatusType.Fail,"Data does not containt 'Adj Close' column", "set_adjusted_close")
            status_obj[STATUS_TICKER_KEY] = self.ticker
            status_obj[STATUS_CLASS_KEY] = self.__class__
            raise AdjustmentException(status_obj)
        self.adjusted_close = self.equity_data["Adj Close"]

    def get_corporate_actions(self):
        return self.corporate_actions_holder

    def _do_additional_initialization(self, **kwargs):
        factors_data = self.corporate_actions_holder.ca_data
        factors = factors_data[factors_data["Action"] == "SPLIT"]
        self.split_multiplicative_adjustment_factor = pd.Series(1, index=self.equity_data.index)
        if len(factors):
            factors.loc[:, "Value"] = factors["Value"].apply(lambda x:float(x.split(":")[0]) / float(x.split(":")[1]))
            factors.loc[:, "Value"] = factors["Value"].cumprod()
            factors.index = pd.DatetimeIndex(factors.index)
            factors = factors.reindex(self.equity_data.index)
            factors.sort_index(ascending=False, inplace=True)
            factors = factors.shift(1)
            factors.ffill(inplace=True)
            factors.fillna(1, inplace=True)
            adj_volume = self.equity_data[[VolumeLabel]]
            volume_data = pd.merge(adj_volume, factors[["Value"]], left_index=True, right_index=True, how="left")
            self.split_multiplicative_adjustment_factor = volume_data["Value"]
            self.equity_data[VolumeLabel] = volume_data[VolumeLabel] / volume_data["Value"]

    def cut_data_between_adjustment_dates(self, cut_end_date=None):
        self.equity_data = self.equity_data[self.equity_data.index >= self.start_date]
        self.full_multiplicative_adjustment_factor = self.full_multiplicative_adjustment_factor[
            self.full_multiplicative_adjustment_factor.index >= self.start_date]
        self.split_multiplicative_adjustment_factor = self.split_multiplicative_adjustment_factor[
            self.split_multiplicative_adjustment_factor.index >= self.start_date]
        if cut_end_date:
            self.equity_data = self.equity_data[self.equity_data.index <= cut_end_date]
            self.full_multiplicative_adjustment_factor = self.full_multiplicative_adjustment_factor[
                self.full_multiplicative_adjustment_factor.index <= cut_end_date]
            self.split_multiplicative_adjustment_factor = self.split_multiplicative_adjustment_factor[
                self.split_multiplicative_adjustment_factor.index <= cut_end_date]


class RedshiftEquityDataHolderBaseClass(EquityDataHolder):

    def __init__(self, ticker, start_date=None, end_date=None, as_of_date=None, override_with_lineage=None, **kwargs):
        self.ticker = ticker
        self.start_date = start_date if start_date else get_config()["adjustments"]["data_start"]
        self.end_date = end_date if end_date else get_config()["adjustments"]["data_end"]
        self.as_of_date = as_of_date if as_of_date else get_config()["adjustments"]["as_of_date"]
        self.override_query = override_with_lineage
        EquityDataHolder.__init__(self, ticker, self.start_date, self.end_date, self.as_of_date, **kwargs)

    def get_engine(self):
        return get_redshift_engine()

    @retry(Exception, tries=6, delay=2, backoff=2, logger=logger)
    def _query_engine(self, query):
        engine = self.get_engine()
        if self.override_query:
            equity_data = pd.read_sql_query(self.override_query, engine, index_col="cob", parse_dates=["cob"])
            self.data_lineage = self.override_query
        else:
            equity_data = pd.read_sql_query(query, engine, index_col="cob", parse_dates=["cob"])
            self.data_lineage = query
        engine.dispose()
        return equity_data


class RedshiftEquityDataHolder(RedshiftEquityDataHolderBaseClass):
    """ Query to redshift database for equity minute data per ticker """
    DICT_COLUMNS = {
        "open": OpenLabel, "high": HighLabel, "low": LowLabel, "close": CloseLabel, "volume": VolumeLabel
    }

    def read_data(self):
        query = """
            SELECT "open", "high", low, close, volume, cob
            FROM {table}
            WHERE ticker = '{ticker}' and as_of_end IS NULL
            """.format(ticker=self.ticker, table=get_config()["redshift"]["equity_price_table"])

        self.equity_data = self._query_engine(query)
        self.equity_data.rename(columns=self.DICT_COLUMNS, inplace=True)
        self.equity_data.index.name = DateLabel
        self.equity_data.sort_index(inplace=True)
        self.equity_data.tz_localize("UTC")
        self.data_lineage = get_config()["redshift"]["equity_price_table"]

    def set_internal_flags(self):
        self.adjusted_data_flag = True
        self.daily_frequency_flag = False

    def _do_additional_initialization(self, **kwargs):
        pass


class RedshiftRawTickDataDataHolder(RedshiftEquityDataHolderBaseClass):
    DICT_COLUMNS = {
        "open": OpenLabel, "high": HighLabel, "low": LowLabel, "close": CloseLabel, "volume": VolumeLabel
    }

    # TODO: Napoleon proposed to simplify the queries by removing JOIN since in raw_equity_prices there is no records
    #  where min(as_of) != max(as_of), thus there is no need to select max(as_of).
    #  To check this statement run in WorkBench the following:
    #    WITH dd SS
    #    (SELECT ticker, cob, min(as_of) as minasof, max(as_of) as maxasof FROM raw_equity_prices GROUP BY ticker, cob)
    #    SELECT * FROM dd WHERE minasof <> maxasof
    #  Therefore, the queries for raw prices retrieval, both single- and multi-symbols, can be simplified to:
    #    SELECT  "open", "high", "low", "close", volume, cob
    #    FROM {raw_table}
    #    WHERE ticker='{ticker}' and as_of <= '{as_of_date}' and date(cob) BETWEEN '{start_date}' and '{end_date}'
    #    OR ticker=.... (only for multi-symbols query)
    #  This should be tested more carefully and implemented since it gives [at least] X 2 faster query execution time!!
    #  Though, if somehow happen that there will be 2+ records on same cob with different as_of -> it will not work!

    def _prepare_single_symbol_sql_query(self, raw_table, ticker, *timestamps):
        utc_start, utc_end, utc_as_of = timestamps
        no_tz_as_of = convert_pandas_timestamp_to_bigquery_datetime(utc_as_of)

        """
        !!!DO NOT CHANGE THIS CODE WITHOUT ENSURING THAT THE QA PROCESS IS ALSO CHANGED ACCORDINGLY!!!
        """
        #BQ_TESTING
        query = """
        SELECT rep.open, rep.high, rep.low, rep.close, rep.volume, rep.cob
        FROM {raw_table} rep
        JOIN (SELECT ticker, cob, max(as_of) as as_of, farm_fingerprint
        FROM {raw_table}
        WHERE ticker = '{ticker}' AND
              farm_fingerprint = FARM_FINGERPRINT('{ticker}') AND
              as_of <= '{as_of_date}' and date(cob) BETWEEN '{start_date}' and '{end_date}'
        GROUP BY ticker, cob, farm_fingerprint) l ON
          rep.ticker = l.ticker AND
          rep.farm_fingerprint = FARM_FINGERPRINT(l.ticker) AND
          rep.cob = l.cob AND
          rep.as_of = l.as_of
        WHERE rep.ticker='{ticker}' AND rep.farm_fingerprint=FARM_FINGERPRINT('{ticker}') AND
              rep.as_of <= '{as_of_date}' AND date(rep.cob) BETWEEN '{start_date}' and '{end_date}';
        """.format(ticker=ticker,
                   raw_table=raw_table,
                   start_date=utc_start.date(),
                   end_date=utc_end.date(),
                   as_of_date=no_tz_as_of,
                   table=raw_table)

        return query

    def _prepare_multi_symbols_sql_query(self, raw_table, ticker_history_df, *timestamps):
        utc_start, utc_end, utc_as_of = timestamps
        no_tz_as_of = convert_pandas_timestamp_to_bigquery_datetime(utc_as_of)

        query_header = """
            SELECT rep.open, rep.high, rep.low, rep.close, rep.volume, rep.cob
            FROM {raw_table} rep
            JOIN
            (SELECT ticker, cob, max(as_of) as as_of, farm_fingerprint FROM {raw_table}
            """.format(raw_table=raw_table)

        query_body = ""
        query_footer_where = ""
        for index, value in ticker_history_df.iterrows():
            symbol = value['Symbol']
            start_date = value['as_of_start'] if utc_start < value['as_of_start'] else utc_start
            end_date = value['as_of_end'] if value['as_of_end'] is not pd.NaT else pd.Timestamp.today('UTC').normalize()
            end_date = end_date if utc_end > end_date else utc_end

            if index == 0:
                query_body += """
                    WHERE (ticker='{symbol}' AND farm_fingerprint=FARM_FINGERPRINT('{symbol}')
                          AND as_of <= '{as_of_date}' AND date(cob) BETWEEN '{start_date}' AND '{end_date}')
                    """.format(symbol=symbol, as_of_date=no_tz_as_of, start_date=start_date.date(), end_date=end_date.date())

                query_footer_where += """
                    (rep.ticker='{symbol}' and rep.farm_fingerprint=FARM_FINGERPRINT('{symbol}'))
                    """.format(symbol=symbol)
            else:
                query_body += """
                    OR (ticker='{symbol}' AND farm_fingerprint=FARM_FINGERPRINT('{symbol}')
                       AND as_of <= '{as_of_date}' AND date(cob) BETWEEN '{start_date}' AND '{end_date}')
                    """.format(symbol=symbol, as_of_date=no_tz_as_of, start_date=start_date.date(), end_date=end_date.date())

                query_footer_where += """
                    OR (rep.ticker='{symbol}' and rep.farm_fingerprint=FARM_FINGERPRINT('{symbol}'))
                    """.format(symbol=symbol)

        query_footer = """
            GROUP BY ticker, cob, farm_fingerprint) l
            ON
              rep.ticker = l.ticker AND
              rep.farm_fingerprint = l.farm_fingerprint AND
              rep.cob = l.cob AND
              rep.as_of = l.as_of
            WHERE rep.as_of <= '{as_of_date}' and date(rep.cob) BETWEEN '{start_date}' and '{end_date}'
            """.format(start_date=utc_start.date(),
                       end_date=utc_end.date(),
                       as_of_date=no_tz_as_of,
                       table=raw_table)
        query_footer += "and (" + query_footer_where + ");"

        query_composed = query_header + query_body + query_footer
        query = '\n'.join([line.strip() for line in query_composed.split('\n') if line.strip() != ''])
        return query

    def _pull_equity_data(self, query):
        self.equity_data = self._query_engine(query)
        self.equity_data.rename(columns=self.DICT_COLUMNS, inplace=True)
        self.equity_data.index.name = DateLabel
        self.equity_data.sort_index(inplace=True)
        self.equity_data.index = self.equity_data.index.tz_localize("US/Eastern")

    def read_data(self):
        ticker = self.ticker
        utc_start = pd.Timestamp(self.start_date)
        utc_end = pd.Timestamp(self.end_date)
        utc_as_of = pd.Timestamp(self.as_of_date)
        ticker_name_history = S3TickDataCorporateActionsHandler.get_tickdata_ca_ticker_name_change_history(
            ticker, utc_start, utc_end, utc_as_of)
        raw_table = get_config()["redshift"]["raw_tickdata_equity_price_table"]
        timestamps = map(convert_localize_timestamps, [utc_start, utc_end, utc_as_of])

        # Here we select symbol(s) that are used by given ticker only after the utc_start date.
        # For instance: Ticker AMN has 2 symbols as of 2018-12-14: AHS [2001-11-13 -- 2016-10-27) and
        # AMN [2016-10-27 -- ...). If utc_start = 2018-10-27, we can limit ticker_name_history table to only
        # AMN symbol since utc_start >= AHS['as_of_end']. The only symbol that covers requested utc_start is symbol AMN.
        ticker_name_history = ticker_name_history.loc[(ticker_name_history['as_of_end'] >= utc_start)] \
            .reset_index(drop=True)

        if ticker_name_history.empty:  # this is the case when ticker was delisted
            self.equity_data = pd.DataFrame(columns=EquityPriceLabels)
            self.equity_data.index.name = DateLabel
            logger.warn("Ticker {0} has no symbol change history available on start_date={1}. It might be delisted "
                        "or there is a problem with CA pull".format(ticker, utc_start.date()))
        elif ticker_name_history.shape[0] == 1:  # single symbol name in ticker_name_history
            query = self._prepare_single_symbol_sql_query(raw_table, ticker, *timestamps)
            self._pull_equity_data(query)
        else:  # multiple symbols were used by company (i.e. > 1 symbols in ticker_name_history)
            ticker_history_df = ticker_name_history if isinstance(ticker_name_history, pd.DataFrame) \
                else ticker_name_history.ticker_history
            ticker_history_df["as_of_start"] = list(map(convert_localize_timestamps, ticker_history_df["as_of_start"]))
            ticker_history_df["as_of_end"] = list(map(convert_localize_timestamps, ticker_history_df["as_of_end"]))
            query = self._prepare_multi_symbols_sql_query(raw_table, ticker_history_df, *timestamps)
            self._pull_equity_data(query)

    def set_internal_flags(self):
        self.adjusted_data_flag = False
        self.daily_frequency_flag = False

    def _do_additional_initialization(self, **kwargs):
        pass


def _get_last_adjustment_dates(ticker_list, as_of_date):
    # collect last adjustment date
    table_name = get_config()["redshift"]["equity_price_table"]
    if as_of_date.tzinfo:
        as_of_date = pd.Timestamp(as_of_date).tz_convert("UTC").tz_localize(None)
    sql_engine = get_redshift_engine()

    ticker_str_list = ",".join(map(lambda x:"'{0}'".format(x), ticker_list))
    farm_fingerprint_list = ",".join(map(lambda x:"farm_fingerprint('{0}')".format(x), ticker_list))

    sql_command = (
        "SELECT ticker, MIN(as_of_start) AS min_as_of "
        "FROM {table_name} "
        "WHERE (as_of_start <= '{asof}' AND (as_of_end >= '{asof}' OR as_of_end IS NULL)) "
        "AND ticker in ({ticker_str_list}) "
        "AND farm_fingerprint in ({farm_fingerprint_list}) "
        "GROUP BY ticker"
    ).format(asof=as_of_date, table_name=table_name,
             ticker_str_list=ticker_str_list, farm_fingerprint_list=farm_fingerprint_list)

    df = read_sql_with_retry(sql_command, sql_engine)
    sql_engine.dispose()
    last_adjustment_dates = dict(zip(df["ticker"], df["min_as_of"]))
    last_adjustment_dates = {k:last_adjustment_dates.get(k, pd.Timestamp("1900-01-01")) for k in ticker_list}
    return last_adjustment_dates


def _get_latest_ca_dates(ticker_list, as_of_date, run_date):
    start_dt = (as_of_date - pd.tseries.frequencies.to_offset("5B")).normalize()
    end_dt = as_of_date.normalize()
    last_ca_dates = {}
    for ticker in ticker_list:
        try:
            all_data = S3TickDataCorporateActionsHolder(ticker, run_date=run_date, as_of_date=as_of_date,
                                                        start_dt=start_dt, end_dt=end_dt)
            all_data = all_data.ca_data_normalized
            adjustment_cas = pd.concat([all_data["RATIO"].dropna().reset_index().drop_duplicates()
                                            .rename(columns={"RATIO": "ADJUSTMENT FACTOR"}),
                                           all_data["ADJUSTMENT FACTOR"].dropna().reset_index().drop_duplicates()])

            last_ca_date = adjustment_cas["ACTION DATE"].max().strftime("%Y-%m-%d") if len(adjustment_cas) \
                    else all_data.ix["HEADER", :].index[0].strftime("%Y-%m-%d")
            last_ca_dates[ticker] = pd.Timestamp(last_ca_date)
        except BaseException as e:
            last_ca_dates[ticker] = pd.Timestamp("1900-01-01")
    return last_ca_dates


def determine_if_full_adjustment_is_required(ticker_list, as_of_date, run_date):
    if not hasattr(ticker_list, "__iter__") or isinstance(ticker_list, str):
        ticker_list = [ticker_list]

    last_adj_dates = _get_last_adjustment_dates(ticker_list, as_of_date)
    last_ca_dates = _get_latest_ca_dates(ticker_list, as_of_date, run_date)

    full_adjustments = {k: last_ca_dates[k].tz_localize(None).normalize()
                        > last_adj_dates[k].tz_localize(None).normalize() for k in ticker_list}
    return full_adjustments


if __name__ == "__main__":
    # === Test1
    # determine_if_full_adjustment_is_required(["AAPL", "JPM", "SPY"], pd.Timestamp.now("UTC"),
    #                                          pd.Timestamp("2017-12-08"))
    #
    # test = S3YahooEquityDataHolder("AAPL",
    #                                S3YahooCorporateActionsHolder("AAPL", run_date=pd.Timestamp("2017-06-28"),
    #                                                              as_of_date=pd.Timestamp("2017-06-30")),
    #                                start_date=pd.Timestamp("2002-07-24"),
    #                                end_date=pd.Timestamp("2017-06-28"),
    #                                as_of_date=pd.Timestamp("2017-06-30"))
    # ca_holder = S3TickDataCorporateActionsHolder("AAPL", run_date=pd.Timestamp("2017-06-28"),
    #                                              as_of_date=pd.Timestamp.now())
    # print("done")

    # === Test2
    from data_retrieval_workflow_steps import DATA_TICKDATA_PRICE_HANDLER_LABEL
    from pipelines.prices_ingestion.data_retrieval_workflow_steps import create_data_retrieval_stage
    from etl_workflow_steps import EquitiesETLTaskParameters, compose_main_flow_and_engine_for_task

    ticker = 'AMN'
    start_dt = pd.Timestamp("2011-10-27")
    end_dt = pd.Timestamp("2018-06-25")
    run_dt = pd.Timestamp("2018-06-25")
    as_of_dt = pd.Timestamp("2018-06-26 08:30:00")  # pd.Timestamp.now()

    task_params = EquitiesETLTaskParameters(ticker=ticker, as_of_dt=as_of_dt, run_dt=run_dt,
                                            start_dt=start_dt, end_dt=end_dt)

    data_retrieval_stage = create_data_retrieval_stage(True)
    task_engine, main_flow = compose_main_flow_and_engine_for_task("Data_Retrieval_Sample", task_params,
                                                                   data_retrieval_stage)

    task_engine.run(main_flow)
    data_retrieval_results = task_engine.storage.fetch_all()

    raw_equity_data_retrieval_stage = data_retrieval_results[DATA_TICKDATA_PRICE_HANDLER_LABEL] \
        .get_data_holder().equity_data
    raw_equity_data_retrieval_query = data_retrieval_results[DATA_TICKDATA_PRICE_HANDLER_LABEL] \
        .get_data_holder().get_data_lineage()

    print(raw_equity_data_retrieval_query)
