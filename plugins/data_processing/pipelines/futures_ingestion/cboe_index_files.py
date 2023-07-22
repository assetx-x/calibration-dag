import re
from StringIO import StringIO

import pandas as pd
import pytz
import requests
import xlrd

from base import TaskWithStatus
from commonlib.config import get_config
from pipelines.prices_ingestion.etl_workflow_aux_functions import store_data_in_s3, build_s3_url, \
    get_data_keys_between_dates, save_to_s3


class PullCboeIndexFileTask(TaskWithStatus):
    url_base = get_config()["raw_data_pull"]["Cboe"]["Index"]["url_base"]
    s3_bucket = get_config()["raw_data_pull"]["base_data_bucket"]
    s3_key_prefix = get_config()["raw_data_pull"]["Cboe"]["Index"]["data_location_prefix"]

    def collect_keys(self):
        if self.file_name[0] == '!':
            cboe_file_url = self.file_name[1:]
        else:
            cboe_file_url = self.url_base + self.file_name
        self.logger.info("Downloading CBOE %s index prices from %s", self.ticker, cboe_file_url)
        response = requests.get(cboe_file_url)
        response.raise_for_status()
        self.logger.info("Downloaded %d bytes of data for %s", len(response.content), self.ticker)
        index_data = StringIO(response.content)
        s3_bucket, s3_key = store_data_in_s3(index_data, self.s3_bucket, self.s3_key_prefix, self.ticker, self.date)
        self.logger.info("Saved raw %s data file to %s", self.ticker, build_s3_url(s3_bucket, s3_key))


class AdjustCboeIndexFileTask(TaskWithStatus):
    s3_bucket = get_config()["raw_data_pull"]["base_data_bucket"]
    s3_key_prefix = get_config()["raw_data_pull"]["Cboe"]["Index"]["data_location_prefix"]

    columns_names = {
        "last_only": ["date", "close"],
        "last_copied_ohlc": ["date", "open", "high", "low", "close"],
        "all_prices_ohlc": ["date", "open", "high", "low", "close"],
    }

    as_of_start = pd.Timestamp.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")

    reDate = re.compile(r"(\d+)/(\d+)/(\d+)")

    # noinspection PyAttributeOutsideInit
    def collect_keys(self):
        dt = pd.Timestamp(self.date)
        as_of_dt = pd.Timestamp.utcnow()
        keys_to_read = get_data_keys_between_dates(self.s3_bucket, self.s3_key_prefix, self.ticker, dt, dt, as_of_dt)
        if not keys_to_read:
            raise RuntimeError("Didn't find any files for CBOE index %s" % self.ticker)

        self.from_date = dt - pd.Timedelta(days=1)
        # self.from_date = pd.Timestamp(1900, 1, 1)
        self.to_date = dt

        adjusted_data = None
        for key_segment in keys_to_read:
            if adjusted_data is not None:
                raise RuntimeError("Only expected to find one file for %d on %s" % (self.ticker, self.date_ymd))
            s3_object = key_segment[1]
            self.logger.info("Found CBOE index %s data file %s", self.ticker, s3_object)
            if self.file_format.startswith("xls"):
                adjusted_data = self._adjust_data_xls(s3_object)
            else:
                adjusted_data = self._adjust_data(s3_object)
            self._save_adjusted_data(adjusted_data)

    def _adjust_data_xls(self, s3_object):
        data_buffer = s3_object.get()['Body'].read()
        book = xlrd.open_workbook(file_contents=data_buffer)
        df = pd.read_excel(book, engine='xlrd', usecols=["Date", self.ticker])
        df.columns = ['date', 'close']
        df['date'] = [xlrd.xldate_as_datetime(dt, 0) if isinstance(dt, int) else dt for dt in df['date']]
        df = df.loc[(self.from_date < df['date']) & (df['date'] <= self.date)]
        self._add_eod_timestamp_to_date(df)
        self._add_missing_columns(df)
        df = df[["symbol", "date", "open", "close", "high", "low", "as_of_start", "as_of_end"]]
        data_buffer = StringIO()
        df.to_csv(data_buffer, date_format="%Y-%m-%d %H:%M:%S", index=False)
        return data_buffer.getvalue()

    def _add_eod_timestamp_to_date(self, df):
        df['date'] = df['date'].dt.tz_localize("US/Eastern")
        df['date'] = df['date'] + pd.Timedelta(hours=16)
        df['date'] = df['date'].dt.tz_convert(pytz.utc)

    def _add_missing_columns(self, df):
        df["symbol"] = self.ticker
        df["open"] = pd.np.nan
        df["high"] = pd.np.nan
        df["low"] = pd.np.nan
        df["as_of_start"] = pd.Timestamp.utcnow()
        df["as_of_start"] = df["as_of_start"].dt.tz_convert(pytz.utc)
        df["as_of_end"] = pd.NaT

    def _adjust_data(self, s3_object):
        raw_lines = s3_object.get()['Body'].read().split('\n')
        from_run_date = self._date_as_int(date=self.from_date)
        to_run_date = self._date_as_int(date=self.date)
        index_data = [line.strip() for line in raw_lines
                      if line and self._line_within_dates(line, from_run_date, to_run_date)]
        adjusted_data = self._adjust_data_records(index_data)
        return "\n".join(adjusted_data)

    def _line_within_dates(self, line, from_date, to_date):
        m = self.reDate.search(line)
        if m:
            line_date = self._date_as_int(m.group(3), m.group(1), m.group(2))
            return from_date < line_date <= to_date
        return False

    @staticmethod
    def _date_as_int(year=None, month=None, day=None, date=None):
        if date is not None:
            year, month, day = date.year, date.month, date.day
        return int(year) * 10000 + int(month) * 100 + int(day)

    def _adjust_data_records(self, index_data):
        adjusted_data = ["symbol,date,open,close,high,low,as_of_start,as_of_end"]
        for data_record in index_data:
            data_record = self._add_ticker(data_record)
            data_record = self._fix_cob_date(data_record)
            data_record = self._fix_price_fields(data_record)
            data_record = self._add_as_of_fields(data_record)
            adjusted_data.append(data_record)
            if len(data_record.split(',')) != 8:
                self.logger.error("********* Invalid record!!!!!: " + data_record)
                raise RuntimeError("********* Invalid record!!!!!")
        self.logger.info("Adjusted %d records for %s", len(adjusted_data) - 1, self.ticker)
        return adjusted_data

    def _add_ticker(self, line):
        return self.ticker + "," + line

    def _fix_cob_date(self, line):
        m = self.reDate.search(line)
        if m:
            trade_timestamp = pd.Timestamp("%s-%s-%s 16:00:00" % (m.group(3), m.group(1), m.group(2)), tz="US/Eastern")
            trade_timestamp = trade_timestamp.tz_convert(pytz.utc)
            return self.reDate.sub(trade_timestamp.strftime("%Y-%m-%d %H:%M:%S"), line)
        return line

    def _fix_price_fields(self, line):
        if self.file_format == "last_only":
            return self._fix_price_fields_last_only(line)
        if self.file_format == "last_copied_ohlc":
            return self._fix_price_fields_last_copied_ohlc(line)
        if self.file_format == "all_prices_ohlc":
            return self._fix_price_fields_all_prices_ohlc(line)
        if self.file_format == "call_put_total_ratio":
            return self._fix_price_fields_call_put_total_ratio(line)
        if self.file_format == "ratio_put_call_total":
            return self._fix_price_fields_ratio_put_call_total(line)
        raise RuntimeError("Unknown file format '%s'" % self.file_format)

    def _fix_price_fields_last_only(self, line):
        return re.sub(r"^([^,]+),([^,]+),\s*([^,]*),?$", r"\1,\2,,\3,,", line)

    def _fix_price_fields_last_copied_ohlc(self, line):
        return re.sub(r",\s*([^,]+),\s*([^,]+),\s*([^,]+),\s*([^,]+)$", r",,\4,,", line)

    def _fix_price_fields_all_prices_ohlc(self, line):
        line = re.sub(r",\s*n/a", r",", line)
        return re.sub(r",\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*([^,]*)$", r",\1,\4,\2,\3", line)

    def _fix_price_fields_call_put_total_ratio(self, line):
        return re.sub(r",\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*([^,]*)$", r",,\4,,", line)

    def _fix_price_fields_ratio_put_call_total(self, line):
        return re.sub(r",\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*([^,]*)$", r",,\1,,", line)

    def _add_as_of_fields(self, line):
        return "%s,%s," % (line, self.as_of_start)

    def _save_adjusted_data(self, adjusted_data):
        s3_url = self.output()["adjustment_daily_data"].path
        save_to_s3(adjusted_data, s3_url=s3_url)
        self.logger.info("Saved adjusted %s index data to %s", self.ticker, s3_url)


def _try_pandas_to_open_xls():
    # ["symbol,date,open,close,high,low,as_of_start,as_of_end"]
    pd.set_option('expand_frame_repr', False)
    df = pd.read_excel(r"D:\Downloads\vxth_dailydata.xls", usecols=["Date", "VXTH"])
    df.columns = ['date', 'close']
    import xlrd
    df['date'] = [xlrd.xldate_as_datetime(dt, 0) if isinstance(dt, int) else dt for dt in df['date']]
    df['date'] = df['date'].dt.tz_localize("US/Eastern")
    df['date'] = df['date'] + pd.Timedelta(hours=16)
    df['date'] = df['date'].dt.tz_convert(pytz.utc)
    df["symbol"] = "VXTH"
    df["open"] = pd.np.nan
    df["high"] = pd.np.nan
    df["low"] = pd.np.nan
    # df["as_of_start"] = pd.to_datetime(pd.Timestamp.utcnow(), utc=True)
    df["as_of_start"] = pd.Timestamp.utcnow()
    df["as_of_start"] = df["as_of_start"].dt.tz_convert(pytz.utc)
    df["as_of_end"] = pd.NaT
    df = df[["symbol", "date", "open", "close", "high", "low", "as_of_start", "as_of_end"]]
    data_buffer = StringIO()
    df.to_csv(data_buffer, date_format="%Y-%m-%d %H:%M:%S", index=False)
    value = data_buffer.getvalue()
    print(value)


if __name__ == '__main__':
    _try_pandas_to_open_xls()
