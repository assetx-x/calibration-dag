from datetime import datetime

import pytz
from luigi import ListParameter, BoolParameter, WrapperTask, Parameter

from base import S3Target, RedshiftTarget, DCMTaskParams
from pipelines.futures_ingestion.adjust_futures import AdjustVIXTask, AdjustIndexTask
from pipelines.futures_ingestion.cboe_index_files import AdjustCboeIndexFileTask, PullCboeIndexFileTask
from pipelines.futures_ingestion.merge_futures_data import MergeAdjustedFutureDataTask
from pipelines.futures_ingestion.merge_indexes_data import MergeAdjustedIndexDataTask
from pipelines.futures_ingestion.merge_raw_data import MergeRawDataTask
from pipelines.futures_ingestion.pull_futures import PullVIXTask, PullFutureTask, PullIndexTask, PullDailyIndexTask, \
    PullDailyVIXTask

# noinspection SpellCheckingInspection
from pipelines.prices_ingestion import PricePipelineHelper

universe = {
    "VX1": ("VIX", "FUT", "CFE", 1),
    "VX2": ("VIX", "FUT", "CFE", 2),
    "SPX": ("SPX", "IND", "CBOE", None),
}

# last_only - only one column with last price in input file
# last_copied_ohlc - four columns (open/high/low/close) with last price copied into all four
# all_prices_ohlc - four columns (open/high/low/close) with all four prices provided
# call_put_total_ratio - one column with call/put ratio that should be stored in "last"
# ratio_put_call_total - one column with call/put ratio that should be stored in "last"
# xls_price_under_ticker - xls document with price column named as symbol

# noinspection SpellCheckingInspection
cboe_index_files = [
    ("BFLY", "last_only", "BFLY_history.csv"),
    ("BXD", "last_only", "bxddailyprices.csv"),
    ("BXM", "last_only", "bxmcurrent.csv"),
    ("BXMC", "last_only", "BXMC_history.csv"),
    ("BXMD", "last_only", "BXMD_history.csv"),
    ("BXN", "last_only", "bxn-dailyprice.csv"),
    ("BXR", "last_only", "bxrdailyprices.csv"),
    ("BXRC", "last_only", "BXRC_History.csv"),
    ("BXY", "last_only", "bxydailyprice.csv"),
    ("CLL", "last_only", "clldailyhistory.csv"),
    ("CLLR", "last_only", "CLLR_History.csv"),
    ("CLLZ", "last_only", "CLLZ_history.csv"),
    ("CMBO", "last_only", "CMBO_history.csv"),
    ("CNDR", "last_only", "CNDR_history.csv"),
    ("LOVOL", "all_prices_ohlc", "lovol.csv"),
    ("PPUT", "last_only", "PPUT_history.csv"),
    ("PUT", "last_only", "putdailyprice.csv"),
    ("PUTR", "last_only", "PUTR_History.csv"),
    ("RVX", "all_prices_ohlc", "rvxdailyprices.csv"),
    ("RXM", "last_only", "rxm_historical.csv"),
    ("SPAI", "last_copied_ohlc", "SPAI_HistoricalValues.csv"),
    ("SPEN", "last_copied_ohlc", "spen_historicalvalues.csv"),
    ("SPRI", "last_copied_ohlc", "SPRI-historical-values.csv"),
    ("SPRO", "last_copied_ohlc", "spro-historical-values.csv"),
    ("VIX", "all_prices_ohlc", "vixcurrent.csv"),
    ("VIX3M", "all_prices_ohlc", "vix3mdailyprices.csv"),
    ("VPD", "last_only", "vpddailyprice.csv"),
    ("VPN", "last_only", "vpndailyprice.csv"),
    ("VSTG", "last_only", "VSTG_history.csv"),
    ("VVIX", "last_only", "vvixtimeseries.csv"),
    ("VXD", "all_prices_ohlc", "vxdohlcprices.csv"),
    ("VXMT", "all_prices_ohlc", "vix6mdailyprices.csv"),
    ("VXN", "all_prices_ohlc", "vxncurrent.csv"),
    ("VXST", "all_prices_ohlc", "vix9ddailyprices.csv"),
    ("WPUT", "last_only", "WPUT_history.csv"),
    ("WPTR", "last_only", "WPTR_History.csv"),

    ("VXTH", "xls_price_under_ticker", "!http://www.cboe.com/publish/vxthdata/vxth_dailydata.xls"),

    ("PCR_EQUITY", "call_put_total_ratio", "equitypc.csv"),
    ("PCR_ETP", "call_put_total_ratio", "etppc.csv"),
    ("PCR_INDEX", "call_put_total_ratio", "indexpc.csv"),
    ("PCR_TOTAL", "call_put_total_ratio", "totalpc.csv"),
    ("PCR_VIX", "ratio_put_call_total", "vixpc.csv"),
]

s3_root = "s3://dcm-data-test/fluder/futures_ingestion/"


class PullVIXStep(PullVIXTask):
    treat_failed_as_completed = False

    @property
    def stage_id(self):
        return "futures-ingestion-%s-pull" % self.alias

    def output(self):
        return {
            "stage": super(PullVIXStep, self).output(),
            "data": S3Target(s3_root + "ib_data/", force_exists=True)
        }


class PullDailyVIXStep(PullDailyVIXTask):
    treat_failed_as_completed = False

    @property
    def stage_id(self):
        return "futures-ingestion-%s-pull-daily" % self.alias

    def output(self):
        return {
            "stage": super(PullDailyVIXStep, self).output(),
            "data": S3Target(s3_root + "ib_daily_data/", force_exists=True)
        }


class PullIndexStep(PullIndexTask):
    treat_failed_as_completed = False

    @property
    def stage_id(self):
        return "futures-ingestion-%s-pull" % (self.ticker,)

    def output(self):
        return {
            "stage": super(PullIndexStep, self).output(),
            "data": S3Target(s3_root + "ib_data/", force_exists=True)
        }


class PullDailyIndexStep(PullDailyIndexTask):
    treat_failed_as_completed = False

    @property
    def stage_id(self):
        return "futures-ingestion-%s-pull-daily" % (self.ticker,)

    def output(self):
        return {
            "stage": super(PullDailyIndexStep, self).output(),
            "data": S3Target(s3_root + "ib_daily_data/", force_exists=True)
        }


class PullCboeIndexFileStep(PullCboeIndexFileTask):
    ticker = Parameter()
    file_name = Parameter(significant=False)

    @property
    def stage_id(self):
        return "futures-ingestion-%s-pull-cboe-file" % self.ticker

    def output(self):
        return {
            "stage": super(PullCboeIndexFileStep, self).output(),
        }


class MergeRawDataStep(MergeRawDataTask):
    stage_id = "futures-ingestion-merge-raw-data"
    tickers = ListParameter(default=[])

    def requires(self):
        for ticker, (symbol, security_type, exchange, index) in universe.iteritems():
            if self.tickers and ticker not in self.tickers:
                continue

            if security_type == "FUT":
                yield PullVIXStep(date=self.date, index=index)
                yield PullDailyVIXStep(date=self.date, index=index)
            else:
                yield PullIndexStep(date=self.date, exchange=exchange, security_type=security_type, ticker=symbol)
                yield PullDailyIndexStep(date=self.date, exchange=exchange, security_type=security_type, ticker=symbol)

    def output(self):
        return {
            "stage": super(MergeRawDataStep, self).output(),
            "raw_future_prices": RedshiftTarget("raw_future_prices", force_exists=True),
            "raw_daily_future_prices": RedshiftTarget("raw_daily_future_prices", force_exists=True),
            "manifest": S3Target(s3_root + "raw_manifests/raw_data_manifest_%s.json" % self.date_ymd, force_exists=True),
            "daily_manifest": S3Target(s3_root + "raw_manifests/raw_data_daily_manifest_%s.json" % self.date_ymd, force_exists=True)
        }


class AdjustVIXStep(AdjustVIXTask):
    tickers = ListParameter(default=[])

    @property
    def stage_id(self):
        return "futures-ingestion-%s-adjust" % self.alias

    def requires(self):
        return MergeRawDataStep(date=self.date, tickers=self.tickers)

    def input(self):
        return {
            "raw_future_prices": super(AdjustVIXStep, self).input()["raw_future_prices"],
            "raw_daily_future_prices": super(AdjustVIXStep, self).input()["raw_daily_future_prices"],
            "future_prices": RedshiftTarget("continuous_futures_prices_test"),
        }

    def output(self):
        return {
            "stage": super(AdjustVIXStep, self).output(),
            "adjustment_data": S3Target(s3_root + "adjusted/adjustment_data_%s_%s.csv" % (self.alias, self.date_ymd), force_exists=True),
            "adjustment_daily_data": S3Target(s3_root + "adjusted/adjustment_daily_data_%s_%s.csv" % (self.alias, self.date_ymd), force_exists=True)
        }


class MergeAdjustedFutureDataStep(MergeAdjustedFutureDataTask):
    tickers = ListParameter(default=[])
    stage_id = "futures-ingestion-merge-adjusted-future-data"

    def requires(self):
        for ticker, (symbol, security_type, exchange, index) in universe.iteritems():
            if self.tickers and ticker not in self.tickers:
                continue

            if security_type == "FUT":
                yield AdjustVIXStep(date=self.date, index=index, tickers=self.tickers)

    def output(self):
        return {
            "stage": super(MergeAdjustedFutureDataStep, self).output(),
            "future_prices": RedshiftTarget("continuous_futures_prices_test", force_exists=True),
            "daily_future_prices": RedshiftTarget("daily_continuous_futures_prices", force_exists=True),
            "manifest": S3Target(s3_root + "adjusted_manifests/adjusted_future_data_manifest_%s.json" % self.date_ymd, force_exists=True),
            "daily_manifest": S3Target(s3_root + "adjusted_manifests/adjusted_future_data_daily_manifest_%s.json" % self.date_ymd, force_exists=True)
        }


class AdjustIndexStep(AdjustIndexTask):
    tickers = ListParameter(default=[])

    @property
    def stage_id(self):
        return "futures-ingestion-%s-adjust" % self.alias

    def requires(self):
        return MergeRawDataStep(date=self.date, tickers=self.tickers)

    def input(self):
        return {
            "raw_future_prices": super(AdjustIndexStep, self).input()["raw_future_prices"],
            "raw_daily_future_prices": super(AdjustIndexStep, self).input()["raw_daily_future_prices"],
            "future_prices": RedshiftTarget("index_prices"),
        }

    def output(self):
        return {
            "stage": super(AdjustIndexStep, self).output(),
            "adjustment_data": S3Target(s3_root + "adjusted/adjustment_data_%s_%s.csv" % (self.alias, self.date_ymd), force_exists=True),
            "adjustment_daily_data": S3Target(s3_root + "adjusted/adjustment_daily_data_%s_%s.csv" % (self.alias, self.date_ymd), force_exists=True)
        }


class MergeAdjustedIndexDataStep(MergeAdjustedIndexDataTask):
    tickers = ListParameter(default=[], significant=False)
    stage_id = "futures-ingestion-merge-adjusted-index-data"

    def requires(self):
        for ticker, (symbol, security_type, exchange, index) in universe.iteritems():
            if self.tickers and ticker not in self.tickers:
                continue

            if security_type == "IND":
                yield AdjustIndexStep(date=self.date, security_type=security_type, exchange=exchange, ticker=symbol, tickers=self.tickers)

    def output(self):
        return {
            "stage": super(MergeAdjustedIndexDataStep, self).output(),
            "index_prices": RedshiftTarget("index_prices", force_exists=True),
            "daily_index_prices": RedshiftTarget("daily_index", force_exists=True),
            "manifest": S3Target(s3_root + "adjusted_manifests/adjusted_index_data_manifest_%s.json" % self.date_ymd, force_exists=True),
            "daily_manifest": S3Target(s3_root + "adjusted_manifests/adjusted_index_data_daily_manifest_%s.json" % self.date_ymd, force_exists=True)
        }


class AdjustCboeIndexFileStep(AdjustCboeIndexFileTask):
    ticker = Parameter()
    file_format = Parameter(significant=False)
    file_name = Parameter(significant=False)

    @property
    def stage_id(self):
        return "futures-ingestion-%s-adjust-cboe-file" % self.ticker

    def requires(self):
        return PullCboeIndexFileStep(date=self.date, ticker=self.ticker, file_name=self.file_name)

    def output(self):
        return {
            "stage": super(AdjustCboeIndexFileStep, self).output(),
            "adjustment_daily_data": S3Target(s3_root + "adjusted/adjustment_daily_data_%s_%s.csv" % (self.ticker, self.date_ymd), force_exists=True)
        }


class MergeCboeIndexFileDataStep(MergeAdjustedIndexDataTask):
    tickers = ListParameter(default=[], significant=False)
    stage_id = "futures-ingestion-merge-cboe-index-files"

    def requires(self):
        for ticker, file_format, file_name in cboe_index_files:
            if self.tickers and ticker not in self.tickers:
                continue
            yield AdjustCboeIndexFileStep(date=self.date, ticker=ticker, file_format=file_format, file_name=file_name)

    def output(self):
        return {
            "stage": super(MergeCboeIndexFileDataStep, self).output(),
            "daily_index_prices": RedshiftTarget("daily_index", force_exists=True),
            "daily_manifest": S3Target(s3_root + "adjusted_manifests/adjusted_index_data_daily_manifest_%s.json"
                                       % self.date_ymd, force_exists=True)
        }


class FuturesIngestionPipeline(DCMTaskParams, WrapperTask):
    tickers = ListParameter(default=[], significant=False)
    ignore_trading_hours = BoolParameter(default=True, significant=False)

    def __init__(self, *args, **kwargs):
        kwargs["date"] = PricePipelineHelper.get_proper_run_date(kwargs.get("date", datetime.utcnow()))
        super(FuturesIngestionPipeline, self).__init__(*args, **kwargs)

    def requires(self):
        return [
            MergeAdjustedFutureDataStep(date=self.date, tickers=self.tickers),
            MergeAdjustedIndexDataStep(date=self.date, tickers=self.tickers),
            MergeCboeIndexFileDataStep(date=self.date, tickers=self.tickers)
        ]

    def complete(self):
        now = self._now_local()
        weekday = now.weekday()
        if weekday > 4:  # Mon - 0, ..., Sat - 5, Sun 6
            self.logger.warning("%s is a weekend. Terminating".format('Saturday' if weekday == 5 else 'Sunday'))
            return True
        if not self.ignore_trading_hours and 8 <= now.hour <= 17:
            self.logger.warning("Inside trading hours. Time is {0}. Terminating".format(now))
            return True
        return super(FuturesIngestionPipeline, self).complete()

    @staticmethod
    def _now_local():
        return pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone("US/Eastern"))


def _main():
    import os
    from luigi import build as run_luigi_pipeline

    file_dir = os.path.dirname(os.path.realpath(__file__))
    logging_conf_path = os.path.join(file_dir, "..", "..", "conf", "luigi-logging.conf")

    date = PricePipelineHelper.get_proper_run_date(datetime.now())
    # date = datetime(2003, 8, 18)
    run_luigi_pipeline([MergeCboeIndexFileDataStep(date=date, tickers=["PPUT"])],
                       local_scheduler=True, logging_conf_file=logging_conf_path)


if __name__ == '__main__':
    _main()
