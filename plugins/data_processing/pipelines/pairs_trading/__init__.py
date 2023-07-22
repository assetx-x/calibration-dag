from luigi import ExternalTask, Parameter
from datetime import datetime, timedelta, time
from base import S3Target, DCMTaskParams, credentials_conf
from pipelines.pairs_trading.close_prices_ingestion import ClosePricesIngestionTask
from datetime import date
from luigi import WrapperTask
import sqlalchemy as sa

from pipelines.pairs_trading.coint_filter import CointFilterTask
from pipelines.pairs_trading.delta_local_filter import DeltaLocalFilterTask
from pipelines.pairs_trading.format_calibration import FormatCalibrationTask
from pipelines.pairs_trading.stat_filter import StatFilterTask

OUTPUT_DIR = "s3://dcm-production-processes/overnight/calibrations/cointegration/results/%s/"
config = {
    "start_date": date(2013, 7, 23),
    "executor_memory": "10G",
    "close_prices_s3_path": OUTPUT_DIR + "%s_close_prices.csv",
    "coint_filter_s3_path": OUTPUT_DIR + "%s_coint_filter_result.csv",
    "delta_local_filter_s3_path": OUTPUT_DIR + "%s_delta_local_filter_result.csv",
    "stat_filter_s3_path": OUTPUT_DIR + "%s_filteredDelta.csv",
    "calibration_s3_path": OUTPUT_DIR + "%s_pairsParameters.csv"
}


class TickersUniverseStep(ExternalTask):
    def output(self):
        return S3Target("s3://dcm-data-test/fluder/pairs_trading/pairs_trading_universe.csv")


class ClosePricesIngestionStep(ClosePricesIngestionTask):
    stage_id = "pairs-trading-close-prices-ingestion"

    def requires(self):
        return {
            "universe_file": TickersUniverseStep()
        }

    def output(self):
        next_day = self.date + timedelta(1)

        return {
            "stage": super(ClosePricesIngestionStep, self).output(),
            "data": S3Target(config["close_prices_s3_path"] % (
                next_day.strftime("%Y%m%d"),
                next_day.strftime("%Y%m%d"),
            ), force_exists=True)
        }


class CointFilterStep(CointFilterTask):
    executor_memory = config["executor_memory"]
    stage_id = "pairs-trading-coint-filter"
    start_date = config["start_date"]

    def requires(self):
        return {
            "close_prices": ClosePricesIngestionStep(date=self.date)
        }

    def output(self):
        next_day = self.date + timedelta(1)

        return {
            "stage": super(CointFilterTask, self).output(),
            "data": S3Target(config["coint_filter_s3_path"] % (
                next_day.strftime("%Y%m%d"),
                next_day.strftime("%Y%m%d")
            ), force_exists=True)
        }


class DeltaLocalFilterStep(DeltaLocalFilterTask):
    executor_memory = config["executor_memory"]
    stage_id = "pairs-trading-delta-local-filter"
    start_date = config["start_date"]

    def requires(self):
        return {
            "pairs": CointFilterStep(date=self.date),
            "close_prices": ClosePricesIngestionStep(date=self.date)
        }

    def output(self):
        next_day = self.date + timedelta(1)

        return {
            "stage": super(DeltaLocalFilterStep, self).output(),
            "data": S3Target(config["delta_local_filter_s3_path"] % (
                next_day.strftime("%Y%m%d"),
                next_day.strftime("%Y%m%d")
            ), force_exists=True)
        }


class StatFilterStep(StatFilterTask):
    executor_memory = config["executor_memory"]
    stage_id = "pairs-trading-stat-filter"
    start_date = config["start_date"]

    def requires(self):
        return {
            "pairs": DeltaLocalFilterStep(date=self.date),
            "close_prices": ClosePricesIngestionStep(date=self.date)
        }

    def output(self):
        next_day = self.date + timedelta(1)

        return {
            "stage": super(StatFilterStep, self).output(),
            "data": S3Target(config["stat_filter_s3_path"] % (
                next_day.strftime("%Y%m%d"),
                next_day.strftime("%Y%m%d")
            ), force_exists=True)
        }


class FormatCalibrationStep(FormatCalibrationTask):
    stage_id = "pairs-trading-format-calibration"

    def requires(self):
        return {
            "pairs": StatFilterStep(date=self.date)
        }

    def output(self):
        next_day = self.date + timedelta(1)

        return {
            "stage": super(FormatCalibrationStep, self).output(),
            "data": S3Target(config["calibration_s3_path"] % (
                next_day.strftime("%Y%m%d"),
                next_day.strftime("%Y%m%d")
            ), force_exists=True)
        }


def get_last_tue_or_thu(_date):
    weekday = _date.weekday()
    if weekday == 0:
        new_date = _date - timedelta(4)
    elif weekday < 3:
        new_date = _date - timedelta(weekday - 1)
    else:
        new_date = _date - timedelta(weekday - 3)

    return new_date


class PairsTradingPipeline(DCMTaskParams, WrapperTask):
    def __init__(self, *args, **kwargs):
        current_date = kwargs.get("date", datetime.now())
        kwargs["date"] = get_last_tue_or_thu(current_date)
        kwargs["date"] = datetime.combine(kwargs["date"].date(), time(0, 0)) - timedelta(days=1)

        super(PairsTradingPipeline, self).__init__(*args, **kwargs)

    reset_stage = Parameter(default=None)
    reset_executed = False
    stage_reset_map = {
        "close_prices": [
            "{date}-pairs-trading-close-prices-ingestion",
            "{date}-pairs-trading-coint-filter",
            "{date}-pairs-trading-delta-local-filter",
            "{date}-pairs-trading-stat-filter",
            "{date}-pairs-trading-format-calibration"
        ],
        "coint_filter": [
            "{date}-pairs-trading-coint-filter",
            "{date}-pairs-trading-delta-local-filter",
            "{date}-pairs-trading-stat-filter",
            "{date}-pairs-trading-format-calibration"
        ],
        "delta_local_filter": [
            "{date}-pairs-trading-delta-local-filter",
            "{date}-pairs-trading-stat-filter",
            "{date}-pairs-trading-format-calibration"
        ],
        "stat_filter": [
            "{date}-pairs-trading-stat-filter",
            "{date}-pairs-trading-format-calibration"
        ],
        "calibration": [
            "{date}-pairs-trading-format-calibration"
        ]
    }

    def requires(self):
        if self.reset_stage is not None and not PairsTradingPipeline.reset_executed:
            subsequent_stages = self.stage_reset_map[self.reset_stage]
            engine = sa.create_engine(
                "postgresql+psycopg2://%s:%s@%s:%s/%s" % (
                    credentials_conf["postgres"]["username"],
                    credentials_conf["postgres"]["password"],
                    credentials_conf["postgres"]["host"],
                    credentials_conf["postgres"]["port"],
                    credentials_conf["postgres"]["database"]
                ), echo=True)

            stages_to_reset = set()
            for stage in subsequent_stages:
                stages_to_reset.add(
                    stage.format(date=self.date.strftime("%Y%m%d")))
            in_clause = "('" + "', '".join(stages_to_reset) + "')"
            engine.execute(
                "DELETE FROM pipeline_status WHERE stage_id in " + in_clause)
            engine.dispose()
            PairsTradingPipeline.reset_executed = True

        yield FormatCalibrationStep(date=self.date)
