from datetime import datetime, date, timedelta, time

import boto3
import pytz
from luigi import WrapperTask, DateParameter, ExternalTask, Parameter, BoolParameter, ListParameter

from adjustment_factors import AdjustmentFactorsTask
from base import DCMTaskParams, StageResultTarget


class ExternalStageDependency(ExternalTask):
    stage_id = Parameter(description="Id of a stage of an external pipeline on which this stage depends on")
    treat_failed_as_completed = BoolParameter(default=True, significant=False)

    def output(self):
        return StageResultTarget(self.stage_id, treat_failed_as_completed=self.treat_failed_as_completed)


# noinspection PyTypeChecker
class AdjustmentFactorsStep(AdjustmentFactorsTask):
    tickers = ListParameter(default=[], significant=False)
    start_date = DateParameter(significant=False)
    end_date = DateParameter(significant=False)

    stage_id = "adjustment-factors"

    def requires(self):
        return [
            ExternalStageDependency("%s-price-ingestion-yahoo-%s" % (self.date_ymd, str(ticker)))
            for ticker in self.tickers
        ]

    def output(self):
        return {
            "stage": super(AdjustmentFactorsStep, self).output()
        }


class AdjustmentFactorsPipeline(DCMTaskParams, WrapperTask):
    start_date = DateParameter(default=date(2000, 1, 3), significant=False)
    end_date = DateParameter(default=None, significant=False)

    def __init__(self, *args, **kwargs):
        run_date = self._default_pipeline_date()
        kwargs["date"] = kwargs.get("date", run_date)
        super(AdjustmentFactorsPipeline, self).__init__(*args, **kwargs)
        self.end_date = kwargs.get("end_date", run_date)
        self.tickers = self._load_tickers()

    def requires(self):
        return AdjustmentFactorsStep(tickers=self.tickers, date=self.date,
                                     start_date=self.start_date, end_date=self.end_date)

    def complete(self):
        now = self._now_local()
        if now.hour < 4:
            self.logger.warning("Won't run until 4 am, terminating. Time is %s" % str(now))
            return True
        return super(AdjustmentFactorsPipeline, self).complete()

    def _default_pipeline_date(self):
        now = self._now_local()
        weekday_offsets = [3, 1, 1, 1, 1, 1, 2]  # Mon - 0, ..., Sat - 5, Sun 6
        run_date = now - timedelta(days=weekday_offsets[now.weekday()])
        return datetime.combine(run_date.date(), time(0, 0))

    @staticmethod
    def _now_local():
        return pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone("US/Eastern"))

    @staticmethod
    def _load_tickers():
        s3 = boto3.client("s3")
        tickers = [
            line.strip() for line in s3.get_object(
                Bucket="dcm-data-test",
                Key="chris/universe_adjustment_factors.txt"
            )["Body"].read().split("\n")
            if line.strip()
        ]
        return tickers
