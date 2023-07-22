from datetime import datetime, time, timedelta

import pandas as pd
from luigi import build as run_luigi_pipeline
from luigi import DateParameter
from luigi import Parameter
from luigi import WrapperTask
from sqlalchemy import create_engine

from base import DCMTaskParams, StagedTask, credentials_conf
from etl_workflow_steps import EarningsCalendarTaskParams, BaseETLStage
from pipelines.earnings_ingestion.earnings_ingestion_steps import compose_earnings_calendar_workflow
from pipelines.earnings_ingestion.pull_earnings import PullEarningsTask
from pipelines.prices_ingestion import get_config


class EarningsIngestionStep(StagedTask):
    def get_task_params(self):
        forward_horizon = \
            pd.tseries.frequencies.to_offset(get_config()["consolidated_earnings"]["forward_time_offset"])
        backward_horizon = \
            pd.tseries.frequencies.to_offset(get_config()["consolidated_earnings"]["backward_time_offset"])
        if self.start_date:
            start_date = pd.Timestamp(self.start_date)
        else:
            start_date = pd.Timestamp(self.date)
        end_date = pd.Timestamp(self.end_date or self.date)

        return EarningsCalendarTaskParams(
            as_of_dt=pd.Timestamp(self.pull_as_of + timedelta(days=1)),
            run_dt=pd.Timestamp(self.date),
            start_dt=start_date,
            end_dt=end_date,
            backward_horizon=backward_horizon,
            forward_horizon=forward_horizon
        )

    def output(self):
        return {
            "stage": super(EarningsIngestionStep, self).output()
        }


class EarningsPullStep(EarningsIngestionStep):
    @property
    def stage_id(self):
        return "%s-earnings-ingestion-pull" % (self.date.strftime("%Y%m%d"),)

    def create_stage_func(self):
        return compose_earnings_calendar_workflow()[0]


class EarningsQAStep(EarningsIngestionStep):
    @property
    def stage_id(self):
        return "%s-earnings-ingestion-qa" % (self.date.strftime("%Y%m%d"),)

    def requires(self):
        return EarningsPullStep(
            date=self.date,
            pull_as_of=self.pull_as_of,
            start_date=self.start_date,
            end_date=self.end_date
        )

    def create_stage_func(self):
        stages = compose_earnings_calendar_workflow()
        return BaseETLStage("QA", "Stage", stages[1], stages[2], stages[3])


class EarningsIngestionPipeline(DCMTaskParams, WrapperTask):
    start_date = DateParameter(default=None)
    end_date = DateParameter(default=None)
    pull_as_of = DateParameter(default=None)
    reset_stage = Parameter(default='', significant=False)

    stage_reset_map = {
        "pull": [
            "{date}-earnings-ingestion-pull",
            "{date}-earnings-ingestion-qa"
        ],
        "qa": [
            "{date}-earnings-ingestion-qa"
        ]
    }
    reset_executed = False

    def __init__(self, *args, **kwargs):
        kwargs["date"] = kwargs.get("date", datetime.now())
        kwargs["pull_as_of"] = kwargs.get("pull_as_of", datetime.now())

        # Adjust for previous day until 04:00AM
        kwargs["date"] = datetime.combine(kwargs["date"].date(), time(0, 0))

        super(EarningsIngestionPipeline, self).__init__(*args, **kwargs)

    def requires(self):
        if self.reset_stage and not EarningsIngestionPipeline.reset_executed:
            subsequent_stages = self.stage_reset_map[self.reset_stage]
            engine = create_engine("postgresql+psycopg2://%s:%s@%s:%s/%s" % (
                credentials_conf["postgres"]["username"],
                credentials_conf["postgres"]["password"],
                credentials_conf["postgres"]["host"],
                credentials_conf["postgres"]["port"],
                credentials_conf["postgres"]["database"]
            ), echo=True)

            stages_to_reset = set()
            for stage in subsequent_stages:
                stages_to_reset.add(stage.format(date=self.date.strftime("%Y%m%d")))
            in_clause = "('" + "', '".join(stages_to_reset) + "')"
            engine.execute("DELETE FROM pipeline_status WHERE stage_id in " + in_clause)
            engine.dispose()
            EarningsIngestionPipeline.reset_executed = True

        yield EarningsQAStep(
            date=self.date,
            pull_as_of=self.pull_as_of,
            start_date=self.start_date,
            end_date=self.end_date
        )


if __name__ == '__main__':
    import os
    file_dir = os.path.dirname(os.path.realpath(__file__))
    logging_conf_path = os.path.join(file_dir, "..", "..", "conf", "luigi-logging.conf")

    run_luigi_pipeline([EarningsIngestionPipeline()], local_scheduler=True, logging_conf_file=logging_conf_path)
