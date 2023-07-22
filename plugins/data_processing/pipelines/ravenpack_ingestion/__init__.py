from base import S3Target, DCMTaskParams, credentials_conf, StageResultTarget
from luigi import Parameter, WrapperTask
import sqlalchemy as sa
from datetime import datetime, time, timedelta
from pipelines.ravenpack_ingestion.ravenpack_from_s3_to_redshift import RavenpackFromS3ToRedshiftTask
from pipelines.ravenpack_ingestion.ravenpack_ingestion import RavenpackIngestionTask


class RavenpackIngestionStep(RavenpackIngestionTask):
    stage_id = "ravenpack-ingestion"
    bucket = 'dcm-ravenpack-ingest'

    def output(self):
        return {
            "s3_file": S3Target(self.s3_target_name(self.date)),
        }

    @staticmethod
    def s3_target_name(task_date):
        file_date = (task_date.date() - timedelta(1)).strftime("%Y-%m-%d")
        file_name = "RPNA_{file_date}_4.0-equities.csv".format(file_date=file_date)
        return "s3://%s/%s" % (RavenpackIngestionStep.bucket, file_name)


class RavenpackFromS3ToRedshiftStep(RavenpackFromS3ToRedshiftTask):
    stage_id = "ravenpack-from-s3-to-redshift"

    def requires(self):
        return {
            "s3_file": RavenpackIngestionStep(date=self.date)
        }

    def output(self):
        return {
            "stage": super(RavenpackFromS3ToRedshiftStep, self).output()
        }


class RavenpackIngestionPipeline(DCMTaskParams, WrapperTask):
    reset_stage = Parameter(default='', significant=False)
    reset_executed = False
    stage_reset_map = {
        "merge": [
            "{date}-ravenpack-from-s3-to-redshift"
        ]
    }

    def __init__(self, *args, **kwargs):
        if 'date' not in kwargs:
            task_date = datetime.combine(datetime.now().date(), time(3))
            # task_date = datetime(2017, 6, 1, 3, 0)
            lookback = 31
            while not self._prev_day_target_exists(task_date) and lookback > 0:
                task_date = task_date - timedelta(days=1)
                lookback -= 1
            kwargs["date"] = task_date
            print("*** Running for date " + str(task_date))
        super(RavenpackIngestionPipeline, self).__init__(*args, **kwargs)

    @staticmethod
    def _prev_day_target_exists(task_date):
        check_date = task_date - timedelta(days=1)
        # return S3Target(RavenpackIngestionStep.s3_target_name(check_date))
        stage_id = "%s-%s" % (check_date.strftime("%Y%m%d"), RavenpackFromS3ToRedshiftStep.stage_id)
        return StageResultTarget(stage_id).exists()

    def requires(self):
        if self.reset_stage and not RavenpackIngestionPipeline.reset_executed:
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
                stages_to_reset.add(stage.format(date=self.date.strftime("%Y%m%d")))
            in_clause = "('" + "', '".join(stages_to_reset) + "')"

            engine.execute("DELETE FROM pipeline_status WHERE stage_id in " + in_clause)
            engine.dispose()
            RavenpackIngestionPipeline.reset_executed = True

        yield RavenpackFromS3ToRedshiftStep(self.date)
