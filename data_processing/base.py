import os
import sys
import pickle
import shutil
import logging
import tempfile
import pandas as pd
import sqlalchemy as sa

from pprint import pformat
from zipfile import ZipFile
from pyhocon import ConfigFactory
from contextlib import contextmanager
from datetime import date, timedelta, datetime
from etl_workflow_steps import EquitiesETLTaskParameters, compose_main_flow_and_engine_for_task
from google.cloud import storage
from luigi import Config, Parameter, DateParameter, BoolParameter, Task, ExternalTask, LocalTarget, \
    DateMinuteParameter, IntParameter
from luigi.contrib.redshift import RedshiftTarget as LuigiRedshiftTarget
from luigi.contrib.postgres import PostgresTarget as LuigiPostgresTarget
from luigi.contrib.gcs import GCSFlagTarget as LuigiS3FlagTarget
from luigi.contrib.gcs import GCSTarget as LuigiS3Target

# from pyspark import SparkContext, SparkConf, HiveContext
# from pyspark.serializers import AutoSerializer


try:
    credentials_conf = ConfigFactory.parse_file(os.path.join(os.path.dirname(__file__), "conf", "credentials.conf"))
except Exception:
    credentials_conf = None


class LuigiTaskParameter(Parameter):
    def parse(self, luigi_task):
        if not isinstance(luigi_task, Task):
            raise ValueError("External dependency " + type(luigi_task).__name__ + " must be Luigi task")
        return luigi_task

    def serialize(self, luigi_task):
        return luigi_task.task_id


class RedshiftTarget(LuigiRedshiftTarget):
    # !!! see below in 'exists()' function an explanation on how to reset this target

    def __init__(self, table_name, force_exists=None):
        self.force_exists = force_exists
        super(RedshiftTarget, self).__init__(
            credentials_conf["redshift"]["host"],
            credentials_conf["redshift"]["database"],
            credentials_conf["redshift"]["username"],
            credentials_conf["redshift"]["password"],
            table_name,
            table_name,
            port=credentials_conf["redshift"]["port"]
        )

    def uri(self, prefix=None):
        return "redshift{}://{}:{}@{}:{}/{}".format(
            ("+" + prefix) if prefix else "",
            self.user,
            self.password,
            self.host,
            self.port,
            self.database
        )

    @contextmanager
    def engine(self):
        engine = sa.create_engine(self.uri("psycopg2"), echo=True)
        yield engine
        engine.dispose()

    def to_spark_dataframe(self, sql_context, query=None):
        uri = "redshift://%s:%s/%s?user=%s&password=%s" % (
            self.host,
            self.port,
            self.database,
            self.user,
            self.password
        )

        reader = sql_context.read \
            .format("com.databricks.spark.redshift") \
            .option("url", "jdbc:" + uri) \
            .option("tempdir", "s3a://" + credentials_conf["redshift"]["s3_temp_dir"])

        if query:
            return reader.option("query", query.format(table=self.table)).load()
        else:
            return reader.option("dbtable", self.table).load()

    def from_spark_dataframe(self, df):
        uri = "redshift://%s:%s/%s?user=%s&password=%s" % (
            self.host,
            self.port,
            self.database,
            self.user,
            self.password
        )

        df \
            .write.format('com.databricks.spark.redshift') \
            .option("url", "jdbc:" + uri) \
            .option("dbtable", self.table) \
            .option("tempdir", "s3a://" + credentials_conf["redshift"]["s3_temp_dir"]) \
            .mode("overwrite") \
            .save()
        self.touch()

    def exists(self, connection=None):
        if self.force_exists is not None:
            return self.force_exists
        else:
            # To reset this target and make Luigi task run again:
            # !!! => delete a record, corresponding to this table, from 'table_updates' table
            # e.g. "delete from table_updates where target_table = '<your table name>'"
            # dropping the target table won't help, Luigi is not checking its existence
            return super(RedshiftTarget, self).exists(connection=connection)


class S3Target(LuigiS3Target):
    def __init__(self, path, force_exists=None):
        self.force_exists = force_exists
        super(S3Target, self).__init__(path, client=storage.Client())

    def to_spark_dataframe(self, sql_context):
        return sql_context.read \
            .load(
                self.path.replace("s3://", "s3a://"),
                format="com.databricks.spark.csv",
                header="true",
                inferSchema="true"
            )

    def from_spark_dataframe(self, df):
        df.write.format("com.databricks.spark.csv").save(self.path.replace("s3://", "s3a://"))

    def parquet_from_spark_dataframe(self, df, **kwargs):
        return df.write.parquet(self.path.replace("s3://", "s3a://"), **kwargs)

    def exists(self):
        if self.force_exists is not None:
            return self.force_exists
        else:
            return super(S3Target, self).exists()


class S3FlagTarget(LuigiS3FlagTarget):
    def __init__(self, path):
        super(S3FlagTarget, self).__init__(path, client=storage.Client())

    def to_spark_dataframes(self, sql_context):
        dfs = []
        for file in self.fs.listdir(self.path):
            if file.split("/")[-1].startswith("_"):
                continue
            try:
                dfs.append(
                    sql_context.read.load(
                        file.replace("s3://", "s3a://"),
                        format="com.databricks.spark.csv",
                        header="true",
                        inferSchema="true"
                    )
                )
            except Exception:
                pass

        return dfs


class EfsTarget(LocalTarget):
    def __init__(self, path):
        super(EfsTarget, self).__init__("%s/%s" % (credentials_conf["efs"]["mount_point"], path))


# noinspection PyUnresolvedReferences
class HdfStorageMixin(object):
    # requires self.logger and self.df_data_dir
    def _load_df(self, df_name):
        df_path = self.full_path(df_name)
        if os.path.exists(df_path):
            self.logger.info("Loading '%s' from %s" % (df_name, df_path))
            df = pd.read_hdf(df_path, key=df_name)
            self.logger.info("Done loading '%s'" % df_name)
            return df
        return None

    def _store_df(self, df, df_name):
        df_path = self.full_path(df_name)
        self.logger.info("Saving '%s' to %s" % (df_name, df_path))
        df.to_hdf(df_path, key=df_name, complevel=9, complib="blosc")
        self.logger.info("Done saving '%s'" % df_name)

    def full_path(self, df_name):
        return os.path.join(self.df_data_dir, df_name + '.h5')


class DCMTaskParams(Config):
    date = DateMinuteParameter(default=None, description="Date to base task on")

    logger = logging.getLogger('luigi-interface')

    def __init__(self, *args, **kwargs):
        super(DCMTaskParams, self).__init__(*args, **kwargs)
        self.date_ymd = self.date.strftime("%Y%m%d") if self.date else "NO_DATE"
        self._override_log_formatter()

    def date_only(self, days_offset=None):
        if days_offset is None:
            return self.date.date()
        else:
            return self.date.date() + timedelta(days_offset)

    def log(self, text):
        self.logger.info(text)

    def _override_log_formatter(self):
        # noinspection PyProtectedMember
        if len(self.logger.handlers) == 1 and "msecs" not in self.logger.handlers[0].formatter._fmt:
            self.logger.handlers[0].formatter = logging.Formatter(
                fmt="%(asctime)s.%(msecs)03d %(levelname)s %(filename)s:%(lineno)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S")


class ExternalServicesMixin(object):
    def __init__(self, *args, **kwargs):
        super(ExternalServicesMixin, self).__init__()
        self.pg_engine = sa.create_engine("postgresql+psycopg2://%s:%s@%s:%s/%s" % (
            credentials_conf["postgres"]["username"],
            credentials_conf["postgres"]["password"],
            credentials_conf["postgres"]["host"],
            credentials_conf["postgres"]["port"],
            credentials_conf["postgres"]["database"]
        ), echo=True)
        self.redshift_engine = sa.create_engine("redshift+psycopg2://%s:%s@%s:%s/%s" % (
            credentials_conf["redshift"]["username"],
            credentials_conf["redshift"]["password"],
            credentials_conf["redshift"]["host"],
            credentials_conf["redshift"]["port"],
            credentials_conf["redshift"]["database"]
        ), echo=True)
        self.s3 = storage.Client()


class ExternalLuigiTask(ExternalTask):
    external_task = LuigiTaskParameter()

    def complete(self):
        return self.external_task.complete()


class PySparkTask(Task):
    executor_memory = "2G"
    driver_memory = "2G"
    executor_cpu_cores = 0

    jars = [
        "jars/RedshiftJDBC41-1.1.17.1017.jar"
    ]

    packages = [
        "com.databricks:spark-csv_2.11:1.4.0",
        "org.apache.hadoop:hadoop-aws:2.7.1",
        "com.databricks:spark-redshift_2.10:0.6.0"
    ]

    @property
    def conf(self):
        env = {
            "spark.executorEnv.%s" % key: value
            for key, value in os.environ.iteritems()
            if key in ["GIT_USER", "GIT_PASSWORD", "REDSHIFT_USER", "REDSHIFT_PWD", "S3_ACCESS_KEY", "S3_SECRET_KEY"]
        }
        conf = {
            "fs.s3n.awsAccessKeyId": credentials_conf["s3"]["access_key"],
            "fs.s3n.awsSecretAccessKey": credentials_conf["s3"]["secret_key"],
            "spark.hadoop.fs.s3a.access.key": credentials_conf["s3"]["access_key"],
            "spark.hadoop.fs.s3a.secret.key": credentials_conf["s3"]["secret_key"],
            "spark.executor.memory": self.executor_memory,
            "spark.mesos.executor.docker.image": "registry.marathon.mesos:5000/dcm-luigi-worker:latest",
            "spark.mesos.coarse": "true",
            #"spark.mesos.uris": "http://dcm-data-test.s3.amazonaws.com/fluder/task-stuff.zip"
            #"spark.mesos.constraints": "efs:true",
            "spark.mesos.executor.docker.volumes": "/mnt/backtest:/mnt/efs/backtest_data:rw",
            "spark.driver.memory": self.driver_memory
        }

        if self.executor_cpu_cores > 0:
            conf['spark.executor.cores'] = str(self.executor_cpu_cores)

        conf.update(env)

        return conf

    def _prepare_spark_context(self):
        if hasattr(PySparkTask, "spark_context"):
            PySparkTask.spark_context.stop()

        zip_path = os.path.join(self.run_path, "pyfiles.zip")
        zip_file = ZipFile(zip_path, "w")
        for root, dirs, files in os.walk(os.path.dirname(__file__)):
            self.logger.info("Adding directory to spark context:" + root)
            for file in [f for f in files if not f.endswith('.pyc')]:
                arcname = os.path.join(root, file)[len(os.path.dirname(__file__)) + 1:]
                self.logger.info("    adding file to spark context: " + arcname)
                zip_file.write(os.path.join(root, file), arcname)
        zip_file.close()

        packages_args = "--packages %s --driver-memory %s" % (",".join(self.packages), self.driver_memory)
        jars_args = "--jars %s" % ",".join(self.jars)
        os.environ["PYSPARK_SUBMIT_ARGS"] = "%s %s pyspark-shell" % (packages_args, jars_args)

        spark_conf = SparkConf()
        spark_conf.setAll(self.conf.items())

        PySparkTask.spark_context = SparkContext(
            master=credentials_conf["spark"]["master"],
            pyFiles=[zip_path],
            #serializer=AutoSerializer(),
            conf=spark_conf,
        )
        PySparkTask.sql_context = HiveContext(self.spark_context)

    def run(self):
        self.run_path = tempfile.mkdtemp()
        self._prepare_spark_context()
        try:
            self.run_spark()
        finally:
            shutil.rmtree(self.run_path)

    def run_spark(self):
        raise NotImplementedError()


class StageResultTarget(LuigiPostgresTarget):
    def __init__(self, stage_id, treat_failed_as_completed=True):
        super(StageResultTarget, self).__init__(
            credentials_conf["postgres"]["host"],
            credentials_conf["postgres"]["database"],
            credentials_conf["postgres"]["username"],
            credentials_conf["postgres"]["password"],
            "pipeline_status",
            "pipeline_status",
            port=credentials_conf["postgres"]["port"]
        )
        self.stage_id = stage_id
        self.treat_failed_as_completed = treat_failed_as_completed

    def exists(self, connection=None):
        cursor = self.connect().cursor()
        cursor.execute("SELECT stage_id, status_type FROM pipeline_status WHERE stage_id = %s", (self.stage_id,))
        rows = [r for r in cursor.fetchall()]
        if self.treat_failed_as_completed:
            target_exists = len(rows) > 0
        else:
            target_exists = len(rows) > 0 and len([row for row in rows if row[1] != 'Success']) == 0
        return target_exists

    def get_statuses(self):
        connection = self.connect()
        cursor = connection.cursor()
        cursor.execute("SELECT serialized_data FROM pipeline_status WHERE stage_id = %s ORDER BY task_start", (self.stage_id,))
        return [pickle.loads(row[0]) for row in cursor.fetchall()]

    def commit_statuses(self, statuses):
        connection = self.connect()
        connection.autocommit = False
        cursor = connection.cursor()
        cursor.execute("DELETE FROM pipeline_status WHERE stage_id = %s", (self.stage_id,))

        for status in [s for s in statuses if self.treat_failed_as_completed or s["status_type"] == "Success"]:
            cursor.execute(
                "INSERT INTO pipeline_status (stage_id, substage_id, step_name, status_type, task_start, task_end, run_dt, serialized_data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (
                    self.stage_id,
                    status.get("Sub-Stage", "NA"),
                    status["step_name"],
                    status["status_type"],
                    status["task_start"],
                    status["task_end"],
                    str(status["run_dt"].to_pydatetime().date()),
                    pickle.dumps(status, protocol=0).decode("utf8", errors="replace")
                )
            )
        connection.commit()

# noinspection PyTypeChecker
class StagedTask(DCMTaskParams, Task):
    create_stage_func = None
    stage_id = None

    start_date = DateParameter(default=date(2000, 1, 3), significant=False)
    end_date = DateParameter(default=None, significant=False)
    pull_as_of = DateParameter(default=datetime.now(), significant=False)
    treat_failed_as_completed = BoolParameter(default=True, significant=False)

    def get_task_params(self):
        return EquitiesETLTaskParameters(
            ticker=getattr(self, "ticker", None),
            as_of_dt=pd.Timestamp(self.pull_as_of + timedelta(days=1)),
            run_dt=pd.Timestamp(self.date),
            start_dt=pd.Timestamp(self.start_date),
            end_dt=pd.Timestamp(self.end_date or self.date)
        )

    def run(self):
        task_params = self.get_task_params()
        stages = self.create_stage_func()
        if not isinstance(stages, list):
            stages = [stages]
        engine, main_flow = compose_main_flow_and_engine_for_task(self.stage_id, task_params, *stages)
        engine.run(main_flow)
        results = engine.storage.fetch_all()
        self.process_results(results)

        step_results = list(filter(None, results["step_results"]))
        if isinstance(self.output(), dict):
            self.output()["stage"].commit_statuses(step_results)
        else:
            self.output().commit_statuses(step_results)
        for status in step_results:
            if status["status_type"] == "Fail":
                try:
                    traceback_str, exception_str = status["error"]["traceback_str"], status["error"]["exception_str"]
                except Exception:
                    self.logger.error("StagedTask %s, step %s failed" % (self.stage_id, status["step_name"]))
                else:
                    self.logger.error("StagedTask %s, step %s failed with exception: %s" %
                                      (self.stage_id, status["step_name"], exception_str))

    def process_results(self, results):
        pass

    def output(self):
        return StageResultTarget(self.stage_id, treat_failed_as_completed=self.treat_failed_as_completed)


# noinspection PyTypeChecker
class TaskWithStatus(DCMTaskParams, Task):
    stage_id = None
    treat_failed_as_completed = BoolParameter(default=True, significant=False)

    def collect_keys(self):
        raise NotImplementedError()

    def on_finally(self):
        pass

    def run(self):
        task_start = datetime.now()
        try:
            extra_keys = self.collect_keys() or {}
            task_end = pd.Timestamp(datetime.now())
            status = {
                "step_name": self.output()["stage"].stage_id,
                "task_start": task_start,
                "task_end": task_end,
                "status_type": "Success",
                "run_dt": pd.Timestamp(self.date)
            }
            status.update(extra_keys)
            self.output()["stage"].commit_statuses([status])
        except (Exception, AssertionError):
            self.logger.error("TaskWithStatus: stage %s, step %s failed" %
                              (self.stage_id, self.output()["stage"].stage_id))
            import traceback
            self.logger.error("*****  " + traceback.format_exc())
            task_end = pd.Timestamp(datetime.now())
            self.output()["stage"].commit_statuses([{
                "step_name": self.output()["stage"].stage_id,
                "task_start": task_start,
                "task_end": task_end,
                "status_type": "Fail",
                "run_dt": pd.Timestamp(self.date)
            }])
        finally:
            try:
                self.on_finally()
            except Exception:
                pass

    def output(self):
        # noinspection PyUnresolvedReferences
        return StageResultTarget("%s-%s" % (self.date.strftime("%Y%m%d"), self.stage_id),
                                 treat_failed_as_completed=self.treat_failed_as_completed)
