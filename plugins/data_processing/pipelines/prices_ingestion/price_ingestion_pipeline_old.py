import pickle
import boto3

from datetime import datetime, date, timedelta, time
from pandas import Timestamp, read_csv
from luigi import Parameter, WrapperTask, DateParameter, ListParameter
from sqlalchemy import create_engine
from taskflow.patterns import linear_flow

from base import StagedTask, DCMTaskParams, RedshiftTarget, S3Target, credentials_conf
from pipelines.prices_ingestion.adjustment_workflow_steps import create_adjustment_stage
from pipelines.prices_ingestion.collect_manifests import CollectManifestsTask
from pipelines.prices_ingestion.config import get_config
from pipelines.prices_ingestion.data_pull_workflow_steps import create_data_pull_stage, \
    create_full_tickdata_pull_stage
from pipelines.prices_ingestion.data_retrieval_workflow_steps import create_data_retrieval_stage
from pipelines.prices_ingestion.etl_workflow_aux_functions import build_s3_url
from pipelines.prices_ingestion.merge_data import MergeDataTask
from pipelines.prices_ingestion.qa_workflow_steps import create_qa_stage

pipeline_bucket = get_config()["adjustments"]["data_bucket"]
pipeline_temp_location = get_config()["adjustments"]["pickle_location"]


class TickDataPullStep(StagedTask):
    treat_failed_as_completed = False

    @property
    def stage_id(self):
        return "%s-price-ingestion-tickdata" % self.date.strftime("%Y%m%d")

    @staticmethod
    def create_stage_func():
        return create_full_tickdata_pull_stage()

    def process_results(self, results):
        with self.output()["last_adjustment_dates"].open("w") as output_fd:
            pickle.dump(results["RedshiftLastAdjustmentDates"], output_fd)

    def output(self):
        pickle_file_path = "%s/asof_%s_last_adjustment_dates.pickle" % (pipeline_temp_location, self.date_ymd)
        return {
            "last_adjustment_dates": S3Target(build_s3_url(pipeline_bucket, pickle_file_path), force_exists=True),
            "stage": super(TickDataPullStep, self).output()
        }


class TickDataPullMissingStep(DCMTaskParams, WrapperTask):
    def requires(self):
        requirements = []
        max_raw_cob = date(2016, 9, 30)

        while max_raw_cob < (self.date or datetime.now()).date():
            max_raw_cob += timedelta(days=1)

            if max_raw_cob.weekday() < 5:
                pull_step = TickDataPullStep(
                    date=datetime.combine(max_raw_cob, time(0, 0)),
                    pull_as_of=date.today(),
                    start_date=date(2000, 1, 3),
                    end_date=max_raw_cob
                )
                requirements.append(pull_step)

        return requirements


class YahooPullTickerStep(StagedTask):
    ticker = Parameter(description="Ticker")

    @property
    def stage_id(self):
        return "%s-price-ingestion-yahoo-%s" % (self.date.strftime("%Y%m%d"), str(self.ticker))

    @staticmethod
    def create_stage_func():
        return create_data_pull_stage()

    def process_results(self, results):
        with self.output()["last_ca_dates"].open("w") as output_fd:
            pickle.dump(results["TickDataLastCADates"], output_fd)

    def output(self):
        pickle_file_path = "%s/asof_%s_last_ca_dates_%s.pickle" % (pipeline_temp_location, self.date_ymd, self.ticker)
        return {
            "last_ca_dates": S3Target(build_s3_url(pipeline_bucket, pickle_file_path), force_exists=True),
            "stage": super(YahooPullTickerStep, self).output()
        }


class AdjustTickerStep(StagedTask):
    ticker = Parameter(description="Ticker")

    def requires(self):
        return {
            "yahoo": YahooPullTickerStep(
                date=self.date,
                pull_as_of=self.pull_as_of,
                start_date=self.start_date,
                end_date=self.end_date,
                ticker=self.ticker
            ),
            "tickdata": TickDataPullStep(
                date=self.date,
                pull_as_of=self.pull_as_of,
                start_date=self.start_date,
                end_date=self.end_date
            )
        }

    @property
    def stage_id(self):
        return "%s-price-ingestion-adjust-%s" % (self.date.strftime("%Y%m%d"), str(self.ticker))

    def create_stage_func(self):
        with self.input()["tickdata"]["last_adjustment_dates"].open() as input_fd:
            last_adjustment_dates = pickle.loads(input_fd.read())

        with self.input()["yahoo"]["last_ca_dates"].open() as input_fd:
            last_ca_dates = pickle.loads(input_fd.read())

        try:
            last_ca = Timestamp(last_ca_dates[str(self.ticker)]).tz_localize(None).normalize()
            last_adj = Timestamp(last_adjustment_dates[str(self.ticker)]).tz_localize(None).normalize()
            full_adjustment_needed = str(self.ticker) not in last_adjustment_dates or (last_ca > last_adj)
        except Exception:
            full_adjustment_needed = False

        # TODO: remove once process is stable and find a more elegant solution to be able to force full adjustment
        try:
            require_full_adj = list(read_csv("http://dcm-data-test.s3.amazonaws.com/jack/market-views-v2/full_adjustment_required.csv")["Ticker"])
        except:
            require_full_adj = []
        if str(self.ticker) in require_full_adj:
            full_adjustment_needed = True

        if full_adjustment_needed:
            self.log("FULL ADJUSTMENT")

        data_retrieval = create_data_retrieval_stage(full_adjustment_needed)
        data_qa = create_qa_stage(full_adjustment_needed)
        data_adjustment = create_adjustment_stage(full_adjustment_needed)

        return linear_flow.Flow("FullDataProcess").add(data_retrieval, data_qa, data_adjustment)

    def run(self):
        super(AdjustTickerStep, self).run()
        engine = create_engine("postgresql+psycopg2://%s:%s@%s:%s/%s" % (
            credentials_conf["postgres"]["username"],
            credentials_conf["postgres"]["password"],
            credentials_conf["postgres"]["host"],
            credentials_conf["postgres"]["port"],
            credentials_conf["postgres"]["database"]
        ), echo=True)

        with engine.begin() as connection:
            connection.execute("DELETE FROM adjustment_merge_queue "
                               "WHERE run_dt = %s AND ticker = %s", (self.date.date(), self.ticker))
            connection.execute("INSERT INTO adjustment_merge_queue "
                               "(run_dt, ticker) VALUES (%s, %s)", (self.date.date(), self.ticker))


class MergeDataStep(MergeDataTask):
    start_date = DateParameter(default=date(2000, 1, 3))
    end_date = DateParameter(default=None, significant=False)
    pull_as_of = DateParameter(default=None, significant=False)
    tickers = ListParameter(default=[], significant=False)
    reset_stage = Parameter(default=None, significant=False)
    stage_id = "price-ingestion-merge"

    def __init__(self, *args, **kwargs):
        super(MergeDataStep, self).__init__(*args, **kwargs)
        self.tickers = kwargs.get("tickers")

    def requires(self):
        return {
            ticker: AdjustTickerStep(
                date=self.date,
                pull_as_of=self.pull_as_of,
                start_date=self.start_date,
                end_date=self.end_date,
                ticker=ticker
            )
            for ticker in self.tickers
        }

    def output(self):
        return {
            "prices": RedshiftTarget(get_config()["redshift"]["equity_price_table"], force_exists=True),
            "whitelist": RedshiftTarget(get_config()["redshift"]["whitelist_table"], force_exists=True),
            "daily_prices": RedshiftTarget(get_config()["redshift"]["daily_price_table"], force_exists=True),
            "adjustment_factors": RedshiftTarget(get_config()["redshift"]["adjustment_factors_table"], force_exists=True),
            "stage": super(MergeDataStep, self).output()
        }


class PricesIngestionPipeline(DCMTaskParams, WrapperTask):
    start_date = DateParameter(default=date(2000, 1, 3))
    end_date = DateParameter(default=None)
    pull_as_of = DateParameter(default=None)
    tickers = ListParameter(default=[])
    reset_stage = Parameter(default='', significant=False)

    reset_executed = False

    stage_reset_map = {  # TODO: this should be generated automatically, using fast and naive way right now
        "yahoo": [
            "{date}-price-ingestion-yahoo-{ticker}",
            "{date}-price-ingestion-adjust-{ticker}",
            "{date}-price-ingestion-merge"
        ],
        "tickdata": [
            "{date}-price-ingestion-tickdata",
            "{date}-price-ingestion-adjust-{ticker}",
            "{date}-price-ingestion-merge"
        ],
        "adjust": [
            "{date}-price-ingestion-adjust-{ticker}",
            "{date}-price-ingestion-merge"
        ],
        "merge": [
            "{date}-price-ingestion-merge"
        ]
    }

    def __init__(self, *args, **kwargs):
        kwargs["date"] = kwargs.get("date", datetime.now())
        kwargs["pull_as_of"] = kwargs.get("pull_as_of", datetime.now())

        # Adjust for previous day until 04:00AM
        if kwargs["date"].hour < 4:
            kwargs["date"] -= timedelta(days=2)
        else:
            kwargs["date"] -= timedelta(days=1)

        while kwargs["date"].date().weekday() >= 5:
            kwargs["date"] -= timedelta(days=1)

        kwargs["date"] = datetime.combine(kwargs["date"].date(), time(0, 0))

        super(PricesIngestionPipeline, self).__init__(*args, **kwargs)

    def requires(self):
        universe_bucket = get_config()["bucket"]
        universe_key = get_config()["prefix"] + "universe.txt"

        s3 = boto3.client("s3")
        tickers = [
            line.strip() for line in s3.get_object(
                Bucket=universe_bucket,
                Key=universe_key
            )["Body"].read().split("\n")
            if line.strip()
        ]

        if self.tickers:
            tickers = filter(lambda _ticker: _ticker in self.tickers, tickers)

        if self.reset_stage and not PricesIngestionPipeline.reset_executed:
            subsequent_stages = self.stage_reset_map[str(self.reset_stage)]
            engine = create_engine("postgresql+psycopg2://%s:%s@%s:%s/%s" % (
                credentials_conf["postgres"]["username"],
                credentials_conf["postgres"]["password"],
                credentials_conf["postgres"]["host"],
                credentials_conf["postgres"]["port"],
                credentials_conf["postgres"]["database"]
            ), echo=True)

            stages_to_reset = set()

            for ticker in tickers:
                for stage in subsequent_stages:
                    stages_to_reset.add(stage.format(ticker=ticker, date=self.date.strftime("%Y%m%d")))

            in_clause = "('" + "', '".join(stages_to_reset) + "')"
            engine.execute("DELETE FROM pipeline_status WHERE stage_id in " + in_clause)
            engine.dispose()
            PricesIngestionPipeline.reset_executed = True

        yield MergeDataStep(
                date=self.date,
                pull_as_of=self.pull_as_of,
                start_date=self.start_date,
                end_date=self.end_date,
                tickers=tickers,
                reset_stage=self.reset_stage
            )
