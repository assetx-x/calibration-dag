import logging.config

from pyhocon import ConfigFactory

from commonlib.util_functions import retry
from pipelines.prices_ingestion.merge_workflow_steps import RedshiftMergeForAllEquityData
from pipelines.prices_ingestion.stage_result_transfer import AdjustedTickersPublisher, AdjustedTickersRetriever

logging_conf = ConfigFactory.parse_file("../../conf/logging.conf")
logging.config.dictConfig(logging_conf)

import os
from datetime import date, datetime, timedelta, time
from uuid import uuid4

from luigi import DateParameter, IntParameter, Parameter
from luigi import build as run_luigi_pipeline
from taskflow.patterns import linear_flow

from base import DCMTaskParams
from etl_workflow_steps import compose_main_flow_and_engine_for_task
from pipelines.common.base_stages import PipelineStageWithResults, TaskFlowPipelineStageWithTickers, \
    PipelineStageWithTickers

from pipelines.prices_ingestion import create_data_pull_stage, create_data_retrieval_stage, create_qa_stage, \
    create_adjustment_stage, create_full_tickdata_pull_stage
from pipelines.prices_ingestion.config import get_config
from pipelines.prices_ingestion.equity_data_holders import determine_if_full_adjustment_is_required

logger = logging.getLogger("price_ingestion")
pipeline_config = get_config()


# noinspection PyTypeChecker
class PriceIngestionStageParams(DCMTaskParams):
    start_date = DateParameter(default=date(2000, 1, 3), significant=False)
    end_date = DateParameter(default=None, significant=False)
    pull_as_of = DateParameter(default=None, significant=False)


class PriceIngestionStageWithTickers(PriceIngestionStageParams, TaskFlowPipelineStageWithTickers):
    pass


class PullPricesStage(PriceIngestionStageParams, PipelineStageWithResults):
    @property
    def stage_id(self):
        return self.date_ymd + '-price-ingestion-price-pull'

    def _build_stage_flow(self):
        task_params = self._get_task_params(None)
        data_pull = create_full_tickdata_pull_stage()
        full_flow = linear_flow.Flow("PullPricesFlow").add(data_pull)
        return compose_main_flow_and_engine_for_task("PullPricesProcess", task_params, full_flow)


class PullCorporateActionsStage(PriceIngestionStageWithTickers):
    partition_id = IntParameter()

    @property
    def stage_id(self):
        return self.date_ymd + '-price-ingestion-corp-action-pull'

    def requires(self):
        return PullPricesStage(run_id=self.run_id, date=self.date, start_date=self.start_date,
                               end_date=self.end_date, pull_as_of=self.pull_as_of)

    def _build_stage_flow_for_ticker(self, ticker):
        task_params = self._get_task_params(ticker)
        data_pull = create_data_pull_stage()
        full_flow = linear_flow.Flow("PullCorporateActionsFlow").add(data_pull)
        return compose_main_flow_and_engine_for_task("PullCorporateActionsProcess", task_params, full_flow)


class AdjustTickersPartitionStage(PriceIngestionStageWithTickers):
    partition_id = IntParameter()

    @property
    def stage_id(self):
        return self.date_ymd + '-price-ingestion-adjust'

    def requires(self):
        return PullCorporateActionsStage(run_id=self.run_id, date=self.date, start_date=self.start_date,
                                         end_date=self.end_date, pull_as_of=self.pull_as_of,
                                         partition_id=self.partition_id, ticker_str=self.ticker_str)

    def _process_tickers(self):
        tp = self._get_task_params(None)
        self.full_adjustment = determine_if_full_adjustment_is_required(self.tickers, tp.as_of_dt, tp.run_dt)
        self.result_publisher = AdjustedTickersPublisher(self.stage_id, self.date, self.partition_id, self.run_id)
        super(AdjustTickersPartitionStage, self)._process_tickers()
        self._publish_results()

    def _build_stage_flow_for_ticker(self, ticker):
        full_adjustment_needed = self.full_adjustment[ticker]
        if full_adjustment_needed:
            self.logger.info("Ticker %s requires FULL ADJUSTMENT", ticker)

        task_params = self._get_task_params(ticker)
        data_retrieval = create_data_retrieval_stage(full_adjustment_needed)
        data_qa = create_qa_stage(full_adjustment_needed)
        data_adjustment = create_adjustment_stage(full_adjustment_needed)

        full_flow = linear_flow.Flow("AdjustTickersPartitionFlow").add(data_retrieval, data_qa, data_adjustment)
        return compose_main_flow_and_engine_for_task("AdjustTickersPartitionProcess", task_params, full_flow)

    def on_stage_success(self, stage_key, stage_results):
        self.result_publisher.add_ticker(stage_key, stage_results)

    def _publish_results(self):
        with self.result_publisher.connect() as publisher:
            publisher.send_results()
        self.result_publisher = None


class ProgressiveMergeStage(PriceIngestionStageWithTickers):
    def __init__(self, *args, **kwargs):
        super(ProgressiveMergeStage, self).__init__(*args, **kwargs)
        if len(self.tickers):
            self.partitions = self._partition_work()

    @property
    def stage_id(self):
        return self.date_ymd + '-price-ingestion-merge'

    def requires(self):
        for i in range(len(self.partitions)):
            yield AdjustTickersPartitionStage(run_id=self.run_id, date=self.date, start_date=self.start_date,
                                              end_date=self.end_date, pull_as_of=self.pull_as_of,
                                              partition_id=i, ticker_str=','.join(self.partitions[i]))

    def _partition_work(self):
        num_partitions = pipeline_config["adjustments.partitions"]
        partition_size = ((len(self.tickers) - 1) // num_partitions) + 1
        return [self.tickers[i:i + partition_size] for i in range(0, len(self.tickers), partition_size)]

    def _process_tickers(self):
        self.start_time = datetime.utcnow()
        self.retriever = AdjustedTickersRetriever(self.stage_id, self.date)
        self.adjusted_tickers, self.adjusted_ticker_files = self._select_tickers_for_merging(self.retriever.get_results())
        flow_engine, stage_flow = self._build_stage_flow()
        step_results = self._run_stage_flow(flow_engine, stage_flow)
        self._process_stage_results('stage', step_results)

    def _select_tickers_for_merging(self, adjusted_ticker_files):
        tickers_to_merge = []
        files_to_merge = []
        for ticker, ticker_files in adjusted_ticker_files.items():
            if ticker in self.tickers:
                tickers_to_merge.append(ticker)
                files_to_merge.append(ticker_files)
        return tickers_to_merge, files_to_merge

    def _build_stage_flow(self):
        task_params = self._get_task_params(None)
        merge_step = RedshiftMergeForAllEquityData(self.adjusted_ticker_files)
        merge_flow = linear_flow.Flow("ProgressiveMergeFlow").add(merge_step)
        return compose_main_flow_and_engine_for_task("ProgressiveMergeProcess", task_params, merge_flow)

    def on_stage_success(self, stage_key, stage_results):
        end_time = datetime.utcnow()
        self._save_progress(self.start_time, end_time, self.adjusted_tickers)
        self.retriever.acknowledge_progress()
        self.logger.info("... done. Successfully merged %d tickers", len(self.adjusted_tickers))

    def on_stage_failure(self, stage_key, stage_results):
        self.logger.info("... done. Failed to merge %d tickers", len(self.adjusted_tickers))


# noinspection PyAttributeOutsideInit
class ProgressivePriceIngestionPipeline(PriceIngestionStageParams, PipelineStageWithTickers):
    run_id = Parameter(default=None, significant=False)

    def __init__(self, *args, **kwargs):
        logger.info("*************************************************************************")
        logger.info("********** Starting pipeline ProgressivePriceIngestionPipeline **********")
        logger.info("*************************************************************************")
        kwargs["run_id"] = self._get_run_id()
        kwargs["date"] = kwargs.get("date", datetime.now())
        kwargs["pull_as_of"] = kwargs.get("pull_as_of", datetime.now())

        if kwargs["date"].hour < 6:
            kwargs["date"] -= timedelta(days=2)
        else:
            kwargs["date"] -= timedelta(days=1)

        while kwargs["date"].date().weekday() >= 5:
            kwargs["date"] -= timedelta(days=1)

        kwargs["date"] = datetime.combine(kwargs["date"].date(), time(0, 0))

        super(ProgressivePriceIngestionPipeline, self).__init__(*args, **kwargs)
        self._merge_step = None

    def requires(self):
        if not self._merge_step:
            self._merge_step = ProgressiveMergeStage(run_id=self.run_id, date=self.date, start_date=self.start_date,
                                                     end_date=self.end_date, pull_as_of=self.pull_as_of,
                                                     ticker_str=self.ticker_str)
        return self._merge_step

    def complete(self):
        return self.requires().complete()

    def _get_unprocessed_tickers(self, requested_tickers):
        if requested_tickers:
            return self._cleanup_raw_tickers(requested_tickers)
        else:
            return self._fetch_universe_from_s3()

    @retry(Exception, tries=6, delay=2, backoff=2, logger=logger)
    def _fetch_universe_from_s3(self):
        s3 = boto3.client("s3")
        universe_bucket = pipeline_config["bucket"]
        universe_key = pipeline_config["prefix"] + "universe.txt"
        logger.info("Fetching ticker universe from s3://%s/%s", universe_bucket, universe_key)
        universe = s3.get_object(Bucket=universe_bucket, Key=universe_key)["Body"].read()
        universe = self._cleanup_raw_tickers(universe.decode().split("\n"))
        logger.info("Fetched %d tickers from the universe file", len(universe))
        return universe

    @staticmethod
    def _cleanup_raw_tickers(raw_tickers):
        # cleanup accidental spaces and remove empty strings
        return list(filter(bool, list(map(lambda ticker: ticker.strip(), raw_tickers))))

    @staticmethod
    def _get_run_id():
        try:
            mesos_task_part = os.environ["MESOS_TASK_ID"].rsplit('.', 1)[1].split('-', 1)[0]
            timestamp_part = datetime.now().strftime("%Y%m%d-%H:%M:%S")
            return "%s-%s" % (mesos_task_part, timestamp_part)
        except:
            return str(uuid4())


if __name__ == '__main__':
    pipeline_config["adjustments"]["partitions"] = 1
    #ticker_str = ','.join(['AAPL', 'AMZN', 'NFLX', 'TSLA'])
    ticker_str = ','.join(['AAME'])
    run_luigi_pipeline([ProgressivePriceIngestionPipeline(ticker_str=ticker_str)], local_scheduler=True,
                       logging_conf_file="../../conf/luigi-logging.conf")
