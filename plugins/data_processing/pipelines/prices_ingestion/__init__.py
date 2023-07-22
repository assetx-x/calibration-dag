import pytz
import numpy as np

from uuid import uuid4
from base import DCMTaskParams
from init_logging import logging
from datetime import date, datetime
from google.cloud import storage
from taskflow.patterns import linear_flow
from luigi import build as run_luigi_pipeline
from luigi import DateParameter, IntParameter, Parameter
from pipelines.prices_ingestion.config import get_config
from pipelines.common.stage_store import PipelineStageStore
from etl_workflow_steps import compose_main_flow_and_engine_for_task
from pipelines.common.pipeline_util import get_prev_bus_day, TimePart
from pipelines.prices_ingestion.qa_workflow_steps import create_qa_stage
from pipelines.prices_ingestion.adjustment_workflow_steps import create_adjustment_stage
from pipelines.prices_ingestion.merge_workflow_steps import RedshiftMergeForAllEquityData
from pipelines.prices_ingestion.data_retrieval_workflow_steps import create_data_retrieval_stage
from pipelines.prices_ingestion.equity_data_holders import determine_if_full_adjustment_is_required
from pipelines.prices_ingestion.stage_result_transfer import AdjustedTickersPublisher, AdjustedTickersRetriever
from pipelines.prices_ingestion.data_pull_workflow_steps import create_full_tickdata_pull_stage, create_data_pull_stage
from pipelines.common.base_stages import PipelineStageWithResults, TaskFlowPipelineStageWithTickers, \
    PipelineStageWithTickers


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

    def complete(self):
        return self._check_stage_status()

    def _build_stage_flow(self):
        self.logger.info("Creating task flow sequence for the stage")
        task_params = self._get_task_params(None)
        data_pull = create_full_tickdata_pull_stage()
        full_flow = linear_flow.Flow("PullPricesFlow").add(data_pull)
        return compose_main_flow_and_engine_for_task("PullPricesProcess", task_params, full_flow)

    def _mark_stage_as_complete(self, start_time, completed_successfully):
        if completed_successfully:
            super(PullPricesStage, self)._mark_stage_as_complete(start_time, completed_successfully)


# noinspection PyTypeChecker
class PullCorporateActionsStage(PriceIngestionStageWithTickers):
    partition_id = IntParameter()

    # noinspection PyStringFormat
    def __init__(self, *args, **kwargs):
        super(PullCorporateActionsStage, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger("%s[%d]" % (self.__class__.__name__, self.partition_id))
        self.logger.info("Created new stage partition")

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


# noinspection PyTypeChecker
class AdjustTickersPartitionStage(PriceIngestionStageWithTickers):
    partition_id = IntParameter()

    # noinspection PyStringFormat
    def __init__(self, *args, **kwargs):
        super(AdjustTickersPartitionStage, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger("%s[%d]" % (self.__class__.__name__, self.partition_id))
        self.logger.info("Created new stage partition")

    @property
    def stage_id(self):
        return self.date_ymd + '-price-ingestion-adjust'

    def requires(self):
        return PullCorporateActionsStage(run_id=self.run_id, date=self.date, start_date=self.start_date,
                                         end_date=self.end_date, pull_as_of=self.pull_as_of,
                                         partition_id=self.partition_id, ticker_str=','.join(self.tickers))

    def _process_tickers(self):
        tp = self._get_task_params(None)
        self.full_adjustment = determine_if_full_adjustment_is_required(self.tickers, tp.as_of_dt, tp.run_dt)
        self.result_publisher = AdjustedTickersPublisher(self.stage_id, self.date, self.partition_id, self.run_id)
        super(AdjustTickersPartitionStage, self)._process_tickers()
        self._publish_results()

    def _build_stage_flow_for_ticker(self, ticker):
        require_full_adjustment = PricePipelineHelper.load_tickers("tickers_require_full_adjustment.txt")
        full_adjustment_needed = self.full_adjustment[ticker] or ticker in require_full_adjustment
        if full_adjustment_needed:
            self.logger.info("Ticker %s requires FULL ADJUSTMENT", ticker)

        task_params = self._get_task_params(ticker)
        data_retrieval = create_data_retrieval_stage(full_adjustment_needed)
        data_qa = create_qa_stage(full_adjustment_needed)
        data_adjustment = create_adjustment_stage(full_adjustment_needed)

        full_flow = linear_flow.Flow("AdjustTickersPartitionFlow").add(data_retrieval, data_qa, data_adjustment)
        return compose_main_flow_and_engine_for_task("AdjustTickersPartitionProcess", task_params, full_flow)

    def on_stage_success(self, stage_key, stage_results):
        self.logger.info("Inside overridden 'on_stage_success()'. As expected")
        self.result_publisher.add_ticker(stage_key, stage_results)

    def _publish_results(self):
        with self.result_publisher.connect() as publisher:
            publisher.send_results()
        self.result_publisher = None


class ProgressiveMergeStage(PriceIngestionStageWithTickers):
    def __init__(self, *args, **kwargs):
        super(ProgressiveMergeStage, self).__init__(*args, **kwargs)
        if len(self.tickers):
            self.partitions = PricePipelineHelper.partition_tickers(self.tickers)

    @property
    def stage_id(self):
        return self.date_ymd + '-price-ingestion-merge'

    def _process_tickers(self):
        self.start_time = datetime.utcnow()
        self.retriever = AdjustedTickersRetriever(self.stage_id, self.date)
        #empty_records_only = True
        #while True:
        adjusted_ticker_files = self.retriever.get_results()
        self._select_tickers_for_merging(adjusted_ticker_files)
        if self.files_to_merge:
            #empty_records_only = False
            flow_engine, stage_flow = self._build_stage_flow()
            step_results = self._run_stage_flow(flow_engine, stage_flow)
            self._process_stage_results('stage', step_results)
        else:
            logger.info("All tickers have been merged already for this run")
            #if empty_records_only:
            self.retriever.acknowledge_progress()
            #break

    def _select_tickers_for_merging(self, adjusted_ticker_files):
        self.tickers_to_merge = []
        self.files_to_merge = []
        for ticker, ticker_files in adjusted_ticker_files.items():
            if ticker in self.tickers:
                self.tickers_to_merge.append(ticker)
                self.files_to_merge.append(ticker_files)

    def _build_stage_flow(self):
        logger.info("About to merge %d tickers", len(self.files_to_merge))
        task_params = self._get_task_params(None)
        merge_step = RedshiftMergeForAllEquityData(self.files_to_merge)
        merge_flow = linear_flow.Flow("ProgressiveMergeFlow").add(merge_step)
        return compose_main_flow_and_engine_for_task("ProgressiveMergeProcess", task_params, merge_flow)

    def on_stage_success(self, stage_key, stage_results):
        end_time = datetime.utcnow()
        self._save_progress(self.start_time, end_time, self.tickers_to_merge)
        self.retriever.acknowledge_progress()
        self.logger.info("... done. Successfully merged %d tickers", len(self.tickers_to_merge))

    def on_stage_failure(self, stage_key, stage_results):
        self.logger.info("... done. Failed to merge %d tickers", len(self.tickers_to_merge))


class PricePipelineHelper(object):
    @staticmethod
    def get_proper_run_date(run_date, start_time=None):
        return get_prev_bus_day(run_date, start_time, time_part=TimePart.Normalize)

    @staticmethod
    def get_run_id():
        try:
            mesos_task_part = os.environ["MESOS_TASK_ID"].rsplit('.', 1)[1].split('-', 1)[0]
            timestamp_part = datetime.now().strftime("%Y%m%d-%H:%M:%S")
            return "%s-%s" % (mesos_task_part, timestamp_part)
        except:
            return str(uuid4())

    @staticmethod
    def get_unprocessed_tickers(requested_tickers):
        if requested_tickers:
            return PricePipelineHelper._cleanup_raw_tickers(requested_tickers)
        else:
            return PricePipelineHelper.load_tickers()

    @staticmethod
    def partition_tickers(all_tickers):
        num_partitions = pipeline_config["adjustments.partitions"]
        partition_size = ((len(all_tickers) - 1) // int(num_partitions)) + 1
        return [all_tickers[i:i + partition_size] for i in range(0, len(all_tickers), partition_size)]

    @staticmethod
    def _cleanup_raw_tickers(raw_tickers):
        # cleanup accidental spaces and remove empty strings
        return list(filter(bool, list(map(lambda ticker: ticker.strip(), raw_tickers))))

    @staticmethod
    def load_tickers(filename="universe.txt"):
        universe_bucket = pipeline_config["bucket"]
        universe_key = pipeline_config["prefix"] + filename
        logger.info("Fetching ticker universe from gs://%s/%s", universe_bucket, universe_key)
        storage_client = storage.Client()
        universe = storage_client.bucket(universe_bucket).blob(universe_key).download_as_string().decode("utf-8")
        universe = [k for k in PricePipelineHelper._cleanup_raw_tickers(universe.split("\n"))]
        logger.info("Fetched %d tickers from the universe file", len(universe))
        return universe


# noinspection PyAttributeOutsideInit
class PartitionedPriceAdjustmentPipeline(PriceIngestionStageParams, PipelineStageWithTickers):
    run_id = Parameter(default=None, significant=False)

    def __init__(self, *args, **kwargs):
        logger.info("*************************************************************************")
        logger.info("********** Starting pipeline PartitionedPriceAdjustmentPipeline **********")
        logger.info("*************************************************************************")
        kwargs["run_id"] = PricePipelineHelper.get_run_id()
        kwargs["pull_as_of"] = kwargs.get("pull_as_of", datetime.now())
        kwargs["date"] = PricePipelineHelper.get_proper_run_date(kwargs.get("date", datetime.utcnow()))

        super(PartitionedPriceAdjustmentPipeline, self).__init__(*args, **kwargs)
        if len(self.tickers):
            self.partitions = PricePipelineHelper.partition_tickers(self.tickers)
        self.adjustment_stages = []

    @property
    def stage_id(self):
        return self.date_ymd + '-price-ingestion-adjust'

    def requires(self):
        if not self.adjustment_stages:
            for i in range(len(self.partitions)):
                self.adjustment_stages.append(
                    AdjustTickersPartitionStage(run_id=self.run_id, date=self.date, start_date=self.start_date,
                                                end_date=self.end_date, pull_as_of=self.pull_as_of, partition_id=i,
                                                ticker_str=','.join(self.partitions[i])))
        return self.adjustment_stages

    def price_adjustment_pipeline_completed(self):
        with PipelineStageStore(self.run_id, self.date_only(), self.stage_id) as progress:
            stage_completed = progress.stage_key_has_been_processed(key="pipeline")
            logger.info(">>> Stage %s is '%s'" % (self.stage_id, 'completed' if stage_completed else 'incomplete'))
        return stage_completed

    def complete(self):
        return self.price_adjustment_pipeline_completed() or all(r.complete() for r in self.requires())

    def _get_unprocessed_tickers(self, requested_tickers):
        all_tickers = PricePipelineHelper.get_unprocessed_tickers(requested_tickers)
        self.all_tickers = all_tickers
        # return super(PartitionedPriceAdjustmentPipeline, self)._get_unprocessed_tickers(all_tickers)
        return all_tickers

    def _run_stage(self):
        # Get processed tickers
        with PipelineStageStore(self.run_id, self.date_only(), self.stage_id) as progress:
            processed_tickers = progress.get_completed_stage_keys()
        processed_tickers = list(filter(lambda x: "pipeline" not in x, processed_tickers))

        max_failed_allowed = pipeline_config.get("adjustments.max_missing_tickers")
        if isinstance(max_failed_allowed, str) and max_failed_allowed.endswith('%'):
            max_failed_allowed = int(np.ceil(float(max_failed_allowed[:-1]) * 0.01 * len(self.all_tickers)))
        else:
            max_failed_allowed = int(max_failed_allowed)

        # Number of unprocessed [or failed] tickers
        unprocessed_tickers = sorted(list(set(self.all_tickers) - set(processed_tickers)))
        if len(unprocessed_tickers) < max_failed_allowed:
            with PipelineStageStore(self.run_id, self.date_only(), self.stage_id) as progress:
                progress.save_progress(datetime.utcnow(), datetime.utcnow(), "pipeline")


# noinspection PyAttributeOutsideInit
class ProgressivePriceMergePipeline(PriceIngestionStageParams, PipelineStageWithTickers):
    # ProgressivePriceIngestionPipeline
    run_id = Parameter(default=None, significant=False)

    def __init__(self, *args, **kwargs):
        logger.info("*************************************************************************")
        logger.info("********** Starting pipeline ProgressivePriceMergePipeline **********")
        logger.info("*************************************************************************")
        kwargs["run_id"] = PricePipelineHelper.get_run_id()
        kwargs["pull_as_of"] = kwargs.get("pull_as_of", datetime.now())
        kwargs["date"] = PricePipelineHelper.get_proper_run_date(kwargs.get("date", datetime.utcnow()))

        super(ProgressivePriceMergePipeline, self).__init__(*args, **kwargs)
        self._merge_step = None

    def requires(self):
        if not self._merge_step:
            self._merge_step = ProgressiveMergeStage(run_id=self.run_id, date=self.date, start_date=self.start_date,
                                                     end_date=self.end_date, pull_as_of=self.pull_as_of,
                                                     ticker_str=','.join(self.tickers))
        return self._merge_step

    def complete(self):
        return self.requires().complete()

    def _get_unprocessed_tickers(self, requested_tickers):
        return PricePipelineHelper.get_unprocessed_tickers(requested_tickers)


if __name__ == '__main__':
    import os
    file_dir = os.path.dirname(os.path.realpath(__file__))
    logging_conf_path = os.path.join(file_dir, "..", "..", "conf", "luigi-logging.conf")

    pipeline_config["adjustments"]["partitions"] = 1
    tickers = ['AAPL']
    # run_luigi_pipeline([PartitionedPriceAdjustmentPipeline(ticker_str=','.join(tickers))], local_scheduler=True, logging_conf_file=logging_conf_path)
    run_luigi_pipeline([ProgressivePriceMergePipeline(ticker_str=','.join(tickers))], local_scheduler=True, logging_conf_file=logging_conf_path)
    # run_luigi_pipeline([PartitionedPriceAdjustmentPipeline(ticker_str="", date=datetime(2019, 3, 6))], local_scheduler=True, logging_conf_file=logging_conf_path)
    # run_luigi_pipeline([ProgressivePriceMergePipeline(ticker_str="")], local_scheduler=True, logging_conf_file=logging_conf_path)

