import logging
import pandas as pd

from base import DCMTaskParams
from datetime import datetime, timedelta
from luigi import Task as LuigiTask, Parameter
from etl_workflow_steps import EquitiesETLTaskParameters
from pipelines.common.stage_store import PipelineStageStore
from pipelines.common.stage_result_store import PipelineStageResultsStore


class DataPipelineStage(DCMTaskParams, LuigiTask):
    run_id = Parameter(significant=False)

    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Creating ...")
        super(DataPipelineStage, self).__init__(*args, **kwargs)
        self.already_executed = False

    def run(self):
        self.logger.info("Running ...")
        try:
            self._run_stage()
        finally:
            self.already_executed = True
            self.logger.info("... done")

    def complete(self):
        return self.already_executed or self._check_stage_status()

    def _check_stage_status(self):
        with PipelineStageStore(self.run_id, self.date_only(), self.stage_id) as progress:
            return len(progress.get_completed_stage_keys()) > 0

    def _run_stage(self):
        pass


class PipelineStageWithTickers(DataPipelineStage):
    ticker_str = Parameter(default="", significant=False)

    # noinspection PyUnresolvedReferences
    def __init__(self, *args, **kwargs):
        super(PipelineStageWithTickers, self).__init__(*args, **kwargs)
        self.all_tickers = []
        self.tickers = self._get_unprocessed_tickers(self.ticker_str.split(',') if self.ticker_str.strip() else [])

    def _get_unprocessed_tickers(self, all_tickers):
        self.logger.info("Fetching processed tickers from stage store")
        with PipelineStageStore(self.run_id, self.date_only(), self.stage_id) as progress:
            processed_tickers = progress.get_completed_stage_keys()
        processed_tickers = filter(lambda x: "pipeline" not in x, processed_tickers)  # get just tickers
        unprocessed_tickers = sorted(list(set(all_tickers) - set(processed_tickers)))
        processed_tickers = sorted(list(set(all_tickers) - set(unprocessed_tickers)))
        self.logger.info("%d out of %d tickers have already been processed", len(processed_tickers), len(all_tickers))
        self.logger.info("There are %d tickers left to process", len(unprocessed_tickers))
        return unprocessed_tickers

    def complete(self):
        is_complete = self.already_executed or self.tickers is None or len(self.tickers) == 0
        if is_complete:
            self.tickers = None
        return is_complete

    def _run_stage(self):
        self._process_tickers()

    def _process_tickers(self):
        pass

    def _save_progress(self, start_time, end_time, processed_keys):
        if len(processed_keys) > 0:
            with PipelineStageStore(self.run_id, self.date_only(), self.stage_id) as progress:
                progress.save_progress(start_time, end_time, processed_keys)


class PipelineStageWithResults(DataPipelineStage):
    def _get_task_params(self, ticker):
        return EquitiesETLTaskParameters(
            ticker=ticker,
            as_of_dt=pd.Timestamp(self.pull_as_of + timedelta(days=1)),
            run_dt=pd.Timestamp(self.date),
            start_dt=pd.Timestamp(self.start_date),
            end_dt=pd.Timestamp(self.end_date or self.date)
        )

    def _run_stage(self):
        start_time = datetime.utcnow()
        self.logger.info("Running stage, delegating to task flow")
        flow_engine, stage_flow = self._build_stage_flow()
        step_results = self._run_stage_flow(flow_engine, stage_flow)
        successfully_processed = self._process_stage_results('stage', step_results)
        if successfully_processed:
            self.logger.info("... done. Successfully executed task flow")
        else:
            self.logger.info("... done. Failed to execute task flow")
        self._mark_stage_as_complete(start_time, successfully_processed)

    def _mark_stage_as_complete(self, start_time, completed_successfully):
        end_time = datetime.utcnow()
        with PipelineStageStore(self.run_id, self.date_only(), self.stage_id) as progress:
            progress.save_progress(start_time, end_time, 'stage')

    def _build_stage_flow(self):
        raise RuntimeError("_build_stage_flow must be implemented")

    def _run_stage_flow(self, flow_engine, stage_flow):
        self.logger.info("Executing task flow ...")
        flow_engine.run(stage_flow)
        results = flow_engine.storage.fetch_all()
        return list(filter(None, results["step_results"]))

    def _process_stage_results(self, stage_key, stage_results):
        self.logger.info("Processing stage %s results: %d records", stage_key, len(stage_results))
        with PipelineStageStore(self.run_id, self.date_only(), self.stage_id) as progress:
            if progress.stage_key_has_been_processed(stage_key):
                self.logger.info("Stage %s[%s] has been already processed by someone else. Discarding", self.stage_id, stage_key)
                return None
        successfully_processed = True
        # with PipelineStageResultsStore(self.run_id, self.date_only(), self.stage_id) as result_store:
        #     result_store.save_results(stage_key, stage_results)
        for step_result in stage_results:
            if step_result["status_type"] == "Fail":
                self._log_error_details(step_result)
                successfully_processed = False
        if successfully_processed:
            self.logger.info("Processing stage %s results: successfully processed", stage_key)
            self.on_stage_success(stage_key, stage_results)
        else:
            self.logger.info("Processing stage %s results: recording failure", stage_key)
            self.on_stage_failure(stage_key, stage_results)
        return successfully_processed

    def _log_error_details(self, step_result):
        try:
            step_error = step_result["error"]
            traceback_str, exception_str = step_error["traceback_str"], step_error["exception_str"]
        except Exception:
            self.logger.error("Step %s failed" % step_result["step_name"])
        else:
            self.logger.error("Step %s failed with exception: %s" % (step_result["step_name"], exception_str))
            self.logger.error("\n" + traceback_str)

    def on_stage_success(self, stage_key, stage_results):
        self.logger.info("Inside base class 'on_stage_success()'")
        pass

    def on_stage_failure(self, stage_key, stage_results):
        self.logger.info("Inside base class 'on_stage_failure()'")
        pass


class TaskFlowPipelineStageWithTickers(PipelineStageWithTickers, PipelineStageWithResults):
    def _process_tickers(self):
        start_time = datetime.utcnow()
        processed_tickers = []
        for ticker in self.tickers:
            if not self._has_ticker_been_processed(ticker):
                self._process_single_ticker(ticker, processed_tickers)
        end_time = datetime.utcnow()
        self._save_progress(start_time, end_time, processed_tickers)

    def _has_ticker_been_processed(self, ticker):
        with PipelineStageStore(self.run_id, self.date_only(), self.stage_id) as progress:
            if progress.stage_key_has_been_processed(ticker):
                self.logger.info("Task flow for ticker %s has been already executed, possibly by another worker", ticker)
                return True
        return False

    def _process_single_ticker(self, ticker, processed_tickers):
        self.logger.info("Executing task flow for ticker %s ...", ticker)
        flow_engine, stage_flow = self._build_stage_flow_for_ticker(ticker)
        step_results = self._run_stage_flow(flow_engine, stage_flow)

        successfully_processed = self._process_stage_results(ticker, step_results)
        if successfully_processed is not None:
            if successfully_processed:
                processed_tickers.append(ticker)
                self.logger.info("... done. Successfully executed task flow for ticker %s", ticker)
            else:
                self.logger.info("... done. Failed to execute task flow for ticker %s", ticker)

    def _build_stage_flow_for_ticker(self, ticker):
        raise RuntimeError("_build_stage_flow_for_ticker must be implemented")
