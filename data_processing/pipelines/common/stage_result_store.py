import pickle
import logging

from pipelines.common.pipeline_util import get_postgres_engine

logger = logging.getLogger("stage_result_store")


class PipelineStageResultsStore(object):
    def __init__(self, run_id, run_date, stage_id):
        self.run_id = run_id
        self.run_date = run_date
        self.stage_id = stage_id
        self.db_engine = None

    def open(self):
        if not self.db_engine:
            self.db_engine = get_postgres_engine()
        return self

    def close(self):
        if self.db_engine:
            self.db_engine.dispose()
            self.db_engine = None

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def save_results(self, stage_key, stage_results):
        logger.info("Saving results of %s, %s", self.stage_id, stage_key)
        for single_step_results in stage_results:
            self._save_single_step_result(stage_key, single_step_results)

    def _save_single_step_result(self, stage_key, single_step_results):
        self.db_engine.execute(
            """INSERT INTO pipeline_stage_results (run_id, run_date, stage_id, stage_key, substage_id, step_name, step_status, step_start, step_end, serialized_data)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            self.run_id,
            self.run_date,
            self.stage_id,
            stage_key,
            single_step_results.get("Sub-Stage", "NA"),
            single_step_results["step_name"],
            single_step_results["status_type"],
            single_step_results["task_start"],
            single_step_results["task_end"],
            pickle.dumps(single_step_results).decode("utf8", errors="replace")
        )
