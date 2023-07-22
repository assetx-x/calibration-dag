import logging
import pandas as pd
from pipelines.common.pipeline_util import get_postgres_engine

logger = logging.getLogger("stage_store")


class PipelineStageStore(object):
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

    def save_progress(self, start_time, end_time, completed_keys):
        if hasattr(completed_keys, "__iter__") and not isinstance(completed_keys, str):
            logger.info("Saving progress of %s, %d keys", self.stage_id, len(completed_keys))
            for key in completed_keys:
                self._save_single_key_progress(start_time, end_time, key)
        else:
            logger.info("Saving progress of '%s': '%s'", self.stage_id, completed_keys)
            self._save_single_key_progress(start_time, end_time, completed_keys)

    def stage_key_has_been_processed(self, key):
        query = """SELECT * FROM pipeline_stage_status
                   WHERE run_date = '{run_date}'
                   AND stage_id = '{stage_id}'
                   AND stage_key = '{stage_key}'
                """.format(run_date=self.run_date, stage_id=self.stage_id, stage_key=key)
        stage_results = pd.read_sql(query, self.db_engine)
        return len(stage_results) > 0

    def get_completed_stage_records(self):
        query = """SELECT * FROM pipeline_stage_status
                   WHERE run_date = '{run_date}'
                   AND stage_id = '{stage_id}'
                """.format(run_date=self.run_date, stage_id=self.stage_id)
        stage_results = pd.read_sql(query, self.db_engine)
        logger.info("Retrieved %d records for stage %s", len(stage_results), self.stage_id)
        return stage_results

    def get_completed_stage_keys(self):
        completed_stage_keys = self.get_completed_stage_records()["stage_key"].tolist()
        return completed_stage_keys

    def reset_stage_status(self, key):
        sql_command = """DELETE FROM pipeline_stage_status
                         WHERE run_date = '{run_date}'
                         AND stage_id = '{stage_id}'
                         AND stage_key = '{stage_key}'
                      """.format(run_date=self.run_date, stage_id=self.stage_id, stage_key=key)
        self.db_engine.execute(sql_command)
        logger.info("Status record of stage '%s' with stage key '%s' has been dropped" % (self.stage_id, key))

    def _save_single_key_progress(self, start_time, end_time, single_key):
        self.db_engine.execute(
            """INSERT INTO pipeline_stage_status (run_id, run_date, stage_id, stage_key, stage_start, stage_end)
               VALUES (%s)""" % self._format_values(single_key, start_time, end_time)
        )

    def _format_values(self, key, start_time, end_time):
        return "'%s', '%s', '%s', '%s', '%s', '%s'" % (
                self.run_id, self.run_date, self.stage_id, key, start_time, end_time)
