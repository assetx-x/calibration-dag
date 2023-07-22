import logging

import pandas as pd
import sqlalchemy as sa

from monitoring_service.metrics.dcm_metric import Metric, get_postgres_engine, Goodness

_logger = logging.getLogger("metrics.earnings_ingestion")


class EarningsIngestionMetric(Metric):
    def __init__(self, *args, **kwargs):
        super(EarningsIngestionMetric, self).__init__(*args, **kwargs)
        self._postgres_engine = get_postgres_engine()

    def metric_aliases(self):
        return "calendar"

    def _calculate_metric(self):
        expected_date = pd.Timestamp.now(self.timezone)
        results = self.__read_results(expected_date)

        if len(results) <= 0:
            status_message = "The pipeline didn't run yet"
            self._goodness = Goodness.ERROR
        else:
            status_message = "SUCCESS"
            self._goodness = Goodness.GOOD

            last_success = ''
            for index, result in results.iterrows():
                last_status = result["status_type"]
                last_step = result["step_name"]
                if last_status == 'Success':
                    last_success = last_step
                elif last_status == 'Fail':
                    status_message = "FAILED: %s step has failed" % last_step
                    self._goodness = Goodness.ERROR
                elif self._goodness != Goodness.ERROR and last_status == 'Warn':
                    status_message = "%s step finished with a warning" % last_step
                    self._goodness = Goodness.WARNING
                elif self._goodness != Goodness.ERROR:
                    status_message = "FAILED: %s step status is %s" % (last_step, last_status)
                    self._goodness = Goodness.ERROR

            if self._goodness == Goodness.GOOD and last_success != 'ConsolidatedEarningsRedshiftUpdate':
                status_message = "The pipeline didn't finish yet. Last step is %s" % last_success
                self._goodness = Goodness.WARNING

        _logger.info(status_message)
        return {
            "Expected date": expected_date.strftime("%Y-%m-%d"),
            "Status": status_message
        }

    def __read_results(self, expected_date):
        select_str = sa.text((
                                 "SELECT status_type, step_name FROM pipeline_status "
                                 "WHERE stage_id like '%s-earnings-ingestion-%%'"
                                 "ORDER BY task_start"
                             ) % expected_date.strftime("%Y%m%d"))

        connection = self._postgres_engine.connect()
        try:
            return pd.read_sql(select_str, connection)
        finally:
            connection.close()
