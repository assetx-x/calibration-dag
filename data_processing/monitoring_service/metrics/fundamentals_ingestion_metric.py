import pandas as pd

from monitoring_service.metrics.dcm_metric import Metric, get_redshift_engine, Goodness


class FundamentalsIngestionMetric(Metric):
    def metric_aliases(self):
        return "quandl"

    def _calculate_metric(self):
        expected_date = pd.Timestamp.now(self.timezone).replace(tzinfo=None).normalize()
        last_import_date_time = self._get_data_from_redshift()
        result_dict = {
            "Expected date": expected_date.strftime("%Y-%m-%d %H:%M"),
            "Last import date": last_import_date_time.strftime("%Y-%m-%d %H:%M"),
            "Status": "SUCCESS"}
        self._goodness = Goodness.GOOD
        if last_import_date_time < expected_date:
            result_dict["Status"] = "FAILED"
            self._goodness = Goodness.ERROR
        return result_dict

    def _get_data_from_redshift(self):
        redshift_engine = get_redshift_engine()
        connection = redshift_engine.connect()
        select_str = (
            "SELECT max(import_time) as max_date "
            "FROM fundamental_data;"
        )
        result = pd.read_sql(select_str, connection, parse_dates=["max_date"])
        connection.close()

        return pd.Timestamp(result["max_date"].values[0])
