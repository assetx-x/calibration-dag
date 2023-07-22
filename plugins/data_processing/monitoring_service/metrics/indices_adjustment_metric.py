import pandas as pd

from commonlib.market_timeline import marketTimeline
from monitoring_service.metrics.dcm_metric import Metric, read_s3_file, metrics_config, get_redshift_engine, Goodness


class IndicesAdjustmentMetric(Metric):
    @staticmethod
    def _get_universe():
        body = read_s3_file(metrics_config["s3"]["bucket"], metrics_config["s3"]["universe_indices"])
        return pd.read_csv(body, names=["symbol"], header=None)

    def metric_aliases(self):
        return "index,cboe"

    @staticmethod
    def _get_last_timestamps():
        engine = get_redshift_engine()
        connection = engine.connect()
        select_str = (
            "SELECT MAX(date) as date, symbol "
            "FROM daily_index "
            "GROUP BY symbol;"
        )
        df = pd.read_sql(select_str, connection, parse_dates=["date"])
        connection.close()
        df["date"] = df["date"].map(pd.Timestamp.date)
        return df

    def _calculate_metric(self):
        universe_df = self._get_universe()
        timestamps_df = self._get_last_timestamps()
        df = universe_df.merge(timestamps_df, how="outer", on="symbol")
        expected_date = marketTimeline.get_previous_trading_day(pd.Timestamp.now(tz=self.timezone)).date()
        df["is_ok"] = df["date"] == expected_date

        universe_size = len(df)
        adjusted_count = df["is_ok"].astype(int).sum()
        adjusted_percentage = adjusted_count / universe_size * 100
        failed_count = universe_size - adjusted_count
        failed_percentage = failed_count / universe_size * 100
        self._goodness = Goodness.GOOD if failed_count == 0 else Goodness.ERROR

        return {
            "Failed count": failed_count,
            "Failed percentage": failed_percentage,
            "Adjusted count": adjusted_count,
            "Adjusted percentage": adjusted_percentage,
            "Expected date": expected_date.strftime("%Y-%m-%d")
        }


def _main():
    import logging
    import sys
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    from monitoring_service.dcm_metric_publishers import SlackMetricPublisher
    slack_publisher = SlackMetricPublisher("TestSlackPublisher", channel="#b.skidan", alert_channel="#test_alerts")
    metric = IndicesAdjustmentMetric("Index Ingestion", publishers=[slack_publisher], timezone="US/Eastern")
    metric.publish()


if __name__ == '__main__':
    _main()
