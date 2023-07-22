from datetime import datetime, time

import pytz

from monitoring_service.metrics.dcm_metric import Metric, get_redshift_engine, Goodness
from pipelines.common.stage_store import PipelineStageStore
from pipelines.prices_ingestion import PricePipelineHelper


class EquityAdjustmentMetric(Metric):
    """ Metric for checking multiple measures related to adjustment process """

    def __init__(self, *args, **kwargs):
        super(EquityAdjustmentMetric, self).__init__(*args, **kwargs)
        self._redshift_engine = get_redshift_engine()

    def metric_aliases(self):
        return "prices,ingestion"

    def publish(self, update=True):
        super(EquityAdjustmentMetric, self).publish(update)
        self._verify_required_tickers()

    def _calculate_metric(self):
        self.run_date = PricePipelineHelper.get_proper_run_date(datetime.now(tz=pytz.timezone(self.timezone)))
        prev_date = PricePipelineHelper.get_proper_run_date(datetime.combine(self.run_date, time(9, 30)))
        universe = PricePipelineHelper.load_tickers()
        pulled_tickers = self._get_results(self.run_date, 'price-ingestion-corp-action-pull')
        adjusted_tickers = self._get_results(self.run_date, 'price-ingestion-adjust')
        merged_tickers = self._get_results(self.run_date, 'price-ingestion-merge')
        prev_merged = self._get_results(prev_date, 'price-ingestion-merge')
        greenlist_count = self._get_green_list_count(self.run_date)
        return self._progress_report(universe, pulled_tickers, adjusted_tickers, merged_tickers, prev_merged, greenlist_count)

    @staticmethod
    def _get_results(run_date, stage_name):
        stage_id = run_date.strftime("%Y%m%d-") + stage_name
        with PipelineStageStore('ignored', run_date.date(), stage_id) as progress:
            return progress.get_completed_stage_keys()

    def _get_green_list_count(self, run_date):
        connection = self._redshift_engine.connect()
        select_str = (
            "SELECT COUNT(*) "
            "FROM whitelist "
            "WHERE DATE(run_dt) = '%s' AND as_of_end IS NULL;"
        ) % run_date.strftime("%Y-%m-%d")
        result = connection.execute(select_str).scalar()
        connection.close()
        return int(result)

    def _progress_report(self, universe, pulled_tickers, adjusted_tickers, merged_tickers, prev_merged, greenlist_count):
        universe_size = len(universe)
        pulled_progress, _ = self._get_progress(len(pulled_tickers), universe_size)
        adjusted_progress, _ = self._get_progress(len(adjusted_tickers), universe_size)
        merged_progress, merged_pct = self._get_progress(len(merged_tickers), universe_size)
        greenlist_progress, _ = self._get_progress(greenlist_count, universe_size)

        self._set_goodness(merged_pct)

        result = [
            ("Run date", self.run_date.strftime("%Y-%m-%d")),
            ("Universe size", universe_size),
            ("Corp actions pulled", pulled_progress),
            ("Adjusted", adjusted_progress),
            ("Merged", merged_progress),
            ("Green list", greenlist_progress),
        ]

        self.missing_tickers = sorted(list(set(universe) - set(merged_tickers)))
        prev_missing = sorted(list(set(universe) - set(prev_merged)))
        missing_count = len(self.missing_tickers)
        for i, ticker in enumerate(self.missing_tickers):
            if ticker not in prev_missing:
                self.missing_tickers[i] = "*" + ticker + "*"
        not_missing_anymore = ["~" + t + "~" for t in sorted(list(set(prev_missing) - set(self.missing_tickers)))]
        if missing_count > 0 or len(not_missing_anymore) > 0:
            result.append(("%d missing tickers" % missing_count, ', '.join(self.missing_tickers + not_missing_anymore)))

        return result

    @staticmethod
    def _get_progress(processed, total):
        pct = round(float(processed) / total * 100, 2)
        progress = "{processed}/{total} ({percentage}%)".format(processed=processed, total=total, percentage=pct)
        return progress, pct

    def _set_goodness(self, adjusted_percentage):
        goodness = Goodness.GOOD
        if 85 <= adjusted_percentage < 99:
            goodness = Goodness.WARNING
        elif adjusted_percentage < 85:
            goodness = Goodness.ERROR
        self._goodness = goodness

    def _verify_required_tickers(self):
        required_tickers = PricePipelineHelper.load_tickers("tickers_alert_if_failed.txt")
        required_but_missing = [missing for missing in self.missing_tickers if missing in required_tickers]
        if required_but_missing:
            for publisher in self._publishers:
                publisher.publish_alert(
                    "Equity Adjustment Critical Failure",
                    "The following tickers are required but did not get adjusted",
                    ", ".join(required_but_missing))


def _main():
    import logging
    import sys
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    from monitoring_service.dcm_metric_publishers import SlackMetricPublisher
    slack_publisher = SlackMetricPublisher("TestSlackPublisher", channel="#b.skidan", alert_channel="#test_alerts")
    metric = EquityAdjustmentMetric("Equity Adjustment", publishers=[slack_publisher], timezone="US/Eastern")
    metric.publish()


if __name__ == '__main__':
    _main()
