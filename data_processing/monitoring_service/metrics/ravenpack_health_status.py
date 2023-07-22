import logging
import re

import pandas as pd

from commonlib.market_timeline import marketTimeline
from monitoring_service.metrics.dcm_metric import Metric, get_redshift_engine, metrics_config, Goodness

_logger = logging.getLogger("metrics.ravenpack")

class RavenpackHealthStatus(Metric):
    def __init__(self, metric_name, publishers, timezone):
        super(RavenpackHealthStatus, self).__init__(metric_name, publishers, timezone)
        ravenpack_config = metrics_config["ravenpack"]
        self._acceptable_realtime_gap = pd.Timedelta(ravenpack_config["acceptable_realtime_gap"])
        self._acceptable_historical_gap = pd.Timedelta(ravenpack_config["acceptable_historical_gap"])
        self._engine = get_redshift_engine()

    def metric_aliases(self):
        return "news,sentiment"

    def _get_last_timestamp(self):
        _logger.info("Retrieving RavenPack last insertion timestamp")
        connection = self._engine.connect()
        select_str = "SELECT MAX(timestamp_utc) FROM ravenpack_equities;"
        result = connection.execute(select_str)
        connection.close()
        return pd.Timestamp(result.scalar(), tz="UTC")

    def _calculate_metric(self):
        _logger.info("Calculating RavenPack health metric")
        last_timestamp = self._get_last_timestamp()
        utc_now = pd.Timestamp.now(tz="UTC")

        elapsed = utc_now - last_timestamp
        realtime_is_healthy = elapsed <= self._acceptable_realtime_gap

        last_timestamp = last_timestamp.tz_convert(self.timezone).strftime("%Y-%m-%d %H:%M:%S %Z")
        last_update_description = "%s (%s ago)" % (last_timestamp, self._format_time_delta(elapsed, True))
        _logger.info("Last update happened at %s", last_update_description)

        longest_gap, time_of_longest_gap = self._get_longest_gap()
        historical_is_healthy = longest_gap <= self._acceptable_historical_gap

        time_of_longest_gap = time_of_longest_gap.tz_localize("UTC").tz_convert(self.timezone)
        time_of_longest_gap = time_of_longest_gap.strftime("%Y-%m-%d %H:%M:%S %Z")
        longest_gap_description = "%s (at %s)" % (self._format_time_delta(longest_gap), time_of_longest_gap)
        _logger.info("Longest historical gap is %s", longest_gap_description)

        self._goodness = Goodness.GOOD if realtime_is_healthy and historical_is_healthy else Goodness.ERROR
        return {
            "Realtime Status": "Running" if realtime_is_healthy else "Not Running",
            "Last Updated": last_update_description,
            "Longest Gap": longest_gap_description
        }

    def _get_longest_gap(self):
        _logger.info("Calculating RavenPack longest historical gap ...")
        now_utc = pd.Timestamp.now(tz="UTC")
        start_time = marketTimeline.get_previous_trading_day(now_utc) + pd.Timedelta(hours=16)
        start_time = start_time.tz_localize("US/Eastern").tz_convert("UTC")
        query = """
            with ravenpack_requested_range as (
                select date_trunc('minute', timestamp_utc) as timestamp_utc from ravenpack_equities 
                where timestamp_utc >= '{}' and timestamp_utc <= '{}'
            )
            select timestamp_utc, count(*) from ravenpack_requested_range
            group by timestamp_utc
            order by timestamp_utc
        """.format(start_time.strftime("%Y-%m-%d %H:%M"), now_utc.strftime("%Y-%m-%d %H:%M"))

        _logger.info("Retrieving RavenPack historical event counts from database")
        data_quality = pd.read_sql(query, self._engine)

        _logger.info("Calculating longest event gap")
        data_quality.index.name = 'from_time'
        data_quality.reset_index(inplace=True)
        data_quality['to_time'] = data_quality['timestamp_utc'].shift(-1).fillna(data_quality['timestamp_utc'].iloc[-1])
        data_quality["gap"] = data_quality['to_time'] - data_quality['timestamp_utc']
        longest_gap = data_quality["gap"].max()
        time_of_longest_gap = data_quality.loc[data_quality["gap"] == longest_gap].timestamp_utc.iloc[0]
        return longest_gap, time_of_longest_gap

    def _format_time_delta(self, time_delta, include_seconds=False):
        result = ""
        if time_delta.components.days > 0:
            result += str(time_delta.components.days) + (" day, " if time_delta.components.days == 1 else " days, ")
        if time_delta.components.hours > 0:
            result += str(time_delta.components.hours) + (" hour, " if time_delta.components.hours == 1 else " hours, ")
        if time_delta.components.minutes > 0:
            result += str(time_delta.components.minutes) + (
                " minute, " if time_delta.components.minutes == 1 else " minutes, ")
        if include_seconds and time_delta.components.seconds > 0:
            result += str(time_delta.components.seconds) + (
                " second, " if time_delta.components.minutes == 1 else " seconds, ")
        result = result.strip().strip(',')
        return re.sub(r",([^,]+)$", r" and\1", result)


def _main():
    import logging
    import sys
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    from monitoring_service.dcm_metric_publishers import SlackMetricPublisher
    slack_publisher = SlackMetricPublisher("TestSlackPublisher", channel="#b.skidan", alert_channel="#test_alerts")
    metric = RavenpackHealthStatus("RavenPack Sentiment", publishers=[slack_publisher], timezone="US/Eastern")
    metric.publish()


if __name__ == '__main__':
    _main()
