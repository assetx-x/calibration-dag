import logging.config
import os

_config_dir = os.path.join(os.path.dirname(__file__), "config")
logging.config.fileConfig(os.path.join(_config_dir, "logging.conf"))

import pandas as pd
from pyhocon import ConfigFactory

from monitoring_service.metrics.earnings_ingestion_metric import EarningsIngestionMetric
from monitoring_service.metrics.equity_adjustment_metric import EquityAdjustmentMetric
from monitoring_service.metrics.fundamentals_ingestion_metric import FundamentalsIngestionMetric
from monitoring_service.metrics.futures_adjustment_metric import FuturesAdjustmentMetric
from monitoring_service.metrics.indices_adjustment_metric import IndicesAdjustmentMetric
from monitoring_service.metrics.ravenpack_health_status import RavenpackHealthStatus


import monitoring_service.dcm_metric_publishers as dcm_metric_publishers
from monitoring_service.dcm_metric_schedulers import MetricScheduler, Schedule

logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("apscheduler.scheduler").setLevel(logging.WARNING)

app_config = ConfigFactory.parse_file(os.path.join(_config_dir, "app.conf"))

_timezone = app_config["timezone"]

logger = logging.getLogger("monitoring.service")

if __name__ == "__main__":

    logger.info("Starting monitoring service")

    slack_publisher = dcm_metric_publishers.SlackMetricPublisher("StandardSlack", channel="#devops")

    equity_adjustment_metric = EquityAdjustmentMetric(
        "Equity Adjustment Process", publishers=[slack_publisher], timezone=_timezone)

    index_adjustment_metric = IndicesAdjustmentMetric(
        "Index Adjustment Process", publishers=[slack_publisher], timezone=_timezone)

    futures_adjustment_metric = FuturesAdjustmentMetric(
        "Futures Adjustment Process", publishers=[slack_publisher], timezone=_timezone)

    earnings_ingestion_metric = EarningsIngestionMetric(
        "Earnings Calendar Ingestion Health Check", publishers=[slack_publisher], timezone=_timezone)

    fundamentals_ingestion_metric = FundamentalsIngestionMetric(
        "Fundamental Ingestion", publishers=[slack_publisher], timezone=_timezone)

#    ravenpack_health_check = RavenpackHealthStatus(
#        "RavenPack Sentiment", publishers=[slack_publisher], timezone=_timezone)

    all_metrics = [
        equity_adjustment_metric,
        index_adjustment_metric,
        futures_adjustment_metric,
        earnings_ingestion_metric,
        fundamentals_ingestion_metric,
#        ravenpack_health_check
    ]

    schedule = Schedule()
    schedule.add(days=1, start_date=pd.Timestamp("04:30:00", tz=_timezone))
    schedule.add(days=1, start_date=pd.Timestamp("06:30:00", tz=_timezone))
    # schedule.add(days=1, start_date=pd.Timestamp.now(tz=_timezone) + pd.Timedelta(seconds=3))

    scheduler = MetricScheduler()
    scheduler.register_metrics(all_metrics, schedule)
    scheduler.run()

    logger.info("All monitors are scheduled, waiting for user requests")
    while True:
        slack_publisher.check_update_requests(all_metrics)
        logging.time.sleep(2)
