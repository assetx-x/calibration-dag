import json
import logging
from datetime import datetime, time, timedelta

from dateutil.parser import parse as parse_time
# noinspection PyProtectedMember
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

from pipelines.prices_ingestion.config import get_config
from pipelines.prices_ingestion.merge_workflow_steps import EquityDataForMerge

logger = logging.getLogger("stage_result_transfer")

kafka_bootstrap_servers = get_config()["kafka"]["bootstrap_servers"]
ssl_cafile = get_config()["kafka"]["ssl_cafile"]
ssl_certfile = get_config()["kafka"]["ssl_certfile"]
ssl_keyfile = get_config()["kafka"]["ssl_keyfile"]


adjusted_tickers_topic = "price_ingestion_pipeline.adjusted_tickers.results"


class StageResultPublisher(object):
    def __init__(self, client_id, topic, data):
        self.topic = topic
        self.data = data
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            security_protocol="SSL",
            client_id=client_id,
            ssl_cafile=ssl_cafile,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.producer.close()
        self.producer = None

    def send_results(self):
        if self.data:
            logger.info("Publishing all prepared tickers through Kafka")
            self.producer.send(self.topic, self.data)
        else:
            logger.info("No tickers were added to be published. Exiting")


class AdjustedTickersPublisher(object):
    def __init__(self, stage_id, run_date, partition_id, run_id):
        timestamp = str(datetime.utcnow())
        run_date = run_date.strftime("%Y-%m-%d")
        logger.info("Creating AdjustedTickersPublisher: %s, %s, %d, %s, %s", run_id, stage_id, partition_id, run_date, timestamp)
        self.stage_id = stage_id
        self.results = {
            'run_id': run_id,
            'stage_id': 'price_ingestion.adjust_tickers',
            'partition_id': partition_id,
            'timestamp': timestamp,
            'run_date': run_date,
            'tickers': []
        }

    def add_ticker(self, ticker, results):
        logger.info("Adding ticker %s to be published for merging", ticker)
        ticker_files = {}
        ticker_results = {'ticker': ticker, 'files': ticker_files}
        for result in results:
            if result['step_name'] == 'DepositAdjustedDataIntoS3':
                ticker_files['minute_data_file'] = result['S3_Location']
                logger.info("    %s: adding files for step '%s'", ticker, result['step_name'])
            elif result['step_name'] == 'DepositDailyAdjustedDataIntoS3':
                ticker_files['daily_data_file'] = result['S3_Location']
                logger.info("    %s: adding files for step '%s'", ticker, result['step_name'])
            elif result['step_name'] == 'DepositAdjustmentFactorsIntoS3':
                ticker_files['adjustment_factor_file'] = result['S3_AdjFactorsLocation']
                logger.info("    %s: adding files for step '%s'", ticker, result['step_name'])
        self.results['tickers'].append(ticker_results)

    def connect(self):
        num_tickers = len(self.results['tickers'])
        logger.info("Preparing %d added tickers as json for publishing", num_tickers)
        topic = adjusted_tickers_topic
        data = json.dumps(self.results).encode() if num_tickers > 0 else None
        publisher = StageResultPublisher(self.stage_id, topic, data)
        self.results = None
        return publisher


class AdjustedTickersRetriever(object):
    def __init__(self, stage_id, run_date, group_id=None):
        self.run_date = run_date
        group_id = group_id if group_id else 'price_ingestion_merge'
        self.consumer = KafkaConsumer(
            bootstrap_servers=kafka_bootstrap_servers,
            security_protocol="SSL",
            client_id=stage_id,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            ssl_cafile=ssl_cafile,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
        )
        self.consumer.assign([TopicPartition(adjusted_tickers_topic, 0)])
        self.ticker_timestamps = {}
        self.ticker_files = {}
        logger.info("*** Created AdjustedTickersRetriever %s, group=%s, topic=%s",
                    stage_id, group_id, adjusted_tickers_topic)

    def get_results(self):
        self.ticker_timestamps = {}
        self.ticker_files = {}

        records = []
        while True:
            partitions = self.consumer.poll(5000, max_records=150)
            if not any(partitions):
                return {}

            records = [record for partition in partitions.values() for record in partition]
            if records:
                logger.info("Received %d adjusted ticker records. Offsets %d - %d",
                            len(records), records[0].offset, records[-1].offset)
                for record in records:
                    self._process_record(record.value)
                logger.info("Received %d relevant messages for processing", len(self.ticker_files))
                if len(self.ticker_files) > 0:
                    return self.ticker_files
            else:
                logger.info("!!! Did not receive any adjusted ticker records")
                return {}

    def acknowledge_progress(self):
        self.consumer.commit()

    def _process_record(self, record_json):
        try:
            record = json.loads(record_json)
            run_date = parse_time(record['run_date'])
            if run_date == self.run_date:
                record_timestamp = parse_time(record['timestamp'])
                self._process_tickers(record_timestamp, record['tickers'])
            else:
                logger.warning("Not processing record, incorrect run date '%s', expected '%s'", run_date, self.run_date)
        except:
            logger.error("Failed to parse json: " + record_json)

    def _process_tickers(self, record_timestamp, adjusted_tickers):
        for ticker_info in adjusted_tickers:
            ticker = ticker_info['ticker']
            if ticker not in self.ticker_timestamps or record_timestamp > self.ticker_timestamps[ticker]:
                logger.info("Processing ticker price file info for '%s'", ticker)
                self.ticker_timestamps[ticker] = record_timestamp
                self.ticker_files[ticker] = self._parse_files(ticker_info['files'])

    @staticmethod
    def _parse_files(file_info):
        return EquityDataForMerge(minute_data_file=file_info['minute_data_file'],
                                  daily_data_file=file_info['daily_data_file'],
                                  adjustment_factor_file=file_info['adjustment_factor_file'])


if __name__ == '__main__':
    test_num = 3
    test_run_date = datetime.combine((datetime.now() - timedelta(days=1)).date(), time(0, 0))

    def client(client_id):
        return "retriever-test-%d-client-%d" % (test_num, client_id)

    def group(group_id):
        return "retriever-test-%d-group-%d" % (test_num, group_id)

    atr = AdjustedTickersRetriever(client(1), test_run_date, group(1))
    ticker_data = atr.get_results()
    atr.acknowledge_progress()

    atr = AdjustedTickersRetriever(client(2), test_run_date, group(1))
    ticker_data = atr.get_results()
    atr.acknowledge_progress()

    atr = AdjustedTickersRetriever(client(1), test_run_date, group(2))
    ticker_data = atr.get_results()
    atr.acknowledge_progress()

    atr = AdjustedTickersRetriever(client(2), test_run_date, group(2))
    ticker_data = atr.get_results()
    atr.acknowledge_progress()

    print("breakpoint")
