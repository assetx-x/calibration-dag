import csv
import json
from datetime import date

from pipelines.earnings_ingestion.capiq import CapIQClient
from dateutil.relativedelta import relativedelta
from luigi import DateParameter, IntParameter, DictParameter, BoolParameter, Task

from base import credentials_conf, DCMTaskParams


capiq_request_item_template = """<CIQ {params} id="{id}" Layout="0" cellAddress="{tracking}"/>"""


# noinspection PyTypeChecker
class PullFundamentalsTask(DCMTaskParams, Task):
    required_metrics = DictParameter(description="List of metrics to pull", default={}, significant=False)
    start_date = DateParameter(description="Start date", default=date(2000, 1, 1), significant=False)
    interval = IntParameter(description="Interval in months", default=3, significant=False)
    pull_missing_only = BoolParameter(description="Only missing metrics should be retrieved", default=False)

    def __init__(self, *args, **kwargs):
        super(PullFundamentalsTask, self).__init__(*args, **kwargs)
        self.raw_json = None

    def run(self):
        if self.pull_missing_only:
            df_missing_items = self._load_missing_items()
            request_items = self._generate_capiq_missing_request_items(df_missing_items)
        else:
            tickers = self._read_tickers_universe()
            request_items = self._generate_capiq_request_items(tickers)
        with CapIQClient(username=credentials_conf["capiq"]["username"],
                         password=credentials_conf["capiq"]["password"]) as capiq:
            self.raw_json = self._collect_capiq_responses(capiq, request_items)
        self._store_raw_data_in_s3(self.raw_json)

    def _read_tickers_universe(self):
        self.logger.info("Reading ticker universe")
        with self.input()["universe"].open() as universe_fd:
            tickers = [line.strip() for line in universe_fd.read().split("\n")]
        # tickers = ['AAPL', 'FB', 'AMZN', 'TSLA']
        # tickers = ['FB']

        self.logger.info("Reading ETF")
        with self.input()["etf"].open() as etf_fd:
            etf_list = [row[0] for row in csv.reader(etf_fd)][1:]

        tickers = set(tickers) - set(etf_list)

        with self.input()["daily_equity_prices"].engine() as engine:
            tickers = {
                row["ticker"]: row["start_date"].date()
                for row in engine.execute(
                    "select ticker, min(date) as start_date from %s group by ticker"
                    % self.input()["daily_equity_prices"].table
                )
                if row["ticker"] in tickers
            }
        return tickers

    def _generate_capiq_request_items(self, tickers):
        self.logger.info("Generating CapIQ request items")
        rel_delta_9mo = relativedelta(months=9)
        rel_delta_int = relativedelta(months=int(self.interval))
        items = []
        next_id = 1
        as_of_date = self.date.date()
        current_date = self.start_date
        while current_date < as_of_date:
            current_date_ymd = current_date.strftime("%Y-%m-%d")
            next_date = current_date + rel_delta_int
            for group, mapping in dict(self.required_metrics).iteritems():
                for metric_name, (capiq_metric_name, template) in mapping.iteritems():
                    for ticker, first_valid_date in tickers.iteritems():
                        if "_ltm" in metric_name:  # FIXME: need to know about ltm/cq in specification
                            first_valid_date += rel_delta_9mo
                        if next_date >= first_valid_date:
                            items.append(
                                capiq_request_item_template.format(
                                    params=template.format(
                                        ticker=ticker,
                                        metric=capiq_metric_name,
                                        date=current_date_ymd,
                                    ).strip(),
                                    id=next_id,
                                    tracking="%s;%s;%s" % (ticker, current_date_ymd, metric_name)
                                )
                            )
                            next_id += 1
                            if next_id % 250000 == 0:
                                self.logger.info("So far generated %d items" % next_id)
            current_date = next_date
        return items

    def _generate_capiq_missing_request_items(self, df_missing_items):
        self.logger.info("Generating CapIQ missing request items")
        items = []
        mappings = dict()
        for group, mapping in dict(self.required_metrics).iteritems():
            mappings.update(mapping)
        for index, item in df_missing_items.iterrows():
            capiq_metric_name, template = mappings[item.metric]
            items.append(
                capiq_request_item_template.format(
                    params=template.format(
                        ticker=item.ticker,
                        metric=capiq_metric_name,
                        date=item.date.strftime("%d.%m.%Y"),
                    ).strip(),
                    id=index,
                    tracking="%s;%s;%s" % (item.ticker, item.date.strftime("%Y-%m-%d"), item.metric)
                )
            )
        return items

    def _collect_capiq_responses(self, capiq, request_items):
        self.logger.info("Collecting responses")
        response_chunks = []
        for i, chunk in enumerate(self._split_by(request_items, 25000)):
            self.logger.info("Pulling %d/%d" % (min((i + 1) * 25000, len(request_items)), len(request_items)))
            request_body = capiq.render_request_body(chunk)
            http_response = capiq.post_request(request_body)
            content = capiq.decode_response(http_response)
            self.logger.info("Got %d bytes" % len(content))
            response_chunks.append(content)
        return response_chunks

    @staticmethod
    def _split_by(lst, size):
        chunk = []
        for item in lst:
            chunk.append(item)
            if len(chunk) == size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

    def _store_raw_data_in_s3(self, raw_json):
        self.logger.info("Saving fundamentals raw data json to s3 for backup")
        with self.output().open("w") as output_fd:
            json.dump(raw_json, output_fd)
        self.logger.info("Done saving fundamentals raw data to s3")
