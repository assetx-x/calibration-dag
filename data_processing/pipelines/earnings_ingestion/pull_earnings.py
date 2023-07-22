import csv
from collections import deque
from datetime import datetime, date, timedelta

import re
from bs4 import BeautifulSoup
from luigi import Task, DateParameter
from tornado.gen import coroutine, Return
from tornado.queues import Queue

from base import DCMTaskParams
from utils import if_not_raise, HttpPool


class PullEarningsTask(DCMTaskParams, Task):
    start_date = DateParameter(description="Start date", default=date(2000, 1, 3))

    def _get_date_from_url(self, url):
        return datetime.strptime(url.split("/")[-1].split(".html")[0], "%Y%m%d").date()

    def _get_url_from_date(self, date_obj):
        return "https://biz.yahoo.com/research/earncal/%s.html" % date_obj.strftime("%Y%m%d")

    def run(self):
        #if self.date != date.today():
        #    raise ValueError("No data for specified date")

        queued_urls = deque((self._get_url_from_date(self.start_date), ))

        parsed_data = []
        seen_urls = set()
        queued_urls = Queue()

        @coroutine
        def _process_response(response):
            print("[%d] Processing %s" % (queued_urls.qsize(), response.effective_url))
            if response.code != 200:
                print("No earnings at %s" % response.effective_url)
                raise Return()

            current_date = self._get_date_from_url(response.effective_url)
            page_soup = BeautifulSoup(response.body, "lxml")

            for link in reversed(page_soup.find_all("a")):
                if not link.has_attr("href"):
                    continue
                if re.match(r"^\d{8}.html$", link["href"]):
                    discovered_url = "https://biz.yahoo.com/research/earncal/%s" % link["href"]
                elif re.match(r"^/research/earncal/\d{8}.html$", link["href"]):
                    discovered_url = "https://biz.yahoo.com%s" % link["href"]
                else:
                    continue

                discovered_date = self._get_date_from_url(discovered_url)

                # Try sibling dates
                for i in range(60):
                    possible_date = discovered_date + timedelta(days=i)
                    possible_url = self._get_url_from_date(possible_date)

                    if possible_url in seen_urls \
                        or possible_date <= self.start_date:
                        continue

                    seen_urls.add(possible_url)
                    yield queued_urls.put(possible_url)

            for table_soup in page_soup("table"):
                if "Symbol" not in str(table_soup) or "Time" not in str(table_soup):
                    continue
                next_row_soup = table_soup("tr")[2]  # Skipping first row of data, i.e data_table headings

                while next_row_soup is not None:
                    next_row_soup = next_row_soup.next_sibling

                    company = if_not_raise(
                        lambda: next_row_soup("td")[0].string.strip().replace("\n", ""), default="")
                    ticker = if_not_raise(
                        lambda: next_row_soup("a")[0].string.strip().replace("\n", "").split(".")[0], default="")
                    time = if_not_raise(
                        lambda: next_row_soup("small")[0].string.strip().replace("\n", ""), default="")

                    if not company and not ticker:
                        continue

                    parsed_data.append((
                        company,
                        ticker,
                        time,
                        current_date.strftime("%Y-%m-%d")
                    ))
                break
            else:
                print("No earnings at %s" % response.effective_url)

        HttpPool(1, _process_response, [self._get_url_from_date(self.start_date)], sleep_time=5).run(queued_urls)

        with self.output().open("w") as output_fd:
            csv_writer = csv.writer(output_fd, delimiter=",", quotechar="\"")
            csv_writer.writerow(["Company", "Ticker", "Time", "Date"])
            for row in parsed_data:
                csv_writer.writerow([column.encode("utf8") for column in row])
