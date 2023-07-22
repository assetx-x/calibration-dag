import pandas as pd
from tornado.queues import Queue
from datetime import timedelta

from bs4 import BeautifulSoup
from tornado.gen import coroutine, Return

from utils import HttpPool, if_not_raise

url_format = "http://finance.yahoo.com/calendar/earnings?day={day}&size=100&offset=0"


def pull_yahoo_earnings(start_date, end_date):
    current_date = start_date
    queued_urls = Queue()
    while current_date != end_date:
        url = url_format.format(day=current_date.strftime("%Y-%m-%d"))
        queued_urls.put_nowait(url)
        current_date += timedelta(days=1)

    parsed_data = []
    yahoo_error_responses = []

    def _process_header(header_xml):
        ticker_index, company_index, time_index = None, None, None
        for index, header_field in enumerate(header_xml("th")):
            field_name = header_field.text.strip()
            if field_name == "Symbol":
                ticker_index = index
            elif field_name == "Company":
                company_index = index
            elif "Time" in field_name:
                time_index = index
        if ticker_index is None or company_index is None or time_index is None:
            raise RuntimeError("Failed to parse Yahoo earnings table header. Possibly format has changed.")
        return ticker_index, company_index, time_index

    @coroutine
    def _process_response(response):
        print("[%d] Processing %s" % (queued_urls.qsize(), response.effective_url))
        if response.code != 200:
            print("No earnings at %s" % response.effective_url)
            print("response error return code: {}".format(response.code))
            yahoo_error_responses.append((response.effective_url, response.code))
            raise Return()

        base_url, url_params = _get_params_from_url(response.effective_url)
        page_soup = BeautifulSoup(response.body, "lxml")

        for table_soup in page_soup("table"):
            if "Symbol" not in str(table_soup) or "Time" not in str(table_soup):
                continue

            ticker_index, company_index, time_index = _process_header(table_soup("tr")[0])
            next_row_soup = table_soup("tr")[1]  # Skip header, grab first data row

            num_rows = 0
            while next_row_soup is not None:
                num_rows += 1
                ticker = if_not_raise(lambda: next_row_soup("td")[ticker_index].text.strip(), default="")
                company = if_not_raise(lambda: next_row_soup("td")[company_index].text.strip(), default="")
                time = if_not_raise(lambda: next_row_soup("td")[time_index].text.strip(), default="")
                next_row_soup = next_row_soup.next_sibling

                if not company and not ticker:
                    continue

                parsed_data.append((company, ticker, time, url_params['day']))

            if num_rows >= url_params['size']:
                url_params['offset'] = url_params['offset'] + url_params['size']
                next_url = "{base_url}?day={day}&size={size}&offset={offset}".format(base_url=base_url, **url_params)
                queued_urls.put_nowait(next_url)

            break
        else:
            print("No earnings at %s" % response.effective_url)
            print("response error code: {}".format(response.code))

    # Yahoo blocks the API call after 30-40 attempts and does not pull data reliably.
    # Reduced number of worker to 1 and add sleep_time of 5sec between each call. Post
    # this change the pipeline runs in <20 mins as opposed to a few minutes
    HttpPool(1, _process_response, [], sleep_time=5).run(queued_urls)

    print("Yahoo earnings API error responses count: {}".format(len(yahoo_error_responses)))
    print("Yahoo earnings error urls: {}".format(yahoo_error_responses))

    result = []
    for row in parsed_data:
        result.append({
            "Company": row[0],
            "Ticker": row[1],
            "Time": row[2],
            "Date": row[3]
        })

    return pd.DataFrame(result)


def _get_params_from_url(url):
    url, url_params = url.split('?')
    url_params = dict(tuple(param.split('=')) for param in url_params.split('&'))
    return url, {
        'day': url_params['day'],
        'size': int(url_params['size']),
        'offset': int(url_params['offset'])
    }
