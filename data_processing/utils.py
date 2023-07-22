import traceback
import time

from tornado.gen import coroutine
from tornado.ioloop import IOLoop
from base import credentials_conf
from sqlalchemy import create_engine
from tornado.httpclient import AsyncHTTPClient

ticker_name_mapping = None


def if_not_raise(func, default=None):
    try:
        return func()
    except Exception:
        return default


def translate_ticker_name(value, from_system, to_system):
    assert to_system in ("unified", "ib", "yahoo", "tickdata", "capiq")
    assert from_system in ("unified", "ib", "yahoo", "tickdata", "capiq")

    global ticker_name_mapping
    if ticker_name_mapping is None:
        engine = create_engine(
            'bigquery://{project_id}/{default_dataset}'.format(
                **credentials_conf['gcp']))
        try:
            ticker_name_mapping = [row for row in engine.execute("SELECT * FROM ticker_mapping")]
        finally:
            engine.dispose()

    for row in ticker_name_mapping:
        if row[from_system] == value:
            return row[to_system]

    return value


class HttpPool(object):
    def __init__(self, count, worker_func, initial_urls, sleep_time=0):
        self.count = count
        self.worker_func = worker_func
        self.initial_urls = initial_urls
        self.sleep_time = sleep_time

    @coroutine
    def _worker(self, queue):
        http_client = AsyncHTTPClient()
        while True:
            time.sleep(self.sleep_time)
            url = yield queue.get()
            response = yield http_client.fetch(url, raise_error=False)

            try:
                yield self.worker_func(response)
            except Exception:
                traceback.print_exc()
                IOLoop.current().stop()
                return

            queue.task_done()

    def run(self, queue):
        @coroutine
        def _coroutine():
            for url in self.initial_urls:
                print(url)
                yield queue.put(url)

            for i in range(self.count):
                IOLoop.current().spawn_callback(self._worker, queue)

            yield queue.join()

        IOLoop.current().run_sync(_coroutine)


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]
