import os

import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from pyhocon import ConfigFactory

from commonlib.market_timeline import marketTimeline

_config_dir = os.path.join(os.path.dirname(__file__), "config")
app_config = ConfigFactory.parse_file(os.path.join(_config_dir, "app.conf"))


class MetricScheduler(object):
    def __init__(self):
        self._timezone = app_config["timezone"]
        self._scheduler = BackgroundScheduler(timezone=self._timezone)

    @staticmethod
    def _filter_and_run(callable_fn, filter_fn, *args, **kwargs):
        if filter_fn(*args, **kwargs):
            callable_fn()

    def _is_business_day(self, business_days_only):
        if not business_days_only:
            return True
        return marketTimeline.isTradingDay(pd.Timestamp.now(self._timezone))

    @staticmethod
    def _py_dt(some_dt):
        return some_dt.to_pydatetime() if isinstance(some_dt, pd.Timestamp) else some_dt

    def schedule(self, metric, start_date=None, end_date=None, weeks=0, days=0, hours=0, minutes=0, seconds=0,
                 custom_dates=None, business_days_only=False):
        function_kwargs = {"callable_fn": metric,
                           "filter_fn": self._is_business_day,
                           "business_days_only": business_days_only}

        if hasattr(custom_dates, "__iter__"):
            if start_date or end_date or weeks or days or hours or minutes or seconds:
                raise AttributeError("Only one of custom_dates or the other parameters should be provided")

            for date in custom_dates:
                self._scheduler.add_job(self._filter_and_run, trigger="date", run_date=self._py_dt(date),
                                        kwargs=function_kwargs)
        else:
            self._scheduler.add_job(self._filter_and_run, trigger="interval", start_date=self._py_dt(start_date),
                                    end_date=self._py_dt(end_date), weeks=weeks, days=days, hours=hours,
                                    minutes=minutes, seconds=seconds, kwargs=function_kwargs)

    def run(self):
        self._scheduler.print_jobs()
        self._scheduler.start()

    def register_metrics(self, metrics, schedule):
        for metric in metrics:
            for item in schedule:
                args, kwargs = item
                self.schedule(metric, *args, **kwargs)


class Schedule:
    def __init__(self):
        self._schedule_list = []

    def __iter__(self):
        for item in self._schedule_list:
            yield item

    def add(self, *args, **kwargs):
        self._schedule_list.append((args, kwargs))
