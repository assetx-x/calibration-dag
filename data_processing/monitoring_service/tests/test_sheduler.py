import pytest
import pandas as pd
from monitoring_service.dcm_metric_schedulers import Schedule, MetricScheduler


class TestSchedule():
    @pytest.mark.parametrize("items", [range(x) for x in range(5)])
    def test_schedule(self, mocker, items):
        schedule = Schedule()
        schedule.add(*items)
        mock = mocker.Mock()

        for item in schedule:
            args, kwargs = item
            mock(*args, **kwargs)

        mock.assert_called_with(*items)


def get_date_range(periods):
    now = pd.Timestamp.now()
    timedelta = pd.Timedelta(minutes=now.minute, seconds=now.second)
    return pd.date_range(now, freq=timedelta, periods=periods)


def run(job, kwargs, **kw):
    job(**kwargs)


class TestMetricScheduler():
    def test_run(self, mocker):
        start_mock = mocker.Mock()
        mocker.patch("apscheduler.schedulers.background.BackgroundScheduler.start", start_mock)
        scheduler = MetricScheduler()
        scheduler.run()

        start_mock.assert_called()

    @pytest.mark.parametrize("start_date", get_date_range(2))
    @pytest.mark.parametrize("end_date", get_date_range(2))
    @pytest.mark.parametrize("weeks", range(2))
    @pytest.mark.parametrize("days", range(2))
    @pytest.mark.parametrize("hours", range(2))
    @pytest.mark.parametrize("minutes", range(2))
    @pytest.mark.parametrize("seconds", range(2))
    @pytest.mark.parametrize("business_days_only", [True, False, None])
    def test_schedule(self, mocker, start_date, end_date, weeks, hours, days, minutes, seconds, business_days_only):
        add_job_mock = mocker.Mock(side_effect=run)
        job_mock = mocker.Mock()
        mocker.patch("apscheduler.schedulers.background.BackgroundScheduler.add_job", add_job_mock)
        scheduler = MetricScheduler()
        scheduler.schedule(job_mock, start_date=start_date, end_date=end_date,
                           weeks=weeks, hours=hours, days=days,
                           minutes=minutes, seconds=seconds,
                           business_days_only=business_days_only)

        if scheduler._is_business_day(business_days_only):
            job_mock.assert_called()
        else:
            job_mock.assert_not_called()

    @pytest.mark.parametrize("dates", [get_date_range(x+1) for x in range(5)])
    def test_custom_dates(self, mocker, dates):
        add_job_mock = mocker.Mock(side_effect=run)
        job_mock = mocker.Mock()
        mocker.patch("apscheduler.schedulers.background.BackgroundScheduler.add_job", add_job_mock)
        scheduler = MetricScheduler()
        scheduler.schedule(job_mock, custom_dates=dates)

        job_mock.assert_called()

    def test_attribute_error(self, mocker):
        mock = mocker.Mock()
        mocker.patch("apscheduler.schedulers.background.BackgroundScheduler.add_job", mock)
        scheduler = MetricScheduler()

        with pytest.raises(AttributeError):
            scheduler.schedule(mock, custom_dates=[pd.Timestamp.now], days=1)
        mock.assert_not_called()
