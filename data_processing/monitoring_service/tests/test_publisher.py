import logging

import pytest
import mock
import slack
import pandas as pd
import os

from slack import WebClient
from pyhocon import ConfigFactory
from monitoring_service.dcm_metric_publishers import MetricPublisher, SlackMetricPublisher
from monitoring_service.metrics.dcm_metric import Goodness


logging.disable(logging.CRITICAL)


TEST_CHANNEL = "#test_channel"
TEST_CHANNEL_ID = "test_channel_id"
TEST_ALERT_CHANNEL = "#test_alert_channel"
TEST_ALERT_CHANNEL_ID = "test_alert_channel_id"

DIR = os.path.dirname(os.path.realpath(__file__)) + "/"
CONFIG = ConfigFactory.parse_file(DIR + "../config/publishers.conf")


class DummyPublisher(MetricPublisher):
    def __init__(self, name="Dummy publisher"):
        super(DummyPublisher, self).__init__(name)
        self.publish = mock.Mock()

    def publish(self, metrics, group_name=None, goodness=None):
        pass


class TestPublisher(object):
    def test_empty_constructor(self):
        with pytest.raises(TypeError):
            MetricPublisher()

    def test_not_implemented(self):
        publisher = MetricPublisher("Not implemented")
        with pytest.raises(NotImplementedError):
            publisher.publish(mock.Mock())

    @pytest.mark.parametrize("name", ["", "Test name"])
    def test_get_name(self, name):
        publisher = DummyPublisher(name=name)
        assert publisher.get_name() == name

    @pytest.mark.parametrize("dummy_value", range(5))
    @pytest.mark.parametrize("dummy_group", range(5))
    def test_publisher_call(self, dummy_value, dummy_group):
        publisher = DummyPublisher()
        publisher(dummy_value, group_name=dummy_group)
        publisher.publish.assert_called_with(dummy_value, group_name=dummy_group)


class DummySlackPublisher(SlackMetricPublisher):
    def __init__(self, name="Dummy slack publisher", channel=TEST_CHANNEL, alert_channel=TEST_ALERT_CHANNEL):
        super(DummySlackPublisher, self).__init__(name, channel, alert_channel)
        self.publish = mock.Mock(side_effect=super(DummySlackPublisher, self).publish)
        self._channel_id = TEST_CHANNEL_ID
        self._alert_channel_id = TEST_ALERT_CHANNEL_ID


def mock_slack(mocker, *args, **kwargs):
    keys = {"username", "text", "attachments", "channel"}
    assert keys.intersection(set(kwargs.keys())) == keys
    assert kwargs.get("username") == CONFIG["slack"]["username"]
    assert kwargs.get("channel") == TEST_CHANNEL_ID
    attachments = kwargs.get("attachments")
    assert type(attachments) == list
    assert len(attachments) > 0


dummy_metric_values = [
    {'metric': 'Equity Adjustment Process',
     'updated_at': pd.Timestamp('2017-02-09 15:02:50.371801+0000', tz='UTC'),
     'value': {
         'Failed percentage': 1.9,
         'Adjusted count': 1137,
         'Failed count': 22,
         'Adjusted percentage': 98.1
        }
     },
    {'metric': 'Ravenpack Health Status',
     'updated_at': pd.Timestamp('2017-02-09 10:02:55.072675-0500', tz='UTC'),
     'value': {
         'Health Status': 'Healthy',
         'Latest Ravenpack Timestamp': '2017-02-09 10:02:28'
        }
     },
    [{'metric': 'Index Adjustment Process',
      'updated_at': pd.Timestamp('2017-02-09 15:03:00.789069+0000', tz='UTC'),
      'value': '9379992'
      },
     {'metric': 'Futures Adjustment Process',
      'updated_at': pd.Timestamp('2017-02-09 15:04:00.789069+0000', tz='UTC'),
      'value': {
          'Expected date': '2017-02-08',
          'Failed percentage': {
              'Subvalue1': 11,
              'Subvalue2': {
                  'Subvalue3': '------'
              }
          }
        }
      }
     ]
]


class TestSlackPublisher(TestPublisher):
    def test_constructor(self):
            WebClient.__init__ = mock.Mock(return_value=None)
            DummySlackPublisher()
            WebClient.__init__.assert_called()

    @pytest.mark.parametrize("metric", dummy_metric_values)
    @pytest.mark.parametrize("name", [None, "", "Test name"])
    @pytest.mark.parametrize("goodness", list(Goodness))
    def test_publish(self, mocker, metric, name, goodness):
        slack.WebClient.api_call = mocker.Mock(side_effect=mock_slack)
        publisher = DummySlackPublisher()
        publisher.publish(metric, group_name=name, goodness=goodness)
        publisher.publish.assert_called_with(metric, group_name=name, goodness=goodness)

        _, call_kwargs = slack.WebClient.api_call.call_args

        color = call_kwargs["attachments"][0].get("color")

        if goodness == Goodness.NONE:
            assert color == CONFIG["slack"]["colors"]["default"]
        elif goodness == Goodness.GOOD:
            assert color == CONFIG["slack"]["colors"]["good"]
        elif goodness == Goodness.WARNING:
            assert color == CONFIG["slack"]["colors"]["warning"]
        elif goodness == Goodness.ERROR:
            assert color == CONFIG["slack"]["colors"]["error"]
