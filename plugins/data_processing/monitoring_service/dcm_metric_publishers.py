import logging
import numbers
import os
import pandas as pd

import datadog
from pyhocon import ConfigFactory
from slack import WebClient

from monitoring_service.metrics.dcm_metric import Goodness, get_redshift_engine

_config_dir = os.path.join(os.path.dirname(__file__), "config")
publishers_config = ConfigFactory.parse_file(os.path.join(_config_dir, "publishers.conf"))

_logger = logging.getLogger("monitoring.publishers")


class MetricPublisher(object):
    """ Parent class of all publishers, objects that can display metrics on some specific media"""

    def __init__(self, publisher_name):
        self._name = publisher_name

    def __call__(self, *args, **kwargs):
        return self.publish(*args, **kwargs)

    def get_name(self):
        return self._name

    def publish(self, metrics, group_name=None, goodness=None):
        raise NotImplementedError()

    def publish_alert(self, alert_title, alert_header, alert_text):
        pass


def format_dict(d, depth=0, bold=str):
    text = "\n"
    for key, value in d.items():
        if isinstance(value, dict):
            value = format_dict(value, depth + 1, bold)
        text += "\t\t" * depth + bold(key) + ": " + str(value) + "\n"

    return text


class SlackMetricPublisher(MetricPublisher):
    COLORS = publishers_config["slack"]["colors"]

    @staticmethod
    def bold(text):
        return "*" + str(text) + "*" if text else ""

    @staticmethod
    def italic(text):
        return "_" + str(text) + "_" if text else ""

    def __init__(self, name, channel, alert_channel="#alerts", token=None, bot_id=None):
        super(SlackMetricPublisher, self).__init__(name)
        self._app_name = publishers_config["slack"]["app_name"]
        self._channel = channel
        self._client = WebClient(token=token or publishers_config["slack"]["token"])
        channels = self._client.conversations_list(types="private_channel,public_channel").get("channels")
        channels += self._client.users_list().get("members")
        #_logger.info("Discovered the following channels:\n    %s",
                     #"\n    ".join(["%s: #%s" % (c.get("id"), c.get("name")) for c in channels]))
        self._channel_id = next(c.get("id") for c in channels if c.get("name") == self._channel[1:])
        self._alert_channel_id = next(c.get("id") for c in channels if c.get("name") == alert_channel[1:])
        self._latest_time_read = self._get_epoch()
        self.bot_id = bot_id or "B01CDC0CW9H"

    def _get_epoch(self):
        return pd.Timestamp.now("UTC").asm8.astype(pd.np.int64)/1000000000

    def _get_fields(self, metric):
        value = metric.get("value")

        if isinstance(value, dict):
            value = value.items()
        elif not isinstance(value, list):
            return str(value)

        fields = []
        long_fields = []
        for key, value in value:
            if isinstance(value, dict):
                value = format_dict(value, bold=self.bold)
            elif isinstance(value, numbers.Number):
                value = "%g" % value
            if len(value) > 30:
                long_fields.append({"title": key, "value": value})
            else:
                fields.append({"title": key, "value": value, "short": "true"})

        return fields + long_fields

    def _get_template(self, metric, goodness=None):
        color = self.COLORS["default"]
        if goodness == Goodness.GOOD:
            color = self.COLORS["good"]
        elif goodness == Goodness.WARNING:
            color = self.COLORS["warning"]
        elif goodness == Goodness.ERROR:
            color = self.COLORS["error"]

        metric_name = metric.get("metric")
        return {
            "color": color,
            "pretext": self.bold(metric_name),
            "fields": self._get_fields(metric),
            "mrkdwn_in": ["text", "pretext", "fields"]
        }

    def publish(self, metrics, group_name=None, goodness=None):
        attachments = []

        if isinstance(metrics, list):
            for metric in metrics:
                attachments.append(self._get_template(metric, goodness=goodness))
        else:
            attachments.append(self._get_template(metrics, goodness=goodness))

        result = self._client.chat_postMessage(username=self._app_name, channel=self._channel_id,
                                               text=self.bold(self.italic(group_name)), attachments=attachments)
        return result

    def _report_error(self, message):
        self._client.chat_postMessage(username=self._app_name, channel=self._channel_id, text=message)

    def _get_command_handler(self, requested_command):
        handlers = {
            'update': self.update_metrics_on_demand
        }

        command_prefix = requested_command.lower()
        matching_commands = [cmd for cmd in sorted(handlers.keys()) if cmd.startswith(command_prefix)]
        if len(matching_commands) == 1:
            return handlers[matching_commands[0]]

        if len(matching_commands) == 0:
            self._report_error("Unknown command: %s" % requested_command)
        else:
            self._report_error("Ambiguous command: %s\n"
                               "Possible matches: %s" % (requested_command, ' '.join(matching_commands)))
        return None

    def check_update_requests(self, metrics):
        new_epoch = self._get_epoch()
        read_result = self._client.conversations_history(channel=self._channel_id, oldest=self._latest_time_read, latest=new_epoch - 0.1,
                                                         inclusive=True).get("messages")
        for item in read_result:
            item_type, channel, text = item.get("type"), item.get("channel"), item.get("text")
            _logger.info("Received Slack %s on channel %s: %s", item_type, channel, text)
            self_message = 'bot_id' in item and item.get("bot_id") == (self.bot_id or 'B01CDC0CW9H')
            if item_type == "message" and not self_message:
                tokens = text.strip().split(' ')
                requested_command = tokens[0].strip()
                if requested_command:
                    handler = self._get_command_handler(requested_command)
                    if handler:
                        handler(metrics, tokens)
        self._latest_time_read = new_epoch

    def get_acct_linkuid_mapping(self, acct_no_list):
        engine = get_redshift_engine()
        #Remove duplicates
        acct_no_list = list(set(acct_no_list))
        account_nos = str(acct_no_list)[1:-1]
        text_sql_query = f"SELECT account_id, link_uid FROM subscriptions_client_brokers WHERE account_id IN ({account_nos}) AND link_confirmed=True AND is_active=True"
        #print(text_sql_query)
        result = engine.execute(text_sql_query).fetchall()
        result_dict = {acct_id:link_uid for acct_id,link_uid in result}
        #print(result_dict)
        return result_dict

    def check_trade_control_requests(self):
        #new_epoch = self._get_epoch()
        new_epoch = pd.Timestamp.now('US/Eastern').asm8.astype(pd.np.int64)/1000000000
        oldest_ts = pd.Timestamp.now('US/Eastern').normalize().asm8.astype(pd.np.int64)/1000000000
        read_result = self._client.conversations_history(channel=self._channel_id, oldest=oldest_ts, latest=new_epoch - 0.1,
                                                         inclusive=True).get("messages")
        ignore_list_dict = {}
        ignore_list_present = False
        for item in read_result:
            if item['text'].startswith('IGNORE_LIST'):
                #print(f"IGNORE_LIST available at {item['ts']}")
                ignore_list_str= item['text'].split('=')[1].replace(' ','')
                ignore_list_ts = eval(item['ts'])
                ignore_list_dict.update({ignore_list_ts:ignore_list_str})
                ignore_list_present = True
        if ignore_list_present==True:
            ignore_list_str = ignore_list_dict[max(list(ignore_list_dict.keys()))]
            ignore_list = ignore_list_str.split(']')[0].split('[')[1].split(',')
            acct_strategy_ignore_dict = {}
            for acctno_strategy in ignore_list:
                #print(acctno_strategy.split(':'))
                acct_strategy_ignore_dict.update({acctno_strategy.split(':')[0]:acctno_strategy.split(':')[1]})
            #print(list(acct_strategy_ignore_dict.keys()))
            acct_linkuid_mapping = self.get_acct_linkuid_mapping(list(acct_strategy_ignore_dict.keys()))
            #ignored_traders_list = [acct_linkuid_mapping[k]+':'+v for k,v in acct_strategy_ignore_dict.items()]
            ignored_traders_list = [acct_linkuid_mapping[i.split(':')[0]]+':'+i.split(':')[1] for i in ignore_list]
            return ignored_traders_list

    def update_metrics_on_demand(self, metrics, request):
        requested_metrics = request[1:]
        if requested_metrics:
            selected_metrics = filter(lambda m: m.filter(requested_metrics), metrics)
        else:
            selected_metrics = metrics
        if selected_metrics:
            for metric in selected_metrics:
                metric()
        else:
            self._report_error("Couldn't match any requested metrics: %s" % ' '.join(requested_metrics))

    def publish_alert(self, alert_title, alert_header, alert_text):
        self.publish_alert_to_channel(alert_title, alert_header, alert_text, self._channel_id)
        self.publish_alert_to_channel(alert_title, alert_header, alert_text, self._alert_channel_id)

    def publish_alert_to_channel(self, alert_title, alert_header, alert_text, channel):
        result = self._client.chat_postMessage(
            username=self._app_name,
            channel=channel,
            text=self.bold(alert_title),
            attachments=[{
                "color": "danger",
                "fields": [{"title": alert_header, "value": alert_text}],
                "mrkdwn_in": ["text", "pretext"]
            }]
        )
        return result


def format_name(name):
    return str(name).lower().replace(" ", "_")


class DataDogPublisher(MetricPublisher):
    def __init__(self, publisher_name):
        super(DataDogPublisher, self).__init__(publisher_name)
        self._api_key = publishers_config["data_dog"]["api_key"]
        self._app_key = publishers_config["data_dog"]["app_key"]
        datadog.initialize(self._api_key, self._app_key)

    def _send_metric(self, metric_name, value):
        if type(value) == dict:
            for key in value:
                self._send_metric("%s.%s" % (metric_name, format_name(key)), value[key])
        else:
            datadog.api.Metric.send(metric=format_name(metric_name), points=value, type="gauge")

    def publish(self, metrics, group_name=None, goodness=None):
        if type(metrics) == list:
            for metric in metrics:
                self._send_metric(metric.get("metric"), metric.get("value"))
        else:
            self._send_metric(metrics.get("metric"), metrics.get("value"))


def main():
    from datetime import datetime
    #pub = SlackMetricPublisher("test_metric_publisher", "#boris", alert_channel="#test_alerts")
    #pub.publish({
        #"metric": "Test Metric 1",
        #"updated_at": datetime.now(),
        #"value": [
            #("Short value 1", "1234"),
            #("Short value 2", "String Value"),
            #("Short value 3", "Test"),
            #("Long value 1", "datadog.api.Metric.send(metric=format_name(metric_name), points=value, type=\"gauge\")"),
            #("Long value 2", "NAME " * 15),
        #]
    #}, goodness=Goodness.GOOD)
    token = publishers_config['slack']['token']
    bot_id = publishers_config['slack']['bot_id']
    channel = '#' + publishers_config['slack']['channel']

    slack_publisher = SlackMetricPublisher("StandardSlack", channel=channel, \
                                                                 alert_channel=channel, token=token, bot_id=bot_id)
    print(slack_publisher.check_trade_control_requests())


if __name__ == '__main__':
    main()
