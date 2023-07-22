import os
import requests
from datetime import datetime
from pyhocon import ConfigFactory
from kafka import KafkaConsumer, TopicPartition

_config_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), "config"))
metrics_config = ConfigFactory.parse_file(os.path.join(_config_dir, "metrics.conf"))
app_config = ConfigFactory.parse_file(os.path.join(_config_dir,"app.conf"))

class JenkinsStatusReader():
    def __init__(self):
        self.maintenance_task_url=metrics_config["jenkins"]["maintenance_tasks_url"]
        self.anaytics_calculations_url=metrics_config["jenkins"]["analytics_calculations_url"]

    def read_maintenance_tasks_status(self):
        username = metrics_config["jenkins"]["username"]
        access_token = metrics_config["jenkins"]["access_token"]
        url = self.maintenance_task_url
        req_handler_job=requests.get(url, auth=(username,access_token))
        json_data = req_handler_job.json()
        result_dict = {}
        job_name = json_data['name']
        result_dict[job_name] = {}
        for sub_job in json_data['jobs']:
            sub_job_name = sub_job['name']
            if sub_job_name == 'deploy_ticker_universe':
                continue
            sub_job_url = sub_job['url']+"lastSuccessfulBuild/api/json"
            sub_job_req_handler=requests.get(sub_job_url, auth=(username,access_token))
            sub_job_json_data = sub_job_req_handler.json()
            result_dict[job_name][sub_job_name] = {}
            result_dict[job_name][sub_job_name]['build_number'] = sub_job_json_data['number']
            result_dict[job_name][sub_job_name]['timestamp'] = sub_job_json_data['timestamp']
            result_dict[job_name][sub_job_name]['build_status'] = sub_job_json_data['result']
        return result_dict

    def read_analytics_calculations_status(self):
        username = metrics_config["jenkins"]["username"]
        access_token = metrics_config["jenkins"]["access_token"]
        url = self.anaytics_calculations_url
        req_handler_job=requests.get(url, auth=(username,access_token))
        json_data = req_handler_job.json()
        result_dict = {}
        job_name = json_data['name']
        result_dict[job_name] = {}
        for sub_job in json_data['jobs']:
            sub_job_name = sub_job['name']
            sub_job_url = sub_job['url']+"lastSuccessfulBuild/api/json"
            sub_job_req_handler=requests.get(sub_job_url, auth=(username,access_token))
            sub_job_json_data = sub_job_req_handler.json()
            result_dict[job_name][sub_job_name] = {}
            result_dict[job_name][sub_job_name]['build_number'] = sub_job_json_data['number']
            result_dict[job_name][sub_job_name]['timestamp'] = sub_job_json_data['timestamp']
            result_dict[job_name][sub_job_name]['build_status'] = sub_job_json_data['result']
        return result_dict

    def read_jenkins_status(self):
        maintenance_task_stat_dict = self.read_maintenance_tasks_status()
        analytics_calculations_stat_dict = self.read_analytics_calculations_status()
        result_dict = maintenance_task_stat_dict
        result_dict.update(analytics_calculations_stat_dict)
        return result_dict

class KafkaStatusReader():
    def __init__(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers=app_config["kafka"]["bootstrap_servers"],
            ssl_cafile=app_config["kafka"]["ssl_cafile"],
            ssl_certfile=app_config["kafka"]["ssl_certfile"],
            ssl_keyfile=app_config["kafka"]["ssl_keyfile"],
            security_protocol="SSL",
            client_id="status_reader",
            group_id="status_reader",
            enable_auto_commit=False
        )

    def read_last_n_messages(self,topic_list,msg_count):
        result_dict = {}
        for topic in topic_list:
            topic_partition = TopicPartition(topic, 0)
            self.consumer.assign([topic_partition])
            current_offset = self.consumer.position(topic_partition)
            if current_offset == 0:
                continue
            if current_offset < msg_count:
                msg_count = current_offset
            self.consumer.seek(topic_partition,current_offset-msg_count)
            partitions = self.consumer.poll(timeout_ms=1000, max_records=msg_count)
            if not any(partitions):
                continue
            message_block = [record for partition in partitions.values() for record in partition]
            result_dict[topic] = {}
            for message in message_block:
                result_dict[topic][message.offset] = {}
                result_dict[topic][message.offset]['identity'] = topic
                result_dict[topic][message.offset]['message_type'] = "Trading Operations"
                result_dict[topic][message.offset]['timestamp'] = message.timestamp
        return result_dict

if __name__=="__main__":
    jenkins_status_reader = JenkinsStatusReader()
    jenkins_dict = jenkins_status_reader.read_jenkins_status()
    kafka_status_reader = KafkaStatusReader()
    topic_list = ["gan_growth_long_30","gan_growth_short_30"]
    kafka_dict = kafka_status_reader.read_last_n_messages(topic_list,5)
    merged_dict = {}
    merged_dict['build_stats'] = jenkins_dict
    merged_dict['msg_stats'] = kafka_dict
