import os
import argparse
import six
import json
import pandas as pd
from flask import Flask, jsonify
from flask_restful import Resource, Api
from flask_cors import CORS
from pyhocon import ConfigFactory

import monitoring_service.dcm_metric_publishers as dcm_metric_publishers
from monitoring_service.metrics.dcm_metric import get_postgres_engine
from monitoring_service.status_reader import JenkinsStatusReader, KafkaStatusReader
from commonlib.resource_with_error_reporting import ResourceWithErrorReporting

_config_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), "../config"))
app_config = ConfigFactory.parse_file(os.path.join(_config_dir,"app.conf"))

app = Flask(__name__)
CORS(app)
api = Api(app)

DEBUG_FLAG = False

ResourceClass = Resource if DEBUG_FLAG else ResourceWithErrorReporting
__MONITORING_SERVICE_PORT = 9091
__MONITORING_SERVICE_IP = "0.0.0.0"

def set_monitoring_service_ip(ip):
    global __MONITORING_SERVICE_IP
    __MONITORING_SERVICE_IP = ip

def get_monitoring_service_ip():
    global __MONITORING_SERVICE_IP
    return __MONITORING_SERVICE_IP

def set_monitoring_service_port(port):
    global __MONITORING_SERVICE_PORT
    __MONITORING_SERVICE_PORT = port

def get_monitoring_service_port():
    global __MONITORING_SERVICE_PORT
    return __MONITORING_SERVICE_PORT


class Status(ResourceClass):
    def get(self):
        jenkins_status_reader = JenkinsStatusReader()
        jenkins_dict = jenkins_status_reader.read_jenkins_status()
        
        kafka_dict = {}
        kafka_status_reader = KafkaStatusReader()
        topic_list = [
            "gan_growth_short_30",
            "gan_growth_long_30",
            "gan_growth_long_short_30",
            "gan_value_long_short_30",
            "gan_value_long_30",
            "gan_value_short_30"
        ]
        topic_prefix = app_config["kafka"]["MODEL_TOPIC_PREFIX"]
        topic_list = [ "{}{}".format(topic_prefix,topic) for topic in topic_list ]
        kafka_dict = kafka_status_reader.read_last_n_messages(topic_list,6)
        
        merged_dict = {}
        merged_dict['build_stats'] = jenkins_dict
        merged_dict['msg_stats'] = kafka_dict        
        return jsonify(merged_dict)

api.add_resource(Status, '/monitoring_service/status')

def get_arg_parser():
    parser = argparse.ArgumentParser(description="\n\nData Capital Management Monitoring API\n\n")
    parser.add_argument("-i", "--ip", default=__MONITORING_SERVICE_IP, help="IP where the service must run [default: %(default)s]")
    parser.add_argument("-p", "--port", default=__MONITORING_SERVICE_PORT, help="Port where the service must run [default: %(default)s]")
    return parser

def run_app():
    app.run(host=get_monitoring_service_ip(),port=get_monitoring_service_port(), debug=DEBUG_FLAG, use_reloader=not DEBUG_FLAG)


if __name__=="__main__":
    parser = get_arg_parser()
    args = parser.parse_args()
    set_monitoring_service_ip(args.ip)
    set_monitoring_service_port(args.port)
    run_app()

