import logging.config
import os

from pyhocon import ConfigFactory

file_dir = os.path.dirname(os.path.realpath(__file__))
logging_conf_path = os.path.join(file_dir, "conf", "logging.conf")
logging_conf = ConfigFactory.parse_file(logging_conf_path)
logging.config.dictConfig(logging_conf)
