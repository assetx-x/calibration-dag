#!/usr/bin/env python
# coding:utf-8
"""
  Author:  Chris Ingrassia --<c.ingrassia@datacapitalmanagement.com>
  Purpose: Intuition Configuration System
  Created: 7/16/2015
"""

__author__ = "Chris Ingrassia, Ali Nazari, Jack Kim, Karina Uribe"
__copyright__ = "Copyright (C) 2016, The Data Capital Management Intuition project - All Rights Reserved"
__credits__ = ["Chris Ingrassia", "Napoleon Hernandez", "Ali Nazari", "Jack Kim",
               "Karina Uribe"]
__license__ = "Proprietary and confidential. Unauthorized copying of this file, via any medium is strictly prohibited"
__version__ = "1.0.0"
__maintainer__ = "Chris Ingrassia"
__email__ = "it@datacapitalmanagement.com"
__status__ = "Development"

import os
from commonlib.intuition_base_objects  import DCMException
from commonlib.util_functions import create_directory_if_does_not_exists
from pyhocon import ConfigFactory, ConfigTree, ConfigParser
from pyhocon.exceptions import ConfigMissingException
import logging.config
from contextlib import closing
from collections import Mapping
from commonlib.custom_logging_filters import *
from commonlib.intuition_loggers import log

from functools import reduce

__config = None

def get_config():
    global __config
    if __config:
        return __config
    else:
        return load()

def set_config(conf):
    global __config
    __config = conf

def _load_and_configure_logging(context=None):
    if 'logging' in get_config():
        if "config_location" in get_config()["logging"] and "config_filename" in get_config()["logging"]:
            logging_config_path = os.path.realpath(os.path.join(get_config()["logging"]["config_location"],
                                                                get_config()["logging"]["config_filename"]))

            # read config; need to add get_config() to the context for it to work
            context["get_config"] = get_config
            logging_config = _load_conf(logging_config_path, "logging", context)
            del context["get_config"]

            # if succesful, configure logging
            if logging_config:
                logging.config.dictConfig(logging_config)
                return logging_config
        else:
            logging.warn("There was a problem configuring logging during configuration loading. The section exists on"
                         "the configuration but is missing one of the keys 'config_location' or 'config_filename'. "
                         "Revise your configuration files.")

def _create_context_for_loading_config_files():
    import pandas as pd
    import pytz
    from datetime import timedelta, datetime, time
    from os import path, environ
    context = {"pd":pd, "pytz":pytz, "timedelta":timedelta, "datetime":datetime, "time":time, "path":path,
               "AddAsOfDateFilter": AddAsOfDateFilter, "__file__":__file__, "environ": environ,
               "AddPickleAndPrettyPrintDataFilter":AddPickleAndPrettyPrintDataFilter}
    return context

def load(defaults=None, app_config=None, user_config=None, context=None, load_and_setup_logging=True):
    global __config
    context = _create_context_for_loading_config_files() if not context else context
    configs = [_f for _f in [_load_conf(defaults or default_config_path(), "default", context),
                            _load_conf(app_config or app_config_path(), "app", context),
                            _load_conf(user_config or user_config_path(), "user", context)
                            ] if _f]
    if not any(configs):
        raise DCMException("No valid configurations found in any of {defaults}, {app], {user}".format(defaults=defaults,
                                                                                                      app=app_config,
                                                                                                      user=user_config))
    conf = reduce(_merge_configs, configs)
    logging.info("%d items loaded into config", len(conf))
    c = ConfigParser.resolve_substitutions(conf)
    __config = c
    if load_and_setup_logging:
        logging_dir = conf["logging"]["logging_location"]
        create_directory_if_does_not_exists(logging_dir)
        logging_config = _load_and_configure_logging(context)
        __config["logging"]["__logging_config"] = logging_config
    return __config

def intuition_home():
    "Returns the current intuition home directory (INTUITION_HOME environment variable)"
    #default = os.path.realpath(os.path.join(os.path.dirname(__file__),"..","..","..", "strategies","cointegration"))
    default = os.path.realpath(os.path.join(os.path.dirname(__file__), "configs"))
    return os.environ.get('INTUITION_HOME', default)

def default_config_path():
    """
    Get the default configuration file path.  Defaults to INTUITION_HOME/defaults.conf.
    Can be overridden by setting the environment variable INTUITION_DEFAULTS
    :return:
    The full path to the configuration file
    """
    return os.environ.get('INTUITION_DEFAULTS', os.path.join(intuition_home(), "defaults.conf"))

def app_config_path():
    """
    Get the application configuration file path.  Defaults to INTUITION_HOME/intuition.conf.
    Can be overridden by setting the environment variable INTUITION_CONFIG
    :return:
    The full path to the configuration file
    """
    return os.environ.get('INTUITION_CONFIG', os.path.join(intuition_home(), "intuition.conf"))

def user_config_path():
    """
    Get the application configuration file path.  Defaults to ~/.intuition.conf
    Can be overridden by setting the environment variable INTUITION_USER_CONFIG
    :return:
    The full path to the configuration file
    """
    return os.environ.get('INTUITION_USER_CONFIG', os.path.expanduser(os.path.join("~", ".intuition.conf")))

def _load_conf(path, configuration_type_str, context=None):
    if path == "None":
        logging.warn("CONFIG: Received none as path for loading '{0}' configuration, skipping".format(configuration_type_str))
        return None
    if context:
        context["__file__"] = path
    conf = _load_python_conf(path, context) or _load_hocon_conf(path)
    if context:
        context.pop("__file__",None)
    if conf is None:
        logging.warn("CONFIG : Could not load '{0}' config from {1} using any known method".format(configuration_type_str,
                                                                                                 path))
    else:
        logging.warn("CONFIG : Succesfully loaded '{0}' config from {1}".format(configuration_type_str, path))
    return conf


def _load_python_conf(filepath, context=None):
    try:
        with closing(open(filepath, 'r')) as f:
            return ConfigFactory.from_dict(eval(f.read(), context))
    except Exception as e:
        logging.error("Could not load config from %s as python dict: %s", filepath, e)
    return None


def _load_hocon_conf(path):
    try:
        # Don't resolve substitution references in this file since they might depend on things
        # loaded in previous configs that aren't available until we merge
        return ConfigFactory.parse_file(path, resolve=True)
    except Exception as e:
        log.error("Could not load config from %s as hocon conf: %s", path, e)
    return None

def _merge_configs(a, b, p=None):
    """Does a recursive merge of b into a, effectively overriding configuration
    keys in a with those in b
    """
    if p is None: p = []
    for key in b:
        if key in a:
            if (isinstance(a[key], ConfigTree) or isinstance(a[key], Mapping)) and (isinstance(b[key], ConfigTree) or isinstance(b[key], Mapping)):
                # ConfigTree.merge_configs(a[key], b[key])
            # if isinstance(a[key], Mapping) and isinstance(b[key], Mapping):
                _merge_configs(a[key], b[key], p + [str(key)])
            elif a[key] != b[key]:
                a[key] = b[key]
        else:
            a[key] = b[key]
    return a

# ensure the config gets constructed
#get_config()
