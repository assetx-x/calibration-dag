import os

import pkg_resources
from pyhocon import ConfigFactory, ConfigTree, ConfigParser
import logging.config
from contextlib import closing
from collections import Mapping
from commonlib.intuition_loggers import log
from commonlib.config import get_config, set_config, load

from functools import reduce

def _create_context_for_loading_config_files():
    import pandas as pd
    import pytz
    from datetime import timedelta, datetime, time
    from os import path

    return {
        "pd": pd,
        "pytz": pytz,
        "timedelta": timedelta,
        "datetime": datetime,
        "time": time,
        "path": path,
        "__file__": os.getcwd()
    }

set_config(load(defaults=os.path.join(os.path.dirname(__file__), "defaults.conf"),
                app_config=os.path.join(os.path.dirname(__file__), "intuition.conf"),
                context=_create_context_for_loading_config_files(),
                load_and_setup_logging=False))