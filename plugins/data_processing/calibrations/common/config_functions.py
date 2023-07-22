import os
import pyhocon

__config = None


def __load_config():
    global __config
    # TODO: generalize this to different pipeline one may want to run
    pipeline_dir = os.path.dirname(__file__)
    base_config = pyhocon.ConfigFactory.parse_file(os.path.join(pipeline_dir, "..", "configuration",
                                                                "market_view_calibration_pipeline.conf"))
    __config = base_config


def get_config():
    global __config
    if not __config:
        __load_config()
    return __config


def set_config(cfg):
    global __config
    __config = cfg