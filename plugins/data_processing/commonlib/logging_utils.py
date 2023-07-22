import logging
import sys


def configure_logging_defaults(logging_level=logging.DEBUG):
    log_format = "%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s"
    log_formatter = logging.Formatter(log_format, "%Y-%m-%d %H:%M:%S")
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setLevel(logging_level)
    console_handler.setFormatter(log_formatter)
    logging.getLogger().setLevel(logging_level)
    logging.getLogger().handlers = [console_handler]
