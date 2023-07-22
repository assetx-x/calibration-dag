import logging
import sys

# setup logging to stdout
logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
calibration_logger = logging.getLogger("TaskflowPipeline")
calibration_logger.disabled = False
