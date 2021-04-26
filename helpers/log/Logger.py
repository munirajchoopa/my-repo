"""
This will provide generic platform for logging
DEBUG
INFO
WARNING
ERROR
CRITICAL
"""
import json
import logging
import logging.config
import os
from pkg_resources import resource_string,resource_filename

# Get the file path for the logging configuration
file_validation=resource_filename(__name__, "logging_config.json")


def setup_logging(
    default_path=file_validation, default_level=logging.INFO
):
    """Setup logging config
    """
    path = default_path
    if os.path.exists(path):
        with open(path, "rt") as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)

