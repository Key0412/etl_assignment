import json
import logging
import logging.config
import logging.handlers
from pathlib import Path


def get_logger() -> logging.Logger:
    """Retrieves the logger instance for the ETL pipeline.

    This function initializes or retrieves a logger named 'etl_logger', which can be used throughout
    the project to log messages related to ETL processes.

    Returns:
        logging.Logger: The logger instance with the name 'etl_logger'.
    """
    logger = logging.getLogger(name="etl_logger")
    return logger


def setup_logging() -> None:
    """Sets up the logging configuration for the ETL pipeline.

    This function loads the logging configuration from a JSON file located at
    'etl_assignment/config/logging_config.json'.
    """
    config_file = Path("etl_assignment/config/logging_config.json")
    with open(config_file) as json_file:
        config = json.load(json_file)
    log_dir = Path(config["handlers"]["file"]["filename"])
    log_dir.parent.mkdir(exist_ok=True)
    logging.config.dictConfig(config)
