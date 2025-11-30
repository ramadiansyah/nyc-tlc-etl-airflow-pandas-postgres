# utils/logger.py
"""
capstone 3
project 1

logger.py

Utility module for configuring and retrieving a consistent logger instance.
Primarily used across Airflow tasks or other services to standardize logging format.
"""

import logging
import os
import inspect

from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
LOG_MODE = os.getenv("LOG_MODE")

class CustomFormatter(logging.Formatter):
    """
    A custom logging formatter with colored output based on log level.
    
    This formatter changes the log message color depending on the log level:
    - DEBUG: grey
    - INFO: green
    - WARNING: yellow
    - ERROR: red
    - CRITICAL: red

    Attributes:
        grey (str): ANSI escape code for grey.
        green (str): ANSI escape code for green.
        yellow (str): ANSI escape code for yellow.
        red (str): ANSI escape code for red.
        reset (str): ANSI escape code to reset formatting.
        format (str): Log message format template.
        FORMATS (dict[int, str]): Mapping of log levels to their respective colored formats.
    """
    grey: str = "\x1b[38;21m"
    green: str = "\x1b[32m"
    yellow: str = "\x1b[33;21m"
    red: str = "\x1b[31;21m"
    reset: str = "\x1b[0m"
    format: str = "[%(asctime)s] [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)"

    FORMATS: dict[int, str] = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: green + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: red + format + reset,
    }

    def format(self, record: logging.LogRecord) -> str:
        """
        Format the specified record as text with color based on its log level.

        Args:
            record (logging.LogRecord): The log record to be formatted.

        Returns:
            str: The formatted log message.
        """
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def setup_logger(name: str = "etl_logger", level: int = logging.INFO) -> logging.Logger:
    """
    Set up and return a logger with the custom colored formatter.

    Args:
        name (str, optional): Name of the logger. Defaults to "etl_logger".
        level (int, optional): Logging level (e.g., logging.INFO). Defaults to logging.INFO.

    Returns:
        logging.Logger: Configured logger instance.
    """
    handler: logging.StreamHandler = logging.StreamHandler()
    handler.setFormatter(CustomFormatter())
    
    logger: logging.Logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if not logger.hasHandlers():
        logger.addHandler(handler)
    
    return logger


def get_logger(name="airflow.task", level: int = logging.INFO) -> logging.Logger:
    """
    Returns a configured logger instance.

    If the logger with the given name has not been initialized before,
    this function will attach a stream handler with a standard formatter.
    The log level is set to INFO by default.

    Args:
        name (str): The name of the logger to retrieve. Defaults to "airflow.task".

    Returns:
        logging.Logger: A logger instance with the specified name.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)
    return logger



# capstone 2 logger
def setup_logger(log_dir="logs/custom", log_name=None):

    # If no log_name is provided, use the name of the calling script/module
    if log_name is None:
        caller_name = inspect.stack()[1].filename
        log_name = os.path.splitext(os.path.basename(caller_name))[0]

    os.makedirs(log_dir, exist_ok=True)

    # Create log filename based only on today's date
    today_str = datetime.now().strftime("%Y%m%d")
    log_filename = os.path.join(log_dir, f"{today_str}.log")

    logger = logging.getLogger("nyc_project_logger")  # Single global logger name
    logger.setLevel(logging.DEBUG) # set this to debugging

    if not logger.handlers:  # avoid adding multiple handlers
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(logging.INFO) 

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Also log to console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger
