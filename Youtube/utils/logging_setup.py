# File: utils/logging_setup.py

import json
import logging
import logging.config
import pathlib
from typing import Optional

from Youtube.config import LOG_CONFIG_PATH


def setup_logging(config_path: Optional[str] = None) -> None:
    """
    Configures the logging system for the application.

    This function sets up logging based on a specified configuration file. It ensures that the necessary
    directories for log files exist, creates them if they do not, and initializes the logging system
    with the parameters and structure defined in the configuration.

    Args:
        config_path: Optional; The path to the logging configuration file. If not provided,
            a default file path specified by the variable `LOG_CONFIG_PATH` is used.

    """
    config_path = config_path or LOG_CONFIG_PATH
    pathlib.Path("logs").mkdir(parents=True, exist_ok=True)

    with open(config_path, "rt", encoding="utf-8") as fp:
        cfg = json.load(fp)

    # Create any other directories declared in handler filenames
    for handler in cfg.get("handlers", {}).values():
        filename = handler.get("filename")
        if filename:
            pathlib.Path(filename).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)

    logging.config.dictConfig(cfg)
