"""
This module provides some common methods and classes.
Author: Ei Yamaguchi
Date: 2023-12-23
"""
import logging
import structlog
import pandera as pa

from pandera.typing import Series
from dataclasses import dataclass
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
# from logger_cloudwatch_structlog import AWSCloudWatchLogs 

import os
class Logger:
    """Defining the log class using structlog.

    The log has three types:
        info.
        warning.
        error.
    """

    def __init__(self):
        
        # logger_factory=structlog.PrintLoggerFactory(),
        # logging.basicConfig(filename='test.log', encoding='utf-8', level=logging.INFO)
        
        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.processors.add_log_level,
                structlog.processors.StackInfoRenderer(),
                structlog.dev.set_exc_info,
                structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S %z", key="ts", utc=True),
                # AWSCloudWatchLogs(callouts=["status", "event"]),
                structlog.dev.ConsoleRenderer()
            ],
            wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(),
            # logger_factory=structlog.stdlib.LoggerFactory(),
            # logger_factory=structlog.WriteLoggerFactory(file=Path("app").with_suffix(".log").open("w")),
            cache_logger_on_first_use=False
        )

        self.logger = structlog.get_logger()

    def info(self, message, **kwargs):
        self.logger.info(message, **kwargs)

    def warning(self, message, **kwargs):
        self.logger.warning(message, **kwargs)

    def error(self, message, **kwargs):
        self.logger.error(message, **kwargs)


@dataclass
class Params:
    """Defining the structure of param.

    The key-value pairs in param can be defined directly here.
    """

    ten_id: str = "10001"


class Pandas_Schema(pa.SchemaModel):
    """データをcheck.

    Use pandera to specify the type and range of data
    """

    ten_id: Series[str]
    day: Series[str]
    count: Series[int] = pa.Field(in_range={"min_value": 20, "max_value": 100})
