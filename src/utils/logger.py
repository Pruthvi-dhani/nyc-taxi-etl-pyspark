"""
Structured logger for the pipeline.

Uses Python's built-in ``logging`` module with a consistent format
so that messages appear cleanly in both local consoles and EMR
CloudWatch / S3 log aggregation.
"""

from __future__ import annotations

import logging
import os
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Return a configured :class:`logging.Logger` for *name*.

    Log level is controlled by the ``LOG_LEVEL`` environment variable
    (default ``INFO``).
    """
    level = os.getenv("LOG_LEVEL", "INFO").upper()

    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(getattr(logging, level, logging.INFO))
    logger.propagate = False
    return logger

