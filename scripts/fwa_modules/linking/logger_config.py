"""
Shared logger configuration for the linking module.

Usage:
    from .logger_config import get_logger
    logger = get_logger(__name__)

Environment variable:
    Set FWA_LINKING_DEBUG=1 to enable debug output
"""

import logging
import os
import sys


# Global flag to control debug mode
DEBUG_MODE = os.getenv("FWA_LINKING_DEBUG", "0") == "1"

# Store configured loggers to avoid duplicate setup
_configured_loggers = set()


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with consistent configuration for the linking module.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Only configure once per logger
    if name in _configured_loggers:
        return logger

    # Set level based on debug mode
    if DEBUG_MODE:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.WARNING)

    # Add handler if not already present
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # Prevent propagation to root logger (avoid duplicate output)
    logger.propagate = False

    _configured_loggers.add(name)
    return logger


def enable_debug():
    """Enable debug mode for all linking loggers."""
    global DEBUG_MODE
    DEBUG_MODE = True

    # Update all existing loggers
    for name in _configured_loggers:
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)


def disable_debug():
    """Disable debug mode for all linking loggers."""
    global DEBUG_MODE
    DEBUG_MODE = False

    # Update all existing loggers
    for name in _configured_loggers:
        logger = logging.getLogger(name)
        logger.setLevel(logging.WARNING)
