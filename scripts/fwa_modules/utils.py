"""
Shared utilities for FWA processing.
"""

import logging
import sys
from typing import Optional


def setup_logging(name: str = __name__, level: int = logging.INFO) -> logging.Logger:
    """Configure logging with consistent format.

    Args:
        name: Logger name
        level: Logging level

    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def clean_watershed_code(code: str) -> Optional[str]:
    """Remove placeholder segments (000000) from watershed codes.

    Example: "200-ABC123-000000" -> "200-ABC123"

    Args:
        code: Raw FWA_WATERSHED_CODE string

    Returns:
        Cleaned code with only meaningful segments, or None if invalid
    """
    if not isinstance(code, str) or code == "":
        return None
    parts = code.split("-")
    # Filter out the 000000 placeholder values
    valid_parts = [p for p in parts if p != "000000"]
    return "-".join(valid_parts) if valid_parts else None


def get_parent_code(clean_code: str) -> Optional[str]:
    """Get the parent watershed code by removing the last segment.

    This allows us to find the "parent river" for tributary naming.
    Example: "200-ABC123-XYZ456" -> "200-ABC123"

    Args:
        clean_code: Watershed code with 000000 segments already removed

    Returns:
        Parent code, or None if already at top level
    """
    if not clean_code or "-" not in clean_code:
        return None
    # Remove last segment to get parent
    return clean_code.rsplit("-", 1)[0]


def get_code_depth(code: str) -> int:
    """Calculate hierarchy depth of a watershed code.

    Deeper codes represent smaller tributaries.
    Example: "200" has depth 1, "200-ABC123" has depth 2

    Args:
        code: Raw FWA_WATERSHED_CODE string

    Returns:
        Integer depth (number of valid segments)
    """
    if not isinstance(code, str):
        return 0
    # Count segments, excluding 000000 placeholders
    return len([x for x in code.split("-") if x != "000000"])
