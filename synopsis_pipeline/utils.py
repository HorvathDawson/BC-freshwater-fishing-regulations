#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utility functions shared across the synopsis pipeline.
"""

import re


def normalize_name(name):
    """Normalize a waterbody name for comparison and indexing.

    Removes double quotes, parentheses, extra whitespace, and converts to lowercase
    to enable fuzzy matching when users search for waterbodies.

    Examples:
        'Adams Lake (North Arm)' -> 'adams lake'
        '"Shuswap River"' -> 'shuswap river'
        "O'Connell Lake" -> "o'connell lake"  # Preserves apostrophes

    Args:
        name: Raw waterbody name from GNIS or user input

    Returns:
        Cleaned lowercase string suitable for matching
    """
    if not name or str(name) == "nan":
        return ""

    # Remove double quotes only (preserve single quotes/apostrophes)
    clean = str(name).replace('"', "")
    clean = clean.replace("\u201c", "").replace("\u201d", "")  # Smart double quotes
    clean = clean.replace("\u2018", "'").replace("\u2019", "'")

    # Remove parentheses and their contents (e.g., "(North Arm)")
    clean = re.sub(r"\([^)]*\)", "", clean)

    # Collapse multiple spaces into single space
    clean = re.sub(r"\s+", " ", clean).strip()

    # Lowercase for case-insensitive comparison
    clean = clean.lower()

    return clean
