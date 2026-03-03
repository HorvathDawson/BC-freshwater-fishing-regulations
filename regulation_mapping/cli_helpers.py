"""
Shared CLI formatting helpers for regulation_mapping test scripts.

Consolidates ANSI colours, terminal-width detection, and section
formatting so that every CLI test uses the same presentation code.
"""

import shutil
import warnings


# --- ANSI colour codes ---
RED = "\033[91m"
YELLOW = "\033[93m"
GREEN = "\033[92m"
BLUE = "\033[94m"
CYAN = "\033[96m"
RESET = "\033[0m"


def tw(default: int = 80) -> int:
    """Return the current terminal width (columns)."""
    try:
        return shutil.get_terminal_size((default, 20)).columns
    except Exception as e:
        warnings.warn(f"Failed to detect terminal width, using default={default}: {e}")
        return default


def divider(char: str = "=") -> None:
    """Print a full-width divider line."""
    print(char * tw())


def header(text: str) -> None:
    """Print a section header surrounded by divider lines."""
    print()
    divider("=")
    print(f"  {text}")
    divider("=")
    print()


def sub_header(text: str) -> None:
    """Print a sub-section header with a dash underline."""
    print(f"\n{text}")
    print("-" * tw())
