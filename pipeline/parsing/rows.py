"""Shared synopsis row loader — single source of truth for row ordering.

Both the Gemini parser (``parser.run``) and the agent-parsing tools
(``pipeline.agent_parsing``) load rows through this function so that global
row indices align across engines and the shared ``session_state.json``
checkpoint.  If two code paths flattened the raw pages independently they
could drift, silently corrupting the index → result mapping.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional


def load_synopsis_rows(raw_path: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Flatten ``synopsis_raw_data.json`` pages into an ordered row list.

    Each row is copied (``dict(row)``) so the raw pages are never mutated, and
    the page-level ``region`` is backfilled only when the row does not already
    carry one.  Ordering is deterministic (page order, then row order) and is
    the contract every downstream index relies on.

    Parameters
    ----------
    raw_path:
        Path to ``synopsis_raw_data.json``.  When ``None`` the path is resolved
        from ``project_config.get_config().synopsis_raw_data_path``.
    """
    if raw_path is None:
        from project_config import get_config

        raw_path = get_config().synopsis_raw_data_path

    with open(raw_path, encoding="utf-8") as f:
        pages = json.load(f)

    rows: List[Dict[str, Any]] = []
    for page in pages:
        region = page.get("context", {}).get("region")
        for row in page.get("rows", []):
            row_dict = dict(row)
            if region and not row_dict.get("region"):
                row_dict["region"] = region
            rows.append(row_dict)
    return rows
