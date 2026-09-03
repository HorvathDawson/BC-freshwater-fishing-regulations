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

# Synopsis symbol (from pipeline.regs.extraction) that flags a whole row as extending to tributaries —
# the entry-level "[Includes Tributaries]" marker. Single source of truth for parser + curation.
TRIBUTARIES_SYMBOL = "Incl. Tribs"


def symbols_include_tributaries(symbols: Optional[List[Any]]) -> bool:
    """True when a row's `symbols` list carries the tributaries marker (entry-level includes)."""
    return any(TRIBUTARIES_SYMBOL.lower() in str(s).lower() for s in symbols or [])


def row_includes_tributaries(row: Dict[str, Any]) -> bool:
    """True when a synopsis row is symbol-flagged as including its tributaries."""
    return symbols_include_tributaries(row.get("symbols"))



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
        from ``GENERATED.regs.extraction / 'synopsis_raw_data.json'``.
    """
    if raw_path is None:
        from pipeline.common.curated import GENERATED

        raw_path = GENERATED.regs.extraction / "synopsis_raw_data.json"

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
