"""Shared helpers for the agent-parsing workflow.

Centralises path resolution, the row-content digest (data-drift guard), and
the *fail-loud* loading of the shared session checkpoint so that both
``batch_exporter`` and ``ingest`` behave identically and never silently start
from a blank session.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List

import yaml

from pipeline.parsing.session import _SESSION_VERSION


def resolve_parsing_dir() -> Path:
    """Return the shared parsing output dir (holds ``session_state.json``)."""
    from project_config import get_config

    config = get_config()
    with open(config.project_root / "config.yaml") as f:
        cfg = yaml.safe_load(f)
    return config.project_root / cfg["output"]["pipeline"]["parsing"]


def default_agent_dir() -> Path:
    """Return the default agent-parsing working dir (batches/responses)."""
    from project_config import get_config

    config = get_config()
    with open(config.project_root / "config.yaml") as f:
        cfg = yaml.safe_load(f)
    # Sibling of the parsing dir: output/pipeline/agent_parsing
    parsing = Path(cfg["output"]["pipeline"]["parsing"])
    return config.project_root / parsing.parent / "agent_parsing"


def compute_rows_digest(rows: List[Dict[str, Any]]) -> str:
    """Content digest over (index, water, raw_regs) for every row.

    Used to detect if the underlying synopsis data changed between export and
    ingest — a change would silently break the index → water mapping, so ingest
    refuses to proceed on a mismatch.
    """
    hasher = hashlib.sha256()
    payload = [
        [i, row.get("water", ""), row.get("raw_regs", "")]
        for i, row in enumerate(rows)
    ]
    hasher.update(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    )
    return hasher.hexdigest()


def load_session_state(session_dir: Path, expected_total: int) -> Dict[str, Any]:
    """Load ``session_state.json``, failing loudly on any inconsistency.

    Unlike ``ParsingSession`` (which silently starts fresh on mismatch), this
    raises so the agent tools never operate on a blank/misaligned session.
    """
    session_file = session_dir / "session_state.json"
    if not session_file.exists():
        raise FileNotFoundError(
            f"No session_state.json at {session_file}. Run the parser first "
            f"(python -m pipeline --step parse) to create the checkpoint."
        )
    with open(session_file, encoding="utf-8") as f:
        state = json.load(f)

    version = state.get("version")
    if version != _SESSION_VERSION:
        raise ValueError(
            f"Session version mismatch: file={version}, "
            f"expected={_SESSION_VERSION}. Refusing to proceed."
        )
    total = state.get("total")
    if total != expected_total:
        raise ValueError(
            f"Row count mismatch: session total={total} but current data has "
            f"{expected_total} rows. The synopsis data changed — refusing to "
            f"proceed (indices would be misaligned)."
        )
    return state
