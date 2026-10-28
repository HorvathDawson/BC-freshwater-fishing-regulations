"""PRE-VERIFIED PROPOSALS (owner 2026-10-27): a second, independent method's verdict on each proposal,
shown beside it so the owner can take the agreed ones first. READ ONLY — nothing here accepts
anything: the owner's mark stays the only write.

    dfo(location_id)      the DFO suggestion's check   (`handoff/PLAN/curation-review/dfo-preverify.json`,
                          `pipeline.regs.dfo_salmon.preverify`)
    tidal(candidate_id)   the tidal candidate's check  (`.../tidal/tidal-preverify.json`,
                          `pipeline.tools.audit.tidal_preverify`, a DEM tidal limit)
    entry_items(entry_id) the entry's split proposals  (`.../entry-split-suggestions.json`,
                          `pipeline.regs.review.split_suggest`), each with its own check
    entry(entry_id)       one verdict for the entry's queue row

Each verdict is {"status": pre-verified | disagree | unchecked, "why", "method", "first", "delta_m",
"tolerance_m", "duplicate_of"}. "pre-verified" means two independent methods agree within tolerance
— never that the owner agreed. Each file is re-read when it changes (mtime), no restart.
"""
from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path

import reuse

_PLAN = reuse._ROOT / "handoff/PLAN/curation-review"
FILES = {
    "dfo": Path(os.environ.get("CURATION_DFO_PREVERIFY") or _PLAN / "dfo-preverify.json"),
    "tidal": Path(os.environ.get("CURATION_TIDAL_PREVERIFY") or _PLAN / "tidal/tidal-preverify.json"),
    "entry": Path(os.environ.get("CURATION_ENTRY_SPLIT_SUGGESTIONS") or _PLAN / "entry-split-suggestions.json"),
}
STATUSES = ("pre-verified", "disagree", "unchecked")
KEEP = ("status", "why", "method", "first", "delta_m", "tolerance_m", "duplicate_of")


@lru_cache(maxsize=8)
def _load(path: str, _mtime: float) -> dict:
    return json.loads(Path(path).read_text()).get("items") or {}


def _items(which: str) -> dict:
    p = FILES[which]
    try:
        return _load(str(p), p.stat().st_mtime)
    except (OSError, ValueError):
        return {}


def _compact(v: dict | None) -> dict | None:
    if not v or v.get("status") not in STATUSES:
        return None
    return {k: v.get(k) for k in KEEP}


def dfo(location_id: str) -> dict | None:
    return _compact(_items("dfo").get(location_id))


def tidal(candidate_id: str) -> dict | None:
    return _compact(_items("tidal").get(candidate_id))


def entry_items(entry_id: str) -> list[dict]:
    """The entry's split proposals as written (each with `preverify`), for the detail screen."""
    return list(_items("entry").get(entry_id) or [])


def entry(entry_id: str) -> dict | None:
    """One verdict for the entry's row: disagree if any proposal disagrees, pre-verified when every
    proposal that is not a duplicate is, else unchecked. None when the entry has no proposal."""
    items = entry_items(entry_id)
    if not items:
        return None
    own = [i for i in items if not i.get("duplicate_of")] or items
    sts = [(i.get("preverify") or {}).get("status") or "unchecked" for i in own]
    if "disagree" in sts:
        status = "disagree"
    elif all(s == "pre-verified" for s in sts):
        status = "pre-verified"
    else:
        status = "unchecked"
    n_pre = sum(s == "pre-verified" for s in sts)
    whys = [f"{i.get('kind')}: {(i.get('preverify') or {}).get('why') or i.get('reason') or ''}" for i in own]
    return {"status": status, "why": f"{n_pre}/{len(own)} proposal(s) pre-verified — " + " · ".join(whys)[:600],
            "method": None, "first": None, "delta_m": None, "tolerance_m": None,
            # an entry is never hidden as a duplicate: it is reviewed for more than its splits
            "duplicate_of": None,
            "n": len(own), "n_pre": n_pre}
