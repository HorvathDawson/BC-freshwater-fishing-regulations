"""Q154: THE SAMPLE PONDS as a review target — a mark of scope "q154" is held against these.

Q154 (owner 2026-10-28): should a small unnamed lake polygon a river runs through (the `river_widenings` set)
take the river's rules? The proposed cutoff (q154-examples.md §4): under 1 ha AND avg width <= 2 x the
modelled river width, man-made excluded. To decide, the owner marks a SAMPLE on the satellite map: "pool"
(river rules) or "real lake"; "unsure" is a mark with P ?. The tally against the cutoff's prediction is the
evidence. The sample is a read-only generated file (`handoff/PLAN/curation-review/q154/sample.json`,
`sample.py`); nothing here writes it, and a mark changes no data.

A q154 mark's prints: `meaning` the measures shown (area, widths, ratio, caught), `binding` the outline —
a regenerated sample that redraws or re-measures a pond makes its mark stale.
"""
from __future__ import annotations

import hashlib
import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[3]
SAMPLE = ROOT / "handoff/PLAN/curation-review/q154/sample.json"
MEANING = ("ha", "avg_width_m", "river_width_m", "ratio", "man_made", "caught")


def _path() -> Path:
    return Path(os.environ.get("CURATION_Q154_SAMPLE") or SAMPLE)


def _mtime(p: Path) -> float:
    try:
        return p.stat().st_mtime
    except OSError:
        return 0.0


@lru_cache(maxsize=2)
def _load(p: str, _m: float) -> dict:
    try:
        doc = json.loads(Path(p).read_text())
    except OSError:
        return {"cutoff": {}, "rows": [], "by_id": {}}
    rows = doc.get("rows") or []
    return {"cutoff": doc.get("cutoff") or {}, "rows": rows, "by_id": {r["id"]: r for r in rows}}


def load() -> dict:
    p = _path()
    return _load(str(p), _mtime(p))


def pond(pid: str) -> Optional[dict]:
    return load()["by_id"].get(pid)


def _h(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]


def prints(row: dict) -> dict:
    """THE PRINTS of a sample pond (`marks.Prints` shape): no book text; one `q154:<id>` record."""
    return {"text": None, "records": [{"key": f"q154:{row['id']}", "meaning": _h({k: row.get(k) for k in MEANING}),
                                       "binding": _h({"rings": row.get("rings")})}]}


def prints_of(pid: str) -> Optional[dict]:
    row = pond(pid)
    return prints(row) if row is not None else None
