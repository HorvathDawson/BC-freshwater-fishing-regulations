"""T212: THE TIDAL CANDIDATES as a review target — a mark of scope "tidal" is held against these.

The candidates are a proposal file (`handoff/PLAN/curation-review/tidal/tidal-candidates.json`, written by
`pipeline.tools.audit.tidal_candidates`): one per stream, where its tidal boundary would be cut. The
curation review shows them (its Tidal mode); ACCEPTING one is a mark (scope "tidal", answer P) that carries
the proposed cut as a `NewSplit` — never a splits.json edit (T185 turns marks into reviewed changes).

A tidal mark's prints are its candidate's: `meaning` the proposal (where the cut goes), `binding` what it
stands on (the polygon's crossing and the sources). A regenerated file that moves the cut makes the mark
stale ("rule changed"); one that only adds a source makes it stale as "binding moved".
"""
from __future__ import annotations

import hashlib
import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[3]
#: the candidates file; `CURATION_TIDAL_CANDIDATES` points it at a copy (tests)
DEFAULT_PATH = ROOT / "handoff/PLAN/curation-review/tidal/tidal-candidates.json"


def path() -> Path:
    env = os.environ.get("CURATION_TIDAL_CANDIDATES")
    return Path(env) if env else DEFAULT_PATH


@lru_cache(maxsize=2)
def _load(p: str, _mtime: float) -> dict:
    doc = json.loads(Path(p).read_text())
    return {"coverage": doc.get("coverage") or {}, "about": doc.get("_about") or "",
            "by_id": {r["id"]: r for r in doc.get("candidates") or []},
            "order": [r["id"] for r in doc.get("candidates") or []]}


def load(p: Path | None = None) -> dict:
    """{coverage, about, by_id, order} — empty when the file is absent (no proposals yet)."""
    p = Path(p or path())
    try:
        mt = p.stat().st_mtime
    except OSError:
        return {"coverage": {}, "about": "", "by_id": {}, "order": []}
    return _load(str(p), mt)


def candidate(cid: str, p: Path | None = None) -> Optional[dict]:
    return load(p)["by_id"].get(cid)


def _h(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]


def prints(row: dict) -> dict:
    """THE PRINTS of a candidate (`marks.Prints` shape): no book text; one `split:<id>` record."""
    pr = row.get("proposed") or {}
    meaning = {"water": row.get("water"), "blk": row.get("blk"), "measure": pr.get("measure"),
               "label": pr.get("split_label")}
    binding = {"polygon": row.get("polygon"),
               "sources": sorted(json.dumps([s.get("kind"), s.get("source"), s.get("measure")])
                                 for s in row.get("sources") or [])}
    return {"text": None, "records": [{"key": f"split:{row['id']}", "meaning": _h(meaning),
                                       "binding": _h(binding)}]}


def prints_of(cid: str, p: Path | None = None) -> Optional[dict]:
    row = candidate(cid, p)
    return prints(row) if row is not None else None
