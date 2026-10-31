"""T224: THE NAME-VARIANT CANDIDATES as a review target — a mark of scope "name" is held against these.

The candidates are mined read-only from local sources (`handoff/PLAN/curation-review/name-variants/mine.py` →
`candidates.json`): one row per (FWA target, proposed name), every source with its own evidence.

OWNER RULINGS (2026-10-28):
  - an accepted name is searchable, AND it is the shown name of a water with no official name, labelled with its
    source; an official name always wins on a named water;
  - AUTO-ACCEPT every `auto_accept_eligible` row (high confidence, ≥ 80 % overlap or an ECCC station, one
    candidate, no rival, FWA-unnamed) — the owner spot-checks a sample of them in the review app;
  - every other row is a REVIEW row: accepted only by the owner's mark (scope "name", call accept / reject);
  - 08MH079's line (blk 356314888): BOTH — "Kilgard Creek" shown, "Lonzo Creek (North Fork)" searchable.

A name mark's prints: `meaning` = the name, its target and its sources; `binding` = the target. A re-mined file that
changes either makes the mark stale.
"""
from __future__ import annotations

import hashlib
import json
import os
import random
import re
from functools import lru_cache
from pathlib import Path
from typing import Optional

from pipeline.common.curated import GENERATED

ROOT = Path(__file__).resolve().parents[3]
CANDIDATES = GENERATED.review / "name-variants/candidates.json"
SPOT_SAMPLE = 30

#: the candidate source key -> (NameSource code, label when the candidate's own is not better)
SOURCE_CODE = {"osm": "osm", "eccc": "gauge", "dfo": "dfo", "synopsis": "regulation", "bcgnis_cache": "gazette"}

#: OWNER DECISIONS on single rows (2026-10-28), by candidate id: call, and optionally the name to store / display
OWNER = {
    "blk:356314888~kilgard-creek": {"call": "accept", "display": True,
                                     "why": "owner 2026-10-28: 08MH079's line — Kilgard Creek shown"},
    "blk:356314888~lonzo-creek": {"call": "accept", "name": "Lonzo Creek (North Fork)",
                                   "why": "owner 2026-10-28: 08MH079's line — Lonzo Creek (North Fork) searchable"},
    # owner 2026-10-29: a name that came in by an ECCC station is accepted when the station's name plainly names this
    # water; a diversion / headpond name may name a structure, so it is HELD (not applied) until the owner says
    "blk:356370187~inkaneep-creek": {"call": "accept", "why": "owner 2026-10-29: the gauge 08NM012 (Inkaneep Creek "
                                     "near Oliver) names this water"},
    "blk:354151912~noble-creek": {"call": "accept", "why": "owner 2026-10-29: the gauge 08HA028 (Noble Creek at the "
                                  "mouth) names this water"},
    # owner 2026-10-29: accepted though LOW confidence — an assumption from the forest roads named for it (GOTCHAS.md)
    "blk:360846083~sheldens-creek": {"call": "accept", "why": "owner 2026-10-29: accepted as an ASSUMPTION — the "
                                     "unnamed line into Sheldens Bay, named only by the forest roads around it "
                                     "('SP 19(1) TO SHELDONS', SHEL100–103); low confidence, see GOTCHAS"},
    "blk:356368998~meadow-creek-diversion": {"call": "accept", "why": "owner 2026-10-29 (after a hold): accepted, "
                                             "credited to the gauge 08NH128 (Meadow Creek Diversion below John Creek)"},
    "blk:354088905~quinsam-river-diversion-headpond": {"call": "accept", "why": "owner 2026-10-29 (after a hold): "
                                                       "accepted, credited to the gauge 08HD034"},
}
#: owner 2026-10-29: every auto-accepted row that came in by LINE COVERAGE (>= 80 %) is the owner's accept
COVERAGE_ACCEPTED = "owner 2026-10-29: accepted (auto rule, line coverage >= 80 %)"
_SMALL = {"near", "at", "the", "above", "below", "of", "and", "on", "in", "to"}


def gauge_label(station: str, station_name: str) -> str:
    """How a name taken from an ECCC station is credited (the app: "Name from <label>"): the gauge and its own
    name, e.g. "the Environment Canada gauge 08NM012 (Inkaneep Creek near Oliver)"."""
    words = (station_name or "").replace("(LOWER STATION)", "").replace("(UPPER STATION)", "").split()
    nice = " ".join(w.lower() if k and w.lower() in _SMALL else w.title() for k, w in enumerate(words))
    return f"the Environment Canada gauge {station}" + (f" ({nice})" if nice else "")

GROUPS = ("owner", "auto", "review")
CALLS = ("accept", "reject")
#: an owner call in `OWNER` that keeps a row OUT of the accepted set pending the owner (owner 2026-10-29)
HOLD = "hold"


def key(s: str) -> str:
    s = (s or "").lower().replace("’", "").replace("'", "").replace('"', "")
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def tkey(t: dict) -> str:
    return t.get("item_id") or (f"blk:{t['blk']}" if t.get("blk") else f"wbk:{t.get('wbk')}")


def cid(c: dict) -> str:
    return f"{tkey(c['target'])}~{key(c['proposed']).replace(' ', '-')}"


def _path() -> Path:
    return Path(os.environ.get("CURATION_NAME_CANDIDATES") or CANDIDATES)


@lru_cache(maxsize=2)
def _load(p: str, _m: float) -> dict:
    try:
        doc = json.loads(Path(p).read_text())
    except OSError:
        return {"by_id": {}, "order": [], "spot": set()}
    rows = doc.get("candidates") or []
    by_id = {}
    for c in rows:
        by_id.setdefault(cid(c), c)
    auto = sorted(i for i, c in by_id.items() if c.get("auto_accept_eligible") and i not in OWNER)
    spot = set(random.Random(224).sample(auto, min(SPOT_SAMPLE, len(auto))))
    rank = {"high": 0, "medium": 1, "low": 2}

    def order(i):
        c = by_id[i]
        return (GROUPS.index(group_of(i, c)), i not in spot, rank[c["confidence"]], c["proposed"], i)
    return {"by_id": by_id, "order": sorted(by_id, key=order), "spot": spot}


def load() -> dict:
    p = _path()
    try:
        m = p.stat().st_mtime
    except OSError:
        m = 0.0
    return _load(str(p), m)


def group_of(i: str, c: dict) -> str:
    if i in OWNER:
        return "owner"
    return "auto" if c.get("auto_accept_eligible") else "review"


def candidate(i: str) -> Optional[dict]:
    return load()["by_id"].get(i)


def _h(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]


def prints(i: str, c: dict) -> dict:
    meaning = {"name": c["proposed"], "relation": c["relation"],
               "sources": sorted((s["source"], s.get("source_ref", "")) for s in c["sources"])}
    return {"text": None, "records": [{"key": f"name:{i}", "meaning": _h(meaning), "binding": _h(c["target"])}]}


def prints_of(i: str) -> Optional[dict]:
    c = candidate(i)
    return prints(i, c) if c is not None else None


def name_records(i: str, c: dict, *, call_by: str) -> list[dict]:
    """The `name_variants` name entries an ACCEPTED candidate writes: one per source, each with its real source,
    label, ref and evidence (a city's or OSM's name is never official)."""
    dec = OWNER.get(i, {})
    nm = dec.get("name") or c["proposed"]
    out = []
    for s in c["sources"]:
        src = s["source"]
        code = "municipal" if src.startswith("municipal:") else SOURCE_CODE.get(src, "alias")
        ev = s.get("evidence") or {}
        ref = s.get("source_ref", "")
        label = s["source_label"]
        if src == "eccc" and ev.get("station"):
            ref = f"ECCC station {ev['station']} ({ev.get('station_name', '')})"
            label = gauge_label(ev["station"], ev.get("station_name", ""))
        elif src == "bcgnis_cache" and ev.get("feature_id"):
            ref = f"BC GNIS feature {ev['feature_id']}"
        elif src.startswith("municipal:") and ev.get("source_ids"):
            ref = f"{ref}#{','.join(str(x) for x in ev['source_ids'][:5])}"
        rec = {"name": nm, "source": code, "source_label": label, "source_ref": ref,
               "official": code == "gazette",
               "evidence": {k: v for k, v in ev.items() if k in ("blk_overlap", "source_overlap", "near_m",
                                                                  "source_len_m", "distance_m", "trust", "lonlat")},
               "accepted": call_by, "candidate": i, "confidence": c["confidence"]}
        if dec.get("display"):
            rec["display"] = True
        out.append(rec)
    return out


def target_entry(c: dict) -> dict:
    t = c["target"]
    if t.get("blk"):
        return {"blks": [t["blk"]]}
    if t.get("wbk"):
        return {"wbks": [t["wbk"]]}
    a, _, b = (t.get("item_id") or "").partition(":")
    return {f"{a}s" if a in ("blk", "wbk") else "gnis_ids": [b]} if a in ("blk", "wbk", "gnis") else {}
