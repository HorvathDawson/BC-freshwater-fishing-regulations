"""AUTO-APPROVE WHOLE-WATER DFO PLACES (owner 2026-10-28).

A DFO place whose words are ONLY a water's name (no specific area, no locator — from / to / above /
below / between / except / mouth / bridge / sign / km / tributaries / a lake inside a river's name …)
and that is the ONLY place DFO lists on that water is the whole water: there is nothing to decide.
Each one gets an owner-attributed mark — T, R, P answered y, extent fix `whole` — recorded through
the review app's own mark route (`POST /api/marks`, the one write), so the store is only ever
appended to by the app. A place already marked (any mark in force) is skipped.

    python -m pipeline.regs.review.dfo_auto                 # dry run: the count and the list
    python -m pipeline.regs.review.dfo_auto --apply         # POST each to the running review app
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Mapping, Optional

NOTE = "owner 2026-10-28: auto-approved — no locator, single row"
REVIEWER = "dawson"
#: words that make a place smaller than its water (any digit, or a bracket, too)
LOCATOR = re.compile(
    r"\b(from|to|above|below|between|except|excluding|including|not|mouth|bridge|signs?|km|kms|"
    r"metres?|meters?|m|tributar(?:y|ies)|confluence|upstream|downstream|portion|section|channel|"
    r"boundary|markers?|waters|within|near|outlet|inlet|line|area|weir|dam|falls|pool|hatchery|"
    r"north|south|east|west|upper|lower|side|arm|bay)\b|\d|[()]", re.I)
#: a water's own type word, allowed as the LAST word of its name only ("Comox Lake", not
#: "Cowichan River at the Lake")
TYPE_TAIL = re.compile(r"\s+(river|rivers|creek|lake|slough)\s*$", re.I)


def _norm(s: Optional[str]) -> str:
    return " ".join(re.sub(r"[^a-z0-9 ]", " ", (s or "").lower()).split())


def why_not(loc: Mapping, water: Optional[Mapping], places_on_water: int) -> Optional[str]:
    """None when the place is a whole water to auto-approve; else the reason it is not."""
    st = loc.get("source_text") or {}
    if (st.get("specific_area") or "").strip():
        return "has a specific area"
    words = (st.get("waters") or loc.get("water") or "").strip()
    body = TYPE_TAIL.sub("", words)
    # a bare "<Name> River" is a name, never a locator ("Bridge River")
    bare = re.fullmatch(r"[A-Z][\w'-]*\s+(River|Rivers|Creek|Lake|Slough)", words)
    if not bare and (LOCATOR.search(body) or re.search(r"\blake\b", body, re.I)):
        return "the water's words carry a locator"
    if places_on_water != 1:
        return "more than one place on the water"
    if not (water or {}).get("item_ids"):
        return "no water matched"
    if (st.get("op") or "whole_water") not in ("whole_water", "described"):
        return f"the notice's op is {st.get('op')}"
    return None


def candidates(locs: Mapping, marked: set[str]) -> tuple[list[dict], Counter]:
    """The places to auto-approve (location id, water, status, rows) and why the rest are not."""
    from pipeline.regs.review import dfo as D
    per_water = Counter(loc.get("water_id") for _s, loc, _w in locs.values())
    out, skipped = [], Counter()
    for lid, (slug, loc, water) in locs.items():
        why = why_not(loc, water, per_water[loc.get("water_id")])
        if why is None and lid in marked:
            why = "already marked"
        if why:
            skipped[why] += 1
            continue
        out.append({"location_id": lid, "region": slug, "water": loc.get("water"),
                    "status": loc.get("status") or "active", "n_rows": len(D.rule_rows(loc)[0]),
                    "extents": (loc.get("binding") or {}).get("extents")})
    return out, skipped


def payload(lid: str) -> dict:
    return {"scope": "dfo", "target": lid, "answers": {"T": "y", "R": "y", "P": "y"},
            "note": NOTE, "reviewer": REVIEWER,
            "extent_fix": [{"scope": "entry", "op": "whole", "points": [], "items": []}]}


def main(argv: list[str] | None = None) -> int:
    from pipeline.common.curated import CURATED
    from pipeline.regs.review import dfo as D
    from pipeline.regs.review import marks as M
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.review.dfo_auto")
    ap.add_argument("--apply", action="store_true", help="POST each mark to the running review app")
    ap.add_argument("--url", default="http://127.0.0.1:8787")
    a = ap.parse_args(argv)
    marked = {m.target for m in M.latest(M.load(Path(CURATED.regulations.review.marks))).values()
              if m.scope == "dfo"}
    got, skipped = candidates(D.locations(), marked)
    print(f"{len(got)} whole-water places to auto-approve "
          f"({Counter(r['status'] for r in got)}); skipped: {dict(skipped)}")
    for r in got:
        print(f"  {r['location_id']:<45} {r['status']:<8} rows={r['n_rows']} extents={json.dumps(r['extents'])}")
    if not a.apply:
        return 0
    bad = 0
    for r in got:
        req = urllib.request.Request(f"{a.url}/api/marks", data=json.dumps(payload(r["location_id"])).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                st = json.loads(resp.read()).get("status")
            print(f"  marked {r['location_id']}: {st}")
        except Exception as ex:                   # said, and the run goes on
            bad += 1
            body = getattr(ex, "read", lambda: b"")()
            print(f"  REFUSED {r['location_id']}: {ex} {body[:300]!r}", file=sys.stderr)
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
