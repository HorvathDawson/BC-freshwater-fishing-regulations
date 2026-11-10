"""CLOSED AREAS INSIDE A DFO PLACE -> A SECOND ENTRY (T573, owner 2026-11-10: "it is like it should become 2 entries?").

A few DFO rows say, inside one place's words, that named stretches of it are closed: the Thompson's "Please note that
no fishing is allowed in the following areas, year-round: Deadman - from … to …, Juniper - …, Ashcroft - …". The
scrape keeps those as the location's `source_text.excludes`. One location cannot say "these rules here, no fishing
there", so the row becomes TWO ENTRIES:

    A  the place itself: DFO's rules, on the whole stretch its words name
    B  a CLOSURE derived from the same row: one rule, "No fishing, year-round", on the named closed areas — each area
       its own piece, with DFO's own words for it

B is narrower than A, so the salmon chain's own order ("narrowest first", `deliver.salmon.chain_order`) lets B win on
its sections: no new precedence.

This module only DERIVES entry B from the stored rows (pure: no fetch, no feed, no places file is written). `main`
writes the derived entries to `<generated>/review/dfo_closure_entries.json` as PROPOSALS for the review app; putting
an accepted one into the places file and its rule into the feed is the build-side step (T573).

Only a row whose own words say no fishing / closed is derived (a wording that merely says "except:" takes it from a
wording of the same water with the identical list). Babine Lake's "within a 400 m radius of …" exclusions are radius
closures around tributary mouths, not named stretches: left alone, noted on the ticket.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterable, Mapping, Optional

#: the words that make an exclusion list a CLOSURE
CLOSED_RE = re.compile(r"\bno\s+fishing\b|\bclosed\b|\bclosure\b", re.I)
#: "… year-round" in the place's words: the closure has no season
YEAR_ROUND_RE = re.compile(r"year[\s-]*round", re.I)
#: "Deadman - from the fishing boundary signs …": the area's name, then DFO's words for it
AREA_RE = re.compile(r"^\s*(?P<name>[^-–—:]{2,60}?)\s*[-–—:]\s+(?P<words>.+?)\s*$", re.S)
SUFFIX = "#closed"


def _text(loc: Mapping) -> str:
    return str((loc.get("source_text") or {}).get("specific_area") or "")


def _excludes(loc: Mapping) -> tuple[str, ...]:
    return tuple(e.strip() for e in (loc.get("source_text") or {}).get("excludes") or [] if isinstance(e, str) and e.strip())


def says_closed(loc: Mapping) -> bool:
    """the place's own words (or an exclusion's) say no fishing / closed, AND the list names stretches ("Deadman -
    from … to …": at least one). A list of bare tributary names (Babine Lake's 400 m radius exclusions) is a radius
    closure around mouths, not a set of named stretches: not derived here."""
    ex = _excludes(loc)
    return (bool(ex) and any(AREA_RE.match(e) for e in ex)
            and bool(CLOSED_RE.search(_text(loc) + " " + " ".join(ex))))


def areas(loc: Mapping) -> list[dict]:
    """each exclusion as {name, words}: "Deadman - from … to …" -> Deadman / from … to …; one with no name keeps its
    whole text as the words (never dropped)"""
    out = []
    for i, e in enumerate(_excludes(loc), 1):
        m = AREA_RE.match(e)
        out.append({"name": m.group("name").strip(), "words": m.group("words").strip()} if m
                   else {"name": f"area {i}", "words": e})
    return out


def closure_entry(loc: Mapping, because: Optional[str] = None) -> dict:
    """ENTRY B of one location: the closure derived from its exclusion list. `because`: the wording of the same water
    whose words say it, when this one only says "except:"."""
    lid = str(loc["location_id"])
    year_round = bool(YEAR_ROUND_RE.search(_text(loc))) or because is not None
    words = "No fishing, year-round" if year_round else "No fishing"
    return {"id": lid + SUFFIX, "derived_from": lid, "kind": "closure", "water": loc.get("water"),
            "water_id": loc.get("water_id"), "region": loc.get("region"), "status": loc.get("status") or "active",
            "title": f"{loc.get('water') or ''}: {words.lower()} in {len(_excludes(loc))} named areas".strip(": "),
            # the rule in the catalogue's own shape (what the Region 6 section-F closure carries): all salmon, take 0
            "rule": {"type": "retention_limit", "species": ["SA"], "take": 0, "may_target": False, "verbatim": words,
                     **({"when": {"dates": [{"from_month": 1, "from_day": 1, "to_month": 12, "to_day": 31}]}}
                        if year_round else {})},
            "areas": areas(loc), "source_words": _text(loc),
            **({"said_by": because} if because else {})}


def derive(locations: Iterable[Mapping]) -> dict[str, dict]:
    """every closure entry the stored rows give: location id + "#closed" -> entry B. Pure."""
    locs = [l for l in locations if _excludes(l)]
    said = {(l.get("water_id"), _excludes(l)): str(l["location_id"]) for l in locs if says_closed(l)}
    out: dict[str, dict] = {}
    for l in locs:
        if says_closed(l):
            e = closure_entry(l)
        elif (l.get("water_id"), _excludes(l)) in said:      # "…, except:" — the list is another wording's closure list
            e = closure_entry(l, because=said[(l.get("water_id"), _excludes(l))])
        else:
            continue
        out[e["id"]] = e
    return out


def main(argv: Optional[list[str]] = None) -> int:
    import argparse
    from pipeline.common.curated import GENERATED
    from pipeline.regs.review import dfo as D
    ap = argparse.ArgumentParser(description="derive the closure entries (entry B) from the stored DFO rows' exclusion lists")
    ap.add_argument("--out", type=Path, default=Path(GENERATED.review) / "dfo_closure_entries.json")
    a = ap.parse_args(argv)
    got = derive(l for _s, l, _w in D.locations(D.default_dir()).values())
    doc = {"_about": "ENTRY B per DFO place that names closed areas inside its words (pipeline.regs.dfo_salmon.closures). "
                     "PROPOSALS: derived, never applied — nothing in the places files or the feed reads this.",
           "entries": got}
    a.out.write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"entries": len(got), "ids": sorted(got)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
