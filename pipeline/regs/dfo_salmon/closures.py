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
wording of the same water with the identical list).

ONE MODEL, TWO SHAPES (owner 2026-11-10: "i think we could model babine similar? … draw circles at the right radius"):
an excluded area is a PIECE of entry B, keyed to its own item in the row's exclusion list (`piece_key`), and is

    stretch   between two cuts on a stream         the Thompson: "Deadman - from … to …"
    circle    a centre + a radius in metres        Babine Lake: "within a 400 m radius of … Morrison Creek, …"
    line      a line across a lake                 Babine: "closed east of a line from Gullwing Creek to south shore"
                                                   (named and listed; not placed — a lake divided by a line is deferred)

ENTRY B IS NEVER FREE-STANDING (owner 2026-11-10: "circles should only show up when that entry is in the dfo page too
(if they remove it circles should go away)"): `attach` joins the curated / proposed geometry to the LIVE row's items —
per item, not per row. An item DFO dropped keeps its geometry on file marked not live (never drawn, never bound; it
comes back without re-curating if the words return); a reworded item reads "changed, check again"; a radius in the
words that is not the curated one is flagged.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
import hashlib
from typing import Iterable, Literal, Mapping, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

#: the words that make an exclusion list a CLOSURE
CLOSED_RE = re.compile(r"\bno\s+fishing\b|\bclosed\b|\bclosure\b", re.I)
#: "… year-round" in the place's words: the closure has no season
YEAR_ROUND_RE = re.compile(r"year[\s-]*round", re.I)
#: "Deadman - from the fishing boundary signs …": the area's name, then DFO's words for it
AREA_RE = re.compile(r"^\s*(?P<name>[^-–—:]{2,60}?)\s*[-–—:]\s+(?P<words>.+?)\s*$", re.S)
SUFFIX = "#closed"
#: "within a 400 m radius of the following tributary streams" — the radius of every circle the list names
RADIUS_RE = re.compile(r"within\s+a?\s*(?P<m>\d+(?:\.\d+)?)\s*(?P<unit>km|m)\b[^.:]*?\bradius\b", re.I)
#: "(Also) closed east of a line from Gullwing Creek to south shore of Babine Lake"
LINE_RE = re.compile(r"(?:also\s+)?closed\s+(?P<words>(?:east|west|north|south)\w*\s+of\s+a\s+line[^.]*)", re.I)


def piece_key(text: str) -> str:
    """THE KEY OF ONE EXCLUDED ITEM: its words as DFO wrote them, case and spacing aside. A reworded item is another key
    (it reads "changed", never silently kept)."""
    return hashlib.sha1(re.sub(r"\s+", " ", text.strip().lower()).encode()).hexdigest()[:12]


def radius_m(loc: Mapping) -> Optional[float]:
    """the radius the place's words give its list of mouths, in metres"""
    m = RADIUS_RE.search(_text(loc))
    return None if m is None else float(m.group("m")) * (1000.0 if m.group("unit").lower() == "km" else 1.0)


def _text(loc: Mapping) -> str:
    return str((loc.get("source_text") or {}).get("specific_area") or "")


def _excludes(loc: Mapping) -> tuple[str, ...]:
    return tuple(e.strip() for e in (loc.get("source_text") or {}).get("excludes") or [] if isinstance(e, str) and e.strip())


def says_closed(loc: Mapping) -> bool:
    """the place's own words (or an exclusion's) say no fishing / closed, AND the list is one of the two shapes: named
    stretches ("Deadman - from … to …": at least one) or mouths with a radius ("within a 400 m radius of …")"""
    ex = _excludes(loc)
    return (bool(ex) and (any(AREA_RE.match(e) for e in ex) or radius_m(loc) is not None)
            and bool(CLOSED_RE.search(_text(loc) + " " + " ".join(ex))))


def areas(loc: Mapping) -> list[dict]:
    """EACH EXCLUDED ITEM AS A PIECE {key, item, kind, name, words, radius_m?}: "Deadman - from … to …" is a stretch
    named Deadman; under "within a 400 m radius of …" a bare name is a circle of that radius at that stream's mouth;
    "closed east of a line …" in the place's words is a line. One of no known shape keeps its whole text (never
    dropped) as kind unknown."""
    out = []
    r = radius_m(loc)
    for i, e in enumerate(_excludes(loc), 1):
        m = AREA_RE.match(e)
        if m:
            out.append({"key": piece_key(e), "item": e, "kind": "stretch", "name": m.group("name").strip(),
                        "words": m.group("words").strip()})
        elif r is not None:
            out.append({"key": piece_key(f"{r:g} m radius: {e}"), "item": e, "kind": "circle", "name": e, "radius_m": r,
                        "words": f"within a {r:g} m radius of the mouth of {e}"})
        else:
            out.append({"key": piece_key(e), "item": e, "kind": "unknown", "name": f"area {i}", "words": e})
    for m in LINE_RE.finditer(_text(loc)):
        w = m.group("words").strip()
        out.append({"key": piece_key(w), "item": w, "kind": "line", "name": w[:60], "words": "closed " + w})
    return out


def closure_entry(loc: Mapping, because: Optional[str] = None) -> dict:
    """ENTRY B of one location: the closure derived from its exclusion list. `because`: the wording of the same water
    whose words say it, when this one only says "except:"."""
    lid = str(loc["location_id"])
    year_round = bool(YEAR_ROUND_RE.search(_text(loc))) or because is not None
    # THE RULE IS WHAT DFO'S WORDS SAY for the areas: "no fishing … year-round" (the Thompson), or only that the waters
    # are excluded and "closed" (Babine Lake: no season is named, so none is claimed)
    says_no_fishing = bool(re.search(r"\bno\s+fishing\b", _text(loc), re.I)) or because is not None
    words = ("No fishing, year-round" if year_round else "No fishing") if says_no_fishing else "Closed"
    return {"id": lid + SUFFIX, "derived_from": lid, "kind": "closure", "water": loc.get("water"),
            "water_id": loc.get("water_id"), "region": loc.get("region"), "status": loc.get("status") or "active",
            "title": f"{loc.get('water') or ''}: {words.lower()} in {len(areas(loc))} named areas".strip(": "),
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


# --------------------------------------------------------------------------- the geometry of a piece (one schema)

class FixPointRef(BaseModel):
    """one end of a stretch: an existing cut (`split`) or a new one proposed with it (`new`, by its ref)"""
    model_config = ConfigDict(extra="forbid")
    kind: Literal["split", "new"]
    split_id: str = Field(..., min_length=1)


class PieceGeometry(BaseModel):
    """THE STORED SHAPE OF ONE PIECE OF ENTRY B — one record, a `kind`:

        {"key", "water_id", "name", "item", "kind": "stretch", "points": [end, end], "on_item"}
        {"key", "water_id", "name", "item", "kind": "circle",  "centre": [lon, lat], "radius_m", "on_item",
         "centre_from"?, "confidence"?}

    `key` is `piece_key` of the DFO item it was curated for and `item` those words (what `attach` compares with the
    live row); `on_item` the registry water it lies on. Validated wherever it is read or written."""
    model_config = ConfigDict(extra="forbid")
    key: str = Field(..., pattern=r"^[0-9a-f]{12}$")
    water_id: str = Field(..., min_length=1)
    name: str = Field(..., min_length=1)
    item: str = Field(..., min_length=1)
    kind: Literal["stretch", "circle"]
    on_item: str = Field(..., min_length=1)
    points: Optional[list[FixPointRef]] = None
    centre: Optional[tuple[float, float]] = None
    radius_m: Optional[float] = Field(default=None, gt=0, le=20000)
    centre_from: Optional[str] = None
    confidence: Optional[Literal["high", "medium", "low"]] = None
    note: str = ""

    @model_validator(mode="after")
    def _shape(self) -> "PieceGeometry":
        if self.kind == "stretch":
            if not self.points or len(self.points) != 2 or self.centre is not None or self.radius_m is not None:
                raise ValueError("a stretch has exactly two ends and no centre or radius")
        else:
            if self.centre is None or self.radius_m is None or self.points:
                raise ValueError("a circle has a centre and a radius and no ends")
            lon, lat = self.centre
            if not (-140.0 <= lon <= -113.0 and 47.0 <= lat <= 61.0):
                raise ValueError(f"a circle's centre is [lon, lat] in British Columbia, not {self.centre}")
        return self


def circle_polygon(centre: tuple[float, float], radius_m: float, n: int = 72) -> list[list[float]]:
    """A CIRCLE AT TRUE GROUND RADIUS, as a closed [lon, lat] ring: `n` points each `radius_m` along a geodesic from
    the centre (WGS84). A pixel circle or a degree buffer is wrong away from the equator: at Babine Lake (55° N) a
    degree of longitude is 57 % of a degree of latitude."""
    from pyproj import Geod
    g = Geod(ellps="WGS84")
    lon, lat = centre
    ring = []
    for i in range(n):
        x, y, _b = g.fwd(lon, lat, 360.0 * i / n, radius_m)
        ring.append([round(x, 7), round(y, 7)])
    return ring + [ring[0]]


def live_keys(entries: Mapping[str, Mapping], water_id) -> set[str]:
    """the item keys DFO's LIVE page carries for one water: every piece of every entry B derived from a row that is on
    the page now (`status: active`). A wording that is no longer live shows a piece only while a live row names it."""
    return {a["key"] for e in entries.values() if e.get("water_id") == water_id and (e.get("status") or "active") == "active"
            for a in e.get("areas") or []}


def attach(entry: Mapping, geometry: Iterable[Mapping], live: Optional[Iterable[str]] = None) -> list[dict]:
    """ENTRY B'S PIECES WITH THEIR GEOMETRY, BY THE LIVE ROW (the owner's rule: never free-standing). For each item of
    the row -> its piece plus `geometry` (the stored record, or None) and `state`:

        placed     the item is in the row and its geometry was curated for exactly these words
        changed    the item is in the row under other words than the geometry was curated for (same name): shown as
                   "changed, check again" — the geometry is offered, flagged, never silently kept
        radius     a circle whose words give another radius than the stored one: flagged with both numbers
        unplaced   the item is in the row and nothing is on file for it

    and, after them, every stored record of this water whose item the row no longer names: `state: "not live"` — kept
    on file, NEVER drawn or bound (it comes back as placed if DFO restores the words). `live`: the item keys DFO's live
    page carries for this water (`live_keys`); an item of this entry that is not among them (the entry is an older
    wording and the live row dropped the item, or the whole clause) is not live too. None: the entry IS the live row.
    Pure."""
    live = None if live is None else set(live)
    recs = [PieceGeometry.model_validate(g) for g in geometry if g.get("water_id") == entry.get("water_id")]
    by_key = {r.key: r for r in recs}
    by_name = {r.name.strip().lower(): r for r in recs}
    out, used = [], set()
    for a in entry.get("areas") or []:
        r = by_key.get(a["key"])
        state = "placed" if r is not None else "unplaced"
        if r is None and a["name"].strip().lower() in by_name:
            r, state = by_name[a["name"].strip().lower()], "changed"
        note = ""
        if r is not None:
            used.add(r.key)
            if r.kind == "circle" and a.get("radius_m") is not None and abs(r.radius_m - a["radius_m"]) > 0.5:
                state, note = "radius", f"DFO's words say {a['radius_m']:g} m, the stored circle is {r.radius_m:g} m"
            elif state == "changed":
                note = f"curated for “{r.item}”; DFO's words are now “{a['item']}”"
        if live is not None and a["key"] not in live:
            state, note = "not live", "DFO's live page no longer names it: kept on file, not drawn"
        out.append({**a, "state": state, "note": note, "geometry": r.model_dump(mode="json") if r is not None else None})
    for r in recs:
        if r.key not in used:
            out.append({"key": r.key, "item": r.item, "kind": r.kind, "name": r.name, "words": r.item,
                        "state": "not live", "note": "DFO's row no longer names it: kept on file, not drawn",
                        "geometry": r.model_dump(mode="json"), **({"radius_m": r.radius_m} if r.kind == "circle" else {})})
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
