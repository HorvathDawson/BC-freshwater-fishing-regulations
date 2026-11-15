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

This module DERIVES entry B from the stored rows (pure: no fetch, no feed, no places file is written). `main`
writes the derived entries to `<generated>/review/dfo_closure_entries.json` as PROPOSALS for the review app. THE BUILD
SIDE (T573, 2026-11-15, "THE BUILD SIDE" below): each LIVE closed area becomes a DFO location of the build
(`build_pieces`) — bound by the reach run when the owner confirmed it, carried on its water otherwise — and its rule
reaches the bundle and the live feed; still no places file holds it.

An inline "except closed from <A> to <B>" in the place's own words (T743, the middle Shuswap) is one more stretch: the
scrape never makes it an exclusion (`inline_items`).

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

#: AN EXCEPTED STRETCH INSIDE THE PLACE'S OWN WORDS (T743, owner 2026-11-15, the middle Shuswap: "(middle) between
#: Shuswap Falls and Mabel Lake except closed from the water survey station (located at 50°19.105' N …) … to WTFB signs
#: (located at 50°18.181' N …) one km down stream."). The scrape fills `source_text.excludes` only from the cell's
#: `<li>` bullets (parse → untangle → locations, and the feeds worker's twin), so a clause written inline is never
#: one: it is read HERE, from the stored words, as one excluded stretch. Only "except closed / except no fishing from
#: <A> to <B>" — an "except in the areas described below" is a pointer to other rows (Nanaimo, Nitinat, Dean, Skeena),
#: and the Stave's "except you shall not fish for salmon in that portion … known as the Ruskin Spawning Channel …" names
#: side channels that DFO also lists as rows of their own (reading it here would close them twice).
INLINE_RE = re.compile(r"\bexcept\s+(?P<verb>closed|no\s+fishing)\s+(?P<words>from\s+.+?)\s*(?:\bnote\s*:.*)?$",
                       re.I | re.S)
#: a printed coordinate, degrees + decimal minutes (seconds optional): 50°19.105' N, 118°48.857' W
COORD_RE = re.compile(r"(?P<deg>\d{1,3})\s*°\s*(?P<min>\d{1,2}(?:\.\d+)?)\s*['’′]\s*"
                      r"(?:(?P<sec>\d{1,2}(?:\.\d+)?)\s*[\"”″]\s*)?(?P<hem>[NSEW])\b")


def coords_in(words: str) -> list[dict]:
    """every printed coordinate pair in `words`, AS PRINTED and converted exactly: {printed, lat, lon} (decimal degrees,
    W and S negative). A latitude with no longitude after it is kept with `lon` None (never guessed)."""
    out, lat = [], None
    for m in COORD_RE.finditer(words):
        v = int(m.group("deg")) + float(m.group("min")) / 60.0 + float(m.group("sec") or 0) / 3600.0
        hem = m.group("hem").upper()
        v = round(-v if hem in "SW" else v, 7)
        if hem in "NS":
            if lat is not None:
                out.append({"printed": lat[0], "lat": lat[1], "lon": None})
            lat = (m.group(0).strip(), v)
        elif lat is not None:
            out.append({"printed": f"{lat[0]} {m.group(0).strip()}", "lat": lat[1], "lon": v})
            lat = None
    if lat is not None:
        out.append({"printed": lat[0], "lat": lat[1], "lon": None})
    return out


def _split_from_to(words: str) -> Optional[tuple[str, str]]:
    """"from <A> to <B>": the first " to " outside parentheses after "from" (a printed "(located at …)" holds none)"""
    w = re.sub(r"^\s*from\s+", "", words, flags=re.I)
    depth = 0
    for i, ch in enumerate(w):
        depth += ch == "("
        depth -= ch == ")" and depth > 0
        if depth == 0 and re.match(r"\s(?:down(?:stream)?\s+|up(?:stream)?\s+)?to\s", w[i:], re.I):
            a, b = w[:i], re.sub(r"^\s(?:down(?:stream)?\s+|up(?:stream)?\s+)?to\s+", "", w[i:], flags=re.I)
            return a.strip(" ,;"), b.strip(" ,;.")
    return None


def _head(end: str) -> str:
    """an end's landmark, before its "(located …)", ", " or "approximately …": "the water survey station" """
    return re.split(r"\s*\(|,|\s+(?:approximately|approx\.?|located)\b", end, maxsplit=1, flags=re.I)[0].strip()[:60]


def _inline_key(item: str) -> str:
    """THE KEY OF AN INLINE ITEM: `piece_key` of its words with DFO's typing slips aside ("( located" / "(located",
    "200m" / "200 m": the two stored Shuswap wordings), so one closed stretch is one piece across them"""
    return piece_key(re.sub(r"(\d)\s*(k?m)\b", r"\1 \2", re.sub(r"\(\s+", "(", item)))


def inline_items(loc: Mapping) -> list[dict]:
    """THE EXCEPTED STRETCHES WRITTEN INSIDE THE PLACE'S WORDS (`INLINE_RE`), only when the scrape stored no bulleted
    list (a bulleted list is the row's list; never both): [{item, verb, ends: [{words, coords}, …] | None}]"""
    if (loc.get("source_text") or {}).get("excludes"):
        return []
    m = INLINE_RE.search(_text(loc))
    if m is None:
        return []
    item = re.sub(r"\s+", " ", f"{m.group('verb')} {m.group('words')}").strip().rstrip(".").strip()
    ft = _split_from_to(m.group("words"))
    ends = None if ft is None else [{"words": e, "coords": coords_in(e)} for e in ft]
    return [{"item": item, "verb": m.group("verb").lower(), "ends": ends}]


#: THE OWNER'S READING of words DFO leaves open, per water (never guessed in code): Babine Lake's row only "exclud[es] …
#: those waters" and says "closed"; owner 2026-11-10: "babine is closed in the circles (no fishing salmon)". Shown as
#: his reading, beside DFO's own word.
OWNER_READINGS: dict[str, dict] = {
    "6:babine-lake": {"verbatim": "No fishing for salmon", "of": "Closed",
                      "reading": "the owner's reading, 2026-11-10, of DFO's word “closed”: “babine is closed in the "
                                 "circles (no fishing salmon)”"},
}


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
    if inline_items(loc):           # "except closed from … to …": the clause itself says closed
        return True
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
    for it in inline_items(loc):      # an inline "except closed from A to B": one stretch, each end with its printed points
        ends = it["ends"]
        name = (f"{_head(ends[0]['words'])} to {_head(ends[1]['words'])}" if ends else it["item"][:60])
        out.append({"key": _inline_key(it["item"]), "item": it["item"], "kind": "stretch" if ends else "unknown",
                    "name": name, "words": it["item"], "inline": True, **({"ends": ends} if ends else {})})
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
    read = OWNER_READINGS.get(str(loc.get("water_id")))
    read = read if read is not None and read["of"] == words else None      # only of the words it was a reading of
    dfo_words, words = words, (read["verbatim"] if read else words)
    return {"id": lid + SUFFIX, "derived_from": lid, "kind": "closure", "water": loc.get("water"),
            "water_id": loc.get("water_id"), "region": loc.get("region"), "status": loc.get("status") or "active",
            "title": (f"{loc.get('water') or ''}: {words.lower()} in {len(areas(loc))} named "
                      f"area{'' if len(areas(loc)) == 1 else 's'}").strip(": "),
            # the rule in the catalogue's own shape (what the Region 6 section-F closure carries): all salmon, take 0
            "rule": {"type": "retention_limit", "species": ["SA"], "take": 0, "may_target": False, "verbatim": words,
                     **({"when": {"dates": [{"from_month": 1, "from_day": 1, "to_month": 12, "to_day": 31}]}}
                        if year_round else {}),
                     **({"dfo_words": dfo_words, "reading": read["reading"]} if read else {})},
            "areas": areas(loc), "source_words": _text(loc),
            **({"said_by": because} if because else {})}


def derive(locations: Iterable[Mapping]) -> dict[str, dict]:
    """every closure entry the stored rows give: location id + "#closed" -> entry B. Pure."""
    locs = [l for l in locations if _excludes(l) or inline_items(l)]
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
    kind: Literal["stretch", "circle", "line"]
    on_item: str = Field(..., min_length=1)
    points: Optional[list[FixPointRef]] = None
    #: a stretch end that is a NEW cut: its definition (`marks.NewSplit`'s fields: ref, anchor, on_item, coord, …),
    #: carried with the piece so the record stands alone (validated as a NewSplit by the mark that saves it)
    new_splits: Optional[list[dict]] = None
    centre: Optional[tuple[float, float]] = None
    radius_m: Optional[float] = Field(default=None, gt=0, le=20000)
    #: a LINE across a lake ("closed east of a line from … to …"): its two ends, [lon, lat] each — placed by hand
    line: Optional[list[tuple[float, float]]] = None
    centre_from: Optional[str] = None
    confidence: Optional[Literal["high", "medium", "low"]] = None
    note: str = ""

    @model_validator(mode="after")
    def _shape(self) -> "PieceGeometry":
        def in_bc(pt) -> None:
            lon, lat = pt
            if not (-140.0 <= lon <= -113.0 and 47.0 <= lat <= 61.0):
                raise ValueError(f"a point is [lon, lat] in British Columbia, not {tuple(pt)}")
        if self.kind == "stretch":
            if (not self.points or len(self.points) != 2 or self.centre is not None or self.radius_m is not None
                    or self.line is not None):
                raise ValueError("a stretch has exactly two ends and no centre, radius or line")
            refs = {n.get("ref") for n in self.new_splits or []}
            lost = [p.split_id for p in self.points if p.kind == "new" and self.new_splits is not None and p.split_id not in refs]
            if lost:
                raise ValueError(f"a stretch's new end is defined with it: {lost} is not among its new cuts")
        elif self.kind == "circle":
            if self.centre is None or self.radius_m is None or self.points or self.line is not None or self.new_splits:
                raise ValueError("a circle has a centre and a radius and no ends")
            in_bc(self.centre)
        else:
            if (not self.line or len(self.line) != 2 or self.points or self.centre is not None
                    or self.radius_m is not None or self.new_splits):
                raise ValueError("a line has exactly two ends [lon, lat] and nothing else")
            for pt in self.line:
                in_bc(pt)
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


def water_areas(entries: Mapping[str, Mapping], water_id) -> list[dict]:
    """every excluded item ANY stored wording of the water names (first seen wins), for `attach`'s `others`"""
    seen: dict[str, dict] = {}
    for e in entries.values():
        if e.get("water_id") == water_id:
            for a in e.get("areas") or []:
                seen.setdefault(a["key"], a)
    return list(seen.values())


def attach(entry: Mapping, geometry: Iterable[Mapping], live: Optional[Iterable[str]] = None,
           others: Optional[Iterable[Mapping]] = None) -> list[dict]:
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
    `others`: the items OTHER stored wordings of this water name (areas, any order). GENERAL RULE (owner 2026-11-10,
    Pinkut Creek: "we should have both mapped out in case they go back to old wording"): every excluded item of ANY
    stored wording is listed, mappable and reviewable — one this row does not name is "not live" (`held`: placed /
    unplaced says whether a placement is on file for it), and only live ones take effect. Every row carries `live`.
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
            elif state == "placed" and r.kind != a["kind"] and a["kind"] != "unknown":
                state, note = "changed", f"stored as a {r.kind}, DFO's words now read as a {a['kind']}"
            elif state == "changed":
                note = f"curated for “{r.item}”; DFO's words are now “{a['item']}”"
        held = state
        if live is not None and a["key"] not in live:
            state, note = "not live", "DFO's live page no longer names it: kept ready, never bound"
        out.append({**a, "state": state, "held": held, "live": state != "not live", "note": note,
                    "geometry": r.model_dump(mode="json") if r is not None else None})
    named = {a["key"] for a in entry.get("areas") or []}
    for a in others or []:                      # an older (or another) wording's item this row does not name
        if a["key"] in named or a["key"] in used:
            continue
        r = by_key.get(a["key"])
        named.add(a["key"])
        if r is not None:
            used.add(r.key)
        out.append({**a, "state": "not live", "held": "placed" if r is not None else "unplaced", "live": False,
                    "note": "only an older wording names it: kept ready, never bound",
                    "geometry": r.model_dump(mode="json") if r is not None else None})
    for r in recs:
        if r.key not in used:
            out.append({"key": r.key, "item": r.item, "kind": r.kind, "name": r.name, "words": r.item,
                        "state": "not live", "held": "placed", "live": False,
                        "note": "no stored wording names it any more: kept ready, never bound",
                        "geometry": r.model_dump(mode="json"), **({"radius_m": r.radius_m} if r.kind == "circle" else {})})
    return out


# --------------------------------------------------------------------------- THE BUILD SIDE (T573 build step, 2026-11-15)
#
# WHERE ENTRY B ENTERS: one DFO LOCATION PER LIVE CLOSED AREA, beside the curated places — never written into a places
# file. The reach run binds each (`pipeline.atlas.reach.dfo_salmon.resolve_closures`), the bundle puts it in the salmon
# chains with its own rule (`deliver.salmon.load_inputs` -> `with_pieces`), and the live feed computes its calendar
# from the live row (`live.run` -> `live_piece_rules`). Each piece has its PLACE'S TIER (`precedence`) and covers fewer
# sections than its place, so `deliver.salmon.chain_order` ("narrowest first": tier, then size) puts it before the
# place on its sections — no new precedence rule. Its id is the WATER's, not a wording's (`piece_id`: the same item
# of any wording is one piece, as the review mark's target `<water_id>#<key>` is).
#
#   bound      a live item confirmed (`closed_area` mark -> closed_areas.json) for exactly these words: a stretch is
#              `between` its two cuts on its water (`build_reach`); a circle binds the sections wholly inside it
#   carried    every other live item — unconfirmed, reworded, radius differs, a line across a lake (deferred), a
#              stretch whose cut is not in the atlas yet, a circle's sections only partly inside it: on its water with
#              the caveat (the chain's `u`, "parts not mapped"), NEVER widened, never vanished
#   absent     an item no live row names (`attach`: "not live"): its geometry stays on file, nothing is built

from dataclasses import dataclass, field  # noqa: E402

from pipeline.regs.dfo_salmon.entries import EntryLocation  # noqa: E402

PIECE_SEP = SUFFIX + ":"

#: why a live closed area binds nothing — said on its reach row (`closure.why`) and printed by the run
WHY_UNBOUND = {
    "unplaced": "not confirmed (no verified closed_area mark): carried on its water",
    "changed": "DFO's words changed since it was confirmed: carried on its water until confirmed again",
    "radius": "the stored circle's radius is not the one DFO's words give: carried on its water",
    "line": "a line across a lake is deferred (T573): carried on its water",
    "unknown": "DFO's words for it are of no known shape: carried on its water",
}


@dataclass
class ClosureLocation(EntryLocation):
    """ONE CLOSED AREA OF ENTRY B as a DFO location of the build (`build_pieces`). No fingerprint: the live feed joins
    it through its place's row (`live_piece_rules`), never by a wording of its own."""
    derived_from: str = ""
    piece_key: str = ""
    piece_kind: str = ""
    state: str = ""
    #: "" when it can bind; else why it is carried on its water (`WHY_UNBOUND`)
    why: str = ""
    closure_rule: dict = field(default_factory=dict)
    geometry: Optional[dict] = None


def piece_id(water_id: str, key: str) -> str:
    """THE LOCATION ID OF ONE CLOSED AREA: `<water_id>#closed:<piece key>`"""
    return f"{water_id}{PIECE_SEP}{key}"


def is_piece_id(location_id: str) -> bool:
    return PIECE_SEP in str(location_id)


def piece_rule(entry: Mapping, area: Mapping, lid: str) -> dict:
    """THE RULE OF ONE PIECE: entry B's rule ("No fishing, year-round", "No fishing for salmon", …), its words followed
    by DFO's own words for the area (a stretch: the item as printed; a circle: its radius and the creek it names)."""
    rule = dict(entry["rule"])
    words = area["words"] if area.get("kind") == "circle" else area["item"]
    return {**rule, "rule_id": f"{lid}#0", "verbatim": f"{rule['verbatim']} — {words}"}


def _active_entries(locations: Iterable[Mapping]) -> list[dict]:
    """entry B of every ACTIVE row (a dormant wording builds nothing of its own), in id order — the order that decides
    which row a piece of two live wordings takes its place's tier from"""
    got = derive(locations)
    return [got[k] for k in sorted(got) if (got[k].get("status") or "active") == "active"]


def build_pieces(locations: Iterable, confirmed: Iterable[Mapping]) -> list[ClosureLocation]:
    """EVERY LIVE CLOSED AREA OF ONE REGION as a `ClosureLocation` (pure). `locations`: the curated places
    (`entries.EntryLocation`); `confirmed`: the owner's confirmed placements (`review.closed_areas.confirmed()`). An
    unconfirmed or unbuildable item is a piece too, `why` set (carried, never dropped); an item no live row names is
    none (`attach`: never free-standing)."""
    locs = [l for l in locations if hasattr(l, "to_dict") and not isinstance(l, ClosureLocation)]
    by_id = {l.location_id: l for l in locs}
    geo = list(confirmed)
    out, seen = [], set()
    for e in _active_entries([l.to_dict() for l in locs]):
        parent = by_id[e["derived_from"]]
        for row in attach(e, geo, live=live_keys({e["id"]: e}, e["water_id"])):
            if not row["live"] or (e["water_id"], row["key"]) in seen:
                continue
            seen.add((e["water_id"], row["key"]))
            out.append(_piece_location(parent, e, row))
    return out


def _piece_location(parent, entry: Mapping, row: Mapping) -> ClosureLocation:
    from pipeline.regs.dfo_salmon.entries import Binding
    from pipeline.regs.parsing.entry_models import Extent
    lid = piece_id(entry["water_id"], row["key"])
    g = row.get("geometry")
    kind = g["kind"] if g else row["kind"]
    why, extents = "", []
    if row["state"] != "placed":
        why = WHY_UNBOUND.get(row["state"], WHY_UNBOUND["unplaced"])
    elif kind == "line":
        why = WHY_UNBOUND["line"]
    elif kind == "stretch":
        extents = [Extent(op="between", splits=[p["split_id"] for p in g["points"]], item_id=g["on_item"])]
    elif kind != "circle":
        why = WHY_UNBOUND["unknown"]
    return ClosureLocation(
        location_id=lid, region=parent.region, region_number=parent.region_number, kind="closure",
        precedence=parent.precedence, water=parent.water, water_id=parent.water_id, section=parent.section,
        source_text={"specific_area": row["words"] if row.get("kind") == "circle" else row["item"]},
        binding=Binding(extents=extents, tributaries=False), status="active",
        derived_from=parent.location_id, piece_key=row["key"], piece_kind=kind, state=row["state"], why=why,
        closure_rule=piece_rule(entry, row, lid), geometry=g if row["state"] == "placed" else None)


def with_pieces(ef, confirmed: Iterable[Mapping]):
    """the entry file with its region's live closed areas appended (`build_pieces`) — what the BUNDLE reads
    (`deliver.salmon.load_inputs`). Never saved: a places file never holds a piece."""
    import copy
    pieces = build_pieces(getattr(ef, "locations", ()), confirmed)
    if not pieces:
        return ef
    out = copy.copy(ef)
    out.locations = list(ef.locations) + pieces
    return out


def live_piece_rules(ef, found: Iterable) -> dict[str, list[dict]]:
    """THE LIVE FEED'S PIECES (`live.run`): piece id -> [its rule], for every item TODAY'S page names — read from the
    live rows (`found`, the page's scraped locations, joined to their curated place by fingerprint), so a piece DFO
    dropped leaves the feed on the run it goes (the release's piece is still in the chain, the feed says nothing for
    it: the place answers). Confirmation is the release's matter (bound or carried); the feed carries every live one."""
    index = ef.by_fingerprint()
    rows = []
    for f in found:
        hit = index.get(f.fingerprint)
        if hit is None or not hit.water_id:
            continue
        rows.append({"location_id": hit.location_id, "water_id": hit.water_id, "water": hit.water,
                     "region": hit.region, "status": "active",
                     "source_text": {"specific_area": f.specific_area, "excludes": list(f.excludes or [])}})
    out: dict[str, list[dict]] = {}
    for e in _active_entries(rows):
        for a in e["areas"]:
            lid = piece_id(e["water_id"], a["key"])
            out.setdefault(lid, [piece_rule(e, a, lid)])
    return out


def circle_sections(centre, radius_m: float, shapes: Mapping[str, object]) -> tuple[list[str], list[str]]:
    """(inside, touched): of `shapes` (section -> its EPSG:3005 geometry: a lake's outline or a line), those wholly
    inside the circle at true ground radius (`circle_polygon`) and those it only touches. A section partly inside is
    never bound whole (a 400 m circle on Babine Lake would close the lake): it is touched, and carried."""
    from pyproj import Transformer
    from shapely.geometry import Polygon
    to = Transformer.from_crs("EPSG:4326", "EPSG:3005", always_xy=True)
    circ = Polygon([to.transform(x, y) for x, y in circle_polygon(tuple(centre), radius_m)])
    inside, touched = [], []
    for s in sorted(shapes):
        g = shapes[s]
        if g is None or g.is_empty or not g.intersects(circ):
            continue
        (inside if g.within(circ) else touched).append(s)
    return inside, touched


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
