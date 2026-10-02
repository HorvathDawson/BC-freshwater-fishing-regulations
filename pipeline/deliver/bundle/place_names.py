"""Where a bound rule applies, in the book's words — the `place_of` a rule's label is built with.

`catalogue.label` names a rule's place only from `extent_text`, so a rule bound by a cut-point or an
area never said where it was: 226 labels read exactly "No fishing", and 18 pairs of rules in one
entry shared a label although they covered different water. The names are in the atlas — every
registry item carries its cut-points' labels (`boundaries`), and every area item its name — so the
label stays a pure function of the rule's fields plus the atlas's own names, and this module is the
one place that reads them.

THE BOOK'S NAMES, NEVER A GAUGE'S. A cut-point first written as a hydrometric station
(`gauge__08HB006 · Puntledge River At Courtenay`) was later authored where the book names it
("signs 75 m downstream of Puntledge River hatchery fence"); the registry keeps the authored split
canonical and the gauge id as its alias. A rule still naming the gauge id is named by that curated
label. Where no curated name exists the place is NOT printed — the label keeps the bare wording and
the rule is logged (`unnamed`) — rather than telling an angler to find a gauge number on the bank.
The same holds for a cut-point with no name at all ("lake 329465442", a `length:` id).
"""

from __future__ import annotations

import re

#: A Water Survey of Canada station number at the head of a label: "08HB006 · …".
_GAUGE_LABEL = re.compile(r"^\d{2}[A-Z]{2}\d{3}\b")
#: A cut-point that has no name: an unnamed lake, a measure, or a label that is its own id.
_NO_NAME = re.compile(r"^(lake \d+|length:\S+|area:\S+|point|within)$", re.I)
#: A CURATOR'S NOTE, NOT A NAME. Cut-point labels were written as working notes — "boundary signs
#: = QFN", "Koocanusa Reservoir u/s end", "main logging road bridge ~2.4km d/s Chehalis Lk", "old
#: Robson Ferry landing ↔ south-bank sign", a label cut at "…and th", an offset pasted after a comma
#: or an "about". The book prints none of these (it spells out "upstream", "Creek", "approximately"),
#: so such a label names no place — the rule keeps its bare wording and is logged, as for a gauge.
_SHORTHAND = re.compile(r"~|=|↔|\bu/s\b|\bd/s\b|\b(Lk|Ck|confl)\b|\bup of\b|\bor within\b"
                        r"|\b(th|about)\s*\(|,\s*\(|^\s*\(|\bth$|^within\b")
#: "<place> (<n> m upstream)": a cut-point placed at an offset from a named place.
_OFFSET = re.compile(r"^(?P<base>.*?)\s*\((?P<off>\d[\d,.]*\s*m\s+(?:upstream|downstream)[^()]*)\)\s*$")

#: `area_kind` — a FAMILY of areas — in words. A region is no place in a label: the rule's region
#: is the page it is on, so "in Region 4" on every zone rule says nothing.
_AREA_KIND_WORDS = {
    "national_parks": "national parks", "ecological_reserves": "ecological reserves",
    "park": "provincial parks", "wma": "wildlife management areas",
    "indigenous_land": "Indian Reserve land", "restricted_land_access": "restricted-access land",
    "permit_land_access": "permit-access land",
}


def _title(name: str) -> str:
    """An all-capitals name as a reader writes it: "TWEEDSMUIR PARK" -> "Tweedsmuir Park". A lone
    short word is an acronym and stays one: "CPR", not "Cpr"."""
    if name and name == name.upper() and any(c.isalpha() for c in name) \
            and not (len(name.split()) == 1 and len(name) <= 4):
        return " ".join(w.capitalize() if w.isalpha() else w.title() for w in name.split())
    return name


def _named(label: str, bid: str) -> bool:
    return bool(label) and label != bid and not _GAUGE_LABEL.match(label) \
        and not _NO_NAME.match(label) and not _SHORTHAND.search(label)


def _bare(name: str) -> str:
    """A place's name for comparison: "the cascade falls" and "cascade falls" are one place."""
    return re.sub(r"^the\s+", "", name.strip(), flags=re.I).lower()


def _between(a: str, b: str) -> str:
    """Two cut-points: "between A and B" — or, when both are offsets from ONE place, the place said
    once ("from Dickson Falls to 30 m downstream", not "between Dickson Falls and Dickson Falls
    (30 m downstream)")."""
    ma, mb = _OFFSET.match(a), _OFFSET.match(b)
    base_a, off_a = (ma["base"], ma["off"]) if ma else (a, None)
    base_b, off_b = (mb["base"], mb["off"]) if mb else (b, None)
    if _bare(base_a) == _bare(base_b) and (off_a or off_b):
        if off_a and off_b:
            return f"from {off_a} to {off_b} of {base_a}"
        return f"from {base_a} to {off_a or off_b}"
    return f"between {a} and {b}"


class PlaceNamer:
    """`namer(extents, matched=()) -> str | None`: the place a rule's extents draw, or `None` when
    they draw none worth naming (the whole of the entry's own water, a region) or cannot be named
    in the book's words.

    `registry` maps item id -> an item with `.name` and `.boundaries` (each with `.id`, `.label`,
    `.kind`, `.aliases`), as `pipeline.atlas.registry.load_registry` returns it; `split_labels`
    maps a curated split id to its label (`splits.json`)."""

    def __init__(self, registry, split_labels: dict[str, str] | None = None,
                 area_names: dict[str, str] | None = None):
        self.registry = registry
        self.split_labels = dict(split_labels or {})
        #: {area id: its printed name} from the atlas's `area_catalog.gpkg` (`area_names`). A
        #: registry area's `name` is its own id, so without these no area could be named and
        #: "No Fishing within Garibaldi Park" read "No fishing".
        self.area_names = dict(area_names or {})
        self.by_id: dict[str, list[tuple[str, object]]] = {}
        self.by_alias: dict[str, list[tuple[str, object]]] = {}
        for iid, it in registry.items():
            for b in getattr(it, "boundaries", None) or ():
                self.by_id.setdefault(b.id, []).append((iid, b))
                for a in getattr(b, "aliases", None) or ():
                    self.by_alias.setdefault(str(a).removeprefix("split:"), []).append((iid, b))
        #: (split id, why) for every cut-point that could not be named — for the build's report.
        self.unnamed: set[tuple[str, str]] = set()

    # ------------------------------------------------------------------ points
    def point(self, sid: str, scope: list[str]) -> tuple[str | None, str | None]:
        """(the cut-point in words, the item it lies on) — words `None` when it has no book name."""
        cands = self.by_id.get(sid) or self.by_alias.get(sid) or []
        if scope:
            cands = [c for c in cands if c[0] in scope] or cands
        if not cands:
            self.unnamed.add((sid, "not a cut-point in this atlas"))
            return None, None
        iid, b = cands[0]
        labels = [b.label] + [self.split_labels.get(str(a).removeprefix("split:"), "")
                              for a in (getattr(b, "aliases", None) or ())]
        if sid != b.id:
            labels.append(self.split_labels.get(b.id, ""))
        name = next((x for x in labels if _named(x, b.id)), None)
        if name is None:
            self.unnamed.add((sid, f"no book name (atlas label {b.label!r})"))
            return None, iid
        kind = str(getattr(b, "kind", "") or "")
        if kind == "confluence" or "→" in name:
            # "Morrison Creek → Puntledge River (100 m downstream)" keeps its offset.
            tail = re.search(r"\s*(\([^()]*\))\s*$", name)
            name = f"the {name.split('→')[0].strip()} confluence" + (f" {tail.group(1)}"
                                                                    if tail else "")
        return _title(name), iid

    # ------------------------------------------------------------------ one extent
    def _item(self, iid: str) -> str | None:
        it = self.registry.get(iid)
        # " — " separates a label's parts, so a part-lake's name reads "Kootenay Lake, Main Body".
        return (_title(it.name).replace(" — ", ", ")
                if it is not None and getattr(it, "name", "") else None)

    def _area(self, aid: str) -> str | None:
        key = aid if aid.startswith("area:") else f"area:{aid}"
        if key.startswith("area:region:"):
            return None
        if key.startswith("area:basin:"):
            # A WATERSHED BY FWA CODE is named for its river ("Williams Lake River watershed"),
            # from the one table that names basins; an unnamed one names no place.
            from pipeline.atlas.registry.basins import basin_name
            name = basin_name(key)
            return _title(name) if name else None
        it = self.registry.get(key)
        name = getattr(it, "name", "") if it is not None else ""
        if not name or name == key:
            name = self.area_names.get(key, "")
            if not name:
                return None
            # a catalogue id in brackets is not part of the name ("… Research Forest
            # [1166294466]"), and " — " separates a label's parts, as in `_item`
            name = re.sub(r"\s*\[\d+\]$", "", name).replace(" — ", ", ")
            if key.startswith("area:watershed:"):
                name = f"{name} watershed"
        return _title(name)

    def extent(self, x: dict, matched: tuple[str, ...]) -> str | None | bool:
        """The place one extent draws: words, `False` when it draws nothing to name (the entry's
        own water, a region), or `None` when it draws a place this cannot name."""
        op = x.get("op")
        scope = list(x.get("item_ids") or ([x["item_id"]] if x.get("item_id") else []))
        other = [i for i in scope if i not in matched]
        on = None
        if other:
            names = [self._item(i) for i in other]
            if any(n is None for n in names):
                return None
            on = " and ".join(names)
        if op == "whole":
            place = on or False
        elif op == "steelhead_waters":
            # THE BOOK'S STEELHEAD WATERS (`reach.steelhead`) — the reach builder's set, no one water
            place = "known steelhead waters"
        elif op == "within":
            if x.get("area_id"):
                aid = str(x["area_id"])
                if aid.startswith(("area:region:", "region:")):
                    return on or False
                area = self._area(aid)
                if area is None:
                    return None
                # a watershed IS a place ("Williams Lake River watershed"); anything else is
                # a place the water is within
                area = area if aid.startswith("area:basin:") else f"within {area}"
                place = (f"{on}, " if on else "") + area
            elif x.get("area_kind"):
                words = _AREA_KIND_WORDS.get(str(x["area_kind"]))
                if x["area_kind"] == "region":
                    return on or False
                if words is None:
                    return None
                place = (f"{on}, " if on else "") + f"in {words}"
            else:
                return None
        elif op in ("upstream_of", "downstream_of", "between"):
            pts = [self.point(s, scope or list(matched)) for s in (x.get("splits") or [])]
            if not pts or any(w is None for w, _ in pts):
                return None
            words = [w for w, _ in pts]
            if op == "between" and len(words) == 2 and words[0].lower() == words[1].lower():
                # Two cut-points under one name ("CNR bridge" and "CNR bridge") draw a reach the
                # words cannot tell apart; the book's own `extent_text` says it better.
                return None
            where = (_between(words[0], words[1]) if op == "between" and len(words) == 2
                     else f"{op.replace('_', ' ')} {words[0]}")
            if not on and not matched:
                # A zone rule names no water of its own; say which water the cut is on.
                on = " and ".join(sorted({n for _, i in pts if i for n in [self._item(i)] if n}))
            if x.get("watershed"):
                # A PART OF A WATERSHED (`Extent.watershed`) is the river's watershed on that side,
                # not the river: "Fraser River watershed, upstream of Williams Lake River".
                river = on or " and ".join(n for n in (self._item(i) for i in (scope or matched))
                                           if n)
                if not river:
                    return None
                on = f"{river} watershed"
            place = f"{on}, {where}" if on else where
        else:
            return None
        if x.get("within_area"):
            # THE WATER'S OWN WHOLE, WITHIN AN AREA, IS A PLACE: "No Fishing within Garibaldi
            # Park" drew nothing to name before the area, and its label read "No fishing".
            area = self._area(str(x["within_area"]))
            if area:
                place = f"{place}, within {area}" if place else f"within {area}"
        return place

    def __call__(self, extents, matched=()) -> str | None:
        got = [self.extent(x, tuple(matched)) for x in extents or [] if isinstance(x, dict)]
        if not got or any(g is None for g in got):
            return None
        words = list(dict.fromkeys(g for g in got if g))
        if not words:
            return None
        # SEVERAL WHOLE WATERS read as one list ("Comox Lake, Cowichan Lake and Horne Lake");
        # anything else is a union of places, one per extent.
        if len(words) > 1 and all(isinstance(x, dict) and x.get("op") == "whole"
                                  and not x.get("within_area") for x in extents):
            return ", ".join(words[:-1]) + " and " + words[-1]
        return "; ".join(words)

    def for_entry(self, matched):
        """`place_of` for one entry's rules."""
        m = tuple(matched or ())
        return lambda extents: self(extents, m)


def area_names(build_dir) -> dict[str, str]:
    """{area id: name} from a build's `area_catalog.gpkg` — the names the atlas gave its areas.
    Read as plain SQLite (no geometry). A build without the catalogue names no area."""
    import sqlite3
    from pathlib import Path
    p = Path(build_dir) / "area_catalog.gpkg"
    if not p.exists():
        return {}
    con = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    try:
        return {a: n for a, n in con.execute("select area_id, name from areas") if a and n}
    finally:
        con.close()


def split_labels(splits_json: dict) -> dict[str, str]:
    """{split id: label} from `splits.json`."""
    return {s["id"]: s.get("label") or "" for w in splits_json.get("waterbodies") or []
            for s in w.get("splits") or [] if s.get("id")}
