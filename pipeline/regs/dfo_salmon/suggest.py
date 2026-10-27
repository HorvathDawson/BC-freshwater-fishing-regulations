"""DFO EXTENT SUGGESTIONS (T164 / T210 follow-up, owner 2026-10-27): for every DFO location that is not cleanly
bound, read the notice's words and SUGGEST an extent — never apply one.

    "for DFO it would be good to have an agent go through them and match extents to splits already
     there, or if it's an easy split to find (coord provided, for example)."

Per location (`suggest_one`):

  1. the notice's place words are cut into ENDPOINT PHRASES (`endpoints`): "from A downstream to B",
     "between A and B", "N m upstream and downstream of A", one phrase for upstream_of / downstream_of;
  2. each phrase is matched, best first, against
       * the CURATED cuts already on the water — the registry's boundaries (label, the other names at
         that place, the aliases' and splits.json's names), by distinctive words, numbers and offsets;
       * the owner's PROPOSED splits (`marks.proposals`, every DFO mark, read only — T210 dedupe);
       * the water's own MOUTH ("the tidal boundary", "the confluence with <receiving river>");
       * a NEW split where the data pins it: a tributary the FWA watershed code says flows straight
         into this water (its mouth, exact), a lake the water touches, a coordinate the notice gives,
         a named falls (OSM) beside the water — each with a confidence (high when exact);
  3. the suggestion — op, points, items, tributaries — is written with a confidence and a one-line
     reason. A point is an `ExtentFix` point (`marks.FixPoint`): an existing split, or a NEW split
     (`marks.NewSplit`) referenced by its ref, exactly what the review app's extent-fix editor holds.

The review app (DFO mode) shows the suggestion, draws it, and "accept suggestion" PRE-FILLS the
extent-fix editor; the owner saves it as their mark. Nothing here writes splits.json, the catalogue
or the marks store. `main` writes `handoff/PLAN/curation-review/dfo-suggestions.json` (+ a summary).

The FWA / OSM lookups are an injected `Lookup` (`FwaLookup` reads the FWA GeoPackage by bbox and one
attribute scan); the matcher itself is pure, so the tests run it on real locations with no atlas.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Mapping, Optional, Protocol

from pipeline.common.names import key as name_key
from pipeline.common.utils.wsc import groups, is_strictly_below

CONF_RANK = {"high": 3, "medium": 2, "low": 1, "none": 0}

#: words that say nothing about WHERE (dropped)
GENERIC = {
    "the", "a", "an", "of", "at", "on", "in", "to", "from", "and", "or", "located", "approximately",
    "approx", "about", "near", "just", "side", "fishing", "boundary", "sign", "white", "triangle",
    "triangular", "square", "orange", "either", "both", "line", "between", "two", "waters",
    "mainstem", "portion", "that", "those", "is", "are", "be", "may", "present", "delineate",
    "these", "this", "its", "it", "with", "for", "by", "sport", "marker", "opposite", "shore",
    "shoreline", "end", "edge", "point", "foot", "base", "lower", "upper", "point", "bc", "b",
    "c", "british", "columbia", "including", "river", "creek", "all", "open", "s", "known", "as",
    "body", "water", "called", "found", "site", "area", "e", "w", "n", "total", "distance",
}
#: words that say WHAT kind of place (weak evidence on their own)
TYPE_WORDS = {
    "bridge", "dam", "weir", "falls", "fence", "trestle", "confluence", "mouth", "outlet", "inlet",
    "lake", "powerline", "crossing", "hatchery", "canyon", "tailrace", "station", "pool", "road",
    "street", "avenue", "hwy", "railway", "overpass", "fishway", "channel", "lagoon", "park",
    "gates", "dyke", "intake", "rapids", "buoy", "abutments", "cable", "car", "counting", "logging",
    "generating", "survey", "spawning", "ladder", "launch", "boat", "ramp", "pump", "dams",
}
#: words that say WHICH way (paired with a distance into an offset token)
UP_WORDS = {"upstream", "above", "up", "u/s"}
DOWN_WORDS = {"downstream", "below", "down", "d/s"}

TRIB_RE = re.compile(r"\b((?:[A-Z][A-Za-z'’\-]*\s+){1,3}(?:River|Creek|Slough))\b")
LAKE_RE = re.compile(r"\b((?:[A-Z][A-Za-z'’\-]*\s+){1,3}Lakes?)\b")
FALLS_RE = re.compile(r"\b((?:[\"“]?[A-Z][A-Za-z'’\-]*[\"”]?\s+){1,3}Falls)\b")
#: 52° 58.373'N · 49°41.50' north latitude · 50°15'23.0"N
_DMS = r"(\d{2,3})\s*°\s*(\d{1,2}(?:\.\d+)?)\s*['’′]\s*(?:(\d{1,2}(?:\.\d+)?)\s*[\"”″]\s*)?"
LAT_RE = re.compile(_DMS + r"\s*(?:N\b|north\s+latitude)", re.I)
LON_RE = re.compile(_DMS + r"\s*(?:W\b|west\s+longitude)", re.I)
#: text after these is not the place (exceptions, notes, sign boilerplate)
_CUT_RE = re.compile(
    r"(?:;|\bexcept\b|\bexcluding\b|\bplease note\b|\bnote:|\bsquare boundary signs\b|"
    r"\btriangle sport fishing\b|\bthat are open to\b|\band then perpendicular\b|\bthis is approx)",
    re.I)
#: a phrase that IS the water's own downstream end
MOUTH_RE = re.compile(r"\b(tidal (?:boundary|water)|tidal|the mouth\b|river mouth|its mouth)", re.I)


# --------------------------------------------------------------------------- the words

def _norm(text: str) -> str:
    s = (text or "").replace("’", "'").replace("“", '"').replace("”", '"')
    s = s.lower()
    s = re.sub(r"\bhighway\b|\bhwy\.?", " hwy ", s)
    s = re.sub(r"#\s*", " ", s)
    s = re.sub(r"\bpower\s*lines?\b|\bpowerlines\b", " powerline ", s)
    s = re.sub(r"\btransmission[- ]line\b", " powerline ", s)
    s = re.sub(r"\brail\s*way\b|\brailroad\b", " railway ", s)
    s = re.sub(r"\be\s*&\s*n\b", " en ", s)
    s = re.sub(r"\bb\.\s*c\.\s*", " bc ", s)
    s = re.sub(r"\bu/s\b", " upstream ", s)
    s = re.sub(r"\bd/s\b", " downstream ", s)
    s = re.sub(r"\bdown\s+stream\b", " downstream ", s)
    s = re.sub(r"\bmt\.?\s", " mount ", s)
    s = re.sub(r"\bck\.", " creek ", s)
    s = re.sub(r"\blk\b\.?", " lake ", s)
    s = re.sub(r"(\d+)(st|nd|rd|th)\b", r"\1", s)
    # distances: "2.4 km" / "~2.4km" -> "2400m"; "100 m." / "100m" / "100 metres" -> "100m"
    s = re.sub(r"(\d+(?:\.\d+)?)\s*(?:km|kilometres?|kilometers?)\b",
               lambda m: f" {int(round(float(m.group(1)) * 1000))}m ", s)
    s = re.sub(r"(\d+(?:\.\d+)?)\s*(?:m|metres?|meters?)\b\.?",
               lambda m: f" {int(round(float(m.group(1))))}m ", s)
    return s


_PLURAL = {"signs": "sign", "bridges": "bridge", "dams": "dam", "weirs": "weir", "lakes": "lake",
           "fences": "fence", "crossings": "crossing", "rivers": "river", "creeks": "creek"}


def tokens(text: str) -> list[str]:
    """The phrase as tokens: lower case, abbreviations folded, distances as "<n>m", and a distance
    followed by a direction word as an OFFSET token "<n>m-up" / "<n>m-down"."""
    raw = re.findall(r"[a-z0-9][a-z0-9.'&]*", _norm(text))
    toks = [_PLURAL.get(t.rstrip(".'"), t.rstrip(".'")) for t in raw]
    out: list[str] = []
    i = 0
    while i < len(toks):
        t = toks[i]
        if re.fullmatch(r"\d+m", t):
            ahead = toks[i + 1:i + 3]
            d = next((w for w in ahead if w in UP_WORDS or w in DOWN_WORDS), None)
            if d:
                out.append(f"{t}-{'up' if d in UP_WORDS else 'down'}")
                i += 1
                continue
        out.append(t)
        i += 1
    return out


def _weight(t: str, own: set[str]) -> float:
    if t in ("downstream", "upstream"):
        return 0.5          # only counts when both say it (a "u/s sign" / "d/s sign" pair)
    if t in GENERIC or t in UP_WORDS or t in DOWN_WORDS:
        return 0.0
    if re.fullmatch(r"\d+m-(up|down)", t):
        return 3.0
    if re.fullmatch(r"\d+m", t):
        return 2.0
    if t in own:
        return 0.5
    if t in TYPE_WORDS:
        return 1.0
    if t in ("cnr", "cpr", "bcr", "en", "bc"):
        return 2.0
    if re.fullmatch(r"\d+(\.\d+)?[a-z]?", t):
        return 3.0
    return 3.0


def _distinctive(t: str, own: set[str]) -> bool:
    return _weight(t, own) >= 2.0


# --------------------------------------------------------------------------- the phrases

@dataclass
class Phrase:
    text: str
    role: str = ""          # "from" | "to" | "only"


def _outside_parens(t: str, pos: int) -> bool:
    return t[:pos].count("(") <= t[:pos].count(")")


def _clean(text: str) -> str:
    """The place words only: no exceptions / notes / sign boilerplate after them (a ";" inside a
    coordinate's parentheses is not a cut), no "including ..." clause, and "a line between two
    fishing boundary signs on either side of the river" said as "signs"."""
    t = re.sub(r"\s+", " ", (text or "")).strip()
    for m in _CUT_RE.finditer(t):
        if m.start() > 8 and _outside_parens(t, m.start()):
            t = t[:m.start()]
            break
    t = re.sub(r"\ba line (?:drawn )?between (?:2|two) (?:[\w ]*?)signs?(?: located)?"
               r"(?: on (?:either|both) sides? of the [\w' ]+?(?= \d| at| in| from|$))?", "signs", t, flags=re.I)
    t = re.sub(r"\s*(?:and )?(?:its )?tributaries to that (?:part|portion)\b", "", t, flags=re.I)
    t = re.sub(r",?\s*including\b[^.;]*?(?=,? (?:to|downstream|upstream)\b|$)", "", t, flags=re.I)
    return t.strip(" .,:–-")


def _strip_lead(t: str) -> str:
    return re.sub(r"^(?:that portion of the [\w' ]+? (?:river|creek) |the waters |waters |mainstem waters |"
                  r"those waters |[\w ]+ - |\([a-z]+\) |[A-Z][\w ]+ – )", "", t, flags=re.I).strip()


def endpoints(text: str) -> list[Phrase]:
    """THE NOTICE'S PLACE WORDS AS ENDPOINT PHRASES: two for a reach ("from A downstream to B",
    "between A and B", "N m upstream and downstream of A"), one otherwise."""
    t = _strip_lead(_clean(text))
    if not t:
        return []
    # "N m upstream and (M m) downstream of A" / "N m upstream of and M m downstream of A"
    m = re.search(r"(\d+(?:\.\d+)?\s*(?:m|metres?|meters?|km)?)\s*(upstream|above)\s+(?:of\s+)?and\s+"
                  r"(?:(\d+(?:\.\d+)?\s*(?:m|metres?|meters?|km)?)\s*)?(downstream|below)\s+(?:of\s+|from\s+)?(.+)$",
                  t, re.I)
    if m:
        up, down, place = m.group(1), m.group(3) or m.group(1), m.group(5)
        return [Phrase(f"{up} upstream of {place}", "from"), Phrase(f"{down} downstream of {place}", "to")]
    m = re.search(r"\bbetween (?:the )?(?:fishing )?(?:boundary )?signs (?:located )?(?:at|near|around) (.+)$", t, re.I)
    if m and " and " not in m.group(1):
        return [Phrase(f"upstream sign at {m.group(1)}", "from"), Phrase(f"downstream sign at {m.group(1)}", "to")]
    m = re.search(r"\bbetween\s+(.+?)\s+and\s+(?:the\s+)?(.+)$", t, re.I)
    if m and not re.search(r"\babove and below\b", t, re.I):
        return [Phrase(m.group(1), "from"), Phrase(m.group(2), "to")]
    m = re.search(r"\bbetween\s+(.+?)\s+above and below\s+(.+)$", t, re.I)
    if m:
        return [Phrase(f"above {m.group(2)}", "from"), Phrase(f"below {m.group(2)}", "to")]
    # "A (downstream|upstream) (approx N m) to B" — the first " to " outside parentheses
    depth, cut = 0, None
    for mm in re.finditer(r"[()]|\b(?:downstream |upstream |down stream )?(?:approx(?:imately|\.)? ?[\d.,]+ ?k?m\.? )?to\b", t, re.I):
        g = mm.group(0)
        if g == "(":
            depth += 1
        elif g == ")":
            depth = max(0, depth - 1)
        elif depth == 0 and not re.search(r"(close|next|due|perpendicular|according|prior)\s*$",
                                          t[:mm.start()], re.I):
            cut = mm
            break
    if cut is not None:
        a, b = t[:cut.start()].strip(" ,"), t[cut.end():].strip(" ,")
        a = re.sub(r"^(?:from|between)\s+", "", a, flags=re.I)
        if a and b:
            return [Phrase(a, "from"), Phrase(b, "to")]
    return [Phrase(re.sub(r"^(?:from)\s+", "", t, flags=re.I), "only")]


def direction(text: str) -> Optional[str]:
    """upstream_of / downstream_of from the notice's lead words, or None."""
    t = _strip_lead(_clean(text)).lower()
    if re.match(r"^(?:\w+ )?(?:upstream|above)\b", t) or re.search(r"\bupstream (?:of|from)\b", t[:60]):
        return "upstream_of"
    if re.match(r"^(?:\w+ )?(?:downstream|below)\b", t) or re.search(r"\bdownstream (?:of|from)\b", t[:60]):
        return "downstream_of"
    if re.search(r"\bdownstream\s*$", t):
        return "downstream_of"
    if re.search(r"\bupstream\s*$", t):
        return "upstream_of"
    return None


# --------------------------------------------------------------------------- the candidates

@dataclass
class Cut:
    """A cut already on the water (a registry boundary) or one the owner proposed (a mark)."""
    id: str
    label: str
    kind: str                       # split | confluence | lake | outlet | proposed
    item_id: Optional[str] = None
    names: tuple[str, ...] = ()
    proposal: Optional[dict] = None  # a proposed split: its `NewSplit` fields


@dataclass
class Water:
    """The location's water: its registry items, with their names and watershed codes."""
    items: list[str]
    names: dict[str, str]
    wsc: dict[str, list[str]] = field(default_factory=dict)
    kind: dict[str, str] = field(default_factory=dict)


class Lookup(Protocol):
    """What the data pins (FWA / OSM / the registry by name). Each returns None when it cannot."""
    def stream(self, name: str) -> list[dict]: ...           # registry streams of this name
    def lake(self, name: str) -> list[dict]: ...             # registry lakes of this name
    def mouth(self, item_id: str, on_items: list[str]) -> Optional[dict]: ...
    def snap(self, lonlat: tuple[float, float], on_items: list[str]) -> Optional[dict]: ...
    def falls(self, name: str, on_items: list[str]) -> Optional[dict]: ...


class NoLookup:
    def stream(self, name): return []
    def lake(self, name): return []
    def mouth(self, item_id, on_items): return None
    def snap(self, lonlat, on_items): return None
    def falls(self, name, on_items): return None


def own_words(water: Water) -> set[str]:
    out: set[str] = set()
    for n in water.names.values():
        out |= {t for t in tokens(n) if t not in TYPE_WORDS and t not in GENERIC}
    return out


def score(phrase: str, cut: Cut, own: set[str]) -> tuple[float, list[str]]:
    """How well a phrase names a cut: the weight of the words they share, less the cut's own
    distinctive words (names, numbers, offsets) the phrase does not say. -> (score, shared)."""
    p = set(tokens(phrase))
    best, best_shared = -9.0, []
    for lab in (cut.label, *cut.names):
        c = set(tokens(lab))
        shared = p & c
        s = sum(_weight(t, own) for t in shared)
        for t in c - p:
            if re.fullmatch(r"\d+m-(up|down)", t):
                # an offset the phrase does not give (it may give it bare: "100m" ~ "100m-up")
                s -= 1.0 if t.split("-")[0] in {x.split("-")[0] for x in p} else 2.5
            elif _distinctive(t, own) and t not in own:
                s -= 1.5
        for t in p - c:
            if re.fullmatch(r"\d+m-(up|down)", t) and any(x.startswith(t.split("-")[0]) for x in c):
                s -= 2.0          # the same distance, the other side
        for a, b in (("upstream", "downstream"), ("downstream", "upstream")):
            if a in p and b in c and a not in c and b not in p:
                s -= 1.5          # the u/s sign is not the d/s sign
        if (p - c) & TYPE_WORDS:
            s -= 0.5 * len({t for t in c - p if t in TYPE_WORDS and t != "lake"})
        meaningful = {t for t in c if _weight(t, own) > 0}
        if not any(_distinctive(t, own) and t not in own for t in shared):
            if len(meaningful) >= 2 and meaningful <= p:
                s = max(s, 3.5)   # every word of the cut is in the phrase ("lower bridge abutments")
            else:
                s = min(s, 2.0)   # type words alone never make a match
        pt, ct = p & TYPE_WORDS - {"lake"}, c & TYPE_WORDS - {"lake"}
        if pt and ct and not (pt & ct):
            s -= 1.5              # "Cariboo Road" is not "Cariboo Dam"
        if s > best:
            best, best_shared = s, sorted(shared)
    return best, best_shared


# --------------------------------------------------------------------------- a point

@dataclass
class Point:
    """One endpoint resolved: an existing split, a proposal, the water's mouth, a NEW split, or
    nothing. `fix` is the `marks.FixPoint` shape; `new` the `marks.NewSplit` (kind new)."""
    phrase: str
    kind: str                         # split | proposed | new | mouth | none
    confidence: str
    reason: str
    fix: Optional[dict] = None
    new: Optional[dict] = None
    label: str = ""
    coord: Optional[list[float]] = None
    item_id: Optional[str] = None

    def view(self) -> dict:
        return {k: v for k, v in {
            "phrase": self.phrase, "kind": self.kind, "confidence": self.confidence,
            "reason": self.reason, "label": self.label, "fix": self.fix, "new_split": self.new,
            "coord": self.coord, "item_id": self.item_id}.items() if v not in (None, "")}


def _offset(phrase: str) -> Optional[float]:
    """"500 m below the bridge" -> -500, "approximately 250m upstream of X" -> +250."""
    for t in tokens(phrase):
        m = re.fullmatch(r"(\d+)m-(up|down)", t)
        if m:
            return float(m.group(1)) * (1 if m.group(2) == "up" else -1)
    return None


def _dms(g) -> float:
    d, m, s = float(g[0]), float(g[1]), float(g[2] or 0)
    return d + m / 60 + s / 3600


def coords_in(text: str) -> list[tuple[float, float]]:
    """Every (lon, lat) the notice gives in degrees-minutes(-seconds)."""
    lats = [_dms(m.groups()) for m in LAT_RE.finditer(text or "")]
    lons = [-_dms(m.groups()) for m in LON_RE.finditer(text or "")]
    return [(round(lo, 6), round(la, 6)) for la, lo in zip(lats, lons)]


def _anchor_word(phrase: str) -> str:
    p = set(tokens(phrase))
    if "bridge" in p or "trestle" in p or "overpass" in p:
        return "bridge"
    if "dam" in p or "weir" in p or "fence" in p:
        return "dam"
    if "sign" in set(re.findall(r"[a-z]+", _norm(phrase).replace("signs", "sign"))):
        return "sign"
    return "coord"


def _is_receiving(trib_wsc: Iterable[str], water: Water) -> bool:
    """Is the named river the one this water flows INTO (its wsc a prefix of ours)?"""
    mine = [w for ws in water.wsc.values() for w in ws]
    return any(is_strictly_below(m, t) for t in trib_wsc for m in mine)


def _direct_child(trib_wsc: Iterable[str], water: Water) -> Optional[str]:
    """The item of this water the stream flows straight into (one more watershed-code group)."""
    for iid, ws in water.wsc.items():
        for w in ws:
            for t in trib_wsc:
                if is_strictly_below(t, w) and len(groups(t)) == len(groups(w)) + 1:
                    return iid
    return None


def resolve(phrase: str, water: Water, cuts: list[Cut], look: Lookup, *, ref: str) -> Point:
    """ONE ENDPOINT PHRASE -> a point: the best existing cut or proposal, the mouth, a NEW split
    where the data pins it, or none."""
    own = own_words(water)
    ptoks = set(tokens(phrase))
    near = "near" in set(re.findall(r"[a-z]+", phrase.lower()))
    lakes = [m.group(1) for m in LAKE_RE.finditer(phrase)]
    tribs = [m.group(1) for m in TRIB_RE.finditer(phrase)
             if name_key(m.group(1)) not in {name_key(n) for n in water.names.values()}]
    off = _offset(phrase)

    # 1. a lake the water already has a cut at (by its full name)
    for ln in lakes:
        lk = name_key(ln)
        for c in cuts:
            if c.kind in ("lake", "outlet", "proposed", "split") and (
                    name_key(c.label) == lk or lk in {name_key(n) for n in c.names}
                    or (c.proposal and name_key(c.proposal.get("name") or "") == lk)):
                if off and c.kind != "proposed":
                    continue
                pk = "proposed" if c.proposal else "split"
                return Point(phrase, pk, "high", f"{ln}: the {'proposed' if c.proposal else 'curated'} "
                             f"cut `{c.id}` ({c.label})", fix=_fixpt(c), label=c.label, item_id=c.item_id)

    # 2. a tributary the water already has a cut (or a proposal) at
    for tn in tribs:
        tk = name_key(tn)
        for c in cuts:
            head = c.label.split("→")[0].strip() if "→" in c.label else (
                (c.proposal or {}).get("name") if c.proposal and c.proposal.get("anchor") == "confluence" else None)
            if head and name_key(re.sub(r"\(.*?\)", "", head)) == tk:
                want = f"{int(abs(off))}m-{'up' if off > 0 else 'down'}" if off else None
                have = {t for t in tokens(c.label) if re.fullmatch(r"\d+m-(up|down)", t)}
                if (want or None) != (next(iter(have)) if have else None):
                    if not (want and not have and c.proposal and c.proposal.get("offset_m") == off):
                        continue
                conf = "medium" if near else "high"
                pk = "proposed" if c.proposal else "split"
                return Point(phrase, pk, conf, f"confluence of {tn}: the {'proposed' if c.proposal else 'curated'} "
                             f"cut `{c.id}` ({c.label})" + (" — the notice's signs are NEAR it" if conf == "medium" else ""),
                             fix=_fixpt(c), label=c.label, item_id=c.item_id)

    # 3. the best curated cut / proposal by words
    ranked = sorted(((score(phrase, c, own), c) for c in cuts), key=lambda x: -x[0][0])
    if ranked and ranked[0][0][0] >= 3.0:
        (s, shared), c = ranked[0]
        second = ranked[1][0][0] if len(ranked) > 1 else -9
        conf = "high" if s >= 4.0 and s - second >= 2.0 else "medium" if s - second >= 1.0 else "low"
        pk = "proposed" if c.proposal else "split"
        why = (f"\"{', '.join(t for t in shared if _weight(t, own) > 0)}\" -> the "
               f"{'proposed' if c.proposal else 'curated'} cut `{c.id}` ({c.label})")
        if conf == "low":
            why += f"; as close: `{ranked[1][1].id}` ({ranked[1][1].label})"
        bare = _offset(re.sub(r"\([^)]*\)", "", phrase))
        if bare and not any(re.fullmatch(r"\d+m-(up|down)", t) for t in tokens(c.label)):
            conf = "medium" if conf == "high" else conf
            why += f"; the notice's place is {abs(bare):.0f} m {'above' if bare > 0 else 'below'} it"
        return Point(phrase, pk, conf, why, fix=_fixpt(c), label=c.label, item_id=c.item_id)

    # 4. the water's own mouth: the tidal boundary, the receiving river
    if MOUTH_RE.search(phrase) and not tribs and not (ptoks & TYPE_WORDS - {"mouth", "confluence"}):
        return Point(phrase, "mouth", "high", "the water's own downstream end (tidal boundary / mouth)")
    for tn in tribs:
        hits = look.stream(tn)
        if any(_is_receiving(h.get("wsc") or [], water) for h in hits):
            return Point(phrase, "mouth", "high", f"{tn} is the river this water flows into: its mouth")

    # 5. a coordinate the notice gives -> a NEW split there
    cs = coords_in(phrase)
    if cs:
        lonlat = cs[0]
        a = _anchor_word(re.split(r"\(", phrase)[0])
        s = look.snap(lonlat, water.items)
        nm = _short_name(phrase)
        new = {"ref": ref, "anchor": a, "name": nm, "coord": list(lonlat), "quote": phrase}
        if s is None:
            return Point(phrase, "new", "medium", f"the notice's coordinate {lonlat[1]:.5f}, {lonlat[0]:.5f} "
                         "(not snapped here: the app checks it on save)", fix=_newpt(ref), new=new,
                         label=nm, coord=list(lonlat))
        new.update(on_item=s["item_id"], blk=s["blk"], measure=s["measure"])
        d = s["distance_m"]
        conf = "high" if d <= 150 else "medium" if d <= 500 else "low"
        return Point(phrase, "new", conf, f"the notice's coordinate, {d:.0f} m from the water"
                     + ("" if d <= 500 else ": too far — check it"), fix=_newpt(ref), new=new,
                     label=nm, coord=s["coord"], item_id=s["item_id"])

    # 6. a tributary flowing straight into this water (FWA watershed code) -> a NEW confluence
    for tn in tribs:
        hits = [h for h in look.stream(tn) if _direct_child(h.get("wsc") or [], water)]
        if len(hits) != 1:
            continue
        h = hits[0]
        on = _direct_child(h["wsc"], water)
        mouth = look.mouth(h["id"], [on])
        if not mouth or mouth["distance_m"] > 300:
            continue        # its mouth is not on this water's line (a lake between): not exact
        new = {"ref": ref, "anchor": "confluence", "name": h["name"], "item_id": h["id"], "on_item": on,
               "quote": phrase}
        conf, why = "high", f"{tn} ({h['id']}) flows straight into {water.names.get(on, on)} (FWA watershed code)"
        if off:
            new["offset_m"] = off
            conf, why = "medium", why + f"; {abs(off):.0f} m {'above' if off > 0 else 'below'} its mouth (the notice's approximate distance)"
        if near:
            conf, why = "medium", why + "; the notice's place is NEAR the confluence"
        new.update(blk=mouth["blk"], measure=mouth["measure"], coord=mouth["coord"])
        why += f"; its mouth {mouth['distance_m']:.0f} m from the water's line"
        return Point(phrase, "new", conf, why, fix=_newpt(ref), new=new,
                     label=f"confluence of {h['name']}", coord=mouth["coord"], item_id=on)

    # 7. a lake the water touches but has no cut at -> a NEW inlet / outlet
    for ln in lakes:
        hits = look.lake(ln)
        if len(hits) != 1:
            continue
        a = "lake_inlet" if re.search(r"\binlet\b", phrase, re.I) else "lake_outlet"
        new = {"ref": ref, "anchor": a, "name": hits[0]["name"], "item_id": hits[0]["id"],
               "quote": phrase}
        if len(water.items) == 1:
            new["on_item"] = water.items[0]
        return Point(phrase, "new", "low", f"{'inlet' if a == 'lake_inlet' else 'outlet'} of {ln} "
                     f"({hits[0]['id']}) — no cut there yet; the app checks the water touches it",
                     fix=_newpt(ref), new=new, label=f"{a.replace('_', ' ')} of {ln}")

    # 8. a named falls beside the water (OSM)
    for m in FALLS_RE.finditer(phrase):
        fn = m.group(1).replace('"', "").replace("“", "").replace("”", "").strip()
        f = look.falls(fn, water.items)
        if not f:
            continue
        new = {"ref": ref, "anchor": "coord", "name": fn, "coord": f["coord"], "on_item": f["item_id"],
               "blk": f["blk"], "measure": f["measure"], "quote": phrase}
        why = f"{fn}: the OSM waterfall {f['distance_m']:.0f} m from the water"
        if off:
            new["offset_m"] = off
            why += f"; {abs(off):.0f} m {'above' if off > 0 else 'below'} it (approximate)"
        conf = "medium" if f["distance_m"] <= 150 else "low"
        return Point(phrase, "new", conf, why, fix=_newpt(ref), new=new, label=fn, coord=f["coord"],
                     item_id=f["item_id"])

    if ranked and ranked[0][0][0] >= 2.0:
        (s, shared), c = ranked[0]
        return Point(phrase, "split" if not c.proposal else "proposed", "low",
                     f"only \"{', '.join(t for t in shared if _weight(t, own) > 0)}\" in common with `{c.id}` ({c.label})",
                     fix=_fixpt(c), label=c.label, item_id=c.item_id)
    return Point(phrase, "none", "none", "no curated cut, proposal or data point names this place")


def _short_name(phrase: str) -> str:
    t = re.split(r"\(", phrase)[0].strip(" ,.")
    t = re.sub(r"^(?:from|to|the|a)\s+", "", t, flags=re.I)
    return t[:80] or phrase[:80]


def _fixpt(c: Cut) -> dict:
    return {"kind": "new", "split_id": c.id} if c.proposal else {"kind": "split", "split_id": c.id}


def _newpt(ref: str) -> dict:
    return {"kind": "new", "split_id": ref}


# --------------------------------------------------------------------------- one location

_NOTES_RE = re.compile(r"\(\s*see [^)]*\)|\bnote:.*$|\banglers may .*$", re.I)


def _whole(text: str, water: Water) -> Optional[tuple[str, str, list[str], Optional[bool]]]:
    """A notice that names NO reach: the whole water (with / without its tributaries), or one of
    its items by name; "all tributaries of X" cannot be said by an extent fix (none).
    -> (confidence, reason, items, tributaries) or None when the words name a reach."""
    if re.match(r"^\s*note\b", text or "", re.I):
        return "none", "a note on the water's name, not a place", [], None
    t = _NOTES_RE.sub("", re.sub(r"\s+", " ", text or "")).strip(" .,")
    items1 = list(water.items) if len(water.items) == 1 else []
    if not t:
        return "medium", "the notice names no reach: the whole water", items1, None
    if re.match(r"^all tributar(?:y|ies) (?:to|of) ", t, re.I):
        return ("none", "tributaries only (not the mainstem): an extent fix cannot say it — curate by hand",
                [], None)
    if re.fullmatch(r"(?:including|incl\.?) (?:its )?tributaries(?: and (?:the )?[\w' ]+ Lake)?", t, re.I):
        return "high", "\"including tributaries\": the whole water with its tributaries", items1, True
    if re.fullmatch(r"excluding tributaries", t, re.I):
        return "high", "\"excluding tributaries\": the whole water, without its tributaries", items1, False
    if re.fullmatch(r"including tributaries, except in the area described below", t, re.I):
        return ("medium", "the whole water with its tributaries (the exception is another location)",
                items1, True)
    m = re.fullmatch(r"(?:the )?([\w' ]+?) (?:and|including) (?:its )?tributaries", t, re.I)
    if m:
        named = [i for i, n in water.names.items() if name_key(n).startswith(name_key(m.group(1)))]
        if len(named) == 1 and len(water.items) > 1:
            return ("medium", f"\"{t}\": {water.names[named[0]]} with its tributaries", named, True)
        return "high", f"\"{t}\": the whole water with its tributaries", items1, True
    m = re.fullmatch(r"upper and lower river, including tributaries", t, re.I)
    if m:
        return "high", f"\"{t}\": the whole water with its tributaries", items1, True
    m = re.fullmatch(r"including (?:the )?([\w' ]+?(?:River|Creek|Lake))", t, re.I)
    stem = lambda x: re.sub(r" (?:river|creek|lake)$", "", name_key(x))  # noqa: E731 — "Colonial River" is Colonial Creek
    if m and any(stem(n) == stem(m.group(1)) for n in water.names.values()):
        return "medium", f"\"{t}\": the whole water, all its items", [], None
    if re.fullmatch(r"(?:the )?mainstem waters of the [\w' ]+", t, re.I):
        return "medium", "the mainstem only: the whole water without its tributaries", items1, False
    if re.fullmatch(r"all waters(?: unless indicated below)?|all open portions of the [\w' ]+", t, re.I):
        return "medium", "the notice names no reach: the whole water", items1, None
    return None


def suggest_one(loc: Mapping, water: Water, cuts: list[Cut], look: Lookup | None = None) -> dict:
    """THE SUGGESTED EXTENT for one DFO location: {op, points, items, tributaries, new_splits,
    confidence, reason, endpoints, matched} — a suggestion, never applied. `matched` is existing
    (every point a cut that exists or the owner proposed), new (a NEW split in it), whole, partial
    (a point the data does not pin) or none."""
    look = look or NoLookup()
    st = loc.get("source_text") or {}
    text = st.get("specific_area") or ""
    items1 = list(water.items) if len(water.items) == 1 else []
    base = {"location_id": loc["location_id"], "water": loc.get("water") or "", "text": text,
            "notice_op": st.get("op"), "items": list(water.items)}
    if not water.items:
        return {**base, "suggestion": None, "confidence": "none", "matched": "none",
                "reason": "the location's water is not matched to the registry", "endpoints": []}
    w = _whole(text, water)
    if w is not None:
        conf, reason, items, trib = w
        if conf == "none":
            return {**base, "suggestion": None, "confidence": "none", "matched": "none",
                    "endpoints": [], "reason": reason}
        return {**base, "matched": "whole", "confidence": conf, "endpoints": [], "reason": reason,
                "suggestion": {"op": "whole", "points": [], "items": items, "tributaries": trib,
                               "new_splits": [],
                               "words": "the whole water" + {True: " with its tributaries",
                                                             False: " without its tributaries"}.get(trib, "")}}
    if any(k == "lake" for k in water.kind.values()) and all(k == "lake" for k in water.kind.values()):
        return {**base, "suggestion": None, "confidence": "none", "matched": "none", "endpoints": [],
                "reason": "a part of a lake (a line across it / a radius): not a cut on a line — curate by hand"}
    phrases = endpoints(text)
    pts = [resolve(p.text, water, cuts, look, ref=f"sg{i + 1}") for i, p in enumerate(phrases)]
    # the mouth is the water's bottom: "from A downstream to the mouth" is downstream of A
    real = [p for p in pts if p.kind != "mouth"]
    if len(pts) == 2 and len(real) == 1:
        op = "downstream_of"
        pts = real
        note = "the other end is the water's mouth, so downstream of it"
    elif len(pts) == 2:
        op, note = "between", ""
        if pts[0].fix and pts[1].fix and pts[0].fix == pts[1].fix:
            pts, op, note = pts[:1], direction(text) or "downstream_of", "both ends name the same cut"
    elif len(pts) == 1 and pts[0].kind == "mouth":
        return {**base, "suggestion": None, "confidence": "none", "matched": "none",
                "endpoints": [p.view() for p in pts], "reason": "only the mouth is named"}
    else:
        op = direction(text) or {"upstream_of": "upstream_of", "downstream_of": "downstream_of"}.get(
            st.get("op") or "", "downstream_of")
        note = "" if direction(text) else f"the notice reads as {st.get('op')}"
    conf = min((p.confidence for p in pts), key=lambda c: CONF_RANK[c]) if pts else "none"
    resolved = [p for p in pts if p.kind != "none"]
    if not resolved:
        matched = "none"
    elif len(resolved) < len(pts):
        matched = "partial"
    elif any(p.kind == "new" for p in pts):
        matched = "new"
    else:
        matched = "existing"
    new_splits = []
    for p in pts:
        if p.new:
            ns = dict(p.new)
            # owner 2026-10-27: a proposed split carries its confidence — the point's own grade
            ns["confidence"] = p.confidence if p.confidence in ("high", "medium", "low") else "low"
            if op in ("upstream_of", "downstream_of"):
                ns["side"] = "upstream" if op == "upstream_of" else "downstream"
            new_splits.append(ns)
    words = " and ".join(p.label or p.phrase for p in pts)
    reason = "; ".join([f"{p.confidence}: {p.reason}" for p in pts] + ([note] if note else []))
    trib = (False if re.search(r"\bexcluding tributaries\b", text, re.I)
            else True if re.search(r"\b(?:including|and) (?:its )?tributaries\b", text, re.I) else None)
    sugg = {"op": op, "points": [p.fix for p in pts if p.fix], "items": items1, "tributaries": trib,
            "new_splits": new_splits,
            "words": f"{op.replace('_', ' ')} {words}".replace("between", "between", 1)}
    if matched == "partial":
        conf = "none"
    return {**base, "suggestion": sugg if resolved else None, "confidence": conf, "matched": matched,
            "reason": reason, "endpoints": [p.view() for p in pts],
            "points": [{"label": p.label, "coord": p.coord, "kind": p.kind,
                        "split_id": (p.fix or {}).get("split_id")} for p in pts if p.fix]}


# --------------------------------------------------------------------------- the split hunt

#: THE SPLIT HUNT (owner 2026-10-27): hand research for the locations the matcher could not place —
#: each cut a PROPOSAL with an anchor, a lat/lon, a confidence, a description, sources and notes.
HUNT_PATH = Path("handoff/PLAN/curation-review/dfo-split-hunt.json")


def _hunt_new(key: str, c: Mapping, water: Water, look: Lookup) -> tuple[dict, dict]:
    """One hunted cut -> (the `marks.NewSplit` it proposes, its research view). The lat/lon is
    snapped onto the water's FWA line (blk / measure stamped, the snap distance kept); an offset
    is drawn at its own place along the line. The NewSplit's `note` carries the description and
    the sources, so an accepted proposal keeps its evidence in the owner's mark."""
    lonlat = (float(c["lon"]), float(c["lat"]))
    snap = look.snap(lonlat, list(water.items)) if water.items else None
    anchor = c["anchor"]
    ns: dict = {"ref": key[:40], "anchor": anchor, "name": c.get("name") or key}
    if anchor == "split":
        ns["split_id"] = c["split_id"]
    if anchor == "confluence":
        hits = look.stream(c.get("name") or "")
        best = None
        for h in hits:
            m = look.mouth(h["id"], list(water.items))
            if m and (best is None or m["distance_m"] < best[1]["distance_m"]):
                best = (h, m)
        if best:
            ns["item_id"] = best[0]["id"]
            snap = best[1]
        else:
            # the tributary is not in the registry by that name next to this water (a same-named
            # creek elsewhere would be picked by name): the mouth is placed as a point instead
            ns["anchor"] = anchor = "coord"
    if snap:
        ns["on_item"] = snap["item_id"]
        if anchor != "split":
            ns["coord"] = tuple(snap["coord"])
            ns["blk"], ns["measure"] = snap["blk"], snap["measure"]
    elif anchor != "split":
        ns["coord"] = lonlat
    if water.items and "on_item" not in ns:
        ns["on_item"] = water.items[0]
    off = float(c.get("offset_m") or 0)
    baked = ""
    drawn = list(snap["coord"]) if snap else [lonlat[0], lonlat[1]]
    if off and anchor == "split":
        ns["offset_m"] = off                      # the curated cut + an offset: the anchor says it
    elif off and snap:
        # the offset is BAKED IN: the point is where the cut goes (two cuts off one anchor would
        # otherwise share a blk / measure and dedupe into one — `marks.same_anchor`)
        at = getattr(look, "at_measure", lambda *_: None)(snap["blk"], snap["measure"] + off)
        if at:
            drawn = at
            ns["coord"], ns["measure"] = tuple(at), round(snap["measure"] + off, 1)
            if anchor in ("confluence", "lake_inlet", "lake_outlet"):
                ns["anchor"] = "sign"
                ns.pop("item_id", None)
            baked = f" (placed {abs(off):.0f} m {'up' if off > 0 else 'down'}stream of the anchor, offset applied)"
        else:
            ns["offset_m"] = off
    elif off:
        ns["offset_m"] = off
    ns["note"] = (f"[split hunt {c['confidence']}] {c.get('description', '')}{baked}. Sources: "
                  + " ; ".join(c.get("sources") or []) + (f". {c['notes']}" if c.get("notes") else ""))
    view = {"ref": ns["ref"], "anchor": anchor, "name": c.get("label") or ns["name"], "confidence": c["confidence"],
            "description": c.get("description") or "", "sources": list(c.get("sources") or []),
            "notes": c.get("notes") or "", "lat": c["lat"], "lon": c["lon"], "coord": drawn,
            "offset_m": c.get("offset_m"), "split_id": c.get("split_id"),
            "snap_m": snap["distance_m"] if snap else None, "blk": ns.get("blk"), "measure": ns.get("measure")}
    if snap and snap["distance_m"] > 300:
        view["warning"] = f"the point is {snap['distance_m']:.0f} m from the water's FWA line — check the snap"
    elif not snap and water.items:
        view["warning"] = "not snapped to the water's FWA line (no line within 1.5 km)"
    return ns, view


def apply_hunt(row: dict, entry: Mapping, cuts: Mapping[str, Mapping], water: Water, look: Lookup,
               page: Optional[str] = None, labels: Optional[Mapping[str, str]] = None) -> dict:
    """THE SPLIT HUNT on one location: the hand-researched extent replaces the matcher's — its
    ends are curated cuts (`split`), the matcher's own resolved endpoints (`auto`), or hunted cuts
    (`cut`, NEW splits with research). `not_found` keeps the row unplaced and says why. Still a
    suggestion: nothing is applied."""
    if entry.get("not_found"):
        return {**row, "hunt": {"status": "not_found", "why": entry["not_found"],
                                "sources": [page] if page else []}}
    op = entry["op"]
    items1 = list(water.items) if len(water.items) == 1 else []
    pts, new_splits, props, confs, words, why = [], [], [], [], [], []
    for e in entry.get("ends") or []:
        if "split" in e:
            pts.append({"kind": "split", "split_id": e["split"]})
            confs.append(e.get("confidence", "high"))
            words.append((labels or {}).get(e["split"]) or e["split"])
            why.append(f"{e.get('confidence', 'high')}: the curated cut `{e['split']}` — {e.get('note', '')}")
        elif "auto" in e:
            ep = row["endpoints"][e["auto"]]
            pts.append(dict(ep["fix"]))
            confs.append(ep["confidence"])
            words.append(ep.get("label") or ep["phrase"])
            why.append(f"{ep['confidence']}: {ep['reason']}")
            if ep.get("new_split"):
                new_splits.append(dict(ep["new_split"]))
        else:
            c = cuts[e["cut"]]
            ns, view = _hunt_new(e["cut"], c, water, look)
            if op in ("upstream_of", "downstream_of"):
                ns["side"] = "upstream" if op == "upstream_of" else "downstream"
            pts.append({"kind": "new", "split_id": ns["ref"]})
            new_splits.append(ns)
            props.append(view)
            confs.append(c["confidence"])
            words.append(view["name"])
            why.append(f"{c['confidence']}: {c.get('description', '')}")
    if op == "whole":
        conf = entry.get("confidence", "medium")
    else:
        conf = min(confs, key=lambda c: CONF_RANK[c]) if confs else "none"
    trib = entry.get("tributaries", (row.get("suggestion") or {}).get("tributaries"))
    sugg = {"op": op, "points": pts, "items": items1, "tributaries": trib, "new_splits": new_splits,
            "words": "the whole water" if op == "whole" else f"{op.replace('_', ' ')} " + " and ".join(words)}
    reason = "split hunt: " + entry.get("notes", "") + ("; " + "; ".join(why) if why else "")
    view_pts = []
    for p in pts:
        hv = next((v for v in props if p["kind"] == "new" and v["ref"] == p["split_id"]), None)
        ep = next((x for x in row.get("endpoints") or [] if x.get("fix") == p), None)
        view_pts.append({"label": (hv or {}).get("name") or (ep or {}).get("label") or p["split_id"],
                         "coord": (hv or {}).get("coord") or (ep or {}).get("coord"),
                         "kind": p["kind"], "split_id": p["split_id"]})
    return {**row, "suggestion": sugg, "confidence": conf,
            "matched": "whole" if op == "whole" else "new" if props or any(p["kind"] == "new" for p in pts) else "existing",
            "reason": reason, "points": view_pts,
            "hunt": {"status": "found", "notes": entry.get("notes", ""), "proposals": props,
                     "sources": [page] if page else [], "matcher": {"matched": row["matched"],
                                                                    "reason": row.get("reason", "")}}}


def summarize_hunt(rows: list[dict], hunt: Mapping) -> dict:
    """Counts for the split-hunt summary: locations by the confidence of their hunted extent,
    distinct hunted cuts by confidence, and the locations still not found."""
    from collections import Counter
    locs, cuts_c = Counter(), Counter()
    for r in rows:
        h = r.get("hunt")
        if not h:
            continue
        locs["not_found" if h["status"] == "not_found" else r["confidence"]] += 1
    for c in (hunt.get("cuts") or {}).values():
        cuts_c[c["confidence"]] += 1
    return {"locations": dict(locs), "cuts": dict(cuts_c)}


def _hunt_md(rows: list[dict], hunt: Mapping) -> str:
    s = summarize_hunt(rows, hunt)
    hr = [r for r in rows if r.get("hunt")]
    L = ["# DFO split hunt (owner 2026-10-27)", "",
         "Hand research for the DFO locations the matcher left with no suggestion (68) or one end open (15).",
         "Every cut is a PROPOSAL: open the location in the review app's DFO mode — the hunted cuts show",
         "with their confidence, description, sources and notes; **accept suggestion** pre-fills the",
         "editor (the research travels in each new split's note); **Save extent** (your mark) is the only",
         "write. Nothing touched splits.json, the catalogue or marks.jsonl. Input:",
         "`handoff/PLAN/curation-review/dfo-split-hunt.json`; regenerate with",
         "`python -m pipeline.regs.dfo_salmon.suggest`.", "",
         f"## Counts ({len(hr)} locations)", "",
         "| locations | high | medium | low | not found |", "|---|---|---|---|---|",
         f"| {len(hr)} | {s['locations'].get('high', 0)} | {s['locations'].get('medium', 0)} | "
         f"{s['locations'].get('low', 0)} | {s['locations'].get('not_found', 0)} |", "",
         "A location's confidence is its weakest end. Distinct hunted cuts: "
         + ", ".join(f"{k} {s['cuts'].get(k, 0)}" for k in ("high", "medium", "low")) + ".", "",
         "## Found", "", "| location | extent | conf | cuts (conf · snap) |", "|---|---|---|---|"]
    for r in sorted(hr, key=lambda r: r["location_id"]):
        if r["hunt"]["status"] != "found":
            continue
        cs = "; ".join(f"{p['name']} ({p['confidence']} · "
                       + (f"{p['snap_m']:.0f} m" if p.get("snap_m") is not None else "—") + ")"
                       + (" ⚠" if p.get("warning") else "") for p in r["hunt"]["proposals"]) or "curated cuts only"
        L.append(f"| `{r['location_id']}` | {r['suggestion']['words'].replace('|', '/')} | {r['confidence']} | {cs.replace('|', '/')} |")
    L += ["", "## Still not found", "", "| location | why |", "|---|---|"]
    for r in sorted(hr, key=lambda r: r["location_id"]):
        if r["hunt"]["status"] == "not_found":
            L.append(f"| `{r['location_id']}` | {r['hunt']['why'].replace('|', '/')} |")
    syn = hunt.get("synopsis") or {}
    if syn:
        L += ["", "## Synopsis-side cuts (owner 2026-10-27: guessed LOW)", "",
              "Not DFO locations — the synopsis rules' missing cuts (split-name-audit.md); for the owner to place.", ""]
        for rid, v in syn.items():
            L.append(f"- **{rid}** — \"{v['text']}\": {v['proposal']}. Sources: {' ; '.join(v.get('sources') or [])}")
    return "\n".join(L) + "\n"


# --------------------------------------------------------------------------- the corpus

def cuts_on(item_ids: list[str], registry: Mapping[str, dict], splits_meta: Mapping[str, dict],
            proposals: list[dict]) -> list[Cut]:
    """Every cut a location's extent may name: the curated / lake cuts on its items (no gauge-only,
    MU, area or length cuts), and the owner's proposals on the same items."""
    out: list[Cut] = []
    seen: set[str] = set()
    for iid in item_ids:
        it = registry.get(iid) or {}
        for b in it.get("boundaries") or []:
            bid, kind = b.get("id") or "", b.get("kind") or ""
            if (kind in ("mu", "area", "border", "headwaters") or bid.startswith(("gauge__", "length:", "area:"))
                    or re.fullmatch(r"lake \d+", b.get("label") or "") or bid in seen):
                continue
            seen.add(bid)
            names = [n for n in (b.get("names") or []) if not re.match(r"^\d\d[A-Z]{2}\d{3}\b", n)]
            sp = splits_meta.get(bid) or {}
            extra = [x for x in ((sp.get("anchor") or {}).get("_name"), sp.get("label")) if x]
            out.append(Cut(bid, b.get("label") or bid, kind, iid, tuple(dict.fromkeys(names + extra))))
    for p in proposals:
        n = p["split"]
        on = n.get("on_item")
        if on and on not in item_ids:
            continue
        nm = n.get("name") or ""
        label = (f"{nm} → (proposed)" if n.get("anchor") == "confluence" else nm) or p["id"]
        conf = p.get("confidence") or n.get("confidence")
        if conf in ("medium", "low"):           # owner 2026-10-27: its confidence, everywhere
            label += f" ({conf} confidence)"
        out.append(Cut(p["id"], label, "proposed", on, (nm,) if nm else (), proposal=n))
    return out


def water_of(item_ids: list[str], registry: Mapping[str, dict]) -> Water:
    names, wsc, kind = {}, {}, {}
    for iid in item_ids:
        it = registry.get(iid) or {}
        names[iid] = it.get("name") or iid
        wsc[iid] = [r.split(":", 1)[1] for r in it.get("ref_ids") or [] if r.startswith("wsc:")]
        kind[iid] = it.get("kind") or ""
    return Water(list(item_ids), names, wsc, kind)


def needs_suggestion(loc: Mapping, water: Optional[Mapping]) -> bool:
    """Not cleanly bound: no extent curated (whatever the op), or no water matched."""
    return not (loc.get("binding") or {}).get("extents")


def summarize(rows: list[dict]) -> dict:
    from collections import Counter
    c = Counter()
    for r in rows:
        if r["matched"] == "new":
            c[f"new_{r['confidence']}"] += 1
        else:
            c[r["matched"]] += 1
    return dict(sorted(c.items()))


# --------------------------------------------------------------------------- FWA / OSM

class FwaLookup:
    """The data that pins a NEW split: the registry by name, the FWA stream lines (by bbox, from the
    FWA GeoPackage), one attribute scan for tributary mouths, the OSM waterfalls layer. Read only."""

    def __init__(self, registry: Mapping[str, dict], gpkg: str):
        self.reg = registry
        self.gpkg = gpkg
        self._by_name: dict[str, list[dict]] = {}
        for iid, it in registry.items():
            self._by_name.setdefault(name_key(it.get("name") or ""), []).append(it)
        self._mouths: dict[str, dict] = {}
        self._falls = None
        from pyproj import Transformer
        self._to_albers = Transformer.from_crs(4326, 3005, always_xy=True).transform
        self._to_ll = Transformer.from_crs(3005, 4326, always_xy=True).transform

    def _view(self, it):
        return {"id": it["id"], "name": it.get("name"), "kind": it.get("kind"),
                "wsc": [r.split(":", 1)[1] for r in it.get("ref_ids") or [] if r.startswith("wsc:")]}

    def stream(self, name):
        return [self._view(i) for i in self._by_name.get(name_key(name), []) if i.get("kind") == "stream"]

    def lake(self, name):
        return [self._view(i) for i in self._by_name.get(name_key(name), []) if i.get("kind") == "lake"]

    def _blks(self, item_ids):
        return {int(r.split(":", 1)[1]) for i in item_ids for r in (self.reg.get(i) or {}).get("ref_ids") or []
                if r.startswith("blk:")}

    def _snap_xy(self, x, y, item_ids, radius=1500.0):
        import pyogrio
        from shapely.geometry import Point
        blks = self._blks(item_ids)
        if not blks:
            return None
        df = pyogrio.read_dataframe(self.gpkg, layer="streams", bbox=(x - radius, y - radius, x + radius, y + radius),
                                    columns=["BLUE_LINE_KEY", "DOWNSTREAM_ROUTE_MEASURE", "EDGE_TYPE"])
        # the water's own line, not its lake connectors (EDGE_TYPE 1200: a lake's skeleton is the
        # LAKE's, a separate item — a stream entering the lake is not a confluence on the river)
        df = df[df.BLUE_LINE_KEY.isin(blks) & (df.EDGE_TYPE != 1200)]
        if df.empty:
            return None
        pt = Point(x, y)
        d = df.geometry.distance(pt)
        i = d.idxmin()
        g = df.geometry[i]
        from shapely import force_2d
        g = force_2d(g)
        if g.geom_type == "MultiLineString":
            from shapely.ops import linemerge
            g = linemerge(g)
        along = g.project(pt)
        sp = g.interpolate(along)
        lon, lat = self._to_ll(sp.x, sp.y)
        blk = int(df.BLUE_LINE_KEY[i])
        item = next((iid for iid in item_ids if blk in self._blks([iid])), item_ids[0])
        return {"blk": str(blk), "measure": round(float(df.DOWNSTREAM_ROUTE_MEASURE[i]) + along, 1),
                "distance_m": round(float(d[i]), 1), "coord": [round(lon, 6), round(lat, 6)], "item_id": item}

    def snap(self, lonlat, on_items):
        x, y = self._to_albers(*lonlat)
        return self._snap_xy(x, y, on_items)

    def prime_mouths(self, item_ids: Iterable[str]) -> None:
        """One scan of the FWA streams for the bottom segment of every one of these streams."""
        import sqlite3
        want = {b: iid for iid in item_ids for b in self._blks([iid])}
        want = {b: i for b, i in want.items() if i not in self._mouths}
        if not want:
            return
        con = sqlite3.connect(f"file:{self.gpkg}?mode=ro", uri=True)
        rows = con.execute(
            "select fid, BLUE_LINE_KEY from streams where DOWNSTREAM_ROUTE_MEASURE < 1 and BLUE_LINE_KEY in ("
            + ",".join(str(b) for b in want) + ")").fetchall()
        con.close()
        if not rows:
            return
        import pyogrio
        from shapely import force_2d
        df = pyogrio.read_dataframe(self.gpkg, layer="streams", fids=[r[0] for r in rows],
                                    columns=["BLUE_LINE_KEY", "WATERSHED_KEY"])
        for _, r in df.iterrows():
            blk = int(r.BLUE_LINE_KEY)
            if blk != int(r.WATERSHED_KEY):
                continue                       # a side channel of the stream, not its main line
            g = force_2d(r.geometry)
            first = g.geoms[0].coords[0] if g.geom_type == "MultiLineString" else g.coords[0]
            iid = want.get(blk)
            if iid and iid not in self._mouths:
                self._mouths[iid] = {"xy": first}

    def mouth(self, item_id, on_items):
        self.prime_mouths([item_id])
        m = self._mouths.get(item_id)
        if not m:
            return None
        s = self._snap_xy(*m["xy"], on_items, radius=600.0)
        if not s:
            return None
        return s

    def at_measure(self, blk, measure):
        """The [lon, lat] at a route measure on one FWA blue line (an offset cut drawn in place)."""
        import sqlite3
        import pyogrio
        from shapely import force_2d
        con = sqlite3.connect(f"file:{self.gpkg}?mode=ro", uri=True)
        rows = con.execute("select fid, DOWNSTREAM_ROUTE_MEASURE from streams where BLUE_LINE_KEY = ? "
                           "and DOWNSTREAM_ROUTE_MEASURE <= ? order by DOWNSTREAM_ROUTE_MEASURE desc limit 1",
                           (int(blk), float(measure))).fetchall()
        con.close()
        if not rows:
            return None
        df = pyogrio.read_dataframe(self.gpkg, layer="streams", fids=[rows[0][0]], columns=["BLUE_LINE_KEY"])
        g = force_2d(df.geometry.iloc[0])
        if g.geom_type == "MultiLineString":
            from shapely.ops import linemerge
            g = linemerge(g)
        p = g.interpolate(min(max(float(measure) - rows[0][1], 0.0), g.length))
        lon, lat = self._to_ll(p.x, p.y)
        return [round(lon, 6), round(lat, 6)]

    def falls(self, name, on_items):
        import pyogrio
        if self._falls is None:
            self._falls = pyogrio.read_dataframe(self.gpkg, layer="waterfalls", columns=["name"])
        f = self._falls[self._falls["name"].fillna("").map(name_key) == name_key(name)]
        best = None
        for g in f.geometry:
            s = self._snap_xy(g.x, g.y, on_items, radius=600.0)
            if s and (best is None or s["distance_m"] < best["distance_m"]):
                best = s
        return best


# --------------------------------------------------------------------------- write

OUT_DIR = Path("handoff/PLAN/curation-review")


def run(out_dir: Path = OUT_DIR, *, marks_path: Optional[Path] = None, gpkg: Optional[str] = None,
        registry_path: Optional[Path] = None, hunt_path: Optional[Path] = None) -> list[dict]:
    """Every not-cleanly-bound DFO location -> its suggestion; writes dfo-suggestions.json and a
    summary .md. Reads the curated DFO files, the served registry, splits.json, the marks store and
    the FWA GeoPackage; writes nothing else."""
    from datetime import date
    from pipeline.common.curated import CURATED, GENERATED
    from pipeline.regs.review import dfo as D
    from pipeline.regs.review import marks as M
    registry = {i["id"]: i for i in json.loads(Path(registry_path or GENERATED.build() / "registry.json")
                                                  .read_text())["items"]}
    sj = json.loads(Path(CURATED.waters.splits).read_text())
    smeta = {sp["id"]: sp for wb in sj.get("waterbodies") or [] for sp in wb.get("splits") or []
             if isinstance(sp, dict) and sp.get("id")}
    mp = Path(marks_path or CURATED.regulations.review.marks)
    props = [{"id": p["id"], "split": json.loads(p["split"].model_dump_json()),
              "confidence": p["confidence"]}
             for p in M.proposals(M.load(mp), M.load_overrides(
                 CURATED.regulations.review.proposal_overrides))] if mp.exists() else []
    if gpkg is None:
        from project_config import get_config
        gpkg = str(get_config().fwa_data_gpkg)
    look: Lookup = FwaLookup(registry, gpkg) if Path(gpkg).exists() else NoLookup()
    todo = [(lid, loc, w) for lid, (_s, loc, w) in D.locations().items() if needs_suggestion(loc, w)]
    if isinstance(look, FwaLookup):
        names = set()
        for _lid, loc, w in todo:
            names |= {m.group(1) for m in TRIB_RE.finditer((loc.get("source_text") or {}).get("specific_area") or "")}
        look.prime_mouths([h["id"] for n in names for h in look.stream(n)])
    hp = Path(hunt_path or HUNT_PATH)
    hunt = json.loads(hp.read_text()) if hp.exists() else {}
    slugs = {lid: s for lid, (s, _l, _w) in D.locations().items()}
    rows = []
    for lid, loc, w in todo:
        items = list((w or {}).get("item_ids") or [])
        water = water_of(items, registry)
        r = suggest_one(loc, water, cuts_on(items, registry, smeta, props), look)
        h = (hunt.get("locations") or {}).get(lid)
        if h:
            r = apply_hunt(r, h, hunt.get("cuts") or {}, water, look, page=D.page_url(slugs[lid]),
                           labels={c.id: c.label for c in cuts_on(items, registry, smeta, props)})
        r["state"] = "no_water" if not items else "needs_cut" if D.need(loc)["missing"] else "unbound"
        r["status"] = loc.get("status") or "active"
        rows.append(r)
    out_dir.mkdir(parents=True, exist_ok=True)
    doc = {"_about": "DFO extent SUGGESTIONS (T164 / T210 follow-up) — never applied. The review app's DFO mode shows "
                     "each one; 'accept suggestion' pre-fills the extent-fix editor and the owner saves "
                     "it as their mark. Regenerate: python -m pipeline.regs.dfo_salmon.suggest",
           "generated": date.today().isoformat(), "counts": summarize(rows),
           "suggestions": {r["location_id"]: r for r in rows}}
    (out_dir / "dfo-suggestions.json").write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n")
    (out_dir / "dfo-suggestions.md").write_text(_summary_md(rows, doc["counts"]))
    if hunt:
        (out_dir / "dfo-split-hunt.md").write_text(_hunt_md(rows, hunt))
    return rows


def _summary_md(rows: list[dict], counts: dict) -> str:
    lines = ["# DFO extent suggestions (T164 / T210 follow-up)", "",
             "Suggestions only — nothing applied. Open a location in the review app's DFO mode: the",
             "suggestion is shown and drawn (violet); **accept suggestion** pre-fills the extent editor;",
             "**Save extent** records it as your mark. Regenerate with",
             "`python -m pipeline.regs.dfo_salmon.suggest` (reads registry, splits.json, marks, FWA gpkg).", "",
             f"{len(rows)} locations without a curated extent:", ""]
    words = {"existing": "matched to existing cuts / your proposals", "whole": "whole water (no cut needed)",
             "new_high": "with a NEW split — high", "new_medium": "with a NEW split — medium",
             "new_low": "with a NEW split — low", "partial": "partial (one end unresolved)",
             "none": "no suggestion"}
    for k in ("existing", "whole", "new_high", "new_medium", "new_low", "partial", "none"):
        lines.append(f"- {words[k]}: **{counts.get(k, 0)}**")
    lines += ["", "| location | suggestion | conf | reason |", "|---|---|---|---|"]
    order = {"existing": 0, "new": 1, "whole": 2, "partial": 3, "none": 4}
    for r in sorted(rows, key=lambda r: (order.get(r["matched"], 9), r["location_id"])):
        s = r.get("suggestion") or {}
        lines.append(f"| `{r['location_id']}` | {s.get('words', '—') if s.get('op') != 'whole' else 'whole' + (' + tributaries' if s.get('tributaries') else '')} "
                     f"| {r['confidence']} | {r['reason'][:160].replace('|', '/')} |")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", type=Path, default=OUT_DIR)
    ap.add_argument("--marks", type=Path, default=None, help="the marks store (default: the curated one)")
    ap.add_argument("--gpkg", default=None, help="the FWA GeoPackage (default: config fwa_data_gpkg)")
    a = ap.parse_args()
    rs = run(a.out, marks_path=a.marks, gpkg=a.gpkg)
    print(json.dumps(summarize(rs)))
