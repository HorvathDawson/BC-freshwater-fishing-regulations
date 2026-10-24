"""THE CAP SYSTEM AND THE TABLE (ROUND16 1, owner 2026-10-16) — a card group as an exact, client-evaluable
constraint list, and the species × size table read off it.

The owner's model: a TABLE of species × size bands, each cell what you may keep of that fish at that size
here today, and a HISTORY (what you have kept here and elsewhere today) — "can I keep this one?" answered from
the table and the combined limits. This module compiles one group's rules and the reader's verdicts (the card
gate's `Engine` (`engine.py`): which rule CHECKS a fish, which COUNTS a kept one) into:

  kinds   the basket's vocabulary: per species the size bands every rule's own cut points draw (merged where
          no rule tells them apart), split by origin only where the rules tell hatchery from wild —
          {id, species, from, to (None: no top), origin (None: either)}. Where a rainbow over the steelhead
          size IS a steelhead (p.80, `steelhead_cm`), that rainbow is the steelhead's kind, never its own.
  caps    [{take, kinds {here, stream, lake}, scope, by, says, if_kept_here}]: a BASKET — counts per kind
          kept here, and kept on another stream / lake of the region today — is legal IFF every cap holds:
          the basket's fish of the cap's kinds (each place's list) number at most `take`. `if_kept_here`: the
          cap holds only once the basket keeps one of its `here` kinds (a water's own number counts the fish
          kept elsewhere today, rows F9, but never stops a fish kept elsewhere). A take-0 cap is a release,
          closure or size band: none of those kinds may be kept.
  table   {bands {species: [{from, to, says}]}, cells {species: [{n, why, origin, by_origin, as}]}}: bands are
          the cut points that change what may be kept HERE (Bella Coola May 1, cutthroat: under 25 | 25–33 |
          over 33); each cell the most of that fish alone at that size here today, read off the caps (None:
          no limit), `why` when 0 (released / closed / too small / too big / protected size), `origin` when
          only one origin may be kept, `by_origin` when both may with different numbers, `as` "ST" where
          the rainbow is a steelhead (its number is the steelhead's).

Which order fish are kept in matters across waters (RULINGS Q23b); the basket is read as the client asks it:
what may still be kept HERE after everything kept elsewhere today, i.e. the fish kept elsewhere first. Within
one place the order never matters (a rule checking a fish there counts exactly the fish it checks there).

THE BOUNDARY STANDARD (ROUND19 5, RULINGS P37): every band (a table band, a cap kind) says which of its ends it
holds — `from_in` / `to_in` — and its words say the same ("X or less" / "X or longer" include X, "under X" / "over X"
exclude X, `common.band_words`): a fish exactly at a cut belongs to the band whose words include it. Which side a
cut falls to is read off the rule that draws it (`cut_sides`): a band that keeps none gives its bound to the
keeping side (the book's exact bound is legal, `catalogue.EXACT_BOUND_IS_LEGAL`; its "X cm or more / or less" keeps
the bound inside, `closed`); a size clause's "over X" leaves X below; a quota's keep band holds its bounds; the
steelhead size leaves X a rainbow (p.80: OVER 50 cm). A release band's word decides before any other.

THE GATE (`card_check.check_caps`) proves every cell equals the engine's most alone, and that the caps accept
EXACTLY the baskets the engine accepts (`Engine.accepts`, the counter-state solver's compiled fish), on
seeded random baskets with fish kept elsewhere — never trusting this module's own reading.
"""
from __future__ import annotations

import math
from typing import Dict, List, Optional, Sequence, Tuple

from pipeline.deliver.answers import common
from pipeline.common import entry_id as EID

INF = math.inf
PLACES = ("here", "stream", "lake")
_HERE_ASK = ("", "", 0.0, "here")
_AWAY_ASK = ("", "", 0.0, "stream")
_ORIGINS = ("hatchery", "wild")


def _js(v):
    if v is None or v == INF:
        return None
    return int(v) if float(v).is_integer() else v


def _cuts(E, S: str) -> List[float]:
    pts = {0.0}
    for r in E.rules:
        if S not in r.species or S in r.except_:
            continue
        for a, b, _t in E._bands_of.get(id(r)) or _bands(E, r):
            pts.add(float(a))
            if b != INF:
                pts.add(float(b))
    if E.P.steelhead_water and S in ("RB", "ST"):
        pts.add(float(E.st_cm))
    return sorted(pts)


def _bands(E, r):
    from pipeline.deliver.answers.engine import bands_of as b
    got = E._bands_of[id(r)] = b(r)
    return got


def _mid(a: float, b: float) -> float:
    return a + 5 if b == INF else (a + b) / 2


def _redefined(E, S: str, a: float, b: float) -> Optional[str]:
    """Where a rainbow over the steelhead size IS a steelhead (and a steelhead under it a rainbow)."""
    if not E.P.steelhead_water:
        return None
    L = _mid(a, b)
    if S == "RB" and L > E.st_cm:
        return "ST"
    if S == "ST" and L < E.st_cm:
        return "RB"
    return None


def _sig(E, f, here_only: bool) -> tuple:
    """How every rule treats the fish `f` (checks it, counts it for a fish asked here / elsewhere, its band)."""
    out = []
    for r in E.rules:
        c = E.checks(r, f)
        ch = E.counts(r, f, _HERE_ASK)
        ca = False if here_only else E.counts(r, f, _AWAY_ASK)
        if not (c or ch or ca):
            out.append(None)
            continue
        hit = E._band(r, f[2])
        if hit is None:
            band = None
        else:
            bi, take = hit
            band = "free" if take is None and r.k != "gate" else (bi, r.k == "gate" or take == 0)
        out.append((c, ch, ca, band))
    return tuple(out)


def _merge(E, S: str, here_only: bool) -> List[Tuple[float, float, bool]]:
    """The species' bands: (from, to, split by origin), neighbours no rule tells apart merged."""
    pts = _cuts(E, S) + [INF]
    out: List[list] = []
    for a, b in zip(pts, pts[1:]):
        L = _mid(a, b)
        places = ("here",) if here_only else PLACES
        sig = {o: tuple(_sig(E, (S, o, L, p), here_only) for p in places) for o in _ORIGINS}
        red = _redefined(E, S, a, b)
        key = (red, sig["hatchery"], sig["wild"])
        if out and out[-1][3] == key:
            out[-1][1] = b
        else:
            out.append([a, b, sig["hatchery"] != sig["wild"], key])
    return [(a, b, split) for a, b, split, (red, *_s) in out if not red] if not here_only else \
        [(a, b, split, red) for a, b, split, (red, *_s) in out]


def rule_sides(r) -> List[Tuple[float, str, int]]:
    """ROUND19 5: the cuts one rule draws, each with the side a fish exactly at it falls to ("below" / "above") and
    its weight (0: a band keeping none decides; 1: any other band) — see the module docstring."""
    out: List[Tuple[float, str, int]] = []
    for L in (r.x.get("lengths") or []):
        mn, mx, t = L.get("min_cm"), L.get("max_cm"), L.get("take")
        if t == 0:
            inside = bool(L.get("closed"))
            if mn is not None:
                out.append((float(mn), "above" if inside else "below", 0))
            if mx is not None:
                out.append((float(mx), "below" if inside else "above", 0))
        elif mn is not None and mx is None:
            # a quota's keep floor holds its bound ("30 cm or longer"); a clause's "over X" does not
            out.append((float(mn), "above" if r.k == "pool" else "below", 1))
        else:
            if mn is not None:
                out.append((float(mn), "above", 1))
            if mx is not None:
                out.append((float(mx), "below", 1))
    return out


def cut_sides(E, S: str) -> Dict[float, str]:
    """ROUND19 5: per cut point of a species' bands, the side a fish exactly at it falls to: the rules' own words,
    a release band's first (`rule_sides`), then the first rule's; the steelhead size leaves the cut a rainbow."""
    got: Dict[float, Tuple[int, int, str]] = {}
    for i, r in enumerate(E.rules):
        if S not in r.species or S in r.except_:
            continue
        for x, side, w in rule_sides(r):
            if x not in got or (w, i) < got[x][:2]:
                got[x] = (w, i, side)
    if E.P.steelhead_water and S in ("RB", "ST"):
        x = float(E.st_cm)
        if x not in got or got[x][0] > 0:
            got[x] = (1, -1, "below")
    return {x: v[2] for x, v in got.items()}


def edges(sides: Dict[float, str], a: float, b: float) -> Tuple[bool, bool]:
    """(from_in, to_in) of the band [a, b] under the boundary standard (no top: `to_in` False; from 0: True)."""
    return (not a or sides.get(float(a), "below") == "above",
            b is not None and b != INF and sides.get(float(b), "below") == "below")


def _kid(S: str, i: int, o: Optional[str]) -> str:
    return f"{S}.{i}" + ("" if o is None else "." + o[0])


def system(P, total: str, species: Sequence[str]) -> dict:
    """{kinds, caps, steelhead_cm, table} of one group (the module docstring). `species`: the fish of the
    table (the group's kinds, then the fish its row holds that nobody may keep here)."""
    from pipeline.deliver.answers.engine import Engine
    E = Engine(P, total)
    kinds: List[dict] = []
    for S in species:
        sides = cut_sides(E, S)
        for i, (a, b, split) in enumerate(_merge(E, S, here_only=False)):
            fi, ti = edges(sides, a, b)
            for o in (_ORIGINS if split else (None,)):
                kinds.append({"id": _kid(S, i, o), "species": S, "from": _js(a), "to": _js(b), "origin": o,
                              "from_in": fi, "to_in": ti})
    caps = _caps(E, kinds)
    table = _table(E, species, kinds, caps)
    return {"kinds": kinds, "caps": caps, "steelhead_cm": _js(E.st_cm) if P.steelhead_water else None,
            "table": table}


def rep(E, k: dict, place: str, origin: Optional[str] = None, L: Optional[float] = None):
    """A fish of kind `k` kept at `place` (an origin-free kind: `origin`, default hatchery)."""
    a, b = k["from"] or 0, INF if k["to"] is None else k["to"]
    return (k["species"], k["origin"] or origin or "hatchery", _mid(a, b) if L is None else L, place)


def _caps(E, kinds: List[dict]) -> List[dict]:
    """One cap per rule band that limits keeping (the module docstring), then: kinds a take-0 cap holds
    leave every other cap, and a cap another implies goes."""
    raw: List[dict] = []
    for r in E.rules:
        per: Dict[object, dict] = {}
        for k in kinds:
            for place in PLACES:
                f = rep(E, k, place)
                hit = E._band(r, f[2])
                if hit is None:
                    continue
                bi, take = hit
                zero = r.k == "gate" or take == 0
                if take is None and not zero:
                    continue
                c = E.checks(r, f)
                ch = E.counts(r, f, _HERE_ASK)
                key = "zero" if zero else bi
                bs = E._bands_of.get(id(r)) or []
                band = (bs[bi][0], bs[bi][1]) if bs else (0, INF)
                cap = per.setdefault(key, {"take": 0 if zero else int(math.floor(take)), "r": r,
                                           "checked": {p: [] for p in PLACES},
                                           "counted": {p: [] for p in PLACES}, "ranges": set()})
                cap["ranges"].add(band)
                if c:
                    cap["checked"][place].append(k["id"])
                if ch:
                    cap["counted"][place].append(k["id"])
        for key, cap in per.items():
            ck, cn = cap["checked"], cap["counted"]
            if key == "zero":
                if any(ck.values()):
                    raw.append({"take": 0, "kinds": ck, "by": r.key, "if_kept_here": False,
                                "range": _joined(cap["ranges"])})
                continue
            zone = id(r) in E._zone
            if cn["here"] != ck["here"]:
                raise ValueError(f"caps: {r.key} counts other fish here than it checks")
            if zone:
                # a zone rule in force on every water of the region: it checks a fish kept elsewhere and
                # counts the same fish for one kept here — one cap over here and elsewhere
                for p in ("stream", "lake"):
                    if cn[p] != ck[p]:
                        raise ValueError(f"caps: {r.key} counts other fish elsewhere than it checks")
                if any(ck.values()):
                    raw.append({"take": cap["take"], "kinds": ck, "by": r.key, "if_kept_here": False,
                                "range": _joined(cap["ranges"])})
            elif ck["here"]:
                # a water's own number: what is kept here, with the fish kept elsewhere it counts (rows F9)
                away = bool(cn["stream"] or cn["lake"])
                raw.append({"take": cap["take"], "kinds": {"here": ck["here"], "stream": cn["stream"],
                                                           "lake": cn["lake"]},
                            "by": r.key, "if_kept_here": away, "range": _joined(cap["ranges"]),
                            # rows F17: a raised tally is encoded as its quota's rule, worded as itself
                            "rule": r})
    zero = {(p, k) for c in raw if c["take"] == 0 for p in PLACES for k in c["kinds"][p]}
    out: List[dict] = []
    for c in raw:
        if c["take"]:
            c = {**c, "kinds": {p: [k for k in c["kinds"][p] if (p, k) not in zero] for p in PLACES}}
            if not any(c["kinds"].values()):
                continue
            if c["if_kept_here"] and not c["kinds"]["here"]:
                continue                          # held only once a kind kept here none may keep: never
            if not (c["kinds"]["stream"] or c["kinds"]["lake"]):
                c["if_kept_here"] = False
        out.append(c)
    # merge the take-0 caps of one rule; drop a cap another implies
    keep: List[dict] = []
    for i, a in enumerate(out):
        A = {(p, k) for p in PLACES for k in a["kinds"][p]}
        implied = False
        for j, b in enumerate(out):
            if i == j:
                continue
            Bs = {(p, k) for p in PLACES for k in b["kinds"][p]}
            if A <= Bs and b["take"] <= a["take"] and (not b["if_kept_here"] or a["if_kept_here"]):
                if A == Bs and b["take"] == a["take"] and b["if_kept_here"] == a["if_kept_here"] and j > i:
                    continue                      # the same cap twice: the first stays
                implied = True
                break
        if not implied:
            keep.append(a)
    return [_said(E, c, kinds) for c in keep]


def _joined(rs) -> Optional[Tuple[float, float]]:
    """The rule's bands a cap holds, as one range where they touch, else None."""
    xs = sorted(rs)
    lo, hi = xs[0]
    for a, b in xs[1:]:
        if a > hi:
            return None
        hi = max(hi, b)
    return lo, hi


def _union(ks: List[dict]) -> Optional[Tuple[float, float]]:
    """The one length range the kinds cover together, per species alike, else None."""
    per: Dict[str, List[Tuple[float, float]]] = {}
    for k in ks:
        per.setdefault(k["species"], []).append((k["from"] or 0, INF if k["to"] is None else k["to"]))
    got = set()
    for S, xs in per.items():
        xs.sort()
        lo, hi = xs[0]
        for a, b in xs[1:]:
            if a > hi:
                return None
            hi = max(hi, b)
        got.add((lo, hi))
    return next(iter(got)) if len(got) == 1 else None


def _said(E, c: dict, kinds: List[dict]) -> dict:
    """The cap in words: its scope and one sentence — the rule's own fish, the kinds' origin and size."""
    from pipeline.deliver.answers.display import written_name
    r = c.get("rule") or E.P.by_key[c["by"]]
    by_id = {k["id"]: k for k in kinds}
    ks = c["kinds"]
    region = "B.C." if EID.is_province(r.entry_id) else (r.x.get("entry_name") or "the region")
    if not (ks["stream"] or ks["lake"]):
        scope, where = "here", ""
    elif c["if_kept_here"]:
        scope, where = "here, counting fish kept elsewhere today", ""
    elif ks["stream"] and ks["lake"]:
        scope, where = f"a day across {region}", f" in {region}"
    else:
        w = "streams" if ks["stream"] else "lakes"
        scope, where = f"a day from {region} {w}", f" from {region} {w}"
    ids = [by_id[k] for p in PLACES for k in ks[p]]
    fish = common.lc(written_name(r.x, " or " if c["take"] <= 1 else " and ")) if r.x.get("species") else "fish"
    rng = c.get("range") or _union(ids)
    size = ""
    if rng is not None and rng != (0, INF):
        # ROUND19 2/5: in cm (inches), its ends by the boundary standard, read off the rule's own words
        a, b = rng
        sides: Dict[float, str] = {}
        for x, side, w in sorted(rule_sides(r), key=lambda t: t[2]):
            sides.setdefault(x, side)
        size = " " + common.band_words(a, b, *edges(sides, a, b))
    origins = {k["origin"] for k in ids}
    org = f"{next(iter(origins))} " if len(origins) == 1 and None not in origins else ""
    if c["take"] == 0:
        says = f"No {org}{fish}{size} may be kept{where}"
    else:
        says = f"Up to {c['take']} {org}{fish}{size} {scope}"
    # ROUND20: one full stop — "… may be kept in B.C." already ends in one ("in B.C.." before, 48 live occurrences)
    return {"take": c["take"], "kinds": ks, "scope": scope, "by": c["by"], "says": common.full_stop(says),
            "if_kept_here": c["if_kept_here"]}


def common_num(v) -> str:
    return str(int(v)) if float(v).is_integer() else str(v)


def accepts(caps: Sequence[dict], basket: Dict[str, Dict[str, int]]) -> bool:
    """The cap system's answer: every cap holds for the basket ({place: {kind id: count}})."""
    for c in caps:
        n = sum(basket.get(p, {}).get(k, 0) for p in PLACES for k in c["kinds"][p])
        if c["if_kept_here"] and not any(basket.get("here", {}).get(k, 0) for k in c["kinds"]["here"]):
            continue
        if n > c["take"]:
            return False
    return True


def most_here(caps: Sequence[dict], ids: Sequence[str]) -> Optional[int]:
    """The most fish of the kinds `ids` kept here alone that the caps allow (None: no limit)."""
    zero = {k for c in caps if c["take"] == 0 for k in c["kinds"]["here"]}
    free = [k for k in ids if k not in zero]
    if not free:
        return 0
    held = [(c["take"], [k for k in c["kinds"]["here"] if k in free]) for c in caps if c["take"]]
    held = [(t, ks) for t, ks in held if ks]
    if any(not any(k in ks for _t, ks in held) for k in free):
        return None
    # a small integer programme: every kind up to the smallest cap holding it
    ub = {k: min(t for t, ks in held if k in ks) for k in free}
    best = 0

    def go(i: int, left: Tuple[int, ...], got: int):
        nonlocal best
        if i == len(free):
            best = max(best, got)
            return
        if got + sum(ub[k] for k in free[i:]) <= best:
            return
        k = free[i]
        room = min([ub[k]] + [left[j] for j, (_t, ks) in enumerate(held) if k in ks])
        for x in range(room, -1, -1):
            go(i + 1, tuple(v - x if k in held[j][1] else v for j, v in enumerate(left)), got + x)
    go(0, tuple(t for t, _ks in held), 0)
    return best


def _why(E, k: dict, caps: Sequence[dict]) -> Optional[str]:
    from pipeline.deliver.bundle.rules import closure_grade
    whys = []
    for c in caps:
        if c["take"] or k["id"] not in c["kinds"]["here"]:
            continue
        r = E.P.by_key[c["by"]]
        if closure_grade(r.f) is not None:
            whys.append((0, "closed"))
            continue
        hit = E._band(r, rep(E, k, "here")[2])
        bs = E._bands_of.get(id(r)) or []
        if bs and hit is not None:
            a, b, _t = bs[hit[0]]
            whys.append((1, "too small" if not a and b != INF else "too big" if b == INF and a
                         else "protected size" if a and b != INF else "released"))
        else:
            whys.append((2, "released"))
    return min(whys)[1] if whys else None


def _table(E, species: Sequence[str], kinds: List[dict], caps: List[dict]) -> dict:
    """Per species the bands that change what may be kept here, each cell read off the caps; neighbouring
    bands keeping none for the same reason are one ("under 30 cm: too small")."""
    bands: Dict[str, List[dict]] = {}
    cells: Dict[str, List[dict]] = {}
    for S in species:
        bands[S], cells[S] = [], []
        sides = cut_sides(E, S)
        for a, b, _split, red in _merge(E, S, here_only=True):
            T = red or S
            mine = [k for k in kinds if k["species"] == T and (k["from"] or 0) < b
                    and (k["to"] is None or k["to"] > a)]
            cell = _cell(E, mine, caps)
            cell["as"] = red
            if cells[S] and cells[S][-1] == cell and cell["n"] == 0:
                bd = bands[S][-1]
                bd["to"] = _js(b)
                bd["to_in"] = edges(sides, a, b)[1]
                bd["says"] = common.band_words(bd["from"] or 0, b, bd["from_in"], bd["to_in"])
                continue
            fi, ti = edges(sides, a, b)
            # ROUND19 2/5: in cm (inches), the ends it holds by the boundary standard
            bands[S].append({"from": _js(a), "to": _js(b), "says": common.band_words(a, b, fi, ti),
                             "from_in": fi, "to_in": ti})
            cells[S].append(cell)
    return {"bands": bands, "cells": cells}


def _cell(E, mine: List[dict], caps: List[dict]) -> dict:
    ids = [k["id"] for k in mine]
    n = most_here(caps, ids)
    per = {o: most_here(caps, [k["id"] for k in mine if k["origin"] in (None, o)]) for o in _ORIGINS}
    cell: dict = {"n": n, "why": None, "origin": None, "by_origin": None, "as": None}
    if n == 0:
        cell["why"] = next((w for k in mine for w in [_why(E, k, caps)] if w), "released")
    elif per["hatchery"] == 0 or per["wild"] == 0:
        cell["origin"] = "hatchery" if per["hatchery"] != 0 else "wild"
    elif per["hatchery"] != per["wild"]:
        cell["by_origin"] = per
    return cell
