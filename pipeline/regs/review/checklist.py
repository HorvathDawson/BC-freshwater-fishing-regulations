"""THE CHECKLIST ROWS OF AN EXTENT (T532): what a reviewer checks, one claim per row, GENERATED — never hand-written.

    rows(source, extents, ...)   the claims: the water, each piece's from / to (its cuts), every extra piece, the
                                 tributary call, each exception, area membership — each paired with the phrase of the
                                 source words that names it — and the words no row covers, as their own row
    dfo_source(loc, water)       a DFO place's words (title + specific area + DFO's own exclusions)
    rule_source(entry, rule)     a regulation's words (the row's name and ✱, the rule's place words)
    digest(...)                  what a check was made on: the words and the extent — a check whose digest differs from
                                 the place's now is STALE (re-check, never silently kept)
    summary(rows)                {ok, unsure, bad} counts and the place's worst state

One module for both sources (owner 2026-11-09: "ideally the same interface"): a DFO place and a regulation rule are
the same `Extent` model, so the same rows. Pure: everything the build knows (which cuts are built, their labels and
points, the water names, the tributary call) is handed in through `Context` by the review backend, which reads it from
the builder's own modules (AGENTS 16 — this module resolves nothing, it pairs claims with words).

A row's state is honest:
    ok      built, and the words name it (or say nothing the extent contradicts)
    unsure  it cannot be ✓ yet: saved / proposed but its cut is not built, or words no row checks
    bad     the words say something the extent does not (an exception, a tributary call, a place with no cut), or the
            extent says something the words do not (tributaries walked with no word for it, a carve-out never named)
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Iterable, Mapping, Optional, Sequence

OK, UNSURE, BAD = "ok", "unsure", "bad"
STATES = (OK, UNSURE, BAD)
#: the row kinds, in the order a screen may explain them
KINDS = ("water", "where", "from", "to", "also", "area", "tributaries", "except", "unchecked")

#: words that carry no claim of their own (a phrase is paired by what is left)
_STOP = frozenset("""
a an the of at on in to and or from for by with its it is are be as that this these those any all each other
approximately approx about some which where who into onto near nearest located location situated
river creek lake lakes rivers creeks slough channel stream streams water waters
fishing no closed open closure area areas part parts portion portions
including include includes locally known either side starts start begins ends identified waters there
""".split())
#: the place words a phrase of a REGULATION'S verbatim must hold to be a place phrase (the rest — "fly fishing only",
#: "Dec 1-May 31" — is the rule's content, checked on the rules screen, not here)
_PLACE = re.compile(r"\b(upstream|downstream|above|below|between|confluence|mouth|outlet|inlet|bridge|dam|"
                    r"signs?|boundary|marker|km(?!/h)|kilometres?|\d+\s*m|metres|falls|tributar\w*|shore|bay|arm|park|"
                    r"reserve|road|highway|hwy|crossing|weir|lake|river|creek|slough|channel)\b", re.I)
#: synonyms folded before pairing ("Hwy 19" names the same bridge as "Highway 19")
_SYN = {"hwy": "highway", "rd": "road", "mt": "mount", "st": "street", "ave": "avenue", "km": "kilometre",
        "kilometres": "kilometre", "kilometers": "kilometre", "kilometer": "kilometre", "metres": "m", "meters": "m",
        "metre": "m", "meter": "m", "approx": "approximately", "trib": "tributary", "tribs": "tributary",
        "tributaries": "tributary", "u": "upstream", "d": "downstream"}
#: clause boundaries: punctuation, and the words a new claim starts with
_CUT = re.compile(r"[;:()\n]|\.(?=\s|$)|,(?=\s)|\s(?=(?:to|from|except|excluding|including|and that|and the portion|"
                  r"but not|other than)\b)", re.I)
#: a phrase ending in a distance or a placing word: the next "from" / "to" names where it is measured from
_LOCATED = re.compile(r"(\d+(?:\.\d+)?\s*(?:m|km|metres?|meters?|kilometres?|kilometers?)"
                      r"(?:\s+(?:downstream|upstream|above|below|up|down))?|\b(?:located|situated|"
                      r"approximately|approx\.?))\s*$", re.I)
#: a direction word closing a phrase ("… and Slesse Creek downstream")
_TRAILING_DIRECTION = re.compile(r"\s+(downstream|upstream)\s*[.,;]?\s*$", re.I)
_EXCEPT = re.compile(r"\b(except|excluding|exclusive of|exception|other than|but not|not including)\b", re.I)
_TRIB_YES = re.compile(r"(✱|incl\.?\s*tribs|\b(?:includ\w*|and|with|plus)\s+(?:all\s+|its\s+|their\s+)?tributar)", re.I)
_TRIB_NO = re.compile(r"\b(?:not\s+includ\w*|exclud\w*|except|without|no)\s+(?:its\s+|their\s+|the\s+)?tributar", re.I)
_TRIB_ONLY = re.compile(r"\b(?:in\s+(?:any|all)\s+|'s\s+|all\s+)tributar|\btributar\w*\s+only\b|^tributar|S TRIBUTARIES", re.I)
_TRIB_ANY = re.compile(r"(✱|incl\.?\s*tribs|tributar|watershed|drainage)", re.I)
#: a place a "to …" phrase names that a natural end (a mouth, a source) is not
_LOCATOR = re.compile(r"\b(signs?|bridge|dam|weir|km(?!/h)|m(?!/h)|line|marker|road|highway|hwy|crossing|falls|boundary)\b", re.I)
#: fish named after a kind of water ("lake trout") are not a place
_SPECIES = re.compile(r"\b(lake|river)\s+(trout|whitefish|char|chub|sturgeon)\b", re.I)
_UP = re.compile(r"\b(upstream|above|upriver|headwaters?|source)\b", re.I)
_DOWN = re.compile(r"\b(downstream|below|downriver|mouth)\b", re.I)


@dataclass(frozen=True)
class Context:
    """What the build knows, handed in by the review backend (read from the builder's own modules)."""
    #: cut id -> its label (the curated cuts / the registry boundary)
    labels: Mapping[str, str] = field(default_factory=dict)
    #: registry item id -> its name
    items: Mapping[str, str] = field(default_factory=dict)
    #: area id -> its name
    areas: Mapping[str, str] = field(default_factory=dict)
    #: every cut the served atlas has built
    built: frozenset = frozenset()
    #: cuts that exist on paper but not in the atlas: authored in the cuts file, or proposed by a saved mark
    pending: frozenset = frozenset()
    #: a cut id or a registry item id -> (lon, lat), for the row's pin
    points: Mapping[str, Sequence[float]] = field(default_factory=dict)


@dataclass(frozen=True)
class Source:
    """The words a place is checked against. `water`: the title (the DFO waters line / the row's name with its ✱);
    `place`: the place words (DFO's specific area / a rule's extent_text, unresolved locators or its verbatim);
    `excludes`: exclusions the source states apart (DFO's own `excludes`); `place_only`: drop clauses of `place` that
    name no place (a regulation's verbatim carries its content too)."""
    water: str = ""
    place: str = ""
    excludes: tuple = ()
    place_only: bool = False
    #: a part of the water the extent holds as a NOTE, never drawn (`undrawn_part`)
    undrawn: str = ""


# --------------------------------------------------------------------------- words


def _tokens(s: str) -> set[str]:
    out = set()
    for w in re.findall(r"[a-z0-9']+", (s or "").lower().replace("_", " ")):
        w = w.strip("'")
        w = _SYN.get(w, w)
        if w.endswith("'s"):
            w = w[:-2]
        if len(w) > 3 and w.endswith("s") and not w.endswith("ss"):
            w = w[:-1]
        if w and w not in _STOP:
            out.add(w)
    return out


@dataclass
class _Clause:
    i: int
    text: str
    toks: set
    used: bool = False


def _clauses(text: str, place_only: bool) -> list[_Clause]:
    """the source words cut into claims: at punctuation and where a new claim starts ("from", "to", "except" …) — but
    never inside a locator ("100 m FROM the confluence", "1 km TO the bridge" is one place), and a direction that ends
    a phrase ("… Slesse Creek downstream") is its own claim, the reach's other end"""
    parts, at = [], 0
    for m in _CUT.finditer(text or ""):
        if m.group(0).startswith(" ") and _LOCATED.search(text[at:m.start()]):
            continue
        parts.append(text[at:m.start()])
        at = m.start() if m.group(0).startswith(" ") else m.end()
    parts.append((text or "")[at:])
    split: list[str] = []
    for p in parts:
        t = _TRAILING_DIRECTION.search(p)
        if t and p[:t.start()].strip(" ,") and _tokens(p[:t.start()]):
            split += [p[:t.start()], t.group(1)]
        else:
            split.append(p)
    parts = split
    out: list[_Clause] = []
    for p in parts:
        p = p.strip(" ,;:-–\t")
        if not p or not _tokens(p):
            continue
        if place_only and not _PLACE.search(_SPECIES.sub("", p)):
            continue
        out.append(_Clause(len(out), p, _tokens(p)))
    return out


def _best(clauses: list[_Clause], want: set[str], *, prefer: Optional[re.Pattern] = None) -> Optional[_Clause]:
    """the clause sharing the most of `want` (ties: one matching `prefer`, then the first) — None when none shares any"""
    best, score = None, 0.0
    for c in clauses:
        s = len(want & c.toks) / (len(want) or 1)
        if s and prefer is not None and prefer.search(c.text):
            s += 0.01
        if s and not c.used:
            s += 0.02
        if s > score:
            best, score = c, s
    return best if score >= 0.25 else None


def _cut_words(sid: str, ctx: Context) -> set[str]:
    """what a cut is called: its label, and the words of its id after the water ("cowichan_river__holt_creek…")"""
    tail = sid.split("__", 1)[-1]
    return _tokens(ctx.labels.get(sid, "")) | _tokens(tail)


# --------------------------------------------------------------------------- rows


def _row(kind: str, quote: str, ours: str, state: str, reason: str, anchor: Optional[dict] = None) -> dict:
    return {"kind": kind, "quote": quote, "ours": ours, "state": state, "reason": reason, "anchor": anchor or {}}


def _items_of(x: Mapping) -> list[str]:
    return list(x.get("item_ids") or ([x["item_id"]] if x.get("item_id") else []))


def _name(iid: str, ctx: Context) -> str:
    return ctx.items.get(iid) or iid


def _is_region(aid: str) -> bool:
    """a region / zone polygon: a zone table's own scope, which the table's heading states"""
    return aid.startswith(("area:region:", "region:", "area:zone:", "area:mu:")) or aid.rsplit(":", 1)[-1].isdigit()


def _cut_state(sid: str, ctx: Context, landed: bool) -> tuple[str, str]:
    if sid in ctx.built:
        return (OK, "") if landed else (UNSURE, "saved, not applied yet")
    if sid in ctx.pending:
        return UNSURE, "saved, not drawn yet: the cut is not built"
    return BAD, "no cut by that id in the atlas or the cuts file"


def _cut_anchor(sid: str, ctx: Context) -> dict:
    pt = ctx.points.get(sid)
    return {"cuts": [sid], **({"coord": list(pt)} if pt else {})}


def _items_anchor(items: Iterable[str], ctx: Context, *, pin: bool = False) -> dict:
    """the waters a row is about — highlighted, never pinned (a whole water has no one point), unless `pin`: a water
    taken out is a place of its own"""
    items = list(items)
    pt = next((ctx.points[i] for i in items if i in ctx.points), None) if pin else None
    return {"items": items, **({"coord": list(pt)} if pt else {})}


def end_key(end: str, items: Sequence[str]) -> str:
    """the `Context.points` key of a water's natural end: `mouth:<items>` / `source:<items>` (the backend places it)"""
    return f"{end}:{'|'.join(items)}"


def _end_anchor(up: bool, items: list[str], ctx: Context) -> dict:
    k = end_key("source" if up else "mouth", items)
    pt = ctx.points.get(k) if k in ctx.points else None
    return {"items": items, "end": "source" if up else "mouth", **({"coord": list(pt)} if pt else {})}


def _cut_row(kind: str, sid: str, clauses: list[_Clause], ctx: Context, landed: bool, end: str) -> dict:
    label = ctx.labels.get(sid)
    c = _best(clauses, _cut_words(sid, ctx))
    state, why = _cut_state(sid, ctx, landed)
    ours = f"{end} {label}" if label else f"{end} a cut with no label ({sid})"
    if c is not None:
        c.used = True
    elif state == OK:
        state, why = BAD, "no words name this cut"
    if label is None and state == OK:
        state, why = BAD, "the cut has no label"
    r = _row(kind, c.text if c else "", ours, state, why, _cut_anchor(sid, ctx))
    r["_at"] = c.i if c is not None else None
    return r


_NEXT_PIECE = re.compile(r"^(from|including|and that|and the portion|except|excluding|other than)\b", re.I)


def _window(clauses: list[_Clause], at: Optional[int]) -> list[_Clause]:
    """the clauses that belong to the piece whose start is clause `at`: up to the next piece's start"""
    if at is None:
        return []
    out = []
    for c in clauses[at + 1:]:
        if _NEXT_PIECE.match(c.text):
            break
        out.append(c)
    return out


def _natural_end_row(op: str, items: list[str], clauses: list[_Clause], ctx: Context, landed: bool,
                     at: Optional[int] = None) -> dict:
    """an upstream_of / downstream_of runs to the water's source / mouth: paired with the direction the words give"""
    up = op == "upstream_of"
    want, other = (_UP, _DOWN) if up else (_DOWN, _UP)
    ours = "to the source (upstream)" if up else "to the mouth (downstream)"
    near = _window(clauses, at)
    hit = next((c for c in near if want.search(c.text)), None) \
        or next((c for c in clauses if want.search(c.text) and not c.used), None) \
        or next((c for c in clauses if want.search(c.text)), None)
    named_end = next((c for c in (near if at is not None else clauses) if re.match(r"to\b", c.text, re.I)
                      and _LOCATOR.search(c.text) and not c.used), None)
    anchor = _end_anchor(up, items, ctx)
    if named_end is not None:
        named_end.used = True
        return _row("to", named_end.text, ours, BAD, "the words name an end; ours runs on with no cut there", anchor)
    if hit is not None:
        hit.used = True
        return _row("to", hit.text, ours, OK if landed else UNSURE, "" if landed else "saved, not applied yet", anchor)
    if any(other.search(c.text) for c in clauses) and not any(want.search(c.text) for c in clauses):
        c = next(c for c in clauses if other.search(c.text))
        return _row("to", c.text, ours, BAD, f"the words run {'downstream' if up else 'upstream'}", anchor)
    return _row("to", "", ours, OK if landed else UNSURE, "the words give no direction" if landed
                else "saved, not applied yet", anchor)


def _piece_rows(x: Mapping, n: int, own: list[str], clauses: list[_Clause], ctx: Context, landed: bool,
                src: Source) -> list[dict]:
    """the rows of one piece: its where / from / to, its area, its carve-outs (`n` > 0: a piece beyond the first,
    its first row is `also`)"""
    op = str(x.get("op"))
    items = _items_of(x)
    on = [i for i in items if i not in own]
    on_words = (" on " + " and ".join(_name(i, ctx) for i in on)) if on else ""
    out: list[dict] = []
    first = "also" if n else None
    if op == "whole":
        what = ("the whole of " + " and ".join(_name(i, ctx) for i in items)) if items else "the whole water"
        loc = [c for c in clauses if _LOCATOR.search(c.text) or re.search(r"\b(upstream|downstream|between|above|"
                                                                          r"below|portion|part)\b", c.text, re.I)]
        if n:
            c = _best(clauses, set().union(*(_tokens(_name(i, ctx)) for i in items)) if items else set())
            if c is not None:
                c.used = True
            out.append(_row("also", c.text if c else "", what, OK if c else BAD,
                            "" if c else "no words name this water", _items_anchor(items or own, ctx)))
        elif src.undrawn:
            c = _best(clauses, _tokens(src.undrawn))
            if c is not None:
                c.used = True
            out.append(_row("where", c.text if c else src.undrawn, what, BAD,
                            "a part of the water held as a note, never drawn", _items_anchor(items or own, ctx)))
        elif loc:
            c = loc[0]
            c.used = True
            out.append(_row("where", c.text, what, BAD, "the words name a part; ours is the whole water",
                            _items_anchor(items or own, ctx)))
        else:
            out.append(_row("where", "" if clauses else "(no area given: the whole water)", what, OK, "",
                            _items_anchor(items or own, ctx)))
    elif op in ("upstream_of", "downstream_of"):
        sid = (x.get("splits") or ["?"])[0]
        start = _cut_row(first or "from", sid, clauses, ctx, landed, ("from" + on_words).strip())
        out.append(start)
        out.append(_natural_end_row(op, items or own, clauses, ctx, landed, start["_at"]))
    elif op == "between":
        a, b = (list(x.get("splits") or []) + ["?", "?"])[:2]
        out.append(_cut_row(first or "from", a, clauses, ctx, landed, ("from" + on_words).strip()))
        out.append(_cut_row("to", b, clauses, ctx, landed, "to"))
    elif op == "within":
        aid = str(x.get("area_id") or "")
        name = ctx.areas.get(aid) or (aid.rsplit(":", 1)[-1].replace("_", " ") if aid
                                      else str(x.get("area_kind") or "?").replace("_", " "))
        kinds = x.get("feature_types") or []
        c = _best(clauses, _tokens(name), prefer=re.compile(r"\bwithin\b", re.I))
        if c is not None:
            c.used = True
        ours = f"{'every ' + ' and '.join(kinds) if kinds else 'all waters'} within {name}"
        why = "" if c else ("the zone's own table" if _is_region(aid) or x.get("area_kind") == "region"
                            else "the words do not name the area")
        out.append(_row(first or "area", c.text if c else "", ours, OK, why,
                        {"areas": [aid] if aid else [], "area_kind": x.get("area_kind")}))
    elif op == "rest":
        c = next((c for c in clauses if re.search(r"\b(other|rest|remainder)\b", c.text, re.I)), None)
        if c is not None:
            c.used = True
        out.append(_row(first or "where", c.text if c else "", "the rest of the water, after its other places", OK,
                        "", _items_anchor(items or own, ctx)))
    elif op == "steelhead_waters":
        out.append(_row(first or "where", "", "the book's steelhead waters", OK, "", {}))
    else:
        out.append(_row(first or "where", "", op, BAD, f"an op this checklist does not know: {op}", {}))
    if x.get("within_area") and not _is_region(str(x["within_area"])):
        aid = str(x["within_area"])
        name = ctx.areas.get(aid) or aid.rsplit(":", 1)[-1].replace("_", " ")
        c = _best(clauses, _tokens(name))
        if c is not None:
            c.used = True
        out.append(_row("area", c.text if c else "", f"only within {name}", OK if c else BAD,
                        "" if c else "ours keeps only the part within an area the words do not name",
                        {"areas": [aid]}))
    return out


def _carve_rows(extents: Sequence[Mapping], excludes: Sequence[Mapping], clauses: list[_Clause],
                ctx: Context) -> list[dict]:
    """every carve-out the extent makes, each paired with the words that except it — a carve-out no words name is ✗"""
    out = []
    taken: list[tuple[str, set, dict]] = []
    for x in extents:
        for iid in x.get("outside_items") or []:
            taken.append((f"{_name(iid, ctx)} taken out", _tokens(_name(iid, ctx)), _items_anchor([iid], ctx, pin=True)))
        for aid in ([x["outside_area"]] if x.get("outside_area") else []) + list(x.get("outside_areas") or []):
            name = ctx.areas.get(aid) or aid.rsplit(":", 1)[-1].replace("_", " ")
            taken.append((f"{name} taken out", _tokens(name), {"areas": [aid]}))
        if x.get("outside_area_kind"):
            k = str(x["outside_area_kind"]).replace("_", " ")
            taken.append((f"every {k} taken out", _tokens(k), {"area_kind": x["outside_area_kind"]}))
    for x in excludes:
        items = _items_of(x)
        sp = list(x.get("splits") or [])
        name = " and ".join(_name(i, ctx) for i in items) or " and ".join(ctx.labels.get(s, s) for s in sp) or "?"
        want = set().union(*(_tokens(_name(i, ctx)) for i in items)) if items else set()
        for s in sp:
            want |= _cut_words(s, ctx)
        taken.append((f"the tributaries into {name} not walked", want,
                      {**_items_anchor(items, ctx), **({"cuts": sp} if sp else {})}))
    for ours, want, anchor in taken:
        c = _best(clauses, want, prefer=_EXCEPT)
        if c is not None:
            c.used = True
        out.append(_row("except", c.text if c else "", ours, OK if c else BAD,
                        "" if c else "ours takes it out; the words do not say so", anchor))
    return out


def _except_words_rows(clauses: list[_Clause], extra: Sequence[str]) -> list[dict]:
    """an exception the words state that no carve-out answered: ✗ not cut out"""
    out = []
    for c in clauses:
        if not c.used and _EXCEPT.search(c.text) and not re.search(r"tributar", c.text, re.I):
            c.used = True
            out.append(_row("except", c.text, "not cut out", BAD, "the words except it; ours does not take it out"))
    for t in extra:
        out.append(_row("except", t, "not cut out", BAD, "DFO excludes it; ours does not take it out"))
    return out


def _trib_row(src: Source, trib: Optional[bool], only: bool, own: list[str], clauses: list[_Clause],
              ctx: Context) -> dict:
    """the tributary call: what the words say (✱ / including / excluding / nothing) against what ours walks"""
    words = f"{src.water}\n{src.place}"
    says_no = bool(_TRIB_NO.search(words))
    says_only = bool(_TRIB_ONLY.search(words))
    # a mention of tributaries with no negation takes them in ("Tatshenshini mainstem and all other tributaries")
    says_yes = bool(_TRIB_YES.search(words)) or says_only or (bool(_TRIB_ANY.search(words)) and not says_no)
    walks = bool(trib) or only
    ours = ("the tributaries only" if only else "with its tributaries") if walks else "main stem only"
    quote = ""
    for c in clauses:
        if _TRIB_ANY.search(c.text):
            c.used = True
            quote = quote or c.text
    if not quote:
        m = _TRIB_ANY.search(src.water)
        quote = src.water if m else ""
    anchor = {**_items_anchor(own, ctx), "tributaries": True}
    if says_no and walks:
        return _row("tributaries", quote, ours, BAD, "the words leave the tributaries out; ours walks them", anchor)
    if says_yes and not says_no and not walks:
        return _row("tributaries", quote, ours, BAD, "the words take in the tributaries; ours does not walk them",
                    anchor)
    if says_only and walks and not only:
        return _row("tributaries", quote, ours, BAD, "the words are on the tributaries only; ours holds the main stem "
                    "too", anchor)
    if walks and not (says_yes or says_no):
        return _row("tributaries", "(nothing about tributaries)", ours, BAD,
                    "the words say nothing about tributaries; ours walks them", anchor)
    return _row("tributaries", quote or "(nothing about tributaries)", ours, OK, "", anchor)


def rows(source: Source, extents: Sequence[Mapping], *, own_items: Sequence[str], ctx: Context,
         tributaries: Optional[bool] = None, tributaries_only: bool = False,
         tributary_excludes: Sequence[Mapping] = (), landed: Sequence[bool] | None = None,
         no_water_why: str = "") -> list[dict]:
    """THE ROWS of one place's extent (`extents`: its pieces, the place binds their union). `landed[i]` False marks
    piece i saved but not in the curated file yet (a DFO save waiting for its cut or the write-through): its rows are
    ? at best. Every row is numbered (`n`, from 1) — the pin on the map."""
    own = list(own_items)
    clauses = _clauses(source.place, source.place_only)
    landed = list(landed) if landed is not None else [True] * len(extents)
    out: list[dict] = []
    # the water: the title names it; ours, the registry waters it holds on
    if own:
        names = " + ".join(_name(i, ctx) for i in own)
        out.append(_row("water", source.water or "", names, OK, "", _items_anchor(own, ctx)))
    elif extents and all(str(x.get("op")) in ("within", "steelhead_waters") or _items_of(x) for x in extents):
        out.append(_row("water", source.water or "", "no water of its own: each piece names its own", OK, ""))
    else:
        out.append(_row("water", source.water or "", "no water found", BAD, no_water_why or "no registry water "
                        "matches these words: nothing is drawn"))
    if not extents:
        out.append(_row("where", source.place or "(no area given: the whole water)", "no extent", BAD,
                        "nothing binds: the place has no extent"))
    for i, x in enumerate(extents):
        out += _piece_rows(x, i, own, clauses, ctx, landed[i] if i < len(landed) else True, source)
    if own:                          # a place with no water of its own walks nothing of its own
        out.append(_trib_row(source, tributaries, tributaries_only, own, clauses, ctx))
    out += _carve_rows(extents, tributary_excludes, clauses, ctx)
    out += _except_words_rows(clauses, [e for e in source.excludes if isinstance(e, str) and e.strip()])
    # a phrase that only names the place's own water (or the title's) is the water row's
    named = _tokens(source.water).union(*(_tokens(_name(i, ctx)) for i in own)) | {"upstream", "downstream"}
    left = [c.text for c in clauses if not c.used and not c.toks <= named]
    if left:
        out.append(_row("unchecked", " … ".join(left), "words not checked", UNSURE,
                        "no row checks these words: read them"))
    for n, r in enumerate(out, 1):
        r["n"] = n
        r.pop("_at", None)
    return out


def summary(rs: Sequence[Mapping]) -> dict:
    """{ok, unsure, bad, worst}: the counts by state and the place's worst state"""
    c = {s: sum(1 for r in rs if r["state"] == s) for s in STATES}
    worst = BAD if c[BAD] else UNSURE if c[UNSURE] else OK
    return {**c, "worst": worst}


def digest(source: Source, extents: Sequence[Mapping], *, tributaries: Optional[bool] = None,
           tributaries_only: bool = False, tributary_excludes: Sequence[Mapping] = ()) -> str:
    """WHAT A CHECK IS MADE ON: the words and the extent (its pieces as a set, its tributary call, its carve-outs). A
    check whose digest is not the place's now is stale — the words or the extent changed since it was checked."""
    pieces = sorted(json.dumps(dict(x), sort_keys=True) for x in extents)
    body = {"water": source.water, "place": source.place, "excludes": list(source.excludes),
            "undrawn": source.undrawn, "pieces": pieces, "tributaries": tributaries, "only": tributaries_only,
            "tex": sorted(json.dumps(dict(x), sort_keys=True) for x in tributary_excludes)}
    return hashlib.sha1(json.dumps(body, sort_keys=True).encode()).hexdigest()[:16]


# --------------------------------------------------------------------------- the two sources


def dfo_source(loc: Mapping, water: Optional[Mapping]) -> Source:
    """A DFO place's words: the waters line, the specific area, and DFO's own exclusions."""
    st = loc.get("source_text") or {}
    ex = tuple(e for e in st.get("excludes") or [] if isinstance(e, str))
    return Source(water=str(st.get("waters") or loc.get("water") or ""), place=str(st.get("specific_area") or ""),
                  excludes=ex)


def rule_source(entry: Mapping, rule: Mapping) -> Source:
    """A regulation's words: the row's name with its ✱ ("Incl. Tribs"), and the rule's place words — `extent_text`
    and `unresolved_locators` when the parse kept them, else the rule's verbatim (its place phrases only)."""
    name = str(entry.get("name") or "")
    if "Incl. Tribs" in (entry.get("symbols") or []):
        name += " ✱ (Incl. Tribs)"
    own = [str(rule.get("extent_text") or "")] + [str(u) if not isinstance(u, Mapping) else str(u.get("text") or u)
                                                   for u in rule.get("unresolved_locators") or []]
    place = "; ".join(p for p in own if p.strip())
    return Source(water=name, place=place or str(rule.get("verbatim") or ""), place_only=not place,
                  undrawn=str(rule.get("undrawn_part") or "").strip())
