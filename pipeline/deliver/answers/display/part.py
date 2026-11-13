"""Per part of a water, read off the export's decoded model (`codec.expand`): `Water`, a part's runs, place,
hint and label (`runs_label`, `part_place`, `part_label`, `part_labels`), its km, the picker, and the steelhead
line."""
from __future__ import annotations

import math
import re
from typing import Dict, List, Optional, Sequence, Tuple
from pipeline.common import entry_id as EID
from pipeline.common.models.splits import confidence_words
from pipeline.deliver.answers.common import AnswersError, cap, clean, range_txt, when_dates
from pipeline.deliver.answers.display.rule import _js, cut_words, kind_of


def _win_txt(x: dict) -> str:
    w = when_dates(x.get("when"))
    return ", " + ", ".join(range_txt(a, b) for a, b in w) if w else ""


def _js_round(v: float) -> int:
    return int(math.floor(v + 0.5))


class Water:
    """One water of the export, as the page sees it: its parts with a rule set (parts outside
    B.C. carry none and are left out), in the page's order — most sections first, the export's
    order among equals — each part's rules its set's `reach` then `trib` ids, each sorted.

    `doc` is the decoded export (`codec.expand`); a rule's facts are read from `fields`,
    `provenance.rank`, `parts`, `label`, `verbatim`, `family`, `dimension`, `type`."""

    def __init__(self, water_id: str, w: dict, doc: dict):
        self.id, self.w, self.doc = water_id, w, doc
        self.export_ix = [i for i, p in enumerate(w["parts"]) if p.get("ruleset") is not None]
        self.export_ix.sort(key=lambda i: -w["parts"][i]["sections"])
        self.parts = [w["parts"][i] for i in self.export_ix]
        sets = doc["rulesets"]
        self.rules: List[List[str]] = []
        for p in self.parts:
            s = sets[p["ruleset"]]
            self.rules.append(sorted(s.get("reach") or []) + sorted(s.get("trib") or []))

    def r(self, k: str) -> dict:
        return self.doc["rules"][k]

    def f(self, k: str) -> dict:
        return self.r(k).get("fields") or {}

    def flat(self, k: str) -> dict:
        x = self.r(k)
        return {**(x.get("fields") or {}), "type": x.get("type"), "family": x.get("family"),
                "dimension": x.get("dimension")}

    def rank(self, k: str):
        return (self.r(k).get("provenance") or {}).get("rank")


def part_label(W: Water, i: int) -> str:
    """`partLabel` — what sets part i apart (consumer 3.1 step 3)."""
    n = len(W.parts)
    if n == 1:
        return "Whole water"
    counts: Dict[str, int] = {}
    for ks in W.rules:
        for k in ks:
            counts[k] = counts.get(k, 0) + 1

    def own_ok(k):
        rk = W.rank(k)
        return (counts[k] < n and W.r(k).get("family") != "information"
                and ((rk is not None and 0 <= rk <= 2)
                     or (rk == -1 and "ALL_GAME_FISH" in (W.f(k).get("species") or []))))
    own = [k for k in W.rules[i] if own_ok(k)]
    if not own:
        return "The rest of the water"

    def score(k):
        x = W.flat(k)
        return (counts[k] * 10
                - (5 if kind_of(x) == "gate" and "ALL_GAME_FISH" in (x.get("species") or [])
                   else 0)
                - (-3 if x.get("dimension") == "lift" else 0)
                - (8 if _js((W.r(k).get("parts") or {}).get("where")) else 0))
    own.sort(key=score)
    pick = own[0]
    pp = W.r(pick).get("parts") or {}
    any_where = next((k for k in own if _js((W.r(k).get("parts") or {}).get("where"))), None)
    if _js(pp.get("where")) and _js(pp.get("what")):
        dated = _win_txt(W.f(pick)) if when_dates(W.f(pick).get("when")) and \
            not re.search(r"\d", pp["what"]) else ""
        base = f"{cap(pp['where'])}: {pp['what'].lower()}{dated}"
    elif any_where is not None:
        base = (f"{cap(W.r(any_where)['parts']['where'])}: "
                f"{clean(pp.get('what') or W.r(pick).get('label')).lower()}")
    else:
        base = "Stretch with: " + re.sub(r"^No Fishing", "no fishing",
                                         clean(W.r(pick).get("verbatim")))
    base = cut_words(clean(base), 130)

    def key(w):
        return re.sub(r"^(from|between|within|in) ", "", clean(w).lower())
    seen = {key(pp.get("where") or "")}
    extra: List[str] = []
    for k in own[1:]:
        rp = W.r(k).get("parts") or {}
        w = clean(rp.get("where") or "")
        t = re.sub(r"^within ", "", w) if w and key(w) not in seen else \
            clean(rp.get("what") or W.r(k).get("label")).lower()
        if t not in extra:
            extra.append(t)
        seen.add(key(w))
    if extra:
        more = f" (+{len(extra) - 1} more)" if len(extra) > 1 else ""
        return base + f" · also {cut_words(extra[0], 70)}{more}"
    return base


def _region(e: str) -> Optional[str]:
    """A water row's region (`r7:…` -> "7"); None for a table entry."""
    return EID.region_of(e) if EID.is_water_row(e) else None


def part_place(W: Water, i: int) -> str:
    """`partPlace` — the entry a part comes from, when the water's parts come from two or more of
    its own entries (consumer 3.1 step 1)."""
    entries = W.doc["entries"]

    def ents(j):
        out = []
        for k in W.rules[j]:
            x = W.r(k)
            rk = W.rank(k)
            if rk is None:
                raise AnswersError(f"display: rule {k} ships no provenance rank")
            if rk == 0 and \
                    (entries.get(x["entry_id"]) or {}).get("kind") == "water" and \
                    x["entry_id"] not in out:
                out.append(x["entry_id"])
        return out
    every: List[str] = []
    for j in range(len(W.parts)):
        for e in ents(j):
            if e not in every:
                every.append(e)
    if len(every) < 2:
        return ""

    def say(e):
        E = entries.get(e) or {}
        m = re.search(r"\(([^)]+)\)", E.get("full_name") or "")
        sc = re.sub(r"\.$", "", E.get("scope_note") or (m.group(1) if m else "") or "")
        if sc and len(sc.split(" ")) > 1:
            return cap(sc)
        reg = _region(e)
        return f"In Region {reg.upper()}" if reg else ""
    regs = {_region(e) for e in every}
    mine: List[str] = []
    for t in (say(e) for e in ents(i)):
        if t and not (len(regs) < 2 and t.startswith("In Region ")) and t not in mine:
            mine.append(t)
    if len(mine) > 1 and all(t.startswith("In Region ") for t in mine):
        return "Where " + " and ".join(re.sub(r"^In ", "", t) for t in mine) + " meet"
    return " / ".join(mine)


_THE_LESS = re.compile(r"^\d|^[A-Z][\w.]* ?[\w.]*'s ", re.ASCII)


def begins_at(doc: dict, y: str) -> Optional[str]:
    """Where a stream BEGINS, as an end token: the upstream end (`from`) of its top main-stem run, when
    that is a cut of `splits` (the Vedder begins at Vedder Crossing Bridge); None where it begins at
    no cut (answers 2.8: then the place is said as "where it becomes the <name>")."""
    w = doc["waters"].get(y) or {}
    runs = [r for p in w.get("parts") or [] for r in p.get("runs") or []
            if not r.get("branch") and r.get("km_from") is not None]
    if not runs:
        return None
    top = max(runs, key=lambda r: r["km_from"]).get("from")
    return top if top in (doc.get("splits") or {}) else None


#: (T697) the end tokens of a lake that is not a named water (`bundle.spans`: the suffix dropped)
UNNAMED_LAKE = ("lake_inlet", "lake_outlet")


def _crossing_words(doc: dict, base: str, side: str) -> str:
    """A crossing said at its place: its own words — but an unnamed lake, either edge, is "unnamed lake" (owner
    2026-11-12: "unnamed lakes should be unnamed lake near x")."""
    return "unnamed lake" if base in UNNAMED_LAKE else end_name(doc, base, side)


def end_name(doc: dict, e: Optional[str], side: str = "") -> str:
    """`endName` — a run end in words."""
    if not e:
        return ""
    if NEAR in str(e):
        # T517 (owner 2026-11-09): a crossing two parts of one water would otherwise name alike (a border, an area's
        # boundary, an unnamed lake, a river polygon), said near what stands there (`topology.crossing_near`)
        base, at = str(e).split(NEAR, 1)
        there = end_name(doc, at, side)
        return f"near {there}" if base == "polygon" else f"{_crossing_words(doc, base, side)} near {there}"
    if PLACE in str(e):
        # …or, with nothing named within reach, at its km from the mouth
        base, km = str(e).rsplit(PLACE, 1)
        return f"km {km}" if base == "polygon" else f"{_crossing_words(doc, base, side)} (km {km})"
    if str(e).startswith("lake:") and str(e).count(":") == 2:
        # (T520 derived) where the main line enters / leaves a lake: `lake:<wbk>:inlet|outlet`
        _, wbk, which = str(e).split(":")
        nm = (doc["waters"].get(f"wbk:{wbk}") or {}).get("name")
        if not nm:
            raise AnswersError(f"run end {e!r}: lake wbk:{wbk} has no name in the export")
        return f"the {nm} {which}"
    if str(e).startswith("trib:"):
        return end_name(doc, "confluence:" + str(e).split(":", 1)[1], side)       # (T520 derived) a confluence
    if str(e).startswith(("region:", "mu:")) and "|" in str(e):
        # (T520 derived) a region / management-unit boundary crossing, the downstream one first
        head, pair = str(e).split(":", 1)
        a, b = pair.split("|")
        return f"the Region {a} / Region {b} boundary" if head == "region" else f"the MU {a} / MU {b} boundary"
    if str(e).startswith("water:"):
        nm = (doc["waters"].get(str(e).split(":", 1)[1]) or {}).get("name")
        if not nm:
            raise AnswersError(f"run end {e!r}: that water has no name in the export")
        return nm                                       # another named water a part lies near (`topology.part_near`)
    if str(e).startswith("place:"):
        return str(e).split(":", 1)[1]                  # a B.C. town (`topology.crossing_near`)
    if e == "polygon":
        return "a river polygon"                        # never a part's end on its own: always said near or at km
    if e == "mouth":
        return "the mouth"
    if str(e).startswith("continues:"):
        # RULINGS P10: not a mouth — the water goes on under another name; the
        # place is where that water begins (Chilliwack: "Vedder Crossing Bridge")
        y = e.split(":", 1)[1]
        b = begins_at(doc, y)
        nm = (doc["waters"].get(y) or {}).get("name")
        if not nm:
            raise AnswersError(f"run end {e!r}: the water it goes on as has no name in the export")
        return end_name(doc, b, side) if b else f"where it becomes the {nm}"
    if e == "source":
        return "the top of the river" if side == "from" else "the source"
    if e == "bc_border":
        return "the B.C. border"
    if str(e).startswith("km:"):
        # T517: a place no named feature stands near, said by its km from the mouth (`_named_token`)
        return f"km {e.split(':', 1)[1]}"
    if str(e).startswith("near:"):
        # T517: a gauge cut said by the named feature nearest it on the ground (`_named_token`)
        return "near " + end_name(doc, e.split(":", 1)[1], side)
    splits = doc.get("splits") or {}
    sp = splits.get(e)
    if sp:
        dup = bool(sp.get("water_id")) and sp.get("km") is not None and any(
            o is not sp and k != sp.get("same_place_as") and o.get("same_place_as") != e
            and o.get("water_id") == sp.get("water_id")
            and str(o.get("name")).lower() == str(sp.get("name")).lower()
            for k, o in splits.items())
        nm0 = re.sub(r"\bd/s\b", "downstream", re.sub(r"\bu/s\b", "upstream", sp["name"]))
        the = "" if re.match(r"^(area|region_line):", e) or _THE_LESS.search(nm0) else "the "
        km = f" (km {_js_round(sp['km'])})" if dup else ""
        # T207: a cut placed approximately says so wherever it is named ("…, approximate location (low
        # confidence)") — `confidence_words`, the one sentence
        cw = confidence_words(sp.get("confidence"))
        return the + re.sub(r"^the ", "", nm0, flags=re.I) + km + (f", {cw}" if cw else "")
    head, _, ident = e.partition(":")
    nm = (doc["waters"].get(ident) or {}).get("name") if ident else ""
    if ident and not nm and head in ("confluence", "lake_inlet", "lake_outlet"):
        raise AnswersError(f"run end {e!r}: water {ident} has no name in the export")
    if head == "confluence":
        return f"the {nm} confluence" if nm else "a tributary confluence"
    if head == "lake_inlet":
        return nm if nm else "a lake"
    if head == "lake_outlet":
        return f"the {nm} outlet" if nm else "a lake outlet"
    if head == "area":
        return ident
    if head == "region_line":
        return f"the Region {ident} line"
    raise AnswersError(f"run end {e!r}: a token no rule names (T517: no fallback words)")


def _run_txt(doc: dict, r: dict) -> str:
    if r.get("lakes"):
        n = r["lakes"]
        return _run_txt(doc, {**r, "lakes": 0}) + f" (through {n} lake{'s' if n > 1 else ''})"
    if _js(r.get("polygon")):
        return "" if r["polygon"] == "whole" else str(r["polygon"])
    if r.get("from") and r.get("from") == r.get("to") and str(r["from"]).startswith("area:"):
        return "Within " + re.sub(r" boundary$", "", end_name(doc, r["from"]))
    a, b = end_name(doc, r.get("from"), "from"), end_name(doc, r.get("to"), "to")
    t = f"From {b} up to {a}"
    return f"Side channel ({t[:1].lower() + t[1:]})" if r.get("branch") else t


def _joining(side: List[dict]) -> str:
    """Where side channels join their river, from their runs' `km_from` (the main stem's measure
    from the mouth): "joining 4.1 km from the mouth", "joining 1.1-5.5 km from the mouth", or ""
    when no run carries a measure."""
    kms = sorted(round(float(r["km_from"]), 1) for r in side if r.get("km_from") is not None)
    if not kms:
        return ""
    lo, hi = kms[0], kms[-1]
    return (f"joining {lo:g} km from the mouth" if lo == hi
            else f"joining {lo:g}–{hi:g} km from the mouth")


def water_ends_km(w: dict) -> Tuple[Optional[float], Optional[float]]:
    """(T697) The km of a water's own lower and upper end: the lowest `km_to` and the highest `km_from` of the main-stem
    runs of ALL its parts (outside B.C. too) — where its stem begins and ends, cut or not. (None, None): no measured
    main-stem run."""
    main = [r for p in w.get("parts") or [] for r in p.get("runs") or []
            if not r.get("branch") and not _js(r.get("polygon"))]
    los = [float(r["km_to"]) for r in main if r.get("km_to") is not None]
    his = [float(r["km_from"]) for r in main if r.get("km_from") is not None]
    return (min(los) if los else None, max(his) if his else None)


def own_end(tok: Optional[str], km: Optional[float], w_km: Tuple[Optional[float], Optional[float]]) -> Optional[str]:
    """(T697, RULINGS P55) An UNNAMED LAKE at the water's OWN top or bottom is no cut: the water's own end, "source" /
    "mouth", as every water's is. THE ONE PLACE this is decided: the part's name (`builders._names`) and its header
    (`runs_label`) both read it (T711: the header said "up to a lake outlet" under a name reading "→ Source")."""
    lo_w, hi_w = w_km
    if km is None:
        return tok
    if tok == "lake_outlet" and hi_w is not None and abs(km - hi_w) <= 0.05:
        return "source"
    if tok == "lake_inlet" and lo_w is not None and abs(km - lo_w) <= 0.05:
        return "mouth"
    return tok


def runs_label(doc: dict, part: dict, w_km: Tuple[Optional[float], Optional[float]] = (None, None)) -> str:
    """`runsLabel` — where a part runs, said from the mouth upward (consumer 3.1 step 2). `w_km` is the water's own
    ends (`water_ends_km`): an unnamed lake there is said as the water's end (`own_end`), as the part's name says it."""
    rs = [r for r in part.get("runs") or []
          if not _js(r.get("polygon")) or r["polygon"] != "whole"]
    rs = [r if r.get("branch") else {**r, "from": own_end(r.get("from"), r.get("km_from"), w_km),
                                     "to": own_end(r.get("to"), r.get("km_to"), w_km)} for r in rs]
    if not rs:
        return ""
    main0 = sorted((r for r in rs if not r.get("branch")),
                   key=lambda r: -(r.get("km_from") if r.get("km_from") is not None else 0))
    side = [r for r in rs if r.get("branch")]
    main: List[dict] = []
    for r in main0:
        last = main[-1] if main else None
        if last and str(last.get("to") or "").startswith("lake_inlet") and \
                str(r.get("from") or "").startswith("lake_outlet"):
            last["to"], last["km_to"] = r.get("to"), r.get("km_to")
            last["lakes"] = (last.get("lakes") or 0) + 1
        else:
            main.append(dict(r))
    bits = [b for b in (_run_txt(doc, r) for r in reversed(main)) if b]

    def low(s):
        return s[:1].lower() + s[1:]
    if not bits:
        t = ""
    elif len(bits) == 1:
        t = bits[0]
    elif len(bits) == 2:
        t = f"Two stretches: {low(bits[0])}, and {low(bits[1])}"
    else:
        t = f"{len(bits)} stretches, including {low(bits[0])}"
    if not t and side:
        # A PART OF SIDE CHANNELS ONLY says where they join the river (review C2, answers 2.1):
        # each branch run's `km_from` is the main-stem measure it leaves at, from the mouth.
        # Where no branch carries one, the old words stand.
        where = _joining(side)
        if len(side) == 1:
            t = f"Side channel, {where}" if where else _run_txt(doc, side[0])
        else:
            t = f"{len(side)} side channels" + (f", {where}" if where else "")
    # RULINGS P39: a part WITH a main run never mentions its side channels —
    # they are simply part of it ("… CNR Mile 66 Trestle and 1 side channel" confused the owner)
    return t


#: answers 2.7 (A4): a short part label's end names lose the sign-post words the full label keeps.
#: RULINGS P40: no label cuts a name mid-way ("Sikanni Chief River…" was scary) —
#: an end is said whole however long, and a part with no run shows its full label.
_SIGN_WORDS = re.compile(r"^(?:the )?(?:fishing )?(?:boundary )?signs? (?:at |near )?", re.I)


#: T517: a regulation's cut named after the gauge beside it ("above Babine River (WSC gauge 08EB005)"): the gauge
#: reference is not the place's words (it stays an alternate, `end_variants`)
_GAUGE_REF = re.compile(r"\s*\((?:WSC |ECCC )?(?:hydrometric )?(?:gauge|station) \d{2}[A-Z]{2}\d{3}\)", re.I)


def _end_short(doc: dict, e: Optional[str], side: str) -> str:
    """A run end in a few words: "Mouth", "Source", "B.C. border", "Vedder Crossing Bridge"."""
    if e in ("mouth", "source"):
        return cap(e)
    if str(e).startswith("continues:") and begins_at(doc, e.split(":", 1)[1]) is None:
        nm = (doc["waters"].get(e.split(":", 1)[1]) or {}).get("name") or ""
        return cap(f"{nm} begins")
    t = end_name(doc, e, side)
    t = re.sub(r"^the ", "", t)
    t = _SIGN_WORDS.sub("", t) or t
    t = re.sub(r" of Canada\b", "", t) or t
    t = _GAUGE_REF.sub("", t) or t
    return t if t.startswith("km ") else cap(t)       # a unit stays lower case ("km 121.9")


#: T517: a feature this close (metres, on the ground) to a gauge cut stands AT it ("Tanswanket Creek confluence");
#: farther, the end is said as near it ("near the Tanswanket Creek confluence")
SAME_PLACE_M = 200


def _named_token(doc: dict, e: Optional[str]) -> Optional[str]:
    """T517: the token a run end is SAID by. A gauge cut (`kind` gauge) is never the name of a place a regulation
    ends at (its code and station name stay an alternate: `end_variants`, the mark's `alt`): the regulation's own split
    at the same place (`same_place_as`), else the NEAREST NAMED FEATURE on the ground (`near`, the bundle's
    `topology.gauge_near`: a tributary's confluence or a named cut of the water, within `topology.NEAR_M`), said as
    near it beyond `SAME_PLACE_M`; else BY ITS KM from the mouth ("km 121.9": the place has no name — a deliberate
    rule, pinned in test_part_geo). A gauge cut at several places with neither stops the build."""
    splits = doc.get("splits") or {}
    sp = splits.get(e or "")
    if not sp or sp.get("kind") != "gauge":
        return e
    if sp.get("same_place_as") and sp["same_place_as"] in splits:
        return sp["same_place_as"]
    if sp.get("near"):
        return sp["near"] if sp["near_m"] <= SAME_PLACE_M else "near:" + sp["near"]
    if sp.get("km") is None:
        raise AnswersError(f"gauge cut {e!r}: no regulation split, no named feature near it and no one km to say it by")
    return f"km:{round(float(sp['km']), 1):g}"


def end_variants(doc: dict, e: Optional[str], side: str) -> List[str]:
    """T517 (owner 2026-11-08: a boundary that is both a gauge and a regulation's cut "can carry several name variations,
    and any can be used"): EVERY name of a run end, the one said first (`end_words`: the regulation's own), then the
    others it is also known by — the gauge's own name, a same-place cut's, an official spelling — none discarded."""
    first = end_words(doc, e, side)
    splits = doc.get("splits") or {}
    out = [first]
    for t in (e, (splits.get(e or "") or {}).get("same_place_as")):
        sp = splits.get(t or "")
        if not sp:
            continue
        for n in (_end_short(doc, t, side), cap(re.sub(r"\bd/s\b", "downstream", sp["name"])) if _GAUGE_REF.search(
                sp.get("name") or "") else None, sp.get("official_name")):
            if n and n not in out:
                out.append(n)
    return out


#: T517: the separator of an end token said at its place (`at_place`): "<token>@km<km>"
PLACE = "@km"
#: T517: the separator of an end token said near what stands there: "<token>@near:<token>"
NEAR = "@near:"


def at_place(token: str, km: Optional[float]) -> str:
    """T517 (owner 2026-11-09) an end token said at its PLACE — its km from the mouth, one decimal. A part end with no
    km cannot be told from another one: the build stops."""
    if km is None:
        raise AnswersError(f"part end {token!r}: no km to tell it from another end of the same words")
    return f"{token}{PLACE}{round(float(km), 1):g}"


def end_table(doc: dict, extra: Sequence[str] = ()) -> Dict[str, List[str]]:
    """T517 THE ONE NAME SOURCE for a place a run can end at: {token: [words, other names…]} for EVERY split of the
    export (cut, area, region line, lake edge) and every end token a water's runs carry (confluence, lake inlet/outlet,
    mouth, source, a water that goes on as another) — what the part names, the marks and in-season "Where" say. The
    words are the `from` side's (`end_variants`); a token whose `to` side reads otherwise ("the source") carries it
    as `"to:" + words` last."""
    # every cut but a gauge (a gauge is a place's name only where a run ends at it: below), every run end token
    toks = {k for k, v in (doc.get("splits") or {}).items() if v.get("kind") != "gauge"}
    for w in (doc.get("waters") or {}).values():
        for p in w.get("parts") or []:
            for r in p.get("runs") or []:
                toks.update(t for t in (r.get("from"), r.get("to")) if t)
    toks.update(extra)                          # the part ends said at their place (`at_place`)
    out: Dict[str, List[str]] = {}
    for t in sorted(toks):
        v = end_variants(doc, t, "from")
        to = end_words(doc, t, "to")
        out[t] = v + ([f"to:{to}"] if to != v[0] else [])
    return out


def end_words(doc: dict, e: Optional[str], side: str) -> str:
    """T517: ONE end of a run, in the few words every name and mark uses (`_end_short`, the gauge rule above). Never
    empty: a token nothing can name stops the build."""
    t = _end_short(doc, _named_token(doc, e), side)
    if not t:
        raise AnswersError(f"run end {e!r} ({side}) has no words")
    return t


def run_ends(doc: dict, part: dict) -> Optional[tuple]:
    """T517 THE ONE NAME OF A PART: (lower end, upper end) in words, mouth first — its lowest main-stem run's
    downstream end and its highest run's upstream end. None: no main-stem run (a lake, side channels only) — the app says
    the water's name."""
    rs = [r for r in part.get("runs") or [] if not _js(r.get("polygon")) or r["polygon"] != "whole"]
    main = [r for r in rs if not r.get("branch") and not _js(r.get("polygon"))]
    if not main:
        return None
    km = [(r.get("km_from") if r.get("km_from") is not None else 0) for r in main]
    lo, hi = main[km.index(min(km))], main[km.index(max(km))]
    return end_words(doc, lo.get("to"), "to"), end_words(doc, hi.get("from"), "from")


def end_tokens(doc: dict, part: dict) -> Optional[tuple]:
    """T517 (owner 2026-11-09) A PART SHIPS ITS TWO END TOKENS, never a composed name: (lower end, upper end, the
    lower end's km, the upper end's km), mouth first — its lowest main-stem run's downstream end and its highest
    run's upstream end. The words come from the release's `display.ends` (`end_table`), composed by one app function.
    None: no main-stem run (a lake, side channels only, a polygon) — the part keeps its `short`."""
    rs = [r for r in part.get("runs") or [] if not _js(r.get("polygon")) or r["polygon"] != "whole"]
    main = [r for r in rs if not r.get("branch") and not _js(r.get("polygon"))]
    if not main:
        return None
    if any(r.get("km_from") is None for r in main):
        raise AnswersError(f"part runs {[(r.get('from'), r.get('to')) for r in main]}: a main-stem run with no km")
    lo = min(main, key=lambda r: r["km_from"])
    hi = max(main, key=lambda r: r["km_from"])
    if not lo.get("to") or not hi.get("from"):
        raise AnswersError(f"part runs {[(r.get('from'), r.get('to')) for r in main]}: a main-stem end with no token")
    return lo["to"], hi["from"], lo.get("km_to"), hi["km_from"]


def branch_end_tokens(part: dict) -> Optional[tuple]:
    """T517 (owner 2026-11-09) the two end tokens of a part with NO main stem (side channels only), read off its
    side-channel runs the way `end_tokens` reads a main stem — only asked where one water has several such parts (a
    single one is said by its water's name). None: no run with ends (a lake's polygon)."""
    side = [r for r in part.get("runs") or [] if r.get("branch") and r.get("km_from") is not None
            and r.get("from") and r.get("to")]
    if not side:
        return None
    lo = min(side, key=lambda r: r["km_from"])
    hi = max(side, key=lambda r: r["km_from"])
    return lo["to"], hi["from"], lo.get("km_to"), hi["km_from"]


def end_name_of(doc: dict, lo: str, hi: str) -> str:
    """The words two end tokens stand for, as the app composes them from `display.ends` (`spanName`): "<lower> →
    <upper>" — every part, an area's or a border's included (owner 2026-11-09: no "Within")."""
    return f"{end_words(doc, lo, 'to')} → {end_words(doc, hi, 'from')}"





def part_km(part: dict) -> Optional[float]:
    """`partKm` — the picker's mouth-upward sort key: the highest `km_from` of the main-stem runs,
    else of the side channels (they sort where they join), else last."""
    def km(rs):
        return [r["km_from"] for r in rs if r.get("km_from") is not None]
    m = km([r for r in part.get("runs") or [] if not r.get("branch")])
    b = km([r for r in part.get("runs") or [] if r.get("branch")])
    return max(m) if m else max(b) if b else None          # None: no run carries a measure


def part_labels(W: Water) -> List[str]:
    """`partLabels` — each part's picker name, in the page's part order (consumer 3.1)."""
    rls = [runs_label(W.doc, p, water_ends_km(W.w)) for p in W.parts]
    out: List[str] = []
    for i in range(len(W.parts)):
        pl, lab, rl = part_place(W, i), part_label(W, i), rls[i]
        if rl:
            same = any(j != i and rls[j] == rl for j in range(len(W.parts)))
            hint = ""
            if same and not re.match(r"^(The rest of the water|Whole water)", lab):
                h = re.sub(r"^[^:·]{3,90}: ", "", re.sub(r"^Stretch with: ", "", lab))
                hint = h.split(" · also ")[0]                 # whole (RULINGS P40)
            out.append((pl + " · " if pl else "") + rl + (f" — {hint}" if hint else ""))
        elif not pl:
            out.append(lab)
        elif re.match(r"^(Stretch with: |The rest of the water)", lab):
            rest = re.sub(r"^The rest of the water", "the rest",
                          re.sub(r"^Stretch with: ", "", lab))
            out.append(f"{pl}: {rest}")
        else:
            out.append(f"{pl} · {lab}")

    def core(s):
        return re.sub(r" \(\+\d+ more\)$", "", s)
    for i in range(len(out)):
        j = next((k for k in range(i) if core(out[k]) == core(out[i])), None)
        if j is None:
            continue
        mine, theirs = W.rules[i], W.rules[j]
        diff = [k for k in dict.fromkeys(mine) if k not in set(theirs)]
        less = [k for k in dict.fromkeys(theirs) if k not in set(mine)]

        def say(k):
            return clean((W.r(k).get("parts") or {}).get("what") or W.r(k).get("label")).lower()
        if diff:
            tail = f" · plus {cut_words(say(diff[0]), 48)}"
        elif less:
            tail = f" · without {cut_words(say(less[0]), 48)}"
        else:
            tail = " · (same rules, a separate stretch)"
        out[i] = core(out[i]) + tail
    return out


def picker(W: Water, labels: Sequence[str], closed: Sequence[bool]) -> dict:
    """The picker (consumer 3.2): parts closed to every game fish all year are one choice when the
    water has two or more parts; choices sorted open before closed, then mouth upward (`part_km`);
    a heading per entry place when choices come from two or more. `labels` and `closed` are in the
    page's part order; the choices name the EXPORT's part indexes."""
    n = len(W.parts)
    shut = [i for i in range(n) if n > 1 and closed[i]]
    groups = [{"idx": [i], "closed": False} for i in range(n) if i not in shut]
    if shut:
        groups.append({"idx": shut, "closed": True})
    groups.sort(key=lambda g: g["idx"][0])
    # river order is `topology.order_parts` (one implementation): by km, a side channel with no km beside the
    # main-stem part it runs beside (T710), the other parts with no measure last
    from pipeline.deliver.bundle.topology import order_parts
    rank = {p["v"]: r for r, p in enumerate(order_parts(
        [{"v": i, "km": part_km(p), "runs": p.get("runs") or [], "touches": p.get("touches") or []}
         for i, p in enumerate(W.parts)]))}
    groups.sort(key=lambda g: (g["closed"], rank[g["idx"][0]]))

    def label(g):
        if g["closed"]:
            return (f"Closed all year · {len(g['idx'])} stretches" if len(g["idx"]) > 1
                    else f"Closed all year · {labels[g['idx'][0]]}")
        return labels[g["idx"][0]]
    out, heads = [], []
    for g in groups:
        pl = "" if g["closed"] else part_place(W, g["idx"][0])
        if pl not in heads:
            heads.append(pl)
        text = cap(re.sub(r"^( · |: )", "", label(g)[len(pl):])) if pl else label(g)
        out.append({"parts": [W.export_ix[i] for i in g["idx"]], "closed": g["closed"],
                    "sections": sum(W.parts[i]["sections"] for i in g["idx"]),
                    "heading": pl, "text": text})
    return {"choices": out, "headed": len(heads) >= 2}


def steelhead_line(presence: Optional[str], has_row: bool) -> Optional[str]:
    """Consumer 5.6's presence line, as a code: `possible` with a steelhead row ("Steelhead rules
    apply here; steelhead may not be present in this water"), `known` with one ("known to be in
    this water"), `known` without one ("recorded here, but no steelhead rule applies: treat any
    rainbow, however big, as a rainbow trout"); None otherwise. `has_row` already folds in
    `steelhead_rules: false` (ST is never asked there)."""
    if not presence:
        return None
    if presence == "possible":
        return "possible_with_rules" if has_row else None
    return "known_with_rules" if has_row else "known_no_rules"
