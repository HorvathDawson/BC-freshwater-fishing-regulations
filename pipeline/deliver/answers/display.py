"""DERIVED DISPLAY FACTS — what the page re-derives today from fields, shipped instead.

PER RULE (consumer Stage 2.3, 2.4, 6.3), from the rule's own fields:

    kind     the page's 15-step kind test (`kindOf`), in its order and its vocabulary: gear,
             conduct, vessel, anglerclosure, duty, exempt, standing, while, possession, annual,
             sizecap, subcap, gate, pool, size. `closure` marks a gate that is a closure ("No
             fishing for …") rather than a release ("Release every …") — asked of
             `rules.closure_grade`, the one closure predicate (AGENTS 56).
    bands    `lengths` expanded into contiguous bands from 0 cm up, first range wins:
             [[from_cm, to_cm | null (no top), take | null]] (`bands`).
    plain    the rule as one plain sentence (`sayRule`): "In streams, keep up to 2 trout and char a
             day, all kinds together, hatchery only." — generated ONCE here, so which fish ("trout"
             is trout and char unless char are excluded, AGENTS 46), which origin, which size and
             which dates are said by code under test. null where the page writes no sentence (it
             then falls back to the rule's `label`).

PER PART OF A WATER (Stage 3.1, 3.2, 5.6), from the export's decoded model — its waters, parts,
runs, splits and entries are the bundle's, composed by the export (`export_codec.expand` is the
reference decoder; the pair's `about.bundle` digests must equal the bundle's):

    runs     where it runs ("From the mouth up to the Slesse Creek confluence")      `runsLabel`
    place    the entry it comes from ("Upstream of Kamloops Lake")                    `partPlace`
    hint     what sets it apart ("No fishing · also …")                               `partLabel`
    label    the picker's name for it, tie-broken                                     `partLabels`
    km       its sort key (highest km of its main-stem runs; side channels where they join)
    closed_all_year   closed to every game fish on all 366 days — the status index's own answer
             (`status_index.set_profile`), not the page's `isBroad` re-reading
    group / picker    the closed-all-year merge, the mouth-upward order and the entry headings
    steelhead_line    possible_with_rules | known_with_rules | known_no_rules | null   (5.6)

Wording follows the page (v35) so a 1-to-1 check can compare strings; the page keeps styling.
"""
from __future__ import annotations

import math
import re
from typing import Dict, List, Optional, Sequence, Tuple

from pipeline.deliver.answers.common import (cap, clean, join, lc, range_txt, sp_name,
                                             when_dates)
from pipeline.deliver.bundle.rules import closure_grade

INF = float("inf")


def _js(v) -> bool:
    """JavaScript truthiness: an empty list or object is TRUE, 0 / "" / None are false. The page's
    tests read fields this way (`if (f.while)` holds for `while: []`)."""
    if v is None or v is False:
        return False
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return v != ""
    return True


def _num(v):
    """A number as JavaScript prints it: 50.0 -> 50."""
    if isinstance(v, float) and v.is_integer():
        return int(v)
    return v


def _is(v, n) -> bool:
    """`v === n` for a number n (never true of a bool)."""
    return v is not None and not isinstance(v, bool) and isinstance(v, (int, float)) and v == n


def _gt0(v) -> bool:
    return v is not None and not isinstance(v, bool) and isinstance(v, (int, float)) and v > 0


def _no_target(x: dict) -> bool:
    """The page's `may_target === false` (a bundle column reads 0, the export's JSON false)."""
    v = x.get("may_target")
    return v is not None and not v


# --------------------------------------------------------------------------------------------
# Per rule
# --------------------------------------------------------------------------------------------

def kind_of(x: dict) -> str:
    """The page's `kindOf`, test by test in its order (consumer 2.3)."""
    fam, t = x.get("family"), x.get("type")
    if fam == "gear_and_method":
        return "gear"
    if fam == "conduct":
        return "conduct"
    if fam == "vessel":
        return "vessel"
    if t == "angler_closure":
        return "anglerclosure"
    if t == "stop_fishing_after_quota":
        return "duty"
    if x.get("dimension") == "lift" or (_js(x.get("exempts")) and x.get("take") is None
                                        and not _js(x.get("unlimited"))
                                        and not _js(x.get("lengths"))):
        return "exempt"
    if _js(x.get("standing")):
        return "standing"
    if _js(x.get("while")):
        return "while"
    if _js(x.get("per_daily")):
        return "possession"
    if _js(x.get("period")) and x.get("period") != "daily":
        return "annual" if _gt0(x.get("take")) else "duty"
    n = len(x.get("lengths") or [])
    if _js(x.get("within")):
        return "sizecap" if n else "subcap"
    if _is(x.get("take"), 0) and not n:
        return "gate"
    if _gt0(x.get("take")) or _js(x.get("unlimited")):
        return "pool"
    if n:
        return "size"
    return "duty"


def is_closure_gate(x: dict, kind: Optional[str] = None) -> bool:
    """A gate that closes ("No fishing for …"), not one that releases."""
    return (kind or kind_of(x)) == "gate" and closure_grade(x) is not None


def bands(x: dict) -> List[list]:
    """The page's `bands` (consumer 2.4): cut points at every min/max plus 0; each band's midpoint
    tested against the ranges in order, the first that holds gives the take (its own, else the
    rule's); a band no range covers takes 0 on a daily pool, nothing on a cap, clause or yearly
    limit; neighbours with the same take merge. [[from_cm, to_cm or None, take or None]]."""
    L = x.get("lengths") or []
    if not L:
        return []
    pts = {0}
    for r in L:
        if r.get("min_cm") is not None:
            pts.add(r["min_cm"])
        if r.get("max_cm") is not None:
            pts.add(r["max_cm"])
    P = sorted(pts) + [INF]
    if _js(x.get("within")) or (_js(x.get("period")) and x.get("period") != "daily"):
        uncovered = None
    else:
        uncovered = 0 if (_gt0(x.get("take")) or _js(x.get("unlimited"))) else None
    segs: List[list] = []
    for a, b in zip(P, P[1:]):
        m = a + 1 if b == INF else (a + b) / 2
        hit = next((r for r in L if (r.get("min_cm") is None or m >= r["min_cm"])
                    and (r.get("max_cm") is None or m <= r["max_cm"])), None)
        if hit is not None:
            take = hit.get("take") if hit.get("take") is not None else x.get("take")
        else:
            take = uncovered
        if segs and segs[-1][2] == take:
            segs[-1][1] = b
        else:
            segs.append([a, b, take])
    return [[_num(a), None if b == INF else _num(b), _num(t)] for a, b, t in segs]


def written_name(x: dict, conj: str = " and ") -> str:
    """The rule's own fish in words (`writtenName`): "trout" for trout/char minus char (p.80:
    trout includes char unless char are excluded), else each code's name, else "fish"."""
    w = list(x.get("species") or [])
    if w == ["TROUT_CHAR"] and "CHAR" in (x.get("species_except") or []):
        return "trout"
    return join([lc(sp_name(c)) for c in w], conj) if w else "fish"


_MANY = re.compile(r"^(trout|char|trout and char|game fish|fish|bass|whitefish|salmon)\b")


def plain(x: dict, names: Optional[str] = None) -> Optional[str]:
    """The rule as a plain sentence (`sayRule`, consumer 6.3); None where the page writes none.

    `names` replaces the rule's fish word when a role covers only some of a row's fish (the page
    swaps them in from `species`); the shipped sentence is the rule's own (names=None)."""
    from pipeline.regs.parsing.catalogue import SPECIES_GROUPS
    t = x.get("type")
    if t == "stop_fishing_after_quota":
        o = f"{x['origin']} " if _js(x.get("origin")) else ""
        return (f"Once you’ve kept your daily limit of {o}{written_name(x, ' and ')}, stop "
                f"fishing this water for the rest of the day.")
    if t != "retention_limit":
        return None
    if _js(x.get("extent_text")) or _js(x.get("side")) or _js(x.get("standing")):
        return None

    def nm(conj: str) -> str:
        s = re.sub(r"^all ", "", written_name(x, conj))
        s = re.sub(r"^Dolly Varden/bull trout$", "Dolly Varden or bull trout", s)
        s = s.replace("Dolly Varden/bull trout", "Dolly Varden, bull trout")
        if conj == " or ":
            s = re.sub(r"^trout and char", "trout or char", s)
        return s

    species = list(x.get("species") or [])
    sp = names or nm(" and ")
    ex = [s for s in (x.get("species_except") or []) if not (s == "CHAR" and sp == "trout")]
    ex_txt = f" (not {join([lc(sp_name(s)) for s in ex], ' or ')})" if ex and not names else ""
    many = (len(species) > 1 or (bool(_MANY.match(sp)) and sp != "trout")
            or any(s in SPECIES_GROUPS for s in species))
    water = x.get("water")
    where = ("In streams" if water == "stream" else "In lakes" if water == "lake" else "") or \
        ("In tributaries" if _js(x.get("tributaries_only")) else "")
    dates = ", ".join(range_txt(a, b) for a, b in when_dates(x.get("when")))
    org = f"{x['origin']} " if _js(x.get("origin")) else ""
    ln = x.get("lengths") or []
    mn = next((r for r in ln if r.get("min_cm") is not None and r.get("take") is None), None)
    floor = next((r for r in ln if r.get("max_cm") is not None and _is(r.get("take"), 0)), None)
    ceil = next((r for r in ln if r.get("min_cm") is not None and _is(r.get("take"), 0)), None)
    take = x.get("take")

    def wrap(s: str) -> str:
        y = f"{where}, {s[:1].lower() + s[1:]}" if where else s
        if dates:
            y += f", {dates}"
        return y + "."
    if _js(x.get("while")):
        return None
    if _no_target(x):
        return wrap("No fishing" if re.search(r"game fish|^fish", sp) else f"No fishing for {sp}")
    if _js(x.get("per_daily")):
        return wrap(f"Don’t carry more than {_num(x['per_daily'])} days’ worth of {sp}")
    over = f" over {_num(mn['min_cm'])} cm" if mn else ""
    if _js(x.get("record_retention")):
        return wrap(f"Record each {org}{sp}{over} on your licence right away")
    if _js(x.get("unlimited")):
        return wrap(f"No daily limit for {sp}")
    if _is(take, 0):
        return wrap(f"Release every {org}{sp}")
    if x.get("period") == "annual":
        return wrap(f"Keep up to {_num(take)} {sp}{over} a licence year"
                    f"{', all kinds together' if len(species) > 1 else ''}")
    if take is not None and mn and not floor:
        who = re.sub(r" and ([^,]+)$", r" or \1", names) if names else nm(" or ")
        return wrap(f"Only {_num(take)} {org}{who} over {_num(mn['min_cm'])} cm a day{ex_txt}")
    sp += ex_txt
    bits = []
    if take is not None:
        fish = nm(" or ") + ex_txt if _is(take, 1) and not names else sp
        bits.append(f"Keep up to {_num(take)} {fish} a day"
                    f"{', all kinds together' if many and _gt0(take) and take > 1 else ''}")
    else:
        longer = f" {_num(floor['max_cm'])} cm or longer" if floor else ""
        bits.append(f"Keep only {sp}{longer}")
    if floor and take is not None:
        bits.append("over 50 cm" if species == ["ST"] and _is(floor["max_cm"], 50)
                    else f"{_num(floor['max_cm'])} cm or longer")
    if ceil:
        bits.append(f"none over {_num(ceil['min_cm'])} cm")
    if _js(x.get("origin")):
        bits.append(f"{x['origin']} only")
    if len(bits) == 1 and take is None and not floor:
        return None
    return wrap(", ".join(bits))


def rule_facts(x: dict) -> dict:
    """{kind, closure?, bands?, plain?} for one rule — the per-rule display facts."""
    k = kind_of(x)
    out: dict = {"kind": k}
    if is_closure_gate(x, k):
        out["closure"] = True
    b = bands(x)
    if b:
        out["bands"] = b
    p = plain(x)
    if p is not None:
        out["plain"] = p
    return out


# --------------------------------------------------------------------------------------------
# Per part of a water — read off the export's decoded model (`export_codec.expand`)
# --------------------------------------------------------------------------------------------

def cut_words(t: str, n: int) -> str:
    """`cutWords`: at most n characters, cut back to a word boundary, with an ellipsis."""
    if len(t) <= n:
        return t
    return re.sub(r"[\s,;:·–-]+\S*$", "", t[:n]) + "…"


def _win_txt(x: dict) -> str:
    w = when_dates(x.get("when"))
    return ", " + ", ".join(range_txt(a, b) for a, b in w) if w else ""


def _js_round(v: float) -> int:
    return int(math.floor(v + 0.5))


class Water:
    """One water of the export, as the page sees it: its parts with a rule set (parts outside
    B.C. carry none and are left out), in the page's order — most sections first, the export's
    order among equals — each part's rules its set's `reach` then `trib` ids, each sorted.

    `doc` is the decoded export (`export_codec.expand`); a rule's facts are read from `fields`,
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


_REGION = re.compile(r"^r(\d+[ab]?)")


def _region(e: str) -> Optional[str]:
    m = _REGION.match(e)
    return m.group(1) if m else None


def part_place(W: Water, i: int) -> str:
    """`partPlace` — the entry a part comes from, when the water's parts come from two or more of
    its own entries (consumer 3.1 step 1)."""
    entries = W.doc["entries"]

    def ents(j):
        out = []
        for k in W.rules[j]:
            x = W.r(k)
            rk = W.rank(k)
            if (4 if rk is None else rk) == 0 and \
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


def end_name(doc: dict, e: Optional[str], side: str = "") -> str:
    """`endName` — a run end in words."""
    if not e:
        return ""
    if e == "mouth":
        return "the mouth"
    if e == "source":
        return "the top of the river" if side == "from" else "the source"
    if e == "bc_border":
        return "the B.C. border"
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
        return the + re.sub(r"^the ", "", nm0, flags=re.I) + km
    head, _, ident = e.partition(":")
    nm = (doc["waters"].get(ident) or {}).get("name") if ident else ""
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
    return e.replace("_", " ")


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


def runs_label(doc: dict, part: dict) -> str:
    """`runsLabel` — where a part runs, said from the mouth upward (consumer 3.1 step 2)."""
    rs = [r for r in part.get("runs") or []
          if not _js(r.get("polygon")) or r["polygon"] != "whole"]
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
        t = _run_txt(doc, side[0]) if len(side) == 1 else f"{len(side)} side channels"
    elif side:
        t += f" and {len(side)} side channel{'s' if len(side) > 1 else ''}"
    return t


def part_km(part: dict) -> float:
    """`partKm` — the picker's mouth-upward sort key: the highest `km_from` of the main-stem runs,
    else of the side channels (they sort where they join), else last."""
    def km(rs):
        return [r["km_from"] for r in rs if r.get("km_from") is not None]
    m = km([r for r in part.get("runs") or [] if not r.get("branch")])
    b = km([r for r in part.get("runs") or [] if r.get("branch")])
    return max(m) if m else max(b) if b else 1e9


def part_labels(W: Water) -> List[str]:
    """`partLabels` — each part's picker name, in the page's part order (consumer 3.1)."""
    rls = [runs_label(W.doc, p) for p in W.parts]
    out: List[str] = []
    for i in range(len(W.parts)):
        pl, lab, rl = part_place(W, i), part_label(W, i), rls[i]
        if rl:
            same = any(j != i and rls[j] == rl for j in range(len(W.parts)))
            hint = ""
            if same and not re.match(r"^(The rest of the water|Whole water)", lab):
                h = re.sub(r"^[^:·]{3,90}: ", "", re.sub(r"^Stretch with: ", "", lab))
                hint = cut_words(h.split(" · also ")[0], 60)
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
    groups.sort(key=lambda g: (g["closed"], part_km(W.parts[g["idx"][0]])))

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


DECISIONS = [
    "D1 Part facts are read off the export's DECODED model (`export_codec.expand`, the reference "
    "decoder): the runs, splits and entries are composed from the bundle by the export "
    "(`part_runs`, `spans.compose_runs`), and recomposing them here would be a second "
    "implementation. The pair's `about.bundle` digests must equal the bundle's, or it is refused.",
    "D2 Part labels are computed in the page's part order (most sections first, the export's "
    "order among equals) because the tie-breaks ('· plus …') depend on it; every part is named "
    "by its EXPORT index in the output.",
    "D3 Closed all year is the status index's answer (`status_index.set_profile` all CLOSED: every "
    "game fish under a speaking full closure every day), not the page's re-reading (`isBroad`: "
    "a closure or a federal release naming ALL_GAME_FISH). One definition of closed (AGENTS 56).",
    "D4 The steelhead line asks whether a steelhead ROW exists as: ST is a fish the part asks about "
    "(5.1: a part with steelhead, a rule naming ST, steelhead rules not false, ST among the "
    "applying retention rules' fish) and, on the day, the reader has a speaking gate or pool for "
    "ST (5.2's winner exists). Shipped as runs over the part's segments.",
    "D5 `plain` is the page's `sayRule` for the rule's own fish (no row context); null where the "
    "page writes no sentence (gear, conduct, vessel, `while`, a place in words, a side, standing) "
    "and falls back to `label`.",
    "D6 Splits for run-end names are the export's whole `splits` table (the page embeds the subset "
    "its 28 waters name); a '(km N)' tie-break can differ only where an unnamed-by-any-run split "
    "on the same water shares a name.",
]

STEELHEAD_LINES = ("possible_with_rules", "known_with_rules", "known_no_rules")


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


# --------------------------------------------------------------------------------------------
# Builders: every rule, every part
# --------------------------------------------------------------------------------------------

def build_rules(B) -> List[dict]:
    """The per-rule facts, in the export's `rules` order (`common.rule_index`)."""
    order = sorted(B.index, key=B.index.__getitem__)
    return [rule_facts(B.rules[k]) for k in order]


def load_export(data_path, guide_path, B) -> dict:
    """The export pair decoded by the reference decoder, refused unless it was read from this
    bundle (`about.bundle` reach digest and section handles)."""
    import json
    from pathlib import Path
    from pipeline.tools import export_codec
    data = json.loads(Path(data_path).read_text(encoding="utf-8"))
    guide = json.loads(Path(guide_path).read_text(encoding="utf-8"))
    doc = export_codec.expand(data, guide)
    about = (doc.get("about") or {}).get("bundle") or {}
    for k in ("reach_digest", "section_handles"):
        if about.get(k) != B.digest.get(k):
            raise SystemExit(f"answers: the export was read from another bundle ({k} "
                             f"{about.get(k)} != {B.digest.get(k)}) — regenerate it")
    return doc


def closed_all_year(B, key) -> bool:
    """Closed to every game fish on all 366 days: the status index's own profile is CLOSED every
    day. Quick refusals first, with the index's predicate: a day on which the bound full closures
    in force do not cover every game fish is not closed."""
    from pipeline.deliver import status_index as SI
    from pipeline.deliver.bundle import read
    every = B.rules
    bound = B.sets.get(key.set_id, [])
    # a STANDING closure ("no fishing within 23 m of a fish ladder") is only ever "shown" by the
    # reader, so it never closes a day; leaving it out of the quick test changes no answer
    shut = [(e, r) for e, r, _ in bound if (e, r) in every and SI.is_full_closure(every[(e, r)])
            and not every[(e, r)].get("standing")]
    if not shut:
        return False
    covers = {k: frozenset(f for f in SI.GAME_FISH if read.speaks_for(every[k], f))
              for k in shut}
    from pipeline.deliver.answers.common import month_day
    for d in range(1, 367):
        held = set()
        for k in shut:
            if read.in_force(every[k].get("when"), month_day(d)) == "yes":
                held |= covers[k]
        if len(held) < len(SI.GAME_FISH):
            return False
    prof = SI.set_profile(bound, key.steelhead_water, B.path, key.steelhead_rules)
    return all(c == SI.CLOSED for c in prof)


_ASKED_KINDS = ("gate", "pool", "subcap", "sizecap", "size", "annual")


def asks_steelhead(B, key, kind: Optional[str], presence: Optional[str],
                   rules_off: bool) -> bool:
    """Consumer 5.1 for ST: the part has steelhead, some rule of the place names ST, the
    steelhead rules are not off, and ST is among the fish of its applying retention rules."""
    from pipeline.deliver.answers.common import expand
    if not presence or rules_off:
        return False
    xs = [B.rules[(e, r)] for e, r, _ in B.sets.get(key.set_id, []) if (e, r) in B.rules]
    if not any("ST" in (x.get("species") or []) for x in xs):
        return False
    for x in xs:
        if kind and x.get("water") and x["water"] != kind:
            continue
        if x.get("type") == "retention_limit" and kind_of(x) in _ASKED_KINDS and \
                "ALL_GAME_FISH" not in (x.get("species") or []) and \
                "ST" in expand(x.get("species")):
            return True
    return False


def steelhead_runs(B, key, presence: Optional[str], asked: bool) -> List[list]:
    """The steelhead line over the key's year, [[start_day, code]]: the row exists on a day when
    ST is asked and the reader has a speaking gate or pool for it (5.2's winner)."""
    from pipeline.deliver.answers.common import month_day, rule_vectors, segments
    from pipeline.deliver.bundle import read
    if not presence:
        return [[1, None]]
    if not asked:
        return [[1, steelhead_line(presence, False)]]
    bound = B.sets.get(key.set_id, [])
    runs, readings = segments(rule_vectors(B.rules[(e, r)] for e, r, _ in bound
                                           if (e, r) in B.rules))
    codes = []
    for d in readings:
        rows = read.effective_rules_bound(bound, key.steelhead_water, month_day(d), "ST",
                                          B.path, steelhead_rules_here=key.steelhead_rules)
        has = any(x["state"] == "speaks" and x.get("type") == "retention_limit"
                  and kind_of(x) in ("gate", "pool") for x in rows)
        codes.append(steelhead_line(presence, has))
    out: List[list] = []
    for d, i in runs:
        if not out or out[-1][1] != codes[i]:
            out.append([d, codes[i]])
    return out


def part_key_c(p: dict) -> str:
    """Agent C's reference part key (`golden.js partKey`), for joining with the page goldens."""
    s = lambda v: "" if v is None else str(v)                     # noqa: E731 (JS join)
    return "|".join([s(p.get("ruleset")), s(p.get("licensing_set")),
                     "sw" if p.get("anadromous_rainbow") else "", p.get("steelhead") or "",
                     "sr0" if p.get("steelhead_rules") is False else "",
                     "+".join(p.get("province_except") or []),
                     ",".join(p["home_region"]) if isinstance(p.get("home_region"), list)
                     else (p.get("home_region") or "")])


class PartKeyError(SystemExit):
    """A part whose sections do not agree on one evaluation key: the part tuple would not decide
    its answers, so the build stops (no fallback)."""


def parts_of_bundle(B) -> Dict[Tuple[str, str], dict]:
    """Every part of every named water, from the bundle's sections: `{(item_id, part key):
    {"rule": RuleKey, "licence": LicenceKey | None, "sections": n}}`. The part key is agent C's
    (`reference/golden.js partKey`): `set|lset|sw|steelhead|sr0|province_except|home_region` —
    `sw` where a rainbow over 50 cm is a steelhead, `sr0` on a KNOWN part no steelhead rule
    applies to (the export's `steelhead_rules: false`), the province-exception kinds joined by
    "+", the straddling sections' home regions joined by ",". Parts with no rule set (outside
    B.C.) are left out. A part whose sections give two rule keys or two licence keys is refused
    (`PartKeyError`)."""
    from collections import defaultdict
    from pipeline.deliver.answers.common import RuleKey, connect
    from pipeline.deliver.answers.licence import section_keys
    db = connect(B.path)
    try:
        rows = db.execute(
            "SELECT i.item_id, s.sid, r.set_id, l.set_id FROM item i JOIN item_section s "
            "ON s.ord = i.ord JOIN section_ruleset r ON r.sid = s.sid "
            "LEFT JOIN section_licensing l ON l.sid = s.sid").fetchall()
        sids = {r[1] for r in rows}
        pe: Dict[int, List[str]] = defaultdict(list)
        for k, sid in db.execute("SELECT area_kind, sid FROM province_except ORDER BY area_kind"):
            pe[sid].append(k)
        sw = {s for (s,) in db.execute("SELECT DISTINCT sid FROM steelhead_water")}
        sr = {s for (s,) in db.execute("SELECT sid FROM section_steelhead_rules")}
        st = {s: {1: "known", 2: "possible"}[c]
              for s, c in db.execute("SELECT sid, code FROM section_steelhead")}
        home = dict(db.execute("SELECT sid, region FROM section_home"))
        lic = section_keys(db, sids)
    finally:
        db.close()
    groups: Dict[tuple, dict] = defaultdict(lambda: {"rule": set(), "licence": set(),
                                                     "sr": set(), "home": set(), "n": 0})
    for item_id, sid, rs, ls in rows:
        t = (item_id, rs, ls, tuple(pe.get(sid, [])), sid in sw, st.get(sid))
        g = groups[t]
        g["rule"].add(RuleKey(rs, sid in sw, sid in sr))
        g["licence"].add(lic.get(sid))
        g["sr"].add(sid in sr)
        if sid in home:
            g["home"].add(home[sid])
        g["n"] += 1
    out: Dict[Tuple[str, str], dict] = {}
    bad = []
    for (item_id, rs, ls, pes, swv, stv), g in sorted(groups.items(), key=lambda kv: str(kv[0])):
        if len(g["rule"]) != 1 or len(g["licence"]) != 1 or len(g["sr"]) != 1:
            bad.append((item_id, rs, ls))
            continue
        apply = next(iter(g["sr"]))
        pk = "|".join([str(rs), "" if ls is None else str(ls), "sw" if swv else "", stv or "",
                       "sr0" if stv == "known" and not apply else "", "+".join(pes),
                       ",".join(sorted(g["home"]))])
        out[(item_id, pk)] = {"rule": next(iter(g["rule"])),
                              "licence": next(iter(g["licence"])), "sections": g["n"]}
    if bad:
        raise PartKeyError(f"answers: {len(bad)} part(s) whose sections give more than one "
                           f"evaluation key (e.g. {bad[:3]})")
    return out


def part_keys(B, doc: dict) -> Dict[tuple, dict]:
    """Every export part with a rule set, mapped to its bundle part: `{(item_id, export part
    index): {"pk", "rule", "licence", "sections"}}`. An export part with no bundle part, or with
    another section count, is refused (`PartKeyError`)."""
    by = parts_of_bundle(B)
    out: Dict[tuple, dict] = {}
    bad = []
    for item_id, w in doc["waters"].items():
        for i, p in enumerate(w["parts"]):
            if p.get("ruleset") is None:
                continue
            pk = part_key_c(p)
            got = by.get((item_id, pk))
            if got is None or got["sections"] != p["sections"]:
                bad.append((item_id, i, pk))
                continue
            out[(item_id, i)] = {"pk": pk, **got}
    if bad:
        raise PartKeyError(f"answers: {len(bad)} export part(s) with no matching bundle part "
                           f"(e.g. {bad[:3]})")
    return out


def paper_licence(B, key, kind: Optional[str]) -> List[str]:
    """The record duties that reach this part (7.7 step 7, "Keep a hatchery steelhead? Carry your
    paper licence"): its set's rules with `record_retention` for this kind of water — the
    `records` of `zp:licence_administration#carry_paper_licence` that bind here."""
    return [f"{e}::{r}" for e, r, _ in B.sets.get(key.set_id, [])
            if (e, r) in B.rules and B.rules[(e, r)].get("record_retention")
            and not (kind and B.rules[(e, r)].get("water")
                     and B.rules[(e, r)]["water"] != kind)]


def produce_rules(B) -> Dict[str, dict]:
    """PURE: `{"entry::rule": {kind, closure?, bands?, plain?}}` for every rule of the bundle."""
    return {f"{e}::{r}": rule_facts(x) for (e, r), x in
            sorted(B.rules.items(), key=lambda kv: f"{kv[0][0]}::{kv[0][1]}")}


def produce_parts(B, doc: dict, log=print) -> Dict[str, dict]:
    """PURE: every named water's part facts, `{item_id: {"parts": {part key: facts},
    "picker": {...}, "unresolved_licensing": [record id]}}` where a part's facts are
    {export, order, label, runs, place, hint, km, closed_all_year, steelhead_line
    {start_day: code}, rule_key, licence_key}. The picker names parts by part key."""
    keys = part_keys(B, doc)
    closed_memo: Dict = {}
    st_memo: Dict = {}
    out: Dict[str, dict] = {}
    unresolved: Dict[str, List[str]] = {}
    for k, x in (doc.get("licensing") or {}).items():
        if x.get("placement") == "unresolved":
            unresolved.setdefault(x["entry_id"], []).append(k)
    for item_id in sorted(doc["waters"]):
        w = doc["waters"][item_id]
        W = Water(item_id, w, doc)
        if not W.parts:
            continue
        labels = part_labels(W)
        closed = []
        parts: Dict[str, dict] = {}
        pks = []
        for i, (ex, p) in enumerate(zip(W.export_ix, W.parts)):
            k = keys[(item_id, ex)]
            rk, lk = k["rule"], k["licence"]
            if rk not in closed_memo:
                closed_memo[rk] = closed_all_year(B, rk)
            closed.append(closed_memo[rk])
            presence = p.get("steelhead")
            off = p.get("steelhead_rules") is False
            sk = (rk, w.get("kind"), presence, off)
            if sk not in st_memo:
                st_memo[sk] = steelhead_runs(
                    B, rk, presence, asks_steelhead(B, rk, w.get("kind"), presence, off))
            pks.append(k["pk"])
            parts[k["pk"]] = {
                "export": ex, "order": i, "label": labels[i], "runs": runs_label(doc, p),
                "place": part_place(W, i), "hint": part_label(W, i), "km": part_km(p),
                "closed_all_year": closed[-1],
                "steelhead_line": {d: c for d, c in st_memo[sk]},
                "paper_licence": paper_licence(B, rk, w.get("kind")),
                "rule_key": rk, "licence_key": lk}
        pick = picker(W, labels, closed)
        for c in pick["choices"]:
            c["parts"] = [pks[W.export_ix.index(ex)] for ex in c["parts"]]
        out[item_id] = {"parts": parts, "picker": pick,
                        "unresolved_licensing": sorted(k for e in w.get("entries") or []
                                                       for k in unresolved.get(e, []))}
    log(f"  parts: {sum(len(v['parts']) for v in out.values())} parts of {len(out)} waters; "
        f"{sum(closed_memo.values())} of {len(closed_memo)} rule keys closed all year")
    return out
