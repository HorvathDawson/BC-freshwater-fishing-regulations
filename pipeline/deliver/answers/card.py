"""THE QUOTA CARD (answers 2.8, ROUND9 A1-A3) — water-first, each fact once.

A group total (`rows.group_of`: the pool several kinds share, its sub-caps, what a water's own rule lifts
out of them) is said as the angler reads a water:

  headline    the number from THIS water with no lift: on a Region 2 stream "2 a day from this stream"
              (the stream share holding every kind), on a lake the total ("4 a day");
  origin      "Keep hatchery fish only — wild trout and char go back." where every kind keeps only one origin
              (ROUND19 6);
  lifts       each water's rule lifting kinds out of the share, said positively with whose rule it is:
              "On this river, hatchery rainbow trout 50 cm or less: up to 4 a day — this river’s rule
              replaces Region 2’s 2-from-streams limit for them." (`by`: the page cites its Book page);
              ONE line per lifting entry, each kind with its number, highest first (ROUND10 W1): "On
              this river, its own rule replaces Region 4’s 2-from-streams limit for rainbow trout (up
              to 5 a day) and Dolly Varden/bull trout (up to 2 a day)." — `each` [{kinds, n, by, fish}]
              per lifting rule, `by`/`n` the first (highest); the headline's `also` lists every lifted
              number above it, highest first;
  total_line  under a share, the region's total: "No more than 4 trout and char a day across Region 2,
              lakes included.";
  buckets     (ROUND12 2: a water's own rule bucket(s) first, then the share, then the rest)
              the kinds grouped under the cap they draw from — "Share the 2 from streams" (steelhead;
              cutthroat, brook and brown; Dolly Varden and lake trout), the lifting rule ("This river’s
              rule": rainbow trout 4), or the total — each kind with its number, origin and size, its
              duties (10 a year, record it, stop after 2) and its SPECIES (one picture each, P5); a size
              cap is a SIZE GROUP of the bucket (ROUND15 1, `sizes`), naming exactly the kinds it can bind
              ("Brown trout, Dolly Varden/bull trout, lake trout or brook trout: only 1 over 50 cm between
              them"; a cap spanning buckets on the first) — never on a kind row; a count spanning buckets
              stays a note; an own-rule bucket says its shared cap (ROUND15 5, `between`);
  replaces    (ROUND13 D) on a water's own-rule bucket, what its lifts replace, generated from them —
              each lifted cap of the region named: "Replaces Region 2’s 2-from-streams limit and 30 cm
              minimum size for hatchery rainbow trout" (ROUND19 6: the fish by name; the page's lift box is gone;
              `lifts` stay for proof);
  not_kept    (ROUND13 S) every fish the card's row holds that nobody may keep here, with whose rule:
              "Steelhead: no fishing for them" (Bella Coola r13), "Dolly Varden/bull trout: release every
              one" — so "not listed" never stands for a closure (the gate proves none may be kept);
  worked      ONE list (ROUND12 4): RULINGS Q23b with the count worked through, for a lift past the share
              ("Keep 4 rainbows here → that’s your trout and char for the day, everywhere."), then the
              kept-elsewhere examples it does not repeat ("Kept some at a lake?", over 50 cm across
              waters) — on a water with no lift those examples alone — each with the basket it starts
              from (`at`, `over_cm`) and the numbers the gate proves (`here`, `other` — another water of
              this kind, `away` — the other kind; `most` exactly or at most);
  states      which cap each card line STATES ({r, line}) — the gate refuses a cap stated twice.

Every word is built here from the rows' own arrangement (decided answers, row items, sub-caps); the card
gate (`card_check.py`) proves every number against baskets kept under the rules and verdicts.
"""
from __future__ import annotations

import math
import re
from typing import Dict, List, Optional, Sequence, Tuple

from pipeline.deliver.answers import common

INF = math.inf
_WATER_NOUNS = {"river": "river", "rivers": "river", "creek": "creek", "creeks": "creek",
                "slough": "slough", "lake": "lake", "lakes": "lake", "reservoir": "reservoir",
                "channel": "channel", "brook": "brook", "pond": "pond"}


def _js(v):
    from pipeline.deliver.answers.rows import _js as js
    return js(v)


def _names(codes: Sequence[str], conj: str = " and ") -> str:
    g = common.group_for(codes)
    if g:
        return common.lc(g)
    return common.join([common.lc(common.sp_name(c)) for c in codes], conj)


def _cm(v, inch: bool = False) -> str:
    """A size: "50 cm", or with `inch` (ROUND19 2) "50 cm (20 in)"."""
    return common.cm_in(v) if inch else f"{_js(v)} cm"


def _size_words(lo: float, hi: float, inch: bool = False) -> str:
    if not lo and hi == INF:
        return "any size"
    if not lo:
        return f"{_cm(hi, inch)} or less"
    if hi == INF:
        return f"{_cm(lo, inch)} or longer"
    return common.range_cm_in(lo, hi) if inch else f"{_js(lo)}–{_js(hi)} cm"


def _lifted_ends(P, model: dict, S: str) -> Tuple[bool, bool]:
    """(a minimum, a maximum) a water's rule LIFTS for this kind here (ROUND12 1): a rule the reader says
    is lifted for a kept origin whose take-0 band runs from 0 (a floor: Region 2's "none under 30 cm",
    which the Vedder's r9 lifts for hatchery rainbows) or up to no end (a ceiling)."""
    from pipeline.deliver.answers.rows import _bands
    lo = hi = False
    for o in _kept_origins(model, S):
        for key, v in ((P.ladder.get(S) or {}).get(o) or {}).items():
            r = P.by_key.get(key)
            if r is None or not v or v[0] != "lifted":
                continue
            for a, b, t in _bands(r):
                if t == 0 and not a and b != INF:
                    lo = True
                if t == 0 and a and b == INF:
                    hi = True
    return lo, hi


def size_segments(P, model: dict, S: str, lo: float, hi: float) -> List[Tuple[float, float]]:
    """The ranges a kind is kept in here (ROUND18 2, the kind's `sizes`): its keep range less every slot its
    answer releases inside it ("60 cm or less, or 90 cm or longer")."""
    from pipeline.deliver.answers.rows import main_res
    R_ = model["R"].get(S) or {}
    m = main_res(R_.get("H"), R_.get("W"))
    slots = sorted((l["a"], l["b"]) for l in (m or {}).get("lines") or []
                   if l["t"] == "rel" and l["a"] and l["b"] != INF and lo < l["a"] and l["b"] < hi)
    segs, at = [], lo
    for a, b in slots:
        if a > at:
            segs.append((at, a))
        at = max(at, b)
    segs.append((at, hi))
    return segs


def _detail_words(P, model: dict, S: str, lo: float, hi: float, inch: bool = False) -> str:
    """A kind row's size (ROUND12 1/3): what is kept AND where a usual limit is lifted — the Vedder's
    hatchery rainbow "any length, 50 cm or less" (p.22 "of any length 50 cm or less": Region 2's 30 cm
    minimum is lifted); a slot as its ranges, "60 cm or less, or 90 cm or longer" (Bennett Lake's lake
    trout, none between 60 and 90 cm); every other range exactly as `_size_words`."""
    segs = size_segments(P, model, S, lo, hi)
    if len(segs) == 1:
        lifted_lo, lifted_hi = _lifted_ends(P, model, S)
        a, b = segs[0]
        if lifted_lo and not a:
            return "any length" if b == INF else f"any length, {_cm(b, inch)} or less"
        if lifted_hi and b == INF and a:
            return f"{_cm(a, inch)} or longer, any length"
    return ", or ".join(_size_words(a, b, inch) for a, b in segs)


def _speaks_for(P, model: dict, r, S: str) -> bool:
    """The reader says `r` speaks for this kind (a kept origin) here."""
    return any((((P.ladder.get(S) or {}).get(o) or {}).get(r.key) or [None])[0] == "speaks"
               for o in _kept_origins(model, S))


def _noun(P, L) -> str:
    """The water's own noun for its rule ("river", "creek", "lake"), else the part's kind."""
    name = (L.x.get("entry_name") or "").strip()
    last = re.sub(r"[^a-z]", "", (name.split() or [""])[-1].lower())
    return _WATER_NOUNS.get(last) or ("lake" if P.kind == "lake" else "stream")


def _whose(P, L) -> Tuple[str, str]:
    """(where, whose rule): ("On this river", "this river’s rule"); a ✱ row the walk carries
    ("On the Duncan River and its tributaries", "the Duncan River’s rule"); an area's or region's by
    its name."""
    from pipeline.deliver.answers.rows import badge_of
    name = re.sub(r"^the ", "", (L.x.get("entry_name") or "").strip(), flags=re.I)
    the = common.the_name(name)            # ROUND15 8: "the Bella Coola River", "Trout Lake"
    if L.via == "trib" and name:
        if L.f.get("tributaries_only"):
            # ROUND15 6 (owner): a rule the row prints for its TRIBUTARIES ONLY is said as that — Ape Creek's
            # char release is "the Bella Coola River’s rule for its tributaries", never the mainstem's rule
            return f"On {the}’s tributaries", f"{the}’s rule for its tributaries"
        return f"On {the} and its tributaries", f"{the}’s rule"
    b = badge_of(L)
    if b == "water":
        n = _noun(P, L)
        # a row naming a lake that also holds a stream (Shuswap Lake's row on the Little River): the
        # water by its name, never "this lake" on a river
        if name and (n in ("lake", "reservoir", "pond")) != (P.kind == "lake"):
            return f"On {the}", f"{the}’s rule"
        return f"On this {n}", f"this {n}’s rule"
    if b == "bc":
        return "In B.C.", "the B.C. rule"
    return (f"In {name}", f"{name}’s rule") if name else ("In this area", "this area’s rule")


def _kept_origins(model: dict, S: str) -> List[str]:
    from pipeline.deliver.answers.rows import _keepish
    R_ = model["R"].get(S) or {}
    return [o for o, k in (("hatchery", "H"), ("wild", "W"))
            if R_.get(k) and _keepish(R_[k]["status"])]


def _items_of(rows_out: List[dict], home: int, S: str) -> List[dict]:
    """Every row item holding `S`, the group's own row first."""
    order = [rows_out[home]] + [r for i, r in enumerate(rows_out) if i != home]
    return [it for r in order for it in r.get("items") or [] if S in it["members"]]


def _kind_n(rows_out: List[dict], home: int, S: str, total) -> Tuple[Optional[float], int]:
    """The most of one kind, and which item says it: its OWN row's item (its bands) — a
    cross-reference on the group's row ("has its own limit, counts toward this one", rows F4) only
    bounds it by the number it counts toward. The Shuswap's "char 1 (none under 60 cm)" on a stream
    under it: 1, not the stream share's 4; the Flathead's tributaries' trout 1: 1, not 2. Kinds
    share a kind row only when one item holds them (one limit together): Bridge Lake's lake trout 1
    (its own row) and Region 5's Dolly Varden 1 are two rows, not "1" for both together."""
    its = _items_of(rows_out, home, S)
    got = [(n, it) for it in its if (n := _item_n(it, total)) is not None]
    if not got:
        return None, 0
    own = next((it for it in its if not it.get("xref") and it.get("bands")), got[0][1])
    return min(n for n, _ in got), id(own)


def _item_n(it: dict, total) -> Optional[float]:
    """The most of one kind the row's item keeps: its bands' top, or what its own row allows against
    the number it counts toward (a cross-reference, rows F4)."""
    if it.get("bands"):
        tops = [INF if mx is None else mx for _a, _b, mx in it["bands"]]
        return max(tops) if tops else None
    if it.get("xref"):
        against = INF if it.get("against") in (None, "unlimited") else it["against"]
        ns = [INF if x.get("n") is None else x["n"] for x in it.get("origins") or [] if not x.get("rel")]
        own = max(ns) if ns else against
        return min(own, against)
    return None


def _duties(P, model: dict, S: str, n) -> List[str]:
    from pipeline.deliver.answers.rows import main_res
    R_ = model["R"].get(S) or {}
    m = main_res(R_.get("H"), R_.get("W"))
    out: List[str] = []
    for l in (m or {}).get("lines") or []:
        r = l["r"]
        if l["t"] == "annual" and isinstance(r.f.get("take"), (int, float)):
            w = f"{_js(r.f['take'])} a year"
        elif l["t"] == "record" or (l["t"] == "duty" and r.f.get("record_retention")):
            w = "write each one on your licence right away"          # UR1 G (user panel: plain words)
        elif l["t"] == "duty" and r.type == "stop_fishing_after_quota":
            w = f"after you keep {_js(n)}, stop fishing for the day"        # UR2 5 (user panel: plain words)
        else:
            continue
        if w not in out:
            out.append(w)
    return out


def _caps_here(P, model: dict, grp: dict, kinds: Sequence[str], share: Optional[dict]
               ) -> Tuple[List[dict], List[dict]]:
    """(size caps, count caps) that hold some of the group's kinds here: the total's own size clauses
    (and a stream share's, on a stream: Region 8's "2 over 30 cm from streams"), its count clauses
    other than the share, and every cap OUTSIDE the total the reader says speaks for a kind here
    (ROUND12 3) — a kind's own row's size clause (Bennett Lake's lake trout "only 1 over 90 cm"), the
    region's "1 over 50 cm" beside a lake's own trout quota, a count clause over two kinds (the Bella
    Coola's "of which only one may be a trout (cutthroat or rainbow)"). Each {r, take, over_cm, of}."""
    from pipeline.deliver.answers.rows import _bands, _num
    sizes = [s for s in grp["subcaps"] if s["over_cm"] is not None and s["under_cm"] is None
             and s["water"] in (None, P.kind)]
    counts = [s for s in grp["subcaps"] if s["over_cm"] is None and s["under_cm"] is None
              and s is not share and s["water"] in (None, P.kind)]
    seen = {s["r"] for s in grp["subcaps"]} | {grp["pool"]}
    for r in P.cands:
        if r.key in seen or r.k not in ("sizecap", "subcap") or r.f.get("water") not in (None, P.kind):
            continue
        of = [S for S in kinds if _speaks_for(P, model, r, S)]
        if not of:
            continue
        if r.k == "sizecap":
            hit = next(((a, b, t) for a, b, t in _bands(r) if t and a and b == INF), None)
            if hit is not None:
                sizes.append({"r": r.key, "take": _num(hit[2]), "over_cm": _num(hit[0]), "under_cm": None,
                              "of": of, "water": r.f.get("water"), "lifted": []})
        elif isinstance(r.f.get("take"), (int, float)) and r.f["take"] > 0 and not r.f.get("lengths"):
            counts.append({"r": r.key, "take": _num(r.f["take"]), "over_cm": None, "under_cm": None,
                           "of": of, "water": r.f.get("water"), "lifted": []})
    return sizes, counts


def kind_rows(b: dict) -> List[dict]:
    """A bucket's kind rows, its nested groups' included (ROUND14 3)."""
    return list(b["kinds"]) + [k for g in b.get("groups") or [] for k in g["kinds"]]


def _say_caps(P, model: dict, grp: dict, kinds: Sequence[str], info: Dict[str, dict],
              where_of: Dict[str, dict], buckets: List[dict], share: Optional[dict], what: str,
              states: List[dict]) -> None:
    """Every cap inside the group said ONCE, as a note of the bucket it governs (ROUND11 A1, ROUND12 3):
    {says, n, over_cm (0: a count of any length), kinds, not_counting, then [{n, over_cm, kinds}]} —
    a count over two kind rows ("Only 1 of these may be cutthroat trout or rainbow trout"), a size cap
    the kinds share ("Only 1 of these may be over 50 cm, not counting steelhead"), a kind's own
    ("Only 1 rainbow trout may be over 90 cm"), nested caps in order on one note ("Only 2 of these may
    be over 30 cm, and only 1 over 50 cm"). A cap is said where it binds; one another cap already
    implies (as small a size, no larger number, every kind it holds: Wahleach Lake's 1 over 40 cm under
    Region 2's 1 over 50 cm) or a count holding all its kinds with no larger number (the stream share
    of 2 under "2 over 50 cm", steelhead included) is not said."""
    sizes, counts = _caps_here(P, model, grp, kinds, share)
    stated = {x["r"] for x in states}
    rows_of = [(b, k) for b in buckets for k in b["kinds"]]

    def holding_of(K):
        return [b for b in buckets if any(where_of[S] is b for S in K)]

    # -- counts over two kind rows ------------------------------------------------------------------
    for c in sorted(counts, key=lambda c: c["take"]):
        K = [S for S in kinds if S in c["of"]]
        rk = [(b, k) for b, k in rows_of if set(k["species"]) & set(K)]
        if len(rk) < 2 or c["r"] in stated:
            continue
        free = sum(INF if k["n"] is None else k["n"] for _b, k in rk)
        holding = holding_of(K)
        if len(holding) == 1 and holding[0]["cap"] is not None:
            free = min(free, holding[0]["cap"])
        if c["take"] >= free:
            continue
        # ROUND14 3 (owner): a count over SOME of one bucket's kind rows (each wholly inside it) is a NESTED
        # GROUP of that bucket, holding those kinds with their own sizes — the Bella Coola mainstem's "of
        # which only one may be a trout (cutthroat or rainbow)": "Cutthroat trout or rainbow trout: 1 between
        # them"; a quota group comes before the size notes. Size caps stay notes.
        if len(holding) == 1 and all(set(k["species"]) <= set(K) for _b, k in rk) \
                and len(rk) < len(holding[0]["kinds"]):
            b0 = holding[0]
            moved = [k for _b, k in rk]
            b0["kinds"] = [k for k in b0["kinds"] if k not in moved]
            b0["groups"].append({"says": f"{common.cap(_names(K, ' or '))}: {_js(c['take'])} between them",
                                 "n": c["take"], "by": c["r"], "kinds": moved})
            states.append({"r": c["r"], "line": f"bucket {b0['title'] or 'the card'} group"})
            stated.add(c["r"])
            continue
        # one bucket: "Only 1 of these may be cutthroat trout or rainbow trout"; across buckets (Region 2's
        # Dolly Varden and lake trout 1 beside Chilliwack Lake's own Dolly Varden rule): the kinds named
        says = f"Only {_js(c['take'])} of these may be {_names(K, ' or ')}" if len(holding) == 1 else \
            f"Only {_js(c['take'])} {_names(K, ' or ')} a day in all"
        holding[0]["notes"].append({"says": says,
                                    "n": c["take"], "over_cm": 0, "kinds": K, "not_counting": [],
                                    "then": []})
        states.append({"r": c["r"], "line": f"bucket {holding[0]['title'] or 'the card'} note"})
        stated.add(c["r"])

    # -- size caps, smallest size first ---------------------------------------------------------------
    said: List[Tuple[dict, List[str], dict]] = []          # (cap, its kinds over the size, its note)
    covered: Dict[float, set] = {}
    for sc in sorted(sizes, key=lambda s: (s["over_cm"], s["take"])):
        cm = sc["over_cm"]
        over = [S for S in kinds if S in sc["of"] and info[S]["hi"] > cm]
        if not over or sc["r"] in stated:
            continue
        if any(o["over_cm"] <= cm and o["take"] <= sc["take"] and set(over) <= set(ov) for o, ov, _n in said):
            continue                                       # implied by a cap already said
        if any(set(over) <= set(c["of"]) and c["take"] <= sc["take"]
               for c in counts + ([share] if share is not None else [])):
            continue                                       # a count holding them all says it
        binds = []
        for S in over:
            b, n, lo = where_of[S], info[S]["n"], info[S]["lo"]
            cap_b = b["cap"] if b["cap"] is not None else INF
            shared = any(where_of[x] is not b for x in over if x != S)
            if sc["take"] < n or (lo >= cm and len(sc["of"]) > 1 and (sc["take"] < cap_b or shared)):
                binds.append(S)
        cov = covered.setdefault(cm, set())
        if not [S for S in binds if S not in cov]:
            continue
        cov |= set(over)
        holding = holding_of(over)
        first = holding[0]
        n_w = "none" if not sc["take"] else f"only {_js(sc['take'])}"
        # nested in a size group already said on this bucket: a smaller size holding every one of these
        # with a larger number — "…: only 2 over 30 cm between them, and only 1 over 50 cm"
        host = next((nt for o, ov, nt in said if nt in first["sizes"] and o["over_cm"] < cm
                     and o["take"] > sc["take"] and set(over) <= set(ov) and not nt["not_counting"]),
                    None)
        if host is not None and len(holding) == 1:
            host["says"] += f", and {n_w} over {_js(cm)} cm"
            host["then"].append({"n": sc["take"], "over_cm": cm, "species": book_order(over)})
            said.append((sc, over, host))
            states.append({"r": sc["r"], "line": f"bucket {first['title'] or 'the card'} size group"})
            stated.add(sc["r"])
            continue
        pool = kind_rows(first) if len(holding) == 1 else [k for b in buckets for k in kind_rows(b)]
        aside = [S for k in pool for S in k["species"] if S not in sc["of"] and info[S]["hi"] > cm]
        if aside and len(over) <= len(aside):
            # a cap on a few of the kinds (Region 4's "rainbow and cutthroat trout, no more than 1 over 50
            # cm"): it claims nothing of the rest (another region's cap may hold them on a boundary water)
            aside = []
        # ROUND15 1 (owner): a size cap is a BOXED GROUP like the count groups, naming EXACTLY the kinds it
        # can bind today — kept and able to pass the size (Bella Coola May 1: brown trout, Dolly Varden/bull
        # trout, lake trout and brook trout; never cutthroat ≤33 cm, rainbow ≤50 cm or a closed steelhead)
        sp = book_order(over)
        group = {"says": f"{_named_list(sp)}: {n_w} over {_js(cm)} cm"
                         + (" between them" if len(sp) > 1 or len(over) > 1 else ""),
                 "n": sc["take"], "over_cm": cm, "by": sc["r"], "species": sp,
                 "not_counting": book_order(aside), "then": []}
        first["sizes"].append(group)
        said.append((sc, over, group))
        states.append({"r": sc["r"], "line": f"bucket {first['title'] or 'the card'} size group"})
        stated.add(sc["r"])


#: the order a bucket's replaced caps are named in: counts (the share first), size caps, minimums, maximums
_CAP_ORDER = ("share", "count", "over", "min", "max")


def _cap_name(r, inch: bool = False) -> Optional[Tuple[str, str]]:
    """(its order, its name) of a cap a water's rule lifts (ROUND13 D), from the rule's own number and
    bands: the share "2-from-streams limit", a count "1-a-day limit", a size clause "1-over-50-cm
    limit", a floor "30 cm minimum size", a ceiling "50 cm maximum size"; None for anything else (a
    closure, a release)."""
    from pipeline.deliver.answers.rows import _bands
    take = r.f.get("take")
    if isinstance(take, (int, float)) and take > 0 and not r.f.get("lengths"):
        w = r.f.get("water")
        return ("share", f"{_js(take)}-from-{w}s limit") if w else ("count", f"{_js(take)}-a-day limit")
    for a, b, t in _bands(r):
        if t and a and b == INF:
            # ROUND18 2: with inches, "limit of 1 over 50 cm (20 in)"
            return "over", (f"limit of {_js(t)} over {_cm(a, True)}" if inch else f"{_js(t)}-over-{_js(a)}-cm limit")
    for a, b, t in _bands(r):
        if t == 0 and not a and b != INF:
            return "min", f"{_cm(b, inch)} minimum size"
        if t == 0 and a and b == INF:
            return "max", f"{_cm(a, inch)} maximum size"
    return None


def _replaces(P, model: dict, ks: Sequence[str], lifters: set, inch: bool = False) -> Optional[dict]:
    """A water's own-rule bucket's statement of what it replaces (ROUND13 D, owner: the lift box goes;
    the bucket says it): {says, rules} — every cap the reader says the bucket's lifting rules (`lifters`)
    LIFT for its kinds (a kept origin), each named by its owner (`_cap_name`), for the fish it is lifted
    for, by name (ROUND19 6): the Vedder's "Replaces Region 2’s 2-from-streams limit and 30 cm minimum size for
    hatchery rainbow trout"; the Duncan's "Replaces Region 4’s 2-from-streams limit for …, and its 1-over-50-cm limit for
    rainbow trout". None when no cap is lifted."""
    got: Dict[str, List[str]] = {}
    for S in ks:
        for o in _kept_origins(model, S):
            for key, v in ((P.ladder.get(S) or {}).get(o) or {}).items():
                r = P.by_key.get(key)
                if r is None or not v or v[0] != "lifted" or v[3] not in lifters or _cap_name(r) is None:
                    continue
                if S not in got.setdefault(key, []):
                    got[key].append(S)
    if not got:
        return None
    keys = sorted(got, key=lambda k: (_CAP_ORDER.index(_cap_name(P.by_key[k])[0]), k))
    every = set(ks)
    # one phrase per set of fish, the caps lifted for every kind of the bucket first
    sets: List[Tuple[frozenset, List[str]]] = []
    for k in sorted(keys, key=lambda k: set(got[k]) != every):
        fs = frozenset(got[k])
        hit = next((x for x in sets if x[0] == fs), None)
        if hit is None:
            sets.append((fs, [k]))
        else:
            hit[1].append(k)
    said: set = set()
    parts = []
    for fs, ks_ in sets:
        owners: Dict[str, List[str]] = {}
        for k in ks_:
            x = P.by_key[k].x
            owners.setdefault(x.get("entry_name") or "the region", []).append(_cap_name(P.by_key[k], inch)[1])
        # ROUND18 2: "Region 4’s 2-from-streams limit and the limit of 1 over 50 cm (20 in)"
        whose = common.join([f"{'its' if o in said else o + '’s'} "
                             f"{common.join([('the ' + n if i and n.startswith('limit of') else n) for i, n in enumerate(ns)], ' and ')}"
                             for o, ns in owners.items()], " and ")
        said |= set(owners)
        # ROUND19 6 (owner: "… minimum size for these fish" read as the brook trout's): the fish it is replaced for,
        # by name, with their origin where they keep only one ("for hatchery rainbow trout")
        mine = [S for S in ks if S in fs]
        org = {tuple(_kept_origins(model, S)) for S in mine}
        for_ = (f"{org.pop()[0]} " if len(org) == 1 and len(next(iter(org))) == 1 else "") + _names(mine)
        parts.append(f"{whose} for {for_}")
    return {"says": "Replaces " + ", and ".join(parts),
            "rules": [k for _fs, ks_ in sets for k in ks_]}


def book_order(codes: Sequence[str]) -> List[str]:
    """Fish in the book's order (p.80, `catalogue.BOOK_SPECIES`: rainbow trout, steelhead, cutthroat
    trout, brown trout, Dolly Varden, lake trout, brook trout, …); anything else after, as given."""
    from pipeline.regs.parsing.catalogue import BOOK_SPECIES
    rank = {S: i for i, S in enumerate(BOOK_SPECIES)}
    return sorted(codes, key=lambda S: (rank.get(S, len(rank)), list(codes).index(S)))


def _named_list(codes: Sequence[str]) -> str:
    """ROUND15 1: the kinds a size group binds, each named ("Brown trout, Dolly Varden/bull trout, lake
    trout or brook trout") — never a group's name, which would claim kinds it does not bind."""
    return common.cap(common.join([common.lc(common.sp_name(c)) for c in codes], " or "))


def _named_with_members(codes: Sequence[str]) -> str:
    """ROUND14 6 (owner): a closed group said WITH its members, "Char (Dolly Varden/bull trout, lake trout
    and brook trout)"; fish that are no group joined, "Rainbow trout and cutthroat trout"."""
    names = [common.lc(common.sp_name(c)) for c in codes]
    g = common.group_for(codes)
    if g:
        return f"{common.cap(g)} ({common.join(names)})"
    return common.cap(common.join(names))


def _not_kept(P, model: dict, row: dict, kinds: Sequence[str]) -> List[dict]:
    """ROUND13 S (owner): every fish the card's row holds (`all_members`) that no origin may keep here,
    SAID — {species, status: closed | release, says, by, rules, whose}: "Steelhead: no fishing for them"
    with "this river’s rule" (the Bella Coola's r13; the page cites `by`'s Book page), "Dolly Varden/bull
    trout: release every one". ROUND14 6 (owner): ONE line per status and deciding ROW — the Vedder's May
    hatchery rainbow and cutthroat releases (r6, r7) are "Rainbow trout and cutthroat trout: release every
    one", `rules` every deciding rule, `by` the first — fish in the book's order (`book_order`), lines in
    the order of their first fish, a closed group named with its members (`_named_with_members`)."""
    from pipeline.deliver.answers.rows import main_res
    out: Dict[tuple, dict] = {}
    for S in book_order(row.get("all_members") or []):
        if S in kinds:
            continue
        R_ = model["R"].get(S) or {}
        m = main_res(R_.get("H"), R_.get("W"))
        if not m or m["status"] not in ("closed", "release") or m.get("win") is None:
            continue
        whose = _whose(P, m["win"])[1]
        key = (m["status"], m["win"].entry_id, whose)
        x = out.get(key)
        if x is None:
            x = out[key] = {"species": [], "status": m["status"], "by": m["win"].key, "rules": [],
                            "whose": whose}
        x["species"].append(S)
        if m["win"].key not in x["rules"]:
            x["rules"].append(m["win"].key)
    for x in out.values():
        name = "Steelhead" if x["species"] == ["ST"] and P.steelhead_water else _named_with_members(x["species"])
        # UR1 F (user panel: may I catch and release?): the book's p.4 legend — "you may not deliberately fish for the
        # species named even if your intention is to release … If you accidentally catch [one], you must release it"
        x["says"] = f"{name}: " + ("no fishing for them, not even catch and release — release any you hook by accident"
                                   if x["status"] == "closed" else "release every one")
    return [{"species": x["species"], "status": x["status"], "says": x["says"], "by": x["by"],
             "rules": x["rules"], "whose": x["whose"]} for x in out.values()]


def card_of(P, model: dict, grp: dict, rows_out: List[dict], home: int) -> dict:
    """The card fields of one group total (see the module docstring)."""
    from pipeline.deliver.answers.rows import _lift_words, _kept, badge_of, written_name_of, keep_range
    T = P.by_key[grp["pool"]]
    total = grp["total"]
    unlimited = total == "unlimited"
    kinds = list(grp["kinds"])
    what = written_name_of(T)
    states: List[dict] = []
    # the stream (lake) share holding EVERY kind of the total (the lead share, rows 7)
    share = next((s for s in grp["subcaps"] if s["water"] and s["over_cm"] is None
                  and s["under_cm"] is None
                  and set(s["of"]) | {S for L in s["lifted"] for S in L["members"]} == set(kinds)),
                 None)
    sizes = [s for s in grp["subcaps"] if s["over_cm"] is not None and not s["water"]]

    by_lift = {S: L["by"] for L in (share or {}).get("lifted", []) for S in L["members"]}
    cap_all = INF if unlimited else total

    # -- every kind: its own number, keep range, and the INNER pool holding it --------------------
    # A kind whose decided winner is another pool than the total (a water's own "trout 1": the St.
    # Mary's tributaries, Abbott Lake, the Dean; an area's: the Liard watershed's bull trout 1)
    # draws from THAT cap, smaller than the share or the total it would otherwise share: it gets
    # a bucket of its own, and the headline is what this water allows (RULINGS P9, ROUND9 P1/P4).
    info: Dict[str, dict] = {}
    for S in kinds:
        n, said_by = _kind_n(rows_out, home, S, total)
        if n is None:
            n = cap_all
        lo, hi = keep_range(P, model, [S]) or (0, INF)
        default = share["take"] if share is not None and S in share["of"] else cap_all
        m = _kept(model, S)
        W = m["win"] if m else None
        # ROUND14 3 (owner): WHOSE RULE a bucket is. A water's own pool smaller than the share or the total
        # draws its kinds into its own bucket; one printing the SAME number as the region's share does too
        # — the Bella Coola mainstem's "trout/char daily quota = 2" (Apr 1-May 31) is the river's rule,
        # never "all Region 5 streams combined" though Region 5's 2 from streams says the same
        own_equal = W is not None and share is not None and S in share["of"] and badge_of(W) == "water"
        inner = W if (W is not None and W.k == "pool" and W.key != grp["pool"] and S not in by_lift
                      and isinstance(W.f.get("take"), (int, float)) and not W.f.get("unlimited")
                      and (W.f["take"] < default or (own_equal and W.f["take"] == default))) else None
        info[S] = {"n": n, "said_by": said_by, "lo": lo, "hi": hi, "inner": inner}

    def capacity(K: Sequence[str], base) -> Optional[float]:
        """How many of the kinds `K` this water lets you keep together, at most `base`: each inner
        pool holds its kinds to its number, every other kind to its own; a sub-cap of the total
        holding every one of them (a count clause, the share, a size clause every one of them is
        wholly inside) holds them together."""
        best = INF if base is None else base
        held: Dict[str, float] = {}
        free = 0
        for S in K:
            W = info[S]["inner"]
            if W is not None:
                held[W.key] = held.get(W.key, 0) + info[S]["n"]
            else:
                free += info[S]["n"]
        best = min(best, free + sum(min(v, P.by_key[k].f["take"]) for k, v in held.items()))
        for sc in grp["subcaps"]:
            if sc["under_cm"] is not None or not set(K) <= set(sc["of"]):
                continue
            if sc["over_cm"] is not None and not all(info[S]["lo"] >= sc["over_cm"] for S in K):
                continue
            best = min(best, sc["take"])
        if best == INF:
            return None
        return int(best) if float(best).is_integer() else best

    # -- headline: this water's number with no lift ------------------------------------------------
    if unlimited:
        head = {"n": None, "unit": f"no daily limit on {what}", "says": f"No daily limit on {what}"}
    else:
        base = share["take"] if share is not None else total
        n_head = capacity(kinds, base)
        # UR2 1 (user panel: the big "2 a day from this stream" read as the party's limit): whose number it is
        unit = f"a day per angler from this {share['water']}" if share is not None else "a day per angler"
        head = {"n": n_head, "unit": unit, "says": f"{_js(n_head)} {unit}"}
        inners = {info[S]["inner"].key for S in kinds if info[S]["inner"] is not None}
        if n_head == base:
            states.append({"r": share["r"] if share is not None else grp["pool"], "line": "headline"})
        elif len(inners) == 1 and all(info[S]["inner"] is not None for S in kinds) \
                and n_head == P.by_key[next(iter(inners))].f["take"]:
            states.append({"r": next(iter(inners)), "line": "headline"})

    # -- origin ----------------------------------------------------------------------------------
    os_ = {tuple(_kept_origins(model, S)) for S in kinds}
    origin = None
    # ROUND19 6 (owner: "Hatchery only — release wild trout and char" read as releasing the listed fish): what to
    # keep first, then what goes back
    if os_ == {("hatchery",)}:
        origin = f"Keep hatchery fish only — wild {what} go back."
    elif os_ == {("wild",)}:
        origin = f"Keep wild fish only — hatchery {what} go back."
    one_origin = origin is not None

    # -- lifts out of the share, and the total under it ------------------------------------------
    region = (P.by_key[share["r"]].x.get("entry_name") if share else None) or "the region"
    # ROUND10 W1: ONE line per lifting entry (the Duncan's rainbow and Dolly Varden rules are one row),
    # each kind with its number, highest first; `each` keeps every rule's kinds, number and `by`
    lifts: List[dict] = []
    lift_cap: Dict[str, float] = {}
    each_of: Dict[str, List[dict]] = {}
    for L in (share or {}).get("lifted", []):
        R_ = P.by_key[L["by"]]
        own = R_.f.get("take") if isinstance(R_.f.get("take"), (int, float)) \
            and not R_.f.get("unlimited") else None
        n = min(own, total) if own is not None and not unlimited else (total if not unlimited else None)
        lift_cap[L["by"]] = n
        each_of.setdefault(R_.entry_id, []).append(
            {"kinds": list(L["members"]), "n": n, "by": L["by"],
             "fish": _lift_words(P, L["by"], L["members"])})
        states.append({"r": L["by"], "line": "lift"})
    limit = f"{region}’s {_js(share['take'])}-from-{share['water']}s limit" if share is not None else ""
    for each in each_of.values():
        each.sort(key=lambda e: -(INF if e["n"] is None else e["n"]))     # stable: highest first
        where, whose = _whose(P, P.by_key[each[0]["by"]])
        allows = ["any number" if e["n"] is None else f"up to {_js(e['n'])} a day" for e in each]
        if len(each) == 1:
            says = f"{where}, {each[0]['fish']}: {allows[0]} — {whose} replaces {limit} for them."
        else:
            named = common.join([f"{e['fish']} ({a})" for e, a in zip(each, allows)], " and ")
            says = f"{where}, its own rule replaces {limit} for {named}."
        ns = [e["n"] for e in each]
        lifts.append({"says": says, "by": each[0]["by"],
                      "kinds": list(dict.fromkeys(S for e in each for S in e["kinds"])),
                      "n": None if None in ns else max(ns), "each": each})
    # the headline's "or N if they’re …": every lifted number above it, highest first
    also = sorted((e for L in lifts for e in L["each"]
                   if e["n"] is not None and head.get("n") is not None and e["n"] > head["n"]),
                  key=lambda e: -e["n"])
    if also:
        head["also"] = [{"n": e["n"], "says": f"{_js(e['n'])} if they’re {e['fish']}"} for e in also]
    total_line = None
    if not unlimited and (share is not None or (head["n"] is not None and head["n"] < total)):
        wk = share["water"] if share is not None else ("stream" if P.kind == "stream" else "lake")
        other = "lakes" if wk == "stream" else "streams"
        scope = T.x.get("entry_name") or "the region"
        total_line = f"No more than {_js(total)} {what} a day per angler across {scope}, {other} included."   # UR1 A
        states.append({"r": grp["pool"], "line": "total"})

    # -- buckets and kind rows -------------------------------------------------------------------
    share_b = None
    if share is not None:
        share_b = {"title": f"Up to {_js(share['take'])} a day between them — all {region} "
                            f"{share['water']}s combined",
                   "cap": share["take"], "by": share["r"], "why": "share", "kinds": []}
    # one bucket per lifting entry (Duncan: its rainbow and its Dolly Varden rules are one row)
    lift_bucket: Dict[str, dict] = {}
    for L in (share or {}).get("lifted", []):
        R_ = P.by_key[L["by"]]
        b = lift_bucket.get(R_.entry_id)
        if b is None:
            b = {"title": common.cap(_whose(P, R_)[1]), "cap": 0, "by": L["by"], "why": "rule",
                 "kinds": []}
            lift_bucket[R_.entry_id] = b
        c = lift_cap[L["by"]]
        b["cap"] = None if c is None or b["cap"] is None else b["cap"] + c
    for b in lift_bucket.values():
        if b["cap"] is not None and not unlimited:
            b["cap"] = min(b["cap"], total)
    # one bucket per entry whose own pool holds kinds (the Shuswap's char 1 and rainbow 1: one)
    own_bucket: Dict[str, dict] = {}
    rest = {"title": None, "cap": None if unlimited else total, "by": grp["pool"], "why": "total",
            "kinds": []}
    members: Dict[int, List[str]] = {}

    def bucket_of(S):
        if S in by_lift:
            return lift_bucket[P.by_key[by_lift[S]].entry_id]
        W = info[S]["inner"]
        if W is not None:
            b = own_bucket.get(W.entry_id)
            if b is None:
                b = own_bucket[W.entry_id] = {"title": common.cap(_whose(P, W)[1]), "cap": 0,
                                              "by": W.key, "why": "rule", "kinds": [], "_w": set()}
            if W.key not in b["_w"]:
                b["_w"].add(W.key)
                b["cap"] += W.f["take"]
            return b
        if share_b is not None and S in share["of"]:
            return share_b
        return rest

    where_of = {S: bucket_of(S) for S in kinds}
    # ROUND12 2 (owner): a water's own rule bucket(s) FIRST — it is what the regs name — then the
    # region's share, then the rest of the total
    buckets: List[dict] = [b for b in (list(lift_bucket.values()) + list(own_bucket.values()) + [share_b]
                                       + [rest]) if b is not None]
    for b in buckets:
        b.pop("_w", None)
        ks = [S for S in kinds if where_of[S] is b]
        if not ks:
            continue
        if b is not share_b and b["why"] == "rule" and b not in lift_bucket.values():
            b["cap"] = min(b["cap"], share["take"] if share is not None and set(ks) <= set(share["of"])
                           else cap_all)
        b["cap"] = capacity(ks, b["cap"])
    if rest["kinds"] is not None and len([b for b in buckets if any(where_of[S] is b for S in kinds)]) > 1:
        rest["title"] = f"The rest of the {_js(total)} a day"

    rows_by: Dict[tuple, dict] = {}
    side: Dict[int, dict] = {}            # ROUND18 2: each kind row's words with inches and its ranges (the tree)
    st_cm = None
    for S in kinds:
        n, said_by, lo, hi = info[S]["n"], info[S]["said_by"], info[S]["lo"], info[S]["hi"]
        b = where_of[S]
        if b["cap"] is not None and n >= b["cap"]:
            said_by = 0                        # the bucket's own cap holds them together
        o = _kept_origins(model, S)
        ow = o[0] if len(o) == 1 else ""
        if S == "ST" and P.steelhead_water:
            from pipeline.deliver.bundle import read as RD
            st_cm = st_cm or RD._steelhead_min_cm()
            name, detail = "Steelhead", " ".join(x for x in (ow, "rainbow trout over 50 cm") if x)
            # ROUND19 1: the tree names the origin with the fish ("hatchery steelhead"), never in the detail
            detail_in = f"rainbow trout over {common.cm_in(st_cm)}"
            segs = [(st_cm, INF)]
        else:
            name = None
            detail = ", ".join(x for x in (ow, _detail_words(P, model, S, lo, hi)) if x)
            detail_in = _detail_words(P, model, S, lo, hi, inch=True)
            segs = size_segments(P, model, S, lo, hi)
        if n == 0:
            # ROUND13 Z (owner): a kind keeping none here says no size — every band of it releases (the
            # Bella Coola's cutthroat on Oct 10: "none under 25 cm and all cutthroat trout catch and
            # release" is "catch and release", never "25 cm or longer")
            detail = detail_in = "catch and release"
            segs = []
        # ROUND11 A1: a size cap is never on a kind row — the bucket's note says it once (below)
        notes: List[str] = []
        if b["why"] == "rule" and not unlimited:
            if b not in lift_bucket.values() and share is not None and S in share["of"]:
                # (ROUND14 3: not under the water's own rule printing the share's very number)
                if info[S]["inner"] is None or info[S]["inner"].f["take"] != share["take"]:
                    notes.append(f"counts toward the {_js(share['take'])} from {share['water']}s")
            else:
                notes.append(f"counts toward the {_js(total)} a day")
        duties = _duties(P, model, S, n)
        key = (id(b), said_by, n, name, detail, tuple(notes), tuple(duties))
        k = rows_by.get(key)
        if k is None:
            k = {"species": [], "name": name, "detail": detail, "n": None if n == INF else n,
                 "notes": notes, "duties": duties}
            rows_by[key] = k
            b["kinds"].append(k)
            side[id(k)] = {"detail": detail_in, "segs": segs, "origins": o}
        k["species"].append(S)
    for b in buckets:
        for k in b["kinds"]:
            if k["name"] is None:
                k["name"] = common.cap(_names(k["species"]))
            # ROUND15 8 (owner): each kept kind says its origin ("hatchery, 30 cm or longer") even where the
            # origin line says it for every kind — a row read alone must not look like any origin is kept
    buckets = [b for b in buckets if b["kinds"]]
    for b in buckets:
        b["notes"] = []
        b["groups"] = []
        b["sizes"] = []
    _say_caps(P, model, grp, kinds, info, where_of, buckets, share, what, states)
    # ROUND15 5 (owner): an own-rule bucket SAYS its shared cap where it holds its kinds together — the
    # Bella Coola's "Up to 2 a day between them" (the tester added the group's 1 to the other kinds' 2 and
    # got 3); one kind, or a cap no smaller than its rows together, needs none
    for b in buckets:
        rows_n = [k["n"] for k in b["kinds"]] + [s["n"] for s in b["groups"]]
        many = len({S for k in kind_rows(b) for S in k["species"]}) > 1
        free = INF if any(n is None for n in rows_n) else sum(rows_n)
        b["between"] = f"Up to {_js(b['cap'])} a day between them" \
            if b["why"] == "rule" and b["cap"] is not None and many and b["cap"] < free else None
    if len(buckets) == 1 and buckets[0]["why"] == "total":
        buckets[0]["title"] = None
    for b in buckets:
        lifters = {L["by"] for L in (share or {}).get("lifted", [])
                   if lift_bucket.get(P.by_key[L["by"]].entry_id) is b}
        b["replaces"] = _replaces(P, model, [S for S in kinds if where_of[S] is b], lifters) \
            if lifters else None
    not_kept = _not_kept(P, model, rows_out[home], kinds)

    # -- the worked count (RULINGS Q23b) -----------------------------------------------------------
    worked: List[dict] = []
    if share is not None and lifts and not unlimited:
        parts = [e for L in lifts for e in L["each"]]              # every lifting rule (W1)
        main = max(parts, key=lambda L: (L["n"] or 0))
        q = main["n"] or 0
        S0 = main["kinds"][0]
        # UR2 2 (user panel: "Keep 3 rainbows here → 1 more: any kind …" confused three of six): the fish by origin and
        # name, "If you keep 3 hatchery rainbow trout: 1 more fish of any kind, from here or from a lake."
        from pipeline.deliver.answers.tree import kind_names
        said_origin = P.by_key[main["by"]].f.get("origin")           # the lifting rule's own (None: any origin)
        m_origin = said_origin or "hatchery"
        plural = kind_names(main["kinds"], main["kinds"] == ["ST"], origin=said_origin)
        other_kind = "lake" if share["water"] == "stream" else "stream"
        # ROUND20b C4 (owner: fewer words): the two ends of the range only — all the lifted fish ("that’s your day"),
        # and as many as the share ("none from other streams — up to N more from here or from lakes"); the lines
        # between are arithmetic the reader does not need
        for k in sorted({q, share["take"]}, reverse=True):
            if k <= 0 or k < share["take"]:
                continue
            left = total - k
            # RULINGS Q23b (refined 2026-10-12): on its own water the lifted kind does not use the share
            # for the other kinds — here: the lifted kinds' rest, and the share's own number
            here = min(left, share["take"] + sum(max(0, (L["n"] or 0) - (k if L is main else 0))
                                                 for L in parts))
            if left == 0:
                a = f"that’s your {what} for the day, everywhere."
            elif k == share["take"]:
                a = (f"none from other {share['water']}s today — up to {_js(left)} more "
                     f"from here or from {other_kind}s.")
            elif left == 1:
                a = f"1 more fish of any kind, from here or from a {other_kind}."
            else:
                a = f"{_js(left)} more fish of any kind, from here or from {other_kind}s."
            worked.append({"kept": k, "q": f"If you keep {k} {plural}", "a": a, "species": [S0],
                           "origin": m_origin,
                           "share": share["r"], "here": here, "other": 0, "away": left,
                           "at": "here", "over_cm": None, "most": "exactly", "of": None})
    # ROUND12 4 (owner): ONE list — the keep-N lines, then the kept-elsewhere examples they do not
    # repeat (on a lifted water the stream-tally line is the keep-N count read backwards); on a water
    # with no lift the kept-elsewhere examples are the list. Each keeps the basket it starts from and
    # the number the gate proves.
    el = rows_out[home].get("elsewhere") or {}
    for x in el.get("lines") or []:
        if worked and x["kind"] == "stream_tally":
            continue
        # "Kept 2 elsewhere today? Keep 3 more here" is the total's arithmetic: said where this water's
        # number IS the total (Ross Lake's own trout 1 counts the fish kept elsewhere too — the
        # buckets say it)
        if x["kind"] == "tally" and (head.get("n") is None or head["n"] != rows_out[home].get("daily")):
            continue
        worked.append({"kept": x["kept"], "q": x["q"], "a": x["a"], "species": x["species"],
                       "origin": x["origin"], "share": None, "here": x["here"], "other": None,
                       "away": None, "at": x["at"], "over_cm": x["over_cm"], "most": x["most"],
                       "of": x["of"]})
    # ROUND16 1 (owner): the species × size table and the exact cap system a client checks a basket with
    from pipeline.deliver.answers import caps as CP
    sysm = CP.system(P, grp["pool"], book_order(list(dict.fromkeys(
        kinds + [S for x in not_kept for S in x["species"]]))))
    table = {**sysm["table"], "steelhead_cm": sysm["steelhead_cm"]}
    # ROUND18 1/2/3/4/7/8 (owner): the card as a TREE of tiers holding the kinds, and its compact header
    from pipeline.deliver.answers import tree as TR
    tree, header = TR.tree_of(P, model, TR.Ctx(
        grp=grp, buckets=buckets, info=info, side=side, share=share, unlimited=unlimited, head=head,
        origin_one=(os_.pop()[0] if one_origin else None), lifting=[b for b in lift_bucket.values() if b in buckets],
        what=what))
    return {"headline": head, "origin": origin, "lifts": lifts, "total_line": total_line,
            "buckets": buckets, "not_kept": not_kept, "worked": worked, "states": states,
            "table": table, "cap_kinds": sysm["kinds"], "caps": sysm["caps"], "tree": tree, "header": header}
