"""THE QUOTA CARD (answers 2.8, ROUND9 A1-A3) — water-first, each fact once.

A group total (`rows.group_of`: the pool several kinds share, its sub-caps, what a water's own rule lifts
out of them) is said as the angler reads a water:

  headline    the number from THIS water with no lift: on a Region 2 stream "2 a day from this stream"
              (the stream share holding every kind), on a lake the total ("4 a day");
  origin      "Hatchery only — release wild trout and char." where every kind keeps only one origin;
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
  buckets     the kinds grouped under the cap they draw from — "Share the 2 from streams" (steelhead;
              cutthroat, brook and brown; Dolly Varden and lake trout), the lifting rule ("This river’s
              rule": rainbow trout 4), or the total — each kind with its number, origin and size, its
              duties (10 a year, record it, stop after 2) and its SPECIES (one picture each, P5); a size
              cap the kinds share is the bucket's NOTE, said once (ROUND11 A1: "Only 1 of these may be
              over 50 cm, not counting steelhead"; a cap spanning buckets on the first, "Only 1 of the
              day’s trout and char may be over 50 cm") — never on a kind row;
  worked      RULINGS Q23b with the count worked through, for a lift past the share: "Keep 4 rainbows
              here → that’s your trout and char for the day, everywhere." … each with the numbers the
              gate proves (`here`, `other` — another water of this kind, `away` — the other kind);
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
#: a short plural for the worked count ("Keep 3 rainbows here")
SHORT_PLURAL = {"RB": "rainbows", "CT": "cutthroat", "EB": "brook trout", "GB": "brown trout",
                "DV": "Dolly Varden", "BT": "bull trout", "LT": "lake trout", "ST": "steelhead",
                "KO": "kokanee"}


def _js(v):
    from pipeline.deliver.answers.rows import _js as js
    return js(v)


def _names(codes: Sequence[str], conj: str = " and ") -> str:
    g = common.group_for(codes)
    if g:
        return common.lc(g)
    return common.join([common.lc(common.sp_name(c)) for c in codes], conj)


def _size_words(lo: float, hi: float) -> str:
    if not lo and hi == INF:
        return "any size"
    if not lo:
        return f"{_js(hi)} cm or less"
    if hi == INF:
        return f"{_js(lo)} cm or longer"
    return f"{_js(lo)}–{_js(hi)} cm"


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
    if L.via == "trib" and name:
        return f"On the {name} and its tributaries", f"the {name}’s rule"
    b = badge_of(L)
    if b == "water":
        n = _noun(P, L)
        # a row naming a lake that also holds a stream (Shuswap Lake's row on the Little River): the
        # water by its name, never "this lake" on a river
        if name and (n in ("lake", "reservoir", "pond")) != (P.kind == "lake"):
            return f"On the {name}", f"the {name}’s rule"
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
            w = "record each one on your licence"
        elif l["t"] == "duty" and r.type == "stop_fishing_after_quota":
            w = f"stop fishing for the day after {_js(n)}"
        else:
            continue
        if w not in out:
            out.append(w)
    return out


def card_of(P, model: dict, grp: dict, rows_out: List[dict], home: int) -> dict:
    """The card fields of one group total (see the module docstring)."""
    from pipeline.deliver.answers.rows import _lift_words, _kept, written_name_of, keep_range
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
        inner = W if (W is not None and W.k == "pool" and W.key != grp["pool"] and S not in by_lift
                      and isinstance(W.f.get("take"), (int, float)) and not W.f.get("unlimited")
                      and W.f["take"] < default) else None
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
        unit = f"a day from this {share['water']}" if share is not None else "a day"
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
    if os_ == {("hatchery",)}:
        origin = f"Hatchery only — release wild {what}."
    elif os_ == {("wild",)}:
        origin = f"Wild only — release hatchery {what}."
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
        total_line = f"No more than {_js(total)} {what} a day across {scope}, {other} included."
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
    buckets: List[dict] = [b for b in ([share_b] + list(lift_bucket.values()) + list(own_bucket.values())
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
    for S in kinds:
        n, said_by, lo, hi = info[S]["n"], info[S]["said_by"], info[S]["lo"], info[S]["hi"]
        b = where_of[S]
        if b["cap"] is not None and n >= b["cap"]:
            said_by = 0                        # the bucket's own cap holds them together
        o = _kept_origins(model, S)
        ow = o[0] if len(o) == 1 else ""
        if S == "ST" and P.steelhead_water:
            name, detail = "Steelhead", " ".join(x for x in (ow, "rainbow trout over 50 cm") if x)
        else:
            name = None
            detail = ", ".join(x for x in (ow, _size_words(lo, hi)) if x)
        # ROUND11 A1: a size cap is never on a kind row — the bucket's note says it once (below)
        notes: List[str] = []
        if b["why"] == "rule" and not unlimited:
            if b not in lift_bucket.values() and share is not None and S in share["of"]:
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
        k["species"].append(S)
    for b in buckets:
        for k in b["kinds"]:
            if k["name"] is None:
                k["name"] = common.cap(_names(k["species"]))
            if not one_origin:
                continue
            # the origin line says it once for every kind
            k["detail"] = re.sub(r"^(hatchery|wild), ", "", k["detail"])
    buckets = [b for b in buckets if b["kinds"]]
    for b in buckets:
        b["notes"] = []
    # ROUND11 A1 (owner): a size cap the kinds share is said ONCE, as a bucket note — "Only 1 of these
    # may be over 50 cm, not counting steelhead" — where its kinds sit together; a cap spanning buckets
    # (Chilliwack Lake: its own Dolly Varden 1 and Region 2's lake trout) on the first bucket it touches,
    # naming the day's fish. A cap is said where it binds: a kind keeps more than it allows, or a kind
    # wholly over the size shares it with another kind (the old row notes' "only" / "uses your").
    covered: set = set()
    for sc in sorted(sizes, key=lambda s: (s["take"], s["over_cm"])):
        cm = sc["over_cm"]
        over = [S for S in kinds if S in sc["of"] and info[S]["hi"] > cm]
        binds = []
        for S in over:
            b, n, lo = where_of[S], info[S]["n"], info[S]["lo"]
            cap_b = b["cap"] if b["cap"] is not None else INF
            shared = any(where_of[x] is not b for x in over if x != S)
            if sc["take"] < n or (lo >= cm and len(sc["of"]) > 1 and (sc["take"] < cap_b or shared)):
                binds.append(S)
        if not [S for S in binds if S not in covered]:
            continue
        covered |= set(over)
        holding = [b for b in buckets if any(where_of[S] is b for S in over)]
        first = holding[0]
        pool = first["kinds"] if len(holding) == 1 else [k for b in buckets for k in b["kinds"]]
        aside = [S for k in pool for S in k["species"] if S not in sc["of"] and info[S]["hi"] > cm]
        n_w = "None" if not sc["take"] else f"Only {_js(sc['take'])}"
        if aside and len(over) <= len(aside):
            # a cap on a few of the kinds (Region 4's "rainbow and cutthroat trout, no more than 1
            # over 50 cm"): name them — and claim nothing of the rest (another region's cap may hold
            # them on a boundary water)
            says = f"{n_w} {_names(over, ' or ')} may be over {_js(cm)} cm"
            aside = []
        else:
            who = "these" if len(holding) == 1 else f"the day’s {what}"
            says = f"{n_w} of {who} may be over {_js(cm)} cm"
            if aside:
                says += f", not counting {_names(aside, ' or ')}"
        first["notes"].append({"says": says, "n": sc["take"], "over_cm": cm, "kinds": over,
                               "not_counting": aside})
        states.append({"r": sc["r"], "line": f"bucket {first['title'] or 'the card'} note"})
    if len(buckets) == 1 and buckets[0]["why"] == "total":
        buckets[0]["title"] = None

    # -- the worked count (RULINGS Q23b) -----------------------------------------------------------
    worked: List[dict] = []
    if share is not None and lifts and not unlimited:
        parts = [e for L in lifts for e in L["each"]]              # every lifting rule (W1)
        main = max(parts, key=lambda L: (L["n"] or 0))
        q = main["n"] or 0
        S0 = main["kinds"][0]
        plural = SHORT_PLURAL.get(S0) or _names(main["kinds"])
        other_kind = "lake" if share["water"] == "stream" else "stream"
        for k in range(q, share["take"] - 1, -1):
            if k <= 0:
                break
            left = total - k
            # RULINGS Q23b (refined 2026-10-12): on its own water the lifted kind does not use the share
            # for the other kinds — here: the lifted kinds' rest, and the share's own number
            here = min(left, share["take"] + sum(max(0, (L["n"] or 0) - (k if L is main else 0))
                                                 for L in parts))
            if left == 0:
                a = f"that’s your {what} for the day, everywhere."
            elif k == share["take"]:
                a = (f"no more {what} from any other {share['water']} today; up to {_js(left)} more "
                     f"from here or from {other_kind}s.")
            elif left == 1:
                a = f"1 more: any kind from here, or one from a {other_kind}."
            else:
                a = f"{_js(left)} more: any kind from here, or from {other_kind}s."
            worked.append({"kept": k, "q": f"Keep {k} {plural} here", "a": a, "species": [S0],
                           "origin": P.by_key[main["by"]].f.get("origin") or "hatchery",
                           "share": share["r"], "here": here, "other": 0, "away": left})
    return {"headline": head, "origin": origin, "lifts": lifts, "total_line": total_line,
            "buckets": buckets, "worked": worked, "states": states}
