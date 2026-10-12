"""THE QUOTA CARD (answers 2.8, ROUND9 A1-A3) — water-first, each fact once.

A group total (`rows.group_of`: the pool several kinds share, its sub-caps, what a water's own rule lifts
out of them) is said as the angler reads a water:

  headline    the number from THIS water with no lift: on a Region 2 stream "2 a day from this stream"
              (the stream share holding every kind), on a lake the total ("4 a day");
  origin      "Hatchery only — release wild trout and char." where every kind keeps only one origin;
  lifts       each water's rule lifting kinds out of the share, said positively with whose rule it is:
              "On this river, hatchery rainbow trout 50 cm or less: up to 4 a day — this river’s rule
              replaces Region 2’s 2-from-streams limit for them." (`by`: the page cites its Book page);
  total_line  under a share, the region's total: "No more than 4 trout and char a day across Region 2,
              lakes included.";
  buckets     the kinds grouped under the cap they draw from — "Share the 2 from streams" (steelhead;
              cutthroat, brook and brown; Dolly Varden and lake trout), the lifting rule ("This river’s
              rule": rainbow trout 4), or the total — each kind with its number, origin and size, its
              notes (over 50 cm said HERE, once: "only 1 over 50 cm", "uses your 1 over 50 cm"), its
              duties (10 a year, record it, stop after 2) and its SPECIES (one picture each, P5);
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
        return f"On this {n}", f"this {n}’s rule"
    if b == "bc":
        return "In B.C.", "the B.C. rule"
    return (f"In {name}", f"{name}’s rule") if name else ("In this area", "this area’s rule")


def _kept_origins(model: dict, S: str) -> List[str]:
    from pipeline.deliver.answers.rows import _keepish
    R_ = model["R"].get(S) or {}
    return [o for o, k in (("hatchery", "H"), ("wild", "W"))
            if R_.get(k) and _keepish(R_[k]["status"])]


def _item_of(rows_out: List[dict], home: int, S: str) -> Optional[dict]:
    order = [rows_out[home]] + [r for i, r in enumerate(rows_out) if i != home]
    for r in order:
        for it in r.get("items") or []:
            if S in it["members"]:
                return it
    return None


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
    from pipeline.deliver.answers.rows import _lift_words, written_name_of, keep_range
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

    # -- headline: this water's number with no lift ------------------------------------------------
    if unlimited:
        head = {"n": None, "unit": f"no daily limit on {what}", "says": f"No daily limit on {what}"}
    elif share is not None:
        unit = f"a day from this {share['water']}"
        head = {"n": share["take"], "unit": unit, "says": f"{_js(share['take'])} {unit}"}
        states.append({"r": share["r"], "line": "headline"})
    else:
        head = {"n": total, "unit": "a day", "says": f"{_js(total)} a day"}
        states.append({"r": grp["pool"], "line": "headline"})

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
    lifts: List[dict] = []
    lift_cap: Dict[str, float] = {}
    for L in (share or {}).get("lifted", []):
        R_ = P.by_key[L["by"]]
        own = R_.f.get("take") if isinstance(R_.f.get("take"), (int, float)) \
            and not R_.f.get("unlimited") else None
        n = min(own, total) if own is not None and not unlimited else (total if not unlimited else None)
        lift_cap[L["by"]] = n
        where, whose = _whose(P, R_)
        fish = _lift_words(P, L["by"], L["members"])
        allows = f"up to {_js(n)} a day" if n is not None else "any number"
        lifts.append({"says": f"{where}, {fish}: {allows} — {whose} replaces {region}’s "
                              f"{_js(share['take'])}-from-{share['water']}s limit for them.",
                      "by": L["by"], "kinds": list(L["members"]), "n": n})
        states.append({"r": L["by"], "line": "lift"})
    total_line = None
    if share is not None and not unlimited:
        other = "lakes" if share["water"] == "stream" else "streams"
        scope = T.x.get("entry_name") or "the region"
        total_line = f"No more than {_js(total)} {what} a day across {scope}, {other} included."
        states.append({"r": grp["pool"], "line": "total"})

    # -- buckets and kind rows -------------------------------------------------------------------
    by_lift = {S: L["by"] for L in (share or {}).get("lifted", []) for S in L["members"]}
    buckets: List[dict] = []
    if share is not None:
        buckets.append({"title": f"Share the {_js(share['take'])} from {share['water']}s",
                        "cap": share["take"], "by": share["r"], "why": "share", "kinds": []})
    # one bucket per lifting entry (Duncan: its rainbow and its Dolly Varden rules are one row)
    lift_bucket: Dict[str, dict] = {}
    for L in (share or {}).get("lifted", []):
        R_ = P.by_key[L["by"]]
        b = lift_bucket.get(R_.entry_id)
        if b is None:
            b = {"title": common.cap(_whose(P, R_)[1]), "cap": 0, "by": L["by"], "why": "rule",
                 "kinds": []}
            lift_bucket[R_.entry_id] = b
            buckets.append(b)
        c = lift_cap[L["by"]]
        b["cap"] = None if c is None or b["cap"] is None else b["cap"] + c
    for b in lift_bucket.values():
        if b["cap"] is not None and not unlimited:
            b["cap"] = min(b["cap"], total)
    rest = None
    if share is None or any(S not in share["of"] and S not in by_lift for S in kinds):
        rest = {"title": None if share is None else f"The rest of the {_js(total)} a day",
                "cap": None if unlimited else total, "by": grp["pool"], "why": "total", "kinds": []}
        buckets.append(rest)

    def bucket_of(S):
        if S in by_lift:
            return lift_bucket[P.by_key[by_lift[S]].entry_id]
        if share is not None and S in share["of"]:
            return buckets[0]
        return rest

    rows_by: Dict[tuple, dict] = {}
    for S in kinds:
        it = _item_of(rows_out, home, S)
        n = _item_n(it, total) if it is not None else None
        if n is None:
            n = INF if unlimited else total
        b = bucket_of(S)
        kr = keep_range(P, model, [S]) or (0, INF)
        lo, hi = kr
        o = _kept_origins(model, S)
        ow = o[0] if len(o) == 1 else ""
        if S == "ST" and P.steelhead_water:
            name, detail = "Steelhead", " ".join(x for x in (ow, "rainbow trout over 50 cm") if x)
        else:
            name = None
            detail = ", ".join(x for x in (ow, _size_words(lo, hi)) if x)
        notes: List[str] = []
        for sc in sorted(sizes, key=lambda s: s["take"]):
            if S not in sc["of"]:
                continue
            if sc["take"] < n:
                notes.append(f"only {_js(sc['take'])} over {_js(sc['over_cm'])} cm")
                break
            cap_b = b["cap"] if b["cap"] is not None else INF
            if lo >= sc["over_cm"] and sc["take"] < cap_b and len(sc["of"]) > 1:
                notes.append(f"uses your {_js(sc['take'])} over {_js(sc['over_cm'])} cm")
                break
        if b is not None and b["why"] == "rule" and not unlimited:
            notes.append(f"counts toward the {_js(total)} a day")
        duties = _duties(P, model, S, n)
        key = (id(b), n, name, detail, tuple(notes), tuple(duties))
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
    # a cap stated on a kind row once; the other rows it holds say they share it
    said: Dict[str, str] = {}
    for b in buckets:
        for k in b["kinds"]:
            for i, t in enumerate(k["notes"]):
                m = re.match(r"^only (\d+) over (\d+) cm$", t)
                if not m:
                    continue
                sc = next(s for s in sizes if _js(s["take"]) == m.group(1) and set(k["species"]) <= set(s["of"])
                          and _js(s["over_cm"]) == m.group(2))
                if sc["r"] in said:
                    k["notes"][i] = f"shares the {m.group(1)} over {m.group(2)} cm above"
                else:
                    said[sc["r"]] = t
                    states.append({"r": sc["r"], "line": f"kind {'/'.join(k['species'])}"})
    if len(buckets) == 1 and buckets[0]["why"] == "total":
        buckets[0]["title"] = None

    # -- the worked count (RULINGS Q23b) -----------------------------------------------------------
    worked: List[dict] = []
    if share is not None and lifts and not unlimited:
        main = max(lifts, key=lambda L: (L["n"] or 0))
        q = main["n"] or 0
        S0 = main["kinds"][0]
        plural = SHORT_PLURAL.get(S0) or _names(main["kinds"])
        # every lifted kind may fill what is left here (Duncan: rainbows or Dolly Varden)
        plurals = common.join(list(dict.fromkeys(
            [plural] + [SHORT_PLURAL.get(L["kinds"][0]) or _names(L["kinds"]) for L in lifts])), " or ")
        ones = common.join(list(dict.fromkeys(
            " ".join(x for x in (P.by_key[L["by"]].f.get("origin") or "", _names(L["kinds"])) if x)
            for L in [main] + lifts)), " or ")
        other_kind = "lake" if share["water"] == "stream" else "stream"
        for k in range(q, share["take"] - 1, -1):
            if k <= 0:
                break
            left = total - k
            here = min(left, sum(max(0, (L["n"] or 0) - (k if L is main else 0)) for L in lifts))
            if left == 0:
                a = f"that’s your {what} for the day, everywhere."
            elif k == share["take"]:
                a = (f"you’re done on every other {share['water']}; up to {_js(left)} more from here "
                     f"({plurals}) or from {other_kind}s.")
            elif left == 1:
                a = f"1 more: another {ones} here, or a fish from a {other_kind}."
            else:
                a = f"{_js(left)} more: {plurals} here, or fish from {other_kind}s."
            worked.append({"kept": k, "q": f"Keep {k} {plural} here", "a": a, "species": [S0],
                           "origin": P.by_key[main["by"]].f.get("origin") or "hatchery",
                           "share": share["r"], "here": here, "other": 0, "away": left})
    return {"headline": head, "origin": origin, "lifts": lifts, "total_line": total_line,
            "buckets": buckets, "worked": worked, "states": states}
