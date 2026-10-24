"""Consumer 5.6-5.8: a row's conditions (`row_conds`), its fish (`species_items`), each kind's keep range
and bands, the origin lines, and the real daily limit when every kind has a smaller cap (`eff_cap`)."""
from __future__ import annotations

import json
from typing import Dict, List, Optional, Sequence, Tuple
from pipeline.deliver.answers.common import AnswersError
from pipeline.deliver.bundle import read
from pipeline.deliver.answers.rows.rule import INF, R, _bands, _covers, _ge, _js, _keep_range, _keepish
from pipeline.deliver.answers.rows.evaluate import main_res


def _who_n(l: dict, row: dict) -> Optional[List[str]]:
    """The fish a line is about, or None when it is about every fish of the row. Decision F3:
    the SETS are compared (the page compares counts, so a go-back line for as many other fish as
    the row has was read as everyone's and dropped)."""
    if not l.get("members") or l.get("general") or set(l["members"]) == set(row["members"]):
        return None
    return list(l["members"])


def _steel_carve(P: "Part", model: dict, l: dict, row: dict) -> bool:
    """Decision F1b: a cap written for every fish of the row but rainbow trout, where the
    rainbow's own answer leaves it out only because a rainbow over 50 cm is a steelhead here
    (the `steel` line), is general — with the rainbow carved out."""
    w = l.get("members")
    if not w or l.get("general") or not P.steelhead_water or "RB" in w:
        return False
    if set(row["members"]) - set(w) != {"RB"}:
        return False
    m = main_res(model["R"]["RB"]["H"], model["R"]["RB"]["W"])
    return bool(m) and any(x["t"] == "steel" for x in m["lines"]) and l["a"] >= 50


def _not_counted(P: "Part", model: dict, row: dict, r: R) -> List[str]:
    """Decision F12 (user test 2026-10-08; was the page's `exemptOf`): the fish of a row a general
    size cap does NOT count — fish its rule covers that are kept here but whose answers give the
    cap no role and no line (a lift took them out of it). A rainbow on a steelhead water is never
    one where the cap counts steelhead: a rainbow over 50 cm IS a steelhead there (p.80), so it
    is counted, as a steelhead (Region 6's "1 over 50 cm (quota includes hatchery steelhead)",
    p.49). Where the cap leaves steelhead out (Region 2's "1 over 50 cm" beside its own "2
    hatchery steelhead over 50 cm", p.21) the carve-out names them (`except: [ST]`)."""
    R_ = model["R"]
    seen: List[str] = []
    for S in list(row["allMembers"]) + [S for g in row["groups"] for S in g["members"]]:
        if S not in seen:
            seen.append(S)
    out = []
    for S in seen:
        if not _covers(r, S):
            continue
        rs = [x for x in ((R_.get(S) or {}).get("H"), (R_.get(S) or {}).get("W"))
              if x and _keepish(x["status"])]
        if not rs:
            continue
        if not all((x["roles"].get(r.key) or {}).get("role") in ("lifted", None)
                   and not any(l["r"].key == r.key for l in x["lines"]) for x in rs):
            continue
        if S == "RB" and P.steelhead_water and _covers(r, "ST") \
                and any(l["t"] == "steel" for x in rs for l in x["lines"]):
            continue
        out.append(S)
    return out


class Raised(R):
    """Decision F17 (owner ruling 2026-10-21, the Kitimat): a size clause of a zone's total that a water's
    quota of steelhead LIFTED for them, RAISED to that quota for every fish over the size — the tally
    `raises` reads. Its key is the lifting rule's (the number is that rule's; it is said, and encoded,
    as that rule), its fields the clause's with the quota's take, its entry the water's (a water's own
    number: it counts the fish kept elsewhere today, rows F9, and never stops one kept elsewhere).
    `lifted` the (fish, origin) the clause was lifted for — they count toward the tally here."""
    __slots__ = ("clause", "lifter", "lifted")


def raised(c: R, L: R, lifted, take) -> Raised:
    """The tally of size clause `c` that `L`'s lift for `lifted` raises to `take` (`raises`)."""
    fish = {S for S, _o in lifted}
    x = {**c.x, "take": take, "entry": L.entry_id, "entry_id": L.entry_id,
         "entry_name": L.x.get("entry_name"),
         "species_except": [s for s in c.x.get("species_except") or [] if s not in fish]}
    out = Raised(L.key, x, L.via)
    out.clause, out.lifter, out.lifted = c, L, frozenset(lifted)
    return out


def raises(P: "Part") -> List[Raised]:
    """Decision F17 (owner ruling 2026-10-21 on the Kitimat; RULINGS Q16's Region 2 shape). A zone size clause
    over X cm with a number K ("1 over 50 cm", Region 6's trout_char_quota.r2, "quota includes hatchery
    steelhead") that the reader says a water's own steelhead quota LIFTED for steelhead (the Kitimat's
    r5 "Hatchery steelhead (>50 cm) daily quota = 2"), where every steelhead that quota keeps is over X and
    its number M is larger than K, is not left behind for the steelhead: it is "+1 for steelhead" (the
    owner: "look at the prototype vedder it is similar"). The other fish stay at K over X; every fish over X,
    the steelhead included, counts toward ONE tally of M — the Vedder's "1 over 50 cm (2 hatchery steelhead
    over 50 cm allowed)", which Region 2 prints as two rules (r2 not steelhead, r2b every fish). Keep 2
    hatchery steelhead and no other trout over 50 cm; keep 1 and 1 other may be. Read off the rules and the
    reader's verdicts (`ladder` [state, partly, reason, by]); one per (clause, lifting rule)."""
    st_cm = read._steelhead_min_cm()
    found: Dict[Tuple[str, str], list] = {}
    for c in P.cands:
        if c.k not in ("sizecap", "subcap") or not _covers(c, "ST"):
            continue
        band = next(((a, b, t) for a, b, t in _bands(c) if t and a and b == INF), None)
        if band is None:
            continue
        X, K = band[0], band[2]
        for o, v in sorted((P.ladder.get("ST") or {}).items()):
            got = (v or {}).get(c.key)
            if not got or got[0] != "lifted" or got[3] not in P.by_key:
                continue
            L = P.by_key[got[3]]
            M = L.f.get("take")
            lo = _keep_range(L)[0]
            if not isinstance(M, (int, float)) or L.f.get("unlimited") or M <= K \
                    or not (lo >= X or (P.steelhead_water and st_cm >= X)):
                continue
            found.setdefault((c.key, L.key), [c, L, M, []])[3].append(("ST", o))
    return [raised(c, L, lifted, M) for c, L, M, lifted in found.values()]


def _raise_of(P: "Part", c: R) -> List[Raised]:
    return [x for x in raises(P) if x.clause.key == c.key]


def _limit_take(l: dict):
    return l.get("take") if l.get("take") is not None else l["r"].f.get("take")


def row_conds(P: "Part", model: dict, row: dict) -> dict:
    """The page's `rowConds`, structurally: every condition on keeping from the row, one entry
    each ({c, who, sub, take, a, b, o, r, status}), in the page's order. `n` is the row's number,
    `scope` the pool's reach (water | region | bc)."""
    allf = row["everyone"] + [l for g in row["groups"] for l in g["facts"]]
    n = row["daily"]
    limits0 = [l for l in allf if l["t"] in ("subcap", "cap", "outercap", "outersize", "origin2")]

    def big(l):
        v = l["r"].f.get("take") if l["t"] == "outercap" else _limit_take(l)
        return _ge(v, n)
    limits = [l for l in limits0 if l["t"] == "origin2" or
              (not (big(l) and l["t"] != "cap") and not (l["t"] == "outersize" and _ge(l.get("take"), n)))]
    R = model["R"]

    def moot_cap(l):
        if l["t"] not in ("cap", "outersize") or l["b"] != INF:
            return False
        for S in (l.get("members") or row["members"]):
            for o in ("H", "W"):
                r = (R.get(S) or {}).get(o)
                if not (not r or not _keepish(r["status"]) or r["daily"] <= l["take"]
                        or any(x["t"] == "rel" and x["a"] <= l["a"] and x["b"] == INF
                               for x in r["lines"])):
                    return False
        return True
    for i in range(len(limits) - 1, -1, -1):
        L = limits[i]
        if not L.get("carveOf") and not any(x.get("carveOf") is L for x in limits) and moot_cap(L):
            limits.pop(i)
    if row["narrow"] is not None and row["narrow"].f.get("water") == P.kind \
            and row["narrow"].f.get("take") < n:
        limits.append({"t": "streamcap", "r": row["narrow"]})
    carves = [l for l in limits if l.get("carveOf")]
    back = sorted([l for l in allf if l["t"] in ("origin", "rel", "exc")],
                  key=lambda x: ["origin", "exc", "rel"].index(x["t"]))
    conds: List[dict] = []
    everyone_back = [l for l in back if not _who_n(l, row)]
    by_set: Dict[str, list] = {}
    for l in back:
        w = _who_n(l, row)
        if w:
            by_set.setdefault(",".join(sorted(w)), []).append(l)

    def goes_back(S):
        m = main_res((R.get(S) or {}).get("H"), (R.get(S) or {}).get("W"))
        return bool(m) and not _keepish(m["status"])
    for l in limits:
        if l["t"] == "subcap" and _who_n(l, row):
            keepers = [S for S in l["members"] if not goes_back(S)]
            if not keepers:
                continue
            by_set.setdefault(",".join(sorted(keepers)), []).append(l)
    for l in everyone_back:
        if l["t"] == "origin":
            conds.append({"c": "origin", "o": l["o"], "r": l["r"]})
        elif l["t"] == "rel":
            conds.append({"c": "size", "a": l["a"], "b": l["b"], "r": l["r"]})
    for k, L in by_set.items():
        who = k.split(",")
        x = next((l for l in L if l["t"] == "exc"), None)
        if x:
            conds.append({"c": "back", "who": who, "status": x["status"], "r": x["r"]})
            continue
        parts = []
        org = next((l for l in L if l["t"] == "origin"), None)
        if org:
            parts.append({"origin": "hatchery" if org["o"] == "wild" else "wild"})
        for l in L:
            if l["t"] == "rel":
                parts.append({"a": l["a"], "b": l["b"]})
        sc = next((l for l in L if l["t"] == "subcap"), None)
        if parts or sc:
            conds.append({"c": "group", "who": who, "keep": parts,
                          "sub": sc["r"].f.get("take") if sc else None,
                          "r": sc["r"] if sc else None})
    for l in limits:
        if l.get("carveOf") or (l["t"] == "subcap" and _who_n(l, row)):
            continue
        c2 = [x for x in carves if x.get("carveOf") is l]
        if l["t"] == "cap" and c2:
            cv = [S for x in c2 for S in (_who_n(x, row) or [])]
            conds.append({"c": "cap", "a": l["a"], "b": l["b"], "take": l["take"], "except": cv,
                          "r": None, "general": l["a"] > 0 and l["b"] == INF, "sub": l["take"]})
            for x in c2:
                conds.append({"c": "cap", "who": _who_n(x, row), "a": x["a"], "b": x["b"],
                              "take": x["take"], "sub": x["take"], "r": x["r"]})
            continue
        w = _who_n(l, row)
        e = {"c": l["t"], "r": l["r"]}
        if l["t"] == "streamcap":
            e["sub"] = None
        elif l["t"] == "outercap":
            e["sub"] = l["r"].f.get("take")
        else:
            e["sub"] = _limit_take(l)
        if l["t"] in ("cap", "outersize"):
            e.update(a=l["a"], b=l["b"], take=l["take"])
        if l["t"] == "cap":
            if w is not None and _steel_carve(P, model, l, row):     # decision F1b (as amended)
                w = None
            e["of"] = w                         # the fish the cap is written for (page: "(…)")
            e["general"] = w is None and l["a"] > 0 and l["b"] == INF
        raised_by = []
        if l["t"] in ("cap", "outersize") and w is None and not e.get("except"):
            ex = _not_counted(P, model, row, l["r"])       # decision F12
            if ex:
                e["except"] = ex
                # decision F17: the steelhead a water's quota took out of the cap still count toward the
                # tally it raises the cap to ("+1 for steelhead", the Kitimat)
                raised_by = [x for x in _raise_of(P, l["r"]) if {S for S, _o in x.lifted} <= set(ex)]
        if l["t"] == "origin2":
            e.update(o=l["o"], daily=l["daily"], min=l["min"], max=l["max"])
            if w is not None:                   # decision F2: whose other origin it is
                e["who"] = sorted(w)
        if l["t"] == "subcap":
            e["members"] = list(l["members"])
        conds.append(e)
        for x in raised_by:
            conds.append({"c": "cap", "r": x.lifter, "sub": x.f["take"], "a": l["a"], "b": l["b"],
                          "take": x.f["take"], "of": None, "general": e.get("general")})
    return {"conds": conds, "n": n, "hw": any(l["t"] == "origin" for l in back),
            "extra": [l for l in allf if l["t"] in ("annual", "possession_cap", "outer")]}


def _short(l: dict) -> bool:
    """Whether the page's `fact(l).short` is a non-empty text (it lists the line under a kind)."""
    return l["t"] not in ("partly", "tnote", "outercap", "outersize") and l["t"] in (
        "origin", "subcap", "cap", "rel", "annual", "possession_cap", "record", "duty", "exc",
        "xref", "outer", "also", "origin2", "orphan", "caution", "steel")


_COVER = ("cap", "rel", "subcap", "origin", "outercap", "outersize", "tnote", "partly")


def keep_range(P: "Part", model: dict, members: Sequence[str]) -> Optional[Tuple[float, float]]:
    lo, hi, anyk = 0, INF, False
    R = model["R"]
    for S in members:
        m = main_res((R.get(S) or {}).get("H"), (R.get(S) or {}).get("W"))
        if not m or not _keepish(m["status"]):
            continue
        anyk = True
        for l in m["lines"]:
            if l["t"] == "rel":
                if l["a"] == 0:
                    lo = max(lo, l["b"])
                if l["b"] == INF:
                    hi = min(hi, l["a"])
        if S == "ST" and P.steelhead_water and lo < 50:
            lo = 50
        # decision F5c: on a steelhead water a rainbow over 50 cm is a steelhead (as origin_lines)
        if S == "RB" and P.steelhead_water and any(l["t"] == "steel" for l in m["lines"]):
            hi = min(hi, 50)
    return (lo, hi) if anyk else None


def fish_bands(P: "Part", model: dict, S: str, facts: List[dict], sub: Optional[dict],
               row: dict, rc: dict) -> Optional[List[dict]]:
    """One fish's keep range split into bands with their own number (the page's `quotaLines`,
    for ONE kind — decision F1): the row's number, less the fish's sub-limit, a stream share
    holding here and every sub-limit covering it; each band capped by the fish's own size caps
    and the row's general ones (the zone's outer size cap too: F5b); a band inside a slot ('none
    between X and Y cm') keeps 0 (F5a)."""
    r = keep_range(P, model, [S])
    if not r:
        return None
    R = model["R"]
    n = rc["n"]
    base = min(n, sub["sub"] if sub else n)
    nw = row["narrow"]
    if nw is not None and nw.f.get("water") == P.kind and P.in_dates(nw) and _covers(nw, S) \
            and S in R and any(((R[S][o] or {}).get("roles") or {}).get(nw.key, {}).get("role")
                               != "lifted" for o in ("H", "W")):
        base = min(base, nw.f.get("take"))
    for l in facts + row["everyone"]:
        # an ORPHAN is a count clause of another table that still speaks for the fish (its own pool
        # displaced): Region 5's "1 Dolly Varden" under Polley Lake's "trout 8" (RULINGS Q2: the
        # clause naming the fish beats the lake's group number) — it caps the fish like a sub-cap
        if l["t"] in ("outercap", "subcap", "orphan") and l["r"].f.get("take") is not None \
                and (S in l["members"] if l.get("members") else True) and _covers(l["r"], S) \
                and (l["t"] != "orphan" or (l["r"].f.get("water") in (None, P.kind)
                                            and P.in_dates(l["r"]))):
            base = min(base, l["r"].f["take"])
    capt = ("cap", "outersize")
    own = [l for l in facts if l["t"] in capt and l.get("take") is not None]
    gen = [l for l in row["everyone"] if l["t"] in capt and l.get("take") is not None
           and not any(o["a"] == l["a"] and o["b"] == l["b"] for o in own)]
    caps = own + gen
    m = main_res(R[S]["H"], R[S]["W"])
    slots = [(l["a"], l["b"]) for l in m["lines"] if l["t"] == "rel" and l["a"] > 0 and l["b"] < INF]
    pts = {r[0], r[1]}
    for a, b in [(c["a"], c["b"]) for c in caps] + slots:
        for v in (a, b):
            if v is not None and r[0] < v < r[1]:
                pts.add(v)
    Pp = sorted(pts)
    out: List[dict] = []
    for i in range(len(Pp) - 1):
        a, b = Pp[i], Pp[i + 1]
        mid = a + 1 if b == INF else (a + b) / 2
        mx = min([base] + [c["take"] for c in caps
                           if mid >= (c["a"] or 0) and mid <= (INF if c["b"] is None else c["b"])])
        if any(x < mid < y for x, y in slots):
            mx = 0
        if out and out[-1]["mx"] == mx:
            out[-1]["b"] = b
        else:
            out.append({"a": a, "b": b, "mx": mx})
    return out


def _against(row: dict, model: dict, S: str):
    """The number a fish with its own row counts toward on this row (decision F4): the row's
    outer total where the fish's own answer lifts (or replaces) the row's narrowing clause, else
    the row's number."""
    nw = row["narrow"]
    R = model["R"].get(S) or {}
    m = main_res(R.get("H"), R.get("W"))
    if nw is not None and m and (m["roles"].get(nw.key) or {}).get("role") in ("lifted",
                                                                                "replaced"):
        return INF if row["pool"].f.get("unlimited") else row["pool"].f.get("take")
    return row["daily"]


def species_items(P: "Part", model: dict, row: dict, rc: dict) -> List[dict]:
    """The fish of a keep row as the card lists them (the page's `speciesItems`), ONE ITEM PER
    KIND (decision F1): each fish's bands, sub-limit, go-back, own-row reference and the row's
    conditions about it are worked out for that fish alone; fish share an item when exactly
    `sig` agrees (has lines, bands, sub-limit, go-back, own-row ref and `against`, condition
    indexes) — other facts do not split items (they name their own fish in the row's facts).
    (The page builds items from the fact groups, so one fish could sit in two items and an item
    showed one range for fish with different ones.)"""
    got = rc.get("_items")
    if got is not None:
        return got
    conds = rc["conds"]
    gfacts = [l for g in row["groups"] for l in g["facts"]]
    universe: List[str] = []
    for S in (list(row["allMembers"]) + [S for l in gfacts for S in l["members"]]
              + [S for c in conds if c.get("who") for S in c["who"]]):
        if S not in universe:
            universe.append(S)
    R = model["R"]
    by_sig: Dict[str, dict] = {}
    for S in universe:
        facts = [l for l in gfacts if S in l["members"]]
        ci = [i for i, c in enumerate(conds) if c.get("who") and S in c["who"]]
        cs = [conds[i] for i in ci]
        back = next((c for c in cs if c["c"] == "back"), None)
        sub = next((c for c in cs if c["c"] == "group" and c.get("sub") is not None), None)
        xref = next((l for l in facts if l["t"] == "xref"), None)
        others = [l for l in facts if (not cs or l["t"] not in _COVER) and _short(l)]
        has_lines = bool(cs) or (not back and bool(others)) or bool(sub)
        if not has_lines and not (S in R and _keepish(
                (main_res(R[S]["H"], R[S]["W"]) or {}).get("status"))):
            continue
        q = None if (row["kind"] != "keep" or back or xref) else \
            fish_bands(P, model, S, facts, sub, row, rc)
        against = _against(row, model, S) if xref else None
        sig = json.dumps([has_lines, None if q is None else [[_js(x["a"]), _js(x["b"]), _js(x["mx"])]
                                                             for x in q],
                          sub["sub"] if sub else None,
                          [back["status"], back["r"].key if back["r"] else None] if back else None,
                          [xref["r"].key, _js(xref["daily"]), _js(against)] if xref else None, ci])
        it = by_sig.get(sig)
        if it is None:
            it = by_sig[sig] = {"members": [], "cs": cs, "conds": ci, "facts": [], "sub": sub,
                                "back": back, "xref": xref, "q": q, "against": against,
                                "has_lines": has_lines}
        it["members"].append(S)
        for l in facts:
            if not any(l is x for x in it["facts"]):
                it["facts"].append(l)
    # the page's order: its items came from the fact groups (in order), then the conditions
    # naming other fish; an item stands where its first fish first stood
    gpos = {}
    for gi, g in enumerate(row["groups"]):
        for S in g["members"]:
            gpos.setdefault(S, gi)
    for ci_, c in enumerate(c for c in conds if c.get("who")):
        for S in c["who"]:
            gpos.setdefault(S, len(row["groups"]) + ci_)
    items = sorted(by_sig.values(), key=lambda it: min(gpos.get(S, INF) for S in it["members"]))
    lined = sorted([it for it in items if it["has_lines"]], key=lambda it: -len(it["members"]))
    sti = next((i for i, it in enumerate(lined) if it["members"] == ["ST"]), -1)
    rbi = next((i for i, it in enumerate(lined) if "RB" in it["members"]), -1)
    if sti >= 0 and rbi >= 0 and sti != rbi + 1:
        st = lined.pop(sti)
        j = next(i for i, it in enumerate(lined) if "RB" in it["members"])
        lined.insert(j + 1, st)
    out = lined + [it for it in items if not it["has_lines"]]
    rc["_items"] = out
    return out


def quota_lines(P: "Part", model: dict, it: dict, row: dict, rc: dict) -> Optional[List[dict]]:
    """An item's bands (decision F1: every fish of an item has the same)."""
    return it["q"]


def origin_lines(P: "Part", model: dict, S: str) -> List[dict]:
    R = model["R"].get(S)
    if not R:
        return []

    def one(r, o):
        if not r:
            return None
        if not _keepish(r["status"]):
            return {"o": o, "rel": True}
        lo = max([0] + [l["b"] for l in r["lines"] if l["t"] == "rel" and l["a"] == 0])
        his = [l["a"] for l in r["lines"] if l["t"] == "rel" and l["b"] == INF]
        if S == "ST" and P.steelhead_water and lo < 50:
            lo = 50
        if S == "RB" and P.steelhead_water and any(l["t"] == "steel" for l in r["lines"]):
            his.append(50)
        return {"o": o, "n": r["daily"], "lo": lo, "hi": min(his) if his else INF}
    return [x for x in (one(R["H"], "hatchery"), one(R["W"], "wild")) if x]


def _shared_caps(capped: List[dict], general: List[Tuple[float, float]], steel: bool):
    """Decision F5d (was R8): every general 'only K over X cm' cap of the row, not only the
    first. The most the capped kinds can give, nested caps worked from the largest size down:
    the kinds from X up give at most K. Returns (the reduction, the caps that bind)."""
    elig = [x for x in capped if not ("ST" in x["it"]["members"] and steel)]
    caps: Dict[float, float] = {}
    for X, K in general:
        caps[X] = min(K, caps.get(X, INF))
    Xs = sorted(caps)
    if not Xs:
        return 0, []
    bound, binding = None, []
    for i in range(len(Xs) - 1, -1, -1):
        X, K = Xs[i], caps[Xs[i]]
        nxt = Xs[i + 1] if i + 1 < len(Xs) else INF
        seg = sum(x["c"] for x in elig if X <= x["lo"] < nxt)
        inner = seg + (bound or 0)
        if len([x for x in elig if x["lo"] >= X]) > 1 and inner > K:
            bound = K
            binding.append({"take": K, "over_cm": X})
        else:
            bound = inner
    over_all = sum(x["c"] for x in elig if x["lo"] >= Xs[0])
    return over_all - (bound or 0), sorted(binding, key=lambda c: c["over_cm"])


def eff_cap(P: "Part", model: dict, row: dict, rc: dict) -> Optional[dict]:
    """The real daily limit (5.7, the page's `effCap`): when every kind of the row has its own
    smaller cap, the real most is their sum, less what the shared 'only K over X cm' caps allow."""
    if row["kind"] != "keep" or not row["pool"]:
        return None
    n = row["daily"]
    items = species_items(P, model, row, rc)
    if not items:
        return None
    allm = [S for it in items for S in it["members"]]
    if len(set(allm)) != len(allm):
        raise AnswersError("rows: a fish sits in two items of one row (decision F1)")
    caps = []
    for it in items:
        q = it["q"]
        xref = it["xref"]
        if q:
            lo = q[0]["a"]
        elif xref:
            lo = min(min([INF] + [x.get("lo") or 0 for x in origin_lines(P, model, S)
                                  if not x.get("rel")]) for S in it["members"])
        else:
            lo = 0
        c = min(n, max(x["mx"] for x in q)) if q else (min(xref["daily"], it["against"]) if xref
                                                         else (0 if it.get("back") else n))
        caps.append({"it": it, "lo": lo, "c": c})
    capped = [x for x in caps if x["c"] < n and x["c"] > 0]
    open_ = [x for x in caps if x["c"] >= n]
    total = sum(x["c"] for x in capped)
    general = [(c["a"], c["take"]) for c in rc["conds"]
               if c["c"] == "cap" and not c.get("who") and c.get("general")]
    cut, shared = _shared_caps(capped, general, P.steelhead_water)
    # decision F11: a COUNT limit SHARED by several kinds ("1 trout from streams, Jul 1-Oct 31",
    # "3 Dolly Varden and lake trout combined") gives them that many together, never that many
    # each: the kinds it holds are worked as one unit (nested limits innermost first)
    units, shared_n = _shared_counts(P, model, row, capped, n)
    total = min(total - cut, sum(u["c"] for u in units))
    if not capped or (total >= n and not open_):
        return None
    out = {"n": n, "all": not open_, "sum": None if open_ else total, "capped_sum": total,
           "rb": any("RB" in x["it"]["members"] for x in capped), "shared_cap": shared or None,
           "capped": [S for x in capped for S in x["it"]["members"]],
           "open": [S for x in open_ for S in x["it"]["members"]]}
    if shared_n:
        out["shared_count"] = shared_n
    return out


def _under(model: dict, S: str, L: R) -> bool:
    """Does count limit `L` hold fish `S` on this reading: the fish's decided answer (the main
    origin's, as the card reads it) is narrowed by it or limited by it — never where a lift took
    the fish out of it."""
    R_ = model["R"].get(S) or {}
    m = main_res(R_.get("H"), R_.get("W"))
    if not m or not _keepish(m["status"]):
        return False
    role = (m["roles"].get(L.key) or {}).get("role")
    return m.get("narrow") is L or role in ("narrows", "limit")


def _shared_counts(P: "Part", model: dict, row: dict, capped: List[dict], n) -> Tuple[list, list]:
    """Decision F11 (user test 2026-10-08, Babine/Skeena): the units of a row's capped kinds under
    every COUNT limit (a take, no size band) that holds more than one of them: the row's narrowing
    clause (a stream or lake share) and every clause `within` the row's pool. A limit merges the
    units it wholly holds into one, worth at most its take; limits holding fewer kinds go first,
    so nested limits nest. Returns (units, the limits that bind [{take, members}])."""
    units = [{"members": list(x["it"]["members"]), "c": x["c"]} for x in capped]
    cands: List[R] = []
    if row.get("narrow") is not None:
        cands.append(row["narrow"])
    pool = row.get("pool")
    for r in P.cands:
        if pool is not None and r.f.get("within") == pool.rule_id and r.entry_id == pool.entry_id \
                and r not in cands:
            cands.append(r)
    held = []
    for L in cands:
        K = L.f.get("take")
        if not isinstance(K, (int, float)) or K <= 0 or K >= n or L.f.get("lengths") \
                or (L.f.get("period") or "daily") != "daily" or not P.in_dates(L):
            continue
        if L.f.get("water") and L.f["water"] != P.kind:
            continue
        ins = [u for u in units if u["members"] and all(_under(model, S, L) for S in u["members"])]
        if len(ins) > 1:
            held.append((len(ins), L, K))
    binding = []
    for _, L, K in sorted(held, key=lambda t: (t[0], t[1].key)):
        ins = [u for u in units if all(_under(model, S, L) for S in u["members"])]
        if len(ins) < 2 or sum(u["c"] for u in ins) <= K:
            continue
        merged = {"members": [S for u in ins for S in u["members"]], "c": K}
        units = [u for u in units if u not in ins] + [merged]
        binding.append({"take": K, "members": merged["members"]})
    return units, binding
