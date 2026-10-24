"""Consumer 5.1 / 5.3: the fish asked about (`species_at`) and the part's model — one row per shared limit,
the fish that go back, the cross-references (`build_model`, the page's `buildModel`)."""
from __future__ import annotations

import json
from typing import Dict, List, Optional, Tuple
from pipeline.deliver.bundle.rules import catch_and_release, closure_grade
from pipeline.deliver.answers.rows.rule import INF, _js, _keepish
from pipeline.deliver.answers.rows.evaluate import counts_toward, eval_sp, main_res


#: T127 (found by the Perry Creek card-gate fix): a pool the reader DISPLACED for a fish yields no "counts
#: toward this one" cross-reference — `counts_toward` decides every cross-reference. False only in the mutation pin
#: (test_answers_t127_displaced_xref.py): the St. Mary's "trout/char 1 on all tributaries" then lists Perry Creek's brook
#: trout again, though Perry Creek's own "brook trout 20" displaced it (reason own_row).
A_DISPLACED_POOL_IS_NO_CROSS_REFERENCE = True


def _icu_key(s: str):
    """`String.prototype.localeCompare` (ICU root) closely enough for fish names: letters by their
    case-folded value first, punctuation and spaces before letters, case last (lower first)."""
    prim = []
    for ch in s:
        if ch.isalpha():
            prim.append((3, ch.casefold()))
        elif ch.isdigit():
            prim.append((2, ch))
        elif ch.isspace():
            prim.append((0, " "))
        else:
            prim.append((1, ch))
    return (prim, [0 if ch.islower() else 1 for ch in s if ch.isalpha()])


def _fact_key(l: dict) -> str:
    t = l["t"]
    if t == "origin2":
        return f"o2{l['o']}{_js(l['daily'])}{_js(l['min'])}{_js(l['max'])}"
    if t == "tnote":
        return "tnote" + ("true" if l["only"] else "false")
    if t == "caution":
        return "caution" + str(l["says"])
    if t == "rel":
        return f"rel{_js(l['a'])}-{_js(l['b'])}"
    if t in ("annual", "possession_cap"):
        r = l["r"]
        return f"{'yr' if t == 'annual' else 'pc'}{_js(r.f.get('take'))}|{json.dumps(r.f.get('lengths') or '', separators=(',', ':'))}|{r.f.get('origin') or ''}"
    if t == "cap":
        return f"cap{_js(l['a'])}-{_js(l['b'])}-{_js(l['take'])}-{l['r'].key}"
    return t + l["r"].key


def _min_max(res: dict) -> Tuple[Optional[float], Optional[float]]:
    """An answer's keep range from its own release lines: (none under, none over) in cm."""
    mn = [l["b"] for l in res["lines"] if l["t"] == "rel" and l["a"] == 0]
    mx = [l["a"] for l in res["lines"] if l["t"] == "rel" and l["b"] == INF]
    return (max(mn) if mn else None, min(mx) if mx else None)


def _origin2_rule(other: dict, m: dict) -> "R":
    """The rule an origin2 line cites: the other origin's narrowing clause or winner where its
    number or winner differs, else (decision F2) the release line that gives it another range."""
    if other["daily"] != m["daily"] or other["win"] is not m["win"]:
        return other["narrow"] or other["win"]
    mine = {(l["a"], l["b"], l["r"].key) for l in m["lines"] if l["t"] == "rel"}
    return next((l["r"] for l in other["lines"] if l["t"] == "rel"
                 and (l["a"] == 0 or l["b"] == INF) and (l["a"], l["b"], l["r"].key) not in mine),
                other["narrow"] or other["win"])


def _line_key(res: Optional[dict]) -> str:
    if not res:
        return "-"
    return res["status"] + res["win"].key + _js(res["daily"]) + ",".join(
        l["t"] + l["r"].key + ("" if l.get("a") is None else _js(l["a"])) for l in res["lines"])


FACT_ORDER = ["origin", "origin2", "caution", "steel", "exc", "xref", "rel", "cap", "also", "subcap",
              "outer", "outercap", "outersize", "orphan", "partly", "annual", "possession_cap",
              "record", "duty", "tnote"]


def species_at(P: "Part") -> List[str]:
    from pipeline.regs.parsing.catalogue import PROTECTED_FISH
    s: List[str] = []
    for r in P.cands:
        if P.applies(r) and r.type == "retention_limit" \
                and r.k in ("gate", "pool", "subcap", "sizecap", "size", "annual",
                            "possession_cap") \
                and "ALL_GAME_FISH" not in (r.f.get("species") or []):
            for x in r.species:
                if x not in PROTECTED_FISH and x not in s:
                    s.append(x)
    return s


def build_model(P: "Part") -> dict:
    """The page's `buildModel`: the fish asked about, one answer per fish and origin, the rows."""
    from pipeline.regs.parsing.catalogue import BOOK_FAMILIES, PROTECTED_FISH
    steel_named = any("ST" in (r.f.get("species") or []) for r in P.cands)
    spp = [S for S in species_at(P)
           if S != "ST" or (P.presence and steel_named and not P.rules_off_in_export)]
    Rm = {S: {"H": eval_sp(P, S, "hatchery"), "W": eval_sp(P, S, "wild")} for S in spp}
    keep_rows: Dict[str, dict] = {}
    rest: Dict[str, dict] = {}
    for S in spp:
        m = main_res(Rm[S]["H"], Rm[S]["W"])
        if m and _keepish(m["status"]):
            k = m["win"].key
            keep_rows.setdefault(k, {"kind": m["status"], "pool": m["win"], "members": [],
                                     "exc": [], "xref": []})["members"].append(S)
    for S in spp:
        m = main_res(Rm[S]["H"], Rm[S]["W"])
        if not m:
            continue
        if _keepish(m["status"]):
            for k, v in m["roles"].items():
                # T127: the pools the fish counts toward (`counts_toward`); without the ruling (mutation only) a pool the
                # reader displaced for it cross-referenced it too
                if k in keep_rows and k != m["win"].key and (
                        counts_toward(m, k) or (not A_DISPLACED_POOL_IS_NO_CROSS_REFERENCE and v["role"] == "replaced")):
                    keep_rows[k]["xref"].append({"S": S, "by": m["win"], "daily": m["daily"]})
            continue
        parent = None
        for want in ("moot", "replaced"):
            if parent is None:
                for k, v in m["roles"].items():
                    if v["role"] == want and k in keep_rows:
                        parent = k
                        break
        if parent:
            keep_rows[parent]["exc"].append({"S": S, "res": m})
        else:
            k = m["status"] + "|" + m["win"].key
            rest.setdefault(k, {"kind": m["status"], "win": m["win"], "members": []})["members"].append(S)
    rows = []
    for row in keep_rows.values():
        row["members"].sort(key=lambda S: _icu_key(P.sp_name(S)))
        M = row["members"]
        facts: Dict[str, dict] = {}

        def add(l: dict, S: str):
            k = _fact_key(l)
            if k not in facts:
                facts[k] = {**l, "rules": [], "members": []}
            f = facts[k]
            if S not in f["members"]:
                f["members"].append(S)
            if l["r"] not in f["rules"]:
                f["rules"].append(l["r"])
        narrow = None
        for S in M:
            H, W = Rm[S]["H"], Rm[S]["W"]
            m = main_res(H, W)
            if m["narrow"]:
                narrow = m["narrow"]
            if H and W and _line_key(H) != _line_key(W):
                other = W if m is H else H
                if not _keepish(other["status"]):
                    add({"t": "origin", "o": other["o"], "keepO": m["o"], "r": other["win"],
                         "status": other["status"]}, S)
                elif other["daily"] != m["daily"] or other["win"] is not m["win"] \
                        or _min_max(other) != _min_max(m):            # decision F2
                    mn, mx = _min_max(other)
                    add({"t": "origin2", "o": other["o"], "daily": other["daily"],
                         "min": mn, "max": mx,
                         "r": _origin2_rule(other, m)}, S)
            for l in m["lines"]:
                add(l, S)
        for e in row["exc"]:
            add({"t": "exc", "status": e["res"]["status"], "r": e["res"]["win"]}, e["S"])
        for x in row["xref"]:
            add({"t": "xref", "r": x["by"], "daily": x["daily"]}, x["S"])
        lst = list(facts.values())
        for l in lst:
            if l["t"] == "cap":
                c = next((o for o in lst if o is not l and o["t"] == "cap" and o["a"] == l["a"]
                          and o["b"] == l["b"] and len(o["members"]) < len(l["members"])), None)
                if c:
                    l["general"] = True
                    c["carveOf"] = l
        lst.sort(key=lambda l: FACT_ORDER.index(l["t"]))
        everyone, groups = [], {}
        for l in lst:
            if l.get("general") or (len(l["members"]) == len(M) and l["t"] not in ("exc", "xref")):
                everyone.append(l)
                continue
            key = ",".join(sorted(l["members"]))
            groups.setdefault(key, {"members": list(l["members"]), "facts": []})["facts"].append(l)
        first = main_res(Rm[M[0]]["H"], Rm[M[0]]["W"])
        rows.append({"kind": row["kind"], "pool": row["pool"], "members": M,
                     "daily": first["daily"], "narrow": narrow, "everyone": everyone,
                     "groups": sorted(groups.values(), key=lambda g: len(g["members"])),
                     "allMembers": M + [e["S"] for e in row["exc"]],
                     "liftNotes": first["liftNotes"]})
    rows.sort(key=lambda r: (-len(r["members"]), r["pool"].rank))
    # closures and releases of an OPEN SUBJECT (protected species): a row of their own
    opens = []
    for r in P.open_gates():
        sp = r.f.get("species") or []
        pr = all(c in PROTECTED_FISH for c in sp)
        shut = closure_grade(r.f) is not None           # the one closure predicate
        k = "open|" + ("PROT" if pr else ",".join(sp)) + ("false" if shut else "true"
                                                          if catch_and_release(r.f) else "undefined")
        if k not in rest:
            rest[k] = {"kind": "closed" if shut else "release", "win": r,
                       "members": [], "prot": [] if pr else None, "wins": []}
        x = rest[k]
        x["wins"].append(r)
        if pr:
            for c in sp:
                if c not in x["prot"]:
                    x["prot"].append(c)
    for r in sorted(rest.values(), key=lambda r: ((r["kind"] == "closed"), r["win"].rank)):
        ms = sorted(r["members"], key=lambda S: _icu_key(P.sp_name(S)))
        rows.append({"kind": r["kind"], "win": r["win"], "pool": None,
                     "members": ms, "allMembers": list(ms), "everyone": [], "groups": [],
                     "prot": sorted(r["prot"], key=lambda S: _icu_key(P.sp_name(S)))
                     if r.get("prot") is not None else None,
                     "wins": r.get("wins"), "liftNotes": P.lift_notes_of(r["win"]),
                     "daily": None, "narrow": None})
    fam = {c: f for f, cs in BOOK_FAMILIES.items() for c in cs}

    def sprank(S):
        f = fam.get(S)
        if f in ("TROUT", "CHAR"):
            return 0
        if S == "KO":
            return 1
        if f == "WHITEFISH" or S in ("GR", "IN"):
            return 2
        if S == "CRA":
            return 4
        return 3

    def row_rank(r):
        return min(sprank(S) for S in r["members"]) if r["members"] else 5
    rows = sorted(rows, key=row_rank)                      # stable: the order above breaks ties
    return {"R": Rm, "rows": rows, "spp": spp}
