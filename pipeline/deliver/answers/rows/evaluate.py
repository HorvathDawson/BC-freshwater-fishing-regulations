"""Consumer 5.2: one answer per fish and origin (the page's `evalSp`) — the winner after its clauses, the
number kept, the lines under it — and when a water's line restates its region's (`restates`)."""
from __future__ import annotations

import json
import re
from functools import cmp_to_key
from typing import Dict, List, Optional, Tuple
from pipeline.deliver.answers import common
from pipeline.deliver.answers.common import AnswersError
from pipeline.deliver.answers.display import is_closure_gate
from pipeline.common import entry_id as EID
from pipeline.deliver.answers.rows.rule import INF, R, _bands, _covers, _ge, _js, _keepish, size_lines


def _stable_sort(xs, key):
    return sorted(xs, key=key)


#: POLICY (T1251, owner 2026-11-27: "The most upstream row, in 0.10.0"): THE MOST UPSTREAM ROW IS NAMED. Of two
#: closures of two entries tied on rank that both close the day, the more upstream one governs and is the one the card
#: names, and comes first in a closed day's banner (`display.section`, the same comparison: `upstream_cmp`): the
#: water's OWN extent before an inherited one (the water itself is the most upstream), and of two INHERITED closures
#: (both reaching the section by the tributary walk, `via` "trib") the one whose row's water is the more upstream
#: (`read._upstream_of`, the bundle's `trib_upstream`: `trib_nearer` and the rows whose tributary walks nest). Words
#: only: both still close (RULINGS T14: a closure is never beaten by a nearer row), the day, the fish and the status are
#: the same. Two closures in two rows' own extents are not ordered by it (an open question, T1251). Asked before T206's
#: year-round-before-dated and the fish count. False = T206's order alone (Lower Campbell Lake's creeks name John Hart
#: Lake's closure by entry key, and the banner lists closures in rule order).
THE_MOST_UPSTREAM_ROW_IS_NAMED = True


#: POLICY (T1251, owner 2026-11-27, verbatim: "(a) Own row first, then most upstream (Recommended)"): ON ONE RUNG THE
#: WATER'S OWN ROW FIRST. Of two closures that both close the day, the water's own row (the bundle's `set_own`: the row
#: whose matched water holds the part) is named first, a closure printed by a row's own extent before one it inherits,
#: and only then the most upstream: the North Alouette River names its own "No Fishing upstream of 216th Street (Fifth
#: Ave), May 1-June 30", not the Alouette River's "(including North Alouette River)"; the Campbell River's 100 m below
#: Strathcona Dam keeps its own all-year closure over Lower Campbell Lake's. False = the most upstream alone, two
#: own-extent closures by T206's order (the mutation pin).
OWN_ROW_FIRST = True


def upstream_cmp(a, b, upstream: frozenset, own: frozenset = frozenset()) -> int:
    """THE ONE comparison of two closures of two entries on one rung (`THE_MOST_UPSTREAM_ROW_IS_NAMED`, `OWN_ROW_FIRST`;
    each has `entry_id` and `via`): the water's own row (`own`), then an own extent before an inherited one, then of two
    closures reaching the water the same way the more upstream row (`upstream`, the (near, far) pairs). -1 / 1 when it
    orders them, 0 when it has no say. The card's naming (`_closure_cmp`) and the banner's order
    (`most_upstream_first`) both ask it; never recomputed per surface."""
    if not THE_MOST_UPSTREAM_ROW_IS_NAMED:
        return 0
    va, vb = getattr(a, "via", None), getattr(b, "via", None)
    if OWN_ROW_FIRST:
        ao, bo = a.entry_id in own, b.entry_id in own
        if ao != bo:
            return -1 if ao else 1
        if {va, vb} == {"reach", "trib"}:
            return -1 if va == "reach" else 1      # a row's own extent before what the water inherits
    if va == vb and (va == "trib" or OWN_ROW_FIRST) and upstream:
        if (a.entry_id, b.entry_id) in upstream:
            return -1
        if (b.entry_id, a.entry_id) in upstream:
            return 1
    return 0


def _upstream_first_in(items: list, upstream: frozenset, own: frozenset) -> list:
    """`items` (one rung) with the one `upstream_cmp` puts first moved forward to the place of the one it must precede:
    the next place takes the first remaining item, or the first remaining item that must precede it (and so on up the
    chain); two closures no chain relates keep their order. A circle (which the bundle's `prove` refuses) keeps the
    order given."""
    rest, out = list(items), []
    while rest:
        x, seen = rest[0], set()
        while id(x) not in seen:
            seen.add(id(x))
            y = next((y for y in rest
                      if y is not x and y.entry_id != x.entry_id and upstream_cmp(y, x, upstream, own) < 0), None)
            if y is None:
                break
            x = y
        else:
            x = rest[0]
        rest.remove(x)
        out.append(x)
    return out


def most_upstream_first(items: list, upstream: frozenset, own: frozenset = frozenset()) -> list:
    """`items` (each with `entry_id`, `via`, `rank`), each RUNG reordered only within the places it already holds
    (`_upstream_first_in`): a closure never moves across another rung's, so rung order and every other place stay. The
    water's own row is no rung of its own: it is ordered first within its rung."""
    places: Dict[int, List[int]] = {}
    for i, x in enumerate(items):
        places.setdefault(x.rank, []).append(i)
    out = list(items)
    for rank, idx in places.items():
        for i, x in zip(idx, _upstream_first_in([items[i] for i in idx], upstream, own)):
            out[i] = x
    return out


def upstream_pairs(B) -> frozenset:
    """THE ONE READ of the (near, far) pairs for a bundle (`read._upstream_of`, the table `trib_upstream`): the card's
    naming and the banner's order both take it here. A bundle object with no `path` (one a test builds in memory) has
    no pairs: the documented empty case, never a fallback for a real bundle (a real one without the table is refused)."""
    from pipeline.deliver.bundle import read
    path = getattr(B, "path", None)
    return frozenset() if path is None else read._upstream_of(path)


def own_rows(B, set_id: int) -> frozenset:
    """THE ONE READ of a rule set's own rows (`read._own_of`, the table `set_own`); no `path`: none, as `upstream_pairs`."""
    from pipeline.deliver.bundle import read
    path = getattr(B, "path", None)
    return frozenset() if path is None else read._own_of(path).get(set_id, frozenset())


def _closure_cmp(a, b, upstream: frozenset = frozenset(), own: frozenset = frozenset()) -> int:
    """Which of two closures speaking for a fish governs (T206, owner 2026-10-26: never by entry key):
    the nearer rank; two rules of ONE entry keep the book's order (rule order), as before; between two
    entries, the more upstream (`upstream_cmp`: own extent before inherited, then the (near, far) pairs), then a
    standing (year-round) closure before a dated one, then the one naming more fish:
    Region 4's invasive-species notice (bass, pike, walleye, perch) over the quota box's single
    "0, CLOSED" lines, which key order let win once `z4:species_quotas` became `z4:daily_quotas`,
    splitting the one folded row into five."""
    if a.rank != b.rank:
        return -1 if a.rank < b.rank else 1
    if a.entry_id == b.entry_id:
        return 0                                   # one entry: the book's rule order
    got = upstream_cmp(a, b, upstream, own)
    if got:
        return got
    da, db = bool(a.f.get("when")), bool(b.f.get("when"))
    if da != db:
        return 1 if da else -1
    if len(a.species) != len(b.species):
        return -1 if len(a.species) > len(b.species) else 1
    return 0


closure_order = cmp_to_key(_closure_cmp)


def closure_order_for(upstream: frozenset, own: frozenset = frozenset()):
    """`closure_order` with the bundle's upstream pairs and the part's own rows (`upstream_pairs`, `own_rows`)."""
    return cmp_to_key(lambda a, b: _closure_cmp(a, b, upstream, own))




#: OWNER 2026-10-28 (Q87): a pool naming FEWER fish than the winning pool (a strict subset: the Liard's bull trout 1
#: beside Beattie Lake's trout 1, both speaking since the reader's `named_inside_group`) sits INSIDE the winner: it is
#: never read as the winner's outer total, and its own size bands hold for the fish. False = the Liard's 1 read as an
#: outer again, and Beattie's bull trout loses its 30 cm floor (the mutation pin).
A_NAMED_POOL_IS_INSIDE = True


def _inside(p: R, win: R) -> bool:
    """`p` holds a strict subset of the winner's fish and keeps no more of them than the winner does (a narrower pool
    with a LARGER number — Region 8's "20 brook trout from streams" over Granby River's trout/char 1 — stays the
    winner's outer, as before)."""
    return (set(p.species) < set(win.species) and not (set(p.species) & set(win.except_))
            and not p.f.get("unlimited") and p.f.get("take") is not None
            and (win.f.get("unlimited") or (win.f.get("take") is not None and p.f["take"] <= win.f["take"])))


def eval_sp(P: "Part", S: str, o: str) -> Optional[dict]:
    """The decided answer for fish `S` of origin `o`: status, winner, the day's number after the
    winner's clauses, every line, every rule's role — or None (no rule in scope speaks)."""
    verdict = P.verdict(S, o)
    roles: Dict[str, dict] = {}

    def setr(r: R, role: str, by: Optional[str] = None):
        if r.key not in roles:
            roles[r.key] = {"role": role, "by": by}

    def in_scope(r: R) -> bool:
        return ((r.type == "retention_limit" or r.k == "duty")
                and r.k not in ("while", "caught", "exempt", "standing") and r.dimension != "lift"
                and (not r.f.get("origin") or r.f.get("origin") == o))

    for r in P.cands:
        v = verdict.get(r.key)
        if v is None or not in_scope(r):
            continue
        if v[0] == "displaced":
            setr(r, "replaced", v[3])
        elif v[0] == "lifted":
            setr(r, "lifted", v[3])
        elif v[0] == "moot":                                   # decision R1
            setr(r, "moot", v[3])
    A = [r for r in P.cands if in_scope(r) and (verdict.get(r.key) or [None])[0] == "speaks"]
    partly = [r for r in A if verdict[r.key][1]]
    steelhead = S == "RB" and P.steelhead_water
    pools = [r for r in A if r.k == "pool"]
    closed = _stable_sort([r for r in A if is_closure_gate(r.f, r.k)], key=closure_order_for(upstream_pairs(P.B), own_rows(P.B, P.set_id)))
    rel = _stable_sort([r for r in A if r.k == "gate" and not is_closure_gate(r.f, r.k)], key=lambda r: r.rank)

    def tk(r):
        return 1e9 if r.f.get("unlimited") else r.f.get("take")
    by_take = _stable_sort(pools, key=lambda r: (tk(r), r.rank))
    win = (closed or rel or by_take or [None])[0]
    if win is None:
        win = _size_release(P, S, A)                       # decision F13
    if win is None:
        return None
    shut = _range_released(P, S, A) if win.k == "pool" else None     # decision F16
    if shut is None and win.k == "pool":
        shut = _subcap_released(S, A, partly)                          # T846
    if shut is not None:
        win = shut
    if win.k == "gate":
        status = "closed" if is_closure_gate(win.f, win.k) else "release"
    elif win.k == "size" or shut is not None:
        status = "release"
    else:
        status = "no_limit" if win.f.get("unlimited") else "keep"
    setr(win, "governs")
    for r in closed + rel:
        if r is not win:
            setr(r, "agrees", win.key)
    outers = []
    if win.k == "pool":
        for p in pools:
            if p is win:
                continue
            if A_NAMED_POOL_IS_INSIDE and _inside(p, win):
                continue                                   # owner 2026-10-28 (Q87): never the winner's outer
            if ((len(p.species) > len(win.species) and all(x in p.species for x in win.species))
                    or (p.rank >= 2 and win.rank <= 1
                        and (p.f.get("unlimited") or _ge(p.f.get("take"), win.f.get("take"))))):
                outers.append(p)
    for O in outers:
        setr(O, "contains")
    also = [p for p in by_take if p is not win and p not in outers] if win.k == "pool" else []
    # RULINGS P21: a rule of the winner's OWN ROW saying the very same thing (fish, number, days,
    # sizes, origin, kind of water) is the same statement printed twice — the Lower West Arm's r5, the
    # combined kokanee quota with the Upper West Arm, modelled as "kokanee 5, Saturday and Sunday" beside r3:
    # it agrees with the winner (one ladder line); its words are the row's note (`restates`)
    for p in [p for p in also if restates(p, win)]:
        setr(p, "agrees", win.key)
        also.remove(p)
    for p in also:
        setr(p, "also")
    if win.k == "gate" or (shut is not None and shut.k == "subcap"):
        for p in pools:
            setr(p, "moot", win.key)
    res = {"S": S, "o": o, "status": status, "win": win, "daily": None, "lines": [],
           "roles": roles, "narrow": None, "liftNotes": P.lift_notes(S, o, win)}
    if _keepish(status):
        Pw = win
        daily = INF if Pw.f.get("unlimited") else Pw.f.get("take")
        lines = res["lines"]

        def chain(root: R) -> List[R]:
            ids = {root.rule_id}
            cl: List[R] = []
            grew = True
            while grew:
                grew = False
                for r in A:
                    if r.f.get("within") and r.entry_id == root.entry_id \
                            and r.f["within"] in ids and r not in cl:
                        cl.append(r)
                        ids.add(r.rule_id)
                        grew = True
            return cl
        cl = chain(Pw)
        for r in [r for r in cl if r.k == "subcap"]:
            if all(x in r.species for x in Pw.species):
                daily = min(daily, r.f.get("take"))
                setr(r, "narrows")
                res["narrow"] = r
            else:
                setr(r, "limit")
                lines.append({"t": "subcap", "r": r})
        sc = _stable_sort([r for r in cl if r.k == "sizecap"], key=lambda r: len(r.species))
        seen = []
        top_len = max((len(x.species) for x in sc), default=0)
        for r in sc:
            key = json.dumps(r.f.get("lengths"), sort_keys=True)
            beat = next((s for s in seen if s[0] == key), None)
            if beat:
                setr(r, "replaced", beat[1].key)
                continue
            seen.append((key, r))
            setr(r, "limit")
            tmp: list = []
            size_lines(r, daily, tmp)
            for l in tmp:
                if len(sc) > 1 and len(r.species) < top_len:
                    l["carve"] = True
                lines.append(l)
        for r in A:
            if r.k == "size":
                setr(r, "floor")
                size_lines(r, daily, lines)
        for p in also:
            if A_NAMED_POOL_IS_INSIDE and _inside(p, Pw):
                # (Q87) a quota naming fewer fish, inside the winner's group: its own sizes hold for the fish —
                # Beattie Lake's bull trout is kept only at the Liard's 30–50 cm, inside the lake's 1
                size_lines(p, daily, lines)
            big = bool(p.f.get("unlimited")) or (p.f.get("take") is not None and p.f["take"] > daily)
            if big and p.rank >= 2 and Pw.rank <= 1:
                lines.append({"t": "outer", "r": p})
            else:
                lines.append({"t": "also", "r": p, "capped": big})
        if steelhead:
            lines.append({"t": "steel", "r": Pw})
        size_lines(Pw, daily, lines)
        if steelhead:
            lines[:] = [l for l in lines if l["t"] not in ("cap", "rel") or l["a"] < 50]
        for O in outers:
            lines.append({"t": "outer", "r": O})
            for r in chain(O):
                if r.k == "subcap" and not all(x in r.species for x in O.species):
                    setr(r, "limit")
                    lines.append({"t": "outercap", "r": r, "outer": O})
                elif r.k == "sizecap":
                    setr(r, "limit")
                    tmp = []
                    size_lines(r, O.f.get("take"), tmp)
                    for l in tmp:
                        lines.append({**l, "t": "outersize" if l["t"] == "cap" else l["t"],
                                      "outer": O})
                elif r.k == "subcap":
                    setr(r, "narrows")
                    if r.f.get("take") < daily:
                        daily = r.f["take"]
        in_chain = set()
        for x in [Pw] + outers:
            in_chain.add(x.key)
            in_chain.update(c.key for c in chain(x))
        for r in A:
            if r.k in ("subcap", "sizecap") and r.key not in in_chain \
                    and not _ge(r.f.get("take"), daily):
                setr(r, "limit")
                if r.k == "subcap":
                    if r.f.get("take") < daily and all(x in r.species for x in Pw.species):
                        daily = r.f["take"]
                    else:
                        lines.append({"t": "orphan", "r": r})
                else:
                    size_lines(r, daily, lines)
        for r in partly:
            lines.append({"t": "partly", "r": r})
        for r in A:
            for e in [e for e in (r.f.get("exempts") or []) if e.get("caution")][:1]:
                lines.append({"t": "caution", "r": r, "says": e["caution"].get("says")})
        if "TROUT_CHAR" in (Pw.f.get("species") or []):
            lines.append({"t": "tnote", "r": Pw,
                          "only": "CHAR" in (Pw.f.get("species_except") or [])})
        for r in A:
            if r.k in ("annual", "possession_cap"):
                if r.k == "possession_cap":                   # decision F6 (display.kind_of)
                    setr(r, "possession_cap")
                    lines.append({"t": "possession_cap", "r": r})
                else:
                    setr(r, "season")
                    lines.append({"t": "annual", "r": r})
        for r in A:
            if r.k == "duty":
                setr(r, "duty")
                lines.append({"t": "duty", "r": r})
        if any(r.f.get("record_retention") for r in A):
            for r in A:
                if r.f.get("record_retention"):
                    lines.append({"t": "record", "r": r})
        if S == "ST" and P.steelhead_water:
            lines[:] = [l for l in lines if not (l["t"] == "rel" and l["a"] == 0 and l["b"] <= 50)]
        mins = [l for l in lines if l["t"] == "rel" and l["a"] == 0]
        if len(mins) > 1:
            top = max(l["b"] for l in mins)
            lines[:] = [l for l in lines if not (l["t"] == "rel" and l["a"] == 0 and l["b"] < top)]
        res["daily"] = daily
    for r in A:
        if r.key in roles:
            continue
        if r.k == "possession":
            setr(r, "possession")
        elif r.f.get("within"):
            p = f"{r.entry_id}::{r.f['within']}"
            if p not in P.all_rules:
                raise AnswersError(f"rows: {r.key} falls within {p}, which is not a rule (L1)")
            setr(r, "falls", p)
        elif r.k in ("size", "subcap", "sizecap", "annual", "possession_cap", "duty"):
            setr(r, "moot", win.key)
    return res


_SAME_FIELDS = ("species", "species_except", "take", "unlimited", "when", "lengths", "origin", "water",
                "while", "when_targeting", "dimension", "type")


def restates(p: R, win: R) -> bool:
    """RULINGS P21: `p` is a rule of the winner's own row stating exactly what the winner states."""
    return p.entry_id == win.entry_id and p.key != win.key and p.via == win.via \
        and all(json.dumps(p.f.get(k), sort_keys=True) == json.dumps(win.f.get(k), sort_keys=True)
                for k in _SAME_FIELDS)


_COMBINED = re.compile(r"(?i)\bcombined daily quota for (?P<fish>[a-z ]+?) from (?P<a>.+?) and (?P<b>.+?) "
                       r"cannot exceed (?P<n>\d+)\b")


def _two_places(a: str, b: str) -> str:
    """"the Lower West Arm" + "the Upper West Arm" -> "the Lower and Upper West Arms" (their shared last words said
    once, plural); else "A and B"."""
    A, B = a.split(), b.split()
    k = 0
    while k < min(len(A), len(B)) - 1 and A[-1 - k] == B[-1 - k]:
        k += 1
    if not k:
        return f"{a} and {b}"
    lead = A[0] if A[0].lower() == "the" and B[0].lower() == "the" else ""
    a0, b0 = A[1 if lead else 0:len(A) - k], B[1 if lead else 0:len(B) - k]
    if not a0 or not b0:
        return f"{a} and {b}"
    tail = A[len(A) - k:]
    tail[-1] += "s"
    return " ".join(([lead] if lead else []) + a0 + ["and"] + b0 + tail)


def combined_words(r: R) -> Optional[str]:
    """UR3 6 (user panel: "the combined daily quota … (when open to kokanee harvest) … cannot exceed 5" read as legal
    text): a quota two waters share, in plain words — "Kokanee from the Lower and Upper West Arms together: no more than
    5 a day." The fish and the number are the rule's own fields (`species`, `take`); the two places the book's words,
    this water's first (its entry's name). None unless the book's sentence is that shape and agrees with the fields."""
    w = re.sub(r"\s+", " ", r.f.get("verbatim") or "")
    m = _COMBINED.search(w)
    sp, take = list(r.f.get("species") or []), r.f.get("take")
    if not m or len(sp) != 1 or take is None or float(take) != float(m.group("n")) \
            or common.sp_name(sp[0]).lower() != m.group("fish").strip().lower():
        return None
    a, b = (re.sub(r"\s*\([^)]*\)", "", m.group(g)).strip() for g in ("a", "b"))
    here = (r.x.get("entry_name") or "").lower()
    if re.sub(r"^the ", "", b.lower()) in here and re.sub(r"^the ", "", a.lower()) not in here:
        a, b = b, a
    return f"{common.sp_name(sp[0])} from {_two_places(a, b)} together: no more than {_js(take)} a day."


def restated_words(r: R) -> str:
    """The book's words of a restating rule, as a note: "NOTE: the combined daily quota … cannot exceed 5" ->
    "The combined daily quota … cannot exceed 5." (whitespace folded, the "NOTE:" lead dropped); a quota two waters
    share in plain words (`combined_words`, UR3 6)."""
    got = combined_words(r)
    if got is not None:
        return got
    w = re.sub(r"\s+", " ", r.f.get("verbatim") or r.f.get("label") or "").strip()
    w = re.sub(r"(?i)^note:\s*", "", w)
    return common.cap(w.rstrip(".")) + "."


def same_statement(p: R, win: R) -> bool:
    """RULINGS P28: `p` states exactly what the winner states (same fish, number, days, sizes, origin, water)."""
    return p.key != win.key and all(json.dumps(p.f.get(k), sort_keys=True) == json.dumps(win.f.get(k), sort_keys=True)
                                    for k in _SAME_FIELDS)


def nearer_than(P: "Part", a: R, b: R) -> bool:
    """RULINGS P28 (the T14 principle — closest wins): `a`'s row is nearer this stream than `b`'s — a's
    water drains into b's (`trib_nearer`); else a sits higher on the ladder (`rank`); else, on one rung, a
    water's own row beside a zone table's line naming the water (Williston Lake — Zone B's row beside Zone B's
    "all from Williston Lake and its tributaries")."""
    from pipeline.deliver.bundle import read
    path = getattr(P.B, "path", None)
    pairs = read._nearer_of(path) if path else frozenset()
    if (a.entry_id, b.entry_id) in pairs or (b.entry_id, a.entry_id) in pairs:
        return (a.entry_id, b.entry_id) in pairs
    if a.rank != b.rank:
        return a.rank < b.rank
    return EID.is_water_row(a.entry_id) and EID.is_table(b.entry_id)


def row_restates(P: "Part", model: dict, row: dict) -> List[dict]:
    """RULINGS P21: the rules of a row's winners' own rows that restate them, as notes [{r, of, says}].
    RULINGS P28: so is ANOTHER row's identical statement, no nearer the stream than the
    winner's — shown ONCE, credited to the nearer row, its words a note: Williston Lake — Zone B's
    "Arctic grayling catch and release" with Zone B's table line for "Williston Lake and its tributaries"
    beside it ("Zone B also prints this: “…”"). Never one NEARER than the winner (`nearer_than`)."""
    out: List[dict] = []
    for S in row["allMembers"]:
        for v in (model["R"].get(S) or {}).values():
            if not v:
                continue
            for k, x in v["roles"].items():
                r = P.by_key.get(k)
                if x["role"] != "agrees" or r is None or any(o["r"] == k for o in out):
                    continue
                if r.k == "pool" and restates(r, v["win"]):
                    out.append({"r": k, "of": v["win"].key, "says": restated_words(r)})
                elif r.entry_id != v["win"].entry_id and (EID.is_water_row(r.entry_id) or r.via == "trib") \
                        and same_statement(r, v["win"]) and not nearer_than(P, r, v["win"]):
                    # a water's row, or a table's line reaching the stream by the walk ("Williston Lake and
                    # its tributaries") — two region tables' identical lines are RU-8's, not this
                    who = common.the_name(re.sub(r"^the ", "", r.x.get("entry_name") or "", flags=re.I)) or "Another row"
                    out.append({"r": k, "of": v["win"].key,
                                "says": f"{common.cap(who)} also prints this: “{restated_words(r).rstrip('.')}”."})
    return out


def fish_range(P: "Part", S: str) -> Tuple[float, float]:
    """The lengths fish `S` has on this part: a rainbow over 50 cm is a steelhead on a steelhead
    water (p.80; F5c), so there the rainbow's range ends at 50 cm and the steelhead's starts there."""
    if P.steelhead_water and S == "RB":
        return 0, 50
    if S == "ST" and (P.steelhead_water or A_STEELHEAD_IS_OVER_50_EVERYWHERE):
        return 50, INF
    return 0, INF


#: T805 part D (adversarial review, Shamrock Creek blk:356196726): p.80 "steelhead: a rainbow trout longer than 50 cm in
#: waters where anadromous rainbow trout are found" — a steelhead is over 50 cm WHEREVER one is answered, not only where a
#: big rainbow is known to be one. Shamrock Creek (steelhead possible, Lonzo Creek's "Trout/char 20-30 cm in length Jun
#: 16-Jan 31", p.24, by the walk) read "Up to 2 hatchery steelhead, 20–30 cm"; the 20-30 cm band now releases every
#: steelhead there (decision F16), as on Lonzo Creek itself. False = the mutation pin (the impossible steelhead again).
A_STEELHEAD_IS_OVER_50_EVERYWHERE = True


#: T805 (RULINGS C5, owner 2026-11-16 q.38): a take-0 size clause of a quota (`within`) releases its band in decision F16
#: exactly as the same words printed as a rule of their own do, so the seven Region 2 lakes printing "Wild trout/char
#: daily quota = 2 (no wild trout over 40 cm)" store their char one way (hatchery only). False = the mutation pin: Lois
#: and Windsor lakes' wild char read "no wild … 60 cm or longer" again (an empty wild range said as a keepable one).
A_SIZE_CLAUSE_RELEASES_AS_A_RULE = True


def _range_released(P: "Part", S: str, A: List[R]) -> Optional[R]:
    """Decision F16: a fish whose every length on this part is released by the take-0 bands of the
    rules speaking for it TOGETHER is released, whatever its pool says — Marshall (Lonzo) Creek's
    "Trout/char 20-30 cm" beside Region 2's "Dolly Varden … none under 60 cm" (p.24, p.21): no char
    may be kept there, not "1"; the Coquitlam's "Hatchery trout 2 (20-30 cm only)" on a steelhead
    water: no steelhead (rainbow over 50 cm) may be kept. The winner is the lowest-ranked (the
    water's own first) rule whose band releases part of the range. None where some length is kept.
    T805 (RULINGS C5): a size clause written INSIDE a quota (`within`, a `sizecap`) releases its band as a
    stand-alone one does — Lois and Windsor lakes print "(no wild trout 40 cm or more)" as a clause of their
    wild quota, Dodd and five more as a rule of its own; beside Region 2's "none under 60 cm" either way no
    wild char may be kept (`A_SIZE_CLAUSE_RELEASES_AS_A_RULE`)."""
    lo, hi = fish_range(P, S)
    kinds = ("size", "pool", "sizecap") if A_SIZE_CLAUSE_RELEASES_AS_A_RULE else ("size", "pool")
    rel = sorted(((a, b, r) for r in A if r.k in kinds
                  for a, b, t in _bands(r) if t == 0), key=lambda x: (x[0], x[1], x[2].key))
    if not rel:
        return None
    at, grew = lo, True
    while grew and at < hi:
        grew = False
        for a, b, _r in rel:
            if a <= at < b:
                at, grew = b, True
    if at < hi:
        return None
    return min((r for a, b, r in rel if b > lo and a < hi), key=lambda r: r.rank)


#: T846 (T810 L re-review, 2026-11-18): a take-0 share of a quota (`subcap`) naming the fish releases it, whatever the
#: quota keeps of the rest. The Bella Coola tributaries' "all cutthroat trout catch and release" sits inside the
#: rivers' own trout quota (atnarko_bella_coola_rivers.r4): the decided answer said keep, daily 1, where the book and the
#: engine keep no cutthroat. False = the mutation pin (keep again).
A_ZERO_SHARE_RELEASES = True


def _subcap_released(S: str, A: List[R], partly: List[R]) -> Optional[R]:
    """T846: the lowest-ranked speaking take-0 `subcap` covering fish `S` whole (no sizes, not only part of the day),
    or None."""
    if not A_ZERO_SHARE_RELEASES:
        return None
    zero = [r for r in A if r.k == "subcap" and r.f.get("take") == 0 and not r.f.get("lengths")
            and _covers(r, S) and r not in partly]
    return min(zero, key=lambda r: (r.rank, r.key)) if zero else None


def _size_release(P: "Part", S: str, A: List[R]) -> Optional[R]:
    """Decision F13: where no gate and no pool speaks for the fish, a speaking SIZE release (a
    take-0 band with no count, "hatchery rainbow trout catch and release (50 cm or less)") that
    releases the fish's WHOLE range on this part is the answer — release, that rule the winner.
    Vedder r6, May 1-31: a hatchery rainbow of 50 cm or less is released (p.22); over 50 cm it is
    a steelhead, answered as ST. A size release covering only PART of the range, with nothing
    speaking for the rest, is refused: the card cannot say what the rest keeps (no fallback)."""
    lo, hi = fish_range(P, S)
    sizes = [r for r in A if r.k == "size" and any(t == 0 for _, _, t in _bands(r))]
    for r in sorted(sizes, key=lambda r: r.rank):
        at = lo
        for a, b, t in sorted(_bands(r)):
            if t == 0 and a <= at < b:
                at = b
        if at >= hi:
            return r
    if sizes:
        raise AnswersError(f"rows: {S} — only a size release speaks ({sizes[0].key}) and it does "
                           f"not release the fish's whole range {lo}-{hi} cm (decision F13)")
    return None


def fish_answer(h: Optional[dict], w: Optional[dict]) -> dict:
    """One fish's stored answers (`FishAnswer`): its hatchery and wild answers and, T161, which of them LEADS
    (`common.lead_name`) — shipped so the app reads the leader instead of picking it."""
    return {"hatchery": h, "wild": w, "lead": common.lead_name(h, w)}


def main_res(H: Optional[dict], W: Optional[dict]) -> Optional[dict]:
    """The fish's leading origin answer (T120: `common.lead_origin`, the one function)."""
    return common.lead_origin(H, W)


def counts_toward(m: Optional[dict], key: str) -> bool:
    """T127: the fish whose decided answer is `m` COUNTS TOWARD the pool `key` here — the reader's "who governs this
    kind here": the pool governs it (the winner) or holds it as the total around its winner (`contains`, a pool the
    reader says SPEAKS for it). A pool the reader displaced for the fish (role `replaced`: Perry Creek's own "brook trout
    20" displaced the St. Mary's "trout/char 1 on all tributaries", reason own_row) or lifted holds nothing: no
    cross-reference ("counts toward this one"), no group membership, no outer total. The one predicate behind
    `build_model`'s xref facts, `group_of`'s kinds and `total_of`."""
    if not m:
        return False
    return m["win"].key == key or (m["roles"].get(key) or {}).get("role") == "contains"
