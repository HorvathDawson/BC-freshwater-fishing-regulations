"""Consumer 5.2: one answer per fish and origin (the page's `evalSp`) — the winner after its clauses, the
number kept, the lines under it — and when a water's line restates its region's (`restates`)."""
from __future__ import annotations

import json
import re
from typing import Dict, List, Optional, Tuple
from pipeline.deliver.answers import common
from pipeline.deliver.answers.common import AnswersError
from pipeline.deliver.answers.display import is_closure_gate
from pipeline.common import entry_id as EID
from pipeline.deliver.answers.rows.rule import INF, R, _bands, _ge, _js, _keepish, size_lines


def _stable_sort(xs, key):
    return sorted(xs, key=key)


def closure_order(r) -> tuple:
    """Which of several closures speaking for a fish governs: the nearest rank, then the one naming
    the most fish (the page's own order, settle.ts `closing`). Never the entry key: Region 4's
    invasive-species notice (bass, pike, walleye, perch) ties on rank with the quota box's "0, CLOSED"
    lines, and breaking that by key split the one folded row into five when `z4:species_quotas`
    became `z4:daily_quotas` (T206, owner 2026-10-26)."""
    return (r.rank, -len(r.species))


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
    closed = _stable_sort([r for r in A if is_closure_gate(r.f, r.k)], key=closure_order)
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
    if win.k == "gate":
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
    if P.steelhead_water and S == "ST":
        return 50, INF
    return 0, INF


def _range_released(P: "Part", S: str, A: List[R]) -> Optional[R]:
    """Decision F16: a fish whose every length on this part is released by the take-0 bands of the
    rules speaking for it TOGETHER is released, whatever its pool says — Marshall (Lonzo) Creek's
    "Trout/char 20-30 cm" beside Region 2's "Dolly Varden … none under 60 cm" (p.24, p.21): no char
    may be kept there, not "1"; the Coquitlam's "Hatchery trout 2 (20-30 cm only)" on a steelhead
    water: no steelhead (rainbow over 50 cm) may be kept. The winner is the lowest-ranked (the
    water's own first) rule whose band releases part of the range. None where some length is kept."""
    lo, hi = fish_range(P, S)
    rel = sorted(((a, b, r) for r in A if r.k in ("size", "pool")
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
