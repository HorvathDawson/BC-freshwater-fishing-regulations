"""THE ROWS OF TODAY'S CARD — consumer Stages 5.2 step 5 to 5.8, from the reader's traced ladder.

The consumer page v35 (`reference/page_v35.js`, the SPECIFICATION ported here) turns the ladder's
speaking rules into the card an angler reads: the number they may keep after the winner's clauses
(5.2 step 5), the lines under it (steps 6-11), the fish grouped into one row per shared limit with
the fish that go back and the cross-references (5.3), the conditions on keeping (`rowConds`), the
real daily limit when every kind has its own smaller cap (5.7, "Really 2 a day here"), and each
kind's keep range and band numbers (5.8). This module computes exactly those, from:

  * the reader's traced verdict per fish and origin (the `ladder` section's value: `read.
    effective_rules_bound(trace=True, origin=…)`), which says which rule SPEAKS, which was
    displaced or lifted and by whom, and which is partly lifted;
  * the rules' own fields (the bundle's rule table) and D's display facts (`display.kind_of`, the
    page's 15-step kind; `display.bands`, the size bands);
  * the part: its water kind, whether a rainbow over 50 cm is a steelhead there, the steelhead
    presence and whether the steelhead rules apply.

IT NEVER RE-DECIDES A RULE. Which rules speak is the reader's; this module only arranges the
speakers into the page's answer (winner, number, lines, rows) the way the page arranges its own
ladder's. Every place the page is unclear, or where our reader and the page's ladder name things
differently, is written down in `DECISIONS`.

Words are the page's (it keeps presentation): every value here is structured — rule ids, fish
codes, numbers, line types, band edges. A rule is named by its id ("entry::rule"); the encoder
writes it as the export's rule index.
"""
from __future__ import annotations

import json
import math
from typing import Dict, List, Optional, Sequence, Tuple

from pipeline.deliver.answers import common
from pipeline.deliver.answers.common import AnswersError, expand
from pipeline.deliver.answers.display import bands as rule_bands
from pipeline.deliver.answers.display import kind_of

INF = math.inf

DECISIONS = [
    "R1 The speakers are the reader's (the `ladder` section, per origin). A rule our ladder "
    "marks `moot` (step 5b, a size clause under an outright release) gets the role `moot` with "
    "its `by` — what the page's own trailing loop gives the same clause under a release winner.",
    "R2 A rule's rung is the reader's place (`read.source_of(rule).rank`; a water row reached by "
    "the tributary walk ranks 1). The page ranks any walked rule of rank >= 0 as 1; the two "
    "agree for every walked rule of rank 0 (all walked rules in the corpus are water rows).",
    "R3 The page's order of a part's rules (its set's reach members, then its trib members, each "
    "by id) is kept: it breaks ties between equal ranks and orders lines and roles.",
    "R4 Fish asked about (5.1) are the page's `speciesAt` plus ST where the part has steelhead, a "
    "rule names ST and the export does not say `steelhead_rules: false`. A fish the page would "
    "ask that the ladder does not answer is a build error, never a skipped row.",
    "R5 Rows of an OPEN SUBJECT (protected species, salmon) come from gate rules that SPEAK when "
    "the reader is asked about the subject's first named fish (the rule's first protected code, "
    "chinook for SALMON, the group itself when it names none): the page's `ctx.active`.",
    "R6 A lift note (`liftNotes`, 'Lifted only when fishing for …') is the page's settle fact: an "
    "`exempts` entry naming a target, a means or sizes, of a rule in force today on this kind of "
    "water (`read.in_force` 'yes'; not in an undrawn part, not one side of the channel), whose own "
    "dates hold — for the winning rule, whatever fish is asked. It decides nothing (the reader "
    "already left the rule standing, partly lifted).",
    "R7 Fish names sort as the page's `localeCompare` does (ICU root collation: case and "
    "punctuation are secondary to letters).",
    "R8 The real daily limit (5.7) reads the general 'only K over X cm' cap structurally: the first "
    "general cap condition of the row (no fish list) on a band from X cm up — what the page finds "
    "by matching its own sentence.",
]


# --------------------------------------------------------------------------------------------
# Rules as the page reads them
# --------------------------------------------------------------------------------------------

class R:
    """One rule of the part, with what the page derives once (`species`, `k`, `rank`, `wins`)."""
    __slots__ = ("key", "x", "f", "k", "rank", "species", "except_", "via", "entry_id", "rule_id",
                 "type", "dimension", "wins")

    def __init__(self, key: str, x: dict, via: str):
        self.key, self.x, self.f, self.via = key, x, x, via
        self.entry_id, self.rule_id = x["entry"], x["rule"]
        self.type, self.dimension = x.get("type"), x.get("dimension")
        self.k = kind_of(x)
        base = x["_rank"]
        self.rank = 1 if via == "trib" and base == 0 else base
        self.species = expand(x.get("species"))
        self.except_ = expand(x.get("species_except"))
        self.wins = common.when_dates(x.get("when"))

    def take(self):
        return self.f.get("take")


def _closed_gate(r: R) -> bool:
    v = r.f.get("may_target")
    return r.k == "gate" and v is not None and not v


def _keepish(s: Optional[str]) -> bool:
    return s in ("keep", "nolimit")


def _covers(r: R, S: str) -> bool:
    return S in r.species and S not in r.except_


def _ge(a, b) -> bool:
    """JavaScript `a >= b` for a number b and a possibly-missing a (null reads 0; undefined is
    never >=)."""
    if a is None:
        return False
    return a >= b


def _bands(r: R) -> List[Tuple[float, float, Optional[float]]]:
    return [(a, INF if b is None else b, t) for a, b, t in rule_bands(r.x)]


def size_lines(r: R, pool_take, into: list) -> None:
    """The page's `sizeLines`: a band of take 0 is a release line, a band keeping fewer than the
    day (or any band of a clause) a cap line."""
    for a, b, t in _bands(r):
        if t == 0:
            into.append({"t": "rel", "a": a, "b": b, "r": r})
        elif t is not None and t > 0 and (r.f.get("within") or t < pool_take):
            into.append({"t": "cap", "a": a, "b": b, "take": t, "r": r})


# --------------------------------------------------------------------------------------------
# 5.2 One answer per fish and origin (the page's evalSp)
# --------------------------------------------------------------------------------------------

def _stable_sort(xs, key):
    return sorted(xs, key=key)


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
                and r.k not in ("while", "exempt", "standing") and r.dimension != "lift"
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
    closed = _stable_sort([r for r in A if _closed_gate(r)], key=lambda r: r.rank)
    rel = _stable_sort([r for r in A if r.k == "gate" and not _closed_gate(r)], key=lambda r: r.rank)

    def tk(r):
        return 1e9 if r.f.get("unlimited") else r.f.get("take")
    by_take = _stable_sort(pools, key=lambda r: (tk(r), r.rank))
    win = (closed or rel or by_take or [None])[0]
    if win is None:
        return None
    if win.k == "gate":
        status = "closed" if _closed_gate(win) else "release"
    else:
        status = "nolimit" if win.f.get("unlimited") else "keep"
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
            if r.k == "annual":
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
            setr(r, "falls", p if p in P.all_rules else None)
        elif r.k in ("size", "subcap", "sizecap", "annual", "duty"):
            setr(r, "moot", win.key)
    return res


def main_res(H: Optional[dict], W: Optional[dict]) -> Optional[dict]:
    if H and _keepish(H["status"]):
        return H
    if W and _keepish(W["status"]):
        return W
    return H or W


# --------------------------------------------------------------------------------------------
# 5.1 / 5.3 The model: fish asked about, rows (the page's speciesAt and buildModel)
# --------------------------------------------------------------------------------------------

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
    if t == "annual":
        r = l["r"]
        return f"yr{_js(r.f.get('take'))}|{json.dumps(r.f.get('lengths') or '', separators=(',', ':'))}|{r.f.get('origin') or ''}"
    if t == "cap":
        return f"cap{_js(l['a'])}-{_js(l['b'])}-{_js(l['take'])}-{l['r'].key}"
    return t + l["r"].key


def _js(v) -> str:
    """A number as JavaScript prints it in a template string."""
    if v is None:
        return "null"
    if v == INF:
        return "Infinity"
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def _line_key(res: Optional[dict]) -> str:
    if not res:
        return "-"
    return res["status"] + res["win"].key + _js(res["daily"]) + ",".join(
        l["t"] + l["r"].key + ("" if l.get("a") is None else _js(l["a"])) for l in res["lines"])


FACT_ORDER = ["origin", "origin2", "caution", "steel", "exc", "xref", "rel", "cap", "also", "subcap",
              "outer", "outercap", "outersize", "orphan", "partly", "annual", "record", "duty",
              "tnote"]


def species_at(P: "Part") -> List[str]:
    from pipeline.regs.parsing.catalogue import PROTECTED_FISH
    s: List[str] = []
    for r in P.cands:
        if P.applies(r) and r.type == "retention_limit" \
                and r.k in ("gate", "pool", "subcap", "sizecap", "size", "annual") \
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
                if v["role"] in ("replaced", "contains") and k in keep_rows and k != m["win"].key:
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
                elif other["daily"] != m["daily"] or other["win"] is not m["win"]:
                    mn = [l["b"] for l in other["lines"] if l["t"] == "rel" and l["a"] == 0]
                    mx = [l["a"] for l in other["lines"] if l["t"] == "rel" and l["b"] == INF]
                    add({"t": "origin2", "o": other["o"], "daily": other["daily"],
                         "min": max(mn) if mn else None, "max": min(mx) if mx else None,
                         "r": other["narrow"] or other["win"]}, S)
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
        mt = r.f.get("may_target")
        k = "open|" + ("PROT" if pr else ",".join(sp)) + ("false" if mt is not None and not mt
                                                          else "undefined" if mt is None else "true")
        if k not in rest:
            rest[k] = {"kind": "closed" if (mt is not None and not mt) else "release", "win": r,
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


# --------------------------------------------------------------------------------------------
# rowConds, speciesItems, quotaLines, keepRange, originLines, effCap (5.6-5.8)
# --------------------------------------------------------------------------------------------

def _who_n(l: dict, row: dict) -> Optional[List[str]]:
    if not l.get("members") or l.get("general") or len(l["members"]) == len(row["members"]):
        return None
    return list(l["members"])


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
            e["of"] = w                         # the fish the cap is written for (page: "(…)")
            e["general"] = w is None and l["a"] > 0 and l["b"] == INF
        if l["t"] == "origin2":
            e.update(o=l["o"], daily=l["daily"], min=l["min"], max=l["max"])
        if l["t"] == "subcap":
            e["members"] = list(l["members"])
        conds.append(e)
    return {"conds": conds, "n": n, "hw": any(l["t"] == "origin" for l in back),
            "extra": [l for l in allf if l["t"] in ("annual", "outer")]}


def _short(l: dict) -> bool:
    """Whether the page's `fact(l).short` is a non-empty text (it lists the line under a kind)."""
    return l["t"] not in ("partly", "tnote", "outercap", "outersize") and l["t"] in (
        "origin", "subcap", "cap", "rel", "annual", "record", "duty", "exc", "xref", "outer",
        "also", "origin2", "orphan", "caution", "steel")


_COVER = ("cap", "rel", "subcap", "origin", "outercap", "outersize", "tnote", "partly")


def species_items(P: "Part", model: dict, row: dict, rc: dict) -> List[dict]:
    """The page's `speciesItems`: the fish of a keep row grouped as the card shows them."""
    with_who = [c for c in rc["conds"] if c.get("who")]
    used = set()
    items = []
    for gi, g in enumerate(row["groups"]):
        cs = [c for c in with_who if sorted(c["who"]) == sorted(g["members"])
              and len(c["who"]) == len(g["members"])]
        for c in cs:
            used.add(id(c))
        items.append({"members": g["members"], "cs": cs, "facts": g["facts"], "key": f"g{gi}"})
    for i, c in enumerate(c for c in with_who if id(c) not in used):
        items.append({"members": c["who"], "cs": [c], "facts": None, "key": f"x{i}"})
    for it in items:
        others = [l for l in (it["facts"] or []) if (not it["cs"] or l["t"] not in _COVER)
                  and _short(l)]
        it["sub"] = next((c for c in it["cs"] if c.get("sub") is not None and c["c"] == "group"),
                         None)
        it["back"] = next((c for c in it["cs"] if c["c"] == "back"), None)
        it["has_lines"] = bool(it["cs"]) or (not it["back"] and bool(others)) or bool(it["sub"])
        it["only_steel"] = bool(it["facts"]) and not it["cs"] and all(
            l["t"] in ("steel", "tnote") for l in it["facts"])
    out = []
    for it in items:
        if it["only_steel"]:
            home = next((o for o in items if o is not it and all(S in o["members"]
                                                                 for S in it["members"])), None)
            if home:
                if any(l["t"] == "steel" for l in it["facts"]):
                    home["has_lines"] = True
                    home.setdefault("notes", []).append("steel")
                continue
        out.append(it)
    items = sorted([it for it in out if it["has_lines"]], key=lambda it: -len(it["members"]))
    sti = next((i for i, it in enumerate(items) if it["members"] == ["ST"]), -1)
    rbi = next((i for i, it in enumerate(items) if "RB" in it["members"]), -1)
    if sti >= 0 and rbi >= 0 and sti != rbi + 1:
        st = items.pop(sti)
        j = next(i for i, it in enumerate(items) if "RB" in it["members"])
        items.insert(j + 1, st)
    seen = {S for it in items for S in it["members"]}
    R = model["R"]
    rest = [S for S in row["allMembers"] if S not in seen and S in R
            and _keepish((main_res(R[S]["H"], R[S]["W"]) or {}).get("status"))]
    if rest:
        items.append({"members": rest, "cs": [], "facts": None, "key": "rest", "plain": True,
                      "sub": None, "back": None})
    return items


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
    return (lo, hi) if anyk else None


def quota_lines(P: "Part", model: dict, it: dict, row: dict, rc: dict) -> Optional[List[dict]]:
    """The page's `quotaLines`: an item's keep range split into bands with their own number."""
    if row["kind"] != "keep" or it.get("back") or any(l["t"] == "xref" for l in (it["facts"] or [])):
        return None
    r = keep_range(P, model, it["members"])
    if not r:
        return None
    n = rc["n"]
    base = min(n, it["sub"]["sub"] if it.get("sub") else n)
    nw = row["narrow"]
    R = model["R"]
    if nw is not None and nw.f.get("water") == P.kind and P.in_dates(nw) and all(
            _covers(nw, S) and S in R and any(
                ((R[S][o] or {}).get("roles") or {}).get(nw.key, {}).get("role") != "lifted"
                for o in ("H", "W"))
            for S in it["members"]):
        base = min(base, nw.f.get("take"))
    for l in list(it["facts"] or []) + row["everyone"]:
        if l["t"] in ("outercap", "subcap") and l["r"].f.get("take") is not None and all(
                (S in l["members"] if l.get("members") else True) and _covers(l["r"], S)
                for S in it["members"]):
            base = min(base, l["r"].f["take"])
    own = [l for l in (it["facts"] or []) if l["t"] == "cap" and l.get("take") is not None]
    gen = [l for l in row["everyone"] if l["t"] == "cap" and l.get("take") is not None
           and not any(o["a"] == l["a"] and o["b"] == l["b"] for o in own)]
    caps = own + gen
    pts = {r[0], r[1]}
    for c in caps:
        for v in (c["a"], c["b"]):
            if v is not None and r[0] < v < r[1]:
                pts.add(v)
    Pp = sorted(pts)
    out: List[dict] = []
    for i in range(len(Pp) - 1):
        a, b = Pp[i], Pp[i + 1]
        mid = a + 1 if b == INF else (a + b) / 2
        mx = min([base] + [c["take"] for c in caps
                           if mid >= (c["a"] or 0) and mid <= (INF if c["b"] is None else c["b"])])
        if out and out[-1]["mx"] == mx:
            out[-1]["b"] = b
        else:
            out.append({"a": a, "b": b, "mx": mx})
    return out


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


def eff_cap(P: "Part", model: dict, row: dict, rc: dict) -> Optional[dict]:
    """The real daily limit (5.7, the page's `effCap`): when every kind of the row has its own
    smaller cap, the real most is their sum, less what a shared 'only K over X cm' allows."""
    if row["kind"] != "keep" or not row["pool"]:
        return None
    n = row["daily"]
    items = species_items(P, model, row, rc)
    if not items:
        return None
    allm = [S for it in items for S in it["members"]]
    if len(set(allm)) != len(allm):
        return None
    caps = []
    for it in items:
        q = quota_lines(P, model, it, row, rc)
        xref = next((l for l in (it["facts"] or []) if l["t"] == "xref"), None)
        if q:
            lo = q[0]["a"]
        elif xref:
            lo = min(min([INF] + [x.get("lo") or 0 for x in origin_lines(P, model, S)
                                  if not x.get("rel")]) for S in it["members"])
        else:
            lo = 0
        c = min(n, max(x["mx"] for x in q)) if q else (min(xref["daily"], n) if xref else
                                                         (0 if it.get("back") else n))
        caps.append({"it": it, "lo": lo, "c": c})
    capped = [x for x in caps if x["c"] < n and x["c"] > 0]
    open_ = [x for x in caps if x["c"] >= n]
    total = sum(x["c"] for x in capped)
    shared = None
    gc = next((c for c in rc["conds"] if c["c"] == "cap" and not c.get("who") and c.get("general")),
              None)
    if gc:
        K, X = gc["take"], gc["a"]
        over = [x for x in capped if x["lo"] >= X
                and not ("ST" in x["it"]["members"] and P.steelhead_water)]
        os_ = sum(x["c"] for x in over)
        if len(over) > 1 and os_ > K:
            total -= os_ - K
            shared = {"take": K, "over_cm": X}
    if not capped or (total >= n and not open_):
        return None
    return {"n": n, "all": not open_, "sum": None if open_ else total, "capped_sum": total,
            "rb": any("RB" in x["it"]["members"] for x in capped), "shared_cap": shared,
            "capped": [S for x in capped for S in x["it"]["members"]],
            "open": [S for x in open_ for S in x["it"]["members"]]}


# --------------------------------------------------------------------------------------------
# The part, one reading
# --------------------------------------------------------------------------------------------

class Part:
    """What the rows read of one part on one reading: its rules in the page's order, its water
    kind and steelhead facts, the reader's verdict per fish and origin."""

    def __init__(self, B, set_id: int, steelhead_water: bool, steelhead_rules: bool, kind,
                 presence, ladder_value: dict, md, open_states: Optional[dict] = None):
        self.B = B
        self.kind, self.presence = kind, presence
        self.steelhead_water = steelhead_water
        self.steelhead_rules = steelhead_rules
        # the export says `steelhead_rules: false` only on a KNOWN part none applies to
        self.rules_off_in_export = presence == "known" and not steelhead_rules
        self.md = md
        bound = B.sets[set_id]
        reach = [(e, r, v) for e, r, v in bound if v == "reach"]
        trib = [(e, r, v) for e, r, v in bound if v == "trib"]
        self.cands: List[R] = []
        for e, r, v in reach + trib:                       # decision R3: the page's order
            x = B.rules[(e, r)]
            if x.get("family") == "information":
                continue
            self.cands.append(R(f"{e}::{r}", x, v))
        self.by_key = {r.key: r for r in self.cands}
        self.all_rules = {f"{e}::{r}" for e, r in B.rules}
        self.ladder = ladder_value
        self.open_states = open_states or {}
        self._notes: Optional[dict] = None

    def applies(self, r: R) -> bool:
        return not (r.f.get("water") and r.f["water"] != self.kind)

    def in_dates(self, r: R) -> bool:
        """The rule's dates hold today (the reader's calendar, `read.in_force`; some hours count —
        the page reads only the dates here)."""
        from pipeline.deliver.bundle import read
        return read.in_force(r.f.get("when"), self.md) != "no"

    def sp_name(self, S: str) -> str:
        return common.sp_name(S)

    def verdict(self, S: str, o: str) -> dict:
        v = self.ladder.get(S)
        if v is None:
            raise AnswersError(f"rows: the ladder does not answer {S} for this part (decision R4)")
        return v[o]

    def lift_notes(self, S: str, o: str, win: R) -> List[dict]:
        return self.lift_notes_of(win)

    def lift_notes_of(self, win: R) -> List[dict]:
        """Decision R6: the qualified lifts of rules in force here today that name `win`."""
        from pipeline.deliver.bundle import read
        if self._notes is None:
            self._notes = {}
            for L in self.cands:
                if not self.applies(L) or read.in_force(L.f.get("when"), self.md) != "yes" \
                        or read.not_yet_mapped(L.f) or L.f.get("side"):
                    continue
                for e in L.f.get("exempts") or []:
                    t = f"{e['entry_id']}::{e['rule_id']}"
                    if t == L.key or ("when" in e and read.in_force(e["when"], self.md) == "no"):
                        continue
                    q = {k: e[k] for k in ("when_targeting", "while", "lengths") if e.get(k)}
                    if q:
                        self._notes.setdefault(t, []).append({"by": L.key, "q": q})
        return self._notes.get(win.key, [])

    def open_gates(self) -> List[R]:
        """Decision R5: the open-subject gate rules that speak (the page's `ctx.active`)."""
        from pipeline.regs.parsing.catalogue import PROTECTED_FISH, SPECIES_GROUPS, OPEN_SUBJECTS
        out = []
        for r in self.cands:
            sp = r.f.get("species") or []
            if not (r.type == "retention_limit" and r.k == "gate" and sp and not r.f.get("while")
                    and self.applies(r)):
                continue
            if not all((c in OPEN_SUBJECTS and c != "ALL_FIN_FISH") or c in PROTECTED_FISH
                       for c in sp):
                continue
            st = self.open_states.get(r.key)
            if st is None:
                raise AnswersError(f"rows: no reader answer for the open-subject rule {r.key}")
            if st[0] == "speaks":
                out.append(r)
        out.sort(key=lambda r: r.rank)
        return out


def open_fish(x: dict) -> str:
    """The fish the reader is asked about for an open-subject rule (decision R5)."""
    from pipeline.regs.parsing.catalogue import SALMON_FISH
    sp = list(x.get("species") or [])
    for c in sp:
        if c not in ("SALMON", "PROTECTED_SPECIES", "ALL_FIN_FISH"):
            return c
    if "SALMON" in sp:
        return next(f for f, g in sorted(SALMON_FISH.items()) if g == "SALMON")
    return sp[0]


# --------------------------------------------------------------------------------------------
# Wire projection: rule objects -> rule ids, infinity -> null
# --------------------------------------------------------------------------------------------

def _num(v):
    if v is None or v == INF:
        return None
    if isinstance(v, float) and v.is_integer():
        return int(v)
    return v


_LINE_FIELDS = ("a", "b", "take", "o", "keepO", "status", "daily", "min", "max", "says", "only",
                "capped", "carve")


def line_out(l: dict) -> dict:
    out = {"t": l["t"], "r": l["r"].key}
    for k in _LINE_FIELDS:
        if k in l:
            out[k] = _num(l[k]) if k in ("a", "b", "take", "daily", "min", "max") else l[k]
    if "outer" in l:
        out["outer"] = l["outer"].key
    return out


def res_out(res: Optional[dict]) -> Optional[dict]:
    if res is None:
        return None
    return {"status": res["status"], "win": res["win"].key, "daily": _num(res["daily"]),
            "narrow": res["narrow"].key if res["narrow"] else None,
            "lines": [line_out(l) for l in res["lines"]],
            "roles": [[k, v["role"], v["by"]] for k, v in res["roles"].items()],
            "lift_notes": [[n["by"], n["q"]] for n in res["liftNotes"]]}


def fact_out(l: dict) -> dict:
    out = line_out(l)
    out["members"] = list(l["members"])
    out["rules"] = [r.key for r in l["rules"]]
    if l.get("general"):
        out["general"] = True
    if l.get("carveOf") is not None:
        out["carve_of"] = [_num(l["carveOf"]["a"]), _num(l["carveOf"]["b"]),
                           _num(l["carveOf"]["take"])]
    return out


def cond_out(c: dict) -> dict:
    out = {}
    for k, v in c.items():
        if k == "r":
            out["r"] = v.key if v is not None else None
        elif k in ("a", "b", "take", "sub", "daily", "min", "max"):
            out[k] = _num(v)
        elif k == "keep":
            out[k] = [{kk: _num(vv) if kk in ("a", "b") else vv for kk, vv in p.items()}
                      for p in v]
        else:
            out[k] = v
    return out


def scope_of(P: "Part", row: dict) -> Optional[dict]:
    """The row badge (5.5): whose count the pool is — this water's, the region's or B.C.'s — and
    whether a stream (or lake) share of it holds here."""
    if not row.get("pool") or row["kind"] != "keep":
        return None
    p = row["pool"]
    who = "bc" if p.rank == 4 else "region" if p.rank >= 3 else "water"
    n = row["narrow"]
    share = bool(who != "water" and n is not None and n.f.get("water")
                 and n.f["water"] == P.kind and n.f.get("take") < p.f.get("take"))
    return {"of": who, "entry": p.entry_id if who != "water" else None, "share": share}


def produce(P: Part) -> dict:
    """Everything the card's rows show, for one part on one reading: per fish and origin the
    decided answer (`fish`), and the rows with their conditions, items and real daily limit."""
    model = build_model(P)
    fish = {S: {"hatchery": res_out(v["H"]), "wild": res_out(v["W"])} for S, v in model["R"].items()}
    rows = []
    for row in model["rows"]:
        o = {"kind": row["kind"], "pool": row["pool"].key if row.get("pool") else None,
             "win": row["win"].key if row.get("win") else None,
             "members": list(row["members"]), "all_members": list(row["allMembers"]),
             "daily": _num(row["daily"]),
             "narrow": row["narrow"].key if row.get("narrow") else None,
             "everyone": [fact_out(l) for l in row["everyone"]],
             "groups": [{"members": g["members"], "facts": [fact_out(l) for l in g["facts"]]}
                        for g in row["groups"]],
             "prot": row.get("prot"), "wins": [r.key for r in row["wins"]] if row.get("wins") else None,
             "lift_notes": [[n["by"], n["q"]] for n in row["liftNotes"]],
             "scope": scope_of(P, row)}
        if row.get("pool"):
            rc = row_conds(P, model, row)
            o["conds"] = [cond_out(c) for c in rc["conds"]]
            items = species_items(P, model, row, rc)
            o["items"] = []
            for it in items:
                q = quota_lines(P, model, it, row, rc)
                xref = next((l for l in (it["facts"] or []) if l["t"] == "xref"), None)
                io = {"members": list(it["members"]),
                      "bands": [[_num(x["a"]), _num(x["b"]), _num(x["mx"])] for x in q] if q else None,
                      "back": bool(it.get("back")), "xref": bool(xref),
                      "sub": _num(it["sub"]["sub"]) if it.get("sub") else None}
                if xref and len(it["members"]) == 1:
                    io["origins"] = [{k: _num(v) if k in ("n", "lo", "hi") else v
                                      for k, v in x.items()}
                                     for x in origin_lines(P, model, it["members"][0])]
                o["items"].append(io)
            ec = eff_cap(P, model, row, rc)
            o["real_daily"] = ec
        rows.append(o)
    from pipeline.deliver.answers.display import steelhead_line
    has_row = not P.rules_off_in_export and any("ST" in r["allMembers"] for r in model["rows"])
    return {"spp": model["spp"], "fish": fish, "rows": rows,
            "steelhead_line": steelhead_line(P.presence, has_row)}


# --------------------------------------------------------------------------------------------
# The `rows` section of the answers file
# --------------------------------------------------------------------------------------------

def section_scope(key: tuple, B):
    """The card reads the rule key, the water's kind (which rules apply, a stream share holding
    here) and the steelhead presence (whether ST is asked about, the presence line)."""
    k = common.key_dict(key)
    return (common.rule_key(key), k["kind"], k["steelhead"])


def open_states(ctx, rk, md) -> dict:
    """Decision R5: the reader's state of every open-subject gate rule of the set, asked about the
    subject's first named fish: {rule id: (state, lifted_in_part_by)}."""
    from pipeline.deliver.bundle import read
    from pipeline.regs.parsing.catalogue import OPEN_SUBJECTS, PROTECTED_FISH
    ck = ("rows_open", rk, md)
    got = ctx.cache.get(ck)
    if got is not None:
        return got
    bound = ctx.sets[rk.set_id]
    by_fish: Dict[str, List[str]] = {}
    for e, r, _ in bound:
        x = ctx.rules[(e, r)]
        sp = x.get("species") or []
        if x.get("type") == "retention_limit" and sp and all(
                (c in OPEN_SUBJECTS and c != "ALL_FIN_FISH") or c in PROTECTED_FISH for c in sp):
            by_fish.setdefault(open_fish(x), []).append(f"{e}::{r}")
    out = {}
    for f, ks in sorted(by_fish.items()):
        ans = {read.rid(y): y for y in read.effective_rules_bound(
            bound, rk.steelhead_water, md, f, ctx.bundle,
            steelhead_rules_here=rk.steelhead_rules, trace=True)}
        for k in ks:
            y = ans.get(k)
            if y is None:
                raise AnswersError(f"rows: the reader does not answer {k} asked about {f}")
            out[k] = (y["state"], y.get("lifted_in_part_by"))
    ctx.cache[ck] = out
    return out


def to_refs(x, ref):
    """Every rule id ("entry::rule") in a value, as its export index (`ref`)."""
    if isinstance(x, str):
        return ref(x) if "::" in x else x
    if isinstance(x, list):
        return [to_refs(v, ref) for v in x]
    if isinstance(x, dict):
        return {k: to_refs(v, ref) for k, v in x.items()}
    return x


def section_derive(ladder_value: dict, scope, ctx, first_day: int) -> dict:
    """The card's rows for one part scope on one reading (the ladder's), rules by export index."""
    rk, kind, presence = scope
    md = common.month_day(first_day)
    P = Part(ctx.B, rk.set_id, rk.steelhead_water, rk.steelhead_rules, kind, presence,
             ladder_value, md, open_states(ctx, rk, md))
    return to_refs(json.loads(json.dumps(produce(P))), ctx.rule_index.__getitem__)
