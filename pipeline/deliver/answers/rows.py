"""THE ROWS OF TODAY'S CARD — consumer Stages 5.2 step 5 to 5.8, from the reader's traced ladder.

The consumer page v35 (`archive/pipeline/answers-reference-v35/page_v35.js`, the SPECIFICATION ported here) turns the ladder's
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
import re
from typing import Dict, List, Optional, Sequence, Tuple

from pipeline.deliver.answers import common
from pipeline.deliver.answers.common import AnswersError, expand
from pipeline.deliver.calendar import month_day
from pipeline.deliver.answers.display import bands as rule_bands
from pipeline.deliver.answers.display import is_closure_gate, kind_of
from pipeline.deliver.bundle import read
from pipeline.deliver.bundle.rules import catch_and_release, closure_grade
from pipeline.common import entry_id as EID

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
    "R8 The real daily limit (5.7) reads the general 'only K over X cm' caps structurally: every "
    "general cap condition of the row (no fish list) on a band from X cm up (F5d; the page reads "
    "only the first, by matching its own sentence).",
    # Rows fixes (2026-10-06, ROWS-REVIEW): where the page is wrong, the card follows the book.
    "F1 ONE ITEM PER KIND (review A1): each fish's bands, sub-limit, go-back, own-row reference "
    "and the row's conditions about it are worked out for that fish alone. Fish share an item "
    "when exactly these agree: whether it has lines, its bands, sub-limit, go-back, own-row "
    "reference (rule, number, `against`) and the indexes of the row's `conds` about it. Facts "
    "that are not conditions (steel, caution, also, orphan, tnote, partly, a release line that "
    "moves no band) do NOT split an item: they are not shipped on items, and each one names its "
    "own fish in the row's `everyone`/`groups`. The page builds items from fact groups, so a fish sat in two items and an "
    "item showed one range for fish with different ones (Anderson R., Jul: brook, brown and "
    "cutthroat 'keep 1 over 60 cm'; the book: up to 4, only 1 over 50).",
    "F1b A cap written for every fish of a row but rainbow trout, left out only because a "
    "rainbow over 50 cm is a steelhead there, is general — and counts that steelhead (amended "
    "2026-10-08: it was `except: [RB]`, which the page read as 'steelhead don't count' under "
    "Region 6's '1 over 50 cm (quota includes hatchery steelhead)', p.49).",
    "F2 origin2 (the other origin's own limit) also when only its keep RANGE differs (review A2: "
    "'No wild trout over 50 cm' on Chilliwack, Cultus, Harrison, … lakes), citing the release "
    "line that makes the difference; its condition names its fish (`who`).",
    "F3 A line is about every fish of the row only when its fish SET is the row's (review A3; the "
    "page compares counts: Kitimat R. in Feb lost the cutthroat and brown trout release).",
    "F4 A fish with its own row that is lifted out of (or replaced in) the row's narrowing clause "
    "counts toward the row's OUTER total (`items[].against`), not the narrowed number (review "
    "A4: Vedder hatchery rainbow 4 of Region 2's 4, not 'min(4, 2)').",
    "F5a A slot ('none between X and Y cm') is a keep band of 0 (review A5: Teslin, Tagish, "
    "Morley, Bennett). F5b The zone's outer size cap (`outersize`) caps a water's own bands "
    "like a cap (A6). F5c On a steelhead water a rainbow's keep range ends at 50 cm (A7, as "
    "`origin_lines`). F5d Every general shared cap counts (R8: Region 8's 'only 2 over 30 cm'), "
    "nested caps worked from the largest size down; `shared_cap` lists those that bind.",
    "F6 A possession limit with a number is `possession_cap` (line, role and fact), never a "
    "yearly limit (review B1: 'Per licence year: 1 lake trout in possession'). ONE source: "
    "`display.kind_of` returns `possession_cap`; rows read it as the rule's kind, display ships "
    "it as the rule's kind and words its sentence from it (page bug d10: the page says annual).",
    "F7 `scope.apart`: a water's (or area's) pool counts its fish apart from a wider total only "
    "when it lifts a wider POOL that binds here and no `outer` line is left (review B4: Vedder "
    "r9 and Kitimat r4/r5 lift only a stream share; their fish still count toward the region's).",
    "F8 An area's pool (rank 2: Haida Gwaii, Bowron Lake Park, the Liard watershed) is badged "
    "`of: area` with the area's entry, not 'this water' (review B3).",
    "F9 Every daily limit counts fish kept elsewhere today, a water's own too (the book's daily "
    "quota; review B2): the page's 'Only fish kept on this lake count' is page wording, wrong.",
    "F10 Fish asked about (R4) are the book's: the bundle holds no presence data for any fish but "
    "steelhead (stocking records are not presence), so the group wording stays (brown trout on "
    "Shuswap is named because the zone's trout row names it).",
    "F11 A COUNT limit shared by several kinds gives them that many TOGETHER (user test "
    "2026-10-08, Babine/Skeena): the real daily limit merges the kinds a count clause holds "
    "(the row's stream or lake share, a clause `within` the pool) into one unit worth its take, "
    "innermost first; `real_daily.shared_count` names the limits that bind. Region 6's '1 trout "
    "from streams Jul 1-Oct 31' (p.49): 'Only 1 of the 5 can be brown, cutthroat, rainbow trout "
    "or steelhead', not 3 (each kind's own 1, summed).",
    "F12 Which fish a general size cap does NOT count is decided here (`conds[].except`), never "
    "on the page (it was the page's `exemptOf`): a covered, kept fish whose answers give the cap "
    "no role or line; never a rainbow on a steelhead water where the cap counts steelhead. "
    "Steelhead leave a zone's trout/char size line only where the same table prints its own "
    "steelhead quota for that size (`catalogue.steelhead_scope_problems`): Regions 1 and 2, not "
    "3, 5, 6, 7 or 8.",
    # answers 2.5 (2026-10-10, the angler-eval round)
    "F13 A SIZE RELEASE IS AN ANSWER (bug: Vedder, May 15, p.22 'hatchery rainbow trout catch and "
    "release (50 cm or less)'): where no gate or pool speaks for a fish, a speaking take-0 size band "
    "that releases the fish's whole range on the part (a rainbow's ends at 50 cm on a steelhead "
    "water) decides it: status release, that rule the winner (`_size_release`). Before, the "
    "decided answer was null and the hatchery rainbow vanished from the card. One covering only "
    "part of the range is refused.",
    "F14 GROUP TOTALS ARE THE ROWS' (user 2026-10-10; the page's `groupBlock` derived '2 a day in "
    "total, all kinds together' and 'Kept 2 on another stream? Keep 0 here' from a narrowed "
    "number): a keep row carries `group` — the TOTAL its fish count toward (the row's pool, or the "
    "outer pool its fish are contained by, F4), every kind on the card counting toward it "
    "(steelhead included: user 2026-10-10), each sub-cap of the total that holds here (a stream "
    "share, a count clause, a size clause) with the kinds a water's own rule LIFTS out of it, and "
    "one plain sentence — on ONE row per total; and `elsewhere`, the kept-elsewhere line, qualified "
    "by those lifts ('Keep 0 here, except hatchery rainbow trout 50 cm or less: up to 4 in all').",
    # answers 2.8 (2026-10-12, ROUND9)
    "F15 THE CARD IS WATER-FIRST AND SAYS EACH FACT ONCE (owner 2026-10-12): `group` keeps the decision "
    "(pool, total, kinds, subcaps) and carries the card (`card.card_of`): the headline is this water's "
    "number with no lift, a lift is a positive line naming whose rule, the total a line under a share, "
    "the kinds grouped under the cap they draw from with over 50 cm on their rows; the 2.6 lead and the "
    "2.7 items are gone (they restated the rows). RULINGS Q23b: lifted fish count toward the stream "
    "tally (`elsewhere`, `worked`). Every number is proved by `card_check` against baskets kept under "
    "the rules and the reader's verdicts.",
    # round 9 card gate (2026-10-13)
    "F16 A FISH WHOSE EVERY LENGTH IS RELEASED IS RELEASED (`_range_released`): the take-0 bands of the "
    "rules speaking for it, together, cover its whole range on the part — Marshall (Lonzo) Creek's "
    "'Trout/char 20-30 cm' beside Region 2's char 'none under 60 cm' (no char may be kept), the "
    "Coquitlam's 'Hatchery trout 2 (20-30 cm only)' on a steelhead water (no steelhead). Before, the "
    "card said '1' and '2'. An ORPHAN count clause (another table's, still speaking: Region 5's '1 Dolly "
    "Varden' under Polley Lake's 'trout 8', RULINGS Q2) caps the fish's bands like a sub-cap.",
    # owner ruling 2026-10-21 (the Kitimat, ambiguity (b) of pipeline/tests/test_answers_kitimat_steelhead_lift.py)
    "F17 A STEELHEAD QUOTA THAT LIFTS A SIZE CLAUSE RAISES IT ('+1 for steelhead', `raises`): Region 6's "
    "'1 over 50 cm (quota includes hatchery steelhead)' (p.49) lifted for hatchery steelhead by the Kitimat's "
    "'Hatchery steelhead (>50 cm) daily quota = 2' (p.51) holds the other trout/char to 1 over 50 cm and "
    "counts EVERY fish over 50 cm, the steelhead included, toward one tally of the quota's 2 — the Vedder's "
    "'1 over 50 cm (2 hatchery steelhead over 50 cm allowed)' (p.21, Region 2's r2 + r2b), which the owner "
    "named as the same ('look at the prototype vedder it is similar'). Keep 2 hatchery steelhead and no other "
    "trout over 50 cm; keep 1 and 1 other may be. Before, the steelhead left the clause altogether "
    "(`except: [ST]` and nothing else) and 2 hatchery steelhead + 1 cutthroat over 50 cm was legal. The "
    "tally is said as the quota's (a row condition, a group sub-cap, the card's size tier 'Of these, only 1 "
    "may be over 50 cm (20 in) — 2 if one is a hatchery steelhead', the gate's engine and the cap system).",
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
        self.rank = read.place(x, via)
        self.species = expand(x.get("species"))
        self.except_ = expand(x.get("species_except"))
        self.wins = common.when_dates(x.get("when"))

    def take(self):
        return self.f.get("take")


def _keepish(s: Optional[str]) -> bool:
    return s in common.KEEPS


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
    closed = _stable_sort([r for r in A if is_closure_gate(r.f, r.k)], key=lambda r: r.rank)
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
    # ROUND15 3 (owner): a rule of the winner's OWN ROW saying the very same thing (fish, number, days,
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
    """ROUND15 3: `p` is a rule of the winner's own row stating exactly what the winner states."""
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
    """ROUND16 Q2: `p` states exactly what the winner states (same fish, number, days, sizes, origin, water)."""
    return p.key != win.key and all(json.dumps(p.f.get(k), sort_keys=True) == json.dumps(win.f.get(k), sort_keys=True)
                                    for k in _SAME_FIELDS)


def nearer_than(P: "Part", a: R, b: R) -> bool:
    """ROUND16 Q2 (owner, the T14 principle — closest wins): `a`'s row is nearer this stream than `b`'s — a's
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
    """ROUND15 3: the rules of a row's winners' own rows that restate them, as notes [{r, of, says}].
    ROUND16 Q2 (owner 2026-10-16): so is ANOTHER row's identical statement, no nearer the stream than the
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


def main_res(H: Optional[dict], W: Optional[dict]) -> Optional[dict]:
    """The fish's leading origin answer (T120: `common.lead_origin`, the one function)."""
    return common.lead_origin(H, W)


#: T127 (2026-10-22, found by the Perry Creek card-gate fix): a pool the reader DISPLACED for a fish yields no "counts
#: toward this one" cross-reference — `counts_toward` decides every cross-reference. False only in the mutation pin
#: (test_answers_t127_displaced_xref.py): the St. Mary's "trout/char 1 on all tributaries" then lists Perry Creek's brook
#: trout again, though Perry Creek's own "brook trout 20" displaced it (reason own_row).
A_DISPLACED_POOL_IS_NO_CROSS_REFERENCE = True


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


# --------------------------------------------------------------------------------------------
# rowConds, speciesItems, quotaLines, keepRange, originLines, effCap (5.6-5.8)
# --------------------------------------------------------------------------------------------

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


# --------------------------------------------------------------------------------------------
# The part, one reading
# --------------------------------------------------------------------------------------------

class Part:
    """What the rows read of one part on one reading: its rules in the page's order, its water
    kind and steelhead facts, the reader's verdict per fish and origin."""

    def __init__(self, B, set_id: int, steelhead_water: bool, steelhead_rules: bool, kind,
                 presence, ladder_value: dict, md, open_states: Optional[dict] = None,
                 at=None):
        self.B = B
        self.kind, self.presence = kind, presence
        self.steelhead_water = steelhead_water
        self.steelhead_rules = steelhead_rules
        # the export says `steelhead_rules: false` only on a KNOWN part none applies to
        self.rules_off_in_export = presence == "known" and not steelhead_rules
        self.md = md
        #: the moment the reading is asked at (`calendar.Moment`; answers 2.1): a weekday or
        #: hours rule is in force at the moments it covers, out at the others
        self.at = at
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
        """M3: a rule bound here is for this kind of water — its binding enforced `water`
        (feature_types). One that is not is a binding defect: refused, never filtered."""
        if r.f.get("water") and self.kind and r.f["water"] != self.kind:
            raise AnswersError(f"rows: {r.key} is for {r.f['water']}s, bound to a {self.kind}")
        return True

    def in_dates(self, r: R) -> bool:
        """The rule's dates hold today (the reader's calendar, `read.in_force`; some hours count —
        the page reads only the dates here)."""
        from pipeline.deliver.bundle import read
        return read.in_force(r.f.get("when"), self.md, self.at) != "no"

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
                if not self.applies(L) or read.in_force(L.f.get("when"), self.md, self.at) != "yes" \
                        or read.not_yet_mapped(L.f) or L.f.get("side"):
                    continue
                for e in L.f.get("exempts") or []:
                    t = f"{e['entry_id']}::{e['rule_id']}"
                    if t == L.key or ("when" in e
                                      and read.in_force(e["when"], self.md, self.at) == "no"):
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
                    and not r.f.get("caught") and self.applies(r)):
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


def stored_role(P: "Part", key: str, role: str) -> str:
    """rows 31 (owner rule 2026-10-21; page v36 ladderHtml): a RECORD-IT RULE THAT DECIDES IS A DUTY. A rule with
    `record_retention` holding a deciding role (display `WINS`: Kootenay Lake's "Write each rainbow trout over 50 cm
    on your licence", Shuswap's two stamp records, both `floor`) is stored as `duty`, so a row's "How this was
    decided" lists it under "Other rules" — page v36 moved it there in the page; the app only reads the role. The
    computation keeps its own role (sizes, totals); only the stored answer says duty."""
    from pipeline.deliver.answers.display import WINS
    if role not in WINS or role == "duty":
        return role
    e, r = key.split("::", 1)
    return "duty" if P.B.rules[(e, r)].get("record_retention") else role


def res_out(res: Optional[dict], P: "Part") -> Optional[dict]:
    if res is None:
        return None
    return {"status": res["status"], "win": res["win"].key, "daily": _num(res["daily"]),
            "narrow": res["narrow"].key if res["narrow"] else None,
            "lines": [line_out(l) for l in res["lines"]],
            "roles": [[k, stored_role(P, k, v["role"]), v["by"]] for k, v in res["roles"].items()],
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


def badge_of(r: R) -> str:
    """Whose count a pool is (M2): from the rule's two typed axes (`read.source_of`: authority
    and scope), never the folded rank — a rule reached by the tributary walk is the water's; an
    area's pool is the area's; a region-wide one the region's, or B.C.'s when the province
    wrote it; a superior authority's (a national park's) is the place it names: its area, or the
    water."""
    from pipeline.deliver.bundle import read as RD
    src = RD.source_of(r.x)
    if r.via == "trib" or src.scope in (RD.Scope.water, RD.Scope.inherited):
        return "water"
    if src.scope is RD.Scope.area:
        return "area"
    return "bc" if src.authority is RD.Authority.province else "region"


def scope_of(P: "Part", row: dict) -> Optional[dict]:
    """The row badge (5.5): whose count the pool is — this water's, an area's (decision F8:
    Haida Gwaii, Bowron Lake Park, the Liard watershed), the region's or B.C.'s — whether a
    stream (or lake) share of it holds here, and (decision F7) whether the row's fish are
    counted apart from a bigger total: the pool lifts a wider pool that binds here and no
    `outer` line (a total they still count toward) is left on the row."""
    if not row.get("pool") or row["kind"] != "keep":
        return None
    p = row["pool"]
    who = badge_of(p)
    n = row["narrow"]
    share = bool(who != "water" and n is not None and n.f.get("water")
                 and n.f["water"] == P.kind and n.f.get("take") < p.f.get("take"))
    apart = False
    if who in ("water", "area"):
        lifts = [P.by_key.get(f"{e['entry_id']}::{e['rule_id']}") for e in p.f.get("exempts") or []]
        outer = any(l["t"] == "outer" for l in
                    row["everyone"] + [l for g in row["groups"] for l in g["facts"]])
        apart = any(t is not None and t.k == "pool" and t.rank >= 2 for t in lifts) and not outer
    # rows 31 (owner rule 2026-10-21; page v36 narrowAll): the narrowing clause binds EVERY fish of the row — the
    # badge then says "All Region 2 streams", not "Region 2-wide" (the app reads it, never re-derives it)
    narrow_all = n is not None and all(_covers(n, S) for S in row["members"])
    return {"of": who, "entry": p.entry_id if who != "water" else None, "share": share,
            "apart": apart, "narrow_all": narrow_all}


# --------------------------------------------------------------------------------------------
# Group totals and the kept-elsewhere line (decision F14, answers 2.5)
# --------------------------------------------------------------------------------------------

def _kept(model: dict, S: str) -> Optional[dict]:
    """The fish's decided answer as the card reads it (the main origin's), when it keeps."""
    R_ = model["R"].get(S) or {}
    m = main_res(R_.get("H"), R_.get("W"))
    return m if m and _keepish(m["status"]) else None


def total_of(P: "Part", model: dict, row: dict) -> R:
    """The TOTAL a keep row's fish count toward (decisions F4, F14): the outer pool every member's
    answer is contained by (Vedder r9's hatchery rainbow: Region 2's 4), else the row's pool."""
    outs: Optional[List[str]] = None
    for S in row["members"]:
        m = _kept(model, S)
        ks = [k for k in m["roles"] if k != m["win"].key and counts_toward(m, k)] if m else []
        outs = ks if outs is None else [k for k in outs if k in ks]
    return P.by_key[outs[0]] if outs else row["pool"]


def _names(codes: Sequence[str], conj: str = " or ") -> str:
    g = common.group_for(codes)
    if g:
        return common.lc(g)
    return common.join([common.lc(common.sp_name(c)) for c in codes], conj)


def _names_short(codes: Sequence[str]) -> str:
    """ROUND16 2: the fish named shortly — a closed group by its name, one missing a member or two as the
    group "other than" them ("trout and char other than steelhead"), else each fish."""
    g = common.group_for(codes)
    if g:
        return common.lc(g)
    s = set(codes)
    for name, members in common._closed_groups():
        rest = sorted(members - s)
        if len(s) >= 3 and s < members and len(rest) <= 2:
            from pipeline.deliver.answers.card import book_order
            return (f"{common.lc(common.sp_name(name))} other than "
                    f"{common.join([common.lc(common.sp_name(x)) for x in book_order(rest)], ' or ')}")
    return common.join([common.lc(common.sp_name(c)) for c in codes], " or ")


def _range_words(a, b) -> str:
    """A keep range in the book's words (bounds of a keep band are inclusive: "50 cm or less"), in cm AND inches
    (ROUND19 2: "50 cm (20 in) or less", "over 50 cm (20 in)", "20–30 cm (8–12 in)")."""
    if not a and (b is None or b == INF):
        return ""
    if not a:
        return f"{common.cm_in(b)} or less"
    if b is None or b == INF:
        return f"over {common.cm_in(a)}"
    return common.range_cm_in(a, b)


def _keep_range(r: R) -> Tuple[float, float]:
    """A rule's own keep range from its bands (a band of 0 or of no number keeps nothing)."""
    keep = [(a, b) for a, b, t in _bands(r) if t]
    return (keep[0][0], keep[-1][1]) if keep else (0, INF)


def _clauses(P: "Part", T: R) -> List[R]:
    """Every count or size clause of a total (`within` it, nested), in the part's rule order."""
    ids, out, grew = {T.rule_id}, [], True
    while grew:
        grew = False
        for r in P.cands:
            if r.f.get("within") and r.entry_id == T.entry_id and r.f["within"] in ids \
                    and r not in out:
                out.append(r)
                ids.add(r.rule_id)
                grew = True
    return [r for r in out if r.k in ("subcap", "sizecap")]


def _clause_take(c: R):
    """A clause's number: its own, or its band's ("[{min_cm: 50, take: 1}]")."""
    if c.f.get("take") is not None:
        return c.f["take"]
    return next((t for _, _, t in _bands(c) if t), None)


def _lift_words(P: "Part", by: str, fish: List[str], sized: bool = True) -> str:
    """The fish a water's rule lifts out of a sub-cap, said as that rule keeps them: "hatchery
    rainbow trout 50 cm or less" (Vedder r9), "rainbow trout" (Duncan r4). Out of a SIZE clause
    the fish alone (`sized` False): its own range is no part of an "over 50 cm" cap."""
    L = P.by_key[by]
    a, b = _keep_range(L)
    return " ".join(x for x in (L.f.get("origin") or "", _names(fish),
                                _range_words(a, b) if sized else "") if x)


def group_of(P: "Part", model: dict, T: R, own_row: bool = False) -> Optional[dict]:
    """One total on today's card (decision F14): `pool` the total's rule, `total` its number
    ("unlimited" for none), `kinds` every fish of the card counting toward it — the total's own
    row and every fish with its own row contained by it (F4; steelhead included, user 2026-10-10),
    `subcaps` each count or size clause of the total holding some of those kinds here
    ({r, take, water, over_cm, under_cm, of, lifted [{members, by}]} — `of` the kinds it holds,
    `lifted` those a water's own rule lifts out of it). None for a total of one kind with no
    sub-cap, unless that kind is a trout or char and the total is its row's own pool (`own_row`)
    on every kind of water (`A_TROUT_CHAR_TOTAL_OF_ONE_KIND_IS_A_GROUP`).
    THE DECISION ONLY: how the card SAYS it is `card.card_of` (answers 2.8 — the lead sentence
    and the items of 2.6/2.7 are gone: they restated the kind rows)."""
    kinds = [S for S in model["spp"] if counts_toward(_kept(model, S), T.key)]          # T127: the one predicate
    kinds.sort(key=lambda S: _icu_key(P.sp_name(S)))
    if not kinds:
        return None
    # the stream (or lake) share first, then the count clauses, then the size clauses
    chain = sorted(_clauses(P, T), key=lambda c: (0 if c.f.get("water") else 1 if c.k == "subcap"
                                                  else 2, -sum(_covers(c, S) for S in kinds),
                                                  _clause_take(c) or 0))
    in_chain = {c.key for c in chain} | {T.key}
    unlimited = bool(T.f.get("unlimited"))
    total = "unlimited" if unlimited else _num(T.f.get("take"))
    subcaps = []
    for c in chain:
        held, lifted = [], {}
        for S in kinds:
            m = _kept(model, S)
            v = m["roles"].get(c.key) or {}
            # a clause the card's lines fold into an equal-sized one of the same total (Region 2's
            # "2 over 50 cm" beside "1 over 50 cm, not steelhead": Q16) still holds the fish
            if m.get("narrow") is c or v.get("role") in ("narrows", "limit") \
                    or (v.get("role") == "replaced" and v.get("by") in in_chain):
                held.append(S)
            elif v.get("role") in ("lifted", "replaced") and v.get("by") \
                    and v["by"] not in in_chain and v["by"] in P.by_key:
                lifted.setdefault(v["by"], []).append(S)
        if not held:
            continue
        hit = next(((a, b, t) for a, b, t in _bands(c) if t), None) if c.k == "sizecap" else None
        band = hit[:2] if hit else None
        take = _clause_take(c)
        if not take:
            # a slot ("none between 36 cm and 44 cm", Teslin) keeps no number: a release band the
            # row's lines and the item's bands show (F5a), never a sub-cap of the total
            continue
        sc = {"r": c.key, "take": _num(take), "water": c.f.get("water"),
              "over_cm": _num(band[0]) if band and band[0] else None,
              "under_cm": _num(band[1]) if band and band[1] != INF else None,
              "of": held,
              "lifted": [{"members": fish, "by": by} for by, fish in lifted.items()]}
        # a size clause another of the total makes redundant on this card — the same band, holding
        # every kind it holds, with no larger number (Region 1's "2 over 50 cm" beside "1 over 50
        # cm" on a lake with no steelhead) — is not said twice
        if band and any(o["over_cm"] == sc["over_cm"] and o["under_cm"] == sc["under_cm"]
                        and set(held) <= set(o["of"]) and o["take"] <= sc["take"]
                        for o in subcaps):
            continue
        subcaps.append(sc)
        # decision F17: the tally a water's steelhead quota raises the clause to, over every fish the clause
        # holds AND the steelhead it lifted ("+1 for steelhead"; the Vedder's r2b beside its r2), as that
        # quota's — a sub-cap the card says with the clause (`raise_of`), never on its own
        if band and band[1] == INF:
            for x in _raise_of(P, c):
                fish = [S for S in kinds if S in held or (S in {F for F, _o in x.lifted}
                                                          and S in lifted.get(x.lifter.key, []))]
                if set(fish) - set(held):
                    subcaps.append({"r": x.lifter.key, "take": _num(x.f["take"]), "water": None,
                                    "over_cm": sc["over_cm"], "under_cm": None, "of": fish, "lifted": []})
    if len(kinds) < 2 and not subcaps \
            and not (A_TROUT_CHAR_TOTAL_OF_ONE_KIND_IS_A_GROUP and own_row and not T.f.get("water")
                     and set(kinds) <= _trout_char()):
        return None
    return {"pool": T.key, "total": total, "kinds": kinds, "subcaps": subcaps}


#: D19 (a), T80 3 (owner call 11, Q11: the backend answers, the app only reads): a TROUT/CHAR total of ONE kind with
#: no sub-cap is still a group WHERE THE TOTAL IS ITS ROW'S OWN POOL — its row's headline is what the app's Keep tab
#: draws a pooled row by (core `keep.ts`: a group, else a plain row with no pool). Without it the row was drawn nowhere:
#: Kootenay Lake — Main Body's rainbow 10 a day (klmb_1010), Tranquille Lake's rainbow, the Region 7 brook trout
#: lakes, Ross Lake's and 14 other waters' own brook trout quotas. Other fish rows keep no group: the Other fish list
#: draws them by its own compact path. A one-kind row whose total is ANOTHER rule's (Duncan River's Dolly Varden 2,
#: inside Region 4's 5) is not covered: a group of that total would say "5" over one kind that can never reach it (the
#: card gate refuses it); nor is a total limited to a kind of water (Region 8's "20 brook trout from streams", z8 trout_char_quota.r5): the card gate proves a zone
#: total over fish kept on lakes too, and its worked "kept elsewhere" line is off by one there. Both stay drawn nowhere
#: until T80's follow-up (the gate's places for a water-limited total; a contained one-kind row).
A_TROUT_CHAR_TOTAL_OF_ONE_KIND_IS_A_GROUP = True

#: T80 follow-up (batch-3 deliver, 2026-10-22): the one-kind trout/char group is for a row DRAWN ON ITS OWN — one no
#: other pool holds (`parent_of`, rows 31; core `drawnRows` draws no article for a held row). Perry Creek's "brook trout
#: 20" row is held by the St. Mary's "trout/char 1 on all tributaries" row (which cross-references the brook trout):
#: a group there was never drawn, and the card worded it "1" through that cross-reference while the reader had
#: DISPLACED the St. Mary's 1 for brook trout (20 may be kept) — the card gate's 30 problems. The held row's fish stay
#: said inside the holding pool's article, as before T80. `model.RowsFrame._one_kind_groups` refuses the shape.
#: (T127 then removed that displaced cross-reference: Perry Creek's row is held by nothing now and is a group of 20.)
A_ONE_KIND_GROUP_ONLY_ON_A_DRAWN_ROW = True


def _trout_char() -> frozenset:
    return common.trout_char()                          # the model (model.py) accepts by the same predicate


def raise_of(grp: dict, s: dict) -> Optional[dict]:
    """Decision F17: the size sub-cap of `grp` that sub-cap `s` RAISES — the clause at the same size whose
    fish `s`'s rule lifted out of it (`s` the tally over them all) — or None where `s` raises none."""
    return next((o for o in grp["subcaps"] if o is not s and o["over_cm"] is not None
                 and o["over_cm"] == s["over_cm"] and o["under_cm"] == s["under_cm"]
                 and any(L["by"] == s["r"] and set(L["members"]) <= set(s["of"]) for L in o["lifted"])
                 and set(o["of"]) < set(s["of"])), None)


def written_name_of(r: R) -> str:
    from pipeline.deliver.answers.display import written_name
    return written_name(r.x)


def _scope_name(sc: dict, pool: R) -> Optional[str]:
    """Whose count a badge says (the page's `scopeOf`): B.C., the region's or area's entry name."""
    if sc["of"] == "bc":
        return "B.C."
    if sc["of"] in ("region", "area"):
        return pool.x.get("entry_name") or ("the area" if sc["of"] == "area" else "the region")
    return None


def _kept_origin(model: dict, S: str) -> Optional[str]:
    m = _kept(model, S)
    if m is None:
        return None
    R_ = model["R"].get(S) or {}
    return "hatchery" if R_.get("H") is m else "wild"


def _line(q: str, a: str, model: dict, kinds: Sequence[str], kept, at: str, here,
          over_cm=None, most: str = "exactly", kind: str = "other_kind", of=None) -> dict:
    """One line of the card's one list (ROUND12 4): the question and answer, and the basket it starts
    from — `kept` fish of one kind of `kinds` (the gate takes the first it may keep there) kept at a
    `at` (another stream or lake, or "elsewhere": either; over `over_cm`) — and
    `here`, the most more here (the gate proves it: exactly, or `most` "at_most" for a ceiling the
    answer states; `of` the kinds that number counts, None every kind of the card). `kind`: the line's
    example — "stream_tally" (the share used up on another stream: a worked keep-N count already says
    it), "other_kind" (some kept on the other kind of water), "tally" (k kept anywhere, the total's
    number less k here), "over" (one over the size kept elsewhere)."""
    sp = [x for x in kinds if _kept_origin(model, x) is not None]
    return {"q": q, "a": a, "kept": kept, "at": at, "species": sp,
            "origin": _kept_origin(model, sp[0]) if sp else None, "over_cm": over_cm,
            "here": here if sp else None, "most": most, "kind": kind,
            "of": None if of is None else list(of)}


def elsewhere_of(P: "Part", model: dict, row: dict, sc: dict, T: R,
                 grp: Optional[dict]) -> dict:
    """The kept-elsewhere line of a keep row (rows F9, F14) — `says` one sentence, `ask`
    [[question, answer]] worked examples — qualified by what the total's sub-caps lift: on the
    Vedder, Oct 10, "Kept 2 on another stream? Keep 0 here, except hatchery rainbow trout 50 cm or
    less: up to 4 in all" (it was "Keep 0 here": p.22 lifts the stream share for them). `lines` the
    same examples with the basket each starts from and the number it states (`_line`, ROUND12 4: the
    card folds them into `group.worked`, its one list)."""
    n, pool, nw = row["daily"], row["pool"], row["narrow"]
    s = _scope_name(sc, pool)
    lines: List[dict] = []
    if sc["share"] and nw is not None and all(_covers(nw, S) for S in row["members"]):
        wk = nw.f["water"]
        other = "lake" if wk == "stream" else "stream"
        mine = next((x for x in (grp or {}).get("subcaps", []) if x["r"] == nw.key), None)
        tot = "unlimited" if T.f.get("unlimited") else T.f.get("take")
        says = f"Your own limit, counted across every {s} {wk} you fish today."   # ROUND20i 6c (UR4 Maya: "shared" with Dad?)
        # RULINGS Q23b (owner 2026-10-12): the stream tally counts every trout/char kept from ANY
        # stream today; a kind THIS water lifts may still go past it here, to its own quota and the
        # total — after n kept on another stream, up to min(its own, total - n) more of it here
        ex = []
        for L in (mine or {}).get("lifted", []):
            own = P.by_key[L["by"]].f.get("take")
            more = (tot - n) if own is None or tot == "unlimited" else min(own, tot - n) \
                if tot != "unlimited" else own
            ex.append((_lift_words(P, L["by"], L["members"]), more))
        # ROUND15 8: no bare "0" in an answer (it reads as a count): "None more here"
        a1 = "None more here"
        if ex and tot != "unlimited":
            a1 += ", except " + common.join([f"{w}: up to {_js(m)} more" for w, m in ex], "; ") \
                + f" ({_js(tot)} in all)"
        elif ex:
            a1 += ", except " + common.join([w for w, _m in ex], " or ")
        held = [S for S in row["members"] if S in (mine or {}).get("of", row["members"])]
        lifted_own = [P.by_key[L["by"]].f.get("take") for L in (mine or {}).get("lifted", [])]
        unl = tot == "unlimited" or any(x is None for x in lifted_own)
        here1 = None if unl else min(tot - n, sum(m for _w, m in ex)) if ex else 0
        here2 = None if unl else min(tot - 1, n + sum(lifted_own))
        # ROUND13 P (owner): the lake line says how many are left here after 1 kept there (the gate
        # proves `here`), then why: "Keep 2 more here. Fish from lakes count toward the 4 a day, not
        # toward the 2 from streams."
        if tot == "unlimited":
            a2 = f"They don’t count toward the {_js(n)} from {wk}s"
        elif here2 is None:
            a2 = f"They count toward the {_js(tot)} a day, not toward the {_js(n)} from {wk}s"
        else:
            a2 = (f"Keep {_js(here2)} more here. Fish from {other}s count toward the {_js(tot)} a day, not "
                  f"toward the {_js(n)} from {wk}s.")
        lines = [_line(f"Kept {_js(n)} on another {wk}?", a1, model, held, n, wk, here1, kind="stream_tally"),
                 _line(f"Kept 1 at a {other}?" if here2 is not None else f"Kept some at a {other}?", a2,
                       model, row["members"], 1, other, here2)]
    elif s:
        says = f"Your own limit, counted across every water in {s} you fish today" + \
            (", lakes and streams alike." if not pool.f.get("water") else ".")
        if row["kind"] == "keep" and n is not None and n > 1:
            k = 2 if n > 2 else 1
            a = f"Keep {_js(n - k)} more here"
            # a stream (lake) share of the total holding some of the row's kinds here still binds
            # them (Region 6's "1 trout from streams", Kitimat's tributaries): said, with its lifts
            sh = next((x for x in (grp or {}).get("subcaps", []) if x["water"] == P.kind
                       and x["take"] < n and set(x["of"]) & set(row["members"])), None) \
                if grp is not None and grp["pool"] == T.key else None
            if sh is not None:
                of = "" if set(row["members"]) <= set(sh["of"]) else f" {_names(sh['of'])}"
                a += f", but no more than {_js(sh['take'])}{of} from {sh['water']}s in all"
                ex = common.join([_lift_words(P, L["by"], L["members"]) for L in sh["lifted"]])
                if ex:
                    # RULINGS Q23b: lifted fish count toward the tally; they may go past it here
                    a += f" ({ex} may go past it here)"
            lines = [_line(f"Kept {k} elsewhere today?", a, model, (grp or {}).get("kinds") or row["members"],
                           k, "elsewhere", None if sh is not None else n - k, kind="tally")]
    else:
        # ROUND16 2 (owner): SPECIFIC — whose limit, its number, and which fish kept elsewhere today count
        # toward it (the row's kinds: Ape Creek's char released on Oct 10 are not among them): "The Bella
        # Coola River’s rule: 1 a day here. Brown trout, cutthroat trout or rainbow trout you kept anywhere
        # else today count toward that 1." (was "This river’s own limit. Fish you kept elsewhere today count
        # toward it too." on a creek the river's rule reached by the walk)
        from pipeline.deliver.answers.card import _whose
        whose = common.cap(_whose(P, pool)[1])
        fish = _names_short(row["members"])
        if n is None:
            says = f"{whose}: no daily limit here."
        else:
            that = "that 1" if n == 1 else f"those {_js(n)}"
            says = (f"{whose}: {_js(n)} a day here. {common.cap(fish)} you kept anywhere else today count "
                    f"toward {that}.")
        if sc["apart"]:
            says += " These fish are counted apart from the region’s total: they don’t use it up."
        if T is not pool:
            whose = T.x.get("entry_name") if badge_of(T) != "water" else None
            whose = "B.C.’s" if badge_of(T) == "bc" else f"{whose}’s" if whose else "the"
            says += (f" They also count toward {whose} {_js(T.f.get('take'))} {written_name_of(T)} a "
                     f"day." if not T.f.get("unlimited") else
                     f" They also count toward {whose} {written_name_of(T)}, which has no daily "
                     f"limit.")
    lines += over_elsewhere(P, model, row, grp)
    return {"says": says, "ask": [[x["q"], x["a"]] for x in lines], "lines": lines}


def over_elsewhere(P: "Part", model: dict, row: dict, grp: Optional[dict]) -> List[dict]:
    """RULINGS Q16 across waters (answers 2.8, A2): a size clause of the total holding the row's kinds
    counts fish kept anywhere today — said in the kept-elsewhere sheet, never on the card: "Kept a fish
    over 50 cm elsewhere today?" — "It counts here too: at most 2 over 50 cm a day in all — only 1 of
    them may be anything other than a hatchery steelhead." The line's basket: one of the kinds the
    smaller clause holds (not a steelhead) kept over the size on another water of this kind; at most
    the larger clause's number less one more over the size here."""
    if grp is None:
        return []
    sizes = [x for x in grp["subcaps"] if x["over_cm"] is not None and not x["water"]
             and set(x["of"]) & set(row["members"])]
    if not sizes:
        return []
    by_band: Dict[tuple, List[dict]] = {}
    for x in sizes:
        by_band.setdefault((x["over_cm"], x["under_cm"]), []).append(x)
    out = []
    for (a, b), xs in sorted(by_band.items(), key=lambda kv: (kv[0][0], kv[0][1] or 0)):
        xs.sort(key=lambda x: -x["take"])
        band = _range_words(a, INF if b is None else b)
        big = xs[0]
        say = f"It counts here too: at most {_js(big['take'])} {band} a day in all"
        small = big
        if len(xs) > 1 and set(xs[1]["of"]) < set(big["of"]):
            small = xs[1]
            rest = [S for S in big["of"] if S not in xs[1]["of"]]
            who = " ".join(w for w in ("hatchery" if "ST" in rest and P.steelhead_water else "",
                                       _names(rest)) if w)
            say += (f" — only {_js(xs[1]['take'])} of them may be anything other than "
                    f"{'a ' if len(rest) == 1 else ''}{who}")
        from pipeline.deliver.answers.display import written_name
        # ROUND UR5 (D1): the fish the clause holds HERE — Region 4's "rainbow trout or cutthroat trout" clause reaches the
        # Kootenay Lake main body's trout and char only for cutthroat (rainbow has its own row there); naming rainbow read as a
        # daily cap on big rainbows. The rule's own words where the clause holds every fish it names.
        # decision F17: a tally a water's quota raises a clause to is said in the clause's words (its fish)
        rx = P.by_key[(small if raise_of(grp, big) is small else big)["r"]].x
        rsp = list(rx.get("species") or [])
        narrower = rsp and all(c not in common._species().SPECIES_GROUPS for c in rsp) and set(rsp) - set(big["of"]) and set(big["of"]) <= set(rsp)
        who = _names([c for c in rsp if c in big["of"]]) if narrower else written_name(rx, " or ").replace(" and ", " or ")
        kinds = [S for S in small["of"] if S != "ST"] or list(small["of"])
        out.append(_line(f"Kept {who} {band} elsewhere today?", say + ".", model, kinds, 1, "elsewhere",
                         max(0, big["take"] - 1), over_cm=a if b is None else None, most="at_most",
                         kind="over", of=big["of"]))
        if b is not None:
            out[-1]["here"] = None
    return out


def parent_of(rows: Sequence[dict], i: int) -> Optional[int]:
    """rows 31 (owner rule 2026-10-21; page v36 parentRow): THE POOL ROW THAT HOLDS A POOLED ROW — of the stored rows
    (`produce`'s, in card order), the first other pooled row holding every fish of row `i` (among its `all_members`,
    or in one of its fact groups) whose facts cross-reference one of them (an `xref` fact: the other pool counts
    them, decision F4); else (T139, `A_ROW_HELD_BY_ITS_TOTALS_GROUP`) the row that carries the group of row `i`'s
    total where that total is ANOTHER rule's (`total`: `total_of`'s key; `draws`: the totals whose group a row
    carries, `produce`'s `home`) — that group's kinds are row `i`'s fish too, and its article says them. The card
    draws no article for such a row: its fish are said inside that pool's; None for every other row (and every row
    with no pool)."""
    o = rows[i]
    if not o.get("pool"):
        return None
    M = o["members"]
    for j, r in enumerate(rows):
        if j == i or not r.get("pool"):
            continue
        holds = all(S in r["all_members"] or any(S in g["members"] for g in r["groups"]) for S in M)
        if holds and any(l["t"] == "xref" and any(S in g["members"] for S in M)
                         for g in r["groups"] for l in g["facts"]):
            return j
    T = o.get("total")
    if A_ROW_HELD_BY_ITS_TOTALS_GROUP and T is not None and T != o["pool"]:
        return next((j for j, r in enumerate(rows) if j != i and r.get("pool") and T in (r.get("draws") or ())),
                    None)
    return None


#: T139 (2026-10-23): a pooled row whose total is ANOTHER rule's, carried as a group on another row, is held by that
#: row. Beattie Lake (r7, in the Liard River watershed, wbk:329665706): the reader lets the Liard's "bull trout 1
#: (30-50 cm)" displace the lake's "trout 1" for bull trout, and both count toward Zone B's 5 (`contains`), whose group
#: sits on the lake's trout row and already says the bull trout (the bucket "LIARD RIVER WATERSHED's rule: Dolly
#: Varden/bull trout, 30-50 cm, 1, counts toward the 5 a day"). Until T127 the bull trout row was held through the lake
#: row's cross-reference of a pool the reader DISPLACED; T127 rightly removed that cross-reference, and the row was
#: left pooled, groupless and unheld — drawn nowhere (core `keep.ts`), a duplicate of the group's bucket. False only in
#: the mutation pin (test_answers_one_kind_group.py).
A_ROW_HELD_BY_ITS_TOTALS_GROUP = True


def produce(P: Part) -> dict:
    """Everything the card's rows show, for one part on one reading: per fish and origin the
    decided answer (`fish`), and the rows with their conditions, items and real daily limit."""
    model = build_model(P)
    fish = {S: {"hatchery": res_out(v["H"], P), "wild": res_out(v["W"], P)} for S, v in model["R"].items()}
    # decision F14: every keep row's total; each total's group on ONE row — the row whose pool it
    # is, else the first row whose fish count toward it
    totals = {id(row): total_of(P, model, row) for row in model["rows"]
              if row.get("pool") is not None and row["kind"] in ("keep", "no_limit")}
    home: Dict[str, int] = {}
    for row in model["rows"]:
        T = totals.get(id(row))
        if T is not None and row["pool"] is T:
            home.setdefault(T.key, id(row))
    for row in model["rows"]:
        T = totals.get(id(row))
        if T is not None:
            home.setdefault(T.key, id(row))
    # rows 31: the pool row holding each pooled row (page v36 parentRow), decided once, before the groups — a row held
    # by another pool is drawn inside that pool's article, never as its own (`A_ONE_KIND_GROUP_ONLY_ON_A_DRAWN_ROW`)
    stubs = [{"pool": row["pool"].key if row.get("pool") else None, "members": list(row["members"]),
              "all_members": list(row["allMembers"]),
              "groups": [{"members": g["members"], "facts": [fact_out(l) for l in g["facts"]]} for g in row["groups"]],
              # T139: the row's total, and the totals whose group it carries (`parent_of`'s second hold)
              "total": totals[id(row)].key if id(row) in totals else None,
              "draws": [k for k, h in home.items() if h == id(row)]}
             for row in model["rows"]]
    parents = {i: p for i in range(len(stubs)) if (p := parent_of(stubs, i)) is not None}
    own_row = {T.key for i, row in enumerate(model["rows"]) if (T := totals.get(id(row))) is not None
               and row["pool"] is T and not (A_ONE_KIND_GROUP_ONLY_ON_A_DRAWN_ROW and i in parents)}
    groups = {k: group_of(P, model, P.by_key[k], own_row=k in own_row) for k in home}
    rows = []
    for row, stub in zip(model["rows"], stubs):
        o = {"kind": row["kind"], "pool": stub["pool"],
             "win": row["win"].key if row.get("win") else None,
             "members": stub["members"], "all_members": stub["all_members"],
             "daily": _num(row["daily"]),
             "narrow": row["narrow"].key if row.get("narrow") else None,
             "everyone": [fact_out(l) for l in row["everyone"]],
             "groups": stub["groups"],
             "prot": row.get("prot"), "wins": [r.key for r in row["wins"]] if row.get("wins") else None,
             "lift_notes": [[n["by"], n["q"]] for n in row["liftNotes"]],
             "scope": scope_of(P, row)}
        rs = row_restates(P, model, row)
        if rs:
            o["restates"] = rs
        if row.get("pool"):
            rc = row_conds(P, model, row)
            o["conds"] = [cond_out(c) for c in rc["conds"]]
            items = species_items(P, model, row, rc)
            o["items"] = []
            for it in items:
                q = it["q"]
                xref = it["xref"]
                io = {"members": list(it["members"]),
                      "bands": [[_num(x["a"]), _num(x["b"]), _num(x["mx"])] for x in q] if q else None,
                      "back": bool(it.get("back")), "xref": bool(xref),
                      "sub": _num(it["sub"]["sub"]) if it.get("sub") else None,
                      "conds": list(it["conds"])}
                if xref:
                    # decision F4; an unlimited outer total is the word, never null
                    io["against"] = "unlimited" if it["against"] == INF else _num(it["against"])
                if xref and len(it["members"]) == 1:
                    io["origins"] = [{k: _num(v) if k in ("n", "lo", "hi") else v
                                      for k, v in x.items()}
                                     for x in origin_lines(P, model, it["members"][0])]
                if xref and len(it["members"]) > 1:
                    # gap G4 (answers 2.3): a cross-reference of several fish carries EACH fish's
                    # keep range (`keep_range`, its own decided answer) — the page read it off the
                    # fish's own row; [fish, from_cm, to_cm | null (no top)], fish that keep none
                    # left out
                    io["ranges"] = [[S, _num(r[0]), None if r[1] == INF else _num(r[1])]
                                    for S in it["members"]
                                    for r in [keep_range(P, model, [S])] if r is not None]
                o["items"].append(io)
            ec = eff_cap(P, model, row, rc)
            o["real_daily"] = ec
            T = totals.get(id(row))
            if T is not None:                                  # decision F14
                grp = groups[T.key]
                o["group"] = grp if home[T.key] == id(row) else None
                o["elsewhere"] = elsewhere_of(P, model, row, o["scope"], T, grp) \
                    if o["scope"] is not None else None
        rows.append(o)
    from pipeline.deliver.answers.card import card_of                   # answers 2.8
    for i, o in enumerate(rows):
        if o.get("group") is not None:
            o["group"].update(card_of(P, model, o["group"], rows, i))
            # ROUND12 4: the card's one list holds the kept-elsewhere examples (`group.worked`)
            if o.get("elsewhere") is not None:
                o["elsewhere"]["ask"] = []
    for o in rows:
        if o.get("elsewhere") is not None:
            o["elsewhere"].pop("lines", None)
    # rows 31 (owner rule 2026-10-21): the pool row that holds a pooled row's fish (page v36 parentRow), decided here
    for i, p in parents.items():
        rows[i]["parent"] = p
    from pipeline.deliver.answers.display import steelhead_line
    has_row = not P.rules_off_in_export and any("ST" in r["allMembers"] for r in model["rows"])
    line = steelhead_line(P.presence, has_row)
    # ROUND15 7 (owner): where the whole water is closed to all fishing (every row closed: the Vedder on Jun
    # 15) there is no steelhead section to read, and no presence line is needed
    if not all(o["kind"] == "closed" for o in rows if o["all_members"]):
        place_presence(rows, line, P.steelhead_water)
    return {"spp": model["spp"], "fish": fish, "rows": rows, "steelhead_line": line}


#: ROUND14 4 (owner): the presence line, in angler words, said INSIDE the steelhead section
PRESENCE_WORDS = {"possible_with_rules": "Steelhead rules apply here; steelhead may not be present in this water.",
                  "known_with_rules": "Steelhead are known to be in this water."}


def presence_words(line: str, anadromous: bool) -> str:
    """The presence line's words. UR2 4 (user panel: "Steelhead are known …" beside a harvest limit read as a warning
    with no point): where a big rainbow IS a steelhead (`steelhead_water`: known, a stream, steelhead rules apply —
    AGENTS 54) the line says what follows from it, in the book's p.80 definition ("steelhead: a rainbow trout longer
    than 50 cm in waters where anadromous rainbow trout are found"): "Steelhead are known to be in this water, so a
    rainbow trout over 50 cm (20 in) counts as a steelhead." Elsewhere (a lake whose own row prints steelhead) the
    words as they were."""
    if line == "known_with_rules" and anadromous:
        from pipeline.deliver.bundle import read
        # ROUND20g 2 (owner: fewer words; the ⓘ beside it says why — steelhead are known here)
        return f"A rainbow trout over {common.cm_in(read._steelhead_min_cm())} here counts as a steelhead."
    return PRESENCE_WORDS[line]


def place_presence(rows: List[dict], line: Optional[str], anadromous: bool = False) -> None:
    """ROUND14 4 (owner): where steelhead are known (or steelhead rules apply), the presence line
    (`presence` {line, says}) sits IN THE STEELHEAD SECTION, exactly once: the card's steelhead kind row
    (kept), its not-kept steelhead line (closed or released), else the row holding steelhead (a release
    or closed row, or a keep row with no card). A line with no steelhead section is refused — never the
    note without the section. `known_no_rules` is about rainbow trout and stays the frame's own."""
    if line not in PRESENCE_WORDS:
        return
    note = {"line": line, "says": presence_words(line, anadromous)}
    for r in rows:
        if "ST" not in (r.get("all_members") or []):
            continue
        g = r.get("group") or {}
        for b in g.get("buckets") or []:
            for k in list(b["kinds"]) + [k for s in b.get("groups") or [] for k in s["kinds"]]:
                if "ST" in k["species"]:
                    k["presence"] = note
                    # ROUND18 1: the same kind's node in the card's tree carries it too
                    from pipeline.deliver.answers.tree import walk
                    for x, _t in walk(g.get("tree") or []):
                        if x["node"] == "kind" and "ST" in x["species"]:
                            x["presence"] = note
                    return
        for x in g.get("not_kept") or []:
            if "ST" in x["species"]:
                x["presence"] = note
                return
        r["presence"] = note
        return
    raise AnswersError(f"rows: steelhead line {line!r} with no steelhead section on the card")


def weekdays_words(days: Sequence[str]) -> str:
    """ROUND14 5: a set of weekdays in words — "Monday to Friday", "on Saturdays and Sundays" (T104: said by
    `common.when_words`, plain style)."""
    return common.when_words({"weekdays": list(days)}, "plain")


def weekdays_short(days: Sequence[str]) -> str:
    """ROUND15 3: a set of weekdays in a short label — "Sat–Sun", "Mon–Fri", "Mon, Wed" (`when_words`, title)."""
    return common.when_words({"weekdays": list(days)}, "title")


def weekdays_label(days: Sequence[str]) -> str:
    """ROUND15 3: a set of weekdays as the status names it — "Weekends", "Weekdays", else the short label
    (`when_words`, status)."""
    return common.when_words({"weekdays": list(days)}, "status")


def weekdays_when(days: Sequence[str]) -> str:
    """ROUND15 3: the days a part holds in words — "Monday to Friday", "Saturday and Sunday" (`when_words`, days)."""
    return common.when_words({"weekdays": list(days)}, "days")


def _week_answer(frame: dict, S: str) -> Optional[Tuple[str, Optional[float], str]]:
    """(status, daily, winner) of one fish in a rows frame: its leading origin's, the main row's (T120:
    `common.lead_origin`; it used to take the most permissive keeper, so a weekday row could say a bigger quota)."""
    v = (frame.get("fish") or {}).get(S) or {}
    best = common.lead_origin(v.get("hatchery"), v.get("wild"))
    if best is None:
        return None
    return best["status"], best.get("daily"), best["win"]


_WEEK_RANK = {"no_limit": 3, "keep": 2, "release": 1, "closed": 0}


def say_weekdays(frames: Sequence[dict], moments: Sequence[Optional[dict]]) -> List[dict]:
    """ROUND14 5 (owner): A WEEKDAY WINDOW SAID AS WEEKDAYS. `frames` are one day's rows frames, one per
    moment (`moments`: `calendar.Moment.as_json`, weekdays and no hours window). Every row of every frame
    whose fish answer differently by weekday carries `week`: Kootenay Lake's Lower West Arm kokanee —
      says    "Keep up to 5 on Saturdays and Sundays; release Monday to Friday" (one sentence);
      status  (ROUND15 3) "Weekends: keep 5 · Weekdays: release" — the row's status, never one moment's;
      parts   (ROUND15 3) [{days "Sat–Sun", when "Saturday and Sunday", weekdays, status, n, says "keep up to
              5", rules [the deciding rule], today}] — how it was decided, each day-part with its rule; `today`
              marks the frame's own moment —
    the same on the Saturday frame and the Monday one, so no moment's card contradicts another. A row's
    `restates` notes (the combined quota with the Upper West Arm) are carried to every moment's frame.
    Returns new frames (the inputs are not changed); frames cut by an hours window are returned as they are."""
    import copy
    if len(frames) < 2 or any(m is None or m.get("hours") for m in moments):
        return list(frames)
    fish = sorted({S for f in frames for S in (f.get("fish") or {})})
    split: Dict[str, List[Tuple[Tuple[str, Optional[float]], List[str], List[str]]]] = {}
    for S in fish:
        by: Dict[tuple, Tuple[List[str], List[str]]] = {}
        for f, m in zip(frames, moments):
            a = _week_answer(f, S)
            if a is None:
                break
            days, rules = by.setdefault(a[:2], ([], []))
            days.extend(m["weekdays"])
            if a[2] not in rules:
                rules.append(a[2])
        else:
            if len(by) > 1:
                split[S] = sorted(((k, d, r) for k, (d, r) in by.items()),
                                  key=lambda x: (-_WEEK_RANK.get(x[0][0], -1), x[1]))
    if not split:
        return list(frames)

    def verb(status, daily, short=False) -> str:
        if status == "no_limit":
            return "no limit" if short else "keep any number"
        if status == "keep":
            return f"keep {_js(daily)}" if short else f"keep up to {_js(daily)}"
        if status == "release":
            return "release" if short else "release every one"
        return "closed" if short else "no fishing for them"

    def words(pattern) -> str:
        out = []
        for (status, daily), days, _r in pattern:
            what = ("keep any number" if status == "no_limit" else
                    f"keep up to {_js(daily)}" if status == "keep" else
                    "release" if status == "release" else "no fishing for them")
            out.append(f"{what} {weekdays_words(days)}")
        return common.cap("; ".join(out))

    restated: Dict[tuple, List[dict]] = {}
    for f in frames:
        for r in f.get("rows") or []:
            for x in r.get("restates") or []:
                got = restated.setdefault(tuple(r.get("all_members") or []), [])
                if x not in got:
                    got.append(x)

    got = []
    for f, m in zip(frames, moments):
        f2 = copy.deepcopy(f)
        for r in f2.get("rows") or []:
            mine = [S for S in r.get("all_members") or [] if S in split]
            if not mine:
                continue
            pats = sorted({json.dumps(split[S]) for S in mine})
            named = len(pats) > 1 or len(mine) < len(r["all_members"])
            says_l, status_l, parts = [], [], []
            for p in pats:
                pat = json.loads(p)
                w = words(pat)
                who = [S for S in mine if json.dumps(split[S]) == p]
                lead = f"{common.cap(_names(who, ' and '))}: " if named else ""
                # several fish of the row: each named ("Kokanee: keep up to 5 …")
                says_l.append(f"{lead}{w[:1].lower()}{w[1:]}" if named else w)
                status_l.append(lead + " · ".join(f"{weekdays_label(d)}: {verb(st, n, True)}"
                                                  for (st, n), d, _r in pat))
                for (st, n), d, rules in pat:
                    parts.append({"species": who, "days": weekdays_short(d), "when": weekdays_when(d),
                                  "weekdays": d, "status": st, "n": _num(n) if st == "keep" else None,
                                  "says": verb(st, n), "rules": rules,
                                  "today": bool(set(m["weekdays"]) & set(d))})
            r["week"] = {"species": mine, "says": "; ".join(says_l), "status": "; ".join(status_l),
                         "parts": parts}
            notes = restated.get(tuple(r.get("all_members") or []))
            if notes:
                r["restates"] = notes
        got.append(f2)
    return got


# --------------------------------------------------------------------------------------------
# The `rows` section of the answers file
# --------------------------------------------------------------------------------------------

def section_scope(key: tuple, B):
    """The card reads the rule key, the water's kind (which rules apply, a stream share holding
    here) and the steelhead presence (whether ST is asked about, the presence line)."""
    k = common.key_dict(key)
    return (common.rule_key(key), k["kind"], k["steelhead"])


def open_states(ctx, rk, md, at=None) -> dict:
    """Decision R5: the reader's state of every open-subject gate rule of the set, asked about the
    subject's first named fish: {rule id: (state, lifted_in_part_by)} — the STORED verdicts
    (`verdicts.sqlite`, origin not known; the verdicts ask every fish a member rule names)."""
    from pipeline.deliver.calendar import day_of
    from pipeline.regs.parsing.catalogue import OPEN_SUBJECTS, PROTECTED_FISH
    ck = ("rows_open", rk, md, at)
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
    key = ctx.B.key_ix[rk]
    ms = ctx.store.moments(key)
    reading = ctx.store.reading_of(key, day_of(md), None if len(ms) == 1 else ms.index(at))
    for f, ks in sorted(by_fish.items()):
        ans = ctx.ladder_dict(ctx.store.verdict_id(key, reading, f, "none"))
        for k in ks:
            y = ans.get(k)
            if y is None:
                raise AnswersError(f"rows: the verdict does not hold {k} asked about {f}")
            out[k] = (y[0], y[1])
    ctx.cache[ck] = out
    return out


def refs(out: dict, ix) -> dict:
    """`produce`'s rows with every rule named by its export index (`ix("entry::rule")`) — FIELD
    BY FIELD, the fields that hold a rule (L1: a blind scan turned any string holding "::" into a
    rule, a caution's text included)."""
    def res(d):
        if d is None:
            return None
        return {**d, "win": ix(d["win"]), "narrow": None if d["narrow"] is None else ix(d["narrow"]),
                "lines": [line(l) for l in d["lines"]],
                "roles": [[ix(k), role, None if by is None else ix(by)] for k, role, by in d["roles"]],
                "lift_notes": [[ix(by), q] for by, q in d["lift_notes"]]}

    def line(l):
        l = {**l, "r": ix(l["r"])}
        if "outer" in l:
            l["outer"] = ix(l["outer"])
        if "rules" in l:
            l["rules"] = [ix(r) for r in l["rules"]]
        return l

    from pipeline.deliver.answers.tree import map_rules

    def row(r):
        o = {**r, "pool": None if r["pool"] is None else ix(r["pool"]),
             "win": None if r["win"] is None else ix(r["win"]),
             "narrow": None if r["narrow"] is None else ix(r["narrow"]),
             "everyone": [line(l) for l in r["everyone"]],
             "groups": [{**g, "facts": [line(l) for l in g["facts"]]} for g in r["groups"]],
             "wins": None if r["wins"] is None else [ix(w) for w in r["wins"]],
             "lift_notes": [[ix(by), q] for by, q in r["lift_notes"]]}
        if r.get("restates"):
            o["restates"] = [{**x, "r": ix(x["r"]), "of": ix(x["of"])} for x in r["restates"]]
        if "conds" in r:
            o["conds"] = [{**c, "r": None if c.get("r") is None else ix(c["r"])} for c in r["conds"]]
        if r.get("group") is not None:
            g = r["group"]
            o["group"] = {**g, "pool": ix(g["pool"]),
                          "subcaps": [{**s, "r": ix(s["r"]),
                                       "lifted": [{**L, "by": ix(L["by"])} for L in s["lifted"]]}
                                      for s in g["subcaps"]],
                          "lifts": [{**L, "by": ix(L["by"]),
                                     "each": [{**e, "by": ix(e["by"])} for e in L["each"]]}
                                    for L in g["lifts"]],
                          "buckets": [{**b, "by": ix(b["by"]),
                                       "groups": [{**s, "by": ix(s["by"])} for s in b["groups"]],
                                       "sizes": [{**s, "by": ix(s["by"])} for s in b.get("sizes") or []],
                                       "replaces": None if b["replaces"] is None else
                                       {**b["replaces"], "rules": [ix(k) for k in b["replaces"]["rules"]]}}
                                      for b in g["buckets"]],
                          "not_kept": [{**x, "by": ix(x["by"]), "rules": [ix(k) for k in x["rules"]]}
                                       for x in g["not_kept"]],
                          "worked": [{**w, "share": None if w["share"] is None else ix(w["share"])}
                                     for w in g["worked"]],
                          "states": [{**x, "r": ix(x["r"])} for x in g["states"]],
                          "caps": [{**c, "by": ix(c["by"])} for c in g["caps"]],
                          "tree": map_rules(g["tree"], ix),
                          "possession": None if g.get("possession") is None else
                          {**g["possession"], "by": ix(g["possession"]["by"])}}
        return o
    return {**out, "fish": {S: {o: res(d) for o, d in v.items()} for S, v in out["fish"].items()},
            "rows": [row(r) for r in out["rows"]]}


def section_derive(ladder_value: dict, scope, ctx, first_day: int, at=None) -> dict:
    """The card's rows for one part scope on one reading (the ladder's: its first day and the
    moment it is asked at), rules by export index."""
    rk, kind, presence = scope
    md = month_day(first_day)
    P = Part(ctx.B, rk.set_id, rk.steelhead_water, rk.steelhead_rules, kind, presence,
             ladder_value, md, open_states(ctx, rk, md, at), at=at)
    out = produce(P)
    no_answer_gate(out, ladder_value, ctx.by_id, f"rule set {rk.set_id}, {md}")
    return refs(json.loads(json.dumps(out)), ctx.rule_index.__getitem__)


def no_answer_gate(out: dict, ladder_value: dict, by_id: Dict[str, dict], where: str) -> None:
    """EVERY FISH THE ZONE HAS A RULE FOR HAS AN ANSWER (answers 2.6). A decided answer is null
    only where no rule in scope speaks; it is REFUSED where a zone or provincial table's quota or
    release for the fish (`retention_limit` of a `z…` entry) took part in the competition — it
    speaks, or was displaced — and still nothing decides. That is a reader that lost every rule (a
    cycle: Clearwater Creek gnis:7628, Arctic grayling, before `read._step4_competition` broke it
    at the own row), never an answer."""
    for S, v in out["fish"].items():
        for o in ("hatchery", "wild"):
            if v[o] is not None:
                continue
            lad = (ladder_value.get(S) or {}).get(o) or {}
            took = sorted(r for r, (st, *_rest) in lad.items()
                          if st in ("speaks", "displaced") and EID.is_table(r)
                          and (by_id.get(r) or {}).get("type") == "retention_limit")
            if took:
                raise AnswersError(f"rows: {S} ({o}) has no decided answer at {where}, but the "
                                   f"zone's {took[:3]} took part — a reader that lost every rule")
