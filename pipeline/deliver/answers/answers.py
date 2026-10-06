"""THE ANSWERS LAYER (v0) — every decided reading of the rules, computed once, beside the UI export.

    python -m pipeline.deliver.answers.cli build --bundle FILE --export-dir DIR --out FILE

The UI export (`ui-rules-export.json` + `ui-rules-guide.json`) ships what the book SAYS. This module
works out what it COMES TO for every part of every named water, on every day and for every fish,
and `encode.py` writes it as a third file, `ui-rules-answers.json`, stamped with the export pair's
`about.bundle` digests. The export is not changed; nothing here replaces it (handoff
ANSWERS/DESIGN.md, user constraint 2026-10-05).

ONE SOURCE PER FACT. Every rule state, every loser's reason and `by`, comes from
`read.effective_rules_bound(trace=True, origin=…)` — the one reference reader, called here and never
re-implemented. This module only ARRANGES: which questions to ask (keys, segments, fish, origins), and
the one derivation the consumer page made on top of the reader, the decided answer (consumer
Stage 5.2 steps 1-4, `decide`), written down as code with every choice named.

NO FALLBACKS. A question that cannot be answered stops the build with an `AnswersError` naming it:
an export pair that does not match the bundle, a part whose sections disagree, a rule the export does
not list, a fish the reader refuses. A state that IS an answer is stated explicitly (`no_rule`,
`by_origin`), never left out.

THE KEYING, shared by every section (the coordinator's requirement, 2026-10-06):

  part key   one per distinct part tuple of the export's named waters — the reference harness's
             `partKey` (`compare.py`): (ruleset, licensing_set, steelhead_water, steelhead presence,
             steelhead_rules, province_except, home_region), with `steelhead_rules` the bundle's fact
             for the part's sections (the export ships it only where it is false on a known part).
  segment    a run of days (1..366, the catalogue's leap calendar) on which EVERY section's inputs
             read the same: the union of every section's breakpoints. v0's sections both read one
             signature per day — every member rule's `when` and every lift's `when`, through
             `read.in_force` (the status index's `set_profile` signature) — so the segments are the
             days on which any member rule or lift comes into or goes out of force.

  A section's value is a function of (part key, segment). Adding a section (rows, gear, licence,
  display — `RESERVED`) is a new `Section` with its own `scope`/`signature`/`produce`: the existing
  sections' code does not change, and the segments become the union of all of them.

v0 SECTIONS
  ladder   per fish x origin: every rule's state (speaks, beside, shown, not_yet_mapped) and every
           loser (lifted, displaced, moot) with its reason and `by` — the traced reader, verbatim.
  answer   per fish x origin: the decided answer — status (closed / release / keep / no_limit /
           no_rule / by_origin), the daily number, the winner rule (`decide`).
"""
from __future__ import annotations

import json
import os
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from multiprocessing import get_context
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from pipeline.deliver.bundle import read

#: The days of the year, on the catalogue's leap calendar (`catalogue._day_index`): Jan 1 is 1,
#: Feb 29 is 60, Mar 1 is 61 in EVERY year, Dec 31 is 366.
DAYS = 366

#: The origins every question is asked for: "none" is the reader's default (origin not known,
#: `origin=None`); "hatchery" / "wild" are `read.ASKABLE_ORIGINS`.
ORIGINS = ("none",) + tuple(read.ASKABLE_ORIGINS)

#: The decided statuses (`decide`). `no_rule`: no rule in scope speaks for the fish (the page shows
#: no row). `by_origin`: asked with the origin unknown, the hatchery and wild answers differ — read
#: those two.
STATUSES = ("closed", "release", "keep", "no_limit", "no_rule", "by_origin")

#: The part key's fields, in order (the reference harness's `partKey`).
PART_KEY_FIELDS = ("ruleset", "licensing_set", "steelhead_water", "steelhead", "steelhead_rules",
                   "province_except", "home_region")


class AnswersError(RuntimeError):
    """A question the answers layer cannot answer: the build stops, naming it."""


# --------------------------------------------------------------------------------------------
# Inputs: the bundle and the export pair it is paired with
# --------------------------------------------------------------------------------------------

@dataclass
class Inputs:
    bundle: str
    data: dict                      # ui-rules-export.json as shipped
    guide: dict                     # ui-rules-guide.json as shipped
    rule_index: Dict[str, int]      # "entry::rule" -> index into the export's `rules`
    sets: Dict[int, List[Tuple[str, str, str]]] = field(default_factory=dict)  # set id -> bound


def load_export(export_dir: Path) -> Tuple[dict, dict]:
    d = Path(export_dir)
    data = json.loads((d / "ui-rules-export.json").read_text(encoding="utf-8"))
    guide = json.loads((d / "ui-rules-guide.json").read_text(encoding="utf-8"))
    return data, guide


def _meta(db) -> dict:
    return dict(db.execute("SELECT k, v FROM meta"))


def check_inputs(bundle: str, data: dict, guide: dict) -> Inputs:
    """The export pair must be ONE pair, cut from THIS bundle, and its rule sets must be the
    bundle's — otherwise its integer rule refs would point at other rules."""
    from pipeline.tools.export_codec import FORMAT, expand
    if not Path(bundle).is_file():
        raise AnswersError(f"answers: no bundle at {bundle}")
    if data.get("about", {}).get("format") != FORMAT or guide.get("about", {}).get("format") != FORMAT:
        raise AnswersError(f"answers: the export pair is not format {FORMAT}")
    if data["about"]["bundle"] != guide["about"]["bundle"]:
        raise AnswersError("answers: ui-rules-export.json and ui-rules-guide.json carry different "
                           "about.bundle digests — not one pair")
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    try:
        meta = _meta(db)
        stamp = data["about"]["bundle"]
        for k in ("reach_digest", "section_handles"):
            if meta.get(k) != stamp.get(k):
                raise AnswersError(f"answers: the export's about.bundle.{k} {stamp.get(k)!r} is not "
                                   f"the bundle's meta.{k} {meta.get(k)!r}")
        sets: Dict[int, List[Tuple[str, str, str]]] = defaultdict(list)
        for set_id, e, r, via in db.execute("SELECT set_id, entry_id, rule_id, via FROM ruleset "
                                            "ORDER BY set_id, entry_id, rule_id"):
            sets[set_id].append((e, r, via))
        bundle_rules = {f"{e}::{r}" for e, r in db.execute("SELECT entry_id, rule_id FROM rule")}
    finally:
        db.close()
    ids = data["rule_ids"]
    if ids != sorted(ids) or len(set(ids)) != len(ids):
        raise AnswersError("answers: the export's rule_ids are not the codec's order (sorted, unique)")
    if set(ids) != bundle_rules:
        miss = sorted(bundle_rules - set(ids))[:3] + sorted(set(ids) - bundle_rules)[:3]
        raise AnswersError(f"answers: the export's rules are not the bundle's (e.g. {miss})")
    model = expand(data, guide)
    for sid, s in model["rulesets"].items():
        want = sorted((i.split("::", 1)[0], i.split("::", 1)[1], via)
                      for via, members in s.items() if via != "sections" for i in members)
        if want != sorted(sets.get(int(sid), [])):
            raise AnswersError(f"answers: export rule set {sid} is not the bundle's set {sid}")
    if len(model["rulesets"]) != len(sets):
        raise AnswersError("answers: the export and the bundle hold different numbers of rule sets")
    return Inputs(bundle, data, guide, {k: i for i, k in enumerate(ids)}, dict(sets))


# --------------------------------------------------------------------------------------------
# Part keys: the export's parts, keyed by what their answers can depend on
# --------------------------------------------------------------------------------------------

def part_keys(inp: Inputs) -> Tuple[List[tuple], Dict[str, List[Optional[int]]]]:
    """(keys, parts): `keys` the distinct part tuples (`PART_KEY_FIELDS`), `parts` {item_id: [key
    index per export part, in the export's order]}. A part with no rule set is `None` — and must be
    wholly outside B.C. (the export's own `ruleset: null`), or the build stops.

    The export groups a water's sections by (ruleset, licensing_set, province_except,
    steelhead_water, steelhead presence); the same grouping is read back here per part, to add the
    one fact the export ships only in part: whether the steelhead rules apply (`steelhead_rules`,
    which every section of a part shares — the export refuses a part whose sections disagree)."""
    db = sqlite3.connect(f"file:{inp.bundle}?mode=ro", uri=True)
    try:
        db.execute("CREATE TEMP TABLE _st (sid INTEGER PRIMARY KEY, code INTEGER NOT NULL)")
        db.execute("INSERT INTO _st SELECT sid, code FROM section_steelhead")
        db.execute("CREATE TEMP TABLE _sr (sid INTEGER PRIMARY KEY)")
        db.execute("INSERT INTO _sr SELECT sid FROM section_steelhead_rules")
        db.execute("CREATE TEMP TABLE _out (sid INTEGER PRIMARY KEY)")
        db.execute("INSERT INTO _out SELECT DISTINCT sid FROM outside_bc")
        facts: Dict[tuple, dict] = {}
        for item, rs, ls, pe, sw, st, sr, home, out in db.execute(
                "SELECT i.item_id, r.set_id, l.set_id, "
                "(SELECT group_concat(k, ',') FROM (SELECT p.area_kind AS k FROM province_except p "
                " WHERE p.sid = s.sid ORDER BY p.area_kind)), "
                "EXISTS (SELECT 1 FROM steelhead_water w WHERE w.sid = s.sid), "
                "(SELECT CASE h.code WHEN 1 THEN 'known' WHEN 2 THEN 'possible' END FROM _st h "
                " WHERE h.sid = s.sid), "
                "EXISTS (SELECT 1 FROM _sr x WHERE x.sid = s.sid), "
                "(SELECT region FROM section_home h WHERE h.sid = s.sid), "
                "EXISTS (SELECT 1 FROM _out o WHERE o.sid = s.sid) "
                "FROM item i JOIN item_section s ON s.ord = i.ord "
                "LEFT JOIN section_ruleset r ON r.sid = s.sid "
                "LEFT JOIN section_licensing l ON l.sid = s.sid"):
            f = facts.setdefault((item, rs, ls, pe, bool(sw), st),
                                 {"sr": set(), "home": set(), "out": set()})
            f["sr"].add(bool(sr))
            f["out"].add(bool(out))
            if home:
                f["home"].add(home)
    finally:
        db.close()

    keys: List[tuple] = []
    index: Dict[tuple, int] = {}
    parts: Dict[str, List[Optional[int]]] = {}
    for item, w in inp.data["waters"].items():
        row: List[Optional[int]] = []
        for pi, arr in enumerate(w["parts"]):
            rs, ls = arr[0], arr[1]
            flags = arr[4] if len(arr) > 4 else {}
            pe = ",".join(flags["province_except"]) if flags.get("province_except") else None
            ident = (item, rs, ls, pe, bool(flags.get("anadromous_rainbow")), flags.get("steelhead"))
            f = facts.get(ident)
            if f is None:
                raise AnswersError(f"answers: water {item} part {pi} {ident[1:]} has no sections in "
                                   f"the bundle")
            if rs is None:
                if f["out"] != {True}:
                    raise AnswersError(f"answers: water {item} part {pi} has no rule set but is not "
                                       f"wholly outside B.C.")
                row.append(None)
                continue
            if len(f["sr"]) != 1:
                raise AnswersError(f"answers: water {item} part {pi}: its sections disagree on "
                                   f"whether the steelhead rules apply")
            sr = next(iter(f["sr"]))
            if flags.get("steelhead_rules") is False and sr:
                raise AnswersError(f"answers: water {item} part {pi}: the export says steelhead "
                                   f"rules do not apply, the bundle says they do")
            home = tuple(sorted(f["home"]))
            if home != tuple(flags.get("home_region") or ()):
                raise AnswersError(f"answers: water {item} part {pi}: home_region {home} is not the "
                                   f"export's {flags.get('home_region')}")
            key = (rs, ls, bool(flags.get("anadromous_rainbow")), flags.get("steelhead"), sr,
                   tuple(flags.get("province_except") or ()), home)
            if key not in index:
                index[key] = len(keys)
                keys.append(key)
            row.append(index[key])
        parts[item] = row
    return keys, parts


def key_dict(key: tuple) -> dict:
    return dict(zip(PART_KEY_FIELDS, key))


# --------------------------------------------------------------------------------------------
# The calendar
# --------------------------------------------------------------------------------------------

def month_day(day: int) -> Tuple[int, int]:
    """1..366 -> (month, day) on the leap calendar (day 60 is Feb 29)."""
    from pipeline.regs.parsing.catalogue import _LAST_DAY
    m = 1
    while day > _LAST_DAY[m]:
        day -= _LAST_DAY[m]
        m += 1
    return m, day


def day_of(month: int, day: int) -> int:
    from pipeline.regs.parsing.catalogue import _day_index
    return _day_index(month, day)


def segments_of(signatures: Sequence[tuple]) -> List[int]:
    """The start days of the runs of equal signatures over days 1..366. Day 1 always starts a
    segment: a reading running across New Year is two segments (the last and the first), which
    share their values — the file stores a value once however many segments point at it."""
    if len(signatures) != DAYS:
        raise AnswersError(f"answers: a signature per day must cover {DAYS} days")
    starts = [1]
    for d in range(2, DAYS + 1):
        if signatures[d - 1] != signatures[d - 2]:
            starts.append(d)
    return starts


# --------------------------------------------------------------------------------------------
# The rule-set evaluation: one per (ruleset, steelhead_water, steelhead_rules)
# --------------------------------------------------------------------------------------------
#
# The reader's answer for a section depends on three things only: its bound rules (the rule set),
# whether a rainbow over 50 cm is a steelhead there, and whether the steelhead rules apply there
# (`read.effective_rules`, the status index's key). Every part key holding the same three shares it.

def eval_key(key: tuple) -> Tuple[int, bool, bool]:
    k = key_dict(key)
    return (k["ruleset"], k["steelhead_water"], k["steelhead_rules"])


def fish_of(bound: Sequence[tuple], rules: dict, steelhead_rules: bool) -> Tuple[str, ...]:
    """THE FISH ASKED ABOUT for a rule set: every game fish (the book's list, p.80,
    `catalogue.BOOK_SPECIES`) any member rule names in `species` (groups expanded), plus "ST"
    where the steelhead rules apply. A rule naming only an open group (`ALL_FIN_FISH`) names no
    fish by itself. This is a superset of the consumer page's Stage 5.1 (which asks only fish named
    by applying retention rules, minus protected species and `ALL_GAME_FISH` rules); the page
    filters as it does now (DESIGN D6)."""
    from pipeline.regs.parsing.catalogue import BOOK_SPECIES, expand_species
    named = set()
    for e, r, _ in bound:
        x = rules.get((e, r))
        if x is None:
            raise AnswersError(f"answers: rule set member {e}::{r} is not in the bundle's rules")
        named.update(f for f in expand_species(list(x.get("species") or [])) if f in BOOK_SPECIES)
    if steelhead_rules:
        named.add("ST")
    return tuple(f for f in BOOK_SPECIES if f in named)


def reading_signature(bound: Sequence[tuple], rules: dict, md: Tuple[int, int]) -> tuple:
    """What the reader reads of the calendar on one day: every member rule's `when` and every
    lift's `when` (`read.in_force`: "yes" / "no" / "part"). Two days with one signature get one
    answer (the status index's `set_profile` shortcut, proved there against the reader)."""
    keys = [(e, r) for e, r, _ in bound]
    whens = tuple(read.in_force(rules[k].get("when"), md) for k in keys)
    lifts = tuple(read.in_force(x.get("when"), md) for k in keys
                  for x in (rules[k].get("exempts") or []) if "when" in x)
    return whens + lifts


def ladder_verdict(rows: Iterable[dict]) -> dict:
    """The traced reader's answer -> {rule id: [state, partly_lifted_by | None, reason | None,
    by | None]}: exactly what it returned, nothing dropped."""
    out = {}
    for x in rows:
        k = read.rid(x)
        if k in out:
            raise AnswersError(f"answers: the reader returned {k} twice")
        st = x["state"]
        if st in read.SPEAKER_STATES:
            if "reason" in x or "by" in x:
                raise AnswersError(f"answers: speaker {k} carries a reason")
            partly = sorted(x["lifted_in_part_by"]) if x.get("partly_lifted") else None
            if x.get("partly_lifted") and not partly:
                raise AnswersError(f"answers: {k} is partly lifted by no rule")
            out[k] = [st, partly, None, None]
        else:
            reason, by = x.get("reason"), x.get("by")
            if reason not in read.LOSS_REASONS or read.LOSS_REASONS[reason] != st or not by:
                raise AnswersError(f"answers: loser {k} has state {st!r}, reason {reason!r}, by {by!r}")
            out[k] = [st, None, reason, by]
    return out


# ---- the decided answer (consumer Stage 5.2 steps 1-4) ---------------------------------------

def kind_of(x: dict) -> str:
    """The consumer page's rule kind (Stage 2.3, `kindOf` in page_v35.js), on the reader's rule
    fields — the first test that holds. Only `gate` (a release or a closure: take 0, no lengths)
    and `pool` (a daily number) can win; the other kinds take no part in `decide`, but the order of
    the tests decides which rules are gates and pools, so all fifteen are kept as written."""
    if x.get("family") == "gear_and_method":
        return "gear"
    if x.get("family") == "conduct":
        return "conduct"
    if x.get("family") == "vessel":
        return "vessel"
    if x.get("type") == "angler_closure":
        return "anglerclosure"
    if x.get("type") == "stop_fishing_after_quota":
        return "duty"
    # JS `!f.lengths` is true only for a missing/null `lengths` (an empty list is truthy)
    if x.get("dimension") == "lift" or (x.get("exempts") and x.get("take") is None
                                        and not x.get("unlimited") and x.get("lengths") is None):
        return "exempt"
    if x.get("standing"):
        return "standing"
    if x.get("while"):
        return "while"
    if x.get("per_daily"):
        return "possession"
    if x.get("period") and x.get("period") != "daily":
        return "annual" if (x.get("take") or 0) > 0 else "duty"
    has_lengths = bool(x.get("lengths"))
    if x.get("within"):
        return "sizecap" if has_lengths else "subcap"
    if x.get("take") == 0 and not has_lengths:
        return "gate"
    if (x.get("take") or 0) > 0 or x.get("unlimited"):
        return "pool"
    if has_lengths:
        return "size"
    return "duty"


def closes(x: dict) -> bool:
    """A gate that may not be fished for (`may_target` false): a closure, not a release. The
    bundle stores `may_target` as 0/1 (null when the book says nothing: not a closure)."""
    return x.get("may_target") is not None and not x["may_target"]


def in_scope(x: dict, origin: str) -> bool:
    """Stage 5.2's scope: a retention rule or a duty, not a `while` limit, a lift or a standing
    rule, of no origin or the origin asked."""
    k = kind_of(x)
    return ((x.get("type") == "retention_limit" or k == "duty")
            and k not in ("while", "exempt", "standing") and x.get("dimension") != "lift"
            and (not x.get("origin") or x.get("origin") == origin))


def rank_here(x: dict, via: str) -> int:
    """The rule's rung where it is bound (`read.source_of(...).rank`; a water row reaching the
    section by the tributary walk ranks 1, as the reader's `place` reads it)."""
    r = read.source_of(x).rank
    return 1 if via == "trib" and r == 0 else r


def decide(verdict: dict, origin: str, rules: dict, via: dict, order: dict) -> list:
    """THE DECIDED ANSWER for one fish and one KNOWN origin (consumer Stage 5.2 steps 1-4, page_v35
    `evalSp`), from the reader's verdict only — the rules that SPEAK, in scope (`in_scope`):

      1. WINNER: the closure of the lowest rank, else the release of the lowest rank, else the
         pool with the smallest number (unlimited is the largest), lower rank first between equal
         numbers. Ties beyond that go to the rule earlier in the export's `rules` order (`order`)
         — the page keeps its part's member order (reach before trib, rule order within each),
         which is the same order for every tie the corpus holds between two rules of one rank.
      2. STATUS: a closure "closed", a release "release", an unlimited pool "no_limit", any other
         pool "keep"; no winner "no_rule".
      3. DAILY: the winning pool's take (null for a gate and for no_limit). The page then narrows
         it by the winner's sub-caps and orphan clauses (step 5) — v1 (`rows`), not here.
      4. Roles are v1.

    Returns [status, daily, winner rule id | None]."""
    if origin not in read.ASKABLE_ORIGINS:
        raise AnswersError(f"answers: decide is asked for a known origin, not {origin!r}")
    A = [k for k, v in verdict.items() if v[0] == "speaks" and in_scope(rules[k], origin)]

    def rk(k):
        return (rank_here(rules[k], via[k]), order[k])

    gates = [k for k in A if kind_of(rules[k]) == "gate"]
    closed = sorted((k for k in gates if closes(rules[k])), key=rk)
    rel = sorted((k for k in gates if not closes(rules[k])), key=rk)
    pools = sorted((k for k in A if kind_of(rules[k]) == "pool"),
                   key=lambda k: (float("inf") if rules[k].get("unlimited") else rules[k]["take"],)
                   + rk(k))
    win = (closed or rel or pools or [None])[0]
    if win is None:
        return ["no_rule", None, None]
    x = rules[win]
    if kind_of(x) == "gate":
        return ["closed" if closes(x) else "release", None, win]
    if x.get("unlimited"):
        return ["no_limit", None, win]
    return ["keep", int(x["take"]), win]


def decide_unknown(h: list, w: list) -> list:
    """The answer with the origin NOT known: the hatchery and the wild answers when they agree,
    else `by_origin` (no daily, no winner — read the two)."""
    return list(h) if h == w else ["by_origin", None, None]


# --------------------------------------------------------------------------------------------
# Sections
# --------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Section:
    """One named, versioned part of the answers file.

    `scope(key)`        what of the part key the section's answers depend on (hashable);
    `prepare(scope, ctx)` -> (signature index per day [366], [value per distinct signature]);
    `derive_from`       or: the name of a section of the SAME scope whose values this one is a
                        function of, and `derive(value_of_that, scope, ctx)` -> value."""
    name: str
    version: int
    scope: Callable[[tuple], tuple]
    prepare: Optional[Callable] = None
    derive_from: Optional[str] = None
    derive: Optional[Callable] = None


class Context:
    """What every producer reads, loaded once per process."""

    def __init__(self, bundle: str, sets: Dict[int, list], rule_index: Dict[str, int]):
        self.bundle = bundle
        self.sets = sets
        self.rule_index = rule_index
        self.rules = read._rules_of(bundle)            # the reader's own rule table
        self.by_id = {f"{e}::{r}": x for (e, r), x in self.rules.items()}


def _ladder_prepare(scope: tuple, ctx: Context):
    set_id, sw, sr = scope
    bound = ctx.sets.get(set_id)
    if not bound:
        raise AnswersError(f"answers: rule set {set_id} has no members in the bundle")
    fish = fish_of(bound, ctx.rules, sr)
    sigs = [reading_signature(bound, ctx.rules, month_day(d)) for d in range(1, DAYS + 1)]
    distinct: Dict[tuple, int] = {}
    per_day = []
    for s in sigs:
        per_day.append(distinct.setdefault(s, len(distinct)))
    first = {}
    for d, i in enumerate(per_day, start=1):
        first.setdefault(i, d)
    values = []
    for i in range(len(distinct)):
        md = month_day(first[i])
        v = {}
        for f in fish:
            v[f] = {o: ladder_verdict(read.effective_rules_bound(
                bound, sw, md, f, ctx.bundle, steelhead_rules_here=sr,
                origin=None if o == "none" else o, trace=True)) for o in ORIGINS}
        values.append(v)
    return per_day, values


def _answer_derive(ladder_value: dict, scope: tuple, ctx: Context) -> dict:
    set_id = scope[0]
    via = {f"{e}::{r}": v for e, r, v in ctx.sets[set_id]}
    out = {}
    for f, by_origin in ladder_value.items():
        rules = {}
        for verdict in by_origin.values():
            for k in verdict:
                if k not in via:
                    raise AnswersError(f"answers: the reader answered with {k}, not a member of "
                                       f"rule set {set_id}")
                rules[k] = ctx.by_id[k]
        h = decide(by_origin["hatchery"], "hatchery", rules, via, ctx.rule_index)
        w = decide(by_origin["wild"], "wild", rules, via, ctx.rule_index)
        out[f] = {"none": decide_unknown(h, w), "hatchery": h, "wild": w}
    return out


SECTIONS: Tuple[Section, ...] = (
    Section("ladder", 0, eval_key, prepare=_ladder_prepare),
    Section("answer", 0, eval_key, derive_from="ladder", derive=_answer_derive),
)

#: Sections named and reserved for v1 — ABSENT from a v0 file (never present empty or partial).
RESERVED = {
    "rows": "the narrowed daily number and every clause line (consumer 5.2 steps 5-11), fish "
            "grouped by shared limit with go-backs and cross-references (5.3), the row scope and "
            "badge (5.5-5.6), the real daily limit (5.7), keep ranges and band numbers (5.8), "
            "roles per rule (5.2 step 3, 6.1); derived from `ladder` + `answer`",
    "gear": "resolved gear slots per part key x segment (consumer 7.1): agent D's "
            "pipeline/deliver/answers/gear.py",
    "licence": "documents per part key x segment x angler profile (consumer 7.7): agent D's "
               "pipeline/deliver/answers/licence.py",
    "display": "derived display facts — rule `kind`, `bands`, `plain` sentences and tags, part "
               "labels / place / hint / order / group and closed-all-year, the part's status per "
               "segment (open / closed / open except some parts) and the closures that close it, "
               "the fish the card asks about (5.1), the steelhead presence line (consumer 2.3, "
               "2.4, 3.1-3.4, 5.1, 5.6, 6.3): agent D's display.py",
}


# --------------------------------------------------------------------------------------------
# Build
# --------------------------------------------------------------------------------------------

_WORKER: Optional[Context] = None


def _init_worker(bundle, sets, rule_index):
    global _WORKER
    _WORKER = Context(bundle, sets, rule_index)


def _run_scope(task):
    """(section name, scope) -> (section name, scope, per_day, {section: values}) for the section
    and every section derived from it."""
    name, scope = task
    sec = next(s for s in SECTIONS if s.name == name)
    per_day, values = sec.prepare(scope, _WORKER)
    out = {name: values}
    for d in SECTIONS:
        if d.derive_from == name:
            if d.scope is not sec.scope:
                raise AnswersError(f"answers: section {d.name} derives from {name} with another scope")
            out[d.name] = [d.derive(v, scope, _WORKER) for v in values]
    return name, scope, per_day, out


@dataclass
class Model:
    """The answers, decoded: what `encode.encode` writes and `encode.decode` returns."""
    about: dict
    keys: List[tuple]
    parts: Dict[str, List[Optional[int]]]
    segments: List[List[int]]                     # per key: start days
    sections: Dict[str, List[List[object]]]       # name -> per key -> per segment -> value
    versions: Dict[str, int]


def build(bundle: str, export_dir: Path, *, workers: int = 0, items: Optional[Iterable[str]] = None,
          log=print) -> Model:
    """Every answer for every part of every named water in the export (or only `items`)."""
    import time
    t0 = time.time()
    data, guide = load_export(export_dir)
    inp = check_inputs(bundle, data, guide)
    keys, parts = part_keys(inp)
    if items is not None:
        want = set(items)
        unknown = sorted(want - set(parts))
        if unknown:
            raise AnswersError(f"answers: no such water in the export: {unknown[:5]}")
        parts = {i: p for i, p in parts.items() if i in want}
        used = sorted({k for p in parts.values() for k in p if k is not None})
        remap = {k: n for n, k in enumerate(used)}
        keys = [keys[k] for k in used]
        parts = {i: [None if k is None else remap[k] for k in p] for i, p in parts.items()}
    roots = [s for s in SECTIONS if s.prepare is not None]
    tasks = sorted({(s.name, s.scope(k)) for s in roots for k in keys}, key=repr)
    log(f"answers: {len(keys)} part keys, {len(tasks)} section scopes to evaluate")
    workers = workers or max(1, (os.cpu_count() or 2) - 1)
    results: Dict[Tuple[str, tuple], tuple] = {}
    args = (bundle, inp.sets, inp.rule_index)
    if workers == 1:
        _init_worker(*args)
        got = map(_run_scope, tasks)
    else:
        pool = get_context("spawn").Pool(workers, initializer=_init_worker, initargs=args)
        got = pool.imap_unordered(_run_scope, tasks, chunksize=4)
    for n, (name, scope, per_day, by_section) in enumerate(got, start=1):
        results[(name, scope)] = (per_day, by_section)
        if n % 250 == 0:
            log(f"  {n}/{len(tasks)} scopes, {time.time() - t0:.0f} s")
    if workers != 1:
        pool.close()
        pool.join()

    segments: List[List[int]] = []
    sections: Dict[str, List[List[object]]] = {s.name: [] for s in SECTIONS}
    for key in keys:
        per_root = {s.name: results[(s.name, s.scope(key))] for s in roots}
        combined = list(zip(*(per_root[s.name][0] for s in roots)))
        starts = segments_of(combined)
        segments.append(starts)
        for s in SECTIONS:
            root = s.name if s.prepare is not None else s.derive_from
            per_day, by_section = per_root[root]
            vals = by_section[s.name]
            sections[s.name].append([vals[per_day[d - 1]] for d in starts])
    about = {
        "what": "What the rules come to, for every part of every named water in the paired UI "
                "export, on every day, for every fish and origin. Generated by "
                "pipeline/deliver/answers (v0): every state from read.effective_rules_bound.",
        "bundle": data["about"]["bundle"],
    }
    log(f"answers: built in {time.time() - t0:.0f} s")
    return Model(about=about, keys=keys, parts=parts, segments=segments, sections=sections,
                 versions={s.name: s.version for s in SECTIONS})
