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

THE KEYING, shared by every section, is `common.py` (the one keying module): the bundle
(`common.load`), the export pairing (`common.check_export`), the part keys (`common.part_keys`), the
rule key (`common.RuleKey`), the calendar and the segments (`common.segments`, `common.segments_of`).
A section's value is a function of (part key, segment); the file's segments are the union of every
section's cuts. Adding a section is a new `Section` with its own `scope` and producer: the existing
sections' code does not change.

v0 SECTIONS
  ladder   per fish x origin: every rule's state (speaks, beside, shown, not_yet_mapped) and every
           loser (lifted, displaced, moot) with its reason and `by` — the traced reader, verbatim.
  answer   per fish x origin: the decided answer — status (closed / release / keep / no_limit /
           no_rule / by_origin), the daily number, the winner rule (`decide`).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from multiprocessing import get_context
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from pipeline.deliver.answers import common
from pipeline.deliver.answers.common import (DAYS, PART_KEY_FIELDS, AnswersError, RuleKey,  # noqa: F401
                                             day_of, key_dict, load_export, month_day, per_day,
                                             rule_vectors, segments, segments_of)
from pipeline.deliver.bundle import read

#: The origins every question is asked for: "none" is the reader's default (origin not known,
#: `origin=None`); "hatchery" / "wild" are `read.ASKABLE_ORIGINS`.
ORIGINS = ("none",) + tuple(read.ASKABLE_ORIGINS)

#: The decided statuses (`decide`). `no_rule`: no rule in scope speaks for the fish (the page shows
#: no row). `by_origin`: asked with the origin unknown, the hatchery and wild answers differ — read
#: those two.
STATUSES = ("closed", "release", "keep", "no_limit", "no_rule", "by_origin")


# --------------------------------------------------------------------------------------------
# The rule-set evaluation: one per rule key (common.RuleKey)
# --------------------------------------------------------------------------------------------
#
# The reader's answer for a section depends on three things only: its bound rules (the rule set),
# whether a rainbow over 50 cm is a steelhead there, and whether the steelhead rules apply there
# (`read.effective_rules`, the status index's key). Every part key holding the same three shares it.

def eval_key(key: tuple) -> RuleKey:
    """The ladder's and the answer's scope: the part key's rule key (`common.rule_key`)."""
    return common.rule_key(key)


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
    """What every producer reads, loaded once per process: the bundle (`common.load`) and the
    export's rule index (`common.check_export`)."""

    def __init__(self, bundle: str, rule_index: Dict[str, int]):
        self.B = common.load(bundle)
        self.bundle = self.B.path
        self.sets = self.B.sets
        self.rule_index = rule_index
        self.rules = self.B.rules                      # the reader's own rule table
        self.by_id = {common.rule_id(k): x for k, x in self.rules.items()}


def _ladder_prepare(scope: RuleKey, ctx: Context):
    bound = ctx.sets.get(scope.set_id)
    if not bound:
        raise AnswersError(f"answers: rule set {scope.set_id} has no members in the bundle")
    for e, r, _ in bound:
        if (e, r) not in ctx.rules:
            raise AnswersError(f"answers: rule set member {e}::{r} is not in the bundle's rules")
    fish = fish_of(bound, ctx.rules, scope.steelhead_rules)
    runs, readings = segments(rule_vectors([ctx.rules[(e, r)] for e, r, _ in bound]))
    values = []
    for first in readings:
        md = month_day(first)
        v = {}
        for f in fish:
            v[f] = {o: ladder_verdict(read.effective_rules_bound(
                bound, scope.steelhead_water, md, f, ctx.bundle,
                steelhead_rules_here=scope.steelhead_rules,
                origin=None if o == "none" else o, trace=True)) for o in ORIGINS}
        values.append(v)
    return per_day(runs), values


def _answer_derive(ladder_value: dict, scope: RuleKey, ctx: Context) -> dict:
    set_id = scope.set_id
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


def _init_worker(bundle, rule_index):
    global _WORKER
    _WORKER = Context(bundle, rule_index)


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
    B = common.load(bundle)
    rule_index = common.check_export(B, data, guide)
    keys, parts = common.part_keys(B, data)
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
    args = (B.path, rule_index)
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
