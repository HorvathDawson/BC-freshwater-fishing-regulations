"""THE ANSWERS LAYER (answers/2) — every decided reading of the rules, arranged once, beside the UI
export.

    python -m pipeline.deliver.answers.cli build --bundle FILE --export-dir DIR --out FILE

The UI export (`ui-rules-export.json` + `ui-rules-guide.json`) ships what the book SAYS. This module
works out what it COMES TO for every part of every named water, on every day and for every fish,
and `encode.py` writes it as a third file, `ui-rules-answers.json`, stamped with the export pair's
`about.bundle` digests.

ONE SOURCE PER FACT (DATAFLOW). Every rule state, every loser's reason and `by`, is the reader's,
STORED ONCE by the verdicts stage (`verdicts.sqlite`, `read.effective_rules_bound(trace=True)` per
rule key, reading, fish and origin); nothing here calls the reader. The parts are the bundle's
(`part`), the calendar is `pipeline.deliver.calendar`, closed is `reading.closed`. The decided
answer is `rows` alone (the v0 `answer` section and its `decide` are gone: one decided answer).

NO FALLBACKS. A question that cannot be answered stops the build with an `AnswersError` naming it:
an export pair that does not match the bundle, a rule bound to the other kind of water, a rule the
export does not list, a fish the verdicts were not asked. Every section value is validated against
its answers/2 model (`model.py`: strict, extra=forbid) before it is encoded.

THE KEYING, shared by every section, is `common.py`: the bundle (`common.load`), the export pairing
(`common.check_export`), the part keys (`common.part_keys`, from the bundle's parts). A section's
value is a function of (part key, segment); the file's segments are the union of every section's
cuts. Adding a section is a new `Section` with its own `scope` and producer.

SECTIONS
  ladder   per fish x origin, every fish the verdicts asked: every rule's state (speaks, beside,
           shown, not_yet_mapped) and every loser (lifted, displaced, moot) with its reason and
           `by` — the stored verdict, verbatim
  rows     the card (consumer 5.1-5.8): per fish and origin the decided answer, the rows
  gear     7.1-7.6 · licence 7.7 · display: status per day, per-rule and per-part facts
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from multiprocessing import get_context
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from pipeline.deliver.answers import common
from pipeline.deliver.answers.common import (PART_KEY_FIELDS, AnswersError, RuleKey,  # noqa: F401
                                             key_dict, load_export)
from pipeline.deliver.calendar import (DAYS, day_of, month_day, per_day,  # noqa: F401
                                       rule_vectors, segments, segments_of)
from pipeline.deliver import types as T
from pipeline.deliver.bundle import read

#: The origins every question is asked for: "none" is the reader's default (origin not known,
#: `origin=None`); "hatchery" / "wild" are `read.ASKABLE_ORIGINS`.
ORIGINS = ("none",) + tuple(read.ASKABLE_ORIGINS)



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


# --------------------------------------------------------------------------------------------
# Sections
# --------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Section:
    """One named, versioned part of the answers file. Every section is keyed by the part key and
    the date segment; what differs is what of the part key its answers depend on.

    `scope(key, B)`         what of the part key the answers depend on (hashable);
    `prepare(scope, ctx)`   -> (reading index per day [366], [value per reading]);
    `derive_from`, `derive` or: a section whose values are a function of another's, per reading
                            (`derive(value_of_that, scope, ctx, first_day)` -> value). Its scope
                            may be FINER than the parent's (it may read more of the part key), but
                            the parent's scope must be a function of it: it adds no day cuts;
    `static(ctx, data, guide, keys, parts)` -> {table: value}: tables of the section that are not keyed by
                            (part key, segment) — per export rule, per water."""
    name: str
    version: int
    scope: Callable
    prepare: Optional[Callable] = None
    derive_from: Optional[str] = None
    derive: Optional[Callable] = None
    static: Optional[Callable] = None


class Context:
    """What every producer reads, loaded once per process: the bundle (`common.load`), the
    export's rule index (`common.check_export`) and each producer's own cache (`cache`)."""

    def ladder_dict(self, verdict: int) -> dict:
        """One stored verdict as the ladder states it: {rule id: [state, partly lifting rules |
        None, reason | None, by | None]} — ONE object per verdict id (shared by every frame that
        holds it)."""
        got = self._ladder.get(verdict)
        if got is None:
            ids = self.store.rule_ids
            got = self._ladder[verdict] = {
                ids[r]: [T.by_code(T.RuleState, s).value, [ids[x] for x in lift] or None,
                         None if rs is None else T.by_code(T.LossReason, rs).value,
                         None if by is None else ids[by]]
                for r, s, rs, by, lift in self.store.rows(verdict)}
        return got

    def expand_ladder(self, value: dict) -> dict:
        """A ladder value of verdict ids -> its verdicts (`ladder_dict`)."""
        return {f: {o: self.ladder_dict(v) for o, v in by_o.items()} for f, by_o in value.items()}

    def __init__(self, bundle: str, rule_index: Dict[str, int],
                 licence_reps: Optional[dict] = None, doc: Optional[dict] = None,
                 verdicts: Optional[str] = None):
        from pipeline.deliver.verdicts.store import VerdictStore
        self.B = common.load(bundle)
        #: the reader's every answer, stored (`verdicts.sqlite`): what the sections look up
        self.store = VerdictStore.open(verdicts, bundle) if verdicts else None
        #: the lowest section of each licence key (`licence.representatives`, read in the parent)
        self.licence_reps = licence_reps
        #: the export pair, decoded ONCE (`export_codec.expand`), for the statics (parent only)
        self.doc = doc
        self.bundle = self.B.path
        self.sets = self.B.sets
        self.rule_index = rule_index
        self.rules = self.B.rules                      # the reader's own rule table
        self.by_id = {common.rule_id(k): x for k, x in self.rules.items()}
        self.cache: dict = {}
        self._ladder: Dict[int, dict] = {}


def _ladder_scope(key: tuple, B) -> RuleKey:
    return eval_key(key)


def _ladder_prepare(scope: RuleKey, ctx: Context):
    """The key's year and, per reading, every asked fish's verdict id per origin — the stored
    verdicts (`verdicts.sqlite`, DATAFLOW P6): the reader is not called here. Every fish the
    verdicts asked is listed (every game fish, and the extras a member rule names)."""
    st = ctx.store
    k = ctx.B.key_ix[scope]
    runs = [list(r) for r in st.runs(k)]
    fish = st.fish(k)
    values = []
    for rd in st.readings(k):
        values.append({f: {o: st.verdict_id(k, rd.ix, f, o) for o in ORIGINS} for f in fish})
    return per_day(runs), values


def _sections() -> Tuple[Section, ...]:
    from pipeline.deliver.answers import display, gear, licence, rows
    return (
        Section("ladder", 0, _ladder_scope, prepare=_ladder_prepare),
        Section("rows", 2, rows.section_scope, derive_from="ladder", derive=rows.section_derive),
        Section("gear", 2, gear.section_scope, prepare=gear.section_prepare,
                static=gear.section_static),
        Section("licence", 2, licence.section_scope, prepare=licence.section_prepare,
                static=licence.section_static),
        Section("display", 2, display.section_scope, prepare=display.section_prepare,
                static=display.section_static),
    )


SECTIONS: Tuple[Section, ...] = _sections()

#: Sections named and reserved for a later version — ABSENT from the file (never present empty).
RESERVED: Dict[str, str] = {}


# --------------------------------------------------------------------------------------------
# Build
# --------------------------------------------------------------------------------------------

_WORKER: Optional[Context] = None


def _init_worker(bundle, rule_index, licence_reps, verdicts):
    global _WORKER
    _WORKER = Context(bundle, rule_index, licence_reps, verdicts=verdicts)


def _first_days(per_day: Sequence[int]) -> List[int]:
    first: Dict[int, int] = {}
    for d, i in enumerate(per_day, start=1):
        first.setdefault(i, d)
    return [first[i] for i in range(len(first))]


def _run_scope(task):
    """(root section, scope, {derived section: [its scopes]}) -> (name, scope, per_day, values,
    {(derived section, scope): values}): a root section's year and every section derived from it,
    for every derived scope that reads this root scope."""
    name, scope, derived = task
    sec = next(s for s in SECTIONS if s.name == name)
    per_day, values = sec.prepare(scope, _WORKER)
    if len(per_day) != DAYS or sorted(set(per_day)) != list(range(len(values))):
        raise AnswersError(f"answers: section {name} returned a year that does not index its values")
    days = _first_days(per_day)
    # the ladder travels as verdict ids; what derives from it reads the verdicts themselves
    full = [_WORKER.expand_ladder(v) for v in values] if name == "ladder" else values
    out = {}
    for dname, scopes in derived:
        d = next(s for s in SECTIONS if s.name == dname)
        for ds in scopes:
            out[(dname, ds)] = [d.derive(v, ds, _WORKER, days[i]) for i, v in enumerate(full)]
    return name, scope, per_day, values, out


@dataclass
class Model:
    """The answers, decoded: what `encode.encode` writes and `encode.decode` returns."""
    about: dict
    keys: List[tuple]
    parts: Dict[str, List[Optional[int]]]
    segments: List[List[int]]                     # per key: start days
    sections: Dict[str, List[List[object]]]       # name -> per key -> per segment -> value
    versions: Dict[str, int]
    statics: Dict[str, dict] = field(default_factory=dict)   # name -> {table: value}


def build(bundle: str, export_dir: Path, *, workers: int = 0, items: Optional[Iterable[str]] = None,
          sections: Optional[Sequence[str]] = None, log=print,
          export: Optional[Tuple[dict, dict]] = None, verdicts: Optional[str] = None) -> Model:
    """Every answer for every part of every named water in the export (or only `items`), for
    every section (or only `sections`, with the sections they derive from). `export` is the pair
    already read (`load_export`), so a caller holding it does not read it twice (M9)."""
    import time
    from pipeline.tools.export_codec import expand
    t0 = time.time()
    verdicts = str(verdicts or Path(bundle).with_name("verdicts.sqlite"))
    data, guide = export if export is not None else load_export(export_dir)
    B = common.load(bundle)
    doc = expand(data, guide)                  # decoded ONCE (M9.4): the check and the statics
    rule_index = common.check_export(B, data, guide, doc)
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
    chosen = [s for s in SECTIONS if sections is None or s.name in sections]
    for s in list(chosen):
        if s.derive_from and all(c.name != s.derive_from for c in chosen):
            chosen.insert(0, next(c for c in SECTIONS if c.name == s.derive_from))
    chosen = [s for s in SECTIONS if s in chosen]
    roots = [s for s in chosen if s.prepare is not None]
    # each derived scope reads exactly one root scope (it adds no day cuts)
    parent_of: Dict[Tuple[str, object], object] = {}
    for s in chosen:
        if s.derive_from is None:
            continue
        parent = next(c for c in chosen if c.name == s.derive_from)
        for k in keys:
            ds, ps = s.scope(k, B), parent.scope(k, B)
            if parent_of.setdefault((s.name, ds), ps) != ps:
                raise AnswersError(f"answers: section {s.name}'s scope {ds!r} reads two scopes of "
                                   f"{parent.name}")
    tasks = []
    for s in roots:
        for sc in sorted({s.scope(k, B) for k in keys}, key=repr):
            derived = [(d.name, sorted({ds for (n, ds), ps in parent_of.items()
                                        if n == d.name and ps == sc}, key=repr))
                       for d in chosen if d.derive_from == s.name]
            tasks.append((s.name, sc, derived))
    log(f"answers: {len(keys)} part keys, {len(tasks)} section scopes to evaluate")
    workers = workers or min(4, max(1, (os.cpu_count() or 2) - 1))
    results: Dict[Tuple[str, object], tuple] = {}
    derived_vals: Dict[Tuple[str, object], list] = {}
    from pipeline.deliver.answers import licence as _licence
    lic_scopes = [sc for name, sc, _ in tasks if name == "licence"]
    licence_reps = _licence.representatives(B.path, lic_scopes) if lic_scopes else {}
    args = (B.path, rule_index, licence_reps, verdicts)
    if workers == 1:
        _init_worker(*args)
        got = map(_run_scope, tasks)
    else:
        pool = get_context("spawn").Pool(workers, initializer=_init_worker, initargs=args)
        got = pool.imap_unordered(_run_scope, tasks, chunksize=2)
    for n, (name, scope, pd, values, dv) in enumerate(got, start=1):
        results[(name, scope)] = (pd, values)
        derived_vals.update(dv)
        if n % 250 == 0:
            log(f"  {n}/{len(tasks)} scopes, {time.time() - t0:.0f} s")
    if workers != 1:
        pool.close()
        pool.join()

    ctx = Context(B.path, rule_index, licence_reps, doc, verdicts=verdicts)
    # the ladder's verdict ids -> the verdicts, ONE object per verdict id (M9: the parent no
    # longer holds a copy of every rule state per key, reading, fish and origin)
    for k, (pd, vals) in list(results.items()):
        if k[0] == "ladder":
            results[k] = (pd, [ctx.expand_ladder(v) for v in vals])
    segs: List[List[int]] = []
    out: Dict[str, List[List[object]]] = {s.name: [] for s in chosen}
    for key in keys:
        per_root = {s.name: results[(s.name, s.scope(key, B))] for s in roots}
        combined = list(zip(*(per_root[s.name][0] for s in roots)))
        starts = segments_of(combined)
        segs.append(starts)
        for s in chosen:
            if s.prepare is not None:
                pd, vals = per_root[s.name]
            else:
                pd = per_root[s.derive_from][0]
                vals = derived_vals[(s.name, s.scope(key, B))]
            out[s.name].append([vals[pd[d - 1]] for d in starts])
    statics = {s.name: s.static(ctx, data, guide, keys, parts) for s in chosen
               if s.static is not None}
    about = {
        "what": "What the rules come to, for every part of every named water in the paired UI "
                "export, on every day, for every fish and origin, every angler profile: the "
                "ladder, the decided answer, the card's rows, gear, licence and display facts. "
                "Generated by pipeline/deliver/answers: every state from "
                "read.effective_rules_bound, every requirement from read.requirements_in_force.",
        "bundle": data["about"]["bundle"],
    }
    log(f"answers: built in {time.time() - t0:.0f} s")
    return Model(about=about, keys=keys, parts=parts, segments=segs, sections=out,
                 versions={s.name: s.version for s in chosen}, statics=statics)
