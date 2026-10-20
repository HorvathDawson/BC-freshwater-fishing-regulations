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
    `prepare(scope, ctx)`   -> (reading index per SLOT [366 x the key's moments, day-major; 366
                            for a section that does not read the rule key], [value per reading]);
    `derive_from`, `derive` or: a section whose values are a function of another's, per reading
                            (`derive(value_of_that, scope, ctx, first_day, at)` -> value, `at`
                            the reading's moment). Its scope
                            may be FINER than the parent's (it may read more of the part key), but
                            the parent's scope must be a function of it: it adds no day cuts;
    `static(ctx, data, guide, keys, parts)` -> {table: value}: tables of the section that are not keyed by
                            (part key, segment) — per export rule, per water;
    `compose_from`, `compose` a value finished from OTHER sections' values at the same part key and
                            segment (`compose(value, {section: value})` -> value; answers 2.6: the
                            display frame's strip code reads the rows and gear frames there). The
                            sections it reads are built with it."""
    name: str
    version: int
    scope: Callable
    prepare: Optional[Callable] = None
    derive_from: Optional[str] = None
    derive: Optional[Callable] = None
    static: Optional[Callable] = None
    compose_from: Tuple[str, ...] = ()
    compose: Optional[Callable] = None


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

    def moments_of(self, scope) -> list:
        """The moments a section scope's values are cut by: its rule key's (the verdicts'), or
        [ALWAYS] for a scope that is no rule key of the bundle (the tidal scope, licence keys)."""
        from pipeline.deliver.calendar import ALWAYS
        rk = scope if isinstance(scope, RuleKey) else None
        if rk is None and isinstance(scope, tuple) and scope and isinstance(scope[0], RuleKey):
            rk = scope[0]
        k = self.B.key_ix.get(rk) if rk is not None else None
        return [ALWAYS] if k is None else self.store.moments(k)


def _ladder_scope(key: tuple, B) -> RuleKey:
    return eval_key(key)


# --------------------------------------------------------------------------------------------
# SLOTS: a day of the year at one MOMENT of the key (answers 2.1)
# --------------------------------------------------------------------------------------------
#
# A section that reads the rule key reads it at every MOMENT of the key (`calendar.moments`: its
# weekday classes, inside / outside its hours window — one, ALWAYS, for almost every key). Its
# `prepare` returns one value index per SLOT, slot = (day - 1) * M + moment for the key's M
# moments, so a key with one moment is exactly its 366 days. A section that does not read the rule
# key (the licence) returns 366 and holds at every moment.

def slots(store, key: int) -> List[int]:
    """The reading of each slot of a rule key: day-major, moment-minor (`store.runs` per
    moment)."""
    m_count = len(store.moments(key))
    out = [0] * (DAYS * m_count)
    for m in range(m_count):
        for d, r in enumerate(per_day(store.runs(key, m))):
            out[d * m_count + m] = r
    return out


def _ladder_prepare(scope: RuleKey, ctx: Context):
    """The key's year (one reading per slot: day x moment) and, per reading, every asked fish's
    verdict id per origin — the stored verdicts (`verdicts.sqlite`, DATAFLOW P6): the reader is not
    called here. Every fish the verdicts asked is listed (every game fish, and the extras a member
    rule names)."""
    st = ctx.store
    k = ctx.B.key_ix[scope]
    fish = st.fish(k)
    values = []
    for rd in st.readings(k):
        values.append({f: {o: st.verdict_id(k, rd.ix, f, o) for o in ORIGINS} for f in fish})
    return slots(st, k), values


def _sections() -> Tuple[Section, ...]:
    from pipeline.deliver.answers import display, gear, licence, rows
    return (
        # answers 2.1: ladder 1, rows 3, gear 3, display 3 — cut by MOMENT (a weekday or hours
        # rule decides at its moments); display adds `closing` (gap G1)
        Section("ladder", 1, _ladder_scope, prepare=_ladder_prepare),
        # answers 2.3: rows 5 (G4 `items[].ranges`), gear 4 (Q38 `caught`), licence 4 (G2
        # `others`/`guiding` with their printed paths), display 4 (Z12 `unless_opened`, G3
        # `rules[].subsets`, Q15/Q38 words in `rules[].plain`)
        # answers 2.5: rows 6 (F13 a size release is an answer; F14 `rows[].group` /
        # `rows[].elsewhere`, the group totals and the kept-elsewhere line)
        # answers 2.8: rows 8 (`group` is the CARD: headline, origin, lifts, total_line, buckets, worked,
        # states — `says`/`items` gone; RULINGS Q23b in `elsewhere`)
        # ROUND10 W1: rows 9 (`group.lifts[]` one per lifting entry with `each` [{kinds, n, by, fish}])
        # ROUND11 A1: rows 10 (`group.buckets[].notes`: a shared size cap said once, on its bucket)
        # ROUND12 1-4: rows 11 (a kind row's size says a lifted limit, "any length, 50 cm or less", and
        # a slot's ranges; a water's own bucket first; every cap inside the group a bucket note, nested
        # in order (`notes[].then`), a count `over_cm` 0; ONE list `group.worked` folding the
        # kept-elsewhere examples in, each line with its basket (at, over_cm, of, most); the card
        # row's `elsewhere.ask` empty)
        # ROUND13 Z/S/D/P: rows 12 (a kind keeping none says "catch and release"; `group.not_kept` every
        # fish of the row nobody may keep, with whose rule; a lifting bucket's `replaces`; the lake line
        # says the count left)
        # ROUND14 3-6: rows 13 (`buckets[].groups` nested count groups, a water's own equal pool its own
        # bucket; `presence` inside the steelhead section; `rows[].week` a weekday window in words;
        # `not_kept` one line per status and row, book order, groups with their members, `rules`)
        # ROUND15 1/3/5-8: rows 14 (`buckets[].sizes` a size cap is a group naming the kinds it binds, the
        # size notes gone; `buckets[].between` an own-rule bucket's shared cap; `rows[].restates` a same-row
        # restatement as a note, one ladder line; `week.status` / `week.parts` [{days, when, weekdays, status,
        # n, says, rules, today}]; a tributaries-only rule "…’s rule for its tributaries"; no presence on a
        # wholly closed water; each kept kind says its origin; "None more here"; no "the" before a lake)
        # ROUND16 1/2/Q2: rows 15 (`group.table` / `group.cap_kinds` / `group.caps`: the species × size table
        # and the exact cap system, gate-proved; a water's own limit says whose, its number and what counts;
        # `restates` folds another row's identical statement no nearer than the winner's)
        # ROUND18 1/2/3/4/7/8: rows 16 (`group.tree`: the card as nested tiers — total, a water's own rule (with
        # `why_here` where the tributary walk brought it), the share, a count, a size cap (RULINGS Q16 as one tier:
        # "1 over 50 cm / 20 in a day — a second may be a hatchery steelhead") — each kind once as a sentence with its
        # `also_counts`, gate-proved; `group.header` one compact line + an origin tag; sizes in cm AND inches in
        # the tree, the lift lines, the headline's numbers and the kept-elsewhere lines)
        # ROUND19 1/2/3/5/6: rows 17 (a size tier HOLDS every kind it caps — the steelhead Q16 lets past inside, its
        # `size_note`, the tier "Only 1 of these may be over 50 cm (20 in) — or 2, if one is a hatchery steelhead";
        # the origin in the fish's names; sizes "50 cm (20 in)"; table bands and cap kinds `from_in`/`to_in` (the
        # boundary standard); "1 between cutthroat and rainbow trout"; "(part of the 4)"; replaces names the fish)
        # UR1 A/B/D/F/G (user panel round 1): rows 18 (totals "per angler"; the share names the lifted fish that don't
        # use it; a size tier "Only 1 of the fish in this box may be over 50 cm (20 in) — 2 if one is a hatchery
        # steelhead"; a count tier "1 cutthroat or rainbow trout in all"; a species closure "no fishing for them, not
        # even catch and release — release any you hook by accident"; duty "write each one on your licence right away")
        # UR2 1-5 (user panel round 2): rows 19 (headline "2 a day per angler from this stream"; worked lines "If you
        # keep 3 hatchery rainbow trout: 1 more fish of any kind, from here or from a lake."; "Only 1 of the 2 may be
        # …"; "hatchery steelhead only — wild steelhead go back"; the presence line "…, so a rainbow trout over 50 cm
        # (20 in) counts as a steelhead."; "after you keep 2, stop fishing for the day")
        # UR3 3/4/6 (user panel round 3): rows 20 (a count tier inside a water's own tier or the share "(part of the
        # 2)"; the share "2 a day per angler from this stream", a lifted one "— your hatchery rainbow trout count toward
        # the 4, not these 2"; a quota two waters share "Kokanee from the Lower and Upper West Arms together: no more
        # than 5 a day.")
        # ROUND20 1/2 (owner review of v24): rows 21 (`group.header` IS the tree's root tier — `tier` its id, `says` its
        # line; a water's own total says "per angler"; the share "Only 2 of the 4 may be from this stream", lifted
        # ", not counting hatchery rainbow trout")
        # ROUND20b C1-C4 (owner: fewer words): rows 22 (a single-kind own tier names the fish and folds the kind — `folded`;
        # the own tier line drops "Replaces …"; the share before a water's own tiers; the worked keep-N lines say the two
        # ends of the range only)
        # ROUND20f 3: rows 23 (a region's share says "from streams"; a water's own share keeps "from this stream")
        # ROUND20g 1/2: rows 24 (the origin rule on the share's tier where it holds on the share's water only, "From
        # streams, keep hatchery fish only — …"; no steelhead `size_note`; "stop fishing here after 2 a day", "record each on
        # your licence right away"; "A rainbow trout over 50 cm (20 in) here counts as a steelhead.")
        # ROUND20h 3: rows 25 (`group.possession` {per_daily, of, n, by, example, doubles}: the possession quota on the
        # group's total, "With 4 trout and char a day you may have 8: 2 days’ fish.")
        # ROUND20i 1/2/5/6c: rows 26 (kind `size_rule` "keep 30–50 cm (12–20 in); release the rest"; the worked keep-share
        # line "… up to 2 more of any kind, from here or from lakes."; "stop fishing this river after 2 a day"; "Your own
        # limit, counted across every Region 2 stream you fish today.")
        Section("rows", 26, rows.section_scope, derive_from="ladder", derive=rows.section_derive),
        # answers 2.6: gear 5 (static `count_nouns`: "1 line (rod)")
        # ROUND10 W4: gear 6 (`counts[slot].cites`, the Lines row's `says`)
        # ROUND11 A2: gear 7 (`summary` {says, parts, terms, cites, changes}; a stream's Lines row
        # "1 line (rod)")
        # ROUND12 5: gear 8 (`summary.says` with no em dash: "1 line with a single barbless hook (one
        # hook, lure or fly). Weight up to 1 kg."; the weight part "weight up to 1 kg")
        # ROUND13 G/P: gear 9 (no hook named without a hook restriction; "weight on the line up to 1 kg")
        # UR1 G: gear 10 (weight "1 kg (2.2 lb)"; a boat lifting the line count "1 line from shore; any number from a
        # boat", the Lines row "1 line (rod) from shore; no limit from a boat")
        # ROUND20h 1: gear 11 (`bait[].excepted_from` {element, by}: the ban an allowed element is an exception to)
        # ROUND20i 3/4: gear 12 (a change's dates leave out the closed days; "a single barbless hook, no trebles"; fly fishing
        # only "no float or strike indicator, no sinker or split shot, no lure, spinner or other attracting device")
        Section("gear", 12, gear.section_scope, prepare=gear.section_prepare,
                static=gear.section_static),
        # ROUND15 2: licence 5 (frame `under_16` per residency; statics `residency_labels`, `profile_labels`)
        # ROUND16 4: licence 6 (the under-16 answers say the book's p.6: a resident needs nothing, fishes alone and
        # keeps their own limit; a non-resident's companion holds the licences and stamps the fishing needs)
        Section("licence", 6, licence.section_scope, prepare=licence.section_prepare,
                static=licence.section_static),
        # answers 2.6: rows 7 (F15 the group's lead sentence + `items`), display 5 (`strip` per
        # segment from the rows and gear frames there; `rules[].book` / `applies`; statics
        # `strip`, `reasons`)
        # answers 2.7: display 6 (`waters[].parts[].short`, static `licensing` [{book}]); rows 7
        # (wording only: the Q16 size pair said once, a lift names whose rule and what it allows,
        # "steelhead (rainbow trout over 50 cm)")
        # answers 2.8: display 7 (`rules[].where`: "Where you can’t fish", the fishway and fish-facility
        # buffers on the Keep side, D2)
        # ROUND11 A3: display 8 (frame `inherited`: a lift the tributary walk carries, in words)
        # ROUND13 P: display 9 (`rules[].applies` names the book by its title)
        # ROUND14 1: display 10 (static `reasons` gains `nearer_row`: the nearer ✱ row's words, T14)
        # ROUND15 8: display 11 (an inherited lift's owner takes "the" only before a river, channel or arm)
        # ROUND16 3/Q1: display 12 (`rules[].title` every rule's effect in words; `inherited[].caution` where a
        # nearer row prints no such exemption, RULINGS T15)
        # ROUND19 2/5: display 13 (rule titles and sentences say sizes "50 cm (20 in)"; a slot "none over 60 cm
        # (24 in), under 90 cm (35 in)")
        # UR1 G: display 14 (a record duty "Write each … on your licence right away")
        # UR3 2: display 15 (`rules[].plain` of a no-fishing place rule says feet too, "within 23 m (75 ft) …")
        # ROUND20g 3: display 16 (a possession rule's `plain` "Possession: 2 days’ quota." with the home exception as its
        # own sentence)
        Section("display", 16, display.section_scope, prepare=display.section_prepare,
                static=display.section_static, compose_from=("rows", "gear"),
                compose=display.compose),
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


def _first_slots(per_slot: Sequence[int]) -> List[int]:
    first: Dict[int, int] = {}
    for n, i in enumerate(per_slot):
        first.setdefault(i, n)
    return [first[i] for i in range(len(first))]


def _run_scope(task):
    """(root section, scope, {derived section: [its scopes]}) -> (name, scope, per_day, values,
    {(derived section, scope): values}): a root section's year and every section derived from it,
    for every derived scope that reads this root scope."""
    name, scope, derived = task
    sec = next(s for s in SECTIONS if s.name == name)
    per_day, values = sec.prepare(scope, _WORKER)
    ms = _WORKER.moments_of(scope)
    if len(per_day) not in (DAYS, DAYS * len(ms)) or \
            sorted(set(per_day)) != list(range(len(values))):
        raise AnswersError(f"answers: section {name} returned a year that does not index its values")
    m_count = len(per_day) // DAYS
    firsts = _first_slots(per_day)
    # the ladder travels as verdict ids; what derives from it reads the verdicts themselves
    full = [_WORKER.expand_ladder(v) for v in values] if name == "ladder" else values
    out = {}
    for dname, scopes in derived:
        d = next(s for s in SECTIONS if s.name == dname)
        for ds in scopes:
            out[(dname, ds)] = [d.derive(v, ds, _WORKER, firsts[i] // m_count + 1,
                                         ms[firsts[i] % m_count]) for i, v in enumerate(full)]
    return name, scope, per_day, values, out


@dataclass
class Model:
    """The answers, decoded: what `encode.encode` writes and `encode.decode` returns."""
    about: dict
    keys: List[tuple]
    parts: Dict[str, List[Optional[int]]]
    segments: List[List[int]]                     # per key: start days (a start repeats where
                                                  # the day reads differently by moment)
    sections: Dict[str, List[List[object]]]       # name -> per key -> per segment -> value
    versions: Dict[str, int]
    statics: Dict[str, dict] = field(default_factory=dict)   # name -> {table: value}
    #: per key: None (every segment holds at every moment), or per segment its moment
    #: (`calendar.Moment.as_json`: weekdays, and hours with `in`) — answers 2.1
    moments: List[Optional[List[dict]]] = field(default_factory=list)
    #: the plain-language glossary (answers 2.2, `glossary.build`): {version, terms}
    glossary: Optional[dict] = None
    #: THE UNNAMED WATER (answers 2.4, S1; S1b keyed by content): every region set
    #: [{regions, label, names}], and {first, parts}: the keys from `first` on are unnamed-only
    #: contents, `parts[up]` the key of the bundle's unnamed part `up`
    places: List[dict] = field(default_factory=list)
    unnamed: Optional[dict] = None


def build(bundle: str, export_dir: Path, *, workers: int = 0, items: Optional[Iterable[str]] = None,
          sections: Optional[Sequence[str]] = None, log=print,
          export: Optional[Tuple[dict, dict]] = None, verdicts: Optional[str] = None,
          unnamed: Optional[Iterable[int]] = None) -> Model:
    """Every answer for every part of every named water in the export (or only `items`) AND every
    part of the unnamed water (answers 2.4, S1: the bundle's `unnamed_part`, keyed after the named
    parts — all of them, or only `unnamed` (part ids); none when `items` is given without
    `unnamed`), for every section (or only `sections`, with the sections they derive from).
    `export` is the pair already read (`load_export`), so a caller holding it does not read it
    twice (M9)."""
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
    # THE UNNAMED WATER (S1, S1b): a named key where one holds the content, else a new one after
    first = len(keys)
    keys, part_key, region_places = common.unnamed_keys(
        B, keys, unnamed if unnamed is not None else (None if items is None else []))
    chosen = [s for s in SECTIONS if sections is None or s.name in sections]
    for s in list(chosen):
        for need in s.compose_from:
            if all(c.name != need for c in chosen):
                chosen.append(next(c for c in SECTIONS if c.name == need))
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
    log(f"answers: {len(keys)} part keys ({first} named, {len(keys) - first} unnamed), "
        f"{len(tasks)} section scopes to evaluate")
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
    seg_moments: List[Optional[List[dict]]] = []
    out: Dict[str, List[List[object]]] = {s.name: [] for s in chosen}
    for key in keys:
        per_root = {s.name: results[(s.name, s.scope(key, B))] for s in roots}
        ms = moments_of_key(ctx, key, roots, per_root, B)
        starts, at, picks = cut(ms, [per_root[s.name][0] for s in roots])
        segs.append(starts)
        seg_moments.append(at)
        for s in chosen:
            if s.prepare is not None:
                pd, vals = per_root[s.name]
            else:
                pd = per_root[s.derive_from][0]
                vals = derived_vals[(s.name, s.scope(key, B))]
            m_count = len(pd) // DAYS
            out[s.name].append([vals[pd[(d - 1) * m_count + (m if m_count > 1 else 0)]]
                                for d, m in picks])
    # ROUND14 5 (owner): A WEEKDAY WINDOW SAID AS WEEKDAYS — the rows frames of one day's moments (the
    # segments repeating one start) each carry, on a row whose fish answer differently by weekday, the
    # same `week` sentence ("Keep up to 5 on Saturdays and Sundays; release Monday to Friday")
    if "rows" in out:
        from pipeline.deliver.answers.rows import say_weekdays
        for ki in range(len(keys)):
            at, starts = seg_moments[ki], segs[ki]
            if at is None:
                continue
            j = 0
            while j < len(starts):
                k = j
                while k + 1 < len(starts) and starts[k + 1] == starts[j]:
                    k += 1
                if k > j:
                    out["rows"][ki][j:k + 1] = say_weekdays(out["rows"][ki][j:k + 1], at[j:k + 1])
                j = k + 1
    # a value finished from other sections' values at the same key and segment (answers 2.6)
    for s in chosen:
        if s.compose is None:
            continue
        for ki in range(len(keys)):
            out[s.name][ki] = [s.compose(v, {n: out[n][ki][j] for n in s.compose_from})
                               for j, v in enumerate(out[s.name][ki])]
    # a static table may read the sections' values as built (display G3 reads the rows' frames)
    ctx.cache["sections_out"] = out
    statics = {s.name: s.static(ctx, data, guide, keys, parts) for s in chosen
               if s.static is not None}
    about = {
        "what": "What the rules come to, for every part of every named water in the paired UI "
                "export and every part of the unnamed water (every other section in B.C.), on "
                "every day, for every fish and origin, every angler profile: the "
                "ladder, the decided answer, the card's rows, gear, licence and display facts. "
                "Generated by pipeline/deliver/answers: every state from "
                "read.effective_rules_bound, every requirement from read.requirements_in_force.",
        "bundle": data["about"]["bundle"],
    }
    from pipeline.deliver.answers import glossary as _glossary
    gl = _glossary.build(doc)
    log(f"answers: built in {time.time() - t0:.0f} s")
    from pipeline.deliver.answers.display import places_table
    return Model(about=about, keys=keys, parts=parts, segments=segs, sections=out,
                 versions={s.name: s.version for s in chosen}, statics=statics,
                 moments=seg_moments, glossary=gl, places=places_table(region_places),
                 unnamed={"first": first, "parts": part_key})


def moments_of_key(ctx: Context, key: tuple, roots, per_root, B) -> list:
    """A part key's moments: those of the root sections cut by more than one (all the same — the
    part's rule key's), or [ALWAYS]."""
    from pipeline.deliver.calendar import ALWAYS
    got = None
    for s in roots:
        ms = ctx.moments_of(s.scope(key, B))
        if len(per_root[s.name][0]) == DAYS * len(ms) and len(ms) > 1:
            if got is not None and got != ms:
                raise AnswersError(f"answers: two sections cut part key {key} by different moments")
            got = ms
    return got or [ALWAYS]


def cut(ms: list, per_slots: Sequence[Sequence[int]]):
    """THE FILE'S SEGMENTS of one part key: the days where any section's value changes at any
    moment, and at each such start, the moments whose values differ (answers 2.1). Returns
    (`starts` — a start day repeats once per moment group, `at` — None when no start repeats, else
    per segment its moment as `Moment.as_json`, `picks` — per segment the (day, moment index) whose
    values it holds). Moments with equal values at a start are merged: first a weekday class's
    inside and outside of the hours window (into the whole day), then weekday classes alike in
    everything (their weekdays joined). Ordered by the group's first weekday, outside before
    inside. A key with one moment is exactly the old cut."""
    from pipeline.deliver.calendar import ALWAYS, Moment
    m_count = len(ms)

    def val(d, m):
        return tuple(p[(d - 1) * (len(p) // DAYS) + (m if len(p) > DAYS else 0)] for p in per_slots)
    starts0 = segments_of([tuple(val(d, m) for m in range(m_count)) for d in range(1, DAYS + 1)])
    starts: List[int] = []
    picks: List[Tuple[int, int]] = []
    groups_at: List[Optional[Moment]] = []
    for d in starts0:
        vals = [val(d, m) for m in range(m_count)]
        if len(set(vals)) == 1:
            starts.append(d)
            picks.append((d, 0))
            groups_at.append(None)
            continue
        # 1. per weekday class, inside == outside -> the whole day
        by_class: Dict[int, List[Tuple[Moment, int]]] = {}
        for m, mo in enumerate(ms):
            by_class.setdefault(mo.weekdays, []).append((mo, m))
        entries = []          # (weekdays, [(hours, inside, value, m)])
        for wd, lst in by_class.items():
            if len(lst) == 2 and vals[lst[0][1]] == vals[lst[1][1]]:
                lst = [(Moment(wd), lst[0][1])]
            entries.append((wd, [(mo.hours, mo.inside, vals[m], m) for mo, m in lst]))
        # 2. weekday classes alike in everything -> one group
        merged: Dict[tuple, List] = {}
        for wd, lst in entries:
            sig = tuple((h, i, v) for h, i, v, _ in lst)
            g = merged.setdefault(sig, [0, lst])
            g[0] |= wd
        out = []
        for sig, (wd, lst) in merged.items():
            for h, i, _, m in lst:
                out.append((Moment(wd, h, i).check(), m))
        out.sort(key=lambda p: (p[0].weekdays & -p[0].weekdays, p[0].weekdays, bool(p[0].inside)))
        for mo, m in out:
            starts.append(d)
            picks.append((d, m))
            groups_at.append(mo)
    if all(g is None for g in groups_at):
        return starts, None, picks
    return starts, [(g or ALWAYS).as_json() for g in groups_at], picks
