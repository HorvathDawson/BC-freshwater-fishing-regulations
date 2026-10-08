"""THE VERDICTS STAGE — the ONLY production caller of `read.effective_rules_bound` (DATAFLOW P3).

One task per rule key (`rule_key`). For each distinct reading of the key's year
(`calendar.segments` over every member rule's `when` and every lift's — exactly the reader's
signature), each fish asked and each origin, the traced reader answers once; the parent interns
the answers (equal verdicts share one id, in task order, so two builds give the same ids) and
writes `verdicts.sqlite`.

WHICH FISH (Q15). Every game fish on every key (`catalogue.GAME_FISH`: the status predicate is
then a projection, and "ST" is asked everywhere — answered as "RB" where no steelhead rule applies,
RU-6), plus crayfish, chinook and the protected species a member rule NAMES (in `species` or
`when_targeting`; a rule naming `SALMON` names chinook, `catalogue.SALMON_FISH`) — the open-subject
rows (rows R5) and gear's target fish ask those.

WHICH ORIGINS. All three are stored for every fish. Where no member rule lifts anything for one
origin only (`read.origin_matters`), the reader reads no origin at all, so the hatchery and wild
verdicts ARE the none verdict and are not asked again — the reader's own predicate, proved over
every key by `test_verdicts.py` (slow).

MEMORY. ≤ 4 spawned workers, each loading the rules and the rule sets only (no section scan);
a worker returns its key's verdicts as small int tuples, interned locally; the parent holds one
dict of distinct verdicts and writes as results arrive.
"""
from __future__ import annotations

import sqlite3
import sys
import time
from multiprocessing import get_context
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from pipeline.deliver import types as T
from pipeline.deliver.bundle import read
from pipeline.deliver.calendar import month_day, rule_vectors, segments
from pipeline.deliver.verdicts import project
from pipeline.deliver.verdicts.store import VerdictsError, bundle_meta, create

MAX_WORKERS = 4

_STATE = {s.value: T.code(s) for s in T.RuleState}
_REASON = {r.value: T.code(r) for r in T.LossReason}
_FISH = {f.value: T.code(f) for f in T.FishCode}
_ORIGIN = {o.value: T.code(o) for o in T.AskOrigin}
_EXTRA = tuple(f.value for f in T.FishCode if f.value not in T.GAME_FISH)


def asked_fish(rules: Sequence[dict]) -> Tuple[str, ...]:
    """Every game fish, and every other leaf fish a member rule names, in `FishCode` order."""
    from pipeline.regs.parsing.catalogue import SALMON_FISH, expand_species
    named = set()
    for x in rules:
        codes = list(x.get("species") or []) + list(x.get("when_targeting") or [])
        named |= set(expand_species(codes))
        named |= {f for f, g in SALMON_FISH.items() if g in codes}
    return tuple(f.value for f in T.FishCode
                 if f.value in T.GAME_FISH or (f.value in named and f.value in _EXTRA))


def verdict_of(answer: List[dict], rule_ix: Dict[str, int]) -> Tuple[tuple, ...]:
    """The traced reader's answer as `VerdictRow`-shaped int tuples, sorted by rule: exactly what
    it returned — nothing dropped, nothing added — each row checked against the types."""
    rows = []
    for x in answer:
        k = read.rid(x)
        st = x["state"]
        if st in read.SPEAKER_STATES:
            if "reason" in x or "by" in x:
                raise VerdictsError(f"verdicts: speaker {k} carries a reason")
            lifters = tuple(sorted(rule_ix[b] for b in x.get("lifted_in_part_by") or ()))
            if bool(x.get("partly_lifted")) != bool(lifters):
                raise VerdictsError(f"verdicts: {k} partly lifted by {lifters}")
            row = (rule_ix[k], _STATE[st], None, None, lifters)
        else:
            row = (rule_ix[k], _STATE[st], _REASON[x["reason"]], rule_ix[x["by"]], ())
        T.VerdictRow(row[0], T.by_code(T.RuleState, row[1]),
                     None if row[2] is None else T.by_code(T.LossReason, row[2]),
                     row[3], row[4]).check()
        rows.append(row)
    rows.sort()
    if len({r[0] for r in rows}) != len(rows):
        raise VerdictsError("verdicts: the reader returned a rule twice")
    return tuple(rows)


# --------------------------------------------------------------------------------------------
# The worker: one rule key
# --------------------------------------------------------------------------------------------

_W: dict = {}


def _init(bundle: str) -> None:
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    try:
        sets: Dict[int, list] = {}
        for s, e, r, via in db.execute("SELECT set_id, entry_id, rule_id, via FROM ruleset "
                                       "ORDER BY set_id, entry_id, rule_id"):
            sets.setdefault(s, []).append((e, r, via))
        ix = {f"{e}::{r}": i for i, e, r in db.execute("SELECT ix, entry_id, rule_id FROM rule_ix")}
        grade = {i: g for i, g in db.execute(
            "SELECT x.ix, r.closure_grade FROM rule_ix x JOIN rule r "
            "ON r.entry_id = x.entry_id AND r.rule_id = x.rule_id")}
    finally:
        db.close()
    _W.update(bundle=bundle, sets=sets, ix=ix, grade=grade, every=read._rules_of(bundle))


def one_key(task) -> tuple:
    """(key_ix, runs, first days, fish, own, closed per reading, local verdicts, frames)."""
    key_ix, set_id, sw, sr = task
    bundle, every, ix = _W["bundle"], _W["every"], _W["ix"]
    bound = _W["sets"].get(set_id)
    if not bound:
        raise VerdictsError(f"verdicts: rule set {set_id} has no members")
    missing = [f"{e}::{r}" for e, r, _ in bound if (e, r) not in every]
    if missing:
        raise VerdictsError(f"verdicts: rule set {set_id} names rules the bundle lacks: {missing[:3]}")
    rules = [every[(e, r)] for e, r, _ in bound]
    runs, firsts = segments(rule_vectors(rules))
    fish = asked_fish(rules)
    by_origin = read.origin_matters(bound, bundle)
    own = any(not str(e).startswith("z") for e, _, _ in bound)
    local: Dict[tuple, int] = {}
    verdicts: List[tuple] = []
    frames: List[tuple] = []
    closed: List[bool] = []

    def intern(v):
        i = local.get(v)
        if i is None:
            i = local[v] = len(verdicts)
            verdicts.append(v)
        return i

    for ri, first in enumerate(firsts):
        md = month_day(first)
        none_of: Dict[str, tuple] = {}
        for f in fish:
            for o in ("none",) + tuple(read.ASKABLE_ORIGINS):
                if o != "none" and not by_origin:
                    v = none_of[f]
                else:
                    v = verdict_of(read.effective_rules_bound(
                        bound, sw, md, f, bundle, steelhead_rules_here=sr,
                        origin=None if o == "none" else o, trace=True), ix)
                    if o == "none":
                        none_of[f] = v
                frames.append((ri, _FISH[f], _ORIGIN[o], intern(v)))
        closed.append(project.closed(none_of.__getitem__, _W["grade"]))
    return (key_ix, runs, firsts, fish, own, closed, verdicts, frames)


# --------------------------------------------------------------------------------------------
# The parent
# --------------------------------------------------------------------------------------------

def build(bundle: str, out: Path, *, workers: int = MAX_WORKERS, keys: Optional[Sequence[int]] = None,
          log=print) -> dict:
    """Write `verdicts.sqlite` for `bundle` at `out` (every rule key, or only `keys`)."""
    t0 = time.time()
    bundle = str(bundle)
    if not 1 <= workers <= MAX_WORKERS:
        raise VerdictsError(f"verdicts: {workers} workers — 1..{MAX_WORKERS} (the Mac's limit)")
    b = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    try:
        tasks = [(k, s, bool(sw), bool(sr)) for k, s, sw, sr in b.execute(
            "SELECT key_ix, set_id, steelhead_water, steelhead_rules FROM rule_key ORDER BY key_ix")]
    finally:
        b.close()
    if keys is not None:
        want = set(keys)
        tasks = [t for t in tasks if t[0] in want]
    db = create(Path(out), bundle_meta(bundle))
    seen: Dict[tuple, int] = {}
    stats = {"keys": 0, "readings": 0, "frames": 0, "verdicts": 0, "verdict_rows": 0}
    if workers == 1:
        _init(bundle)
        got = map(one_key, tasks)
        pool = None
    else:
        pool = get_context("spawn").Pool(workers, initializer=_init, initargs=(bundle,),
                                         maxtasksperchild=400)
        got = pool.imap(one_key, tasks, chunksize=4)       # ORDERED: deterministic verdict ids
    try:
        for n, (k, runs, firsts, fish, own, closed, verdicts, frames) in enumerate(got, 1):
            ids = []
            for v in verdicts:
                i = seen.get(v)
                if i is None:
                    i = seen[v] = len(seen)
                    db.executemany("INSERT INTO verdict_rule (verdict, rule, state, reason, by_rule) "
                                   "VALUES (?,?,?,?,?)", [(i, r, s, rs, by) for r, s, rs, by, _ in v])
                    db.executemany("INSERT INTO verdict_lifter (verdict, rule, lifter) VALUES (?,?,?)",
                                   [(i, r, l) for r, _, _, _, ls in v for l in ls])
                    stats["verdict_rows"] += len(v)
                ids.append(i)
            db.execute("INSERT INTO key_meta (key_ix, own) VALUES (?,?)", (k, int(own)))
            db.executemany("INSERT INTO key_fish (key_ix, fish) VALUES (?,?)",
                           [(k, _FISH[f]) for f in fish])
            db.executemany("INSERT INTO reading (key_ix, reading, first_day, closed) VALUES (?,?,?,?)",
                           [(k, ri, d, int(c)) for ri, (d, c) in enumerate(zip(firsts, closed))])
            db.executemany("INSERT INTO segment (key_ix, start, reading) VALUES (?,?,?)",
                           [(k, d, r) for d, r in runs])
            db.executemany("INSERT INTO frame (key_ix, reading, fish, origin, verdict) "
                           "VALUES (?,?,?,?,?)", [(k, r, f, o, ids[v]) for r, f, o, v in frames])
            stats["keys"] += 1
            stats["readings"] += len(firsts)
            stats["frames"] += len(frames)
            if n % 250 == 0:
                log(f"  verdicts: {n}/{len(tasks)} keys, {len(seen):,} verdicts, "
                    f"{time.time() - t0:.0f} s")
    finally:
        if pool is not None:
            pool.close()
            pool.join()
    stats["verdicts"] = len(seen)
    db.commit()
    from pipeline.deliver.verdicts.check import check
    problems = check(db, bundle, all_keys=keys is None)
    if problems:
        db.close()
        Path(out).unlink(missing_ok=True)
        raise VerdictsError("verdicts: the written file fails its check — " + "; ".join(problems[:5]))
    db.execute("VACUUM")
    db.close()
    stats["seconds"] = round(time.time() - t0, 1)
    log(f"verdicts: {bundle} -> {out}: {stats}")
    return stats


def main(argv=None) -> int:
    import argparse
    from pipeline.common.curated import GENERATED
    ap = argparse.ArgumentParser(prog="pipeline.deliver.verdicts", description=__doc__.split("\n")[0])
    ap.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    ap.add_argument("--out", type=Path, default=None,
                    help="default: verdicts.sqlite beside the bundle")
    ap.add_argument("--workers", type=int, default=MAX_WORKERS)
    a = ap.parse_args(argv)
    build(str(a.bundle), a.out or a.bundle.with_name("verdicts.sqlite"), workers=a.workers)
    return 0


if __name__ == "__main__":
    sys.exit(main())
