"""THE STRUCTURAL PROOF of a written verdicts file (DATAFLOW §3) — `check(db, bundle)` returns every
problem found (empty: the file holds):

  * every key of the bundle has a key_meta row (when the file is a full build);
  * every key's segments start on day 1 and cover 366 days, and every reading is used, its
    `first_day` the first day of its first segment;
  * every (key, reading, asked fish, origin) has exactly one frame;
  * every rule in a verdict, and every `by` and lifter, is a member of the key's rule set;
  * `reading.closed` equals the predicate (`project.closed`) recomputed from the stored frames
    — a check of the column with the same function, not a second definition;
  * the foreign keys hold.
"""
from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Dict, List

from pipeline.deliver import types as T
from pipeline.deliver.calendar import DAYS
from pipeline.deliver.verdicts import project


def check(db: sqlite3.Connection, bundle: str, *, all_keys: bool = True) -> List[str]:
    out: List[str] = []
    out += [f"foreign key {r}" for r in db.execute("PRAGMA foreign_key_check").fetchall()[:5]]
    b = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    try:
        key_set = dict(b.execute("SELECT key_ix, set_id FROM rule_key"))
        members: Dict[int, set] = defaultdict(set)
        for s, i in b.execute("SELECT r.set_id, x.ix FROM ruleset r JOIN rule_ix x "
                              "ON x.entry_id = r.entry_id AND x.rule_id = r.rule_id"):
            members[s].add(i)
        grade = dict(b.execute("SELECT x.ix, r.closure_grade FROM rule_ix x JOIN rule r "
                               "ON r.entry_id = x.entry_id AND r.rule_id = x.rule_id"))
    finally:
        b.close()
    keys = [k for (k,) in db.execute("SELECT key_ix FROM key_meta ORDER BY key_ix")]
    if all_keys and keys != sorted(key_set):
        out.append(f"{len(set(key_set) - set(keys))} rule keys of the bundle have no verdicts")
    seg: Dict[int, list] = defaultdict(list)
    for k, start, r in db.execute("SELECT key_ix, start, reading FROM segment ORDER BY key_ix, start"):
        seg[k].append((start, r))
    rd: Dict[int, dict] = defaultdict(dict)
    for k, r, first, closed in db.execute("SELECT key_ix, reading, first_day, closed FROM reading"):
        rd[k][r] = (first, closed)
    fish: Dict[int, set] = defaultdict(set)
    for k, f in db.execute("SELECT key_ix, fish FROM key_fish"):
        fish[k].add(f)
    game = {T.code(T.FishCode(f)) for f in T.GAME_FISH}
    n_origin = len(T.AskOrigin)
    rows_of: Dict[int, tuple] = {}

    def rows(v):
        got = rows_of.get(v)
        if got is None:
            lift = defaultdict(tuple)
            for r, l in db.execute("SELECT rule, lifter FROM verdict_lifter WHERE verdict = ?", (v,)):
                lift[r] += (l,)
            got = rows_of[v] = tuple((r, s, rs, by, lift[r]) for r, s, rs, by in db.execute(
                "SELECT rule, state, reason, by_rule FROM verdict_rule WHERE verdict = ?", (v,)))
        return got

    none = T.code(T.AskOrigin.none)
    for k in keys:
        s = seg[k]
        if not s or s[0][0] != 1:
            out.append(f"key {k}: its segments do not start on day 1")
            continue
        if any(a >= b for (a, _), (b, _) in zip(s, s[1:])) or s[-1][0] > DAYS:
            out.append(f"key {k}: its segments are not 1..{DAYS} in order")
        used = {r for _, r in s}
        if used != set(rd[k]):
            out.append(f"key {k}: readings {sorted(set(rd[k]) ^ used)} unused or missing")
        first_of = {}
        for d, r in s:
            first_of.setdefault(r, d)
        if any(rd[k][r][0] != first_of.get(r) for r in rd[k]):
            out.append(f"key {k}: a reading's first_day is not its first segment's start")
        if not game <= fish[k]:
            out.append(f"key {k}: not every game fish is asked")
        frames = db.execute("SELECT reading, fish, origin, verdict FROM frame WHERE key_ix = ?",
                            (k,)).fetchall()
        if len(frames) != len(rd[k]) * len(fish[k]) * n_origin:
            out.append(f"key {k}: {len(frames)} frames, not one per reading x fish x origin")
        mine = members[key_set[k]]
        by_reading: Dict[int, dict] = defaultdict(dict)
        for r, f, o, v in frames:
            for rule, st, rs, by, lifters in rows(v):
                if rule not in mine or (by is not None and by not in mine) \
                        or any(l not in mine for l in lifters):
                    out.append(f"key {k}: verdict {v} names a rule outside its set")
                    break
            if o == none:
                by_reading[r][f] = v
        for r, (first, closed) in rd[k].items():
            if not game <= set(by_reading[r]):
                out.append(f"key {k} reading {r}: a game fish has no origin-none frame")
                continue
            got = project.closed(lambda f: rows(by_reading[r][T.code(T.FishCode(f))]), grade)
            if got != bool(closed):
                out.append(f"key {k} reading {r}: closed {bool(closed)}, the frames say {got}")
        if len(out) > 50:
            break
    return out
