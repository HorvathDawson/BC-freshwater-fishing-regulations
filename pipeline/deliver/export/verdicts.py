"""The verdict store every reader answer the export ships is looked up in (DATAFLOW P5).

`verdicts.sqlite` beside the bundle unless `__main__.main` was handed another; opened once per bundle and held
to the bundle's digests (`verdicts_of`); `answer_on` is one key's answer on a day.
"""
from __future__ import annotations

import sqlite3
from collections import defaultdict
from pathlib import Path
from pipeline.deliver import calendar as CAL
from pipeline.deliver.bundle.read import SPEAKER_STATES as RD_SPEAKERS


#: The verdict store each bundle is read through (`verdicts.sqlite` beside it unless `main` was
#: handed another): every reader answer the export ships is LOOKED UP there (DATAFLOW P5).
_STORES: dict = {}


def verdicts_of(bundle: Path, path: Path | None = None):
    """The bundle's verdicts (`pipeline.deliver.verdicts.store.VerdictStore`), opened once and
    held to the bundle's digests; refused when absent (no fallback to the reader)."""
    from pipeline.deliver.verdicts.store import VerdictStore
    key = str(Path(bundle).resolve())
    if path is not None or key not in _STORES:
        _STORES[key] = VerdictStore.open(path or Path(bundle).with_name("verdicts.sqlite"), bundle)
    return _STORES[key]


def rule_keys_of(bundle: Path) -> dict[int, list[tuple[int, int, bool, bool, int, int]]]:
    """{set_id: [(key_ix, set_id, steelhead water, steelhead rules, sections, lowest section)]},
    each set's keys by their lowest section — the bundle's `rule_key`."""
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    try:
        out: dict = defaultdict(list)
        for k, s, sw, sr, n, rep_ in db.execute(
                "SELECT key_ix, set_id, steelhead_water, steelhead_rules, sections, rep_sid "
                "FROM rule_key ORDER BY set_id, rep_sid"):
            out[s].append((k, s, bool(sw), bool(sr), n, rep_))
        return dict(out)
    finally:
        db.close()


def answer_on(store, key: int, on, fish: str, *, trace: bool = False,
              moment=None) -> list[dict]:
    """The reader's stored answer for a rule key on a day (`(month, day)`) at a moment (an index
    into `store.moments(key)`; required where the key has several), origin not known:
    `[{"id", "state", partly_lifted?, reason?, by?}]` by rule id — the speakers (`trace` adds the
    losers), exactly what `read.effective_rules` returned for a section of the key."""
    from pipeline.deliver import types as T
    out = []
    for r in store.on_day(key, CAL.day_of(on), fish, "none", moment):
        st = r.state.value
        if st not in RD_SPEAKERS and not trace:
            continue
        x = {"id": store.rule_ids[r.rule], "state": st}
        if r.lifted_in_part_by:
            x["partly_lifted"] = True
        if r.reason is not None:
            x["reason"], x["by"] = r.reason.value, store.rule_ids[r.by]
        out.append(x)
    return out
