"""The curation-review FINGERPRINT: what a reviewer's mark is held against (`verification.py`).

A mark records the entry's content hash when it was made; the queue shows `verified` only while the
entry still hashes the same, `stale` once anything stored in it changes, and a flag stands through
edits. No generated data: this is the smoke-tier check of the mark (T96 puts it in `smoke.txt`; the
end-to-end mark tests in `test_curation_review_api.py` need a built atlas).
"""
from __future__ import annotations

import copy
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[2] / "curation-review" / "backend"


@pytest.fixture(scope="module")
def V():
    mp = pytest.MonkeyPatch()
    mp.syspath_prepend(str(BACKEND))
    sys.modules.pop("verification", None)
    import verification                                       # noqa: E402
    yield verification
    mp.undo()


ENTRY = {"entry_id": "r3:tranquille_lake@3-29", "name": "TRANQUILLE LAKE",
         "rules": [{"rule_id": "t.r1", "take": 8, "verbatim": "Rainbow trout daily quota = 8"}]}


def test_the_hash_ignores_key_order_and_nothing_else(V):
    shuffled = {k: ENTRY[k] for k in reversed(list(ENTRY))}
    assert V.entry_hash(shuffled) == V.entry_hash(ENTRY)
    changed = copy.deepcopy(ENTRY)
    changed["rules"][0]["take"] = 7
    assert V.entry_hash(changed) != V.entry_hash(ENTRY)
    assert len(V.entry_hash(ENTRY)) == 16


def test_a_mark_reads_verified_until_the_entry_changes_and_a_flag_stands(V):
    h = V.entry_hash(ENTRY)
    changed = copy.deepcopy(ENTRY)
    changed["rules"][0]["verbatim"] = "Rainbow trout daily quota = 8 (none over 50 cm)"
    got = [
        V.status_of(None, h),
        V.status_of({"state": "verified", "hash": h}, h),
        V.status_of({"state": "verified", "hash": h}, V.entry_hash(changed)),
        V.status_of({"state": "flagged", "hash": h, "note": "wrong cut"}, V.entry_hash(changed)),
    ]
    assert got == ["unverified", "verified", "stale", "flagged"]
