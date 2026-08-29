"""Per-entry result cache, so confirming ONE entry does not re-resolve all 1,392.

Why it matters beyond speed: it is what lets the review app call this builder on every
request instead of keeping its own resolution path. Two implementations of "what does this
rule cover" is how the app a human signed off on drifts from the bundle that ships — and
the divergence would be invisible until a user hit a wrong reach.

**The key must cover everything that can change the answer**, which is more than the entry:

* the entry itself      — its extents, scope, matched items, tributary flags
* the BUILD             — section ids churn ~6% per rebuild, so the same entry resolves
                          differently against a different graph
* the classifier policy — a change to straddler handling or the reason table changes
                          outcomes without touching either of the above

Miss any one and the cache serves a confidently wrong answer, which is worse than no
cache at all. `policy_version` must be bumped when `classify.py`'s behaviour changes;
there is a test that fails if the constant is stale.
"""

from __future__ import annotations

import hashlib
import json
from typing import Callable

#: Bump whenever `classify.py` changes an OUTCOME. Guarded by a test.
POLICY_VERSION = "2"


def entry_key(entry: dict, build_id: str, policy_version: str = POLICY_VERSION) -> str:
    """Content hash of everything that can change this entry's resolution."""
    payload = {
        "build": build_id,
        "policy": policy_version,
        "entry": _resolution_inputs(entry),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()[:20]


def _resolution_inputs(entry: dict) -> dict:
    """Only the fields resolution actually reads.

    Deliberately NOT the whole entry: `reviewed_at`, `revisit_note` and friends change
    when a curator saves, and keying on them would evict the cache on every keystroke
    without any answer having changed.
    """
    return {
        "entry_id": entry.get("entry_id"),
        "matched": list(entry.get("matched") or ()),
        "scope": entry.get("scope") or [],
        "tributaries": entry.get("tributaries") or {},
        "rules": [
            {
                "rule_id": r.get("rule_id"),
                "restriction_type": r.get("restriction_type"),
                "extents": r.get("extents") or [],
                "includes_tributaries": r.get("includes_tributaries"),
                "tributaries_only": r.get("tributaries_only"),
                "tributary_excludes": r.get("tributary_excludes") or [],
                "sections_override": r.get("sections_override"),
                "unresolved_locators": list(r.get("unresolved_locators") or ()),
            }
            for r in (entry.get("rules") or [])
        ],
    }


class ReachCache:
    """In-memory cache keyed by `entry_key`. Process-local and intentionally simple.

    Not persisted: a full pass is ~0.1 s, so the only case worth optimising is a live
    review session, which is one process. A disk cache would add an invalidation problem
    for no measured benefit.
    """

    def __init__(self, build_id: str, policy_version: str = POLICY_VERSION) -> None:
        self.build_id = build_id
        self.policy_version = policy_version
        self._store: dict[str, tuple] = {}
        self.hits = 0
        self.misses = 0

    def get_or_compute(self, entry: dict, compute: Callable[[], tuple]) -> tuple:
        """`compute()` must return ``(bindings, diagnostics)`` for this entry alone."""
        key = entry_key(entry, self.build_id, self.policy_version)
        cached = self._store.get(key)
        if cached is not None:
            self.hits += 1
            return cached
        self.misses += 1
        value = compute()
        self._store[key] = value
        return value

    def invalidate(self, entry: dict) -> None:
        self._store.pop(entry_key(entry, self.build_id, self.policy_version), None)

    def clear(self) -> None:
        self._store.clear()

    @property
    def size(self) -> int:
        return len(self._store)
