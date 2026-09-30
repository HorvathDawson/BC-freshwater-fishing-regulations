"""Per-entry result cache, so confirming ONE entry does not re-resolve all 1,392.

Why it matters beyond speed: it is what lets the review app call this builder on every
request instead of keeping its own resolution path. Two implementations of "what does this
rule cover" is how the app a human signed off on drifts from the bundle that ships — and
the divergence would be invisible until a user hit a wrong reach.

**The key must cover everything that can change the answer**, which is more than the entry:

* the entry itself      — its extents, matched items, tributary flags
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
#: 6: the tributary walk lets a lake into a walk through its MAIN outlet even when the lake's
#:    aggregate order runs ahead of the outlet's (Dester Lake / Meldrum Creek) —
#:    `graph.tributaries._is_main_outlet`. It changes which sections a walk reaches.
#: 7: a DETACHED braid piece (touching no section of its water, only unnamed lakes on its own
#:    blue line) is placed by its own line's nearest placed pieces (`extent._bracket`), and a
#:    watershed the registry did not mint (`area:basin:100-342455-`) resolves from the graph
#:    (`extent.area_sections`) where it used to fail.
#: 8: (2026-09-24 rulings) the walk collects streams only; a lake in the middle of the reach is
#:    the river passing through and its inflows are tributaries; a bifurcation's run follows its
#:    FWA code; a Region 7 row's MUs resolve it to its zone (`outside.entry_regions`).
#: 9: `Extent.watershed` — a part of a river's watershed is cut by FWA code position
#:    (`extent._watershed_part`) and joined after the walk, never walked (`classify`).
#: 10: a section touching two region polygons resolves `area:region:*` to its HOME region only —
#:    the one holding most of its area (a waterbody) or length (a line), `registry.regions`
#:    (user ruling 2026-09-25, Mara Lake). It changes which sections a zone rule binds.
#: 11: a LAKE touching two region polygons resolves `area:region:*` to BOTH (stream pieces keep
#:    their home), and a regional row not printed by another region applies along its water's
#:    whole length (`outside.region_limit`, user ruling 2026-09-25, second half).
#: 12: `Extent` op `rest` — "other parts" binds the rule's water minus its named siblings'
#:    sections (`build._build_rest`); unknown when a sibling does not bind, and a sibling's
#:    straddling pieces are withheld (`classify.COMPLEMENT_*`).
#: 13: (2026-09-29 rulings) a cut at a CONFLUENCE keeps the joining water and its subtree out of the
#:    walk unless the rule's words include it (`build.confluence_excludes`); a carve-out may
#:    `walk_past` its water; a water row's walk is not held to any region
#:    (`outside.WATER_ROW_WALKS_CROSS_REGIONS`); a tidal row's water leaves every other binding
#:    (`outside.tidal_sections`); a code-less floodplain lake takes its side of a watershed cut
#:    from the nearest river piece (`extent.CODE_LAKES_PLACED_BY_POSITION`); a designation stops
#:    at national parks (`licensing.DESIGNATIONS_STOP_AT_NATIONAL_PARKS`); a designation still
#:    walks into a confluence cut's joining water (`tributaries.LICENSING_WALKS_INTO_CONFLUENCE_WATERS`).
#:    Signs the rule's words put BELOW the confluence ("signs located downstream of the Meziadin River
#:    confluence") put it inside a reach running up from them: the joining water is walked
#:    (`build._signs_below`, review 2026-09-29).
POLICY_VERSION = "13"


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

    Deliberately NOT the whole entry: its text, labels and review notes change when a
    curator saves, and keying on them would evict the cache on every keystroke without any
    answer having changed. The entry's `extents` ARE in it: a cache that cannot see the clip
    would serve a reach from before it was applied.
    """
    return {
        "entry_id": entry.get("entry_id"),
        "matched": list(entry.get("matched") or ()),
        "extents": entry.get("extents") or [],
        "includes_tributaries": entry.get("includes_tributaries"),
        "tidal": bool(entry.get("tidal")),
        "rules": [
            {
                "rule_id": r.get("rule_id"),
                "type": r.get("type"),
                # The words decide whether a confluence cut's joining water is taken in
                # ("including Macleod Creek", `build.confluence_excludes`).
                "verbatim": r.get("verbatim"),
                "extent_text": r.get("extent_text"),
                "extents": r.get("extents") or [],
                "includes_tributaries": r.get("includes_tributaries"),
                "tributaries_only": r.get("tributaries_only"),
                "tributary_excludes": r.get("tributary_excludes") or [],
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
