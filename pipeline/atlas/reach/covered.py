"""Which registry items an entry regulates — ONE implementation, used by the builder and
the review app.

`entry.matched` is authoritative: the matcher wrote it against the synopsis row's VERBATIM
name, which is what a combined override is keyed on ("CHILLIWACK / VEDDER RIVERS (does not
include Sumas River) …"). An entry only stores the item's cleaned-up name
("Chilliwack River"), so re-matching can find the Chilliwack but can never learn about the
Vedder or the Vedder Canal.

EMPTY MEANS THE ENTRY BINDS NOTHING. There used to be a live re-match by name for an entry
whose `matched` was empty — a bridge for entries parsed before `matched` was an ingest-time
field. No catalogue entry needs it any more (`backfill_matched` stamped them), and the only
callers still reaching it were the DFO locations, which carry no name at all: of 346, the 19
with an empty `matched` re-matched to nothing (measured 2026-09-23). A fallback nobody needs is
a second, unreviewed answer to "which water is this rule about", so it is gone. An entry that
should bind gets its `matched` stamped (`backfill_matched`, or the review app), never guessed.

`make_matcher` stays: `reparse_candidates` uses it to FIND entries worth curating, which is a
report, not a binding. (The DFO matcher does not: it builds its own indices and reads only this
module's `DEFAULT_OVERRIDES`.)
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pipeline.regs.matching.matcher import (
    build_id_index, build_name_index, build_override_index, load_overrides, match_row,
)
from pipeline.common.curated import CURATED, SOURCE

#: The hand-curated match overrides the BUILD uses. Defaulting to this is not a
#: convenience: without it the matcher returns `ambiguous` for every entry an override
#: disambiguates ("Yakoun River" -> gnis:3485), the builder loses those items, and the
#: bundle silently disagrees with the review app about which water a rule is even on.
#: Passing None here cost 82 rules before it was caught.
DEFAULT_OVERRIDES = CURATED.regulations.overrides


def make_matcher(registry, overrides_path="__default__"):
    """A callable ``(entry) -> MatchResult`` sharing the build's own matcher and overrides.

    Indices are built once; on ~19.7k items that is not free, so callers should hold onto
    the returned function rather than rebuilding per entry.
    """
    if overrides_path == "__default__":
        overrides_path = DEFAULT_OVERRIDES if DEFAULT_OVERRIDES.exists() else None
    overrides = load_overrides(overrides_path)
    name_index = build_name_index(registry)
    id_index = build_id_index(registry)
    ov_index = build_override_index(overrides)

    @lru_cache(maxsize=None)
    def _match(name: str, region: str, mus: tuple[str, ...]):
        row = {"water": name, "region": region, "mu": list(mus)}
        return match_row(0, row, registry, name_index, id_index, ov_index)

    def match(entry: dict):
        # A catalogue entry is flat, and the MUs its synopsis ROW was printed under are the
        # `@` suffix of entry_id.
        eid = str(entry.get("entry_id") or "")
        mus = eid.split("@", 1)[1].split("+") if "@" in eid else []
        return _match(entry.get("name") or "", str(entry.get("region") or ""), tuple(mus))

    return match


def covered_ids(entry: dict, registry) -> list[str]:
    """The registry items this entry regulates, primary first: its `matched`, and nothing else.

    Filtered to items actually present in this build — a `matched` id naming an item the
    build does not have must not silently widen the resolve scope. Empty `matched` is empty:
    the entry binds nothing through its water (its rules may still name an item or an area in
    their own extents).
    """
    return [i for i in (entry.get("matched") or []) if i in registry]


def live_match_ids(entry: dict, registry, match) -> list[str]:
    """What a LIVE re-match by name would give this entry — for reports that look for entries
    worth curating (`reparse_candidates`). Never a binding: `covered_ids` is."""
    mr = match(entry)
    out: list[str] = []
    for iid in (getattr(mr, "item_id", None), *(getattr(mr, "also", ()) or ())):
        if iid and iid in registry and iid not in out:
            out.append(iid)
    return out
