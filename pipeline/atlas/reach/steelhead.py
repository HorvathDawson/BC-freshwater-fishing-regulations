"""HOW SURE WE ARE THAT STEELHEAD ARE HERE — `steelhead: known | possible` per section.

User ruling 2026-10-01 (third). Every STREAM of Regions 1, 2, 3, 5 and 6 carries the steelhead rules
(the wild release, the annual hatchery 10, the record duty, the stamp), but the book names steelhead
on few of them. The attribute tells a reader which:

  known     a stream of a row that NAMES steelhead (`CatalogueEntry.anadromous_rainbow`, the rows
            whose own line prints a steelhead quota, release, closure or "Steelhead Stamp
            mandatory"), or a TRIBUTARY of one — the same walk every "including tributaries" rule
            takes (`build.build_reach`: the row's own scope, `tributaries.expand`, streams only), held
            to no region while it walks (`regional=False`: water rows are region-agnostic) — and then
            KEPT ONLY WHERE THE STEELHEAD RULES APPLY (`ONLY_WHERE_THE_RULES_APPLY`). The walk is the
            HYDROLOGY, not the row's regulation: it walks whether or not the row says "including
            tributaries", and no carve-out (`tributary_excludes`) stops it — a creek the row sends to
            a listing of its own still feeds the steelhead river.
            Also a LAKE whose own row names steelhead (Khartoum and Lois lakes: "Rainbow
            trout/hatchery steelhead quota = 6 in the aggregate") — read from the corpus: the lake
            items of a water row with a rule naming steelhead (`ST`).
  possible  any other stream section a provincial steelhead rule binds (`zp:steelhead`, which binds
            the streams of the steelhead regions): "steelhead rules apply; steelhead may not be
            present in this water".
  absent    everywhere else: lakes (but those two), and the regions whose tables name no steelhead.

`anadromous_rainbow` (a rainbow over 50 cm IS a steelhead, p.86) holds on the KNOWN STREAM sections
only — the bundle's `steelhead_water` — never on a lake and never on a "possible" stream.

Kept apart from the rule bindings (and their digest): this is a fact about the water, not a rule.
"""

from __future__ import annotations

from pipeline.atlas.reach import extent as _resolve

#: The provincial steelhead entry: the streams it binds are the streams that carry the steelhead
#: rules, and those not known are "possible".
PROVINCE_STEELHEAD = "zp:steelhead"

KNOWN, POSSIBLE = "known", "possible"

#: KNOWN ONLY WHERE THE STEELHEAD RULES APPLY (coordinator round 2026-10-01): the walk runs across
#: region lines, but a section the provincial steelhead rules do not bind (`PROVINCE_STEELHEAD` —
#: the streams of Regions 1, 2, 3, 5 and 6, and the own-row steelhead lakes) is neither known nor
#: steelhead water. The Thompson's walk reached 87 Region 8 headwater sections of the Nicola system,
#: the Skeena's 8 in Zone 7A; there a rainbow over 50 cm stays a rainbow.
ONLY_WHERE_THE_RULES_APPLY = True

#: The rule the walk is run as — the row's whole water, clipped by the row's own scope, with its
#: tributaries. Only what resolution reads.
WALK_RULE = {"rule_id": "steelhead_presence", "type": "steelhead_presence",
             "extents": [{"op": "whole"}], "includes_tributaries": True,
             "tributary_excludes": []}


def names_steelhead(e: dict) -> bool:
    """A water row (not a zone or provincial entry) with a rule whose fish names steelhead."""
    return (not str(e.get("entry_id") or "").startswith("z")
            and any("ST" in (r.get("species") or []) for r in e.get("rules") or []))


def own_steelhead_lakes(e: dict, registry, covered) -> list[str]:
    """The LAKE items of a row naming steelhead — the lakes whose own row names steelhead."""
    if not names_steelhead(e):
        return []
    return sorted(i for i in covered if i in registry
                  and str(getattr(registry[i].kind, "value", registry[i].kind) or "").lower()
                  == "lake")


class Presence:
    """Collects the known sections entry by entry (`add_row`, from inside `build_reaches`' loop, in
    each entry's own context), then the possible ones from the final bindings (`finish`)."""

    def __init__(self, registry, graph):
        self.registry, self.graph = registry, graph
        #: section -> (rank, entry_id, scope); the lowest wins: a row's own water (0) before a
        #: tributary (1), then the entry id
        self.known: dict[str, tuple[int, str, str]] = {}
        self.by_entry: dict[str, dict[str, int]] = {}

    def _put(self, s: str, rank: int, eid: str, scope: str) -> None:
        cur = self.known.get(s)
        if cur is None or (rank, eid) < cur[:2]:
            self.known[s] = (rank, eid, scope)

    def add_row(self, e: dict, covered, reach) -> None:
        """`reach(rule)` is `build_reach` bound to this entry's covered items, clip, border and
        tidal water, with `regional=False`."""
        eid = e["entry_id"]
        if e.get("anadromous_rainbow"):
            binding, _ = reach(dict(WALK_RULE))
            trib = frozenset(binding.via_tributary)
            n = {"reach": 0, "trib": 0}
            for s in binding.sections:
                if _resolve._kind_of(self.graph, s) != "stream":
                    continue
                scope = "trib" if s in trib else "reach"
                n[scope] += 1
                self._put(s, 1 if scope == "trib" else 0, eid, scope)
            self.by_entry[eid] = n
        for i in own_steelhead_lakes(e, self.registry, covered):
            secs = self.registry[i].section_ids
            self.by_entry.setdefault(eid, {"reach": 0, "trib": 0})["lake"] = len(secs)
            for s in secs:
                self._put(s, 0, eid, "lake")

    def finish(self, bindings) -> tuple[list[dict], dict]:
        """Every section's row, sorted, and the report's counts."""
        bound = {s for b in bindings if b.entry_id == PROVINCE_STEELHEAD for s in b.sections}
        dropped: dict[str, int] = {}
        if ONLY_WHERE_THE_RULES_APPLY:
            for s in sorted(self.known):
                if s not in bound:
                    eid = self.known.pop(s)[1]
                    dropped[eid] = dropped.get(eid, 0) + 1
        possible: set[str] = set()
        for b in bindings:
            if b.entry_id == PROVINCE_STEELHEAD and b.sections:
                possible |= {s for s in b.sections
                             if s not in self.known
                             and _resolve._kind_of(self.graph, s) == "stream"}
        rows = [{"section_id": s, "steelhead": KNOWN, "entry_id": eid, "scope": scope,
                 "kind": _resolve._kind_of(self.graph, s)}
                for s, (_, eid, scope) in self.known.items()]
        rows += [{"section_id": s, "steelhead": POSSIBLE, "entry_id": PROVINCE_STEELHEAD,
                  "scope": "rules", "kind": "stream"} for s in possible]
        rows.sort(key=lambda r: r["section_id"])
        report = {
            "known": len(self.known),
            "known_streams": sum(1 for r in rows if r["steelhead"] == KNOWN
                                 and r["kind"] == "stream"),
            "known_lakes": sum(1 for r in rows if r["steelhead"] == KNOWN
                               and r["kind"] == "lake"),
            #: known streams the provincial steelhead rules do not bind (0 when they are dropped)
            "known_without_steelhead_rules": sum(1 for s in self.known if s not in bound),
            #: what the walk reached past the steelhead rules, by the row it walked from — dropped
            "dropped_outside_steelhead_rules": {k: dropped[k] for k in sorted(dropped)},
            "possible": len(possible),
            "by_entry": {k: self.by_entry[k] for k in sorted(self.by_entry)},
        }
        return rows, report
