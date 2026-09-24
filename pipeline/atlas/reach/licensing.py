"""Licensing records, placed on sections by the SAME machinery the rules use.

`CatalogueEntry.licensing` is not rules (it never competes and never votes on open/closed), but
where a Classified Water is, where a Creston Valley permit is needed, and where a Yukon licence
is also accepted are REACHES — and a second resolver for them would be a second answer to "which
water does this cover" (AGENTS 16). So every placed record is handed to `build_reach` exactly as a
rule is: its extents, its tributary flag (inheriting the entry's when it has none), its
tributary carve-outs, clipped to the entry's own scope.

FOUR KINDS ARE PLACED, TWO ARE NOT. `designation`, `requirement`, `not_classified` and
`alternative` are about a place. `licence_terms` (how a document is sold) and `exemption` (who
needs no document) are about a document or an angler; binding them to sections was the Dean draw
defect, so they never reach this module.

EVERY PLACED RECORD ENDS IN EXACTLY ONE PLACEMENT, and none of them is "absent":

  sections        bound to >= 1 section; each carries `reach` or `trib`, and a binding whose
                  tributary walk was not done carries `trib_pending` instead
  province        a requirement written for the whole province (`within` every region). It
                  ships with NO section rows — writing a row per section of B.C. for "you need
                  a basic licence" would be most of the bundle saying one sentence
  on_designation  a requirement with no extents of its own and an `on` key: it holds wherever a
                  designation is in force, which the reader decides per date
  unresolved      could not be placed; carries the reach builder's typed reason. The bundle
                  marks the record `uncertain` — for licensing the unsafe direction is
                  UNDER-requiring, so an unplaced requirement must read "check", never "none"

A record with no extents INHERITS ITS ENTRY'S — here, at placement, which is the one place
inheritance happens (a bare RULE is given `whole` at ingest instead, AGENTS 13). One with no
extents on an entry with none is unresolved (`no_extents`), never defaulted to `whole`.
"""

from __future__ import annotations

from dataclasses import dataclass

from pipeline.atlas.reach.models import Diagnostic, Outcome

#: The kinds that are about a place. The other two are never bound to a section.
PLACED_KINDS = ("designation", "requirement", "not_classified", "alternative")

PLACEMENTS = ("sections", "province", "on_designation", "unresolved")


@dataclass(frozen=True)
class LicensingPlacement:
    """One licensing record's placement against one build."""

    entry_id: str
    record_id: str
    kind: str
    placement: str
    sections: tuple[str, ...] = ()
    #: Of `sections`, the ones reached only by the tributary walk.
    via_tributary: tuple[str, ...] = ()
    #: The record extends to tributaries and the walk was not done — `sections` is the direct
    #: part only and nothing may treat it as complete (AGENTS 15).
    tributaries_pending: bool = False
    reason: str | None = None
    detail: str = ""

    def __post_init__(self) -> None:
        # The rules' invariant (RuleBinding), for the same reason: a record never ends
        # bound-but-empty, never unresolved-but-unexplained, and never silently nowhere.
        if self.placement not in PLACEMENTS:
            raise ValueError(f"{self.entry_id}#{self.record_id}: placement {self.placement!r}")
        if (self.placement == "sections") != bool(self.sections):
            raise ValueError(f"{self.entry_id}#{self.record_id}: placement {self.placement} "
                             f"with {len(self.sections)} sections")
        if (self.placement == "unresolved") != (self.reason is not None):
            raise ValueError(f"{self.entry_id}#{self.record_id}: placement {self.placement} "
                             f"with reason {self.reason!r}")


def is_province_wide(extents: list[dict] | None) -> bool:
    """`within` every region and nothing else — the whole province, said the one way the corpus
    says it. Anything narrower (an area id, a feature type, a watershed limit) is a place."""
    return bool(extents) and all(
        x.get("op") == "within" and x.get("area_kind") == "region"
        and set(x) <= {"op", "area_kind"} for x in extents)


def as_rule(rec: dict, extents: list[dict]) -> dict:
    """The fields of a licensing record the reach builder reads, under the names it reads them.

    Only what resolution reads is passed, so nothing about the record can steer the resolver
    except what steers it for a rule."""
    return {
        "rule_id": rec["id"],
        "type": rec["kind"],
        "extents": extents,
        "includes_tributaries": rec.get("includes_tributaries"),
        "tributaries_only": bool(rec.get("tributaries_only")),
        "tributary_excludes": list(rec.get("tributary_excludes") or []),
    }


def place_record(entry: dict, rec: dict, reach) -> tuple[LicensingPlacement, list[Diagnostic]]:
    """Place one record. `reach(rule_dict)` is `build_reach` bound to this entry's covered items
    and clip — the caller's, so the record resolves in exactly the context the entry's rules do."""
    eid, rid, kind = entry["entry_id"], rec["id"], rec["kind"]
    extents = rec.get("extents")
    if kind == "requirement" and extents is None and rec.get("on"):
        return LicensingPlacement(eid, rid, kind, "on_designation"), []
    if extents is None:
        extents = list(entry.get("extents") or [])
    if kind == "requirement" and is_province_wide(extents):
        return LicensingPlacement(eid, rid, kind, "province"), []

    binding, diags = reach(as_rule(rec, extents))
    if binding.outcome is Outcome.unresolved:
        return LicensingPlacement(
            eid, rid, kind, "unresolved", reason=binding.reason.value, detail=binding.detail,
            tributaries_pending=binding.tributaries_pending), diags
    return LicensingPlacement(
        eid, rid, kind, "sections", sections=binding.sections,
        via_tributary=binding.via_tributary,
        tributaries_pending=binding.tributaries_pending), diags


def _year(when) -> frozenset[int] | None:
    """The days of the year a `When` covers; ALL YEAR when it is empty, and `None` when it cannot
    be said (an unparsed season, or hours/weekdays, which a day set does not capture)."""
    from pipeline.regs.parsing.catalogue import _days
    if when is None or when.is_empty():
        return frozenset(range(1, 367))
    if when.unparsed or when.hours or when.weekdays:
        return None
    return frozenset(_days(when.dates))


def why_not_yield(own, inherited) -> str | None:
    """Why the inherited designation may NOT yield to the water's own one — `None` when it may.

    THE OWN DESIGNATION MUST SAY AT LEAST AS MUCH. Yielding drops the inherited designation from
    the section, so whatever it obliged there and the own one does not is lost:

      class      a Class I walk never yields to a Class II own designation (Class I is the
                 stricter licence; dropping it under-requires);
      period     the own classified period covers the inherited one (a Mar 1-May 31 own under an
                 all-year walk would leave the section unclassified for nine months);
      stamp      the own steelhead-stamp obligation is no weaker — a stamp period covering the
                 inherited one, or the inherited one waives the stamp; an own WAIVER never
                 replaces an inherited obligation;
      suspended  both or neither sleep under a closure — an own designation dormant while a
                 closure binds would leave the section with none while the inherited one holds.

    Both are `Designation`s. A period or stamp that cannot be put in days (unparsed) is not shown
    to cover anything, so the designations both stay."""
    if inherited.classified == "I" and own.classified != "I":
        return "class"
    mine, theirs = _year(own.when), _year(inherited.when)
    if mine is None or theirs is None or not theirs <= mine:
        return "period"
    if inherited.steelhead_stamp_during is not None:
        od = own.steelhead_stamp_during
        if od is None:
            return "stamp"
        if od.when != inherited.steelhead_stamp_during.when:
            ms, ts = _year(od.when), _year(inherited.steelhead_stamp_during.when)
            if ms is None or ts is None or not ts <= ms:
                return "stamp"
    elif inherited.steelhead_stamp_waived is None and own.steelhead_stamp_waived is not None:
        return "stamp"
    if bool(own.suspended_while) != bool(inherited.suspended_while):
        return "suspended"
    return None


def own_beats_inherited(placements: list[LicensingPlacement], records: dict
                        ) -> tuple[list[LicensingPlacement], list[Diagnostic]]:
    """A water's OWN designation beats one that reaches it only by another water's tributary walk
    — WHEN IT SAYS AT LEAST AS MUCH.

    It is how rules resolve — a water's own rule beats one arriving from elsewhere — applied to
    the designations. A section bound to a designation by REACH (the designation names this
    water) is taken out of another designation's TRIBUTARY-inherited binding. Without it the
    Suskwa, Class I with its own designation, also read Class II by the Bulkley's walk; the Elk's
    unit covered Wigwam, Michel, Forsyth and Abruzzi, each with its own unit licence; and a
    non-resident on those sections was told to buy two different day licences (design validator
    6, reported as ambiguous units).

    Only designations, and only the INHERITED half: a designation's own reach is never taken from
    it. A section two designations both name by reach is CONTESTED and yields nothing — it stays
    ambiguous and is reported. A `trib_pending` placement's sections are its direct ones, so they
    count as its own. And the inherited one yields only if `why_not_yield` finds nothing the own
    one would lose — period, stamp, suspension. Otherwise BOTH stay on the section and the pair is
    reported (`trib_kept_beside_own`), because silently dropping the stricter designation is the
    under-requiring direction, which is the unsafe one for licensing.

    `records` maps `(entry_id, record_id)` to the validated `Designation`.
    Every removal is a diagnostic (`trib_yields_to_own`), naming the designations it yielded to.
    """
    own: dict[str, set[tuple[str, str]]] = {}
    for p in placements:
        if p.kind == "designation" and p.placement == "sections":
            trib = set(p.via_tributary)
            for s in p.sections:
                if s not in trib:
                    own.setdefault(s, set()).add((p.entry_id, p.record_id))

    out: list[LicensingPlacement] = []
    diags: list[Diagnostic] = []
    for p in placements:
        if p.kind != "designation" or p.placement != "sections" or not p.via_tributary:
            out.append(p)
            continue
        me = (p.entry_id, p.record_id)
        yielded: dict[str, set[tuple[str, str]]] = {}
        kept: dict[tuple[tuple[str, str], str], int] = {}
        for s in p.via_tributary:
            others = own.get(s, set()) - {me}
            if not others:
                continue
            if len(others) > 1:
                kept[(min(others), "contested")] = kept.get((min(others), "contested"), 0) + 1
                continue
            (o,) = others
            why = why_not_yield(records[o], records[me])
            if why:
                kept[(o, why)] = kept.get((o, why), 0) + 1
                continue
            yielded[s] = others
        for (o, why), n in sorted(kept.items()):
            diags.append(Diagnostic(p.entry_id, p.record_id, "trib_kept_beside_own", {
                "sections": n, "own": f"{o[0]}#{o[1]}", "why": why}))
        if not yielded:
            out.append(p)
            continue
        keep = tuple(s for s in p.sections if s not in yielded)
        if not keep:
            raise AssertionError(f"{p.entry_id}#{p.record_id}: every section it binds is another "
                                 f"designation's own — it would end bound to nothing")
        to = sorted({f"{e}#{i}" for v in yielded.values() for e, i in v})
        diags.append(Diagnostic(p.entry_id, p.record_id, "trib_yields_to_own", {
            "removed": len(yielded), "kept": len(keep), "to": to}))
        out.append(LicensingPlacement(
            p.entry_id, p.record_id, p.kind, p.placement, sections=keep,
            via_tributary=tuple(s for s in p.via_tributary if s not in yielded),
            tributaries_pending=p.tributaries_pending, reason=p.reason, detail=p.detail))
    return out, diags


def _owners(eid: str, items: set[str], claims: dict[str, list[str]]) -> list[str]:
    return sorted({e for i in items for e in claims.get(i, ()) if e != eid})


def carve_outs_to_owner(placements: list[LicensingPlacement],
                        carved: dict[tuple[str, str], tuple[set[str], set[str]]],
                        claims: dict[str, list[str]]
                        ) -> tuple[list[LicensingPlacement], list[Diagnostic]]:
    """What a designation's carve-out removes goes to the designation of the water it was removed
    FOR — when that water has exactly one.

    "Atnarko/Bella Coola Rivers (includes tributaries) EXCEPT Burnt Bridge Creek upstream of
    Sitkatapa Creek": the book takes that creek out of the Atnarko's Class II because it is Class II
    in its own right ("Classified, Incl. Tribs"). The carve-out is walked as a BLOCK, so it removes
    the creek, everything above it, and everything that drains to the Atnarko only THROUGH it. Burnt
    Bridge's own designation is walked as a reach, and a reach walk stops at the lakes its creek
    runs through (a lake does not answer to its river). The two disagreed by 9 sections — the four
    lakes Burnt Bridge runs through, the pond it rises from, and a creek joining one of the lakes —
    which the Atnarko had given up and nobody then held.

    WHY HERE AND NOT IN THE WALK. Walking designations through lakes was measured and refused: it
    handed Francois Lake's catchment (2,430 sections) to the Stellako's designation, Kitsumkalum
    Lake's (914) to the Kitsumkalum's and 7,635 sections to 39 designations in all. The carve-out is
    the one place the book itself says whose these sections are, so they go exactly there: the
    removed sections the owner does not already hold join its designation as tributary water
    (`carve_out_handed_to_owner`). An owner with no designation, or several, receives nothing, and
    `carve_out_orphans` reports what is left.
    """
    by_entry: dict[str, list[int]] = {}
    for i, p in enumerate(placements):
        if p.kind == "designation" and p.placement == "sections":
            by_entry.setdefault(p.entry_id, []).append(i)
    out = list(placements)
    diags: list[Diagnostic] = []
    for (eid, rid), (removed, items) in sorted(carved.items()):
        targets = [i for o in _owners(eid, items, claims) for i in by_entry.get(o, [])]
        if len(targets) != 1:
            continue
        p = out[targets[0]]
        give = sorted(removed - set(p.sections))
        if not give:
            continue
        out[targets[0]] = LicensingPlacement(
            p.entry_id, p.record_id, p.kind, p.placement,
            sections=tuple(sorted(set(p.sections) | set(give))),
            via_tributary=tuple(sorted(set(p.via_tributary) | set(give))),
            tributaries_pending=p.tributaries_pending, reason=p.reason, detail=p.detail)
        diags.append(Diagnostic(p.entry_id, p.record_id, "carve_out_handed_to_owner", {
            "from": f"{eid}#{rid}", "sections": give}))
    return out, diags


def carve_out_orphans(placements: list[LicensingPlacement],
                      carved: dict[tuple[str, str], tuple[set[str], set[str]]],
                      claims: dict[str, list[str]]) -> list[Diagnostic]:
    """Water a designation's carve-out removed, that the excluded water's OWN designation must hold.

    "Atnarko/Bella Coola Rivers (includes tributaries) EXCEPT Burnt Bridge Creek upstream of
    Sitkatapa Creek": the carve-out takes that creek and everything above it out of the Atnarko's
    designation because Burnt Bridge Creek has its own ("Classified, Incl. Tribs"). So every section
    it removes must end up in the designation of the entry that claims the excluded water with its
    tributaries — otherwise the water is Class II by the book and holds no designation at all. That
    happened to 9 sections (the lakes Burnt Bridge runs through and rises from, and a creek joining
    one) until `carve_outs_to_owner` handed them over; this is the check that it stays so.

    `carved` maps each carving designation to `(sections its carve-out removed, the excluded items)`;
    `claims` maps an item to the entries that cover it with `includes_tributaries: true`. A removed
    section held by no designation of a claiming entry is a `carve_out_orphan`, reported per carve
    so a curator sees the sections. A carve-out whose water no entry claims asserts nothing here.
    """
    held: dict[str, set[str]] = {}
    for p in placements:
        if p.kind == "designation" and p.placement == "sections":
            held.setdefault(p.entry_id, set()).update(p.sections)
    out: list[Diagnostic] = []
    for (eid, rid), (removed, items) in sorted(carved.items()):
        owners = _owners(eid, items, claims)
        if not owners or not removed:
            continue
        have = set().union(*(held.get(e, set()) for e in owners))
        orphans = sorted(removed - have)
        if orphans:
            out.append(Diagnostic(eid, rid, "carve_out_orphan", {
                "owners": owners, "removed": len(removed), "orphans": orphans}))
    return out
