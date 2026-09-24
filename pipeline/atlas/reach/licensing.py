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


def own_beats_inherited(placements: list[LicensingPlacement]
                        ) -> tuple[list[LicensingPlacement], list[Diagnostic]]:
    """A water's OWN designation beats one that reaches it only by another water's tributary walk.

    It is how rules resolve — a water's own rule beats one arriving from elsewhere — applied to
    the designations. A section bound to a designation by REACH (the designation names this
    water) is taken out of every OTHER designation's TRIBUTARY-inherited binding. Without it the
    Suskwa, Class I with its own designation, also read Class II by the Bulkley's walk; the Elk's
    unit covered Wigwam, Michel, Forsyth and Abruzzi, each with its own unit licence; and a
    non-resident on those sections was told to buy two different day licences (design validator
    6, reported as ambiguous units).

    Only designations, and only the INHERITED half: a designation's own reach is never taken from
    it, and a section two designations both name by reach stays ambiguous and is reported. A
    `trib_pending` placement's sections are its direct ones, so they count as its own. Nothing is
    decided by unit or class: whose water it is decides.

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
        yielded = {s: own[s] - {me} for s in p.via_tributary if own.get(s, set()) - {me}}
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
