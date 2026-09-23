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

A record with no extents INHERITS ITS ENTRY'S, as a rule does (`bundle.rules._rule_row`). One
with no extents on an entry with none is unresolved (`no_extents`), never defaulted to `whole`
(AGENTS 13).
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
