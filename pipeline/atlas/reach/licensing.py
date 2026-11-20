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
                  a basic licence" would be most of the bundle saying one sentence. One whose
                  extent subtracts an area FAMILY (`outside_area_kind: national_parks`) is still
                  `province`; the bundle lists that family's sections once (`province_except`)
  on_designation  a requirement with no extents of its own and an `on` key: it holds wherever a
                  designation is in force, which the reader decides per date. One with extents
                  AND `on` is placed on `sections`, narrowed to where a designation that can
                  satisfy `on` is placed (`on_designations`) — the Dean's "classified portions"
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
    says it. Anything narrower (an area id, a feature type, a watershed limit) is a place.

    ONE SUBTRACTION IS STILL THE PROVINCE: `outside_area_kind` — "Basic and supplementary
    licences and stamps are not valid in National Parks". Placed as sections it would be a row
    per section of B.C. minus seven parks, most of the bundle saying one sentence; so it stays
    `province`, and the bundle ships the family's sections once (`province_except`)."""
    return bool(extents) and all(
        x.get("op") == "within" and x.get("area_kind") == "region"
        and set(x) <= {"op", "area_kind", "outside_area_kind"} for x in extents)


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


#: POLICY (book p.9: "A provincial angling licence is not valid unless otherwise stated for any fresh
#: water within National Parks"; R4 p.33: "Provincial angling regulations and licensing do not apply
#: in the National Parks in this region"): a CLASSIFIED WATER designation stops at a national park.
#: The Kootenay's "upstream of White River, including tributaries: Class II" put the Class II licence
#: on 2,317 sections inside Kootenay National Park (LS-8). `without_national_parks`.
DESIGNATIONS_STOP_AT_NATIONAL_PARKS = True
NATIONAL_PARKS = "national_parks"


def national_park_sections(registry) -> frozenset[str]:
    """Every section of every `area:national_parks:*` item — the family as the one area-family
    reader answers it (`registry.regions.area_family_sections`, T140); empty when there is none."""
    from pipeline.atlas.registry.regions import area_family_sections
    return area_family_sections(registry, NATIONAL_PARKS) or frozenset()


def without_national_parks(placed: LicensingPlacement, parks: frozenset[str]
                           ) -> tuple[LicensingPlacement, list[Diagnostic]]:
    """A designation placed on sections, minus the national parks — reported. One left with nothing
    is unresolved `national_park` (never bound to no water)."""
    if (not DESIGNATIONS_STOP_AT_NATIONAL_PARKS or placed.kind != "designation"
            or placed.placement != "sections" or not parks):
        return placed, []
    gone = set(placed.sections) & parks
    if not gone:
        return placed, []
    kept = tuple(s for s in placed.sections if s not in gone)
    diag = Diagnostic(placed.entry_id, placed.record_id, "national_park", {
        "removed": len(gone), "kept": len(kept),
        "why": "provincial licensing does not apply in national parks (p.9)"})
    if not kept:
        return LicensingPlacement(placed.entry_id, placed.record_id, placed.kind, "unresolved",
                                  reason="national_park",
                                  detail=f"every section it selects ({len(gone)}) is in a "
                                         f"national park"), [diag]
    return LicensingPlacement(placed.entry_id, placed.record_id, placed.kind, "sections",
                              sections=kept,
                              via_tributary=tuple(x for x in placed.via_tributary if x not in gone),
                              tributaries_pending=placed.tributaries_pending), [diag]


#: Why a requirement with a place and an `on` holds nowhere. The bundle refuses a build that has one.
NO_DESIGNATION = "no_designation"


def on_designations(placements: list[LicensingPlacement], records: dict, designations: dict
                    ) -> tuple[list[LicensingPlacement], list[Diagnostic]]:
    """A requirement with BOTH `extents` and `on` holds where both do: on its sections, where a
    designation that can satisfy `on` is placed.

    "All anglers are required to buy a Classified Waters Licence to fish the classified portions of
    the Dean River" is the case. `place_record` put it on all 76 Dean sections, and on the 40 of
    them no designation ever covers it could never fire — a requirement shown where it cannot hold.
    Dropping `extents` would be wrong the other way: as `on_designation` it would show a Dean-worded
    restatement on every classified water in the province. The meaning is the intersection.

    Which designation satisfies `on`: any, for `classified_period`; one with a
    `steelhead_stamp_during`, for `steelhead_period`. A requirement whose intersection is empty is
    UNRESOLVED (`NO_DESIGNATION`) — it names a place no designation reaches, which is a curation
    defect — and the bundle refuses to build with one. Every narrowing is a diagnostic.

    `records` maps `(entry_id, id)` to the raw licensing record; `designations` to the validated
    `Designation`."""
    holds: dict[str, set[str]] = {"classified_period": set(), "steelhead_period": set()}
    for p in placements:
        if p.kind != "designation" or p.placement != "sections":
            continue
        d = designations.get((p.entry_id, p.record_id))
        holds["classified_period"].update(p.sections)
        if d is not None and d.steelhead_stamp_during is not None:
            holds["steelhead_period"].update(p.sections)
    out: list[LicensingPlacement] = []
    diags: list[Diagnostic] = []
    for p in placements:
        rec = records.get((p.entry_id, p.record_id)) or {}
        on = rec.get("on")
        if p.kind != "requirement" or not on or rec.get("extents") is None \
                or p.placement != "sections":
            out.append(p)
            continue
        keep = tuple(s for s in p.sections if s in holds[on])
        if not keep:
            diags.append(Diagnostic(p.entry_id, p.record_id, NO_DESIGNATION, {
                "on": on, "sections": len(p.sections)}))
            out.append(LicensingPlacement(
                p.entry_id, p.record_id, p.kind, "unresolved", reason=NO_DESIGNATION,
                detail=f"its {len(p.sections)} sections carry no designation that satisfies "
                       f"`on: {on}`", tributaries_pending=p.tributaries_pending))
            continue
        if len(keep) != len(p.sections):
            diags.append(Diagnostic(p.entry_id, p.record_id, "on_narrowed", {
                "on": on, "before": len(p.sections), "after": len(keep)}))
        out.append(LicensingPlacement(
            p.entry_id, p.record_id, p.kind, p.placement, sections=keep,
            via_tributary=tuple(s for s in p.via_tributary if s in set(keep)),
            tributaries_pending=p.tributaries_pending, reason=p.reason, detail=p.detail))
    return out, diags


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


#: POLICY (user rulings 2026-10-03), named so a test can switch it off and watch it go red: a
#: section takes the Classified Waters designation of the FIRST classified water it flows into,
#: and a tributary with a row of its own that prints no designation is not classified at all.
FIRST_CLASSIFIED_WATER_DOWNSTREAM = True

#: POLICY (owner 2026-11-16, T798 item 1, RULINGS Li5a): a water a designation EXCEPTS BY PART and walks past
#: (`tributary_excludes` with `walk_past`) is no stop for that designation's own walk. The Elk's "ALL tributaries
#: (EXCEPT Coal Creek downstream of old MF&M Railway Bridge …) are Class II"; Coal Creek's row un-classifies only
#: "the part described", so Coal Creek above the bridge stays the Elk's Class II (190 sections were dropped).
A_WALKED_PAST_EXCEPTION_IS_NO_STOP = True


#: POLICY (owner 2026-11-20 12:10/12:11, T973; the CO question stays open): A UNIT CARRIES THROUGH A ROWED NO-UNIT
#: WATER, AS A "MAY NEED". LI-2 still drops the section from the designation (the placements, the licensing tables and
#: `licensing_digest` are exactly what they were), but the walk goes on down past the rowed water and the section keeps,
#: BESIDE the placements, the unit of the nearest classified water below it (`CarriedUnit`: its designation, the
#: classified water it comes from, and `through`, the rowed no-unit waters passed on the way up). RU-13 holds: the walk
#: stops at the first designation's own reach, so an upstream Class II overrules a downstream Class I above it. Whether
#: a carried unit is "you need this" or "you may need it" is a display choice (the app's "may need" note, owner 12:11);
#: the CO's answer settles it later without a rebuild. False = the mutation pin (nothing carried, as before).
A_ROWED_NO_UNIT_WATER_CARRIES_THE_UNIT = True


#: POLICY (owner 2026-11-20, T973: "this also happens invereted ... for example a upstrem class i overrules a downstream
#: class ii too. (assumign that the classified water sis applied to tributaries) usually it says it?"; default pending
#: the owner's confirmation): ONLY A UNIT THAT REACHES ITS TRIBUTARIES CLAIMS THE SECTIONS ABOVE IT. The nearest
#: classified water wins whatever its class (RU-13), but a designation whose row covers only its own water (no
#: "[Includes Tributaries]", not a tributaries-only row) is passed like a rowed no-unit water: a walked section above it
#: leaves the placements and keeps the nearer tributary-reaching unit below as a carried "may need", that water in
#: `through`. Asked through `claims` (the designations reaching their tributaries, `build_reaches`); without it, every
#: designation claims as before. False = the mutation pin.
ONLY_A_UNIT_ON_ITS_TRIBUTARIES_CLAIMS_THEM = True


@dataclass(frozen=True)
class CarriedUnit:
    """A section LI-2 drops (`own_row_not_classified`) that keeps a unit as a "may need" (T973).

    `entry_id` / `record_id`: the designation of the nearest classified water below the section (its class and unit are
    the designation's); `through`: the rowed waters with no unit the walk passed, each `(item_id, name)`, IN ORDER UP
    FROM THE CLASSIFIED WATER (the Stellako's unit on a creek above the Endako: `(("gnis:…", "Endako River"),)`). Never
    empty: a section that passes no such water is placed as before and is no carried unit."""

    section: str
    entry_id: str
    record_id: str
    through: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        if not self.through:
            raise ValueError(f"{self.section}: a carried unit passed no rowed water — it is an ordinary placement")


def carried_units(carried: list[CarriedUnit], placements: list[LicensingPlacement], unit_of=None
                  ) -> tuple[list[CarriedUnit], dict[str, int]]:
    """The carried units that stand: ONE PER SECTION, and none on a section any final designation holds (a section one
    walk drops and another places has its unit; the note would contradict it). Two walks dropping one section reach the
    same unit through the same waters (the walk down from a section does not depend on who walked it, beyond the walker's
    own water); where they do not, the first in (section, entry, record, through) order is kept and counted
    (`disagree`; `disagree_unit` where `unit_of((entry, record))` — the unit and class — differs too: two rows of one
    water printing one unit, the Stellako's in Regions 6 and 7, are no disagreement in what the note says). Sorted by
    section."""
    held = {s for p in placements if p.kind == "designation" and p.placement == "sections" for s in p.sections}
    by_section: dict[str, set[CarriedUnit]] = {}
    for c in carried:
        by_section.setdefault(c.section, set()).add(c)
    out: list[CarriedUnit] = []
    stats = {"placed_elsewhere": 0, "disagree": 0, **({"disagree_unit": 0} if unit_of else {})}
    for s in sorted(by_section):
        if s in held:
            stats["placed_elsewhere"] += 1
            continue
        cs = sorted(by_section[s], key=lambda c: (c.entry_id, c.record_id, c.through))
        if len({(c.entry_id, c.record_id, c.through) for c in cs}) > 1:
            stats["disagree"] += 1
            if unit_of and len({unit_of((c.entry_id, c.record_id)) for c in cs}) > 1:
                stats["disagree_unit"] += 1
        out.append(cs[0])
    return out, stats


def _walked_past_sections(record, registry) -> set[str]:
    """The sections of every water `record` excepts by part and walks past (`walk_past`)."""
    if not A_WALKED_PAST_EXCEPTION_IS_NO_STOP or record is None:
        return set()
    out: set[str] = set()
    for ex in getattr(record, "tributary_excludes", None) or ():
        ex = ex if isinstance(ex, dict) else getattr(ex, "model_dump", lambda: {})()
        if not ex.get("walk_past"):
            continue
        for item in ([ex["item_id"]] if ex.get("item_id") else []) + list(ex.get("item_ids") or []):
            if item in registry:
                out |= set(registry[item].section_ids)
    return out


def first_classified_downstream(placements: list[LicensingPlacement], records: dict, graph,
                                registry, owned, kind_of, matched_of: dict | None = None,
                                carried: list[CarriedUnit] | None = None, claims: set | None = None
                                ) -> tuple[list[LicensingPlacement], list[Diagnostic]]:
    """ONE CLASSIFIED WATERS UNIT PER SECTION (user rulings 2026-10-03, RU-13 and LI-2).

    Two designations' tributary walks both reach a creek above the Morice (the Bulkley's, the
    Morice's), and 10,227 sections carried two units — a non-resident told to buy two different
    day licences. The ruling: a section walked by a designation takes the designation of the
    FIRST CLASSIFIED WATER IT FLOWS INTO — the nearest water downstream with a designation of its
    OWN (bound by reach, not by walk). Gosnell Creek joins the Morice before the Bulkley: the
    Morice's. The Nanika, above Morice Lake, flows into Morice Lake and so into the Morice: the
    Morice's, although the Morice's own walk stops at the lake at the top of its reach — the
    Bulkley's walk found it and hands it on (`rehomed`).

    And a TRIBUTARY WITH A ROW OF ITS OWN THAT PRINTS NO DESIGNATION IS NOT CLASSIFIED (LI-2):
    the Endako under the Stellako's "[Includes Tributaries]", Gosnell Creek under the Morice's.
    The 2026-09-30 ruling for a joining water at a confluence cut ("whose own row prints no CW —
    the Iltasyuko — does not inherit") holds for every walked tributary: the row is the book's
    word on that water, and it says nothing of a licence. Streams only — "tributaries" are
    streams (p.80) — so a rowed LAKE the flow passes through is not a stop. It and everything
    above it are left out, as at a cut.

    HOW: walking DOWN from each inherited section (`graph.down_adj`, the main flow first), the
    first section met that is any designation's own reach, or a stream of a rowed water, decides:
      · the walker's own reach         → the section stays the walker's;
      · another designation's own      → the section is RE-HOMED to that designation
                                         (its `via_tributary`), whatever its class or period
                                         (one unit per section; `why_not_yield` is reported);
      · a water with a row of its own
        and no designation             → the section is dropped (`own_row_not_classified`);
      · nothing before the sea         → it stays.
    A row of the walker's own entry, or one pointing at it (`build.own_rows`), is no stop, and
    neither is the walker's OWN WATER (`matched_of[entry_id]`, the entry's matched items — a
    tributaries-only row like "Elk River's tributaries" has no reach of its own and its river is
    where every section it binds flows first) — EXCEPT the reach of another designation of the
    SAME entry: the Zymoetz's row prints unit A below Limonite Creek and unit B above it, and a
    section walked by both takes the one whose reach it actually flows into (Limonite Creek and
    the tributaries joining at the A/B cut take the reach the graph joins them to; ruling
    2026-10-04 under the user's 2026-10-03 rule).
    THE ONE PASS (T679 #10, 2026-11-20). A walked section that is ANOTHER designation's own reach is that
    designation's stop, so it is re-homed to it whatever its class or period. One case is decided before the
    walk, because the walker's own water is no stop: a walked section on the WALKER'S OWN WATER (`mine`, outside
    its own reach) that exactly one other designation names by reach goes to that one when it says at least as
    much (`why_not_yield` finds nothing: `trib_yields_to_own`); otherwise — two own designations (contested), or
    one that would lose a period, stamp, class or suspension — the walker keeps it beside them
    (`trib_kept_beside_own`). This was a separate pass before this one (`own_beats_inherited`); every other
    section it decided, this pass re-homed to the same designation, so only this case was its own.

    THE CARRIED UNIT (T973, `A_ROWED_NO_UNIT_WATER_CARRIES_THE_UNIT`). A section dropped at a rowed water with no
    designation is still dropped from the placement; when `carried` is given, the walk goes on down past that water (and
    any further one) to the first stop that decides as above — the walker's own water, another designation's own reach,
    a rowed water with a designation — and appends a `CarriedUnit` with the waters passed (`through`). Nothing below
    (the sea, a braid turning back) leaves it the walker's, as an unpassed section stays. The placements and the
    diagnostics are the same whether or not `carried` is given (`carried_units` then keeps one per section).
    `claims` (T973, `ONLY_A_UNIT_ON_ITS_TRIBUTARIES_CLAIMS_THEM`): the designations whose row reaches its tributaries.
    Given, a designation outside it never takes a walked section: the walk passes its water as it passes a rowed
    no-unit water — the section leaves the placement (`unit_not_on_tributaries`) and, with `carried`, keeps the nearer
    claiming unit below with that water in `through`.

    Every removal and every re-homing is a diagnostic. `records` maps `(entry_id, record_id)`
    to the validated `Designation`; `owned` is `outside.rowed_waters`; `kind_of(section)` is
    the registry's water kind."""
    if not FIRST_CLASSIFIED_WATER_DOWNSTREAM:
        return placements, []
    from pipeline.atlas.reach.build import own_rows
    from pipeline.common.models.graph import MAINSTEM_EDGE_KINDS

    own_of: dict[str, set[tuple[str, str]]] = {}
    by_key: dict[tuple[str, str], LicensingPlacement] = {}
    for p in placements:
        if p.kind == "designation" and p.placement == "sections":
            by_key[(p.entry_id, p.record_id)] = p
            trib = set(p.via_tributary)
            for s in p.sections:
                if s not in trib:
                    own_of.setdefault(s, set()).add((p.entry_id, p.record_id))
    rowed: set[str] = set()
    rowed_items: dict[str, set[str]] = {}       # section -> the rowed items holding it (T973's `through`)
    carrying = carried is not None and A_ROWED_NO_UNIT_WATER_CARRIES_THE_UNIT
    passing = claims is not None and ONLY_A_UNIT_ON_ITS_TRIBUTARIES_CLAIMS_THEM

    def claimers(ds):
        """(T973) Of designations met by the walk, the ones that may claim a section above them: all of them, unless
        `claims` (the designations whose row reaches its tributaries) is given and the policy is on."""
        return set(ds) if not passing else {d for d in ds if d in claims}
    water_of: dict[str, set[str]] = {}          # entry -> the sections of the waters it rows
    desig_rows: dict[str, list[tuple[str, str]]] = {}   # entry -> its placed designations
    for item, rows in (owned or {}).items():
        if item not in registry:
            continue
        secs = set(registry[item].section_ids)
        rowed |= secs
        if carrying or passing:
            for sec in secs:
                rowed_items.setdefault(sec, set()).add(item)
        for row in rows:
            eid = row if isinstance(row, str) else row[0]
            water_of.setdefault(eid, set()).update(secs)
    for (eid, rid) in by_key:
        desig_rows.setdefault(eid, []).append((eid, rid))
    reach_of: dict[str, dict[str, set[str]]] = {}     # entry -> designation -> its own reach
    for s, ds in own_of.items():
        for eid, rid in ds:
            reach_of.setdefault(eid, {}).setdefault(rid, set()).add(s)
    for eid, items in (matched_of or {}).items():
        for item in items or ():
            if item in registry:
                water_of.setdefault(eid, set()).update(registry[item].section_ids)

    from pipeline.atlas.graph import tributaries as _tribs
    outlets = _tribs.dead_end_outlets(graph) if _tribs.A_DEAD_END_FLOWS_WHERE_ITS_WATER_FLOWS else {}

    def is_stop(node: str) -> bool:
        return node in own_of or (node in rowed and kind_of(node) == "stream")

    def options(node: str) -> list[str]:
        """Where the flow goes from `node`, the main flow first: the water continuing or entering
        a lake (`continuation`, `lake_out`, `lake_in`, `outlet`) before a `confluence` — a braid's
        side channel joins its own river by a confluence edge, and the Nanika's pieces join each
        other eleven times before one enters Morice Lake."""
        eis = graph.down_adj.get(node, [])
        rank = {k: 0 for k in MAINSTEM_EDGE_KINDS} | {"lake_in": 1, "outlet": 1}
        got = [graph.edges[i].to_node for i in
               sorted(eis, key=lambda i: (rank.get(graph.edges[i].kind, 2), i))]
        # a dead end flows on where its water does (T944, `A_DEAD_END_FLOWS_WHERE_ITS_WATER_FLOWS`)
        return got + [x for x in outlets.get(node, ()) if x not in got]

    def next_down(node: str) -> str | None:
        got = options(node)
        return got[0] if got else None

    MISSING = object()
    memo: dict[str, str | None] = {}

    def stop_at_or_below(node: str | None) -> str | None:
        """The first stop at `node` or below it: a depth-first walk down the flow, the main flow
        first, every braid followed, no node twice. The nodes on the path that reached the stop
        are memoised with it; a start from which no stop is reachable is memoised `None`."""
        if node is None:
            return None
        got = memo.get(node, MISSING)
        if got is not MISSING:
            return got
        seen = {node}
        parent: dict[str, str | None] = {node: None}
        stack = [node]
        while stack:
            cur = stack.pop()
            known = memo.get(cur, MISSING) if cur != node else MISSING
            if known is not MISSING:
                if known is None:
                    continue
                found = known
            elif is_stop(cur):
                found = cur
            else:
                for nxt in reversed(options(cur)):
                    if nxt not in seen:
                        seen.add(nxt)
                        parent[nxt] = cur
                        stack.append(nxt)
                continue
            x: str | None = cur
            while x is not None:                 # the path that led here flows into the stop
                memo[x] = found
                x = parent.get(x)
            return found
        for x in seen:
            memo.setdefault(x, None)
        return None

    out: list[LicensingPlacement] = []
    diags: list[Diagnostic] = []
    gained: dict[tuple[str, str], dict[str, set[tuple[str, str]]]] = {}
    for p in placements:
        if p.kind != "designation" or p.placement != "sections" or not p.via_tributary:
            out.append(p)
            continue
        me = (p.entry_id, p.record_id)
        # the designation's own reach, and its own WATER beyond it (a row of the same water
        # printed for another stretch, a park-clipped piece: the walker's, never a stop)
        # — less the reaches of the entry's OTHER designations: one entry printing two units
        # on two stretches (the Zymoetz's A and B) hands each walked section to the stretch it
        # actually flows into, never to both (F1: 115 Limonite/Zymoetz sections carried both)
        reach = set(p.sections) - set(p.via_tributary)
        siblings = set().union(*(secs for rid, secs in reach_of.get(p.entry_id, {}).items()
                                 if rid != p.record_id))
        mine = reach | (water_of.get(p.entry_id, set()) - (siblings - reach))
        # a water this designation excepts by part and walks past: its row is no stop (Li5a)
        past = _walked_past_sections(records.get(me), registry)
        rehomed: dict[tuple[str, str], set[str]] = {}
        dropped: dict[str, set[str]] = {}
        passed_by: dict[str, set[str]] = {}       # T973: designations whose unit stops at their own water -> sections
        yielded: dict[str, set[tuple[str, str]]] = {}
        kept: dict[tuple[tuple[str, str], str], int] = {}
        carry_memo: dict[str, tuple[tuple[str, str], tuple[tuple[str, str], ...]]] = {}

        def carry_from(t0: str, me=me, mine=mine, past=past, p=p, memo=carry_memo):
            """(T973) From a stop `t0` on a rowed water with no designation, on down to the first stop that decides:
            (the designation, the rowed no-unit waters passed — up from the classified water)."""
            if t0 in memo:
                return memo[t0]
            met: list[tuple[str, str]] = []           # nearest the section first
            to: tuple[str, str] | None = None
            t: str | None = t0
            seen: set[str] = set()
            while t is not None and t not in seen:    # a braid turning back: stop
                seen.add(t)
                if t in mine:
                    to = me
                    break
                others = own_of.get(t, set()) - {me}
                if others:
                    claim = claimers(others)
                    if claim:
                        to = min(claim)
                        break
                    note_waters(t, met, None, others)      # a unit that stops at its own water: passed
                elif t not in past:
                    rows = own_rows(registry, owned, {t}, p.entry_id)
                    if rows:
                        with_desig = sorted(d for r in rows for d in desig_rows.get(r, ()))
                        claim = sorted(claimers(with_desig))
                        if claim:
                            to = claim[0]
                            break
                        note_waters(t, met, None if with_desig else p.entry_id, with_desig)
                t = stop_at_or_below(next_down(t))
            memo[t0] = got = (to or me, tuple(reversed(met)))
            return got

        def note_waters(t, met, walker, desigs):
            """The waters at stop `t` the walk passes (`through`): the rowed items there (for a rowed no-unit water,
            not the walker's own rows), else the passed designations' units by name."""
            got = []
            for item in sorted(rowed_items.get(t, ())):
                if walker is None or any((r if isinstance(r, str) else r[0]) != walker
                                         and walker not in (() if isinstance(r, str) else r[1]) for r in owned[item]):
                    got.append((item, registry[item].name))
            if not got:
                got = [(f"entry:{d[0]}", records[d].unit_name) for d in sorted(desigs) if d in records]
            for w in got:
                if w not in met:
                    met.append(w)

        for s in p.via_tributary:
            here = own_of.get(s, set()) - {me}
            if here and s in mine:
                # ON THE WALKER'S OWN WATER another designation names by reach (the docstring's one case)
                if len(here) > 1:
                    kept[(min(here), "contested")] = kept.get((min(here), "contested"), 0) + 1
                    continue
                (o,) = here
                why = why_not_yield(records[o], records[me])
                if why:
                    kept[(o, why)] = kept.get((o, why), 0) + 1
                else:
                    yielded[s] = here
                continue
            t = stop_at_or_below(s)
            passed: set[str] = set()
            while t is not None and t not in passed:      # a braid turning back: stop
                passed.add(t)
                if t in mine:
                    break
                others = own_of.get(t, set()) - {me}
                if others:
                    claim = others if t == s else claimers(others)    # its own reach is always its own
                    if claim:
                        rehomed.setdefault(min(claim), set()).add(s)
                    else:                                 # T973: its unit stops at its own water — passed
                        passed_by.setdefault(",".join(sorted(f"{e}#{r}" for e, r in others)), set()).add(s)
                        if carrying:
                            to, through = carry_from(t)
                            carried.append(CarriedUnit(s, to[0], to[1], through))
                    break
                if t in past:                             # walked past: keep going down (Li5a)
                    t = stop_at_or_below(next_down(t))
                    continue
                rows = own_rows(registry, owned, {t}, p.entry_id)
                if rows:
                    # a rowed water: with a designation of its own (its row prints one, though
                    # this section is outside that designation's reach) it is the first
                    # classified water the section flows into; with none, not classified
                    with_desig = sorted(d for r in rows for d in desig_rows.get(r, ()))
                    claim = sorted(claimers(with_desig))
                    if claim:
                        rehomed.setdefault(claim[0], set()).add(s)
                    elif with_desig:                      # T973: its unit stops at its own water — passed
                        passed_by.setdefault(",".join(f"{e}#{r}" for e, r in with_desig), set()).add(s)
                        if carrying:
                            to, through = carry_from(t)
                            carried.append(CarriedUnit(s, to[0], to[1], through))
                    else:
                        dropped.setdefault(",".join(rows), set()).add(s)
                        if carrying:                      # T973: the unit below, as a "may need"
                            to, through = carry_from(t)
                            carried.append(CarriedUnit(s, to[0], to[1], through))
                    break
                t = stop_at_or_below(next_down(t))
        gone = set().union(*rehomed.values(), *dropped.values(), *passed_by.values()) \
            if (rehomed or dropped or passed_by) else set()
        gone |= set(yielded)
        for (o, why), n in sorted(kept.items()):
            diags.append(Diagnostic(p.entry_id, p.record_id, "trib_kept_beside_own", {
                "sections": n, "own": f"{o[0]}#{o[1]}", "why": why}))
        if yielded:
            diags.append(Diagnostic(p.entry_id, p.record_id, "trib_yields_to_own", {
                "removed": len(yielded), "kept": sum(1 for s in p.sections if s not in gone),
                "to": sorted({f"{e}#{i}" for v in yielded.values() for e, i in v})}))
        for d, secs in sorted(rehomed.items()):
            why = why_not_yield(records[d], records[me]) if d in records and me in records \
                else None
            diags.append(Diagnostic(p.entry_id, p.record_id, "first_classified_downstream", {
                "rehomed": len(secs), "to": f"{d[0]}#{d[1]}",
                **({"why_not_yield": why} if why else {})}))
            gained.setdefault(d, {}).update({s: {me} for s in secs})
        for ds, secs in sorted(passed_by.items()):
            diags.append(Diagnostic(p.entry_id, p.record_id, "unit_not_on_tributaries", {
                "removed": len(secs), "passed": ds.split(",")}))
        for rows, secs in sorted(dropped.items()):
            diags.append(Diagnostic(p.entry_id, p.record_id, "own_row_not_classified", {
                "removed": len(secs), "rows": rows.split(",")}))
        if not gone:
            out.append(p)
            continue
        keep = tuple(s for s in p.sections if s not in gone)
        if not keep:
            raise AssertionError(f"{p.entry_id}#{p.record_id}: every section it binds flows "
                                 f"into another classified water first — it would end bound "
                                 f"to nothing")
        out.append(LicensingPlacement(
            p.entry_id, p.record_id, p.kind, p.placement, sections=keep,
            via_tributary=tuple(s for s in p.via_tributary if s not in gone),
            tributaries_pending=p.tributaries_pending, reason=p.reason, detail=p.detail))
    if gained:
        final: list[LicensingPlacement] = []
        for p in out:
            add = gained.get((p.entry_id, p.record_id))
            if not add:
                final.append(p)
                continue
            have = set(p.sections)
            new = sorted(s for s in add if s not in have)
            if not new:                        # already its own: nothing to add, nothing to say
                final.append(p)
                continue
            froms = sorted({f"{e}#{r}" for s in new for e, r in add[s]})
            diags.append(Diagnostic(p.entry_id, p.record_id, "inherited_from_upstream_walk", {
                "added": len(new), "from": froms}))
            final.append(LicensingPlacement(
                p.entry_id, p.record_id, p.kind, p.placement,
                sections=tuple(list(p.sections) + new),
                via_tributary=tuple(list(p.via_tributary) + new),
                tributaries_pending=p.tributaries_pending, reason=p.reason, detail=p.detail))
        out = final
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
    for key, (removed, items) in sorted(carved.items()):
        eid, rid = key[0], key[1]           # (entry, record[, carve-out index])
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
    for key, (removed, items) in sorted(carved.items()):
        eid, rid = key[0], key[1]           # (entry, record[, carve-out index])
        owners = _owners(eid, items, claims)
        if not owners or not removed:
            continue
        have = set().union(*(held.get(e, set()) for e in owners))
        orphans = sorted(removed - have)
        if orphans:
            out.append(Diagnostic(eid, rid, "carve_out_orphan", {
                "owners": owners, "removed": len(removed), "orphans": orphans}))
    return out
