"""The pass: entries + one build's (registry, graph) → bindings, diagnostics, a report.

Orchestration only — every policy decision is in `classify.py`, and all resolution is
`pipeline.atlas.reach.extent`. This module's job is to be *total* (every rule accounted
for) and *deterministic* (sorted throughout), because everything downstream inherits
both.

Measured: the full corpus resolves in ~42 s, 5 s of which is loading the graph. There is
no performance problem here — do not add parallelism.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from pipeline.atlas.reach.covered import covered_ids as _covered_ids
from pipeline.atlas.graph import tributaries as _tribs
from pipeline.atlas.reach import classify as _policy
from pipeline.atlas.reach.classify import classify, wants_tributaries
from pipeline.atlas.reach.models import (
    BuildReport, Diagnostic, Outcome, Reason, RuleBinding, iter_entries,
)
from pipeline.atlas.reach import extent as _resolve
from pipeline.atlas.reach.outside import (
    outside_bc, region_limit, rowed_waters, shared_waters, tidal_sections,
)
from pipeline.regs.parsing.catalogue import Designation
from pipeline.common.models import NodeKind as _NodeKind
from pipeline.atlas.reach.licensing import (
    PLACED_KINDS, LicensingPlacement, as_rule, carve_out_orphans, carve_outs_to_owner,
    national_park_sections, on_designations, own_beats_inherited, place_record,
    without_national_parks,
)


@dataclass
class ReachResult:
    bindings: list[RuleBinding]
    diagnostics: list[Diagnostic]
    report: BuildReport
    #: Every placed licensing record (`pipeline.atlas.reach.licensing`), resolved through the
    #: same `build_reach` as the rules. Kept apart from `bindings` so the rules' digest — the
    #: thing that must not move between identical builds — is exactly what it was.
    licensing: list[LicensingPlacement] = field(default_factory=list)
    licensing_diagnostics: list[Diagnostic] = field(default_factory=list)


def build_reaches(entries, registry, graph, *, build: str = "", handles: str = "",
                  covered_fn=None) -> ReachResult:
    """Resolve every rule in `entries` against one build.

    `entries` is an iterable of ``(region, entry_dict)`` — the shape
    `pipeline.regs.parsing.io.read_entries_dir` and the review app both produce.

    Covered items come from `pipeline.atlas.reach.covered` — `entry.matched`, and nothing else;
    the review app reads the same function, so the bundle and the app never disagree about
    which water a rule is about. Pass `covered_fn(entry, registry)` to override.
    """
    t0 = time.time()
    bindings: list[RuleBinding] = []
    diagnostics: list[Diagnostic] = []
    licensing: list[LicensingPlacement] = []
    lic_diags: list[Diagnostic] = []
    #: item -> entries covering it with `includes_tributaries: true`, and each carving
    #: designation's removed sections — for `carve_out_orphans`.
    claims: dict[str, list[str]] = {}
    carved: dict[tuple, tuple[set[str], set[str]]] = {}
    designations: dict = {}
    report = BuildReport(build=build, handles=handles)
    ents = sorted(iter_entries(entries), key=lambda x: x["entry_id"])
    # A LAKE CUT INTO PARTS IS NOT A PLACE A RECORD MAY NAME. Refused here as well as at ingest
    # (`validate_catalogue.split_parent_refs`): an entry edited in the review app or a registry
    # rebuilt under the corpus must not bind the ghost of a lake whose water now belongs to its
    # parts.
    bad = split_parent_refs(ents, registry, graph)
    if bad:
        raise SystemExit("reach: records bind a lake that is cut into parts — name the part(s):\n  "
                         + "\n  ".join(bad))
    outside = outside_bc(registry, graph)
    # The waters more than one region prints a row for: those rows stay in their own region; every
    # other row applies along its water's whole length (`outside.region_limit`).
    shared = shared_waters(ents)
    # The waters the tables print a row of their own for: a confluence cut's joining water with
    # none goes with the cut, one with its own row stays out (`confluence_excludes`).
    owned = rowed_waters(ents)
    # Water the book calls tidal (Nitinat Lake): out of every row but its own (`tidal_sections`).
    tidal = tidal_sections(ents, registry)
    parks = national_park_sections(registry)

    for e in ents:
        report.n_entries += 1
        entry_id = e["entry_id"]
        covered = (covered_fn(e, registry) if covered_fn
                   else _covered_ids(e, registry))

        clip, scope_failed = entry_scope(e, covered, registry, graph)
        if scope_failed:
            report.scope_unresolved.append(entry_id)
        # PIECES THE ROW'S OWN SCOPE COULD NOT PLACE. `entry_scope` keeps only what its extents
        # place, so a braid straddling the row's cut left EVERY rule of the row with no word said
        # — the Peace's side channels across the Site C reach were covered by none of its three
        # rows. Reported per entry (rule_id ""), never placed: the curator decides (AGENTS 12).
        lost = scope_unclassified(e, covered, registry, graph, clip)
        if lost:
            diagnostics.append(Diagnostic(entry_id, "", "scope_unclassified", {
                "pieces": lost, "why": "the entry's scope straddles or cannot place these; "
                                       "no rule of the entry binds them"}))

        for rule in e.get("rules") or []:
            report.n_rules += 1
            binding, diags = build_reach(e, rule, registry, graph,
                                         covered=covered, clip=clip, outside=outside,
                                         shared=shared, tidal=tidal, owned=owned)
            bindings.append(binding)
            diagnostics.extend(diags)

        if e.get("includes_tributaries") is True:
            for i in covered:
                claims.setdefault(i, []).append(entry_id)

        # LICENSING, in the entry's own context: same covered items, same clip, same resolver.
        for rec in e.get("licensing") or []:
            if rec.get("kind") not in PLACED_KINDS:
                continue
            # `regional=False`: a designation is the water's, not the region's (`build_reach`).
            reach = (lambda r, e=e, covered=covered, clip=clip: build_reach(
                e, r, registry, graph, covered=covered, clip=clip, outside=outside,
                regional=False, tidal=tidal, owned=owned))
            placed, diags = place_record(e, rec, reach)
            placed, parked = without_national_parks(placed, parks)
            diags = list(diags) + parked
            licensing.append(placed)
            if rec.get("kind") == "designation":
                designations[(entry_id, rec["id"])] = Designation.model_validate(rec)
            lic_diags.extend(diags)
            # What a designation's carve-out removes, for the check below.
            if (rec.get("kind") == "designation" and rec.get("tributary_excludes")
                    and placed.placement == "sections"):
                bare, _ = reach(as_rule({**rec, "tributary_excludes": []},
                                        rec.get("extents") if rec.get("extents") is not None
                                        else list(e.get("extents") or [])))
                removed = set(bare.sections) - set(placed.sections) - parks
                # PER CARVE-OUT: each excluded water's own designation takes only what ITS carve-out
                # removed. Pooled, the Atnarko's three EXCEPTs handed Hunlen's and Young's upper
                # creeks (435 sections) to Burnt Bridge Creek — the only one of the three with a
                # designation.
                detail, _ = resolve_carve_outs(e, rec, registry, graph, covered)
                for i, (x, row) in enumerate(zip(rec["tributary_excludes"], detail)):
                    secs = set(row.get("sections") or ())
                    mine = secs if row.get("walk_past") else \
                        secs | set(_tribs.tributaries_of_reach(graph, secs))
                    items = set(([x.get("item_id")] if x.get("item_id") else [])
                                + list(x.get("item_ids") or []))
                    carved[(entry_id, rec["id"], i)] = (removed & mine, items)

    # A water's own designation beats one inherited by another water's tributary walk.
    licensing, yielded = own_beats_inherited(licensing, designations)
    lic_diags.extend(yielded)
    for d in yielded:
        k = f"designation:{d.kind}"
        report.licensing[k] = report.licensing.get(k, 0) + 1
    # Water a carve-out removed goes to the excluded water's own designation, and must end there.
    licensing, handed = carve_outs_to_owner(licensing, carved, claims)
    lic_diags.extend(handed)
    if handed:
        report.licensing["designation:carve_out_handed_to_owner"] = sum(
            len(d.payload["sections"]) for d in handed)
    orphans = carve_out_orphans(licensing, carved, claims)
    lic_diags.extend(orphans)
    if orphans:
        report.licensing["designation:carve_out_orphans"] = sum(
            len(d.payload["orphans"]) for d in orphans)
    # A REQUIREMENT WITH A PLACE AND AN `on` holds where both do: its sections, where a
    # designation that can satisfy `on` is placed. Last, because it reads the final designations.
    rec_of = {(e["entry_id"], x.get("id")): x for e in ents
              for x in (e.get("licensing") or []) if isinstance(x, dict)}
    licensing, narrowed = on_designations(licensing, rec_of, designations)
    lic_diags.extend(narrowed)
    for d in narrowed:
        k = f"requirement:{d.kind}"
        report.licensing[k] = report.licensing.get(k, 0) + 1

    for p in licensing:
        key = f"{p.kind}:{p.placement}"
        report.licensing[key] = report.licensing.get(key, 0) + 1
        if p.tributaries_pending:
            report.licensing["tributaries_pending"] = \
                report.licensing.get("tributaries_pending", 0) + 1

    report.tributaries_pending = sum(1 for b in bindings if b.tributaries_pending)
    for b in bindings:
        report.outcomes[b.outcome.value] = report.outcomes.get(b.outcome.value, 0) + 1
        if b.reason is not None:
            report.reasons[b.reason.value] = report.reasons.get(b.reason.value, 0) + 1
    for d in diagnostics:
        report.diagnostics[d.kind] = report.diagnostics.get(d.kind, 0) + 1
    report.seconds = round(time.time() - t0, 1)

    # Totality (doc 10 ⑪ + ㊳): every rule appears exactly once. RuleBinding's own
    # __post_init__ guarantees no bound-but-empty and no unresolved-but-unexplained.
    if report.total() != report.n_rules:
        raise AssertionError(f"{report.n_rules} rules but {report.total()} outcomes")

    bindings.sort(key=lambda b: (b.entry_id, b.rule_id))
    diagnostics.sort(key=lambda d: (d.entry_id, d.rule_id, d.kind))
    licensing.sort(key=lambda p: (p.entry_id, p.record_id))
    lic_diags.sort(key=lambda d: (d.entry_id, d.rule_id, d.kind))
    return ReachResult(bindings, diagnostics, report, licensing, lic_diags)


def build_reach(entry: dict, rule: dict, registry, graph, *, covered=None,
                clip=None, outside=None, shared=None,
                regional: bool = True, tidal=None,
                owned=None) -> tuple[RuleBinding, list[Diagnostic]]:
    """THE public answer to "what does this rule cover" — resolve, clip, classify, expand.

    One call, so no caller has to remember the order, or that tributaries need expanding.
    The review app got exactly that wrong once: it resolved and classified but never
    expanded, so a curator confirming "including tributaries" was shown the mainstem alone
    and would have signed off on a fraction of the real reach.

    The layers underneath stay separately testable — `extent.resolve_extent`,
    `tributaries.expand`, `classify.classify`. This only removes the chance to skip one.

    Two limits apply to every binding, computed here when the caller does not pass them, so the
    review app gets them by the same call (`pipeline.atlas.reach.outside`): water outside B.C.
    is subtracted (`outside`), and a regional row's reach is held to its region(s) after the walk
    — a PER-REGION row's to the regions its id names, any other row's to the regions its own
    water lies in (`shared`, the corpus's `outside.shared_waters`; the review app passes the same).

    `regional=False` skips the region limit — for LICENSING (`build_reaches` places every licensing
    record with it). A regional row's RULES are exceptions to that region's regulations and stop at
    its line; a CLASSIFIED WATER is one water, designated whole ("Class II water, including
    tributaries"), whichever region's table prints it. Held to the row's region, the Sustut's
    Class I tributaries in Region 7A, the Horsefly's in Region 3 and the West Road's in 6 and 7A
    lost their designation with no other row to give it back — and for licensing the unsafe
    direction is requiring too little (decision 2: licensing never opens or closes water, so
    reaching past a region line cannot change a water's status). The border still applies.

    `tidal` is the corpus's tidal water (`outside.tidal_sections`): taken out of the binding of
    every row that is not itself marked `tidal`, and reported. `None` (a caller that did not
    compute it) takes nothing out.

    `owned` is the corpus's `outside.rowed_waters` — which waters the tables print a row of their
    own for, which decides whether a confluence cut's joining water stays out of the walk or goes
    with the cut (`confluence_excludes`). `None` (a caller that did not compute it) knows of no
    row, so every joining water goes with the cut — the review app passes the same map.
    """
    if tidal and not entry.get("tidal"):
        binding, diags = build_reach(entry, rule, registry, graph, covered=covered, clip=clip,
                                     outside=outside, shared=shared, regional=regional,
                                     owned=owned)
        return _without_tidal(entry, rule, binding, diags, tidal)
    if covered is None:
        covered = _covered_ids(entry, registry)
    if outside is None:
        outside = outside_bc(registry, graph)
    rest = _rest_of(rule)
    if rest is not None:
        return _build_rest(entry, rule, rest, registry, graph, covered=covered, clip=clip,
                           outside=outside, shared=shared, regional=regional, owned=owned)
    region = region_limit(entry, registry, shared) if regional else None

    per: list[dict | None] = []
    clipped = False
    window = None
    out_of_region = 0
    for ex in rule.get("extents") or []:
        got = _resolve.resolve_extent(registry, graph, covered, ex)
        if got is not None:
            # The measure window the extent actually resolved to, handed straight to the
            # tributary walk so it never has to re-derive where the reach starts.
            window = window or got.get("window")
            if region is not None and _row_water(ex, entry.get("matched") or ()):
                before = len(got.get("sections") or ())
                got = _clip(got, region)
                out_of_region += before - len(got["sections"])
                clipped = clipped or len(got["sections"]) < before
            if clip is not None:
                before = len(got.get("sections") or ())
                got = _clip(got, clip, area_scope=_area_scope(entry))
                clipped = clipped or len(got["sections"]) < before
        per.append(got)

    expander = _expander(
        graph, registry, covered, rule, entry, window=window, owned=owned,
        region=region if any(_row_water(ex, entry.get("matched") or ())
                             for ex in rule.get("extents") or []) else None)
    binding, diags = classify(
        entry["entry_id"], rule, per,
        registry=registry, covered_ids=covered,
        scope_clipped=clipped, entry_has_registry=bool(covered),
        tributaries=wants_tributaries(rule, entry),
        tributaries_only=bool(rule.get("tributaries_only")),
        expand_tributaries=expander,
        kind_of=lambda s: _resolve._kind_of(graph, s),
        outside=outside,
    )
    for row in getattr(expander, "confluence", None) or ():
        # REPORTED, never silent: which joining water each confluence cut kept out of the walk,
        # and which the rule's words took in.
        diags.append(Diagnostic(entry["entry_id"], rule["rule_id"], "confluence_cut", {
            "split": row["split"], "water": row["water"], "included": row["included"],
            "in_reach": bool(row.get("in_reach")), "kept_out": len(row["sections"]),
            "own_row": list(row.get("own_row") or ()), "with_cut": bool(row.get("with_cut")),
            "joined": row.get("joined", 0)}))
    walked_out = getattr(expander, "out_of_region", 0)
    if out_of_region or walked_out:
        # REPORTED, never silent: what the row's own region(s) took away, before the walk and
        # from what the walk found.
        diags.append(Diagnostic(entry["entry_id"], rule["rule_id"], "region_clip", {
            "removed": out_of_region, "removed_from_walk": walked_out}))
    return binding, diags


def _without_tidal(entry: dict, rule: dict, binding: RuleBinding, diags: list[Diagnostic],
                   tidal) -> tuple[RuleBinding, list[Diagnostic]]:
    """`binding` minus the tidal sections, reported. A rule left with nothing is unresolved
    `tidal` — never bound to no water."""
    gone = set(binding.sections) & set(tidal)
    if not gone:
        return binding, diags
    kept = tuple(s for s in binding.sections if s not in gone)
    diags = diags + [Diagnostic(entry["entry_id"], rule["rule_id"], "tidal", {
        "removed": len(gone), "kept": len(kept),
        "why": "tidal water: the federal tidal regulations apply, no provincial rule does"})]
    if not kept:
        return RuleBinding(entry["entry_id"], rule["rule_id"], Outcome.unresolved, (),
                           Reason.tidal, f"every section it selects ({len(gone)}) is tidal water",
                           tributaries_pending=binding.tributaries_pending), diags
    return RuleBinding(entry["entry_id"], rule["rule_id"], Outcome.bound, kept,
                       via_tributary=tuple(s for s in binding.via_tributary if s not in gone),
                       tributaries_pending=binding.tributaries_pending), diags


def _rest_of(rule: dict) -> dict | None:
    """The rule's `rest` extent ("other parts"), or None. `CatalogueEntry` holds it to be the
    rule's only extent."""
    for x in rule.get("extents") or []:
        if isinstance(x, dict) and x.get("op") == "rest":
            return x
    return None


def _build_rest(entry: dict, rule: dict, rest: dict, registry, graph, *, covered, clip,
                outside, shared, regional, owned=None) -> tuple[RuleBinding, list[Diagnostic]]:
    """"OTHER PARTS": the rule's water MINUS every section its named siblings bind.

    The water is what `whole` (with the rest extent's own item scope) selects for THIS rule —
    its own tributary walk, carve-outs, region and entry clip, exactly as a `whole` rule of the
    row would bind. Each sibling is resolved by this same function, walk included, so "Galbraith
    Creek to Van Creek [Includes Tributaries]" takes its tributaries out of Bull River's other
    parts, while Findlay Creek's mainstem-only release leaves the tributaries along it in the rest.

    A sibling that does not draw its place, or does not bind, leaves the rest UNKNOWN
    (`classify.COMPLEMENT_UNKNOWN_IF_A_SIBLING_DOES_NOT_BIND`); pieces a sibling reports as
    straddling its end are withheld and reported (`classify.COMPLEMENT_WITHHOLDS_STRADDLERS`).
    The review app calls `build_reach` too, so it shows the same rest."""
    eid, rid = entry["entry_id"], rule["rule_id"]
    tribs = wants_tributaries(rule, entry)
    rules = {r.get("rule_id"): r for r in entry.get("rules") or [] if isinstance(r, dict)}

    def unknown(detail: str) -> tuple[RuleBinding, list[Diagnostic]]:
        return RuleBinding(eid, rid, Outcome.unresolved, (), Reason.complement_unknown, detail,
                           tributaries_pending=tribs), []

    taken: set[str] = set()
    withheld: set[str] = set()
    counts: dict[str, int] = {}
    for sid in rest.get("siblings") or []:
        sib = rules.get(sid)
        if sib is None or sid == rid:
            return unknown(f"rest sibling {sid!r} is not another rule of this entry")
        if (not sib.get("extents") or _rest_of(sib) is not None
                or str(sib.get("undrawn_part") or "").strip() or sib.get("unresolved_locators")
                or sib.get("standing")):
            return unknown(f"rest sibling {sid} does not draw its place, so the rest of the "
                           f"water around it is unknown")
        b, d = build_reach(entry, sib, registry, graph, covered=covered, clip=clip,
                           outside=outside, shared=shared, regional=regional, owned=owned)
        if _policy.COMPLEMENT_UNKNOWN_IF_A_SIBLING_DOES_NOT_BIND and (
                b.outcome is not Outcome.bound or b.tributaries_pending):
            why = (f"{b.reason.value}: {b.detail}" if b.reason is not None
                   else "its tributary walk is pending")
            return unknown(f"rest sibling {sid} does not bind ({why}) — the rest of the water "
                           f"is unknown until it does")
        taken |= set(b.sections)
        counts[sid] = len(b.sections)
        if _policy.COMPLEMENT_WITHHOLDS_STRADDLERS:
            withheld |= {p for x in d if x.kind == "unclassified"
                         for p in (x.payload.get("pieces") or ())}

    whole = {k: v for k, v in rest.items() if k != "siblings"} | {"op": "whole"}
    base, diags = build_reach(entry, {**rule, "extents": [whole]}, registry, graph,
                              covered=covered, clip=clip, outside=outside, shared=shared,
                              regional=regional, owned=owned)
    if base.outcome is not Outcome.bound:
        return base, diags
    held = set(base.sections) & withheld
    sections = set(base.sections) - taken - withheld
    diags.append(Diagnostic(eid, rid, "complement", {
        "siblings": counts, "water": len(base.sections),
        "removed": len(set(base.sections) & taken), "withheld": sorted(held),
        "kept": len(sections),
        "why": "other parts: the water minus the sections its siblings bind; pieces straddling "
               "a sibling's end are withheld, never decided"}))
    if not sections:
        return RuleBinding(eid, rid, Outcome.unresolved, (), Reason.no_sections,
                           "the siblings bind every section of the water — no other parts "
                           "remain", tributaries_pending=tribs), diags
    return RuleBinding(eid, rid, Outcome.bound, tuple(sorted(sections)),
                       via_tributary=tuple(s for s in base.via_tributary if s in sections)), diags


def resolve_carve_outs(entry: dict, rule: dict, registry, graph,
                       covered) -> tuple[list[dict], set[str]]:
    """The rule's tributary carve-outs, and every section they block.

    Returns ``(per_carve_out, blocked)``. Each entry of `per_carve_out` is the authored
    extent plus what it actually resolved to, so a caller can SHOW a curator which streams
    an EXCEPT clause removed instead of only the net section count. `blocked` is the union,
    which is what the walk is given.

    Public because the review app has to display exactly what the builder excluded; if it
    recomputed this itself the two could drift, and a curator would confirm a reach that is
    not the one that ships.
    """
    blocked: set[str] = set()
    detail: list[dict] = []
    # ONE PLACE A CARVE-OUT LIVES: the rule's own `tributary_excludes`. There is no entry-wide
    # list — one would cut every rule in the row, including a rule that is about the very water
    # another rule must not reach.
    for ex in rule.get("tributary_excludes") or []:
        # `walk_past` is the carve-out's own flag, not a place: resolved without it.
        past = bool(ex.get("walk_past"))
        got = _resolve.resolve_extent(registry, graph, covered,
                                      {k: v for k, v in ex.items() if k != "walk_past"})
        row = {"extent": ex, "resolved": got is not None,
               "sections": [], "above": 0}
        if past:
            row["walk_past"] = True
        if got is not None:
            secs = set(got.get("sections") or ())
            row["sections"] = sorted(secs)
            if not past:
                above = _tribs.tributaries_of_reach(graph, secs)
                blocked |= secs | above
                row["above"] = len(above)      # "and everything upstream of it"
        detail.append(row)
    return detail, blocked


def carve_out_passed(detail: list[dict]) -> set[str]:
    """The sections of the `walk_past` carve-outs in `resolve_carve_outs`' detail: removed from the
    rule, walked through — the named water has its own row, the streams feeding it do not."""
    return {s for row in detail if row.get("walk_past") for s in row.get("sections") or ()}


def confluence_excludes(entry: dict, rule: dict, registry, graph, covered,
                        owned=None) -> list[dict]:
    """THE WATER JOINING AT EACH OF THE RULE'S CONFLUENCE CUTS, and which side it goes with
    (`tributaries.CONFLUENCE_CUT_EXCLUDES_THE_JOINING_WATER`, user rulings 2026-09-29/30).

    A cut is at a confluence when its boundary is a curated `confluence` cut (the joining stream's
    mouth sits on it), or a point cut whose LABEL puts it NEAR a named confluence ("fishing boundary
    signs near the Mobbs Creek confluence") and the named stream joins its line within
    `CONFLUENCE_NEAR_M`. A cut placed a stated distance from a confluence is not at it: "signs 100 m
    below the Slesse Creek confluence" closes the Chilliwack upstream, Slesse Creek with it.

    Who decides, in order:
      1. the rule's own words — "upstream of and including Hemmingsen Creek" takes it in (walked);
         "but not including the Muchalat" keeps it out;
      2. signs the words put below (or above) the confluence put it inside the reach (the Nass);
      3. a joining water with a ROW OF ITS OWN (`owned`, the corpus's `outside.rowed_waters`,
         any table row but this one) is kept out, it and everything above it;
      4. one with none GOES WITH THE CUT, on both sides
         (`tributaries.CONFLUENCE_WATER_WITHOUT_A_ROW_GOES_WITH_THE_CUT`): its subtree is added to
         the walk of a rule on either side (`_expander`).
    Licensing records take the same test: a joining water with no row inherits the designation,
    one whose own row prints none does not.

    Returns one row per joining water: ``{"split", "water", "mouths", "sections", "included",
    "own_row", "with_cut", "negated"}``; `sections` is what is KEPT OUT (empty unless it is).
    Public, like `resolve_carve_outs`, so the review app shows what the builder kept out."""
    if not _tribs.CONFLUENCE_CUT_EXCLUDES_THE_JOINING_WATER:
        return []
    words = " ".join(str(rule.get(k) or "") for k in ("verbatim", "extent_text"))
    rows: list[dict] = []
    done: set[tuple[str, str]] = set()
    for ex in rule.get("extents") or []:
        if not isinstance(ex, dict) or ex.get("watershed") \
                or ex.get("op") not in ("upstream_of", "downstream_of", "between"):
            continue
        scope = ex.get("item_ids") or ([ex["item_id"]] if ex.get("item_id") else covered)
        universe = {s for i in scope if i in registry for s in registry[i].section_ids}
        for sid in ex.get("splits") or []:
            for kind, label, blk, m in _cut_places(registry, graph, scope, universe, sid):
                if kind == "confluence":
                    head = label.split("\u2192")[0].strip().lower()
                    got = _tribs.confluence_joiners(graph, blk, m, names=(head,)) if head else ()
                    got = got or _tribs.confluence_joiners(graph, blk, m)
                elif "confluence" in label.lower() and " near " in f" {label.lower()} ":
                    near = _tribs.confluence_joiners(graph, blk, m,
                                                     near=_tribs.CONFLUENCE_NEAR_M)
                    got = frozenset(j for j in near
                                    if (graph.nodes[j].display_name or "").strip()
                                    and graph.nodes[j].display_name.strip().lower()
                                    in label.lower())
                else:
                    continue
                by_name: dict[str, set[str]] = {}
                for j in got:
                    by_name.setdefault((graph.nodes[j].display_name or "").strip(), set()).add(j)
                for water, mouths in sorted(by_name.items()):
                    if (sid, water) in done:
                        continue
                    done.add((sid, water))
                    included = _rule_includes(words, water)
                    negated = not included and _rule_excludes(words, water)
                    own = own_rows(registry, owned, mouths, entry.get("entry_id"))
                    with_cut = bool(
                        not included and not negated and not own
                        and _tribs.CONFLUENCE_WATER_WITHOUT_A_ROW_GOES_WITH_THE_CUT)
                    subtree = sorted(_tribs.subtree(graph, mouths))
                    rows.append({"split": sid, "water": water or "(unnamed)",
                                 "mouths": sorted(mouths), "included": included,
                                 "negated": negated, "own_row": own, "with_cut": with_cut,
                                 "signs_below": _signs_below(words, water),
                                 "signs_above": _signs_above(words, water),
                                 "blk": blk, "m": m, "subtree": subtree,
                                 "sections": [] if (included or with_cut) else subtree})
    return rows


_OWN_INDEX: dict = {}


def own_rows(registry, owned, mouths, entry_id=None) -> tuple[str, ...]:
    """The table rows OF ITS OWN the water at `mouths` has: the entries in `owned`
    (`outside.rowed_waters`) matching a registry item that holds one of the mouths, other than
    `entry_id` (the cut's own row) and any row whose `see` pointer sends part of its water back to
    `entry_id` ("Downstream of Hwy 20: see Atnarko/Bella Coola Rivers" — Young Creek at the
    Atnarko is the Atnarko row's). Empty when `owned` is None. An `owned` value may list bare
    entry ids (no pointers)."""
    if not owned:
        return ()
    key = (id(registry), id(owned))
    idx = _OWN_INDEX.get(key)
    if idx is None or idx[0] is not registry or idx[1] is not owned:
        by_sec: dict[str, set[str]] = {}
        for item in owned:
            for sec in (registry[item].section_ids if item in registry else ()):
                by_sec.setdefault(sec, set()).add(item)
        if len(_OWN_INDEX) > 8:
            _OWN_INDEX.clear()
        idx = (registry, owned, by_sec)
        _OWN_INDEX[key] = idx
    items = {i for m in mouths for i in idx[2].get(m, ())}
    got: set[str] = set()
    for i in items:
        for row in owned[i]:
            eid, sees = (row, ()) if isinstance(row, str) else row
            if eid != entry_id and entry_id not in sees:
                got.add(eid)
    return tuple(sorted(got))


def _rule_excludes(words: str, water: str) -> bool:
    """Do the rule's words keep the joining water OUT — "…, but not including the Muchalat or Heber
    Rivers"? Matched on the water's first word anywhere in the phrase after "not including"."""
    import re
    first = (water or "").split(" ")[0]
    if not first:
        return False
    for m in re.finditer(r"\bnot\s+includ\w*\s+([^.;]{0,80})", words, re.IGNORECASE):
        if re.search(r"\b" + re.escape(first) + r"\b", m.group(1), re.IGNORECASE):
            return True
    return False


def _signs_above(words: str, water: str) -> bool:
    """The mirror of `_signs_below`: signs the words put ABOVE the joining water's confluence
    ("signs located upstream of the X River confluence, and downstream to …") put the confluence
    inside a reach running DOWN from them, and the joining water is walked (user ruling 2026-09-30:
    signs up- or downstream of a confluence mean the joining water is included)."""
    import re
    first = (water or "").split(" ")[0]
    if not first:
        return False
    return bool(re.search(
        r"\bsigns?\b[^.;]{0,60}?\b(?:upstream|above)\s+(?:of\s+)?(?:the\s+)?"
        r"(?:confluence\s+of\s+(?:the\s+)?[^.;]{0,40}?\band\s+)?" + re.escape(first) + r"\b",
        words, re.IGNORECASE))


def _signs_below(words: str, water: str) -> bool:
    """Do the rule's words put the boundary signs BELOW the joining water's confluence — "from white
    triangular fishing boundary signs located downstream of the Meziadin River confluence, and
    upstream to the Hwy 37 bridge" (Nass River)? Then the curated cut sits on the confluence only
    for want of a sign position: the confluence is INSIDE a reach running up from the signs, and
    the joining water is walked like any tributary (the Slesse principle, "signs 100 m downstream
    of the confluence of the Chilliwack River and Slesse Creek"). "Signs … downstream near the
    confluence of Mobbs Creek" (Lardeau) says only where the signs stand, and is not this."""
    import re
    first = (water or "").split(" ")[0]
    if not first:
        return False
    return bool(re.search(
        r"\bsigns?\b[^.;]{0,60}?\b(?:downstream|below)\s+(?:of\s+)?(?:the\s+)?"
        r"(?:confluence\s+of\s+(?:the\s+)?[^.;]{0,40}?\band\s+)?" + re.escape(first) + r"\b",
        words, re.IGNORECASE))


def _rule_includes(words: str, water: str) -> bool:
    """Do the rule's words take the joining water in — "including Macleod Creek", "upstream of and
    including Hemmingsen Creek", "(including Cameron Cr.)"? Matched on the water's first word, which
    is what the book abbreviates least ("Cameron Cr.", "North White River")."""
    import re
    first = (water or "").split(" ")[0]
    if not first:
        return False
    for m in re.finditer(r"\binclud\w*\s+(?:the\s+)?" + re.escape(first) + r"\b", words,
                         re.IGNORECASE):
        # "…, but not including the Muchalat or Heber Rivers" (Gold River) says the opposite.
        if not re.search(r"\bnot\s*$", words[max(0, m.start() - 8):m.start()], re.IGNORECASE):
            return True
    return False


def _cut_places(registry, graph, scope, universe, split_id: str):
    """(boundary kind, label, blk, measure) for every place split `split_id` cuts the scoped water:
    the node bounds that carry it, by the registry boundary's ref (as `extent._cut_at` finds it)."""
    want = {split_id, f"split:{split_id}"}
    refs: dict[str, tuple[str, str]] = {}
    for i in scope:
        it = registry.get(i) if hasattr(registry, "get") else (registry[i] if i in registry else None)
        for b in (it.boundaries if it else ()):
            if b.id == split_id or (set(b.aliases or ()) & want):
                for r in {b.ref, f"split:{b.id}"} | set(b.aliases or ()):
                    if r:
                        refs.setdefault(r, (b.kind, b.label or ""))
    out: set[tuple[str, str, str, float]] = set()
    for nid in universe:
        n = graph.nodes.get(nid)
        if n is None or not n.blk:
            continue
        for bd in (n.lower_bound, n.upper_bound):
            if bd is not None and bd.boundary_id in refs and bd.route_measure is not None:
                kind, label = refs[bd.boundary_id]
                out.add((kind, label, n.blk, round(bd.route_measure, 3)))
    return sorted(out)


def _runs_up_from(graph, reach, row) -> bool:
    """Does `reach` go on UP the cut's line from the confluence at `row` (blk, m)?"""
    for s in reach:
        n = graph.nodes.get(s)
        if n is not None and n.blk == row.get("blk") and n.down_m >= row["m"] - 1.0:
            return True
    return False


def _runs_down_from(graph, reach, row) -> bool:
    """Does `reach` go on DOWN the cut's line from the confluence at `row` (blk, m)?"""
    for s in reach:
        n = graph.nodes.get(s)
        if n is not None and n.blk == row.get("blk") and n.up_m <= row["m"] + 1.0:
            return True
    return False


def _expander(graph, registry, covered, rule, entry, *, window=None, region=None, owned=None):
    """A closure that expands one rule's reach to its tributaries.

    The rule's `tributary_excludes` are resolved to sections and passed as BLOCKED, so each
    removes the named stream *and everything above it*. Blocking during the walk rather than
    subtracting afterwards also stops the walk descending through excluded water into
    catchments that drain only through it.

    `region` holds a REGIONAL ROW's walk to its region(s), applied to what the walk returns — the
    walk is what leaves the region, as it is what leaves a `within_area` (`outside.region_limit`).
    """
    detail, excluded = resolve_carve_outs(entry, rule, registry, graph, covered)
    passed = carve_out_passed(detail)
    # The water joining at a confluence cut, and everything above it (`confluence_excludes`). Never
    # the reach itself: a braid can make a piece of it an ancestor of the joining mouth.
    # Computed on the first walk only: a joining river's subtree can be most of a basin (the
    # Thompson at the Fraser), and most rules never walk.

    def expand(reach, *, only=False):
        if expand.confluence is None:
            rows = confluence_excludes(entry, rule, registry, graph, covered, owned=owned)
            # A joining water whose mouth is IN the reach is the reach going on under another
            # name (the Atnarko's closure runs on up the South Atnarko to Tenas Lake), not a
            # water the cut names to end it.
            for row in rows:
                if set(row["mouths"]) & set(reach):
                    row.update(sections=[], in_reach=True, with_cut=False)
                elif (row.get("signs_below") and _runs_up_from(graph, reach, row)) or (
                        row.get("signs_above") and _runs_down_from(graph, reach, row)):
                    # ...and one whose confluence the rule's words put inside the reach (signs
                    # BELOW it, the reach running up from them: `_signs_below`; or ABOVE it, the
                    # reach running down: `_signs_above`) is walked, whichever piece FWA hung
                    # its mouth on.
                    row.update(sections=[], in_reach=True, with_cut=True)
            expand.confluence = rows
        joining = {s for row in expand.confluence for s in row["sections"]}
        blocked = excluded | (joining - set(reach))
        got = _tribs.expand(graph, reach, only=only, excluded=blocked, passed=passed,
                            window=window)
        # A JOINING WATER THAT GOES WITH THE CUT (no row of its own, or signs that put it inside):
        # its subtree joins the walk on whichever side the rule is — the walk alone would find it
        # on one side only, the piece FWA hung its mouth on. Streams only, like every walk
        # (`tributaries.expand`); a carve-out's water stays out.
        with_cut: set[str] = set()
        for row in expand.confluence:
            if row.get("with_cut"):
                add = {s for s in row["subtree"]
                       if s not in blocked and s not in passed
                       and (not _tribs.STREAMS_ONLY
                            or ((n := graph.nodes.get(s)) is not None
                                and n.kind == _NodeKind.stream))}
                row["joined"] = len(add)
                with_cut |= add
        got = frozenset(got | with_cut)
        if region is None:
            return got
        kept = {s for s in got if s in region}
        expand.out_of_region += len(got) - len(kept)
        return kept

    expand.out_of_region = 0
    expand.confluence = None
    return expand


def entry_scope(e: dict, covered: list[str], registry, graph):
    """The stretch the ENTRY is about, or (None, []) when it is about the whole water.

    The synopsis qualifies a row in its NAME — "FRASER RIVER (upstream of the CPR Bridge
    at Mission)" — and the rules inside almost never restate it. Returns
    ``(sections, failed)``, `failed` the scope extents that did not resolve; a scope that cannot
    be resolved is REPORTED, never treated as "do not clip", which would silently widen a
    regional row to the entire river.

    A REGIONAL ROW'S REGION is applied by `build_reach`, not here: to the extents that mean "this
    row's water" (`_row_water`), whether or not the row states a scope. The Fraser's four regional
    rows each bound all 251 Fraser sections before it.

    Public because the review app clips with it: two copies of "which stretch is this row" is
    how the app and the bundle came to disagree once already (AGENTS 16).
    """
    out: set[str] = set()
    failed: list[dict] = []
    for sc in e.get("extents") or []:
        got = _resolve.resolve_extent(registry, graph, covered, sc)
        if got is None:
            failed.append(sc)
            continue
        out |= set(got.get("sections") or ())
    return (out or None), failed


def scope_unclassified(e: dict, covered: list[str], registry, graph, clip) -> list[str]:
    """The pieces the entry's scope extents report as unplaceable (straddling), minus any its
    scope placed anyway — what `entry_scope` silently leaves out of every rule of the row."""
    out: set[str] = set()
    for sc in e.get("extents") or []:
        if not isinstance(sc, dict) or sc.get("op") in ("whole", "within"):
            continue
        got = _resolve.resolve_extent(registry, graph, covered, sc)
        if got is not None:
            out |= set(got.get("unclassified") or ())
    return sorted(out - set(clip or ()))


def _row_water(ex: dict, matched=()) -> bool:
    """Does this extent reach as far as "the row's own water" goes — so that a regional row's
    region (`outside.region_limit`) must say where it stops?

    Two shapes do: a `whole` of the row's own water (no item, or only items the row matched —
    Region 2's "No Fishing for steelhead, Fraser River mainstem" names the Fraser it matched), and a
    one-sided cut (`upstream_of`, `downstream_of`), which runs to the end of the water. The Fraser's regional rows are matched to
    the whole river: Region 5's `whole` must mean the Region 5 stretch, and Region 3's "exempt from
    spring closure upstream of the Thompson River" must stop where Region 3 does, not run up the
    Fraser through Regions 5 and 7.

    A reach the book bounds at BOTH ends (`between`), a `whole` of ANOTHER item, and an area are
    where the book put them, even across a region line: the Region 7 Stellako row's fly-only reach
    lies between two signs below the François Lake bridge, inside Region 6's polygon, and holding
    it to Region 7 erased it. A `within_area` already says where it stops.
    """
    if not isinstance(ex, dict) or ex.get("within_area"):
        return False
    op = ex.get("op")
    if op == "whole":
        named = ([ex["item_id"]] if ex.get("item_id") else []) + list(ex.get("item_ids") or [])
        return all(i in set(matched or ()) for i in named)
    return op in ("upstream_of", "downstream_of")


def split_parent_refs(entries, registry, graph) -> list[str]:
    """Every place a record names a lake that is cut into parts, in this build (see
    `pipeline.atlas.waters.added_lakes.split_parents`). Empty = clean."""
    from pipeline.atlas.waters.added_lakes.split_parents import refs_to_parents, split_parents
    return refs_to_parents(entries, split_parents(registry))


def _area_scope(entry: dict) -> bool:
    """Is the entry's scope an AREA — every scope extent a `within` (a zone chapter's region)?"""
    ex = entry.get("extents") or []
    return bool(ex) and all(isinstance(x, dict) and x.get("op") == "within" for x in ex)


def _clip(got: dict, clip: set[str], area_scope: bool = False) -> dict:
    """Cut a resolved reach down to the entry's scope.

    An empty result is kept as an empty reach rather than turned into None: "this rule
    selects nothing inside this row's stretch" is a real answer, and `classify` turns it
    into `empty_after_scope` rather than confusing it with an unresolvable extent.

    A walk `seed` (an extent limited by `within_area`, `classify.WALK_BEFORE_AREA`) is cut to a
    REACH scope — a row about part of a river walks from that part — but not to an AREA scope: a
    zone chapter's region is the same kind of limit as `within_area`, so it joins it and is
    applied after the walk. Clipping the seed to Region 6 is what emptied "lake trout from the
    Fraser watershed" in the Region 6 table.
    """
    out = {
        **got,
        "sections": [s for s in got.get("sections") or () if s in clip],
        "unclassified": [s for s in got.get("unclassified") or () if s in clip],
    }
    if "seed" in got:
        if area_scope:
            out["within_area"] = [s for s in got.get("within_area") or () if s in clip]
        else:
            out["seed"] = [s for s in got["seed"] if s in clip]
    return out
