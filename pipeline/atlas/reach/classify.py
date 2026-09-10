"""The decision table: one rule's resolved extents → an outcome and, if it failed, WHY.

EVERY policy decision the reach builder makes lives in this file. That is deliberate —
scattered across the pass they become invisible, and each one is a judgement about
whether a regulation applies somewhere it might not.

`resolve_extent` returns a bare ``None`` for five distinct failures, so the reason is
re-derived here from the extent and the registry. When the resolver grows typed reasons,
`_why` should defer to it and shrink to almost nothing.
"""

from __future__ import annotations

from pipeline.atlas.reach.models import Diagnostic, Outcome, Reason, RuleBinding

# --------------------------------------------------------------------------- #
# Policy — the eight decisions from REACH-BUILDER.md §"open policy"
# --------------------------------------------------------------------------- #

#: NEVER auto-include an `unclassified` piece. Surface it; let a human decide.
#:
#: An earlier version of this file included them for closures, reasoning that
#: over-closing is the safe direction. Measuring the live corpus killed that idea: all
#: **45** unclassified pieces are DETACHED — they have no neighbour inside the item at
#: all — and none is a genuine straddler. Auto-including them added disconnected stubs
#: to closures. On `cowichan_river.r3` ("No fishing, Cowichan Lake outlet to Greendale
#: Trestle") it silently attached a 380 m piece whose only neighbour is Cowichan Lake.
#:
#: That is precisely the silent widening this project treats as a defect (㉗). The
#: builder does not get to decide where a regulation applies; it reports and the curator
#: decides. Doc 10 ㊴ asks for a column, not a policy.
STRADDLERS_INCLUDED_FOR = frozenset()

#: A split id landing at more than one measure on the same blue line has two honest
#: readings, and `_cut_at` silently takes the lower. The Mitchell River bug is proof the
#: lower can be flat wrong (it collapsed a `between` to nothing). We keep the resolver's
#: choice but ALWAYS emit a diagnostic, so it is visible rather than assumed.
#: 63 rules.
AMBIGUOUS_CUT_IS_FATAL = False

#: A rule may carry several extents; the sections are unioned. If SOME resolve and some
#: do not, we bind what resolved and record a diagnostic — dropping the whole rule would
#: under-apply a regulation that is partly known. 9 rules; 0 currently partial.
PARTIAL_EXTENTS_BIND = True


def wants_tributaries(rule: dict, entry: dict) -> bool:
    """Does this rule extend to tributaries?

    Three-valued: `None` on the rule INHERITS `entry.tributaries.included`. Reading only
    the rule's own field undercounts badly — 132 rules set it explicitly, but 554 across
    264 entries are actually in scope once inheritance is applied.
    """
    own = rule.get("includes_tributaries")
    if own is None:
        # The catalogue keeps ONE flag, flat on the entry: "only" moved onto the rule, and a
        # per-rule include/exclude is gone, because two rules on one water disagreeing about
        # what the water IS was never something the book could say. The prose entry nested
        # the same fact under `tributaries.included`.
        own = entry.get("includes_tributaries")
    if own is None:
        own = (entry.get("tributaries") or {}).get("included")
    return bool(own) or bool(rule.get("tributaries_only"))


def classify(
    entry_id: str,
    rule: dict,
    per_extent: list[dict | None],
    *,
    registry,
    covered_ids: list[str],
    scope_clipped: bool,
    entry_has_registry: bool,
    tributaries: bool = False,
    tributaries_only: bool = False,
    expand_tributaries=None,
) -> tuple[RuleBinding, list[Diagnostic]]:
    """Turn one rule's per-extent resolutions into a binding plus its diagnostics.

    `per_extent` is positional against ``rule["extents"]``; ``None`` means that extent
    could not be resolved at all. `scope_clipped` says the entry's own scope removed
    sections that the extent had resolved — which is a different failure from resolving
    to nothing in the first place.
    """
    rid = rule["rule_id"]
    extents = rule.get("extents") or []
    diags: list[Diagnostic] = []

    def unresolved(reason: Reason, detail: str = "") -> tuple[RuleBinding, list[Diagnostic]]:
        return RuleBinding(entry_id, rid, Outcome.unresolved, (), reason, detail,
                           tributaries_pending=tributaries), diags

    # --- nothing was authored -------------------------------------------------
    if not extents:
        if not entry_has_registry:
            return unresolved(Reason.no_registry, "entry has no matched registry item")

        # ⚠️ Do NOT default to `whole` when the rule DESCRIBES A PLACE it could not bind.
        # "500 m upstream and downstream of Causeway Road" with no boundary to bind to is a
        # real, specific location, and widening it to the whole lake arm applies a 500 m
        # closure to kilometres of water.
        #
        # The signal is a LOCATION the rule states and could not resolve — `extent_text` or
        # `unresolved_locators` — and NOT merely the absence of extents. That distinction is
        # new because the corpus is: the prose parser emitted an extent on every rule, so
        # all 12 rules that reached here were the dangerous kind. The catalogue parser puts
        # the reach on the ENTRY and leaves a rule bare when it covers the whole of it, and
        # 1,752 of 1,957 bare rules say nothing about location at all — "Lake trout daily
        # quota = 3" on Atlin Lake. Refusing those binds 73% of the corpus to nothing.
        #
        # The parse prompt states the same default in the other direction: "Every rule needs
        # `extents`. The default is the whole water."
        # NOT defaulted here. `ingest_catalogue` writes `[{"op": "whole"}]` onto a rule that
        # says nothing about location, so the corpus states its own reach and this resolver
        # has one less way to disagree with it. A rule that reaches here now is one that
        # DESCRIBED a place and could not bind it, which is exactly the dangerous kind.
        locs = rule.get("unresolved_locators") or []
        return unresolved(
            Reason.no_extents,
            f"no extent authored; unresolved locators: {locs}" if locs else "no extent authored",
        )

    # --- collect what resolved -------------------------------------------------
    sections: set[str] = set()
    resolved_any = False
    for i, got in enumerate(per_extent):
        if got is None:
            continue
        resolved_any = True
        sections |= set(got.get("sections") or ())

        straddling = got.get("unclassified") or []
        if straddling:
            include = (rule.get("type") or rule.get("restriction_type") or "") \
                in STRADDLERS_INCLUDED_FOR
            if include:
                sections |= set(straddling)
            diags.append(Diagnostic(entry_id, rid, "unclassified", {
                "extent": i, "pieces": sorted(straddling), "included": include,
                "why": "reported for curation; the builder never places these itself",
            }))

        for amb in got.get("ambiguous_cut") or []:
            diags.append(Diagnostic(entry_id, rid, "ambiguous_cut", dict(amb, extent=i)))

    if not resolved_any:
        return unresolved(*_why(extents, registry, covered_ids))

    if len(per_extent) > 1 and any(g is None for g in per_extent):
        diags.append(Diagnostic(entry_id, rid, "partial_extents", {
            "resolved": sum(1 for g in per_extent if g is not None), "total": len(per_extent),
        }))
        if not PARTIAL_EXTENTS_BIND:
            return unresolved(Reason.unknown, "some extents unresolved and partial binding is off")

    # --- resolved, but selects nothing ----------------------------------------
    if not sections:
        if scope_clipped:
            # The Peace case: the rule describes a reach OUTSIDE the row it sits in.
            # The resolver is right; the curation is inconsistent. A real signal.
            return unresolved(Reason.empty_after_scope,
                              "resolved, then the entry scope removed every section")
        return unresolved(*_why(extents, registry, covered_ids))

    via_trib: tuple[str, ...] = ()
    if tributaries:
        if expand_tributaries is None:
            # No graph available (unit tests, or a caller that only wants direct extents).
            # Flag structurally so this can never be mistaken for a complete answer.
            diags.append(Diagnostic(entry_id, rid, "tributaries_pending", {
                "direct_sections": len(sections),
                "why": "rule extends to tributaries but no expander was supplied",
            }))
            return RuleBinding(entry_id, rid, Outcome.bound, tuple(sorted(sections)),
                               tributaries_pending=True), diags

        direct = set(sections)
        sections = set(expand_tributaries(direct, only=tributaries_only))
        # THE INTERSECTION IS APPLIED HERE, after the walk, because the walk is what leaves the
        # area. "Any stream in the Fraser River Watershed OF REGION 5" is a watershed limited to an
        # administrative polygon, and filtering the seed instead would do nothing at all — the seed
        # is already inside the region; it is the tributaries that wander out of it.
        limit: set[str] = set()
        for got in per_extent:
            if got and got.get("within_area"):
                limit |= set(got["within_area"])
        if limit:
            before = len(sections)
            sections &= limit
            diags.append(Diagnostic(entry_id, rid, "within_area", {
                "before": before, "after": len(sections), "removed": before - len(sections),
            }))
            if not sections:
                return unresolved(Reason.no_sections,
                                  "within_area removed every section the walk found — the "
                                  "watershed and the area do not meet")
        # The provenance, taken HERE because this is the only line where the two sets are
        # still apart. `sections - direct` is right for both states: with `only`, the reach
        # is not in `sections` at all, so every section is a tributary one.
        via_trib = tuple(sorted(sections - direct))
        diags.append(Diagnostic(entry_id, rid, "tributaries", {
            "direct": len(direct), "total": len(sections),
            "added": len(sections - direct), "only": tributaries_only,
        }))
        if tributaries_only and not sections:
            # "tributaries only" that finds no tributary selects NOTHING. Shipping that as
            # a bound rule covering zero water would be a silent drop.
            return unresolved(Reason.no_tributaries,
                              f"tributaries_only, but the reach has no tributaries "
                              f"({len(direct)} direct sections)")

    return RuleBinding(entry_id, rid, Outcome.bound, tuple(sorted(sections)),
                       via_tributary=via_trib), diags


def _why(extents: list[dict], registry, covered_ids: list[str]) -> tuple[Reason, str]:
    """Re-derive why every extent came back ``None``.

    The resolver does not say, so this inspects the same inputs it did. Ordered
    most-specific first; `unknown` means the resolver refused for a reason this cannot
    reproduce, which is itself worth surfacing rather than papering over.
    """
    for ex in extents:
        op = ex.get("op")
        if op == "within":
            aid = ex.get("area_id")
            if not aid:
                return Reason.area_scope, "within() with no area_id"
            if aid not in registry:
                return Reason.area_id_dangling, f"area_id {aid!r} is not a registry item"
            return Reason.area_scope, f"within({aid}) is not geometry"

        scope = ex.get("item_ids") or ([ex["item_id"]] if ex.get("item_id") else covered_ids)
        known = [i for i in scope if i in registry]
        if not known:
            return Reason.no_sections_for_items, f"no scoped item is in the registry: {scope}"
        if not any(registry[i].section_ids for i in known):
            return (Reason.no_sections_for_items,
                    f"every scoped item has zero sections: {known}")

        splits = ex.get("splits") or []
        if op in ("upstream_of", "downstream_of", "between") and splits:
            universe = {s for i in known for s in registry[i].section_ids}
            missing = [s for s in splits if not _cut_exists(s, known, registry, universe)]
            if missing:
                return Reason.cut_not_found, f"cut(s) not on the scoped water: {missing}"
            if op == "between" and len(splits) == 2:
                return (Reason.cuts_collapsed,
                        "both cuts resolve but the reach between them is empty — the two "
                        "may have collapsed to one measure (see the Mitchell alias bug)")
    return Reason.unknown, "resolver returned None and the cause could not be re-derived"


def _cut_exists(split_id: str, items: list[str], registry, universe: set[str]) -> bool:
    """Is this split id a boundary on any of the scoped items (directly or by alias)?"""
    want = {split_id, f"split:{split_id}"}
    for i in items:
        it = registry.get(i)
        for b in (it.boundaries if it else ()):
            if b.id == split_id or (set(b.aliases or ()) & want):
                return True
    return False
