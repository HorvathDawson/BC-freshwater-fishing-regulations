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

from pipeline.atlas.reach.covered import covered_ids as _covered_ids, make_matcher
from pipeline.atlas.graph import tributaries as _tribs
from pipeline.atlas.reach.classify import classify, wants_tributaries
from pipeline.atlas.reach.models import (
    BuildReport, Diagnostic, Outcome, RuleBinding, iter_entries,
)
from pipeline.atlas.reach import extent as _resolve
from pipeline.regs.parsing.catalogue import Designation
from pipeline.atlas.reach.licensing import (
    PLACED_KINDS, LicensingPlacement, as_rule, carve_out_orphans, carve_outs_to_owner,
    own_beats_inherited, place_record,
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
                  covered_fn=None, overrides_path="__default__") -> ReachResult:
    """Resolve every rule in `entries` against one build.

    `entries` is an iterable of ``(region, entry_dict)`` — the shape
    `pipeline.regs.parsing.io.read_entries_dir` and the review app both produce.

    Covered items come from `pipeline.atlas.reach.covered` — `entry.matched` when present, else
    a live re-match with the build's own matcher and overrides. That is exactly what the
    review app does, so the bundle and the app never disagree about which water a rule is
    about. Pass `covered_fn(entry, registry)` to override.
    """
    t0 = time.time()
    match = make_matcher(registry, overrides_path)
    bindings: list[RuleBinding] = []
    diagnostics: list[Diagnostic] = []
    licensing: list[LicensingPlacement] = []
    lic_diags: list[Diagnostic] = []
    #: item -> entries covering it with `includes_tributaries: true`, and each carving
    #: designation's removed sections — for `carve_out_orphans`.
    claims: dict[str, list[str]] = {}
    carved: dict[tuple[str, str], tuple[set[str], set[str]]] = {}
    designations: dict = {}
    report = BuildReport(build=build, handles=handles)

    for e in sorted(iter_entries(entries), key=lambda x: x["entry_id"]):
        report.n_entries += 1
        entry_id = e["entry_id"]
        covered = (covered_fn(e, registry) if covered_fn
                   else _covered_ids(e, registry, match))
        has_registry = bool(covered)
        if has_registry and not (e.get("matched") or []):
            # Resolved only via the live re-match. Nothing is broken, but the entry file
            # is stale; `pipeline.regs.parsing.backfill_matched` stamps it permanently.
            report.needs_backfill.append(entry_id)

        clip, scope_failed = _scope_sections(e, covered, registry, graph)
        if scope_failed:
            report.scope_unresolved.append(entry_id)

        for rule in e.get("rules") or []:
            report.n_rules += 1
            binding, diags = build_reach(e, rule, registry, graph,
                                         covered=covered, clip=clip, match=match)
            bindings.append(binding)
            diagnostics.extend(diags)

        if e.get("includes_tributaries") is True:
            for i in covered:
                claims.setdefault(i, []).append(entry_id)

        # LICENSING, in the entry's own context: same covered items, same clip, same resolver.
        for rec in e.get("licensing") or []:
            if rec.get("kind") not in PLACED_KINDS:
                continue
            reach = (lambda r, e=e, covered=covered, clip=clip: build_reach(
                e, r, registry, graph, covered=covered, clip=clip, match=match))
            placed, diags = place_record(e, rec, reach)
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
                items = {i for x in rec["tributary_excludes"]
                         for i in ([x.get("item_id")] if x.get("item_id") else [])
                         + list(x.get("item_ids") or [])}
                carved[(entry_id, rec["id"])] = (set(bare.sections) - set(placed.sections),
                                                 items)

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


def build_reach(entry: dict, rule: dict, registry, graph, *, covered=None, clip=None,
                match=None) -> tuple[RuleBinding, list[Diagnostic]]:
    """THE public answer to "what does this rule cover" — resolve, clip, classify, expand.

    One call, so no caller has to remember the order, or that tributaries need expanding.
    The review app got exactly that wrong once: it resolved and classified but never
    expanded, so a curator confirming "including tributaries" was shown the mainstem alone
    and would have signed off on a fraction of the real reach.

    The layers underneath stay separately testable — `extent.resolve_extent`,
    `tributaries.expand`, `classify.classify`. This only removes the chance to skip one.
    """
    if covered is None:
        covered = _covered_ids(entry, registry, match or make_matcher(registry))

    per: list[dict | None] = []
    clipped = False
    window = None
    for ex in rule.get("extents") or []:
        got = _resolve.resolve_extent(registry, graph, covered, ex)
        if got is not None:
            # The measure window the extent actually resolved to, handed straight to the
            # tributary walk so it never has to re-derive where the reach starts.
            window = window or got.get("window")
            if clip is not None:
                before = len(got.get("sections") or ())
                got = _clip(got, clip)
                clipped = clipped or len(got["sections"]) < before
        per.append(got)

    return classify(
        entry["entry_id"], rule, per,
        registry=registry, covered_ids=covered,
        scope_clipped=clipped, entry_has_registry=bool(covered),
        tributaries=wants_tributaries(rule, entry),
        tributaries_only=bool(rule.get("tributaries_only")),
        expand_tributaries=_expander(graph, registry, covered, rule, entry, window=window),
    )


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
        got = _resolve.resolve_extent(registry, graph, covered, ex)
        row = {"extent": ex, "resolved": got is not None,
               "sections": [], "above": 0}
        if got is not None:
            secs = set(got.get("sections") or ())
            above = _tribs.tributaries_of_reach(graph, secs)
            blocked |= secs | above
            row["sections"] = sorted(secs)
            row["above"] = len(above)          # "and everything upstream of it"
        detail.append(row)
    return detail, blocked


def _expander(graph, registry, covered, rule, entry, *, window=None):
    """A closure that expands one rule's reach to its tributaries.

    The rule's `tributary_excludes` are resolved to sections and passed as BLOCKED, so each
    removes the named stream *and everything above it*. Blocking during the walk rather than
    subtracting afterwards also stops the walk descending through excluded water into
    catchments that drain only through it.
    """
    _, excluded = resolve_carve_outs(entry, rule, registry, graph, covered)

    def expand(reach, *, only=False):
        return _tribs.expand(graph, reach, only=only, excluded=excluded, window=window)

    return expand


def _scope_sections(e: dict, covered: list[str], registry, graph):
    """The stretch the ENTRY is about, or (None, False) when it is about the whole water.

    The synopsis qualifies a row in its NAME — "FRASER RIVER (upstream of the CPR Bridge
    at Mission)" — and the rules inside almost never restate it. Returns
    ``(sections, failed)``; a scope that cannot be resolved is REPORTED, never treated as
    "do not clip", which would silently widen a regional row to the entire river.
    """
    out: set[str] = set()
    failed = False
    for sc in e.get("extents") or []:
        got = _resolve.resolve_extent(registry, graph, covered, sc)
        if got is None:
            failed = True
            continue
        out |= set(got.get("sections") or ())
    return (out or None), failed


def _clip(got: dict, clip: set[str]) -> dict:
    """Cut a resolved reach down to the entry's scope.

    An empty result is kept as an empty reach rather than turned into None: "this rule
    selects nothing inside this row's stretch" is a real answer, and `classify` turns it
    into `empty_after_scope` rather than confusing it with an unresolvable extent.
    """
    return {
        **got,
        "sections": [s for s in got.get("sections") or () if s in clip],
        "unclassified": [s for s in got.get("unclassified") or () if s in clip],
    }
