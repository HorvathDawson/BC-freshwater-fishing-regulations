"""The pass: entries + one build's (registry, graph) → bindings, diagnostics, a report.

Orchestration only — every policy decision is in `classify.py`, and all resolution is
`pipeline.reach.extent`. This module's job is to be *total* (every rule accounted
for) and *deterministic* (sorted throughout), because everything downstream inherits
both.

Measured: the full corpus resolves in ~42 s, 5 s of which is loading the graph. There is
no performance problem here — do not add parallelism.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from pipeline.reach.covered import covered_ids as _covered_ids, make_matcher
from pipeline.reach import tributaries as _tribs
from pipeline.reach.classify import classify, wants_tributaries
from pipeline.reach.models import (
    BuildReport, Diagnostic, Outcome, RuleBinding, iter_entries,
)
from pipeline.reach import extent as _resolve


@dataclass
class ReachResult:
    bindings: list[RuleBinding]
    diagnostics: list[Diagnostic]
    report: BuildReport


def build_reaches(entries, registry, graph, *, build: str = "", covered_fn=None,
                  overrides_path="__default__") -> ReachResult:
    """Resolve every rule in `entries` against one build.

    `entries` is an iterable of ``(region, entry_dict)`` — the shape
    `pipeline.parsing.io.read_entries_dir` and the review app both produce.

    Covered items come from `pipeline.reach.covered` — `entry.matched` when present, else
    a live re-match with the build's own matcher and overrides. That is exactly what the
    review app does, so the bundle and the app never disagree about which water a rule is
    about. Pass `covered_fn(entry, registry)` to override.
    """
    t0 = time.time()
    match = make_matcher(registry, overrides_path)
    bindings: list[RuleBinding] = []
    diagnostics: list[Diagnostic] = []
    report = BuildReport(build=build)

    for e in sorted(iter_entries(entries), key=lambda x: x["entry_id"]):
        report.n_entries += 1
        entry_id = e["entry_id"]
        covered = (covered_fn(e, registry) if covered_fn
                   else _covered_ids(e, registry, match))
        has_registry = bool(covered)
        if has_registry and not (e.get("matched") or []):
            # Resolved only via the live re-match. Nothing is broken, but the entry file
            # is stale; `pipeline.parsing.backfill_matched` stamps it permanently.
            report.needs_backfill.append(entry_id)

        clip, scope_failed = _scope_sections(e, covered, registry, graph)
        if scope_failed:
            report.scope_unresolved.append(entry_id)

        for rule in e.get("rules") or []:
            report.n_rules += 1
            per: list[dict | None] = []
            clipped = False
            for ex in rule.get("extents") or []:
                got = _resolve.resolve_extent(registry, graph, covered, ex)
                if got is not None and clip is not None:
                    before = len(got.get("sections") or ())
                    got = _clip(got, clip)
                    clipped = clipped or len(got["sections"]) < before
                per.append(got)

            binding, diags = classify(
                entry_id, rule, per,
                registry=registry, covered_ids=covered,
                scope_clipped=clipped, entry_has_registry=has_registry,
                tributaries=wants_tributaries(rule, e),
                tributaries_only=bool(rule.get("tributaries_only")),
                expand_tributaries=_expander(graph, registry, covered, rule, e),
            )
            bindings.append(binding)
            diagnostics.extend(diags)

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
    return ReachResult(bindings, diagnostics, report)


def _expander(graph, registry, covered, rule, entry):
    """A closure that expands one rule's reach to its tributaries.

    Carve-outs come from TWO places and both must apply:

    * ``entry.tributaries.excludes`` — the row-level EXCEPT, e.g. "ATNARKO/BELLA COOLA
      RIVERS [Includes Tributaries] EXCEPT: Burnt Bridge Cr. upstream of Sitkatapa Cr.,
      Hunlen Cr. upstream of Hunlen Falls, Young Cr. upstream of Hwy 20". It qualifies
      every rule in the row, exactly as `entry.scope` does.
    * ``rule.tributary_excludes`` — a carve-out on one rule only.

    Each is resolved to sections and passed as BLOCKED, so it removes the named stream
    *and everything above it*. Blocking during the walk rather than subtracting afterwards
    also stops the walk descending through excluded water into catchments that drain only
    through it.
    """
    excluded: set[str] = set()
    carve_outs = list((entry.get("tributaries") or {}).get("excludes") or [])
    carve_outs += list(rule.get("tributary_excludes") or [])
    for ex in carve_outs:
        got = _resolve.resolve_extent(registry, graph, covered, ex)
        if got is not None:
            secs = set(got.get("sections") or ())
            excluded |= secs
            excluded |= _tribs.tributaries_of_reach(graph, secs)

    def expand(reach, *, only=False):
        return _tribs.expand(graph, reach, only=only, excluded=excluded)

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
    for sc in e.get("scope") or []:
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
