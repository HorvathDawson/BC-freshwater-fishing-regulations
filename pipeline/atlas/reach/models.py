"""What the reach builder produces. Pure data — no IO, no resolution logic.

Every table is keyed ``(entry_id, rule_id)``, never ``rule_id`` alone: rule ids are
unique only WITHIN an entry, and 49 collide corpus-wide (both Nation Lakes entries emit
``nation_lakes.r1``). Keying on the id alone silently merges two different rules'
sections, dates and species.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Outcome(str, Enum):
    """Every rule ends as exactly one of these. Never absent — that is the point."""

    bound = "bound"                # resolved to >= 1 section
    unresolved = "unresolved"      # could not be resolved; carries a Reason


class Reason(str, Enum):
    """Why a rule did not bind. A CLOSED set — extend deliberately.

    `resolve_extent` returns a bare ``None`` for several distinct failures, so these are
    derived by `classify` from the extent and the registry rather than reported by the
    resolver. Whenever the resolver grows typed reasons, this should defer to it.
    """

    # --- authored, but nothing to bind to -------------------------------------
    no_registry = "no_registry"                      # entry has no matched item (95 rules)
    no_extents = "no_extents"                        # rule authored with no extent (12 rules)
    no_sections_for_items = "no_sections_for_items"  # every scoped item has 0 sections (14 rules)

    # --- the extent could not be placed ---------------------------------------
    cut_not_found = "cut_not_found"                  # a bound split is not on the scoped water
    cuts_collapsed = "cuts_collapsed"                # between() whose cuts resolve to one measure
    parallel_branches = "parallel_branches"          # between() whose halves do not order
    empty_after_scope = "empty_after_scope"          # resolved, then the entry scope clipped it away

    # --- not geometry at all ---------------------------------------------------
    area_scope = "area_scope"                        # within(area)
    area_id_dangling = "area_id_dangling"            # area_id names nothing

    # --- deliberately deferred -------------------------------------------------
    tributaries_pending = "tributaries_pending"      # direct part bound; no expander supplied
    no_tributaries = "no_tributaries"                # tributaries_only, but there are none

    unknown = "unknown"                              # resolver said None and we could not say why


@dataclass(frozen=True)
class Diagnostic:
    """Something the curator should see, that is not itself a failure.

    Straddling pieces and ambiguous cuts are surfaced by the resolver and would otherwise
    be dropped on the floor between it and the bundle (doc 10 ㊴).
    """

    entry_id: str
    rule_id: str
    kind: str            # "straddling" | "ambiguous_cut"
    payload: dict


@dataclass(frozen=True)
class RuleBinding:
    """One rule's resolution against one build."""

    entry_id: str
    rule_id: str
    outcome: Outcome
    sections: tuple[str, ...] = ()
    reason: Reason | None = None
    detail: str = ""

    #: This rule extends to TRIBUTARIES, and the reach-scoped tributary walk does not
    #: exist yet, so `sections` is the DIRECT part only and is INCOMPLETE.
    #:
    #: Structural rather than a diagnostic on purpose: 554 rules are affected, and without
    #: a field on the binding itself they are indistinguishable from a fully-resolved rule.
    #: Shipping them as complete would silently under-apply a regulation — the ㊳ failure.
    #: Nothing downstream may treat a binding with this set as final.
    tributaries_pending: bool = False

    #: Of `sections`, the ones reached ONLY by the tributary walk — never the reach itself.
    #:
    #: WHY IT IS SEPARATE FROM `sections`. A section knowing WHICH rules cover it is not the
    #: same as knowing WHY, and the why is the difference between "no fishing here" and "no
    #: fishing here, because this creek joins a closed stretch of the Skeena". 567 rules
    #: expand this way, and when one of them is wrong it is wrong over thousands of
    #: kilometres — so the provenance has to survive into the bundle to be checkable at all.
    #:
    #: Empty for the 2,395 rules that do not expand, rather than a copy of `sections`: this
    #: is the exception, and storing the rule twice for the common case buys nothing.
    #: `tributaries_only` makes EVERY section a tributary one, which is exactly right — the
    #: reach is excluded there and the set difference says so without a special case.
    via_tributary: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        # The invariant the whole builder exists to guarantee (doc 10 ⑪ + ㊳):
        # a rule never ends up bound-but-empty, and never unresolved-but-unexplained.
        if self.outcome is Outcome.bound and not self.sections:
            raise ValueError(f"{self.entry_id}:{self.rule_id} bound with zero sections")
        if self.outcome is Outcome.unresolved and self.reason is None:
            raise ValueError(f"{self.entry_id}:{self.rule_id} unresolved with no reason")
        if self.outcome is Outcome.bound and self.reason is not None:
            raise ValueError(f"{self.entry_id}:{self.rule_id} bound but carries a reason")


@dataclass
class BuildReport:
    """What a human reads after a build."""

    build: str = ""
    n_entries: int = 0
    n_rules: int = 0
    outcomes: dict[str, int] = field(default_factory=dict)
    reasons: dict[str, int] = field(default_factory=dict)
    diagnostics: dict[str, int] = field(default_factory=dict)
    scope_unresolved: list[str] = field(default_factory=list)
    #: Rules bound to their DIRECT sections only, pending the tributary walk.
    tributaries_pending: int = 0
    #: entries whose `matched` is empty although the entry names a real item — run
    #: `python -m pipeline.regs.parsing.backfill_matched` rather than working around it.
    needs_backfill: list[str] = field(default_factory=list)
    seconds: float = 0.0

    def total(self) -> int:
        return sum(self.outcomes.values())


def iter_entries(entries):
    """Yield entry dicts from any of the shapes the codebase hands around.

    `pipeline.regs.parsing.io.read_entries_dir` returns ``{entry_id: entry}``; the review app's
    loader returns ``[(region, entry)]``; a caller may pass a plain list. Accepting all
    three here means neither caller has to reshape, and a wrong shape fails loudly instead
    of indexing a string.
    """
    if isinstance(entries, dict):
        entries = entries.values()
    for item in entries:
        if isinstance(item, dict):
            yield item
        elif isinstance(item, (tuple, list)) and len(item) == 2 and isinstance(item[1], dict):
            yield item[1]                      # (region, entry)
        else:
            raise TypeError(f"cannot read an entry from {type(item).__name__}: {item!r:.60}")
