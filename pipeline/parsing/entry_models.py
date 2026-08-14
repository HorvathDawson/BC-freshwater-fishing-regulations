"""Frozen-parse data models — the rebuilt parser's output shape (DESIGN-regs-to-sections.md §4).

This is the NEW curation surface that replaces the flat `location_text` of the Gemini parser
(`models.py`). A rule binds to its reach by **`extents` (a list of `op + split ids`, unioned)** — a
*constrained selection* over the waterbody's curated split ids, not blind geometry extraction. The
file is frozen and checked in (`pipeline/parsing/entries/region-N.json`); re-parsing is a reviewed
merge, never an overwrite.

Shape:
    EntryFile{ region, entries:[ Entry{ identity, regs_verbatim, matched, includes_tributaries,
                                        scope:[Extent], rules:[ Rule{ extents:[Extent], … } ] } ] }

The anti-hallucination chain-of-custody validators from `models.py` are preserved:
    rule_text ⊆ regs_verbatim   ·   location_text ⊆ rule_text   ·   each date ⊆ rule_text
Split-id existence is NOT checked here (the model can't know the allowed ids); the ingest step
validates each `extent.splits` against the waterbody's `splits.json` — see `validate_entry_splits`.
"""

from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


# ---------------------------------------------------------------------------
# Shared enum + verbatim-validation normalizers (self-contained; the Gemini
# parser's models.py is being retired)
# ---------------------------------------------------------------------------


class RestrictionType(str, Enum):
    """Classification of a fishing regulation restriction."""

    CLOSURE = "closure"
    HARVEST = "harvest"
    GEAR_RESTRICTION = "gear_restriction"
    VESSEL_RESTRICTION = "vessel_restriction"
    LICENSING = "licensing"
    NOTE = "note"


def _normalize(text: str) -> str:
    """Normalize for flexible substring checks: strip bold markers, collapse whitespace, lowercase.
    Validation only — never mutates stored data."""
    if not text:
        return ""
    return " ".join(text.replace("**", "").replace("\n", " ").split()).lower()


def _normalize_date(text: str) -> str:
    """Like _normalize but also strips commas/periods (date comparison)."""
    return _normalize(text).replace(",", "").replace(".", "")


# ---------------------------------------------------------------------------
# Extent — the op + split binding
# ---------------------------------------------------------------------------


class Op(str, Enum):
    """How a rule/scope selects sections from the matched item's reach."""

    WHOLE = "whole"                 # the whole matched item (no split refs)
    UPSTREAM_OF = "upstream_of"     # sections above split s        (1 split)
    DOWNSTREAM_OF = "downstream_of" # sections below split s        (1 split)
    BETWEEN = "between"             # sections between a and b       (2 splits)
    WITHIN = "within"              # sections inside an area/polygon (area, not splits)


class Extent(BaseModel):
    """One `op + split ids` binding. A rule's `extents` is a list → UNION (covers "A plus B").

    `item` scopes this extent to a *different* registry item than the entry's `matched`
    (covers "…plus Tenas Lake" or a named side channel). `area`/`kind` carry a `within(area)`.
    """

    model_config = ConfigDict(frozen=True)

    op: Op
    splits: List[str] = Field(default_factory=list, description="curated split ids this extent binds to")
    item: Optional[str] = Field(default=None, description="registry id, if this extent scopes a different item")
    area: Optional[str] = Field(default=None, description="area/park name or registry id (op=within)")
    kind: Optional[str] = Field(default=None, description="admin feature kind (op=within), e.g. 'park'")

    @model_validator(mode="after")
    def _check_arity(self) -> "Extent":
        n = len(self.splits)
        if self.op in (Op.UPSTREAM_OF, Op.DOWNSTREAM_OF) and n != 1:
            raise ValueError(f"op {self.op.value} needs exactly 1 split id, got {n}")
        if self.op == Op.BETWEEN and n != 2:
            raise ValueError(f"op between needs exactly 2 split ids, got {n}")
        if self.op == Op.WHOLE and n != 0:
            raise ValueError(f"op whole takes no split ids, got {n}")
        if self.op == Op.WITHIN and not (self.area or self.splits):
            raise ValueError("op within needs an area (or bounding split ids)")
        return self


# ---------------------------------------------------------------------------
# Rule
# ---------------------------------------------------------------------------


class Rule(BaseModel):
    """A single parsed restriction, bound to its reach by `extents` (op + split ids)."""

    model_config = ConfigDict(frozen=True)

    rule_id: str = Field(..., description="stable id, unique within the entry (e.g. 'atnarko_main.r5')")
    restriction_type: RestrictionType
    details: str = Field(..., description="concise normalized summary, e.g. 'No powered boats'")
    extents: List[Extent] = Field(
        default_factory=list,
        description="op+split bindings, unioned. Empty ONLY when needs_review or sections_override is set; "
        "a whole-reach rule must be explicit: extents=[{op:'whole'}].",
    )
    dates: List[str] = Field(default_factory=list, description="date strings exactly as in rule_text")
    includes_tributaries: Optional[bool] = Field(
        default=None,
        description="per-rule tributary override (null=inherit entry, true/false=override for this rule)",
    )
    sections_override: Optional[List[str]] = Field(
        default=None,
        description="escape hatch: name section ids directly when no split can express the reach",
    )
    needs_review: bool = Field(
        default=False,
        description="parser could not confidently bind this rule -> hand-curation queue",
    )
    review_reason: str = Field(default="", description="why it needs review (required iff needs_review)")
    # verbatim provenance (self-contained; never needs the ephemeral extraction to curate)
    rule_text: str = Field(..., description="exact contiguous substring of regs_verbatim for this rule")
    location_text: str = Field(default="", description="verbatim phrase the extents came from ('upstream of X')")
    exception: str = Field(default="", description="verbatim carve-out qualifying THIS rule (⊆ rule_text)")

    @model_validator(mode="after")
    def _validate_chain(self) -> "Rule":
        errors: List[str] = []
        if not self.rule_text or not self.rule_text.strip():
            raise ValueError("rule_text is empty")
        if not self.details or not self.details.strip():
            errors.append("details is empty")

        for field_name in ("rule_text", "location_text", "exception"):
            val = getattr(self, field_name)
            if val and "..." in val:
                errors.append(f"{field_name} contains '...' — verbatim fields must not be truncated")

        if self.location_text and _normalize(self.location_text) not in _normalize(self.rule_text):
            errors.append(f"location_text not found in rule_text. Location: '{self.location_text[:80]}'")
        if self.exception and _normalize(self.exception) not in _normalize(self.rule_text):
            errors.append(f"exception not found in rule_text. Exception: '{self.exception[:80]}'")

        for date in self.dates:
            if "\n" in date or "*" in date:
                errors.append(f"Date '{date}' contains newlines/asterisks")
            elif _normalize_date(date) not in _normalize_date(self.rule_text):
                errors.append(f"Date '{date}' not found in rule_text. Rule: '{self.rule_text[:100]}'")

        # binding completeness: a confidently-parsed rule must express its reach somehow
        if not self.needs_review and not self.extents and not self.sections_override:
            errors.append("rule has no extents and no sections_override but needs_review is False "
                          "— a whole-reach rule must set extents=[{op:'whole'}]")
        if self.needs_review and not self.review_reason.strip():
            errors.append("needs_review is True but review_reason is empty")

        if errors:
            raise ValueError("; ".join(errors))
        return self


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------


class Identity(BaseModel):
    """Who the entry is about — the matcher uses (name, region, mus) to find the registry item."""

    model_config = ConfigDict(frozen=True)
    name: str = Field(..., description="waterbody name verbatim from the synopsis row")
    region: str = Field(default="", description="management region, e.g. '5'")
    mus: List[str] = Field(default_factory=list, description="management units, e.g. ['5-4']")


class Tributaries(BaseModel):
    """Entry-wide tributary scope, grouped. `excludes` are HAND-CURATED carve-outs subtracted from
    the tributary set (e.g. 'EXCEPT Burnt Bridge upstream of Sitkatapa'); the parser leaves them
    empty. An exclude extent just references the boundary split(s) — the excepted item is inferred
    from the split's own scope, so no `item` is needed."""

    model_config = ConfigDict(frozen=True)
    included: bool = Field(default=False, description="do this entry's rules extend to tributaries?")
    only: bool = Field(default=False, description="entry governs ONLY tributaries (e.g. \"X LAKE'S TRIBUTARIES\")")
    excludes: List[Extent] = Field(default_factory=list, description="carve-outs subtracted from the tributary set")

    @model_validator(mode="before")
    @classmethod
    def _only_implies_included(cls, data):
        if isinstance(data, dict) and data.get("only") and not data.get("included"):
            data = dict(data); data["included"] = True
        return data


class Entry(BaseModel):
    """One frozen parse for a synopsis row: identity + rules. Both the parser's output and the
    file a curator hand-edits (fix an extent, add a sections_override, correct a matched)."""

    model_config = ConfigDict(frozen=True)

    entry_id: str = Field(..., description="stable id (e.g. 'atnarko_main'); rule_ids namespace under it")
    identity: Identity
    regs_verbatim: str = Field(..., description="exact copy of the input raw_regs")
    matched: List[str] = Field(default_factory=list, description="registry ids — written by the matcher, [] from the parser")
    tributaries: Tributaries = Field(default_factory=Tributaries, description="entry-wide tributary scope (included/only/excludes)")
    scope: List[Extent] = Field(default_factory=list, description="entry-wide extents; composes by ∩ with each rule's extents")
    rules: List[Rule] = Field(..., min_length=1)
    audit_log: List[str] = Field(default_factory=list, description="parsing ambiguities / source issues only")

    @model_validator(mode="after")
    def _validate_entry(self) -> "Entry":
        errors: List[str] = []
        if not self.regs_verbatim or not self.regs_verbatim.strip():
            raise ValueError("regs_verbatim is empty")

        regs_norm = _normalize(self.regs_verbatim)
        for i, rule in enumerate(self.rules):
            if _normalize(rule.rule_text) not in regs_norm:
                errors.append(f"rule[{i}].rule_text not found in regs_verbatim. Rule: '{rule.rule_text[:80]}'")

        # rule_ids unique within the entry
        rids = [r.rule_id for r in self.rules]
        dupes = {r for r in rids if rids.count(r) > 1}
        if dupes:
            errors.append(f"duplicate rule_id(s): {sorted(dupes)}")

        # keyword coverage — a known restriction phrase in the source must appear in some rule
        _KEYWORDS = ["no fishing", "class i water", "class ii water", "bait ban", "fly fishing only",
                     "catch and release", "no powered boats", "single barbless hook", "daily quota",
                     "steelhead stamp mandatory"]
        all_rules_norm = " \n ".join(_normalize(r.rule_text) for r in self.rules)
        for kw in _KEYWORDS:
            if kw in regs_norm and kw not in all_rules_norm:
                errors.append(f"Keyword '{kw}' in regs_verbatim but missing from all rule_text — a restriction may be missed")

        if errors:
            raise ValueError("; ".join(errors))
        return self


# ---------------------------------------------------------------------------
# EntryFile — the per-region checked-in artifact
# ---------------------------------------------------------------------------


class EntryFile(BaseModel):
    """The frozen per-region parse: pipeline/parsing/entries/region-N.json."""

    region: str
    entries: List[Entry]

    @model_validator(mode="after")
    def _unique_entry_ids(self) -> "EntryFile":
        ids = [e.entry_id for e in self.entries]
        dupes = {i for i in ids if ids.count(i) > 1}
        if dupes:
            raise ValueError(f"duplicate entry_id(s) in region {self.region}: {sorted(dupes)}")
        return self


# ---------------------------------------------------------------------------
# Ingest-time validation the models can't do alone
# ---------------------------------------------------------------------------


def validate_entry_splits(entry: Entry, allowed_split_ids: set[str]) -> List[str]:
    """Every id referenced by an extent (entry scope + each rule) must exist in the waterbody's
    curated splits. Returns error strings (empty = clean). `sections_override` is NOT checked here
    (section ids only exist after the graph build)."""
    errors: List[str] = []

    def _check(extents: List[Extent], where: str) -> None:
        for ex in extents:
            for sid in ex.splits:
                if sid not in allowed_split_ids:
                    errors.append(f"{where}: extent op={ex.op.value} references unknown split id '{sid}'")

    _check(entry.scope, f"entry {entry.entry_id} scope")
    _check(entry.tributaries.excludes, f"entry {entry.entry_id} tributaries.excludes")
    for rule in entry.rules:
        _check(rule.extents, f"entry {entry.entry_id} rule {rule.rule_id}")
    return errors
