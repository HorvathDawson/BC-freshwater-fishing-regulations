"""
V2 data models for parsed synopsis regulations.

Dramatically simplified from v1:
- No ScopeObject — location_text is a plain verbatim string
- No IdentityObject — match table handles identity
- Rules are flat: text + type + details + location + dates
- Only two scope concepts: includes_tributaries (bool) and tributary_only

Anti-hallucination enforced at every level via verbatim chain validators:
    rule_text ⊆ regs_verbatim
    location_text ⊆ rule_text
    each date ⊆ rule_text
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalize(text: str) -> str:
    """Normalize for flexible substring checks.

    Strips bold markers (**), collapses whitespace, lowercases.
    Used ONLY for validation — never mutates stored data.
    """
    if not text:
        return ""
    s = text.replace("**", "")
    return " ".join(s.replace("\n", " ").split()).lower()


def _normalize_date(text: str) -> str:
    """Like _normalize but also strips commas and periods (date comparison)."""
    s = _normalize(text)
    return s.replace(",", "").replace(".", "")


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class RestrictionType(str, Enum):
    """Classification of fishing regulation restrictions."""

    CLOSURE = "closure"
    HARVEST = "harvest"
    GEAR_RESTRICTION = "gear_restriction"
    VESSEL_RESTRICTION = "vessel_restriction"
    LICENSING = "licensing"
    NOTE = "note"


# ---------------------------------------------------------------------------
# Rule
# ---------------------------------------------------------------------------


class Rule(BaseModel):
    """A single parsed regulation rule — scopeless.

    Location_text provides verbatim spatial context from the regulation text.
    The downstream regulation_builder handles spatial resolution.
    """

    model_config = ConfigDict(frozen=True)

    rule_text: str = Field(
        ...,
        description=(
            "Verbatim substring from raw_regs containing this rule. "
            "Must be an exact, contiguous substring — no ellipsis, "
            "no paraphrasing. For compound sentences split into "
            "multiple rules, use the ENTIRE sentence for each rule."
        ),
    )
    restriction_type: RestrictionType = Field(
        ...,
        description="Classification of this restriction.",
    )
    details: str = Field(
        ...,
        description=(
            "Concise normalized summary of the restriction "
            "(e.g., 'Daily quota of 2 trout', 'No powered boats')."
        ),
    )
    location_text: str = Field(
        default="",
        description=(
            "Verbatim location substring from rule_text describing WHERE "
            "this specific rule applies (e.g., 'upstream of McKinley Lake'). "
            "Empty string means the rule applies to the whole waterbody."
        ),
    )
    dates: List[str] = Field(
        default_factory=list,
        description=(
            "Date strings exactly as they appear in rule_text "
            "(e.g., ['Jan 1 - Apr 30']). Empty if no dates specified."
        ),
    )

    @model_validator(mode="after")
    def _validate_chain(self) -> "Rule":
        """Enforce verbatim chain of custody and anti-hallucination checks."""
        errors: List[str] = []

        # rule_text must be non-empty
        if not self.rule_text or not self.rule_text.strip():
            errors.append("rule_text is empty")
            if errors:
                raise ValueError("; ".join(errors))
            return self

        # details must be non-empty
        if not self.details or not self.details.strip():
            errors.append("details is empty")

        # Anti-hallucination: no ellipsis in verbatim fields
        for field_name in ("rule_text", "location_text"):
            val = getattr(self, field_name)
            if val and "..." in val:
                errors.append(
                    f"{field_name} contains '...' — verbatim fields must not "
                    f"be truncated"
                )

        # Location chain: location_text ⊆ rule_text (only if non-empty)
        if self.location_text:
            if _normalize(self.location_text) not in _normalize(self.rule_text):
                errors.append(
                    f"location_text not found in rule_text. "
                    f"Location: '{self.location_text[:80]}'. "
                    f"Rule: '{self.rule_text[:100]}'"
                )

        # Date chain: each date ⊆ rule_text
        for date in self.dates:
            if "\n" in date:
                errors.append(f"Date '{date}' contains newlines")
            if "*" in date:
                errors.append(f"Date '{date}' contains asterisks")
            if _normalize_date(date) not in _normalize_date(self.rule_text):
                errors.append(
                    f"Date '{date}' not found in rule_text. "
                    f"Rule: '{self.rule_text[:100]}'"
                )

        if errors:
            raise ValueError("; ".join(errors))
        return self


# ---------------------------------------------------------------------------
# ParsedEntry
# ---------------------------------------------------------------------------


class ParsedEntry(BaseModel):
    """Complete parsed result for one synopsis row.

    The parser receives a raw_regs string and must return this structure.
    includes_tributaries is determined from the raw_regs text, not the symbols array.
    tributary_only is for entries about tributaries only (e.g., "X LAKE'S TRIBUTARIES").
    entry_location_text captures spatial scope from the entry title/header
    that applies to ALL rules (e.g., "upstream of Sitkatapa Creek").
    """

    model_config = ConfigDict(frozen=True)

    regs_verbatim: str = Field(
        ...,
        description=(
            "Exact character-for-character copy of the input raw_regs. "
            "The LLM MUST echo this back unchanged."
        ),
    )
    includes_tributaries: bool = Field(
        default=False,
        description=(
            "Entry-level tributary inclusion from raw_regs text: "
            "true = raw_regs contains '[Includes Tributaries]' or 'including tributaries', "
            "false = text is silent or explicitly excludes tributaries."
        ),
    )
    tributary_only: bool = Field(
        default=False,
        description=(
            "True when this entry applies ONLY to tributaries "
            "(e.g., 'KOOTENAY RIVER'S TRIBUTARIES'). "
            "The entry governs tributary streams, not the main waterbody."
        ),
    )
    entry_location_text: str = Field(
        default="",
        description=(
            "Verbatim spatial scope from the entry title/header that applies "
            "to ALL rules (e.g., 'upstream of Sitkatapa Creek', "
            "'downstream of Adams Lake'). Empty if the entry covers the "
            "whole waterbody."
        ),
    )
    rules: List[Rule] = Field(
        ...,
        min_length=1,
        description="Every restriction parsed as a separate rule.",
    )
    audit_log: List[str] = Field(
        default_factory=list,
        description="Parsing ambiguities or source issues only.",
    )

    @model_validator(mode="after")
    def _validate_entry(self) -> "ParsedEntry":
        """Validate verbatim chain and coverage."""
        errors: List[str] = []

        if not self.regs_verbatim or not self.regs_verbatim.strip():
            errors.append("regs_verbatim is empty")
            if errors:
                raise ValueError("; ".join(errors))
            return self

        # rule_text ⊆ regs_verbatim for every rule
        regs_norm = _normalize(self.regs_verbatim)
        for i, rule in enumerate(self.rules):
            rule_norm = _normalize(rule.rule_text)
            if rule_norm and rule_norm not in regs_norm:
                errors.append(
                    f"rule[{i}].rule_text not found in regs_verbatim. "
                    f"Rule: '{rule.rule_text[:80]}'"
                )

        # Keyword coverage: known patterns in regs_verbatim should be in
        # at least one rule — catches missed restrictions.
        _KEYWORDS = [
            "no fishing",
            "class i water",
            "class ii water",
            "bait ban",
            "fly fishing only",
            "catch and release",
            "no powered boats",
            "single barbless hook",
            "daily quota",
            "steelhead stamp mandatory",
        ]
        all_rules_norm = " \n ".join(_normalize(r.rule_text) for r in self.rules)
        for kw in _KEYWORDS:
            if kw in regs_norm and kw not in all_rules_norm:
                errors.append(
                    f"Keyword '{kw}' in regs_verbatim but missing from all "
                    f"rule_text values — a restriction may have been missed."
                )

        if errors:
            raise ValueError("; ".join(errors))
        return self


# ---------------------------------------------------------------------------
# Batch (top-level LLM output)
# ---------------------------------------------------------------------------


class ParsedBatch(BaseModel):
    """Batch of parsed entries — the shape the LLM returns."""

    entries: List[ParsedEntry]


# ---------------------------------------------------------------------------
# Batch validation (compares against input rows)
# ---------------------------------------------------------------------------


def validate_batch(
    entries: List[ParsedEntry],
    input_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Validate a batch of parsed entries against their original input rows.

    Returns a list of error dicts (empty = all valid).
    Each error dict: {"index": int, "water": str, "errors": [str]}.
    """
    errors: List[Dict[str, Any]] = []

    if len(entries) != len(input_rows):
        errors.append(
            {
                "index": -1,
                "water": "BATCH",
                "errors": [
                    f"Count mismatch: {len(entries)} entries vs "
                    f"{len(input_rows)} input rows"
                ],
            }
        )
        return errors

    for i, (entry, row) in enumerate(zip(entries, input_rows)):
        row_errors: List[str] = []
        water = row.get("water", f"row_{i}")
        raw_regs = row.get("raw_regs", "")

        # regs_verbatim must match input raw_regs exactly
        if entry.regs_verbatim != raw_regs:
            row_errors.append(
                f"regs_verbatim mismatch: expected '{raw_regs[:60]}...', "
                f"got '{entry.regs_verbatim[:60]}...'"
            )

        # includes_tributaries should match symbols and raw_regs text
        symbols = row.get("symbols", [])
        has_trib_symbol = any("trib" in s.lower() for s in symbols)
        regs_lower = raw_regs.lower()
        has_trib_text = (
            "[includes tributaries]" in regs_lower
            or "including tributaries" in regs_lower
        )
        if has_trib_symbol and not entry.includes_tributaries:
            row_errors.append(
                "Symbols contain 'Incl. Tribs' but includes_tributaries "
                f"is {entry.includes_tributaries}"
            )
        if entry.includes_tributaries and not has_trib_symbol and not has_trib_text:
            row_errors.append(
                "includes_tributaries is True but no tributary marker "
                "found in raw_regs text or symbols"
            )

        if row_errors:
            errors.append({"index": i, "water": water, "errors": row_errors})

    return errors
