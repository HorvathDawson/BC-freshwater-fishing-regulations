"""
Data models for the BC Fishing Regulations Synopsis Pipeline.

Contains all data classes used across extraction, parsing, and processing stages.
Strictly adheres to the Scope-First JSON architecture and Verbatim Chain of Custody.
"""

import os
import json
from typing import List, Optional, Dict, Any
from datetime import datetime
from attrs import define, asdict


def _input_field(row, key, default=None):
    """Get a field from an input row which may be an object or a dict.

    Returns `default` when the field is missing.
    """
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    # objects: use getattr with default
    return getattr(row, key, default)


def _normalize_text(text: str) -> str:
    """Normalize text for flexible substring comparison.

    Removes newlines, extra spaces, and converts to lowercase.
    Used ONLY for validation checks, not for data mutation.
    """
    if not text:
        return ""
    return " ".join(text.replace("\n", " ").split()).lower()


def _normalize_date_text(text: str) -> str:
    """Normalize text for date comparison only.

    Removes newlines, extra spaces, bold markers (**text**), and converts to lowercase.
    Used ONLY for date validation checks.
    """
    if not text:
        return ""
    # Remove bold markers
    normalized = text.replace("**", "")
    # Remove newlines and normalize whitespace
    normalized = " ".join(normalized.replace("\n", " ").split())
    return normalized.lower()


# ==========================================
#       EXTRACTION DATA MODELS
# ==========================================


@define(frozen=True, cache_hash=True)
class WaterbodyRow:
    """Represents a single waterbody row extracted from the PDF."""

    water: str
    mu: List[str]
    raw_regs: str
    symbols: List[str]
    page: int
    image: str
    region: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WaterbodyRow":
        return cls(**data)


@define(frozen=True, cache_hash=True)
class PageMetadata:
    """Metadata for a single page."""

    page_number: int
    region: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PageMetadata":
        return cls(**data)


@define(frozen=True, cache_hash=True)
class PageResult:
    """Result of extracting a single page."""

    metadata: PageMetadata
    rows: List[WaterbodyRow]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "context": self.metadata.to_dict(),
            "rows": [row.to_dict() for row in self.rows],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PageResult":
        return cls(
            metadata=PageMetadata.from_dict(data["context"]),
            rows=[WaterbodyRow.from_dict(row) for row in data["rows"]],
        )


@define(frozen=True, cache_hash=True)
class ExtractionResults:
    """Results from extracting all pages from the PDF."""

    pages: List[PageResult]

    def to_dict(self) -> List[Dict[str, Any]]:
        return [page.to_dict() for page in self.pages]

    @classmethod
    def from_dict(cls, data: List[Dict[str, Any]]) -> "ExtractionResults":
        return cls(pages=[PageResult.from_dict(page) for page in data])

    def __len__(self) -> int:
        return len(self.pages)

    def __iter__(self):
        return iter(self.pages)

    def __getitem__(self, index):
        return self.pages[index]


# ==========================================
#       PARSING DATA MODELS (SCOPE-FIRST)
# ==========================================


@define(frozen=True, cache_hash=True)
class ScopeObject:
    """Represents a spatial scope for a fishing regulation.

    Defines WHERE a rule applies using geometric operations.
    """

    type: str  # Enum: WHOLE_SYSTEM, DIRECTIONAL, SEGMENT, NAMED_PART, TRIBUTARIES_ONLY, BUFFER, VAGUE
    waterbody_key: str  # "ALL" or specific waterbody name
    location_verbatim: Optional[str]  # Exact substring describing location
    landmark_verbatim: Optional[str]  # Anchor point for spatial reference
    landmark_end_verbatim: Optional[str]  # End point for SEGMENT type
    direction: Optional[
        str
    ]  # Enum: UPSTREAM, DOWNSTREAM, BETWEEN, NORTH_OF, SOUTH_OF, EAST_OF, WEST_OF, NORTHEAST_OF, NORTHWEST_OF, SOUTHEAST_OF, SOUTHWEST_OF, etc.
    includes_tributaries: bool  # True if this scope explicitly includes tributaries

    def validate(self, parent_text: str = None) -> List[str]:
        """Validate this scope object against the spec."""
        errors = []

        # 1. Validate Type
        valid_types = {
            "WHOLE_SYSTEM",
            "DIRECTIONAL",
            "SEGMENT",
            "NAMED_PART",
            "TRIBUTARIES_ONLY",
            "BUFFER",
            "VAGUE",
        }
        if self.type not in valid_types:
            errors.append(
                f"Invalid scope type '{self.type}', must be one of {valid_types}"
            )

        # 2. Validate Direction
        valid_directions = {
            "UPSTREAM",
            "DOWNSTREAM",
            "BETWEEN",
            "NORTH_OF",
            "SOUTH_OF",
            "EAST_OF",
            "WEST_OF",
            "NORTHEAST_OF",
            "NORTHWEST_OF",
            "SOUTHEAST_OF",
            "SOUTHWEST_OF",
            None,
        }
        if self.direction not in valid_directions:
            errors.append(f"Invalid direction '{self.direction}'")

        # 3. Type-Specific Validation
        if self.type == "SEGMENT":
            if not self.landmark_verbatim:
                errors.append("SEGMENT type requires landmark_verbatim")
            if not self.landmark_end_verbatim:
                errors.append("SEGMENT type requires landmark_end_verbatim")
            if self.direction != "BETWEEN":
                errors.append("SEGMENT type must have direction='BETWEEN'")

        if self.type == "DIRECTIONAL":
            if not self.landmark_verbatim:
                errors.append("DIRECTIONAL type requires landmark_verbatim")
            if self.direction is None:
                errors.append("DIRECTIONAL type requires a direction")

        if self.type == "BUFFER":
            if not self.landmark_verbatim:
                errors.append("BUFFER type requires landmark_verbatim")
            if self.location_verbatim:
                loc_lower = self.location_verbatim.lower()
                # Valid buffer patterns: "within", "upstream and downstream", or directional with distance
                has_within = "within" in loc_lower
                has_two_sided = (
                    "upstream and downstream" in loc_lower
                    or "downstream and upstream" in loc_lower
                )
                # Check for distance units: look for patterns like "100 m", "50 km", "400 meters"
                import re

                has_distance = bool(
                    re.search(r"\d+\s*(m\b|km\b|meter|kilomet)", loc_lower)
                )
                has_one_sided = (
                    "upstream" in loc_lower or "downstream" in loc_lower
                ) and has_distance

                if not (has_within or has_two_sided or has_one_sided):
                    errors.append(
                        "BUFFER type should contain 'within', 'upstream and downstream', or directional distance pattern in location_verbatim"
                    )
            else:
                errors.append("BUFFER type requires location_verbatim")

        if self.type == "VAGUE":
            if not self.landmark_verbatim and not self.location_verbatim:
                errors.append(
                    "VAGUE type requires landmark_verbatim or location_verbatim"
                )

        # 4. Landmark containment validation (Level 4 Validation)
        # Ensure landmarks are substrings of the location description
        if self.landmark_verbatim and self.location_verbatim:
            if self.landmark_verbatim not in self.location_verbatim:
                errors.append(
                    f"landmark_verbatim '{self.landmark_verbatim}' not found in location_verbatim"
                )

        if self.landmark_end_verbatim and self.location_verbatim:
            if self.landmark_end_verbatim not in self.location_verbatim:
                errors.append(
                    f"landmark_end_verbatim '{self.landmark_end_verbatim}' not found in location_verbatim"
                )

        return errors

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ScopeObject":
        return cls(
            type=data.get("type", ""),
            waterbody_key=data.get("waterbody_key", "ALL"),
            location_verbatim=data.get("location_verbatim"),
            landmark_verbatim=data.get("landmark_verbatim"),
            landmark_end_verbatim=data.get("landmark_end_verbatim"),
            direction=data.get("direction"),
            includes_tributaries=data.get("includes_tributaries", False),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@define(frozen=True, cache_hash=True)
class RestrictionObject:
    """Represents a specific legal restriction/requirement."""

    type: str  # Enum: closure, harvest, gear_restriction, vessel_restriction, licensing, note
    details: str  # Normalized summary of the restriction
    dates: Optional[List[str]]  # Exact date strings from source text or null

    def validate(self, parent_text: str = None) -> List[str]:
        """Validate this restriction against the spec."""
        errors = []

        valid_types = {
            "closure",
            "harvest",
            "gear_restriction",
            "vessel_restriction",
            "licensing",
            "note",
        }
        if self.type not in valid_types:
            errors.append(f"Invalid restriction type '{self.type}'")

        if not self.details or not self.details.strip():
            errors.append("details is empty")

        if self.dates is not None and not isinstance(self.dates, list):
            errors.append(
                f"dates must be list or None, got {type(self.dates).__name__}"
            )

        # Date hygiene checks
        if self.dates and isinstance(self.dates, list):
            for date in self.dates:
                if "*" in date:
                    errors.append(f"Date '{date}' contains asterisks (must be cleaned)")
                if "\n" in date:
                    errors.append(f"Date '{date}' contains newlines (must be cleaned)")

        return errors

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RestrictionObject":
        dates = data.get("dates")
        if dates == "null" or dates == "null":
            dates = None
        return cls(
            type=data.get("type", ""),
            details=data.get("details", ""),
            dates=dates,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "details": self.details,
            "dates": self.dates if self.dates is not None else "null",
        }


@define(frozen=True, cache_hash=True)
class IdentityObject:
    """Root identity information for a waterbody regulation entry."""

    name_verbatim: str  # Exact character-for-character title from source
    waterbody_key: str  # Core waterbody name ONLY (e.g., "ELK RIVER", "TROUT LAKE") - no location, scope, or modifiers
    identity_type: str  # Enum: LAKE, RIVER, CREEK, STREAM, RESERVOIR, WATERSHED, MANAGEMENT_AREA, PARK, CONFLUENCE, ARM, BAY, SLOUGH, POND, TRIBUTARIES
    alternate_names: List[
        str
    ]  # Alternate/former/quoted names (e.g., ["McNaughton"], ["formerly Tseax River"])
    location_descriptor: Optional[
        str
    ]  # Disambiguating location (e.g., "near Kimberley", "approx. 10 km west of 100 Mile House")
    notes: Optional[
        str
    ]  # Administrative notes (e.g., "permit required see Note on page 34", "see map on page 42")
    global_scope: ScopeObject  # Master spatial constraint (e.g., upstream/downstream, specific segment)
    exclusions: List[ScopeObject]  # Geographic areas explicitly excluded
    inclusions: List[
        ScopeObject
    ]  # Geographic areas explicitly included (e.g., "includes Little Shuswap Lake")

    def validate(self) -> List[str]:
        errors = []

        if not self.name_verbatim or not self.name_verbatim.strip():
            errors.append("name_verbatim is empty")

        if not self.waterbody_key or not self.waterbody_key.strip():
            errors.append("waterbody_key is empty")

        # Validate identity_type
        valid_types = {
            "STREAM",  # All flowing water: rivers, creeks, streams
            "STILL_WATER",  # All standing water: lakes, reservoirs, sloughs, ponds, bays, arms
            "MANAGEMENT_AREA",
            "PARK",
            "CONFLUENCE",
            "TRIBUTARIES",
            "MULTIPLE_WATERBODIES",
            "WATERS",
        }
        if self.identity_type not in valid_types:
            errors.append(
                f"Invalid identity_type '{self.identity_type}', must be one of {valid_types}"
            )

        # Validate component_waterbodies
        if self.component_waterbodies and not isinstance(
            self.component_waterbodies, list
        ):
            errors.append(
                f"component_waterbodies must be a list, got {type(self.component_waterbodies).__name__}"
            )

        if (
            self.identity_type == "MULTIPLE_WATERBODIES"
            and not self.component_waterbodies
        ):
            errors.append(
                "MULTIPLE_WATERBODIES type requires component_waterbodies to be populated"
            )

        # Validate waterbody_key is core name only (no parenthetical content, no scope indicators)
        if self.waterbody_key and self.name_verbatim:
            import re

            # Waterbody_key should NOT contain:
            # - Parentheses (those go in alternate_names, location_descriptor, or scope)
            # - Scope indicators like "upstream of", "downstream of"
            # - Location descriptors like "near X"
            if "(" in self.waterbody_key or ")" in self.waterbody_key:
                errors.append(
                    f"waterbody_key '{self.waterbody_key}' contains parentheses - these should be parsed into alternate_names, location_descriptor, or scope"
                )

            # Check for scope indicators in waterbody_key
            scope_indicators = [
                "upstream of",
                "downstream of",
                "between",
                "from",
                " to ",
                "in zone",
                "near ",
                "approx",
            ]
            for indicator in scope_indicators:
                if indicator.lower() in self.waterbody_key.lower():
                    errors.append(
                        f"waterbody_key '{self.waterbody_key}' contains scope/location indicator '{indicator}' - should be in global_scope or location_descriptor"
                    )

            # Waterbody_key should be extractable from name_verbatim by removing parenthetical/scope content
            # Extract the base name before first parenthesis or scope indicator
            base_name_match = re.match(r"^([^(]+)", self.name_verbatim)
            if base_name_match:
                base_name = base_name_match.group(1).strip()

                # Remove common suffixes from base_name
                for suffix in [
                    "'S TRIBUTARIES",
                    "'s TRIBUTARIES",
                    " TRIBUTARIES",
                    "'S TRIBUTARY",
                    " WATERSHED",
                    " WATERS",
                ]:
                    if base_name.endswith(suffix):
                        base_name = base_name[: -len(suffix)].strip()

                # waterbody_key should match this cleaned base name
                if self.waterbody_key != base_name:
                    # Allow some flexibility for quoted names
                    if not (self.waterbody_key.strip('"') == base_name.strip('"')):
                        errors.append(
                            f"waterbody_key '{self.waterbody_key}' does not match extracted base name '{base_name}' from name_verbatim '{self.name_verbatim}'"
                        )

        # Validate alternate_names
        if self.alternate_names and not isinstance(self.alternate_names, list):
            errors.append(
                f"alternate_names must be a list, got {type(self.alternate_names).__name__}"
            )

        # Validate global_scope
        scope_errors = self.global_scope.validate()
        for err in scope_errors:
            errors.append(f"global_scope: {err}")

        # Validate exclusions
        for idx, exclusion in enumerate(self.exclusions):
            exc_errors = exclusion.validate(self.name_verbatim)
            for err in exc_errors:
                errors.append(f"exclusion {idx}: {err}")

        # Validate inclusions
        for idx, inclusion in enumerate(self.inclusions):
            inc_errors = inclusion.validate(self.name_verbatim)
            for err in inc_errors:
                errors.append(f"inclusion {idx}: {err}")

        return errors

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "IdentityObject":
        return cls(
            name_verbatim=data.get("name_verbatim", ""),
            waterbody_key=data.get("waterbody_key", ""),
            identity_type=data.get("identity_type", ""),
            component_waterbodies=data.get("component_waterbodies", []),
            alternate_names=data.get("alternate_names", []),
            location_descriptor=data.get("location_descriptor"),
            notes=data.get("notes"),
            global_scope=ScopeObject.from_dict(data.get("global_scope", {})),
            exclusions=[ScopeObject.from_dict(e) for e in data.get("exclusions", [])],
            inclusions=[ScopeObject.from_dict(i) for i in data.get("inclusions", [])],
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name_verbatim": self.name_verbatim,
            "waterbody_key": self.waterbody_key,
            "identity_type": self.identity_type,
            "component_waterbodies": self.component_waterbodies,
            "alternate_names": self.alternate_names,
            "location_descriptor": self.location_descriptor,
            "notes": self.notes,
            "global_scope": self.global_scope.to_dict(),
            "exclusions": [e.to_dict() for e in self.exclusions],
            "inclusions": [i.to_dict() for i in self.inclusions],
        }


@define(frozen=True, cache_hash=True)
class RuleGroup:
    """A block of regulation text with its scope and restrictions.

    Corresponds to the 'rules' array items in the spec.
    """

    rule_text_verbatim: List[
        str
    ]  # Array of exact substrings from regulation block (for interleaved rules)
    scope: ScopeObject  # Single scope defining where rules apply
    restrictions: List[RestrictionObject]  # Legal restrictions for this scope

    def validate(self, parent_regs_verbatim: str) -> List[str]:
        """Validate rule group against the authoritative parent text."""
        errors = []

        if not self.rule_text_verbatim or not isinstance(self.rule_text_verbatim, list):
            errors.append("rule_text_verbatim must be a non-empty list")
            return errors  # Cannot continue validation

        if len(self.rule_text_verbatim) == 0:
            errors.append("rule_text_verbatim array is empty")
            return errors

        # Check each element is non-empty
        for idx, text in enumerate(self.rule_text_verbatim):
            if not text or not text.strip():
                errors.append(f"rule_text_verbatim[{idx}] is empty")

        # Level 2 Validation: Each element of rule_text_verbatim must be in parent regs_verbatim
        if parent_regs_verbatim:
            parent_normalized = _normalize_text(parent_regs_verbatim)
            for idx, text in enumerate(self.rule_text_verbatim):
                if _normalize_text(text) not in parent_normalized:
                    errors.append(
                        f"rule_text_verbatim[{idx}] not found in parent regs_verbatim"
                    )

        # Level 3 Validation: Scope location must be in at least one element of rule_text_verbatim
        scope_errors = self.scope.validate()
        for err in scope_errors:
            errors.append(f"scope: {err}")

        if self.scope.location_verbatim and self.rule_text_verbatim:
            location_found = False
            normalized_location = _normalize_text(self.scope.location_verbatim)
            for text in self.rule_text_verbatim:
                if normalized_location in _normalize_text(text):
                    location_found = True
                    break
            if not location_found:
                errors.append(
                    "scope.location_verbatim not found in any rule_text_verbatim element"
                )

        # Validate restrictions
        if not self.restrictions:
            errors.append("No restrictions found")

        # Concatenate all rule_text_verbatim for restriction validation
        combined_text = "\n".join(self.rule_text_verbatim)

        for idx, restriction in enumerate(self.restrictions):
            rest_errors = restriction.validate(combined_text)
            for err in rest_errors:
                errors.append(f"restriction {idx}: {err}")

            # Date Verification Protocol: Dates must be literal substrings in at least one element
            if restriction.dates and isinstance(restriction.dates, list):
                for date in restriction.dates:
                    date_found = False
                    normalized_date = _normalize_date_text(date)
                    for text in self.rule_text_verbatim:
                        if normalized_date in _normalize_date_text(text):
                            date_found = True
                            break
                    if not date_found:
                        errors.append(
                            f"restriction {idx}: Date '{date}' not found in any rule_text_verbatim element"
                        )

        return errors

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RuleGroup":
        # Handle legacy text_verbatim field by converting to array
        rule_text = data.get("rule_text_verbatim")
        if rule_text is None:
            # Fallback for legacy format
            legacy_text = data.get("text_verbatim", "")
            rule_text = [legacy_text] if legacy_text else []
        elif isinstance(rule_text, str):
            # Convert single string to array
            rule_text = [rule_text] if rule_text else []

        return cls(
            rule_text_verbatim=rule_text,
            scope=ScopeObject.from_dict(data.get("scope", {})),
            restrictions=[
                RestrictionObject.from_dict(r) for r in data.get("restrictions", [])
            ],
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_text_verbatim": self.rule_text_verbatim,
            "scope": self.scope.to_dict(),
            "restrictions": [r.to_dict() for r in self.restrictions],
        }


@define(frozen=True, cache_hash=True)
class ParsedWaterbody:
    """Complete parsed result for a single waterbody (Scope-First Spec)."""

    identity: IdentityObject
    regs_verbatim: str  # Exact, character-for-character copy of the input raw_regs
    audit_log: List[
        str
    ]  # List of parsing issues, ambiguities, or source errors (can be empty)
    rules: List[RuleGroup]

    def validate(
        self, expected_name: str = None, expected_raw_text: str = None
    ) -> List[str]:
        """Validate this waterbody result against the Verbatim Chain of Custody."""
        errors = []

        # Validate Identity
        identity_errors = self.identity.validate()
        for err in identity_errors:
            errors.append(f"identity: {err}")

        # Level 1 Validation: Name match
        if (
            expected_name
            and self.identity.name_verbatim.strip() != expected_name.strip()
        ):
            errors.append(
                f"Name mismatch: expected '{expected_name}', got '{self.identity.name_verbatim}'"
            )

        # Level 0 Validation: regs_verbatim match
        if expected_raw_text is not None:
            if self.regs_verbatim != expected_raw_text:
                preview = (
                    expected_raw_text[:50] + "..."
                    if len(expected_raw_text) > 50
                    else expected_raw_text
                )
                errors.append(f"regs_verbatim mismatch. Expected start: '{preview}'")

        # Validate audit_log
        if self.audit_log is None:
            errors.append("audit_log field is missing (must be a list, can be empty)")
        elif not isinstance(self.audit_log, list):
            errors.append(
                f"audit_log must be a list, got {type(self.audit_log).__name__}"
            )

        # Validate Rules
        if not self.rules:
            errors.append("No rules found")
        else:
            for idx, rule in enumerate(self.rules):
                # Rules are validated against the authoritative regs_verbatim
                rule_errors = rule.validate(self.regs_verbatim)
                for err in rule_errors:
                    errors.append(f"rule {idx}: {err}")

                # Check if VAGUE scope requires audit_log entry
                if rule.scope.type == "VAGUE" and (
                    not self.audit_log or len(self.audit_log) == 0
                ):
                    errors.append(
                        f"rule {idx}: VAGUE scope requires at least one audit_log entry explaining why"
                    )

            # Check for duplicate scopes (rules with identical scope should be merged)
            scope_to_rules = {}
            for idx, rule in enumerate(self.rules):
                # Create a scope key based on type and location
                scope_key = (
                    rule.scope.type,
                    rule.scope.location_verbatim or None,
                    rule.scope.includes_tributaries,
                )

                if scope_key not in scope_to_rules:
                    scope_to_rules[scope_key] = []
                scope_to_rules[scope_key].append(idx)

            # Report any duplicate scopes
            for scope_key, rule_indices in scope_to_rules.items():
                if len(rule_indices) > 1:
                    scope_type, location, includes_tribs = scope_key
                    location_str = f"'{location}'" if location else "None"
                    errors.append(
                        f"Duplicate scope found: {len(rule_indices)} rules share identical scope "
                        f"(type={scope_type}, location={location_str}, includes_tributaries={includes_tribs}). "
                        f"Rules {rule_indices} should be merged into a single rule. "
                        f"All restrictions for the same exact scope must be in one rule."
                    )

        return errors

    def to_dict(self) -> Dict[str, Any]:
        return {
            "identity": self.identity.to_dict(),
            "regs_verbatim": self.regs_verbatim,
            "audit_log": self.audit_log,
            "rules": [r.to_dict() for r in self.rules],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ParsedWaterbody":
        return cls(
            identity=IdentityObject.from_dict(data.get("identity", {})),
            regs_verbatim=data.get("regs_verbatim", ""),
            audit_log=data.get("audit_log", []),  # Default to empty list if missing
            rules=[RuleGroup.from_dict(r) for r in data.get("rules", [])],
        )

    @classmethod
    def validate_batch(
        cls, parsed_batch: List[Dict[str, Any]], input_rows: List
    ) -> List[str]:
        """Validate a batch of parsed results matches input order and content.

        Enforces:
        1. Count matches.
        2. Order matches (via Name check).
        3. Header Symbol Logic ("Incl. Tribs").
        4. Internal Data Integrity.
        """
        validation_errors = []

        if not isinstance(parsed_batch, list):
            return [f"Result is not a list, got {type(parsed_batch).__name__}"]

        if len(parsed_batch) != len(input_rows):
            return [f"Expected {len(input_rows)} items, got {len(parsed_batch)}"]

        for idx, entry in enumerate(parsed_batch):
            try:
                row = input_rows[idx]
                expected_name = _input_field(row, "water")
                expected_raw_text = _input_field(row, "raw_regs")

                # 1. Structure Check
                if not isinstance(entry, dict):
                    validation_errors.append(
                        f"Item {idx}: Expected dict, got {type(entry).__name__}"
                    )
                    continue

                # 2. Identity/Order Check
                identity_name = entry.get("identity", {}).get("name_verbatim", "")
                if identity_name != expected_name:
                    validation_errors.append(
                        f"Item {idx}: Name mismatch - expected '{expected_name}', got '{identity_name}'"
                    )

                # 3. Header Symbol Validation (Incl. Tribs)
                # If input symbols contain "Incl. Tribs", output global_scope MUST have includes_tributaries=True
                symbols = _input_field(row, "symbols", []) or []
                header_implies_tribs = any(
                    "incl. tribs" in str(s).lower() for s in symbols
                )

                # Check if waterbody name indicates tributaries or symbols contain tributary markers
                name_implies_tribs = expected_name and (
                    expected_name.lower().endswith("tributaries")
                    or expected_name.lower().endswith("'s tributaries")
                    or "tributary streams" in expected_name.lower()
                )

                if header_implies_tribs or name_implies_tribs:
                    g_scope = entry.get("identity", {}).get("global_scope", {})
                    if not g_scope.get("includes_tributaries", False):
                        reason = (
                            "Header has 'Incl. Tribs'"
                            if header_implies_tribs
                            else "Waterbody name indicates tributaries"
                        )
                        validation_errors.append(
                            f"Item {idx}: {reason} but identity.global_scope.includes_tributaries is False"
                        )

                # 4. Deep Validation
                parsed = cls.from_dict(entry)
                item_errors = parsed.validate(expected_name, expected_raw_text)
                validation_errors.extend([f"Item {idx}: {err}" for err in item_errors])

            except Exception as e:
                validation_errors.append(
                    f"Item {idx}: Failed to parse/validate - {str(e)}"
                )

        return validation_errors


# ==========================================
#       SESSION MANAGEMENT
# ==========================================


@define
class SessionState:
    """Complete session state for resumable parsing."""

    input_rows: List[WaterbodyRow]
    results: List[Optional[ParsedWaterbody]]
    processed_items: List[int]
    failed_items: List[Dict[str, Any]]
    validation_failures: List[Dict[str, Any]]
    retry_counts: Dict[int, int]
    total_items: int
    created_at: str
    last_updated: str
    completed_at: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        # Serialize Input Rows
        input_rows_dicts = []
        for row in self.input_rows:
            if hasattr(row, "to_dict"):
                input_rows_dicts.append(row.to_dict())
            else:
                input_rows_dicts.append(row.__dict__)

        # Serialize Results
        results_dicts = [r.to_dict() if r else None for r in self.results]

        # Retry counts keys to strings for JSON
        retry_counts_str = {str(k): v for k, v in self.retry_counts.items()}

        return {
            "input_rows": input_rows_dicts,
            "results": results_dicts,
            "processed_items": self.processed_items,
            "failed_items": self.failed_items,
            "validation_failures": self.validation_failures,
            "retry_counts": retry_counts_str,
            "total_items": self.total_items,
            "created_at": self.created_at,
            "last_updated": self.last_updated,
            "completed_at": self.completed_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SessionState":
        # Restore Input Rows
        input_rows = []
        for row_dict in data["input_rows"]:
            if hasattr(WaterbodyRow, "from_dict"):
                input_rows.append(WaterbodyRow.from_dict(row_dict))
            else:
                # Fallback for generic objects
                input_rows.append(type("WaterbodyRow", (), row_dict)())

        # Restore Results
        results = []
        for res in data["results"]:
            results.append(ParsedWaterbody.from_dict(res) if res else None)

        retry_counts = {int(k): v for k, v in data.get("retry_counts", {}).items()}

        return cls(
            input_rows=input_rows,
            results=results,
            processed_items=data["processed_items"],
            failed_items=data["failed_items"],
            validation_failures=data.get("validation_failures", []),
            retry_counts=retry_counts,
            total_items=data["total_items"],
            created_at=data["created_at"],
            last_updated=data["last_updated"],
            completed_at=data.get("completed_at"),
        )

    def save(self, filepath: str):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        self.last_updated = datetime.now().isoformat()
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)

    @classmethod
    def load(cls, filepath: str) -> Optional["SessionState"]:
        if not os.path.exists(filepath):
            return None
        with open(filepath, "r", encoding="utf-8") as f:
            return cls.from_dict(json.load(f))

    @classmethod
    def create_new(cls, input_rows: List[WaterbodyRow]) -> "SessionState":
        total = len(input_rows)
        now = datetime.now().isoformat()
        return cls(
            input_rows=input_rows,
            results=[None] * total,
            processed_items=[],
            failed_items=[],
            validation_failures=[],
            retry_counts={},
            total_items=total,
            created_at=now,
            last_updated=now,
            completed_at=None,
        )
