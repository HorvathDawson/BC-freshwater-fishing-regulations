"""
Data models for the BC Fishing Regulations Synopsis Pipeline.

Contains all data classes used across extraction, parsing, and processing stages.
"""

import os
import json
from typing import List, Optional, Dict, Any
from datetime import datetime
from attrs import define, asdict


# ==========================================
#      EXTRACTION DATA MODELS
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
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WaterbodyRow":
        """Create WaterbodyRow from dictionary."""
        return cls(**data)


@define(frozen=True, cache_hash=True)
class PageMetadata:
    """Metadata for a single page."""

    page_number: int
    region: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PageMetadata":
        """Create PageMetadata from dictionary."""
        return cls(**data)


@define(frozen=True, cache_hash=True)
class PageResult:
    """Result of extracting a single page."""

    metadata: PageMetadata
    rows: List[WaterbodyRow]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "context": self.metadata.to_dict(),
            "rows": [row.to_dict() for row in self.rows],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PageResult":
        """Create PageResult from dictionary."""
        return cls(
            metadata=PageMetadata.from_dict(data["context"]),
            rows=[WaterbodyRow.from_dict(row) for row in data["rows"]],
        )


@define(frozen=True, cache_hash=True)
class ExtractionResults:
    """Results from extracting all pages from the PDF."""

    pages: List[PageResult]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return [page.to_dict() for page in self.pages]

    @classmethod
    def from_dict(cls, data: List[Dict[str, Any]]) -> "ExtractionResults":
        """Create ExtractionResults from list of dictionaries."""
        return cls(pages=[PageResult.from_dict(page) for page in data])

    def __len__(self) -> int:
        """Return number of pages."""
        return len(self.pages)

    def __iter__(self):
        """Allow iteration over pages."""
        return iter(self.pages)

    def __getitem__(self, index):
        """Allow indexing into pages."""
        return self.pages[index]


# ==========================================
#      PARSING DATA MODELS
# ==========================================


@define(frozen=True, cache_hash=True)
class ParsedRule:
    """A single parsed fishing regulation rule."""

    verbatim_text: str
    rule: str
    type: str
    dates: Optional[List[str]]
    species: Optional[List[str]]

    def validate(self, parent_text: str) -> List[str]:
        """Validate this rule. Returns list of error messages.

        Args:
            parent_text: The geographic group's raw_text that should contain this rule's verbatim_text
        """
        errors = []

        # Check required fields are non-empty
        if not self.verbatim_text or not self.verbatim_text.strip():
            errors.append("verbatim_text is empty")
        if not self.rule or not self.rule.strip():
            errors.append("rule is empty")

        # Validate verbatim_text actually appears in parent (geographic group's raw_text)
        if self.verbatim_text and parent_text:
            # Normalize for comparison (remove extra whitespace, case insensitive)
            verbatim_normalized = " ".join(
                self.verbatim_text.replace("\n", " ").split()
            ).lower()
            parent_normalized = " ".join(parent_text.replace("\n", " ").split()).lower()

            if verbatim_normalized not in parent_normalized:
                errors.append(f"verbatim_text not found in geographic group's raw_text")

        # Validate rule type
        valid_types = {
            "closure",
            "harvest",
            "gear_restriction",
            "restriction",
            "licensing",
            "access",
            "note",
        }
        if self.type not in valid_types:
            errors.append(
                f"Invalid rule type '{self.type}', must be one of {valid_types}"
            )

        # Validate dates/species are list or None
        if self.dates is not None and not isinstance(self.dates, list):
            errors.append(
                f"dates must be list or None, got {type(self.dates).__name__}"
            )
        if self.species is not None and not isinstance(self.species, list):
            errors.append(
                f"species must be list or None, got {type(self.species).__name__}"
            )

        # Validate dates are clean and appear in verbatim_text
        if self.dates and isinstance(self.dates, list):
            for date in self.dates:
                # Check date doesn't contain asterisks or newlines (CRITICAL: dates must be clean)
                if "*" in date:
                    errors.append(f"Date '{date}' contains asterisks (must be cleaned)")
                if "\n" in date:
                    errors.append(f"Date '{date}' contains newlines (must be cleaned)")

                # Validate date appears in verbatim_text (normalize for comparison)
                if self.verbatim_text:
                    # Normalize both by removing spaces, hyphens, asterisks, newlines for comparison
                    date_normalized = (
                        date.replace(" ", "")
                        .replace("-", "")
                        .replace("*", "")
                        .replace("\n", "")
                        .lower()
                    )
                    verbatim_normalized = (
                        self.verbatim_text.replace(" ", "")
                        .replace("-", "")
                        .replace("*", "")
                        .replace("\n", "")
                        .lower()
                    )

                    # Check if date appears in verbatim_text
                    if date_normalized not in verbatim_normalized:
                        errors.append(f"Date '{date}' not found in verbatim_text")

        return errors

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ParsedRule":
        """Create ParsedRule from dictionary, handling 'null' strings."""
        dates = data.get("dates")
        if dates == "null" or dates == "null":
            dates = None
        species = data.get("species")
        if species == "null" or species == "null":
            species = None

        return cls(
            verbatim_text=data.get("verbatim_text", ""),
            rule=data.get("rule", ""),
            type=data.get("type", ""),
            dates=dates,
            species=species,
        )


@define(frozen=True, cache_hash=True)
class ParsedGeographicGroup:
    """A geographic subdivision of regulations for a waterbody."""

    location: str
    raw_text: str
    cleaned_text: str
    rules: List[ParsedRule]

    def validate(self, waterbody_name: str) -> List[str]:
        """Validate this geographic group. Returns list of error messages."""
        errors = []

        # Check rules array not empty
        if not self.rules or len(self.rules) == 0:
            errors.append(f"Geographic group '{self.location}' has no rules")

        # Validate each rule
        for idx, rule in enumerate(self.rules):
            rule_errors = rule.validate(self.raw_text)
            for err in rule_errors:
                errors.append(f"Rule {idx}: {err}")

        return errors

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ParsedGeographicGroup":
        """Create ParsedGeographicGroup from dictionary."""
        rules = [ParsedRule.from_dict(r) for r in data.get("rules", [])]
        return cls(
            location=data.get("location", ""),
            raw_text=data.get("raw_text", ""),
            cleaned_text=data.get("cleaned_text", ""),
            rules=rules,
        )


@define(frozen=True, cache_hash=True)
class ParsedWaterbody:
    """Complete parsed result for a single waterbody."""

    waterbody_name: str
    raw_text: str
    cleaned_text: str
    geographic_groups: List[ParsedGeographicGroup]

    def validate(self, expected_name: str, expected_raw_text: str = None) -> List[str]:
        """Validate this waterbody result. Returns list of error messages.

        Args:
            expected_name: Required expected name from input - must always be provided
            expected_raw_text: Expected raw regulation text from input - should match exactly
        """
        errors = []

        # Check required fields
        if not self.waterbody_name or not self.waterbody_name.strip():
            errors.append("waterbody_name is empty")

        # Check name matches expected (REQUIRED - no optional)
        if not expected_name:
            errors.append("expected_name not provided to validate()")
        elif self.waterbody_name.strip() != expected_name.strip():
            errors.append(
                f"Name mismatch: expected '{expected_name}', got '{self.waterbody_name}'"
            )

        # Check raw_text matches input raw_regs exactly
        if expected_raw_text is not None:
            if self.raw_text != expected_raw_text:
                # Show a preview of the difference
                preview_len = 100
                expected_preview = expected_raw_text[:preview_len] + (
                    "..." if len(expected_raw_text) > preview_len else ""
                )
                actual_preview = self.raw_text[:preview_len] + (
                    "..." if len(self.raw_text) > preview_len else ""
                )
                errors.append(
                    f"raw_text doesn't match input raw_regs exactly. Expected: '{expected_preview}', Got: '{actual_preview}'"
                )

        # Validate geographic groups
        if not self.geographic_groups or len(self.geographic_groups) == 0:
            errors.append("No geographic groups found")
        else:
            for idx, group in enumerate(self.geographic_groups):
                group_errors = group.validate(self.waterbody_name)
                for err in group_errors:
                    errors.append(f"Group {idx} ({group.location}): {err}")

        return errors

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "waterbody_name": self.waterbody_name,
            "raw_text": self.raw_text,
            "cleaned_text": self.cleaned_text,
            "geographic_groups": [
                {
                    "location": g.location,
                    "raw_text": g.raw_text,
                    "cleaned_text": g.cleaned_text,
                    "rules": [
                        {
                            "verbatim_text": r.verbatim_text,
                            "rule": r.rule,
                            "type": r.type,
                            "dates": r.dates if r.dates is not None else "null",
                            "species": r.species if r.species is not None else "null",
                        }
                        for r in g.rules
                    ],
                }
                for g in self.geographic_groups
            ],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ParsedWaterbody":
        """Create ParsedWaterbody from dictionary."""
        return cls(
            waterbody_name=data["waterbody_name"],
            raw_text=data["raw_text"],
            cleaned_text=data["cleaned_text"],
            geographic_groups=[
                ParsedGeographicGroup.from_dict(g) for g in data["geographic_groups"]
            ],
        )

    @classmethod
    def validate_batch(
        cls, parsed_batch: List[Dict[str, Any]], input_rows: List
    ) -> List[str]:
        """Validate a batch of parsed results matches input order and content.

        This is the critical validation that ensures:
        1. Output has same count as input
        2. Output[idx] corresponds to input[idx] (order preservation)
        3. Names are copied verbatim
        4. All individual waterbody validations pass

        Args:
            parsed_batch: List of parsed waterbody dicts from LLM
            input_rows: List of input WaterbodyRow objects

        Returns:
            List of error messages (empty if valid)
        """
        validation_errors = []

        # Check basic structure
        if not isinstance(parsed_batch, list):
            validation_errors.append(
                f"Result is not a list, got {type(parsed_batch).__name__}"
            )
            return validation_errors

        # Check count matches (ORDER VALIDATION: must have same number of items)
        if len(parsed_batch) != len(input_rows):
            validation_errors.append(
                f"Expected {len(input_rows)} items, got {len(parsed_batch)}"
            )
            return validation_errors

        # Validate each item at its position (ORDER VALIDATION: position idx must match)
        for idx, entry in enumerate(parsed_batch):
            try:
                # ORDER + VERBATIM VALIDATION: output[idx].name must exactly match input[idx].name
                # This single check validates both correct ordering AND verbatim copying
                if entry.get("waterbody_name") != input_rows[idx].water:
                    validation_errors.append(
                        f"Item {idx}: Name/order mismatch - expected '{input_rows[idx].water}', "
                        f"got '{entry.get('waterbody_name')}'"
                    )

                # Convert to dataclass and validate structure
                parsed = cls.from_dict(entry)
                expected_name = input_rows[idx].water
                expected_raw_text = input_rows[idx].raw_regs
                item_errors = parsed.validate(expected_name, expected_raw_text)
                validation_errors.extend([f"Item {idx}: {err}" for err in item_errors])
            except Exception as e:
                validation_errors.append(f"Item {idx}: Failed to parse - {e}")

        return validation_errors


# ==========================================
#      SESSION MANAGEMENT
# ==========================================


@define
class SessionState:
    """Complete session state for resumable parsing.

    CRITICAL: Order preservation is maintained throughout:
    - input_rows: Original input order (index 0 is first input, etc.)
    - results: Indexed array matching input_rows (results[i] corresponds to input_rows[i])
    - Final output iterates through range(total_items) to preserve exact order
    """

    input_rows: List[WaterbodyRow]  # Full input data in original order
    results: List[
        Optional[ParsedWaterbody]
    ]  # Parsed results indexed by position - results[i] = parsed input_rows[i]
    processed_items: List[int]  # Indices of successfully processed items
    failed_items: List[Dict[str, Any]]  # Items that failed with error info
    validation_failures: List[
        Dict[str, Any]
    ]  # Items that failed validation (reset on resume for retry)
    retry_counts: Dict[int, int]  # Track retry attempts per item index
    total_items: int
    created_at: str  # ISO timestamp
    last_updated: str  # ISO timestamp
    completed_at: Optional[str]  # ISO timestamp when all items successfully processed

    def to_dict(self) -> Dict[str, Any]:
        """Convert session state to dictionary for JSON serialization."""
        # Convert input_rows (WaterbodyRow instances) to dicts
        input_rows_dicts = []
        for row in self.input_rows:
            if hasattr(row, "to_dict"):
                input_rows_dicts.append(row.to_dict())
            else:
                # Fallback for simple objects with __dict__
                input_rows_dicts.append({"water": row.water, "raw_regs": row.raw_regs})

        # Convert results (ParsedWaterbody instances or None) to dicts
        results_dicts = []
        for result in self.results:
            if result is not None:
                results_dicts.append(result.to_dict())
            else:
                results_dicts.append(None)

        # Convert retry_counts keys to strings (JSON requires string keys)
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
        """Create SessionState from dictionary loaded from JSON."""
        # Convert input_rows dicts back to WaterbodyRow instances
        input_rows = []
        for row_dict in data["input_rows"]:
            if "water" in row_dict and "raw_regs" in row_dict:
                # If WaterbodyRow has from_dict, use it
                if hasattr(WaterbodyRow, "from_dict"):
                    input_rows.append(WaterbodyRow.from_dict(row_dict))
                else:
                    # Create simple object with required attributes
                    input_rows.append(type("WaterbodyRow", (), row_dict)())
            else:
                input_rows.append(row_dict)

        # Convert results dicts back to ParsedWaterbody instances
        results = []
        for result_dict in data["results"]:
            if result_dict is not None:
                results.append(ParsedWaterbody.from_dict(result_dict))
            else:
                results.append(None)

        # Convert retry_counts keys back to integers
        retry_counts = {int(k): v for k, v in data.get("retry_counts", {}).items()}

        return cls(
            input_rows=input_rows,
            results=results,
            processed_items=data["processed_items"],
            failed_items=data["failed_items"],
            validation_failures=data.get(
                "validation_failures", []
            ),  # Default to empty list for old sessions
            retry_counts=retry_counts,
            total_items=data["total_items"],
            created_at=data["created_at"],
            last_updated=data["last_updated"],
            completed_at=data.get("completed_at"),  # Default to None for old sessions
        )

    def save(self, filepath: str):
        """Save session state to JSON file."""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        self.last_updated = datetime.now().isoformat()
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)

    @classmethod
    def load(cls, filepath: str) -> Optional["SessionState"]:
        """Load session state from JSON file."""
        if not os.path.exists(filepath):
            return None
        with open(filepath, "r", encoding="utf-8") as f:
            return cls.from_dict(json.load(f))

    @classmethod
    def create_new(cls, input_rows: List[WaterbodyRow]) -> "SessionState":
        """Create new session state."""
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
