"""
Data models for pipeline.

Simpler, flatter alternatives to the legacy parsing models.
The goal: every parsed synopsis entry eventually matches directly to a
LinkedRegulation rather than relying on free-text name search alone.

This gives full year-over-year audit transparency — when the synopsis PDF
changes, it is immediately clear which match_criteria fields need updating
and why the match broke.

Start simple, iterate later.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


class WaterbodyType(Enum):
    """Broad category of a matched FWA waterbody feature."""

    LAKE = "lake"
    STREAM = "stream"
    ADMIN = "admin"


class RestrictionType(Enum):
    """Category of a fishing regulation restriction."""

    CLOSURE = "closure"
    HARVEST = "harvest"  # Catch/possession limits
    GEAR_RESTRICTION = "gear_restriction"
    VESSEL_RESTRICTION = "vessel_restriction"
    LICENSING = "licensing"  # Permit or licence requirements
    NOTE = "note"  # Administrative note, no direct restriction


__all__ = [
    "RestrictionType",
    "WaterbodyType",
    "ParsedRule",
    "MatchedFeature",
    "Landmark",
    "MatchCriteria",
    "LinkedRegulation",
]


@dataclass
class ParsedRule:
    """A single extracted regulation from the synopsis text.

    Location and date are stored as plain parsed substrings, not structured
    objects — we keep this flat until we know exactly what queries we need.
    verbatim_text is the source of truth and must always be populated.
    """

    # Exact verbatim substring from the synopsis raw_regs field.
    verbatim_text: str

    # What kind of restriction this rule represents.
    restriction_type: Optional[RestrictionType] = None

    # Parsed location phrase as it appears in text (e.g. "upstream of Hwy 1").
    # None means the rule applies to the whole waterbody / no location qualifier.
    location_text: Optional[str] = None

    # Date string exactly as it appears in source (e.g. "Feb 1 – Apr 30").
    # None means the rule is year-round / no time restriction stated.
    date_text: Optional[str] = None

    # Short plain-English summary of what the rule actually says.
    # Used for display and debugging (e.g. "Fly fishing only", "Closed").
    restriction_summary: str = ""


@dataclass
class MatchedFeature:
    """One FWA waterbody feature that this regulation applies to.

    fid is a string so it can hold any FWA identifier type (GNIS ID, BLK,
    linear_feature_id, etc.).  display_name is optional — useful for
    debugging and for features that have no official GNIS name.
    """

    # FWA identifier. Interpretation depends on waterbody_type and pipeline context.
    fid: str
    waterbody_type: WaterbodyType

    # Human-readable name — populated from FWA lookup, not from user input.
    display_name: Optional[str] = None


@dataclass
class Landmark:
    """A named geographic anchor point within or near a waterbody.

    Landmarks let us link a location_text phrase (e.g. "upstream of Hwy 1
    bridge") to an actual point in FWA geometry later.  fid is optional
    because we may know the landmark name before we have time to look up
    its geometry.
    """

    # Verbatim name from the regulation text.
    name: str

    # FWA feature ID once the landmark has been spatially resolved.
    # Leave as None until that linking step is done.
    fid: Optional[str] = None


@dataclass
class MatchCriteria:
    """The 'fingerprint' used to locate the matching synopsis row.

    If a parsed synopsis entry satisfies all populated fields here, it is
    considered a match for this LinkedRegulation.  Storing the criteria
    explicitly (rather than relying only on name search) means:

      - We can see exactly what the old synopsis said vs. the new one.
      - A mismatch immediately shows which field changed.
      - We never silently match the wrong waterbody.

    mus (management units) only needs to contain the units relevant to this
    regulation — it does not have to be exhaustive.
    """

    # Exact waterbody name as it appears in the synopsis PDF.
    name_verbatim: str

    # Region string as labelled in the synopsis (e.g. "Region 4").
    # None means "any region" — use only when truly region-agnostic.
    region: Optional[str] = None

    # Management unit codes that must match (e.g. ["4-3", "4-12"]).
    # Empty list means MU matching is not used for this entry.
    mus: List[str] = field(default_factory=list)


@dataclass
class LinkedRegulation:
    """A manually-anchored synopsis regulation with its FWA feature matches.

    The key idea: instead of re-deriving matches on every pipeline run via
    name search, each LinkedRegulation stores explicit match_criteria so the
    link is stable across synopsis editions.  When the PDF changes, the
    mismatch surfaces immediately rather than silently producing wrong output.

    Population order (iterating later):
        1. Populate match_criteria + symbols from the synopsis row.
        2. Populate rules via parsing.
        3. Populate matched_features via FWA lookup / manual correction.
        4. Populate landmarks once spatial resolution is implemented.
        5. page + image_path come straight from the extraction metadata.
    """

    # How to find this regulation's row in the parsed synopsis data.
    match_criteria: MatchCriteria

    # Symbol codes extracted from the synopsis (e.g. ["C", "FFO"]).
    symbols: List[str] = field(default_factory=list)

    # --- Synopsis extraction metadata ---
    page: int = 0
    image_path: Optional[str] = None  # Cropped image for this synopsis entry.

    # --- Parsed rules ---
    # Each rule is one restriction with its verbatim text intact.
    rules: List[ParsedRule] = field(default_factory=list)

    # --- FWA feature matches ---
    # All waterbody features (lake, stream, admin) this regulation covers.
    matched_features: List[MatchedFeature] = field(default_factory=list)

    # --- Landmarks / subregions ---
    # Named anchor points referenced by rule location_text fields.
    # Populated during a later spatial-linking pass; empty is valid.
    landmarks: List[Landmark] = field(default_factory=list)
