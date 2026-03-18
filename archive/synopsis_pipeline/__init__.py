"""
Synopsis Pipeline Package
"""

from .models import (
    WaterbodyRow,
    ExtractionResults,
    ParsedWaterbody,
    SessionState,
    ScopeObject,
    RestrictionObject,
    IdentityObject,
    RuleGroup,
)
from .review_parsed import ReviewSession, generate_report

# NOTE: parse_synopsis is not imported here to avoid circular imports
# when running as a main module with python -m synopsis_pipeline.parse_synopsis

__all__ = [
    "WaterbodyRow",
    "ExtractionResults",
    "ParsedWaterbody",
    "SessionState",
    "ScopeObject",
    "RestrictionObject",
    "IdentityObject",
    "RuleGroup",
    "ReviewSession",
    "generate_report",
]
