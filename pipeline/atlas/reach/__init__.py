"""Reach builder — curated intent (authored extents) → per-build section bindings.

A regulation says "between the Tamihi Bridge and Vedder Crossing". The map is made of
numbered pieces of river. This package turns the first into the second, for every rule, and
reports what changed since the previous build.

It does NOT cut rivers: it only selects among sections the sectionizer already made.

    extent       ONE authored extent  -> section ids            (the primitive)
    tributaries  a reach              -> the water draining into it
    covered      an entry             -> the registry items it regulates
    classify     resolved extents     -> bound | unresolved(reason) + diagnostics
    build        the whole corpus     -> bindings + a report
    io / diff    write a run          -> tables, and what changed since last time
    cache        one entry            -> its result, keyed on entry + build + policy

The review app calls the same `classify` and the same tributary expander, so the bundle and
the app can never disagree about what a rule covers.

Design: `pipeline/docs/REACH-BUILDER.md`.
"""

from pipeline.atlas.reach.models import (
    BuildReport, Diagnostic, Outcome, Reason, RuleBinding,
)

__all__ = ["BuildReport", "Diagnostic", "Outcome", "Reason", "RuleBinding"]
