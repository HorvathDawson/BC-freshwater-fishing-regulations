"""Provenance-tagged names carried on chains / nodes / sections."""

from __future__ import annotations

from dataclasses import dataclass

from pipeline.common.models.enums import NameSource


@dataclass(frozen=True)
class NameTuple:
    """One provenance-tagged name. Display = first by NameSource priority; search = all.
    ``note`` carries free-text provenance/context (why the variant was added, from the source).
    ``gnis_id`` = the GNIS this name belongs to when known (e.g. a side-channel inherits the main
    channel's name AND its gnis) — the node's own ``gnis_id`` field stays empty so the channels stay
    distinguishable, but consumers (the registry) can still group the whole river by this gnis."""
    name: str
    source: NameSource
    note: str = ""
    gnis_id: str = ""
