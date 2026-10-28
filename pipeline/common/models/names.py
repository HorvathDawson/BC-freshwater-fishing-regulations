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
    #: T224: who gave the name, in words ("City of Abbotsford stream layer"); empty = the source's own label
    #: (`common.name_sources.LABELS`). `ref` points at the source row (a file#id, a station, a URL).
    label: str = ""
    ref: str = ""

    @property
    def official(self) -> bool:
        """Only a gazetted (BC Geographical Names) name is official — never a regulation's, a city's or OSM's."""
        return self.source == NameSource.gazette

    @property
    def source_label(self) -> str:
        from pipeline.common.name_sources import label_of
        return self.label or label_of(self.source)
