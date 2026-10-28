"""T224 (owner 2026-10-28): EVERY NAME CARRIES ITS REAL SOURCE, in words a reader understands, and only a
gazetted name is official. One label per `NameSource` code; a name may carry a more specific label of its own
(`NameTuple.label`: "City of Burnaby waterways map (2021)").

A name is never labelled with a source it did not come from: a city's or OpenStreetMap's name is never shown as
official, and `alias` ("source not recorded") is what is left to eliminate.
"""
from __future__ import annotations

from pipeline.common.models.enums import NameSource

LABELS: dict[NameSource, str] = {
    NameSource.override: "Project curation (owner decision)",
    NameSource.gazette: "BC Geographical Names (official)",
    NameSource.side_channel: "Main channel's official name (inherited)",
    NameSource.gauge: "ECCC hydrometric station name",
    NameSource.regulation: "Freshwater Fishing Synopsis",
    NameSource.stocking: "Freshwater Fisheries Society stocking records",
    NameSource.bathymetry: "BC lake bathymetry map",
    NameSource.marker: "BC lake bathymetry map label",
    NameSource.vessel_restriction: "Vessel Operation Restriction Regulations",
    NameSource.lake_survey: "BC lake survey",
    NameSource.dfo: "DFO salmon regulations",
    NameSource.municipal: "Municipal stream layer",
    NameSource.osm: "OpenStreetMap",
    NameSource.alias: "Alternate name (source not recorded)",
    NameSource.synopsis: "Freshwater Fishing Synopsis",
}

#: the city stream layers (`added_streams` source key -> label)
CITY_LABELS = {
    "abbotsford": "City of Abbotsford stream layer",
    "burnaby": "City of Burnaby waterways layer",
    "port_moody": "City of Port Moody ESA streams layer",
    "squamish": "District of Squamish watercourses layer",
}


def label_of(source: NameSource | str) -> str:
    try:
        return LABELS[NameSource(source)]
    except ValueError:
        return LABELS[NameSource.alias]


def is_official(source: NameSource | str) -> bool:
    return str(getattr(source, "value", source)) == NameSource.gazette.value
