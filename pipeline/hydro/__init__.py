"""Gauges: which water can be told what its flow is, and how honestly."""

from pipeline.hydro.match import StationMatch, load_aliases, nodes_for, summarise
from pipeline.hydro.shed import (
    TRUST_BANDS, GaugeLink, build_gauge_sheds, is_lake_station, lake_gauge_links, trust_for,
)

__all__ = ["TRUST_BANDS", "GaugeLink", "StationMatch", "build_gauge_sheds",
           "is_lake_station", "lake_gauge_links", "load_aliases", "nodes_for",
           "summarise", "trust_for"]
