"""An automatic cut is labelled by its kind and specifics, never by the water's name (T189 follow-up).

`reuse.cut_label` reads the kind from the boundary id (`sectionizer.minted_id`'s families); the
labels below are the ones a reviewer reads on hover/tap on Iskut River's seven length cuts, a gauge,
a region line, a park boundary and a lake edge.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace as NS

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "curation-review" / "backend"))
import reuse  # noqa: E402


def b(id_, label="", kind="confluence"):
    return NS(id=id_, label=label, kind=kind)


def test_length_cut_says_where_along_the_line():
    assert reuse.cut_label(b("length:360884856:11466", "Iskut River"), None, "Iskut River") == "length cut · 11.5 km"
    assert (reuse.cut_label(b("length:360884856:70305", "McLymont Creek"), None, "Iskut River")
            == "length cut · 70.3 km · McLymont Creek")


def test_gauge_names_station_and_site():
    assert (reuse.cut_label(b("gauge__08HB023", "08HB023 · Ash River Below Moran Creek"))
            == "gauge 08HB023 · Ash River Below Moran Creek")


def test_area_cuts():
    assert reuse.cut_label(b("area:6", "Region 5 – Region 6 boundary")) == "Region 6 line"
    assert reuse.cut_label(b("area:Glacier National Park of Canada")) == "Glacier National Park of Canada boundary"
    assert reuse.cut_label(b("area:Coquitlam Watershed [13480215]")) == "Coquitlam Watershed boundary"


def test_border_and_lake_edge():
    assert reuse.cut_label(b("border:356566436:0", "BC boundary")) == "BC boundary"
    assert reuse.cut_label(b("iskut_river__tatogga_lake", "Tatogga Lake", "lake")) == "lake edge · Tatogga Lake"
    assert reuse.cut_label(b("iskut_river__lake_1", "lake 329305544", "lake")) == "lake edge"
