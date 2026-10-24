"""Tolerances that must agree are declared once (plan 2.10): `pipeline/common/tolerances.py`.

Pins the values (unchanged from the copies they replaced), that every former copy IS the shared value, that no
former home declares its own literal again, and `at_cut`'s edge. No data.
"""

from __future__ import annotations

import importlib
import re
from pathlib import Path

import pytest

from pipeline.common import tolerances as T

ROOT = Path(__file__).resolve().parents[2]

#: (module, its name for it, the shared name)
COPIES = [
    ("pipeline.atlas.splits.sectionizer", "SAME_PLACE_M", "SAME_PLACE_M"),
    ("pipeline.atlas.splits.clean_cut", "SAME_PLACE_M", "SAME_PLACE_M"),
    ("pipeline.atlas.splits.sliver_gate", "SLIVER_M", "SLIVER_M"),
    ("pipeline.atlas.splits.sectionizer", "_MERGE_PROXIMITY_M", "SLIVER_M"),
    ("pipeline.atlas.splits.clean_cut", "DIP_M", "SLIVER_M"),
    ("pipeline.atlas.reach.extent", "WATERSHED_AT_CUT_M", "WATERSHED_AT_CUT_M"),
    ("pipeline.deliver.bundle.spans", "_AT_M", "AT_CUT_M"),
    ("pipeline.deliver.bundle.topology", "TOUCH_M", "TOUCH_M"),
    ("pipeline.tools.water_map_export", "TOUCH_M", "TOUCH_M"),
    ("pipeline.atlas.graph.tributaries", "CONFLUENCE_NEAR_M", "CONFLUENCE_NEAR_M"),
    ("pipeline.deliver.bundle.continuation", "STUB_M", "STUB_M"),
    ("pipeline.deliver.bundle.continuation", "CONFLUENCE_M", "CONFLUENCE_M"),
    ("pipeline.deliver.bundle.continuation", "OWN_M", "OWN_M"),
]


def test_the_values_are_the_ones_the_copies_held():
    assert (T.SAME_PLACE_M, T.SLIVER_M, T.AT_CUT_M, T.WATERSHED_AT_CUT_M, T.TOUCH_M) == (1e-6, 5.0, 1.0, 5.0, 60.0)
    assert (T.CONFLUENCE_NEAR_M, T.STUB_M, T.CONFLUENCE_M, T.OWN_M) == (150.0, 100.0, 200.0, 1000.0)


@pytest.mark.parametrize("module,name,shared", COPIES)
def test_every_former_copy_is_the_shared_value(module, name, shared):
    assert getattr(importlib.import_module(module), name) == getattr(T, shared)


@pytest.mark.parametrize("module,name,shared", COPIES)
def test_no_former_home_declares_a_literal_again(module, name, shared):
    src = (ROOT / (module.replace(".", "/") + ".py")).read_text(encoding="utf-8")
    assert not re.search(rf"^{re.escape(name)}\s*=\s*[\d.]", src, re.M), f"{module}.{name} is a literal again"


def test_at_cut_is_strictly_closer_than_the_tolerance():
    assert T.at_cut(10.0, 10.0) and T.at_cut(10.0, 10.999) and T.at_cut(10.999, 10.0)
    assert not T.at_cut(10.0, 11.0) and not T.at_cut(11.0, 10.0)
    assert T.at_cut(0.0, 4.9, tol_m=5.0) and not T.at_cut(0.0, 5.0, tol_m=5.0)
