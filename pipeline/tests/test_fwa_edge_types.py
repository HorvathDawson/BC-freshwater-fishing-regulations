"""T117 — FWA EDGE_TYPE numbers are named in ONE module, `pipeline.common.fwa_edge_types`.

Mutations, each named where it fails:
  M1 1150 moved into either set (or any number between sets) -> test_the_three_sets_whole
  M2 an edge-type number written in another module again     -> test_no_module_outside_names_an_edge_type_number
  M3 the model's barrier read its own literal again          -> test_the_model_and_the_walk_read_the_one_barrier
"""
from __future__ import annotations

import re
from pathlib import Path

from pipeline.common import fwa_edge_types as E


def test_the_three_sets_whole():
    assert (E.MAIN_FLOW, E.SIDE_CHANNEL, E.CONNECTOR, E.SINGLE_LINE) == (
        frozenset({"1000", "1050", "1200", "1250"}), frozenset({"1100", "1350"}), "2300", "1000")
    probe = ("1000", "1050", "1100", "1150", "1200", "1250", "1350", "2300")
    assert [(t, E.is_main_flow([t]), E.is_side_channel([t]), E.is_barrier([t])) for t in probe] == [
        ("1000", True, False, False), ("1050", True, False, False), ("1100", False, True, False),
        ("1150", False, False, False), ("1200", True, False, False), ("1250", True, False, False),
        ("1350", False, True, False), ("2300", False, False, True)]


def test_a_node_or_its_types_or_none():
    class N:
        edge_types = ("1100", "2300")
    assert (E.is_side_channel(N()), E.is_barrier(N()), E.is_main_flow(N()), E.is_barrier(None),
            E.is_main_flow(())) == (True, True, False, False, False)


def test_the_model_and_the_walk_read_the_one_barrier(monkeypatch):
    """M3: `StreamNode.is_barrier` asks this module."""
    from pipeline.common.models import graph as G
    from pipeline.common.models import NodeKind, StreamNode
    n = StreamNode(node_id="a", kind=NodeKind.stream, edge_types=("1000",))
    assert n.is_barrier is False
    monkeypatch.setattr(G, "_fwa_is_barrier", lambda types: True)
    assert n.is_barrier is True


ROOT = Path(__file__).resolve().parents[2]
_NUMBER = re.compile(r"""["'](1000|1050|1100|1150|1200|1250|1300|1350|1400|1410|1425|1450|1475|1500|1525|1550|"""
                     r"""1600|1700|1800|1850|1875|1900|1950|2000|2100|2300)["']""")


def test_no_module_outside_names_an_edge_type_number():
    """M2: no module but `fwa_edge_types.py` writes an EDGE_TYPE number as a string (comments aside)."""
    bad = []
    for top in ("pipeline", "curation-review"):
        for p in sorted((ROOT / top).rglob("*.py")):
            rel = p.relative_to(ROOT).as_posix()
            if "/tests/" in rel or "/archive/" in rel or rel == "pipeline/common/fwa_edge_types.py":
                continue
            for i, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1):
                if _NUMBER.search(line.split("#", 1)[0]):
                    bad.append(f"{rel}:{i}: {line.strip()}")
    assert bad == []
