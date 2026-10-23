"""FWA EDGE_TYPEs — the ONE place their numbers are named (T117, owner 2026-10-22).

FWA draws every stream fid with an EDGE_TYPE saying what kind of line it is. Three questions are asked of
them, and before this module each was asked with its own literal set in the module that asked it — the
tributary walk's main flow ({1000, 1050, 1200, 1250}), the gauge panels' side channel ({1100, 1350}), the
model's barrier ("2300") — so nothing showed whether they agreed. They are named here, side by side:

  * MAIN FLOW — the river's own line: single line (1000), in a wetland (1050), the construction lines of
    a double-line river (1200, 1250). A lake outlet carrying one is the lake's river, not a distributary
    (`tributaries._main_outlets`).
  * SIDE CHANNEL — a secondary channel of a bigger river, not a river of its own (1100, 1350; 28,903
    sections, median magnitude 1). The gauge panels never size one by its own catchment
    (`build_panels.mainstem_of`).
  * BARRIER — an artificial connector (2300): a canal. The guarded tributary walk never enters one, nor
    anything above it (`tributaries.may_enter`).

1150 (secondary flow in a wetland) is in NEITHER set: not main flow (a lake's outlet carrying only 1150 is
a distributary — Dewar Lake's side channel is 1100/1150), and not a side channel for the panels. That is
how the two sets stood when they were gathered here, kept as they were (a pure move); whether the panels
should treat 1150 as a side channel too is an open owner question (T117).
"""
from __future__ import annotations

from typing import Iterable

#: an ordinary single-line stream — what an added (non-FWA) stream is given
SINGLE_LINE = "1000"
#: the artificial connector: a canal, and the walk's barrier
CONNECTOR = "2300"

MAIN_FLOW = frozenset({"1000", "1050", "1200", "1250"})
SIDE_CHANNEL = frozenset({"1100", "1350"})


def _types(x) -> frozenset[str]:
    """A node's `edge_types`, or an iterable of them."""
    if isinstance(x, str):
        return frozenset({x})
    got = getattr(x, "edge_types", x)
    return frozenset(got or ())


def is_main_flow(x: Iterable[str] | object) -> bool:
    """Does the node (or edge-type set) carry a main-flow line?"""
    return bool(_types(x) & MAIN_FLOW)


def is_side_channel(x: Iterable[str] | object) -> bool:
    """Does the node (or edge-type set) carry a side-channel line?"""
    return bool(_types(x) & SIDE_CHANNEL)


def is_barrier(x: Iterable[str] | object) -> bool:
    """Does the node (or edge-type set) carry an artificial connector (a canal)?"""
    return CONNECTOR in _types(x)
