"""A PART'S LOWER END IS "MOUTH" ONLY WHERE THE WATER TRULY ENDS (owner 2026-10-12, answers 2.8).

The Chilliwack's lowest part read "Mouth → Tamihi Rapids Bridge", but the Chilliwack does not end there:
it goes on downstream as the Vedder, which BEGINS at Vedder Crossing Bridge. A natural lower end that
flows into another named stream at that stream's TOP is `continues:<item>` (`spans.continues_as`), and the
part label names the place where that water begins ("Vedder Crossing Bridge → Tamihi Rapids Bridge");
a water joining another river part way up keeps "Mouth".

The first two tests need NO data (CI's no-data tier): a synthetic graph and a synthetic export doc. The
last pins the real Chilliwack and the Atnarko / Bella Coola on a built bundle (`needs_bundle`).
"""
from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

from pipeline.deliver.answers import display as D
from pipeline.deliver.bundle import spans as SP


def _piece(nid, blk, lo, hi):
    return NS(node_id=nid, kind="stream", blk=blk, down_m=lo, up_m=hi, length_m=hi - lo,
              lower_bound=None, upper_bound=None)


def _graph():
    """W (blk A) flows into V (blk B) 25 m below V's stem top (an FWA stub), the uppermost inflow; T
    (blk C) joins V part way up; U (blk D) joins just below W at the top: a mouth; V ends at the sea."""
    nodes = {"A:0": _piece("A:0", "A", 0, 5000), "B:0": _piece("B:0", "B", 0, 1000),
             "B:1000": _piece("B:1000", "B", 1000, 2025), "C:0": _piece("C:0", "C", 0, 800),
             "D:0": _piece("D:0", "D", 0, 300)}
    edges = [NS(from_node="A:0", to_node="B:1000", at_measure=2000.0, kind="confluence"),
             NS(from_node="D:0", to_node="B:1000", at_measure=1980.0, kind="confluence"),
             NS(from_node="C:0", to_node="B:0", at_measure=600.0, kind="confluence"),
             NS(from_node="B:1000", to_node="B:0", at_measure=1000.0, kind="confluence")]
    handles = {"A:0": 1, "B:0": 2, "B:1000": 3, "C:0": 4, "D:0": 5}
    items = [(1, "W", "stream", [1]), (2, "V", "stream", [2, 3]), (3, "T", "stream", [4]),
             (4, "U", "stream", [5])]
    return nodes, edges, handles, items


def test_a_water_that_goes_on_as_another_named_stream_does_not_end_at_a_mouth():
    nodes, edges, handles, items = _graph()
    rows = {sid: (lo, hi) for sid, _a, _b, lo, hi, _o, _s in SP.compute(nodes, edges, handles, items, {})}
    assert rows[1] == ("continues:V", "source")      # W becomes V at V's top
    assert rows[4] == ("mouth", "source")             # T joins V part way up: a true mouth
    assert rows[5] == ("mouth", "source")             # U joins just below W, at V's top: a mouth
    assert rows[2][0] == "mouth"                      # V ends (the sea)


def test_the_part_label_names_where_the_next_water_begins():
    doc = {"splits": {"vcb": {"name": "Vedder Crossing Bridge", "kind": "point"},
                      "trb": {"name": "Tamihi Rapids Bridge", "kind": "point"}},
           "waters": {"V": {"name": "Vedder River", "parts": [{"runs": [
               {"from": "vcb", "to": "mouth", "km_from": 12.9, "km_to": 0.0}]}]},
               "Y": {"name": "Bella Coola River", "parts": [{"runs": [
                   {"from": "source", "to": "mouth", "km_from": 60.0, "km_to": 0.0}]}]}}}
    chilliwack = {"runs": [{"from": "trb", "to": "continues:V", "km_from": 12.0, "km_to": 0.0}]}
    assert D.short_label(doc, chilliwack, "x") == "Vedder Crossing Bridge → Tamihi Rapids Bridge"
    assert D.runs_label(doc, chilliwack) == "From the Vedder Crossing Bridge up to the Tamihi Rapids Bridge"
    atnarko = {"runs": [{"from": "trb", "to": "continues:Y", "km_from": 20.0, "km_to": 0.0}]}
    assert D.short_label(doc, atnarko, "x") == "Bella Coola River begins → Tamihi Rapids Bridge"
    assert D.runs_label(doc, atnarko).startswith("From where it becomes the Bella Coola River up to")


@pytest.mark.needs_bundle
def test_chilliwack_and_atnarko_lower_ends_on_the_built_export(request):
    from pipeline.deliver.answers import common as C
    from pipeline.deliver.bundle import read as R
    from pipeline.tests.conftest import need, EXPORT_HINT
    from pipeline.tools.export_codec import expand
    d = Path(os.environ.get("ANSWERS_EXPORT_DIR") or Path(R.BUNDLE).parent.parent / "regs")
    need(request, "bundle", d / "ui-rules-export.json", EXPORT_HINT)
    data, guide = C.load_export(d)
    X = expand(data, guide)
    chill = X["waters"]["gnis:8634"]
    lows = [r["to"] for p in chill["parts"] for r in p["runs"] if r.get("km_to") == 0 and not r.get("branch")]
    assert lows == ["continues:gnis:3062"], lows
    labels = [D.short_label(X, p, "") for p in chill["parts"]]
    assert "Vedder Crossing Bridge → Tamihi Rapids Bridge" in labels, labels
    atn = [r["to"] for p in X["waters"]["gnis:17209"]["parts"] for r in p["runs"]
           if r.get("km_to") == 0 and not r.get("branch")]
    assert atn and all(t != "mouth" for t in atn), atn          # the Atnarko becomes the Bella Coola
