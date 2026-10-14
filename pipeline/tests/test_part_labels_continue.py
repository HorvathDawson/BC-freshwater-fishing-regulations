"""A PART'S LOWER END IS "MOUTH" ONLY WHERE THE WATER TRULY ENDS (owner D6, RULINGS P10).

The Chilliwack's lowest part read "Mouth → Tamihi Rapids Bridge", but the Chilliwack does not end there:
it goes on downstream as the Vedder, which BEGINS at Vedder Crossing Bridge. Its natural lower end is
`continues:<item>` and the label names that place. That is a PURE NAME CHANGE, read from FWA topology
alone (`spans.lower_end_into`, no names): the next water's blue line BEGINS at the junction and the
water is all that feeds it there. Where two streams of comparable rank meet to form the next water
(Sproat + Stamp = Somass), each ends at the other's confluence ("Stamp River confluence"). A tributary
joining anywhere, even at the receiver's top, ends at a "Mouth"; a stream entering a lake ends there.

The synthetic tests need NO data (CI's no-data tier). The last pins the real waters on a built export
(`needs_bundle`).
"""
from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

from pipeline.deliver.answers import display as D
from pipeline.deliver.bundle import spans as SP


def _piece(nid, blk, lo, hi, order=1, mag=1, kind="stream"):
    """A graph node as `spans.compute` reads it: FWA stream order (Strahler) and magnitude (Shreve)
    are the piece's maxima, i.e. their values at its downstream end."""
    return NS(node_id=nid, kind=kind, blk=blk, down_m=lo, up_m=hi, length_m=hi - lo,
              stream_order=order, stream_magnitude=mag, lower_bound=None, upper_bound=None)


def _e(a, b, at, kind="confluence"):
    return NS(from_node=a, to_node=b, at_measure=float(at), kind=kind)


def _lower_ends(nodes, edges, items):
    """{item: the lower end token of its first piece}, through `spans.compute`."""
    handles = {n: i + 1 for i, n in enumerate(sorted(nodes))}
    its = [(o, w, k, [handles[n] for n in ns]) for o, (w, k, ns) in enumerate(items)]
    rows = {sid: lo for sid, _a, _b, lo, _hi, _o, _s in SP.compute(nodes, edges, handles, its, {})}
    return {w: rows[handles[ns[0]]] for w, _k, ns in items}


def test_a_pure_name_change_continues():
    """The Chilliwack (blk A: order 6, magnitude 2166) flows into the Vedder's top piece 26 m below
    where the Vedder's blue line begins, and nothing else feeds that piece (its magnitude is the
    Chilliwack's): the same water under another name. The Vedder's tributary C lower down: a mouth."""
    nodes = {"A:0": _piece("A:0", "A", 0, 5000, 6, 2166),
             "B:0": _piece("B:0", "B", 0, 1000, 6, 2170),
             "B:1000": _piece("B:1000", "B", 1000, 2026, 6, 2166),
             "C:0": _piece("C:0", "C", 0, 800, 2, 4)}
    edges = [_e("A:0", "B:1000", 2000), _e("B:1000", "B:0", 1000, "continuation"), _e("C:0", "B:0", 600)]
    ends = _lower_ends(nodes, edges, [("W", "stream", ["A:0"]), ("V", "stream", ["B:0", "B:1000"]),
                                      ("T", "stream", ["C:0"])])
    assert ends == {"W": "continues:V", "V": "mouth", "T": "mouth"}


def test_a_tributary_at_the_receivers_top_is_a_mouth():
    """Jr 4-1-2 (order 1) joins Jr 4-1 14 m below where Jr 4-1's blue line begins, the uppermost
    inflow. Jr 4-1's top carries more than it (magnitude 3 = its own head + Jr 4-1-1 + Jr 4-1-2): a
    tributary, and a tributary ends at a mouth wherever it joins."""
    nodes = {"A:0": _piece("A:0", "A", 0, 177, 1, 1), "B:0": _piece("B:0", "B", 0, 682, 2, 3),
             "C:0": _piece("C:0", "C", 0, 357, 1, 1)}
    edges = [_e("A:0", "B:0", 668), _e("C:0", "B:0", 92)]
    ends = _lower_ends(nodes, edges, [("Jr412", "stream", ["A:0"]), ("Jr41", "stream", ["B:0"]),
                                      ("Jr411", "stream", ["C:0"])])
    assert ends == {"Jr412": "mouth", "Jr41": "mouth", "Jr411": "mouth"}


def test_two_rivers_forming_a_third_meet_at_a_confluence():
    """The Sproat (order 5) and the Stamp (order 6) flow into the Somass's top piece 33 m and 64 m
    below where its blue line begins: neither goes on as the Somass alone. Each ends at the other's
    confluence. A creek of order 2 joining there as well is no partner to either: a mouth."""
    nodes = {"S:0": _piece("S:0", "S", 0, 1924, 5, 694), "T:0": _piece("T:0", "T", 0, 4019, 6, 1628),
             "K:0": _piece("K:0", "K", 0, 300, 2, 3),
             "M:0": _piece("M:0", "M", 0, 9199, 6, 2400),
             "M:9199": _piece("M:9199", "M", 9199, 9513, 6, 2326)}
    edges = [_e("S:0", "M:9199", 9480), _e("T:0", "M:9199", 9449), _e("K:0", "M:9199", 9500),
             _e("M:9199", "M:0", 9199, "continuation")]
    ends = _lower_ends(nodes, edges, [("Sproat", "stream", ["S:0"]), ("Stamp", "stream", ["T:0"]),
                                      ("Kc", "stream", ["K:0"]), ("Somass", "stream", ["M:0", "M:9199"])])
    assert ends == {"Sproat": "confluence:Stamp", "Stamp": "confluence:Sproat", "Kc": "mouth",
                    "Somass": "mouth"}


def test_a_creek_into_a_lake_whose_outlet_is_another_river_does_not_continue():
    """Still Creek flows into Burnaby Lake and the Brunette leaves it; FWA also links the creek's last
    piece straight to the Brunette's top (a connector). The creek ends at the lake: the Brunette is
    not it under another name (Station and Thomas Creeks enter the same lake)."""
    nodes = {"A:0": _piece("A:0", "A", 0, 32, 3, 35),
             "lake:9": _piece("lake:9", "", 0, 0, 4, 36, kind="lake"),
             "B:0": _piece("B:0", "B", 0, 7812, 3, 35)}
    edges = [_e("A:0", "lake:9", 9767, "lake_in"), _e("A:0", "B:0", 7812, "connector"),
             _e("lake:9", "B:0", 7812, "lake_out")]
    ends = _lower_ends(nodes, edges, [("Still", "stream", ["A:0"]), ("Brunette", "stream", ["B:0"])])
    assert ends["Still"] == "mouth", ends


def test_the_part_label_names_where_the_next_water_begins():
    doc = {"splits": {"vcb": {"name": "Vedder Crossing Bridge", "kind": "point"},
                      "trb": {"name": "Tamihi Rapids Bridge", "kind": "point"}},
           "waters": {"V": {"name": "Vedder River", "parts": [{"runs": [
               {"from": "vcb", "to": "mouth", "km_from": 12.9, "km_to": 0.0}]}]},
               "Y": {"name": "Bella Coola River", "parts": [{"runs": [
                   {"from": "source", "to": "mouth", "km_from": 60.0, "km_to": 0.0}]}]},
               "T": {"name": "Stamp River", "parts": []}}}
    chilliwack = {"runs": [{"from": "trb", "to": "continues:V", "km_from": 12.0, "km_to": 0.0}]}
    assert D.short_label(doc, chilliwack, "x") == "Vedder Crossing Bridge → Tamihi Rapids Bridge"
    assert D.runs_label(doc, chilliwack) == "From the Vedder Crossing Bridge up to the Tamihi Rapids Bridge"
    begins = {"runs": [{"from": "trb", "to": "continues:Y", "km_from": 20.0, "km_to": 0.0}]}
    assert D.short_label(doc, begins, "x") == "Bella Coola River begins → Tamihi Rapids Bridge"
    assert D.runs_label(doc, begins).startswith("From where it becomes the Bella Coola River up to")
    sproat = {"runs": [{"from": "trb", "to": "confluence:T", "km_from": 9.0, "km_to": 0.0}]}
    assert D.short_label(doc, sproat, "x") == "Stamp River confluence → Tamihi Rapids Bridge"


@pytest.mark.needs_bundle
def test_lower_ends_on_the_built_export(request):
    """Chilliwack -> Vedder continues (Vedder Crossing Bridge); Atnarko + Talchako and Sproat + Stamp
    form the Bella Coola and the Somass (confluence); Jr 4-1-2 is a tributary of Jr 4-1 (mouth)."""
    from pipeline.deliver.answers import common as C
    from pipeline.deliver.bundle import read as R
    from pipeline.tests.conftest import need, EXPORT_HINT
    from pipeline.tools.export_codec import expand
    d = Path(os.environ.get("ANSWERS_EXPORT_DIR") or Path(R.BUNDLE).parent.parent / "regs")
    need(request, "bundle", d / "ui-rules-export.json", EXPORT_HINT)
    data, guide = C.load_export(d)
    X = expand(data, guide)

    def lows(item):
        return sorted({r["to"] for p in X["waters"][item]["parts"] for r in p["runs"]
                       if r.get("km_to") == 0 and not r.get("branch")})
    assert lows("gnis:8634") == ["continues:gnis:3062"]                     # Chilliwack -> Vedder
    labels = [D.short_label(X, p, "") for p in X["waters"]["gnis:8634"]["parts"]]
    assert "Vedder Crossing Bridge → Tamihi Rapids Bridge" in labels, labels
    assert lows("gnis:17209") == ["confluence:gnis:2770"]                   # Atnarko: Talchako
    assert lows("gnis:25640") == ["confluence:gnis:14533"]                  # Sproat: Stamp
    assert lows("wsc:100-012330-767500-225700-978500") == ["mouth"]         # Jr 4-1-2
