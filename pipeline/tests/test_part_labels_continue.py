"""A PART'S LOWER END IS "MOUTH" ONLY WHERE THE WATER TRULY ENDS (owner D6, RULINGS P10).

The Chilliwack's lowest part read "Mouth → Tamihi Rapids Bridge", but the Chilliwack does not end there:
it goes on downstream as the Vedder, which BEGINS at Vedder Crossing Bridge. Its natural lower end is
`continues:<item>` and the label names that place. Where two streams of comparable rank meet to form
the next water (Sproat + Stamp = Somass), each ends at the other's confluence ("Stamp River
confluence"). A stream entering a lake ends there.

T108 (owner 2026-10-22: unify checks that decide the same thing): WHETHER A WATER GOES ON AS ANOTHER is
decided once, by `continuation.goes_on`, and the span end words read it (`continuation.lower_ends` ->
`spans.compute(ends=…)`): a `continues:` end is exactly a `water_continue` link, so the ▲ button and the
part label can no longer contradict each other. The synthetic tests run the build's own path — `links`
on the bundle tables and a graph, `settle`, `lower_ends`, `spans.compute` — with NO data (CI's no-data
tier). The last pins the real waters on a built export (`needs_bundle`).

Mutations each test catches (run by hand, T108; the `test_water_continue.py` twins in brackets):
  * `spans.compute` ignoring `ends` (the old `lo` kept): every `continues:` / `confluence:` test here.
  * `goes_on` without the same-name refusal: `test_no_water_continues_into_itself`
    [`test_no_water_goes_on_as_another_of_its_own_name`].
  * `goes_on` without the lake-outlet refusal: `test_a_creek_into_a_lake_whose_outlet_is_another_river_ends_at_the_lake`
    [`test_the_graph_flow_keeps_lakes_apart_and_marks_a_creek_that_ends_in_one`, which alone catches dropping the
    into-a-lake refusal].
  * `goes_on` without the two-rivers-a-little-below-the-top branch: `test_two_rivers_meeting_a_little_below_the_top_meet`
    [`test_two_rivers_meeting_just_below_the_top_is_a_confluence_past_the_stub`].
  * `lower_ends` without the comparable-rank test: `test_two_rivers_forming_a_third_meet_at_a_confluence` (the creek)
    [`test_the_span_end_words_are_the_links`].
  * a tolerance re-added to `spans.py`: [`test_no_other_bundle_module_decides_continuation`].
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

from pipeline.deliver.answers import display as D
from pipeline.deliver.bundle import continuation as C
from pipeline.deliver.bundle import spans as SP


def _piece(nid, blk, lo, hi, order=1, mag=1, kind="stream"):
    """A graph node as `spans.compute` and `continuation.graph_flow` read it: FWA stream order (Strahler) and magnitude
    (Shreve) are the piece's maxima, i.e. their values at its downstream end."""
    return NS(node_id=nid, kind=kind, blk=blk, down_m=lo, up_m=hi, length_m=hi - lo, wsc="",
              stream_order=order, stream_magnitude=mag, lower_bound=None, upper_bound=None)


def _e(a, b, at, kind="confluence"):
    return NS(from_node=a, to_node=b, at_measure=float(at), kind=kind)


def _lower_ends(nodes, edges, items, item_down=None, paired=(), names=None):
    """{item: the lower end token of its first piece}, and the settled `water_continue` links as (item, item) — through
    the build's own path: `continuation.links` on the bundle tables and the graph, `settle` with the book's pairings
    `paired` (item pairs), `lower_ends`, `spans.compute`. `item_down` defaults to every stream flowing into every other
    its first piece has an edge into; `names` to the item ids."""
    ids = sorted(nodes)
    handles = {n: i + 1 for i, n in enumerate(ids)}
    its = [(o + 1, w, k, [handles[n] for n in ns]) for o, (w, k, ns) in enumerate(items)]
    ordof = {w: o for o, w, _k, _s in its}
    owner = {n: w for w, _k, ns in items for n in ns}
    if item_down is None:
        item_down = {(owner[e.from_node], owner[e.to_node]) for e in edges
                     if e.from_node in owner and e.to_node in owner and owner[e.from_node] != owner[e.to_node]}
    db = sqlite3.connect(":memory:")
    db.executescript("CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT, name TEXT, kind TEXT);"
                     "CREATE TABLE item_section (ord INTEGER, sid INTEGER);"
                     "CREATE TABLE item_down (ord INTEGER, down_ord INTEGER);")
    db.executemany("INSERT INTO item VALUES (?,?,?,?)",
                   [(o, w, (names or {}).get(w, w), k) for o, w, k, _s in its])
    db.executemany("INSERT INTO item_section VALUES (?,?)", [(o, s) for o, _w, _k, ss in its for s in ss])
    db.executemany("INSERT INTO item_down VALUES (?,?)", [(ordof[a], ordof[b]) for a, b in sorted(item_down)])
    up, down = {}, {}
    for ix, e in enumerate(edges):
        up.setdefault(e.to_node, []).append(ix)
        down.setdefault(e.from_node, []).append(ix)
    graph = NS(nodes=nodes, edges=edges, up_adj=up, down_adj=down)
    found = C.links(db, ids, graph)
    settled = C.settle(found, {(ordof[a], ordof[b]) for a, b in paired})
    item_of = {o: w for o, w, _k, _s in its}
    ends = C.lower_ends(found, settled, item_of, C.ranks(graph, ids, {h for r in found for h in (r[1], r[3])}))
    rows = {sid: lo for sid, _a, _b, lo, _hi, _o, _s in SP.compute(nodes, edges, handles, its, {}, ends=ends)}
    return ({w: rows[handles[ns[0]]] for w, _k, ns in items if handles[ns[0]] in rows},
            sorted((item_of[a], item_of[b]) for a, _s, b, _t in settled))


def test_a_pure_name_change_continues():
    """The Chilliwack (blk A: order 6, magnitude 2166) flows into the Vedder's top piece 26 m below
    where the Vedder's blue line begins, and nothing else feeds that piece: the same water under another name. The
    Vedder's tributary C lower down: a mouth. The end word and the `water_continue` link are one answer."""
    nodes = {"A:0": _piece("A:0", "A", 0, 5000, 6, 2166),
             "B:0": _piece("B:0", "B", 0, 1000, 6, 2170),
             "B:1000": _piece("B:1000", "B", 1000, 2026, 6, 2166),
             "C:0": _piece("C:0", "C", 0, 800, 2, 4)}
    edges = [_e("A:0", "B:1000", 2000), _e("B:1000", "B:0", 1000, "continuation"), _e("C:0", "B:0", 600)]
    ends, links = _lower_ends(nodes, edges, [("W", "stream", ["A:0"]), ("V", "stream", ["B:0", "B:1000"]),
                                             ("T", "stream", ["C:0"])])
    assert (ends, links) == ({"W": "continues:V", "V": "mouth", "T": "mouth"}, [("W", "V")])


def test_a_creek_alone_at_the_receivers_top_goes_on_as_it():
    """REVIEWED BEHAVIOUR CHANGE (T108). Jr 4-1-2 (order 1) joins Jr 4-1 14 m below where Jr 4-1's blue line begins;
    Jr 4-1-1 joins 576 m lower. The old span rule (a magnitude test, no names) called it a tributary's "Mouth" while
    `water_continue` linked it — the owner kept those municipal continuations (p15, 2026-10-21 late), so the one
    answer is "goes on as": `continues:Jr41`, and the ▲ button agrees."""
    nodes = {"A:0": _piece("A:0", "A", 0, 177, 1, 1), "B:0": _piece("B:0", "B", 0, 682, 2, 3),
             "C:0": _piece("C:0", "C", 0, 357, 1, 1)}
    edges = [_e("A:0", "B:0", 668), _e("C:0", "B:0", 92)]
    ends, links = _lower_ends(nodes, edges, [("Jr412", "stream", ["A:0"]), ("Jr41", "stream", ["B:0"]),
                                             ("Jr411", "stream", ["C:0"])])
    assert (ends, links) == ({"Jr412": "continues:Jr41", "Jr41": "mouth", "Jr411": "mouth"}, [("Jr412", "Jr41")])


def test_no_water_continues_into_itself():
    """Jeperson Side Channel 'continued as' Jeperson Side Channel: two items (a gazetted name and an FWA line carrying
    it) of one water. Same shape as the Jr 4-1-2, same name: no link, and a plain mouth."""
    nodes = {"A:0": _piece("A:0", "A", 0, 177, 1, 1), "B:0": _piece("B:0", "B", 0, 682, 2, 3)}
    edges = [_e("A:0", "B:0", 668)]
    items = [("gnis:1", "stream", ["A:0"]), ("blk:2", "stream", ["B:0"])]
    names = {"gnis:1": "Jeperson Side Channel", "blk:2": "Jeperson  side channel"}
    assert _lower_ends(nodes, edges, items, names=names) == ({"gnis:1": "mouth", "blk:2": "mouth"}, [])
    assert _lower_ends(nodes, edges, items)[1] == [("gnis:1", "blk:2")]          # other names: it goes on


def test_two_rivers_forming_a_third_meet_at_a_confluence():
    """The Sproat (order 5) and the Stamp (order 6) flow into the Somass's top piece 33 m and 64 m
    below where its blue line begins: neither goes on as the Somass alone. Each ends at the other's
    confluence. A creek of order 2 joining there as well is no partner to either (P10's comparable rank): a mouth."""
    nodes = {"S:0": _piece("S:0", "S", 0, 1924, 5, 694), "T:0": _piece("T:0", "T", 0, 4019, 6, 1628),
             "K:0": _piece("K:0", "K", 0, 300, 2, 3),
             "M:0": _piece("M:0", "M", 0, 9199, 6, 2400),
             "M:9199": _piece("M:9199", "M", 9199, 9513, 6, 2326)}
    edges = [_e("S:0", "M:9199", 9480), _e("T:0", "M:9199", 9449), _e("K:0", "M:9199", 9500),
             _e("M:9199", "M:0", 9199, "continuation")]
    ends, links = _lower_ends(nodes, edges, [("Sproat", "stream", ["S:0"]), ("Stamp", "stream", ["T:0"]),
                                             ("Kc", "stream", ["K:0"]), ("Somass", "stream", ["M:0", "M:9199"])])
    assert (ends, links) == ({"Sproat": "confluence:Stamp", "Stamp": "confluence:Sproat", "Kc": "mouth",
                              "Somass": "mouth"}, [])


def test_the_river_the_book_pairs_goes_on_and_the_other_meets_it():
    """The Atnarko and the Talchako make the Bella Coola; "ATNARKO/BELLA COOLA RIVERS" names the Atnarko (T59). The
    Atnarko's end now says what its ▲ says — it becomes the Bella Coola (REVIEWED BEHAVIOUR CHANGE, T108: it read
    "Talchako River confluence") — and the Talchako ends at the Atnarko's confluence."""
    nodes = {"A:0": _piece("A:0", "A", 0, 9000, 6, 900), "T:0": _piece("T:0", "T", 0, 7000, 6, 800),
             "B:0": _piece("B:0", "B", 0, 600, 7, 1700)}
    edges = [_e("A:0", "B:0", 590), _e("T:0", "B:0", 560)]
    items = [("Atnarko", "stream", ["A:0"]), ("Talchako", "stream", ["T:0"]), ("BellaCoola", "stream", ["B:0"])]
    assert _lower_ends(nodes, edges, items, paired={("Atnarko", "BellaCoola")}) == (
        {"Atnarko": "continues:BellaCoola", "Talchako": "confluence:Atnarko", "BellaCoola": "mouth"},
        [("Atnarko", "BellaCoola")])
    # unpaired: two rivers meet, each at the other's confluence
    assert _lower_ends(nodes, edges, items)[0] == {"Atnarko": "confluence:Talchako", "Talchako": "confluence:Atnarko",
                                                   "BellaCoola": "mouth"}


def test_two_rivers_meeting_a_little_below_the_top_meet():
    """The Nakina joins the Taku 65 m below its top and the Inklin 154 m: the Inklin is past the 100 m stub, but the
    Nakina is within it, so the two rivers meet there (`goes_on`, one constant set: STUB_M and CONFLUENCE_M). 250 m
    below, the Inklin is a tributary: the Nakina meets no river it can name."""
    nodes = {"I:0": _piece("I:0", "I", 0, 9000, 6, 900), "N:0": _piece("N:0", "N", 0, 7000, 6, 800),
             "T:0": _piece("T:0", "T", 0, 1000, 7, 1700)}
    items = [("Inklin", "stream", ["I:0"]), ("Nakina", "stream", ["N:0"]), ("Taku", "stream", ["T:0"])]
    assert _lower_ends(nodes, [_e("I:0", "T:0", 846), _e("N:0", "T:0", 935)], items)[0] == {
        "Inklin": "confluence:Nakina", "Nakina": "confluence:Inklin", "Taku": "mouth"}
    assert _lower_ends(nodes, [_e("I:0", "T:0", 750), _e("N:0", "T:0", 935)], items)[0] == {
        "Inklin": "mouth", "Nakina": "mouth", "Taku": "mouth"}


def test_a_creek_into_a_lake_whose_outlet_is_another_river_ends_at_the_lake():
    """Still Creek flows into Burnaby Lake and the Brunette leaves it; FWA also links the creek's last
    piece straight to the Brunette's top (a connector). The creek ends at the lake: the Brunette is
    not it under another name (Station and Thomas Creeks enter the same lake)."""
    nodes = {"A:0": _piece("A:0", "A", 0, 32, 3, 35),
             "lake:9": _piece("lake:9", "", 0, 0, 4, 36, kind="lake"),
             "B:0": _piece("B:0", "B", 0, 7812, 3, 35)}
    edges = [_e("A:0", "lake:9", 9767, "lake_in"), _e("A:0", "B:0", 7812, "connector"),
             _e("lake:9", "B:0", 7812, "lake_out")]
    ends, links = _lower_ends(nodes, edges, [("Still", "stream", ["A:0"]), ("Brunette", "stream", ["B:0"])],
                              item_down={("Still", "Brunette")})
    assert (ends["Still"], links) == ("mouth", [])
    # the creek's own link to the lake is what ends it: without it, a lake draining in at the Brunette's top still
    # says the Brunette begins at a lake — not the creek going on
    ends, links = _lower_ends(nodes, edges[1:], [("Still", "stream", ["A:0"]), ("Brunette", "stream", ["B:0"])],
                              item_down={("Still", "Brunette")})
    assert (ends["Still"], links) == ("mouth", [])


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
    """Chilliwack -> Vedder continues (Vedder Crossing Bridge); the Atnarko goes on as the Bella Coola (the
    book pairs them, T59) and the Talchako meets it; Sproat + Stamp form the Somass (confluence); Jr 4-1-2 goes
    on as Jr 4-1 (p15, T108)."""
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
    assert lows("gnis:17209") == ["continues:gnis:11611"]                   # Atnarko -> Bella Coola
    assert lows("gnis:2770") == ["confluence:gnis:17209"]                   # Talchako: Atnarko
    assert lows("gnis:25640") == ["confluence:gnis:14533"]                  # Sproat: Stamp
    assert lows("wsc:100-012330-767500-225700-978500") == [                 # Jr 4-1-2 -> Jr 4-1
        "continues:wsc:100-012330-767500-225700"]
