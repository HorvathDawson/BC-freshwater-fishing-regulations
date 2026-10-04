""""Other parts" — `Extent` op `rest`: the rule's water minus what its named siblings bind.

Bull River's "Other parts: trout/char daily quota = 1 (none under 30 cm), June 15-Oct 31" is the
everyday quota for most of the river. Held as an undrawn part it never decided anything, and an
angler there got only the region's numbers. As `rest` it binds the water the row covers (with the
rule's own tributary scope) minus the sections its siblings bind, their walks included.

Also here, because it is the same review: a place phrase never carries a rule's value, and a part
the book never identifies ("on parts") is never quoted as a place.

Each guard is pinned by a mutation: switch it off and a test goes red.
"""
from __future__ import annotations

import pytest

from pipeline.atlas.reach import build as B
from pipeline.atlas.reach import classify as C
from pipeline.atlas.reach.build import build_reach
from pipeline.atlas.reach.models import Outcome, Reason
from pipeline.common.models import NodeKind, StreamGraph, StreamNode
from pipeline.common.models.registry import RegistryItem
from pipeline.regs.parsing.catalogue import (
    CatalogueEntry, CatalogueRule, label_parts, part_identifies_place, part_words,
)
from pipeline.regs.parsing.entry_models import Extent


# ------------------------------------------------------------------------------------ fixtures
def _node(nid):
    return StreamNode(node_id=nid, kind=NodeKind.stream, blk="1", down_m=0.0, up_m=1.0,
                      length_m=1.0)


def _item(iid, secs):
    return RegistryItem(id=iid, name=iid, kind="stream", section_ids=tuple(secs))


# A river of four pieces; `reach` is the stretch one sibling names (as an item, so no cut-points
# are needed); `quinn` a tributary the other sibling names whole; `t1` a tributary of the reach,
# `t2` a tributary of the rest.
SECS = ["m1", "m2", "m3", "m4", "q1", "t1", "t2"]
G = StreamGraph()
G.nodes = {s: _node(s) for s in SECS}
REG = {"gnis:river": _item("gnis:river", ["m1", "m2", "m3", "m4"]),
       "gnis:reach": _item("gnis:reach", ["m2", "m3"]),
       "gnis:quinn": _item("gnis:quinn", ["q1"])}
#: What each seed's tributary walk adds (stands in for `graph.tributaries.expand`).
TRIBS = {"m2": {"t1"}, "m3": set(), "m4": {"t2"}, "m1": {"q1"}, "q1": set()}


@pytest.fixture(autouse=True)
def _walk(monkeypatch):
    def expand(graph, reach, *, only=False, excluded=(), passed=(), window=None, registry=None):
        got = set() if only else set(reach)
        for s in reach:
            got |= {t for t in TRIBS.get(s, ()) if t not in excluded and t not in passed}
        return got
    monkeypatch.setattr(B._tribs, "expand", expand)


def _rules(tribs_on_reach=None):
    r1 = {"rule_id": "r1", "type": "retention_limit",
          "extents": [{"op": "whole", "item_id": "gnis:reach"}]}
    if tribs_on_reach is not None:
        r1["includes_tributaries"] = tribs_on_reach
    r2 = {"rule_id": "r2", "type": "retention_limit",
          "extents": [{"op": "rest", "siblings": ["r1"]}]}
    return r1, r2


def _entry(rules, **kw):
    return {"entry_id": "r4:river@4-1", "matched": ["gnis:river"], "rules": rules, **kw}


# ------------------------------------------------------------------------------------ builder
def test_the_rest_is_the_water_minus_its_siblings():
    r1, r2 = _rules()
    b, diags = build_reach(_entry([r1, r2]), r2, REG, G)
    assert b.outcome is Outcome.bound and b.sections == ("m1", "m4")
    d = next(d for d in diags if d.kind == "complement")
    assert d.payload["siblings"] == {"r1": 2} and d.payload["removed"] == 2


def test_a_row_that_includes_tributaries_takes_them_into_the_rest_too():
    """Bull River: the row is 'Includes Tributaries' and both rules inherit it — the rest is the
    river and its tributaries, minus the reach AND the reach's tributaries."""
    r1, r2 = _rules()
    b, _ = build_reach(_entry([r1, r2], includes_tributaries=True), r2, REG, G)
    assert set(b.sections) == {"m1", "m4", "q1", "t2"}
    assert set(b.via_tributary) == {"q1", "t2"}


def test_a_mainstem_only_sibling_leaves_its_tributaries_in_the_rest():
    """Findlay Creek: the release is 'mainstem only', the rest 'including tributaries' — so the
    creek joining along the released reach is in the rest."""
    r1, r2 = _rules(tribs_on_reach=False)
    b, _ = build_reach(_entry([r1, r2], includes_tributaries=True), r2, REG, G)
    assert set(b.sections) == {"m1", "m4", "q1", "t1", "t2"}


def test_a_sibling_that_does_not_bind_leaves_the_rest_unknown(monkeypatch):
    """Never the whole water. MUTATION: turning the policy off lets the unbound sibling subtract
    nothing and the rest widen to the whole river."""
    r1 = {"rule_id": "r1", "type": "retention_limit",
          "extents": [{"op": "whole", "item_id": "gnis:missing"}]}
    r2 = {"rule_id": "r2", "type": "retention_limit",
          "extents": [{"op": "rest", "siblings": ["r1"]}]}
    b, _ = build_reach(_entry([r1, r2]), r2, REG, G)
    assert b.outcome is Outcome.unresolved and b.reason is Reason.complement_unknown
    assert "r1" in b.detail
    monkeypatch.setattr(C, "COMPLEMENT_UNKNOWN_IF_A_SIBLING_DOES_NOT_BIND", False)
    b, _ = build_reach(_entry([r1, r2]), r2, REG, G)
    assert b.outcome is Outcome.bound and b.sections == ("m1", "m2", "m3", "m4")


def test_an_undrawn_sibling_leaves_the_rest_unknown():
    r1, r2 = _rules()
    r1["undrawn_part"] = "at the outlet"
    b, _ = build_reach(_entry([r1, r2]), r2, REG, G)
    assert b.reason is Reason.complement_unknown


def test_a_piece_straddling_a_siblings_end_is_withheld(monkeypatch):
    """MUTATION: with the policy off the straddler is handed to 'other parts'."""
    r1, r2 = _rules()
    real = B._resolve.resolve_extent

    def resolve(reg, g, covered, ex, reasons=None):
        got = real(reg, g, covered, ex, reasons)
        if got is not None and ex.get("item_id") == "gnis:reach":
            got = {**got, "unclassified": ["m4"]}
        return got
    monkeypatch.setattr(B._resolve, "resolve_extent", resolve)
    b, diags = build_reach(_entry([r1, r2]), r2, REG, G)
    assert b.sections == ("m1",)
    assert next(d for d in diags if d.kind == "complement").payload["withheld"] == ["m4"]
    monkeypatch.setattr(C, "COMPLEMENT_WITHHOLDS_STRADDLERS", False)
    b, _ = build_reach(_entry([r1, r2]), r2, REG, G)
    assert b.sections == ("m1", "m4")


def test_siblings_that_cover_everything_leave_no_rest():
    r1 = {"rule_id": "r1", "type": "retention_limit", "extents": [{"op": "whole"}]}
    r2 = {"rule_id": "r2", "type": "retention_limit",
          "extents": [{"op": "rest", "siblings": ["r1"]}]}
    b, _ = build_reach(_entry([r1, r2]), r2, REG, G)
    assert b.outcome is Outcome.unresolved and b.reason is Reason.no_sections


def test_the_rest_goes_through_the_whole_build_like_any_rule():
    """`build_reaches` — the corpus pass — binds the rest too; totality still holds."""
    r1, r2 = _rules()
    out = B.build_reaches([_entry([r1, r2])], REG, G)
    got = {b.rule_id: b.sections for b in out.bindings}
    assert got == {"r1": ("m2", "m3"), "r2": ("m1", "m4")}


def test_an_extent_alone_cannot_resolve_a_rest():
    reasons: list = []
    assert B._resolve.resolve_extent(REG, G, ["gnis:river"], {"op": "rest", "siblings": ["r1"]},
                                     reasons) is None
    assert reasons[0][0] == "rest_needs_its_siblings"


# ------------------------------------------------------------------------------------ model
def test_the_extent_shape():
    assert Extent(op="rest", siblings=["a.r1"]).siblings == ["a.r1"]
    for bad, why in ((dict(op="rest"), "needs `siblings`"),
                     (dict(op="rest", siblings=["a", "a"]), "duplicates"),
                     (dict(op="rest", siblings=["a"], splits=["x"]), "takes no"),
                     (dict(op="rest", siblings=["a"], within_area="area:x"), "takes no"),
                     (dict(op="whole", siblings=["a"]), "belongs to op rest")):
        with pytest.raises(ValueError, match=why):
            Extent(**bad)


ROW = "Trout/char catch and release from A to B\nOther parts: trout/char daily quota = 1"


def _cat(r2_ext=None, r1=None, **r2kw):
    r1 = r1 or {"rule_id": "x.r1", "type": "retention_limit",
                "verbatim": "Trout/char catch and release from A to B", "species": ["TROUT_CHAR"],
                "take": 0, "may_target": True,
                "extents": [{"op": "between", "splits": ["a", "b"]}]}
    r2 = {"rule_id": "x.r2", "type": "retention_limit",
          "verbatim": "Other parts: trout/char daily quota = 1", "species": ["TROUT_CHAR"],
          "take": 1, "extents": r2_ext or [{"op": "rest", "siblings": ["x.r1"]}],
          "extent_text": "other parts", **r2kw}
    return {"entry_id": "r4:x@4-1", "name": "X", "regs_verbatim": ROW, "matched": ["gnis:1"],
            "rules": [r1, r2]}


def test_the_sanctioned_entry():
    e = CatalogueEntry.model_validate(_cat())
    assert e.rules[1].extents == [{"op": "rest", "siblings": ["x.r1"]}]


@pytest.mark.parametrize("shape, why", [
    (dict(r2_ext=[{"op": "rest", "siblings": ["x.r9"]}]), "names no other rule"),
    (dict(r2_ext=[{"op": "rest", "siblings": ["x.r2"]}]), "names no other rule"),
    (dict(r2_ext=[{"op": "rest", "siblings": ["x.r1"]}, {"op": "whole", "item_id": "gnis:2"}]),
     "only extent"),
    (dict(extent_text="", undrawn_part="other parts"), "drop undrawn_part"),
])
def test_a_bad_complement_is_refused(shape, why):
    with pytest.raises(ValueError, match=why):
        CatalogueEntry.model_validate(_cat(**shape))


def test_a_sibling_that_does_not_draw_its_place_is_refused():
    """MUTATION: deleting the undrawn-sibling check in `CatalogueEntry._complements` lets this
    entry through."""
    undrawn = {"rule_id": "x.r1", "type": "retention_limit",
               "verbatim": "Trout/char catch and release from A to B",
               "species": ["TROUT_CHAR"], "take": 0, "may_target": True,
               "extents": [{"op": "whole"}], "undrawn_part": "from A to B",
               "review_reason": "no cut-points for A or B yet"}
    with pytest.raises(ValueError, match="holds in a part nothing draws"):
        CatalogueEntry.model_validate(_cat(r1=undrawn))
    bare = {k: v for k, v in undrawn.items() if k not in ("extents", "undrawn_part")}
    with pytest.raises(ValueError, match="has no extents"):
        CatalogueEntry.model_validate(_cat(r1={**bare, "extent_text": "from A to B"}))


def test_rest_belongs_to_rules_only():
    with pytest.raises(ValueError, match="an entry's scope has none"):
        CatalogueEntry.model_validate({**_cat(), "extents": [{"op": "rest", "siblings": ["x.r1"]}]})


# ------------------------------------------------------------------------------------ place words
def _vessel(**kw):
    return CatalogueRule.model_validate({
        "rule_id": "s.r1", "type": "vessel_rule", "verbatim": "Speed restriction on parts (8 km/h)",
        "aspect": "speed", "max_kmh": 8, "extents": [{"op": "whole"}],
        "review_reason": "the parts are not identified", **kw})


def test_a_place_phrase_carries_no_rule_value():
    """Strawberry Slough's part read 'on parts (8 km/h)'. MUTATION: removing the `PLACE_VALUE`
    check in `CatalogueRule._check` lets both through."""
    assert _vessel(undrawn_part="on parts").undrawn_part == "on parts"
    with pytest.raises(ValueError, match="carries a rule value"):
        _vessel(undrawn_part="on parts (8 km/h)")
    with pytest.raises(ValueError, match="carries a rule value"):
        _vessel(extents=[{"op": "whole", "item_id": "gnis:1"}], extent_text="the bay, Mar 15-May 14")


def test_an_unidentified_part_is_said_as_what_is_true():
    assert not part_identifies_place("on parts") and not part_identifies_place("various locations")
    assert part_identifies_place("south half") and part_identifies_place("on parts of the bay")
    assert part_words("on parts") == "parts the regulations do not identify"
    assert part_words("on part") == "a part the regulations do not identify"
    assert label_parts(_vessel(undrawn_part="on parts"))["in_part"] == \
        "parts the regulations do not identify"
