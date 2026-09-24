"""`within(area)` and the feature kinds a zone regulation actually names.

An area extent resolves through the area's CONTAINMENT set — every section inside the
polygon, named or not, which is what lets "no fishing in X Ecological Reserve" reach the
unnamed tributaries that are most of the water in it.

That breadth is also the danger. `Extent.feature_types` narrows it to the kinds the
regulation mentions, and it had been in the model — documented, validated, refusing to be
set on any op but `within` — while NO RESOLVER READ IT. The first zone rule to use it, "No
fishing in any stream in Management Units 1-1 to 1-6", bound all 23,391 sections inside the
area: 16,528 streams, and also 4,151 lakes and 2,712 wetlands the regulation never mentions.
A closure on water a rule did not name is the Fording River failure from the other side.
"""

from __future__ import annotations

from pipeline.atlas.reach.extent import _kind_of


class _Node:
    def __init__(self, kind):
        self.kind = kind


class _Graph:
    def __init__(self, kinds):
        self.nodes = {sid: _Node(k) for sid, k in kinds.items()}


def _filter(area_sections, kinds, graph):
    """The resolver's own narrowing, exercised directly.

    Kept as a helper mirroring `extent.py` rather than a copy of it: what is asserted below
    is the RULE — unknown kinds excluded, empty means everything — and the resolver is
    pinned to the same behaviour by `test_the_resolver_still_narrows_by_kind`.
    """
    if not kinds:
        return set(area_sections)
    want = {k.lower() for k in kinds}
    return {s for s in area_sections if _kind_of(graph, s) in want}


AREA = {"a:0": "stream", "b:0": "stream", "c:0": "lake", "d:0": "wetland"}


def test_a_streams_only_rule_does_not_close_the_lakes_inside_the_area():
    g = _Graph(AREA)
    assert _filter(AREA, ["stream"], g) == {"a:0", "b:0"}


def test_no_feature_types_means_every_kind_inside_the_polygon():
    """The unnamed-tributary case: an ecological reserve closure names no kind and takes
    everything, which is why 94% of the water in Garibaldi is reachable at all."""
    g = _Graph(AREA)
    assert _filter(AREA, [], g) == set(AREA)


def test_several_kinds_union():
    g = _Graph(AREA)
    assert _filter(AREA, ["stream", "lake"], g) == {"a:0", "b:0", "c:0"}


def test_a_section_the_graph_cannot_classify_is_EXCLUDED_not_kept():
    """A rule that says "streams only" must not quietly cover something unclassifiable."""
    g = _Graph(AREA)
    assert _filter({*AREA, "ghost:0"}, ["stream"], g) == {"a:0", "b:0"}


def test_kind_reading_survives_an_enum_or_a_string():
    """The graph stores an enum and the authored extent stores text; they meet here."""
    class _E:
        value = "Lake"
    assert _kind_of(_Graph({"x:0": "STREAM"}), "x:0") == "stream"
    assert _kind_of(_Graph({"x:0": _E()}), "x:0") == "lake"
    assert _kind_of(_Graph({}), "missing:0") == ""


def test_the_resolver_still_narrows_by_kind():
    """If the filter is removed from `extent.py` again, this is what notices."""
    import pathlib

    src = (pathlib.Path(__file__).resolve().parents[1]
           / "atlas/reach/extent.py").read_text(encoding="utf-8")
    assert 'ex.get("feature_types")' in src, (
        "the area resolver no longer reads feature_types — a streams-only rule will close "
        "every lake and wetland in the polygon")
    assert "_kind_of(g, s) in kinds" in src


# --------------------------------------------------------------------------- #
# `within_area` — limiting an extent to a polygon
#
# The one shape the vocabulary could not express. Every op ADDS water; "any stream in the Fraser
# River Watershed OF REGION 5" is a watershed INTERSECTED with an administrative area.
#
# A bounded reach cannot substitute, measured on the live graph: walking the Fraser from its
# region-5 sections leaks 2,074 sections outside the region and misses 23,052 inside it — waters
# that lie in region 5 but drain into the Fraser somewhere else. Region 6 holds no Fraser mainstem
# at all, so there is no reach to bound.
# --------------------------------------------------------------------------- #

class _Reg:
    """Minimal registry item: the resolver only reads `.section_ids`."""
    def __init__(self, sections):
        self.section_ids = list(sections)


class _EmptyGraph:
    nodes: dict = {}


def test_within_area_limits_a_whole_extent():
    from pipeline.atlas.reach.extent import resolve_extent

    reg = {"gnis:1": _Reg(["a", "b", "c"]), "area:region:5": _Reg(["b", "c", "d"])}
    got = resolve_extent(reg, _EmptyGraph(), ["gnis:1"],
                         {"op": "whole", "item_id": "gnis:1", "within_area": "area:region:5"})
    assert got["sections"] == ["b", "c"]
    assert got["within_area"] == ["b", "c", "d"]      # carried forward for the walk


def test_within_area_refuses_an_area_that_does_not_exist():
    """A typo here would silently bind the UNLIMITED set — the widening this guards against."""
    from pipeline.atlas.reach.extent import resolve_extent

    reasons: list = []
    got = resolve_extent({"gnis:1": _Reg(["a"])}, _EmptyGraph(), ["gnis:1"],
                         {"op": "whole", "item_id": "gnis:1", "within_area": "area:region:99"},
                         reasons=reasons)
    assert got is None
    assert any(code == "within_area_not_in_registry" for code, _ in reasons)


def test_the_limit_is_applied_AFTER_the_tributary_walk():
    """Filtering the seed would do nothing: the seed is already inside the region, and it is the
    TRIBUTARIES that wander out of it."""
    from pipeline.atlas.reach.classify import classify

    # what resolve_extent would have handed over: the reach, plus the polygon to limit it to
    per_extent = [{"sections": ["main"], "unclassified": [], "ambiguous_cut": [], "window": None,
                   "waters": (), "within_area": ["main", "in"]}]
    expand = lambda reach, only=False: set(reach) | {"in", "out"}

    binding, _ = classify(
        "e1",
        {"rule_id": "r1", "type": "retention_limit",
         "extents": [{"op": "whole", "item_id": "gnis:1", "within_area": "area:region:5"}]},
        per_extent,
        registry={}, covered_ids=["gnis:1"], scope_clipped=False, entry_has_registry=True,
        tributaries=True, expand_tributaries=expand)

    assert set(binding.sections) == {"main", "in"}, "the out-of-area tributary must be dropped"
    assert "out" not in binding.sections


# --------------------------------------------------------------------------- #
# WALK BEFORE AREA — the named water is walked whole, then the area is applied.
#
# "No fishing in the Fraser River watershed in Region 6" and "lake trout from the Fraser
# watershed" (Region 6's table) bound NOTHING: `whole` Fraser ∩ Region 6 is empty — no Fraser
# mainstem runs through Region 6 — so the walk had no seed. The user's ruling: walk the whole
# named water, then clip what it found to the area.
# --------------------------------------------------------------------------- #

def test_the_seed_is_the_whole_named_water_not_its_part_in_the_area():
    from pipeline.atlas.reach.extent import resolve_extent
    reg = {"gnis:1": _Reg(["m1", "m2"]), "area:region:6": _Reg(["t1", "t2"])}
    got = resolve_extent(reg, _EmptyGraph(), [],
                         {"op": "whole", "item_id": "gnis:1", "within_area": "area:region:6"})
    assert got["sections"] == []                       # no mainstem inside the area…
    assert got["seed"] == ["m1", "m2"]                 # …but the walk starts from all of it
    assert got["within_area"] == ["t1", "t2"]


def test_a_within_extent_has_no_seed():
    """An area extent is already the area; nothing about it changes."""
    from pipeline.atlas.reach.extent import resolve_extent
    reg = {"area:basin:100-": _Reg(["a", "b"]), "area:region:6": _Reg(["b", "c"])}
    got = resolve_extent(reg, _EmptyGraph(), [], {"op": "within", "area_id": "area:basin:100-",
                                                  "within_area": "area:region:6"})
    assert got["sections"] == ["b"] and "seed" not in got


def _walk_from(seen):
    """A walk that records what it was seeded with and returns every tributary of the Fraser."""
    def expand(direct, only=False):
        seen.append(set(direct))
        return {"m1", "m2", "t1", "t2", "x1"} - (set() if not only else {"m1", "m2"})
    return expand


def _fraser_in_region_6():
    return [{"sections": [], "seed": ["m1", "m2"], "unclassified": [], "ambiguous_cut": [],
             "window": None, "within_area": ["t1", "t2"]}]


def test_a_watershed_in_an_area_binds_its_tributaries_inside_the_area():
    from pipeline.atlas.reach.classify import classify
    seen: list = []
    b, _ = classify("z6:iskut_fraser_closure", {"rule_id": "r2", "extents": [{"op": "whole"}]},
                    _fraser_in_region_6(), registry={}, covered_ids=[], scope_clipped=False,
                    entry_has_registry=False, tributaries=True,
                    expand_tributaries=_walk_from(seen))
    assert seen == [{"m1", "m2"}]                       # walked from the whole Fraser
    assert b.outcome.value == "bound" and b.sections == ("t1", "t2")   # then clipped to R6


def test_mutation_clipping_the_seed_first_binds_nothing():
    """The old order, pinned as the failure it was: seeded from the (empty) part inside the
    area, the same rule is unresolved. If WALK_BEFORE_AREA is turned off this is what returns."""
    import pipeline.atlas.reach.classify as C
    seen: list = []
    old = C.WALK_BEFORE_AREA
    C.WALK_BEFORE_AREA = False
    try:
        b, _ = C.classify("z6:iskut_fraser_closure", {"rule_id": "r2", "extents": [{"op": "whole"}]},
                          _fraser_in_region_6(), registry={}, covered_ids=[],
                          scope_clipped=False, entry_has_registry=False, tributaries=True,
                          expand_tributaries=_walk_from(seen))
    finally:
        C.WALK_BEFORE_AREA = old
    assert b.outcome.value == "unresolved" and seen == []


def test_a_rule_that_does_not_walk_is_still_only_the_part_inside_the_area():
    """No widening for the common case: without a walk, `seed` is never read."""
    from pipeline.atlas.reach.classify import classify
    per = [{"sections": ["m2"], "seed": ["m1", "m2"], "unclassified": [], "ambiguous_cut": [],
            "window": None, "within_area": ["m2", "t1"]}]
    b, _ = classify("z7a:sara_sturgeon", {"rule_id": "r1", "extents": [{"op": "whole"}]}, per,
                    registry={}, covered_ids=[], scope_clipped=False, entry_has_registry=False)
    assert b.sections == ("m2",)


def test_an_area_scoped_entry_clips_after_the_walk_and_a_reach_scoped_one_before():
    """A zone chapter's scope (its region) is the same kind of limit as `within_area` and joins
    it; a row's scope ("upstream of the CPR Bridge") still decides where its walk starts."""
    from pipeline.atlas.reach.build import _clip
    got = {"sections": [], "seed": ["m1", "m2"], "unclassified": [], "within_area": ["t1", "t2"]}
    area = _clip(got, {"t2", "m9"}, area_scope=True)
    assert area["seed"] == ["m1", "m2"] and area["within_area"] == ["t2"]
    reach = _clip(got, {"m2"}, area_scope=False)
    assert reach["seed"] == ["m2"] and reach["within_area"] == ["t1", "t2"]
