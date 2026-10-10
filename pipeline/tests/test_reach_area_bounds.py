"""`within_area` and `outside_area` — the two ways an extent is bounded by a polygon.

THE BOOK CARVES ONE REGION OUT OF ANOTHER AND THERE WAS NO WAY TO SAY SO. The synopsis prints
"Region 1 Daily Quotas (excluding Haida Gwaii)" and then prints Haida Gwaii's own table beside
it. Every op in the vocabulary only ever ADDS water, so the exclusion was unsayable: both
tables bound the Yakoun River, and the app stated Trout 4 and Trout/char 5, Kokanee 5 and
Kokanee 10, one above the other, with no way for a reader to choose.

`outside_area` subtracts, mirroring `within_area`, which intersects. Both are applied at the
same point — after the walk, never to the seed — because the walk is what leaves the area.

`within_area` is tested here too because it had no test and no model field: the resolver read
it and the prompt documented it, but `Extent` did not declare it, so pydantic's `extra="ignore"`
dropped it on every round-trip. Three curated regulations use it.
"""

from __future__ import annotations

from pipeline.atlas.reach.extent import resolve_extent
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.regs.parsing.entry_models import Extent


def _n(nid):
    return StreamNode(node_id=nid, kind=NodeKind.stream, blk=nid.split(":")[0],
                      down_m=0.0, up_m=100.0, length_m=100.0, stream_order=1)


def _graph():
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in (_n("a:0"), _n("b:0"), _n("c:0"))}
    g.edges = [FlowEdge(from_node="b:0", to_node="a:0", kind="tributary", at_measure=0.0)]
    g.up_adj, g.down_adj = {"a:0": [0]}, {"b:0": [0]}
    return g


class _Item:
    def __init__(self, sections=()):
        self.section_ids = tuple(sections)
        self.boundaries = ()
        self.kind = "stream"
        self.name = "x"


#: `region` holds all three sections; `island` holds the two that are NOT the excluded one.
REG = {
    "area:region:1": _Item(["a:0", "b:0", "c:0"]),
    "area:mu_group:hg": _Item(["c:0"]),
    "area:mu_group:south": _Item(["a:0"]),
}


def _sections(ex: dict) -> set[str]:
    got = resolve_extent(REG, _graph(), [], ex)
    return set(got["sections"]) if got else set()


def test_within_area_alone_takes_the_whole_area():
    assert _sections({"op": "within", "area_id": "area:region:1"}) == {"a:0", "b:0", "c:0"}


def test_outside_area_subtracts_the_named_polygon():
    """The Region 1 quota table, headed '(excluding Haida Gwaii)'."""
    assert _sections({"op": "within", "area_id": "area:region:1",
                      "outside_area": "area:mu_group:hg"}) == {"a:0", "b:0"}


def test_the_excluded_area_keeps_its_own_rules():
    """Haida Gwaii's own table still reaches Haida Gwaii — the exclusion is one-directional."""
    assert _sections({"op": "within", "area_id": "area:mu_group:hg"}) == {"c:0"}


def test_outside_area_and_within_area_compose():
    """Intersect, then subtract: 'Region 1, but only the south island, and not Haida Gwaii'."""
    assert _sections({"op": "within", "area_id": "area:region:1",
                      "within_area": "area:mu_group:south",
                      "outside_area": "area:mu_group:hg"}) == {"a:0"}


def test_outside_area_naming_nothing_is_a_failure_not_an_empty_carve_out():
    """A misspelled id must not silently mean 'subtract nothing' — that ships the wider rule."""
    assert resolve_extent(REG, _graph(), [], {"op": "within", "area_id": "area:region:1",
                                              "outside_area": "area:mu_group:typo"}) is None


def test_both_fields_survive_the_model():
    """They are DECLARED, not passed through. `extra='ignore'` silently dropped `within_area`
    on every round-trip, which widened the three curated regulations that use it."""
    e = Extent(op="whole", within_area="area:region:5", outside_area="area:mu_group:hg")
    back = Extent(**e.model_dump(exclude_defaults=True))
    assert back.within_area == "area:region:5"
    assert back.outside_area == "area:mu_group:hg"


# --------------------------------------------------------------------------------------
# The corpus, not a fixture. The synthetic tests above prove the operator works; this proves
# it is actually wired to the two entries the book carves Haida Gwaii out of, across every
# Region 1 water in the shipped bundle. That is the assertion that was false for real readers.
# --------------------------------------------------------------------------------------

import collections
import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.common.curated import GENERATED
from pipeline.tests.conftest import need, predates, BUNDLE_HINT

HG_MUS = {"6-12", "6-13"}
HG_QUOTA = {"z1:hg_quota"}
REGION_1_QUOTAS = {"z1:trout_quota", "z1:species_quotas"}
#: Written for every stream of Region 1, Haida Gwaii included — the book's own preamble says
#: "all streams of Region 1 and Haida Gwaii (MUs 6-12, 6-13)".
BOTH = {"z1:single_barbless_hook"}
#: SETTLED 2026-09-24 (zone-rules-are-the-base): Region 1's stream bait ban is Region 1 MINUS
#: Haida Gwaii; Haida Gwaii's streams carry only the Haida Gwaii line. The two never meet.
R1_BAIT, HG_BAIT = "z1:bait_ban_streams", "z1:hg_bait_ban_streams"


def _region1_waters():
    # `UI_EXPORT_BUNDLE` points the corpus checks at a side bundle, as it does the export's.
    bundle = Path(os.environ.get("UI_EXPORT_BUNDLE") or GENERATED.bundle / "bundle.sqlite")
    need(None, "bundle", bundle, BUNDLE_HINT)
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                      "AND name='ruleset'").fetchone():
        predates(f"{bundle} predates the ruleset tables")
    got: dict[str, set[str]] = collections.defaultdict(set)
    names, kinds = {}, {}
    for iid, name, kind, eid in db.execute(
            "SELECT i.item_id, i.name, i.kind, rs.entry_id FROM ruleset rs"
            " JOIN section_ruleset sr ON sr.set_id = rs.set_id"
            " JOIN item_section isec ON isec.sid = sr.sid"
            " JOIN item i ON i.ord = isec.ord"
            " WHERE rs.entry_id LIKE 'z1:%' GROUP BY i.item_id, rs.entry_id"):
        got[iid].add(eid)
        names[iid], kinds[iid] = name, kind
    mus = {iid: {m for (m,) in db.execute(
        "SELECT DISTINCT value FROM entry, json_each(entry.mus) WHERE item_id = ?", (iid,))}
        for iid in got}
    return got, names, kinds, mus


@pytest.mark.needs_bundle
@pytest.mark.slow
def test_haida_gwaii_takes_its_own_quotas_and_not_region_1s():
    """The defect, stated as the corpus: on Haida Gwaii the book prints one quota table and
    the app showed two, contradicting each other, both labelled "Region 1"."""
    got, names, _kinds, mus = _region1_waters()
    hg = [i for i in got if mus[i] and mus[i] <= HG_MUS]
    assert hg, "no Haida Gwaii water carries a Region 1 rule — the fixture is wrong"
    wrong = [(names[i], sorted(got[i] & REGION_1_QUOTAS)) for i in hg
             if got[i] & REGION_1_QUOTAS]
    assert not wrong, f"Region 1's '(excluding Haida Gwaii)' quotas reached Haida Gwaii: {wrong}"
    missing = [names[i] for i in hg if not got[i] & HG_QUOTA]
    assert not missing, f"Haida Gwaii waters with no Haida Gwaii quota table: {missing}"


@pytest.mark.needs_bundle
@pytest.mark.slow
def test_the_island_keeps_region_1s_quotas_and_never_haida_gwaiis():
    """The other half: an exclusion that over-reaches is as wrong as one that under-reaches."""
    got, names, _kinds, mus = _region1_waters()
    island = [i for i in got if mus[i] and not (mus[i] <= HG_MUS)]
    assert island, "no mainland/island Region 1 water in the bundle"
    leaked = [names[i] for i in island if got[i] & HG_QUOTA]
    assert not leaked, f"Haida Gwaii's quotas reached water outside it: {leaked}"
    lost = [names[i] for i in island if not got[i] & REGION_1_QUOTAS]
    assert not lost, f"Region 1 waters that lost the region's quotas: {lost}"


@pytest.mark.needs_bundle
@pytest.mark.slow
def test_the_rules_written_for_both_reach_both():
    """The barbless-hook rule is written for every stream of Region 1 AND Haida Gwaii.
    Subtracting the quota tables must not subtract it with them."""
    got, names, kinds, mus = _region1_waters()
    hg_streams = [i for i in got if kinds[i] == "stream" and mus[i] and mus[i] <= HG_MUS]
    assert hg_streams, "no Haida Gwaii stream in the bundle"
    for iid in hg_streams:
        assert BOTH <= got[iid], (
            f"{names[iid]} lost a rule written for Region 1 including Haida Gwaii: "
            f"{sorted(BOTH - got[iid])}")


@pytest.mark.needs_bundle
@pytest.mark.slow
def test_each_side_of_the_strait_gets_its_own_bait_ban_and_only_that():
    """Region 1's stream bait ban excludes Haida Gwaii (settled 2026-09-24): a Haida Gwaii stream
    carries the Haida Gwaii line and never Region 1's; a stream on the Island or the mainland
    carries Region 1's and never Haida Gwaii's."""
    got, names, kinds, mus = _region1_waters()
    streams = [i for i in got if kinds[i] == "stream" and mus[i]]
    hg = [i for i in streams if mus[i] <= HG_MUS]
    rest = [i for i in streams if not mus[i] & HG_MUS]
    assert hg and rest, "the fixture has no stream on one side of the strait"
    assert not [names[i] for i in hg if R1_BAIT in got[i]], "Region 1's bait ban reached HG"
    assert not [names[i] for i in hg if HG_BAIT not in got[i]], "an HG stream lost its bait ban"
    assert not [names[i] for i in rest if HG_BAIT in got[i]], "HG's bait ban left Haida Gwaii"


def test_outside_areas_subtracts_several():
    """**A residual scope needs more than one carve-out.**

    A rule's extents UNION, so a subtraction can never be written as more extents. DFO Region 6
    section E is the case: "Other Mainland Watersheds" is the region minus the Skeena, the Nass,
    the Fraser and Haida Gwaii — four at once, where `outside_area` holds one.

    The two fields are unioned, so every existing `outside_area` keeps working untouched.
    """
    from pipeline.regs.parsing.entry_models import Extent

    both = Extent(op="within", area_id="area:region:6",
                  outside_areas=["area:basin:400-", "area:basin:500-"],
                  outside_area="area:basin:100-")
    assert both.outside_areas == ["area:basin:400-", "area:basin:500-"]
    assert both.outside_area == "area:basin:100-"

    # the single-value field alone still round-trips
    one = Extent(op="within", area_id="area:region:1", outside_area="area:mu_group:hg")
    assert one.model_dump(mode="json", exclude_defaults=True)["outside_area"] == "area:mu_group:hg"
    assert "outside_areas" not in one.model_dump(mode="json", exclude_defaults=True)


def test_an_area_named_twice_in_a_subtraction_is_refused():
    """Naming the same area in both fields is a curator slip, not a shorthand — and a silent
    duplicate is how a subtraction list drifts out of step with the scope tree it mirrors."""
    import pytest
    from pipeline.regs.parsing.entry_models import Extent

    with pytest.raises(ValueError, match="both outside_area and outside_areas"):
        Extent(op="within", area_id="area:region:6",
               outside_areas=["area:basin:400-"], outside_area="area:basin:400-")


def test_outside_items_takes_a_straddling_water_back_out_of_an_area():
    """**An area row must not land on a lake that only reaches into the area.**

    A lake is never cut, so the atlas flags one that merely touches a polygon as inside it — and
    the Creston Valley WMA's "bass daily quota = unlimited" bound all of Kootenay Lake, 1% of which
    lies in the WMA. `outside_items` subtracts a water by registry id, at the same point as
    `outside_area`; a water it names that the registry does not have is a failure, never a silent
    no-op that leaves the lake covered."""
    reg = {**REG, "wbk:lake": _Item(["b:0"])}
    got = resolve_extent(reg, _graph(), [], {"op": "within", "area_id": "area:region:1",
                                             "outside_items": ["wbk:lake"]})
    assert set(got["sections"]) == {"a:0", "c:0"}
    assert resolve_extent(reg, _graph(), [], {"op": "within", "area_id": "area:region:1",
                                              "outside_items": ["wbk:typo"]}) is None
    e = Extent(op="within", area_id="area:region:1", outside_items=["wbk:lake"])
    assert Extent.model_validate(e.model_dump(mode="json", exclude_defaults=True)).outside_items \
        == ["wbk:lake"]
    import pytest
    with pytest.raises(ValueError):
        Extent(op="within", area_id="area:region:1", outside_items=["area:mu_group:hg"])
    with pytest.raises(ValueError):
        Extent(op="whole", item_id="wbk:lake", outside_items=["wbk:lake"])


def test_outside_area_items_is_idempotent():
    """F2: `atlas.sidecars` reloads a build's registry.json — which already holds the exclusion —
    and applies `outside_area_items` again. The second run must be a no-op, not a refusal; an
    area or item the registry lacks is still refused."""
    import pytest

    from pipeline.atlas.registry import outside_area_items
    from pipeline.common.models.registry import RegistryItem

    def reg():
        return {"area:national_parks:prnp": RegistryItem(
                    id="area:national_parks:prnp", name="Park", kind="area",
                    section_ids=("lake:K", "lake:H", "s1")),
                "wbk:K": RegistryItem(id="wbk:K", name="Kennedy Lake", kind="lake",
                                      section_ids=("lake:K",)),
                "wbk:H": RegistryItem(id="wbk:H", name="Hobiton Lake", kind="lake",
                                      section_ids=("lake:H",))}
    defs = [{"id": "national_parks", "outside_items": {"prnp": ["wbk:K"]}}]
    once = outside_area_items(reg(), defs)
    assert once["area:national_parks:prnp"].section_ids == ("lake:H", "s1")
    twice = outside_area_items(dict(once), defs)
    assert twice == once
    with pytest.raises(SystemExit, match="not in this build's registry"):
        outside_area_items(reg(), [{"id": "national_parks", "outside_items": {"prnp": ["wbk:X"]}}])
    with pytest.raises(SystemExit, match="is not an area of this build"):
        outside_area_items(reg(), [{"id": "national_parks", "outside_items": {"nope": ["wbk:K"]}}])
