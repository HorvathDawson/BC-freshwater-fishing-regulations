"""Owner 2026-10-28: "I had a lot of DFO extents that I labelled before that now do not show up as
done. They had added splits too. I don't want that to happen again."

The cause: T185 landed each DFO mark's own extent fix (and its new splits) in the curated DFO
geography, which moved the location's binding fingerprint — so the mark that ASKED for the change
read "stale, binding moved" and dropped out of done. `proposals.applied_keys` names the marks whose
own fix the files now say; `marks.status(applied=...)` holds them. `marks.health` accounts for every
mark in force (held / landed / stale / orphan) and the review app shows it on a banner.

Pinned here: landing holds the mark; a different extent or a reworded notice still read stale (no
laundering); a re-scrape that changes only the rows stales no DFO mark (T366: the place answer
stands, the rows go to the digest); a target that vanished is an ORPHAN, listed; and the live store
has no orphaned mark in any cheap scope and no stale DFO mark.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.regs.review import dfo as D
from pipeline.regs.review import marks as M
from pipeline.regs.review import proposals as P
from pipeline.tests import under_test

SHA = "0" * 64
LOC = "1:test-river:between-the-signs"
EXTENT = {"op": "between", "splits": ["test_river__a", "test_river__b"], "item_id": "gnis:1"}


def _world(tmp_path, *, extents=(), text="between the signs", rows=("Coho | 2 per day",),
           loc_id=LOC):
    ent, typed = tmp_path / "entries", tmp_path / "typed"
    ent.mkdir(exist_ok=True)
    typed.mkdir(exist_ok=True)
    (ent / "region-1.json").write_text(json.dumps({
        "region": "1", "region_number": 1, "region_name": "Vancouver Island", "structure": {},
        "scopes": [],
        "waters": [{"water_id": "1:test-river", "region": "1", "region_number": 1,
                    "name": "Test River", "item_ids": ["gnis:1"], "tributaries": False}],
        "locations": [{"location_id": loc_id, "region": "1", "region_number": 1, "kind": "water",
                       "precedence": 3, "water": "Test River", "water_id": "1:test-river",
                       "fingerprints": ["fp1"], "status": "active",
                       "source_text": {"waters": "Test River", "specific_area": text,
                                       "op": "between", "anchor_types": ["sign"]},
                       "binding": {"extents": list(extents)}}]}))
    (typed / "region-1.json").write_text(json.dumps({"locators": [
        {"fingerprint": "fp1", "rows": list(rows), "rules": []}]}))
    splits = tmp_path / "splits.json"
    splits.write_text(json.dumps({"waterbodies": []}))
    return ent, typed, splits


def _mark(prints, extent_fix=True):
    fix = [{"scope": "entry", "op": "between", "items": ["gnis:1"],
            "points": [{"kind": "split", "split_id": "test_river__a"},
                       {"kind": "split", "split_id": "test_river__b"}]}] if extent_fix else []
    return M.Mark.model_validate({
        "scope": "dfo", "target": LOC, "verdict": "unsure", "answers": {"T": "y", "R": "y", "P": "?"},
        "reviewer": "dawson", "date": "2026-10-26", "pdf_sha256": SHA, "prints": prints,
        "extent_fix": fix})


def _applied(marks, ent, splits):
    return P.applied_keys(marks, P.Sources(entries_dir=ent, dfo_dir=ent, splits_path=splits))


def test_landing_a_marks_own_extent_keeps_it_done(tmp_path):
    ent, typed, splits = _world(tmp_path)
    m = _mark(D.prints_of(LOC, ent, typed))
    assert M.status(m, D.prints_of(LOC, ent, typed))["status"] == "unsure"
    assert _applied([m], ent, splits) == set()            # not landed yet: a proposal waits
    _world(tmp_path, extents=[EXTENT])                     # T185 lands the mark's own extent
    now = D.prints_of(LOC, ent, typed)
    assert M.status(m, now)["status"] == "stale"          # the old behaviour: the bug
    keys = _applied([m], ent, splits)
    assert keys == {m.key}
    st = M.status(m, now, applied=keys)
    assert st["status"] == "unsure" and st.get("landed") and st["reasons"] == []


def test_a_different_extent_is_not_excused(tmp_path):
    ent, typed, splits = _world(tmp_path)
    m = _mark(D.prints_of(LOC, ent, typed))
    _world(tmp_path, extents=[{**EXTENT, "splits": ["test_river__a", "test_river__c"]}])
    keys = _applied([m], ent, splits)
    assert keys == set()
    assert M.status(m, D.prints_of(LOC, ent, typed), applied=keys)["status"] == "stale"


def test_landing_never_launders_a_notice_change(tmp_path):
    ent, typed, splits = _world(tmp_path)
    m = _mark(D.prints_of(LOC, ent, typed))
    _world(tmp_path, extents=[EXTENT], text="between the two signs")
    keys = _applied([m], ent, splits)
    assert keys == {m.key}                                 # the extent landed ...
    st = M.status(m, D.prints_of(LOC, ent, typed), applied=keys)
    assert st["status"] == "stale" and not st.get("landed")   # ... but the notice's place words moved


def test_a_row_change_never_stales_a_dfo_mark(tmp_path):
    """T366 / T361 (owner 2026-11-03: "only location ones we need to really really review"): DFO's rows turning over
    (in-season notices) keep the place answer; landed or not, the mark holds. The rows go to the digest."""
    ent, typed, splits = _world(tmp_path)
    m = _mark(D.prints_of(LOC, ent, typed))
    _world(tmp_path, rows=("Coho | Sept 1 to Dec 31 | 4 hatchery-marked per day — FN0939",))
    assert M.status(m, D.prints_of(LOC, ent, typed))["status"] == "unsure"
    _world(tmp_path, extents=[EXTENT], rows=("Coho | 1 per day",))
    keys = _applied([m], ent, splits)
    st = M.status(m, D.prints_of(LOC, ent, typed), applied=keys)
    assert st["status"] == "unsure" and st.get("landed") and st["reasons"] == []


def test_a_vanished_target_is_an_orphan_listed(tmp_path, monkeypatch):
    ent, typed, _splits = _world(tmp_path)
    m = _mark(D.prints_of(LOC, ent, typed))
    monkeypatch.setattr(D, "default_dir", lambda: ent)
    monkeypatch.setattr(D, "default_typed_dir", lambda: typed)
    h = M.health([m], entries={}, ctx=None)
    assert h["n_orphans"] == 0 and h["scopes"]["dfo"]["held"] == 1
    _world(tmp_path, loc_id="1:test-river:between-the-two-signs")   # a regenerated id
    h = M.health([m], entries={}, ctx=None)
    assert h["n_orphans"] == 1, h
    assert h["orphans"][0]["target"] == LOC and h["scopes"]["dfo"]["orphan"] == 1


# ------------------------------------------------------------------------------ the live store

#: the scopes whose target reader needs no build (entry / rule / split need the served registry)
CHEAP_SCOPES = ("dfo", "tidal", "slough", "q154", "name")


@pytest.mark.needs_atlas
def test_no_owner_mark_is_orphaned_or_dropped_live():
    """Regenerating the DFO feed, the suggestions, the proposals, the tidal candidates, the slough /
    name lists must never orphan an owner mark, and a DFO mark must never fall out of done because
    its own fix landed. Fails listing every orphan / stale DFO mark with what it said.

    Whether a mark's fix LANDED (`applied_keys`) reads the build's registry: a DFO mark whose proposal
    cuts at a named river (the Campbell's "Quinsam River") is only recognised as landed against it — needs_atlas,
    not a data defect. Without a build `applied_keys` refuses (T421), never "binding moved"."""
    from pipeline.tests.conftest import ATLAS_HINT, need
    path = Path(under_test.curated().review.marks)
    assert path.exists(), f"no marks store at {path} — it is curated, in git"
    marks = [m for m in M.load(path) if m.scope in CHEAP_SCOPES]
    src = P.Sources.live(build=need(None, "atlas", under_test.atlas(), ATLAS_HINT))
    need(None, "atlas", src.build / "registry.json", ATLAS_HINT)
    h = M.health(marks, entries={}, ctx=None, applied=P.applied_keys(M.load(path), src))
    dropped = h["orphans"] + [r for r in h["stale_rows"] if r["scope"] == "dfo"]
    assert not dropped, "owner marks orphaned / undone:\n" + "\n".join(
        f"  {r['scope']} {r['target']} ({r['verdict']}, {r['date']}) "
        f"{r.get('reasons') or r.get('error') or 'target gone'} splits={r['new_splits']}"
        for r in dropped)


# ------------------------------------------------------------------ historic groups, auto-approve

def _loc(lid, text, *, status="active", dup=None, water="Test River", wid="1:test-river"):
    return {"location_id": lid, "water": water, "water_id": wid, "status": status,
            "duplicate_of": dup, "source_text": {"waters": water, "specific_area": text}}


def _locs(*locs, items=("gnis:1",)):
    w = {"water_id": "1:test-river", "item_ids": list(items)}
    return {loc["location_id"]: ("1", loc, w) for loc in locs}


def test_a_historic_rewording_joins_its_current_place():
    locs = _locs(_loc("cur", "from the Highway 4 bridge downstream to the mouth"),
                 _loc("old", "from Highway 4 bridge downstream to mouth", status="dormant"),
                 _loc("dup", "between the signs", status="dormant", dup="cur"),
                 _loc("up", "from the Highway 4 bridge upstream to the mouth", status="dormant"),
                 _loc("num", "from the Highway 5 bridge downstream to the mouth", status="dormant"),
                 _loc("tri", "from the Highway 4 bridge downstream to the mouth including tributaries",
                      status="dormant"))
    g = D.groups(locs)
    assert set(g) == {"cur", "old", "dup"}, g             # direction / number / tributaries differ
    assert g["old"]["id"] == g["cur"]["id"] == "grp:cur" and g["cur"]["members"][0] == {"id": "cur", "why": None}
    whys = {m["id"]: m["why"] for m in g["cur"]["members"]}
    assert "duplicate_of" in whys["dup"] and "alike" in whys["old"]


def test_a_group_is_carried_only_by_a_dfo_mark_and_omitted_when_none(tmp_path):
    ent, typed, _ = _world(tmp_path)
    m = _mark(D.prints_of(LOC, ent, typed))
    assert '"group"' not in M.line(m)
    g = M.Mark.model_validate({**m.model_dump(), "group": "grp:x"})
    assert '"group":"grp:x"' in M.line(g)
    with pytest.raises(ValueError, match="only a DFO mark"):
        M.Mark.model_validate({**m.model_dump(), "scope": "tidal", "group": "grp:x"})


def test_a_grouped_mark_whose_member_vanished_is_an_orphan(tmp_path, monkeypatch):
    ent, typed, _ = _world(tmp_path)
    monkeypatch.setattr(D, "default_dir", lambda: ent)
    monkeypatch.setattr(D, "default_typed_dir", lambda: typed)
    m = M.Mark.model_validate({**_mark(D.prints_of(LOC, ent, typed)).model_dump(), "group": "grp:x"})
    gone = M.Mark.model_validate({**m.model_dump(), "target": "1:test-river:old-wording"})
    h = M.health([m, gone], entries={}, ctx=None)
    assert h["scopes"]["dfo"] == {"held": 1, "landed": 0, "stale": 0, "orphan": 1}
    assert h["orphans"][0]["target"] == "1:test-river:old-wording"


@pytest.mark.parametrize("words,area,places,ok", [
    ("Test River", "", 1, True), ("Bridge River", "", 1, True), ("Comox Lake", "", 1, True),
    ("Test River", "below the bridge", 1, False), ("Test River tributaries", "", 1, False),
    ("Test River (not including Atnarko River)", "", 1, False), ("Test River", "", 2, False)])
def test_auto_approve_takes_only_a_bare_water_named_once(words, area, places, ok):
    from pipeline.regs.review import dfo_auto as A
    loc = {"water": words, "source_text": {"waters": words, "specific_area": area, "op": "whole_water"}}
    assert (A.why_not(loc, {"item_ids": ["gnis:1"]}, places) is None) is ok


def test_a_historic_only_place_is_its_own_item_with_its_dormant_rewordings():
    """The North Alouette (owner 2026-11-02): a dormant place that no live place rewords stayed in
    no group and the live queue dropped it. It is its own place, its dormant rewordings join it,
    and the live queue keeps it while its water is live."""
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "curation-review" / "backend"))
    import dfo as B
    locs = _locs(_loc("up", "upstream of the 216th Street bridge to the signs at Allco park"),
                 _loc("north", "above 216th Street, including the North Alouette River", status="dormant"),
                 _loc("two", "downstream of a line between two signs in Allco Park", status="dormant"),
                 _loc("2", "downstream of a line between 2 signs in Allco Park", status="dormant", dup="two"))
    g = D.groups(locs)
    assert "north" not in g and g["2"]["canonical"] == g["two"]["canonical"] == "two"
    live = {"1:test-river"}
    assert B.historic_only(locs["north"][1], g, live) and B.historic_only(locs["two"][1], g, live)
    assert not B.historic_only(locs["2"][1], g, live)          # a rewording rides its place
    assert not B.historic_only(locs["up"][1], g, live)         # live: shown anyway
    assert not B.historic_only(locs["north"][1], g, set())     # a water off the live page: archived


def test_applied_keys_names_a_missing_build_instead_of_calling_marks_moved(tmp_path):
    """(T421) The live marks hold new points anchored by the registry (the Campbell's "Quinsam River"). With no build,
    `applied_keys` used to fall back to the stamped point, call every such mark "binding moved" and say nothing; now it
    refuses, naming the registry it could not read. Curated marks only (in git): no build needed to prove it."""
    marks = M.load(Path(under_test.curated().review.marks))
    with pytest.raises(FileNotFoundError, match="registry"):
        P.applied_keys(marks, P.Sources.live(build=tmp_path / "no-build"))


def test_a_registry_lookup_without_a_build_is_remembered(tmp_path):
    """(T421) The flag `applied_keys` reads: an item asked for with no registry.json sets it; with one, it stays off."""
    src = P.Sources(entries_dir=tmp_path, dfo_dir=tmp_path, splits_path=tmp_path / "s.json", build=tmp_path / "none")
    assert src.item("gnis:1") is None and src._asked_without_registry
    b = tmp_path / "b"; b.mkdir()
    (b / "registry.json").write_text(json.dumps({"items": [{"id": "gnis:1", "name": "X"}]}))
    src = P.Sources(entries_dir=tmp_path, dfo_dir=tmp_path, splits_path=tmp_path / "s.json", build=b)
    assert src.item("gnis:1")["name"] == "X" and not src._asked_without_registry
