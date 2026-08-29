"""prune_remapped drops entries whose registry item moved — and never a locked one."""

import json

from pipeline.parsing.prune_remapped import prune


def _entry(eid, name, *, locked=False, registry_status="matched"):
    return {
        "entry_id": eid,
        "identity": {"name": name, "region": "2", "mus": []},
        "regs_verbatim": "**No Fishing**",
        "locked": locked,
        "registry_status": registry_status,
        "rules": [{"rule_id": f"{name.lower().replace(' ', '_')}.r1",
                   "restriction_type": "closure", "details": "No fishing",
                   "rule_text": "**No Fishing**", "extents": [{"op": "whole"}]}],
    }


def _write(tmp_path, entries):
    p = tmp_path / "region-2.json"
    p.write_text(json.dumps({"region": "2", "entries": entries}), encoding="utf-8")
    return p


def test_removes_only_the_entry_ids_the_export_no_longer_produces(tmp_path):
    _write(tmp_path, [
        _entry("gnis:39325#fraser_river", "FRASER RIVER"),          # current
        _entry("gnis:10494#fraser_river", "FRASER RIVER"),          # remapped away
        _entry("gnis:8634", "Chilliwack River"),                    # current
    ])
    live = {"gnis:39325#fraser_river": "FRASER RIVER", "gnis:8634": "Chilliwack River"}
    report = prune(tmp_path, live)

    assert [eid for _, eid, _ in report["removed"]] == ["gnis:10494#fraser_river"]
    left = json.loads((tmp_path / "region-2.json").read_text())["entries"]
    assert {e["entry_id"] for e in left} == {"gnis:39325#fraser_river", "gnis:8634"}


def test_a_locked_entry_is_reported_never_deleted(tmp_path):
    path = _write(tmp_path, [_entry("gnis:10494#fraser_river", "FRASER RIVER", locked=True)])
    before = path.read_text()
    report = prune(tmp_path, {"gnis:39325#fraser_river": "FRASER RIVER"})

    assert [eid for _, eid, _ in report["kept_locked"]] == ["gnis:10494#fraser_river"]
    assert report["removed"] == []
    assert path.read_text() == before


def test_no_registry_entries_are_left_alone(tmp_path):
    """`noreg_*` ids aren't registry-keyed, so absence from the live set says nothing about them."""
    _write(tmp_path, [_entry("noreg_some_creek_12", "SOME CREEK", registry_status="no_registry")])
    assert prune(tmp_path, {})["removed"] == []


def test_dry_run_writes_nothing(tmp_path):
    path = _write(tmp_path, [_entry("gnis:10494#fraser_river", "FRASER RIVER")])
    before = path.read_text()
    assert len(prune(tmp_path, {}, dry_run=True)["removed"]) == 1
    assert path.read_text() == before
