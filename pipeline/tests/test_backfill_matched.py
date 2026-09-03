"""backfill_matched stamps the covered registry ids without touching curated content."""

import json

from pipeline.regs.parsing.backfill_matched import backfill


def _entry(eid, name, *, locked=False, matched=None):
    return {
        "entry_id": eid,
        "identity": {"name": name, "region": "2", "mus": []},
        "regs_verbatim": "**No Fishing**",
        "locked": locked,
        "matched": matched or [],
        "registry_status": "matched",
        "rules": [{"rule_id": "r.r1", "restriction_type": "closure", "details": "No fishing",
                   "rule_text": "**No Fishing**", "extents": [{"op": "whole"}]}],
    }


def _write(tmp_path, entries):
    p = tmp_path / "region-2.json"
    p.write_text(json.dumps({"region": "2", "entries": entries}), encoding="utf-8")
    return p


def test_stamps_every_covered_item_id(tmp_path):
    path = _write(tmp_path, [_entry("gnis:8634", "Chilliwack River")])
    want = ["gnis:8634", "gnis:3062", "wbk:329707189"]
    report = backfill(tmp_path, {"gnis:8634": {"matched": want, "name": "CHILLIWACK / VEDDER RIVERS"}})

    assert report["stamped"] == ["gnis:8634"]
    assert [r[2] for r in report["combined"]] == [want]
    written = json.loads(path.read_text())["entries"][0]
    assert written["matched"] == want


def test_locked_entry_gets_matched_but_keeps_its_curation(tmp_path):
    """`matched` is a fact about the registry, not a curation decision — but nothing else may move.

    Both sides are compared AFTER a write, since writing goes through the Entry model and fills in
    schema defaults; the point here is that a second backfill moves only `matched`."""
    path = _write(tmp_path, [_entry("gnis:8634", "Chilliwack River", locked=True)])
    backfill(tmp_path, {"gnis:8634": {"matched": ["gnis:8634"], "name": "Chilliwack River"}})
    before = json.loads(path.read_text())["entries"][0]
    backfill(tmp_path, {"gnis:8634": {"matched": ["gnis:8634", "gnis:3062"],
                                      "name": "CHILLIWACK / VEDDER RIVERS"}})
    after = json.loads(path.read_text())["entries"][0]

    assert before["matched"] == ["gnis:8634"]
    assert after["matched"] == ["gnis:8634", "gnis:3062"]
    assert after["locked"] is True
    assert after["identity"]["name"] == before["identity"]["name"], "the curated name must not move"
    assert {k: v for k, v in after.items() if k != "matched"} == {
        k: v for k, v in before.items() if k != "matched"}


def test_entry_absent_from_the_export_is_left_alone(tmp_path):
    _write(tmp_path, [_entry("gnis:9999", "Ghost Creek")])
    assert backfill(tmp_path, {"gnis:8634": {"matched": ["gnis:8634"], "name": "x"}})["stamped"] == []


def test_dry_run_writes_nothing(tmp_path):
    path = _write(tmp_path, [_entry("gnis:8634", "Chilliwack River")])
    before = path.read_text()
    assert backfill(tmp_path, {"gnis:8634": {"matched": ["gnis:8634", "gnis:3062"], "name": "x"}},
                    dry_run=True)["stamped"]
    assert path.read_text() == before
