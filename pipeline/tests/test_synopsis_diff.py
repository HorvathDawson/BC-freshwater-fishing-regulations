"""T239: `pipeline.tools.synopsis_diff` — entry and row diff between two ingested books (small fixtures)."""
from __future__ import annotations

import copy
import json

import pytest

from pipeline.tools import synopsis_diff as SD


def _entry(eid, name, regs, rules=(), matched=("gnis:1",), pages=(14,), **kw):
    return {"entry_id": eid, "name": name, "region": "1", "regs_verbatim": regs,
            "source_pages": list(pages), "matched": list(matched), "rules": list(rules), **kw}


def _rule(rid, verbatim, **kw):
    return {"rule_id": rid, "type": "closure", "verbatim": verbatim, "extents": [{"op": "whole"}], **kw}


BASE = {
    "r1:adam_river@1-10": _entry("r1:adam_river@1-10", "ADAM RIVER", "No Fishing Apr 1-Jun 30",
                                 [_rule("adam_river.r1", "No Fishing Apr 1-Jun 30")], ("gnis:1",)),
    "r1:alice_lake@1-13": _entry("r1:alice_lake@1-13", "ALICE LAKE", "Bait ban",
                                 [_rule("alice_lake.r1", "Bait ban", type="bait_restriction")], ("wbk:2",),
                                 symbols=["Incl. Tribs", "Stocked"]),
    "r1:gone_creek@1-1": _entry("r1:gone_creek@1-1", "GONE CREEK", "Fly only", [], ("gnis:3",)),
    "z1:region_1": _entry("z1:region_1", "Region 1", "Trout 4 per day", [], ()),
}


def _write(dirpath, entries):
    dirpath.mkdir()
    (dirpath / "region-1.json").write_text(json.dumps({"region": "1", "entries": list(entries.values())}))
    return dirpath


def test_identical_books_have_no_differences(tmp_path):
    d = SD.diff_books(BASE, copy.deepcopy(BASE))
    s = d["summary"]
    assert (s["added"], s["removed"], s["changed"], s["cosmetic"], s["unchanged"]) == (0, 0, 0, 0, 4)
    assert d["review_ids"] == []


def test_added_removed_changed_and_rows(tmp_path):
    new = copy.deepcopy(BASE)
    del new["r1:gone_creek@1-1"]
    new["r1:new_lake@1-5"] = _entry("r1:new_lake@1-5", "NEW LAKE", "No ice fishing", [], ("wbk:9",))
    adam = new["r1:adam_river@1-10"]
    adam["regs_verbatim"] = "No Fishing Apr 1-Jul 15; bait ban"
    adam["rules"][0]["verbatim"] = "No Fishing Apr 1-Jul 15"
    adam["rules"][0]["when"] = {"dates": [{"from_month": 4, "from_day": 1, "to_month": 7, "to_day": 15}]}
    adam["rules"].append(_rule("adam_river.r2", "bait ban", type="bait_restriction"))
    d = SD.diff_books(BASE, new)
    s = d["summary"]
    assert [a["entry_id"] for a in d["added"]] == ["r1:new_lake@1-5"]
    assert [r["entry_id"] for r in d["removed"]] == ["r1:gone_creek@1-1"]
    assert [c["entry_id"] for c in d["changed"]] == ["r1:adam_river@1-10"]
    c = d["changed"][0]
    assert c["rows"]["rules"]["changed"][0]["key"] == "adam_river.r1"
    assert set(c["rows"]["rules"]["changed"][0]["fields"]) == {"verbatim", "when"}
    assert [r["rule_id"] for r in c["rows"]["rules"]["added"]] == ["adam_river.r2"]
    assert c["regs_diff"] == "No Fishing Apr [-1-Jun 30-] {+1-Jul 15; bait ban+}"
    assert (s["rules_added"], s["rules_changed"]) == (1, 1)
    assert d["review_ids"] == ["r1:adam_river@1-10", "r1:new_lake@1-5"]


def test_a_renamed_row_pairs_by_its_matched_items_not_as_add_plus_remove():
    new = copy.deepcopy(BASE)
    e = new.pop("r1:alice_lake@1-13")
    e["entry_id"], e["name"] = "r1:alice_lake_mt_washington@1-13", "ALICE LAKE (Mt. Washington)"
    new[e["entry_id"]] = e
    d = SD.diff_books(BASE, new)
    assert d["added"] == [] and d["removed"] == []
    assert d["renamed"] == [{"old_entry_id": "r1:alice_lake@1-13",
                             "entry_id": "r1:alice_lake_mt_washington@1-13", "paired_by": "matched"}]
    assert d["changed"][0]["fields"].keys() == {"name"}


def test_page_moves_and_symbol_order_are_not_for_review():
    new = copy.deepcopy(BASE)
    new["r1:adam_river@1-10"]["source_pages"] = [16]
    new["r1:alice_lake@1-13"]["symbols"] = ["Stocked", "Incl. Tribs"]
    d = SD.diff_books(BASE, new)
    assert [c["entry_id"] for c in d["cosmetic"]] == ["r1:adam_river@1-10"]
    assert d["changed"] == [] and d["review_ids"] == []


def test_a_renumbered_rule_is_rekeyed_not_added_and_removed():
    new = copy.deepcopy(BASE)
    new["r1:adam_river@1-10"]["rules"][0]["rule_id"] = "adam_river.r7"
    d = SD.diff_books(BASE, new)
    rows = d["cosmetic"][0]["rows"]["rules"]
    assert rows["rekeyed"] == [{"key": "adam_river.r7", "old_key": "adam_river.r1"}]
    assert rows["added"] == rows["removed"] == [] and d["review_ids"] == []


def test_scope_and_ignore():
    new = copy.deepcopy(BASE)
    new["z1:region_1"]["regs_verbatim"] = "Trout 5 per day"
    assert SD.diff_books(BASE, new, scope="water")["summary"]["changed"] == 0
    assert SD.diff_books(BASE, new, scope="table")["review_ids"] == ["z1:region_1"]
    assert SD.diff_books(BASE, new, ignore=["regs_verbatim"])["summary"]["changed"] == 0


def test_cli_reads_dirs_and_writes_json_and_ids(tmp_path, capsys):
    new = copy.deepcopy(BASE)
    new["r1:gone_creek@1-1"]["regs_verbatim"] = "Fly only; bait ban"
    old_dir, new_dir = _write(tmp_path / "old", BASE), _write(tmp_path / "new", new)
    assert SD.main([str(old_dir), str(new_dir), "--json", str(tmp_path / "d.json"),
                    "--ids-out", str(tmp_path / "ids.txt")]) == 0
    out = capsys.readouterr().out
    assert "changed 1" in out and "regs: Fly [-only-] {+only; bait ban+}" in out
    assert json.loads((tmp_path / "d.json").read_text())["review_ids"] == ["r1:gone_creek@1-1"]
    assert (tmp_path / "ids.txt").read_text() == "r1:gone_creek@1-1\n"


def test_an_empty_dir_or_a_duplicate_id_is_refused(tmp_path):
    (tmp_path / "empty").mkdir()
    with pytest.raises(FileNotFoundError):
        SD.load_book(tmp_path / "empty")
    d = tmp_path / "dup"
    d.mkdir()
    e = BASE["r1:adam_river@1-10"]
    (d / "region-1.json").write_text(json.dumps({"region": "1", "entries": [e]}))
    (d / "region-2.json").write_text(json.dumps({"region": "2", "entries": [e]}))
    with pytest.raises(ValueError, match="appears twice"):
        SD.load_book(d)


# ── pipeline.tools.synopsis_book: carry (annual step) and replay (rehearsal) ─────────────────────

def _batches(dirpath, items):
    dirpath.mkdir(parents=True)
    (dirpath / "batch_000.json").write_text(json.dumps({"batch": 0, "items": [
        {"index": i, "entry_id": eid, "raw_regs": regs} for i, (eid, regs) in enumerate(items)]}))
    return dirpath


def test_carry_keeps_unchanged_rows_and_tables_and_leaves_the_rest_to_the_parse(tmp_path):
    from pipeline.tools import synopsis_book as SB
    old = _write(tmp_path / "old", BASE)
    b = _batches(tmp_path / "b", [("r1:adam_river@1-10", "No Fishing Apr 1-Jul 15"),   # changed text
                                  ("r1:alice_lake@1-13", "Bait ban"),                  # unchanged
                                  ("r1:new_lake@1-5", "No ice fishing")])              # new; gone_creek gone
    got = SB.carry(b, old, tmp_path / "new")
    assert got == {"carried": ["r1:alice_lake@1-13"], "changed": ["r1:adam_river@1-10"],
                   "new": ["r1:new_lake@1-5"], "gone": ["r1:gone_creek@1-1"], "reflowed": []}
    carried = SD.load_book(tmp_path / "new")
    assert sorted(carried) == ["r1:alice_lake@1-13", "z1:region_1"]
    assert carried["r1:alice_lake@1-13"] == BASE["r1:alice_lake@1-13"]          # copied as is
    with pytest.raises(FileExistsError):
        SB.carry(b, old, tmp_path / "new")


def test_carry_keeps_a_reflowed_row_and_says_so(tmp_path):
    """(owner 2026-11-05, T415) same words, other line breaks: carried with its curated text, listed as reflowed — and a
    real word change is still a change."""
    from pipeline.tools import synopsis_book as SB
    old = _write(tmp_path / "old", BASE)
    b = _batches(tmp_path / "b", [("r1:alice_lake@1-13", "Bait\n ban"), ("r1:adam_river@1-10", "No Fishing Apr 1-Jul 15")])
    got = SB.carry(b, old, tmp_path / "new")
    assert got["reflowed"] == ["r1:alice_lake@1-13"] and "r1:alice_lake@1-13" in got["carried"]
    assert SD.load_book(tmp_path / "new")["r1:alice_lake@1-13"] == BASE["r1:alice_lake@1-13"]
    assert got["changed"] == ["r1:adam_river@1-10"]


def test_replay_answers_each_batch_item_with_the_catalogue_entry(tmp_path):
    from pipeline.tools import synopsis_book as SB
    b = _batches(tmp_path / "b", [("r1:alice_lake@1-13", "Bait ban"), ("r1:new_lake@1-5", "x")])
    n, missing = SB.replay(b, BASE, tmp_path / "resp")
    assert (n, missing) == (1, ["r1:new_lake@1-5"])
    rows = json.loads((tmp_path / "resp" / "batch_000.json").read_text())
    assert rows == [{"index": 0, "entry": BASE["r1:alice_lake@1-13"]}]
