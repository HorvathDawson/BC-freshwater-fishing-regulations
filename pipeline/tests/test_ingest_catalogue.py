"""What may be written to disk, and what may not.

The gate exists because an agent writes BOTH the passage and the rules that quote it. Left to
itself that makes the chain of custody self-referential — an invented sentence validates against
its own invention, and two did. So ingest takes `regs_verbatim` from the batch and throws away
whatever the model supplied.
"""

from __future__ import annotations

import json

import pytest

from pipeline.regs.parsing.ingest_catalogue import ingest, load_batch, write

SRC = "TRANQUILLE LAKE 3-29 Rainbow trout daily quota = 8. Bait ban."
BATCH = {"r3:tranquille@3-29": {"entry_id": "r3:tranquille@3-29", "raw_regs": SRC,
                                "name": "TRANQUILLE LAKE", "region": "3"}}


def _cand(**over):
    base = {"entry_id": "r3:tranquille@3-29", "name": "TRANQUILLE LAKE", "region": "3",
            "regs_verbatim": SRC,
            "rules": [{"rule_id": "t.r1", "type": "retention_limit",
                       "verbatim": "Rainbow trout daily quota = 8", "species": ["RB"], "take": 8}]}
    base.update(over)
    return base


def test_a_clean_candidate_is_accepted():
    accepted, problems = ingest([_cand()], BATCH)
    assert list(accepted) == ["r3:tranquille@3-29"]
    assert not [p for p in problems if not p.startswith("ADVISORY")]


def test_regs_verbatim_comes_from_the_batch_not_the_model():
    """THE POINT OF THE GATE. A model-supplied passage makes the substring check meaningless."""
    accepted, _ = ingest([_cand(regs_verbatim="whatever the model felt like writing")], BATCH)
    assert accepted["r3:tranquille@3-29"].regs_verbatim == SRC


def test_a_number_not_in_the_source_is_rejected():
    accepted, problems = ingest([_cand(rules=[
        {"rule_id": "t.r1", "type": "retention_limit",
         "verbatim": "Rainbow trout daily quota = 99", "species": ["RB"], "take": 99}])], BATCH)
    assert not accepted and problems


def test_an_entry_id_not_in_the_batch_is_rejected():
    """An invented or altered entry_id would write a regulation onto a water nobody asked about."""
    accepted, problems = ingest([_cand(entry_id="r3:somewhere_else@3-29")], BATCH)
    assert not accepted
    assert any("not in the batch" in p for p in problems)


def test_nothing_partial_is_written():
    """One good entry and one bad one: the good one lands, the bad one does not, and neither is
    half-written. A water with some of its regulations is indistinguishable from one with none."""
    bad = _cand(entry_id="r3:nope@3-29")
    accepted, _ = ingest([_cand(), bad], BATCH)
    assert list(accepted) == ["r3:tranquille@3-29"]


def test_write_validates_the_WHOLE_file_not_just_the_new_rows(tmp_path):
    """A duplicate entry_id or a broken neighbour is a failure of the file; writing it ships the
    break."""
    accepted, _ = ingest([_cand()], BATCH)
    out = tmp_path / "cat"
    out.mkdir()
    (out / "region-3.json").write_text(json.dumps(
        {"region": "3", "entries": [{"entry_id": "r3:other@3-1", "name": "Other",
                                     "regs_verbatim": "Bait ban.",
                                     "rules": [{"rule_id": "o.r1", "type": "bait_restriction",
                                                "verbatim": "Bait ban.",
                                                "extents": [{"op": "whole"}],
                                                "gear": [{"slot": "bait", "ban": ["any_bait"]}]}]}]}))
    written, kept = write(accepted, out, ledger=tmp_path / "ingested.json")
    assert written == {"region-3.json": 1} and kept == []
    both = json.loads((out / "region-3.json").read_text())["entries"]
    assert {e["entry_id"] for e in both} == {"r3:other@3-1", "r3:tranquille@3-29"}


def test_reingesting_replaces_rather_than_duplicates(tmp_path):
    accepted, _ = ingest([_cand()], BATCH)
    out = tmp_path / "cat"; out.mkdir()
    write(accepted, out, ledger=tmp_path / "ingested.json")
    write(accepted, out, ledger=tmp_path / "ingested.json")
    entries = json.loads((out / "region-3.json").read_text())["entries"]
    assert len(entries) == 1


def test_dry_run_writes_nothing(tmp_path):
    accepted, _ = ingest([_cand()], BATCH)
    out = tmp_path / "cat"; out.mkdir()
    write(accepted, out, dry_run=True, ledger=tmp_path / "ingested.json")
    assert not list(out.glob("*.json"))
    assert not (tmp_path / "ingested.json").exists()


# --- extents: aliases are canonicalised, invented ids are refused ------------------------------

def _batch_item(**kw):
    it = {
        "entry_id": "e1", "item_id": "gnis:1", "name": "Okanagan River", "region": "8",
        "raw_regs": "No fishing downstream of McIntyre Dam.",
        "bindable_ids": ["gauge__08NM247", "okanagan_river__mcintyre_dam"],
        "boundaries": [["gauge__08NM247", "Below Mcintyre Dam", "split",
                        ["okanagan_river__mcintyre_dam"]]],
    }
    it.update(kw)
    return it


def _candidate(splits):
    return {
        "entry_id": "e1", "region": "8", "name": "Okanagan River",
        "regs_verbatim": "No fishing downstream of McIntyre Dam.",
        "rules": [{
            "rule_id": "r1", "type": "retention_limit", "species": ["ALL_GAME_FISH"], "take": 0,
            "may_target": False,
            "verbatim": "No fishing downstream of McIntyre Dam.",
            "extents": [{"op": "downstream_of", "splits": splits}],
        }],
    }


def _splits_of(entry):
    return entry.rules[0].extents[0]["splits"]


def test_an_alias_is_rewritten_to_the_canonical_id():
    """The page says "McIntyre Dam"; the surviving boundary id is a gauge number. Both bind, but
    only one spelling is stored, or two rules about one point never compare equal."""
    from pipeline.regs.parsing.ingest_catalogue import ingest
    accepted, problems = ingest([_candidate(["okanagan_river__mcintyre_dam"])],
                                {"e1": _batch_item()})
    assert not [p for p in problems if not p.startswith("ADVISORY")], problems
    assert _splits_of(accepted["e1"]) == ["gauge__08NM247"]


def test_the_canonical_id_passes_through_unchanged():
    from pipeline.regs.parsing.ingest_catalogue import ingest
    accepted, _ = ingest([_candidate(["gauge__08NM247"])], {"e1": _batch_item()})
    assert _splits_of(accepted["e1"]) == ["gauge__08NM247"]


def test_an_invented_split_id_is_refused():
    """The catalogue path had no split check at all — an invented cut-point reached the corpus and
    surfaced much later as a rule that silently selected nothing."""
    from pipeline.regs.parsing.ingest_catalogue import ingest
    accepted, problems = ingest([_candidate(["okanagan_river__invented_dam"])],
                                {"e1": _batch_item()})
    assert "e1" not in accepted
    assert any("invented" in p for p in problems), problems


def test_a_no_registry_row_is_not_split_checked():
    from pipeline.regs.parsing.ingest_catalogue import ingest
    item = _batch_item(no_registry=True, bindable_ids=[], boundaries=[])
    cand = _candidate([])
    cand["rules"][0]["extents"] = []
    cand["rules"][0]["review_reason"] = "no registry match — attach an item and bind extents"
    accepted, problems = ingest([cand], {"e1": item})
    assert not [p for p in problems if not p.startswith("ADVISORY")], problems


def test_within_area_survives_ingest():
    """`within_area` limits an extent to a polygon and is applied after the tributary walk. It is
    a plain dict field, and the one failure mode that matters is silent loss: an extent that loses
    it resolves province-wide instead of bounded. entry_models.Extent DOES drop it — the catalogue
    stores extents as dicts precisely so it cannot."""
    from pipeline.regs.parsing.ingest_catalogue import ingest
    cand = _candidate([])
    cand["rules"][0]["extents"] = [{"op": "whole", "within_area": "area:region:5"}]
    accepted, problems = ingest([cand], {"e1": _batch_item()})
    assert not [p for p in problems if not p.startswith("ADVISORY")], problems
    assert accepted["e1"].rules[0].extents[0]["within_area"] == "area:region:5"


def test_within_area_survives_alongside_a_bound_reach():
    from pipeline.regs.parsing.ingest_catalogue import ingest
    cand = _candidate([])
    cand["rules"][0]["extents"] = [{"op": "downstream_of",
                                    "splits": ["okanagan_river__mcintyre_dam"],
                                    "within_area": "area:region:8"}]
    accepted, _ = ingest([cand], {"e1": _batch_item()})
    ex = accepted["e1"].rules[0].extents[0]
    assert ex["splits"] == ["gauge__08NM247"]        # alias canonicalised
    assert ex["within_area"] == "area:region:8"      # and the limiter kept


def test_a_response_in_any_other_shape_is_refused(tmp_path):
    """One shape: the `[{index, entry}]` array dispatch writes. A bare entry, an `{entries: …}`
    wrapper or a row without its index was produced by something other than this pipeline, and
    reading it anyway is how a prose-era response was once ingested as catalogue output."""
    from pipeline.regs.parsing.ingest_catalogue import response_rows
    good = tmp_path / "good.json"
    good.write_text(json.dumps([{"index": 4, "entry": _cand()}]))
    assert response_rows(good)[0]["_batch_index"] == 4
    for bad in ([_cand()], {"entries": [_cand()]}, [{"entry": _cand()}]):
        p = tmp_path / "bad.json"
        p.write_text(json.dumps(bad))
        with pytest.raises(ValueError, match="bad.json"):
            response_rows(p)


def test_entry_id_comes_from_the_batch_not_the_model():
    """The agent retypes entry_id and drops the `@MU` suffix a third of the time. Stored under the
    truncated id, `--skip-existing` stops recognising the row: a resume re-parses waters already
    done and writes each one twice, under two ids that name one water."""
    from pipeline.regs.parsing.ingest_catalogue import ingest
    cand = _candidate([])
    cand["entry_id"] = "e1"                       # model drops the qualifier
    item = _batch_item(entry_id="e1@8-9", index=7)
    cand["_batch_index"] = 7                      # what run() attaches when it unwraps the envelope
    accepted, problems = ingest([cand], {"e1@8-9": item, "#7": item})
    assert not [p for p in problems if not p.startswith("ADVISORY")], problems
    assert "e1@8-9" in accepted, f"stored under {list(accepted)}"
    assert accepted["e1@8-9"].entry_id == "e1@8-9"


def test_a_rule_that_says_nothing_about_location_gets_the_whole_water():
    """The prompt states this default and the model mostly did not write it: 1,957 of 2,397
    rules in the first full parse carried no extents, and a rule with no extents binds to no
    water at all. Written at ingest so the corpus states its own reach, rather than defaulted
    at resolution time where it would be a second answer to the same question."""
    from pipeline.regs.parsing.ingest_catalogue import ingest
    cand = _candidate([])
    del cand["rules"][0]["extents"]
    accepted, problems = ingest([cand], {"e1": _batch_item()})
    assert not [p for p in problems if not p.startswith("ADVISORY")], problems
    assert accepted["e1"].rules[0].extents == [{"op": "whole"}]


def test_a_rule_that_states_a_place_it_could_not_bind_is_left_alone():
    """"500 m upstream and downstream of Causeway Road" with no boundary to bind to is a real,
    specific location. Widening it to the whole water applies a 500 m closure to kilometres."""
    from pipeline.regs.parsing.validate_catalogue import default_extents
    d = {"rules": [{"rule_id": "r1", "extent_text": "500 m upstream of Causeway Road"},
                   {"rule_id": "r2", "unresolved_locators": ["the outlet"]},
                   {"rule_id": "r3"}]}
    assert default_extents(d) == 1
    assert d["rules"][0].get("extents") is None
    assert d["rules"][1].get("extents") is None
    assert d["rules"][2]["extents"] == [{"op": "whole"}]


def test_identity_comes_from_the_batch_not_the_model():
    """843 of 1,021 entries once came back with a name that was not the synopsis's."""
    from pipeline.regs.parsing.ingest_catalogue import ingest
    item = _batch_item()
    item.update(name="ATNARKO RIVER", display_name="Atnarko River", region="5")
    cand = _candidate([])
    cand.update(name="Marble River", display_name="x", region="9")
    accepted, problems = ingest([cand], {cand["entry_id"]: item})
    e = accepted[cand["entry_id"]]
    assert (e.name, e.display_name, e.region) == ("ATNARKO RIVER", "Atnarko River", "5"), problems


def test_every_row_fact_is_the_batchs_and_none_is_the_models():
    """Classified was kept on 20 of 68 rows, Stocked on 11 of 304, 843 of 1,021 names retyped.
    The batch's value wins, and a field the model wrote where the batch has none is dropped —
    `symbols` are the printed glyphs, one to one."""
    from pipeline.regs.parsing.ingest_catalogue import ingest
    item = _batch_item(symbols=["Classified", "Stocked"], pages=[54], display_name="",
                       also_item_ids=["gnis:2"])
    cand = _candidate([])
    cand.update(symbols=["Includes Tributaries", "Classified"], source_pages=[9],
                display_name="Invented", matched=["gnis:999"])
    accepted, _ = ingest([cand], {cand["entry_id"]: item})
    e = accepted[cand["entry_id"]]
    assert e.symbols == ["Classified", "Stocked"]
    assert e.source_pages == [54] and e.display_name == ""
    assert e.matched == ["gnis:1", "gnis:2"]


# --- a re-parse does not overwrite a curator's edit ----------------------------------------------
#
# There is no `locked` any more. The guard is a ledger of what ingest last wrote: an entry whose
# curated copy is not that is KEPT, and only `replace_edited` overwrites it.

def _ingest_once(tmp_path, **cand):
    out = tmp_path / "cat"
    out.mkdir(exist_ok=True)
    accepted, _ = ingest([_cand(**cand)], BATCH)
    return out, accepted


def _on_disk(out):
    return json.loads((out / "region-3.json").read_text())["entries"]


def test_a_reparse_replaces_an_entry_nobody_touched(tmp_path):
    ledger = tmp_path / "ingested.json"
    out, first = _ingest_once(tmp_path)
    write(first, out, ledger=ledger)
    _, second = _ingest_once(tmp_path, rules=[
        {"rule_id": "t.r1", "type": "bait_restriction", "verbatim": "Bait ban.",
         "gear": [{"slot": "bait", "ban": ["any_bait"]}]}])
    written, kept = write(second, out, ledger=ledger)
    assert kept == [] and written == {"region-3.json": 1}
    assert _on_disk(out)[0]["rules"][0]["type"] == "bait_restriction"


def test_a_reparse_keeps_an_entry_a_curator_edited(tmp_path):
    ledger = tmp_path / "ingested.json"
    out, first = _ingest_once(tmp_path)
    write(first, out, ledger=ledger)
    doc = json.loads((out / "region-3.json").read_text())
    doc["entries"][0]["rules"][0]["extents"] = [{"op": "whole", "within_area": "area:region:3"}]
    (out / "region-3.json").write_text(json.dumps(doc))              # the curator's edit
    _, second = _ingest_once(tmp_path)
    written, kept = write(second, out, ledger=ledger)
    assert kept == ["r3:tranquille@3-29"] and written == {"region-3.json": 0}
    assert _on_disk(out)[0]["rules"][0]["extents"][0]["within_area"] == "area:region:3"
    # …and only an explicit override replaces it
    _, kept = write(second, out, ledger=ledger, replace_edited=True)
    assert kept == []
    assert "within_area" not in _on_disk(out)[0]["rules"][0]["extents"][0]


def test_an_entry_the_ledger_never_saw_is_kept(tmp_path):
    """Parsed before the ledger existed, or written by hand: its provenance is unknown, so a
    re-parse does not get to assume it may be replaced."""
    out, first = _ingest_once(tmp_path)
    write(first, out, ledger=tmp_path / "one.json")
    _, kept = write(first, out, ledger=tmp_path / "another.json")
    assert kept == ["r3:tranquille@3-29"]


def test_a_licensing_record_survives_the_write(tmp_path):
    """`exclude_defaults` dropped a licensing record's `kind` — the tag its union is read back
    by — so every entry with licensing was written in a shape that no longer loaded. The write
    now validates what it WROTE, and the record reads back whole."""
    from pipeline.regs.parsing.catalogue import CatalogueEntry
    from pipeline.regs.parsing.io import (dump_entry, entries_dir, read_entries_dir,
                                          read_entryfile, write_entryfile)
    # A real one from the corpus, so the test follows the model rather than a copy of it.
    raw = next(e for e in read_entries_dir(entries_dir()).values()
               if any(x.get("kind") == "designation" for x in e.get("licensing") or []))
    e = CatalogueEntry.model_validate(raw)
    assert all("kind" in x for x in dump_entry(e)["licensing"])
    path = tmp_path / "region-x.json"
    write_entryfile(path, e.region, [e])
    assert CatalogueEntry.model_validate(read_entryfile(path)[e.entry_id]) == e


def test_an_untouched_neighbour_is_written_byte_for_byte(tmp_path):
    """A write replaces the entries it was given and nothing else: a neighbour read from the
    file is written back exactly, in the file's own indent and order."""
    from pipeline.regs.parsing.io import read_entryfile, write_entryfile
    path = tmp_path / "region-7a.json"
    rules = [{"rule_id": "r1", "type": "bait_restriction", "verbatim": "Bait ban.",
              "extents": [{"op": "within", "area_id": "area:region:7a"}],
              "gear": [{"slot": "bait", "ban": ["any_bait"]}]}]
    doc = {"region": "7a", "entries": [
        {"entry_id": "z7a:b", "name": "B", "regs_verbatim": "Bait ban.", "rules": rules},
        {"entry_id": "z7a:a", "name": "A", "regs_verbatim": "Bait ban.", "rules": rules}]}
    text = json.dumps(doc, indent=1, ensure_ascii=False) + "\n"
    path.write_text(text)
    write_entryfile(path, "7a", read_entryfile(path).values())
    assert path.read_text() == text


def test_a_seeded_ledger_lets_a_repass_replace_an_unedited_entry_and_keeps_an_edited_one(tmp_path):
    """With no ledger every entry is one it "never saw", so the first repass kept all 1,480.
    Seeding records the checked-in corpus as ingest's own write; an edit made AFTER is still kept."""
    from pipeline.regs.parsing.ingest_catalogue import seed_ledger

    ledger = tmp_path / "ingested.json"
    out, first = _ingest_once(tmp_path)
    write(first, out, ledger=tmp_path / "elsewhere.json")          # on disk, unknown to `ledger`
    assert write(first, out, ledger=ledger)[1] == ["r3:tranquille@3-29"], "unseeded: kept"

    assert seed_ledger(out, ledger) == 1
    written, kept = write(first, out, ledger=ledger)
    assert kept == [] and written == {"region-3.json": 1}, "seeded: an untouched entry is replaced"

    doc = json.loads((out / "region-3.json").read_text())
    doc["entries"][0]["rules"][0]["review_reason"] = "a curator's note, after the seed"
    (out / "region-3.json").write_text(json.dumps(doc))
    assert write(first, out, ledger=ledger)[1] == ["r3:tranquille@3-29"], "edited after: kept"
