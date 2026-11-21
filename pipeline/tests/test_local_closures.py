"""T981: waters a local authority closes (`pipeline.regs.local_closures`): the loader, the one shape, the guard against a
book row, the answers' citation, and the closure read end to end on the committed MINI release (`under_test.mini()`:
the invented Bylaw Pond, closed by an invented parks board) and on the live corpus (Lost Lagoon)."""
from __future__ import annotations

import json
from datetime import date

import pytest

from pipeline.regs import local_closures as LC
from pipeline.tests import under_test

LOST_LAGOON = "r2:lost_lagoon_local@2-8"


def _closure(**over) -> dict:
    e = {"entry_id": "r2:test_pond_local@2-8", "name": "TEST POND", "display_name": "Test Pond", "region": "2",
         "regs_verbatim": "No Fishing", "matched": ["wbk:1"], "extents": [{"op": "whole"}],
         "rules": [{"rule_id": "test_pond_local.r1", "type": "retention_limit", "verbatim": "No Fishing",
                    "species": ["ALL_GAME_FISH"], "take": 0, "may_target": False, "extents": [{"op": "whole"}],
                    "authority": "superior"}],
         "authority": {"body": "Test Parks Board", "instrument": "Parks Bylaw, as example.org gives it",
                       "url": "https://example.org/bylaw", "quote": "No person shall fish in Test Pond."}}
    e.update(over)
    return e


def _file(tmp_path, entries, region="2", about="test"):
    d = tmp_path / f"local_closures-{region}-{bool(about)}"
    d.mkdir(exist_ok=True)
    (d / f"region-{region}.json").write_text(json.dumps({"_about": about, "region": region, "entries": entries}))
    return d


def test_the_loader_raises_on_a_missing_source(tmp_path):
    with pytest.raises(FileNotFoundError, match="local-closures source is missing"):
        LC.load(tmp_path / "local_closures")
    with pytest.raises(FileNotFoundError):
        LC.check_file(tmp_path / "region-2.json")


def test_the_loader_reads_a_closure_in_its_one_shape(tmp_path):
    assert list(LC.load(_file(tmp_path, [_closure()]))) == ["r2:test_pond_local@2-8"]


@pytest.mark.parametrize("change, said", [
    (lambda e: e.pop("authority"), "cites no `authority`"),
    (lambda e: e.update(source_pages=[22]), "prints on no synopsis page"),
    (lambda e: e.update(entry_id="r2:test_pond@2-8"), "slug must end '_local'"),
    (lambda e: e.update(entry_id="z2:test_pond_local"), "water row"),
    (lambda e: e.update(matched=["wbk:1", "wbk:2"]), "exactly one water"),
    (lambda e: e["rules"][0].pop("authority"), "superior authority"),
    (lambda e: e["rules"][0].update(take=1), "superior authority"),
    (lambda e: e["rules"].append(dict(e["rules"][0], rule_id="test_pond_local.r2")), "exactly one rule"),
    (lambda e: e["authority"].update(url="http://example.org"), "url"),
])
def test_an_entry_out_of_its_shape_is_refused(tmp_path, change, said):
    e = _closure()
    change(e)
    with pytest.raises(ValueError, match=said):
        LC.load(_file(tmp_path, [e]))


def test_a_file_without_about_or_in_the_wrong_region_is_refused(tmp_path):
    with pytest.raises(LC.LocalClosureError, match="_about"):
        LC.load(_file(tmp_path, [_closure()], about=""))
    with pytest.raises(LC.LocalClosureError, match="water row"):
        LC.load(_file(tmp_path, [_closure()], region="3"))


def test_the_guard_refuses_a_water_the_book_has_a_row_for():
    book_row = {"entry_id": "r2:test_pond@2-8", "matched": ["wbk:1"]}
    zone = {"entry_id": "z2:daily_quotas", "matched": ["wbk:1"]}
    local = _closure()
    merged = {local["entry_id"]: local, book_row["entry_id"]: book_row, zone["entry_id"]: zone}
    origin = {local["entry_id"]: LC.SOURCE, book_row["entry_id"]: "synopsis", zone["entry_id"]: "synopsis"}
    with pytest.raises(SystemExit, match=r"r2:test_pond_local@2-8 closes wbk:1, which the book's r2:test_pond@2-8"):
        LC.check_against_book(merged, origin)
    # a zone table naming the water is no row of its own: no clash
    del merged[book_row["entry_id"]]
    LC.check_against_book(merged, origin)


def test_lost_lagoon_is_the_one_accepted_row_and_cites_its_page():
    """The live source (owner 2026-11-21: Lost Lagoon only, the page he knows for sure)."""
    got = LC.load()
    assert list(got) == [LOST_LAGOON]
    e = got[LOST_LAGOON]
    assert e["matched"] == ["wbk:329532481"] and e["rules"][0]["authority"] == "superior"
    assert e["authority"]["url"] == "https://stanleyparkvan.com/stanley-park-van-location-lost-lagoon.html"
    assert e["authority"]["quote"] == "Boating, swimming, and fishing are prohibited in Lost Lagoon."


def test_the_corpus_reads_the_source_and_holds_the_guard():
    """`read_all_entries` (the reach run's corpus read) takes the second source and runs the guard over the live
    synopsis: no book row rules Lost Lagoon."""
    from pipeline.regs.parsing import io
    assert [n for n, _ in io.entry_sources()] == ["synopsis", LC.SOURCE]
    assert LOST_LAGOON in io.read_all_entries()


def test_the_answers_cite_the_authority_not_a_page():
    """`add_sources` on a local authority's closure: no book pages, no `row`, `applies` naming the authority, `cited`
    linking its page; the model holds book XOR cited."""
    from pipeline.deliver.answers.display.decided import add_sources
    from pipeline.deliver.answers.model import RuleFacts
    auth = LC.load()[LOST_LAGOON]["authority"]
    doc = {"rulesets": {}, "rules": {"k": {"entry_id": LOST_LAGOON, "provenance": {"rank": -1}}},
           "entries": {LOST_LAGOON: {"kind": "water", "name": "Lost Lagoon", "pages": [], "authority": auth}}}
    f = {"kind": "gate", "title": "No fishing", "closure": True}
    add_sources([f], doc, ["k"])
    assert f["book"] == [] and "row" not in f
    assert f["applies"] == ("Closed to fishing by the Vancouver Board of Parks and Recreation: Stanley Park rules, as "
                            "StanleyParkVan.com gives them.")
    assert f["cited"] == {"link": "StanleyParkVan.com", "url": auth["url"], "quote": auth["quote"]}
    RuleFacts.model_validate_json(json.dumps(f))          # as the answers file is read: JSON
    for bad in ({**f, "cited": None}, {**f, "book": [[22, 24]]}, {**f, "row": True}):
        with pytest.raises(ValueError):
            RuleFacts.model_validate_json(json.dumps(bad))


def test_the_mini_release_answers_bylaw_pond_closed_with_its_citation():
    """END TO END on the committed mini release (corpus -> reach -> bundle -> export -> answers -> release): the invented
    Bylaw Pond is closed all year, and its rule cites the invented parks board and its page, no book page."""
    from pipeline.tools.water_map import model as M
    from pipeline.tools.water_map.release import Release
    rel = Release(under_test.mini())
    try:
        w = M.resolve(rel, "Bylaw Pond")
        units, _ = M.units(M.Reader(rel, date(2026, 11, 18)), M.system(rel, w), "freshwater")
        assert [(u.title, u.today.state) for u in units] == [("Bylaw Pond", "closed_all")]
        wire, _ = rel.answers()          # the answers as the release ships them to the app (`province_store`)
    finally:
        rel.close()
    cited = [f for f in _facts({k: v for k, v in wire.items() if k != "schemas"}) if f.get("cited")]
    assert cited == [{"kind": "gate", "title": "No fishing", "closure": True, "plain": "No fishing.", "book": [],
                      "applies": "Closed to fishing by the Mini East Parks Board: Parks Bylaw No. 1, as example.org "
                                 "gives it, s.4.",
                      "cited": {"link": "example.org", "url": "https://example.org/mini-east/parks-bylaw",
                                "quote": "No person shall fish in Bylaw Pond."}}]


def _facts(node):
    """Every rule-facts dict anywhere in the answers (a dict carrying `applies`)."""
    if isinstance(node, dict):
        if "applies" in node and "kind" in node:
            yield node
        for v in node.values():
            yield from _facts(v)
    elif isinstance(node, list):
        for v in node:
            yield from _facts(v)
