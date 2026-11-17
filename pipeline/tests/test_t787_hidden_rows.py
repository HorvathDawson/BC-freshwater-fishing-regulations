"""T787 (owner 2026-11-16: "we should keep reading them for provenance but mark them hidden somehow so they are
ignored") — a row from a table DFO commented out is kept, marked `hidden`, and answers nothing.

Off-season DFO keeps its table of dated openings inside an HTML comment (Regions 4, 5A, 7) and the page shows only the
header "no fishing for salmon until further notice". The parser read the hidden table, so the Nechako / Quesnel
openings sat in the typed rows as live rows (the header's closure was the only net). Now: parse marks the table's rows
hidden (balanced or unterminated comment), the mark rides the scraped and typed rules, the release and the live feed
read places through `deliver.salmon.in_force`, and the reviewer still sees the rows.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.deliver import salmon as S
from pipeline.regs.dfo_salmon import feed as FD
from pipeline.regs.dfo_salmon import live as L
from pipeline.regs.dfo_salmon.locations import extract
from pipeline.regs.dfo_salmon.parse import parse_region, table_hidden
from pipeline.regs.dfo_salmon.untangle import untangle

FIXTURES = Path(__file__).parent / "fixtures" / "dfo_salmon"

_ROW = ("<tr><td>Nechako River</td><td>below the bridge</td><td>Sockeye</td><td>Aug 22 to Sept 21</td>"
        "<td>2 per day</td></tr>")
_ROW2 = ("<tr><td>Stuart River</td><td></td><td>Sockeye</td><td>Aug 1 to Aug 31</td><td>2 per day</td></tr>")


def _page(table: str, header: str = "<p>There is no fishing for salmon in Region 7 until further notice.</p>") -> str:
    return (f'<html><body><main><h1 id="wb-cont">Region 7</h1>{header}{table}'
            '<div class="pagedetails"><time property="dateModified">2026-04-01</time></div></main></body></html>')


def _table(rows: str) -> str:
    return f'<table class="table table-bordered"><thead><tr><th>Waters</th></tr></thead><tbody>{rows}</tbody></table>'


@pytest.mark.parametrize("html, hidden", [
    (_page("<!-- " + _table(_ROW) + "-->"), True),                    # today's deliberate, balanced comment
    (_page("<!-- <ul><li>x</li></ul>" + _table(_ROW)), True),          # unterminated: a browser shows nothing after it
    (_page("<!-- a banner -->" + _table(_ROW)), False),                # a comment closed before the table
    (_page(_table(_ROW)), False),
])
def test_a_table_inside_a_comment_is_hidden(html, hidden):
    parsed = parse_region(html, 7)
    assert [r.hidden for r in parsed.rows] == [hidden] and parsed.table_hidden is hidden
    assert parsed.rows[0].waters == "Nechako River", "the hidden row is still READ (provenance)"


def test_rows_commented_inside_a_visible_table_leave_the_visible_rows_live():
    """Hidden is per table, never per page: a visible row beside commented-out rows stays live (the 2022 Region 7
    shape); the commented rows stay out of the grid as before."""
    parsed = parse_region(_page(_table(_ROW + "<!--" + _ROW2 + "-->")), 7)
    assert [(r.waters, r.hidden) for r in parsed.rows] == [("Nechako River", False)]


@pytest.mark.parametrize("region", [4, 7, "5a"])
def test_the_saved_off_season_pages_read_every_row_hidden(region):
    html = (FIXTURES / f"region{region}-eng.html").read_text(encoding="utf-8")
    parsed = parse_region(html, region)
    assert parsed.rows and all(r.hidden for r in parsed.rows) and table_hidden(html, html.index("<table"))


def _typed(html: str, slug="7"):
    u = untangle(parse_region(html, slug))
    _locs, rules, _sig = extract(u)
    return FD.build({"region": slug, "rules": [r.to_dict() for r in rules]})


def test_the_mark_rides_the_scraped_and_typed_rules():
    feed = _typed(_page("<!-- " + _table(_ROW + _ROW2) + "-->"))
    locs = [l for l in feed["locators"] if not l["fingerprint"].startswith("page:")]
    assert locs and all(l["hidden"] == list(range(len(l["rows"]))) for l in locs)
    assert all(r["hidden"] is True for l in locs for r in l["rules"])
    assert all(r["verbatim"] for l in locs for r in l["rules"]), "the rules are typed (provenance), not dropped"
    live = _typed(_page(_table(_ROW)))
    assert not any("hidden" in l or any("hidden" in r for r in l["rules"]) for l in live["locators"]), \
        "a visible table scrapes as before"


def test_a_hidden_row_answers_nothing():
    """A place whose rows are all hidden has no rule today: `location_rules` gives none (so the header closure does
    not stand in for it either — the header answers through its own place), and its year in the live feed is empty.
    A place with a visible row keeps it. MUTATION: `in_force` returning every rule and the Nechako opens Sep 1."""
    open_ = {"rule_id": "a.r0", "type": "retention_limit", "verbatim": "2 per day", "species": ["SK"], "take": 2,
             "when": {"dates": [{"from_month": 8, "from_day": 22, "to_month": 9, "to_day": 21}]}}
    hid = {**open_, "hidden": True}
    from types import SimpleNamespace
    loc = SimpleNamespace(fingerprints=["fp"], location_id="7:nechako", water_id=None, binding=None)
    shut = [{"rule_id": "h", "type": "retention_limit", "verbatim": "No fishing for salmon", "species": ["SA"],
             "take": 0, "may_target": False, "header_note": "7:closure:x"}]
    assert S.location_rules(loc, {"fp": [hid]}, []) == []
    assert S.location_rules(loc, {"fp": [hid]}, shut) == []
    assert S.location_rules(loc, {"fp": [hid, open_]}, []) == [open_]
    v, g = S._Interner(), S._Interner()
    v(S.NOT_LISTED)
    cal = L.location_calendar([hid], v, g)
    assert all(runs == [0, -1] for runs in cal["sp"])
    assert S.verdict_on([S.in_force([hid])], "SK", 901) is None


def test_a_feed_is_never_typed_from_rows_scraped_before_the_mark(tmp_path):
    """`feed.emit` refuses a rules file scraped by older code (no `scrape_format`): its commented-out table's rows carry
    no mark and would answer. The scrape stamps today's. MUTATION: drop the check and the old file types."""
    import json

    from pipeline.regs.dfo_salmon.locations import SCRAPE_FORMAT, emit_html
    (tmp_path / "rules").mkdir()
    (tmp_path / "rules" / "region-7.json").write_text(json.dumps({"region": "7", "rules": []}))
    with pytest.raises(SystemExit, match="scraped by older code"):
        FD.emit("7", rules_dir=tmp_path / "rules", out_dir=tmp_path / "typed")
    emit_html("7", _page("<!-- " + _table(_ROW) + "-->"), tmp_path)
    assert json.loads((tmp_path / "rules" / "region-7.json").read_text())["scrape_format"] == SCRAPE_FORMAT
    FD.emit("7", rules_dir=tmp_path / "rules", out_dir=tmp_path / "typed")
    typed = json.loads((tmp_path / "typed" / "region-7.json").read_text())
    rules = [r for l in typed["locators"] if not l["fingerprint"].startswith("page:") for r in l["rules"]]
    assert rules and all(r.get("hidden") for r in rules)
