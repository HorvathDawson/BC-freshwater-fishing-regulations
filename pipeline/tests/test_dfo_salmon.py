"""Parser regression tests for pipeline/dfo_salmon.

Fixtures are real snapshots taken 2026-08-29 and live in
`pipeline/tests/fixtures/dfo_salmon/`. They are committed on purpose: two of the
defects below are invisible in synthetic HTML and silent at runtime, so a fixture is
the only thing that stops them coming back.

Nothing here touches the network.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.dfo_salmon.parse import (
    _RE_SECTION,
    _expand_grid,
    parse_region,
)

FIXTURES = Path(__file__).parent / "fixtures" / "dfo_salmon"


def _load(region) -> str:
    p = FIXTURES / f"region{region}-eng.html"
    if not p.exists():
        pytest.skip(f"fixture missing: {p}")
    return p.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def r6():
    return parse_region(_load(6), 6)


# ---------------------------------------------------------------------------
# The unterminated-comment defect (Regions 4 and 7)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("region,expected_rows", [(4, 1), (7, 2), ("5a", 3)])
def test_table_survives_unterminated_html_comment(region, expected_rows):
    """Regions 4 and 7 carry an unclosed `<!--` above the table.

    A whole-document DOM parse swallows the rest of the file, so `find("table")`
    returns None and the region parses to zero rows with no error raised. The parser
    must slice the table out of the raw source instead.
    """
    html = _load(region)
    assert html[: html.index("<table")].count("<!--") > html[: html.index("<table")].count("-->"), (
        "fixture no longer has the unbalanced comment this test exists to cover"
    )
    parsed = parse_region(html, region)
    assert parsed.table_found is True
    assert len(parsed.rows) == expected_rows


def test_region7_row_content():
    parsed = parse_region(_load(7), 7)
    species = {r.species for r in parsed.rows}
    assert species == {"Sockeye", "Pink"}
    for row in parsed.rows:
        assert row.waters == "Nechako River"
        assert row.bait_ban is True
        assert [fn["text"] for fn in row.fishery_notices] == ["FN0851"]


# ---------------------------------------------------------------------------
# Section detection (Region 6 only region with sections)
# ---------------------------------------------------------------------------


def test_region6_sections(r6):
    assert [s.key for s in r6.sections] == ["A", "B", "B(i)", "B(ii)", "C", "D", "E", "F"]


def test_region6_section_a_comes_from_the_waters_column(r6):
    """Section A is opened by `<th id="a">A. All Region 6 waters</th>`, not a banner."""
    a = next(s for s in r6.sections if s.key == "A")
    assert a.part is None
    a_rows = [r for r in r6.rows if r.section_key == "A"]
    assert len(a_rows) == 3
    assert all(r.precedence == 0 for r in a_rows)
    # The letter prefix is stripped off the water name.
    assert all(r.waters == "All Region 6 waters" for r in a_rows)


def test_sections_that_declare_fallback_to_a(r6):
    fallback = {s.key for s in r6.sections if s.falls_back_to_a}
    assert fallback == {"B", "C", "D", "E"}


@pytest.mark.parametrize("text", [
    "Colonial River - see Cayeghle River",
    "Dewdney Slough - See Nicomen Slough",
    "Little Shuswap Lake - see South Thompson River",
    "Chapman Creek",
])
def test_cross_reference_rows_are_not_sections(text):
    """These begin with C/D/L and were parsed as sections until the delimiter
    after the letter was made mandatory."""
    assert _RE_SECTION.match(text) is None


@pytest.mark.parametrize("text,key", [
    ("A. All Region 6 waters", "A"),
    ("B. Skeena River Watershed – Section \"A\" applies", "B"),
    ("B. Part (i): Skeena River Watershed-Waters upstream", "B(i)"),
    ("B(ii). Skeena River Watershed-Waters downstream", "B(ii)"),
    ("F. Fraser River Watershed - There is no fishing for salmon", "F"),
])
def test_real_banners_still_parse(text, key):
    m = _RE_SECTION.match(text)
    assert m is not None
    part = m.group("roman1") or m.group("roman2")
    assert (f"{m.group('letter')}({part.lower()})" if part else m.group("letter")) == key


# ---------------------------------------------------------------------------
# Precedence cascade
# ---------------------------------------------------------------------------


def test_precedence_ranks(r6):
    by_rank = {}
    for row in r6.rows:
        by_rank.setdefault(row.precedence, []).append(row)
    assert set(by_rank) == {0, 1, 2, 3}
    # Rank 0 is region-wide and only ever section A.
    assert {r.section_key for r in by_rank[0]} == {"A"}
    # Every rank-1 row either declares itself a catch-all ("All waters in section
    # B(i)...", "All streams and lakes flowing into tidal waters of Areas 3, 4, 5,
    # and 6") or is a rule synthesised from a section banner.
    from pipeline.dfo_salmon.parse import _RE_CATCHALL

    for rank in (1, 2):
        for row in by_rank[rank]:
            assert row.source == "section_banner" or _RE_CATCHALL.match(
                row.waters or row.specific_area
            ), f"rank-{rank} row is not a catch-all: {row.waters!r}"
    # Rank 2 exists only where a catch-all names tidal Areas (section E).
    assert {r.section_key for r in by_rank[2]} == {"E"}
    assert all(r.areas for r in by_rank[2])


def test_section_bi_catchall_closes_everything_before_june_16(r6):
    """B(i)'s catch-all rows are the fallback for the upper Skeena."""
    catchall = [r for r in r6.rows if r.section_key == "B(i)" and r.precedence == 1]
    assert len(catchall) == 5
    assert all(r.no_fishing for r in catchall)
    closed = next(r for r in catchall if r.species == "All")
    assert closed.dates == "Jan 1 to Jun 15"


# ---------------------------------------------------------------------------
# Banner-only rules must not vanish
# ---------------------------------------------------------------------------


def test_section_f_closure_is_synthesised(r6):
    """Section F states its closure in the banner and publishes no rows beneath it.
    Anything reading only `rows` would miss a whole-watershed closure."""
    f = [r for r in r6.rows if r.section_key == "F"]
    assert len(f) == 1
    assert f[0].source == "section_banner"
    assert f[0].no_fishing is True
    assert f[0].species == "All"


# ---------------------------------------------------------------------------
# Structured cell content
# ---------------------------------------------------------------------------


def test_babine_lake_exclusion_list_stays_a_list(r6):
    """The 12 excluded tributaries are `<li>`s. Flattened into a sentence they are
    unresolvable against a reach."""
    row = next(
        r for r in r6.rows
        if r.waters == "Babine Lake" and r.species == "Sockeye" and "July 31" in r.dates
    )
    assert "Morrison Creek" in row.specific_area_bullets
    assert len(row.specific_area_bullets) == 12
    assert "Morrison Creek" not in row.specific_area  # not duplicated into the prose


def test_fishery_notices_are_captured(r6):
    cited = [r for r in r6.rows if r.fishery_notices]
    assert len(cited) >= 15
    for row in cited:
        for fn in row.fishery_notices:
            assert fn["href"].startswith("https://notices.dfo-mpo.gc.ca/")


def test_derived_flags(r6):
    row = next(r for r in r6.rows if r.waters == "Babine Lake" and "Aug 1 to Aug 27" in r.dates)
    assert row.daily_limit == 2
    assert row.no_fishing is False
    closed = next(r for r in r6.rows if r.no_fishing)
    assert closed.daily_limit == 0


# ---------------------------------------------------------------------------
# Grid expansion
# ---------------------------------------------------------------------------


def test_grid_rows_are_rectangular_or_banners():
    from bs4 import BeautifulSoup
    from pipeline.dfo_salmon.parse import N_COLS, _RE_TABLE, _strip_comments

    html = _load(6)
    table = BeautifulSoup(_strip_comments(_RE_TABLE.search(html).group(0)), "html.parser").find("table")
    grid = _expand_grid(table)
    assert grid
    for row in grid:
        assert len(row) in (1, N_COLS), f"ragged row of width {len(row)}"


def test_colspan_6_on_a_5_column_table_is_clamped(r6):
    """The B(i) banner is authored `colspan="6"`. Trusting it shifts every later
    column by one."""
    assert 'colspan="6"' in _load(6)
    bi = next(s for s in r6.sections if s.key == "B(i)")
    assert "upstream of CNR Railway Bridge at Terrace" in bi.title


def test_region8_rowspan_repeats_the_water_name():
    parsed = parse_region(_load(8), 8)
    shuswap = [r for r in parsed.rows if r.waters == "Shuswap River"]
    assert len(shuswap) == 3
    assert len({r.specific_area for r in shuswap}) == 3


def test_section_e_area_scoped_catchalls(r6):
    """Section E stacks three scope widths: the section, a set of tidal Areas, and a
    named water. All three must be distinguishable or the narrow ones get overruled."""
    e = [r for r in r6.rows if r.section_key == "E"]
    assert {r.precedence for r in e} == {1, 2, 3}
    area5 = [r for r in e if r.areas == [5]]
    assert area5 and all(r.precedence == 2 for r in area5)
    multi = next(r for r in e if len(r.areas) > 1)
    assert multi.areas == [3, 4, 5, 6]


# ---------------------------------------------------------------------------
# Region 5 is three pages, one region number
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("slug,part,rows", [("5a", "A", 3), ("5b", "B", 38)])
def test_cariboo_split_pages(slug, part, rows):
    """region5-eng.html is a 2016 stub; the real Cariboo rules live on 5a (Fraser
    watershed) and 5b (coastal watershed). Both carry region_number 5."""
    parsed = parse_region(_load(slug), slug)
    assert parsed.region == slug
    assert parsed.region_number == 5
    assert parsed.part == part
    assert parsed.table_found is True
    assert len(parsed.rows) == rows
    assert all(r.region_number == 5 for r in parsed.rows)


def test_region5_stub_has_no_table():
    from pipeline.dfo_salmon.fetch import PAGES

    assert PAGES["5"].is_stub is True
    assert PAGES["5a"].region == PAGES["5b"].region == 5
