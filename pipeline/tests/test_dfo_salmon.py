"""Parser regression tests for pipeline/regs/dfo_salmon.

Fixtures are real snapshots taken 2026-08-29 and live in
`pipeline/tests/fixtures/dfo_salmon/`. They are committed on purpose: two of the
defects below are invisible in synthetic HTML and silent at runtime, so a fixture is
the only thing that stops them coming back.

Nothing here touches the network.
"""

from __future__ import annotations

from pathlib import Path

import json
import re
import pytest

from pipeline.regs.dfo_salmon.parse import (
    _RE_SECTION,
    _expand_grid,
    parse_region,
)
from pipeline.common.curated import CURATED, GENERATED, SOURCE
from pipeline.regs.dfo_salmon.typed import decode

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
        assert decode(row.limits_gear)["bait_ban"] is True
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
    from pipeline.regs.dfo_salmon.parse import _RE_CATCHALL

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
    assert all(decode(r.limits_gear)["no_fishing"] for r in catchall)
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
    assert decode(f[0].limits_gear)["no_fishing"] is True
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


def test_the_transcription_reads_nothing_out_of_the_limits_cell(r6):
    """`parse.py` hands over the columns as printed. The decode lives with the rest of the
    lookup in `typed.py`, because a decode split across two modules is one wording fixed in
    two places — which is how ten `Finfish closure` rows came to state nothing at all."""
    assert not hasattr(r6.rows[0], "no_fishing"), "a derived flag is back on the transcription"

    row = next(r for r in r6.rows if r.waters == "Babine Lake" and "Aug 1 to Aug 27" in r.dates)
    says = decode(row.limits_gear)
    assert says["daily_limit"] == 2 and says["no_fishing"] is False

    closed = next(r for r in r6.rows if decode(r.limits_gear)["no_fishing"])
    assert decode(closed.limits_gear)["daily_limit"] == 0


# ---------------------------------------------------------------------------
# Grid expansion
# ---------------------------------------------------------------------------


def test_grid_rows_are_rectangular_or_banners():
    from bs4 import BeautifulSoup
    from pipeline.regs.dfo_salmon.parse import N_COLS, _RE_TABLE, _strip_comments

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
    from pipeline.regs.dfo_salmon.fetch import PAGES

    assert PAGES["5"].is_stub is True
    assert PAGES["5a"].region == PAGES["5b"].region == 5


# ---------------------------------------------------------------------------
# untangle: waters -> reaches -> rules, plus the defaults
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def u6(r6):
    from pipeline.regs.dfo_salmon.untangle import untangle

    return untangle(r6)


def test_untangle_loses_nothing(r6, u6):
    """Every parsed row must land in exactly one rule — no drops, no duplicates."""
    from pipeline.regs.dfo_salmon.untangle import verify

    verify(r6, u6)


@pytest.mark.parametrize("slug", ["4", "5a", "5b", "6", "7", "8"])
def test_untangle_round_trips_every_region(slug):
    from pipeline.regs.dfo_salmon.untangle import untangle, verify

    parsed = parse_region(_load(slug), slug)
    verify(parsed, untangle(parsed))


def test_defaults_are_separated_from_waters(u6):
    kinds = {d.kind for d in u6.defaults}
    assert kinds == {"region", "section", "area", "closure"}
    region = [d for d in u6.defaults if d.kind == "region"]
    assert len(region) == 1 and region[0].key == "A"
    assert all("All Region 6" not in w.name for w in u6.waters)


def test_area_default_falls_back_through_its_section(u6):
    """An Area default sits inside section E, so E fills its gaps before A does."""
    area5 = next(d for d in u6.defaults if d.kind == "area" and d.areas == [5])
    assert area5.falls_back_to == ["E", "A"]
    section_e = next(d for d in u6.defaults if d.kind == "section" and d.key == "E")
    assert section_e.falls_back_to == ["A"]


def test_inheritance_chain(u6):
    babine = next(w for w in u6.waters if w.name == "Babine Lake")
    assert babine.inherits == ["B(i)", "B", "A"]
    nass = next(w for w in u6.waters if w.name == "Nass River")
    assert nass.inherits == ["C", "A"]


def test_skeena_is_split_by_the_section_boundary(u6):
    """The CNR bridge at Terrace is a real cut: one river, two sections, different
    reaches and different rules on each side."""
    skeenas = [w for w in u6.waters if w.name == "Skeena River"]
    assert {w.section for w in skeenas} == {"B(i)", "B(ii)"}
    upper = next(w for w in skeenas if w.section == "B(i)")
    lower = next(w for w in skeenas if w.section == "B(ii)")
    assert len(upper.reaches) > len(lower.reaches)


def test_reaches_are_distinct_scopes_in_source_order(u6):
    """Morice River is published under two names — bare, and "(including
    tributaries)" — which merge into one water carrying all 8 of its scopes."""
    morice = next(w for w in u6.waters if w.name == "Morice River")
    scopes = [r.scope for r in morice.reaches]
    assert len(scopes) == len(set(scopes)) == 8
    assert [r.index for r in morice.reaches] == list(range(8))
    assert any(r.kind == "between" for r in morice.reaches)
    assert morice.tributaries is True


def test_babine_lake_reach_kinds(u6):
    babine = next(w for w in u6.waters if w.name == "Babine Lake")
    assert "tributaries_only" in [r.kind for r in babine.reaches]
    excl = next(r for r in babine.reaches if r.kind == "whole_water_excluding")
    assert len(excl.excludes) == 12
    assert [r.species for r in excl.rules] == ["Sockeye"] * 3


def test_confluence_closure_is_not_a_directional_cut(u6):
    """'waters within the four boundary signs at the Forks' is a closure polygon,
    not an upstream/downstream cut."""
    bulkley = next(w for w in u6.waters if w.name == "Bulkley River")
    forks = next(r for r in bulkley.reaches if "the Forks" in r.scope)
    assert forks.kind == "sign_bounded_zone"


@pytest.mark.parametrize("raw,name,aliases,tribs", [
    ("Zymoetz (Copper) River — Note: The section of river from Hwy 16 bridge "
     "downstream to the Zymotz-Skeena confluence is the Zymotz River.",
     "Zymoetz River", ["Copper River"], None),
    ("Zymagotitz River [also known as Zymachord River] (including tributaries)",
     "Zymagotitz River", ["Zymachord River"], True),
    ("Kispiox River (including tributaries)", "Kispiox River", [], True),
    ("Meziadin Lake", "Meziadin Lake", [], None),
])
def test_split_name(raw, name, aliases, tribs):
    from pipeline.regs.dfo_salmon.untangle import split_name

    got_name, got_aliases, _, got_tribs, _ = split_name(raw)
    assert (got_name, got_aliases, got_tribs) == (name, aliases, tribs)


def test_abbreviated_name_keeps_its_word(u6):
    """A blanket `.rstrip(".")` turned 'Tseax R.' into 'Tseax R'."""
    assert any(w.name == "Tseax River" for w in u6.waters)
    assert not any(w.name.endswith(" R") for w in u6.waters)


def test_name_embedded_scope_becomes_a_reach(u6):
    """'Tatshenshini River (upstream of the BC/Yukon border)' and '(downstream of ...)'
    are one river with two scopes, not two rivers."""
    tats = [w for w in u6.waters if w.name == "Tatshenshini River"]
    assert len(tats) == 1
    assert {"upstream_of", "downstream_of"} <= {r.kind for r in tats[0].reaches}


def test_appended_note_is_split_off_the_name(u6):
    kits = next(w for w in u6.waters if w.name.startswith("Kitsumkalum River"))
    assert kits.name == "Kitsumkalum River"
    assert kits.name_note and "designated by fishing boundary signs" in kits.name_note
    assert kits.tributaries is True  # from "(including tpinributaries)" — sic


def test_reach_carries_anchor_types(u6):
    zym = next(w for w in u6.waters if w.name == "Zymagotitz River")
    reach = next(r for r in zym.reaches if r.kind == "upstream_of")
    assert reach.anchor_types == ["road", "bridge"]
    assert "Highway #16 bridge" in reach.scope    # the landmark stays in the verbatim text


# ---------------------------------------------------------------------------
# churn: the drift matcher (pure — no network)
# ---------------------------------------------------------------------------


def test_drift_matcher_separates_rewording_from_real_change():
    """DFO rewords the same reach constantly — "Highway 37 Bridge" becomes
    "Highway 37 bridge", and the count of boundary signs has flipped 3 <-> 4 and
    back. Keying a curated binding on the scope string would orphan it every time."""
    from pipeline.regs.dfo_salmon.churn import compare_reaches

    prev = {
        ("B(i)", "Skeena River", "mainstem waters near the Kitwanga River mouth, "
                                 "from Mill Creek upstream to the Highway 37 Bridge"),
        ("B(i)", "Kispiox River", "downstream of fishing boundary signs near Kispiox River Resort."),
    }
    cur = {
        ("B(i)", "Skeena River", "mainstem waters near the Kitwanga River mouth, "
                                 "from Mill Creek upstream to the Highway 37 bridge"),
        ("B(i)", "Babine Lake", "within a 400m radius of the mouth of Pinkut Creek"),
    }
    drifted, added, removed = compare_reaches(prev, cur)

    assert [d[1][1] for d in drifted] == ["Skeena River"]      # reworded, same place
    assert [a[1] for a in added] == ["Babine Lake"]            # genuinely new scope
    assert [r[1] for r in removed] == ["Kispiox River"]        # gone (seasonally)


def test_drift_matcher_does_not_pair_across_waters():
    from pipeline.regs.dfo_salmon.churn import compare_reaches

    prev = {("C", "Nass River", "upstream of the Highway 37 bridge")}
    cur = {("C", "Kiteen River", "upstream of the Highway 37 bridge")}
    drifted, added, removed = compare_reaches(prev, cur)
    assert drifted == []
    assert len(added) == len(removed) == 1


# ---------------------------------------------------------------------------
# Historical replay — the decade is a regression corpus, not just evidence
# ---------------------------------------------------------------------------

#: Committed snapshots that predate the current page structure. 2017/2018 Region 6 has
#: no `<time property="dateModified">` element at all and no B(i)/B(ii) split, so it
#: exercises paths the live page cannot.
HISTORICAL = ["region6_20170703172154", "region6_20180524134448",
              "region6_20200408190304"]

#: Optional deeper corpus, populated by `python -m pipeline.regs.dfo_salmon.churn`.
HISTORY_CACHE = Path("cache/dfo_salmon/history")


def _load_historical(stem: str) -> str:
    p = FIXTURES / f"{stem}.html"
    if not p.exists():
        pytest.skip(f"fixture missing: {p}")
    return p.read_text(encoding="utf-8", errors="replace")


@pytest.mark.parametrize("stem", HISTORICAL)
def test_parser_still_reads_decade_old_markup(stem):
    """A parser change that only works on today's page is a regression. Nobody reads
    2017 data, but it is free proof the parser is not overfitted to the current HTML."""
    from pipeline.regs.dfo_salmon.untangle import untangle, verify

    parsed = parse_region(_load_historical(stem), "6")
    assert parsed.table_found is True
    assert parsed.rows
    verify(parsed, untangle(parsed))


def test_date_modified_may_be_absent_in_old_pages():
    """The 2017 page has no dateModified element, which is why change detection keys
    on the content sha256 and never on that field."""
    assert parse_region(_load_historical(HISTORICAL[0]), "6").date_modified is None


def test_the_skeena_section_split_is_newer_than_2018():
    """Section B became B(i)/B(ii) — split at the CNR Railway Bridge at Terrace —
    somewhere after 2018. It is the only structural change in nine years, and it is
    why a curated entry must NOT key on its section."""
    old = {s.key for s in parse_region(_load_historical(HISTORICAL[1]), "6").sections}
    new = {s.key for s in parse_region(_load(6), 6).sections}
    assert "B" in old and "B(i)" not in old
    assert {"B(i)", "B(ii)"} <= new


@pytest.mark.parametrize("stem", HISTORICAL)
def test_water_names_are_append_only(stem):
    """Measured across 2017, 2018 and 2024-2026: no Region 6 water name has ever
    disappeared. If this fails, either DFO broke a nine-year pattern or we broke the
    parser — both need a human, so it must not fail silently."""
    from pipeline.regs.dfo_salmon.untangle import untangle

    old = {w.name for w in untangle(parse_region(_load_historical(stem), "6")).waters}
    now = {w.name for w in untangle(parse_region(_load(6), 6)).waters}
    assert old <= now, f"waters present in {stem} but missing today: {sorted(old - now)}"


def test_region_baseline_is_stable_across_the_decade():
    """Section A's limits are substantively unchanged since 2017. Everything that did
    move is cosmetic — and each instance is a reason not to key anything on this text."""
    from pipeline.regs.dfo_salmon.untangle import untangle

    def baseline(u, normalize):
        d = next(x for x in u.defaults if x.kind == "region")
        return {(normalize(r.species), normalize(r.limits_gear.rstrip("."))) for r in d.rules}

    def loose(t):
        return t.lower().replace(" & ", " and ").replace("apr 01", "apr 1")

    old_u = untangle(parse_region(_load_historical(HISTORICAL[0]), "6"))
    now_u = untangle(parse_region(_load(6), 6))
    assert baseline(old_u, loose) == baseline(now_u, loose)

    # ...and verbatim, the drift is real: species text moved too, not just dates.
    old_raw = baseline(old_u, str)
    now_raw = baseline(now_u, str)
    assert old_raw != now_raw
    assert ("Sockeye, Pink & Chum" in {s for s, _ in old_raw}
            and "Sockeye, pink and chum" in {s for s, _ in now_raw})


@pytest.mark.slow
def test_every_cached_historical_snapshot_parses():
    """Opportunistic sweep of the full history cache when one has been built."""
    from pipeline.regs.dfo_salmon.untangle import untangle, verify

    files = sorted(HISTORY_CACHE.glob("region*_*.html")) if HISTORY_CACHE.exists() else []
    if len(files) < 2:
        pytest.skip("no history cache; run pipeline.regs.dfo_salmon.churn to build one")
    for f in files:
        slug = f.name.split("_")[0].replace("region", "")
        parsed = parse_region(f.read_text(encoding="utf-8", errors="replace"), slug)
        verify(parsed, untangle(parsed))


def test_rows_nested_by_malformed_markup_are_not_dropped():
    """2020-04-08 Region 6 holds 202 <tr> of which only 23 are direct children of
    <tbody> — an unclosed cell tag makes html.parser nest the other 179 inside each
    other. `find_all("tr", recursive=False)` dropped 88% of that page's rules with no
    error, the same silent failure as the unterminated comment in Regions 4/7/5a."""
    from bs4 import BeautifulSoup

    from pipeline.regs.dfo_salmon.parse import _RE_TABLE, _row_tags, _strip_comments
    from pipeline.regs.dfo_salmon.untangle import untangle

    html = _load_historical("region6_20200408190304")
    frag = _strip_comments(_RE_TABLE.search(html).group(0))
    table = BeautifulSoup(frag, "html.parser").find("table")
    body = table.find("tbody")

    assert len(body.find_all("tr", recursive=False)) < 30, (
        "fixture no longer has the nesting this test exists to cover")
    assert len(_row_tags(body, table)) > 190

    parsed = parse_region(html, "6")
    assert len(parsed.rows) > 150
    assert len(parsed.sections) == 8
    # 76, not 77: Tlell River's only 2020 row is a colspan note, which is a note on
    # section D rather than a waterbody carrying rules.
    assert len(untangle(parsed).waters) == 76


def test_nested_row_text_does_not_leak_into_its_parent_cell():
    """Rows nested by malformed markup are emitted separately, so their text must not
    also appear in the cell that (incorrectly) contains them."""
    from pipeline.regs.dfo_salmon.untangle import untangle

    u = untangle(parse_region(_load_historical("region6_20200408190304"), "6"))
    babine = next(w for w in u.waters if w.name == "Babine Lake")
    for reach in babine.reaches:
        assert len(reach.scope) < 700, f"cell swallowed nested rows: {reach.scope[:120]}"


# ---------------------------------------------------------------------------
# locations: the location / regulation split
# ---------------------------------------------------------------------------


def _extract(stem_or_slug, historical=False):
    from pipeline.regs.dfo_salmon.locations import extract
    from pipeline.regs.dfo_salmon.untangle import untangle

    html = _load_historical(stem_or_slug) if historical else _load(stem_or_slug)
    slug = "6" if historical else str(stem_or_slug)
    return extract(untangle(parse_region(html, slug)))


@pytest.mark.parametrize("drifted,same", [
    ("upstream to the Highway 37 Bridge", "upstream to the Highway 37 bridge"),
    ("upstream of Hwy #16 bridge", "upstream of Highway 16 bridge"),
    ("approximately 100 meters downstream", "approx. 100 m downstream"),
    ("Sockeye, Pink & Chum", "Sockeye, pink and chum"),
])
def test_fingerprint_survives_observed_drift(drifted, same):
    """Every pair here is a real rewording from the archives."""
    from pipeline.regs.dfo_salmon.locations import fingerprint

    assert fingerprint("B(i)", "Skeena River", drifted) == fingerprint("B(i)", "Skeena River", same)


@pytest.mark.parametrize("a,b", [
    ("within three white triangular signs", "within the 4 triangular signs"),
    ("upstream of Gosnell Creek", "downstream of Gosnell Creek"),
    ("from Gosnell Creek to Lamprey Creek", "from Gosnell Creek to Morice Lake"),
])
def test_fingerprint_is_sensitive_to_real_change(a, b):
    """Numbers are deliberately NOT normalised: "three signs" vs "4 signs" is a real
    difference in what the source claims and must reach a human."""
    from pipeline.regs.dfo_salmon.locations import fingerprint

    assert fingerprint("B(i)", "Skeena River", a) != fingerprint("B(i)", "Skeena River", b)


def test_every_rule_lands_on_a_location():
    """The split must not lose or orphan a rule."""
    for slug in ("1", "2", "6", "8"):
        locs, rules, _ = _extract(slug)
        known = {l.fingerprint for l in locs}
        assert rules, slug
        assert all(r.fingerprint in known for r in rules), slug


def test_skeena_cascade_is_carried_as_locations():
    """Rules attach to Region 6's defaults exactly as they attach to a named water, so
    the cascade has to be in the location set, not beside it."""
    locs, rules, sig = _extract("6")
    kinds = {l.kind for l in locs}
    assert kinds == {"water", "region_default", "section_default", "area_default", "closure"}

    region = [l for l in locs if l.kind == "region_default"]
    assert len(region) == 1 and region[0].section == "A" and region[0].precedence == 0

    areas = sorted((l.areas for l in locs if l.kind == "area_default"), key=len)
    assert [5] in areas and [6] in areas and [3, 4, 5, 6] in areas
    assert all(l.inherits == ["E", "A"] for l in locs if l.kind == "area_default")

    closure = next(l for l in locs if l.kind == "closure")
    assert closure.section == "F"
    # Includes the container "B", which publishes no rules of its own — so a
    # structural check notices if the Skeena container itself disappears.
    assert sig["sections"] == ["A", "B", "B(i)", "B(ii)", "C", "D", "E", "F"]


def test_spatial_notes_only_caveat_when_they_narrow_the_extent():
    locs, _, _ = _extract("6")
    babine = next(l for l in locs
                  if l.water == "Babine Lake" and "excluding" in l.specific_area)
    assert babine.narrows_extent is True
    assert any("400 m radius" in n for n in babine.notes)
    # The radius clause must not swallow the sentence after its colon.
    assert not any("Gullwing" in n and "400 m radius" in n for n in babine.notes)
    # A gear note is worth carrying but does not narrow the water covered.
    assert any("Barbed hooks" in n for n in babine.notes)
    gear_only = [l for l in locs if l.notes and not l.narrows_extent]
    assert all(not any("radius" in n for n in l.notes) for l in gear_only)


# ---------------------------------------------------------------------------
# entries: identity, seeding, and the reconciler's severity ladder
# ---------------------------------------------------------------------------


def _seeded(slug, historical=False):
    from pipeline.regs.dfo_salmon.entries import EntryFile, apply_seed
    from pipeline.regs.dfo_salmon.fetch import PAGES

    locs, _, sig = _extract(slug, historical)
    base = "6" if historical else str(slug)
    ef = EntryFile(region=base, region_number=PAGES[base].region, region_name=PAGES[base].name)
    apply_seed(ef, locs, sig)
    return ef


def test_seed_is_idempotent_and_never_edits_a_curated_record():
    from pipeline.regs.dfo_salmon.entries import Binding, apply_seed

    locs, _, sig = _extract("6")
    ef = _seeded("6")
    n = len(ef.locations)

    from pipeline.regs.parsing.entry_models import Extent

    victim = next(l for l in ef.locations if l.water == "Babine Lake")
    victim.binding = Binding(extents=[Extent(op="whole")])
    victim.locked = True

    added, dormant = apply_seed(ef, locs, sig)
    assert (added, dormant) == (0, 0)
    assert len(ef.locations) == n
    assert victim.locked is True
    assert [e.op.value for e in victim.binding.extents] == ["whole"]


def test_location_ids_are_readable_and_unique():
    ef = _seeded("6")
    ids = [l.location_id for l in ef.locations]
    assert len(ids) == len(set(ids))
    assert "6:a:region" in ids
    assert "6:e:area:areas-5" in ids
    assert any(i.startswith("6:babine-lake:") for i in ids)


def test_reconcile_clean_run_needs_nobody():
    from pipeline.regs.dfo_salmon.entries import reconcile

    locs, _, sig = _extract("6")
    rep = reconcile(_seeded("6"), locs, sig)
    assert rep.counts().get("ok") == len(locs)
    assert rep.needs_human == []
    assert rep.publishable is True
    assert rep.severity == 0


def test_reconcile_flags_a_reworded_location_as_drift_with_candidates():
    from pipeline.regs.dfo_salmon.entries import reconcile
    from pipeline.regs.dfo_salmon.locations import fingerprint

    ef = _seeded("6")
    locs, _, sig = _extract("6")
    target = next(l for l in locs if l.water == "Zymagotitz River" and l.op == "upstream_of")
    target.specific_area = "upstream of the Highway 16 road bridge crossing"
    target.fingerprint = fingerprint(target.section, target.water, target.specific_area)

    rep = reconcile(ef, locs, sig)
    drift = [o for o in rep.outcomes if o.status == "drift"]
    assert len(drift) == 1
    assert drift[0].candidates, "a reworded reach must arrive with rebind candidates"
    assert drift[0].candidates[0]["location_id"].startswith("6:zymagotitz-river")
    assert rep.publishable is True   # one reach holds itself, not the region


def test_reconcile_marks_absent_locations_dormant_and_revives_them():
    from pipeline.regs.dfo_salmon.entries import apply_seed, reconcile

    ef = _seeded("6")
    locs, _, sig = _extract("6")
    kispiox = [l for l in locs if l.water == "Kispiox River"]
    survivors = [l for l in locs if l.water != "Kispiox River"]

    rep = reconcile(ef, survivors, sig)
    dormant = [o for o in rep.outcomes if o.status == "dormant"]
    assert {o.water for o in dormant} == {"Kispiox River"}
    assert all(SEVERITY_OK(o) for o in dormant)

    apply_seed(ef, survivors, sig)
    assert all(l.status == "dormant" for l in ef.locations if l.water == "Kispiox River")

    # ...and next season it comes back, on the binding that was kept.
    rep2 = reconcile(ef, survivors + kispiox, sig)
    revived = [o for o in rep2.outcomes if o.status == "revived"]
    assert {o.water for o in revived} == {"Kispiox River"}
    assert all(o.location_id for o in revived)


def SEVERITY_OK(outcome):
    from pipeline.regs.dfo_salmon.entries import SEVERITY

    return SEVERITY[outcome.status] == 0


def test_reconcile_escalates_a_section_move():
    """Section B became B(i)/B(ii) once in nine years. A curated binding must survive
    it, but the re-scoping must reach a human."""
    from pipeline.regs.dfo_salmon.entries import SEVERITY, reconcile

    ef = _seeded("6")
    locs, _, sig = _extract("6")
    for l in locs:
        if l.water == "Babine Lake":
            l.section = "B(iii)"          # fingerprint unchanged, section moved
    rep = reconcile(ef, locs, sig)
    moved = [o for o in rep.outcomes if o.status == "section_moved"]
    assert moved and all(o.location_id for o in moved)
    assert rep.severity >= SEVERITY["section_moved"]


def test_the_2020_restructure_holds_the_whole_region():
    """Seeded on 2018 and shown 2020, the reconciler must refuse to publish rather than
    quietly rebind 59 locations. All three structural signals should fire."""
    from pipeline.regs.dfo_salmon.entries import SEVERITY, reconcile

    ef = _seeded("region6_20180524134448", historical=True)
    locs, _, sig = _extract("region6_20200408190304", historical=True)
    rep = reconcile(ef, locs, sig)

    assert rep.publishable is False
    assert rep.severity == SEVERITY["structural"]
    blob = " ".join(rep.structural)
    assert "section set changed" in blob
    assert "location count moved" in blob
    assert "unbound" in blob


def test_a_normal_seasonal_update_does_not_hold_the_region():
    """The counterpart to the test above: an ordinary in-season change must stay
    publishable, or the cron is useless."""
    from pipeline.regs.dfo_salmon.entries import reconcile

    ef = _seeded("6")
    locs, _, sig = _extract("6")
    locs = [l for l in locs if l.water != "Kispiox River"]
    rep = reconcile(ef, locs, sig)
    assert rep.publishable is True
    assert rep.structural == []


# ---------------------------------------------------------------------------
# hand-off to the reach builder
# ---------------------------------------------------------------------------


def test_to_reach_input_shapes_a_watershed_above_a_point():
    """Kispiox River (including tributaries), upstream of a point, is the case the
    whole model exists for: `upstream_of` resolved first, then the tributary walk from
    that window = the watershed above the point."""
    from pipeline.regs.dfo_salmon.entries import Binding, to_reach_input

    ef = _seeded("6")
    loc = next(l for l in ef.locations
               if l.water == "Kispiox River" and "downstream" in l.source_text["specific_area"])
    from pipeline.regs.dfo_salmon.entries import WaterBinding

    from pipeline.regs.parsing.entry_models import Extent

    loc.binding = Binding(
        extents=[Extent(op="downstream_of", splits=["split:kispiox-resort"])],
        tributaries=True,
    )
    # The registry item lives on the water, not the reach.
    water = WaterBinding(water_id=loc.water_id, region="6", region_number=6,
                         name="Kispiox River", item_ids=["item:kispiox"])
    entry, rules = to_reach_input(loc, [{"species": "Pink", "dates": "Jun 16 to Aug 23",
                                         "limits_gear": "2 per day"}], water)

    assert entry == {"entry_id": loc.location_id, "matched": ["item:kispiox"],
                     "includes_tributaries": True}
    assert len(rules) == 1
    assert rules[0]["extents"][0]["op"] == "downstream_of"
    assert rules[0]["extents"][0]["splits"] == ["split:kispiox-resort"]
    assert rules[0]["includes_tributaries"] is True
    # Only what resolution reads: the scraped row's species, dates and gear never reach it.
    assert set(rules[0]) == {"rule_id", "extents", "includes_tributaries", "tributaries_only",
                             "tributary_excludes"}


def test_to_reach_input_carries_tributary_carve_outs():
    """Bulkley: "all tributaries other than Morice and tributaries, Suskwa and
    tributaries, and Two Mile Creek"."""
    from pipeline.regs.dfo_salmon.entries import Binding, to_reach_input

    ef = _seeded("6")
    loc = next(l for l in ef.locations if l.water == "Bulkley River")
    from pipeline.regs.parsing.entry_models import Extent

    loc.binding = Binding(
        extents=[Extent(op="whole")],
        tributaries_only=True,
        tributary_excludes=[Extent(op="whole", item_id="item:morice"),
                            Extent(op="whole", item_id="item:suskwa")],
    )
    from pipeline.regs.dfo_salmon.entries import WaterBinding

    water = WaterBinding(water_id=loc.water_id, region="6", region_number=6,
                         name="Bulkley River", item_ids=["item:bulkley"])
    entry, rules = to_reach_input(loc, [{"species": "All", "dates": "Aug 1 to Dec 31",
                                         "limits_gear": "No natural bait allowed"}], water)
    # The carve-outs and `only` are the RULE's — the catalogue has no entry-wide list.
    assert set(entry) == {"entry_id", "matched", "includes_tributaries"}
    assert rules[0]["tributaries_only"] is True
    assert len(rules[0]["tributary_excludes"]) == 2


def test_unbound_water_resolves_to_nothing():
    """A location whose water has no registry item must resolve to nothing rather than
    silently binding the whole river."""
    from pipeline.regs.dfo_salmon.entries import to_reach_input

    ef = _seeded("6")
    loc = next(l for l in ef.locations if l.kind == "water")
    entry, rules = to_reach_input(loc, [{"species": "Coho"}], ef.water(loc.water_id))
    assert entry["matched"] == []
    assert all(not l.binding.extents for l in ef.locations)


# ---------------------------------------------------------------------------
# cascade: the sections as a tree of spatial scopes
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def scopes6(r6):
    from pipeline.regs.dfo_salmon.cascade import build_scopes
    from pipeline.regs.dfo_salmon.untangle import untangle

    return build_scopes(untangle(r6))


def test_scope_tree_shape(scopes6):
    """A contains everything; B(i)/B(ii) hang off B, not off A; the Area scopes hang
    off E, not off A."""
    parent = {s.scope_id: s.parent for s in scopes6}
    assert parent["A"] is None
    assert parent["B"] == parent["C"] == parent["D"] == parent["E"] == parent["F"] == "A"
    assert parent["B(i)"] == parent["B(ii)"] == "B"
    assert all(p == "E" for k, p in parent.items() if k.startswith("E:areas"))


def test_bi_and_bii_are_the_watershed_above_and_below_one_point(scopes6):
    """B(i)/B(ii) are the same primitive as a named water's reach — upstream_of an
    anchor over a tributary-included item — applied to the whole Skeena."""
    bi = next(s for s in scopes6 if s.scope_id == "B(i)")
    bii = next(s for s in scopes6 if s.scope_id == "B(ii)")
    assert (bi.kind, bii.kind) == ("watershed_above", "watershed_below")
    assert bi.of == bii.of == "Skeena River"
    assert bi.anchor == bii.anchor == "CNR Railway Bridge at Terrace"


def test_b_is_a_container_with_no_rules_of_its_own(scopes6):
    b = next(s for s in scopes6 if s.scope_id == "B")
    assert b.kind == "watershed" and b.of == "Skeena River"
    assert b.container_only is True
    assert next(s for s in scopes6 if s.scope_id == "B(i)").container_only is False


def test_e_is_the_else_branch_and_needs_no_binding_of_its_own(scopes6):
    """"Other Mainland Watersheds, except for the Fraser" reads like a set difference,
    but nothing computes one: a listed water declares its section, and an unlisted one
    falls through B/C/D/F — which are bound anyway."""
    from pipeline.regs.dfo_salmon.cascade import needs_binding, needs_new_machinery

    e = next(s for s in scopes6 if s.scope_id == "E")
    assert e.kind == "residual"
    assert e.minus == ["B", "C", "D", "F"], "the siblings tested before falling through"
    assert e not in needs_binding(scopes6)
    assert e not in needs_new_machinery(scopes6)


def test_watershed_and_island_scopes(scopes6):
    by = {s.scope_id: s for s in scopes6}
    assert by["C"].kind == "watershed" and by["C"].of == "Nass River"
    assert by["F"].kind == "watershed" and by["F"].of == "Fraser River"
    assert by["D"].kind == "island_group"


def test_tidal_area_scopes_carry_their_area_numbers(scopes6):
    areas = sorted((s.areas for s in scopes6 if s.kind == "tidal_areas"), key=len)
    assert areas == [[5], [6], [3, 4, 5, 6]]


@pytest.mark.parametrize("scope_id,chain", [
    ("B(i)", ["B(i)", "B", "A"]),
    ("B(ii)", ["B(ii)", "B", "A"]),
    ("E:areas-5", ["E:areas-5", "E", "A"]),
    ("F", ["F", "A"]),
    ("A", ["A"]),
])
def test_resolution_chain_is_narrowest_first(scopes6, scope_id, chain):
    from pipeline.regs.dfo_salmon.cascade import resolution_chain

    assert resolution_chain(scopes6, scope_id) == chain


def test_only_the_tidal_area_scopes_need_new_machinery(scopes6):
    """Everything else is a named water plus at most two cut points. A tidal-area
    scope is about a stream's OUTLET, which needs the PFMA polygons."""
    from pipeline.regs.dfo_salmon.cascade import needs_new_machinery

    assert {s.scope_id for s in needs_new_machinery(scopes6)} == {
        "E:areas-3-4-5-6", "E:areas-5", "E:areas-6"}


def test_regions_without_sections_have_no_scope_tree():
    from pipeline.regs.dfo_salmon.cascade import build_scopes
    from pipeline.regs.dfo_salmon.untangle import untangle

    assert build_scopes(untangle(parse_region(_load(8), 8))) == []


# ---------------------------------------------------------------------------
# superset seeding — seasonal cycling handled by construction
# ---------------------------------------------------------------------------


def test_superset_seeding_turns_a_revival_into_an_exact_hit():
    """Seed from the union of every published version, so a location that cycles out
    and back is an exact fingerprint hit on an already-bound record.

    Measured across the archives: seeding from one version costs 102 reviews on Region
    6; seeding from the superset costs 3.
    """
    from pipeline.regs.dfo_salmon.entries import EntryFile, apply_seed, reconcile
    from pipeline.regs.dfo_salmon.fetch import PAGES
    from pipeline.regs.dfo_salmon.locations import extract
    from pipeline.regs.dfo_salmon.untangle import untangle

    live = extract(untangle(parse_region(_load(6), 6)))
    old = extract(untangle(parse_region(_load_historical("region6_20200408190304"), "6")))

    ef = EntryFile(region="6", region_number=6, region_name=PAGES["6"].name)
    apply_seed(ef, old[0], {}, mark_dormant=False)       # the superset pass
    apply_seed(ef, live[0], live[2])                     # then the live pass

    only_in_2020 = {l.fingerprint for l in old[0]} - {l.fingerprint for l in live[0]}
    assert only_in_2020, "fixture should carry locations no longer published"
    dormant = {fp for l in ef.locations if l.status == "dormant" for fp in l.fingerprints}
    assert only_in_2020 <= dormant

    # A 2020-only location coming back binds without a human.
    revived = [l for l in old[0] if l.fingerprint in only_in_2020]
    rep = reconcile(ef, live[0] + revived, live[2])
    statuses = {o.status for o in rep.outcomes if o.fingerprint in only_in_2020}
    assert statuses == {"revived"}
    assert rep.needs_human == []


def test_superset_pass_does_not_retire_current_locations():
    """`mark_dormant=False` — absence from a 2017 page says nothing about today."""
    from pipeline.regs.dfo_salmon.entries import EntryFile, apply_seed
    from pipeline.regs.dfo_salmon.fetch import PAGES
    from pipeline.regs.dfo_salmon.locations import extract
    from pipeline.regs.dfo_salmon.untangle import untangle

    live = extract(untangle(parse_region(_load(6), 6)))
    ef = EntryFile(region="6", region_number=6, region_name=PAGES["6"].name)
    apply_seed(ef, live[0], live[2])
    old = extract(untangle(parse_region(_load_historical("region6_20170703172154"), "6")))
    added, dormant = apply_seed(ef, old[0], {}, mark_dormant=False)
    assert dormant == 0, "a superset pass must not retire anything"
    assert added > 0, "the 2017 page should contribute locations the live page lacks"

    live_fps = {l.fingerprint for l in live[0]}
    for l in ef.locations:
        if set(l.fingerprints) & live_fps:
            assert l.status == "active", l.location_id


def test_scopes_round_trip_through_the_entry_file(tmp_path):
    from pipeline.regs.dfo_salmon.cascade import build_scopes
    from pipeline.regs.dfo_salmon.entries import EntryFile, apply_seed, load, save
    from pipeline.regs.dfo_salmon.locations import extract
    from pipeline.regs.dfo_salmon.untangle import untangle

    u = untangle(parse_region(_load(6), 6))
    locs, _, sig = extract(u)
    ef = EntryFile(region="6", region_number=6, region_name="Skeena")
    apply_seed(ef, locs, sig, scopes=build_scopes(u))
    save(ef, tmp_path)
    back = load("6", tmp_path)
    assert [s.scope_id for s in back.scopes] == [s.scope_id for s in ef.scopes]
    assert back.chain_for("E:areas-5") == ["E:areas-5", "E", "A"]
    assert next(s for s in back.scopes if s.scope_id == "E").minus == ["B", "C", "D", "F"]


# ---------------------------------------------------------------------------
# Ragged rows: notes that span, and continuations the source forgot to mark
# ---------------------------------------------------------------------------


def test_a_cell_spanning_the_rule_columns_is_a_note_not_a_rule():
    """`<td>Tlell River</td><td colspan="4">Anglers should note …</td>` read as a rule
    produces a phantom whose species, dates AND limits are all one sentence of prose —
    and a phantom waterbody to hang it on."""
    from pipeline.regs.dfo_salmon.untangle import untangle

    assert '<td colspan="4">Anglers should note' in _load(6)
    parsed = parse_region(_load(6), 6)
    assert not any("Anglers should note" in r.species for r in parsed.rows)
    assert any("Tlell River" in n["text"] and n["section"] == "D" for n in parsed.notes)
    assert not any(w.name == "Tlell River" for w in untangle(parsed).waters)


def test_unmarked_continuation_rows_are_right_aligned():
    """The 2025-03 Region 5a page gives Quesnel Lake all five columns but no
    `rowspan`, so the Chinook and Coho rows below carry only (species, dates, limits).
    Left-aligned, "Chinook" becomes a waterbody."""
    from pipeline.regs.dfo_salmon.untangle import untangle

    hist = HISTORY_CACHE / "region5a_20250320090549.html"
    if not hist.exists():
        pytest.skip("history cache not built")
    u = untangle(parse_region(hist.read_text(encoding="utf-8", errors="replace"), "5a"))
    names = {w.name for w in u.waters}
    assert names == {"Quesnel Lake", "Quesnel River"}
    assert not names & {"Chinook", "Coho", "Sockeye"}
    quesnel = next(w for w in u.waters if w.name == "Quesnel Lake")
    species = {r.species for reach in quesnel.reaches for r in reach.rules}
    assert {"Sockeye", "Chinook", "Coho"} <= species


def test_a_full_width_row_is_never_shifted_right():
    """The guard is total colspan, not cell count: a 2-cell row spanning 1+4 already
    fills the width and must stay where it is."""
    from bs4 import BeautifulSoup

    from pipeline.regs.dfo_salmon.parse import N_COLS, _expand_grid

    html = ('<table><tbody>'
            '<tr><td>Water A</td><td>area</td><td>Coho</td><td>Apr 1</td><td>2 per day</td></tr>'
            '<tr><td>Water B</td><td colspan="4">a note about Water B</td></tr>'
            '<tr><td>Chinook</td><td>May 1</td><td>1 per day</td></tr>'
            '</tbody></table>')
    grid = _expand_grid(BeautifulSoup(html, "html.parser").find("table"))
    assert [len(r) for r in grid] == [N_COLS] * 3
    assert grid[1][0].text == "Water B"          # not shifted
    assert grid[1][1].text == "a note about Water B"
    # The continuation belongs to Water B — but the note text must not be dragged into
    # its specific-area column.
    assert grid[2][0].text == "Water B"
    assert grid[2][1].text == ""
    assert grid[2][2].text == "Chinook"


# ---------------------------------------------------------------------------
# validate: the invariants that catch a silent parse failure
# ---------------------------------------------------------------------------


def test_live_pages_validate_clean():
    from pipeline.regs.dfo_salmon.validate import check_region
    from pipeline.regs.dfo_salmon.untangle import untangle

    for slug in ("1", "2", "3", "4", "5a", "5b", "6", "7", "8"):
        parsed = parse_region(_load(slug), slug)
        findings = check_region(slug, parsed, untangle(parsed))
        errors = [f for f in findings if f.severity == "ERROR"]
        assert errors == [], f"region {slug}: {[str(e) for e in errors]}"


@pytest.mark.parametrize("stem", HISTORICAL)
def test_archived_versions_validate_clean(stem):
    from pipeline.regs.dfo_salmon.validate import check_region
    from pipeline.regs.dfo_salmon.untangle import untangle

    parsed = parse_region(_load_historical(stem), "6")
    errors = [f for f in check_region("6", parsed, untangle(parsed)) if f.severity == "ERROR"]
    assert errors == [], [str(e) for e in errors]


def test_validator_catches_a_note_read_as_a_rule():
    """The Tlell shape: species == dates == limits, all one sentence of prose."""
    from pipeline.regs.dfo_salmon.untangle import untangle
    from pipeline.regs.dfo_salmon.validate import check_region

    parsed = parse_region(_load(6), 6)
    prose = "Anglers should note that tidal water regulations apply below the sign."
    victim = parsed.rows[0]
    object.__setattr__(victim, "species", prose) if hasattr(victim, "__slots__") else None
    victim.species = victim.dates = victim.limits_gear = prose
    checks = {f.check for f in check_region("6", parsed, untangle(parsed))}
    assert "note_read_as_rule" in checks
    assert "prose_in_species" in checks


def test_validator_catches_a_species_filed_as_a_waterbody():
    """The Region 5a shape: an unmarked continuation row left-aligned."""
    from pipeline.regs.dfo_salmon.untangle import untangle
    from pipeline.regs.dfo_salmon.validate import check_region

    parsed = parse_region(_load(6), 6)
    u = untangle(parsed)
    u.waters[0].name = "Chinook"
    errors = [f for f in check_region("6", parsed, u) if f.severity == "ERROR"]
    assert any(f.check == "species_as_waterbody" for f in errors)


def test_validator_catches_a_lost_rule():
    from pipeline.regs.dfo_salmon.untangle import untangle
    from pipeline.regs.dfo_salmon.validate import check_region

    parsed = parse_region(_load(6), 6)
    u = untangle(parsed)
    u.waters[0].reaches[0].rules.pop()
    errors = [f for f in check_region("6", parsed, u) if f.severity == "ERROR"]
    assert any(f.check == "rule_conservation" for f in errors)


# ---------------------------------------------------------------------------
# structured dates
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("text,start,end,open_ended", [
    ("Aug 1 to Aug 27", "Aug 1", "Aug 27", False),
    ("Apr 1 to Mar 31", "Apr 1", "Mar 31", False),
    ("Apr 1 until further notice", "Apr 1", None, True),
    ("May 8 2018 until further notice", "May 8", None, True),
    ("July 1 until futher notice", "Jul 1", None, True),      # sic — source typo
    ("Until further notice", None, None, True),
])
def test_interpret_dates(text, start, end, open_ended):
    """"until further notice" is a real shape — an announced start with no announced
    end — not a parse failure. It appears 35+ times across the archives."""
    from pipeline.regs.dfo_salmon.typed import interpret_dates

    got = interpret_dates(text)
    assert got["open_ended"] is open_ended
    assert got["parsed"] is True
    if start:
        assert got["start"] == start
    if end:
        assert got["end"] == end


@pytest.mark.parametrize("text,start,end", [
    ("Aprl 1 to Jun 15", "Apr 1", "Jun 15"),        # misspelled month
    ("Nov 01- to Dec 31", "Nov 1", "Dec 31"),       # separator glued to the day
    ("June 15 to July 14 2017", "Jun 15", "Jul 14"),  # trailing calendar year
])
def test_unambiguous_typos_are_repaired_and_recorded(text, start, end):
    """All three are real strings from archived pages, and each has exactly one
    possible reading — so repairing them is safe. `repaired_from` keeps the verbatim
    original, so the repair is auditable rather than invisible."""
    from pipeline.regs.dfo_salmon.typed import interpret_dates

    got = interpret_dates(text)
    assert got["parsed"] is True
    assert (got["start"], got["end"]) == (start, end)
    assert got["repaired_from"] == text


@pytest.mark.parametrize("text", ["Mar 1 to Xyz 9", "sometime in the spring"])
def test_ambiguous_text_is_never_repaired_into_a_window(text):
    """The guard on the repair: only ONE candidate month may match. Anything else
    stays unparsed rather than becoming a window the page does not state."""
    from pipeline.regs.dfo_salmon.typed import interpret_dates

    got = interpret_dates(text)
    assert got["parsed"] is False
    assert got["repaired_from"] is None


def test_a_clean_date_is_never_marked_repaired():
    from pipeline.regs.dfo_salmon.typed import interpret_dates

    for t in ("Apr 1 to Mar 31", "Aug 1 to Aug 27", "Sept 1 to Oct 15"):
        assert interpret_dates(t)["repaired_from"] is None


def test_every_live_rule_has_a_window_or_a_reason():
    """The transcription carries the `Dates` cell verbatim and reads nothing out of it, so the
    window is interpreted here, the same way `typed.to_rules` does it."""
    from pipeline.regs.dfo_salmon.locations import extract
    from pipeline.regs.dfo_salmon.typed import interpret_dates
    from pipeline.regs.dfo_salmon.untangle import untangle
    from pipeline.regs.dfo_salmon.validate import NO_WINDOW_DATES

    for slug in ("1", "2", "6"):
        _locs, rules, _ = extract(untangle(parse_region(_load(slug), slug)))
        for r in rules:
            if not interpret_dates(r.dates)["parsed"]:
                assert r.dates.strip().lower() in NO_WINDOW_DATES, (slug, r.dates)


# ---------------------------------------------------------------------------
# op classification — the load-bearing derived field
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("scope,kind", [
    # "between X and Y" is two-ended without saying from/to. These were classified
    # `upstream_of` because "above"/"upstream" appears inside the phrase.
    ("between fishing boundary signs located approximately 100m above and below Red Rock Pool.", "between"),
    ("Between fishing boundary signs approximately 200m upstream of and 500m downstream of Stamp Falls", "between"),
    ("between the Johnson Subdivision Bridge and the powerlines located just upstream of Highway 97", "between"),
    # A trailing Note:/except clause describes neighbouring water, not this reach.
    ("including tributaries NOTE: the section of river from Cranberry-Kiteen junction to Nass R. is part of the Cranberry R.", "tributaries_only"),
    # ...and the ordinary cases still classify as before.
    ("upstream of Highway #16 bridge", "upstream_of"),
    ("downstream of the CNR bridge", "downstream_of"),
    ("from Gosnell Creek to Lamprey Creek", "between"),
    ("", "whole_water"),
])
def test_op_classification(scope, kind):
    from pipeline.regs.dfo_salmon.untangle import classify_scope

    assert classify_scope(scope)[0] == kind


def test_no_location_has_a_contradictory_op():
    """A sweep of the real corpus: no scope may say "between X and Y" while carrying a
    one-sided op, and no `between` may lack a two-ended phrase."""
    import re as _re

    from pipeline.regs.dfo_salmon.locations import extract
    from pipeline.regs.dfo_salmon.untangle import untangle

    for slug in ("1", "2", "3", "5a", "5b", "6", "7", "8"):
        locs, _, _ = extract(untangle(parse_region(_load(slug), slug)))
        for l in locs:
            if l.kind != "water":
                continue
            head = _re.split(r"\bnote:|\bexcept\b|\bunless\b",
                             l.specific_area.lower())[0] or l.specific_area.lower()
            two_ended = bool(_re.search(r"\bbetween\b.+\band\b|\bfrom\b.+\bto\b", head))
            if l.op in ("upstream_of", "downstream_of"):
                assert not two_ended, f"[{slug}] {l.water}: {l.specific_area[:70]}"


# ---------------------------------------------------------------------------
# anchor types — triage only
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("scope,expected", [
    # order is first appearance in the sentence; "Sandy Pool Regional Park" is a
    # place, not a pool feature
    ("from 66 Mile Trestle downstream to the white triangle boundary sign located in "
     "Sandy Pool Regional Park", ["bridge", "boundary_sign", "place_name"]),
    ("downstream of B.C. Hydro Dam to the CPR Railway Bridge", ["dam_or_hatchery", "bridge"]),
    ("upstream of Highway #16 bridge", ["road", "bridge"]),
    ("downstream of the confluence with the Quinsam River", ["confluence"]),
    ("upstream of Parker Creek", ["confluence"]),          # a bare stream name IS a confluence
    ("that portion between the Johnson Subdivision Bridge (52°58.373'N; 122°29.349'W) "
     "and the powerlines", ["bridge", "latlon", "powerline"]),
    ("", []),
    ("including tributaries", []),
])
def test_anchor_types(scope, expected):
    """Types drive triage — a confluence is derivable from the stream graph, a physical
    boundary sign is not. Order is first appearance in the sentence."""
    from pipeline.regs.dfo_salmon.untangle import anchor_types

    assert anchor_types(scope) == expected


def test_named_streams_feed_confluence_matching():
    """`splits.json` stores a confluence as "Parker Creek → Nitinat River", so matching
    a DFO scope to it needs the stream name out of the sentence."""
    from pipeline.regs.dfo_salmon.untangle import named_streams

    assert named_streams("upstream of Parker Creek") == ["Parker Creek"]
    assert named_streams("from Gosnell Creek to Lamprey Creek") == ["Gosnell Creek", "Lamprey Creek"]
    assert named_streams("upstream of Highway #16 bridge") == []


def test_types_are_not_taken_from_an_except_clause():
    """"…downstream to the tidal boundary, except for the canyon as listed below" —
    the canyon belongs to a different reach."""
    from pipeline.regs.dfo_salmon.untangle import classify_scope

    kind, types, _, _ = classify_scope(
        "from the confluence with Crag Creek downstream to the tidal boundary, "
        "except for the canyon as listed below")
    assert kind == "between"
    assert "natural_feature" not in types      # the canyon is in the except clause
    assert "tidal_boundary" in types and "confluence" in types


def test_every_location_carries_types_it_can_be_triaged_on():
    from pipeline.regs.dfo_salmon.locations import extract
    from pipeline.regs.dfo_salmon.untangle import untangle

    directional = {"upstream_of", "downstream_of", "between"}
    typed = untyped = 0
    for slug in ("1", "2", "6"):
        locs, _, _ = extract(untangle(parse_region(_load(slug), slug)))
        for l in locs:
            if l.op in directional:
                typed += bool(l.anchor_types)
                untyped += not l.anchor_types
    assert typed > 4 * untyped, f"{untyped} directional scopes have no anchor type"


# ---------------------------------------------------------------------------
# match: names -> registry items
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name,expect_rung", [
    ("Somass River tributaries", "Somass River"),
    ("Alouette River and tributaries", "Alouette River"),
    ("Chilliwack/Vedder River (including Sumas River)", "Chilliwack River"),
    ("Chehalis River Hatchery", "Chehalis River"),
    ("Adam and Eve Rivers", "Adam River"),
    ("Khutze", "Khutze River"),
    ("Washlawlis River", "Washlawlis Creek"),
])
def test_name_candidate_ladder_reaches_the_real_name(name, expect_rung):
    """DFO writes scope into the name column, compounds two waters, and disagrees with
    the FWA gazetteer on the feature word. Each is a rung on the ladder."""
    from pipeline.regs.dfo_salmon.match import name_candidates

    assert expect_rung in [c for c, _ in name_candidates(name)]


def test_lake_is_never_swapped_for_river_or_creek():
    """A lake and the river draining it are different waters and usually BOTH exist,
    so swapping the feature word silently binds the wrong one. Measured: it turned
    Yakoun River into Yakoun Lake, Lakelse River into Lakelse Lake, and Long Lake into
    Long Creek."""
    from pipeline.regs.dfo_salmon.match import name_candidates

    # endswith, not `in` — "Lakelse River" contains "Lake" as a substring.
    for name in ("Yakoun River", "Lakelse River"):
        assert not any(c.endswith("Lake") for c, _ in name_candidates(name)), name
    for name in ("Long Lake", "Morice Lake"):
        rungs = [c for c, _ in name_candidates(name)]
        assert not any(c.endswith("River") or c.endswith("Creek") for c in rungs), name


def test_river_creek_swap_is_still_offered():
    from pipeline.regs.dfo_salmon.match import name_candidates

    assert "Braverman Creek" in [c for c, _ in name_candidates("Braverman River")]
    assert "Rainy River" in [c for c, _ in name_candidates("Rainy Creek")]


def test_the_ladder_is_ordered_most_faithful_first():
    from pipeline.regs.dfo_salmon.match import name_candidates

    rungs = [c for c, _ in name_candidates("Somass River tributaries")]
    assert rungs[0] == "Somass River tributaries"
    assert rungs.index("Somass River") < len(rungs)


@pytest.mark.slow
def test_match_report_against_the_real_registry():
    """Needs data/generated/atlas/full/registry.json. Guards the two failure modes that matter:
    an ambiguous name must never be 'resolved' by a less faithful rung, and a fuzzy
    near-spelling must never auto-bind."""
    from pipeline.regs.dfo_salmon.entries import load
    from pipeline.regs.dfo_salmon.match import REGISTRY, propose

    if not REGISTRY.exists():
        pytest.skip("registry not built")
    from pipeline.regs.matching.matcher import build_id_index, build_name_index, load_overrides
    from pipeline.atlas.registry.io import load_registry

    reg = load_registry(str(REGISTRY))
    ni, ii = build_name_index(reg), build_id_index(reg)
    ov = load_overrides("__default__")

    # The ENTRY FILE: `propose` walks its waters. This passed `.locations`, a list, and failed on
    # `ef.waters` before asserting anything.
    props = propose(load("6"), reg, ni, ii, ov)
    by_water = {p.water: p for p in props}

    # The name is right and merely ambiguous — a curator picks, nothing auto-binds. (Yakoun,
    # Lakelse and Kwinimass were the examples once; the registry's name variants resolve all three
    # exactly now, so the property is asserted over every proposal rather than over a name list.)
    bear = by_water["Bear River"]
    assert bear.status == "ambiguous" and bear.bindable is False and len(bear.candidates) > 1

    # Nothing but an exact hit or an override ever binds; a suggestion never does.
    for p in props:
        if p.status == "matched":
            assert p.via in ("exact name", "override"), p.water
        else:
            assert p.item_ids == [] and p.bindable is False, p.water


def test_worklist_shows_existing_splits_and_the_full_scope(tmp_path):
    from pipeline.regs.dfo_salmon.match import worklist

    text = worklist("6")
    assert "Babine River" in text
    assert "splits already curated" in text
    assert "Nilkitkwa River" in text          # an existing split is listed for reuse
    assert "SPLITS TO ADD:" in text
    # the verbatim scope is what the curator reads, so it must be present in full
    assert "excluding tributaries and those waters within a 400 m radius" in text


# ---------------------------------------------------------------------------
# grouping drift variants — contestable
# ---------------------------------------------------------------------------


def _ef_with(scopes, water="Test River", section="B(i)", op="upstream_of", tribs=None):
    from pipeline.regs.dfo_salmon.entries import Binding, EntryFile, EntryLocation

    ef = EntryFile(region="6", region_number=6, region_name="Skeena")
    for i, (sc, status) in enumerate(scopes):
        ef.locations.append(EntryLocation(
            location_id=f"6:test:{i}", region="6", region_number=6, kind="water",
            precedence=3, water=water, section=section,
            source_text={"specific_area": sc, "op": op},
            binding=Binding(tributaries=tribs), status=status))
    return ef


def test_grouping_folds_a_reworded_scope():
    from pipeline.regs.dfo_salmon.entries import propose_groups

    ef = _ef_with([("within a 400 m radius of the mouth of Pinkut Creek", "active"),
                   ("within a 400m radius of the mouth of Pinkut Creek", "dormant")])
    changed = propose_groups(ef)
    assert len(changed) == 1
    primary, group = changed[0]
    assert ef.locations[0].location_id == primary        # the live one leads
    assert [v for v, _ in group] == [ef.locations[1].location_id]
    assert ef.locations[1].is_variant is True


def test_grouping_never_folds_opposite_sides_of_the_same_landmark():
    """"upstream of the 112th Street bridge" and "downstream of the 112th Street
    bridge" score 0.92 on text alone — four characters apart, and the two opposite
    halves of the river. `op` has to gate the comparison."""
    from pipeline.regs.dfo_salmon.entries import EntryFile, propose_groups
    from pipeline.regs.dfo_salmon.entries import Binding, EntryLocation

    ef = EntryFile(region="2", region_number=2, region_name="Lower Mainland")
    for i, (sc, op) in enumerate([
            ("upstream of the 112th Street bridge", "upstream_of"),
            ("downstream of the 112th Street bridge", "downstream_of")]):
        ef.locations.append(EntryLocation(
            location_id=f"2:kanaka:{i}", region="2", region_number=2, kind="water",
            precedence=3, water="Kanaka Creek", section=None,
            source_text={"specific_area": sc, "op": op}, binding=Binding()))
    assert propose_groups(ef) == []
    assert all(l.duplicate_of is None for l in ef.locations)


def test_grouping_never_folds_across_tributary_scope():
    """Meziadin Lake publishes "including tributaries" and "excluding tributaries" —
    0.90 on text, and opposite in meaning."""
    from pipeline.regs.dfo_salmon.entries import Binding, EntryFile, EntryLocation, propose_groups

    ef = EntryFile(region="6", region_number=6, region_name="Skeena")
    for i, (sc, tr) in enumerate([("including tributaries", True),
                                  ("excluding tributaries", False)]):
        ef.locations.append(EntryLocation(
            location_id=f"6:meziadin:{i}", region="6", region_number=6, kind="water",
            precedence=3, water="Meziadin Lake", section="C",
            source_text={"specific_area": sc, "op": "whole_water"},
            binding=Binding(tributaries=tr)))
    assert propose_groups(ef) == []


def test_a_contested_variant_stays_separate_forever():
    from pipeline.regs.dfo_salmon.entries import contest, propose_groups

    ef = _ef_with([("within a 400 m radius of the mouth of Pinkut Creek", "active"),
                   ("within a 400m radius of the mouth of Pinkut Creek", "dormant")])
    propose_groups(ef)
    victim = ef.locations[1].location_id
    assert contest(ef, victim) is True
    assert ef.locations[1].is_variant is False
    assert ef.locations[1].duplicate_confirmed is False

    # ...and a later grouping pass must not re-absorb it.
    assert propose_groups(ef) == []
    assert ef.locations[1].duplicate_of is None


def test_grouping_is_idempotent():
    from pipeline.regs.dfo_salmon.entries import propose_groups

    ef = _ef_with([("within a 400 m radius of the mouth of Pinkut Creek", "active"),
                   ("within a 400m radius of the mouth of Pinkut Creek", "dormant")])
    assert len(propose_groups(ef)) == 1
    assert propose_groups(ef) == []


def test_automatic_groupings_never_pair_different_ops():
    """The AUTOMATIC grouper buckets on (water, section, op, tributaries) and must not cross any of
    them — two scopes differing on the op are not the same reach as far as text can tell.

    A CURATOR-CONFIRMED grouping is exempt, and has to be: the Somass's island reach parses
    `upstream_of` ("200 metres above and 150 metres below the island") while its live replacement
    parses `between` ("from the northern boundary of Somass Park downstream to the southern"), and
    they are the same 660 m of river — measured at 661 m and 663 m, two metres apart. The op is a
    property of the WORDING; the reach is a property of the geometry. Only a human (or a resolved
    section comparison) can see past that, which is exactly what `duplicate_confirmed` records.
    """
    from pipeline.regs.dfo_salmon.entries import load

    checked = 0
    for slug in ("1", "2", "6", "8"):
        ef = load(slug)
        by_id = {l.location_id: l for l in ef.locations}
        for loc in ef.locations:
            if not loc.is_variant or loc.duplicate_confirmed:
                continue                      # curator-confirmed groupings may cross ops
            primary = by_id[loc.duplicate_of]
            checked += 1
            assert loc.source_text.get("op") == primary.source_text.get("op"), loc.location_id
            assert loc.binding.tributaries == primary.binding.tributaries, loc.location_id
            assert loc.section == primary.section, loc.location_id
    assert checked, "expected at least one automatic grouping to check"


def test_a_curator_confirmed_grouping_records_why_it_crossed_an_op():
    """An exemption that carries no reasoning is indistinguishable from a mistake."""
    from pipeline.regs.dfo_salmon.entries import load

    for slug in ("1", "2", "5b", "6"):
        for loc in load(slug).locations:
            if not (loc.is_variant and loc.duplicate_confirmed):
                continue
            assert loc.binding.notes, "%s: confirmed grouping with no note" % loc.location_id


# ---------------------------------------------------------------------------
# binding is exact-only, and lives on the water
# ---------------------------------------------------------------------------


def test_a_water_is_bound_once_and_serves_every_reach_on_it():
    from pipeline.regs.dfo_salmon.entries import load

    ef = load("6")
    skeena = [l for l in ef.locations if l.water == "Skeena River"]
    assert len(skeena) > 8
    assert len({l.water_id for l in skeena}) == 1, "one water, one binding"
    w = ef.water(skeena[0].water_id)
    assert w is not None and w.name == "Skeena River"


def test_only_an_exact_hit_is_ever_written():
    """No retry, no near-spelling. Those bound Yakoun River to Yakoun Lake.

    `curator` is the one other allowed provenance, and it is not a loophole: it means a HUMAN
    answered the question the matcher refused to guess at. The rule this protects is that the
    MATCHER never writes a binding it did not get exactly right — see the module docstring.
    """
    from pipeline.regs.dfo_salmon.entries import load

    for slug in ("1", "2", "6"):
        for w in load(slug).waters:
            if not w.bound:
                continue
            via = (w.match or {}).get("via", "")
            assert via in ("exact name", "override", "curator", ""), f"{w.name}: bound via {via!r}"
            if via == "curator":
                assert (w.match or {}).get("reason"), f"{w.name}: a curator binding must say why"


def test_suggestions_are_recorded_but_never_applied():
    """The retry/spelling ladder is DISPLAY ONLY. It surfaces a candidate for a human to look at and
    never writes a binding — a River->Creek retry is how Yakoun River was once bound to Yakoun Lake.

    Asserted as an INVARIANT over every region rather than against one named water: this test was
    pinned to Cayeghle River and then to Washlawlis River, and both got answered out from under it as
    the curation ran. The rule does not depend on which waters are still open.

    Every water resolved so far was answered EXPLICITLY — a name_variants entry, an override, or
    both — never by letting the ladder through.
    """
    from pipeline.regs.dfo_salmon.entries import load
    from pipeline.regs.dfo_salmon.match import ENTRIES_DIR

    seen_any = False
    for path in sorted(ENTRIES_DIR.glob("region-*.json")):
        for w in load(path.stem.split("region-")[1]).waters:
            sug = (w.match or {}).get("suggestions") or []
            if not sug:
                continue
            seen_any = True
            assert w.bound is False, f"{w.name}: a suggestion was applied"
            assert w.item_ids == [], f"{w.name}: bound to {w.item_ids} off a suggestion"
    assert seen_any, "expected at least one water still carrying suggestions"


def test_ambiguous_names_are_left_for_a_curator():
    """Bear River and Lakelse River are no longer here: the SHARED overrides file
    already answers them (Bear River in Region 6 is gnis:15535). That is the argument
    for one overrides file rather than a DFO copy."""
    from pipeline.regs.dfo_salmon.entries import load

    # Long Lake was the LAST genuinely-ambiguous water, and the only one of the four whose candidates
    # were all real: Region 5 has FIVE gazetted Long Lakes (gnis 17503/17508/17509/17513/31514), one
    # per MU, so name+region does not identify it for the PROVINCE. That is what `source: "dfo"` is
    # for — the entry answers a question only this matcher asks, and the provincial matcher never
    # loads it. Scoping by region alone would have bound any future Region-5 Long Lake row to the
    # Smith Inlet one, wrong four times in five.
    ef5b = load("5b")
    w = next(x for x in ef5b.waters if x.name == "Long Lake")
    assert w.bound is True and w.item_ids == ["wbk:329462886"]
    assert (w.match or {}).get("via") == "override"

    # Rainy Creek WAS ambiguous, and its two "candidates" were both wrong — Kootenay creeks 1,000 km
    # from the north-coast water DFO means, matched on the bare name alone. FWA does not name that
    # stream at all, so it was fixed by NAMING it: a name_variants entry gives wsc 910-999299 the
    # name, and the registry build mints an item for it. All three "Rainy Creek"s now exist, but only
    # one is in region 6, so `_disambiguate` picks it by region and no override is needed.
    ef6 = load("6")
    rainy = next(x for x in ef6.waters if x.name == "Rainy Creek")
    assert rainy.bound is True and rainy.item_ids == ["wsc:910-999299"]
    assert (rainy.match or {}).get("via") == "exact name"

    # Yakoun River and Pallant Creek WERE on that list, and both were ambiguous for the same reason:
    # the LAKE beside each carried the river's name as a registry variant (Yakoun Lake had
    # 'YAKOUN RIVER', Mosquito Lake 'PALLANT CREEK'), so an exact-name lookup legitimately hit two
    # items. They were answered with an override first, but the REAL fix was upstream: the registry
    # build now refuses to give a still-water item a flowing-water variant that a stream already owns
    # as its primary name. With the root cause gone the collision does not exist, both resolve by
    # plain exact name, and the overrides were deleted as redundant. `via` proves it.
    for name, iid in (("Yakoun River", "gnis:3485"), ("Pallant Creek", "gnis:8515")):
        w = next(x for x in ef6.waters if x.name == name)
        assert w.bound is True and w.item_ids == [iid], name
        assert (w.match or {}).get("via") == "exact name", name

    bear = next(x for x in ef6.waters if x.name == "Bear River")
    assert bear.bound is True and bear.item_ids == ["gnis:15535"]
    assert (bear.match or {}).get("via") == "override"


def test_the_shared_overrides_file_is_actually_loaded():
    """`load_overrides("__default__")` treats the sentinel as a path, finds nothing and
    returns [] — every match run did that until it was caught. The sentinel belongs to
    `reach.covered.make_matcher`."""
    from pipeline.regs.matching.matcher import load_overrides
    from pipeline.atlas.reach.covered import DEFAULT_OVERRIDES

    assert load_overrides("__default__") == []
    assert DEFAULT_OVERRIDES.exists()
    assert len(load_overrides(DEFAULT_OVERRIDES)) > 400


def test_item_ids_is_a_list_so_one_name_can_be_several_waters():
    """One DFO row can be several registry items, and no heuristic could pick them.

    "Chilliwack/Vedder River (including Sumas River)" is THREE: Chilliwack + Vedder (whose item now
    holds the Vedder Canal polygon too — one flowing water, lines and polygons) plus the Sumas (its
    polygon likewise its river's). The provincial row for the same system is named
    "(does not include Sumas River)", so the two answers genuinely differ and the DFO one is tagged
    source=dfo. "Adam and Eve Rivers" is two, the Eve flowing into the Adam.
    """
    from pipeline.regs.dfo_salmon.entries import WaterBinding, load

    ef = load("2")
    chilliwack = next(w for w in ef.waters if w.name.startswith("Chilliwack/Vedder"))
    assert chilliwack.bound is True
    # THREE since 2026-10-03: the Vedder Canal's polygon is the Vedder River's item and the
    # Sumas's polygon the Sumas River's (`registry.flowing`), so the file names the rivers
    assert chilliwack.item_ids == ["gnis:8634", "gnis:3062", "gnis:26216"]

    adam = next(w for w in load("1").waters if w.name == "Adam and Eve Rivers")
    assert adam.item_ids == ["gnis:28136", "gnis:31112"]

    # the model itself imposes no arity — that is the whole point of the field being a list
    w = WaterBinding(water_id="x", region="1", region_number=1, name="x", item_ids=["gnis:a", "gnis:b"])
    assert w.bound is True and len(w.item_ids) == 2


def test_a_locked_water_is_never_overwritten_by_a_later_match_run():
    from pipeline.regs.dfo_salmon.entries import load, save

    import tempfile
    from pathlib import Path as _P

    with tempfile.TemporaryDirectory() as td:
        ef = load("6")
        w = next(x for x in ef.waters if not x.bound)
        w.item_ids = ["gnis:curated"]
        w.locked = True
        save(ef, _P(td))
        back = load("6", _P(td))
        got = next(x for x in back.waters if x.water_id == w.water_id)
        assert got.item_ids == ["gnis:curated"] and got.locked is True


# ---------------------------------------------------------------------------
# extents are the provincial model, not DFO dicts
# ---------------------------------------------------------------------------


def test_binding_holds_real_extent_objects():
    """One definition of "upstream of split s" in the repo. Storing dicts let the DFO
    side drift from what the resolver implements."""
    from pipeline.regs.dfo_salmon.entries import Binding
    from pipeline.regs.parsing.entry_models import Extent

    b = Binding(extents=[Extent(op="between", splits=["a", "b"])])
    assert isinstance(b.extents[0], Extent)
    assert b.extents[0].op.value == "between"


@pytest.mark.parametrize("op,splits", [
    ("upstream_of", ["a", "b"]),      # needs exactly 1
    ("downstream_of", []),            # needs exactly 1
    ("between", ["a"]),               # needs exactly 2
    ("whole", ["a"]),                 # takes none
])
def test_the_entry_file_cannot_hold_a_malformed_extent(op, splits):
    """Arity is enforced by the model, so a bad extent cannot be written at all."""
    from pipeline.regs.parsing.entry_models import Extent

    with pytest.raises(Exception):
        Extent(op=op, splits=splits)


def test_binding_round_trips_through_json():
    from pipeline.regs.dfo_salmon.entries import Binding
    from pipeline.regs.parsing.entry_models import Extent

    b = Binding(extents=[Extent(op="upstream_of", splits=["s1"])],
                tributaries=True, tributaries_only=False,
                tributary_excludes=[Extent(op="whole", item_id="item:morice")],
                notes=["closed within 400 m"], spatial_caveat=True)
    back = Binding.from_dict(json.loads(json.dumps(b.to_dict())))
    assert back.extents == b.extents
    assert back.tributary_excludes == b.tributary_excludes
    assert back.tributaries is True and back.spatial_caveat is True


def test_reach_input_dumps_models_at_the_boundary():
    """We STORE models and hand build_reach plain dicts."""
    from pipeline.regs.dfo_salmon.entries import Binding, WaterBinding, to_reach_input, load
    from pipeline.regs.parsing.entry_models import Extent

    ef = load("6")
    loc = next(l for l in ef.locations if l.kind == "water")
    loc.binding = Binding(extents=[Extent(op="upstream_of", splits=["s1"])], tributaries=True)
    water = WaterBinding(water_id=loc.water_id, region="6", region_number=6,
                         name=loc.water, item_ids=["gnis:1"])
    entry, rules = to_reach_input(loc, [{"species": "Coho"}], water)
    assert isinstance(rules[0]["extents"][0], dict)
    assert entry["includes_tributaries"] is True
    json.dumps(entry); json.dumps(rules)          # must be serialisable


def test_skip_overrides_are_ignored_on_the_dfo_side():
    """A `skip` is a synopsis-LAYOUT fact ("this row points at another row"), not a
    name fact. All three DFO hit are names the registry resolves on its own; honouring
    the skip only suppressed a good match."""
    from pipeline.regs.dfo_salmon.match import drop_skips
    from pipeline.regs.matching.matcher import load_overrides
    from pipeline.atlas.reach.covered import DEFAULT_OVERRIDES

    allo = load_overrides(DEFAULT_OVERRIDES)
    kept = drop_skips(allo)
    # `skip` has since been removed from the file entirely (see
    # test_no_override_uses_skip_any_more), so this is now a no-op safety net that
    # keeps a re-introduced skip from silently suppressing a DFO match.
    assert len(kept) == len(allo)
    assert all(not e.get("skip") for e in kept)
    assert drop_skips(allo + [{"skip": True, "criteria": {}}]) == kept


@pytest.mark.parametrize("slug,name,item_id", [
    ("6", "Ishkheenickh River", "gnis:4069"),      # renamed Ksi Hlginx
    ("6", "Tseax River", "gnis:3828"),             # renamed Ksi Sii Aks
    ("2", "Little Campbell River", "gnis:7250"),   # MU 2-4, not the Island Campbell
])
def test_renamed_rivers_bind_to_the_same_item(slug, name, item_id):
    """An old name and its current gazetted name are the SAME water. The registry
    already carries the old name as a variant, so it binds — no override needed."""
    from pipeline.regs.dfo_salmon.entries import load

    w = next(x for x in load(slug).waters if x.name == name)
    assert w.bound is True, name
    assert w.item_ids == [item_id]


def test_a_multi_item_override_binds():
    """Requiring exactly one item id silently refused every curated multi-water
    override — Fraser River in Region 2 names 13 items, Nicomen Slough names 7.
    A curated list is the opposite of ambiguous."""
    from pipeline.regs.dfo_salmon.entries import load

    fraser = next(w for w in load("2").waters if w.name == "Fraser River")
    assert fraser.bound is True
    assert len(fraser.item_ids) > 1
    assert (fraser.match or {}).get("via") == "override"


def test_ambiguous_is_still_refused():
    """The guard that matters: several items the matcher could not choose between is
    NOT the same as several items a curator chose."""
    from pipeline.regs.dfo_salmon.match import Proposal

    curated = Proposal("w", "6", "Fraser River", "matched", item_ids=["a", "b"], via="override")
    guessed = Proposal("w", "6", "Bear River", "ambiguous",
                       candidates=[{"item_id": "a"}, {"item_id": "b"}])
    assert curated.bindable is True
    assert guessed.bindable is False


# ---------------------------------------------------------------------------
# overrides: no `skip`, only `not_found`
# ---------------------------------------------------------------------------


def test_no_override_uses_skip_any_more():
    """`skip` conflated two different things: "this name IS that water" (a variant,
    which should LINK) and "there is no correct item" (which should be `not_found`).
    Neither is a reason to refuse a name outright."""
    from pipeline.regs.matching.matcher import load_overrides
    from pipeline.atlas.reach.covered import DEFAULT_OVERRIDES

    ov = load_overrides(DEFAULT_OVERRIDES)
    assert ov, "overrides file should not be empty"
    assert [e for e in ov if e.get("not_found")], "not_found should now be in use"
    offenders = [(e.get("criteria") or {}).get("name_verbatim") for e in ov if e.get("skip")]
    assert offenders == [], f"skip is no longer a valid override field: {offenders}"
    assert not any(e.get("variant_of") for e in ov), \
        "variant_of should have been resolved to direct ids"


def test_not_found_does_not_fall_through_to_a_wrong_name_match():
    """`not_found` is a PLACEHOLDER, so it must block matching. The name usually does
    resolve — just to the wrong water: "REDFERN LAKE" in MU 5-15 finds the MU 7-42
    Redfern Lake. Falling through would bind the wrong lake."""
    from pipeline.regs.matching.matcher import (build_id_index, build_name_index,
                                           build_override_index, load_overrides, match_row)
    from pipeline.atlas.reach.covered import DEFAULT_OVERRIDES
    from pipeline.atlas.registry.io import load_registry

    reg = load_registry(GENERATED.build() / "registry.json")
    ni, ii = build_name_index(reg), build_id_index(reg)
    ovx = build_override_index(load_overrides(DEFAULT_OVERRIDES))

    r = match_row(0, {"water": "REDFERN LAKE", "region": "REGION 5", "mu": ["5-15"]},
                  reg, ni, ii, ovx)
    assert r.status == "unmatched"
    assert r.via == "override_not_found"
    assert r.item_id is None


def test_a_renamed_water_links_instead_of_being_refused():
    from pipeline.regs.matching.matcher import (build_id_index, build_name_index,
                                           build_override_index, load_overrides, match_row)
    from pipeline.atlas.reach.covered import DEFAULT_OVERRIDES
    from pipeline.atlas.registry.io import load_registry

    reg = load_registry(GENERATED.build() / "registry.json")
    ni, ii = build_name_index(reg), build_id_index(reg)
    ovx = build_override_index(load_overrides(DEFAULT_OVERRIDES))
    for name, region, item in [("ISHKHEENICKH RIVER", "REGION 6", "gnis:4069"),
                               ("TSEAX RIVER", "REGION 6", "gnis:3828"),
                               ("COPPER RIVER", "REGION 6", "gnis:3137")]:
        r = match_row(0, {"water": name, "region": region, "mu": []}, reg, ni, ii, ovx)
        assert r.item_id == item, (name, r.status, r.item_id)



def test_a_dfo_answer_never_reaches_a_provincial_row():
    """Every remaining DFO override is `source: "dfo"`, and the provincial matcher must not load one.

    Region scoping was the first attempt at this and is fragile: `_pick_override` scores MU overlap
    3 > region 2, picks with `>`, so two entries tying at 3 are decided by FILE ORDER. The tag
    removes the contest — the provincial matcher never sees the entry at all.

    Only 7 DFO overrides remain. The other 17 were deleted once the registry build stopped creating
    the collisions they existed to work around (a lake answering to its river) and the name_variants
    entries made the rest resolve by name. What is left is the genuinely irreducible set: rows that
    name SEVERAL registry items, tributary-only rows, and Long Lake (five gazetted Long Lakes in one
    region, so name+region cannot identify it).
    """
    from pathlib import Path

    from pipeline.regs.matching.matcher import load_overrides

    path = CURATED.regulations.overrides
    prov, dfo = load_overrides(path), load_overrides(path, source="dfo")
    tagged = [e for e in dfo if (e.get("source") or "") == "dfo"]
    assert tagged, "expected DFO-tagged overrides"
    assert len(dfo) == len(prov) + len(tagged)
    assert not [e for e in prov if e.get("source")], "a tagged override leaked to the province"

    names = {(e.get("criteria") or {}).get("name_verbatim") for e in tagged}
    assert {"LONG LAKE", "ADAM AND EVE RIVERS"} <= names
    # a pure name-mismatch must NOT be here any more — the name_variants entry is the mechanism
    assert not ({"PALLANT CREEK", "YAKOUN RIVER", "RAINY CREEK", "BRAVERMAN RIVER"} & names), \
        "a redundant name-only override came back; it should be a name_variants entry instead"


def test_no_water_is_left_ambiguous_without_a_curator_answer():
    """Every ambiguous water is now resolved, and each by the mechanism its ambiguity called for.

    The four were not the same problem, and treating them the same would have been wrong:

      Yakoun River / Pallant Creek  the LAKE beside each carries the river's name as a registry
                                    variant -> a shared, region-scoped override (the name really
                                    does mean one water in that region).
      Rainy Creek                   the candidates were Region-4 collisions and the real water is
                                    UNNAMED in FWA -> name_variants + an override on its wsc.
      Long Lake                     five real Long Lakes in one region -> a curator binding on the
                                    entry, because the name does NOT determine the water.
    """
    from pipeline.regs.dfo_salmon.entries import load

    from pipeline.regs.dfo_salmon.match import ENTRIES_DIR

    still = []
    for p in sorted(ENTRIES_DIR.glob("region-*.json")):
        slug = p.stem.split("region-")[1]
        for w in load(slug).waters:
            if (w.match or {}).get("status") == "ambiguous":
                still.append(f"{slug}:{w.name}")
    assert not still, f"ambiguous waters left unanswered: {still}"


def test_a_lake_does_not_answer_to_the_river_that_threads_it():
    """FWA hangs a through-river's gazetted name on the lake polygon, so an exact search for the
    RIVER hit two items and had to be disambiguated by hand.

    `_foreign` in registry/build could not catch it: the borrowed name carries the LAKE's own gnis,
    not a neighbour's. 21 items corpus-wide — Yakoun Lake answering to 'YAKOUN RIVER', Mosquito Lake
    to 'PALLANT CREEK', and Lakelse Lake to 'LAKELSE RIVER', which this package's own matcher
    docstring names as a historical WRONG binding.

    Asserted on the build rule rather than the built registry, because the registry on disk predates
    the fix until the next full build.
    """
    from pipeline.atlas.registry.build import STILL_RE, _norm_name
    from pipeline.common.water_kind import flows      # THE one "flows" (2026-10-03), head noun

    for lake, river in (("Yakoun Lake", "YAKOUN RIVER"), ("Mosquito Lake", "PALLANT CREEK"),
                        ("Lakelse Lake", "LAKELSE RIVER"), ("Nation Lakes", "NATION RIVER")):
        assert STILL_RE.search(lake), lake
        assert flows("lake", river), river

    # ...and the guard must NOT fire on a lake's own name, nor on a still-water name that merely
    # contains a flowing word ("River Lakes", "Bear Creek Reservoir": the head noun decides).
    for keep in ("Yakoun Lake", "Nation Lakes", "Rainy Day Lake", "River Lakes",
                 "Bear Creek Reservoir"):
        assert not flows("lake", keep), keep
    assert _norm_name("  YAKOUN   RIVER ") == "yakoun river"



def test_a_dfo_tagged_override_is_invisible_to_the_provincial_matcher():
    """`source: "dfo"` says WHO IS ASKING, which region-scoping could only approximate.

    Six answers are DFO-only. Two of them could not be expressed safely any other way: LONG LAKE
    (five gazetted Long Lakes in Region 5) and ADAM AND EVE RIVERS (one DFO row covering two items
    the province regulates separately). Loading the file without a source must not return them, or a
    provincial row could pick one up.
    """
    from pathlib import Path

    from pipeline.regs.matching.matcher import load_overrides

    p = CURATED.regulations.overrides
    prov, dfo = load_overrides(p), load_overrides(p, source="dfo")
    tagged = [e for e in dfo if (e.get("source") or "") == "dfo"]
    assert tagged, "expected DFO-tagged overrides"
    assert len(dfo) == len(prov) + len(tagged)
    assert not [e for e in prov if e.get("source")], "a tagged override leaked to the province"
    names = {(e.get("criteria") or {}).get("name_verbatim") for e in tagged}
    assert {"LONG LAKE", "ADAM AND EVE RIVERS"} <= names


def test_a_dfo_name_variant_never_relabels_a_gazetted_water():
    """`NameSource.dfo` is how a DFO name reaches the registry, and it ranks BELOW gazette.

    Two shapes, and the difference is `display`:

      Braverman  FWA gazettes 'Braverman Creek'; DFO says 'Braverman River'. The variant makes the
                 DFO name searchable and resolvable, `display: false` so the gazetted label stands.
      Docee /    FWA names the stream NOTHING at all, so there is no gazetted label to protect and
      Rainy      the DFO name IS the display name (`display: true`).

    Braverman is also the case for why the matcher would not do this itself: 'River' -> 'Creek' is on
    its retry ladder, but it only ever writes an EXACT hit — that class of retry once bound Yakoun
    River to Yakoun Lake.
    """
    import json
    from pathlib import Path

    from pipeline.common.models.enums import NameSource

    nv = json.loads((CURATED.waters.name_variants)
                    .read_text(encoding="utf-8"))
    by_wsc = {w: e for e in nv for w in (e.get("target") or {}).get("wscs", [])}

    assert NameSource("dfo") is NameSource.dfo
    order = list(NameSource)
    assert order.index(NameSource.dfo) > order.index(NameSource.gazette), \
        "a DFO name must never outrank the gazetteer"

    brav = by_wsc["950-071751"]["names"][0]
    assert brav["name"] == "Braverman River" and brav["source"] == "dfo"
    assert brav["display"] is False, "FWA gazettes this water as Braverman Creek"

    for wsc, name in (("910-020688", "Docee River"), ("910-999299", "Rainy Creek")):
        n = by_wsc[wsc]["names"][0]
        assert n["name"] == name and n["source"] == "dfo"
        assert n["display"] is True, "FWA names this stream nothing, so the DFO name is the label"


# ---------------------------------------------------------------------------
# The catalogue move — a scraped row becomes a typed rule
#
# The synopsis corpus retired the prose model in `1cc991fa`; DFO is the half that moved
# next. These pin the conversion, and every one of them is a wording the flags dropped
# before the move surfaced it.
# ---------------------------------------------------------------------------


def _row(**kw) -> dict:
    """A scraped row as the transcription hands it over — the printed columns, nothing read."""
    rec = {"species": "Chinook", "dates": "Jul 1 to Aug 31", "limits_gear": "",
           "fishery_notices": []}
    rec.update(kw)
    return rec


def _typed(**kw):
    from pipeline.regs.dfo_salmon.typed import to_rules
    return to_rules(_row(**kw), "t", 0)


def test_take_zero_says_whether_you_may_fish_at_all():
    """The 605-rule defect: `take=0` alone cannot tell a closure from catch-and-release."""
    closed, = _typed(limits_gear="No fishing for chinook")
    assert closed.take == 0 and closed.may_target is False

    release, = _typed(limits_gear="Non-retention")
    assert release.take == 0 and release.may_target is True


@pytest.mark.parametrize("gear", [
    "No retention of coho",
    "No retention of salmon",
    "Open for salmon catch and release",
])
def test_release_written_the_other_three_ways(gear):
    """Only 'Non-retention' was matched; these three read as stating nothing at all, which
    published a water with no rule rather than one you must release."""
    rules = _typed(species="All", limits_gear=gear)
    assert rules[0].take == 0 and rules[0].may_target is True
    assert not any(r.review_reason for r in rules)


def test_finfish_closure_is_a_closure():
    """10 rows. A closure that types as nothing is an open-looking water the page closes."""
    rule, = _typed(species="All", limits_gear="Finfish closure")
    assert rule.take == 0 and rule.may_target is False


def test_dfo_typo_still_closes_the_water():
    """DFO's own 'No fishinf for chinook'. One reading, so it is repaired rather than dropped."""
    rule, = _typed(limits_gear="No fishinf for chinook")
    assert rule.take == 0 and rule.may_target is False


@pytest.mark.parametrize("gear,take", [
    ("4 per day", 4),
    ("4 hatchery marked per day", 4),
    ("4 hatchery marked only per day", 4),   # the 'only' cost 4 rows their limit
    ("4 pink per day", 4),                   # the species word cost 1 more
])
def test_the_count_survives_the_words_between_it_and_per_day(gear, take):
    assert _typed(limits_gear=gear)[0].take == take


def test_a_size_sublimit_is_its_own_rule_pointing_at_its_parent():
    """`z2:trout_char_quota.r2` is the convention: "1 over 50 cm" WITHIN "Trout/char: 4".
    One rule carrying both numbers is the shape that loses the 4."""
    parent, sub = _typed(limits_gear="4 per day, only 2 over 50 cm.")
    assert (parent.take, parent.lengths) == (4, None)
    # "only 2 over 50 cm" COUNTS the big ones and leaves the rest to the parent's 4, so the
    # band is a bare minimum with no denial beneath it.
    assert (sub.take, sub.within) == (2, parent.rule_id)
    assert [b.model_dump(exclude_none=True) for b in sub.lengths] == [{"min_cm": 50}]


def test_none_over_is_a_release_rule_not_a_closure():
    parent, sub = _typed(limits_gear="4 per day, none over 50 cm")
    assert parent.take == 4
    # "none over 50 cm" denies the big ones and says nothing about the small ones — a release,
    # not a closure, which is what `may_target` carries.
    # A SIZE rule (2026-09-29): the band says take 0, the rule states no count of its own.
    assert (sub.take, sub.may_target) == (None, True)
    assert [b.model_dump(exclude_none=True) for b in sub.lengths] == [{"min_cm": 50, "take": 0}]


def test_a_bundled_row_is_two_rules_of_different_types():
    """"2 per day, bait ban" is a quota AND a bait ban — you obey both, so they never compete."""
    from pipeline.regs.parsing.catalogue import RuleType
    quota, bait = _typed(species="Sockeye", limits_gear="2 per day, bait ban — FN0846")
    assert quota.type is RuleType.retention_limit and quota.take == 2
    assert bait.type is RuleType.bait_restriction
    assert [c.model_dump(mode="json") for c in bait.gear] == [{"slot": "bait", "ban": ["any_bait"]}]
    assert quota.notice == bait.notice == "FN0846", "the notice that set the row is provenance"


def test_a_gear_rule_is_scoped_by_what_you_fish_for_not_what_you_may_keep():
    """A hook or bait rule binds every species you may catch. `species` would read it narrower
    than the law; `when_targeting` is the scope the tables actually state."""
    _, bait = _typed(species="Sockeye", limits_gear="2 per day, bait ban")
    assert bait.species == [] and bait.when_targeting == ["SK"]


def test_all_on_a_salmon_page_means_all_salmon():
    """Not all fish. Read as every species it would state a salmon closure over trout."""
    from pipeline.regs.dfo_salmon.typed import species_for
    assert species_for("All") == ["SA"]


def test_to_be_determined_is_a_state_not_a_parse_failure():
    rule, = _typed(species="All", limits_gear="To be determined")
    assert "determined" in rule.review_reason


def test_every_scraped_rule_types_and_keeps_its_chain_of_custody():
    """All 438 rows convert, and every rule's verbatim is a span of its own row — the check
    that caught an invented sub-limit on the synopsis corpus's first sample."""
    from pipeline.regs.dfo_salmon.typed import row_text, to_rules
    from pipeline.regs.parsing.catalogue import squash

    rules_dir = Path(GENERATED.regs.dfo_salmon) / "rules"
    rows = made = 0
    for path in sorted(rules_dir.glob("region-*.json")):
        for i, rec in enumerate(json.loads(path.read_text(encoding="utf-8"))["rules"]):
            typed = to_rules(rec, "t", i)
            assert typed, f"{path.name} row {i} typed to nothing"
            haystack = squash(row_text(rec))
            for rule in typed:
                assert squash(rule.verbatim) in haystack, f"{rule.rule_id}: verbatim is not in its row"
            rows += 1
            made += len(typed)
    assert rows == 438, f"the corpus is 438 rows, got {rows}"
    assert made > rows, "a bundled row states more than one rule"


def test_the_feed_revalidates_from_disk():
    """What is on disk is what the catalogue model accepts — not merely what we wrote."""
    from pipeline.regs.parsing.catalogue import CatalogueRule

    feed_dir = Path(GENERATED.regs.dfo_salmon) / "typed"
    total = 0
    for path in sorted(feed_dir.glob("region-*.json")):
        for loc in json.loads(path.read_text(encoding="utf-8"))["locators"]:
            for rule in loc["rules"]:
                CatalogueRule(**rule)
                total += 1
    assert total == 517, f"expected the 517 typed rules on disk, got {total}"


def test_the_feed_takes_no_curated_input():
    """Rules are a feed and locators are curated: they meet at read time, never in one file.
    A feed that read curated state could not be rebuilt without it — and, worse, a scheduled
    run rewriting 517 rules inside `data/curated/` would be rewriting the half that never moves.
    """
    src = (Path("pipeline/regs/dfo_salmon/feed.py")).read_text(encoding="utf-8")
    body = src[src.index('"""', src.index('"""') + 3):]     # past the module docstring
    assert "CURATED" not in body, "the feed must not read curated data to be written"

    from pipeline.regs.dfo_salmon.feed import build
    scraped = json.loads((Path(GENERATED.regs.dfo_salmon) / "rules" / "region-1.json")
                         .read_text(encoding="utf-8"))
    assert build(scraped)["locators"], "the feed builds from the scrape alone"


def test_no_scraped_rule_lands_on_a_locator_nobody_curated():
    """The join. Every fingerprint the page publishes must resolve to a curated record, or the
    rules on it reach no geometry and vanish — the DFO twin of the 137 provincial rules that
    bound nowhere."""
    from pipeline.regs.dfo_salmon.feed import resolve

    for slug in ("1", "6"):
        rep = resolve(slug)
        assert not rep["unbound"], \
            f"region {slug}: {len(rep['unbound'])} scraped locator(s) no entry file knows"
        assert rep["bound"], f"region {slug}: nothing joined at all"


def test_the_somass_binding_is_what_the_feed_joins_to():
    """The only fully curated water. Its registry item and extent live on the CURATED side and
    the feed carries none of it — that separation is the point."""
    from pipeline.regs.dfo_salmon import entries as E

    ef = E.load("1")
    loc = next(l for l in ef.locations if l.location_id == "1:somass-river:whole")
    assert ef.water(loc.water_id).item_ids == ["gnis:25707"]
    assert [e.op for e in loc.binding.extents] == ["whole"]


def test_an_unreadable_season_goes_to_review_rather_than_publishing_as_none():
    """`CatalogueRule` does not check that a window parses — the retired prose `Rule` did, via
    `date_parse_errors`. So a season of "Smarch 40 to Bluneteen 99" could build without complaint, and
    an unparsed window would reach the app indistinguishable from a rule that states no season.
    They are opposite facts: no window means the rule applies ALL YEAR."""
    from pipeline.regs.parsing.catalogue import CatalogueRule, RuleType

    accepted = CatalogueRule(rule_id="x.r1", type=RuleType.retention_limit, verbatim="2 per day",
                             species=["CH"], take=2,
                             when={"unparsed": ["Smarch 40 to Bluneteen 99"]})
    # The catalogue neither drops an unreadable season on the floor NOR refuses the rule: it
    # keeps the text in `when.unparsed`, the time analogue of `unresolved_locators`. An unparsed
    # season and an ABSENT one are opposite facts, and the second reads as "open all year".
    assert accepted.when.unparsed == ["Smarch 40 to Bluneteen 99"]
    assert not accepted.when.dates, "nonsense must not resolve to a calendar window"

    rules = _typed(dates="Smarch 40 to Bluneteen 99", limits_gear="2 per day")
    assert all(r.review_reason for r in rules)
    assert "do not parse" in rules[0].review_reason

    # An EMPTY cell is a fact, not a failure: the regulation applies all year.
    assert not any(r.review_reason for r in _typed(dates="", limits_gear="2 per day"))


def test_a_repaired_window_is_published_repaired():
    """"Aprl 1 to Jun 15" (2022 Region 6) has one possible reading. Publishing the typo verbatim
    hands the app a season it cannot parse; the repair is recorded, never silent."""
    rules = _typed(dates="Aprl 1 to Jun 15", limits_gear="2 per day")
    assert [d.words() for d in rules[0].when.dates] == ["Apr 1-Jun 15"]
    assert not rules[0].when.unparsed, "a repaired window parses; it does not go to unparsed"
    assert not rules[0].review_reason


def test_a_row_that_says_per_day_always_yields_a_quota():
    """`_QUALIFIER` is a CLOSED vocabulary of the words DFO puts between the count and "per day".
    That is deliberate — a wildcard would read "no more than 2 fish per day" as a limit of 2 —
    but it means a new wording silently drops a quota, which is exactly what "4 hatchery marked
    ONLY per day" did. This is the tripwire: if DFO invents a qualifier, this fails rather than
    publishing a water with no limit."""
    from pipeline.regs.dfo_salmon.typed import decode

    rules_dir = Path(GENERATED.regs.dfo_salmon) / "rules"
    missed = []
    for path in sorted(rules_dir.glob("region-*.json")):
        for rec in json.loads(path.read_text(encoding="utf-8"))["rules"]:
            gear = rec["limits_gear"]
            if re.search(r"per\s+day", gear, re.I) and decode(gear)["daily_limit"] is None:
                missed.append(gear)
    assert not missed, f"a quota the decode cannot read: {missed}"


def test_a_row_that_states_a_size_always_binds_it():
    """Same tripwire for the size sub-limit wordings, of which DFO uses at least six."""
    from pipeline.regs.dfo_salmon.typed import to_rules

    rules_dir = Path(GENERATED.regs.dfo_salmon) / "rules"
    missed = []
    for path in sorted(rules_dir.glob("region-*.json")):
        for i, rec in enumerate(json.loads(path.read_text(encoding="utf-8"))["rules"]):
            if not re.search(r"\d+\s*cm", rec["limits_gear"], re.I):
                continue
            typed = to_rules(rec, "t", i)
            if not any(r.lengths or any(c.slot.value == "hook_gap_mm" for c in r.gear)
                       for r in typed):
                missed.append(rec["limits_gear"])
    assert not missed, f"a size the decode cannot read: {missed}"


def test_the_feed_is_deterministic():
    """A scheduled run rebuilds this every time. If the same page produced a different feed, every
    run would look like a change and the review queue would be noise."""
    from pipeline.regs.dfo_salmon.feed import build

    scraped = json.loads((Path(GENERATED.regs.dfo_salmon) / "rules" / "region-6.json")
                         .read_text(encoding="utf-8"))
    assert json.dumps(build(scraped), sort_keys=True) == json.dumps(build(scraped), sort_keys=True)


def test_the_skeena_cascade_resolves_through_the_join():
    """**The case the whole split has to survive.** Region 6 is the only cascade: eight lettered
    bands where a broad default is progressively narrowed, and read flat it says the opposite of
    what it means. Precedence is a property of the LOCATOR, so it lives on the curated side and
    the feed carries none of it — this asserts the two halves still answer the question together.

    The Kispiox is the worked example: section A opens coho 4/day region-wide, B(i) closes coho
    outright, and the water itself reopens a 5-week window. A join that lost the ranking would
    publish the region default on a water the page closes.
    """
    from pipeline.regs.dfo_salmon import entries as E
    from pipeline.regs.parsing.catalogue import CatalogueRule

    ef = E.load("6")
    feed = json.loads((Path(GENERATED.regs.dfo_salmon) / "typed" / "region-6.json")
                      .read_text(encoding="utf-8"))
    by_fp = {l["fingerprint"]: l for l in feed["locators"]}

    def rules_for(loc):
        return [CatalogueRule(**r) for fp in loc.fingerprints
                for r in by_fp.get(fp, {}).get("rules", [])]

    water = next(l for l in ef.locations
                 if l.water == "Kispiox River" and l.precedence == 3)
    assert water.section == "B(i)"
    assert ef.chain_for(water.section) == ["B(i)", "B", "A"], "narrowest first, widest last"

    # the water reopens coho for a window
    coho = [r for r in rules_for(water) if r.species == ["CO"] and r.take == 4]
    assert coho and [d.words() for d in coho[0].when.dates] == ["Jul 15-Aug 23"]

    # the band it sits in closes coho outright
    band = next(l for l in ef.locations if l.section == "B(i)" and l.precedence == 1)
    closed = [r for r in rules_for(band)
              if r.species == ["CO"] and r.take == 0 and r.may_target is False]
    assert closed, "B(i) must close coho, or the water's reopening says nothing"

    # and the region default it all hangs off is wider still
    region = next(l for l in ef.locations if l.precedence == 0)
    assert region.section == "A"
    assert any(r.species == ["CO"] and r.take == 4 for r in rules_for(region))

    assert water.precedence > band.precedence > region.precedence


def test_every_archived_wording_is_still_a_known_locator():
    """**A locator is never lost.** These pages list openings, so a reach leaves when its fishery
    closes and returns later — the Kispiox River Resort reach has cycled out and back four times.
    The entry files are seeded as a SUPERSET of every archived version, so a returning wording
    matches its own dormant record by fingerprint and revives with its binding intact, rather than
    arriving as a `new` locator a curator has to bind again.

    Measured across the whole history cache: 50 versions, 9 regions, back to 2023 — zero unknown.
    If this fails, either the superset seeding regressed or a version was added without seeding it.
    """
    from pipeline.regs.dfo_salmon import entries as E
    from pipeline.regs.dfo_salmon.locations import extract
    from pipeline.regs.dfo_salmon.parse import parse_region
    from pipeline.regs.dfo_salmon.untangle import untangle

    hist = sorted(Path("cache/dfo_salmon/history").glob("*.html"))
    if not hist:
        pytest.skip("no history cache on this machine")

    unknown, checked = [], 0
    for path in hist:
        slug = re.match(r"region(\w+?)_\d{14}\.html", path.name).group(1)
        try:
            locs, _rules, _sig = extract(untangle(parse_region(
                path.read_text(encoding="utf-8", errors="ignore"), slug)))
        except Exception:
            continue
        idx = E.load(slug).by_fingerprint()
        checked += 1
        unknown += [(path.name, l.water, l.specific_area[:60])
                    for l in locs if l.fingerprint not in idx]

    assert checked, "no archived version parsed"
    assert not unknown, (
        f"{len(unknown)} archived wording(s) no entry file knows — a revival would arrive as a "
        f"NEW locator needing rebinding: {unknown[:5]}")


def test_no_water_name_in_the_archives_is_unknown():
    """The same guarantee at the name level: a water DFO published under an older name must still
    resolve, or its rules land on a locator nobody has bound."""
    from pipeline.regs.dfo_salmon import entries as E
    from pipeline.regs.dfo_salmon.locations import normalize
    from pipeline.regs.dfo_salmon.parse import parse_region
    from pipeline.regs.dfo_salmon.untangle import untangle

    hist = sorted(Path("cache/dfo_salmon/history").glob("*.html"))
    if not hist:
        pytest.skip("no history cache on this machine")

    missing = []
    for path in hist:
        slug = re.match(r"region(\w+?)_\d{14}\.html", path.name).group(1)
        try:
            u = untangle(parse_region(path.read_text(encoding="utf-8", errors="ignore"), slug))
        except Exception:
            continue
        known = {normalize(w.name) for w in E.load(slug).waters}
        missing += [(path.name, w.name) for w in u.waters if normalize(w.name) not in known]

    assert not missing, f"historical water name(s) the entry files do not know: {missing[:5]}"


def test_bind_whole_never_binds_a_locator_that_names_a_place():
    """**The auto-binder reads the scope column, never the parsed op.**

    The op looked like it could stand in and it cannot: `described`, `named_tributaries` and
    `tributary_set` are the classifier's leftover buckets, and they hold "within a 400 m radius of
    the mouth of Pinkut Creek" (Babine Lake), "all tributaries ... other than the Suskwa River"
    (Bulkley) and "the portion ... north of the bridge" (Osoyoos). Binding the first whole applies
    a radius closure to an entire lake.

    So: an empty specific-area column is the only thing that auto-binds. Anything else is a
    curator's call.
    """
    from pipeline.regs.dfo_salmon import entries as E
    from pipeline.regs.dfo_salmon.fetch import ALL_SLUGS, PAGES

    for slug in [s for s in ALL_SLUGS if not PAGES[s].is_stub]:
        ef = E.load(slug)
        before = {l.location_id: list(l.binding.extents) for l in ef.locations}
        wrote, _held = E.bind_whole(ef)
        by_id = {l.location_id: l for l in ef.locations}
        for lid in wrote:
            loc = by_id[lid]
            scope = (loc.source_text or {}).get("specific_area") or ""
            assert not scope.strip(), f"{lid} names a place and was bound whole: {scope!r}"
            assert not loc.binding.spatial_caveat, f"{lid} has a caveat that narrows the extent"
            assert before[lid] == [], f"{lid} already had an extent and was overwritten"
        # re-running writes nothing: every qualifying locator is already bound
        again, _ = E.bind_whole(ef)
        assert not again, f"region {slug}: bind_whole is not idempotent, rewrote {again}"


def test_the_queue_hides_no_unbound_locator():
    """A parsed op may SORT a locator; it may never remove one. The old `SKIP_OPS` filter dropped
    123 unbound locators carrying 253 rules before a human ever saw them, including every scope the
    classifier could not decompose — the ones most needing a person."""
    from pipeline.regs.dfo_salmon import entries as E
    from pipeline.regs.dfo_salmon.fetch import ALL_SLUGS, PAGES
    from pipeline.regs.dfo_salmon.splitwork import _registry, waters

    queued = {r["loc"] for w in waters(_registry()) for r in w["locators"]}
    missing = []
    for slug in [s for s in ALL_SLUGS if not PAGES[s].is_stub]:
        ef = E.load(slug)
        for loc in ef.locations:
            if loc.binding.extents or not loc.water_id:
                continue                      # bound, or a cascade default (no water to queue on)
            w = ef.water(loc.water_id)
            if w and w.item_ids and loc.location_id not in queued:
                missing.append((slug, loc.location_id,
                                (loc.source_text or {}).get("specific_area", "")[:50]))
    assert not missing, f"unbound locators the queue never shows: {missing[:5]}"


def test_a_watershed_walk_passes_through_its_lakes_but_collects_streams():
    """**The walk passes lakes; it does not collect them** (user ruling 2026-09-24).

    "Tributaries" in the book are STREAMS (p86: "all streams that contribute to a larger stream
    or to a lake"), so a tributary walk collects no lake. It must still climb THROUGH every lake:
    a creek feeding Babine Lake is a stream contributing to a lake, and a walk that stopped at the
    lake would drop the whole basin above it. Section B is "All waters in the Skeena River
    Watershed" — waters, lakes included — and a WATERSHED is bound as the basin
    (`area:basin:400-`), not as this walk; the DFO scopes that still bind by walk are reported for
    a data fix, never widened here.

    The second half is the property that makes the section E residual safe: two adjacent basins
    must not share water, or subtracting one would remove water belonging to the other.
    """
    from pipeline.common.curated import GENERATED
    from pipeline.common.io.serialize import read_artifact
    from pipeline.atlas.reach.build import build_reach
    from pipeline.regs.dfo_salmon.splitwork import _registry

    graph_path = Path(GENERATED.build()) / "graph.pkl"
    if not graph_path.exists():
        pytest.skip("no built atlas on this machine")
    reg = _registry()
    g = read_artifact(graph_path)

    def walk(item_id):
        entry = {"entry_id": "t", "matched": [], "includes_tributaries": True}
        rule = {"rule_id": "r1", "extents": [{"op": "whole", "item_id": item_id}],
                "includes_tributaries": True, "tributaries_only": False, "tributary_excludes": []}
        b, _ = build_reach(entry, rule, reg, g)
        return set(b.sections or [])

    skeena, nass = walk("gnis:2936"), walk("gnis:3206")
    assert skeena and nass
    item = set(reg["gnis:2936"].section_ids)

    lakes = {s for s in skeena - item
             if str(getattr(g.nodes.get(s), "kind", "")).endswith("lake")}
    assert not lakes, f"the Skeena walk collected {len(lakes)} lake sections"

    # every major lake in the system is PASSED: a stream flowing into it is in the walk
    for name in ("Babine Lake", "Lakelse Lake", "Kitsumkalum Lake", "Morice Lake", "Sustut Lake"):
        ids = [k for k, v in reg.items()
               if getattr(v, "kind", "") == "lake" and (v.name or "").lower() == name.lower()]
        assert ids, f"no registry lake named {name}"
        secs = set(reg[ids[0]].section_ids)
        feeders = {g.edges[ei].from_node for s in secs for ei in g.up_adj.get(s, [])
                   if str(g.nodes[g.edges[ei].from_node].kind).endswith("stream")}
        assert feeders & skeena, f"the walk stopped at {name}: none of its inflows is in it"

    # and a neighbouring basin's water is NOT in it
    for name in ("Meziadin Lake", "Bowser Lake"):
        ids = [k for k, v in reg.items()
               if getattr(v, "kind", "") == "lake" and (v.name or "").lower() == name.lower()]
        if not ids:
            continue
        secs = set(reg[ids[0]].section_ids)
        feeders = {g.edges[ei].from_node for s in secs for ei in g.up_adj.get(s, [])}
        assert not (feeders & skeena), f"{name}'s inflows leaked into the Skeena walk"
        assert not (secs & skeena), f"{name} leaked into the Skeena walk — the basins are not disjoint"


@pytest.mark.slow
def test_the_region_6_cascade_tiles_its_region():
    """**The completeness check for the whole cascade.**

    Section A is "all Region 6 waters" and B(i)/B(ii)/C/D/E/F divide it. Three properties have to
    hold together, and each failed at least once while this was built:

    * the sections must COVER A — water under no section default silently keeps A's much more
      generous limits;
    * they must not reach OUTSIDE A — this is what caught section F. Bound as the bare Fraser
      watershed it reached 325,598 sections of which 300,569 were outside Region 6, so a Region 6
      closure was shutting salmon across the Fraser in regions 2, 3, 5, 7 and 8. It needs
      `within_area`, and that works only because both sides are area lookups: `within_area` is
      applied inside `resolve_extent`, BEFORE the tributary walk, so on a walk-based extent it
      clips the seed and the walk simply escapes again;
    * they must not OVERLAP, or a section falls under two sections at once and nothing says which.

    Tolerances are loose on purpose — the atlas build is not deterministic — but an order of
    magnitude means something real moved.
    """
    from pipeline.atlas.registry import load_registry
    from pipeline.atlas.reach.build import build_reach
    from pipeline.common.curated import GENERATED
    from pipeline.common.io.serialize import read_artifact
    from pipeline.regs.dfo_salmon import entries as E

    build = Path(GENERATED.build())
    if not (build / "graph.pkl").exists():
        pytest.skip("no built atlas on this machine")
    reg = load_registry(build / "registry.json")
    g = read_artifact(build / "graph.pkl")

    ef = E.load("6")
    got = {}
    for loc in [l for l in ef.locations if not l.water_id and l.status == "active"]:
        entry, rules = E.to_reach_input(loc, [{"species": None}], ef.water(loc.water_id))
        b, _ = build_reach(entry, rules[0], reg, g)
        key = loc.section + ("/areas" if (loc.source_text or {}).get("areas") else "")
        got[key] = set(b.sections or [])

    assert "A" in got, "section A did not resolve"
    a = got["A"]
    kids = {k: v for k, v in got.items() if k != "A" and not k.endswith("/areas")}
    assert set(kids) == {"B(i)", "B(ii)", "C", "D", "E", "F"}, sorted(kids)

    union = set().union(*kids.values())
    assert len(a & union) / len(a) > 0.99, (
        f"only {len(a & union)/len(a):.2%} of Region 6 falls under a section default — the rest "
        f"silently keeps section A's limits")
    assert len(union - a) < 1_000, (
        f"{len(union - a):,} sections reach OUTSIDE Region 6 — a section default is binding water "
        f"in another region, which is what section F did before `within_area`")
    for name, sec in kids.items():
        assert len(sec - a) < 1_000, f"section {name} reaches {len(sec - a):,} sections outside A"

    pairs = sorted(kids)
    for i, x in enumerate(pairs):
        for y in pairs[i + 1:]:
            both = kids[x] & kids[y]
            assert len(both) < 100, (
                f"sections {x} and {y} share {len(both):,} sections — a water cannot sit under "
                f"two lettered sections at once")
