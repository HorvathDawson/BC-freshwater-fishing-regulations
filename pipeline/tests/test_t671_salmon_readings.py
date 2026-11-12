"""THE OWNER'S DFO SALMON READINGS OF 2026-11-12 (T671 D1, D4; T676) — the Python's half. The rows and the place's own
answers both languages must give are in the SHARED file (`app/workers/feeds/test/fixtures/dfo/dfo-rows.json`, read by
`test_t671_salmon_rows.py` and the worker's `dfo.t671.test.ts`): the Tatshenshini, the Koksilah, the Cowichan, an opening
inside an All closure, and the "until further notice" rows. What is pinned here is what only the Python holds.

  D1  An "All" closure wins over a species row on the same place UNLESS the species row is dated inside the All row's
      dates ("Yes to option 1"). The answer's words are the closure's.
  D4  Region 6 section A's "in the aggregate (combined total), in Region 6": "The daily limit for all species of Pacific
      salmon from tidal and fresh waters combined is 4. Individual species limits also apply. Species limit is
      seperate." — the combined 4 is the region's bucket (the header's daily note, a total tier, said as a note); each
      species' limit is its own count, NOT a pool across Region 6 (5B's T349 pool is untouched).
  T676 "<day> until further notice" runs from that day with no end and does what the row says; the release types the
      date cell with the worker's reader (`dates.read_dates`), and the words say since when.
"""
from __future__ import annotations

from pipeline.deliver import salmon as S
from pipeline.regs.dfo_salmon import entries as E
from pipeline.regs.dfo_salmon import header as H
from pipeline.regs.dfo_salmon import quota as Q
from pipeline.regs.dfo_salmon import typed as T
from pipeline.tools.emit import dfo_vectors as V


def _rules(*rows):
    out = []
    for i, (species, dates, limits) in enumerate(rows):
        out += [r.model_dump(mode="json", exclude_defaults=True, by_alias=True)
                for r in T.to_rules({"species": species, "dates": dates, "limits_gear": limits}, "t", i)]
    return out


TATSHENSHINI = _rules(("All", "Jul 24 to Aug 14", "No fishing for salmon"),
                      ("Coho", "Apr 1 to Mar 31", "2 per day"))


def test_d1_the_all_closure_speaks_in_its_own_words_and_the_coho_row_the_rest_of_the_year():
    on = S.verdict_on([TATSHENSHINI], "CO", 801)
    assert (on["s"], on["t"]) == ("closed", "No fishing for salmon (Jul 24–Aug 14)")
    off = S.verdict_on([TATSHENSHINI], "CO", 815)
    assert (off["s"], off["t"], off["tree"]["n"]) == ("open", "2 per day (Apr 1–Mar 31)", 2)


def test_d1_a_species_row_with_no_dates_is_all_year_so_not_inside_a_dated_closure():
    rules = _rules(("All", "Jul 24 to Aug 14", "No fishing for salmon"), ("Coho", "", "2 per day"))
    assert [S.verdict_on([rules], "CO", md)["s"] for md in (723, 724, 815)] == ["open", "closed", "open"]


def test_d1_mutation_the_species_row_beating_every_all_row_would_reopen_the_tatshenshini(monkeypatch):
    """The test above fails on the reading before the ruling (the species' own row always spoke)."""
    monkeypatch.setattr(S, "_all_closure_wins", lambda own, alls, releasing: list(own))
    assert S.verdict_on([TATSHENSHINI], "CO", 801)["s"] == "open"


def test_d1_a_wider_place_is_not_this_place():
    """The ruling is about one place's rows: an All closure at a WIDER place of the chain never reaches a species
    opening at a narrower one (the narrowest place with a rule for the fish answers, as before)."""
    narrow = _rules(("Coho", "Apr 1 to Mar 31", "2 per day"))
    wide = _rules(("All", "Jul 24 to Aug 14", "No fishing for salmon"))
    assert S.verdict_on([narrow, wide], "CO", 801)["s"] == "open"


def _region_6_a():
    return next(l for l in E.load("6").locations if l.location_id == "6:a:region")


def test_d4_region_6_a_is_no_species_pool_each_species_limit_is_its_own():
    loc = _region_6_a()
    assert Q.pool_of(loc) == []
    rules = Q.mark_shared(_rules(("Chinook", "Apr 1 to Mar 31", "4 per day, only 1 over 65 cm."),
                                 ("Coho", "Apr 1 to Mar 31", "4 per day, only 1 over 50 cm.")), Q.pool_of(loc))
    for sp, cm in (("CH", 65), ("CO", 50)):
        tree = S.verdict_on([rules], sp, 801)["tree"]
        assert (tree["tier"], tree["n"], "whose" in tree, tree["species"]) == ("count", 4, False, [sp])
        assert [(c["n"], c["over_cm"]) for c in tree["children"]] == [(1, cm)]


def test_d4_the_combined_4_is_region_6s_bucket_fresh_and_tidal_waters_together():
    notes = H.parse_header((V.PAGES / "region6-eng.html").read_text(encoding="utf-8"), "6")
    b = S.header_answers(notes)["bucket"]
    assert (b["tree"]["tier"], b["tree"]["n"], b["tree"]["says"]) == (
        "total", 4, "Up to 4 salmon a day in total, fresh and tidal waters combined")
    assert b["t"] == ("The daily limit for all species of Pacific salmon from tidal and fresh waters combined is 4. "
                      "Individual species limits also apply.")


def test_d4_region_5bs_pool_by_the_t349_ruling_is_untouched():
    loc = next(l for l in E.load("5b").locations if l.location_id == "5b:region:section")
    assert Q.pool_of(loc) == ["area:mu_group:management_units_5_6_to_5_11"]


def test_t676_until_further_notice_is_read_open_ended_and_said_since_when():
    rules = _rules(("Chinook", "Sep 15 until further notice", "No fishing for chinook"))
    assert rules[0]["when"] == {"dates": [{"from_month": 9, "from_day": 15, "to_month": 12, "to_day": 31}],
                                "open_ended": True}
    v = S.verdict_on([rules], "CH", 115)       # next January: it runs on
    assert (v["s"], v["t"]) == ("closed", "No fishing for chinook (from Sep 15 until further notice)")
    alone = _rules(("All", "Until further notice", "No fishing for salmon"))
    assert S.verdict_on([alone], "PK", 601)["t"] == "No fishing for salmon (until further notice)"


def test_t676_the_typed_feed_says_it_was_typed_by_the_shared_reader():
    assert T.FORMAT == 3
