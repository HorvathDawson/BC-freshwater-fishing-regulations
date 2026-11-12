"""T671 D2: A DFO ROW NOBODY COULD TYPE KEEPS THE SPECIES ITS CELL NAMES (`typed.to_rules`, `deliver.salmon.verdict_on`).

DFO prints "Chum | To be determined | To be determined" and "Sockeye | Apr 1 to Mar 31 | To be determined". Both were
typed with no species:
  * the first read "check DFO" for all five salmon, hiding the place's own openings and a wider place's (Courtenay,
    Puntledge, Campbell);
  * the second, its dates readable, answered for no fish at all — sockeye fell through to the region default and read
    "closed" (Kamloops Lake).
Now the row is unread for the species it names, on the days it holds, and for nothing else. A cell naming no salmon the
table knows keeps no species and reads for every salmon: never for none.

Every test stands on rows it types itself (the scrape's shape: species / dates / limits_gear).
"""
from __future__ import annotations

from pipeline.deliver import salmon as S
from pipeline.regs.dfo_salmon import typed as T


def _rules(*rows):
    out = []
    for i, (species, dates, limits) in enumerate(rows):
        out += [r.model_dump(mode="json", exclude_defaults=True, by_alias=True)
                for r in T.to_rules({"species": species, "dates": dates, "limits_gear": limits}, "t671", i)]
    return out


def _status(chain, sp, md):
    v = S.verdict_on(chain, sp, md)
    return v and v["s"]


REGION_SHUT = _rules(("All", "", "No fishing for salmon"))


def test_a_to_be_determined_row_is_typed_with_the_species_its_cell_names():
    (rule,) = _rules(("Chum", "To be determined", "To be determined"))
    assert rule["type"] == "advisory" and rule["species"] == ["CM"]
    assert rule["review_reason"].startswith(T.TBD)
    (rule,) = _rules(("Sockeye, pink and chum", "Apr 1 to Mar 31", "To be determined"))
    assert rule["species"] == ["SK", "PK", "CM"]
    (rule,) = _rules(("All", "To be determined", "To be determined"))
    assert rule["species"] == ["SA"]


def test_a_row_naming_no_salmon_the_table_knows_keeps_no_species():
    (rule,) = _rules(("Steelhead", "To be determined", "To be determined"))
    assert "species" not in rule


def test_chum_to_be_determined_reads_check_for_chum_only_and_the_other_rows_stand():
    """Courtenay River: chinook and coho open Sep 1-Dec 31, chum to be determined."""
    place = _rules(("Chinook", "Sep 1 to Dec 31", "2 per day"),
                   ("Coho", "Sept 1 to Dec 31", "4 hatchery-marked per day"),
                   ("Chum", "To be determined", "To be determined"))
    chain = [place, REGION_SHUT]
    assert _status(chain, "CM", 601) == "check"
    assert _status(chain, "CM", 1001) == "check"
    assert _status(chain, "CH", 1001) == "open"
    assert _status(chain, "CO", 1001) == "open"
    # the species the row does not name fall to the wider place, as if the row were not there
    assert [_status(chain, sp, 601) for sp in ("CH", "CO", "SK", "PK")] == ["closed"] * 4
    assert S.verdict_on(chain, "CM", 601)["t"] == "To be determined (as printed: To be determined)"


def test_a_narrower_places_undetermined_chinook_no_longer_hides_the_wider_places_openings():
    """Campbell River: the narrower place prints only "Chinook | To be determined"; the wider one opens coho and pink."""
    narrow = _rules(("Chinook", "To be determined", "To be determined"))
    wide = _rules(("Coho", "Sept 1 to Dec 31", "4 hatchery-marked per day"), ("Pink", "Jul 15 to Oct 15", "4 per day"))
    chain = [narrow, wide, REGION_SHUT]
    assert _status(chain, "CH", 1001) == "check"
    assert _status(chain, "CO", 1001) == "open"
    assert _status(chain, "PK", 801) == "open"
    assert _status(chain, "PK", 1101) == "closed"


def test_to_be_determined_with_readable_dates_reads_check_on_those_days_never_the_wider_closure():
    """Kamloops Lake: "Sockeye | Apr 1 to Mar 31 | To be determined" beside closures of pink and coho."""
    lake = _rules(("Chinook", "Aug 28 to Sep 22", "4 per day, only 2 over 50cm"),
                  ("Pink", "Apr 1 to Mar 31", "No fishing for pink"),
                  ("Coho", "Apr 1 to Mar 31", "No fishing for coho"),
                  ("Sockeye", "Apr 1 to Mar 31", "To be determined"))
    chain = [lake, REGION_SHUT]
    assert _status(chain, "SK", 715) == "check"
    assert S.verdict_on(chain, "SK", 715)["t"] == "To be determined (Apr 1–Mar 31)"
    assert _status(chain, "PK", 715) == "closed" and _status(chain, "CO", 715) == "closed"
    assert _status(chain, "CH", 901) == "open" and _status(chain, "CH", 715) == "closed"
    assert _status(chain, "CM", 715) == "closed"


def test_an_undetermined_row_speaks_only_inside_its_own_dates():
    place = _rules(("Sockeye", "Aug 1 to Aug 31", "To be determined"))
    chain = [place, REGION_SHUT]
    assert _status(chain, "SK", 815) == "check"
    assert _status(chain, "SK", 915) == "closed"


def test_a_species_cell_the_table_does_not_know_reads_check_for_every_salmon_never_none():
    place = _rules(("Kokanee salmon", "Apr 1 to Mar 31", "2 per day"))
    assert place[0]["type"] == "advisory" and "species" not in place[0]
    chain = [place, REGION_SHUT]
    assert [_status(chain, sp, 601) for sp in S.SPECIES] == ["check"] * 5


def test_a_row_about_another_fish_and_a_gear_permission_are_not_unread_rows():
    eulachon = _rules(("All", "Apr 1 to Mar 31", "No fishing for eulachon"))
    bait = _rules(("Coho", "Apr 1 to Mar 31", "Natural bait allowed"))
    assert not S.unread_row(eulachon[0]) and not S.unread_row(bait[0])
    assert bait[0]["review_reason"].startswith(T.GEAR_PERMISSION)
    assert [_status([eulachon, REGION_SHUT], sp, 601) for sp in S.SPECIES] == ["closed"] * 5
    assert _status([bait, REGION_SHUT], "CO", 601) == "closed"


def test_the_live_feed_composes_the_same_answer():
    """`live.location_calendar` + `live.compose` (what the worker publishes and the app composes) == the release's
    `chain_calendar` for the Courtenay shape."""
    from pipeline.regs.dfo_salmon import live as L
    place = _rules(("Coho", "Sept 1 to Dec 31", "4 hatchery-marked per day"),
                   ("Chum", "To be determined", "To be determined"))
    v, g = S._Interner(), S._Interner()
    v(S.NOT_LISTED)
    want = S.chain_calendar([place, REGION_SHUT], v, g)
    locs = {"a": L.location_calendar(place, v, g), "b": L.location_calendar(REGION_SHUT, v, g)}
    got = L.compose(["a", "b"], locs, v.items)
    assert got["sp"] == want["sp"] and got["m"] == want["m"]
    chum = [v.items[i]["s"] for i in want["sp"][S.SPECIES.index("CM")][1::2]]
    assert chum == ["check"]


def test_the_release_refuses_rows_typed_by_older_code(tmp_path):
    """`load_inputs` stops on a typed file whose `typed_format` is not today's, naming the command; `feed.build`
    stamps today's."""
    import json

    import pytest

    from pipeline.regs.dfo_salmon import feed as F
    from pipeline.regs.dfo_salmon.fetch import ALL_SLUGS, PAGES
    built = F.build({"region": "1", "rules": []})
    assert built["typed_format"] == T.FORMAT
    (run := tmp_path / "run").mkdir()
    for slug in (s for s in ALL_SLUGS if not PAGES[s].is_stub):
        (tmp_path / f"region-{slug}.json").write_text(json.dumps({"region": slug, "locators": []}))
    with pytest.raises(S.SalmonError) as e:
        S.load_inputs(run, feed_dir=tmp_path)
    assert "typed by older code" in str(e.value) and "python -m pipeline.regs.dfo_salmon.feed" in str(e.value)
