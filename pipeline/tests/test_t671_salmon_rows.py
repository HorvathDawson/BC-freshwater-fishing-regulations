"""T671 D2: A DFO ROW NOBODY COULD TYPE KEEPS THE SPECIES ITS CELL NAMES (`typed.to_rules`, `deliver.salmon.verdict_on`).

DFO prints "Chum | To be determined | To be determined" and "Sockeye | Apr 1 to Mar 31 | To be determined". Both were
typed with no species:
  * the first read "check DFO" for all five salmon, hiding the place's own openings and a wider place's (Courtenay,
    Puntledge, Campbell);
  * the second, its dates readable, answered for no fish at all — sockeye fell through to the region default and read
    "closed" (Kamloops Lake).
Now the row is unread for the species it names, on the days it holds, and for nothing else. A cell naming no salmon the
table knows keeps no species and reads for every salmon: never for none.

THE ROWS AND THE PLACE'S OWN ANSWERS ARE SHARED (T556): `app/workers/feeds/test/fixtures/dfo/dfo-rows.json`, read here and
by the worker's twin `app/workers/feeds/test/dfo.t671.test.ts`, so the two languages cannot each hold a copy that drifts.
What only the Python resolves (the chain: a wider place, the region default) is asserted here on those same rows.
"""
from __future__ import annotations

import json

import pytest

from pipeline.deliver import salmon as S
from pipeline.regs.dfo_salmon import typed as T
from pipeline.tools.emit import dfo_vectors as E

CASES = {c["name"]: c for c in json.loads(E.ROWS.read_text(encoding="utf-8"))["cases"]}
REASONS = {"tbd": T.TBD, "gear_permission": T.GEAR_PERMISSION}


def _rules(name):
    """The shared case's rows, typed (the scrape's shape: species / dates / limits_gear)."""
    out = []
    for i, (species, dates, limits) in enumerate(CASES[name]["rows"]):
        out += [r.model_dump(mode="json", exclude_defaults=True, by_alias=True)
                for r in T.to_rules({"species": species, "dates": dates, "limits_gear": limits}, "t671", i)]
    return out


def _status(chain, sp, md):
    v = S.verdict_on(chain, sp, md)
    return v and v["s"]


REGION_SHUT = _rules("a region closed to salmon")


def test_the_shared_file_was_read():
    assert len(CASES) >= 13 and sum(len(c.get("own", [])) for c in CASES.values()) >= 40
    assert sum(len(c.get("typed", [])) for c in CASES.values()) >= 6


@pytest.mark.parametrize("name", sorted(CASES))
def test_every_shared_case_types_and_answers_as_the_file_says(name):
    """The worker's twin asserts the same file: each named row's first rule, and the place's OWN answer per day."""
    case = CASES[name]
    for i, want in enumerate(case.get("typed", [])):
        species, dates, limits = case["rows"][i]
        rule = T.to_rules({"species": species, "dates": dates, "limits_gear": limits}, "t671", i)[0].model_dump(
            mode="json", exclude_defaults=True, by_alias=True)
        if "type" in want:
            assert rule["type"] == want["type"]
        if "species" in want:
            assert rule.get("species") == want["species"]
        if "reason" in want:
            assert rule["review_reason"].startswith(REASONS[want["reason"]])
    place = _rules(name)
    assert [[sp, md, _status([place], sp, md)] for sp, md, _ in case.get("own", [])] == case.get("own", [])


def test_chum_to_be_determined_reads_check_for_chum_only_and_the_other_rows_stand():
    """Courtenay River under its region: the species the row does not name fall to the wider place."""
    chain = [_rules("Courtenay"), REGION_SHUT]
    assert _status(chain, "CM", 601) == "check" and _status(chain, "CM", 1001) == "check"
    assert _status(chain, "CH", 1001) == "open" and _status(chain, "CO", 1001) == "open"
    assert [_status(chain, sp, 601) for sp in ("CH", "CO", "SK", "PK")] == ["closed"] * 4
    assert S.verdict_on(chain, "CM", 601)["t"] == "To be determined (as printed: To be determined)"


def test_a_narrower_places_undetermined_chinook_no_longer_hides_the_wider_places_openings():
    """Campbell River: the narrower place prints only "Chinook | To be determined"; the wider one opens coho and pink."""
    chain = [_rules("Campbell, the narrower place"), _rules("Campbell, the wider place"), REGION_SHUT]
    assert _status(chain, "CH", 1001) == "check"
    assert _status(chain, "CO", 1001) == "open"
    assert _status(chain, "PK", 801) == "open"
    assert _status(chain, "PK", 1101) == "closed"


def test_to_be_determined_with_readable_dates_reads_check_on_those_days_never_the_wider_closure():
    """Kamloops Lake: "Sockeye | Apr 1 to Mar 31 | To be determined" beside closures of pink and coho."""
    chain = [_rules("Kamloops Lake"), REGION_SHUT]
    assert _status(chain, "SK", 715) == "check"
    assert S.verdict_on(chain, "SK", 715)["t"] == "To be determined (Apr 1–Mar 31)"
    assert _status(chain, "PK", 715) == "closed" and _status(chain, "CO", 715) == "closed"
    assert _status(chain, "CH", 901) == "open" and _status(chain, "CH", 715) == "closed"
    assert _status(chain, "CM", 715) == "closed"


def test_an_undetermined_row_speaks_only_inside_its_own_dates():
    chain = [_rules("sockeye in August"), REGION_SHUT]
    assert _status(chain, "SK", 815) == "check"
    assert _status(chain, "SK", 915) == "closed"


def test_a_species_cell_the_table_does_not_know_reads_check_for_every_salmon_never_none():
    place = _rules("kokanee salmon")
    assert place[0]["type"] == "advisory" and "species" not in place[0]
    assert [_status([place, REGION_SHUT], sp, 601) for sp in S.SPECIES] == ["check"] * 5


def test_a_row_about_another_fish_and_a_gear_permission_are_not_unread_rows():
    eulachon, bait = _rules("eulachon"), _rules("natural bait allowed")
    assert not S.unread_row(eulachon[0]) and not S.unread_row(bait[0])
    assert [_status([eulachon, REGION_SHUT], sp, 601) for sp in S.SPECIES] == ["closed"] * 5
    assert _status([bait, REGION_SHUT], "CO", 601) == "closed"


def test_the_live_feed_composes_the_same_answer():
    """`live.location_calendar` + `live.compose` (what the worker publishes and the app composes) == the release's
    `chain_calendar` for the Courtenay shape."""
    from pipeline.regs.dfo_salmon import live as L
    place = _rules("Courtenay")
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
