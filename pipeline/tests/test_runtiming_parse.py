"""RUN TIMING (T162, data only): the parsed PSF + DFO timings (`pipeline.runtiming.parse`, `timing.json`). Synthetic
NuSEDS rows, plus the committed timing file's own invariants. Integration is a plan: handoff/PLAN/runtiming/PLAN.md.
"""
from __future__ import annotations

import pytest

from pipeline.runtiming import parse as P
from pipeline.runtiming.timing import CREDIT, SLOTS, read_timing, slot_of


# ---- the parse ------------------------------------------------------------------------------------------------------

def test_dates_read_on_the_common_year_and_seasons_start_in_march():
    assert P.nuseds_day("01-JAN-02") == 0
    assert P.nuseds_day("01-MAR-02") == 59
    assert P.nuseds_day("29-FEB-04") == P.nuseds_day("28-FEB-04") == 58
    assert P.nuseds_day("") is None and P.nuseds_day("garbage") is None
    assert P.fmt_day(P.nuseds_day("10-AUG-02")) == "Aug 10"
    assert P.season(59) == 0 and P.unseason(0) == 59


def test_a_survey_year_runs_from_its_first_stated_date_to_its_last():
    # arrival missing: start of spawning starts it; end of spawning missing: the peak's end ends it
    row = {"START_SPAWN_DT_FROM": "22-OCT-02", "PEAK_SPAWN_DT_FROM": "13-NOV-02", "PEAK_SPAWN_DT_TO": "19-NOV-02"}
    a, b, pk = P.year_window(row)
    assert (P.fmt_day(P.unseason(a)), P.fmt_day(P.unseason(b)), P.fmt_day(P.unseason(pk))) == \
        ("Oct 22", "Nov 19", "Nov 13")
    # a chum year crossing New Year is one season, not a 300-day span
    a, b, _ = P.year_window({"STREAM_ARRIVAL_DT_FROM": "06-OCT-90", "END_SPAWN_DT_TO": "15-JAN-91"})
    assert b - a == 101
    # an end before its start, or a span past MAX_SPAN_DAYS, is a typo: no year
    assert P.year_window({"STREAM_ARRIVAL_DT_FROM": "01-SEP-02", "END_SPAWN_DT_TO": "01-AUG-02"}) is None
    assert P.year_window({"PEAK_SPAWN_DT_TO": "01-AUG-02"}) is None


def test_the_presence_curve_is_the_share_of_years_in_each_pentad():
    sep1 = P.season(P.nuseds_day("01-SEP-00"))
    c = P.presence_curve([(sep1, sep1 + 4), (sep1, sep1 + 9)])
    p = P.nuseds_day("01-SEP-00") // 5
    assert len(c) == 73 and c[p] == 100 and c[p + 1] == 100 and c[p + 2] == 50 and sum(c) == 250


def nuseds(gfe, sp, year, run="", fwa="100-064535-057628-000000", arrive="15-SEP", end="30-SEP", **kw):
    yy = f"{year % 100:02d}"
    return {"GFE_ID": gfe, "SPECIES": sp, "ANALYSIS_YR": str(year), "RUN_TYPE": run, "FWA_WATERSHED_CDE": fwa,
            "WATERBODY": kw.get("name", "CHILLIWACK RIVER"), "STREAM_ARRIVAL_DT_FROM": f"{arrive}-{yy}",
            "END_SPAWN_DT_TO": f"{end}-{yy}", "PEAK_SPAWN_DT_FROM": kw.get("peak", "") and f"{kw['peak']}-{yy}"}


def test_dfo_records_need_three_dated_years_and_keep_runs_and_brood_lines_apart():
    rows = [nuseds("1", "Coho", y) for y in (2001, 2002, 2003)]
    rows += [nuseds("1", "Chinook", y, run="FALL", peak="20-SEP") for y in (2001, 2002, 2003)]
    rows += [nuseds("1", "Chinook", y, run="SUMMER", arrive="05-JUN", end="30-AUG") for y in (2001, 2002, 2003)]
    rows += [nuseds("1", "Pink", y) for y in (2001, 2003, 2005, 2002)]          # three odd years, one even
    rows += [nuseds("1", "Sockeye", y) for y in (2001, 2002)]                   # two years: an anecdote
    rows += [nuseds("9", "Coho", y) for y in (2001, 2002, 2003)]                # an aggregate site: not a place
    rows += [nuseds("2", "Steelhead", y) for y in (2001, 2002, 2003)]           # not DFO's to time
    rows += [nuseds("3", "Coho", y, fwa="999-999999-000000") for y in (2001, 2002, 2003)]   # FWA's "no code"
    sites = {"1": (-121.7, 49.1, "Stream"), "9": (-121.7, 49.1, "Stream Aggregate")}
    out, rep = P.dfo_records(rows, sites)
    got = sorted((r["slot"], r["label"], r["years"], r["window"], r["peak"]) for r in out)
    assert got == [("Chinook", "fall", 3, ["Sep 15", "Sep 30"], "Sep 20"),
                   ("Chinook", "summer", 3, ["Jun 5", "Aug 30"], None),
                   ("Coho", None, 3, ["Sep 15", "Sep 30"], None),
                   ("Pink (odd)", None, 3, ["Sep 15", "Sep 30"], None)]
    assert {r["name"] for r in out} == {"Chilliwack River"}
    assert {r["wsc"] for r in out} == {"100-064535-057628"}
    assert all((r["lon"], r["lat"], r["site_kind"]) == (-121.7, 49.1, "Stream") for r in out)
    assert rep["records"] == 4 and rep["under_3_years"] == 2           # sockeye, and the single even pink year


def test_a_psf_curve_is_scaled_to_its_own_peak_and_a_trace_never_rounds_away():
    assert P.scaled([0, 0.001, 5, 10, 0]) == [0, 1, 50, 100, 0]
    with pytest.raises(ValueError):
        P.scaled([0, 0])


def test_slots_split_pink_by_brood_line_and_nothing_else():
    assert slot_of("Pink", "even") == "Pink (even)" and slot_of("Pink", year=2025) == "Pink (odd)"
    assert slot_of("Sockeye", "river-type") == "Sockeye"
    with pytest.raises(ValueError):
        slot_of("Pink")
    assert SLOTS == ("Chinook", "Chum", "Coho", "Pink (even)", "Pink (odd)", "Sockeye", "Steelhead")


def test_the_committed_timings_read_whole_and_credit_both_sources():
    from pipeline.common.curated import CURATED
    t = read_timing(CURATED.runtiming.timing)
    assert {r.source for r in t.psf} == {"psf"} and {r.source for r in t.dfo} == {"dfo"}
    assert len(t.psf) > 300 and len(t.dfo) > 4000
    assert {r.species for r in t.dfo} == {"Chinook", "Chum", "Coho", "Pink", "Sockeye"}     # no DFO steelhead
    assert {r.slot for r in t.psf if r.species == "Steelhead"} == {"Steelhead"}
    assert all(r.wsc for r in t.dfo) and all(r.cuid for r in t.psf)
    assert (t.sources["psf"]["credit"], t.sources["dfo"]["credit"]) == \
        ("Pacific Salmon Foundation (Salmon Watersheds Program)", "Fisheries and Oceans Canada") == \
        (CREDIT["psf"], CREDIT["dfo"])
    assert t.sources["dfo"]["licence"] == "Open Government Licence – Canada"
    # the Skeena's two steelhead runs are two records, named by their CU
    sk = sorted((r.name, r.label) for r in t.psf if r.species == "Steelhead" and r.name.startswith("Skeena"))
    assert ("Skeena Coastal Summers", "summer") in sk and ("Skeena Coastal Winters", "winter") in sk


def test_a_missing_timing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError, match="pipeline.runtiming.parse"):
        read_timing(tmp_path / "timing.json")
