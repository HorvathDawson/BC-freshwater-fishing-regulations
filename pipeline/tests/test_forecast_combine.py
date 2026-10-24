"""ONE COMBINATION OF A FORECAST DAY (T177): `change.combine_day`, asked by the Change, the horizons and every ranked
day; and the run archive (`skill`) that will say, in time, whether CLEVER or COFFEE deserves more weight.

owner 2026-10-24 (Q160): "should combine models for change. look into which models is best to figure out how to
combine." Measured skill on this machine: 5 paired stations at lead 1 on one issue day — too few to weigh (ticket), so
EQUAL weights; the archive starts the record.
"""

from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipeline.gauges.feed import change as C
from pipeline.gauges.feed import publish as pub
from pipeline.gauges.feed import skill as S

FIX = Path(__file__).parent / "fixtures" / "gauge_feed"


def ranker(env):
    return (lambda v, d: pub.percentile_of(v, pub.band_for(env, datetime(d.year, d.month, d.day, 12,
                                                                          tzinfo=timezone.utc)))) if env else None


# --------------------------------------------------------------------------------------------- the one join ----

@pytest.mark.parametrize("values, want", [
    ({"CLEVER": 4.0, "COFFEE": 9.0, "ELF": 1.0}, (("CLEVER", "COFFEE"), 6.0, False)),    # geometric, ELF left out
    ({"COFFEE": 9.0, "CLEVER": 4.0}, (("CLEVER", "COFFEE"), 6.0, False)),                # named in policy order
    ({"CLEVER": None, "COFFEE": 9.0, "ELF": 1.0}, (("COFFEE",), 9.0, False)),
    ({"CLEVER": 4.0, "ELF": 1.0}, (("CLEVER",), 4.0, False)),
    ({"ELF": 0.8}, (("ELF",), 0.8, True)),
    ({"CLEVER": None, "COFFEE": None, "ELF": 0.8}, (("ELF",), 0.8, True)),
    ({"CLEVER": None, "ELF": None}, None),
    ({}, None),
    ({"GUESS": 0.5}, None),                                   # a model the policy does not name never answers
    ({"GUESS": 0.5, "ELF": 0.1}, (("ELF",), 0.1, True)),
    ({"CLEVER": 18.0}, (("CLEVER",), 18.0, False)),           # one model exactly as given (18.0 sits ON p90)
    ({"CLEVER": 0.0}, (("CLEVER",), 0.0, False)),             # a forecast of no flow alone is no flow
])
def test_a_day_is_the_flood_models_joined_else_elf_marked_low_water(values, want):
    got = C.combine_day(values)
    if want is None:
        assert got is None
    else:
        assert (got[0], round(got[1], 9), got[2]) == want


def test_a_join_with_a_zero_pulls_the_mean_down_and_never_raises():
    models, v, low = C.combine_day({"CLEVER": 0.0, "COFFEE": 4.0})
    assert (models, low) == (("CLEVER", "COFFEE"), False) and v == pytest.approx(math.sqrt(1e-9 * 4.0))


def test_the_names_of_a_joined_day():
    assert [C.model_name(m) for m in (("CLEVER", "COFFEE"), ("CLEVER",), ("ELF",))] == ["CLEVER+COFFEE", "CLEVER", "ELF"]


def test_no_second_copy_of_the_combination():
    """`combine_day` is the only place a day's models are chosen per day and averaged. The Change (`change_source`),
    every ranked day (`forecast_days`), the horizons (`forecast_percentiles`) and the skill score call it; none walks the
    model names or takes a mean of its own; T171's first-model `day_model` exists nowhere."""
    src = Path(C.__file__).read_text(encoding="utf-8")
    pub_src = Path(pub.__file__).read_text(encoding="utf-8")
    sk_src = Path(S.__file__).read_text(encoding="utf-8")

    def body(fn: str, text: str = src) -> str:
        return text.split(f"def {fn}", 1)[1].split("\ndef ", 1)[0]

    join = body("combine_day")
    assert "FLOOD_WEIGHTS" in join and "math.exp" in join and "LOW_WATER_MODEL" in join
    assert src.count("math.exp(") == 1 and pub_src.count("math.exp(") == 0
    assert "combine_day(" in body("change_source") and "combine_day(" in body("forecast_days")
    assert "_combine_day(" in body("forecast_percentiles", pub_src)
    assert "combine_day(" in body("score", sk_src) and "math.exp(" not in sk_src
    assert "day_model" not in src.replace(join, "") and "day_model" not in pub_src
    assert [n for n in ('"CLEVER"', '"COFFEE"', '"ELF"', "FLOOD_MODELS", "FLOOD_WEIGHTS", "LOW_WATER_MODEL", "lowWater")
            if n in pub_src] == []


# ------------------------------------------------------------------------------- one day, three readers ----

NOW = datetime(2026, 9, 4, 20, 0, tzinfo=timezone.utc)          # 12:00 on the Centre's clock
FLAT = {str(p): [5.0, 8.0, 11.0, 14.0, 18.0] for p in range(73)}


def both_runs():
    """CLEVER flat at 8.0 m3/s (p25) for ten days; COFFEE's peaks 20 today then 28 (x 10/20 on the gauge: 14, p75)."""
    start = datetime(2026, 9, 4, 0, 0, tzinfo=C.CENTRE_ZONE)
    clever = {"at": [(start + timedelta(hours=h)).isoformat() for h in range(24 * 11)], "mid": [8.0] * (24 * 11),
              "step": "1h"}
    coffee = {"at": [(datetime(2026, 9, 4) + timedelta(days=d)).date().isoformat() for d in range(5)],
              "mid": [20.0, 28.0, 28.0, 28.0, 28.0], "step": "1d"}
    elf = {"at": [(datetime(2026, 9, 4) + timedelta(days=d)).date().isoformat() for d in range(31)],
           "mid": [5.0] * 31, "step": "1d"}
    return {"CLEVER": {"series": clever}, "COFFEE": {"series": coffee}, "ELF": {"series": elf}}


# sqrt(8 x 14) = 10.583 m3/s, a quarter of the way from p25 (8) by 2.583 / 3 of the band to p50 (11): 0.4653.
# First model (T171) would say 0.25; an arithmetic mean 11 -> 0.5; COFFEE unscaled sqrt(8 x 28) -> 0.786.
JOINED = round(0.25 + (math.sqrt(8.0 * 14.0) - 8.0) / 3.0 * 0.25, 4)


def test_every_ranked_day_joins_clever_and_coffee_then_clever_alone_then_never_elf():
    assert JOINED == 0.4653
    got = C.forecast_days(both_runs(), NOW, 10.0, ranker(FLAT))
    assert got == {"from": "2026-09-05", "discharge": [JOINED] * 4 + [0.25] * 6,
                   "model": ["CLEVER+COFFEE"] * 4 + ["CLEVER"] * 6}


def test_the_horizons_join_the_same_way():
    ahead = pub.forecast_percentiles(both_runs(), {"discharge": FLAT}, NOW, {"discharge": 10.0, "level": None})
    assert {h: (v["model"], round(v["discharge"], 4)) for h, v in ahead.items()} == {
        "1": ("CLEVER+COFFEE", JOINED), "3": ("CLEVER+COFFEE", JOINED), "5": ("CLEVER", 0.25), "10": ("CLEVER", 0.25)}


def test_the_change_joins_its_ratios_with_the_same_function():
    """CLEVER 8/8 = 1.0 and COFFEE 28/20 = 1.4 on each of COFFEE's days: sqrt(1.4) = 1.1832; CLEVER alone after."""
    raw = {m: C.run_days(r, NOW) for m, r in both_runs().items()}
    src = C.change_source({m: v for m, v in raw.items() if v is not None})
    assert src["models"] == ["CLEVER", "COFFEE"] and src["lowWater"] is False
    assert [(d, round(lo, 4), round(hi, 4)) for d, lo, hi in src["days"]] == (
        [("2026-09-04", 1.0, 1.0)] + [(f"2026-09-0{d}", 1.1832, 1.1832) for d in (5, 6, 7, 8)]
        + [(f"2026-09-{d:02d}", 1.0, 1.0) for d in range(9, 15)])


def test_coffee_without_a_reading_leaves_clever_alone():
    got = C.forecast_days(both_runs(), NOW, None, ranker(FLAT))
    assert got == {"from": "2026-09-05", "discharge": [0.25] * 10, "model": ["CLEVER"] * 10}


# ------------------------------------------------------------------------------------------ recorded run ----

def test_08hf006_the_coast_storm_joined_day_by_day():
    """Chemainus-area creek 08HF006, feed of 2026-10-22: COFFEE forecast a storm (its +2 peak ~10x the gauge), CLEVER a
    modest rise. Each joined day sits between the two; CLEVER alone from COFFEE's sixth day; ELF only on day ten."""
    r = json.loads((FIX / "forecast_change" / "08HF006.json").read_text(encoding="utf-8"))
    now = datetime.fromisoformat(r["fetchedAt"].replace("Z", "+00:00"))
    days = C.forecast_days(r["forecasts"], now, r["now"]["discharge"], ranker(r["envelope"]["discharge"]))
    assert days == {"from": "2026-10-23",
                    "discharge": [0.5657, 0.95, 0.518, 0.1917, 0.2294, 0.1896, 0.1667, 0.1587, 0.1743, 0.1356],
                    "model": ["CLEVER+COFFEE"] * 4 + ["CLEVER"] * 5 + ["ELF"], "lowWater": [9]}
    alone = {m: C.forecast_days({m: r["forecasts"][m]}, now, r["now"]["discharge"], ranker(r["envelope"]["discharge"]))
             for m in ("CLEVER", "COFFEE")}
    for i in range(4):                                         # each joined day lies between its two models' own
        lo, hi = sorted((alone["CLEVER"]["discharge"][i], alone["COFFEE"]["discharge"][i]))
        assert lo <= days["discharge"][i] <= hi
    assert days["discharge"][4:9] == alone["CLEVER"]["discharge"][4:9]


def test_the_publisher_writes_the_joined_day_and_archives_each_run_once(tmp_path, monkeypatch):
    r = json.loads((FIX / "forecast_days" / "08MH001.json").read_text(encoding="utf-8"))
    now = datetime.fromisoformat(r["fetchedAt"].replace("Z", "+00:00"))
    monkeypatch.setattr(pub, "fetch_station", lambda st: {
        "station": st, "at": r["now"]["at"], "level": r["now"]["level"], "discharge": r["now"]["discharge"],
        "recent": [[r["now"]["at"], r["now"]["level"], r["now"]["discharge"]]]})
    monkeypatch.setattr(pub, "fetch_temperatures", lambda stations, hours=24: {})
    monkeypatch.setattr(pub, "_get", lambda url: None)
    out = tmp_path / "feeds"
    clim = {"release": "test", "stations": {"08MH001": r["envelope"]}}
    first = pub.publish(out, ["08MH001"], clim=clim, forecast={"08MH001": r["forecasts"]}, now=now)
    row = json.loads((out / "index.json").read_text())["stations"]["08MH001"]
    assert row["aheadDays"]["model"] == ["CLEVER+COFFEE"] * 4 + ["CLEVER"] * 5 + ["ELF"]
    assert [row["ahead"][h]["model"] for h in ("1", "3", "5", "10")] == ["CLEVER+COFFEE", "CLEVER+COFFEE", "CLEVER", "ELF"]
    # the archive: beside the published directory, never in it; one line per run; a second publish adds nothing
    assert first["runArchive"]["added"] == 3 and not list(out.glob("*.runs")) and (tmp_path / "feeds.runs").is_dir()
    again = pub.publish(out, ["08MH001"], clim=clim, forecast={"08MH001": r["forecasts"]}, now=now + timedelta(hours=1))
    assert again["runArchive"]["added"] == 0
    lines = (tmp_path / "feeds.runs" / "2026-10.jsonl").read_text().splitlines()
    assert sorted(json.loads(x)["model"] for x in lines) == ["CLEVER", "COFFEE", "ELF"]


# ------------------------------------------------------------------------------------------ the run archive ----

def test_a_run_record_is_its_daily_means_on_the_gauges_scale(tmp_path):
    rec = S.run_record("X", "COFFEE", both_runs()["COFFEE"], NOW, 10.0)
    assert rec == {"station": "X", "model": "COFFEE", "issuedAt": None, "at": "2026-09-04T20:00:00Z", "observed": 10.0,
                   "scale": 0.5, "days": {"2026-09-04": 20.0, "2026-09-05": 28.0, "2026-09-06": 28.0,
                                          "2026-09-07": 28.0, "2026-09-08": 28.0}}
    clever = S.run_record("X", "CLEVER", both_runs()["CLEVER"], NOW, 10.0)
    assert clever["scale"] == 1.0 and sorted(clever["days"]) == [f"2026-09-{d:02d}" for d in range(4, 15)]
    assert S.run_record("X", "CLEVER", {"series": {}}, NOW, 10.0) is None


def test_the_archive_keeps_fourteen_months(tmp_path):
    out = tmp_path / "feeds"
    folder = S.archive_dir(out)
    folder.mkdir(parents=True)
    for k in range(16):
        y, m = divmod(2025 * 12 + 5 + k, 12)
        (folder / f"{y}-{m + 1:02d}.jsonl").write_text("")
    S.append_runs(out, {"X": both_runs()}, NOW, {"X": 10.0})
    kept = sorted(f.name for f in folder.iterdir())
    assert S.ARCHIVE_MONTHS == 14 and len(kept) == 14 and kept[0] == "2025-08.jsonl.gz"
    # the last two months stay plain (this one is appended to), the closed ones are gzipped, and all read back
    assert kept[-2:] == ["2026-08.jsonl", "2026-09.jsonl"] and all(k.endswith(".jsonl.gz") for k in kept[:-2])
    assert [r["model"] for r in S.read_runs(folder)] == ["CLEVER", "COFFEE", "ELF"]


def test_a_gzipped_month_reads_back_and_still_dedupes(tmp_path):
    out = tmp_path / "feeds"
    S.append_runs(out, {"X": both_runs()}, NOW, {"X": 10.0})
    later = NOW + timedelta(days=61)                                    # two months on: September is closed
    S.append_runs(out, {}, later, {})
    folder = S.archive_dir(out)
    assert sorted(f.name for f in folder.iterdir()) == ["2026-09.jsonl.gz"]
    assert len(S.read_runs(folder)) == 3 and len(S.read_runs(folder, ("2026-09",))) == 3


def archive_of(pairs):
    """Archived runs for a station issued day by day: `pairs` of (clever, coffee) forecasts for the next day."""
    recs, obs = [], {}
    for k, (cl, co) in enumerate(pairs):
        d0 = date(2026, 10, 1) + timedelta(days=k)
        at = f"{d0.isoformat()}T09:00:00"
        nxt = (d0 + timedelta(days=1)).isoformat()
        recs += [{"station": "X", "model": "CLEVER", "issuedAt": at, "scale": 1.0, "days": {nxt: cl}},
                 {"station": "X", "model": "COFFEE", "issuedAt": at, "scale": 0.5, "days": {nxt: co * 2}}]
        obs[nxt] = 10.0
    return recs, {"X": obs}


def test_the_score_pairs_the_models_on_the_same_days():
    """Day after day CLEVER says 10 (right), COFFEE 20 on the gauge (twice too high): CLEVER's error 0, COFFEE's 1
    (log2), joined sqrt(200) -> 0.5; ranked, the truth 10 is p42 (normal), COFFEE 20 past p90 (0.95, high)."""
    recs, obs = archive_of([(10.0, 20.0)] * 3)
    found = S.cases(recs, obs, date(2026, 11, 1))
    assert [(c["issued"], c["lead"], c["by"]) for c in found] == [
        (f"2026-10-0{d}", 1, {"CLEVER": 10.0, "COFFEE": 20.0}) for d in (1, 2, 3)]
    t = S.score(found, lambda st: ranker(FLAT), leads=(1,))["1"]
    assert t["paired"]["CLEVER"] == {"n": 3, "bias": 0.0, "mae": 0.0, "pmae": 0.0, "hit": 1.0, "nr": 3}
    assert t["paired"]["COFFEE"] == {"n": 3, "bias": 1.0, "mae": 1.0, "pmae": 0.533, "hit": 0.0, "nr": 3}
    assert t["paired"]["combined"]["bias"] == 0.5
    assert t["paired"]["difference"] == {"n": 3, "mean": -1.0, "se": 0.0}
    assert (t["issueDays"], t["pairedStations"]) == (3, 1)


def test_a_day_only_one_model_reached_is_not_in_the_head_to_head():
    recs, obs = archive_of([(10.0, 20.0)] * 3)
    recs = [r for r in recs if not (r["model"] == "COFFEE" and r["issuedAt"].startswith("2026-10-03"))]
    t = S.score(S.cases(recs, obs, date(2026, 11, 1)), lambda st: ranker(FLAT), leads=(1,))["1"]
    assert (t["own"]["CLEVER"]["n"], t["own"]["COFFEE"]["n"], t["paired"]["CLEVER"]["n"], t["paired"]["difference"]["n"]) \
        == (3, 2, 2, 2)


def test_a_day_not_over_is_not_scored():
    recs, obs = archive_of([(10.0, 20.0)])
    assert S.cases(recs, obs, date(2026, 10, 2)) == []


@pytest.mark.parametrize("n, days, mean, se, want", [
    (5, 1, -0.534, 0.555, {"CLEVER": 1.0, "COFFEE": 1.0}),      # what this machine holds today: too few
    (5, 1, -1.0, 0.0, {"CLEVER": 1.0, "COFFEE": 1.0}),          # too few, however clear it looks
    (400, 5, -1.0, 0.01, {"CLEVER": 1.0, "COFFEE": 1.0}),       # many pairs from few storms: still too few
    (400, 30, 0.02, 0.03, {"CLEVER": 1.0, "COFFEE": 1.0}),       # enough, neither better
    (400, 30, -0.3, 0.05, {"CLEVER": 1.0, "COFFEE": 0.5}),       # enough, CLEVER better: 1/0.2 vs 1/0.4
])
def test_the_rule_equal_unless_the_record_says_otherwise(n, days, mean, se, want):
    row = {"issueDays": days, "paired": {"CLEVER": {"mae": 0.2}, "COFFEE": {"mae": 0.4},
                                         "difference": {"n": n, "mean": mean, "se": se}}}
    assert S.verdict(row)["weights"] == want
    assert (S.MIN_PAIRS, S.MIN_ISSUE_DAYS) == (200, 20)
