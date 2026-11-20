"""(T869) the annual chinook limit's tier in our words; (T870) a full-year gear line reads "all year", and each place's
DFO rows as printed (`printed_rows`, `rows_by_location`) — the release's `pr` and the live feed's `locs[lid].pr`. The
worker's twins are held to these through the dfo vectors (`app/workers/feeds/test/dfo.vectors.test.ts`)."""
from __future__ import annotations

from types import SimpleNamespace

from pipeline.deliver.salmon import gear_text, place_gear, printed_rows
from pipeline.regs.dfo_salmon.live import rows_by_location
from pipeline.regs.dfo_salmon.quota import bucket, period_says

ANNUAL = "The total annual limit for chinook salmon over 50 cm is 10 fish from all fresh waters combined."
MONTHLY = "There is a monthly limit of 4 chinook over 50 cm in Region 8."


def _dates(fm, fd, tm, td):
    return {"dates": [{"from_month": fm, "from_day": fd, "to_month": tm, "to_day": td}]}


def test_the_annual_chinook_limit_counts_adult_chinook_in_our_words():
    assert period_says({"k": "annual", "t": ANNUAL, "sp": ["CH"], "n": 10}) == "10 adult chinook a year from all fresh waters"
    # DFO's sentence naming no fresh waters: the count alone; any other period limit keeps DFO's words
    assert period_says({"k": "annual", "t": "The annual limit for chinook over 50 cm is 10.", "sp": ["CH"], "n": 10}) \
        == "10 adult chinook a year"
    assert period_says({"k": "monthly", "t": MONTHLY, "sp": ["CH"], "n": 4}) == MONTHLY
    assert period_says({"k": "annual", "t": "The annual coho limit is 10.", "sp": ["CO"], "n": 10}) == "The annual coho limit is 10."
    notes = [{"id": "2:daily:a", "k": "daily", "t": "The daily limit … is 4.", "n": 4},
             {"id": "2:annual:b", "k": "annual", "t": ANNUAL, "sp": ["CH"], "n": 10, "lengths": [{"min_cm": 50}]},
             {"id": "8:monthly:c", "k": "monthly", "t": MONTHLY, "sp": ["CH"], "n": 4, "lengths": [{"min_cm": 50}]}]
    tiers = bucket(notes)["tree"]["children"]
    assert [(t["says"], t["period"], t["over_cm"]) for t in tiers] == [
        ("10 adult chinook a year from all fresh waters", "annual", 50), (MONTHLY, "monthly", 50)]


def test_a_gear_line_holding_every_day_reads_all_year():
    ban = {"rule_id": "x.r0_bait", "type": "bait_restriction", "verbatim": "Bait ban", "when": _dates(4, 1, 3, 31)}
    assert gear_text(ban) == "Bait ban (all year)"
    assert place_gear([ban], 1119) == ["Bait ban (all year)"]
    # a part of the year keeps its dates; no dates and an unread season keep their words as they were
    assert gear_text({**ban, "when": _dates(8, 1, 10, 31)}) == "Bait ban (Aug 1–Oct 31)"
    assert gear_text({**ban, "when": {}}) == "Bait ban"
    assert gear_text({**ban, "when": {**_dates(4, 1, 3, 31), "unparsed": ["sometimes"]}}).startswith("Bait ban (Apr 1–Mar 31")


def test_a_places_rows_as_printed_are_the_rows_in_force_each_once():
    loc = {"fingerprint": "f1", "rows": ["All | Apr 1 to Mar 31 | Bait ban", "Coho | Sep 15 to Jan 31 | 1 per day",
                                         "Chum | Oct 1 to Nov 30 | 2 per day", "Coho | Sep 15 to Jan 31 | 1 per day"],
           "hidden": [2]}
    assert printed_rows(loc) == ["All | Apr 1 to Mar 31 | Bait ban", "Coho | Sep 15 to Jan 31 | 1 per day"]
    ef = SimpleNamespace(by_fingerprint=lambda: {"f1": SimpleNamespace(location_id="2:m:whole"),
                                                 "page:2:x": SimpleNamespace(location_id="2:default:x")})
    typed = {"locators": [loc, {"fingerprint": "page:2:x", "rows": ["There is no fishing for salmon … below."]},
                          {"fingerprint": "unknown", "rows": ["Pink | Aug 1 to Sep 30 | 2 per day"]}]}
    assert rows_by_location(ef, typed) == {"2:m:whole": ["All | Apr 1 to Mar 31 | Bait ban", "Coho | Sep 15 to Jan 31 | 1 per day"],
                                           "2:default:x": ["There is no fishing for salmon … below."]}
