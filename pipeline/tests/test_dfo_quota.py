"""T320: DFO salmon quotas per species, under the region's salmon bucket (`pipeline/regs/dfo_salmon/quota.py`).

  * a fixture per page shape — Vancouver Island (Stamp), the Lower Mainland (Vedder, Stave), the Skeena's sections
    (A's region default, the Kispiox in B(i)) — read to the numbers DFO prints;
  * every scraped row of every tracked page (and, where the cache holds them, every archived page) either reads FULLY
    (no number unaccounted for, and the reading round-trips) or is flagged with a NAMED reason — never guessed;
  * the bucket: each place's species limit fits under its region's total, each size / wild allowance under its species';
  * the owner's rules of 2026-10-31: no per-species possession; the header's closure wins (5A, 7); "N of which can be
    unmarked" and the archived monthly quotas are structure.
"""
from __future__ import annotations

import glob
import re
from pathlib import Path

import pytest

from pipeline.deliver import salmon as S
from pipeline.regs.dfo_salmon import feed as FD
from pipeline.regs.dfo_salmon import header as H
from pipeline.regs.dfo_salmon import quota as Q
from pipeline.regs.dfo_salmon.parse import parse_region
from pipeline.regs.dfo_salmon.typed import to_rules

REPO = Path(__file__).resolve().parents[2]
FIX = REPO / "pipeline/tests/fixtures/dfo_salmon"
SLUGS = ["1", "2", "3", "4", "5a", "5b", "6", "7", "8"]


def _page(slug: str) -> str:
    return (FIX / f"region{slug}-eng.html").read_text(encoding="utf-8")


def _row(slug: str, water: str, species: str, limits_start: str):
    for r in parse_region(_page(slug), slug).rows:
        if water in (r.waters or "") and r.species == species and r.limits_gear.startswith(limits_start):
            return r
    raise AssertionError(f"no {water} / {species} / {limits_start!r} row in region {slug}")


def _quota(row) -> dict:
    """The verdict's numbers for a row on a day it holds: the rules it types, through `verdict_on`."""
    rules = [r.model_dump(mode="json", exclude_defaults=True, by_alias=True)
             for r in to_rules({"species": row.species, "dates": row.dates, "limits_gear": row.limits_gear}, "x", 0)]
    sp = rules[0]["species"][0]
    for md in S.DAYS:
        v = S.verdict_on([rules], sp, md)
        if v and v["s"] == "open":
            return v["q"]
    raise AssertionError(f"{row.limits_gear!r} opens no day")


# ── a fixture per page shape ─────────────────────────────────────────────────────────────────────────────────────────

def test_vancouver_island_stamp():
    assert _quota(_row("1", "Stamp River", "Chinook", "2 per day, 1 of which")) == {"d": 2, "o": [[77, 1]]}
    assert _quota(_row("1", "Stamp River", "Coho", "2 per day, hatchery")) == {"d": 2, "h": 1}


def test_lower_mainland_vedder_and_stave():
    assert _quota(_row("2", "Chilliwack/Vedder", "Chinook", "4 per day, only 2 over 62")) == {"d": 4, "o": [[62, 2]]}
    assert _quota(_row("2", "Chilliwack/Vedder", "Coho", "4 hatchery marked per day")) == {"d": 4, "h": 1}
    assert _quota(_row("2", "Stave River", "Coho", "4 hatchery marked per day, only 2 over 35")) == \
        {"d": 4, "h": 1, "o": [[35, 2]]}


def test_skeena_sections():
    a = [r for r in parse_region(_page("6"), "6").rows if r.section_key == "A"]
    assert [(r.species, _quota(r)) for r in a if r.species != "Sockeye, pink and chum"] == \
        [("Chinook", {"d": 4, "o": [[65, 1]]}), ("Coho", {"d": 4, "o": [[50, 1]]})]
    kispiox = _row("6", "Kispiox River", "Coho", "4 per day, only 2 over 50")
    assert kispiox.section_key == "B(i)" and _quota(kispiox) == {"d": 4, "o": [[50, 2]]}


def test_new_shapes_read_as_structure():
    """"N of which can be unmarked" (Region 1, FN1013 / FN1069) and the archived pages' wordings."""
    def q(text):
        return _quota(type("R", (), {"species": "Coho", "dates": "Sep 19 to Oct 2", "limits_gear": text}))
    assert q("2 per day, 1 of which can be unmarked — FN1013") == {"d": 2, "w": 1}
    assert q("2 per day, 2 of which can be unmarked — FN1069") == {"d": 2, "w": 2}
    assert q("4 per day only 1 over 65 cm. Monthly Quota = 1 over 65 cm.") == {"d": 4, "o": [[65, 1]], "mo": [[65, 1]]}
    assert q("Daily limit of 4 of which 1 may be over 65 cm in length. Monthly quota = 1 over 65 cm") == \
        {"d": 4, "o": [[65, 1]], "mo": [[65, 1]]}
    assert q("4 per day, only two over 50 cm.") == {"d": 4, "o": [[50, 2]]}


def test_an_unread_cell_is_flagged_with_its_reason_never_guessed():
    text = "2 per day, only 1 65cm or less and 1 over 65 cm."
    assert Q.unread_reason(text).startswith("a size band the reading does not model")
    q = _quota(type("R", (), {"species": "Chinook", "dates": "Apr 1 to Mar 31", "limits_gear": text}))
    assert q["u"] == 1                                       # the app shows DFO's words, no number


# ── every row: read fully, or flagged with a named reason ────────────────────────────────────────────────────────────

def _corpus():
    pages = [FIX / f"region{s}-eng.html" for s in SLUGS]
    pages += [Path(p) for p in sorted(glob.glob(str(FIX / "region6_*.html")))]
    pages += [Path(p) for p in sorted(glob.glob(str(REPO / "cache/dfo_salmon/history/region*_*.html")))]
    pages += [Path(p) for p in sorted(glob.glob(str(REPO / "cache/dfo_salmon/raw/region*-eng.html")))]
    for p in pages:
        slug = re.search(r"region(\w+?)(?:-eng|_\d+)\.html", p.name).group(1)
        if slug != "5":
            yield p, slug


NAMED = ("DFO has not set this row yet", "a bare '0 per day'", "unmapped species cell", "another fish, not salmon",
         "a recording rule", "a closure of part of the water", "a size band the reading does not model",
         "a gear permission", "dates do not parse")


def test_every_row_reads_fully_or_is_flagged_with_a_named_reason():
    read = flagged = 0
    for path, slug in _corpus():
        for i, r in enumerate(parse_region(path.read_text(encoding="utf-8", errors="replace"), slug).rows):
            got = Q.read_limits(r.limits_gear)
            rules = to_rules({"species": r.species, "dates": r.dates, "limits_gear": r.limits_gear}, "x", i)
            why = [x.review_reason for x in rules if x.review_reason]
            if not got["residue"] and not why:
                # the round trip: the reading, said back, reads to the same numbers
                assert Q.numbers(Q.read_limits(Q.render(got))) == Q.numbers(got), (path.name, r.limits_gear)
                read += 1
                continue
            assert why and all(w.startswith(NAMED) for w in why), (path.name, r.limits_gear, why)
            if got["residue"]:
                assert any(w.startswith(NAMED[4:7]) for w in why), (path.name, r.limits_gear, why)
            flagged += 1
    assert read > 10 * flagged                               # the tracked pages alone: ~470 rows, a handful flagged


# ── the bucket ───────────────────────────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("slug", SLUGS)
def test_each_places_species_rows_fit_under_its_bucket(slug):
    html = _page(slug)
    b = Q.bucket(H.parse_header(html, slug))
    p = parse_region(html, slug)
    feed = FD.build({"region": slug, "rules": [
        {"fingerprint": f"{r.waters}|{r.specific_area}", "species": r.species, "dates": r.dates,
         "limits_gear": r.limits_gear} for r in p.rows]})
    for loc in feed["locators"]:
        for sp in S.SPECIES:
            for md in S.DAYS[::7]:
                v = S.verdict_on([loc["rules"]], sp, md)
                if v and v.get("q"):
                    assert Q.over_bucket(v["q"], b) == [], (slug, loc["fingerprint"], sp, v)


def test_the_bucket_is_the_headers_total_and_possession_only():
    b = Q.bucket(H.parse_header(_page("2"), "2"))
    assert (b["node"], b["tier"], b["n"], b["says"], b["possession"]) == \
        ("tier", "total", 4, "Up to 4 salmon a day in total", 8)
    assert b["min_cm"] == {"CO": 25, "CH": 30, "SK": 30, "PK": 30, "CM": 30}
    assert "pm" not in b                                      # owner 2026-10-31: no per-species possession
    assert Q.bucket(H.parse_header(_page("4"), "4")) is None  # closed until further notice: no bucket


def test_a_closed_header_wins_over_the_tables_rows():
    """Regions 5A and 7 still print the 2025 rows (FN0846, FN0851: Aug 22 - Sep 30, 2025) under the restored closure."""
    for slug in ("5a", "7"):
        closure = Q.header_closure(H.parse_header(_page(slug), slug), slug)
        assert closure and closure[0]["take"] == 0 and closure[0]["may_target"] is False
        v = S.verdict_on([closure], "SK", 901)
        assert v["s"] == "closed" and "There is no fishing for salmon in Region" in v["t"]
    assert Q.header_closure(H.parse_header(_page("2"), "2"), "2") is None
