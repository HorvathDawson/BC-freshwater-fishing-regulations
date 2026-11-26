"""T796 hard cases (break-it review 2026-11-16, the t796 hard-cases folder, cut in T1081): the
salmon card quotes THE BOOK'S LINE for a book closure, and its dates never contradict that line.

S2: the ✱ tributary mark (the bundle's "[Includes Tributaries]") cut the rule's words before its dates. S4: the PDF's
line break stayed inside the quoted line. X3: a rule after a ";" was quoted as a fragment. S1: a closure lifted on
some of its printed days showed "No Fishing Oct 1-May 31 (Oct 1–Apr 30)" with nothing saying why. The rows are the
bundle's own text (r-685b700b2802's inputs). Each test builds its own small data.
"""
from __future__ import annotations

import json
import sqlite3

import pytest

from pipeline.deliver import salmon as S

ISKUT = ("**No Fishing** downstream of Forest Kerr Canyon[Includes Tributaries], Apr 1-Apr 30; no powered boats between "
         "Natadesleen Lake and Kinaskan Lake")
DEENA = ("**No Fishing** upstream of fishing boundary signs at second bridge approximately 5 km upstream of the tidal "
         "boundary[Includes Tributaries], Feb 1-May 31;\nCutthroat catch and release;\nBait ban Nov 1-Apr 30[Includes "
         "Tributaries]; C**lass II water Sept 1-Apr 30; Steelhead Stamp mandatory Dec 1-Apr 30**")
QUINSAM = ("**No Fishing** from the fishing boundary signs at power line crossing (approximately 25 m upstream of Quinsam "
           "Hatchery weir) to fishing\nboundary signs approximately 300 m downstream of weir\n**No Fishing** from the "
           "falls situated downstream of Middle Quinsam Lake to the fishing boundary signs at power line crossing\n"
           "(approximately 25 m upstream of the Quinsam hatchery weir), Dec 1-June 30\nExempt from July 15-Aug 31 "
           "summer closure; wheelchair accessible fishing platform is located near the Hwy 28 Bridge")
CAYCUSE = "**No Fishing** July 15-Aug 31, fly fishing only upstream of and including Hatton Creek"


def test_the_line_keeps_its_dates_past_the_tributary_mark():
    """S2, Iskut p.51 and Deena Creek p.15: the line runs on past the ✱ to its dates and stops at the clause's ";".
    MUTATION: quote the rule's own words and the dates are lost."""
    assert S.book_line("No Fishing downstream of Forest Kerr Canyon", ISKUT, ["no powered boats between Natadesleen "
                       "Lake and Kinaskan Lake"]) == "No Fishing downstream of Forest Kerr Canyon, Apr 1-Apr 30"
    assert S.book_line("No Fishing upstream of fishing boundary signs at second bridge approximately 5 km upstream of "
                       "the tidal boundary", DEENA, ["Cutthroat catch and release", "Bait ban Nov 1-Apr 30"]) == (
        "No Fishing upstream of fishing boundary signs at second bridge approximately 5 km upstream of the tidal "
        "boundary, Feb 1-May 31")


def test_the_line_has_no_pdf_line_break():
    """S4, Quinsam p.18: the PDF's wrap inside the line is a space; the next "No Fishing" line of the row is not
    taken. MUTATION: keep the rule's "\\n" and the card breaks mid-sentence."""
    r1 = ("No Fishing from the fishing boundary signs at power line crossing (approximately 25 m upstream of Quinsam "
          "Hatchery weir) to fishing\nboundary signs approximately 300 m downstream of weir")
    r2 = ("No Fishing from the falls situated downstream of Middle Quinsam Lake to the fishing boundary signs at power "
          "line crossing\n(approximately 25 m upstream of the Quinsam hatchery weir), Dec 1-June 30")
    sibs = [r1, r2, "Exempt from July 15-Aug 31 summer closure"]
    assert S.book_line(r1, QUINSAM, sibs) == " ".join(r1.split())
    assert S.book_line(r2, QUINSAM, sibs) == " ".join(r2.split())
    assert "\n" not in S.book_line(r2, "", ())


def test_a_fragment_is_quoted_from_the_start_of_its_line():
    """X3, Buttle Lake's tributaries p.14: "except Thelwood Creek is closed all year" is quoted with the words before it.
    MUTATION: quote the fragment alone."""
    assert S.book_line("except Thelwood Creek is closed all year", "Fly fishing only; except Thelwood Creek is closed "
                       "all year", ["Fly fishing only"]) == "Fly fishing only; except Thelwood Creek is closed all year"


def test_the_line_never_runs_into_another_rule_of_its_row():
    """Caycuse River: the next words after ", " are the row's next rule (fly fishing only): they stay out of the
    closure's line. A rule its row holds twice is not extended (which one is it?)."""
    assert S.book_line("No Fishing July 15-Aug 31", CAYCUSE, ["fly fishing only upstream of and including Hatton "
                       "Creek"]) == "No Fishing July 15-Aug 31"
    assert S.book_line("No Fishing", "**No Fishing** Nov 1-Apr 30\n**No Fishing** in the inlet", ()) == "No Fishing"


def _db(rules, sets, entries):
    """A bundle with what `book_lifts` and `_entry_line` read: rules (entry, rule, verbatim, when, exempts), the
    sections' rule sets, the rows."""
    db = sqlite3.connect(":memory:")
    db.executescript("CREATE TABLE rule (entry_id, rule_id, verbatim, when_, exempts);"
                     "CREATE TABLE entry (entry_id, verbatim, pages, name);"
                     "CREATE TABLE ruleset (set_id, entry_id, rule_id);"
                     "CREATE TABLE section_ruleset (sid, set_id);")
    db.executemany("INSERT INTO rule VALUES (?,?,?,?,?)",
                   [(e, r, v, json.dumps(w) if w else None, json.dumps(x) if x else None) for e, r, v, w, x in rules])
    db.executemany("INSERT INTO entry VALUES (?,?,?,?)", [(e, v, json.dumps(p), n) for e, v, p, n in entries])
    for set_id, members in sets.items():
        db.executemany("INSERT INTO ruleset VALUES (?,?,?)", [(set_id, e, r) for e, r in members])
    db.executemany("INSERT INTO section_ruleset VALUES (?,?)", [(sid, set_id) for set_id in sets for sid in (set_id,)])
    return db


def _dates(fm, fd, tm, td):
    return {"dates": [{"from_month": fm, "from_day": fd, "to_month": tm, "to_day": td}]}


THOMPSON = "r3:thompson@3-13"
CLOSE = f"{THOMPSON}::thompson.r1"
OPENING = ("Additional opening from the CNR Bridge downstream of Deadman River to CNR Bridge upstream of Bonaparte "
           "River, May 1-31")


def _thompson(with_lift=True):
    rules = [(THOMPSON, "thompson.r1", "No Fishing Oct 1-May 31", _dates(10, 1, 5, 31), None),
             (THOMPSON, "thompson.r3o", OPENING, _dates(5, 1, 5, 31),
              [{"entry_id": THOMPSON, "rule_id": "thompson.r1", "when": _dates(5, 1, 5, 31)}] if with_lift else None)]
    return _db(rules, {7: [(THOMPSON, "thompson.r1"), (THOMPSON, "thompson.r3o")]},
               [(THOMPSON, f"**No Fishing** Oct 1-May 31\n{OPENING}", [32], "Thompson River")])


def _days(fm, fd, tm, td):
    return frozenset(i for i, md in enumerate(S.DAYS) if S.holds_on({"when": _dates(fm, fd, tm, td)}, md))


def test_a_lifted_closure_says_what_lifts_it():
    """S1, Thompson below Kamloops Lake p.32: "No Fishing Oct 1-May 31" closes Oct 1-Apr 30 there (the row's
    "Additional opening … May 1-31" lifts May): the card says so after the line, so its dates "(Oct 1–Apr 30)" do not
    contradict "Oct 1-May 31". MUTATION: no lift words and the card reads "No Fishing Oct 1-May 31 (Oct 1–Apr 30)"."""
    db = _thompson()
    closes = _days(10, 1, 4, 30)
    lifts = S.book_lifts(db, {7: {CLOSE: closes}})
    assert lifts == {7: {CLOSE: f"lifted May 1–31 by Thompson River (p.32): {OPENING}"}}
    words = {CLOSE: S._entry_line(db, THOMPSON, "thompson.r1") | {"region": "3"}}
    assert words[CLOSE]["verbatim"] == "No Fishing Oct 1-May 31"
    (payload, sections) = _one_chain()
    out, secs = S.with_book_closures(payload, sections, {7: {CLOSE: closes}}, words, lifts)
    ch = out["chains"][dict(secs)[7]]
    from pipeline.deliver.salmon_who import _run_at
    t = out["verdicts"][_run_at(ch["sp"][0], S.DAYS.index(1010))]["t"]
    assert t == (f"Closed to all fishing by the B.C. Synopsis (p.32): No Fishing Oct 1-May 31; lifted May 1–31 by Thompson "
                 f"River (p.32): {OPENING} (Oct 1–Apr 30)")


def test_a_closure_missing_days_nothing_lifts_stops_the_build():
    """No rule of the section lifts the closure on the days it does not close: never a card whose dates contradict its
    line in silence. Feb 29 alone is not a gap (the verdicts read a "to Feb 28" as closing Feb 29 too: Kawkawa Lake
    "Dec 1-Feb 28" closes Feb 29; a leap day read apart is never a lift). MUTATION: count Feb 29 and the second
    case stops."""
    with pytest.raises(S.SalmonError, match="fewer days than it prints"):
        S.book_lifts(_thompson(with_lift=False), {7: {CLOSE: _days(10, 1, 4, 30)}})
    db = _db([("r3:spring@3", "spring.r1", "No Fishing Jan 1-June 30", _dates(1, 1, 6, 30), None)],
             {3: [("r3:spring@3", "spring.r1")]}, [("r3:spring@3", "**No Fishing** Jan 1-June 30", [28], "S")])
    rule = "r3:spring@3::spring.r1"
    assert S.book_lifts(db, {3: {rule: _days(1, 1, 6, 30) - {S.DAYS.index(229)}}}) == {}


def _one_chain():
    """One DFO place open for salmon all year on section 7 (and section 8 beside it)."""
    from types import SimpleNamespace
    loc = SimpleNamespace(location_id="3:thompson:below", precedence=3, fingerprints=["fp"], water="Thompson River",
                          water_id="3:thompson", source_text={"specific_area": "below Kamloops Lake"},
                          tidal_water=False, region_number=3)
    rule = {"rule_id": "fp.r0", "type": "retention_limit", "verbatim": "2 per day", "species": ["SA"], "take": 2}
    feed = {"locators": [{"fingerprint": "fp", "rules": [rule]}]}
    placed = [{"location_id": loc.location_id, "mapped": True, "sections": ["s7", "s8"]}]
    ef = SimpleNamespace(region_name="R3", locations=[loc], water=lambda wid: None)
    return S.build({"3": ef}, {"3": feed}, placed, {"s7": 7, "s8": 8}, section_regions=None)


SUPERIOR = ("Freshwater fishing is prohibited in National Parks unless opened under the National Parks Fishing "
            "Regulations. Where open, anglers require a National Park Fishing Permit. Fishing is prohibited in "
            "Ecological Reserves in B.C.")


def test_a_conditional_closure_says_its_proviso():
    """X2 (owner 2026-11-16: "Closed, book's wording (Recommended)"), p.9: the national parks row stays closed (RULINGS
    Z12) but is never "closed to all fishing" outright: its head says "unless opened under the National Parks Fishing
    Regulations" and its line keeps the permit sentence. The reserves' row beside it keeps the plain head.
    MUTATION: the plain head and the card says closed to all fishing in Yoho."""
    r1 = "Freshwater fishing is prohibited in National Parks unless opened under the National Parks Fishing Regulations."
    sibs = ["Where open, anglers require a National Park Fishing Permit", "Fishing is prohibited in Ecological Reserves"]
    line = S.book_line(r1, SUPERIOR, sibs)
    assert line == f"{r1} Where open, anglers require a National Park Fishing Permit."
    t = S.book_rule("book:x", "zp::r1", range(len(S.DAYS)), {"zp::r1": {"verbatim": line, "pages": [9]}})["verbatim"]
    assert t == ("Closed by the B.C. Synopsis unless opened under the National Parks Fishing Regulations (p.9): "
                 f"{line}")
    reserve = S.book_line("Fishing is prohibited in Ecological Reserves in B.C.", SUPERIOR, sibs)
    t3 = S.book_rule("book:y", "zp::r3", range(len(S.DAYS)), {"zp::r3": {"verbatim": reserve, "pages": [9]}})
    assert t3["verbatim"] == "Closed to all fishing by the B.C. Synopsis (p.9): Fishing is prohibited in Ecological " \
                             "Reserves in B.C."
