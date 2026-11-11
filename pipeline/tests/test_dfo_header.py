"""THE REGION HEADER (owner 2026-10-28, `pipeline/regs/dfo_salmon/header.py`) — no network, no atlas.

  * every header sentence becomes a typed note carrying DFO's words (a nested item with its lead-in); a bare lead-in
    ("You must be aware of these ...:") is no note; nothing is dropped (an unknown sentence is `other`);
  * the "adult chinook ... except ... over 62 cm:" list: a region-wide note plus one scoped note per exception, its
    waters split and its date window as day indexes;
  * DFO's unclosed <li> (Region 5B) does not glue two notes together;
  * a scoped water binds only through the curated DFO waters (exact, then the qualifier dropped); a watershed adds what
    is upstream; a name that binds nothing is unresolved, never matched by nearness;
  * the live feed carries the notes, and a change of the header's words tells the owner once;
  * `deliver.salmon.notes_for` (the app's golden reference): region-wide notes on the region's pages, scoped ones only
    on their items and in their windows, an excepted water left out.
"""
from __future__ import annotations

import glob
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline.deliver import salmon as S
from pipeline.tests.conftest import need
from pipeline.regs.dfo_salmon import header as H

ROOT = Path(__file__).resolve().parents[2]

HEADER = """<html><main><h1 property="name" id="wb-cont">Region 6 - Skeena: Recreational salmon fishing limits</h1>
<!-- <div>System maintenance is planned</div> -->
<ul>
  <li>You can only fish for salmon in Region 6 during daylight hours.</li>
  <li>No fishing is allowed within 100&nbsp;meters of any government facility operated for counting, passing or rearing fish.</li>
  <li>You must be aware of these possession limits:
    <ul>
      <li>The daily limit for all species of Pacific salmon from tidal and fresh waters combined is 4. Individual species limits also apply.</li>
      <li>The possession limit for all salmon from all waters is twice the daily limit. You cannot possess more than 8 salmon in total, except for salmon that are at your ordinary residence.</li>
      <li>The total annual limit for chinook salmon over 50&nbsp;cm is 10 fish from all fresh waters combined.</li>
    </ul></li>
  <li>Kispiox watershed: the aggregate daily limit of hatchery-marked coho in the entire Kispiox non-tidal systems combined is one (1).</li>
  <li>Salmon measurements are taken from tip of nose to tail fork. You must be aware of these measurements:
    <ul>
      <li>All retained coho must measure 25&nbsp;cm or more.</li>
      <li>All retained chinook, chum, pink, and sockeye must measure 30&nbsp;cm or more. </li>
      <li>An "adult chinook" in Region 6 is defined as being over 50&nbsp;cm except in the following areas where an "adult chinook" is defined as being over 62&nbsp;cm:
        <ul>
          <li>the Kispiox River from Sep 1 to Dec 31</li>
          <li>the Babine and Bulkley Rivers from Sep 01 to Dec 31</li>
        </ul></li>
    </ul></li>
  <li>Annual bait ban in effect in all streams of Region 6 as follows:
    <ul><li>All streams in Region 6 except: Kispiox River; Zymoetz River downstream of the canyon, unless otherwise described below.</li></ul></li>
  <li>Reminder: There is no fishing for eulachon in Region 6.</li>
</ul>
<h3>Related links</h3><ul><li><a href="#">Learn more about fishing for salmon in B.C.</a></li></ul>
<table><tr><td>row</td></tr></table></main></html>"""


def _kinds(notes):
    return [n["k"] for n in notes]


def test_every_header_sentence_is_a_typed_note_in_dfos_words():
    notes = H.parse_header(HEADER, "6")
    assert _kinds(notes) == ["daylight", "facility", "daily", "possession", "annual", "aggregate", "measure", "min_size",
                             "min_size", "adult", "adult", "adult", "bait_ban", "other"]
    by = {n["k"]: n for n in notes}
    assert by["facility"]["m"] == 100 and "100 meters" in by["facility"]["t"]       # nbsp read as a space
    assert (by["daily"]["n"], by["possession"]["n"]) == (4, 8)
    assert (by["annual"]["sp"], by["annual"]["lengths"], by["annual"]["n"]) == (["CH"], [{"min_cm": 50}], 10)
    assert not any("cm" in n for n in notes)                  # T358: a length is `lengths` (LengthBand), never `cm`
    assert by["measure"]["t"] == "Salmon measurements are taken from tip of nose to tail fork."
    agg = by["aggregate"]
    assert (agg["sp"], agg["n"], agg["hatchery"], agg["w"]) == \
        (["CO"], 1, True, [{"n": "Kispiox River", "ws": True, "key": "kispiox river+ws"}])
    assert all(n["id"].startswith("6:") for n in notes) and len({n["id"] for n in notes}) == len(notes)
    assert not any("maintenance" in n["t"] or "Learn more" in n["t"] for n in notes)


def test_the_adult_definition_and_its_exceptions():
    adult = [n for n in H.parse_header(HEADER, "6") if n["k"] == "adult"]
    region, kispiox, two = adult
    assert (region["lengths"], "w" in region) == ([{"min_cm": 50}], False)
    assert kispiox["lengths"] == [{"min_cm": 62}] and [w["n"] for w in kispiox["w"]] == ["Kispiox River"]
    assert kispiox["on"] == [[244, 365]]                                             # Sep 1 .. Dec 31, leap calendar
    assert kispiox["t"].endswith("over 62 cm: the Kispiox River from Sep 1 to Dec 31")
    # the region-wide note quotes the exceptions, as DFO lists them
    assert region["t"].endswith("over 62 cm: the Kispiox River from Sep 1 to Dec 31; the Babine and Bulkley Rivers "
                                "from Sep 01 to Dec 31.")
    assert [w["n"] for w in two["w"]] == ["Babine River", "Bulkley River"] and two["on"] == [[244, 365]]


def test_the_bait_ban_excepts_whole_waters_and_keeps_a_part_verbatim():
    (ban,) = [n for n in H.parse_header(HEADER, "6") if n["k"] == "bait_ban"]
    assert [w["n"] for w in ban["x"]] == ["Kispiox River"]
    assert ban["xp"] == ["Zymoetz River downstream of the canyon"]
    assert ban["t"].startswith("Annual bait ban in effect in all streams of Region 6 as follows: All streams")


def test_an_unclosed_li_glues_nothing():
    html = HEADER.replace("</ul></li>\n  <li>Kispiox", "</ul>\n  <li>Kispiox")
    notes = H.parse_header(html, "6")
    assert [n for n in notes if n["k"] == "daily"][0]["t"].startswith("The daily limit")
    assert not any(n["t"].startswith("You must be aware") for n in notes)


def _ef(*waters):
    return SimpleNamespace(waters=[SimpleNamespace(name=n, aliases=[], item_ids=list(i)) for n, i in waters])


def test_names_bind_only_through_the_curated_waters():
    notes = H.parse_header(HEADER, "6")
    ef = _ef(("Kispiox River", ["gnis:1"]), ("Babine River (including Nilkitkwa)", ["gnis:2"]))
    scopes, unresolved = H.resolve(notes, ef, upstream=lambda items: ["gnis:9"] if "gnis:1" in items else [])
    assert scopes["kispiox river+ws"] == ["gnis:1", "gnis:9"]                        # the watershed: + upstream
    assert scopes["kispiox river"] == ["gnis:1"] and scopes["babine river"] == ["gnis:2"]   # qualifier dropped
    assert unresolved == [{"note": next(n["id"] for n in notes if n.get("w") and "Bulkley" in n["t"]), "name": "Bulkley River"}]
    # two curated waters of one name: never picked between
    _, un = H.resolve(notes, _ef(("Kispiox River", ["gnis:1"]), ("Kispiox River", ["gnis:3"])))
    assert {"Kispiox River"} <= {u["name"] for u in un}


@pytest.mark.needs_cache
def test_every_cached_region_page_parses_and_region_2_reads_as_printed():
    need(None, "cache", ROOT / "cache/dfo_salmon/raw/region2-eng.html", "python -m pipeline.regs.dfo_salmon.fetch")
    for p in sorted(glob.glob(str(ROOT / "cache/dfo_salmon/raw/region*-eng.html"))):
        slug = Path(p).name[6:-9]
        if slug == "5":
            continue
        notes = H.parse_header(Path(p).read_text(encoding="utf-8"), slug)
        assert notes, slug
        assert not any(n["t"].startswith(("You must be aware", "Learn more", "This table")) for n in notes), slug
    r2 = H.parse_header((ROOT / "cache/dfo_salmon/raw/region2-eng.html").read_text(encoding="utf-8"), "2")
    kinds = {n["k"] for n in r2}
    assert {"daylight", "facility", "daily", "possession", "annual", "aggregate", "min_size", "adult"} <= kinds


def test_notes_for_a_page():
    notes = [{**n} for n in H.parse_header(HEADER, "6")]
    scopes, _ = H.resolve(notes, _ef(("Kispiox River", ["gnis:1"])), upstream=lambda i: ["gnis:9"])
    payload = {"regions": {"6": {"notes": notes}}, "scopes": {"6": scopes}}
    ids = {n["id"]: n for n in notes}
    kinds = lambda xs: [ids[i]["k"] for i in xs]                                     # noqa: E731
    oct1, jul1 = 274, 182
    on_kispiox = S.notes_for(payload, ["6"], "gnis:1", oct1)
    assert kinds(on_kispiox["water"]) == ["aggregate", "adult"]
    assert "bait_ban" not in kinds(on_kispiox["region"])                             # excepted by name
    assert kinds(S.notes_for(payload, ["6"], "gnis:1", jul1)["water"]) == ["aggregate"]   # the 62 cm window is closed
    assert kinds(S.notes_for(payload, ["6"], "gnis:9", oct1)["water"]) == ["aggregate"]   # upstream: the watershed only
    other = S.notes_for(payload, ["6"], "gnis:5", oct1)
    assert other["water"] == [] and "bait_ban" in kinds(other["region"])
    assert S.notes_for(payload, ["2"], "gnis:1", oct1) == {"region": [], "water": []}


def test_the_feed_carries_the_header_and_a_change_tells_once(tmp_path, monkeypatch):
    from pipeline.regs.dfo_salmon import live as L
    from pipeline.regs.dfo_salmon import entries as E
    # the table is not what this test reads: a page that types to no locations, reconciled with no structure change
    monkeypatch.setattr(L, "type_page", lambda html, slug: ({"date_modified": "2026-10-28", "locators": []}, [], {}))
    monkeypatch.setattr(E, "reconcile", lambda ef, found, structure: SimpleNamespace(structural=[], outcomes=[]))
    sent = []
    fetch = lambda html: (lambda slug: (html, "2026-10-28T00:00:00Z"))               # noqa: E731
    one = L.run(tmp_path, fetcher=fetch(HEADER), regions=["6"], now="2026-10-28T00:00:00Z", notify_url="http://n",
                sender=lambda u, t: sent.append(t), entries_dir=tmp_path / "none")
    reg = one["index"]["regions"]["6"]
    assert _kinds(reg["notes"])[:2] == ["daylight", "facility"] and reg["header"]
    assert not any("general text changed" in t for t in sent)
    assert reg["bucket"]["tree"]["n"] == 4 and reg["texts"]                    # T320: the bucket and the kept text
    changed = HEADER.replace("is 4. Individual", "is 2. Individual")
    two = L.run(tmp_path, fetcher=fetch(changed), regions=["6"], now="2026-10-28T06:00:00Z", notify_url="http://n",
                sender=lambda u, t: sent.append(t), entries_dir=tmp_path / "none")
    assert [n["n"] for n in two["index"]["regions"]["6"]["notes"] if n["k"] == "daily"] == [2]
    assert sum("general text changed" in t for t in sent) == 1
    # T320 (owner 2026-10-31): old vs new, for a human — the feed reads no rule from it
    told = next(t for t in sent if "general text changed" in t)
    assert "- was: The daily limit for all species of Pacific salmon from tidal and fresh waters combined is 4." in told
    assert "+ now: The daily limit for all species of Pacific salmon from tidal and fresh waters combined is 2." in told
    assert two["index"]["regions"]["6"]["bucket"]["tree"]["n"] == 2
    L.run(tmp_path, fetcher=fetch(changed), regions=["6"], now="2026-10-28T12:00:00Z", notify_url="http://n",
          sender=lambda u, t: sent.append(t), entries_dir=tmp_path / "none")
    assert sum("general text changed" in t for t in sent) == 1
    # a failed fetch keeps the header
    three = L.run(tmp_path, fetcher=lambda s: (_ for _ in ()).throw(OSError("down")), regions=["6"],
                  now="2026-10-28T18:00:00Z", notify_url="http://n", sender=lambda u, t: sent.append(t),
                  entries_dir=tmp_path / "none")
    assert three["index"]["regions"]["6"]["notes"] == two["index"]["regions"]["6"]["notes"]


def test_the_release_resolves_the_newest_pages_waters_too():
    """A release whose rules predate today's page still carries the scopes today's header names (the feed's notes
    find their waters by key), its own notes staying its page's."""
    from pipeline.tests.test_salmon_file import _fixture
    files, feeds, placed, sid_of = _fixture()
    files["6"].waters = [SimpleNamespace(name="Kispiox River", aliases=[], item_ids=["gnis:4674"])]
    payload, _ = S.build(files, feeds, placed, sid_of, headers={"6": []},
                         scope_headers={"6": H.parse_header(HEADER, "6")}, upstream=lambda i: [], section_regions=None)
    assert payload["regions"]["6"]["notes"] == []
    assert payload["scopes"]["6"]["kispiox river"] == ["gnis:4674"]


def test_the_header_sizes_resolve_per_species_a_water_before_its_region():
    """(T356 / T358) `quota.sizes`: per species the header's sizes as LengthBands, ORDERED — a water's own size note an
    entry of its own (with the region's other sizes), before the region's; the app takes the first that holds."""
    from pipeline.regs.dfo_salmon import quota as Q
    notes = H.parse_header(HEADER, "6")
    got = Q.sizes(notes)
    ch = [e for e in got if "CH" in e["sp"]]
    assert [(e["adult"], e.get("w"), e.get("on")) for e in ch] == [
        ({"min_cm": 62}, ["kispiox river"], [[244, 365]]),
        ({"min_cm": 62}, ["babine river", "bulkley river"], [[244, 365]]),
        ({"min_cm": 50}, None, None)]
    assert ch[0]["min"] == ch[-1]["min"] == {"min_cm": 30}       # the water entry carries the region's minimum
    with pytest.raises(ValueError, match="excepts parts of waters"):
        Q.sizes([{**next(n for n in notes if n["k"] == "min_size"), "xp": ["the part below the falls"]}])


def test_t343_an_excepted_watershed_gets_the_default_adult_chinook_and_a_page_without_one_too():
    """(T343, owner) R6's adult chinook is over 65 cm "other than Fraser River Watershed": that watershed gets an entry
    of its own with the region's other sizes and the owner's default adult chinook (50 cm), before the region's 65.
    A page with sizes but no adult chinook length (5B) gets the default region-wide."""
    from pipeline.regs.dfo_salmon import quota as Q
    six = [{"id": "6:min_size:a", "k": "min_size", "t": "…", "sp": ["CH"], "lengths": [{"min_cm": 30}]},
           H.classify('An "adult chinook" in Region 6 (other than Fraser River Watershed) is defined as being over 65 cm'
                      ' measured from the tip of the nose to tail fork') | {"id": "6:adult:b"}]
    assert six[1]["x"] == [{"n": "Fraser River", "ws": True, "key": "fraser river+ws"}]
    used: set = set()
    got = Q.sizes(six, used)
    assert got == [{"sp": ["CH"], "min": {"min_cm": 30}, "adult": {"min_cm": 50}, "w": ["fraser river+ws"]},
                   {"sp": ["CH"], "min": {"min_cm": 30}, "adult": {"min_cm": 65}}]
    assert used == {"6:min_size:a", "6:adult:b"}
    five_b = [{"id": "5b:min_size:a", "k": "min_size", "t": "…", "sp": ["CH", "CO"], "lengths": [{"min_cm": 30}]}]
    assert Q.sizes(five_b) == [{"sp": ["CH"], "min": {"min_cm": 30}, "adult": {"min_cm": 50}},
                               {"sp": ["CO"], "min": {"min_cm": 30}}]


def test_t343_every_header_note_is_folded_dropped_or_left_as_words():
    """(T343) `header_answers` marks each note said elsewhere: the daily / possession / annual limits by the bucket (its
    total says "fresh and tidal waters combined"), the sizes by the sizes line, the closure by the closure; the 100 m
    facility rule, how to measure and R1's stream bait ban (the province's own) are dropped; daylight is the page's
    `hours` (solar, DFO's definition — R2 prints none: 1 hour either side, owner). Only the eulachon reminder stays as words."""
    from pipeline.deliver.salmon import header_answers
    folds = {}
    for slug in T356_SLUGS:
        got = header_answers(H.parse_header((T356_FIXTURE.parent / f"region{slug}-eng.html").read_text(encoding="utf-8"), slug))
        if got.get("bucket"):
            assert got["bucket"]["tree"]["says"].endswith("salmon a day in total, fresh and tidal waters combined")
        for n in got["notes"]:
            folds.setdefault(n["k"], set()).add(n.get("fold"))
        if slug == "2":
            assert got["hours"]["says"] == ("Salmon fishing in daylight only: one hour before sunrise to one hour after "
                                            "sunset")          # R2 prints no definition: the usual hour (owner)
        if slug == "8":
            assert got["hours"]["says"] == ("Salmon fishing in daylight only: one hour before sunrise to one hour after "
                                            "sunset")
    assert folds.pop("facility") == folds.pop("measure", {"drop"}) == {"drop"}
    assert folds.pop("closed") == {"closure"}
    assert {k: v for k, v in folds.items() if k in ("daily", "possession", "annual", "monthly")} == {
        k: {"bucket"} for k in ("daily", "possession", "annual", "monthly") if k in folds}
    assert folds.pop("min_size") == folds.pop("adult") == folds.pop("jack") == {"sizes"}
    assert folds.get("aggregate", {"shared"}) == {"shared"}
    assert folds.pop("daylight") == {"hours"} and folds.pop("bait_ban") == {"drop"}
    assert folds.get("other", {None}) == {None}


T356_FIXTURE = Path(__file__).parent / "fixtures" / "dfo_salmon" / "t356_header.json"
T356_SLUGS = ("1", "2", "4", "5a", "5b", "6", "7", "8")


def t356_header() -> dict:
    """(T356) per fixture page: the header notes and what they resolve to (`deliver.salmon.header_answers`) — the feeds
    worker's port (`app/workers/feeds/test/dfo.quota.test.ts`) is held to it."""
    from pipeline.deliver.salmon import header_answers
    out = {}
    for slug in T356_SLUGS:
        notes = H.parse_header((T356_FIXTURE.parent / f"region{slug}-eng.html").read_text(encoding="utf-8"), slug)
        out[slug] = {"notes": notes, **header_answers(notes)}
    return out


def test_the_worker_header_fixture_is_current():
    want = json.dumps(t356_header(), indent=1, sort_keys=True, ensure_ascii=False) + "\n"
    assert T356_FIXTURE.read_text(encoding="utf-8") == want, \
        "regenerate: python -c 'from pipeline.tests.test_dfo_header import *; import json; " \
        "T356_FIXTURE.write_text(json.dumps(t356_header(), indent=1, sort_keys=True, ensure_ascii=False) + \"\\n\")'"
