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
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline.deliver import salmon as S
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
    assert (by["annual"]["sp"], by["annual"]["cm"], by["annual"]["n"]) == (["CH"], 50, 10)
    assert by["measure"]["t"] == "Salmon measurements are taken from tip of nose to tail fork."
    agg = by["aggregate"]
    assert (agg["sp"], agg["n"], agg["hatchery"], agg["w"]) == \
        (["CO"], 1, True, [{"n": "Kispiox River", "ws": True, "key": "kispiox river+ws"}])
    assert all(n["id"].startswith("6:") for n in notes) and len({n["id"] for n in notes}) == len(notes)
    assert not any("maintenance" in n["t"] or "Learn more" in n["t"] for n in notes)


def test_the_adult_definition_and_its_exceptions():
    adult = [n for n in H.parse_header(HEADER, "6") if n["k"] == "adult"]
    region, kispiox, two = adult
    assert (region["cm"], "w" in region) == (50, False)
    assert kispiox["cm"] == 62 and [w["n"] for w in kispiox["w"]] == ["Kispiox River"]
    assert kispiox["on"] == [[244, 365]]                                             # Sep 1 .. Dec 31, leap calendar
    assert kispiox["t"].endswith("over 62 cm: the Kispiox River from Sep 1 to Dec 31")
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
    assert unresolved == [{"note": next(n["id"] for n in notes if "Bulkley" in n["t"]), "name": "Bulkley River"}]
    # two curated waters of one name: never picked between
    _, un = H.resolve(notes, _ef(("Kispiox River", ["gnis:1"]), ("Kispiox River", ["gnis:3"])))
    assert {"Kispiox River"} <= {u["name"] for u in un}


@pytest.mark.skipif(not glob.glob(str(ROOT / "cache/dfo_salmon/raw/region*-eng.html")), reason="no cached DFO pages")
def test_every_cached_region_page_parses_and_region_2_reads_as_printed():
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
    assert not any("header changed" in t for t in sent)
    changed = HEADER.replace("is 4. Individual", "is 2. Individual")
    two = L.run(tmp_path, fetcher=fetch(changed), regions=["6"], now="2026-10-28T06:00:00Z", notify_url="http://n",
                sender=lambda u, t: sent.append(t), entries_dir=tmp_path / "none")
    assert [n["n"] for n in two["index"]["regions"]["6"]["notes"] if n["k"] == "daily"] == [2]
    assert sum("header changed" in t for t in sent) == 1
    L.run(tmp_path, fetcher=fetch(changed), regions=["6"], now="2026-10-28T12:00:00Z", notify_url="http://n",
          sender=lambda u, t: sent.append(t), entries_dir=tmp_path / "none")
    assert sum("header changed" in t for t in sent) == 1
    # a failed fetch keeps the header
    three = L.run(tmp_path, fetcher=lambda s: (_ for _ in ()).throw(OSError("down")), regions=["6"],
                  now="2026-10-28T18:00:00Z", notify_url="http://n", sender=lambda u, t: sent.append(t),
                  entries_dir=tmp_path / "none")
    assert three["index"]["regions"]["6"]["notes"] == two["index"]["regions"]["6"]["notes"]
