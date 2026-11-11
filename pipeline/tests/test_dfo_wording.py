"""T321: DFO's limits wording as a closed registry (`pipeline/regs/dfo_salmon/wording.py`,
`data/curated/dfo_salmon/wording.json`) — no network, no model.

  * every saved page (tracked fixtures; the cache's raw pages and history where present) reads with ZERO unknown clauses;
  * the registry: one entry per template, a known kind and status, a structure entry names what reads it, and the
    builder keeps every entry's meaning and status (a rebuild never drops or re-guesses one);
  * the three wordings that passed T320 silently: the hours in words are structure (solar hours on the rule), "in the
    aggregate" and "(see exception below)" are notes the card shows in DFO's words;
  * THE MUTATION: an unseen clause in a known cell flags the row (review_reason "new wording", the verdict `u`, no
    number), the location's card rows carry it, and the live run tells the owner once per template, never again.

When the live feed emails a new wording: read DFO's words, add the template to the registry, and add a line to
`test_known_wordings_read` below (the cell, what it must read to).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.deliver import salmon as S
from pipeline.regs.dfo_salmon import live as L
from pipeline.regs.dfo_salmon import quota as Q
from pipeline.regs.dfo_salmon import wording as W
from pipeline.regs.dfo_salmon.typed import to_rules

FIX = Path(__file__).parent / "fixtures" / "dfo_salmon"
AT = "2026-10-31T18:00:00Z"


def _rules(cell: str, species: str = "Coho", dates: str = "Sep 1 to Oct 31") -> list:
    return [r.model_dump(mode="json", exclude_defaults=True, by_alias=True)
            for r in to_rules({"species": species, "dates": dates, "limits_gear": cell}, "x", 0)]


def _open_verdict(rules: list) -> dict:
    sp = next(r["species"][0] for r in rules if r.get("species"))
    for md in S.DAYS:
        v = S.verdict_on([rules], sp, md)
        if v and v["s"] == "open":
            return v
    raise AssertionError("opens no day")


# ── the corpus ───────────────────────────────────────────────────────────────────────────────────────────────────────

def test_every_saved_page_reads_with_zero_unknown_clauses():
    pages = W.saved_pages()
    assert sum(1 for p, _ in pages if p.parent == FIX) == 12               # the tracked fixtures, 5 (an index stub) left out
    # (T549 item 6) a clause is unknown until the owner ACCEPTS its entry: the only unknown clauses of the saved pages
    # are the ones whose entry is still a proposal (never a clause with no entry at all)
    reg = W.registry()
    proposed = {t for t, e in reg.items() if e["status"] == "proposed"}
    bad = [(name, r.limits_gear, c) for name, r in W.saved_rows(pages) for c in W.unknown(r.limits_gear)
           if W.template(c) not in proposed]
    assert bad == []
    assert {W.template(c) for _, r in W.saved_rows(pages) for c in W.unknown(r.limits_gear)} <= proposed


def test_the_registry_is_one_entry_per_template_each_with_a_meaning():
    doc = json.loads(W.REGISTRY.read_text(encoding="utf-8"))
    assert doc["format"] == W.FORMAT
    ts = [e["template"] for e in doc["entries"]]
    assert len(ts) == len(set(ts)) and len(ts) >= 78       # the 2026-10-31 baseline: 85; T332's split tidy: 78
    for e in doc["entries"]:
        assert e["kind"] in W.KINDS and e["status"] in W.STATUSES, e
        assert W.template(e["example"]) == e["template"], e                  # the key is the example's template
        if e["kind"] == "structure":
            assert e["reads"], f"{e['template']!r}: structure, but nothing reads it"


def test_the_builder_keeps_every_entrys_meaning(tmp_path):
    reg = tmp_path / "wording.json"
    reg.write_text(W.REGISTRY.read_text(encoding="utf-8"), encoding="utf-8")
    before = {e["template"]: e for e in json.loads(reg.read_text())["entries"]}
    got = W.build(reg, pages=[(FIX / "region2-eng.html", "2")])               # one page: most templates unseen
    after = {e["template"]: e for e in got["entries"]}
    assert set(after) == set(before)
    for t, e in before.items():
        assert {k: after[t][k] for k in ("kind", "reads", "status")} == {k: e[k] for k in ("kind", "reads", "status")}
    W.write(got, reg)
    assert json.loads(reg.read_text()) == got                                # the one-entry-a-line writer round-trips


def test_a_rejected_entry_is_unknown():
    reg = {t: dict(e) for t, e in W.registry().items()}
    assert W.unknown("4 per day, bait ban", reg) == []
    reg["bait ban"]["status"] = "rejected"
    assert W.unknown("4 per day, bait ban", reg) == ["bait ban"]


def test_a_proposed_entry_is_unknown_until_the_owner_accepts_it():
    """T549 item 6: `build` adds every clause DFO newly prints as `proposed`, its meaning GUESSED by regex. Read as
    known, running the builder cleared the "new wording: no number + urgent email" guard before anyone read the clause."""
    reg = {t: dict(e) for t, e in W.registry().items()}
    assert reg["bait ban"]["status"] == "accepted" and W.unknown("4 per day, bait ban", reg) == []
    reg["bait ban"]["status"] = "proposed"
    assert W.unknown("4 per day, bait ban", reg) == ["bait ban"]
    assert W.reason("4 per day, bait ban", reg) == "new wording: 'bait ban'"
    assert W.notes("4 per day, bait ban", reg) == []
    # the one entry the owner has not ticked (T332) is unknown today, in the file both readers load
    cell = "4 per day, only 1 over 65 cm. No fishing for chinook from a point 100 m above Fort Babine bridge to Nichyeskwa Creek."
    assert W.unknown(cell) == ["No fishing for chinook from a point 100 m above Fort Babine bridge to Nichyeskwa Creek."]


def test_a_template_the_builder_adds_does_not_clear_the_guard(tmp_path):
    """T549 item 6, end to end: a page prints a clause the registry never held; `build` adds it as a proposal; the cell
    is STILL new wording when read with the rebuilt registry."""
    reg = tmp_path / "wording.json"
    doc = json.loads(W.REGISTRY.read_text(encoding="utf-8"))
    gone = next(e for e in doc["entries"] if e["status"] == "accepted" and e["template"] == "bait ban")
    doc["entries"] = [e for e in doc["entries"] if e is not gone]
    W.write(doc, reg)
    before = W.registry(reg)
    assert "bait ban" not in before and W.unknown("4 per day, bait ban", before) == ["bait ban"]
    built = W.build(reg)                                                      # every saved page: "bait ban" is printed on them
    added = next(e for e in built["entries"] if e["template"] == "bait ban")
    assert added["status"] == "proposed"
    W.write(built, reg)
    assert W.unknown("4 per day, bait ban", W.registry(reg)) == ["bait ban"]


# ── T332: the split tidied, every entry checked ───────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("cell,want", [
    # a lettered section title is no clause break: Region 6's section F is a watershed SCOPE, one note
    ("F. Fraser River Watershed - There is no fishing for salmon in the portions of the Fraser River watershed in Region 6.",
     ["F. Fraser River Watershed - There is no fishing for salmon in the portions of the Fraser River watershed in Region 6."]),
    ("No fishing for sockeye, pink, chum.", ["No fishing for sockeye, pink, chum."]),            # a species list
    ("No fishing for sockeye, pink or chum salmon, unless otherwise stated below",
     ["No fishing for sockeye, pink or chum salmon", "unless otherwise stated below"]),
    ("Single, barbless hook in tidal and non-tidal portions of all streams.",                    # DFO's comma
     ["Single, barbless hook in tidal and non-tidal portions of all streams."]),
    ("2 per day, 1 of which may be more than 77 cm. in length",                                  # no break before lower case
     ["2 per day", "1 of which may be more than 77 cm. in length"]),
    ("No fishing for chinook. — FN0372", ["No fishing for chinook", "FN0372"]),                  # leading dashes
    ("4 per day, only 1 over 65 cm. Monthly limit=1 over 65 cm. — — Adult chinook caught",
     ["4 per day", "only 1 over 65 cm", "Monthly limit=1 over 65 cm", "Adult chinook caught"]),
])
def test_the_clause_split_makes_no_scraps(cell, want):
    assert W.clauses(cell) == want


def test_no_scrap_is_a_registry_entry_and_every_checked_entry_is_accepted():
    reg = W.registry()
    for scrap in ("f", "fraser river watershed", "in length", "single", "pink", "chum", "sockeye", "—", "— fn#"):
        assert scrap not in reg
    # T332 (owner 2026-10-30): accepted after a check against real cells; the one doubtful entry stays a proposal
    assert {t for t, e in reg.items() if e["status"] != "accepted"} == \
        {"no fishing for chinook from a point n m above fort babine bridge to nichyeskwa creek"}
    assert all("watershed" not in t or e["kind"] == "note" for t, e in reg.items())   # a watershed is a scope, never a limit


# ── the three silent gaps ────────────────────────────────────────────────────────────────────────────────────────────

def test_hours_in_words_are_structure():
    cell = "4 per day, one hour before sunrise to one hour after sunset — FN0853"
    rules = _rules(cell, "Pink", "Aug 22 to Sep 30")
    assert rules[0]["when"]["hours"] == {"start": {"solar": "sunrise", "offset_min": -60},
                                          "end": {"solar": "sunset", "offset_min": 60}}
    assert not any(r.get("review_reason") for r in rules)
    v = _open_verdict(rules)
    assert Q.words(v["tree"]) == "4 a day" and "u" not in v and "one hour before sunrise to one hour after sunset" in v["t"]
    got = Q.read_limits(cell)
    assert got["hours"] == ["one hour before sunrise", "one hour after sunset"] and not got["residue"]
    assert Q.numbers(Q.read_limits(Q.render(got))) == Q.numbers(got)          # the round trip
    assert W.row_words({"species": "Pink", "dates": "", "limits_gear": cell}) is None


@pytest.mark.parametrize("cell", ["4 coho per day in the aggregate, must be hatchery marked, only 2 of which may be "
                                  "greater than 40 cm — FN1281",
                                  "4 per day, only 1 over 65 cm (See exception below).",
                                  "No fishing for coho. (see exception below)"])
def test_aggregate_and_see_exception_are_notes_in_dfos_words(cell):
    w = W.row_words({"species": "Coho", "dates": "Oct 1 to Dec 31", "limits_gear": cell})
    assert w == {"k": "note", "sp": "Coho", "d": "Oct 1 to Dec 31", "t": cell}
    assert not any(r.get("review_reason") for r in _rules(cell))              # the numbers are still read


# ── the mutation ─────────────────────────────────────────────────────────────────────────────────────────────────────

NEW = "fish from shore on Sundays"


def test_an_unseen_clause_in_a_known_cell_is_flagged_never_read():
    cell = f"4 per day, only 2 over 50 cm, {NEW}"
    assert W.unknown(cell) == [NEW]
    rules = _rules(cell)
    assert rules[0]["review_reason"] == f"new wording: {NEW!r}"
    assert _open_verdict(rules)["u"] == 1                              # no number: the app shows DFO's words
    assert W.row_words({"species": "Coho", "dates": "", "limits_gear": cell}) == \
        {"k": "new", "sp": "Coho", "d": "", "t": cell, "new": [NEW]}
    gear = _rules(f"Bait ban, {NEW}")                                         # no limit rule: the gear rule carries it
    assert [r["review_reason"] for r in gear] == [f"new wording: {NEW!r}"]


def _region8(mutate: bool) -> str:
    html = (FIX / "region8-eng.html").read_text(encoding="utf-8")
    old = "<td>4 per day, only 2 over 50&nbsp;cm.</td>"
    assert old in html
    return html.replace(old, f"<td>4 per day, only 2 over 50&nbsp;cm, {NEW}.</td>") if mutate else html


class _Sender:
    def __init__(self):
        self.sent: list = []

    def __call__(self, url, text):
        self.sent.append(text)


def test_the_live_run_flags_the_row_and_tells_the_owner_once(tmp_path):
    clean = L.run(tmp_path / "a", fetcher=lambda s: (_region8(False), AT), regions=["8"], now=AT,
                  notify_url="http://n", sender=_Sender())
    assert not any(c.get("x") for c in clean["index"]["locs"].values())
    assert not any(n["key"].startswith("dfo-wording:") for n in clean["notices"])

    s = _Sender()
    got = L.run(tmp_path / "b", fetcher=lambda s: (_region8(True), AT), regions=["8"], now=AT,
                notify_url="http://n", sender=s)
    keys = [n["key"] for n in got["notices"] if n["key"].startswith("dfo-wording:")]
    assert keys == [f"dfo-wording:{W.template(NEW)}"]
    assert any(NEW in t and "data/curated/dfo_salmon/wording.json" in t for t in s.sent)
    flagged = {lid: c["x"] for lid, c in got["index"]["locs"].items() if c.get("x")}
    assert flagged                                                            # the places the row is printed for (Mabel Lake: 2)
    idx = got["index"]
    for lid, rows in flagged.items():
        assert [r["k"] for r in rows] == ["new"] and NEW in rows[0]["t"]
        assert any(idx["verdicts"][v].get("u") == 1 for runs in idx["locs"][lid]["sp"] for v in runs[1::2])

    s2 = _Sender()                                                            # the next run: told already
    L.run(tmp_path / "b", fetcher=lambda s: (_region8(True), AT), regions=["8"], now="2026-11-01T00:00:00Z",
          notify_url="http://n", sender=s2)
    assert not any(NEW in t for t in s2.sent)


# ── known wordings: one line each (add one when a new wording is registered) ────────────────────────────────────────

@pytest.mark.parametrize("cell,daily", [
    ("2 per day, 1 of which can be unmarked — FN1013", 2),
    ("Hook restriction, single barbless hook less than 15mm from point to shank", None),
    ("No fishing for sockeye, pink or chum salmon, unless otherwise stated below", 0),
])
def test_known_wordings_read(cell, daily):
    assert W.unknown(cell) == []
    assert Q.read_limits(cell)["daily"] == daily
