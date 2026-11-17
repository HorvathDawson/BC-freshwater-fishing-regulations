"""T796 A5 (owner 2026-11-16, question 29) — a stretch the rule book closes to all fishing reads closed for salmon too.

Book p.77: "when fresh waters are closed to fishing … the regulations apply to fishing for salmon as well". The
release puts the book's full closure FIRST in the salmon chains of the sections it closes: a static place
(`p[id].static`, `p[id].book`), its own year in payload `static`, closed for every salmon on the days the stored
verdicts say the rule closes every game fish. Each test builds its own small data.
"""
from __future__ import annotations

from types import SimpleNamespace

from pipeline.deliver import salmon as S
from pipeline.deliver import types as T
from pipeline.regs.dfo_salmon import live as L

RULE = "r1:cowichan@1::cowichan_river.r6"
WORDS = {RULE: {"verbatim": "No Fishing downstream of the CNR Mile 66 Trestle between Sept 1-Nov 15", "pages": [15],
                "name": "COWICHAN RIVER", "region": "1"}}
SEP_1, NOV_15 = S.DAYS.index(901), S.DAYS.index(1115)
CLOSED_DAYS = frozenset(range(SEP_1, NOV_15 + 1))


def _built():
    """One DFO place, 'catch and release' for coho all year, on two sections."""
    loc = SimpleNamespace(location_id="1:cowichan:66-mile", precedence=3, fingerprints=["fp"], water="Cowichan River",
                          water_id="1:cowichan", source_text={"specific_area": "66 Mile Trestle"}, tidal_water=False,
                          region_number=1)
    rule = {"rule_id": "fp.r0", "type": "retention_limit", "verbatim": "Open for salmon catch and release",
            "species": ["SA"], "take": 0, "may_target": True}
    feed = {"locators": [{"fingerprint": "fp", "rules": [rule]}]}
    placed = [{"location_id": loc.location_id, "mapped": True, "sections": ["s1", "s2"]}]
    ef = SimpleNamespace(region_name="R1", locations=[loc], water=lambda wid: None)
    return S.build({"1": ef}, {"1": feed}, placed, {"s1": 1, "s2": 2}, section_regions=None), rule


def _at(runs, day):
    from pipeline.deliver.salmon_who import _run_at
    return _run_at(runs, day)


def test_the_book_closure_answers_first_on_its_days():
    """Cowichan below the trestle, Oct 10: DFO's catch and release gives way to the book's closure, named; Jul 15 DFO's
    answer stands. The other section keeps its chain. MUTATION: put the book place LAST and Oct 10 reads release."""
    (payload, sections), _ = _built()
    out, secs = S.with_book_closures(payload, sections, {2: {RULE: CLOSED_DAYS}}, WORDS)
    lid = f"book:{RULE}"
    ch = out["chains"][dict(secs)[2]]
    assert ch["l"] == [lid, "1:cowichan:66-mile"]
    co = S.SPECIES.index("CO")
    oct10, jul15 = S.DAYS.index(1010), S.DAYS.index(715)
    v = out["verdicts"][_at(ch["sp"][co], oct10)]
    assert v == {"s": "closed", "t": "Closed to all fishing by the rule book (p.15): No Fishing downstream of the CNR "
                                     "Mile 66 Trestle between Sept 1-Nov 15 (Sep 1–Nov 15)"}
    assert out["verdicts"][_at(ch["sp"][co], jul15)]["s"] == "release"
    assert (_at(ch["a"][co], oct10), _at(ch["a"][co], jul15)) == (0, 1)
    assert S.MAP_CODES[_at(ch["m"], oct10)] == "closed" and S.MAP_CODES[_at(ch["m"], jul15)] == "release"
    assert dict(secs)[1] == dict(sections)[1], "a section the book does not close keeps its chain"
    assert out["p"][lid] == {"r": "1", "water": "COWICHAN RIVER", "static": 1, "book": {"rule": RULE, "pages": [15]}}


def test_a_chain_dfo_closes_every_day_takes_the_book_place_too(monkeypatch):
    """(Owner 2026-11-16: "Raise to 100 kB") the book place joins EVERY DFO-listed chain it closes, a chain DFO closes
    every day included — so it still answers when the live feed opens that chain in season. The old size policy
    (`BOOK_ONLY_WHERE_DFO_LETS_FISH` True) left such a chain out. MUTATION: the policy back to True and the shut chain
    has no book place."""
    (payload, sections), _ = _built()
    shut = {"s": "closed", "t": "No fishing for salmon"}
    payload = {**payload, "verdicts": [*payload["verdicts"], shut]}
    ix = len(payload["verdicts"]) - 1
    payload["chains"] = [*payload["chains"][:1],
                         *({**c, "sp": [[0, ix]] * len(S.SPECIES)} for c in payload["chains"][1:])]
    assert S.BOOK_ONLY_WHERE_DFO_LETS_FISH is False
    out, secs = S.with_book_closures(payload, sections, {2: {RULE: CLOSED_DAYS}}, WORDS)
    assert out["chains"][dict(secs)[2]]["l"][0] == f"book:{RULE}"
    monkeypatch.setattr(S, "BOOK_ONLY_WHERE_DFO_LETS_FISH", True)
    out, secs = S.with_book_closures(payload, sections, {2: {RULE: CLOSED_DAYS}}, WORDS)
    assert dict(secs) == dict(sections) and "static" not in out


def test_the_salmon_head_gate_is_the_owners_100_kb():
    from pipeline.deliver.release.build import SIZE_GATES
    assert SIZE_GATES["salmon"] == 100_000


def test_the_owners_raises_for_the_book_closures():
    """(owner 2026-11-16 ~14:55, RULINGS O10) salmon-6 and the web's first taps raised; the unnamed tap unchanged."""
    from pipeline.deliver.release.build import SIZE_GATES
    from pipeline.deliver.release.contract import WEB_BUDGET
    assert SIZE_GATES["salmon-6"] == 460_000
    assert (WEB_BUDGET["first_tap"]["gate"], WEB_BUDGET["first_tap_low"]["gate"],
            WEB_BUDGET["first_tap_unnamed"]["gate"]) == (4_400_000, 3_600_000, 6_200_000)


def test_the_static_year_composes_like_the_live_feed():
    """Under a live feed the app composes the chain from each place's year: the book's (payload `static`) first, then
    DFO's. `live.compose` over those years gives the release's chain."""
    (payload, sections), rule = _built()
    out, secs = S.with_book_closures(payload, sections, {2: {RULE: CLOSED_DAYS}}, WORDS)
    ch = out["chains"][dict(secs)[2]]
    v = S._Interner()
    for x in out["verdicts"]:
        v(x)
    g = S._Interner()
    locs = {**out["static"], "1:cowichan:66-mile": L.location_calendar([rule], v, g)}
    got = L.compose(ch["l"], locs, v.items)
    assert got["sp"] == ch["sp"] and got["m"] == ch["m"]


class _Store:
    """A verdict store of one rule key with `moments` moments: reading 1 (closed) from Sep 1, reading 0 before."""

    def __init__(self, moments=1, fish_of=None, open_moment=None):
        self.rule_ids = [RULE, "zp:x::spring.r1"]
        self.grade = {0: "full", 1: "full"}
        self._n, self._open = moments, open_moment
        self._fish = fish_of or {0: T.GAME_FISH}

    def moments(self, key):
        return [None] * self._n

    def reading_of(self, key, day, moment=None):
        if moment is not None and moment == self._open:
            return 0
        return 1 if S.DAYS.index(901) + 1 <= day <= S.DAYS.index(1115) + 1 else 0

    def closed(self, key, reading):
        return reading == 1

    def verdict_id(self, key, reading, fish, origin):
        return (reading, fish)

    def rows(self, verdict):
        reading, fish = verdict
        speaks = T.code(T.RuleState.speaks)
        return tuple((r, speaks, None, None, ()) for r, fs in sorted(self._fish.items()) if reading and fish in fs)


def test_only_a_rule_closing_every_game_fish_by_itself_counts():
    """A full closure of every game fish: its days. A rule closing some fish only (a species closure) never, even
    where other rules close the rest (the status index's CLOSED, but no single book closure)."""
    assert S.book_closures(_Store(), {7: 3}) == {7: {RULE: CLOSED_DAYS}}
    half = len(T.GAME_FISH) // 2
    split = _Store(fish_of={0: T.GAME_FISH[:half], 1: T.GAME_FISH[half:]})
    assert S.book_closures(split, {7: 3}) == {}
    assert S.book_closures(_Store(moments=2, open_moment=1), {7: 3}) == {}, "open at some moment: not the day"


def test_day_ranges_read_back_and_wrap_the_year():
    assert S._day_ranges(CLOSED_DAYS) == [{"from_month": 9, "from_day": 1, "to_month": 11, "to_day": 15}]
    wrap = set(range(S.DAYS.index(1115), len(S.DAYS))) | set(range(0, S.DAYS.index(415) + 1))
    assert S._day_ranges(wrap) == [{"from_month": 11, "from_day": 15, "to_month": 4, "to_day": 15}]
    rule = S.book_rule("book:x", RULE, wrap, WORDS)
    assert all(S.holds_on(rule, S.DAYS[d]) == (d in wrap) for d in range(len(S.DAYS)))
