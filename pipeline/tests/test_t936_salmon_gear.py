"""T936: the build WARNING for a water whose salmon are open on a game-fish-closed day that carries a B.C. gear rule
(`pipeline.deliver.release.salmon_gear`). One synthetic store + salmon payload, built here: only the stretch closed by
species closures alone, under a fishable salmon chain, with a bait ban, is listed."""
from __future__ import annotations

import json
import sqlite3

from pipeline.deliver import types as T
from pipeline.deliver.release import salmon_gear as G

GAME = list(T.GAME_FISH)
HALF = len(GAME) // 2
OPEN = {"status": "own", "closing": []}
#: two species closures that together close every game fish (no single full closure)
SPECIES_CLOSED = {"status": "closed", "closing": [[1, GAME[:HALF]], [2, GAME[HALF:]]], "closers": [1, 2], "strip": 1}
#: one rule closing every game fish: a full closure (salmon close too, RULINGS L25)
FULL_CLOSED = {"status": "closed", "closing": [[3, GAME]], "closers": [3], "strip": 1}
NO_GEAR, BAIT_BAN = {}, {"bait_ban": True}
FLY = {"fly": "fly_fishing_only"}


class FakeStore:
    """The few tables of a regs store `salmon_gear` reads, JSON frames held as text."""

    def __init__(self, keys, sections, items=(), parts=()):
        self.con = sqlite3.connect(":memory:")
        self.con.executescript("CREATE TABLE display_frame (id INTEGER PRIMARY KEY, j TEXT);"
                               "CREATE TABLE gear_frame (id INTEGER PRIMARY KEY, j TEXT);"
                               "CREATE TABLE seglist (seg INTEGER PRIMARY KEY, starts TEXT);"
                               "CREATE TABLE akey (akey INTEGER PRIMARY KEY, seg INTEGER);"
                               "CREATE TABLE cell (akey INTEGER, s INTEGER, display INTEGER, gear INTEGER);"
                               "CREATE TABLE str (id INTEGER PRIMARY KEY, text TEXT);"
                               "CREATE TABLE item (item_ord INTEGER, item_id INTEGER, name TEXT);"
                               "CREATE TABLE part (item_ord INTEGER, part_ix INTEGER, label_s INTEGER);")
        frames = {"display_frame": [], "gear_frame": []}

        def ix(tbl, v):
            j = json.dumps(v, sort_keys=True)
            if j not in frames[tbl]:
                frames[tbl].append(j)
            return frames[tbl].index(j)
        for a, (starts, cells) in keys.items():
            self.con.execute("INSERT INTO seglist VALUES (?, ?)", (a, json.dumps(starts)))
            self.con.execute("INSERT INTO akey VALUES (?, ?)", (a, a))
            for s, (d, g) in enumerate(cells):
                self.con.execute("INSERT INTO cell VALUES (?, ?, ?, ?)", (a, s, ix("display_frame", d), ix("gear_frame", g)))
        for tbl, js in frames.items():
            self.con.executemany(f"INSERT INTO {tbl} VALUES (?, ?)", list(enumerate(js)))
        strs = []
        for o, wid, name in items:
            strs.append(wid)
            self.con.execute("INSERT INTO item VALUES (?, ?, ?)", (o, len(strs) - 1, name))
        for o, p, label in parts:
            strs.append(label)
            self.con.execute("INSERT INTO part VALUES (?, ?, ?)", (o, p, len(strs) - 1))
        self.con.executemany("INSERT INTO str VALUES (?, ?)", list(enumerate(strs)))
        self._akeys = dict(sections)

    def val(self, tbl, j):
        return json.loads(j)

    def s(self, i):
        return None if i is None else self.con.execute("SELECT text FROM str WHERE id = ?", (i,)).fetchone()[0]

    def section_akeys(self):
        return dict(self._akeys)

    def unnamed_name(self, sid):
        return "Unnamed stream · Region 2"


def _payload():
    # verdict 0 closed (DFO's not-opened), 1 open; chain 0 closed every day, chain 1 coho open day index 149..199
    shut = [0, 0]
    return {"verdicts": [{"s": "closed", "d": 1}, {"s": "open"}],
            "chains": [{"sp": [shut] * 5}, {"sp": [shut, [0, 0, 149, 1, 200, 0], shut, shut, shut]}]}


def _world():
    # every akey: segment 1 (Jan 1-Apr 8) open, segment 2 (from day 100, Apr 9) closed as named, with the gear named
    keys = {1: ([1, 100], [(OPEN, BAIT_BAN), (SPECIES_CLOSED, BAIT_BAN)]),      # the case
            2: ([1, 100], [(OPEN, BAIT_BAN), (FULL_CLOSED, BAIT_BAN)]),         # a full closure: salmon closed too
            3: ([1, 100], [(OPEN, NO_GEAR), (SPECIES_CLOSED, NO_GEAR)]),        # no B.C. gear rule
            4: ([1, 100], [(OPEN, FLY), (SPECIES_CLOSED, FLY)])}                # unnamed, fly fishing only
    sections = [(10, 1), (11, 1), (12, 2), (13, 3), (14, 4)]
    salmon = [(10, 1), (11, 0), (12, 1), (13, 1), (14, 1)]
    store = FakeStore(keys, sections, items=[(0, "gnis:1", "Test River")], parts=[(0, 2, "below the falls")])
    return store, salmon, (lambda: {10: (0, 2), 11: (0, 2), 12: (0, 2), 13: (0, 2)})


def test_only_species_closed_salmon_open_gear_days_are_listed():
    store, salmon, parts = _world()
    found = G.rows(store, _payload(), salmon, parts)
    assert found == [
        {"water": "Test River", "item_id": "gnis:1", "part": 2, "label": "below the falls", "sections": 1,
         "example_sid": 10, "days": "May 29-Jul 18", "n_days": 51, "species": ["CO"], "gear": ["bait ban"]},
        {"water": "Unnamed stream · Region 2", "item_id": None, "part": None, "label": None, "sections": 1,
         "example_sid": 14, "days": "May 29-Jul 18", "n_days": 51, "species": ["CO"], "gear": ["fly fishing only"]}]
    assert G.say(found, 1) == (
        "WARNING (T936): 2 stretch(es) where game fish are closed by species closures (no full closure), DFO has salmon "
        "open or release, and a B.C. bait ban / fly rule holds — the salmon bar draws no gear mark on those days (2 "
        "sections). First 1:\n"
        "  Test River part 2 (below the falls) [gnis:1]: May 29-Jul 18; salmon CO open or release; B.C. bait ban; "
        "1 section(s), e.g. 10")


def test_none_is_said_and_a_day_open_at_some_moment_is_not_closed():
    assert G.say([]) == "T936 salmon-open, gear-hidden days: none"
    # two moments: one closed by species closures, one open (a night closure never closes the day)
    assert G.day_gear([SPECIES_CLOSED, OPEN], [BAIT_BAN, BAIT_BAN], GAME) is None
    assert G.day_gear([SPECIES_CLOSED, SPECIES_CLOSED], [BAIT_BAN, FLY], GAME) == ("bait ban", "fly fishing only")
    # a full closure at every moment is no case; at one moment only, the other moment's species closures leave it one
    assert G.day_gear([FULL_CLOSED, FULL_CLOSED], [BAIT_BAN, BAIT_BAN], GAME) is None
    assert G.day_gear([FULL_CLOSED, SPECIES_CLOSED], [BAIT_BAN, BAIT_BAN], GAME) == ("bait ban",)


def test_the_full_list_is_written(tmp_path):
    store, salmon, parts = _world()
    found = G.rows(store, _payload(), salmon, parts)
    js, txt = G.write(found, tmp_path / "r-test")
    assert json.loads(js.read_text())["count"] == 2
    assert txt.read_text().splitlines()[0].startswith("WARNING (T936): 2 stretch(es)")
