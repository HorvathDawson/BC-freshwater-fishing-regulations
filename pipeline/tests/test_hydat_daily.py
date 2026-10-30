"""T274: the stored daily means keep Feb 29 in slot 59, flow only, null gaps, and the newest WINDOW_YEARS."""
import sqlite3

from pipeline.gauges.feed import hydat_daily as h


def _db(path):
    db = sqlite3.connect(path)
    cols = ",".join(f"FLOW{d} REAL" for d in range(1, 32))
    db.execute(f"CREATE TABLE DLY_FLOWS (STATION_NUMBER TEXT, YEAR INT, MONTH INT, {cols})")
    def row(st, y, m, vals):
        v = [vals.get(d) for d in range(1, 32)]
        db.execute(f"INSERT INTO DLY_FLOWS VALUES (?,?,?,{','.join('?' * 31)})", [st, y, m, *v])
    row("08AA001", 1996, 2, {29: 3.5})
    row("08AA001", 1997, 3, {1: 12345.678, 31: 1.0})
    row("08AA001", 1997, 4, {31: 9.0})            # HYDAT pads day 31 of a 30-day month: ignored
    db.commit()
    return db


def test_build(tmp_path):
    p = tmp_path / "h.sqlite3"
    _db(p).close()
    out = h.build(p, ["08AA001", "08ZZ999"], "R")
    assert set(out) == {"08AA001"}
    d = out["08AA001"]
    assert (d["from"], len(d["q"])) == (1996, 2)
    assert d["q"][0][59] == 3.5                       # 29 Feb 1996: slot 59
    assert d["q"][1][60] == 12350.0 and d["q"][1][90] == 1.0     # 1 Mar = slot 60 (4 sig. figs), 31 Mar = slot 90
    assert sum(v is not None for v in d["q"][1]) == 2


def test_index_stations_are_the_feed_index_keys(tmp_path):
    # (T301) the GitHub workflow has no local feed dir: it reads the staging feed's index.json
    import json
    p = tmp_path / "index.json"
    p.write_text(json.dumps({"fetchedAt": "x", "hydat": {}, "stations": {"08MH024": {}, "07EA004": {}}}))
    assert h.index_stations(p) == ["07EA004", "08MH024"]
