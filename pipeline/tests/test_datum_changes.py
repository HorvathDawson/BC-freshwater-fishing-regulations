"""T1047: a gauge's new zero (datum). The curated change list, the remark proposals, and the one consumer.

Five stations report live levels on another zero than their HYDAT record (08HD006, 08EF001, 07FD001 by HYDAT remark;
08KH019, 08MH153 with none). `data/curated/gauges/datum_change_stations.json` holds them, every row `proposed`; only an
ACCEPTED row with an offset puts the record (band, recent years, rating) on today's zero (`climatology.build`), and
with none, clim.json and the publisher's files are byte-identical.

Mutation pins (2026-11-24, each run; the first test it fails):
  `datum.accepted` keeps a proposed row               -> test_proposed_rows_leave_clim_and_the_publisher_byte_identical
  `climatology._read` drops the offset                -> test_an_accepted_change_puts_the_record_on_todays_zero
  `climatology._daily` ignores the moves              -> test_an_accepted_change_puts_the_record_on_todays_zero (rating)
  `build` ignores `datum`                             -> test_an_accepted_change_puts_the_record_on_todays_zero
  `offset_before` uses `<=` (shifts the new day)      -> test_offset_before_counts_only_the_changes_after_the_day
  `parse` lets an accepted row through with no offset -> test_the_file_refuses_a_bad_row
  `check_remarks` skips the comparison                -> test_a_quoted_remark_must_be_the_records
  `the_offset` drops the sign of "drop in stage"      -> test_the_five_remarks_parse
  review fixes (2026-11-24, each run):
  the remark check raises for a proposed row / never raises -> test_a_quoted_remark_must_be_the_records
  the this-year guard removed                         -> test_an_accepted_change_inside_the_this_year_line_is_refused
  a moved station's levels back to `_sig4`            -> test_an_accepted_change_keeps_the_records_millimetres
  end-of-record date kept / AFTER not the next day / "datum is similar" a change / the datum-signed size unflipped
                                                      -> test_other_wordings_sort_and_parse (its 08EE003, 08KD006,
                                                         08NE087, 08GA026, 08MG014 rows)
  the owner's two (lane L16, 2026-11-24, each run; the first test it fails):
  `plain_line` sign flipped / source words swapped    -> test_the_plain_line_per_station
  `record_changes` drops the undo line                -> test_record_changes_lists_a_new_acceptance_and_an_undo_...
  `_check_decision` not called / a proposed row may carry one -> test_a_decided_row_says_who_when_what_was_asked_...
  `rows_at` answers [] for a file the commit lacks    -> test_rows_at_reads_the_file_as_committed
  `set_status` sets every row                         -> test_set_status_moves_one_row_only_and_undoes
  (release side, test_release_command.py) the checks do not print the lines / compare against no commit / ignore the
  live release's file                                 -> test_a_moved_gauge_level_record_is_its_own_line_...,
                                                         test_gauge_records_reads_the_datum_file_at_the_live_commit
"""

from __future__ import annotations

import ast
import json
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipeline.common.curated import SOURCE
from pipeline.gauges.feed import datum
from pipeline.gauges.feed import publish as pub
from pipeline.gauges.feed.climatology import build, level_record
from pipeline.tests import under_test
from pipeline.tests.conftest import need
from pipeline.tools.audit import datum_remarks as R

ROOT = Path(__file__).resolve().parents[2]
HD006_REMARK = ("The station was relocated on June 5, 2025, 1.5km downstream to the Sayward Road bridge- the original "
                "station location from establishment in 1956 to 1981. The datum from that period is preserved, with "
                "original benchmark elevations used (converted from feet to meters). This datum was not preserved when "
                "the station was relocated in 1981 and again in 2015 to the most recent location. The datum change "
                "results in a ~5m drop in stage on the 2025 relocation date.")
EF001_REMARK = ("Station moved approximately 1km upstream of original location on 2025-01-30, causing a 2.370m jump in "
                "stage due to change in elevation.")
FD001_REMARK = "STATION RELOCATED ON 2026-08-05."


# ------------------------------------------------------------------------------------------------ fixtures

def _hydat(path: Path, step_at: int | None = None, remarks: list[tuple] = ()) -> Path:
    """08ZZ001: daily flow and level 2010 to last year (the recent years `climatology` keeps), level = 4 + q/100
    (1 + q/100 before `step_at`, a datum shift in the record)."""
    db = sqlite3.connect(path)
    cols = ", ".join(f"{{c}}{d} REAL" for d in range(1, 32))
    for table, c in (("DLY_LEVELS", "LEVEL"), ("DLY_FLOWS", "FLOW")):
        db.execute(f"CREATE TABLE {table} (STATION_NUMBER TEXT, YEAR INT, MONTH INT, {cols.format(c=c)})")
    db.execute("CREATE TABLE STN_REMARKS (STATION_NUMBER TEXT, REMARK_TYPE_CODE INT, YEAR INT, REMARK_EN TEXT, REMARK_FR TEXT)")
    db.execute("CREATE TABLE STN_REMARK_CODES (REMARK_TYPE_CODE INT, REMARK_TYPE_EN TEXT, REMARK_TYPE_FR TEXT)")
    db.executemany("INSERT INTO STN_REMARK_CODES VALUES (?, ?, '')", [(2, "ANNUAL HYDROMETRIC"), (5, "HISTORICAL WATER LEVELS")])
    by_month: dict[tuple, dict] = {}
    d = date(2010, 1, 1)
    while d <= date(date.today().year - 1, 12, 31):
        q = 10.0 + (d.timetuple().tm_yday % 50)
        h = (1.0 if step_at and d.year < step_at else 4.0) + q / 100
        by_month.setdefault((d.year, d.month), {})[d.day] = (h, q)
        d += timedelta(days=1)
    for (y, m), days in by_month.items():
        for table, k in (("DLY_LEVELS", 0), ("DLY_FLOWS", 1)):
            vals = [days.get(i, (None, None))[k] for i in range(1, 32)]
            db.execute(f"INSERT INTO {table} VALUES (?, ?, ?, {', '.join('?' * 31)})", ["08ZZ001", y, m, *vals])
    db.executemany("INSERT INTO STN_REMARKS VALUES (?, ?, ?, ?, '')", list(remarks))
    db.commit()
    db.close()
    return path


DECIDED = {"by": "owner", "on": "2026-11-24", "asked": "shift it?", "words": "Yes"}


def _row(**kw) -> dict:
    """A change row; a decided (accepted / rejected) one carries a `decision` unless the test gives its own."""
    base = {"station": "08ZZ001", "effective": "2025-01-01", "offset_m": -3.0, "offset_from": "remark",
            "source": "owner", "status": "proposed", "note": "test"}
    row = {**base, **kw}
    if row["status"] in ("accepted", "rejected") and "decision" not in kw:
        row["decision"] = dict(DECIDED)
    return row


def _file(tmp_path: Path, rows: list[dict]) -> Path:
    p = tmp_path / "datum_change_stations.json"
    p.write_text(json.dumps({"_about": "test", "changes": rows}), encoding="utf-8")
    return p


def _dump(clim: dict) -> str:
    return json.dumps(clim, separators=(",", ":"))       # what `climatology.main` writes


NOW = datetime(2026, 11, 17, 16, 0, tzinfo=timezone.utc)
AT = "2026-11-17T07:55:00-08:00"


def _publish(out: Path, monkeypatch, clim: dict, reading: tuple[float, float]) -> dict[str, bytes]:
    """The Python publisher on one live 08ZZ001 reading (no network), every file it writes, by name."""
    monkeypatch.setattr(pub, "fetch_station", lambda st: {
        "station": st, "at": AT, "discharge": reading[0], "level": reading[1], "recent": [[AT, reading[1], reading[0]]]})
    monkeypatch.setattr(pub, "fetch_temperatures", lambda stations, hours=24: {})
    monkeypatch.setattr(pub, "_get", lambda url: None)
    monkeypatch.setattr(pub, "_sleep", lambda s: None)
    pub.publish(out, ["08ZZ001"], clim=clim, now=NOW)
    return {p.name: p.read_bytes() for p in sorted(out.iterdir()) if p.is_file()}


# ------------------------------------------------------------------------------------------------ the consumer

def test_proposed_rows_leave_clim_and_the_publisher_byte_identical(tmp_path, monkeypatch):
    hy = _hydat(tmp_path / "hydat.sqlite3")
    rows = datum.parse({"_about": "t", "changes": [_row(), _row(station="08ZZ002", effective="2024-01-01"),
                                                   _row(effective="2023-01-01", status="rejected")]})
    plain, held = build(hy, ["08ZZ001"], "t"), build(hy, ["08ZZ001"], "t", datum.accepted(rows))
    assert datum.accepted(rows) == {} and _dump(held) == _dump(plain)
    # the publisher's every file, on a live level read on the new zero (3 m under the record): left out both ways
    a = _publish(tmp_path / "a", monkeypatch, plain, (30.0, 1.3))
    b = _publish(tmp_path / "b", monkeypatch, held, (30.0, 1.3))
    assert a == b and json.loads(a["index.json"])["stations"]["08ZZ001"]["reports"] == ["discharge"]


def test_the_real_rows_held_as_proposals_leave_clim_byte_identical(tmp_path):
    """The live file's rows, every one as a proposal, on a fixture carrying their station ids: nothing moves."""
    rows = [dict(r, status="proposed") for r in datum.load()]
    assert {r["station"] for r in rows} >= {"08HD006", "08EF001", "07FD001", "08KH019", "08MH153"}
    hy = _hydat(tmp_path / "hydat.sqlite3")
    assert _dump(build(hy, ["08ZZ001"], "t", datum.accepted(rows))) == _dump(build(hy, ["08ZZ001"], "t"))
    assert datum.accepted(rows) == {}


def test_an_accepted_change_puts_the_record_on_todays_zero(tmp_path, monkeypatch):
    hy = _hydat(tmp_path / "hydat.sqlite3")
    plain = build(hy, ["08ZZ001"], "t")
    # the change is today: the whole record (through last year) is on the old zero
    moved = build(hy, ["08ZZ001"], "t", datum.accepted([_row(status="accepted", effective=date.today().isoformat())]))
    for p, band in plain["stations"]["08ZZ001"]["level"].items():
        assert moved["stations"]["08ZZ001"]["level"][p] == pytest.approx([v - 3.0 for v in band], abs=0.002)
    assert moved["stations"]["08ZZ001"]["discharge"] == plain["stations"]["08ZZ001"]["discharge"]     # flow untouched
    flat = lambda rating, off: [v for q, lo, hi in rating for v in (q, lo + off, hi + off)]   # noqa: E731
    assert flat(moved["ratings"]["08ZZ001"], 0.0) == pytest.approx(flat(plain["ratings"]["08ZZ001"], -3.0), abs=0.002)
    for y, days in plain["recentYears"]["08ZZ001"]["level"].items():
        assert moved["recentYears"]["08ZZ001"]["level"][y] == pytest.approx(
            [None if v is None else v - 3.0 for v in days], abs=0.002)
    assert moved["stats"] == plain["stats"]
    # the live level on the new zero is now on the rating: reported and ranked
    a = json.loads(_publish(tmp_path / "a", monkeypatch, moved, (30.0, 1.3))["index.json"])["stations"]["08ZZ001"]
    assert a["reports"] == ["discharge", "level"] and a["level"] is not None


def test_a_change_inside_the_record_shifts_only_the_days_before_it(tmp_path):
    """A record that steps 1 m -> 4 m in 2016 (the shift detector cuts it there): accepted +3 m from 2016-01-01 makes it
    one record on today's zero, so nothing is cut and the days from 2016 on are left as they are."""
    hy = _hydat(tmp_path / "hydat.sqlite3", step_at=2016)
    db = sqlite3.connect(f"file:{hy}?mode=ro", uri=True)
    assert level_record(db, "08ZZ001")["since"] == 2016
    moves = datum.accepted([_row(effective="2016-01-01", offset_m=3.0, status="accepted")])
    rec = level_record(db, "08ZZ001", moves["08ZZ001"])
    db.close()
    assert rec["shifts"] == [] and rec["since"] is None
    out = build(hy, ["08ZZ001"], "t", moves)
    level = out["stations"]["08ZZ001"]["level"]
    assert out["stats"]["08ZZ001"]["level"]["from_year"] == 2010                   # the whole record is comparable now
    assert 4.0 <= min(b[0] for b in level.values()) and max(b[-1] for b in level.values()) <= 4.6


def test_an_accepted_change_keeps_the_records_millimetres(tmp_path):
    """+94.826 m (07FD001's size): `_sig4` would keep 1 cm of ~99 m; a moved station's levels keep 3 decimals."""
    hy = _hydat(tmp_path / "hydat.sqlite3")
    plain = build(hy, ["08ZZ001"], "t")
    moved = build(hy, ["08ZZ001"], "t", datum.accepted([_row(status="accepted", offset_m=94.826,
                                                             effective=date.today().isoformat())]))
    got = [v for band in moved["stations"]["08ZZ001"]["level"].values() for v in band]
    want = [v + 94.826 for band in plain["stations"]["08ZZ001"]["level"].values() for v in band]
    assert got == pytest.approx(want, abs=0.0006) and all(v == round(v, 3) for v in got)
    days = [v for d in moved["recentYears"]["08ZZ001"]["level"].values() for v in d if v is not None]
    assert days and all(v == round(v, 3) and abs(v - round(v, 2)) < 0.0051 for v in days)
    assert any(abs(v - round(v, 2)) > 0.0009 for v in days)                # millimetres survive, not centimetres
    assert moved["stations"]["08ZZ001"]["discharge"] == plain["stations"]["08ZZ001"]["discharge"]


def test_an_accepted_change_inside_the_this_year_line_is_refused():
    """The station file's `daily` (this year, as it came) would mix both zeros: 07FD001 (2026-08-05) and 08KH019
    (2026-07-07) cannot be accepted before the year turns; 08HD006 (2025-06-05) can."""
    today = date(2026, 11, 24)
    with pytest.raises(datum.DatumFileError, match="inside the this-year line"):
        datum.parse({"_about": "t", "changes": [_row(status="accepted", effective="2026-08-05")]}, today=today)
    assert datum.parse({"_about": "t", "changes": [_row(status="proposed", effective="2026-08-05")]}, today=today)
    assert datum.parse({"_about": "t", "changes": [_row(status="accepted", effective="2025-06-05")]}, today=today)
    assert datum.parse({"_about": "t", "changes": [_row(status="accepted", effective="2026-01-01")]}, today=today)
    # in January the line starts on 1 December
    with pytest.raises(datum.DatumFileError, match="inside the this-year line"):
        datum.parse({"_about": "t", "changes": [_row(status="accepted", effective="2026-12-15")]},
                    today=date(2027, 1, 10))
    assert datum.parse({"_about": "t", "changes": [_row(status="accepted", effective="2026-08-05")]},
                       today=date(2027, 1, 10))


def test_offset_before_counts_only_the_changes_after_the_day():
    moves = [(date(2016, 1, 1), 3.0), (date(2025, 6, 5), -5.0)]
    assert datum.offset_before(moves, date(2015, 12, 31)) == -2.0
    assert datum.offset_before(moves, date(2016, 1, 1)) == -5.0
    assert datum.offset_before(moves, date(2025, 6, 5)) == 0.0


# ------------------------------------------------------------------------------------------------ the file

def test_only_an_accepted_row_moves_anything():
    rows = datum.parse({"_about": "t", "changes": [
        _row(station="08ZZ001", status="accepted"), _row(station="08ZZ002", status="proposed"),
        _row(station="08ZZ003", status="rejected"), _row(station="08ZZ004", status="proposed", effective=None, offset_m=None,
                                                     offset_from=None)]})
    assert datum.accepted(rows) == {"08ZZ001": [(date(2025, 1, 1), -3.0)]}


def test_the_stage_key_moves_with_accepted_rows_only():
    a = datum.parse({"_about": "t", "changes": [_row(station="08ZZ001")]})
    b = datum.parse({"_about": "t", "changes": [_row(station="08ZZ001", note="another note", offset_m=-2.0)]})
    c = datum.parse({"_about": "t", "changes": [_row(station="08ZZ001", status="accepted")]})
    assert datum.digest(a) == datum.digest(b) == datum.digest([]) != datum.digest(c)


@pytest.mark.parametrize("bad, says", [
    (_row(status="accepted", offset_m=None, offset_from=None), "needs its effective date and offset_m"),
    (_row(status="accepted", effective=None), "needs its effective date and offset_m"),
    (_row(status="maybe"), "status"),
    (_row(source="hydat_remark"), "quotes the remark"),
    (_row(remark="x"), "no HYDAT remark"),
    (_row(offset_m="5"), "metres"),
    (_row(offset_m=True), "metres"),
    (_row(offset_from=None), "offset_from"),
    (_row(effective="5 June 2025"), "YYYY-MM-DD"),
    (_row(station="08hd006"), "station id"),
    (_row(note=""), "note"),
    ({**_row(), "extra": 1}, "unknown"),
])
def test_the_file_refuses_a_bad_row(bad, says):
    with pytest.raises(datum.DatumFileError, match=says):
        datum.parse({"_about": "t", "changes": [bad]})


def test_the_file_refuses_a_bad_shape_and_a_twice_listed_change():
    with pytest.raises(datum.DatumFileError, match="_about, changes"):
        datum.parse({"changes": []})
    with pytest.raises(datum.DatumFileError, match="two rows"):
        datum.parse({"_about": "t", "changes": [_row(), _row()]})


def test_a_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError, match="never an empty list"):
        datum.load(tmp_path / "absent.json")


def test_a_quoted_remark_must_be_the_records(tmp_path, capsys):
    hy = _hydat(tmp_path / "hydat.sqlite3", remarks=[("08ZZ001", 5, -999, "STATION RELOCATED ON 2025-01-01.\n")])
    ok = datum.load(_file(tmp_path, [_row(source="hydat_remark", remark="station relocated on  2025-01-01.",
                                          status="accepted")]))
    assert datum.check_remarks(ok, hy) == []                               # case and spacing aside
    # an ACCEPTED row whose quote HYDAT no longer holds stops the run: it would move a record
    bad = datum.parse({"_about": "t", "changes": [_row(source="hydat_remark", remark="STATION MOVED ON 2025-01-01.",
                                                       status="accepted")]})
    with pytest.raises(datum.DatumFileError, match="holds no station remark"):
        datum.check_remarks(bad, hy)
    # a proposed or rejected one is a warning, shown and returned (a reworded remark must not stop the HYDAT workflow)
    for status in ("proposed", "rejected"):
        held = datum.parse({"_about": "t", "changes": [_row(source="hydat_remark", remark="STATION MOVED ON 2025-01-01.",
                                                            status=status)]})
        got = datum.check_remarks(held, hy)
        assert len(got) == 1 and "08ZZ001" in got[0] and status in got[0]
        assert "08ZZ001" in capsys.readouterr().out
    with pytest.raises(FileNotFoundError):
        datum.check_remarks(ok, tmp_path / "absent.sqlite3")


def test_the_live_file_is_the_curated_one_and_parses():
    assert datum.DATUM_FILE == under_test.curated().gauges.datum_change_stations
    assert datum.DATUM_FILE.name == "datum_change_stations.json"
    rows = datum.load()
    datum.validate_text(datum.DATUM_FILE.read_text(encoding="utf-8"))
    assert datum.accepted(rows) == {r["station"]: [(date.fromisoformat(r["effective"]), float(r["offset_m"]))]
                                    for r in rows if r["status"] == "accepted"}


@pytest.mark.needs_source
def test_the_live_rows_quote_the_live_record(request):
    hydat = need(request, "source", Path(SOURCE) / "hydat.sqlite3", "python data/fetch_data.py --layers hydat")
    datum.check_remarks(datum.load(), hydat)


def test_no_reader_names_the_file_but_the_loader():
    """Every reader goes through `datum` (`accepted` drops a proposed row): no other module names the curated key."""
    got = set()
    for p in sorted((ROOT / "pipeline").rglob("*.py")):
        rel = p.relative_to(ROOT).as_posix()
        if "/tests/" in rel:
            continue
        tree = ast.parse(p.read_text(encoding="utf-8"))
        if any(isinstance(n, ast.Attribute) and n.attr == "datum_change_stations" for n in ast.walk(tree)):
            got.add(rel)
    assert got == {"pipeline/gauges/feed/datum.py"}
    src = (ROOT / "pipeline/gauges/feed/climatology.py").read_text(encoding="utf-8")
    calls = {n.func.attr for n in ast.walk(ast.parse(src)) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
             and isinstance(n.func.value, ast.Name) and n.func.value.id == "datum_file"}
    assert calls == {"load", "check_remarks", "accepted", "plain_line"}


# ------------------------------------------------------------------------------------------------ the owner's two (2026-11-24)

OWNER_WORDS = "Yes but make sure it is a clean well documented update that happens which is easy to see"


def test_the_owner_accepted_exactly_08HD006_and_08EF001_in_his_words():
    """Owner 2026-11-24: 08HD006 and 08EF001 accepted, his answer verbatim on each row; the three he was not asked about
    stay proposed with no decision."""
    rows = {r["station"]: r for r in datum.load()}
    assert {st for st, r in rows.items() if r["status"] == "accepted"} == {"08HD006", "08EF001"}
    for st in ("08HD006", "08EF001"):
        d = rows[st]["decision"]
        assert d["by"] == "owner" and d["on"] == "2026-11-24" and d["words"] == OWNER_WORDS
    for st in ("07FD001", "08KH019", "08MH153"):
        assert rows[st]["status"] == "proposed" and "decision" not in rows[st]
    assert datum.accepted(list(rows.values())) == {"08EF001": [(date(2025, 1, 30), 2.37)],
                                                   "08HD006": [(date(2025, 6, 5), -5.0)]}


def test_a_decided_row_says_who_when_what_was_asked_and_the_answer():
    for status in ("accepted", "rejected"):
        with pytest.raises(datum.DatumFileError, match="records the decision"):
            datum.parse({"_about": "t", "changes": [_row(status=status, decision=None)]})
        with pytest.raises(datum.DatumFileError, match="records the decision"):
            datum.parse({"_about": "t", "changes": [_row(status=status, decision={"by": "owner", "on": "2026-11-24"})]})
        with pytest.raises(datum.DatumFileError, match="decision.words is text"):
            datum.parse({"_about": "t", "changes": [_row(status=status, decision={**DECIDED, "words": " "})]})
        with pytest.raises(datum.DatumFileError, match="decision.on"):
            datum.parse({"_about": "t", "changes": [_row(status=status, decision={**DECIDED, "on": "24 Nov"})]})
    with pytest.raises(datum.DatumFileError, match="a proposed row has no `decision`"):
        datum.parse({"_about": "t", "changes": [_row(decision=dict(DECIDED))]})


def test_the_plain_line_per_station():
    rows = {r["station"]: r for r in datum.load()}
    assert datum.plain_line(rows["08HD006"]) == ("08HD006 level record moved 5.0 m down to the gauge's new zero "
                                                 "(Environment Canada note, 2025-06-05)")
    assert datum.plain_line(rows["08EF001"]) == ("08EF001 level record moved 2.37 m up to the gauge's new zero "
                                                 "(Environment Canada note, 2025-01-30)")
    assert datum.plain_line(_row(status="accepted", offset_m=1.011, effective="2026-07-07")) == (
        "08ZZ001 level record moved 1.011 m up to the gauge's new zero (measured from the live feed, 2026-07-07)")


def test_record_changes_lists_a_new_acceptance_and_an_undo_and_nothing_else():
    a, b = _row(station="08ZZ001", status="accepted"), _row(station="08ZZ002", status="accepted", offset_m=2.0)
    held = _row(station="08ZZ003")
    assert datum.record_changes([a, held], [a, held]) == []                     # nothing moved: nothing said
    assert datum.record_changes([a], [a, b, held]) == [
        "08ZZ002 level record moved 2.0 m up to the gauge's new zero (measured from the live feed, 2025-01-01)"]
    assert datum.record_changes([a, b], [dict(a, status="proposed", decision=None), b]) == [
        "08ZZ001 level record back on HYDAT's own zero: the 3.0 m down change of 2025-01-01 is no longer accepted"]
    # an accepted offset changed: the old one goes back, the new one moves
    assert datum.record_changes([a], [dict(a, offset_m=-2.5)]) == [
        "08ZZ001 level record back on HYDAT's own zero: the 3.0 m down change of 2025-01-01 is no longer accepted",
        "08ZZ001 level record moved 2.5 m down to the gauge's new zero (measured from the live feed, 2025-01-01)"]


def test_rows_at_reads_the_file_as_committed(tmp_path, monkeypatch):
    import subprocess
    repo = tmp_path / "repo"
    f = repo / "gauges" / "datum_change_stations.json"
    f.parent.mkdir(parents=True)
    git = lambda *a: subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *a], cwd=repo, check=True,
                                    capture_output=True, text=True).stdout.strip()
    git("init", "-q")
    (repo / "x").write_text("x")
    git("add", "x")
    git("commit", "-qm", "before the file")
    first = git("rev-parse", "HEAD")
    f.write_text(json.dumps({"_about": "t", "changes": [_row(status="accepted")]}))
    git("add", ".")
    git("commit", "-qm", "the file")
    monkeypatch.setattr(datum, "DATUM_FILE", f)
    assert datum.rows_at(first, repo) is None                                   # not in that commit: said, not []
    assert [r["status"] for r in datum.rows_at(git("rev-parse", "HEAD"), repo)] == ["accepted"]
    with pytest.raises(subprocess.CalledProcessError):
        datum.rows_at("0" * 40, repo)                                           # a commit git does not have raises


def test_set_status_moves_one_row_only_and_undoes(tmp_path, monkeypatch):
    from pipeline.tests.conftest import curated_root
    root = curated_root(monkeypatch, tmp_path / "curated")
    p = root / "gauges" / "datum_change_stations.json"
    old = json.loads(p.read_text(encoding="utf-8"))
    monkeypatch.setenv("CURATED_WRITE_BY", "the test")
    datum.set_status("08HD006", "proposed", None, p)                            # the undo: one command
    new = json.loads(p.read_text(encoding="utf-8"))
    moved = [(o["station"], k) for o, n in zip(old["changes"], new["changes"]) for k in set(o) | set(n) if o.get(k) != n.get(k)]
    assert sorted(moved) == [("08HD006", "decision"), ("08HD006", "status")] and new["_about"] == old["_about"]
    datum.set_status("08HD006", "accepted", old["changes"][[r["station"] for r in old["changes"]].index("08HD006")]["decision"], p)
    assert json.loads(p.read_text(encoding="utf-8")) == old                    # and back, byte for byte the same rows
    with pytest.raises(datum.DatumFileError, match="records the decision"):
        datum.set_status("08HD006", "accepted", None, p)                        # an acceptance needs its decision
    with pytest.raises(datum.DatumFileError, match="has 0 rows"):
        datum.set_status("08ZZ999", "proposed", None, p)


# ------------------------------------------------------------------------------------------------ the proposals

def test_the_five_remarks_parse():
    k = R.kind(HD006_REMARK)
    assert k == "change" and R.the_date(HD006_REMARK) == ("2025-06-05", None)
    assert R.the_offset(HD006_REMARK, k) == (-5.0, None, True)                 # "~5m drop": approximate, down
    assert R.the_date(EF001_REMARK) == ("2025-01-30", None)
    assert R.the_offset(EF001_REMARK, R.kind(EF001_REMARK)) == (2.37, None, False)
    assert R.the_date(FD001_REMARK) == ("2026-08-05", None)
    assert R.the_offset(FD001_REMARK, R.kind(FD001_REMARK)) == (None, "no size stated", False)


@pytest.mark.parametrize("text, kind, day, offset", [
    ("STATION RELOCATED APPROX. 0.5 KM DOWNSTREAM ON MAY 30, 2002. THIS RESULTED IN THE GAUGE DATUM BEING LOWERED BY "
     "3.516M.", "change", "2002-05-30", 3.516),                            # the zero down: every reading up
    ("AUG 12, 2003 LOWERED DATUM 1.000M", "change", "2003-08-12", 1.0),
    ("On 2014-01-01, 1.000m was added to the datum to avoid negative water levels.", "change", "2014-01-01", 1.0),
    ("DATUM CHANGE resulting in 5.000m step in water level data on 2020-09-01.", "change", "2020-09-01", None),
    ("ADD 161.021M TO CONVERT TO PACIFICA PAPER DATUM", "conversion", None, None),
    ("STATION MOVED 30M UPSTREAM AND ACROSS TO THE RIGHT BANK ON SEP 9, 2005. DATUM MAINTAINED.", "kept", "2005-09-09", None),
    ("Datum stability is less than that defined by typical WSC standards for the period 2015 - 2018.", "quality", None, None),
    ("STATION AND WATER LEVEL LOCATION MOVED UP 250 M FROM OUTLET OF FRANCOIS LAKE", "change", None, None),
    # review 2026-11-24: an end-of-record date is no change's date (08EE003, 08KD006); "AFTER" a date is the next day
    # (08NE087); "datum is similar" is no change (08GA026); a signed change OF THE DATUM moves readings the other way
    # (08MG014: HYDAT reads ~5.3 m higher from 2013)
    ("RELOCATED 130 M DOWNSTREAM IN 1971. DISCONTINUED DEC 31 1971. STATION RE-ESTABLISHED IN APR 1980 FOR B.C. WATER "
     "MANAGEMENT BRANCH, VICTORIA, B.C. STATION RELOCATED IN 2010 DUE TO OPERATIONAL REASONS.", "change", None, None),
    ("STATION DESTROYED. NO DATA AFTER SEP 15, 2011. STATION WILL HAVE TO BE MOVED TO A NEW LOCATION", "other", None, None),
    ("STATION TO BE MOVED UPSTREAM AFTER DEC 31, 2000.", "change", "2001-01-01", None),
    ("The station was re-established on 2023-09-06. The original benchmark network was found intact, and every effort "
     "was made to ensure the new station datum is similar to the historical station datum. Instability in the benchmark "
     "network indicates the current assumed datum may vary +/- 0.020m from the historical assumed datum.",
     "quality", "2023-09-06", None),
    ("The station's assumed datum changed when re-established in 2013. The change is approximately -4.923 m difference "
     "from the 1938 to 1984 datum. The accuracy of the given datum adjustment has not been assessed at this time.",
     "change", None, 4.923),
])
def test_other_wordings_sort_and_parse(text, kind, day, offset):
    k = R.kind(text)
    assert k == kind and R.the_date(text)[0] == day and R.the_offset(text, k)[0] == offset
    if offset is None:
        assert R.the_offset(text, k)[1]                                    # a null always says why


def test_two_dates_or_a_year_only_give_no_date():
    assert R.the_date("STATION RELOCATED MAY 23 1979 2.6 KM UPSTREAM. RELOCATED ON MAY 30, 2002.") == (
        None, "2 dates stated (1979-05-23, 2002-05-30): which one the zero changed on is the reader's call")
    assert R.the_date("STATION LOCATION MOVED 7 KM UPSTREAM IN 2003") == (None, "a year only (2003), no day")


def test_the_scan_keeps_roster_stations_and_their_wording_sorted(tmp_path):
    hy = _hydat(tmp_path / "hydat.sqlite3", remarks=[
        ("08ZZ001", 2, 2025, EF001_REMARK), ("08ZZ001", 5, -999, FD001_REMARK), ("08ZZ001", 2, 2024, "WATER LEVEL NOT AVAILABLE"),
        ("Z9", 5, -999, FD001_REMARK)])                                    # Z9 is not on the roster
    roster = tmp_path / "roster.json"
    roster.write_text(json.dumps([{"station": "08ZZ001", "name": "TEST CREEK"}]))
    a = R.build(hy, roster)
    assert [(r["station"], r["year"]) for r in a["remarks"]] == [("08ZZ001", None), ("08ZZ001", 2025)]
    assert a["remarks"][1]["remark"] == EF001_REMARK and a["remarks"][1]["offset_m"] == 2.37
    assert a["stations"] == 1 and a["change_stations"] == ["08ZZ001"] and a["roster"] == 1
    assert json.dumps(R.build(hy, roster)) == json.dumps(a)                 # deterministic
