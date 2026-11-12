"""T555 item 4: the app-version gate is derived from one generated table of what each app version reads, every file of
the release contract; a recorded row never changes without the app version moving."""
import pytest

from pipeline.deliver.release import app_gate as G
from pipeline.deliver.release import site_gate as SG
from pipeline.tools.emit import app_reads as A

#: three app versions: 0.3.0 learned paint/5 (and still takes paint/4), 0.7.0 moved water to water/3 and added `lines`
APPS = {"0.2.0": {"paint": ["paint/4"], "water": ["water/1"], "water-1": ["water-slice/1"]},
        "0.3.0": {"paint": ["paint/5", "paint/4"], "water": ["water/1"], "water-1": ["water-slice/1"]},
        "0.7.0": {"paint": ["paint/5", "paint/4"], "water": ["water/3"], "water-1": ["water-slice/4"], "lines": ["lines/1"]}}


def _m(stamp=None, **files):
    return {**({"minAppVersion": stamp} if stamp else {}),
            "files": {k.replace("_", "-"): {"format": v} for k, v in files.items()}}


def test_the_oldest_reader_is_derived_per_format_and_for_the_whole_release():
    m = _m(paint="paint/5", water="water/3", water_1="water-slice/4", lines="lines/1")
    # `lines` is a file 0.2.0 and 0.3.0 do not know: it is a newer release's and asks nothing of them
    assert G.needs(m, APPS) == {"paint/5": "0.3.0", "water/3": "0.7.0", "water-slice/4": "0.7.0", "lines/1": "0.2.0"}
    assert G.oldest_reader(m, APPS) == "0.7.0"
    assert G.oldest_reader(_m(paint="paint/4"), APPS) == "0.2.0"          # 0.3.0 and 0.7.0 still take paint/4


def test_a_stamp_below_the_oldest_reader_is_refused_naming_every_format_and_the_version_to_use():
    m = _m("0.3.0", paint="paint/5", water="water/3", water_1="water-slice/4")
    assert G.problems(m, None, APPS) == [
        "water-slice/4 needs app >= 0.7.0 but the oldest app allowed is 0.3.0; rerun with MIN_APP_VERSION=0.7.0 (or newer)",
        "water/3 needs app >= 0.7.0 but the oldest app allowed is 0.3.0; rerun with MIN_APP_VERSION=0.7.0 (or newer)"]
    assert G.problems(m, "0.7.0", APPS) == []                             # MIN_APP_VERSION wins over the stamp
    assert G.problems(_m("0.7.0", paint="paint/5", water="water/3"), None, APPS) == []
    # no stamp at all: refused for every format the first app could not read, passed when every app reads it
    assert len(G.problems(_m(paint="paint/5"), None, APPS)) == 1
    assert G.problems(_m(paint="paint/4"), None, APPS) == []


def test_a_format_no_current_app_reads_is_refused_whatever_the_stamp():
    for fmt in ("water/9", "water/1"):                                    # one not built yet, one the newest app dropped
        got = G.problems(_m("0.7.0", water=fmt), None, APPS)
        assert len(got) == 1 and got[0].startswith(f"{fmt} is read by no current app (the newest recorded is 0.7.0)")
        assert G.oldest_reader(_m(water=fmt), APPS) is None


def test_the_site_gate_asks_for_the_same_oldest_reader():
    m = _m("0.3.0", paint="paint/5", water="water/3")
    assert SG.required(m, None, APPS) == "0.7.0"
    assert SG.required(m, "0.8.0", APPS) == "0.8.0"                       # a newer stamp stands
    assert SG.required(None, None, APPS) is None and SG.required(None, "0.5.0", APPS) == "0.5.0"


# ---------------------------------------------------------------------------------- the table and its keeper

FILES = {"paint": {"format": "paint/5"}, "water": {"format": "water/3"}}
READS = A.reads_of(FILES, {"paint": ["paint/4", "paint/5"]})


def test_a_row_is_the_contracts_format_then_the_older_ones_the_reader_still_takes():
    assert READS == {"paint": ["paint/5", "paint/4"], "water": ["water/3"]}


def test_the_check_wants_the_current_version_recorded_and_equal_to_the_code():
    assert A.check_problems({"apps": {"0.7.5": {"reads": READS}}}, "0.7.5", READS) == []
    missing = A.check_problems({"apps": {}}, "0.7.5", READS)
    assert len(missing) == 1 and missing[0].startswith("app 0.7.5 has no row in pipeline/deliver/release/app_reads.json")


def test_a_format_moved_without_a_version_bump_fails_the_check_and_is_not_recorded():
    doc = {"apps": {"0.7.5": {"reads": READS}}}
    moved = A.reads_of({**FILES, "water": {"format": "water/4"}}, {"paint": ["paint/4", "paint/5"]})
    said = A.check_problems(doc, "0.7.5", moved)
    assert len(said) == 1 and "water: recorded ['water/3'], the code reads ['water/4']" in said[0]
    assert "bump the version in app/apps/mobile/app.json" in said[0]
    with pytest.raises(SystemExit, match="refused"):
        A.record(doc, "0.7.5", moved)
    # the version bumped: a new row, the old one untouched
    out, what = A.record(doc, "0.7.6", moved)
    assert what == "app 0.7.6: recorded" and out["apps"]["0.7.5"] == {"reads": READS} and out["apps"]["0.7.6"] == {"reads": moved}
    # no build of it left this machine: said, with the reason
    out, what = A.record(doc, "0.7.5", moved, replace="0.7.5 was never built")
    assert what == "app 0.7.5: row REPLACED (0.7.5 was never built)" and out["apps"]["0.7.5"] == {"reads": moved}
    # recording what is already recorded writes nothing new
    assert A.record(doc, "0.7.5", READS) == (doc, "app 0.7.5: already recorded, unchanged")


def test_the_file_is_one_line_per_version_oldest_first_and_round_trips(tmp_path):
    doc = {"apps": {"0.10.0": {"reads": READS}, "0.7.5": {"reads": READS, "backfilled": True, "commit": "abc"}}}
    text = A.dump(doc)
    assert [ln.split(":")[0].strip() for ln in text.splitlines()[3:5]] == ['"0.7.5"', '"0.10.0"']
    p = tmp_path / "app_reads.json"
    p.write_text(text)
    assert A.load(p)["apps"] == doc["apps"]
    with pytest.raises(FileNotFoundError, match="is missing"):
        A.load(tmp_path / "nowhere.json")


def test_an_older_format_the_app_takes_that_this_does_not_know_stops_the_emitter(tmp_path):
    root = tmp_path
    for rel, text in ((A.MANIFEST_TS, "const ALSO_READS: Readonly<Record<string, readonly string[]>> = { paint: PAINT_READS, water: WATER_READS };"),
                      (A.PAINT_TS, 'export const PAINT_READS: readonly string[] = ["paint/4", "paint/5"];')):
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text(text)
    with pytest.raises(SystemExit, match="ALSO_READS has an entry this does not know"):
        A.current_reads(root)
    (root / A.MANIFEST_TS).write_text("// no table here")
    with pytest.raises(SystemExit, match="no longer declares ALSO_READS"):
        A.current_reads(root)


# ---------------------------------------------------------------------------------- the committed table

def test_the_committed_table_holds_the_current_app_and_is_what_the_code_reads():
    """The guard `pnpm check` runs (`--check`), in the pytest suite too: a format moved with no version bump fails here."""
    assert A.check_problems(A.load(), A.app_version(), A.current_reads()) == []


def test_the_committed_table_gives_the_first_readers_the_hand_tables_had():
    """PAINT_NEEDS {5: 0.3.0}, SALMON_NEEDS {4: 0.5.0} and the water/3 + geo/2 stamp of 0.7.0, now derived."""
    apps = G.table()
    got = G.needs(_m(paint="paint/5", salmon="salmon/4", water="water/3", geo="geo/2"), apps)
    assert got == {"paint/5": "0.3.0", "salmon/4": "0.5.0", "water/3": "0.7.0", "geo/2": "0.7.0"}
