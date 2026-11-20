"""T556: A LIVE FEED'S FORMAT IS TYPED ONCE (`pipeline/common/feed_formats.py`) and generated for the app and the worker.

Every Python writer's constant IS the table's value; the committed TypeScript is what the emitter writes today; a bump
made in the table alone leaves the generated file stale (so the bump cannot land on one side only).
"""
from __future__ import annotations

from pipeline.common import feed_formats as FF
from pipeline.tools.emit import feed_formats as E


def test_every_python_writer_reads_the_table():
    from pipeline.deliver import salmon_pairs
    from pipeline.gauges.feed import publish as gauges
    from pipeline.regs.dfo_salmon import live as dfo
    from pipeline.regs.inseason import publish as inseason
    from pipeline.runtiming.feed import build as runtiming
    assert {
        "dfo_feed": dfo.FORMAT, "inseason_links": inseason.FORMAT, "gauge_feed": gauges.FEED_FORMAT,
        "runtiming_index": runtiming.INDEX_FORMAT, "runtiming_runs": runtiming.RUNS_FORMAT,
        "runtiming_paint": runtiming.PAINT_FORMAT, "dfo_pairs": salmon_pairs.FORMAT,
        # written and read in TypeScript only (the feeds worker writes, the app reads): one table all the same
        "inseason_feed": FF.FEED_FORMATS["inseason_feed"],
    } == FF.FEED_FORMATS


def test_every_format_is_a_name_and_a_version_and_says_who_writes_and_reads_it():
    import re
    for key, (fmt, why) in FF.FEEDS.items():
        assert re.fullmatch(r"[a-z]+(-[a-z]+)+/[1-9]\d*", fmt), (key, fmt)
        assert "write" in why and "read" in why, key
    assert len(set(FF.FEED_FORMATS.values())) == len(FF.FEED_FORMATS)


def test_the_committed_typescript_is_what_the_table_says_today():
    assert E.main(["--check"]) == 0, "run: python -m pipeline.tools.emit.feed_formats"
    assert E.OUT.read_text(encoding="utf-8") == E.render()
    for key, fmt in FF.FEED_FORMATS.items():
        assert f'{E._camel(key)}: "{fmt}",' in E.render()


def test_a_bump_in_the_table_alone_fails_the_check(monkeypatch, capsys):
    bumped = {**FF.FEEDS, "dfo_feed": ("dfo-feed/4", FF.FEEDS["dfo_feed"][1])}
    monkeypatch.setattr(E, "FEEDS", bumped)
    assert E.main(["--check"]) == 1
    assert "is stale or hand-edited" in capsys.readouterr().err


#: (T676 item 2) THE PAIRS, PINNED: a feed index's format and the oldest app that reads it. A bump of a format without a
#: new minimum fails, saying what to do: an installed app with the gate then refuses the bumped index and shows the
#: update prompt, instead of reading "no feed".
PINNED_MIN_APP = {
    "dfo_feed": ("dfo-feed/3", "0.0.0"),
    "inseason_feed": ("inseason-feed/1", "0.0.0"),
    "gauge_feed": ("gauge-feed/1", "0.0.0"),
    "runtiming_index": ("runtiming-index/1", "0.0.0"),
}


def _pins_problems(formats: dict, mins: dict) -> list[str]:
    import re
    out = []
    for key, need in mins.items():
        if not re.fullmatch(r"\d+\.\d+\.\d+", need):
            out.append(f"{key}: {need!r} is not a version")
        was = PINNED_MIN_APP.get(key)
        if was is None:
            out.append(f"{key}: a new gated feed; pin it in PINNED_MIN_APP")
        elif formats[key] != was[0] and need == was[1]:
            out.append(f"{key}: {was[0]} -> {formats[key]} but FEED_MIN_APP stayed {need}: set it to the first app "
                       "version that reads the new format (pipeline/common/feed_formats.py), then pin both here")
        elif (formats[key], need) != was:
            out.append(f"{key}: update PINNED_MIN_APP to {(formats[key], need)}")
    return out


def test_a_format_bump_raises_the_oldest_app_that_reads_the_index():
    assert _pins_problems(FF.FEED_FORMATS, FF.FEED_MIN_APP) == []
    assert set(FF.FEED_MIN_APP) <= set(FF.FEED_FORMATS)
    # the mutation: a bump in the format alone is refused, by name
    bumped = {**FF.FEED_FORMATS, "dfo_feed": "dfo-feed/4"}
    assert any("FEED_MIN_APP stayed" in p for p in _pins_problems(bumped, FF.FEED_MIN_APP))


def test_every_python_index_writer_reads_the_oldest_app_from_the_table():
    """The Python writers' constants are the table's. What each WRITES is pinned where it is run on a fixture, on the
    written file: `test_dfo_live.py::test_todays_pages_join_every_location` (dfo index), `test_feed_gaps.py::
    test_a_mid_window_gap_is_filled_from_the_archive` (gauge index + station file), `test_runtiming_feed.py::
    test_each_section_takes_its_relations_and_the_paint_index_says_when` (run timing index); the worker's in its tests."""
    from pipeline.gauges.feed import publish as gauges
    from pipeline.regs.dfo_salmon import live as dfo
    from pipeline.runtiming.feed import build as runtiming
    assert (dfo.MIN_APP, gauges.MIN_APP, runtiming.MIN_APP) == (
        FF.FEED_MIN_APP["dfo_feed"], FF.FEED_MIN_APP["gauge_feed"], FF.FEED_MIN_APP["runtiming_index"])


def test_the_typescript_carries_the_minimums():
    tail = E.render().split("export const FEED_MIN_APP")[1]
    for key, need in FF.FEED_MIN_APP.items():
        assert f'  {E._camel(key)}: "{need}",' in tail
