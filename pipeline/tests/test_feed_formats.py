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
