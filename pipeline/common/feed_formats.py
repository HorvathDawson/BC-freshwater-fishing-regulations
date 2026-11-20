"""THE FEED FORMATS: the one place a live feed's format string is typed (T556, AGENTS 40).

A feed is written by one program and read by another, in another language, at an UNVERSIONED key
(`feeds/dfo/index.json`, `feeds/inseason/matches.json`, …), and the reader refuses any format but its own. The string
was hand-copied on each side — `dfo-feed/3` in `regs/dfo_salmon/live.py`, the worker's `dfo/live.ts` and core
`salmonFeed.ts` — and a bump made on one side only broke staging once (2026-11-09: the pipeline published
`inseason-links/3` while the deployed worker still required /2).

Every Python writer reads its format from `FEED_FORMATS`; `python -m pipeline.tools.emit.feed_formats` writes
`app/packages/core/src/feedFormats.generated.ts`, which the app and the feeds worker read (`--check` in CI, `pnpm
check` and fast-fail). `scripts/check-format-pairs.py` refuses one of these strings typed as a literal anywhere else.
A bump is ONE edit here plus the emit; the deploy order is still the bumper's to think about. (T676 item 2) A bump of an
index feed also raises its `FEED_MIN_APP` (below) to the first app version that reads the new format, so an installed
app with the feed gate keeps its last capture and shows the update prompt instead of reading "no feed".
"""
from __future__ import annotations

#: key -> (format, who writes it -> who reads it). Keys are the generated TypeScript's names in snake_case.
FEEDS = {
    "dfo_feed": ("dfo-feed/3", "feeds/dfo/index.json: the feeds worker (dfo/live.ts) and regs/dfo_salmon/live.py write, "
                               "the app reads (core salmonFeed.ts)"),
    "dfo_pairs": ("dfo-pairs/1", "R2 private/feeds/dfo/pairs.json (T668): deliver/salmon_pairs.py writes through "
                                 "sync_inputs, the feeds worker reads it and carries it into feeds/dfo/index.json "
                                 "`pairs`, the app reads it there (core salmonFeed.ts)"),
    "inseason_feed": ("inseason-feed/1", "feeds/inseason/index.json: the feeds worker writes (inseason.ts), the app "
                                         "reads (core inseason.ts)"),
    "inseason_links": ("inseason-links/4", "feeds/inseason/matches.json: regs/inseason/publish.py writes, the feeds "
                                           "worker and the app read (core inseason.ts)"),
    "gauge_feed": ("gauge-feed/1", "feeds/live/: the feeds worker and gauges/feed/publish.py write index.json, the "
                                   "station files and obs.json; the app reads (core gaugeFeed.ts)"),
    "runtiming_index": ("runtiming-index/1", "runtiming/index.json: runtiming/feed/build.py writes, the app reads "
                                             "(core runTiming.ts)"),
    "runtiming_runs": ("runtiming-runs/1", "runtiming/runs: runtiming/feed/build.py writes, the app reads"),
    "runtiming_paint": ("runtiming-paint/1", "runtiming/paint: runtiming/feed/build.py writes, the app reads"),
}

#: key -> the format string
FEED_FORMATS = {key: fmt for key, (fmt, _) in FEEDS.items()}

#: (T676 item 2, owner 2026-11-20: a per-feed minimum app version, not a versioned key) THE OLDEST APP THAT READS EACH
#: FEED INDEX's format, written into the index as `minAppVersion` by every writer of it (Python and the feeds worker).
#: An app that carries the gate (core `feedGate.ts`) and is older refuses the index, keeps its last capture and raises
#: the update prompt the release gate raises ("New data needs the latest app") — instead of reading "no feed".
#: "0.0.0": every app that carries the gate reads today's format. A FORMAT BUMP RAISES THIS to the first app version that
#: reads the new format, in the same edit (`test_feed_formats.py` pins the pairs, so a bump alone fails). An app older
#: than the gate ignores the field (the readers take extra keys), so a bump still strands those, as before.
#: Keys: the index feeds (the files under them are fetched from the index, verified, and never on their own).
FEED_MIN_APP = {
    "dfo_feed": "0.0.0",
    "inseason_feed": "0.0.0",
    "gauge_feed": "0.0.0",
    "runtiming_index": "0.0.0",
}
