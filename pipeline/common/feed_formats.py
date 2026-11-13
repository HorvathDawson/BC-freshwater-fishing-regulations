"""THE FEED FORMATS: the one place a live feed's format string is typed (T556, AGENTS 40).

A feed is written by one program and read by another, in another language, at an UNVERSIONED key
(`feeds/dfo/index.json`, `feeds/inseason/matches.json`, …), and the reader refuses any format but its own. The string
was hand-copied on each side — `dfo-feed/3` in `regs/dfo_salmon/live.py`, the worker's `dfo/live.ts` and core
`salmonFeed.ts` — and a bump made on one side only broke staging once (2026-11-09: the pipeline published
`inseason-links/3` while the deployed worker still required /2).

Every Python writer reads its format from `FEED_FORMATS`; `python -m pipeline.tools.emit.feed_formats` writes
`app/packages/core/src/feedFormats.generated.ts`, which the app and the feeds worker read (`--check` in CI, `pnpm
check` and fast-fail). `scripts/check-format-pairs.py` refuses one of these strings typed as a literal anywhere else.
A bump is ONE edit here plus the emit; the release's app-version gate and the deploy order are still the bumper's to
think about (a published format an installed app refuses reads as "no feed").
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
