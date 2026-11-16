# T759 — A DFO place missing from the live feed can make a water read MORE OPEN than DFO says

Status: OPEN (before closed testing; found 2026-11-15; fix proposed, owner to confirm the reading)

Source: main chat 2026-11-15, after T758 (the worker feed lacked the region-default places). Read-only trace with real
data: `handoff/PLAN/curation-review/feed-missing-location-2026-11-15.md` (file:line for every case).

## The defect
The app composes, per section, a chain of DFO places (narrowest first) with today's live feed. `composeChain`
(app/packages/core/src/salmonFeed.ts:279) drops a place the feed lacks and lets the next, wider place answer (:288). In
68 of the release's 435 chains, removing the narrowest place makes some species or day read looser (verified on release
r-f749475d0962).

When a place goes missing:
- DFO drops its row: Python feed silent, worker "gone" + notice; the wider place answers. Right if DFO really lifted the
  restriction; FAILS OPEN if the page was misread.
- DFO rewords the row and the best match is still this place: Python feed drops it from `locs` (FAILS OPEN; a test,
  test_dfo_live.py:115, asserts this as intended); the worker reads "check" (live.ts:570-582).
- DFO rewords the row and nothing matches, or a sibling place matches better: both feeds let the wider place answer.
  FAILS OPEN; the card shows only an "unplaced" caveat.
- A place id renamed between release and feed, or a release newer than the worker's entry copies: FAILS OPEN, silent
  (inferred from code, not run).
- Page fetch fails / row unparseable / one region's places all missing: fails closed or reads "not known". Fine.

Worst case found: DFO rewords the Campbell River "Maple Street to the cement block" closure, a sibling place matches
better, and the section reads open ("4 hatchery-marked") on Sep 1 instead of closed.

## Proposed fix (proposal; the owner confirms the reading)
1. Feeds (worker live.ts:576-585 and Python live.py, same rule): a curated place the feed would list as gone reads
   "check" (not known) when the same water has an unplaced live row, or when the place's last answer was stricter than
   what would answer in its stead. A place DFO plainly removed with no unplaced row on its water still goes.
2. App backstop (salmonFeed.ts): a release-named place absent from a feed region that was carried and not held reads
   "not known, check the DFO notice", reported once (reportFailure). Never silently the wider place.
3. Tests: no-candidate reword, sibling reword, renamed id, release newer than the feed; the Python test that asserts
   the fail-open is rewritten.

## Owner question
When DFO's page no longer shows a restricting place and our reader cannot tell "removed" from "reworded", should the
app say "not known, check the DFO notice" for that stretch (recommended) or follow the wider rule?

## Who
Python feed: main chat. Worker and app: the iOS chat (with T757/T758). Not part of Play build 33.
