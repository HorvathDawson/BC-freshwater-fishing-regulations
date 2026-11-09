# T521 — A new DFO location with no mapped place still shows in the app

Status: DONE — c7d4226b2 + 1650a4b2f. Feeds worker NOT deployed: it ships with the next app release (until then the
live unplaced.json rows have no `items` and fail to parse, so the app reports the read and says it on the card's feed
line and in the bell).

Owner 2026-11-09: when DFO publishes a location no curated place matches, "we need to rebuild (still will show on
front end tho so it isnt missing before it is fixed and matched)".

## Today
- The feeds worker (`workers/feeds/src/dfo/live.ts`) holds such rules in `feeds/dfo/unplaced.json`, emails an URGENT
  notice and lists it on the live queue (T366). No app code reads `unplaced.json`: the rule is invisible to anglers
  until a curator places it and a release ships.

## What (owner 2026-11-09, revised)
- (a) The water IS matched, the location within it is not: the rule shows in that water's SALMON section (the Salmon
  card's "On this water (DFO)" lines, with its other DFO rules): "Location not yet on the map: <DFO's place> — <DFO's row>".
  Not the generic in-part caveat block.
- (b) The water itself is not matched (DFO adds a new water): a global notice in the bell, like the in-season notices,
  naming the DFO region and DFO's words.
- Both from the same feed read as salmon status; a failed read is shown (card feed line + bell) and reported. No paint,
  no part change. Tests for (a), (b) and zero.

## Done (c7d4226b2, 1650a4b2f)
- Worker `live.ts` + Python `live.py`: each unplaced row carries `items`, the app item ids of the water binding its DFO
  water name / alias names. Non-empty = (a); empty = (b) (`new_water`, or a binding with no registry item). DFO_REV bumped.
- App `data/feed/salmon.ts`: when index.json counts unplaced places, `unplaced.json` is read in the same fetch (and on
  the update service's `offer`), kept in the same capture; zero: not asked for. Failure: `reportFailure`.
- Core: (a) `salmonCard` appends the lines to `waterRules`; (b) `dfoNoticeRows` (salmonFeed.ts) → the bell's DFO
  section (`InSeasonNotice` `dfo`, `@app/ui` `useDfoNotice` over the same `SalmonFeedSource`, Shell + App wired), counted
  on the bell and in its label. Unread list: "(N DFO places not yet on the map could not be read)" on the Salmon card's
  feed line, and a red line + count in the bell.
- Proof: `core/src/salmonFeed.test.ts` (T521: a, b, zero, unread, parse), `data/src/feed/salmon.test.ts` (read beside
  index, capture, offer, failure reported), `ui-native/src/DfoNotice.test.tsx` (bell rows, unread, zero), worker
  `dfo.worker.test.ts` (items in both modes). No light/dark shot taken.
- Not done: the bell's DFO rows do not open the bell by themselves once per device (in-season rows do); they are counted
  on it. `pickSalmon` choosing the release's own file (release newer than the capture) shows no unplaced rows.
