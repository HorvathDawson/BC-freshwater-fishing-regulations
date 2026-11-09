# T521 — A new DFO location with no mapped place still shows in the app

Status: DONE — c7d4226b2. Feeds worker NOT deployed: it ships with the next app release (until then the live
unplaced.json has no `items`, so the app reports the read and says it on the feed line).

Owner 2026-11-09: when DFO publishes a location no curated place matches, "we need to rebuild (still will show on
front end tho so it isnt missing before it is fixed and matched)".

## Today
- The feeds worker (`workers/feeds/src/dfo/live.ts`) holds such rules in `feeds/dfo/unplaced.json`, emails an URGENT
  notice and lists it on the live queue (T366). No app code reads `unplaced.json`: the rule is invisible to anglers
  until a curator places it and a release ships.

## What
- The water page of the water the DFO row names (the worker already knows the water/region) shows the rule in the
  unplaced block (same block as the regs' own unplaced rules, `caveats.ts`): "DFO rule, location not yet on the map:
  <DFO's own wording>". No paint (we do not know where), no part change.
- One load path: the same feed read that fills salmon status; a failed read is reported, never silent.
- Proof: a golden feed with one unplaced row shows it on its water (light/dark shot); 0 unplaced shows nothing.

## Done (c7d4226b2)
- Worker `live.ts` (and the Python `live.py`, same field) gives each unplaced row `items`: the app item ids of the
  water binding its DFO water name / alias names ([] for `new_water`). `DFO_REV` bumped so carried regions re-type.
- App `data/feed/salmon.ts`: when index.json counts unplaced places, `unplaced.json` is read in the same fetch (and on
  the update service's `offer`), kept in the same capture. Zero: not asked for. Failed read: `reportFailure` + the card's
  feed line "(N DFO places not yet on the map could not be read)".
- Core `salmon.ts`: the salmon card's in-part caveat (`inPartBlock`, caveats.ts) gets one item per DFO row:
  "DFO rule, location not yet on the map: All · Aug 1 until further notice · No fishing for salmon", Where: DFO's place.
  No paint, no part change, never folded behind "Other dates"; "DFO does not list this water" is not said beside it.
- Proof: `core/src/salmonFeed.test.ts` (T521 block: one row shows on its water, rows + map status unchanged, other
  water none; zero shows nothing; unread said), `data/src/feed/salmon.test.ts` (read beside index, capture, offer,
  failure reported), worker `dfo.worker.test.ts` (items in both modes). No light/dark shot taken (needs a dev feed
  with a row; the block is the existing CaveatBlock).
- Not covered: a `new_water` row (DFO water no binding knows) has no water page, so it shows nowhere in the app —
  still on the live queue + URGENT email. Owner decision if it should show somewhere (e.g. a region line).
