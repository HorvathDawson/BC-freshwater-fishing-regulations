# T521 — A new DFO location with no mapped place still shows in the app

Status: OPEN — not started.

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
