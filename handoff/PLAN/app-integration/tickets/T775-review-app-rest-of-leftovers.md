# T775 — Review app, rest-of places after T761: wrong preview words, empty checklist, listed places not on the map, card notes

Status: PARTIAL — items 1-3 DONE (c77eeb601, 2026-11-22); item 4 (the PHONE app card naming a skipped listed place) not done: it is the bundle/deliver side (`pipeline/deliver`), outside the review-app lane. left (Claude): item 4, the phone card naming a skipped listed place.

Source: T761 lane report and the sweep, 2026-11-15 night.

## What is wrong
1. A rest-of whose own sections are all inside its listed places previews "binds NOTHING alone — the build refuses
   it". The build does not refuse it (it stands behind the listed places, T761). The words must say that.
2. The rest-of checklist fills only after a preview has run; on open it is empty.
3. The listed places are not drawn on the map beside the rest.
4. The app card does not yet name a listed place that was skipped (dormant or cannot be drawn); the reach
   diagnostic does (`DFO_REST_SKIPS_A_LISTED_PLACE_THAT_DOES_NOT_BIND`).

## What to build
The four fixes above; route table regenerated if a route changes; worked in a worktree, never under the owner's
running app.

State 2026-11-22 (stopped for credits): see Status above; code c77eeb601, ledger f087c8968; branch fix/T775-review-app-leftovers. fast-fail not confirmed (was still running).
