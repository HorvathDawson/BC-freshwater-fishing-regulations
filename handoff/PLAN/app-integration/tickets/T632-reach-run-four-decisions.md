# T632 — Owner: four decisions from the reach run (T546 items 4 and 5)

Status: OPEN

Source: T546 report 2026-11-11.

1. DFO placement: subtract tidal water from a DFO place, or not? 18 places are affected.
2. Should every unplaced-piece reason stop the run, with a per-piece accept list?
3. A curated "expected to match nothing" note for overrides (today 1 of 492: Okanagan River Oxbows, 26 dead ids).
4. Six entries whose `matched` disagrees with the matcher; four hold ids a curator added. Keep the curator's, or the matcher's?

## Acceptance
- The owner answers each; T546 items 4 and 5 are finished to match.

Depends: T546. Size: S. Owner: four answers.

## Owner answers 2026-11-11
1. Tidal: always subtract, unless there is a stated reason not to for a place. Send the 18 examples for a quick double check.
2. Unplaced pieces: stop the build unless accepted (per-piece accept list).
3. Dead overrides: no "expected" note. A check makes it impossible to build with one (fast-fail should run it), then fix Okanagan River Oxbows: it matched before; find why it stopped (owner's guess: lake became a stream polygon).
4. Matched disagreements: the curator's saved match wins; send the six with both versions for a tick.
