# T107 — One day's envelope: Python and TypeScript place the band and rank the value differently

Status: DONE — 0bb3ea68 (review 796efa34)

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding gauges 3 =
cross 3 (CONFIRMED in code).

## Decision being unified
"What is today's normal envelope for this station, and where does today's value rank in it?"

## Where it is decided today
- Python `pipeline/gauges/feed/publish.py:514` `band_for`: pentad centres (`pos = (yday-1)/5 - 0.5`), wraps at year end.
- TypeScript `app/packages/core/src/flow.ts:183` `bandAt`: pentad starts, no centre offset, no wrap.
- Ranking: Python `percentile_of` clamps to 0.05/0.95; TypeScript `placeInBand` returns "below"/"above".

How they differ: a half-pentad shift every day and no wrap over New Year. Measured on 08MF005 day 109: 1,818 vs 2,058.
The feed's percentile and the app's band can disagree on the same day.

## Work
- Pick one owner of the formula: either the feed ships today's band (TypeScript stops computing it), or `bandAt` ports
  `band_for` exactly, including the wrap. Then one ranking: `placeInBand` matches `percentile_of`'s clamp, or the
  app reads the feed's percentile.
- Pin: a one-formula-style parity test that runs the Python over all 366 days for a few stations and compares the
  TypeScript output (extend `app/tools` one-formula).

## Acceptance
The parity test is equal on 366 days × the fixture stations, including Dec 31 → Jan 1; it fails on a mutation that
drops the centre offset.

Rebuild: none (app), or a feed publish if the feed ships the band. Depends: none. Size: S. Owner: no.

## As built (2026-10-23, 0bb3ea68)
Owner of the formula: the feed's Python (`band_for`, `percentile_of`, unchanged). The app ports it; nothing is
published differently, so no rebuild and no feed publish.
- `app/packages/core/src/flow.ts` `bandAt`: line-for-line port of `band_for` — `pos = (day-1)/5 - 0.5`, the two
  nearest pentad centres, wrap mod 73 (`PENTADS`), one-side-missing falls back. Callers unchanged
  (`data/src/bundle/shared.ts` chart bands, lead-up `yearBand`).
- `percentileInBand` (new, exported): line-for-line port of `percentile_of` — at or past p10 → 0.05 (`BELOW_BAND`), at
  or past p90 → 0.95 (`ABOVE_BAND`), linear between knots. THE ranking.
- `placeInBand` is no longer a second ranking: it calls `percentileInBand` and only words the clamp ("below" /
  "above") and rounds to a thousandth. Saved-spot `DayPlace` shape unchanged. Behaviour change: a value EXACTLY on
  p10 / p90 now says "below" / "above" (the feed's 0.05 / 0.95), where it said the 10th / 90th.
- Pins: `app/tools/one-formula.test.ts` "one day's envelope, two languages (T107)" runs the real Python and the
  TypeScript over all 366 days (2024) × 4 fixture stations (snowmelt, ramp, thin pentads incl. both sides of New
  Year, dry creek with flat zero knots) and 16 values per day; bands and ranks compare EXACTLY (toEqual, no
  tolerance). "one day's envelope has one home": no non-test app source but `flow.ts` states the knot list or
  `(day - 1) / 5`. Unit pins in `core/src/flow.test.ts` (day 4 = [11,…], Dec 31 / Jan 1 between pentads 72 and 0)
  and `core/src/leadup.test.ts`.
- Mutations run (each fails the named tests, restored after): drop `- 0.5` → parity "same band" + flow.test
  centres + wrap; clamp instead of wrap → parity "same band" + flow.test wrap; `<` for `<=` at p10 → parity "same
  rank" + leadup.test edge; a pasted `(dayOfYear - 1) / 5` in `data/src` → "one home".
- Checks: targeted vitest (one-formula, core flow/leadup, data spots) green; strict `REGS_STORE_REQUIRED=1 pnpm -C app
  check` via heavy.sh: every gate through typecheck green, tests 7217 passed / 5 failed — all 5 are the regs
  conformance release tests (T02/T03/T07/T08/T09) failing on the shared store digest mismatch (regs.sqlite
  43115d8c… vs oracles a2307766…, "re-run app/design/regs-v11/port/taps.py"), unrelated to this change. The parity
  test needs `.venv/bin/python` at the repo root; it skips without it (as the existing one-formula tests do).

## Adversarial review (2026-10-23)
Fixed:
- WRONG WORDING on the edge. The as-built change made a value ON p10 / p90 rank 0.05 / 0.95, but the lead-up words still
  said "below the 10th percentile for the day" / "<p10" (and "above" / ">p90") — false for exactly the case the ticket
  created, e.g. a dry creek at 0 on a day whose p10 is 0. `data/src/spots/words.ts` now says "at or below the 10th
  percentile for the day" / "at or above the 90th…" and the chips "≤p10" / "≥p90" (the feed's own docstring: "at or
  beyond the bottom tenth"). Whole-string pins updated in `data/src/spots/leadupWords.test.ts`, `ui/src/leadUp.test.ts`,
  `ui-native/src/SpotScreen.test.tsx`; new pin "says a value ON p10 / p90 truthfully" ranks 0 and 1.7 on
  `[0, 0, 0, 0.4, 1.7]` through `placeInBand`. Mutations: "below the 10th" back → that pin + "says each day whole" fail;
  "<p10" back → that pin + "draws each day as a value…" fail.
- "One home" scan was narrow: it only caught the knots spelled `0.10, 0.25…` and the pentad position under the names
  `dayOfYear` / `yday` / `day`. A copy is written by someone who did not know about the first, under their own name.
  Now any spelling of the knots (`.1, .25, .5, .75`) and `(<any name> - 1) / 5`. Mutations: appending
  `(doy - 1) / 5` or `[.1, .25, .5, .75, .9]` to `data/src/spots/leadup.ts` fails "one home"; neither did before.
Checked and found sound:
- Port fidelity: `bandAt` / `percentileInBand` against `publish.py` `band_for` / `percentile_of` line by line (centre
  offset, mod-73 wrap, one-side fallback, `<=`/`>=` edges, linear segment search; the `span == 0` branch is unreachable
  in both, since a value on a flat segment's `lo` is caught by the segment before it or by the p10 clamp).
- Mutations re-run, each failing as claimed: drop `- 0.5` (parity band + 2 flow.test); clamp for wrap (parity band +
  wrap test); `>` for `>=` at p90 (parity rank + leadup edge test); `1 - frac` (parity band + 2 flow.test); a missing
  side returning null (parity band + fallback test); `placeInBand` not rounding (leadup "rounds nothing else away").
- The parity test really runs: it needs `.venv/bin/python` at the checkout root and SKIPS without it — in a worktree
  that has no `.venv` it is silently skipped. Run with the venv linked in: all 3 parity tests execute and pass.
- Missed call sites: none. Every `bandAt` caller (`data/src/bundle/shared.ts` year + hourly series) and every
  `placeInBand` caller (`data/src/spots/leadup.ts`) goes through flow.ts; no other band ranking in app source
  (`ui/hydrograph.ts` only draws `b[0]`/`b[4]`; the gauge dots and Conditions read the feed's published percentile).
- Day of year: the two app `dayOfYear(iso)` helpers (shared.ts, leadup.ts) are the true calendar day from the UTC date,
  the same as the feed's `now.timetuple().tm_yday` on a UTC `now` — Mar 1 is 60 in a common year in both, and the 366-
  long `yearBand` is indexed consistently. (`core/statusIndex` `dayOfYear` is the regulations' leap calendar and is not
  used for envelopes.)
- Platform split: `check-platform` green (18 variant sets). Typecheck green.
- Strict `REGS_STORE_REQUIRED=1 pnpm -C app check` via heavy.sh: every gate through typecheck green; tests 7218 passed /
  5 failed — the same 5 regs conformance release tests (T02/T03/T07/T08/T09) on the shared store digest mismatch
  (43115d8c… vs a2307766…), unrelated.
- Not a formula difference, noted only: the app's pentads come from the bundle as float32, the feed's from its envelope
  JSON, so the two bands for one station can differ in the 7th significant figure. Parity is of the formula.

## Owner questions
- Exactly-on-the-edge reading: the feed's `percentile_of` uses `<=` / `>=`, so a value equal to p10 ranks 0.05 and a
  lead-up day now reads "at or below the 10th percentile for the day" / "≤p10" (review: was "below" / "<p10") — including a dry creek at 0 with p10 = 0. Default
  built: follow the feed (as the ticket asked). The alternative is `<` / `>` in BOTH (`percentile_of` and
  `percentileInBand`; the parity test keeps them together), which needs a feed publish.

## Left / follow-ups
- Spots frozen before this change keep their stored `dischargePlace` / `levelPlace` (ranked against the old,
  half-pentad-shifted band); nothing re-ranks them. Harmless unless the owner wants frozen days recomputed.
