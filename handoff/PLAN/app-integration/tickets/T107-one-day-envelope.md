# T107 — One day's envelope: Python and TypeScript place the band and rank the value differently

Status: OPEN.

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
