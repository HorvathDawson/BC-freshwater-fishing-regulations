# T453 — Flow alerts set by the day's percentile, not only a fixed value

Status: OPEN (owner 2026-11-06, chat 5).

Owner: "improve notifications so that you can set rise and fall threshold with respect to the percentile of flow
instead for that day."

## What
T234's alerts (`@app/core` `alerts.ts`) have four kinds: rises above Y · falls below Y · rises by Z % in 24 h · falls by
Z % in 24 h. Add two that follow the season: **rises above the Pth percentile** / **falls below the Pth percentile** for
that day of the year. "High for the season" in May and in September are different flows. A fixed m³/s cannot say that.

## Work
1. Rank the newest reading with the climatology the app already ranks every reading in (core `percentileInBand`, the
   feed's `percentile_of`; T170 uses the same envelope). There is one reader, so the alert and the Conditions page give
   the same answer.
2. Two new kinds in `alerts.ts`: percentile pickers in the alert sheet, with the same cadence, hysteresis and
   once-per-crossing rule as T234 v1.1. The sentence is composed in core and the test pins the whole string (rule 33).
3. A gauge with no climatology for that day, or a level-only station if the envelope is flow-only, does not offer the
   kind. It is never answered with a guess (no silent fallback, rule 46).
4. Tests: a crossing that happens at the same flow on two different days fires on one day and not the other. Also cover
   a missing envelope day, and a migration of saved alerts (none change).

## Open questions (defaults until the owner answers)
- Q-T453a: does the picker offer free percentiles or the app's own words/bands (e.g. "above normal", "high")?
  Default: the app's band words, so the alert matches what the Conditions page says.

Depends: T234 (POSTPONED; this lands when alerts resume). Related: T170, T52. Size: S–M. Owner: phone check.
