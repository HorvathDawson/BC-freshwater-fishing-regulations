# T563 — A percentile alert follows each day's percentile, not the line of the day it was saved

Status: OPEN.

Source: Owner 2026-11-09 (owner checks page, T527): "it should be the percentile of each day so you set percentile on nov 4 then on dec 31 you want it to alert if the percentile crosses that bound for the percentile on that day not the day it was set".

## Work
1. A percentile alert stores the percentile (e.g. 75), not a value. At each check the line is that percentile for the reading's own day, from the gauge record the device holds.
2. The editor says so ("75th percentile for the day", with today's value as an example).
3. This replaces T527's as-built "kept line" and T554's item on an alert keeping the HYDAT vintage it was saved on: with the line taken per day from the current record, a new HYDAT applies by itself.
4. Existing saved percentile alerts convert (they already hold the percentile); none is dropped silently.
5. Same day rule as T550 (one day for the percentile, feed and app).

## Acceptance
Test: an alert saved on Nov 4 at p75 fires on Dec 31 when the reading crosses Dec 31's p75, and not when it crosses Nov 4's. Emulator run of the T527 checklist step 4. Custom percentiles stay whole numbers 10–90 (owner did not object).

Depends: —. Size: M. Owner: no.
