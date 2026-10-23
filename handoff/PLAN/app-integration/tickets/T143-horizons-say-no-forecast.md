# T143 — Conditions +1/+3/+5/+10 say "no forecast" for every gauge (Change works)

Status: OPEN

Source: Owner 2026-10-23 (app walk). "when i click on +1, +3, +5, +10 it says no forecast for gauges here even tho there definitely is. this is for
every gauge. but the change option works." Likely since T123 (donorState: a horizon with no percentile → "no-forecast")
or a feed/app format mismatch (T106 declared quantity, T126; the feed republish is still pending on OWNER-TODO).

## Work
Reproduce on the owner's live data path (the feed the app actually reads), find where the horizon percentile goes null
(feed index `ahead`, `reading(row, quantity, horizon)`, the quantity asked, T123's callers), fix at the one function,
and pin with a test that runs on a real feed index (not a fixture that already matches the code). If the app needs a
feed republish, make the app read BOTH shapes until it is published and say so in OWNER-TODO.

## Acceptance
Every horizon chip shows the forecast percentile where the feed has one; a gauge without one says so. Web shot of a
real gauge at +1/+3/+5/+10. Size: S. Priority: high (live regression).
