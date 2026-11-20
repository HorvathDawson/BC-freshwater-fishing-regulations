# T972 — Search for "Thompson" lists "Thompson Plateau · Locality" at least five times, identical lines

Status: OPEN.

Source: L4 Android device-looks pass 2026-11-20 (T730 look, emulator).

## What was seen
Typing "Thompson" in the map search: PLACES shows "Thompson · Suburb", then "Thompson Plateau · Locality · every named
water around it" five times in a row (more under the keyboard), each line identical. Shot:
`handoff/PLAN/app-integration/L4-android-shots-2026-11-20/T730/thompson-results-duplicate-locality-rows.png`.

## Work
Find where places come from (the town / locality index): whether these are one name with several records (a locality
split in pieces, a name-variant duplicate, T932's audit) or one record listed repeatedly. One row per place a reader can tell
apart; rows that differ get something that tells them apart (the near-town or the distance). Test over the index for
adjacent rows with identical text.

Depends: —. Size: S. Owner: no.
