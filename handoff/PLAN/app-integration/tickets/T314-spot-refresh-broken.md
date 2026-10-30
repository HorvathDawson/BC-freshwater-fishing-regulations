# T314 — Spot data refresh doesn't work in 0.3.0

Status: IN REVIEW — fixed + unit-pinned (21d047df8); emulator check pending (owner 2026-10-31, on the 0.3.0 APK: "the refresh for spot data isn't working").

## Work
Reproduce on the emulator (0.3.0 build, staging): a spot saved offline or with pending weather/flow, then online → refresh
(the spot page's refresh / the app's reconnect backfill, T294 refresh across visits, T307 fast-fail backfill). Find why it
doesn't fill (T307's fastFail answering "failed" permanently? online detection never flips? refresh only for visit 1?
paint v5 / release swap?), fix, and pin with a test (pending → online → filled). Check both spot-level and visit-level.

Depends: T294, T307. Size: S. Owner: no.

## Done (21d047df8)
**Cause (reproduced in a unit test, capture → refresh):** a spot/visit saved OFFLINE on its own day kept its station but no
reading, and — the feed index unreachable — no panel at all. A refresh filled weather + the week before but never the
gauges: the visit day's flow is `pending` ("not over") all day, nothing re-read the live reading, and the panel's members
were never kept, so there was nothing to read. The Refresh button / reconnect refresh ran, changed nothing visible.
**Fix** (`data/spots/dayFlow.ts` `pendingPanel` / `liveFillable` / `fillLive`; capture + `captureVisit`; `refresh.ts`):
offline capture keeps the panel's members as `panelDay: pending` (on-device lookup); a refresh ON the visit day reads the
panel over the live index and the lead station's reading (only a reading observed that day; marked "filled in
afterwards"); after the day, the day's record fills them (`fillDayPanel` / `fillDayFlow`, as for a backdated spot).
`savedGauges` says "pending" for such a spot until then. `useSpots.refresh`: one spot's failed fill no longer stops the rest.
**Pin:** `data/spots/refresh.offline.test.ts` — offline today → online → reading/panel/weather/week filled, needsRefresh
false; still offline → unchanged; next day → day's record, never the live reading; a visit added offline the same.
Mutation (no `fillLive`): 3 of 4 fail.
**Left:** emulator: spot saved in airplane mode → online → filled (`spots` scenario), in the next heavy.sh batch.
