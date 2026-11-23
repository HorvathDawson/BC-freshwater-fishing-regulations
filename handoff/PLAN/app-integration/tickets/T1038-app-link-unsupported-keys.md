# T1038 — Carry a phone link's date, tab, sheet, scroll, age, res, horizon and whole (today they are reported and dropped)

Status: OPEN — 0.9.0 batch.

Source: lane L15 on T1034, 2026-11-23. The web start link carries these keys (`parseStartLink`); the phone's link path
(`parseAppLink`) did not, and listed them as read while dropping them. T1034 now REPORTS each one (`reportFailure`: "<key>=
is not opened by the phone yet (T1038): dropped") and opens the water without it, so nothing is dropped silently.

## Work
1. Carry `date` (the Shell's day) and `tab` first (the most useful), then `sheet`, `scroll`, `whole`, `horizon`; `age` and
   `res` set the angler. Each through the same state the web start link sets (`StartLink` → Shell props), one load path.
2. Remove each key from `UNSUPPORTED_KEYS` (appLink.ts) as it is carried; a test per key.

Depends: T1034. Size: M. Owner: no.
