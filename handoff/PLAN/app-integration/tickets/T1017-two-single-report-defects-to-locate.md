# T1017 — Two one-report defects to locate: `expression.evaluate is not a function` (web) and iOS "Unable to open URL" on Runs

Status: OPEN — next release batch.

Source: T1005 triage (production `GET /crashes`, 2026-11-22). Neither could be placed from its minified stack or its
scrubbed message; the web one needs the source maps T1008 serves, the iOS one the link it tried.

1. `f529f96593774436` (web 0.8.4, 2026-11-21 02:50 UTC): `TypeError: this.expression.evaluate is not a function` in
   MapLibre's `updatePaintArray` ← `setFeatureState` ← `coalesceChanges`: a feature-state write landed on a paint
   property whose value is a plain constant (no expression object) — likely a look change replacing a data-driven paint
   with a constant while feature-state writes are queued. Map it with the 0.8.4 source map, find the caller, fix it
   where the paint is set; a test with a feature-state write after the paint changes.
2. `7db2e9aa9ab7dbbc` (iOS 27.2.1, 0.8.3 build 40, route `runs`): `Unable to open URL: …` — `Linking.openURL` refused a
   link from the Runs tab (a source's link: PSF / DFO). List every link Runs can open and try each on the iOS
   simulator; fix the one iOS refuses (a scheme, an unencoded character), and the error says which link.

Depends: T1008 (source maps) for item 1. Size: S each. Owner: no.
