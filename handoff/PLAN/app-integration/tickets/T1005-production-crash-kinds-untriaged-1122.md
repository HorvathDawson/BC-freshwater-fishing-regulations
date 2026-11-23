# T1005 — Production crash reports 2026-11-15 → 11-22: about 20 kinds no ticket names

Status: OPEN — triage first (each kind: a ticket, a known cause, or "not a defect" for T595's classes).

Source: `GET /crashes?since=2026-11-15` on the production feeds worker (Bearer RUN_TOKEN), read 2026-11-22: 20 kinds,
all class `defect`; none of their messages appears in `handoff/PLAN`. The largest (count, versions, message as stored):
- 77, 0.8.4 (42): `Error: … HTTP 404`. 24 + 18, 0.8.4 (42): `in-season: could not ask this release which matched
  waters it holds: … HTTP 404`. 20, 0.8.3/0.8.4: `AJAXError: Not Found (404)`. 6: `outside mask: HTTP 404`. 4: `AJAXError
  (404)`. 3, 0.8.4: `HTTP 404 for …`. A guess to test first, not a finding: 0.8.4 phones still asking for files of a
  data release that is no longer served (releases are pruned to live + previous).
- 46, every version 0.8.1 → 0.8.5: `Wrong magic number for PMTiles archive` (a tiles fetch answered with something
  that is not a PMTiles file).
- 5, 0.8.1 → 0.8.5: an `Error:` whose message is an object (an error thrown or reported as a non-Error).
- 1–3 each: `Unable to parse the tile … Unimplemented type: 3`; `this.expression.evaluate is not a function`;
  `Cannot read properties of null` (0.8.5); `update: … Unexpected end of JSON input`; `in-season: this release's section
  lines could not be read`; `dfo feed: no feed read, so no checked time`; `Unable to open URL`; `The source image could
  not be decoded`; `AsyncRequireError: Loading module … failed`; a gauge page `cards/08GA043.json: HTTP 404`.

## Work
1. Per kind: read its sample (stack, route kind, pane, release), reproduce or explain; group kinds with one cause.
2. Each real defect → its own ticket (or folds into an open one); expected ones → T595's classes so they stop arriving
   as defects. Record the triage here, kind by kind.

Depends: T595 (classes). Size: M. Owner: no.

## Hashes (read 2026-11-22 18:15, `GET /crashes?since=2026-11-21`, production unless said)
`3d5ded1b72ff865f` (77, HTTP 404), `c920e3c59673b7ec` (47, PMTiles magic number, 0.8.1 → 0.8.5, still arriving
22:11Z), `2c766955748940d9` + `77d7b2f24fd83f8a` (24 + 18, in-season matched waters 404), `f6d0af3f051de23a` (20,
AJAXError 404), `bb863fcb6535d696` (6, outside mask 404), `1bd22eea73a9dfb8` (5, non-Error object), `b5804f0e35787dbe`
(4, AJAXError 404), `804e01a3d7d4fd97` (3, Android 0.8.4 c1f14c1, `HTTP 404 for …`), `be0617731470214c` (3, tile parse
type 3), `c77d892e10eefbd1` (1, 0.8.5, null property), `f9bf618550381745` (1, in-season section lines),
`5588b32d5eacc9e3` (1, update JSON end), `f529f96593774436` (1, expression.evaluate). STAGING `a10bd70844e3fa61` (1,
Android 0.8.3 (41)): "an exit record was refused by the crash server: HTTP 429" — the app's own exit record hit the
worker's rate limit; triage with the rest (a 429 on our own record should be retried later, not reported as a crash).
