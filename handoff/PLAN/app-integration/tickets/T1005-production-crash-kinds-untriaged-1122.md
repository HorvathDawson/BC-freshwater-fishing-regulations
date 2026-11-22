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
