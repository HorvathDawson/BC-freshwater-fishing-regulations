# T1005 — Production crash reports 2026-11-15 → 11-22: about 20 kinds no ticket names

Status: IN PROGRESS — triage recorded 2026-11-22 (lane L4), app fixes 127296a64, fe477309b; left (Claude): the worker's classes below (lane L5, `app/workers/feeds`), then a `GET /crashes` read that lists only T1014/T1016/T1017's kinds

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1005 | Production crash reports 11-15 → 11-22: ~20 kinds no ticket names (0.8.4 404s, PMTiles magic number, …): triage | IN PROGRESS — triaged kind by kind (11 of 20 from one search-engine renderer); app fixes in; left (Claude): the worker's classes (lane L5) | 127296a64, fe477309b | lane L5: the worker's classes (T1005 "What the worker needs"); then a /crashes read |

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

## Triage (lane L4, 2026-11-22, read `GET /crashes?since=2026-11-15` and `?class=network,expected` on production)

**One install, `a916f6f81cd61f75`, is a search engine's page renderer, not a reader.** It sent 11 of the 20 defect
kinds (202 of their 220 reports) and most of the network / expected ones (`Failed to fetch` ×212, three "release r-… is
not one this app reads" kinds ×35). Evidence from the stored samples: every report `created` at 00:00:00–00:00:04 UTC
(a pinned clock); one install id across 4 days and every web version 0.8.1 → 0.8.5; on 2026-11-22 it booted manifests
naming r-fc137c4040fa and r-6258ffded676, no longer named by our pointer (its own cache, not honouring `no-cache`); it
lands on the SEO `/water/` pages. Not proven by a user agent (the worker keeps none).

| kind(s) | count / installs | cause | action |
|---|---|---|---|
| `c920e3c59673b7ec` PMTiles "Wrong magic number" (47), `be0617731470214c` tile parse "Unimplemented type: 3" (3) | a916 only | The renderer answers a byte-range request with bytes cached for ANOTHER range of the same URL: the header read at offset 0 gets tile bytes (wrong magic), a tile read gets the wrong slice (MVT parse fails). Checked: every archive on R2 answers `Range: bytes=0-16383` with 206 and the `PM` magic; pmtiles' own `FetchSource` throws "Bad response code" on any HTTP ≥ 300, so this needs a 2xx with foreign bytes. No reader has ever sent it. | crawler class (L5 1); app now names the platform `web (crawler)` (127296a64) |
| `3d5ded1b72ff865f` `… HTTP 404` (77), `2c766955748940d9` + `77d7b2f24fd83f8a` in-season matched waters 404 (42), `f6d0af3f051de23a` + `b5804f0e35787dbe` AJAXError 404 `light@2x.png` (24), `bb863fcb6535d696` outside mask 404 (6), `f9bf618550381745` in-season section lines (1), `1359978074f322d3` dfo no checked time (1) | a916 only | The guess tested: files of r-fc137c4040fa and its tile folder `4ae7b62955bc9990.9674331adca9` (atlas, `bc_outside.geojson`, sprites) all 404 on production now; the release was pruned when r-fec3fab4c5aa went live 2026-11-21 02:00 UTC (previous = r-6258ffded676), and the 404s start 02:20. But the asker was the renderer above holding a cached r-fc137c4040fa manifest, not a 0.8.4 phone: the Android 0.8.4 phones report r-fec3fab4c5aa, which is still served. | crawler class (L5 1). A READER's tab open across two releases would meet the same 404s: T1016 |
| `240b0d41d6fc2c88` AsyncRequireError (1) | a916 | a chunk of an older site deploy, from the renderer's cache | crawler class; the reader's twin (a tab open across a site deploy) in T1016 |
| `1bd22eea73a9dfb8` Error whose message is an object (5) | 5 installs, web, every version | MapLibre's `_setupPainter`: no WebGL ("Failed to initialize WebGL", thrown as JSON with the context's details; the scrubber reduced its quoted values to dots). It threw out of the map's effect: a blank map, nothing said. | FIXED 127296a64: the map says why (core `MAP_UNAVAILABLE_WORDS`) and reports one `MapUnavailable: Failed to initialize WebGL (…)` (a map with no `onError`, MiniMap, through `reportFailure` once a page: fe477309b); L5 2 classes it `expected` (the browser) |
| `c77d892e10eefbd1` TypeError null `getSource` (1) | web 0.8.5 | `useMarks`' cleanup ran on the map a rebuild had removed (the archives changed: a release applied in the session; the map instance's own cleanup removes the old map a microtask later, the marks' effect is re-run at the next render) | FIXED 127296a64 (`markers.ts`: clears only the map still in use; `markers.render.test.tsx` fails on the old code) |
| `804e01a3d7d4fd97` `HTTP 404 for …` in `readJson` (3) | 3 Android 0.8.4 (c1f14c1, production) phones | The Play update check (T894): Play offers versionCode 43 (0.8.5 build 43, a STAGING-data build put on Play Internal + Closed), and the app reads `data.canifishthis.ca/app/android/play/43.json`: 404 (it exists only on the staging bucket; production has 42.json) | new ticket T1014 (the version file of every Play upload on both buckets) |
| `5588b32d5eacc9e3` `update: … Unexpected end of JSON input` (1) | web 0.8.4, a no-WebGL browser | a feed index whose body was cut on the line (truncated JSON) in the update service | network class (L5 3) |
| `04ee0761cc3b8f45` in-season matched waters "no whole file in # ms" (1) | web 0.8.3 | the resilient download's own stall timeout: a slow line | network class (L5 3) |
| `9e825d06c018b12a` "The source image could not be decoded" (4) | 2 installs, web 0.8.1, both also in the `Failed to fetch` kind | a sprite/image body cut on a failing line; not seen since 0.8.1 (11-19) | network class (L5 3); reopen if a later version sends it |
| `26c207729b758da7` gauge page `cards/08GA043.json` 404 (1) | the gauge page, 11-19 05:29 | the SEO gauge page went up before the feeds worker drew that station's card; the file is there now (200) | none: transient, resolved |
| `f529f96593774436` `this.expression.evaluate is not a function` (1), `7db2e9aa9ab7dbbc` iOS "Unable to open URL" on Runs (1) | one each | not located from the minified stack / the scrubbed URL | new ticket T1017 (locate with the source maps T1008 serves) |
| STAGING `a10bd70844e3fa61` "an exit record was refused by the crash server: HTTP 429" (1) | Android 0.8.3 | the app reported its own rate-limited send as a new error kind | FIXED 127296a64 + fe477309b: the day limit's 429 is kept and sent again at the next start, said in the log line, not reported; sends are spaced ≥ 1.1 s (the worker's 1 s gap), exit records go oldest first and the mark passes only stored ones; a gap 429 is reported |

## Done
- App fixes 127296a64, fe477309b (see the table).
- Worker classes, lane L5 PR #39 (3452aae26, dc338949b; `app/workers/feeds/src/crashes.ts`): a `crawler` class (platform
  `web (crawler)` / `web (automated)`, or a crawler `User-Agent` stored only as a verdict; a per-kind count, so a kind is
  `crawler` only when every report came from one); EXPECTED += `/Failed to initialize WebGL/` (`MapUnavailable`) and the
  day limit's 429; NETWORK += `/no whole file in \d+ ms/`, `/^update: .*Unexpected end of JSON input/`, `/The source image
  could not be decoded/`; the gap 429 and `HTTP 404 for …/app/android/play/` (T1014) stay `defect`. Kinds stored before
  keep their old class until they age out (90 days), the 11 a916 kinds included. Staging runs 9b90767a4.

## New kind (read 2026-11-22 23:00)
`9621cbcdb5053a66` (1, web 0.8.5, route map): `SyntaxError: Unexpected end of JSON input` at `JSON.parse` inside a
`Promise.all` of the boot reads — a cut response body, the same cause as `5588b32d5eacc9e3` but without the `update:`
prefix, so the worker's NETWORK pattern (`/^update: .*Unexpected end of JSON input/`) misses it. Decide: the app names
the file in the message (no bare JSON.parse error), and the worker classes it network.

## Left
- The new kind above (app names the file; worker classes it network).
- Then a `GET /crashes` read that lists only T1014/T1016/T1017's kinds.

Depends: T595 (classes). Size: M. Owner: no.
