# T737 — Web: the first river click waits for the background download of the whole release files

Status: IN PROGRESS — a43e750b9, e974cabf7, 673b1d8e3, bf23b3819, b35f91e0b, d29bb35ca, 1a6b29c17; fix on branch and deployed with the websites; the smoke first-tap step is built (opt-in); left: after 0.10.0, a lane measures the tap ahead of the start-up downloads and brings him the numbers before anything is built (owner 2026-11-28); owner: see ## Owner (the speed-up list). (Earlier note: IN PROGRESS (fix a43e750b9; unnamed-stream background order e974cabf7; multi-region water 673b1d8e3; the ahead lane + unnamed reads together bf23b3819, b35f91e0b: all on `redesign/stream-sections` (2026-11-14/15) and on the staging and production websites since; the smoke's first-tap step (item 4) is built (lane L3 2026-11-20, d29bb35ca); left (Claude): main chat review + merge; region slices of regs-sid/down/lines are the owner's call))

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T737 | Web: the first river click waits for the background download of the whole release files (owner: "The first river click takes a very long time to open"; 8-9 s on staging when clicked 3-6 s after the map paints) | IN PROGRESS — tap first, start-up downloads after (owner 2026-11-28): measure first, numbers to him before building | a43e750b9, e974cabf7, 673b1d8e3, bf23b3819, b35f91e0b, d29bb35ca, 1a6b29c17 | after 0.10.0: measure the tap ahead of the start-up downloads, numbers to him before building; the easy speed-ups list to him |

Source: owner 2026-11-14 evening, verbatim: "On staging.canifishthis.ca  The first river click takes a very long time
to open". Staging = b2dba0297 (app 0.7.10) on release r-f749475d0962. The staging smoke's "tap a named water" step said
0.9 s: it taps only after the Salmon and Run timing steps (well over 15 s after the map painted), when the background
download is over.

Owner 2026-11-14 night, verbatim: "could we slow load the stuff needed for unnamed streams in background without it
effecting the lighthouse seo scores?" Owner 2026-11-15 on live staging (dff2c7a22), verbatim: "i just tried it again and
iot still feels slow" — then: "i clicked a section of the fraser in region 2".

## What
Cause (fixed): a tap's read went through `ReleaseLoader.dbFor`, which read the WHOLE file whenever it was "here or on its
way" (`dbs.has(name) || ahead.has(name)`), so a tap in the first seconds after paint waited for the background prefetch's
2.5 MB `water` or the 1.5 MB `core` instead of its region's 0.17 MB slice; then geo / access in series. Not a regression
(rule from T256, 2026-10-29, with T253/T238).

Done (web only; nothing on a page changes):
- a43e750b9: `loader.ts` `wholeHere` counts a whole file only once opened or its prefetch FINISHED; `prefetch.ts` idle
  core fetched ahead, then opened (tests: `loader.test.ts` "(T737) a whole file still fetched ahead does not hold a
  tap"; `prefetch.test.ts` "(T737) idle core is fetched ahead, then opened"; mutation-checked). Desktop Fraser 3 s after paint 3.6 → 1.7 / 0.8 / 1.0 s; search → Open 3.3 / 5.4 → 1.0 /
  1.9 s.
- e974cabf7: background order per region (named-tap files, then regs-sid and down, then the whole water; `lines` not
  ahead); nothing new starts while the reader downloads. Lighthouse unchanged on every page measured (SEO is page markup
  only; only Performance on `/` can move).
- 673b1d8e3: only a FINISHED open counts (the in-season check's whole-core open held the tap); `useRegulationKeys`
  dropped from `useShellNav` (`useWaterRegionsAhead`); `warmWaterPage` asks `geo` with the slice; `READER_GRACE_MS` 1.5 s.
  The two swallowing catches are listed in tools/failures.test.ts FINE with their reasons.
- bf23b3819, b35f91e0b: THE AHEAD LANE (`release/lane.ts` in `drivers/source.web.ts`): every file ahead down one lane, one
  at a time, `fetch` priority `low`, aborted after `LANE_JOIN_MS` (150 ms) when a reader download starts, restarted after
  `LANE_GRACE_MS` (1.5 s); `BACKGROUND_DELAY_MS` 4 s → 1 s; `warmUnnamedPage` asks regs-sid, down, the water slice and access
  together; the address bar's and Share's `lines` fetched ahead (`share.ts`). Desktop unnamed 3 s 3.6-5.3 → 2.4-4.0 s;
  named and 8 s taps unchanged (0.8-1.9 s at 3 s, 0.1 s at 8 s). Lighthouse `/` Perf 52 → 54, SEO 92 → 92.
- d29bb35ca (L3 2026-11-20, item 4): `app/tools/smoke-staging.mjs --only firsttap` (OPT-IN, not in the default run: a
  timing measure on a slow line must not stop a deploy): Fraser Region 2 part (`CPR Bridge at Mission →`) 3 s after
  paint, then a pan to Lillooet (Region 3, `… Bridge River Rapids →`); readable > 4 s WARNs, > 10 s FAILs. Staging
  2026-11-20: Region 2 0.3 s readable / 0.6 s complete, PASS; Lillooet 0.1 s / 0.1 s but WARN: 7 sweep taps came first
  (the pan leaves the camera tens of pixels off).
Measurements, scripts, JSON: `~/bcfish-session-backup-2026-11-12/first-click-web/` (`results.md`, `multi-1115/`
`measure.mjs --complete --initiators`, `lane-1115/`).

## Work (left: the owner's call, see ## Owner)
- An unnamed tap at 3 s is still the two whole files' 2.4 MB on the line (`regs-sid` 1.25 MB, `down` 1.17 MB; ~2.5 s at
  ~16 Mbit/s): at 3 s nothing is fetched ahead yet (the lane yields to the boot's own reads, incl. the in-season check's
  whole core). `access` (0.2 MB, a whole bin file) for both kinds of page; `lines` (1.3 MB) only for the address
  (unnamed) and a part-covering in-season notice (named). A slow line costs water slice + geo (0.75 MB) before the
  page. Only region slices (or fetching them before the boot reads) get it to ~1 s.
- Region slices (option c, not built — a pipeline/release-format change). Measured on r-f749475d0962 (block-level,
  today's handle order): a region's handles touch 13-78 % of regs-sid's 479 blocks and 9-88 % of lines' 303 (region 2:
  51 % / 69 %), because handles are ordered by stream key, not by region — so a block slice saves about half (region 2:
  regs-sid ≈ 0.63 MB). Re-keyed per region (only the region's handles, plus a handle → slot index) the files would be
  ~4-30 % of the whole (region 2 has 72k of 1.96M handles). `down` is per section with the downstream walk (crosses
  regions to the mouth). Touches pipeline `slices.py`/regs store, the contract (WEB_ONLY, budget), `loader.dbFor` routing
  for regs-sid/down, the release-budget pins. Ticket-sized.
- A page shell with the name at 100 ms: the click shows the part lit on the map at once, but the page is shown only whole
  (T498/T716: the held pane); a shell would change that design.
- Smaller: a tap AT first paint (before `loader.focus`) reads the whole `core` (option: focus from the start camera's
  region at boot; `startRegion` is only a link's today); a whole file the background has IN FLIGHT at the click still holds
  a read that names no slice keys (seen once: Thompson, 2.5 MB `water`, 7.7 s). Not done: tried and dropped — the whole
  core moved after regs-sid/down (named Fraser 3 s: 4.2 s); `access` in `warmWaterPage`.

## Acceptance
- Before/after table on the same Playwright/CDP measurement (staging vs a local export of the fix on data-staging).
- The two tests above pass, and fail with the fix reverted.
- The smoke step of item 4 (follow-up).

Depends: none. Size: S. Owner: no (a look after the next staging deploy).

## Owner

- [x] 2026-11-15 — Your call: cut region slices of geo / lines / regs-sid / down, and a page shell at 100 ms, from the first web tap's wait? (left of this ticket). → "Measure it first" (2026-11-27). Measured on staging (night notes `owner-questions/T737/`): cold tap to answer 1.2 s wifi / 2.2 s good 4G / about 20 s slow 4G; the tap's 1.27 MB competes with about 4 MB of start-up downloads; region slices save about 15 %. Then: "Tap first, start-up downloads after (Recommended)" (2026-11-28).
- [ ] 2026-11-28 — His follow-up: "are there no other easy speedups?" (2026-11-28). A speed-up hunt is running; its ranked list comes to him.
