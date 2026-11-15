# T737 — Web: the first river click waits for the background download of the whole release files

Status: IN PROGRESS (lane: first-click investigation 2026-11-14; fix committed, staging deploy is the main chat's)

Source: owner 2026-11-14 evening, verbatim: "On staging.canifishthis.ca  The first river click takes a very long time
to open". Staging = b2dba0297 (app 0.7.10) on release r-f749475d0962. The staging smoke's "tap a named water" step said
0.9 s: it taps only after the Salmon and Run timing steps (well over 15 s after the map painted), when the background
download is over.

## What was measured
Headless Chrome, a fresh profile per run (empty cache, no service worker, no storage; the first-run notice seeded as
seen), staging over the real network. Shots, JSON and logs: `~/bcfish-session-backup-2026-11-12/first-click-web/`
(`before-*` = staging, `after-*` = this fix served locally against data-staging). Driver: the session scratchpad
`fc/measure.mjs` (taps a vertex of the water projected from HOME, as `tools/map-tap.mjs`).

Desktop, Fraser River, by how long after the map painted the click came (staging, before):

| click after paint | page readable | what the tap waited on |
|---|---|---|
| 0 s | 2.4 s | the idle open of the whole `core` (1.5 MB) in flight |
| 0.8 s | 2.3 s | `core` (just finished), then water-2 slice + geo |
| 3 s | 9.0 s | the background prefetch's whole `water` (2.5 MB, 7.9 s) |
| 6 s | 8.4 s | the same `water` download, then geo + access after it |
| 12 s | 1.1 s | nothing big (the prefetch is on the other regions' regs) |

Live staging, click 3 s after paint (fresh profile): desktop Fraser 3.3-10.5 s, Vedder 7.8 s, Elk Creek 7.0 s, an
unnamed stream 10.8 s; phone Fraser 5.1-5.4 s, Elk Creek 5.8 s, unnamed 12.0 s; search → Open 4.6 s (desktop); a
`/water/<slug>` link 2.0 s desktop / 3.2 s phone (SEO page, no map). A second click and a reload in the same browser:
0.05 s (the release cache holds the files). In every slow run the whole `water` (often + `core`) was downloading.
The full table (way in × cold/warm × desktop/phone) is in the results folder (`results.md`).

## Before / after (local exports of b2dba0297 and b2dba0297 + this fix, both on data-staging, alternated)

| click 3 s after paint | before | after |
|---|---|---|
| desktop Fraser (3 runs) | 3.6 / 0.8 / 4.3 s | 1.7 / 0.8 / 1.0 s |
| desktop Fraser, 10 Mbit/40 ms | 3.6 s | 1.3 s |
| phone Elk Creek | 4.9 s | 1.0 s |
| phone Fraser, 10 Mbit/40 ms | 4.3 s | 1.0 s |
| search → Open, desktop / phone | 3.3 / 5.4 s | 1.0 / 1.9 s |
| unnamed stream, desktop (2) / phone (2) | 4.3, 4.0 / 4.2, 4.6 s | 5.6, 5.4 / 3.4, 4.5 s (unchanged: see Left) |

## Left (not this fix; follow-up candidates)
- An UNNAMED stream's first tap still takes 4-6 s: it needs `regs-sid` (1.2 MB), `down` (1.1 MB) and `lines` (1.3 MB),
  whole files with no slices. Options: region slices of regs-sid/down/lines (pipeline `slices.py`), or put them first
  in the background order.
- A tap AT first paint (before the map's region settle calls `loader.focus`) reads the whole `core` (1.5 MB) and waits
  for it (2-8 s measured, before and after). Option: focus from the start camera's region at boot (`startRegion` is
  only a link's today).

## Cause
After first paint the web starts two downloads nobody asked for: the idle open of the whole `core`
(`release/prefetch.ts`, 1.5 MB) and the T253/T238 background prefetch (`release/background.ts`, one file at a time:
whole `water` 2.5 MB, access, regs-sid 1.2 MB, down 1.1 MB, then every region's regs, ~9 MB over ~19 s on staging).
The tap's reads go through `ReleaseLoader.dbFor` (T256 answer slices), which reads the WHOLE file whenever it is "here
or on its way": `dbs.has(name) || ahead.has(name)`. A whole file still downloading counted as here, so a tap in that
window waited for the 2.5 MB `water` (or the 1.5 MB `core`) instead of reading its region's 0.17 MB slice. Then the
reads after it (geo, access) started only once it was in: one long serial wait with the map held and the tap's cue lit.

Not a regression: no data-layer change between the previous staging (9048fbc20) and b2dba0297; the rule dates from
T256 (2026-10-29) with the background prefetch (T253/T238).

## Work
1. `loader.ts` `wholeHere`: a whole file counts only once it is opened or its prefetch has FINISHED (`aheadDone`), not
   while it downloads ahead. Opens in flight still count (the phone's path is unchanged: it never prefetches).
2. `prefetch.ts` idle core: fetched ahead first, then opened (an open in flight would hold a tap again).
3. Tests: `loader.test.ts` "(T737) a whole file still fetched ahead does not hold a tap"; `prefetch.test.ts` "(T737)
   idle core is fetched ahead, then opened". Both fail on the code before (mutation-checked).
4. Smoke (`tools/smoke-staging.mjs`, follow-up, not done here): a FIRST tap on a fresh profile 3 s after the map paints
   (before any other step), timed to the Regulations header visible to the reader (not merely in the DOM: the held
   pane is in the DOM, hidden), failing over 4 s.

## Acceptance
- Before/after table on the same Playwright/CDP measurement (staging vs a local export of the fix on data-staging).
- The two tests above pass, and fail with the fix reverted.
- The smoke step of item 4 (follow-up).

Depends: none. Size: S. Owner: no (a look after the next staging deploy).
