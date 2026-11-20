# T737 — Web: the first river click waits for the background download of the whole release files

Status: IN PROGRESS (fix a43e750b9; unnamed-stream background order e974cabf7; multi-region water 673b1d8e3; the ahead lane + unnamed reads together bf23b3819, b35f91e0b: all on `redesign/stream-sections` (2026-11-14/15) and on the staging and production websites since; left (Claude): the smoke's first-tap step (item 4); region slices of regs-sid/down/lines are the owner's call)

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

## Unnamed streams in the background, without moving Lighthouse (2026-11-14 night)
Owner, verbatim: "could we slow load the stuff needed for unnamed streams in background without it effecting the
lighthouse seo scores?" Answer: **yes, with limits** — done (e974cabf7, unpushed); a tap in the first ~5 s after paint
stays slow on a slow line (only slices fix that, below). Lighthouse: unchanged on every page measured.

**Facts (HEAD before this change).**
- An unnamed page waits for `regs-sid` (1.24 MB gz) and `down` (1.17 MB), then its region's water slice (0.17 MB,
  serial). It also asks for `lines` (1.28 MB) but does NOT wait for it (tap at 15 s: readable in 0.07 s with `lines`
  still downloading).
- The background tier (`release/background.ts`) starts 4 s after `requestIdleCallback` after the map's first region
  report, one file at a time: (core from the idle tier) → whole `water` 2.57 MB → access → regs-sid → down → salmon →
  the other regions' regs + salmon (~9 MB). Measured (desktop, data-staging): water 4.0-8.0 s after paint, regs-sid
  in at ~12.8 s, down at ~14.7 s. So an unnamed tap was fast from ~15 s after paint, slow (5-11 s) before it.
- Lighthouse is not in CI. `tools/seo` writes a `lighthouserc.json` for the water pages; the T196/T409 runs were water
  pages + region hubs (mobile, simulated). Region hubs are static (3 requests, no app). Water pages are the app with
  the pane prerendered, but the map is built only when seen (T409), so the map never reports a region and the
  background tier never starts: no regs-sid/down/water/lines in any water-page run. Only `/` (the app shell, not an
  SEO landing page) runs it, and there it already fell inside Lighthouse's window in 3 of 5 runs BEFORE this change.
- The SEO category is page markup only (titles, meta, crawlability): no download timing can move it. What timing can
  move is Performance (LCP, TBT, Speed Index, CLS) — so the worry applies to Performance on `/`, not to SEO.

**Change (web only; the phone never runs the background tier).** (a) Per region: what a named tap reads (core, access,
regs-<r>, salmon, salmon-<r>), then regs-sid and down, then the whole water (it helped no tap since the fix above:
a tap reads its slice while it comes). `lines` is not fetched ahead (the page does not wait for it; fetched ahead it
slowed a named tap at 8 s, 0.8 → 2.2 s on a phone, in a first pass). Still one at a time, still after idle + 4 s.
(b) While the reader downloads (any release download under the `ahead` priority, net activity) nothing new starts
(`reader()`, wired in `useBackgroundPrefetch`); a tap on a file in flight already joined it (web source `inflight`)
and asked the others in parallel — now pinned by a test. Holds unchanged: Save-Data, effectiveType not 4g, measured
under 1.5 Mbit/s, low-data mode, metered (cellular) without the setting, hidden, low battery (tests pass as before).
Not done: `fetch` priority "low" on ahead files — a tap that joins a file in flight would inherit the low priority.
Tests: `background.test.ts` (order, yield, join + parallel, readerDownloading), `prefetch.render.test.tsx` (hook
wiring; and the idle-core test fixed: fetch, then open after it settles — CI red on 4cc85a914). Mutation-checked
(order off → 6 fail, yield off → 1, `<=` → 2, join off → 1, hook off → 1). `pnpm check:fast` clean.

**Timing** (local exports of HEAD vs this change against data-staging, alternated per scenario; page readable after
the tap; the real line was slow tonight, ~3-4 Mbit/s, so the 10 Mbit throttle mostly did not bind):

| tap after paint | desktop real | phone real | desktop 10 Mbit | phone 10 Mbit |
|---|---|---|---|---|
| unnamed 3 s | 5.7 → 1.6 s | 7.2 → 5.6 s | 7.8 → 4.2 s | 4.5 → 7.6 s |
| unnamed 8 s | 5.9 → 0.8 s | 10.8 → 10.5 s | 6.1 → 1.0 s | timeout → 1.5 s |
| unnamed 15 s | 0.1 → 0.1 s | 0.1 → 0.1 s | 0.1 → 0.1 s | 0.1 → 0.1 s |
| Fraser 3 s | 1.5 → 1.2 s | 1.2 → 1.0 s | 1.7 → 1.4 s | 2.1 → 1.2 s |
| Fraser 8 s (3 / 2 runs) | 1.0, 1.0, 0.5 → 2.5, 0.6, 0.3 s | 4.7, 0.3 → 2.3, 0.3 s | | |

Named taps: no slower (the 2.5 s desktop run fetched its 0.17 MB water slice on a slow spell; at 8 s the whole water is no longer in, so a named tap reads its slice — as at 3 s). At 3 s nothing has been fetched ahead yet in either build (the tier starts ~5 s after paint), so 3 s moves only with
the line. The phone 8 s 10.5 s run: the idle core was still coming (the line ~1.4 Mbit/s then).

**Lighthouse 12.8.2, mobile preset, alternated, median of the valid runs** (a run whose data-staging requests failed
is left out): `/` Perf 48 → 48, LCP 7.84 → 7.84 s, TBT 668 → 692 ms, SI 6.68 → 6.76 s, SEO 92 → 92; `/water/vedder-river`
Perf 51 → 54, LCP 19.3 → 18.1 s, TBT 352 → 368 ms, SI 6.7 → 8.2 s, SEO 100 → 100; `/region/2` 100 → 100 (LCP 0.90 s,
TBT 0), SEO 100 → 100. Run-to-run spread is ±8 points. The water page's LCP is the app's "Before you fish" first-run
notice drawn after hand-over (a fresh profile; T409's rig numbers differ for this reason).

**Left: region slices (option c, not built — a pipeline/release-format change).** Only slices make a tap in the first
seconds fast on a slow line: ~2.4 MB must arrive first today. Measured on r-f749475d0962 (block-level, today's handle
order): a region's handles touch 13-78 % of regs-sid's 479 blocks and 9-88 % of lines' 303 (region 2: 51 % / 69 %),
because handles are ordered by stream key, not by region — so a block slice saves about half (region 2: regs-sid
≈ 0.63 MB). Re-keyed per region (only the region's handles, plus a handle → slot index) the files would be ~4-30 % of
the whole (region 2 has 72k of 1.96M handles). `down` is per section with the downstream walk (crosses regions to the
mouth). Touches pipeline `slices.py`/regs store, the contract (WEB_ONLY, budget), `loader.dbFor` routing for
regs-sid/down, the release-budget pins. Ticket-sized; owner's call.

## A multi-region water: "it still feels slow" (2026-11-15, 673b1d8e3, unpushed)
Owner on live staging (dff2c7a22), verbatim: "i just tried it again and iot still feels slow" — then: "i clicked a
section of the fraser in region 2". His panel after the click: regs-3, access, the whole `lines`, regs-5 (and 7A off
screen). The lane's earlier test clicks were Region 2 waters that touch no other region.

**Reproduced (live staging, fresh profile, Fraser part in Region 2, click 3 s after paint, desktop):** cue (the part lit
on the map) at once; page readable 14.5 s, complete 17.0 s. Waterfall: the whole `core` 1.5 MB (9.4 s), then `water-2`
(0.5 s), then `geo` (2.6 s), page; then `regs-3` → `regs-5` → `regs-7a` one after another, `access` and `lines` beside.
Script: `~/bcfish-session-backup-2026-11-12/first-click-web/multi-1115/` (`measure.mjs`: `--complete` waits for every
page download and picture, `--initiators` records who asked each file — the source-mapped stacks named the code below;
`panTo` drags the map to a target, as a person pans). Line tonight: slow and variable (0.3-6 Mbit/s); every pair below
is alternated before/after.

**Cause — four, three in series.**
1. The in-season check (`Shell` `holdsWater` → `itemOrd` of every water a notice names, province-wide) opens the WHOLE
   `core` a few seconds after paint. `loader.wholeHere` counted an open in flight as "here", so the tap's `core` read
   waited for 1.5 MB though `core-2` held its answer (the case a43e750b9 fixed for prefetches, not for opens).
2. `useShellNav` still ran the P11 placeholder `useRegulationKeys`: it opened the region file of EVERY part of the open
   water, one after another (Fraser: regs-3, regs-5, regs-7a, ~1 MB in series beside the page). The page never used it:
   it reads its own part's file; the others serve a step or the parts list.
3. `geo` (0.6 MB, every named water's open: the solo check and the parts) was asked only after the water slice (the
   group read needs the slice first): 0.5-0.9 s + 1-2.6 s in series.
4. `lines` (1.3 MB): the Fraser has in-season notices on PART of it (`useInSeasonCovers` places them on the section
   lines), so its page asks the whole file once its rules are read. It holds no rule; it fills the banner in later.

**Change (web only, nothing on the page changes).** (1) `loader.ts`: only a FINISHED open counts. (2) `useRegulationKeys`
dropped from `useShellNav`; the open water's regions are handed to the background tier (`BackgroundPrefetch.want`,
`useWaterRegionsAhead`, `waterRegionsOf`) — fetched next, behind the reader, under every hold (Save-Data, slow,
metered, low-data, hidden, battery). (3) `warmWaterPage` + `warmedFirst`: the opener's part / solo questions ask `geo`
at once, so slice and geo come together. (`access` was tried there too and dropped: on a ~1 Mbit/s spell it held the
readable page back by its share of the line.) (4) `backgroundOrder`: the open water's regions; the view's water slice +
geo FIRST (a named tap reads them; the whole core it does not); the view's files (named, then regs-sid/down/water);
the neighbours; `lines`; the province. (5) The background starts nothing until the line has been free of the reader for
1.5 s (`READER_GRACE_MS`): a 1.2 MB regs-sid started 6 ms after geo landed and 24 ms before the page asked `lines`.
Tests (each fails with its piece reverted): loader "a whole file still being OPENED does not hold a tap"; background
"the whole order", "want", the grace; `warmWaterPage.test.ts`; `prefetch.render.test.tsx` "the open water's other
regions first". Run: release + sql + prefetch UI tests 219/219, ui + ui-native 2212/2212, check:fast clean (its
`tickets` step was red in the worktree only for T742, fixed on HEAD).

**Before / after** (local exports of HEAD f3028f9c3 and + this change, data-staging, alternated; readable = rules on
screen; complete = every download and picture of the page in, incl. `lines`):

| click | before readable / complete | after readable / complete |
|---|---|---|
| Fraser part R2, desktop, 3 s after paint (6 pairs) | 1.7/6.1, 9.5/19.1, 1.3/4.5, 0.9/2.9, 1.9/5.4, 1.8/6.2 | 0.8/2.2, 1.7/5.2, 0.6/1.7, 1.7/5.2 (slow spell), 1.1/2.7, 1.5/3.7 |
| Fraser part R2, desktop, 8 s (4) | 3.9/8.8, 2.0/7.0, 3.3/14.1, 5.7/19.2 | 0.1/1.7, 3.7/7.6, 1.9/5.9, 14.7/26.5 (line ~0.3 Mbit/s) |
| Fraser part R2, phone, 3 s / 8 s | 4.9/14.1 / 3.2/7.5, 1.7/5.0 | 1.3/5.3 / 0.6/15.2, 0.15/2.1 |
| Fraser at Lillooet (R3), after a 14 s pan (2) | 0.9/2.2, 2.6/8.8 | 0.1/5.2, 0.1/7.4 (`lines` on a slow line) |
| Thompson at Spences Bridge, after a pan (4) | 2.2, 2.7, 1.3, 1.0 | 5.4 (line stall), 7.7 (a 2.5 MB water in flight), 0.13, 0.12 |
| Vedder (R2 only), 3 s (4) / 8 s (2) | 2.5, 1.5, 0.7, 0.7 / 6.6, 2.0 | 0.8, 0.9, 0.8, 0.7 / 2.5, 0.1 |
| Elk Creek 3 s (2), Cultus Lake 3 s | 2.8, 21.6; 3.4 | 1.6, 0.9; 1.7 |
| Unnamed R2, 3 s (2) / 8 s (2) | 5.1, 2.6 / 2.1, 1.3 | 3.1, 2.7 / 0.2, 7.1 |
| search → Open, Fraser | 0.85 | 0.98 |

Lighthouse 12.8.2 mobile on `/`, 5 alternated runs each (the build before the last two order tweaks): Perf 43 → 43,
LCP 7.87 → 7.86 s, TBT 864 → 881 ms, SI 7.71 → 6.96 s, SEO 92 → 92.

**Left (not this change).**
- A slow line still costs the sum of what the page reads: water slice + geo (0.75 MB) before the page, `lines` 1.3 MB
  before a part-water's in-season banner. Region slices of geo/lines are the only fix (pipeline; owner's call, as above).
- An unnamed tap 3-8 s after paint can be slower than before on a slow line (one run 1.3 → 7.1 s): the view's slice +
  geo now come before regs-sid/down. The trade is the named tap (the owner's case) first.
- A whole file the background has IN FLIGHT at the click still holds a read that names no slice keys (seen once:
  Thompson, 2.5 MB `water`, 7.7 s).
- The click shows the part lit on the map at once, but the page is shown only whole (T498/T716: the held pane). A page
  shell with the name at 100 ms would change that design: owner's call, not done.
- Smoke step (item 4, still to add): a FIRST click on a Fraser part in Region 2 on a fresh profile 3 s after paint, and
  one after panning to Lillooet, timed to readable (≤ 4 s) AND to complete (every page download in; `measure.mjs
  --complete`).

- 2026-11-15 (CI red on e0785054a): the two new swallowing catches were listed in tools/failures.test.ts FINE with their reasons, not reported: `warmWaterPage`'s geo warm (regs.ts; the loader forgets a failed open and the page's own `store.geo` read asks again and says it) and `useWaterRegionsAhead` (prefetch.ts; only the background order is lost, its store / akey_region reads are the page's own open). `pnpm check:fast` now runs the two inventories (`pnpm inventories`).

## The unnamed tap behind three whole files; a reader never behind a file ahead (2026-11-15, late)
Main chat on live staging (3d21000ad, fresh profile, quiet machine, a 17 Mbit/s line): named waters fixed (Fraser R2 part
0.7-1.3 s), but the first tap on an UNNAMED stream 3 s after paint read 6.4 / 5.1 / 3.6 s desktop, 5.4 / 13.7 / 3.0 s
phone (dff2c7a22: 0.7 / 0.8 s). Request list: `down`, `lines`, `regs-sid` (1.1-1.3 MB each) start at the tap, `water-2`
(166 KB) only at 5.9 s, after `regs-sid`.

**Cause (traced in the request lists and the code).**
- Those three were NOT background downloads: they are the tap's own. The local runs with source-mapped initiators
  (`multi-1115/b-`, `f-desk-unnamed-*`) flag them reader (the live run's minified stacks cannot say: `bg` false for all).
  `regs-sid` = the page's inputs (`sectionAkey` / `sectionPlace`), `down` = "Tributary of …" (`useDownstreamWater`),
  `lines` = the ADDRESS BAR's place for an unnamed reach (`systemBack.web.ts` → `linkSearch` → `placeOf`, 38 ms after the
  tap; the page never waits for it). Nothing was fetched ahead at 3 s (the tier started idle + 4 s, the view's slice +
  geo first since 673b1d8e3).
- `water-2` waited on a DATA dependency, not a connection: the section's answer key comes out of `regs-sid`, and only a
  read naming that key is routed to the slice; `access` then waited for the inputs (the page's subject). Serial:
  regs-sid → water-2 → access. data-staging is HTTP/2 (h3 offered): no per-host connection limit.
- Not a regression in the code: dff2c7a22's live 0.7 s run (`live-desk-unnamed-s3000.json`) has the SAME request shape
  (down/lines/regs-sid at the tap, water-2 after regs-sid) — 3.7 MB in 0.52 s, a ~55 Mbit/s line that night. Tonight's
  line moved it.
- Whole files with no slice an unnamed page still needs: `regs-sid` (1.25 MB) and `down` (1.17 MB) — the floor at 3 s on
  a cold profile is their 2.4 MB unless fetched ahead before the tap; `access` (0.2 MB, a whole bin file, no slices) for
  both kinds of page; `lines` (1.3 MB) only for the address (unnamed) and a part-covering in-season notice (named).
- Up to three files ahead could share the line at once (the idle tier's region file and core, the background's file,
  now the address bar's lines): each tier had its own one-at-a-time, none a common one; and a file ahead in flight at a
  tap kept its share of the line to the end (only nothing NEW started).

**Change (web only; nothing on a page changes; commit below).**
1. `release/lane.ts` (in `drivers/source.web.ts`): THE AHEAD LANE. Every file fetched ahead goes down one lane, one at a
   time, `fetch` priority `low`; the reader's downloads go out at once at `high`. A reader download starting aborts the
   file ahead on the network after `LANE_JOIN_MS` (150 ms) unless the reader asked for it (then it is the reader's, never
   aborted); the aborted file waits at the head of the lane and starts again from scratch (release files are served
   `Content-Encoding: gzip`: no Range offsets) once the line has been free of the reader for `LANE_GRACE_MS` (1.5 s) —
   its promise is the same throughout, so no prefetch fails and nothing is lost for a tap. A reader asking for a file
   still WAITING in the lane takes it out and starts it at once as its own. The gate is told of a raise only for a file
   already on the network (a gate entry for one not yet started would wait on itself). An abort is not a failure in the
   activity (`done`, not `failed`).
2. `background.ts`: `others(busy)` (`othersAhead` over the net activity, wired in `useBackgroundPrefetch`): another tier's
   file ahead on the network holds the background — one at a time whoever asked. A file the reader made wait is not
   measured (`yielded`): a parked download never reads as a slow line. `BACKGROUND_DELAY_MS` 4 s → 1 s: "core first,
   alone" is the lane's now, and a tap no longer shares the line with a file ahead, so the tier may start as soon as the
   idle core is under way (an unnamed tap 3 s after paint finds regs-sid / down on their way or in). Holds unchanged
   (Save-Data, slow, metered, low-data, hidden, battery).
3. The unnamed tap's reads together: `OpenerOptions.unnamed` → `warmUnnamedPage` (regs-sid, down, `loader.warmSlice
   ("water")` = the focus region's 0.17 MB slice, access), asked the moment the tap is known unnamed.
4. The address bar's `lines` for an unnamed reach is fetched AHEAD first (`loader.prefetch("lines")`): it waits in the
   lane until the page's files are in, then the place is written (the address only; opening a link with a place still
   waits for `lines`, as before).

Tests (each fails with its piece reverted, mutation-checked): `lane.test.ts` (one at a time; a tap's download aborts the
file ahead, same promise, again after the grace; joined never aborted; waiting → started as the reader's; nothing in the
grace; aborted then asked); `background.test.ts` "a reader's request never waits …" (the real web source + loader: the
tap's request goes out at once at high, the background's aborted and refetched, no prefetch failure; at most one ahead;
a waiting file the tap needs goes at once), "one file ahead at a time" (`others`, `othersAhead`, a yield not measured);
`loader.test.ts` warmSlice; `warmWaterPage.test.ts` (the unnamed reads asked together; the opener tells it for an
unnamed tap only); `prefetch.render.test.tsx` (the hook feeds `others`). Run: release + sql + net + prefetch + warm +
openWater + place + inventories 256/256 + 67/67; `pnpm check:fast` clean.

5. (found in the first after-run) the water header's Share control makes its link AHEAD at the tap (`useShare`) — the
   second reader of `lines` (1.3 MB) at the tap. It now fetches `lines` ahead first too (`share.ts`), so it waits in the
   lane; the phone's source answers an ahead ask on an installed file with nothing read (`source.native.ts`). Test:
   `share.test.ts` (unnamed asks it, named never; mutation-checked).
Tried and dropped (measured worse): the whole core moved after regs-sid/down and the in-season check's core through the
lane — with neither, sql.js, `core-2` and `water-shared` were cold at the tap (named Fraser 3 s: 4.2 s).

**Timing** (local exports of 3d21000ad (before, bundle = live staging's) vs the change, data-staging, release
r-f749475d0962; alternated per run; page readable after the tap, s; line = a 6 MB Range of the synopsis from
data-staging before and after every pair; load = 1-min uptime; macOS's own load (mediaanalysisd …) had fallen from ~16 to
3-7 by the runs). Scripts + JSON: `~/bcfish-session-backup-2026-11-12/first-click-web/lane-1115/`.

| tap | before | after | line Mbit/s | load |
|---|---|---|---|---|
| desktop unnamed 3 s | 3.6 / 5.3 / 5.0 | 2.4 / 4.0 / 2.5 | 14-17 | 3-5 |
| desktop Fraser R2 3 s | 1.5 / 1.0 / 1.2 | 0.9 / 1.9 / 0.8 | 16-18 | 3-4 |
| desktop Vedder 3 s | 1.2 / 12.6 / 0.7 | 2.3 / 0.8 / 0.9 | 0 (a dropped curl)-17 | 3-5 |
| desktop Thompson after a pan, 3 s | 0.1 ×3 | 0.1 ×3 | 16-17 | 3-7 |
| desktop, every water, 8 s | 0.1 ×12 | 0.1 ×11 (1 run lost) | 12-17 | 2-7 |
| phone unnamed 3 s | 3.6 / 3.6 / 2.9 | 3.4 / 2.8 / 3.5 | 15-18 | 3-4 |
| phone Fraser R2 3 s | 0.8 / 1.3 / 2.0 | 1.4 / 1.2 / 1.5 | 14-18 | 3-5 |
| phone unnamed 8 s | 2.8 / 0.1 / 0.1 | 0.7 / 0.1 / 0.1 | 15-18 | 3 |
| phone Fraser / Thompson 3 + 8 s | 0.1 ×9 (Fraser 8 s, Thompson) | 0.1 ×9 | 14-18 | 3-5 |

(Phone Vedder: no tap, the point is off the phone's start view — the harness.) Reading: the unnamed tap at 3 s now
fetches only what its page needs, together (regs-sid, down, water-2, access at once; `lines` after the page), so it is
the two whole files' 2.4 MB on the line: ~2.5 s at ~16 Mbit/s, down from 3.6-5.3 s desktop; the phone barely moved
(its CPU, not the line, after the files land). It is NOT ~1 s: at 3 s nothing is fetched ahead yet (the lane yields to
the boot's own reads, incl. the in-season check's whole core, and their grace), so regs-sid + down are a cold 2.4 MB.
Only their region slices (pipeline, owner's call) or fetching them before the boot reads would get it to ~1 s. Named
taps and every 8 s tap: unchanged (0.8-1.9 s at 3 s, 0.1 s at 8 s).

**Lighthouse 12.8.2 mobile on `/`, 3 alternated runs each, median:** Perf 52 → 54, LCP 7.81 → 7.82 s, TBT 602 → 536 ms,
SI 5.38 → 5.22 s, CLS 0.010 → 0.010, SEO 92 → 92 (after: `regs-sid` lands inside Lighthouse's window, 6.2 vs 5.6 MiB
of release files; last request ends 5.8 vs 6.8 s).
