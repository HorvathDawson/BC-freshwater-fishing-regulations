# Plan to app 0.5.0 (and iOS)

APPROVED by the owner 2026-11-03: salmon clean-up (batch 3) lands before the cut; 0.5.0 is Android only, iOS
ships next as its own TestFlight release; duplicates folded. Built from the ledger check
(`handoff/PLAN/audit/ticket-review-2026-11-03.md`, every open ticket listed there with size and area).

## Why 0.5.0 is the gate
The salmon data moved to a new format (salmon/3, T332/T349, already merged). App 0.4.0 cannot read it, so
`deploy/staging/upload.sh` refuses to upload it until an app of 0.5.0 or later exists (the `SALMON_NEEDS` guard). Until
0.5.0 ships, every new data release (T322's slough fold, T326 tidal, T349 DFO) is built and promoted locally only, and
the feeds-worker deploy (T315 gauge week) waited too — DEPLOYED EARLY 2026-11-03 by the owner's call (version fa62c851):
crash/2 + report/2 are stored; the live DFO feed is dfo-feed/2, so 0.4.0 shows the release's salmon answers until 0.5.0.

## Batches, in order
Each batch is one sitting on one area. Tickets in a batch are done together.

| # | Batch | Tickets | Size | Notes |
|---|---|---|---|---|
| 1 | Slough fold release (data) | T322, T337 check | heavy, few credits | DONE 2026-11-03: gate PASS, release r-4df5bccc9360 local (labels refreshed; ships T354's smaller book). |
| 2 | Crash, memory, rule-book PDF | T360 → T351, T354, T345, T352 (app side) | M–L | One emulator sitting. T360 measures first. PDF: incremental append + image compression (T354, owner 11-03). |
| 3 | DFO salmon clean-up | T356, T358, T357, T338, T343, T366 (T361 folded into T366; the marks stand) | L | Changes what the salmon data looks like, so it must land BEFORE 0.5.0, or we need salmon/4 later. T366 makes the live refresh safe, then pulls region 2's new Fraser notice (FN1179). |
| 3b | Data correctness (owner 11-03) | T374, T372, T373, T375 | M–L | Trib walk collecting lakes (reach rerun + bundle); partial coverage the trout way, reusing the trout path; run timing built with each release; forecast every day (code done). |
| 4 | Feeds and map colours | T347, T348, T376 | S–M | One "feed changed" signal; weekday forecast rule; the Conditions view's watershed (map centre / a water's own) + basin names for the UI chat's day list. |
| 5 | Map feel (owner 2026-11-03: in 0.5.0) | T312 (T353 + T273 folded in) | M–L | One emulator gesture pass: 1:1 tracking, fling, pan lag; basemap already simplified (T280); measure pan on a real phone. |
| 6 | Small app fixes | T342, T344, T339, T340 | S each | Phone long-press copy, species swipe, Whonock label/Bedford gauge, test bed taps. |
| 6b | UI text clean-up (owner 11-03) | T367 | M, rounds | Back-and-forth: owner note → plan → fix on the dev web app → repeat. Regs, conditions map, More. Into the APK only when done. |
| 6c | Failures, fallbacks, reporting (owner 11-03: in 0.5.0) | T368, T369 | M | App-wide: every catch/fallback rethrown, or shown + `reportFailure`, or listed; one failure face; a scan net. Split from T333. T369: one load path per screen, same output whatever the way in (refresh, tab, search, map tap, step, pick). |
| 6d | Housekeeping and checks (owner 11-03: in 0.5.0) | T50, T65, T77, T178, T239, T328 | M | Emulator parity + system bars, determinism proof (heavy), phone testing, CLEVER vs COFFEE re-score, annual synopsis runbook, archive old slough inputs. |
| 6e | Launch prep (owner 11-03: in 0.5.0) | T240, T370 | M | Production launch checklist (prod switch-on itself stays the owner's); one shared web file instead of ~0.7 MB repeated per region slice. |
| 7 | Release check and cut | T307, T308, T310, T315, T316, T293 checks, T371 (closed testing); full suites once | M, mostly heavy | Then APK 0.5.0 → Play internal AND Closed testing (T371: starts Google's 12 testers × 14 days for production), staging upload with `MIN_APP_VERSION=0.5.0`, feeds-worker redeploy if changed since 2026-11-03. |

After 0.5.0:
- iOS release: T313, T336, T88 (signed build + TestFlight), T362–T365 (simulator work may run earlier between batches);
- T317 overlay once-over (follows the map-feel pass);
- pipeline hygiene (T306, T341, T305, T304, T331);
- the audits T333–T335 (you scope them first; see the tickets; T333's app failure/fallback part moved to T368, in 0.5.0);
- SEO, hosting and prod launch (T86 block) stay parked until you unpark them.

## iOS: what "working well" needs
Covered by tickets now: T313 (simulator run), T336 (walkthrough step 17 hidden from VoiceOver), T244 (store listing),
T250 (Expo SDK upgrade, best done before a store release), T258 (screen-reader walk).

Filed 2026-11-03:
1. T88 (re-scoped): signed build and TestFlight upload script (today's script builds for the Simulator only). Needs the $99 Apple
   Developer account (yours).
2. T362 iOS crash reports. Android's exit-info has no iOS twin, so an iOS crash leaves no record today.
3. T363 iOS memory on a real iPhone. The Simulator showed 420 MB, peaking at 665 MB while panning; Android crashed at
   406–435 MB.
4. T364 A real-iPhone run: location prompt, offline first download, edge-back gesture, gauge pill padding.
5. T365 App Review compliance: privacy manifest (`PrivacyInfo.xcprivacy`), tracking declaration, final bundle id and name
   ("CanIFish" vs "Can I Fish?").

## Duplicate tickets folded (done 2026-11-03)
T132 → T164; T88 re-scoped to "ship iOS to TestFlight" (its simulator half is T313); T353 + T273 → T312, the one
map-feel ticket; T55 shrunk to "owner confirms slough marks" (T322 carries the fold).

## Owner 2026-11-04: fewest rebuilds — group the heavy work
- ONE pipeline rebuild (atlas → reach → bundle → release), carrying every data change: T381 (one section for a water with
  no splits), T386 (`section_measure` for unnamed reaches), and what is already waiting (T372 `n`, T376 basin tables,
  T339 gauge names, T370 `water-shared`). T65 rides it: the atlas built twice from the same inputs, diffed.
- ONE APK, one emulator sitting, carrying every app change: T312 fling patch, T382 B.C. clock + sun line, T384 open on my
  location, T385 share, T340 test-bed fix, then the device checks (T342, T344, T360, T50).
- T239 after 0.5.0; T240 = a closed-testing checklist only.

## Remaining, in order (2026-11-04, owner)
1. T387 lands (every tap on a part gives the same page; the Salmon card fix found on the dev web app with it).
2. THE ONE REBUILD, right after (owner 2026-11-04: the dev web app's data must be current — pre-T372 salmon file):
   bundle → release (T372 `n`, T376 basins, T339 names, T387 part data, T370 `water-shared`, T386 `lines`), promoted
   locally, release pins re-cut, the request walk re-pinned; T65 = the atlas built twice, diffed.
   Commands (dry run 2026-11-04: clim + tiles up to date; reach + deliver run — deliver writes the release + LATEST):
   `scripts/heavy.sh build 50 -- python -m pipeline build --jobs 1`; T65 after it: `scripts/heavy.sh atlas 50 --
   python -m pipeline build --atlas` (a side atlas + its parity report vs the promoted one; never `--promote`).
3. T178 monthly score (worker) + T371 drafts (running); T377 item 1 (day list by watershed).
4. ONE emulator sitting on ONE APK (fling, clock + sun, open on location, share; T340 fix; T307/T308/T293/T50 checks).
5. Full suites once; staging upload (minAppVersion 0.5.0); feeds worker deploy with the app; AAB → Play internal +
   Closed (T371; owner may start with fewer testers). No separate phone walk (owner: the testing tracks cover it).
