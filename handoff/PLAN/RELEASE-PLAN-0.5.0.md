# Plan to app 0.5.0 (and iOS)

APPROVED by the owner 2026-11-03: salmon clean-up (batch 3) lands before the cut; 0.5.0 is Android only, iOS
ships next as its own TestFlight release; duplicates folded. Built from the ledger check
(`handoff/PLAN/audit/ticket-review-2026-11-03.md`, every open ticket listed there with size and area).

## Why 0.5.0 is the gate
The salmon data moved to a new format (salmon/3, T332/T349, already merged). App 0.4.0 cannot read it, so
`deploy/staging/upload.sh` refuses to upload it until an app of 0.5.0 or later exists (the `SALMON_NEEDS` guard). Until
0.5.0 ships, every new data release (T322's slough fold, T326 tidal, T349 DFO) is built and promoted locally only, and
the feeds-worker deploy (T242 crash reports, T291, T315 gauge week) waits too.

## Batches, in order
Each batch is one sitting on one area. Tickets in a batch are done together.

| # | Batch | Tickets | Size | Notes |
|---|---|---|---|---|
| 1 | Slough fold release (data) | T322, T337 check | heavy, few credits | Running now. Local promote only (staging is held). |
| 2 | Crash, memory, rule-book PDF | T360 → T351, T354, T345, T352 (app side) | M–L | One emulator sitting. T360 measures first; you pick the PDF option. |
| 3 | DFO salmon clean-up | T356, T358, T357, T338, T343 (+ T361, yours) | L | Changes what the salmon data looks like, so it must land BEFORE 0.5.0, or we need salmon/4 later. |
| 4 | Feeds and map colours | T347, T348 | S–M | One "feed changed" signal; weekday forecast rule. |
| 5 | Map feel (owner 2026-11-03: in 0.5.0) | T312 (T353 + T273 folded in) | M–L | One emulator gesture pass: 1:1 tracking, fling, pan lag; T273's basemap-tile choice goes to the owner. |
| 6 | Small app fixes | T342, T344, T339, T340 | S each | Phone long-press copy, species swipe, Whonock label/Bedford gauge, test bed taps. |
| 7 | Release check and cut | T307, T308, T310, T315, T316, T293 checks; full suites once | M, mostly heavy | Then APK 0.5.0 → Play internal, staging upload with `MIN_APP_VERSION=0.5.0`, feeds-worker deploy. |

After 0.5.0:
- iOS release: T313, T336, T88 (signed build + TestFlight), T362–T365 (simulator work may run earlier between batches);
- T317 overlay once-over (follows the map-feel pass);
- pipeline hygiene (T306, T341, T305, T301, T304, T331);
- the audits T333–T335 (you scope them first; see the tickets);
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
