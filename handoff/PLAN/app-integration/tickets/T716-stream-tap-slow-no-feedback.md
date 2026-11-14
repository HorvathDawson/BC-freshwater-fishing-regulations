# T716 — Tapping a stream takes a long time to open, with no feedback

Status: IN PROGRESS (before closed testing; round 2 done, rides the next APK; left (Claude): a look on the next build)

Source: owner 2026-11-13, build 0.7.8 on their Android phone, verbatim: "When you click on a stream it is taking a very
long time to open with no feedback it feels bad and slow. Want before closed testing"

Owner follow-up 2026-11-13 (after 3180f75a8), verbatim: "This is still so long.. (Fraser freeze: its first open went from
1.6 s to 0.5 s...)... The salmon build seems way to long? Maybe a card builds its item when it needs it?" — still before
closed testing. Asked of this round: (1) the salmon build lazy, per chain, when a page needs it — no ~1 s stall anywhere
(cold start, first Fraser open, the Salmon / Run timing map, the in-season inbox), same answers; (2) start the open at the
finger's lift so the page shows within ~50 ms of the click; (3) measure (not ship) showing the page before the double-tap
wait ends.

## Goal
- Within about 100 ms of a tap the reader sees something.
- The real time from tap to the water's content drops where the evidence points. No cache that can show stale rules;
  no change to what rules are shown. One load path; no silent fallbacks.

## How it was measured
Test bed `regs-open` (`node app/tools/testbed/run.mjs regs-open`), release-mode test-bed APK of HEAD (patched T420
MapLibre), staging release on the test channel (r-a9914d6b25b7), emulator `n1_pixel`. Each case starts from a COLD
START (tap 1 = first map tap after launch; the water was opened once from search to frame the map), then 2 warm taps.
New in the bench (T716): `press` = device clock right after the injected tap → the map's JS press handler; `cue` =
first lit set on screen after the tap; `shown` = the commit that puts the pane on screen (+ one frame). Waters:
small = Norrish Creek, unnamed = the nearest other stream beside it (an unnamed stream, Region 2), mid = Coquitlam River,
Fraser, Skeena, lake = Cultus Lake.

## Cause (evidence)
1. **300 ms before the app hears the tap, every tap.** MapLibre Android reports a click only from
   `onSingleTapConfirmed` (it waits out a double tap): 300-332 ms from the injected tap to the JS press, on all 18 taps.
2. **Nothing drawn until the pane is whole.** Since T498 the pane is built hidden behind the map (no flicker), so the
   screen did not change at all until the regulations were shown: 400-580 ms on the emulator, longer on a phone.
3. **A 1.0-1.1 s JS-thread stall on the first tap of a big river.** Fraser tap 1: 1,626 ms to the pane. The open probe
   (`[work] dfo pick 1010.5 ms`, `[stall] 1029 ms`) put it in `pickSalmon` → `withSalmonFeed`: the live DFO feed laid
   over the release's salmon head — every chain of the province — recomposed for EACH set of regions asked (a Fraser page
   asks its 4 regions, a new set), though it reads only the head and the feed. The JS thread was blocked so long that
   even the pane hold's 1.2 s timeout could not fire. (The feed itself was not fetched on the tap: `dfo latest` 0.3 ms.)

## What changed
- **The tap's cue** (`@app/ui` `pressCue.ts`, Shell + BrowseHost): the tapped section is lit on the map with the
  existing selection highlight the moment the finger lifts, held while the water opens, put out when nothing opens
  (dropped / stale open, `opener.tap` now says whether it opened) or when the map is back in front, and by itself after
  700 ms when no click follows (a double tap). Same for every map tap that opens a water (Map tab, Conditions map).
- **Binding patch** (`app/patches/@maplibre__maplibre-react-native@11.5.0.patch`, Android): `onTapUp`, a still
  single-finger tap said at its lift (not for a drag, pinch, long press or the second tap of a double tap). `MapProps.
  onPressStart` carries it (web: said in the same turn as the click, which is not delayed there).
- **The lift answers its click** (`map/src/tapLift.ts`): the click of the same tap reads the lift's hit test instead
  of asking again (the `hit` stage 18-135 ms → 0.1 ms).
- **The feed composed once per head and feed** (`core` `withSalmonFeed` → `feedPayload`, a WeakMap keyed by the two
  objects): the same answer (test: equal to composing alone), a new feed or release head composes afresh.
- Probe additions (test bed only): `press`, `cue`, `shown`, `[target-other]`, `[work] dfo pick / salmon facts / card`.

## Before / after (emulator, ms after the tap; cold = tap 1)

| water | before: first visible = pane shown (cold / warm / warm) | after: cue | after: pane shown |
|---|---|---|---|
| small (Norrish) | 420 / 434 / 438 | 40 / 37 / 32 | 418 / 416 / 414 |
| unnamed stream | 564 / 408 / 404 | 28 / 37 / 36 | 421 / 397 / 398 |
| mid (Coquitlam) | 504 / 434 / 453 | 40 / 59 / 39 | 478 / 440 / 399 |
| Fraser | **1,626** / 423 / 403 | 40 / 37 / 39 | **514** / 436 / 413 |
| Skeena | 578 / 496 / 461 | 36 / 48 / 48 | 546 / 460 / 445 |
| lake (Cultus) | 395 / 422 / 417 | 42 / 73 / 52 | 380 / 407 / 420 |

The ~310 ms of every "pane shown" is the double-tap wait (cause 1), kept: the double-tap zoom stays. JS after the press
is now 73-227 ms. Runs: before `app/tools/testbed/out/runs/2026-11-14T04-23-30`, after
`…/2026-11-14T05-03-48` (all 18 taps PASS with the new cue check), recordings `…/2026-11-14T04-50-01`.
Screens (Fraser): `~/bcfish-session-backup-2026-11-12/tap-feedback/` (before-fraser-strip.png: the map unchanged for
1.6 s, then the page; after-fraser-strip.png and after-fraser-cue-crop.png: the tapped stretch lit, then the page;
the mp4s).

## Checks
- `regs-open` FAILS a tap with no cue or a cue later than `OPEN_CUE_MAX` (150 ms): the pre-fix build fails it
  ("no cue: nothing was lit on the map after the tap"), the fixed build passes 18/18.
- vitest: `pressCue.test.ts`, `TapCue.shell.test.tsx` (the real Shell: lit at the lift, held through the open, unlit
  after back; a lift with no click goes out), `tapLift.test.ts`, `openWater.test.ts` (tap says whether it opened),
  `salmonFeed.test.ts` (composed once per head + feed, afresh for a new feed, equal to composing alone).

## Left
- A look on the next APK (0.7.9, built by another lane), on the owner's phone: is a lit stretch enough of an answer at
  low zoom (it is one section, the existing selection colour)? If not, the owner picks a stronger cue.
- The feed composition itself still costs ~1 s once per feed per session (now paid once, not per region set); making
  `withSalmonFeed` lazy per chain would remove it. Not done here.
- iOS: no `onTapUp` (benched); the cue there comes with the click.

## Round 2 (owner follow-up, 2026-11-13)

### 1. The salmon build: per chain, when read, and the waste cut
- **Where the ~1 s sat after round 1:** at the COLD START. The map's salmon colouring asks every region's slices, and
  the DFO feed was composed over all 435 chains in one go: `tap-stalls` on the round-1 build, `[work] dfo pick 1167 ms`,
  `[stall] 1185 ms` at start, and again (`1449 ms`) when an applied release brought a new salmon head.
- **Why one build cost ~1 s:** `composeChain` asked every one of the 366 days, per fish, per place, and `at()` scanned
  each place's runs from the start every time (plus a JSON `stable()` per day on two-region chains): ~0.7 ms a chain on
  a desktop (290 ms for the province), ~1 s on the emulator's JS thread. A place's calendar only changes on its run
  starts. **Now** it is asked only on those days and held to the next (`changeDays`): the whole province 290 → ~10 ms on
  a desktop. And **each chain is built when first read** (`lazyList` in `composeFeedPayload`): a page builds its own
  water's chains, the Salmon map those of the sections it draws; the payload itself costs ~1 ms.
- **Same answers:** `conformance/src/regs/salmonLazy.release.test.ts`: on the live release and a frozen feed
  (`app/fixtures/dfo-feed-2026-11-13.json`), every chain read lazily in reverse order equals, by content, the
  every-day whole build (`composeChainEveryDay`, the old pass). Mutation-checked: dropping change days fails it. The
  pipeline-made goldens (`salmonFeed.test.ts`) pass unchanged.
- **No stall left (emulator, `tap-stalls`, fails at 300 ms):** cold start worst 125 ms (was 1,185), Salmon colouring
  0, the bell 0, back to Standard 0 (was 1,464). Run timing 262 ms: the tab's own restyle, no salmon work in it (it
  was 141-151 ms before this round too). First Fraser open: in the table below.

### 2. The open starts at the lift
- `tapAhead` (`@app/ui` pressCue.ts): at the finger's lift the one opener resolves the water and the pane is built
  hidden behind the map (T498's hold) during MapLibre's double-tap wait; the Shell adds one wait to the hold until the
  click (`useTapAwaiting`), so the page is shown as soon as the click comes. The click reuses the lift's hit test.
- A lift with no click (a double tap: a zoom) is taken back after 700 ms: the lookup dropped (`opener.cancel`) or the
  open undone (back), never shown. The map stays touchable while a lift waits for its click (else the second tap of a
  double tap never reached it and the detector confirmed the first as a click). `regs-open` now plays a raw double tap
  on Norrish Creek: zooms, no page, cue lit then out, no failure reported — PASS.
- One load path: the lift's open is `opener.tap`, the same action a click, search or link makes.
- Found by that check and fixed: every water open asked the PRODUCTION site for the share link's slug shard; that site
  (the old webapp) answers every path with its page (200, text/html), so each new shard reported "JSON Parse error".
  `slugReader` now reads an HTML answer as "no shards" (as a 404); a body that claims JSON and is not one is still
  reported (`startLink.test.ts`).

### Before / after (emulator; ms after the tap; cold first tap, then two more)

| water | round 0 (0.7.8 code): page shown | round 1: page shown | round 2: cue | round 2: page shown | round 2: click → page |
|---|---|---|---|---|---|
| small (Norrish) | 420 / 434 / 438 | 418 / 416 / 414 | 54 / 45 / 43 | 354 / 360 / 371 | 43 / 39 / 52 |
| unnamed stream | 564 / 408 / 404 | 421 / 397 / 398 | 32 / 22 / 32 | 357 / 402 / 361 | 47 / 66 / 43 |
| mid (Coquitlam) | 504 / 434 / 453 | 478 / 440 / 399 | 50 / 73 / 61 | 377 / 364 / 384 | 47 / 42 / 49 |
| Fraser | 1,626 / 423 / 403 | 514 / 436 / 413 | 55 / 66 / 39 | 352 / 352 / 363 | 45 / 49 / 43 |
| Skeena | 578 / 496 / 461 | 546 / 460 / 445 | 59 / 83 / 44 | 356 / 384 / 371 | 47 / 78 / 47 |
| lake (Cultus) | 395 / 422 / 417 | 380 / 407 / 420 | 58 / 112 / 93 | 357 / 374 / 378 | 46 / 52 / 47 |

The click itself arrives 303-336 ms after the tap (the double-tap wait): the page now follows it by 39-78 ms.
Run: `app/tools/testbed/out/runs/2026-11-14T06-07-46` (regs-open 18/18 + double tap + tap-stalls, all PASS).

### 3. Showing the page at the lift (measured, NOT shipped — the owner's call)
Experiment build (`EXPO_PUBLIC_TAP_AT_LIFT=1`, test bed only): the page is shown **117-278 ms** after the tap (Norrish
117 / 126, Fraser 278 / 144, Cultus 176 / 160), ~200 ms sooner than now. As built it does not work: once the page is
up the browse map is gone, so MapLibre never confirms the click, the open is taken back after 700 ms and the map
redraws (recording: `~/bcfish-session-backup-2026-11-12/tap-feedback/variant-show-at-lift-small.mp4` / `-strip.png`:
page at once, then map, grey, map). Making it work needs a choice the owner has to make: once the page covers the map
the second tap of a double tap lands on the page, so either (a) a double tap on a water opens it (no double-tap zoom on
a water; zoom still by pinch and on empty map), or (b) keep the page hidden until the double-tap window passes (today's
behaviour). (a) would show the page at ~120-280 ms instead of ~350-400 ms.

