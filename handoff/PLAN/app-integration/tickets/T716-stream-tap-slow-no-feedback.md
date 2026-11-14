# T716 — Tapping a stream takes a long time to open, with no feedback

Status: IN PROGRESS (before closed testing; code done, rides the next APK; left (Claude): a look on the next build)

Source: owner 2026-11-13, build 0.7.8 on their Android phone, verbatim: "When you click on a stream it is taking a very
long time to open with no feedback it feels bad and slow. Want before closed testing"

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
