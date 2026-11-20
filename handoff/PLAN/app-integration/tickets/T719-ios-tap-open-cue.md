# T719 — iOS: tap open speed and cue, T716 parity (IOS-2)

Status: DONE — 49b7226b6, efd9ada22, d6afcf124, d15784a4f. iOS tap cue at the lift (the tap-up patch) and the regs-open twin, merged 2026-11-16; the owner's iPhone look on build 36 passed.

Closed by the 2026-11-20 audit: owner on build 36, 2026-11-16 ~21:22: "a-d worked good ... yes to (E) and (f)" (OWNER-TODO: 'iPhone build 36 test (a–f) done'); check e was T719's left; patch merged in d15784a4f.

Source: owner 2026-11-14, verbatim: "Maybe we could make a handoff to start improving the ios versions? Making sure it is
rock solid too?" and "We want to do as much work as possible without the Apple account". Stub IOS-2 in
`handoff/PLAN/ios/START-HERE-ios.md` (approved to start there, "Unattended day"). Simulator only; no Apple account.

## Work
**IOS-2. Tap open speed and cue on iOS (T716 parity).** Goal: a tapped water shows something within ~100 ms of the tap
and opens in the Android time class. Do: add `regs-open` to `run-ios.mjs` (press / cue / shown from the `[open]` lines);
measure whether iOS waits for a double-tap before the click; then decide: accept (if click is prompt), or an iOS
`onTapUp` in the patch (decision 4). Acceptance: cold + 2 warm taps on small/mid/lake waters, numbers beside Android's,
double tap still zooms and does not open twice. Size: M (L if a patch).
  Sub-steps: port `scenarios/regs-open.mjs` (press/cue/shown) using the `[open]` lines; measure click delay with and
  without a double-tap recogniser; cold + 2 warm taps on small/unnamed/mid/Fraser/lake waters; double-tap-zoom check;
  write numbers beside Android's (T716); lean iOS `onTapUp` only per the default. Mirrors: Android `regs-open`.

## Acceptance
cold + 2 warm taps on small/mid/lake waters, numbers beside Android's, double tap still zooms and does not open twice.

Depends: T718. Size: M. Owner: yes (decision 4: an iOS tap-up patch). Default: measure first; a lean patch only as a proposal if the delay matches Android's.

## Results

### Phase 1 (2026-11-14, iOS test-bed lane, code only: nothing run on the simulator yet)
**Ported:** `app/tools/testbed/scenarios-ios/regs-open.mjs` (the twin of `scenarios/regs-open.mjs`), run by name:
`node app/tools/testbed/run-ios.mjs regs-open` (not in `all`, so the T718 baseline stays the 11). Same six waters
(small / unnamed / mid / Fraser / Skeena / lake), cold start per case + 2 warm taps (`OPEN_TAPS`, `OPEN_CASES`,
`OPEN_COLD`, `OPEN_CUE_MAX` as Android), the map framed by opening the water from search and its page's back (T340),
the tap at the app's own `[target]` / `[target-other]` (iOS points = dp, no density). New flows `ios-tap-at.yaml`
(one tap, `retryTapIfNoChange: false`) and `ios-double-tap-at.yaml`. Per tap: `[open]`, `cue`, `shown`, `[work]`,
`[hold]` into `T719-open.txt`; fails on a spinner, no cue, cue > 150 ms, or one tap opening twice. Double tap (small):
UIKit must see exactly 2 taps, no `[open]/[shown]/[failure]`, still on the map, >= 20 % of the map changed (zoom).
Twin, not shared: the Android file calls `adb`/`input tap`/`wm`/root. **Unify after the Android tap/back work lands.**

**How the press is timed on iOS (no `input tap`, Maestro's call is a JVM start wide):** from UIKit's own log lines in
the app process (`[com.apple.UIKit:EventDispatch] Evaluating dispatch of UIEvent …; type: 0;`, one per touches event,
~17 ms apart; `lib/device-ios.mjs` `touchLog`) on the Mac clock, which is the simulator app's `Date.now()`. `press` =
`[open].wall` − the tap's LAST touches event (the lift: Android's `tappedAt` is right after `input tap` sent down+up,
so this is the comparable number); `pressFromDown` and `hold` beside it; `gestureMs` = UIKit "send gesture actions"
lines after the lift. Guards: no touch line or no `wall` fails the step (never a host-clock guess); the app/log clock
skew (`[heap].at` vs its log stamp, `clockSkew`) must be within 50 ms. Found while porting: Node 2026a parses the
log's local time an hour wrong (UTC−8 after 1 Nov); `logTime` uses macOS's `date +%z` (skew on ios-t313g: 0 ms).

**Expectation (not yet measured on 0.7.9), with evidence:**
1. The binding's click waits for the double tap to fail. `MLRNMapView.m` init adds its own `UITapGestureRecognizer`
   (`didTapMap:`) and calls `requireGestureRecognizerToFail:` on every `UITapGestureRecognizer` MLNMapView already
   has, MapLibre's double-tap zoom among them; the patch's Obj-C half has no tap code (no `onTapUp` on iOS).
2. Size: about 350 ms from the lift. ios-t313g (0.5.0, 17 Maestro map taps, `app-os.log` re-read 2026-11-14): a
   UIKit gesture action fired 350-352 ms after the lift every time (lift = last touches event; down-to-lift 116-133
   ms). That build had no `wall`, so it is not yet tied to the JS press: the run confirms it if `press` ≈ 350 ms + a
   few ms and `gestureMs` holds ~350; refutes it if `press` is under ~100 ms.
3. So the cue (lit with the click on iOS) is expected at ~360-400 ms: the cue limit (150 ms) is expected to FAIL on
   every tap. Android before T716: 300-332 ms press. This is decision 4's input; no patch in this phase.
4. Double tap: expected to zoom with no cue at all (nothing lights without `onTapUp`) and no open, IF Maestro's two
   taps land inside UIKit's double-tap window; the step prints the gap between them and fails as "not a double tap"
   otherwise (an injector limit, not an app result).

**To prove (phase 2):** `HEAVY_LOCK=$HOME/.claude/heavy-job-2.lock scripts/heavy.sh testbed-ios 30 -- bash
app/tools/testbed/session-ios.sh --max-min 45 -- node app/tools/testbed/run-ios.mjs regs-open` (a `--testbed` build).
Not done: "with and without a double-tap recogniser" needs a build with `doubleTapZoom` off (app code, `packages/map`,
another lane's now).

### Phase 2 (2026-11-14): measured (run `ios-open2`, build 8ba815a23 `--testbed`, iPhone 17 sim, iOS 26.5)
`node app/tools/testbed/run-ios.mjs regs-open`. Cold start per case, then 2 warm taps. On iOS every time is measured
from the lift, which compares with Android's (Android's `input tap` sends down and up together). Android numbers are
T716 "after" (emulator, with its `onTapUp` cue).

| water | iOS press (lift → JS click) | iOS cue | iOS page shown | Android cue | Android page shown |
|---|---|---|---|---|---|
| small (Norrish) | 352 / 352 / 352 | 448 / 407 / 407 | 448 / 408 / 407 | 40 / 37 / 32 | 418 / 416 / 414 |
| unnamed stream | 352 / 352 / 352 | 371 / 401 / 402 | 444 / 401 / 402 | 28 / 37 / 36 | 421 / 397 / 398 |
| mid (Coquitlam) | 352 / 352 / 352 (re-run `ios-mid`, 081d93f90, iPhone 17) | 366 / 434 / 454 | 575 / 434 / 454 | 40 / 59 / 39 | 478 / 440 / 399 |
| Fraser | 351 / 352 / 351 | 382 / 427 / 426 | 545 / 427 / 426 | 40 / 37 / 39 | 514 / 436 / 413 |
| Skeena | 351 / 351 / 351 | 402 / 451 / 495 | 553 / 451 / 495 | 36 / 48 / 48 | 546 / 460 / 445 |
| lake (Cultus) | 351 / 351 / 351 | 429 / 403 / 402 | 429 / 403 / 402 | 42 / 73 / 52 | 380 / 407 / 420 |

- **The expectation is confirmed.** On all 15 taps the JS click came 350-352 ms after the lift (467-485 ms after the
  finger went down; Maestro holds a tap 116-133 ms). UIKit's own "send gesture actions" line fired at the same
  350-352 ms on every tap. So iOS waits about 350 ms for the double tap to fail, about 40 ms longer than Android's
  ~310 ms. JS after the click took 49-202 ms (Android 73-227).
- **No early cue on iOS:** the cue arrives with the page or 0-150 ms before it, 371-495 ms after the lift. Android's
  is 28-73 ms. The cue check (150 ms) fails on every iOS tap. That is the finding for decision 4, not a test-bed fault.
- **Page shown:** iOS 401-553 ms, Android 397-546 ms. Same class: the extra 40 ms of iOS wait is offset by faster JS
  on the Mac.
- **Double tap (small): PASS.** UIKit saw 2 taps 233 ms apart, the map zoomed (32 % of the map changed), no page,
  nothing reported, no cue lit (nothing lights without an `onTapUp`).
- `mid`: the first run's probe found no point on Coquitlam River (`[target] {"none":true}`). The re-run found one
  and measured the same 352 ms. That was a one-off of the probe after the search framing, not the app.

**A lean iOS `onTapUp` (a design for the main lane's decision; NOT written):**
- *Why not just override `touchesEnded:` in `MLRNMapView`* (it already overrides `touchesBegan:`): UIKit holds a
  view's `touchesEnded` while a recogniser with `delaysTouchesEnded = YES` (UIKit's default) could still recognise. A
  double-tap recogniser that has seen one tap is exactly that case, so the override would most likely also arrive at
  ~350 ms. This is UIKit's documented behaviour; it was not measured here.
- *The design:* a tiny private recogniser in the patch's Obj-C half, in `MLRNMapView.m`:
  `@interface MLRNTapUpRecognizer : UIGestureRecognizer`, with `cancelsTouchesInView = NO`,
  `delaysTouchesBegan = NO`, `delaysTouchesEnded = NO`, and a delegate that returns YES to
  `shouldRecognizeSimultaneouslyWithGestureRecognizer:`.
  - It is added in `initWithFrame:` beside the existing `tap`. It is not a `UITapGestureRecognizer`, so the
    `requireGestureRecognizerToFail:` loop just above skips it.
  - `touchesBegan`: note the time and point. A second finger, or a touch with `tapCount > 1` (the second tap of a
    double tap), spoils it.
  - `touchesMoved`: more than 10 pt from the start spoils it. `touchesCancelled` spoils it.
  - `touchesEnded`: if the tap is still clean and lasted under 0.5 s (long press), call `reactOnTapUp` with
    `[MLRNMapTouchEvent makeTapEvent:self withPoint:p].toJSON`, the same payload as `onPress`. Then always
    `self.state = UIGestureRecognizerStateFailed`, so it never recognises, never blocks and never delays anything.
  - `MLRNMapView.h` gets 1 line: the `reactOnTapUp` block property.
  - `MLRNMapViewComponentView.mm` `prepareView` gets about 13 lines: `setReactOnTapUp` emits
    `MLRNMapViewEventEmitter::onTapUp`, a copy of the `onPress` block. The `onTapUp` codegen event already exists:
    the patch added it to the shared TS spec for Android.
  - About 45 added lines in total, Obj-C only. No JS change: `Map.native.tsx` already passes `onTapUp`, and
    `tapLift.ts` already answers the click from the lift.
- *Double tap:* the first tap of a double sends `onTapUp` (the cue lights), the second (`tapCount 2`) sends nothing,
  and MapLibre zooms. The app's `pressCue` puts the cue out by itself after 700 ms when no click follows, as on
  Android. The double-tap step above would then see "lit, then out" (Android's pass condition) instead of "never lit".
- *Expected effect:* cue about 30-50 ms after the lift, as Android; page shown unchanged (the click still opens).
- *Risk:* one more file to re-apply on a binding bump (the iOS half grows by about 45 lines), and the simulator and
  emulator pans to re-run (`app/deps.md`).

### Phase 3: iOS onTapUp (proposal, branch ios/t719-tapup)
**Built (95ef83d77, not merged):** the design above, in the Obj-C half of
`app/patches/@maplibre__maplibre-react-native@11.5.0.patch` (edited with `pnpm patch` / `pnpm patch-commit`; lockfile:
only the patch hash changed):
- `MLRNMapView.m`: `MLRNTapUpRecognizer`, a plain `UIGestureRecognizer` (`cancelsTouchesInView`/`delaysTouchesBegan`/
  `delaysTouchesEnded` = NO, its own delegate answering YES to simultaneous recognition). It never recognises: every
  touch ends in `Failed`. A single-finger touch with `tapCount` 1 that lifts within 10 pt of where it went down and
  under 0.5 s calls `reactOnTapUp` with `[MLRNMapTouchEvent makeTapEvent:…].toJSON` (the click's payload). Added after
  the init loop, so the binding's `tap` does not wait on it. About 80 lines with the comment.
- `MLRNMapView.h`: the `reactOnTapUp` block (1 property). `MLRNMapViewComponentView.mm`: `setReactOnTapUp` emits
  `MLRNMapViewEventEmitter::onTapUp`, a copy of the `onPress` block.
- The codegen spec already had `onTapUp` (T716): nothing added to it. JS: comments only (`Map.native.tsx`, `tapLift.ts`,
  `BrowseHost.tsx`, the iOS scenario header, `app/deps.md`). The same `onTapUp` → `onPressStart` → `tapAhead` path
  Android uses now runs on iOS; the double tap's "lit, then out after 700 ms with no click" is the same `pressCue` code.

**Measured** (`regs-open`, build 294de8421 `--testbed`, iPhone 17 Pro Max sim, iOS 26.5; run
`app/tools/testbed/out/runs/ios-2026-11-15T03-23-26`, 18/18 taps + double tap PASS). ms after the lift, cold / warm / warm:

| water | before (phase 2): cue | after: cue | before: page shown | after: page shown | after: press (click) | Android cue (T716 r2) |
|---|---|---|---|---|---|---|
| small (Norrish) | 448 / 407 / 407 | 18 / 72 / 8 | 448 / 408 / 407 | 378 / 379 / 380 | 352 / 352 / 352 | 54 / 45 / 43 |
| unnamed stream | 371 / 401 / 402 | 18 / 68 / 60 | 444 / 401 / 402 | 380 / 379 / 378 | 353 / 352 / 351 | 32 / 22 / 32 |
| mid (Coquitlam) | (no target) | 22 / 19 / 19 | (no target) | 385 / 384 / 378 | 352 / 351 / 352 | 50 / 73 / 61 |
| Fraser | 382 / 427 / 426 | 18 / 67 / 69 | 545 / 427 / 426 | 377 / 374 / 385 | 351 / 351 / 352 | 55 / 66 / 39 |
| Skeena | 402 / 451 / 495 | 18 / 115 / 121 | 553 / 451 / 495 | 381 / 390 / 383 | 351 / 352 / 352 | 59 / 83 / 44 |
| lake (Cultus) | 429 / 403 / 402 | 19 / 59 / 51 | 429 / 403 / 402 | 382 / 376 / 375 | 353 / 352 / 352 | 58 / 112 / 93 |

- **Cue: 8-121 ms** after the lift (was 371-495); every tap within the 150 ms limit. Each tap opened its page once.
- **Page shown: 374-390 ms** (was 401-553): the click still comes 351-353 ms after the lift (the double-tap wait is
  kept), but the open now starts at the lift (`tapAhead`), so click → page is 22-38 ms (was 49-202 ms).
- **Double tap (small): PASS**: UIKit saw 2 taps 217 ms apart, the map zoomed (32 % changed), no page, nothing
  reported, cue lit then out (`1,0`), Android's pass condition.
- **`tap-stalls`: PASS** on this build (run `…/ios-2026-11-15T02-18-02`): cold start worst 200 ms, Salmon 0, Run timing
  160, bell 0, back to Standard 0.
- Earlier runs of the same build failed only on the test bed's data: `--fresh` on a new simulator downloads 479 MB, and
  each cold start of the scenario killed the download, which restarts every file from 0 (`attempt 1, 0 of N bytes on
  disk` on every launch). Run 2 (`…/ios-2026-11-15T02-18-02`, `first-open regs-open tap-stalls`) let it finish; its
  mid / Fraser / lake taps (cue 6-77 ms) agree with the table. Its `first-open` "More → About" step failed (the `More`
  tab not found by text on the Pro Max; not looked into, not this change).

**Not proven:** a real iPhone (the simulator runs on the Mac's CPU; touch timing is Maestro's XCTest synthesis, one
finger, no drift); a real finger's drift between down and lift (the click's point must be within 2 dp of the lift's for
the click to reuse its hit test, else it asks again, as before); a pinch / pan / long press starting on a water (the
recogniser fails on a second finger, > 10 pt of movement and >= 0.5 s, by reading, not by a scenario); VoiceOver.

**Android:** expected to see nothing different. The patch's Kotlin sections and every JS/TS section (the shared codegen
specs, `lib/`, `src/`) are byte-identical to the T734 patch (checked file section by file section: only
`ios/components/map-view/MLRNMapView.{h,m}` and `MLRNMapViewComponentView.mm` differ); the app's JS changed only in
comments. The Android benches are the main lane's to run before any merge.
