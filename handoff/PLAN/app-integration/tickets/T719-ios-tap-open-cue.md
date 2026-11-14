# T719 — iOS: tap open speed and cue, T716 parity (IOS-2)

Status: IN PROGRESS (iOS chat, test-bed lane: `regs-open` ported; the simulator numbers and the tap-up design are in the run).

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
| mid (Coquitlam) | no target: `[target] {"none":true}` (the probe found no point on the framed river; test bed, see below) | | | 40 / 59 / 39 | 478 / 440 / 399 |
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
- `mid`: the app's own `[target]` search found no point on Coquitlam River after the search framing (T689's "searched,
  found none"). A test-bed case to re-run (Android found one). Not an app open-speed result.

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
