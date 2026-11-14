# T719 — iOS: tap open speed and cue, T716 parity (IOS-2)

Status: OPEN.

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
