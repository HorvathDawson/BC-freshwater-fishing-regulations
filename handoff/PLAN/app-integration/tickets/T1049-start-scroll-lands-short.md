# T1049 — A start link's scroll lands short: it jumps to the first measured section top and never re-checks

Status: DONE — ae65ef767, a2478c6dc, d2239c54d, b3de2bbc1 (below), 09f6346e0, eb8e2092a, 5f8a37cbc (cold start on a slow phone, 0.9.1: the follow waits on wall time for the page to settle and for the salmon card still coming, capped 30 s and said; device: slow cold `tab=gear` 6/6, `tab=licence` 6/6 landed, was 2/12 and 0/11; a drag stops it; warm re-apply lands; web `tab=gear` 1156, `tab=licence` 2075 px). Earlier: a reader's drag, tab press or own scroll ends the follow; a tab never drawn said after ~5 s; a start off Regulations dropped and said; web check 2026-11-23: cold `tab=gear` Gear 1156 px, `tab=licence` Licence 2020 (was Methods 1818), `tab=methods` Methods 1676, `view=map&tab=gear` the Map with the drop said.

## 0.9.0 device pass (Android)
- A cold start from `https://staging.canifishthis.ca/?regs=gnis:3062&tab=licence` (once the phone opens links with a
  page start, T1038): Licence selected, the page at Licence's top, not Methods.

Source: independent review of #80 (T1038), 2026-11-23; on main too. On staging the Keep tab grows 903 → 1104 px about
35 ms after the first layout, so a start's jump (`RegsScreen`, polled until the tab's section has a top, then once) lands
on the old top: `tab=licence` showed Methods (1818 px), `tab=gear` landed short on a cold load (937 vs 1139). The same
code serves the web start link and the phone's link. Also: a link to a water's Map or Conditions view kept its
tab / sheet / scroll as a start that waited for the next Regulations page and landed there unasked.

## Work
1. The start's jump follows its section while the page settles: made again while the target moves, done once it has held
   (two 100 ms ticks), at most ~2 s. Web and phone (one component).
2. A start on another view than Regulations is dropped, and its keys are said (`dropped`, `reportDropped`), never kept to
   land later — one rule for the web and the phone (`pageStartOn`; the phone's link reports them as its problems).
3. Tests that fail on main first, proved by mutation; web check; device check on the 0.9.0 device pass.

Depends: none (T1038 #80 touches the same effect: whichever lands second is rebased). Size: S. Owner: no.

## Review lows (0.9.1)
Source: the review of PR #83, two lows, both confirmed in the code.
1. "Late own-scroll event past 150 ms on slow phone": real. `RegsScreen` told the follow's own jump from the reader's by
   `START_JUMP_MS = 150` after each jump; one component serves web and phone, so one fix. Now by position
   (`isStartOwnScroll`, core `tabScroll.ts`): an event at a y the start asked for (within 1 px; every y asked, so a late
   event of an earlier jump counts) is its own; a y asked beyond the page's end lands on the end (content - view, read from
   the scroll event) and that is its own too; before any jump only a scroll off the top is the reader's. A programmatic
   jump has no momentum, so nothing else needs a case.
2. "Drag stop has no own test": real (the tests only fired scroll events). Added: the native `onScrollBeginDrag` prop is
   called (react-native-web never fires it itself) and the follow stops.
Tests fail on the old code first (late event, reader at a y inside the old window, clamped). Mutations killed: the time
window restored (3 tests), the drag stop dropped (1), the clamp case dropped (2), only the last y kept (1). Slow-phone
case proven by test only (no emulator).


## Cold start on a slow phone (0.9.1)
Source: the 0.9.0 device pass (sideload release build, live staging feeds): a cold start from `…?regs=gnis:3062&tab=gear`
landed 0 of 6 (Gear's top at 1266 px, the scroll's top at 737), `tab=licence` 1 of 6 (short on Methods); warm landed.

Measured (lane L13, emulator `n1_pixel_b` on E-cores, R8 test-bed APK, the `[start]` probe lines of every run, the
`start-land` scenario; the bench proxy holds every connection 16 s so the live feeds land after the page):
- CAUSE: the salmon card is not in at the page's first layout. `salmon.latest()` asks the network for the DFO index on
  every cold start, so the page first draws the card's pending face; the card lands 0.4-15 s later and the sections move
  down ~150-200 dp (gear 1011 → 1160, licence 1965 → 2114 content px). The follow had already ended "held" ~0.35 s after
  mount: its two 100 ms ticks ran back to back on the busy JS thread, in the same ms as the jump and before the jump's own
  scroll event. No run ended "taken": no scroll event was ever judged the reader's. Without a late feed (a fast line, or
  offline: the capture answers at once) the card is in at the first layout and every start lands, before and after.
- Not the cause: the CPU alone (6 busy loops on the phone, page at 38 s: landed), the native ScrollView (no scroll moved
  by a layout), late own-scroll events.
- #107 (the position rule) A/B, one APK with a JS-only switch per run, same slow line: gear 1/6 with it, 1/6 without;
  licence 0/6 with, 0/5 without (one run's page never showed). The two rules agreed on every scroll event logged: #107 is
  no different here (kept on release/0.9.0).

Fix (09f6346e0): core `startFollow.ts` (`startTick`, pure, wall time): the start re-jumps while its target moves or the
page could not take a jump whole (clamped at the end, then the page grew; a scroll left off its landing 300 ms), and is
done once target, content height and scroll held still 300 ms with nothing still `coming`; capped 30 s from the section
being drawn, and the cap is reported (`reportFailure`). `WaterScreen` passes `coming` = the salmon card expected and still
loading (the same condition the pane waits on). `isStartOwnScroll` also owns a jump clamped at an end the page had while
it followed (the #107 review's remaining low). A drag, a tab press or a reader's scroll still ends it at once.

Device, after (same setup, build eb8e2092a): slow cold `tab=gear` 6/6 landed (all six with the card landing late,
0.4-12.8 s after the page: two jumps each, Gear at the scroll's top), slow `tab=licence` 6/6 (four with the card late, up
to 15.4 s); an earlier batch of the same build 5/5 gear (one run lost to an adb timeout) and 6/6 licence while the phone
had gone offline; no proxy 4/4. A drag 0.3 s into a follow still waiting on the card: done "taken" 66 ms later, no jump
after. Warm re-apply of `tab=gear` on the open page: landed (1160). Before (main + the probe, same setup): gear 2/12,
licence 0/11.
Web (Expo web, staging, 390 px, cold): `tab=gear` Gear at 1156 px, `tab=licence` Licence at 2075 (the scroll's top); a
reader's scroll during the follow stays where the reader put it. The web's card comes with the page here, so the web did
not exercise the wait; the same component and condition serve it.
Tests: core `startFollow.test.ts` (the device pass's card 6 s on, steps over 22 s with a 3 s pause, a clamped target, a
layout moving the scroll, a page moving for ever, ticks back to back), `RegsScreen.test.tsx` (3 fail on main first),
`WaterScreen.startComing.test.tsx`; 14 mutations all killed.
Not verified: a real phone; iOS; a start whose card never arrives (the 30 s cap and its report, by test only).
