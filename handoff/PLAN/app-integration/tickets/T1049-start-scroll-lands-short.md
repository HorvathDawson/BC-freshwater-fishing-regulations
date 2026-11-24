# T1049 — A start link's scroll lands short: it jumps to the first measured section top and never re-checks

Status: PARTIAL — ae65ef767, a2478c6dc, d2239c54d (a reader's drag, tab press or own scroll ends the follow; a tab never drawn said after ~5 s; the jump follows its section until it holds two ticks, at most ~2 s; a start off Regulations dropped and said; tests fail on main first, 8 mutations all killed; web check 2026-11-23: cold `tab=gear` Gear 1156 px, `tab=gear` Gear 1139, `tab=licence` Licence 2020 (was Methods 1818), `tab=methods` Methods 1676, `view=map&tab=gear` the Map with "start link: tab= does nothing on view=map: dropped" in the console); review lows fixed b3de2bbc1 (below); left (Claude): the device check below.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1049 | A start link's scroll lands short: it jumps to the first measured section top and never re-checks | PARTIAL — fixed + web check; left (Claude): 0.9.0 device pass | ae65ef767, a2478c6dc, d2239c54d, b3de2bbc1 | review of #80 (T1038), 2026-11-23 |

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

