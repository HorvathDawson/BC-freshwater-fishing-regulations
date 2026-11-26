# T1049 — A start link's scroll lands short: it jumps to the first measured section top and never re-checks

Status: PARTIAL — ae65ef767, a2478c6dc, d2239c54d, b3de2bbc1 (below), 09f6346e0, eb8e2092a, 5f8a37cbc, 53e9106e2 (cold start on a slow phone, 0.9.1: the follow waits on wall time for the page to settle and for the salmon card still coming, bounded (30 s with nothing coming: a defect report; 180 s with a part still coming: dropped as the line's), spent once for its page; device: slow cold `tab=gear` 6/6, `tab=licence` 6/6 landed, was 2/12 and 0/11; a drag stops it; warm re-apply lands; web `tab=gear` 1156, `tab=licence` 2075 px). Earlier: a reader's drag, tab press or own scroll ends the follow; a tab never drawn said after ~5 s; a start off Regulations dropped and said; web check 2026-11-23: cold `tab=gear` Gear 1156 px, `tab=licence` Licence 2020 (was Methods 1818), `tab=methods` Methods 1676, `view=map&tab=gear` the Map with the drop said. left (Claude): review and merge of #122, then the device check on a release build.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1049 | A start link's scroll lands short: it jumps to the first measured section top and never re-checks | PARTIAL — 0.10.0: a slow cold start waits for the salmon card, the in-season notices and a settled page; device gear 6/6, licence 6/6, was 2/12, 0/11; #107 no different; left (Claude): review and merge of #122, then the device check on a release build | ae65ef767, a2478c6dc, d2239c54d, b3de2bbc1, 09f6346e0, eb8e2092a, 5f8a37cbc, 53e9106e2 | review of #80 (T1038), 2026-11-23; 0.9.0 device pass |

Source: independent review of #80 (T1038), 2026-11-23; on main too. On staging the Keep tab grows 903 → 1104 px about
35 ms after the first layout, so a start's jump (`RegsScreen`, polled until the tab's section has a top, then once) lands
on the old top: `tab=licence` showed Methods (1818 px), `tab=gear` landed short on a cold load (937 vs 1139). The same
code serves the web start link and the phone's link. Also: a link to a water's Map or Conditions view kept its
tab / sheet / scroll as a start that waited for the next Regulations page and landed there unasked.

## 0.9.0 device pass (Android)
- A cold start from `https://staging.canifishthis.ca/?regs=gnis:3062&tab=licence` (once the phone opens links with a
  page start, T1038): Licence selected, the page at Licence's top, not Methods.

## Done
- ae65ef767, a2478c6dc, d2239c54d, b3de2bbc1: the start's jump follows its section while the page settles; a start on
  another view than Regulations is dropped and its keys said (`dropped`, `reportDropped`, `pageStartOn`); one component
  serves the web and the phone.
- 0.9.1 review lows (PR #83): own-scroll told by position, not `START_JUMP_MS = 150` (`isStartOwnScroll`, core
  `tabScroll.ts`); a drag stop test (`onScrollBeginDrag`).
- 09f6346e0, eb8e2092a (cold start on a slow phone). Cause (lane L13, emulator `n1_pixel_b`, `start-land` scenario, a
  bench proxy holding every connection 16 s): the salmon card is not in at the first layout (`salmon.latest()` asks the
  network for the DFO index on every cold start; the card lands 0.4-15 s later, sections move down ~150-200 dp) and the
  follow had ended "held" ~0.35 s after mount. #107 (the position rule) A/B: no different (kept on release/0.9.0). Fix:
  core `startFollow.ts` (`startTick`, pure, wall time): re-jumps while its target moves or the page could not take the
  jump whole; done once target, content height and scroll held still 300 ms with nothing `coming`; `WaterScreen` passes
  `coming`. Device after (build eb8e2092a): slow cold `tab=gear` 6/6, `tab=licence` 6/6 (before: gear 2/12, licence
  0/11); a drag stops it; warm re-apply lands. Web: `tab=gear` 1156 px, `tab=licence` 2075. Tests: `startFollow.test.ts`,
  `RegsScreen.test.tsx`, `WaterScreen.startComing.test.tsx`; 14 mutations killed.
- 5f8a37cbc, 53e9106e2 (review of #122, 28 mutations killed): with a part still `coming` the 30 s bound goes on to
  `START_COMING_MAX_MS` (180 s, above the feed's own give-up ~152 s; pinned by `data/src/net/startFollow.policy.test.ts`),
  then dropped and said as the line's (`environmentError`); the 30 s bound with nothing coming stays a defect report
  (`startEndSentence`); a start is spent once, for its page (`onStarted`, `startSpent`); in-season notices count as
  coming (`useInSeason`, `useInSeasonBanners`); a sheet the reader opens ends the follow; its own clock (gaps ≤ 1 s, none
  while the app is away); a page end kept for the clamp rule only once real (`startEndOf`). Device (build 53e9106e2,
  emulator-5556, slow line): `tab=gear` 3/3, `tab=licence` 3/3, drag "taken" 67 ms later, warm re-apply landed.

STILL NOT COVERED: growth that lands more than 300 ms after a still page with nothing said to be coming: fonts or
images that resize late, or any part the page does not count as `coming`. Such a start lands on the last top it held.
Not verified: a real phone; iOS; a start whose card never arrives (the bounds and their reports, by test only).

## Left
- Review and merge of #122, then the device check on a release build (the 0.9.0 device pass above).

Depends: none (T1038 #80 touches the same effect: whichever lands second is rebased). Size: S. Owner: no.
