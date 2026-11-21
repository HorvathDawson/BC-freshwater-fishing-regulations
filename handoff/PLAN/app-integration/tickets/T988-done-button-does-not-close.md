# T988 — The Sources menu's Done button doesn't close the menu; check every menu/sheet like it

Status: IN PROGRESS — lane/085-t988: pin test landed (all green); cause NOT reproduced off-device; left (Claude): merge + combined review.

## 2026-11-21 lane note
- Read every Done/Close/Back/scrim path (Sheet phone + wide, SheetHost incl. the regs Sources sheet, DaySheet native/web, MenuPage, FirstRunNotice -> Sources page, bell, SyncError, SpotDrop, JoinTestCard). `app/packages/ui-native/src/closingControls.test.tsx` (49 tests) mounts each under the real back layers, with and without the browser-history sync, presses each control and requires the surface to leave the tree: all close. No code defect found in jsdom, so no source change.
- Unverified (device): the report may be an iOS/Android Modal hand-off (first-run notice stepping aside while the Sources page presents) that jsdom cannot show. Needs a device repro: which screen the Sources menu was opened from.

Source: owner 2026-11-21: "the done button in sources menu doesnt close menu check for other menus like this".

## Work
- Reproduce: Sources menu, tap Done: the menu stays open. Fix at the cause.
- Audit every menu, sheet and dialog with a Done / Close / OK / Cancel button (and the scrim / back gesture): each one
  closes. Same on Android, iPhone and web (AGENTS 34a: a platform fix goes in that platform's file).
- Pin with one test per closing control (or one table-driven test over all of them).

## Acceptance
Every Done/Close button closes its menu on every platform; the test fails if one stops closing.

## Owner 2026-11-21
Where: the trout table's Sources menu. Platform: Android (0.8.4).
