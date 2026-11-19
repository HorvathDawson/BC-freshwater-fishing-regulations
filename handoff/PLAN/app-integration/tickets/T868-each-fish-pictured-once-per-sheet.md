# T868 — Each fish's picture once per sheet (the Whitefish sheet drew them twice)

Status: PARTIAL — 7b0738a3f + 940dde702 on lane app/fix-dup-pics (off app/integrate-081-web); left (Claude): merge into
app/integrate-081-web (0.8.1 web; phones at their next build).

Owner 2026-11-18 ~20:20 (Android, dark theme; Coquitlam River → Regulations → What you can keep → the Whitefish sheet),
verbatim: "Has duplicate images here too".

What the screenshot shows: the sheet's head row draws Lake whitefish and Mountain whitefish with their links, then the
group card ("Whitefish REGION 2-WIDE · 15 a day …") draws the SAME two pictures and links again under its keep line "Up
to 15 whitefish combined · any size".

Cause: `core/src/regs/sheets/other.ts` `otherSheet` pushed a `pictures` block of the row's members first, then, for a row
with a stored group, the `group` block, whose kind rows picture the same fish (`tree.ts` `kindRow`). Two presenters, each
right alone, both drawn on one sheet.

Fix (core; the components draw what they are handed):
- The keep line keeps the pictures (it names the count and the size); the head row keeps only a fish the card does not
  picture (`tree.ts` `picturedIn`), and is dropped when that leaves none.
- Inside a group card, each fish pictured once (`tree.ts` `onePictureEach`, applied in `keep.ts` `groupOf`): the first
  keep line keeps it; a later kind row, a release row in the tree, or a not-kept line drops it. No live page has such a
  card today (measured below); it is the guard for the same pattern.
- (ui-native) With the head row gone, the card's heavy rule sat one line under the sheet's title rule: a group card
  that opens its sheet (`SheetBody` first block) draws no rule of its own (`GroupCard` `ruled={false}`); anywhere else it
  keeps it.

Sweep (every water of release r-fc137c4040fa, its first part, on Nov 18, Jun 15, Apr 1, Jan 10 and Sep 10: every Other
fish sheet, the Keep tab's group cards, plain rows and trout table; on Nov 18 also every src:, share:, multi:, sp: and tf:
sheet the page can open):
- Other fish sheet with a group card: 43,565 doubled fish on Nov 18 (29,651 to 43,199 on the other days). Fixed: 0.
- Group card (kind / release / not-kept), plain row (release pictures), src/share/multi/sp/tf sheets: 0 found.
- Keep tab group folds: none drawn since the T810 cutover (`ui-native/src/regs/testing/otherGroup.ts`: `keep.groups` is
  empty on every page); a group card is drawn only in an Other fish sheet.
- Trout table: 20 to 44 fish a day (Dodd Lake, Horseshoe Lake, Humphrys Creek …) drawn on two keep rows, the hatchery row and the
  wild row. Left: each is its own table row with its own limit and origin label, and the tile is the row's picture in the
  table's grid (owner 2026-11-17 "Keep the tile"), not a second picture of a line already drawn.

Tests: `ui-native/src/regs/OnePicture.test.tsx` (every Other fish sheet of the committed regs fixture, built by core and
drawn by SheetBody: no `pic:` twice, a group sheet's pictures sit in the card's keep row; failed before the fix with 84
sheets, Wilkes Creek LW/MW first; and the card under the title rule draws no rule of its own),
`core/src/regs/tree.test.ts` "(T868) each fish pictured once in a group card".

Pictures (390 wide, dark, the Coquitlam River's Whitefish sheet from release r-fc137c4040fa, drawn by the real SheetHost
in headless Chrome): `t868-shots/coquitlam-whitefish-390-dark-before.png` and `-after.png` in the app chat's scratchpad.
