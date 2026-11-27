# T1207 — A water closed all year to salmon and game fish shows ONE schedule, not two

Status: IN PROGRESS — option A built (lane T1207, branch feat/T1207-one-schedule-when-all-closed); release: 0.10.0 re-cut.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1207 | A water closed all year to salmon and game fish shows one schedule, not two (splits again when they differ) | IN PROGRESS — A built, 0.10.0 re-cut | see git | owner picked A 11-26; built behind `closedWithSalmon`; review + the 0.10.0 re-cut |

Source: the owner, 2026-11-26, via the planning chat, verbatim: "we could combine the salmon and game fish schedules for
year round closures tho if they are both always closed (they could split again if it ever changes)."

## What the page shows today (all-closed water, Nelson Creek `gnis:20408`, live release r-0aec88ed3edb, 412 px)
`KeepTab.tsx` draws, top to bottom: the SALMON `[B.C. SYNOPSIS]` block (`salmonBar`, `SalmonYearStrip`: "✕Closed today", a
year bar, "✕ Closed" legend), the GAME FISH `[B.C.]` block (`YearStrip`: the same again), then the closed banner ("Closed
to all fishing all year, salmon included", the book row's line, Source). The three say one thing. The Species and limits
list is NOT drawn on this water (no rows; the Salmon fold is dropped under the merged banner, `KeepTab.tsx` `folds`:
`!keep.banner?.salmon`). Where the water is not merged, the list is drawn (Kirkup, Old Kootenay in the contact sheet).
Pictures: `T1207-shots/contact-sheet.png` (today | A | B | C | A dark | Kirkup | Old Kootenay).

## How it changes T1052 and T1078
- T1052 (owner 2026-11-24, "One block, both calendars"): merged the two STATEMENTS and kept both calendars on purpose
  ("the 2 schedule calendars are okay though"). T1207 reverses that half: on the same waters the two calendars become one.
  Nothing else of T1052 changes (the headline, the line words A/B, which are still an open ask there).
- T1078 (owner 2026-11-26: the banner cites ONE closer, the primary one) is untouched: the banner text stays.

## When the schedules combine, and when they split
Gate = core `closedWithSalmon` (`core/src/regs/withSalmon.ts`), unchanged: `banner.salmon === true` is the one switch,
and the UI draws one schedule exactly when it is true. It splits again by itself the moment the page stops passing the
gate (a day or release where either side differs), because the page is rebuilt from the data every time; no state is kept.
Counted on the live release r-0aec88ed3edb on Nov 26, 2026, every part of every water, DFO live feed not included
(20,956 parts): **936 parts on 936 waters combine** (as T1052 measured). Why the rest of the "game fish closed all year"
pages stay two:
- 181 parts: same rules but `unless_opened` (a park closure another authority may lift) or another refusal. Stay two.
- 107 parts (48 waters): "closed all year on N stretches" (merged banner). Stay two.
- 6 parts: salmon closed all year by a DIFFERENT book rule than the game-fish closure. Stay two (two sources).
- 2 parts (Kirkup `gnis:12661`, `gnis:1806`): salmon closed only part of the year by the book. Stay two.
- 5 parts: salmon decided by DFO, not the book. Stay two. 1 part: a notice / finfish / caveat on the card. Stay two.
- 296 parts (260 waters): game fish closed today but reopens in the year (part-year). Stay two; the 130 of them closed by
  the same book rule are the T1052 follow-up ("Yes, merge those too"), pictures first: it would use the same switch.
- A DFO in-season notice or finfish closure arriving: `card.issues` / `card.finfish` fail the gate, so the page splits at once.
- A water with sections: the gate is per page (part), as T1052. Each part decides on its own.
The 6 different-rule parts could be combined only by changing the gate (both closed all year, either source): not
proposed, since it hides which source closes which fish.

## Options (pictures: `T1207-shots/nelson-{today,A,B,C}-light.png`, `nelson-A-dark.png`)
- A. ONE block, ONE bar: title "Salmon and game fish", "✕Closed today", one year bar, the banner below. Smallest change.
- B. ONE block, NO bar: the banner alone leads ("Closed to all fishing all year, salmon included", the line, Source). A
  bar that is all closed adds nothing the headline does not; it also loses the "today" marker and the calendar.
- C. As A, with both source tags small beside the title ("Salmon · Game fish" [DFO · B.C.]): keeps the DFO / B.C. label.
Where the two differ (Old Kootenay River Channel `gnis:2123`: salmon closed, game fish open with a spring closure) every
option splits back to today's two blocks (`old-kootenay-differ-light.png`).
Species and limits on a merged water: not drawn today (nothing to keep); proposal: leave it so.
Recommended: A. The owner asked for the calendars combined, so B (no calendar) is more than he asked; C's second tag
reads as clutter on a page with no second source.

## Work (after the pick)
1. `KeepTab.tsx`: on `keep.banner?.salmon` draw the chosen block in place of `salmonBar` + the game-fish strip (no logic
   in the UI: core says which; `strip` stays the game-fish strip, same status as the salmon's by the gate).
2. Test: through `WaterScreen.oneClosure.test.tsx` the merged water has one calendar; the six split cases keep two.
3. Pictures after: Nelson light + dark, Kirkup (split), Old Kootenay (split).

## Acceptance
Nelson Creek draws one schedule; Kirkup, Old Kootenay and every non-gated page draw two as today; counts above hold
(936 waters); a mutant that draws one schedule on a page the gate refuses fails a test.

Size: S. Depends: T1052 (merged gate). Owner: yes (the pick). Default: no change until he picks.

## Built (option A, owner pick 2026-11-26)
- Core: `closedWithSalmon` (`core/src/regs/withSalmon.ts`) also sets `banner.schedule` = `{ title: SOURCE_WORDS.both, tag: SOURCE_WORDS.salmonTag.book }`
  ("Salmon and game fish", tagged "B.C. Synopsis": the merged closures are the rule book's, never DFO's); type in `model.ts`, words in `glance.ts`.
  One gate, one function; the screen only draws. No stored state: a page that fails the gate has no `schedule` and draws two blocks.
- UI: `KeepTab.tsx` draws no Salmon block and gives the game-fish block `schedule.title` / `schedule.tag`. `SourceTag.tsx`: the title row wraps (the tag
  drops under the name at a large text size, kept one line); the block's header names both fish for a screen reader.
- Other places checked: Gear / Methods / Licence tabs are hidden on a fully closed page (strip closed), as before; the water's Map view and the map card draw
  no salmon block (nothing to change); the public water page (`tools/seo/pane.tsx`) draws the app's own `Shell`, so it follows the same code.
- Tests: `core/src/regs/withSalmon.test.ts` (schedule set on Nelson, absent in every split case), `ui-native/src/WaterScreen.oneClosure.test.tsx` (one block / one bar on a merged
  water; two on DFO-decided, another rule, part-year), `tools/one-schedule.release.test.ts` (live release: Nelson one; Kirkup and Old Kootenay two),
  `tools/one-schedule-title.test.ts` (title and tag whole at 320 / 360 / 412 wide, text 1 to 1.3, light and dark).
- Pictures of the built app: `T1207-shots/built-sheet.png` (today vs built, dark, 360 wide at 1.3x emulated by scaling the page's font sizes, the two split waters unchanged).

## Owner

- [x] 2026-11-26 — Combine the two year calendars on an all-closed water: A, B or C? → "One block \"Salmon and game fish\" with one year bar" (2026-11-26)
