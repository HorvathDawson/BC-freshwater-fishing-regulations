# T907 — "Species and limits" table: no B.C./DFO tag on rows, one value column, where each answer comes from

Status: IN PROGRESS — built on lane app/t907-species-table (2026-11-19 ~10:30); left (Claude): owner sees pictures, then integrate (main chat) + a new staging data release.

Owner 2026-11-19 ~06:50 (main chat), on a dark staging shot of the "Species and limits" table (Salmon DFO closed;
Trout and char B.C. Release ⌄; Kokanee B.C. release ›; Whitefish 15 a day; …), verbatim: "for 4. in the menu for each
species it is good. we should have somethign simialr in table too. also the trable is weird spacing with the same words
liek release (look image) and the bc/dfo tag in the table can go i think. the tag next to the calendar llike salmon dfo
+ rule book or game fish b.c are fine still".

## Work
1. Drop the B.C. / DFO tag from each table row (T892's table marks); the tags beside the calendar ("Salmon · DFO +
   rule book", "Game fish · B.C.") stay.
2. One value column: the same case, weight, size and right edge for every row ("Release" on Trout and char vs
   "release" on Kokanee today; the trout fold's ⌄ shifts its value left of the others' ›).
3. "Something similar in the table" to the species sheet's "How this was decided": see the owner's pick below.
Pictures 390 light + dark and desktop before it lands. Related: T879, T892, T883, T890.

## Owner 2026-11-19 ~06:55: which line (dialog)
Asked whether every table row should name its source, only tributary rows, or only non-region rows; owner, verbatim:
"i meant in the trout table. simialr to how it has "this water" ". So: the TROUT table's rows say a tributary source
the way the trout table already marks "this water" (same slot, same style, the sheet's kicker words: "Trout Lake’s
tributaries"), rather than a separate grey sentence under the fish. The other fish rows are not asked about here.

## What was built (2026-11-19 ~10:30, lane app/t907-species-table)
- **The trout table's "this water" mark** is the keep box's tag: "Lake’s rule" / "Creek’s rule" / "River’s rule"
  (`words.tag_of`, from "This lake’s rule"; 210 tables in r-379e72fc9524, e.g. Granby River, Kootenay River), drawn in
  the box strip beside its head: kicker face, accent colour, 1 px accent outline (KeepBox `keep-tag`). A row has no
  "this water" mark of its own.
- **3. The tributary source in that look, on the row:** a tag beside the fish name, the same component as the box's
  (`Tag`, testID `keep-row-tag`), words "Trout Lake’s tributaries" / "A’s and B’s tributaries" (the sheet kicker's
  waters). The grey "… rule for its tributaries" line under the name is no longer drawn. Python composes the words:
  NEW optional `TRow.tag` (`words.row_tag`, the same waters as `row_down`, written by `down.with_down` beside `down`;
  model `TAG_FORM`, never without `down`; J `down_problems` reads it back; app `checkTrout` the same refusals).
  `TRow.down` stays in the data (old words) so the installed 0.8.x apps keep their grey line and read the release
  (they ignore `tag`); the trout section stays 5, no minAppVersion needed. A release before this carries no tag: the
  new app then shows neither (counted, not failed, in `release-tributary-lines.test.ts`, `BEFORE_T907`).
  Not done: hoisting a tag to the box when every row of a box shares it (Moyie River's 6 rows each tagged): boxes of
  kind release/closed may not carry a tag today (model + both apps' checks), so it is a section-version change; ask
  the owner if the repeat bothers him.
- **1. No B.C. / DFO tag on the table rows** (Salmon, Trout and char, every Other fish row); the two calendar heads keep
  theirs ("Salmon · DFO" / "DFO + rule book", "Game fish · B.C."). App only (KeepTab / KeepBlocks).
- **2. One value column:** every row ends in one 18 px end slot (`row-end`: the Other fish ›, the folds' ⌄/⌃, or empty
  on a plain row), so every value ends on the same right edge; same face (regsBodyBold) as before. Case: **lower
  case**, the app's own for these values (`WORDS.RELEASE_LC`, `CLOSED_LC`, "15 a day", the status key "release ⓘ
  closed ⓘ", Salmon "closed"): Python's trout fold words lowered (`FOLD_RELEASE/CLOSED/NONE` "release", "closed",
  "none to keep"; `fold_apart` "up to 6 rainbow, 4 others"). The trout TABLE's own "↩ Release" column word is
  unchanged. A quota group's "Details" fold (other fish) keeps its button.
- Data: the mini release re-cut (r-0516d5fc9a3f; fold words). The trout fixture release / vectors are frozen
  (refreshed only by `scripts/trout-refresh.sh` from a release) and still say "Release": they move with the next
  release, not here. **A new staging data release is needed** for 2 (fold case) and 3 (the tag); the app alone draws
  1 and the column today.
- Tests: pytest `test_trout_down.py` (row_tag words, model forms incl. tag-without-line, J tag read-back), trout
  words/check/table goldens for the lowered fold (all trout pytest files: 4 failures that fail identically on the base
  commit in a lane worktree — they need the full local bundle); vitest core `trout.test.ts`, ui-native `TroutCard`,
  `KeepTab` (no row tags, one end slot), SEO `troutRow`; `pnpm -C app check:fast` green; `mini_release --check`.
- Pictures (Glenn Creek, Nov 19; BEFORE: base app on r-379e72fc9524; AFTER: lane app on a SCRATCH copy of
  r-379e72fc9524 whose trout tables carry what this build writes — tag + lowered fold, patched for the picture only):
  scratchpad (session 1db4ea05) `t907-shots/t907-contact.png`, singles `{before,after}-{390-light,390-dark,1440-light}.png`.
