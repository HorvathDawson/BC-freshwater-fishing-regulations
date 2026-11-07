# T482 — Salmon section: region rules into the species rows, daylight one line, the DFO source a closed fold

Status: IN REVIEW — merged into redesign/stream-sections 2026-11-07 (a7235f996), after r-1b4f54fb6ed9 and the 0.6.9
APKs. Ships with the next release; the owner was sent the before/after Vedder shots 11-07.

Owner 2026-11-07, Vedder River regs pane, salmon (DFO) section: "Also I thought a long time ago with salmon we got rid
of the items here and combine it into the individual salmon species? It is too much and the source of the regs should
be collapsible."

What he saw between the species rows (all Closed) and "Trout and char": the closure words ("There is no fishing for
salmon in waters of Region 2 flowing into Areas 28 and 29…"), "On this water (DFO)" (the adult chinook 62 cm note),
"Region rules (DFO) · Region 2 Lower Mainland" with DAYLIGHT, SIZE Coho 25 cm, SIZE Chinook/Sockeye/Pink/Chum 30 cm,
ADULT Chinook + "Full wording ›", then the open source block (agency, page date, credit, fish pictures, "Both apply").

## History — regression or never finished?

Both, in two layers.

- **Decided:** T309 (10-31) made the region rules readable rows; T318 made the species table; T343 (owner 10-31,
  data side DONE 11-03/04) said "fold or drop every header note": min size / adult / jack → the species' sizes, daily /
  possession / annual → the bucket line, daylight → a `hours` line (round 2), facility / measuring / R1 bait ban → drop.
  T393 (11-05) put each species' sizes on its own row, closed rows too. T343 items 6–7 (images; **DFO source info one
  collapsible line, collapsed by default**) and (c) "draw the card's `hours` line, then drop the daylight words" went to
  T367 (UI chat).
- **Never finished (UI):** T367 closed 11-04 "owner call; leftovers → T377" without doing either salmon item — none of
  its rounds touches the salmon card, and T377's list does not carry them. Item 6 (spawning images) was done by T394;
  item 7 (the source fold) and (c) (the `hours` line) never were. `core/salmon.ts` still says "hours stays words too
  until the card draws `hours` — T367".
- **Regression (data):** the folds never reached Regions 1, 2 and 6. `regs/dfo_salmon/feed.py` stamped TODAY's header
  format on whatever notes the scraped rules file held. The R1/R2/R6 rules files were scraped 11-03 20:54 with the
  pre-T358 note shape (`cm`, no `lengths`, no daylight `hours`); the typed feeds rebuilt 11-04 00:04 were stamped
  `dfo-header/2`, so `deliver.salmon.load_headers` trusted them instead of re-reading the cached page. In the current
  release (r-67b456b5e64b) R1/R2/R6 ship `sizes: []`, no `hours`, and their min_size / adult / daylight notes carry no
  `fold` — so every one stays loose words in "Region rules". R3, R5B, R8 (no typed notes → re-read) fold correctly.
  Also T343 round 2 added `hours` to the daylight note without bumping the header format.
- Plus one presenter gap: a page where every species is closed (`allClosed`, the Vedder today) computed no per-species
  sizes, so even correct data would leave the size notes in the region block.

## Work (done in the worktree)

1. **Pipeline (data):** `header.FORMAT` → `dfo-header/3` (daylight `hours`; no /2 feed is trusted, every typed feed's
   header is re-read from its own cached page at the next build). `feed.build` carries the scraped file's OWN
   `header_format` (none → none), never today's; the writers that parse the header (`live.type_page`,
   `locations` rules file) stamp it. Test: `test_dfo_live.py::test_a_feed_carries_its_scraped_header_format_never_todays`.
   Cascade stays in Python: re-parsed, R2 gives per-species sizes (chinook 30 cm, adult over 62 cm on the
   Chilliwack/Vedder and Capilano; 50 cm elsewhere; Fraser/Chehalis 62 cm Sep 1–Dec 31; coho 25 cm; sockeye/pink/chum
   30 cm) and the hours line.
2. **Core `salmonCard`:** sizes on each species' row when every species is closed too (the "Salmon — closed" row
   carries `sizesBy`); a daylight note folded to `hours` is hidden where its region ships `hours` (kept as words where it
   does not — nothing lost). `withSalmonFeed` carries the feed region's `hours` with its notes (was the release's).
   Word: `SALMON_WORDS.sourceFold` "Source (DFO)".
3. **ui-native `SalmonCard`:** the `hours` line under the species rows; the source block (agency · region, page date,
   checked, credit, fish-picture credit, "Both apply") is `SourceFold`, closed by default (Methods Reference / T479
   examples pattern: kicker + chevron, `aria-expanded`); the walkthrough's `regs:both` rings its summary.
4. **"Trout and char / Hide" row:** not a misplaced toggle. It is the next row of the "Species and limits" table
   (the Trout and char group's own fold), showing "Hide" because the owner had opened it too; its card follows it.
   No change.

Kept as is: the closure words under the table ("There is no fishing for salmon in waters of Region 2 flowing into
Areas 28 and 29 …") — DFO's reason the rows are closed, said once (T318); rewording needs the owner.

## Proof
- Tests: core `salmon.test.ts` (T482 all-closed sizes; hours fold + kept-as-words), `salmonFeed.test.ts` (feed hours),
  ui-native `SalmonCard.test.tsx` (fold closed then opened; hours line), pipeline `test_dfo_live.py`.
- Scratch rebuild from the T468 reaches (format bump applied) → shots, Vedder + Squamish (coho open), before/after,
  390 px, in the session scratchpad `salmonfold/`.
- Then: owner look at the shots; ships with the next release (its build re-reads R1/R2/R6 headers).

## State 2026-11-07 (worktree commit d10043ccd on `t482-salmon-fold`, rebased on 3dd72dfcb)
- Scratch rebuild `data/generated/scratch/t482` (T468 reaches; the shared tree's code + the /3 bump via a sitecustomize,
  so the subprocess stages saw it): r-f56b90ad2540, vintage clean. R1/R2/R6 now ship sizes (3 / 6 / 4 entries), every
  min_size / adult / jack note `fold: sizes`, R2 `hours` + daylight `fold: hours`; R6 "other" (eulachon) stays words.
  The cached pages for R1/R2/R6 carried the typed feeds' dates, so the re-read raised nothing.
- Shots (390 px, cropped to the salmon section): Vedder 1,187 → 605 px tall (no region block, no "On this water",
  sizes on each row, source folded); Squamish (coho open) 1,236 → 841 px (hours line, sizes on rows). Session
  scratchpad `salmonfold/` {before,after}-{vedder,squamish}.png, after-vedder-source-open.png.
- Tests: pipeline DFO suites 492 pass (test_dfo_live/header/quota/salmon); app regs + salmon vitest pass; `pnpm check`
  steps all pass except `tickets` (T483's file has no Status line in its first 6 lines — not this lane's).
