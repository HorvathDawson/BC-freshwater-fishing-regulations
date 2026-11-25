# T1031 — Sources round 3, the rest: one inline source format, a breakdown per species, one species sheet with tabs

Status: IN PROGRESS — trout per-row pages: A built 285449f89 (lane L3), then owner 2026-11-24 "Go on to B": B built 1ba8cddbd (lane L15, app only, section at the end); item 3 built, 26839d85d + 5fdd46510 + 315d8b3bb (lane L22, 2026-11-23, on T991's #63): ONE SOURCE LINE, core `sourceLineOf`
(`tabSources.ts`) drawn by ui-native `SourceRow`: "Source  B.C. Synopsis p.8 ↗  p.49 ↗  <web> ↗  Who says so ›" ("↗" leaves the
app, "›" opens a sheet; last line of the block it supports). On it: the Gear / Ways to fish / Boats / Reference lines (level words
"Province · Region 6 · This water's" fold into the section's Sources sheet behind "Who says so ›"), the trout table (its pages,
then "Who says so ›" = `src:trout`, was "Sources ›"), a sheet's source block ("Source ›" → "Who says so ›"), a closure on the
closed banner (the boxed SOURCE chip + bare links; Lost Lagoon's "StanleyParkVan.com ↗"), the salmon card's book closure (was the
string "Rule book p.9 · …", now its row + "B.C. Synopsis p.9 ↗" linked through the page's PDF map) and DFO line (was link-coloured
text "Fisheries and Oceans Canada · Region 6 Skeena" that opened nothing, now "DFO notices, Region 6 Skeena ↗" to that region's
page), the in-season banner and inbox ("source ›" → "B.C. in-season changes ↗"), the every-source sheet's group heads. Kept: a
rule's own page link inside its sentence (owner round 2; renamed, below), card chips (labels). Names: `core/src/sourceNames.ts`; guard
`app/tools/source-names.test.ts`. Not changed: what is cited.
Pictures `T1031-shots/inline-{before,after}-{gear,methods,salmon,trout}-{light,dark}.png` (web 390 px, Skeena Class II part,
staging data). Items 1–2 (the owner's items 5 and 4), lane L23, 11d0e1915 + bb3836936 (PR #71): one species sheet per fish line, tabs
"Identification | Sources" (each tab its own key: `tf:<row>` / `tf:<row>|id`, `other:oN` / `other:oN|id`, a drawn card's or a
salmon species' `sp:<code>|src` / `sp:<code>`; the picture opens Identification, the rest of the line Sources), and on Sources
"Where each rule comes from": the line's deciding rules by authority, each its role and its own highlighted pages; a salmon
species: today's answer, DFO's places answering it and the book's closure. Pictures `T1031-shots/species-*.png`.
OWNER ANSWERS 2026-11-23 ~10:50, verbatim: inline look "Yes, as built (Recommended)"; the book's name "'B.C. Synopsis' everywhere
(Recommended)" — so a rule's in-sentence link reads "B.C. Synopsis p.N ↗" (was "Rule book p.N ↗") and so does the salmon rows'
place label ("B.C. Synopsis p.9 · Skeena River"), the chips, the download list and every sentence naming the book; one constant,
`sourceNames.ts` `RULE_BOOK` = `SOURCE_AUTHORITIES.synopsis.name`; the guard refuses "rule book" in any app string, any case.
#69 review fixes folded in: the trout fish sheet's hatchery link is a `link` block (one Source line per sheet, last; pinned on
the fixture pages with the cited pages per block against the base branch, conformance `sourceLine.fixture.test.ts`); the trout
line and the salmon book closures reach the foot row (`drawnRegsOf`'s drawn lines); no silent default (trout sources required,
the sheet title required, no unlinked salmon closure).
#69 re-review (47f921746): the trout table's line is "Source  Who says so ›" with no page of its own — a Source line cites what
its block draws, and the table draws none (a row's `src` lists every rule behind it without saying which it shows; the app
never cuts it), so citing its sheet's pages listed p.6 on the Vedder (the steelhead quota and record duty the table never says).
The pipeline's own words follow the rename through `pipeline/common/source_names.py` (held equal to the app's name,
`test_source_names.py` refuses "rule book" in what deliver writes): the outside-area note ("its closure (B.C. Synopsis p.9) does
not apply here") and the salmon book verdicts ("Closed to all fishing by the B.C. Synopsis (p.N): …") — NEEDS THE 0.9.0 DATA
RELEASE (they are written into the bundle and the salmon file).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1031 | Sources round 3, the rest: one inline source format, a breakdown per species, one species sheet with Identification / Sources tabs | IN PROGRESS | T991 | items 3–5 owner 11-22; item 3 lane L22 (26839d85d, 5fdd46510, 315d8b3bb); items 1–2 lane L23 (11d0e1915, bb3836936); row pages A lane L3 (285449f89), B lane L15 (1ba8cddbd, owner 11-24) |

Source: owner 2026-11-22 ~23:55 (the T991 round-2 answer, recorded verbatim in T991): "We also should clean up our inline
sources. Right now the way they display is inconsistent.... We still want to have the break down source like the sheet in
trout too. Each specieis have them as well? Also right now we have identification and sources open from clicking different
parts of the a specieis line in table. Maybe we could have that be the same sheet with different tabs?" Split out of T991 by
lane L21 (2026-11-22): T991 built the per-tab Source rows (its items 1–2); these three are each bigger than a lane's day.

Pictures of today (web, Skeena River Class II part, 390 px): `T991-shots/round3-inline-now-gear.png` (the Gear section's
"Province · Region 6 · This water's ›" + "Rule book p.N ↗" line), `round3-species-now-identification.png` (the species
picture's `sp:` sheet), `round3-species-now-sources.png` (the species row's `other:` sheet: answer, "How this was decided",
"See every regulation line ›").

## Work
1. **One species sheet, two tabs (owner item 5; do first).** Today a species row of the table opens its answer sheet
   (`other:oN`, a card row's `src:rN` behind "See every regulation line ›"), and its picture / "Kokanee ›" opens the
   identification sheet (`sp:<code>`); a trout row's picture opens `tf:<row>` (both in one scroll). Make it ONE sheet with
   two tabs, "Identification | Sources": a press on the picture opens it on Identification, a press on the row or its
   "See every regulation line ›" on Sources. Needs: a tabbed `SheetVM` (core: `tabs: [{ label, blocks }]`, the start tab in
   the key or beside it), a tab strip in `SheetBody` (role=tab, `aria-selected`, AGENTS 34), deep links (`sheet=`) for both
   tabs, `parseSheetKey` + `appKeyOf`/`goldenKeyOf` and the golden sheet keys re-mapped. Size M–L.
2. **A source breakdown per species (owner item 4).** The Sources tab of 1 is that species' breakdown, drawn as the page's
   every-source sheet is (`src:all`, T990: grouped by authority, one row per rule, each its pages), from the row's own
   deciding rules (`decided.rowSources`, the WINS roles) — the trout rows already have theirs (`tf:` "Sources" /
   "How this was decided"). Size M.
3. **One inline source format (owner item 3).** Inventory: T991's table (18 surfaces, 9 code families). Put every inline
   source through one core composer + one component: a rule's "Rule book p.N ↗" (kept, owner round 2), the Gear / Methods
   section line (its LEVEL words "Province · Region 6 · This water's ›" fold into its Sources sheet, round-2 proposal),
   the in-season banner's bare "source ›" (→ "B.C. in-season changes ↗"), the salmon card's origin line (a STRING today,
   `salmon.ts` "Rule book p.9 · Skeena River", not a link), the DFO fold, the trout "Sources ›", the Lost Lagoon chip
   (names the site, not the Park Board). Names from `@app/core` `SOURCE_AUTHORITIES` (T991, `tabSources.ts`); a guard
   test that no surface types an authority name outside it. The golden regs oracles pin these strings
   (`app/fixtures/golden/taps.json`, `plain()` parity): changing a word is a golden re-cut, and the owner picks the
   inline look from before/after pictures first. Size L.

## Acceptance
Shots before/after (light + dark, web 390 px) of the species sheet's two tabs and of every inline source surface; the
owner's pick recorded; tests pinning each composed string whole (AGENTS 33), mutation-proved; the guard test of 3.

Depends: T991 (the per-tab Source rows, `SOURCE_AUTHORITIES`). Size: L (three parts; 1 then 2 then 3). Owner: yes (the
inline look in 3, the tab words in 1). Default: build 1 and 2 with the words above; 3 waits for the pick.

## Owner answers 2026-11-23 ~10:50 (items 4-5, verbatim picks)
- Which part of a fish line opens Identification: "Picture only (Recommended)" — as built: the picture opens Identification,
  the rest of the line (its name included) opens Sources.
- The sheet's words: "Keep these words (Recommended)" — as built: "Identification | Sources", "Where each rule comes from".
- The book's name: "'B.C. Synopsis' everywhere (Recommended)" — item 3 (lane L22, #69) sets it once in
  `core/src/sourceNames.ts`; the breakdown's page links are `bookText` runs, so they read it from there with no word of their own.

## Review of #71 (2026-11-23), fixed in bb3836936
A salmon card row's `sp:<code>|src` opens its page line's Sources tab where the page answers that fish; an Other fish row
with no picture still ends with its deciding rules; the trout / salmon row's two presses each hold their own 44 px; a closed
salmon species names its closure's source. Left for when #69 lands: the salmon Sources rows' place titles ("Rule book p.9 ·
…", salmon.ts `placeFromBook`) to #69's source format and the book's name from `sourceNames.ts`.

## Trout per-row source pages (0.9.1 lane L3, 2026-11-24, branch `feat/T1031-trout-row-pages`)
Asked (owner 2026-11-22, on the Regulations page's Source row): "the source there should be maybe one for each page
referenced in the card? Also make sure highlighting is still there for each? ... We still want to have the break down
source like the sheet in trout too." Left after 0.9.0: each trout/char ROW to name the page(s) that decided THAT row.

**What exists (measured on the 54 shared vectors, 356 rows, trout fixture release).**
- Python already says per row which rules decided it: `TRow.src` (contract Q-D6: the deciding rule, size, origin,
  possession, the duty, the date line; uncut, 2 to 9 rules) and `FishSheet.src` (that list cut to 4 by Python,
  `trout/sheets.py` `A_SOURCES_CUT_TO_FOUR`). Each rule's pages and highlight boxes are the store's (`RuleFacts.book` /
  `book_box`, written once by `display/decided.add_sources`). Every rule ref in the vectors has a page (0 without).
- On the row's sheet (`tf:<row>`, Sources tab) pages reach the reader in two places: "How this was decided" (the rail:
  every rule of `TRow.src` with its page, where the stored roles cover the list) and "Where each rule comes from" (the
  breakdown: the FOUR of `FishSheet.src`, each "B.C. Synopsis p.N ↗" with that rule's highlight).
- **The gap:** (1) the sheet's Source line, its last block, was "Source  All sources ›" with no page at all (every other
  sheet's line names its pages); (2) the cut to 4 drops rules whose words the sheet says, and on 19 of 356 rows (5%)
  with them a page the breakdown never shows: Lois Lake's Dolly Varden / lake trout (the lake's own row, p.24: "Hatchery
  only: wild ones go back"; Khartoum, Windsor, Dodd, Horseshoe, Nanton and Ireland lose a page past Region 2's p.21 the
  same way), Nation Lakes' lake trout (p.61
  possession), Shuswap (p.7, the record-on-your-licence duty). Where no rail is drawn that page is nowhere on the sheet.

**Built (app only, no frame change).** The row sheet's Source line cites the pages of every rule of `TRow.src`
(Python's whole list, mapped one index to that rule's own `ruleBook` runs, in Python's order; the one composer
`lineSourcesOf` → `sourceLineOf` draws each page once, ascending, every citing rule's highlight boxes on it), then
"All sources ›": "Source  B.C. Synopsis p.21 ↗  p.24 ↗  All sources ›" on Lois Lake's Dolly Varden. Core
`sheets/trout.ts` `rowSourceLine`. The breakdown (four) and every word are unchanged; nothing in the table itself.
**No data side:** the frame already carries the rule list and the store the pages; a page list written into the trout
frame would type the pages twice (AGENTS 56) and need a data release. So this needs only an app build (any data, 0.9.0 on).
Tests: `trout.release.test.ts` "(T1031 per-row pages)" over every vector row (the line's pages = the pages of `TRow.src`,
each with exactly its rules' boxes; the whole Lois line pinned; >10 rows carry a page the breakdown lacks), and
`sourceLine.fixture.test.ts` `ROW_PAGES` (per fixture page, each row sheet's cited pages). Mutation-proved: a line built
from `FishSheet.src` fails both; dropping the boxes fails the first.
Pictures (web 390 px, light + dark, trout fixture release, harness `app/tools/rowpages-shots.test.ts`):
`T1031-shots/rowpages-{before,after}-{lois-dv,vedder-rb}-{light,dark}.png`; option B mock-up
`rowpages-option-b-whole-list-lois-dv-{light,dark}.png`.
**Owner question:** A (built) the Source line names every page behind the row, the breakdown keeps its four; or B the
breakdown also lists every rule of the row (Lois Lake's p.24 rule then has its own entry; a Python change to
`A_SOURCES_CUT_TO_FOUR`, Q-D6, and a data release).

## Option B built (0.9.1 lane L15, 2026-11-24, branch `feat/T1031-trout-row-breakdown-all-rules`)
**Owner answer, 2026-11-24 morning, verbatim: "Go on to B".** It overrides the cut to 4 for this list: contract Q-D6 ("A
sheet's `src` is that list cut to 4", from build spec 3.4 "this fish's rules first, up to 4", a design-spec line, not a
RULINGS.md ruling) and spec 3.4's Sources row now say so (`sheets.py` is left untouched: any deliver-code edit moves the committed mini release's code digest).
**Built (app only, 1ba8cddbd).** The row sheet's "Where each rule comes from" draws EVERY rule of `TRow.src` (Python's whole
list, deciding rule first, the old four as its head, then the rest in Python's order; by authority as before) instead of
`FishSheet.src`. No word changes; the Source line (A) is unchanged and now names exactly the breakdown's pages.
**Why not the Python change the brief planned:** an installed 0.9.0 holds every trout frame to `sheet.src.length <= 4`
(`core/src/regs/trout.ts`, on `release/0.9.0` too) and on a breach reports and draws NO table. Uncutting `FishSheet.src`
would have blanked the trout table on 632 of 1,442 tables (1,087 of 1,799 answer keys with a table; 1,570 of 9,128 rows
cite more than 4 rules) of the live release r-0aec88ed3edb for every 0.9.0 phone. `TRow.src` already carries the whole
list in Python's order (`FishSheet.src` == its first 4 on every row of that release, checked), so the frame is unchanged:
trout section bytes unchanged, no app_reads / pack format change, `answers_diff` lists nothing. Needs only the 0.9.1 app
build (any data from 0.9.0 on), no data release.
Tests: `trout.release.test.ts` (every vector row: the breakdown's keys == `TRow.src`, the frame's list <= 4 and its head;
the Source line's pages all in the breakdown; "(T1031 option B)" pins Lois Lake DV (6 rules) and the Vedder ST (9) whole),
`SheetHost.test.tsx` (a row with more than 4 rules draws them all, in order). Fail on main / mutation-proved: drawing
`FishSheet.src` fails 3 release tests + the SheetHost test; sorting the list fails 2. 118 of 356 vector rows grow.
Pictures (web 390 px, light + dark, trout fixture release, harness `app/tools/rowpages-shots.test.ts`, `ROWPAGES_H=3000`,
backdrop cropped): `T1031-shots/rowpages-b-built-{before,after}-{lois-dv,vedder-st,vedder-rb}-{light,dark}.png` (Vedder
rainbow: 4 rules, unchanged). Lois DV equals the option-B mock-up, continued to the sixth rule and the Source line.
