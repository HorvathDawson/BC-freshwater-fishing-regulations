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

Pictures of the before (web, Skeena River Class II part, 390 px): `T991-shots/round3-inline-now-gear.png`,
`round3-species-now-identification.png`, `round3-species-now-sources.png`.

Trout per-row pages, asked (owner 2026-11-22, on the Regulations page's Source row): "the source there should be maybe
one for each page referenced in the card? Also make sure highlighting is still there for each? ... We still want to
have the break down source like the sheet in trout too."

## Done (details in Status)
1. **One species sheet, two tabs** (owner item 5; lane L23, 11d0e1915 + bb3836936, PR #71): a tabbed `SheetVM`, the
   tab strip in `SheetBody`, keys `tf:<row>` / `tf:<row>|id`, `other:oN` / `other:oN|id`, `sp:<code>|src` / `sp:<code>`.
   #71 review (bb3836936): a salmon card row's `sp:<code>|src` opens its page line's Sources tab; each of a row's two
   presses holds its own 44 px; a closed salmon species names its closure's source.
2. **A source breakdown per species** (owner item 4): the Sources tab's "Where each rule comes from", from the row's
   own deciding rules (`decided.rowSources`).
3. **One inline source format** (owner item 3; lane L22, 26839d85d + 5fdd46510 + 315d8b3bb, #69; re-review
   47f921746): core `sourceLineOf` + ui-native `SourceRow`; names `core/src/sourceNames.ts`, guard
   `app/tools/source-names.test.ts`; pipeline twin `pipeline/common/source_names.py` (`test_source_names.py`).
- Trout per-row pages: A (lane L3, 285449f89, branch `feat/T1031-trout-row-pages`): the row sheet's Source line cites
  the pages of every rule of `TRow.src` (core `sheets/trout.ts` `rowSourceLine`, via `lineSourcesOf` → `sourceLineOf`),
  e.g. "Source  B.C. Synopsis p.21 ↗  p.24 ↗  All sources ›" on Lois Lake's Dolly Varden. The gap it closed: the cut
  to 4 (`trout/sheets.py` `A_SOURCES_CUT_TO_FOUR`) hid a page on 19 of 356 vector rows (Lois Lake p.24, Nation Lakes
  p.61, Shuswap p.7 …).
- B (lane L15, 1ba8cddbd, branch `feat/T1031-trout-row-breakdown-all-rules`; owner 2026-11-24 morning, verbatim: "Go on
  to B"): "Where each rule comes from" draws EVERY rule of `TRow.src` (deciding rule first, the old four as its head)
  instead of `FishSheet.src`; contract Q-D6 and spec 3.4's Sources row say so. App only: an installed 0.9.0 holds every
  trout frame to `sheet.src.length <= 4` (`core/src/regs/trout.ts`) and on a breach draws NO table, so uncutting
  `FishSheet.src` in Python would have blanked 632 of 1,442 trout tables of r-0aec88ed3edb on every 0.9.0 phone;
  `sheets.py` is left untouched (any deliver-code edit moves the committed mini release's code digest). Tests:
  `trout.release.test.ts` "(T1031 per-row pages)" / "(T1031 option B)", `SheetHost.test.tsx`,
  `sourceLine.fixture.test.ts` `ROW_PAGES`; mutation-proved. Pictures `T1031-shots/rowpages-*.png` (harness
  `app/tools/rowpages-shots.test.ts`).

## Work (left)
The pipeline's renamed words ("B.C. Synopsis" in the outside-area note and the salmon book verdicts) are written into
the bundle and the salmon file: they ship with the 0.9.0 data release.

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
