# T1031 — Sources round 3, the rest: one inline source format, a breakdown per species, one species sheet with tabs

Status: IN PROGRESS — item 3 built, 26839d85d + 5fdd46510 + 315d8b3bb (lane L22, 2026-11-23, on T991's #63): ONE SOURCE LINE, core `sourceLineOf`
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
staging data). Items 1–2: lane L23.
OWNER ANSWERS 2026-11-23 ~10:50, verbatim: inline look "Yes, as built (Recommended)"; the book's name "'B.C. Synopsis' everywhere
(Recommended)" — so a rule's in-sentence link reads "B.C. Synopsis p.N ↗" (was "Rule book p.N ↗") and so does the salmon rows'
place label ("B.C. Synopsis p.9 · Skeena River"), the chips, the download list and every sentence naming the book; one constant,
`sourceNames.ts` `RULE_BOOK` = `SOURCE_AUTHORITIES.synopsis.name`; the guard refuses "rule book" in any app string, any case.
#69 review fixes folded in: the trout fish sheet's hatchery link is a `link` block (one Source line per sheet, last; pinned on
the fixture pages with the cited pages per block against the base branch, conformance `sourceLine.fixture.test.ts`); the trout
line and the salmon book closures reach the foot row (`drawnRegsOf`'s drawn lines); no silent default (trout sources required,
the sheet title required, no unlinked salmon closure).

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
