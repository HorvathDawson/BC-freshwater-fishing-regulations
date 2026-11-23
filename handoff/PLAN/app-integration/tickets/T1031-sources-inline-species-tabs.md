# T1031 — Sources round 3, the rest: one inline source format, a breakdown per species, one species sheet with tabs

Status: PARTIAL — left (Claude): item 3 (one inline source format, lane L22). Items 4-5 built, 11d0e1915 (lane L23, 2026-11-23): one species sheet per fish line, tabs
"Identification | Sources" (each tab its own key: `tf:<row>` / `tf:<row>|id`, `other:oN` / `other:oN|id`, a drawn card's
or a salmon species' `sp:<code>|src` / `sp:<code>`; the picture opens Identification, the rest of the line Sources), and on
Sources "Where each rule comes from": the line's deciding rules by authority (`SOURCE_AUTHORITIES`), each its role and its
own highlighted pages; a salmon species: today's answer, DFO's places answering it (rows as printed) and the book's closure.
Pictures `T1031-shots/species-{trout,salmon}-{sources,identification}-{light,dark}.png` (Vedder River). The tab words and
the head "Where each rule comes from" are kept (owner 2026-11-23, below).

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
