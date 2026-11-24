# T1036 — The every-source sheet ("All sources ›") lists rules the screen does not draw, and misses the drawn gear on salmon-only days

Status: PARTIAL — d29a19c5a, ea1155dee (lane L5); second group a65823345 (lane L17, owner 2026-11-24); left (Claude): put the head question to the owner (lane L17 report, `T1036-shots/other-group-*`) and apply his pick

Source: the independent review of PR #63 (T991 round 3, 2026-11-23), probing the live release r-741815c6090d over 8 waters ×
parts × 9 days. The Source row was fixed in #63 to cite exactly what the drawn tabs cite (`drawnRegsOf`,
`WaterScreen.sources.test.tsx`). The sheet behind its "All sources ›" (`src:all`, T990,
`core/regs/sheets/allsources.ts` `pageSourceRules`) has the same two faults and was left as it was:
1. It reads `page.gear` / `page.methods`. On a day only salmon may be fished, Gear and Methods draw the day's gear "if
   fished" (`gearTabsFor`, salmon.ts), so the sheet misses those rules (p.8 / p.10 on gnis:2308 Jan–Jun, gnis:27804 part 1,
   gnis:10061 Apr–Sep; p.70 on gnis:10061) and lists the closed day's rules instead.
2. It adds every card row's deciding rules (`env.card.rows`), even on a closed day, and `closeForFinfish` (finfish.ts) keeps
   the original sheet. So rules nothing on the screen cites are listed (p.68 on gnis:10061 Aug–Sep, p.13 on gnis:27804
   part 1, p.64 on gnis:14619 part 2, the p.6 / p.7 general pages on open days; p.7 is the licences page the owner rejected
   on Sweltzer, T916).

## Work
Build `src:all` from the drawn view models, as the Source row is built: a screen-level wrap of `page.sheet` (as
`withInSeason` already does for the in-season changes), fed `drawnRegsOf(page, tabs, licence)`. Each listed rule must be one
a drawn line cites (its key on a drawn source line, a drawn card row, the drawn trout table). Decide with the owner whether a
species sheet's own rules (a row's "See every regulation line ›") belong in the page's sheet: today they do.

## Acceptance
The probe of `WaterScreen.sources.test.tsx` extended to the sheet: the sheet's pages equal the drawn tabs' pages, on the
fixture cases and the built salmon-only day; mutation-proved.

Depends: T991. Size: M. Owner: the question in Work. Default: list only what the drawn tabs cite.

## Done (d29a19c5a, lane L5, 2026-11-24)
`src:all` is the screen's: core `withDrawnSources(page, drawn, rules, salmonBook)` (`regs/sheets/allsources.ts`), fed
`drawnRegsOf(page, tabs, licence, lines)` in WaterScreen's `shown` (the Source row's own input) and the salmon card's book
closures. A rule is listed where a drawn line cites it: a drawn section Source line's `keys` (never a per-row gear line's,
`srcs:gl|`), a drawn line naming its `rule` beside its "Rule book p.N ↗" (or a local authority's page), the drawn trout
table's `src` (its "Who says so ›" opens them), a salmon book closure's rule (one the page lacks: said by its card line's
words and pages, and reported, `AllSourcesRuleMissing`). The page's own `src:all` (unwrapped) is the page drawn alone
(`gearTabsFor(page, null, null, null)`). `pageSourceRules` (page.gear / methods / every card row) is gone; `drawnRegsOf`
moved beside the sheet (`tabSources.ts` re-exports it); `SalmonBookVM` carries `rule`.

Proof: `WaterScreen.sources.test.tsx` / `WaterScreen.salmonOnly.test.tsx` open "All sources ›" (`regs/testing/allSources.ts`):
the sheet's pages = the drawn tabs' pages ∪ the drawn trout table's, on the 9 fixture cases, the Vedder's table and the
salmon-only Chilliwack day (p.8 / p.21 "if fished" in it); `allsources.test.ts` pins the closed day, the salmon-only day, a
ruled line vs a sheet-only row, the trout table, the book closures. All fail on main; 7 mutations each killed.

Owner question (default built): a species sheet's own rules (a row's "See every regulation line ›") and a row that only
opens a sheet (Possession: its rule shows no page on the tab) are NOT listed. Pictures, Qualicum River part 1 on Jun 1
(closed; before: "Possession: twice the daily quota" p.13, which nothing on the screen cites): `T1036-shots/`
before-/after-qualicum-jun1-{light,dark}.png. The trout table's rules ARE listed (its line opens them): on the Vedder
that brings p.6 (the province's steelhead quota), which the table never says.

#96 review (ea1155dee): the Source row now names DFO from the sheet's own walk (`drawnSourceRules`; no row changes today:
neither r-741815c6090d nor r-0aec88ed3edb holds a DFO-source rule); a cited rule the index lacks is reported
(`AllSourcesRuleMissing`) and gets no row; the probe compares rule keys (`rule-row:<key>`) and pins the salmon closures
argument (Region 4's spring closure on the Chilliwack, cited by nothing else).

## Owner's answer (2026-11-24, verbatim)
Asked whether the rules shown only inside a species sheet, and Possession, should also be listed on the page's "All
sources" sheet (built: no): **"Add back but maybe have them separate?"**

## Built (lane L17, 0.9.1, 2026-11-24, a65823345): a second group, under its own head — a proposal for the owner's pick
**What is in it, exactly.** Below the drawn lines' rules (group 1, unchanged), under one head (`ALL_SOURCES.OTHER`, built:
"Also in the fish and possession sheets"), grouped by source as group 1: every rule a sheet ONE TAP from the drawn
screen cites that no drawn line cites. Core `allsources.ts`:
- the sheets (`reachableSheets`): every sheet key a drawn line opens (its `open`) — a fish row's sheet (`other:` /
  `sp:` / `tf:`), Possession's (`pos:pos`), a rule's or a term's sheet a drawn line opens — and each one's tabs
  (Identification | Sources); never the every-source sheet itself, never a per-row gear line's (`srcs:gl|…`, not
  drawn since T900);
- what a sheet cites (`sheetCitations`): each row of its Sources list (never an "Overruled" card: it does not apply
  here), each rule whose sheet a line opens beside that rule's page ("Who says so ›" + "B.C. Synopsis p.N", as
  Possession's sheet does), a rule's own sheet its rule; one tap only: a link inside a sheet ("See every regulation
  line ›", "Fishing more than one water today? ›") is not followed;
- minus group 1: no rule is in both. A card the screen does not draw (a closed day's) opens no sheet, so nothing of it
  is listed. A cited rule the index lacks is reported (`AllSourcesRuleMissing`, by "sheet") and gets no row (W46).
Built from the same drawn view models and the page's own sheets, when the sheet is opened (lazily, once).
`withInSeason` keeps the second group after the first.

**Qualicum River part 1, Jun 1** (live release r-0aec88ed3edb): the page draws no Possession row and no fish row (closed
all year to all fishing: the Keep tab is the two closed strips and the "Closed all year" card), so no sheet is one tap
away and the second group is empty: Possession p.13 is NOT back there. It came (before #96) from the undrawn card's
deciding rules, which this definition leaves out. If the owner wants p.13 on that page, the definition must widen to
the undrawn card (a different question). **p.7**: listed only where a reachable sheet really cites it — on the Peace
River (Aug 1) the White sturgeon sheet's "No fishing for white sturgeon" (p.7) is in group 2; the T916 licences row
that came from the card rows stays out.

**Proof.** `allsources.test.ts` (the second group's rows and order by source, the overruled card, the tab, the two-tap
link, the page-less rule link, the undrawn gear line and undrawn sheet, the reported missing rule, in-season keeping
it, a page whose drawn lines cite nothing); the UI probe (`WaterScreen.sources` / `salmonOnly`,
`regs/testing/allSources.ts` `expectedOtherKeys`, read by these words, not by core's walk): group 1 = the drawn rules,
group 2 = the reachable sheets' rules minus group 1, one head (none where the group is empty: gnis:8634 part 3), no
rule in both, on the 9 fixture cases, the Vedder and the salmon-only days. 12 fail on main; 9 mutations killed.

**Pictures** (web 390 px, light + dark, the live release): `../T1036-shots/other-group-peace-aug1-before-after-*.png`
(before: the sheet ends; after: the second group below), `other-group-peace-aug1-headings-*.png` (A built "Also in the
fish and possession sheets" | B "Other rules for this water"), `other-group-qualicum-jun1-before-after-*.png` (no
change: nothing one tap away).

**Owner question**: the second group's head: A "Also in the fish and possession sheets" (built: says where the rules
are) or B "Other rules for this water" (shorter); or not separate after all.
