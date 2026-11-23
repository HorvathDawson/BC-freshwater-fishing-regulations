# T991 — Sources display audit: make how sources are shown consistent across the whole app

Status: PARTIAL — left (Claude): T1031 (the owner's items 3–5), T1036 (the every-source sheet's list). Round 3 built, 3428dcb83 + #63 review fixes 3e3a1e2ae (lane L21, 2026-11-22/23), from the owner's answer to round 2 (2026-11-22 ~23:55,
recorded verbatim below by lane L17, PR #57): ONE SOURCE ROW AT THE FOOT OF EACH TAB. Regulations: one "B.C. Synopsis
p.N ↗" link per synopsis page the DRAWN tabs cite (never one fixed page, never a sheet's rules; the water's own row among them,
marked "p.N (this water) ↗"), each opening the book with
every highlight its rules hold on that page, then "B.C. in-season changes ↗" / "DFO notices ↗" only where the page shows them,
then "All sources ›" (the T990 sheet). Conditions: "Environment Canada ↗ BC River Forecast Centre ↗ Open-Meteo ↗" as the
Sources-and-notes fold's head (the verbatim credits stay inside). Map: none (the map's own attribution). Names: core
`SOURCE_AUTHORITIES` (`tabSources.ts`). Pictures `T991-shots/round3-*` (regs foot before/after, conditions after, light + dark).
T1031 holds the rest (one inline source format, a breakdown per species, one species sheet with Identification | Sources
tabs), each bigger than a lane's day.
Owner 2026-11-21: next release, not 0.8.5.

Source: owner 2026-11-21, verbatim: "Add another ticket for a sources display audit (for next release we want to make
sources much more consistent across the board) not for this release".

## Work
- Inventory every place a source is shown: the regs pane's per-section "Sources" buttons and the T990 all-sources
  button, rule-book page links ("Rule book p.N ↗"), in-season notices (Province), DFO salmon notices, T981 local
  authority citations, the Conditions tab (gauges, forecasts, weather), the public website pages, More → Sources and
  licences, the first-run notice.
- For each: its words, its look (button, link, chip, row), where it sits, what a tap opens, and whether it names the
  authority the same way.
- Propose ONE pattern (words, look, placement, tap target) with before/after pictures; owner picks in a dialog round.
- Build the pick in one place in core + one component; every surface uses it.

## Inventory (2026-11-22, read off the live web app canifishthis.ca at 390 px, light + dark, and the code)

Pictures: `handoff/PLAN/app-integration/T991-shots/` (`before-*-light|dark.png`). The web pane is the same component tree as the
phone's, so the regs rows hold for both; the Android emulator was not run (see "Not checked").

| # | Surface | Words (as drawn) | Look | Placement | Tap opens | How it names the authority | file:line | shot |
|---|---|---|---|---|---|---|---|---|
| 1 | Gear / Methods / Keep section source line | "Province · Region 2 ›" then "Rule book p.8 ↗" "Rule book p.21 ↗" | accent bold meta text, no box; levels join with " · " | under each section's last row | words: the Sources sheet of those rules; each page: the PDF at that page | by LEVEL ("Province", "Region 2", "This water's", the downstream water), not by authority | core/regs/gear.ts:98,124 (`sourceLine`, `mergedSource`), whoWord gear.ts:89; ui-native/regs/SourceLine.tsx:20 | before-gear-source, before-methods-source |
| 2 | Section Sources sheet (`srcs:`) | title "Sources"; rows = rule title, quote, "Rule book p.N ↗" | sheet, one row per rule | opened by #1 | each row: that rule's own sheet; page: PDF | none per row; "Province" only in the line above | core/regs/sheets/sources.ts:37-50; words.ts:229 | - |
| 3 | Page-foot "Sources ›" (T990) | "Sources ›" (aria "All sources for this page") | accent bold meta, 44 tall, left | last item of the Regulations pane, above "Report a problem" | every-source sheet: groups "Fishing regulations synopsis", "Province in-season changes", "DFO salmon notices", "Local authority" | its group heads name the authority; the synopsis group is "Fishing regulations synopsis" while its rows say "Rule book p.N" | ui-native/regs/RegsScreen.tsx:279-284; core/regs/words.ts:433-439; sheets/allsources.ts:31-57 | before-foot, before-allsources-sheet |
| 4 | "Rule book p.N ↗" links (inline on lines, group heads, tails, banners, sheets) | "Rule book p.N ↗"; bare "p.N ↗" after a "Rule book" label; "This water in the rule book p.22 ↗" at the page foot | accent link, arrow ↗ | on the rule's own line | the synopsis PDF at the page (with a highlight box) | "Rule book" | core/regs/line.ts:49 (the one spelling), words.ts:22-28; ui-native/regs/RuleBookLink.tsx | before-gear-source |
| 5 | Authority chips on a card head | "RULE BOOK", "B.C.", "DFO", "DFO + RULE BOOK" | small boxed chip, not tappable | beside the species/fish heading | nothing | "B.C." for the Province here, "Province" in #1, "Rule book" for the same document | core/glance.ts:30, salmon.ts:569-570 (`badgeBook/badgeBoth`) | before-foot (top of pane), before-lagoon |
| 6 | Salmon card origin line + fold | "Rule book p.9 · Skeena River" under a closure; "Region 2 · Chilliwack/Vedder River…" per row; fold "SOURCE (DFO)" ("Sources (rule book and DFO)" with a book closure) holding "DFO'S ROWS BEHIND THESE ANSWERS"; card line "Fisheries and Oceans Canada · Region 2 …"; "DFO page dated Nov 18, 2026"; "Source: <credit>. Check DFO's page before you fish." | plain text, bold accent text (not a button); fold toggle | card foot | the fold opens inline; NOTHING links to DFO's page | "DFO", "Fisheries and Oceans Canada", "Region 2" place words | core/salmon.ts:562-570, 599, 604, 632, 1210-1215; ui-native/regs/SalmonCard.tsx:196-216 | before-salmon |
| 7 | In-season banner on a water | "In-season change (dates): <text>" + "Where: …" + "Shown as posted; the answers on this page do not include it yet." + "source ›" | grey card, black left bar, accent bold "source ›", 44 tall | top of the pane | the Province's in-season changes page (web) | "source" - the authority is never named; arrow is "›" though it leaves the app | core/inseason.ts:359-363, 405-413; ui-native/regs/InSeasonBanner.tsx:66-70 | before-inseason |
| 8 | Bell: in-season inbox | same words, "source ›" | accent bold meta | per row | the Province's page | as #7 | ui-native/InSeasonInbox.tsx:90-93 | - |
| 9 | Bell: DFO waters not on the map | "DFO Region 2 <name> · <water>" + the rule text; title "New DFO waters" | bordered row, no link | in the notice sheet | nothing (no source link) | "DFO Region N" | core/salmonFeed.ts:637-643; ui-native/InSeasonNotice.tsx:135-146 | - |
| 10 | Local authority (T981, Lost Lagoon) | chip "SOURCE" then "StanleyParkVan.com ↗" (the cited site's own words; the bylaw is not named) | chip + accent link | under the closure line | the cited web page | the SITE'S name, not the authority (Park Board) | core/regs/book.ts:22-29 (`facts.cited {link,url,quote}`); sheets/allsources.ts:41 | before-lagoon |
| 11 | Conditions tab: gauges, forecast, weather | block "SOURCES AND NOTES" (fold) -> "SOURCES": "Basemap © OpenStreetMap contributors · © Protomaps", "Hydrometric data: Environment and Climate Change Canada", the Province's verbatim forecast notice (with a bare URL), "Stocking: Province of British Columbia, Fisheries Inventory Data Queries", "Weather from Open-Meteo.com (CC BY 4.0)" | plain faint meta text, no links, a fold at the foot | foot of the Conditions tab | nothing | full publisher names, each in its own grammar ("X:" / "X from Y" / "Forecast data provided by…") | core/menu/sources.ts:105-121 (`MAP_CREDITS`); ui-native/Credits.tsx; ConditionsPanel.tsx:396-403 | before-conditions, before-conditions-credits |
| 12 | Public website: water pages | same pane as #1-#10 (prerendered) + footer "Not a legal document. Check the official BC Freshwater Fishing Regulations Synopsis before you fish." | footer text | page foot | - | "official BC Freshwater Fishing Regulations Synopsis" | tools/seo/html.ts:20,146; site.ts:223 | before-lagoon (web) |
| 13 | Public website: gauge page | "Readings: Water Survey of Canada real-time data (Environment and Climate Change Canada), provisional. Forecast data provided by the BC River Forecast Centre…" ; "CLEVER (dashed): issued …"; weather "Weather from Open-Meteo.com (CC BY 4.0), fetched …" | plain text under the chart | between chart and "Waters this gauge serves" | nothing (URL printed bare in the notice) | "Water Survey of Canada" + ECCC; differs from #11 ("Hydrometric data: ECCC") | tools/seo/gauges.ts:231,336; gaugeClient.ts:265 | before-gaugepage |
| 14 | Public website: About / Privacy | "Where the rules come from": "…the Province of British Columbia's Freshwater Fishing Regulations Synopsis. River gauges come from Environment and Climate Change Canada and the Water Survey of Canada; salmon notices from Fisheries and Oceans Canada; stocking records from the Freshwater Fisheries Society of BC." + "Official synopsis (PDF)" link | paragraph | About page | the synopsis PDF (only link) | a third wording of the stocking source: "Freshwater Fisheries Society of BC" here vs "Province of British Columbia, Fisheries Inventory Data Queries" in #11 | tools/seo/html.ts:142-146 | before-about |
| 15 | More tab: Data list | "Synopsis · Valid to …", "In-season changes", "DFO salmon", "Freshwater Atlas", …; "Province's in-season changes ↗" | rows with ages | More tab | the Province's page | short names | core/menu/words.ts:122-130; more.ts | before-more |
| 16 | More -> Sources and licences | per dataset: name, publisher + one sentence ("Province of British Columbia. The printed regulations this app reads."), licence, the required wording, "Open the publisher's page" | page of cards | More | licence + publisher links | full publisher names (`PUBLISHERS`); one place that IS single-sourced | core/menu/words.ts:20-33,47,122; menu/licences.ts:147-190; ui-native/menu/Licences.tsx | before-sources-page |
| 17 | First-run notice | "The official sources decide: The synopsis, the Province's in-season changes and DFO's notices are the authority."; link "Sources and licences" | body text + link button | first launch | More -> Sources and licences | "the Province's", "DFO's", "The synopsis" | core/menu/legal.ts:20-31; ui-native/FirstRunNotice.tsx:44-61 | before-firstrun |
| 18 | Regs pane foot disclaimer | "Not legal advice — the B.C. synopsis and DFO notices are the authority." | meta text | pane foot | - | "the B.C. synopsis", "DFO notices" | core (legal words), RegsScreen.tsx footer | before-foot |

Counts: 18 surfaces (rows) over 9 code families. Distinct names for the Province's rule book alone: "Rule book", "RULE BOOK" (chip),
"Fishing regulations synopsis", "Synopsis", "the synopsis", "the B.C. synopsis", "official BC Freshwater Fishing Regulations Synopsis",
"2025-2027 BC Freshwater Fishing Regulations Synopsis" = 8. For the Province as authority: "Province", "B.C.", "Province of British Columbia",
"the Province's", "the Province" (in-season) = 5. DFO: "DFO", "DFO notices", "DFO salmon", "DFO salmon notices", "Fisheries and Oceans Canada" = 5.
Gauge authority: "Environment and Climate Change Canada", "Water Survey of Canada", "Hydrometric data" = 3. Weather: "Weather from Open-Meteo.com (CC BY 4.0)" vs
"Weather data by Open-Meteo.com" (licences.ts:163) = 2. The "go" mark is "›" on #1/#3/#7/#8 (the first opens a sheet, the last two LEAVE the app) and "↗" on #4/#10/#15.
Tap target: 44 px on #3 and #7; #1's is the text's height widened to a 44 hit box; #5 chips and #11/#13 are not tappable at all.

What does not agree (the case for the audit):
1. The same document is five different names; "source ›" (#7) never says whose source.
2. "›" and "↗" mean the same thing in two places (leave the app) and different things in others (open a sheet).
3. A source is a link in #1/#4/#7/#10, a chip in #5, a fold in #6, plain text in #11/#13, and absent in #9 (DFO has no link anywhere).
4. #10 names the site (StanleyParkVan.com), not the Park Board that made the rule.
5. Credits text is composed in 6 places (words.ts, inseason.ts, salmon.ts, menu/sources.ts, menu/licences.ts, tools/seo/html.ts + gauges.ts): AGENTS 33 says one place.

## Proposed pattern (owner picks A or B; nothing built)

One table in core (`SOURCE_AUTHORITIES`: id -> short name, full name, what it is) and ONE `Source` row component. Every surface composes its
source words from that table and nowhere else (guard test: no string literal of an authority name outside the table).

Names, fixed: "B.C. Synopsis" (the rule book; page as "p.8"), "B.C. in-season changes", "DFO" (salmon notices; region in the link text only), the local
authority by ITS name from the data ("Vancouver Park Board"), "Environment Canada" (gauges), "BC River Forecast Centre", "Open-Meteo". Full names (and
licences) only on More -> Sources and licences, which stays the one page of full names.
Marks: "↗" ALWAYS means "leaves the app to a document/page"; "›" ALWAYS means "opens a sheet in the app". Never the other way round.

OPTION A (recommended): a "Source" line. A quiet label "Source" then accent links, 44 tall, the LAST line of the card/section it supports:
"Source  B.C. Synopsis p.8 ↗  p.21 ↗" / "Source  B.C. in-season changes ↗" / "Source  DFO, Region 2 notice ↗" / "Source  Vancouver Park Board bylaw ↗" /
"Source  Environment Canada ↗  BC River Forecast Centre ↗". Tap opens the document itself (PDF at the page, the notice page, the bylaw page).
The per-section "who says so" levels (Province / Region 2) move into the section's Sources sheet, which is opened from the page-foot
"All sources for this page ›" (T990, kept, full-width row instead of a small link). The Conditions credits (#11) keep their legally exact
verbatim text in the fold but add the same Source line above the fold. Pictures: `after-A-light.png`, `after-A-dark.png`.

OPTION B: an authority chip. The existing boxed chip (today's "RULE BOOK", "B.C.", "DFO") becomes THE source: tappable (44 hit box), named from the
table, "B.C. Synopsis p.8 ↗", on the card's head line instead of a line at the foot. Smaller, but the chip competes with the status chip beside
it, wraps badly at 2x text, and a section with three sources (Gear) needs three chips on the head. Pictures: `after-B-light.png`, `after-B-dark.png`.

The after pictures are static mock-ups (HTML, `after-A.html`, `after-B.html` rendered at 390 px), not the app; the before pictures are the live app.

## Not checked
Android emulator (the web app is the same component tree; native-only hit boxes and 2x text unmeasured); iOS; the in-season inbox (#8) and the DFO
bell rows (#9) were read from code, not photographed; whether the verbatim forecast notice may carry a
link beside it (Province wording is legally exact; the Source line is proposed ABOVE it, not inside).

## Acceptance
An inventory table in this ticket; the owner's pick recorded verbatim; every surface on the one pattern; a guard test
that no surface composes source words outside the one place.

Depends: T990 (DONE). Size: M. Owner: pick the pattern.

## Owner answer 2026-11-22 23:20 (dialog with pictures: today, A, B)
Verbatim: "One source row in bottom. But should have different ones for each tab too. Like condition sources don't need
to be on regulations. Please think about it more we also still have inline sources?"
Reading: option A's single Source row at the bottom, but PER TAB — each tab (Regulations, Conditions, Map, …) lists
only the sources of what that tab shows; and decide what happens to the inline per-rule source lines (keep, fold into
the row, or keep only where a rule cites a page). Next: a refined proposal with pictures (per-tab rows + the inline
question answered with a recommendation), then the owner picks again.

## Refined proposal, round 2 (2026-11-22, lane L17) — one Source row at the bottom of EACH tab, only that tab's sources
Pictures `T991-shots/round2-<tab>-before|after-light|dark.png` and `round2-inline-before|after-*.png`: the live app (web 390 px, Skeena River) with the row put in
by editing the page. MOCK-UPS (the picture says so in its red strip), not a build. "Before" is the live app.
- Regulations tab, last row above "Report a problem": `Source  B.C. Synopsis p.8 ↗  B.C. in-season changes ↗  DFO notices ↗  <local authority>` (only the authorities
  this water's answers actually use; the row replaces today's "Sources ›" link, whose sheet stays as the full list behind the row's own "All sources ›").
- Conditions tab, replacing the fold's header line: `Source  Environment Canada ↗  BC River Forecast Centre ↗  Open-Meteo ↗` (the gauges, forecasts and weather
  the page shows; no synopsis, no DFO). The fold below keeps its legally exact verbatim text.
- Map tab: `Source  © OpenStreetMap ↗  Protomaps ↗  Esri imagery ↗` (the map's own credits, Esri only while satellite is drawn), above "Report a problem".
- Never on a tab: another tab's authorities (Conditions credits are not shown on Regulations, as the owner says).
INLINE SOURCES (the owner's question): keep the per-rule "Rule book p.N ↗" where a rule cites a page — it is a proof link beside the rule it proves, not a source
list — and its words stay as T991's pattern names them (one spelling, one table). Remove the per-section line's LEVEL words ("Province · Region 6 · This water's ›",
`round2-inline-*`): they say who made the rule, which is the tab row's job; the level detail moves into the Sources sheet behind "All sources ›". Salmon's origin
line ("Rule book p.9 · Skeena River") and the in-season banner's "source ›" become "B.C. in-season changes ↗" from the same table. A card's authority chip
(RULE BOOK / B.C. / DFO) stays a label, not a link.
Recommendation: this per-tab row (option A of the first round, one row per tab) + inline page links kept + the section level words folded into the sheet.
Question for the owner: take it? (Not built.)
