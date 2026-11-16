# Trout display review: notes

Date: 2026-11-15. Release read: `data/release/r-f749475d0962` (what `data/release/LATEST` names). Branch `redesign/stream-sections`. Nothing in the repo was changed.

## How the trout display is built today

- The pipeline ships per answer key a `rows` section (version 33). A trout row carries `daily`, `narrow`, `items` (fish members with size `bands` `[from_cm, to_cm, number]`, `back`, `origins`), `conds` (caps, origin, groups), `real_daily`, `group` (the stored tree of tiers and kinds, headline, `not_kept`, `worked` examples) and a per-row `year`.
- `app/packages/core/src/regs/` composes every sentence: `keep.ts` (`keepOf`) builds `KeepVM`; `grouphead.ts`, `tree.ts`, `tail.ts` build `GroupVM` (head, origin, tiers, kinds, not-kept, worked examples); `plainrow.ts` builds the release-only / closed rows and the "Through the year" line; `glance.ts` builds the folded row.
- `app/packages/ui-native/src/regs/KeepTab.tsx` draws the blocks in `keep.order`; `GroupCard.tsx` draws one group (head, origin, year, tree of `Tier` / `Kind` / `Release`, `NotKept`, the folded examples, "See every regulation line"). No logic in components.
- With the glance table (on in staging), "Trout and char" is a folded row with a "Details" button and no value; opened, the full `GroupCard` is drawn in place.
- A release-only or closed trout answer is not a group card at all: it is a `PlainRow`, drawn after the species table.

## How the pages were read

- `work/dump.ts` (bundled to `work/dump.cjs` with the repo's esbuild, output in the scratchpad) opens the release with `@app/data/regs` `openRegsStore` + `loadRegsInputs` and calls core `buildRegsPage`: the app's own view model. `scan` mode built one page per answer key (2,121 keys, two dates) to find shapes; `page` mode dumped the chosen waters with their source sheets.
- `work/render.py` flattens a `KeepVM` to the lines `GroupCard` draws, in order.
- Staging (`https://staging.canifishthis.ca/?regs=<item>&ix=<part>&theme=light`) was screenshotted at 390 px, 2x, after dismissing the first-run notice and opening the trout fold. The Vedder card text on staging equals the view model line for line, so staging is on the same answers.
- The start link refuses past dates, so only Nov 15 could be screenshotted. Skeena's summer card is redrawn from the Jul 15 view model.

## The six waters

| Water | Item / part | Date | Why |
|---|---|---|---|
| Vedder River, mouth to cable car crossing | `gnis:3062` / 0 | Nov 15 | Steelhead, hatchery only, river's own looser rule (4 hatchery rainbow) outside the stream share but inside the region total |
| Little River | `gnis:33426` / 0 | Nov 15 | Shuswap Lake's rule nested inside the region's size band; yearly limits; dated lake trout release; one part of the river differs (map A) |
| Skeena River, Sedan Creek to Mill Creek | `gnis:2936` / 12 | Jul 15 | One count limit across four fish, repeated three ways; weekend rule for some anglers; winter card is a different shape |
| Kootenay Lake, Main Body | `wbk:-20` / 0 | Nov 15 | Two cards: rainbow counted apart (10 a day), bull trout 1 inside the 5 |
| Elk River, mouth to Elko Dam | `gnis:16880` / 0 | Nov 15 | Season water: release now, closed in spring, keep 1 in summer with a bait ban; today's answer is a plain row under the other fish |
| Fraser River, Region 5 boundary to Region 3 boundary | `gnis:39325` / 14 | Nov 15 | Straddles two regions (the lower stream number wins), steelhead closed outright, lake trout release |

Other shapes seen in the scan and not drawn: "Trout" card plus a separate "Char" release row (Region 1, e.g. Clayoquot River); a "Brook trout" card counted apart (Region 4 and 8 lakes); hours rules (Slocan River 12:00 to 00:00 in summer, Harrison River at night).

## Measured (staging, 390 px wide, trout card open)

| Water | Card height | Text lines | Numbers | Fish named |
|---|---|---|---|---|
| Vedder | 1,157 px | 31 | 26 | 8 |
| Little River | 1,337 px | 34 | 26 | 8 |
| Skeena Nov 15 | 690 px | 20 | 13 | 8 |
| Skeena Jul 15 (view model) | n/a | 28 | 30 | 8 |
| Kootenay Lake | 714 + 433 px | 16 + 10 | 15 | 7 |
| Fraser | 1,182 px | 31 | 23 | 8 |
| Elk | about 430 px | 12 | 4 dates | 6 |

On the Vedder card the word "hatchery" appears 6 times in 4 places. There are 14 links on that card.

## Diagnosis in one paragraph

The card mirrors the structure of the regulation (region total, stream share, size band, kinds, exception) instead of the angler's question. Numbers nest and do not add up to the headline. The same fish appears in two rows (rainbow and steelhead). Release and closed fish come last. The most generous rule on the Vedder is at the bottom under "Exception to regional quota". The display changes form by date (group card in season, plain row out of season). The folded row shows no answer.

## The four alternatives

- **A. Fish by fish**: one row per fish or book group with verdict, number, size, origin; shared limits in a strip on top; release fish in the same list. Needs a new core composer over `rows.items` (bands, origins, back) and `conds`, plus one ui-native component. No pipeline change expected.
- **B. The answer first**: one number, a short checklist of conditions, a put-back list; the rest folded. Needs new headline and checklist sentences for every rule shape; best as stored sentences in the pipeline display model, or composed in core from `real_daily`. Whole-sentence tests (AGENTS 33).
- **C. The usual rule and what is different here**: differences first, the region's rule in a standard box. Needs a "region base card for this kind of water" and a comparison against it, which the data does not ship today (it ships provenance per rule, `over_region`, and the ladder's beaten / lifted roles). Largest pipeline work.
- **D. Check my fish**: fish, length, fin clipped gives keep or release and what it counts against. Needs decision logic in core (never in a component) over `items.bands` / `conds` / `origins`, with band-edge tests. Only works beside A or B.

Pick: A with B's one line on top; a "this river's rule" tag from C on rows; D later as a button.

## Things noticed in the data (not design)

1. Kootenay Lake rainbow: the card's duty line reads "20 a year · Record each on your licence right away". The source sheet has "Up to 20 rainbow trout over 50 cm (20 in) a licence year" and "Write each rainbow trout over 50 cm (20 in) on your licence right away". The card drops "over 50 cm". Worth a ticket.
2. Fraser part 14: the trout row's stored year is keep 273 days, closed 31, keep 61 (closed Oct 1 to 31) while the water's own strip has no closed days. On Nov 15 the card shows "You can keep them until Sep 30. Then closed from Oct 1." with no reopening date. I did not trace which rule closes October.
3. The "See every regulation line" sheets for the Vedder and Little River trout groups do not list the water's own rule (4 hatchery rainbow; Shuswap Lake's 1 a day). Not traced.
4. Skeena Jul 15: the count tier "Up to 1 brown trout, cutthroat trout, rainbow trout or steelhead combined" comes from a rule dated Jul 1 to Oct 31; the card does not show the dates.

## What is illustrative on the page

- The wording inside options A to D is mine. The facts are the app's answers.
- The checker's logic in option D is hand-written per water for the mock.
- Summer numbers shown on the Elk in winter come from the Jul 15 answer of the same release.
- "One screen = 640 px" is my assumption for the fold marks.
- Fish drawings are left out of the options.

## Not verified

- I did not check the options against every rule shape in the province, only the six waters.
- The page was looked at once at phone width in light and dark (no overflow, no script errors); one layout fix after that was not looked at again.
- Staging screenshots were written to the browser tool's own folder (`.playwright-mcp/`, git-ignored), copied here, and my files there were removed.

## Files

- Page: `trout-display.html` (1.25 MB, six staging screenshots embedded)
- Template and build inputs: `work/page.tpl.html`, `shots/today-*.jpg`
- View-model dumps: `work/pages1.json`, `work/pages2.json`, `work/scan.json`
- Preview shots of the page: `shots/own/`

---

# Round 2 (2026-11-15): one refined design, A + B as a fish table

Owner: "expand and refine the a/b design idea ... super simple and clean with the fish icons in the table format".

## The design

- One top line (the most trout and char you can take home today), an optional "Clipped fin only" line, the shared limits as "Also counts" lines, then a table: one row per fish = the app's drawing (64 x 32, as the species list draws it), the name, one condition under the name, and the answer in a right-hand column.
- Fish that share one number share ONE answer box (a merged cell). Release and closed fish are rows of the same table; the box edge is solid / dashed / double and the words carry a mark, so colour is not needed.
- Fish that share a smaller limit inside a group share a row ("Bull trout or lake trout: 60 cm or longer · 1 only").
- Dates go in the answer box with both ends, the whole year where the answer changes (Elk: until Mar 31 / closed Apr 1-Jun 14 / keep 1 Jun 15-Oct 31 / release from Nov 1; Fraser: "except Oct 1-31: closed").
- Part and hours rules are note rows at the top of the same table.
- Everything else (whose rule, page, yearly limits and duties, kept-elsewhere examples) is behind a tap on the row; the page lists it per water.

## Fish drawings

- `app/apps/mobile/assets/fish/thumbs/*.webp` (240 x 120, transparent), first picture per species from `app/packages/core/src/regs/fish.generated.ts`. Drawn by `KeepBlocks.tsx` `Thumb` at 64 x 32; a fish with no picture gets the silhouette SVG there (Protected species).
- All seven trout and char have a picture: RB, ST, CT, EB studio photos (Freshwater Fisheries Society of BC), DV studio photo (Joel Sartore), GB painting (Duane Raver), LT colour plate (Denton). Not one set; all face left on clear backgrounds. CT shows the coastal fish everywhere.

## Top line grammar (work/grammar.py over work/shapes.json)

`work/rows.ts` reads the stored trout rows for one part per answer key (2,121 keys) on 11 dates (the 15th of every month but February): 23,331 answers.

| form | answers |
|---|---|
| Up to N today | 13,265 |
| (water closed that day: no trout line) | 7,339 |
| Release every one until <date> | 1,434 |
| Up to N today, only K of them <fish> (`real_daily.shared_count`) | 718 |
| Up to A <fish> and N other trout and char today (a separate keep row) | 426 |
| Up to N today, or M if they're <fish> (`headline.also`, a stored phrase) | 77 |
| Closed for trout and char until <date> | 9 |
| Up to A <fish> today | 8 |
| Nothing to keep today | 5 |
| tidal (no rows) | 11 |
| NO FORM | 39 (8 keys) |

No form: 35 answers / 7 keys where a tier is flagged looser than the region (`over_region`) but `headline.also` is empty (Kaslo River, Keen Creek, Humphrys Creek, Kitimat River ...), and Ross Lake (4 answers: three keep rows, stored headline 2 vs `real_daily.n` 4). Both worth a look by the pipeline lane.

Fields read: `group.headline.n`, `group.headline.also[].says`, `real_daily.shared_count`, `scope.apart`, `parent`, `kind`, `year`.

## Waters

Round 1's six, plus Skeena in winter, plus four new (Nov 15, staging screenshots):
- Harrison River `gnis:15333` / 0: hours rule (daylight only), hatchery only. Staging labels the stretch "Mouth -> Harrison Lake outlet"; the local release labels part 0 "Mouth -> Hwy 7 bridge ...". Trout card text identical.
- Shuswap Lake `wbk:329518145`: a lake in parts (one closed today, one release-only part, three spring closures), lake trout with its own dates.
- Clayoquot River `gnis:10431`: "Trout" card plus a separate "Char" release row.
- Premier Lake `wbk:329523945`: brook trout counted apart (20), a lake rule for rainbow inside the 5, a part that closes in spring.

## Budget (set before measuring): one screen 640 px, 12 numbers, 60 words, one condition per row

height px · words · numbers, 390 px wide, nothing opened

| Water | Today | Table | Plain A | Plain B |
|---|---|---|---|---|
| Vedder | 1157 · 209 · 25 | 653 · 101 · 18 | 696 · 157 · 20 | 660 · 142 · 15 |
| Little River | 1337 · 196 · 25 | 629 · 88 · 15 | 849 · 170 · 25 | 726 · 155 · 16 |
| Skeena summer | ? · 205 · 29 | 647 · 86 · 14 | 825 · 151 · 24 | 760 · 138 · 15 |
| Skeena winter | 690 · 103 · 12 | 557 · 53 · 8 | | |
| Kootenay Lake | 1147 · 140 · 13 | 482 · 56 · 7 | 492 · 87 · 12 | 535 · 103 · 13 |
| Elk | ~430 · 77 · 4 | 362 · 38 · 8 | 309 · 81 · 11 | 413 · 69 · 13 |
| Fraser | 1182 · 170 · 22 | 551 · 72 · 15 | 661 · 125 · 17 | 584 · 98 · 13 |
| Harrison | 1072 · 181 · 24 | 635 · 90 · 19 | | |
| Shuswap Lake | 1156 · 165 · 24 | 654 · 93 · 18 | | |
| Clayoquot | ~1260 · 183 · 16 | 599 · 70 · 9 | | |
| Premier Lake | 1233 · 170 · 20 | 562 · 68 · 10 | | |

One screen holds on 8 of 11; 12 numbers on 5 of 11; 60 words on 3 of 11 (the word budget was too tight for a named row per fish). At 200% text the base table has no sideways overflow on any of the 11 (713 to 1,573 px tall); the size-column variant overflows on 6 of 11.

## Variants (one change each)

1. answer as a mark (filled / dashed / hatched square) 2. size in its own column 3. drawings on a plain backing 4. shared limits under the table. Pick: base + 3.

## Known facts shown corrected

- Kootenay Lake rainbow: "over 50 cm: 20 a year, record each" (T763).
- Fraser: closure with both dates (T762).

## Checks

- Last edit, then: 390 px wide, light and dark, all 11 waters, 100% and 200% text, round 1 section open: 0 px sideways scroll, no script errors, no broken images. Rows open on tap. Every button in the base mock is at least 44 px tall.
- Not checked: a real device, a screen reader, or text scaling done by the phone itself (the 200% switch doubles the mock's text only).

## Files (round 2)

- `trout-display.html` (2.6 MB: ten staging screenshots and 18 fish thumbnails embedded)
- `work/page2.tpl.html`, `work/build2.py` (round 1's template is spliced in under "Earlier options")
- `work/rows.ts`, `work/shapes.json`, `work/grammar.py`, `work/coverage.json`
- `shots/today-*.jpg`, `shots/r2/` (my checks of the page)

---

# Round 3 (2026-11-15): folded row, size placement, grouping under stress, 9 more waters

Owner: likes the folded row's answer text, leans to the size as a second column, wants more complex examples with weird groupings.

## Folded row (adopted)

Text per top-line form: "N a day" (plain, only) · "N to M a day" (or) · "A <fish>, N others" (two; never "A + N") · "A <fish> a day" · "release" / "closed" with the date small under it · "none to keep" · "several limits" (no top line). The whole row is the press target; mark option 1 is a down arrow (neighbours use "›"), option 2 a plus. At 200% text the answer drops under the name; no overflow on any of 20 waters at 390 or 360 px wide.

## Size placement

Five placements drawn on every water: (e) under the name, (a) true column, (b) column that drops, (c) in the answer box, (d) tag.
Short size forms: "30 cm+" (includes 30), "up to 50 cm" (includes 50), "30–50 cm" (both ends), "over / under N cm" (excludes N, only where the app says so), "not 60–90 cm" (slot), "any size" written out. Inches behind the tap.
Rule for (b): whole table uses the column when every size is 12 characters or fewer and text is normal; else every size goes under its name. With the short forms all 20 waters pass at normal text.
Measured, 390 px: at 100% no placement overflows on any of 20. At 200%: (a) overflows on 9 of 20; (e), (b), (c), (d) on none. Average height: (e) 576, (a)/(b) 547, (c) 576, (d) 550 px.
Pick (b); runner-up (e).

## Grouping device

- One box per limit the top line names; a limit inside a limit is a small inner box ("only 1") spanning its fish; a single-fish limit is a bold line under the name; fish sorted so each box is one piece.
- Round 2's three Skeena boxes (5, 3, 1) became one box of 5 with inner limits. No more two fish on one row.
- At 200% the inner box becomes a bold line under each of its fish ("only 1 of these 2") with a rail.
- Alternatives drawn: side bracket, heading row per group. Pick: the box, sorted. In the app's fish order the Fraser's box of 2 splits into three.

## New waters (found in the scan; real)

| Water | Item / part / date | Grouping |
|---|---|---|
| Kaslo River (= Keen Creek) | `gnis:11345` / 0 / Jul 15 | 2 from streams + river's own 2 bull trout outside the 2; no top line |
| Kitimat River (= 2 more stretches, Humphrys Creek) | `gnis:3225` / 0 / Jul 15 | one box of 5 with five inner limits; stored rows split rainbow into hatchery 5 (to 50 cm) and wild 1 (30–50 cm); no top line |
| Williston Lake, Zone B | `wbk:-19` / Nov 15 | 5; bull + lake trout "only 4"; inside that 1 and 3; no top line |
| Williston Lake, Zone A | `wbk:-18` / Nov 15 | same fish, no shared char limit |
| Ross Lake | `wbk:328961736` / Jul 15 | brook 5 apart; 1 trout (40 cm+); 1 lake trout (60 cm+); bull trout release; no top line |
| Bella Coola River, spring | `gnis:11611` / 0 / Apr 15 | 2; only 1 a cutthroat or rainbow; cutthroat 25–33 cm; steelhead closed |
| Bella Coola River, summer | same / Jul 15 | 1; cutthroat now release-only |
| Tagish Lake | `wbk:328961735` / Jul 15 | 5; lake + bull trout only 3; lake trout only 2 with a 60–90 cm release slot and 1 over 90 |
| Inkaneep Creek | `blk:356370187` / Jul 15 | 4 with a stepped group size limit (2 over 30 cm, 1 over 50 cm); brook trout 20 apart (in the other-fish list today) |

Not found anywhere in the release: a keep row that mixes trout with kokanee, whitefish or burbot.

## The 8 rule sets with no top line

Kaslo River, Keen Creek (same shape); Kitimat River x3 stretches, Humphrys Creek (same shape); Williston Lake Zone B; Ross Lake. For Kitimat / Humphrys / Williston B the own rule is "part of the 5", so "Up to 5 today" would be true. Kaslo / Keen need a tenth form ("Up to 2 today, plus 2 bull trout"). Ross Lake: stored headline says 2, `real_daily.n` says 4.

## Possible defect to check

Kitimat River (and Humphrys Creek): the stored item `origins` for rainbow are hatchery n=5 (0–50 cm) and wild n=1 (30–50 cm); the card composed by core shows one line, "Up to 5 rainbow trout, 50 cm (20 in) or less". Not traced.

## Budget now (height px · words · numbers; 390 px; pick (b))

vedder 651·88·16 · little 604·70·13 · skeena 641·74·12 · skeena winter 554·51·8 · kootenay 461·52·7 · elk 416·42·8 · fraser 544·66·15 · harrison 633·77·17 · shuswap 629·76·16 · clayoquot 596·68·9 · premier 551·64·10 · kaslo 515·58·12 · kitimat 688·85·23 · williston B 436·43·10 · williston A 467·43·9 · ross 484·62·13 · bella spring 562·67·15 · bella summer 464·41·6 · tagish 502·62·11 · inkaneep 533·73·13.
One screen holds on 17 of 20, 12 numbers on 11, 60 words on 7.

## Checks after the last edit

390 px wide, light and dark, 20 waters, 100% and 200% text, both folded sections open: 0 px page sideways scroll, no overflow inside the main mock or the folded rows, every button at least 44 px tall, no broken images, no script errors. At 360 px wide and 200% text, 4 of 20 tables are 6–10 px too wide inside the frame (said on the page). Phone mocks run the full screen width below 430 px so they are true to size.

## Files (round 3)

`trout-display.html` (2.66 MB) · `work/page3.tpl.html`, `work/build3.py` · `work/pages3.json`, `work/todaylines.json`, `work/meas3.json` · `shots/r3/`
