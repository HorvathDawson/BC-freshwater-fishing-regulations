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

---

# Round 4 (2026-11-16): one design; sizes unified with cm and inches; group size limits; Dolly Varden

Owner: "take the stuff you like best then we need to unify over x size and x+ also we want both in and cm. And we want to make the stuff like 1 over 50cm on group easier to tell. Also dolly are not in list?"

## Adopted as THE design (no more options side by side)

Top line · one row per fish (drawing on a plain backing, name as the book prints it, size under the name in cm and inches) · one box per number, sorted by box · small inner box for a limit inside a box · a size limit on a whole box written in the box · release and closed fish in the same table · hatchery and wild as two rows only where limits differ · folded row with its answer and a down arrow · a proposed top line for the 8 rule sets that had none (Kaslo/Keen "Up to 2 today, plus 2 Dolly Varden/bull trout"; Kitimat x3, Humphrys, Williston B "Up to 5 today"; Ross Lake "Up to 5 brook trout and 2 other trout and char today"), marked "proposed" on the page. Rounds 1 to 3 are folded under "How we got here".

## Size grammar

The app already has one standard: `pipeline/deliver/answers/common.py` `band_words` (ROUND19 5, RULINGS P37): "X or less" / "X or longer" include X; "under X" / "over X" exclude X; "A–B" holds both ends; "between A and B" (a slot) holds neither. Inches: `inches()` = nearest whole inch, half up; `cm_in()` = "50 cm (20 in)"; ranges "25–33 cm (10–13 in)". Round 3's "30 cm+", "up to 50 cm", "not 60–90 cm" were my own short forms and are gone.

Every size text the app's Keep view model says (work/sizes.ts over 2,121 rule sets x 4 dates = 8,484 pages; work/sizes.json): 27 distinct texts in 11 patterns.

| case | app wording | occurrences |
|---|---|---|
| no limit | "any size" (+ "any length", 23, 4 rule sets) | 5,727 |
| floor, mark included | "N cm (N in) or longer" (3,531 as "must be …; release any shorter", 192 bare) | 3,723 |
| ceiling, mark included | "N cm (N in) or less" (217 as "must be …; release any longer", 468 bare) | 685 |
| band, both ends | "N–N cm (N–N in)" | 868 |
| slot that goes back | "release any between N cm and N cm" | 16 (4 rule sets) |
| steelhead definition | "rainbow trout over 50 cm (20 in)" | 846 |
| "under N" as a keep size | never | 0 |

Finding: a fish's own keep size is always the inclusive kind. "Over" appears only in big-fish limits and the steelhead definition.

Big-fish / group limits (tier lines and "also" lines): "Only 1 of all your trout and char may be over 50 cm" 2,464 (+789 with "2 if one is a hatchery steelhead"); subset forms 23; "Up to N over N cm" 997; "Up to N cutthroat or rainbow trout over N cm" 450; stepped "Up to N over N cm, and up to N over N cm" 88 (27 rule sets); cross-conditional forms ("…if you haven't already kept a …") on Salmo River, Premier Lake, Williston B, Kamloops Lake; "counting any lake trout over N cm" on Hummingbird Creek.

## Both units on the row: placement re-measured (22 waters, 390 px)

Under the name: no overflow at 100% or 200%; narrowest name cell 113 px; average height 571 px. Middle column: no overflow at 100% but the narrowest name cell is 50 px; overflows at 200% on 6 of 22; average height 564 px. Pick: under the name.

## Group size limits

Placement by scope: whole box -> in the answer box under the number; some fish of a box -> small inner box; one fish -> bold line under the name; several boxes -> a framed line across the top of the table (Vedder, Ross Lake). "Also counts" now holds only the region's daily total.
Three ways drawn for a whole-box limit: in the box (picked), a band across the top of the box, a ladder of dots. At 200% text the box limit becomes the band (automatic), which removed the three overflows the box text caused.
A row hides "any size" when its box carries a size limit.

## Dolly Varden: facts

- One species entry, code `DV`, named "Dolly Varden/bull trout" in every sentence the app composes: 1,574 kind lines across 568 rule sets, never "bull trout" alone. AGENTS rule 45: `BT` is refused; the book (p.80) counts a bull trout in the Dolly Varden quota.
- No water has different rules for the two (one code; checked all 2,121 rule sets).
- Regional wordings of the same fish in the quotes: R3 "bull trout (Dolly Varden)", R2 "char (bull trout, Dolly Varden, or lake trout)", R5/R6 "Dolly Varden/bull trout". One stored sentence says "Dolly Varden or bull trout".
- `fish.generated.ts` `DV` has two pictures: `bull-trout.webp` (first: the one every row shows, caption "Bull trout") and `dolly-varden.webp` (caption "Dolly Varden"). So today's picture caption drops "Dolly Varden" even though the name line has it.
- "Bull trout" alone on the round 3 rows was my shortening. Fixed on every row.

## New waters (round 4)

- Kamloops Lake `wbk:329563838`, Jan 15: 5; only 1 over 50 cm; Dolly Varden/bull trout (60 cm or longer, only 1) and rainbow (50 cm or less, only 2) share "only 3"; lake trout release.
- Hummingbird Creek `gnis:21657` / 0, Jul 15: 4 from streams; only 1 over 50 cm (whole box); only 2 over 30 cm among brown, cutthroat, rainbow and lake trout (not brook).

## Budget now (height px · words · numbers; 390 px; both units)

vedder 734·129·27 · little 672·86·18 · skeena 655·100·22 · skeena winter 531·59·11 · kootenay 519·63·8 · elk 416·43·8 · fraser 538·76·19 · harrison 654·114·29 · shuswap 672·90·21 · clayoquot 572·77·12 · premier 554·71·11 · kaslo 588·88·15 · kitimat 755·130·34 · williston B 499·69·16 · williston A 464·55·15 · ross 602·113·22 · bella spring 571·99·28 · bella summer 483·68·16 · tagish 497·71·15 · inkaneep 517·73·14 · kamloops 499·57·14 · hummingbird 572·70·18.
One screen holds on 16 of 22, 60 words on 4, 12 numbers on 5. Against round 3 the Vedder grew 83 px, 41 words, 11 numbers.

## Checks after the last edit

390 px and 360 px wide, light and dark, 22 waters, 100% and 200% text, all three history folds open: 0 px page sideways scroll; no script errors; no broken images; every button (rows, folded rows, chips, controls, water list) at least 44 px tall; taps open. At 390 px no table or folded row overflows its frame at either text size. At 360 px and 200% text, 5 of 22 tables (Vedder, Skeena summer, Kaslo, Bella Coola x2) are 6 to 11 px too wide inside the frame (said on the page).
Not checked: a real phone, a screen reader, the phone's own text scaling (the 200% switch doubles the mock's text only), widths under 360 px, and the not-picked variants at 200% (they scroll inside their frame). The "what you should read" lines are my reading of the app's answer, not a second source.

## Temp

Node runs used TMPDIR inside the scratch folder (work/tmp), removed. Browser screenshots went to the tool's `.playwright-mcp/` and were moved here; none left there. Preview server stopped by PID.

## Files (round 4)

`trout-display.html` (2.69 MB) · `work/page4.tpl.html`, `work/build4.py` · `work/sizes.ts`, `work/sizes.json` · `work/pages4.json`, `work/meas4.json` · `shots/r4/`

---

# Round 5 (2026-11-16): one table from three fresh takes. Mockups only.

Owner on round 4: the fish picture moves when a row opens; the opened part should hold species identification; the big "Clipped fin only" note should go, with a "how to tell hatchery" link at the top instead; still a little clunky. Three fresh designers each refined round 4 on five waters (`fresh-calm/`, `fresh-reader/`, `fresh-open/`). Round 5 is one table on all 22.

## The design

- Top: kicker line with "How to tell a hatchery fish ›" (only on waters with a hatchery rule; opens the app's existing "Hatchery or wild?" sheet); headline; plain small lines with a grey label ("Hatchery fish only:", "Also counts:", "Across both columns:", "Through the year:").
- Table: one row per fish (picture tile top-aligned, name, grey word "hatchery"/"wild" where only some rows need it, size in the app's wording with both units). A pale filled column on the right is one limit: "Keep N in all" at its top. A white box in that column is a tighter limit level with its fish: "only 3" (one fish), "only 1 of these 2" (shared). A whole-column size limit is one sentence along the top: "Of the 5, only 1 over 50 cm (20 in)". Put-back and closed fish: no fill, faded picture, "↩ Release" / "✕ Closed".
- A fish that sits in a shared white box and has its own limit keeps its own limit as a bold line under its name (Kamloops Lake, Williston Lake B, Tagish Lake, Premier Lake, Hummingbird Creek).
- At 200% text the right-hand column folds away: each group gets a heading line and every "only" becomes a line under its fish (rail on the left for a shared one).
- Type: four sizes (1.6, 1.53 number, 1, .87 em), three weights.

## Calls on the three disagreements

(a) Total: side column (calm), not a box around the fish with "Keep" on every row (reader). (b) "only N": white boxes level with the fish, worded "only N of these M"; no bracket (cannot nest or overlap). (c) A fish opens as a bottom sheet (fresh-open's pick); in place kept as the one switch on the page.

## Opening a fish

- Sheet: fixed bottom panel, Done / tap outside / Escape; the table is not touched.
- In place: the opened block is inside the name cell, pulled left under the picture, with `width:0; min-width:calc(100% + picture)` so it adds nothing to the table's column widths; every cell is top-aligned. Measured: 292 opens (every fish, 22 waters, 100% and 200% text) at 390 and 360 px, light and dark: 0 px change in picture top, picture left, name top or right-hand cell top, on open and again after close. (The first build failed this by up to 228 px because the opened block widened the name column; fixed.)
- Inside, in order: where the fish sits in the table (sheet only); licence rules; the app's pictures (both for Dolly Varden/bull trout); "What to look for" = the real `FISH_TIPS` lines from `app/packages/core/src/regs/fish.generated.ts`; the clipped-fin drawing and a link to "Hatchery or wild?" on hatchery rows; a dashed PLACEHOLDER for "often mistaken for"; fine print.
- Hatchery sheet: the app's own title, captions and paragraph (from the `hatch` sheet in `ui-native/src/regs/fixtures/vedder_1010.json`) and its two drawings (`assets/fish/adipose-*.webp`), plus one line about this water.

## New wording, needs the data side's agreement

1. "only N of these M" (shared tighter limit). 2. "Of the N, only K over X cm (Y in)" (the app says "Only 1 of all your trout and char may be over…"). 3. "uses the 1 over 50 cm (20 in)" on a fish whose smallest keep size is above the mark, or a steelhead under a plain 1-over-50 limit (derived, not stored). 4. "Hatchery fish only: wild trout and char go back" as a plain top line. 5. A put-back row "Steelhead wild" on Skeena summer and Kitimat (the app says it inside the steelhead line). 6. "Through the year:" lines for Elk and Fraser (whole-year form; relates to T762). 7. "only Oct 15–Jan 31" under a dated put-back fish. Proposed top lines for the 8 rule sets stay as in round 4.

## Data touches for round 5 (facts unchanged)

Wild steelhead row added to the release group of Skeena summer and Kitimat; Skeena winter's brook trout "only 1 over 50 cm" moved from the row to the column sentence; `uses` flags: Vedder DV, LT; Little River DV, EB; Harrison DV, LT; Shuswap DV, EB; Fraser DV; Ross LT; Kamloops DV; Hummingbird LT; Skeena summer ST.

## What was taken and left

- calm: took the filled column, hairlines, one right-hand column for every "only", the top sentence, plain top lines, quiet release rows, four sizes. Left "hatchery" after every name on hatchery-only waters, and dropping ↩ ✕.
- reader: took the diagnosis (loudest number was the loosest; "only 1" needs a noun; join the 60 cm fish to the 50 cm limit), the wild steelhead row, the year line, dates with a word. Left the box around the fish with "Keep" per row, and the bracket.
- open: took the sheet, the chevron on the picture tile, the order inside, the existing marks and hatchery sheet, the top link. Left "clipped fin only" in each box, and the measured-pinning lane.

## Budget (height px · words · numbers; 390 px; nothing opened)

vedder 711·151·32 · little 708·108·25 · skeena 744·120·26 · skeena winter 574·61·11 · kootenay 522·61·6 · elk 464·52·9 · fraser 641·97·22 · harrison 725·141·36 · shuswap 720·112·28 · clayoquot 611·85·12 · premier 574·72·10 · kaslo 597·92·14 · kitimat 750·144·35 · williston B 554·74·17 · williston A 490·57·15 · ross 630·121·22 · bella spring 634·104·29 · bella summer 513·67·15 · tagish 540·76·16 · inkaneep 585·78·13 · kamloops 553·71·18 · hummingbird 619·86·22.
Taller than round 4 on 20 of 22, about 40 px on average; one screen holds on 15 of 22 (16 in round 4). Calmer, not smaller. (Numbers above are from before the last cosmetic edit, which removed one blank line in single-fish boxes: Vedder, Kootenay, Premier, Kaslo, Ross, Inkaneep are a few px shorter than listed.)

## Embedding the fresh mocks

Each `mock.html` is embedded as a string and shown in an `<iframe srcdoc>` created when its fold opens; a JPEG picture of each sits under it in case the host blocks frames. In the string, `</`, `<!--` and `<script` are escaped so the page's own script element is not cut. Works in local preview; NOT verified on claude.ai (the page contract does not say whether srcdoc frames are allowed).

## Checks after the last edit

390 and 360 px, light and dark, 22 waters, 100% and 200% text, every fold open: 0 px page sideways scroll; no overflow in the table or the folded row; every press target (rows, top link, Done, chips, switches, water list) at least 44 px; 0 of 292 in-place opens move anything; the sheet opens with marks and placeholder, leaves the table's height unchanged, and closes; the three fresh frames load; no broken images; no script errors. Page 3.9 MB.
Not checked: a real phone, a screen reader, the phone's own text scaling, widths under 360 px, whether claude.ai shows the three framed mocks (pictures are there as a fallback), the sheet's focus trap (focus moves to the title and back; Tab is not held inside), the fresh mocks' own claims beyond opening them, and round 4/3/2 fold mocks at 200% (they scroll inside their frames).

## Temp

Preview server stopped by PID; `work/preview.html` and `work/tmp` removed; browser screenshots moved out of `.playwright-mcp/` (none of mine left). No browser profile was created by me (the shared tool's own contexts were closed).

## Files (round 5)

`trout-display.html` (3.9 MB) · `work/page5.tpl.html`, `work/build5.py` · `work/r5res.json`, `work/meas5.json` · `shots/r5/`

---

# Round 6 (2026-11-16): less on top, the sheet, the year bar, sources. Mockups only.

Owner on round 5: reduce the text above the table; the sheet is the better option, refine it; could reuse the calendar element; likes the binning look; where do the sources go.

## Kept

The table (pale column per limit, white "only N" boxes), sizes in the app's wording with both units, "Dolly Varden/bull trout" in full, all 22 waters with their "what you should read" lines. The "in place" opening is gone from the lead and lives in the folded round 5.

## Text above the table (measured at 390 px, 22 waters)

Average: 134 px, 5 lines, 29 words -> 87 px, 2 lines, 13 words. Whole card average 609 -> 591 px; one 640 px screen holds on 16 of 22 (round 5: 15 by the same measure). Vedder 193 px / 61 words -> 73 px / 13 words, card 696 -> 632 px.
Where each cut thing lives now:
- second half of the answer line ("or 4 if…", "only 1 of them…") -> the table (the rainbow's own column; the white box "only 1 of these 4"). Vedder's column of 2 is drawn first to match the line. Kept on the proposed Kaslo line ("Up to 2 today, plus 2 Dolly Varden/bull trout") because there it is the answer.
- "Hatchery fish only: …" -> the first-line link "Hatchery fish only · how to tell ›" (hatchery-only waters); the sentence is on the hatchery sheet and in every fish sheet ("Must be a hatchery fish…").
- "Also counts: N a day across Region X" -> the "Fished elsewhere today?" sheet; on the Vedder (4 + 2 > 4) it is in the table's last line "All together".
- "Across both columns: only 1 over 50 cm…" (Vedder, Ross) -> the table's last line.
- "Through the year" -> the year bar.
- part / hours notes -> those in force on the day stay above, one line each (Shuswap x2, Little River, Harrison, Skeena winter); the rest are one line under the table.
- "Proposed line…" -> under the mock, off the card.

## The sheet

Header: picture, full name, origin word, today's answer for this fish ("Keep · only 1 · counts toward the 5", "↩ Put it back · only Oct 15–Jan 31", "✕ Closed · do not fish for it"), Done. Then "Here, today" (size; hatchery/wild; "Counts toward the N in all, together with …"; "Only K of these M: this fish and …"; "This fish: only N"; the column's size limit with "A kept one is always over 50 cm (20 in), so it is that one" where true; the all-together line), "Licence rules", "When" (year bar, only if this fish has its own stored year), "How to tell it" (app pictures, both for Dolly Varden/bull trout; `FISH_TIPS`; clipped-fin drawing and link; a link to the hatchery/wild sibling row), the look-alike placeholder, "Sources" (this fish's rules first, up to 4, and a link to all), Previous / Next with "3 of 8".
Measured on 390 x 780: 639 px visible; header + "Here, today" + licence rules are above the fold on the four hardest fish; pictures start at 271 to 345 px; Vedder steelhead's sheet is the longest (1,201 px).
Closes by Done, tap outside, Escape. Not in the mock: swipe down, swipe between fish.

## Year bar

Modelled on the app's strip (months, today mark with its date, legend, hatched closed); states keep / release / closed. Drawn under the table only where the trout answer changes while the water stays open: Elk, Kaslo, Kitimat, Fraser (4 of 22). In the sheet for fish with their own stored year: Vedder rainbow (release in May), lake trout on Shuswap / Kamloops / Fraser (release Oct 15 to Jan 31), Williston B's two char.
Limit found: the stored `year` is per table ROW, not per fish. Skeena's trout row is "closed Jan 1 to May 31, keep Jun 1 to Dec 31" and Bella Coola's is "keep all year", so the bar cannot show the Skeena's winter put-backs or the Bella Coola cutthroat leaving the box. Needs a per-fish year from the pipeline.
I did not read `YearStrip.tsx` again this round; the look is from the round 1 staging screenshots and the round 1 `.calbar` styles. Marks ✓ / ✕ are the app's; "↩ Release" in the legend is the app's release mark from its rows.

## Sources

Both: "Sources ›" under the table (one sheet: whose rules, every rule with its title, the book's quoted words, "Rule book p.N ↗"), and a "Sources" section in each fish sheet. Real lines from the app's own "every regulation line" sheets (`work/src6.json`, 2 to 12 rules per water). In-season / DFO: an empty dashed slot at the top of the sources sheet; how it would look is a proposal. Matches the Gear / Methods tabs' source line in purpose; I did not re-read `SourceLine.tsx` this round (its shape is in round 1's notes of `model.ts`: who, pages, a `srcs:` sheet).

## New wording this round (needs the data side's agreement)

"Hatchery fish only · how to tell ›" · "All together: no more than 4 a day across Region 2" · "Counts toward the N in all, together with …" · "Only K of these M: this fish and …" · "This fish: only N" · "A kept one is always over 50 cm (20 in), so it is that one" · "↩ Put it back" (sheet header) · the year bar's "✓ Keep" legend word.

## Checks after the last edit

390 and 360 px, light and dark, 22 waters, 100% and 200% text, every fold open: 0 px page sideways scroll; no overflow in the table or the folded row; every press target at least 44 px (rows, top link, links under the table, chips, switches, Done, Previous / Next); all 146 fish sheets open with "Here, today" and "Sources", none wider than the screen, the table's height unchanged while open; Next works; the sources sheet opens; no broken images; no script errors. Page 4.0 MB.
Not checked: a real phone; a screen reader; the phone's own text scaling; widths under 360 px; whether claude.ai shows the framed fresh takes (picture fallback kept); the sheet at 200% text by eye (measured for width only); swipe gestures; the focus trap inside the sheet; the dark sheet by eye this round.

## Temp

Preview server stopped by PID; preview file removed; browser screenshots moved out of `.playwright-mcp/`.

## Files (round 6)

`trout-display.html` (4.0 MB) · `work/make6.py` (derives `page6.tpl.html` from round 5's template), `work/r6.css`, `work/r6.js`, `work/build6.py` · `work/src6.json`, `work/meas6.json` · `shots/r6/`
