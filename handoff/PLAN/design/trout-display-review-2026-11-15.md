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
