# T608 D — Depth maps and stocked lakes: where things stand, and a plan

Read-only audit, 2026-11-10. Nothing was built, fetched into the repo or changed. Live release read: `r-a178299f6005`.

## Summary
- **Depth charts are close.** All 2,685 B.C. chart PDFs are already on this Mac (768 MB). 2,222 of the 2,265 chart
  ids land on a lake in the live release, which gives 2,240 lakes a chart. Nothing shows them yet.
- **Stocking has nothing fetched.** The matcher is written and tested on made-up rows only. There is no roster, so
  the count of matched stocked lakes today is zero and the real number is unknown.
- **The app is ready for both and fed by neither.** The Stocked and Depth map colourings, the release slot
  (`lakes/1`) and the lake-info calls exist. All are empty and greyed.
- **Biggest gap: permission, not code.** Today the B.C. catalogue marks the stocking records and the traced
  contours "Access Only" (reproduction not permitted). GoFishBC says written consent is needed. Only the scanned
  chart PDFs are under the open licence. T218 says the contour licence is settled; the catalogue says otherwise.
- **Second gap: no frozen match file** for either. `config.yaml` has both set to `null`.
- **Recommended display:** a fourth view on a lake, called "Lake", shown only when the lake has something. It
  holds a Depth chart card now and a Stocking card later. The Regulations view stays rules only.
- **First step:** the chart matcher and its one reviewed match file (size M). About 2,122 match cleanly, about
  230 need your look. In parallel you send two permission requests (size S, only you can).
- **First thing an angler sees:** "Depth chart" on about 2,240 lakes, opened in the app from our own server.
  Three tickets: M + S + M.

## 1. State

| Item | State | Evidence | Counts |
|---|---|---|---|
| Chart list, open table (CSV) | built (fetched) | `data/source/wsa_bathymetry_maps.csv` | 2,741 rows, 2,246 lake ids |
| Chart list, map layer (WFS) | built (fetched) | `data/source/bc_bathymetry_sheets.json`, `data/fetch_data.py` `fetch_bathymetry_sheets` | 2,744 sheets, 2,265 ids, 2,596 files |
| Chart PDFs | built (fetched, not in git) | `data/source/bathymetry_pdfs/`, `fetch_bathymetry_pdfs`, `_bathy_pdf_filename` | 2,685 files, 768 MB; 5 are 88-byte "File not found" pages; 1 wanted file missing |
| Chart id to lake, audit | half | `data/generated/bathymetry/audit.json` (run by hand 2026-09-02) | 2,122 name-verified, 143 lake has no name, 12 name disagrees, 17 no lake found |
| Chart matcher | idea | `pipeline/bathymetry/README.md` ("`generate/match.py` does not exist yet") | 0 |
| Chart frozen match file | idea | `config.yaml` `curated.bathymetry.matches: null`; no `data/curated/bathymetry/` | 0 |
| The audit script itself | gone | deleted as dead code in `971f40575` (T436); both READMEs still say it exists | — |
| Traced contours (digital depth) | idea | T218; `fetch_data.py` layer `bathymetry_contours`; tiles hold no contour layer (T158) | 28 lakes, 137 shapes (T218's figures) |
| Tracing the scans ourselves | idea, measured | T219 (prototype was in a scratchpad, no repo code) | 4 of 5 simple sheets fit; about 15 min of checking per lake |
| Stocking fetch | removed | `pipeline/stocking/fetch.py` deleted in `f13f623a8` (owner 2026-10-21, no open licence) | old run: 71,072 rows, about 18 MB |
| Stocking roster | missing | `data/source/bc_stocked_waterbodies.json` does not exist | 0 |
| Stocking matcher | half | `pipeline/stocking/generate/match.py`, `identifiers.py`, `pipeline/tests/test_stocking_match.py` | tested on made-up rows only |
| Stocking frozen match file | idea | `config.yaml` `curated.stocking.matches: null` | 0 |
| Stocking id audit | half | `data/generated/stocking/identifier_audit.json` | run over the 2,741 CHART rows, not stocking rows: 2,523 verified, 151 no name, 65 no lake, 2 conflicts |
| Binding rule + release file `lakes/1` | built, not fed | `pipeline/deliver/lake_facts.py`, `pipeline/tests/test_lake_facts.py`, `app/packages/core/src/lakeFacts.ts` | no release carries one |
| Bundle tables | built, empty | `pipeline/deliver/bundle/schema.sql` (`stock_water`, `stock_code`, `chart`) | 0 rows |
| App data calls | built, empty | `app/packages/data/src/release/source.ts` (`lakeInfo` null, `stockingHistory` empty), `types.ts` `LakeInfo` | — |
| Map colourings Stocked / Depth | built, greyed | `app/packages/core/src/layers.ts`, `LayersSheet.tsx` | enable themselves when a release carries values |
| Depth contour colour | built | T158 (deep navy in all themes) | draws nothing: no contours in tiles |
| PDF viewer | built for the rule book only | `app/packages/ui-native/src/regs/PdfViewer.native.tsx` (web opens a browser tab) | takes one file today |
| Check step lines | built | `pipeline/tools/audit/check_curated.py` lines 150–161 | prints the two "not built" lines |
| Slow-feed pattern | built | T412, `app/workers/feeds/src/runtiming.ts`, `dispatch.ts` | names bathymetry as a follower |
| Tickets | all postponed 2026-11-05 | T200, T201, T218, T219 (`tickets/README.md` rows 248–267) | 4 |
| Research | done | `handoff/PLAN/bathymetry/SOURCES.md`, `decisions.md` line 120 | — |
| Memory notes | none on this topic | memory index; nearest are gauge-match-one-frozen-file, everything-added-is-a-proposal | 0 |

Stale words found (not fixed, this audit changes nothing):
- Both READMEs list `generate/audit.py` as "written". It was deleted.
- `data/fetch_data.py` names `scripts/seed-r2.sh` and `pipeline.regs.matching.bathymetry_matcher`. Neither exists.
- `app/packages/core/src/menu/licences.ts` line 143 lists "Fish stocking and lake surveys" as open-licence, and
  `menu/sources.ts` line 121 credits Stocking on the map. Nothing is shown, and the stocking licence is not open.
- T218 says "it is OGL-BC; no licence email needed". The catalogue today says "Access Only" (section 2).
- `SOURCES.md` gives the open set's last change as 2026-01-28. The catalogue record says 2023-08-23.

## 2. Sources (checked on the web 2026-11-10)

| What | URL | Format | How often it changes | Licence |
|---|---|---|---|---|
| Bathymetric Maps - Open (table + PDF links) | catalogue.data.gov.bc.ca/dataset/1427d389-cd21-4fe2-8ed9-282d9bdcb7e2 | CSV, one row per sheet, each with a PDF link on a100.gov.bc.ca | "asNeeded"; last changed 2023-08-23; status "underDevelopment" | **Open Government Licence - British Columbia** |
| Bathymetric Maps (sheet outlines) | catalogue: `bathymetric-maps`, layer `WHSE_FISH.BATH_SURVEY_MAP_SHEETS_SVW` | WFS / WMS / KML | record touched 2026-11-07; services "daily" | **Access Only** |
| Bathymetric Maps of Surveyed Lakes (traced contours) | catalogue: `bathymetric-maps-of-surveyed-lakes`, layer `WHSE_FISH.BATH_LAKE_BATHYMETRIC_SP` | WFS / WMS / KML polygons | "asNeeded" / "annually"; status "completed" | **Access Only** |
| Provincial Fish Stocking Reports | catalogue: `provincial-fish-stocking-reports`, view `WHSE_FISH.FISH_RELEASES_PUBLIC_VW` | custom download only, no file, no API | "daily"; 1900 to present | **Access Only** |
| FIDQ (the Province's query pages) | a100.gov.bc.ca/pub/fidq | web forms | "asNeeded" | **Access Only** |
| GoFishBC stocking report (FFSBC) | gofishbc.com/stocked-fish, data at `/wp-json/ffsbc-stocking/v1/releases` | JSON, one request by date range | their page says every couple of days in spring and fall (from a search result, not read on the page itself) | **All rights reserved; written consent** |

The terms that matter, quoted:
- Open licence (B.C.): "Copy, modify, publish, translate, adapt, distribute or otherwise use the Information in
  any medium, mode or format for any lawful purpose." You must "Acknowledge the source of the Information by
  including any attribution statement specified by the Information Provider(s)". Default line: "Contains
  information licensed under the Open Government Licence – British Columbia."
- Access Only: the catalogue links it to the Province's copyright page, which says that for catalogue material
  marked "access only", reproduction is not permitted, and other material "may not be reproduced or redistributed
  without the prior written permission of the Province of British Columbia." The request form is
  forms.gov.bc.ca/copyright-permission-request (email QPIPPCopyright@gov.bc.ca).
- GoFishBC: "Copyright © 2003 - 2026, Freshwater Fisheries Society of BC. All Rights Reserved." and "You require
  written consent to copy or display any portion of this website."

What this means under the self-host rule (data only from our servers):
- **Chart PDFs: yes.** They are the open set. We can copy them to our storage and serve them, with the credit line
  and "not for navigation".
- **49 chart ids are only in the Access Only layer** (Harrison Lake is one). Hold them back until permission.
- **Traced contours: not without permission.** This contradicts T218.
- **Stocking: not without permission**, from either the Province or FFSBC. A link out to GoFishBC is the only
  thing allowed today, and it is not self-hosted, so it is your call.

The GoFishBC endpoint still works. I asked it once for 2026-10-25 to 2026-11-10 and got 29 rows. Each row has the
date, town, lake id (`wbid`, for example `00728KHOR`), lake name, species, strain, stage, size in grams and count.
The lake id is the same code the chart lists use.

Could not confirm: the exact update days of either stocking source; whether FFSBC or the Province would grant
permission; whether GoFishBC has a stable per-lake page to link to.

## 3. Matching

The rule from the gauges: one frozen, reviewed file per kind. A program only proposes. You accept. The build and
the app both read the frozen file and never match on their own.

Both kinds use the same key. A chart sheet and a stocking row both carry the Province's 50K lake code. Our lakes
layer carries that code too, so the join is exact, then checked by name. The rule is already written in
`lake_facts.bind`: a code that covers several lakes binds only the ones whose names agree, and a code whose names
all disagree is refused.

Depth charts, counted today against the live release (7,595 lake items):

| Group | Count | What happens |
|---|---|---|
| Code found and name agrees | 2,122 | proposed as clean; you approve in bulk or spot-check |
| Lake has no name in the Province's data | 143 | need a look (100 land on one of our lakes, 43 land on none) |
| Name disagrees | 12 | need a look (for example a sheet called Cripple Lake on Nendatoo Lake) |
| No lake found for the code | 17 | need a look (for example Lower Arrow Lake, McIvor Lake) |
| One code, several lakes | 51 | need a look; some are inside the 2,122 |
| Only in the Access Only layer | 49 | held until permission |
| Only in the open table | 30 | run through the matcher like the rest |
| Broken or missing PDF | 6 | 5 "File not found" pages, 1 never downloaded |

Result today: 2,222 codes land on a lake item and 2,240 lakes get a chart. 548 of those lakes have rules of their
own in the book. About 230 rows need a look, well under an hour in the review app if it shows the sheet beside
the lake.

Stocking: zero matched, because nothing is fetched. What is known: the same code join hit 97.6 % on the chart
rows (2,674 of 2,741), and the old fetch noted "a few hundred hatchery ponds" with FFSBC's own ids that bind
nowhere. Expect the same shape: most clean, a few hundred to look at or mark "not a public lake".

The match is stored by the durable lake id (`wbk:<key>`), never by a section id (AGENTS rule 5).

## 4. Freshness

### Stocking feed on the worker
Follows the in-season notices feed (`app/workers/feeds/src/inseason.ts`): the worker fetches, compares, publishes
to our storage, and the app reads only our copy.

- **Once, on the Mac:** fetch the whole history (about 18 MB), run the matcher, you review, the match file is
  frozen. The release carries each lake's last stocked day (`lakes/1`, under 10 KB) so the map works offline.
- **Daily, on the worker:** one request for the last 45 days. Add new rows to the stored list. Place each on its
  lake through the frozen match file.
- **Cadence:** once a day, all year. One request. Their own page moves every couple of days at the busiest.
- **Change detection:** hash the rows. Same hash, nothing is rewritten except the "checked" time, so the app can
  tell a stopped worker from a quiet week (the T548 heartbeat).
- **A lake id the match file does not know:** it is not shown. It goes on a proposal list and you get an email.
  Never guessed, never dropped quietly.
- **A failed fetch:** a failed run on the status page and a health alert. Never an empty list.
- **Words in the app:** "Last stocked 6 Nov 2026: 1,500 rainbow trout (catchable)". Under it: "Stocking records
  as of 10 Nov 2026." If the worker has not checked for 3 days: "Stocking records may be out of date (last
  checked 7 Nov)." A lake with no record: "No stocking record", never "not stocked".
- **Cost per run:** one outside request, one or two small writes, a few milliseconds. Inside the current plan, so
  no added money. Data moved per run: a few KB in autumn (29 rows for 16 days). The spring peak was not measured.
- **Blocked on permission** (section 2). Nothing here should be built before the answer.

### Depth charts
- **PDFs are too big for the release.** 768 MB in total. Median 215 KB, nine in ten under 512 KB, largest 5.4 MB.
  Keep them as on-demand files on our storage, named by content, fetched when the angler taps. Show the size
  first. Keep the file on the phone after the first open.
- **The list of charts goes in the release:** lake, title, year, sheet number, file, bytes. About 2,700 rows. My
  estimate is 40 to 60 KB packed. Not measured.
- **Freshness:** the open table last changed in 2023. Use the T412 slow-feed pattern: the worker checks the
  catalogue record's date once a week and emails you if it moves. No GitHub schedule.
- **Traced contours** would ride in the tiles, and T218 expects them small. Blocked on permission.

## 5. Display

| Option | What it costs | What it crowds | Notes |
|---|---|---|---|
| A. Fourth view "Lake" on the water pane | M. One entry in `WATER_VIEWS` (shared by web and phone), one screen, cards | The strip goes from 3 to 4 words, only on lakes that have data | Room for chart, stocking, later depth figures. Regulations stay clean |
| B. A section under Regulations | S | The rules page; an angler may read stocking as a rule | Works against the "no wrong regs" gate |
| C. In the Map view of the water | S | Little | Hard to find; a PDF is not a map layer |
| D. Map colouring: Stocked / Depth | Already built; S to switch on, plus the colour-blind ramp fix left by T158 | Nothing new | Answers "which lakes near me", not "what about this lake" |
| E. Public water page (web) | S per item once the data exists | Little | Good for search traffic; same permission limits |

**Recommendation: A plus D.** A "Lake" view for the one lake, and the colourings that already exist for finding
lakes on the map. B is cheaper, but it puts facts that are not rules on the rules page. The view strip can already
hide views per water (`stepView` takes a `within` list), so rivers and lakes with no data keep three views. E comes
after. Words and rows come from `core`, so web and phone show the same thing.

## 6. Staged plan (proposed tickets, none filed)

| # | Title | Size | Depends | Who |
|---|---|---|---|---|
| D1 | Ask permission: FFSBC for stocking data; the Province for the Access Only stocking view, traced contours and sheet layer. Correct T218's licence line | S | — | owner |
| D2 | Chart matcher and the one frozen match file (`data/curated/bathymetry/matches.json`): about 2,122 clean proposals, about 230 to review; open table only | M | — | Claude, then owner review |
| D3 | Charts on our storage (content-named PDFs) and the chart list in the release; weekly source check by the worker | S | D2 | Claude |
| D4 | "Lake" view with a Depth chart card: opens the PDF in the app (phone) or a browser tab from our server (web); says the size, says when offline; Depth map colouring switches on | M | D3 | Claude, owner look |
| D5 | Stocking: fetch history, run the matcher, frozen match file, review | M | D1 granted | Claude, then owner review |
| D6 | Stocking feed on the worker (daily, as-of, stale words, proposals for unknown lakes) | M | D5 | Claude |
| D7 | Stocking card in the "Lake" view, Stocked map colouring on, colour-blind ramp fix | M | D6 | Claude, owner look |
| D8 | Traced contours for the 28 lakes (T218 as written) | S | D1 granted, D4 | Claude |
| D9 | Trace contours from scans for the top lakes (T219) | L | D8 | stays postponed |

**Smallest useful step:** D2, D3, D4. An angler opens a lake and sees its depth chart. Nothing in it waits on
permission. D1 costs you two emails and unblocks everything else.

## What you must decide
1. Send the two permission requests (D1)? Without them stocking and traced contours stay off.
2. Is a "Lake" fourth view the place, or do you want it under Regulations?
3. While stocking permission is pending, do you want a plain link out to GoFishBC on a lake, or nothing?
4. The 49 lakes whose chart is only in the Access Only layer: hold back (my default) or ask in the same request?

## Not checked
- Whether the 5 broken PDFs and the 1 missing one exist under another file name.
- Why the sheet layer flags 697 sheets (517 lakes) as "vectorized" when the contour layer holds 28 lakes. There
  may be more digital depth data than T218 found.
- T218's counts (28 lakes, 137 shapes) and T219's fit numbers. Taken from the tickets, not re-run.
- The size of the chart list in a release, and stocking row counts in spring. Both are estimates.
- Stocking match counts on real rows. No bulk data was downloaded.
- The GoFishBC page's own words on update days (the page text did not load; the cadence is from a search result).
- What the Access Only licence page says in full. I read the Province's copyright page it points to.
- Whether the old matcher code and the atlas still agree. No builds, tests or servers were run.
- `curation-review/` was not opened, so I do not know how much of a chart review screen already exists there.
- iOS behaviour of the PDF viewer for a second kind of file.
