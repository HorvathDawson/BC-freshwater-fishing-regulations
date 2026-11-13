# T671 — The T462 audit's salmon findings (D1–D5)

Status: IN PROGRESS — every finding settled: D2 and D3 fixed (6b02feb06, 188392691); D1, D4 and D5 built on the owner's answers of 2026-11-12 (4bc03ac14; D1 6,254 sections toward closed, D4 no change, D5 wording); the scraped rows typed again 2026-11-12 (`typed_format` 3); the owner's two follow-ups of 2026-11-12 built (b7749b726; 5B aggregate: the worker's live join now pools as T349 does, RULINGS Q29; D5 extension: catch-and-release / check part-headlines, P47); left (Claude): the D3 tiers render test in `SalmonCard.test.tsx` (the file passes today)

Source: `handoff/PLAN/app-integration/T462-audit.md` (85ba2e821), findings D1–D5. Owner instruction 2026-11-11 (through
the coordinator): a finding that needs a choice between two faithful readings is the owner's call. Fix only what the
parser or the screen drops or mis-types.

Measured on release `r-303b4007a70a`, rebuilt in memory with the tree's code (no release run). The tree's rebuild
without this ticket equals T668's numbers (449 chains, 1,949,842 sections).

## Fixed (mechanical)

### 1. D2 — a row nobody could type keeps the species its cell names. FIXED (6b02feb06)
- Reproduced. All 9 "To be determined" rows (8 places: 3 in Region 1, 5 in Region 3) were typed with no species.
  - `Chum | To be determined | To be determined` (Courtenay, Puntledge): all five species read "Check DFO".
  - `Chinook | To be determined | To be determined` (Campbell River, the narrower place): hid the wider place's
    `Coho | Sept 1 to Dec 31 | 4 hatchery-marked per day — FN0939` and `Pink | Jul 15 to Oct 15 | 4 per day`.
  - `Sockeye | Apr 1 to Mar 31 | To be determined` (Kamloops Lake and 4 undrawn Thompson / Fraser places): the dates
    read, so the row answered for no fish and sockeye fell to "closed".
- Fix: `typed.to_rules` keeps the row's species on a row it cannot type; `deliver.salmon.verdict_on` reads such a row
  (`unread_row`) as "check" for those species on the days it holds. A species cell the table does not know reads for
  every salmon, never none. A row about another fish and a gear permission are not unread rows. Same in the worker
  (`workers/feeds/src/dfo/typed.ts`, `salmon.ts`, both modes).
- Before → after, sections whose status changes on some day: 8.
  - toward the named species only: chinook 5, coho 7, sockeye 7, pink 7, chum 2 sections go "check" → "closed" (the
    row never named them);
  - Kamloops Lake sockeye "closed" → "check" (1 section, 366 days); the same on 4 undrawn Region 3 places (caveat);
  - toward OPEN, 2 sections, both the Campbell River below the Quinsam: coho "check" → open Sep 1–Dec 31 and pink
    "check" → open Jul 15–Oct 15. These are DFO's own rows quoted above, hidden until now by the chinook-only row.
- The typed feed now carries `typed_format` (`typed.FORMAT` = 2). `salmon.load_inputs` refuses typed files of another
  number and names the command, so rows typed by older code cannot ship.
- Tests: `pipeline/tests/test_t671_salmon_rows.py` (10; 7 fail on the code before), `workers/feeds/test/dfo.t671.test.ts`
  (12, the same rows and answers, both modes). Goldens `salmonFeed.golden.ts` and `salmonStraddle.golden.ts` are
  unchanged (both generators report current).
- Shows: release path after the typed rows are typed again and the next release; live path on staging now (worker
  deployed), production with the owner's worker deploy.

### 2. D3 — the bucket's period limits are drawn. FIXED (188392691)
- Reproduced. The release holds them as child tiers of each region's bucket tree (Regions 1, 2, 3, 5B, 6, 8: "The total
  annual limit for chinook salmon over 50 cm is 10 fish from all fresh waters combined."; Region 8 also "There is a
  monthly limit of 4 chinook over 50 cm in Region 8."). `bucketOf` drew the total tier only. 433,268 sections have a
  day when chinook opens under such a region (433,203 on the tree).
- Fix (app, presentation only): `SalmonBucketVM.tiers`, one line per child tier in the store's words, while a species
  it names may be kept that day; `SalmonCard` draws them under the total. Two regions' buckets are one only when their
  whole trees are equal (Region 8's monthly limit made them differ and was dropped by the old key).
- Test: `core/src/salmon.test.ts` "(T671 D3)". Render test (2026-11-12): `ui-native/src/regs/SalmonCard.test.tsx` "(T671 D3) the
  bucket's period limits are drawn under the total…" (a coho limit drawn, a chinook one not while only coho may be kept;
  mutation: the card not mapping `tiers` fails it).
- Shows: with the next app build; no release needed.

## Ruled by the owner 2026-11-12, built (4bc03ac14)

Measured on release `r-483b295de57c` (LATEST): every chain's 366 days recomputed in memory by the code before and after
(the curated places, the scraped rows typed by each side's code, the release's chains and section counts; no release run).

### D1 — "Yes to option 1 but add to gotcha list". BUILT
- On ONE place, an "All" closure wins over a species row unless the species row is dated inside the All row's dates
  (`deliver.salmon._all_closure_wins`, called by `verdict_on`; the worker's `salmon.ts` `allClosureWins`). An All row that
  is no closure never displaces a species row; a wider place of the chain is not the same place. RULINGS L24; gotcha in
  `pipeline/docs/GOTCHAS.md` ("An "All" closure against a species row on the same DFO place").
- Every place on today's pages with an All closure and a species row that is not a closure: only two are "not inside" —
  the Tatshenshini below the border and the Koksilah. No species row on any page has dates EQUAL to an All closure's, so
  no case needed a choice beyond the ruling.
- Effect: 2 chains, 6,254 sections (Tatshenshini below the border) — coho open → closed Jul 24–Aug 14, 137,588
  section-days, all TOWARD CLOSED; the Koksilah's own (undrawn) chain the same (coho open only Jun 1–30, Oct 1–Nov 30).
  Nothing moves toward open. Words only: on 34 more chains (68,089 sections: Region 6 B(i) and its waters, the
  Tatshenshini above the border, the Kloiya) a closed species' line on the All closure's days now quotes it ("No fishing
  for salmon (Jan 1–Jun 15)") instead of the species' own closure — closed either way. Cowichan 66 Mile unchanged (its All row is catch and release).
- Tests: shared rows (Tatshenshini both sides of the border, Koksilah, Cowichan, an opening dated inside an All
  closure) in `dfo-rows.json`, read by `test_t671_salmon_rows.py` and `dfo.t671.test.ts`; `test_t671_salmon_readings.py`
  `test_d1_*` (the closure's words, an undated row, a mutation pin, a wider place).

### D4 — "The daily limit for all species of Pacific salmon from tidal and fresh waters combined is 4. Individual species limits also apply. Species limit is seperate." SETTLED, no change
- The combined 4 is Region 6's bucket — the header's daily note, already a total tier on the card ("Up to 4 salmon a day
  in total, fresh and tidal waters combined"), a note that never decides open/closed. Each species' limit is its own line
  ("4 a day · only 1 over 65 cm"), not a pool across Region 6: `quota.pool_of` gives `6:a:region` no pool. That is how the
  release and the feed already read, so nothing is built and no section changes (quota trees identical on every chain).
  RULINGS Q28. Pinned: `test_t671_salmon_readings.py` `test_d4_*`.
- Region 5B's per-species pool over MUs 5-6 to 5-11 (T349, the same DFO phrase) is untouched; the owner's D4 words read
  that phrase the other way for Region 6. Raised in the report, not changed.

### D5 — "Closed today · open in part". BUILT
- Core `salmonCard`: where the page reads closed today and a DFO place on its water not drawn yet is OPEN today (the
  pipeline's verdicts for that place, looked up), the headline and the in-part caveat's title read "Closed today · open
  in part", the caveat's line "DFO opens salmon in a part of this water the map can’t draw yet: check DFO for where.
  Everywhere else, follow this page." The page's status, bar and map stay the page's. RULINGS P47; `salmon.test.ts`
  "(T671 D5 …)".
- Effect: 87,419 section-days on 717 sections (Nass section C, Kitsumkalum, Skeena B(i), Region 8, Kitimat …). Where the
  undrawn place is only catch-and-release or "check" (11,888 section-days) the caveat keeps its ordinary words — the
  ruling names an OPEN place.
- Shows with the next app build.

## The owner's follow-ups of 2026-11-12, built (b7749b726)

Measured on release `r-483b295de57c` (LATEST) with `measure.py` / `compare.py` (every chain's 366 days recomputed in memory
by the tree's code, before and after): the release's verdicts, quota trees and words are identical (0 chains differ) —
both changes are in the live feed and the app.

### 5B "in the aggregate" — RULINGS Q29. The release already matched; the worker's live join did not
- Owner: "5b says this on the ALL row (All lakes or streams or part thereof in the aggregate (combined total), in
  Management Units 5-6 to 5-11 in Region 5B, unless otherwise stated below (i.e. Region 5 not including the Fraser River
  watershed).)... So any water body not in the table will be with the total in aggregate (shared total) .. so basically
  items not in the table in mu 5-6 to 5-11 are shared but other row listed items have their own quota. Also the 4 salmon a
  day limit in total of all species is still a thing here too".
- What T349 built: `quota.pool_of` pools a place whose words say "in the aggregate" / "combined total" across its own
  `within` areas — only `5b:region:section` (`area:mu_group:management_units_5_6_to_5_11`) on today's pages; `mark_shared`
  marks its chinook and coho 4-a-day rows (`deliver.salmon.build`, the Python `live.join`); the verdict's tree is the
  `total` tier "Up to 4 chinook a day in total, all waters of Management Units 5-6 to 5-11 combined". Listed waters
  (Atnarko, Bella Coola, Chuckwalla, Kilbella, Docee, Wannock, Dean, Long Lake) have no pool. The 5B bucket is "Up to 4
  salmon a day in total, fresh and tidal waters combined" (+ the annual 10 chinook over 50 cm). Same as the owner's reading.
- What differed: the feeds worker's live join (`live.ts` `join`) never called a pool, and its `quota.ts` had no pooled
  tier: the live feed read 5B's ALL row as "4 a day" per water. Ported `poolOf` / `poolWords` / `markShared` and the
  `total` tier; `join` marks the rules as the Python does.
- Effect: release none. Live feed (with the owner's worker deploy): the pooled words on 5B's ALL-row answers (92,835
  sections in one-region chains; straddlers show no number, Z4a). No status moves.
- Checked for a row the words leave ambiguous — a listed water whose row is silent on a species on some day would fall to
  the pooled ALL row: on no drawn chain on any day (every 5B chain with a listed place answers all five species itself or
  through the region's "limit is zero" sentence). Nothing held. Noted for the labelling: the Dean River (41 sections) and
  Long Lake (1) have their own rows, but those places are not drawn, so their sections show the pooled ALL row with the
  in-part caveat.
- Tests: shared cases in `dfo-rows.json` (`place`, `quota`: an unlisted 5B water pooled, the Bella Coola's own row, Region
  6 A not pooled) read by `test_t671_salmon_rows.py` and `dfo.t671.test.ts`; a chain test and a mutation pin (no pool →
  "4 a day") in the Python file.

### D5 extension — RULINGS P47
- Core `salmonCard`: a page closed today whose undrawn place is only catch and release reads "Closed today · catch and
  release in part"; one reading "check" reads "Closed today · check DFO for part"; each with its own caveat line pointing to
  DFO (`SALMON_WORDS.closedReleaseInPart` / `releaseInPartCalm`, `closedCheckInPart` / `checkInPartCalm`). Several kinds on
  one page-day: open, then catch and release, then check. Status, bar and map stay the page's.
- Effect on LATEST (a section's page, its chain's named undrawn places): check 11,888 section-days on 33 sections (3
  chains: the Fraser and South Thompson in Region 3, undrawn places printing sockeye "To be determined"); catch and
  release 0; open unchanged (93,447 section-days, 764 sections, by this count). Nothing moves toward open.
- Tests: `salmon.test.ts` "(T671 D5 extension) …" (check; precedence across two undrawn places); the T372 and
  all-closed tests now read the release headline on Sep 15 (the fixture's undrawn Cowichan place).

## As held for the owner (2026-11-11, kept for the record)

### 3. D1 — an "All" closure window against a species' own opening. HELD
Today: within one DFO place, a species' own row speaks before an "All" row (`deliver/salmon.py` `verdict_on`). The
class exists in Regions 1 and 6 only. Every case, with DFO's rows in full:

| pattern | place (DFO page) | DFO rows | sections | today | if the "All" row wins |
|---|---|---|---|---|---|
| A. species opening printed all year, "All" closure dated inside it | Tatshenshini River, downstream of the BC/Yukon border (Region 6, section E) | `All \| Jul 24 to Aug 14 \| No fishing for salmon` · `Chinook \| Apr 1 to Mar 31 \| No fishing for chinook` · `Coho \| Apr 1 to Mar 31 \| 2 per day` · `Sockeye \| Apr 1 to Mar 31 \| No fishing for sockeye` | 6,254 drawn | coho open all year, 2 a day; chinook, sockeye, pink, chum closed | coho closed Jul 24–Aug 14 (22 days), open the rest |
| A | Koksilah River, including tributaries (Region 1) | `Coho \| Apr 1 to Mar 31 \| 1 per day, maximum size 35 cm` · `All \| July 1 to Sept 30 \| No fishing` · `All \| Dec 1 to May 31 \| No fishing` | not drawn; caveat on 11 sections | coho open all year | coho open only Jun 1–30 and Oct 1–Nov 30 |
| B. species opening dated around the closure (no overlap) | Tatshenshini River, upstream of the border: Blanchard River; mainstem and other tributaries (Region 6, section E; two places) | `All \| Jul 24 to Aug 14 \| No fishing for salmon` · `Chinook \| Apr 1 to Mar 31 \| No fishing for chinook` · `Coho \| Aug 15 to Jul 23 \| 2 per day` · `Sockeye \| Apr 1 to Mar 31 \| No fishing for sockeye` | 287 + 769 | coho closed Jul 24–Aug 14, open the rest | the same |
| C. species opening dated inside an "All" row that is not a closure | Cowichan River, 66 Mile Trestle to the Sandy Pool sign (Region 1) | `All \| Sep 1 to July 14 \| Open for salmon catch and release` · `Coho \| Nov 1 to Dec 31 \| 1 per day` · `Chum \| Nov 4 to Dec 31 \| 2 per day — FN1196` | 43 | release only Sep 1–Jul 14; coho kept Nov 1–Dec 31, chum Nov 4–Dec 31; closed Jul 15–Aug 31 | coho and chum release only on those days: the two species rows would never apply |
| D. "All" closure beside species closures only | Region 6 section B(i) default; Skeena mainstem above Cedarvale; Kloiya River | e.g. `All \| Jan 1 to Jun 15 \| No fishing for salmon` · `Coho \| Apr 1 to Mar 31 \| No fishing for coho` … | 58,056; 40; undrawn | closed either way | the same |

What DFO's page structure signals:
- No row in any of these places carries exception words. "unless otherwise stated below" appears only on defaults
  (Region 6 A's `Sockeye, pink and chum | … | No fishing for sockeye, pink or chum salmon, unless otherwise stated
  below`; the page sentences "Unless otherwise stated in the table below, the daily limit … is zero"), and Region 5B
  prints "(see exception below)" on two rows. None of them is an "All" row against a species row.
- Pattern B is the same river with the same closure, and there DFO dated coho around it (`Aug 15 to Jul 23`). That
  reads as: the closure is meant to shut coho, and pattern A's `Apr 1 to Mar 31` is the same intent written loosely.
- Pattern C points the other way: a short species opening inside a long "All" season, one of them set by an in-season
  notice (FN1196), is the exception to the "All" row.
- One rule that fits all four: the row with the shorter season inside the other's is the exception. It is an
  inference from the dates, not something DFO states.

Owner's call: (a) leave as is; (b) the "All" closure wins when the species row is not dated inside it (changes A only:
6,254 sections on 22 days, plus the Koksilah caveat); (c) an "All" row always wins (also turns Cowichan coho and chum
to release only).

### 4. D4 — Region 6 section A's "in the aggregate (combined total), in Region 6". HELD
- Reproduced. DFO's cell: `A. All Region 6 waters | All lakes or streams or parts thereof in the aggregate (combined
  total), in Region 6, unless otherwise stated below. Please review sections B, C, D, E and F carefully.` with
  `Chinook | Apr 1 to Mar 31 | 4 per day, only 1 over 65 cm.` and `Coho | Apr 1 to Mar 31 | 4 per day, only 1 over 50
  cm.` The scrape keeps the section title ("All Region 6 waters") as the place's words and drops the cell, so
  `quota.pool_of` sees no pool. `6:a:region` answers an opening on 393,903 sections (26 chains) and reads "4 a day ·
  only 1 over 65 cm" as if per water.
- Region 5B prints the same phrase and the owner settled it as a pool (T349): "Up to 4 … a day in total, all waters
  of Management Units 5-6 to 5-11 combined" (92,956 sections).
- Two readings: (a) as 5B, a pool over Region 6: the card says "in total, all waters of Region 6 combined"; (b) the
  phrase is boilerplate and the region's 4-a-day bucket already says it. Under (a) a 6/7 or 5/6 straddler shows both
  regions' words with no number (a pooled count is not compared, Z4a).
- If (a): carry the cell through the scrape (`untangle` → `locations` → the typed locator), `pool_of` reads it, and
  `pool_words` learns "Region 6"; the same in the worker.

### 5. D5 — an undrawn DFO place that is looser than the page. HELD
- Reproduced. 2,430 sections carry an undrawn place that has rules (2,272 on the tree, after T668's fix 1). On
  101,896 section-days every drawn species is closed while the undrawn place is not (44 chains); on 159,160
  section-days some species is looser at the undrawn place. The card says "Closed today" and the caveat is titled
  "Fishing is different in some spots today" with "Everywhere else, follow this page."
- Example (the audit's): the Harrison on Nov 11, page closed, the undrawn place opens coho 4 hatchery-marked and chum 2.
- Two readings of what to show: (a) keep the drawn answer as the headline and re-word the caveat ("part of this water
  is not on the map yet", and a headline such as "Closed today · open in part"); (b) the whole water reads "Check DFO"
  while an undrawn place is looser, which is what the card did before T372 (owner 2026-11-04 chose the trout way).
- The lasting fix is drawing the places (the owner's DFO labelling).

## Not done, not verified
- The typed files on disk were not rewritten (the review app reads them); the command is above.
- The staging feeds worker is deployed (version cfd5d140; its three jobs answered 200). The published DFO feed was not read back. The deploy took the working tree, which held another lane's uncommitted `crashes.ts` / `reports.ts` edits (the suite passed with them).
- No device or emulator was driven. The D3 line was checked in the view model only.
- D1's "if the All row wins" column was worked out from the rows, not built.
