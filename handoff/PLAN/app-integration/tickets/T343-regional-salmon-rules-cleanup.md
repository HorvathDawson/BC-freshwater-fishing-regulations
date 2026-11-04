# T343 — Clean up the regional salmon rules (DFO header notes) on the water page

Status: DONE 2026-11-04 (561115e24, c706d67ac, ec5b37257; items 6–7 → T367, UI chat). items 1–5, 8 DONE 2026-11-03 (data side; needs the batch-3 rebuild); items 6–7 are UI → T367 (UI chat). Owner list below. (owner 2026-10-31). Depends on T332 (salmon in the shared quota tree) — do on top of it.

The region header notes (`regs.dfo_salmon.header`, shown as "region rules" on the salmon card) are mostly not worth
showing on their own. Work each into the place it belongs, or drop it:

1. **"No fishing within 100 m of a government fish facility"** — the freshwater regs already carry the same rule and the
   page already shows it. Drop the DFO copy (no duplicate line).
2. **Daily salmon limit, fresh + tidal combined** — say it where the card states the daily total at the top of the salmon
   breakdown (the bucket line), not as a separate note.
3. **"Adult chinook" size** — make it a per-water value: default 50 cm, except the waters DFO names otherwise (at least in
   Region 2; R6 says over 65 cm except the Fraser watershed). Feed it into the size tiers/lengths where "adult" matters,
   not as a note.
4. **How-to-measure notes** ("tip of nose to tail fork", etc.) — drop.
5. **Every other header note** — list them all (every region's page), and for each: fold into a structured place (bucket,
   size, closure, season, gear) or drop. Nothing stays as a free-floating note unless it changes what an angler may do
   and has no structured home — list those for the owner.
6. **Salmon images (owner 2026-10-31):** the COLLAPSED salmon section shows one image, the ocean-phase chinook
   (`chinook-salmon-ocean-phase.webp`). When OPENED, each species row uses its own spawning-phase image
   (`<species>-salmon-spawning-phase.webp`; every species has one). Assets in app/apps/mobile/assets/fish/.
7. **DFO source-page info is one collapsible line, collapsed by default** (page link, date modified, notice numbers).
   Opened, it shows the full source details.
8. Tests: the five T320 spot-check places still read correctly; no header text lost without being folded or listed as
   dropped (a dropped-notes list in the ticket).
Size: M.

Unblocked 2026-11-03: its dependency (T332 merged 0c276aa10 / T350) is DONE.

## Done 2026-11-03 (items 1–5, 8)
Every header note now carries `fold` (`deliver.salmon.header_answers`, worker `headerAnswers`): `bucket` / `sizes` /
`shared` / `closure` where that structure read it, `drop` for the dropped kinds. The card (core `salmon.ts`) shows a note
as words only without one (a sizes note still shows where no sizes line says its species; a bucket note where the page
has no single bucket). Notes stay in the file (ids, closure `c`). Format unchanged (salmon/4: a new optional field).
- **1 facility 100 m:** `drop` (all 6 pages).
- **2 daily, fresh + tidal:** the bucket's total says "Up to 4 salmon a day in total, fresh and tidal waters combined";
  daily, possession, annual and monthly notes fold into the bucket (`bucket.ids`).
- **3 adult chinook per water:** `quota.ADULT_CHINOOK` = 50 cm (owner default) where a page names none (5B) and on waters
  a region-wide length excepts: R6 "(other than Fraser River Watershed)" is now parsed as `x` → that watershed gets 50,
  the rest of R6 65. R2's 62 cm waters (Fraser, Chehalis, Chilliwack/Vedder, … Sep–Dec) were already per water.
  **Owner:** R6's "Fraser River" binds no R6 DFO water, so the notice "the header names 'Fraser River', which no curated DFO
  water binds" now fires (DFO review: bind it, or say R6 has no Fraser-watershed salmon water).
- **4 how to measure:** `drop`; the min-size notes that also say it fold into the sizes line.
- **5 every other note** (61 notes, 14 kinds, all pages): closed → closure; min_size / adult / jack → sizes; aggregate
  (Squamish) → shared; "Region 5 is divided into 2 parts:" → no longer a note (a pointer, no rule).
  **Left as words (change what an angler may do, no structured home — owner):** daylight only (R2, R3, R8: "1 hour before
  sunrise to 1 hour after sunset"), R1's stream bait ban (all streams except Quatse, Somass, Sproat, Stamp …), and the
  eulachon reminder (R5B, R6: not salmon — drop?).
- **8 tests:** test_dfo_header (T343 folds + the R6 exception), test_dfo_quota, test_dfo_live (golden, Fraser notice),
  core salmon.test (folded notes hidden), worker dfo.quota (fixture parity incl. `fold`). No header text is lost: a folded
  note's words are still in the file and in its structure's `t`.
- **6–7 → T367** (UI chat files): collapsed salmon section = ocean-phase chinook image, opened rows = each species'
  spawning-phase image; DFO source line one collapsible row, collapsed by default.

## Owner round 2 (2026-11-03: "fix all these", eulachon stays)
- **Daylight → `hours`:** a `daylight` note carries `hours` (the catalogue's `Hours`, solar ends): DFO's definition for
  R3 / R8 ("1 hour before sunrise … 1 hour after sunset"); R2 prints none, read as sunrise to sunset. The region ships
  `hours: {hours, says, id}`; the card's VM has `hours` ("Salmon fishing in daylight only: …", null when all closed).
  Fold `hours`. Its words stay in the region block until the card draws `hours` (T367, UI chat) — then one line in
  core `asWords` drops them.
- **R1 bait ban → dropped:** the province already carries "Bait ban: all streams of Region 1, all year" (z1 base), and
  each river DFO excepts (Quatse, Somass, Sproat, Stamp) has its own provincial seasonal bait ban — the card's gear line
  says it, as for trout.
- **Eulachon:** stays as words (owner).
