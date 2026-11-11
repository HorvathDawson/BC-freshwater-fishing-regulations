# T634 — Lake tab: depth charts and stocking, to the owner's decisions of 2026-11-11

Status: OPEN

Source: the depth and stocking audit (T608 D, page https://claude.ai/artifact/7Dn9FwZu3N1aMGxsCR4ea3) and the owner's answers 2026-11-11.

## Owner decisions
- Licence: stocking and lake bathymetry are Open Government Licence in the B.C. Data Catalogue; no permission requests.
  (The audit page had marked the placed contours and stocking "Access Only": re-read each record's licence field, correct
  the page, and show the owner any record that does not say OGL before building on it.)
- Fetch the 684 vector depth files (about 200 MB) to this Mac, outside git; build and test the reader and the placing. Nothing published yet.
- Lake information lives in a fourth "Lake" tab.
- Scan-only lakes: the PDF only. Placing a scan over the map is a deferred future task.
- The PDFs are too big: compress to the smallest that keeps the smallest printed depth numbers readable; report the size saved.
- Chart matching A7 (name column only): use the name variants already curated. A name never picks a lake on its own: the lake
  must be in the same area (code, position, size), so one "Trout Lake" can never take another's chart. The owner recalls
  matching code already existed: find it and reuse or retire it, do not write a second matcher.
- A2 (code also covers unnamed ponds): bind only the named lake. Codes with two named lakes still go to the owner.
- Clean-match limits stay: 100 m, and half to double the area.
- Stocking match: lake code plus name (with curated variants), never name alone; a first-time lake goes to the owner. Read the
  ORIGINAL stocking match scripts first: they used the group code and may carry more information; reuse what is sound.

## Work (first pass, nothing shipped)
1. Licence re-check per catalogue record; correct the audit page.
2. Find the existing chart and stocking match code and scripts (git history too); say what each used (group code etc.) and what is kept.
3. Fetch the vector files outside git; a reader; a placing test on the 308 one-sheet lakes.
4. PDF compression: method, sizes before and after, five sample charts.
5. Proposals file for chart matches per the rules above (everything a proposal; one frozen match file; nothing pre-accepted).
6. Then: the Lake tab (app), after the owner sees the first pass.

Related: T200, T201, T218, T219 (postponed 2026-11-05; this ticket restarts the data half on the owner's word).
Depends: none. Size: L.

## First pass (2026-11-10, items 1–5; nothing shipped, app/ untouched)

### 1. Licence, per B.C. Data Catalogue record (CKAN `package_show`, read 2026-11-10)
| Record | URL | `license_title` | `download_audience` | OGL? |
|---|---|---|---|---|
| Bathymetric Maps - Open (the PDFs + reference table) | https://catalogue.data.gov.bc.ca/dataset/1427d389-cd21-4fe2-8ed9-282d9bdcb7e2 | "Open Government Licence - British Columbia" | "Public" | **yes** |
| Bathymetric Maps (sheet layer `WHSE_FISH.BATH_SURVEY_MAP_SHEETS_SVW`; the only place the vector zips are linked) | https://catalogue.data.gov.bc.ca/dataset/bathymetric-maps | "Access Only" | "Public" | **no** |
| Bathymetric Maps of Surveyed Lakes (placed contours, 28 lakes) | https://catalogue.data.gov.bc.ca/dataset/bathymetric-maps-of-surveyed-lakes | "Access Only" | "Public" | **no** |
| Lake Inventory Surveys (lake survey table) | https://catalogue.data.gov.bc.ca/dataset/lake-inventory-surveys | "Open Government Licence - British Columbia" | "Public" | **yes** |
| Provincial Fish Stocking Reports | https://catalogue.data.gov.bc.ca/dataset/provincial-fish-stocking-reports | "Access Only" | "Public" | **no** |
| Fish Information Data Query (FIDQ) | https://catalogue.data.gov.bc.ca/dataset/fish-information-data-query | "Access Only" | "NA" | **no** |
| Bathymetric Lakes for the Cariboo Region | https://catalogue.data.gov.bc.ca/dataset/bathymetric-lakes-for-the-cariboo-region | "Access Only" | "Government" | **no** |

The 2001 vector zips have no record of their own. The open record's lineage says the sheet layer "is not to be
considered an Open Government Licence - British Columbia catalogue, as it also contains lakes that are not Province of
BC ownership", and its words name only "PDF bathymetric map files". So: the audit page was right on the placed contours
and stocking (Access Only); the owner's note is right for the chart PDFs and the lake survey table only. Per the
owner's rule nothing is built ON the Access Only records: the vector work below is local measurement only (fetched to
this Mac by the owner's decision, nothing published). The audit page (artifact 7Dn9FwZu3N1aMGxsCR4ea3) was NOT edited:
this lane publishes nothing.

### 2. Old matchers (git history read)
- Current `pipeline/deliver/lake_facts.py` `bind`/`core`: code first, names confirm, `wbk:` keys — **sound, reused**.
- Current `pipeline/stocking/generate/match.py` + `identifiers.py`: nearest-of-several by a point the feed lacks,
  unconfirmed code hit = matched, "same name within 2,000 m" fallback with substring names, node ids — **unsound,
  not reused; retire with the stocking work** (stocking README).
- Deleted `pipeline/{bathymetry,stocking}/generate/audit.py` (971f40575): the two-name test (printed title vs gazetted
  column) and the warning that names written by the old matchers are circular — **reasoning reused** in `propose`.
- v1 chain `a40989161^:archive/pipeline/recurring/anglerinfo/` (the "group code" one): WDIC code check, group-code
  join, hand-checked `wbid_overrides.json` (12 rows) sound; its name search ran first and won by nearest with no limit
  — unsound. `687bdee26^:pipeline/matching/bathymetry_matcher.py` + `bathymetry_overrides.json` (19 hand rows): T1
  (code, name breaks a tie) sound; overlap-only and watershed tiers not. The two override tables are not imported
  (everything is a proposal); they can be shown beside the review.

### 3. Vector files (outside git: `~/data-offgit/bc-bathymetry-vectors/`)
- `python -m pipeline.bathymetry.fetch_vectors --out ~/data-offgit/bc-bathymetry-vectors`: sequential, 0.5 s apart,
  resumable, stops on 403/429/5xx (none happened). 684 names tried (513 `VECTOR_FILE_URL` + per-sheet image names):
  **510 zips exist, 187 MB**; 174 "File not found" (every second-or-later sheet name: a multi-sheet lake has ONE zip).
  516 of the 517 flagged codes have a file; 3 flagged sheets have none (01206KETL, 00379BULL, 00456LFRA).
- Reader `pipeline/bathymetry/e00.py`: 504 of 510 read; 6 are compressed E00 (`EXP 1`), not read yet (00153501,
  00162001, 00185501, 00219001, 00219101, 00225301).
- Units, only where the file says "DEPTHS IN …": metres 397, feet 4 (Bridge, Sheridan, Sproat, Lakelse), not stated 104
  (68 of those print "N ft" figures in their statistics block: likely feet, to be ticked from the PDF).
- Placing (`place.py`, rigid turn + shift, never a stretch) on the **318** one-sheet-one-lake codes (audit said 308;
  here: one layer sheet, one zip not shared, one FWA lake under the code): fitted 291; no shoreline closed 22;
  unreadable 5. IoU ≥ 0.95: 89, 0.90–0.95: 117, 0.80–0.90: 45, 0.60–0.80: 17, < 0.60: 23. Median shore distance
  ≤ 10 m: 201, ≤ 25 m: 53, ≤ 50 m: 14, ≤ 100 m: 8, > 100 m: 15. Area sheet/lake within 0.9–1.1: 238 (true size holds).
  **Strict fit (IoU ≥ 0.9, median ≤ 25 m, area 0.8–1.25): 202 of 318 (64 %)**; loose (IoU ≥ 0.8, area 0.8–1.25): 251.
  The worst are shorelines that closed round the sheet frame (area ratio 15–128x), not bad turns. A mirror fit beat the
  true fit 4 times. Every placing still needs the owner's tick beside the PDF.

### 4. PDF compression (sample of 20, size-stratified; method in the scratch notes, nothing rewritten)
- Method: pdfium renders the page, threshold at 160, one 1-bit CCITT G4 image per page at the page's own size
  (pikepdf). Tried: lossless re-save (−4 %), 8-bit scans to 1-bit at native size (−75 % on those), whole page at
  300 / 200 / 150 dpi.
- **Proposed: 200 dpi 1-bit** for bilevel (2,316 files, 495 MB) and 8-bit gray (301 files, 226 MB) charts: sample sizes
  11.1 MB -> 1.57 MB. 150 dpi starts to close the smallest soundings (Marie Lake "35.6": the 6 fills). Projected total
  **768 MB -> about 244 MB** (150 dpi: about 191 MB). Colour scans (53, 42 MB) and vector-only PDFs (10) left as they
  are; 5 broken files (88 bytes) untouched.
- Caveat: the whole page becomes one image, so the typed disclaimer stops being selectable text (still visible).
- Shots (original / 200 dpi / 150 dpi, same paper area at 300 dpi of paper): `T634-shots/1-cherry-lake-gray-scan.png`,
  `2-marie-lake-smallest-sounding.png`, `3-alice-lake-soundings.png`, `4-jacko-lake-contour-labels.png`,
  `5-draw-lake-gray-scan.png`.

### 5. Chart -> lake proposals (`python -m pipeline.bathymetry.propose`, writes `data/generated/bathymetry/proposals.json`)
2,294 chart ids + 44 sheets with no code. Name variants: `data/curated/waters/name_variants.json`
(`CURATED.waters.name_variants`); a variant counts only when its source is not `bathymetry`/`stocking` (those were
written by the v1 matchers from these rows). Word order ("Lake Maxwell") passes by its own test.
| Rule | Ids | Audit |
|---|---|---|
| clean | **1,995** (equal 1,735, title contains the name 244, curated variant 12, word order 4); 31 of them bound the named lake and left unnamed ponds out (A2) | 1,944 |
| two_named (owner picks) | 16 | A2 54 (all multi-lake codes) |
| unnamed | 143 | A3 140 |
| no_lake | 17 | A1 17 |
| variant_circular (agrees only through a bathymetry/stocking-sourced variant) | 69 | in A7 |
| gazetted_only | 8 | A7 86 (with the 69) |
| name_disagrees | 2 | A4 12 |
| position_or_size | 17 | A6 14 |
| no_outline (open table only) | 27 | A6 27 |
| not_in_app | 0 | A5 0 |
Watershed code is evidence only (the audit gated on it; compared without hyphens it agrees on 1,994 of the 1,995 clean).
49 ids (27 of them clean) are only in the Access Only sheet layer, flagged `in_open_table: false`: held for licence. Owner-only `python -m pipeline.bathymetry.accept` is the one writer of
`data/curated/bathymetry/matches.json` (not created: nothing is accepted).

## Second pass 2026-11-11 (owner answers A, B; nothing shipped, app/ untouched)

### A. Chart PDFs at 200 dpi 1-bit (owner: "Yes, 200 dpi B&W")
- `scripts/heavy.sh pdf200 50 -- python -m pipeline.bathymetry.compress` (`pipeline/bathymetry/compress.py`): pdfium
  renders each page at 200 dpi grey, threshold 160, one CCITT G4 image per page on a page of the original's shown size;
  4 workers; source only read; output + `manifest.json` in `~/data-offgit/bc-bathymetry-pdf-200dpi/` (outside git).
  Every chart, any kind (bilevel, grey, colour, vector), goes the same way; the disclaimer is kept as image.
- Open table: **2,599 charts in, 2,599 out, 0 failed. 748.1 MB -> 201.4 MB (-73 %)**; on disk 198 MB.
  By original kind: bilevel 2,250 (484.0 -> 181.9 MB), grey 298 (223.5 -> 16.8), colour 50 (40.4 -> 2.5), vector 1
  (0.1 -> 0.1). None grew. Every chart is one page.
- Checked per chart after writing: page count and every page size equal the original's to 0.01 pt (2,599 of 2,599).
  Deterministic: a second full run gave byte-identical files (2,598 compared; the 2,599th failed in run 1).
  Source folder fingerprint (shasum of every PDF) identical before and after.
- Failed / broken, flagged: the first run FAILED **00170001.PDF (Kamloops L.)**: a 96 x 69 in page is 268 M pixels at
  200 dpi, over Pillow's decompression-bomb guard. The guard is now raised to a stated 600 M pixels (our own render,
  never switched off); the rerun converted it (468 KB -> 428 KB). The **5 known broken files (88 bytes: 00352701,
  00363201, 00353501, 00353201, 00353401)** are NOT in the open table (sheet-layer names only), so not in this set; the
  tool reports any non-PDF as `failed` with a non-zero exit. "6 known broken" in the owner's note: only these 5 + the
  Kamloops failure were seen.
- Sounding check (crop of original | stored, 200 dpi of paper, each opened and read): `T634-shots/`
  `pass2-1-friendly-lake-small-bilevel.png`, `pass2-2-kamloops-lake-largest-page-bilevel.png`,
  `pass2-3-kidney-cottle-round-gray-worst-ratio.png` (99 % smaller), `pass2-4-canty-lake-largest-gray.png`,
  `pass2-5-dina-lakes-colour-worst-ratio.png` (".11.7"), `pass2-6-daisy-lake-small-colour.png` (".4" ".9"). Every
  sounding in the six crops reads the same in both.

### B. variant_circular goes to the owner
- The 69 already carried `verdict: review`; what was open was the bulk path (`accept --rule variant_circular` would
  have taken them whole). Now: `propose` writes a `review` list (ids per rule) and `counts_by_verdict`;
  `BULK_ACCEPTABLE = {clean}` and `accept --rule` refuses any other rule (one id at a time with `--ids`). Each row also
  carries `position_size_ok` as evidence only (68 of the 69 pass position + size).
- Regenerated `data/generated/bathymetry/proposals.json`: **clean 1,995; review 299** (two_named 16, unnamed 143,
  no_lake 17, name_disagrees 2, gazetted_only 8, **variant_circular 69**, no_outline 27, position_or_size 17) **+ 44
  sheets with no code = 343 on the review list**. No proposal changed rule or lake (2,294 compared with the first pass).
  Nothing accepted; `data/curated/bathymetry/matches.json` not created.

Not verified (second pass): only 6 charts were looked at by eye; the other 2,593 rely on the threshold the first pass
set on 20 samples. Faint pencil soundings on a grey scan lighter than grey 160 would vanish; the worst-ratio grey and
colour charts were the ones checked, not every faint one.
