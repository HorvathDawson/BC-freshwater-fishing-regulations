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
