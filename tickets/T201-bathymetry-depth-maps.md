# T201 — bathymetry: feed the prepared Depth mode and draw contours

Status: OPEN — the vector depth maps are wanted IN 0.10.0 (owner 2026-11-28, below); the gate is the lake-matching checks (T1188, his look). Was: start soon, next after the re-cut work (2026-11-26); before that, next after closed testing.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T201 | bathymetry: feed the prepared Depth mode, depth charts in the Lake tab; NEXT AFTER CLOSED TESTING, second (plan: docs/research/bathymetry.md; reuse bathymetry/propose.py, PDFs first, vector contours later) | OPEN — vector depth maps in 0.10.0 (owner 2026-11-28); gate: the lake-matching checks (T1188) | — | the vector depth lines on the map for the lakes whose placing he passed; which lakes is to ask; where the permission is written; the depth v1 pick |

Source: owner 2026-10-25 ("at the end, alongside the stocked and bathy maps"); decisions.md "OWNER 2026-10-21 — lake
Stocked / Depth modes: prepared, not fed yet"; T158 (tiles carry no contour layer — a follow-up of its own).

## Next after closed testing (owner 2026-11-13)
Owner, 2026-11-13, verbatim: "after the closed release we will do stocking and depth maps next. so add that to next after
closed tickets." Starts once Play closed testing is running, after T200. Carried with T634 (items 1-3, 5-6). T218 and T219
stay postponed behind it.
Feasibility note: [`docs/research/bathymetry.md`](../docs/research/bathymetry.md).

**The note supersedes the old Work (pick a source, contours first) and main's name-first matcher** (name picks the nearest same-named lake, no
distance cap, nobody reviews; 252 titles shared by 587 codes). Here the chart's lake code picks; name, position and
size only confirm. Contours in tiles come last, not first (then: tile-size budget, T64/T85 numbers; drawn per T158's
no-access-vs-contour rules; tests + size diff; owner look).
- **First step:** do not write a matcher. Rank `pipeline/bathymetry/propose.py` output into classes (A 1,739 code+name
  exact, B 256 weaker, C 343 owner-review rows); the owner bulk-ticks A/B after a spot check and reviews C into one
  frozen file; then the 2,599 200-dpi PDFs (201 MB) to staging R2, then the Lake tab "Depth chart" card (option 2 + 5).
- **Matcher:** code -> FWA `WATERBODY_KEY_GROUP_CODE_50K`; point-in-polygon/IoU against the sheet outline instead of box
  centre; sheets an ordered list on one row. Sources with no code (NONNA, Hakai, OBWB) match by footprint, class C.
- **Data shapes:** `data/curated/bathymetry/matches.json` (`bathy-matches/1`, keyed by chart id -> `wbk:` item, `trust`,
  `evidence`, `sheets`); bundle `chart` table (+ `sheet_no, bytes, sha256, units, licence, source`; 40-60 KB); `lakes/1`
  survey state under 10 KB; PDFs on R2 `bathymetry/<sha256-12>.pdf`, never bundled; later a separate `depth/1` PMTiles
  (25 lakes under 1 MB; about 415 placed lakes 10-30 MB, estimate).
- **Sizes:** PDF card M (plus owner review hours); vector contours L for about 400 lakes, XL to trace scans; Okanagan
  Lake has no open vector (6 scans only).
- **Owner questions (in the note):** (1) licence link for the three Access Only records; (2) NONNA for Harrison/Pitt
  (derivative contours with CHS notice); (3) Okanagan/Kootenay/Shuswap: CHS licences, trace scans, or PDF-only; (4)
  emails to OBWB, Hakai, UNBC; (5) bulk-tick classes A/B after a sample of 50?; (6) modelled depth (GLOBathy) at all.
  Defaults to build with: ask for the link, no NONNA, PDF-only, no emails sent, bulk-tick with sample, no modelled depth.

## Plan 2026-11-25 night
See T200's section of the same date (one plan for both; notes `tickets/archive/night-notes-2026-11-25.tar.gz` `stocking-depth-plan.md`): the
bathymetric PDFs are OGL-BC and can ship now; the first slice is a "Depth chart" card with the official PDF plus a
tint on the map; contours later (placed contours are still "Access Only"); the Lake tab is built once, with T200.

## 2026-11-26: licence granted, start soon, the matching checks first
Owner, verbatim: "we have the license permission for this all vector lake maps. so that is not an issue. also the
bathy map pdfs we would convert to iamges if shipping with app to compress them best. we still dont want to do this
yet becausde it will involve a lot of checks on the matching. update the ticket and set it to start soon."
- The licence for the Province's vector lake maps is GRANTED (his word): the "Access Only" gap of the study is closed.
  Where it is written is open (below), so the credits page can cite it.
- The chart PDFs ship as compressed IMAGES, not PDFs.
- Start soon, next after the re-cut work; the gate is the matching checks (his eye on each placing stays).
- The study (planning chat notes `bathymetry-pdf-to-vector`): 2,673 of 2,685 chart PDFs on the Mac are scanned
  pictures, 10 are true vector; about 400 lakes need no tracing (the Province's 2001 vector files: 298 tight fits,
  117 loose, 83 poor; and the 28-lake contour layer, T218); 86 PDFs come from FIDQ, not the open table: not covered
  by OGL until checked. Corrections: depth does NOT ship today, and the `lakes/1` slot has no producer
  (`deliver release` ships one only when handed `--lake-facts`). Order: the chart card, the 28-lake layer, the ~300
  tight-fit vector lakes, then a 3-lake tracing pilot; no bulk tracing.
- A sample page of the placings exists, private to him. His comment, verbatim: "It seems like a lot of the poor ones
  could be reoriented to match better? Not all but definitely some. Then others like elk lake one need to be
  reoriented then it also matches 2 lakes. This could be considered for actually implementing them?" So a fitter
  step comes BEFORE any lake ships: (1) poor and loose placings retried with a wider search (every rotation from
  several starts, the mirror image, a little scale freedom where the printed scale is off); (2) a sheet of two lakes
  (Beaver and Elk) fitted to the union of the neighbouring lake outlines, its contours then split per lake;
  (3) a chart on the wrong lake (Stake, fitted to an unnamed lake) searched against nearby lakes by shape; (4) his
  eye-check per placing. A read-only refit trial on the 83 poor and 117 loose placings is running (his "Start it now").

## 2026-11-28: the vector depth maps in 0.10.0
Owner, 2026-11-28 01:05 MST, verbatim: "what is current state what is everything left before 0.10 now (we want the vector maps in for it".
Reading (the planning chat's): the Province's vector depth lines are drawn on the map in 0.10.0 for the lakes whose placing he
has passed (T1188's review page); which lakes (the `high` class only, or `high` plus those he marks Right) is not yet asked.

## Owner

- [ ] 2026-11-25 — Picks (defaults first): depth v1 = the PDF card plus a tint / add max and mean depth (needs the licence) / contours; the look: chart card plus tint.
- [x] 2026-11-26 — The licence for the Province's vector lake maps, and when → "we have the license permission for this all vector lake maps. so that is not an issue. ... update the ticket and set it to start soon." (2026-11-26; full words above)
- [ ] 2026-11-26 — Where the permission for the vector lake maps is written, so the credits page can cite it. You said "I will find it later" (2026-11-26).
- [x] 2026-11-26 — A refit trial of the poor and loose placings → "Start it now" (2026-11-26)
- [ ] 2026-11-28 — The vector depth lines in 0.10.0: which lakes get them, the `high` class only or `high` plus every lake you mark Right on the review page (T1188)? Not yet asked; nothing ships without your look at each placing.

## 2026-11-26: the fitter work is T1188
The owner's reorientation idea and "no stretch except small ones" are T1188 (wider rigid search, two-lake fit, flagged small stretches); T201 keeps the order, the licence and the ship decision.
