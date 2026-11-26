# T201 — bathymetry: feed the prepared Depth mode and draw contours

Status: OPEN — NEXT AFTER CLOSED TESTING (second, after T200). Was POSTPONED (deferred, owner 2026-11-05).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T201 | bathymetry: feed the prepared Depth mode, depth charts in the Lake tab; NEXT AFTER CLOSED TESTING, second (plan: docs/research/bathymetry.md; reuse bathymetry/propose.py, PDFs first, vector contours later) | OPEN | — | owner 2026-11-13 (same words); starts once Play closed testing is running; owner: licence link, NONNA, CHS/Okanagan, emails, review lanes, modelled depth (questions in the … (detail: ticket file) |

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

## Owner

- [ ] 2026-11-25 — Picks (defaults first): depth v1 = the PDF card plus a tint / add max and mean depth (needs the licence) / contours; the look: chart card plus tint.
