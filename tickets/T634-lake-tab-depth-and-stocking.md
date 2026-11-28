# T634 — Lake tab: depth charts and stocking, to the owner's decisions of 2026-11-11

Status: POSTPONED (deferred, owner 2026-11-13: "after the closed release we will do stocking and depth maps next"; the owner's 2026-11-20 answer marked it POSTPONED like T695/T699). Left (Claude) when it restarts: the path to ship in the ticket and item 6 (the app's Lake tab).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T634 | Lake tab: depth charts and stocking, to the owner's decisions (OGL, vector files, name variants, PDF compression) | POSTPONED (deferred, owner 2026-11-13: after the closed release; marked POSTPONED 2026-11-20 like T695/T699) | d55900aba, b8e65f2f0, a1d7da363 | owner 2026-11-11; restarts the data half of T200/T201/T218; NOT part of T495 (ships in a later release). First pass d55900aba; second pass b8e65f2f0 (PDFs 200 dpi 1-bit 748->201 … (detail: ticket file) |

Source: the depth and stocking audit (T608 D, page https://claude.ai/artifact/7Dn9FwZu3N1aMGxsCR4ea3) and the owner's answers 2026-11-11.

## Owner decisions
- Licence: stocking and lake bathymetry are Open Government Licence in the B.C. Data Catalogue; no permission requests.
  (The audit page had marked the placed contours and stocking "Access Only": re-read each record's licence field, correct
  the page, and show the owner any record that does not say OGL before building on it.)
- Fetch the 684 vector depth files (about 200 MB) to this Mac, outside git; build and test the reader and the placing. Nothing published yet.
- Lake information lives in a fourth "Lake" tab. SUPERSEDED 2026-11-27 by T1189's card tab Info (Runs, Stocked, Depth), on his "Yes this structure except ..." (2026-11-27).
- Scan-only lakes: the PDF only. Placing a scan over the map is a deferred future task.
- The PDFs are too big: compress to the smallest that keeps the smallest printed depth numbers readable; report the size saved.
- Chart matching A7 (name column only): use the name variants already curated. A name never picks a lake on its own: the lake
  must be in the same area (code, position, size), so one "Trout Lake" can never take another's chart. The owner recalls
  matching code already existed: find it and reuse or retire it, do not write a second matcher.
- A2 (code also covers unnamed ponds): bind only the named lake. Codes with two named lakes still go to the owner.
- Clean-match limits stay: 100 m, and half to double the area.
- Stocking match: lake code plus name (with curated variants), never name alone; a first-time lake goes to the owner. Read the
  ORIGINAL stocking match scripts first: they used the group code and may carry more information; reuse what is sound.

**Not part of the closed-testing goal (T495)** (owner 2026-11-11): the lake work (charts + stocking) ships in a LATER
release.

**Owner decision 2026-11-11 (third pass): build now on the three "Access Only" records** (Provincial Fish Stocking
Reports; the placed contours, 28 lakes; the sheet layer + the 510 vector zips): "i want you to do the work for them and
then I will find the website later for it". The owner OWES the licence link. Every artefact built from these three
carries `licence: "owner says OGL, catalogue says Access Only, unconfirmed"`. **Confirm the licence before any
production publish.**

## Done (three research passes; nothing shipped, app/ untouched)
- First pass d55900aba: licence per record (CKAN `package_show` 2026-11-10: OGL = Bathymetric Maps - Open (the PDFs +
  reference table) and Lake Inventory Surveys; Access Only = the sheet layer `WHSE_FISH.BATH_SURVEY_MAP_SHEETS_SVW` (the
  only place the vector zips are linked), the placed contours (28 lakes), Provincial Fish Stocking Reports, FIDQ, the
  Cariboo lakes). The audit page (artifact 7Dn9FwZu3N1aMGxsCR4ea3) was NOT edited.
- Old matchers: `pipeline/deliver/lake_facts.py` `bind`/`core` (code first, names confirm, `wbk:` keys) sound, reused;
  `pipeline/stocking/generate/match.py` + `identifiers.py` unsound, not reused — retire them when the feed is built. The
  v1 `wbid_overrides.json` (12 rows) and `bathymetry_overrides.json` (19) are not imported; they can be shown beside the review.
- Second pass b8e65f2f0 (owner: "Yes, 200 dpi B&W"): `pipeline/bathymetry/compress.py` → `~/data-offgit/bc-bathymetry-pdf-200dpi/`:
  **2,599 charts, 0 failed, 748.1 MB -> 201.4 MB (-73 %)**, deterministic, page sizes equal to 0.01 pt. The 5 broken files
  (88 bytes: 00352701, 00363201, 00353501, 00353201, 00353401) are not in the open table. variant_circular (69) goes to
  the owner: `BULK_ACCEPTABLE = {clean}`, `accept --rule` refuses any other rule.
- Third pass a1d7da363: `licence: "owner says OGL, catalogue says Access Only, unconfirmed"` on every derived row; the
  49 layer-only ids take the normal rules.
- Proposals (nothing accepted; `data/curated/bathymetry/matches.json` not created):
  - Charts (`python -m pipeline.bathymetry.propose` → `data/generated/bathymetry/proposals.json`): **clean 1,995; review 299**
    (two_named 16, unnamed 143, no_lake 17, name_disagrees 2, gazetted_only 8, variant_circular 69, no_outline 27,
    position_or_size 17) **+ 44 sheets with no code = 343 on the review list**.
  - Stocking (`python -m pipeline.stocking.fetch_fidq` → `~/data-offgit/bc-stocking-fidq/`, 64,991 release rows, 2,318 FIDQ
    waterbodies; `python -m pipeline.stocking.propose` → `data/generated/stocking/proposals.json`): **clean 1,751**,
    unnamed 521, no_lake 32, two_named 9, name_disagrees 4, variant_circular 1; all `first_time`. Accept:
    `python -m pipeline.bathymetry.accept --set stocking` (the one writer of `data/curated/stocking/matches.json`).
  - Contours (`~/data-offgit/bc-bathymetry-contours/contours.geojson`; `python -m pipeline.bathymetry.contours` →
    `data/generated/bathymetry/contour_proposals.json`): **clean 25**, outline_disagrees 3 (Frog Lakes 00631FROG 0.72,
    Tacheeda Lakes 0.63, Yellowhead Lake 0.71); `LAKE_MAX_DEPTH_M` / `LAKE_MEAN_DEPTH_M` filled on all 28; a `99999`
    polygon is the "deepest area", not a depth. Accept: `accept --set contours`.
  - Vectors (`~/data-offgit/bc-bathymetry-vectors/`, 510 zips, reader `pipeline/bathymetry/e00.py` + `e00.decompress`;
    `python -m pipeline.bathymetry.place_more` → `placing_all.json`): **strict 298** (IoU >= 0.9, median <= 25 m, area
    0.8-1.25), **loose 117**, **poor 83**, no_shore 6, no_candidate 6 (00095CAMB, 00402GATA, 00426SANJ, 01446MAHD,
    01267FIRE, 00099LARL), duplicate 2; a mirror beats the true fit 13 times. Units: metres 396, feet 4 (00015001, 00095501, 00215801, 00282501), **not stated 110**.
- Shots: `T634-shots/` (1-5 first pass, pass2-1..6 soundings, pass3-1..6 placings).

Not verified: only 6 charts looked at by eye (faint pencil soundings lighter than grey 160 would vanish); the 109 FIDQ
`no_block` groups were not checked one by one; FIDQ streams were not fetched (lakes only); the stocking `unnamed` 521 were
not looked at; contour proposals were not drawn; only 6 of 498 placings were looked at; the not-stated units were not
ticked against the PDFs; FIDQ alias lists split on commas only (some use ";").

### Path to ship (later release; not T495)
1. **Licence** (owner, S): the link or written word for the three Access Only records; until then nothing derived
   from them is published.
2. **Chart PDFs** (S): the 2,599 200-dpi files (201 MB) to R2 `bathymetry/` (staging by Claude, prod by the owner).
3. **Owner review of chart matches** (owner hours; a small review page would make it M): bulk-accept clean 1,995,
   then the 343-row review list (69 variant_circular, 143 unnamed, 44 no-code, …) by id.
4. **Stocking** (M): owner bulk-accepts clean 1,751, reviews 567; retire `stocking/generate/`; a weekly fetch run by
   the feeds worker or by hand (no GitHub cron), first_time lakes to the owner.
5. **Bundle + lakes/1** (M): writers for the waiting `chart`, `stock_water`, `stock_code` tables; max/mean depth from
   the 25 contour lakes and the chart statistics.
6. **Lake tab in the app** (item 6, L; now the Info tab of T1189): PDF viewer for scan-only lakes, stocking history, depth facts.
7. **Placed vector depths on the map** (L, optional, after 6): owner ticks the 415 strict+loose placings beside the
   PDFs and the 110 unstated units, then a tile layer of placed contours; the 83 poor + 12 failed stay PDF-only.

### Next after closed testing (owner 2026-11-13)
Owner, 2026-11-13: "after the closed release we will do stocking and depth maps next." This ticket's "Path to ship"
runs under **T200** (stocking, first) then **T201** (depth maps, second); each has the feasibility note's first step,
matcher and owner questions: `docs/research/stocking.md`, `docs/research/bathymetry.md`.

Related: T200, T201, T218, T219 (postponed 2026-11-05; this ticket restarts the data half on the owner's word).
Depends: none. Size: L.

## Owner answer 2026-11-20

Owner 2026-11-20 (audit question 8, answered "Couldn't we just assume most of these are done already?"): POSTPONED like T695 and T699, the owner's own deferral of 2026-11-13 stands. T200/T201 keep their own step lists.

Status line before this answer: OPEN — three research passes done, nothing shipped, app untouched: first d55900aba (licence per record, vector reader/placing, PDF trial, chart->lake proposals), second b8e65f2f0 (PDFs 200 dpi 1-bit, 748 -> 201 MB), third a1d7da363 (Access Only records built on the owner's word, licence flag; stocking 1,751 clean, contours 25/28, vectors 298 strict); left (Claude): the path to ship in the ticket and item 6 (the app's Lake tab), after closed testing

## Ledger detail (moved out of the ledger row, T540, 2026-11-22)
owner 2026-11-11; restarts the data half of T200/T201/T218; NOT part of T495 (ships in a later release). First pass d55900aba; second pass b8e65f2f0 (PDFs 200 dpi 1-bit 748->201 MB, variant_circular to review); third pass (Access Only records built on the owner's word, licence flag, confirm before prod: stocking 1,751 clean, contours 25/28, vectors 298 strict) + path to ship in the ticket; item 6 (app) waits
