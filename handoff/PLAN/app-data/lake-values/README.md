# Lake modes' values: Stocked and Depth from the release (2026-10-21)

Owner rule: stocked-lake colouring and the depth (surveyed) colouring go through the ONE colouring pathway, and their
colours update from data alone — no tile change. Before this, the style's `stocked` / `surveyed` modes read
feature-state `stocked` / `survey` and nothing set them (colour-latency README, "Lake modes").

## What ships

`pipeline/deliver/lake_facts.py` (`python -m pipeline.deliver lake_facts`) writes `lake_facts.bin` beside the bundle,
pinned to its `section_handles`; the release copies it as the file **`lakes`** (format `lakes/1`, kind bin, size gate
100 KB). Per lake SECTION: survey (none / sheet / digitised) and the last stocked day (days since 1900-01-01), plus
the roster's `asof` day. `deliver all` now runs `lake_facts` before `release` and passes it through.

Sources, all `data/source/`:

| value | source | in repo? |
|---|---|---|
| surveyed (sheet) | `wsa_bathymetry_maps.csv` + `bc_bathymetry_sheets.json` (both sheet rosters, already fetched) | yes |
| digitised | `vectorized_flag = Y` in `bc_bathymetry_sheets.json` | yes |
| last stocked | FFSBC's stocking report endpoint, `https://www.gofishbc.com/wp-json/ffsbc-stocking/v1/releases?...&per_page=all` (the request its "export CSV" makes), fetched by `python -m pipeline.stocking.fetch` into `data/source/stocking/ffsbc_releases.json` (gitignored; the file records `url`, `fetched`, `generated_at`) | NO — a fetch away (one request, 71,072 rows, ~18 MB, fetched 2026-10-21) |

Not used: BC Data Catalogue "Provincial Fish Stocking Reports" (WHSE_FISH.FISH_RELEASES_PUBLIC_VW) is a BCGW
custom-download view only (no WFS, no file; licence "Access Only"), and FIDQ's stocking page is an interactive form.
OWNER QUESTION: the FFSBC roster carries no stated open licence (gofishbc.com copyright); the app shows only a derived
recency band per lake, but confirm that use is fine before shipping.

The join (both sources): the 50K waterbody identifier = FWA lakes `WATERBODY_KEY_GROUP_CODE_50K` → lakes under the
code (`lake:<WATERBODY_KEY>` in the handle table). When a source name agrees with one of the FWA's three gazetted names
(generic word dropped, "(formerly)" honoured) it binds the lakes whose names agree plus the code's unnamed parts —
never a differently-named neighbour sharing the code (review 2026-10-21: the first cut bound every lake under the code,
so BUNTZEN coloured McCombe Lake, MARA LAKE Rosemond Lake, MEADOW LAKE Pigeon Lake, CARP Lho Lakes, LNRS Intata Reach;
`test_built_from_the_live_inputs_the_hand_checked_lakes_hold` pins three of them). When the FWA names none it binds
every lake under the code; otherwise refused. Live counts (per identifier, unchanged by the fix):

| | verified | FWA unnamed (bound) | name disagrees (refused) | no polygon |
|---|---|---|---|---|
| survey sheets | 2,122 | 143 | 12 | 17 |
| FFSBC waters | 1,684 | 518 | 12 | 415 (292 are FFSBC's own pond ids) |

The survey verdicts equal `data/generated/bathymetry/audit.json`'s exactly (2,122 / 143 / 12 / 17). 3,519 lake
sections carry a fact (3,523 before the review fix); 15,417 B raw.

## Size

After the review fix, against the current live bundle / store / status index / export (the release tests' inputs;
their fixture now passes `lake_facts.bin`): scratch `data/generated/scratch_lakes/release2/r-89afadd26893` totals
**13,677,305 B gz** with `lakes` = **8,962 B gz** (gate 14,000,000; 322,695 left). Before the fix (figures below, kept
for the record) it was 8,967 B gz.

Scratch release `data/generated/scratch_lakes/release/r-bef55d6dbc94` (worktree, gitignored), built by
`release.build` from the live bundle, regs store, status index and export: **byte-identical to the live
r-1bb06fa28daf in every file**, plus `lakes` = **8,967 B gz**. Total 13,711,156 → **13,720,123 B gz** (gate 14,000,000;
279,877 left).

## App

`@app/core` `decodeLakeFacts` / `stockRecency` / `dayNumber`; `@app/data` `ReleaseLoader.lakes()` and
`RegsSource.lakeFacts()` (null for the bundle, the fixture, a release without the file); `@app/ui` `useLakeColour` (proven
pair + per-tile renderer only); `@app/map` two engine channels, `survey` (label = sheet / digitised) and `stocked`
(label = the `stock_recency` band for today; a new day relabels), handed in as `MapProps.lakes` and given to the engine
through `toChannel` like status and standing. The values ARE the labels: no style change, no paint call. Handed over in
every lake mode, so a mode switch stays one paint change.

## Evidence

- `colouring.test.ts` (engine, call by call): both channels label loaded lakes and paint nothing; a new day relabels
  only the stocked bands; a new release clears, restores and relabels (status put back); withdrawn = cleared.
- `one-engine.render.test.tsx` "A NEW RELEASE ALONE recolours the lakes": the real `Map.web.tsx` over a recording GL —
  feature-state only, no paint / layout / source / query call, the same map instance.
- (Built before the review's binding fix; the view below holds none of the over-bound lakes.) Live, expo web on :39732 + scratch data server :39731 (both stopped), Map tab at (−120.28, 50.56) z11, 1280×800:
  release r-bef55d6dbc94 → rendered lakes carry `stocked` (26: season 3, year 14, decade 1, old 8) and `survey` (17:
  sheet 13, digitised 4), release files fetched core, lakes, paint; the SAME tiles with r-1bb06fa28daf → 0 lakes with
  either key (only `p`). Shots: `with-lakes-stocked.png`, `with-lakes-depth.png`, `without-lakes-*.png`; numbers in
  `with-lakes.json`, `without-lakes.json`.
- Mutation runs (each failed the named tests, then reverted): drop Map.web's survey effect → the house check and the
  render test fail; the stocked channel without `relabels` → "A NEW DAY" fails; accept a disagreeing name → the binding
  and join tests fail; skip the release's handle check → "another atlas" fails.

## Not done

- Native (N2): the phone map takes pushed data only; its lakes stay plain in Stocked / Depth.
- A release swap installs the two lake channels in turn: two clears and relabels of the loaded tiles, in one commit.
- Search's `stocked` signal and the lake sheet's `lakeInfo` / `stockingHistory` are still empty in the release reader.
- The FFSBC roster has no scheduled refresh: stocking colours move only with a new release.
