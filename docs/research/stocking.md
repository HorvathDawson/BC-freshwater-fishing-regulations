# Stocked lakes: what is built, what the research found

Stocking is not in the app (open tickets T200, T634). The matching code is a prototype kept on purpose and fed to no
build stage (`pipeline/stocking/README.md`). The old app scraped the Province's FIDQ stocking pages and matched a
lake by name nearest to a point with no distance limit; the owner's rule now is that the lake code picks the lake and
names only confirm.

## What is built

- `pipeline/stocking/fetch_fidq.py`: the FIDQ bulk preview per watershed group, lakes only, to
  `~/data-offgit/bc-stocking-fidq/`.
- `pipeline/stocking/propose.py`: code picks, name confirms, `wbk:` stored, proposals to
  `GENERATED.base/stocking/proposals.json`; accepted with `python -m pipeline.bathymetry.accept --set stocking`.
- `pipeline/stocking/generate/identifiers.py` (group code to lake nodes) and `match.py` (registry item per stocked
  waterbody; not sound under the owner's rules, retire it when the feed is built).
- Tests: `pipeline/tests/test_stocking_match.py` (synthetic rows); `test_doc_drift.py` keeps the README rows real.
- The app has `stock_water` / `stock_code` in `pipeline/deliver/bundle/schema.sql` (the app's prepared query reads
  `stock_water`) and `app/packages/core/src/lakeFacts.ts`; nothing writes them. To feed it: a fetch step in
  `data/fetch_data.py`, accepted matches in `data/curated/stocking/` (`config.yaml` `curated.stocking.matches` is null),
  a bundle writer, lake values in `lakes/1` (`pipeline/deliver/lake_facts.py`).

## Sources

| Source | Carries | Licence |
|---|---|---|
| FIDQ stocking pages (`WHSE_FISH.FISH_RELEASES_PUBLIC_VW` front end) | `wbody_id`, WBID, 50K watershed code, name, dates, species, strain, brood year, hatchery, stage, counts; no coordinates | "Access Only" (the owner's "OGL" has no link yet) |
| Known BC Fish Observations (`WHSE_FISH.FISS_FISH_OBSRVTN_PNT_SP`, `SOURCE LIKE 'Releases Database%'`) | 74,879 stocking-release points with coordinate, WBID, FWA watershed code, `WBODY_ID` (= FIDQ id); 2,317 of the 2,318 stocked FIDQ lakes; no counts, strain or weight | OGL |
| GoFishBC (FFSBC) `/wp-json/ffsbc-stocking/v1/releases` | the most current report; counts, sizes; a Cloudflare challenge met a scripted request | all rights reserved; needs FFSBC's consent |
| FWA lakes, manmade waterbodies | `WATERBODY_KEY` (our `wbk:` id) and `WATERBODY_KEY_GROUP_CODE_50K` = the WBID | OGL |
| FWA 20K-50K waterbody cross-reference | WBID to waterbody key, primary and secondary matches; custom download only | OGL |

The Province's sources stopped at 2026-06-29 when read, about 4.5 months behind GoFishBC. A WBID can sit on several FWA
polygons (a lake in pieces, ponds under a lake's code). FISS and FWA can be copied and served with the OGL credit line;
FIDQ counts and strains and anything from GoFishBC wait on the licence answer.

## Matching, measured on the 2,318 stocked FIDQ lakes

Lake code first, then the open FISS point (inside the polygon or within 100 m; median 18 m outside), then the name as a
check. The point is a check on the code, not a picker.

| Class | Rule | Lakes |
|---|---|---|
| A | code to one named lake, name agrees, point inside or within 100 m | 1,674 (72.2 %) |
| B | code to one unnamed lake, point agrees | 490 (21.1 %) |
| C | name and code agree but the point is in another polygon (28) or over 100 m away (49) | 77 |
| D | the code covers 2 or more named lakes (Buntzen, Mara, Moffat, Illusion) | 9 |
| E | no lake for the code | 32 |
| F | other (name disagrees, circular variants, unnamed code with 2 to 3 polygons, no point) | 17 |

A plus B is 2,164 auto-match candidates (93.4 %; 95 % of lakes stocked since 2020); C to F is 154 for the owner. The 77
class-C rows were not looked at on a map, and the 100 m limit was carried over from charts, not tuned.

Hard cases: the name never picks; a lake split into parts matches the lake's item; several FIDQ ids on one lake all
attach; a hatchery pond the owner marks `refused: not_public` stays refused across refreshes; a first-time id in a
refresh goes to the proposals, never guessed; **streams** (657 waterbodies, 8,333 rows) are a place (watershed code to
blue line key plus the point's measure), not a whole water.

## Shapes and the next step

- A curated `data/curated/stocking/matches.json` (`stocking-matches/1`, keyed by FIDQ waterbody id) and one
  release-independent feed `feeds/stocking/index.json` (`stocking-feed/1`, registered in
  `pipeline/common/feed_formats.py`, keyed by item id, copying the `inseason-links/4` publisher pattern). The app shows a
  Stocking card in the Lake tab (shared with depth charts) with its as-of date, and the Stocked colouring switches on.
- Sizes: matching and review M, streams as places M, the feed publisher M, the app card M (the Lake tab itself L).
- First step: add the FISS point and the A to F classes to `propose`, regenerate, show the owner the 154 review rows.
  No credits, nothing published.
- Risks: licence (nothing from FIDQ counts publishes until the link arrives); a 4-month lag against GoFishBC's
  near-live report; FIDQ is a scrape, so a broken fetch must fail the run, not publish an empty feed; FISS to FIDQ row
  equivalence is assumed (66,487 against 64,991 rows); 109 FIDQ groups returned no data block.

## Open owner questions

The OGL link (or ask the Province, and FFSBC for GoFishBC); an open-only first version from FISS (date, species, stage)
or wait; class B bulk-accept with a sample or tick each; streams in scope or lakes only; history depth (5 years or all);
whether a 4-month lag is acceptable.
