# Mouth / tidal ends of DFO places vs each stream's tidal boundary (audit, 2026-11-15)

Owner ask (2026-11-15, verbatim): "also we want to make sure all the mouth/ tidal splits are at the correct one (not
the proposed one but at the tidal ones we give every stream)".

Read-only. Nothing applied. These are candidates for the owner, not verdicts. What a wording means is the owner's call.
Tags: **[ran]** = checked by running a script on the files; **[read]** = read in code or data; **[inferred]** = my reading.

## Counts

| | |
|---|---|
| DFO places in the queue (9 region files) | 352 (258 active, 94 dormant) |
| places whose words or extent have an end at the mouth / tidal boundary / the sea | **25** |
| ...the extent in force uses the stream's tidal boundary | **17** (Dean and Kitsault are not in the live reach run yet) |
| ...wholly tidal places (`tidal_water`, bind nothing; the boundary is their upper end) | 2 |
| ...use a different point | **1** (Pallant Creek, 56.6 m above the boundary) |
| ...no extent yet (the proposal decides) | 5 (Nass x2, Seymour, Illiance, Wilauks) |
| ...stream has no tidal boundary | 0 |
| places whose words name a freshwater confluence, a lake bay or an area, not the sea (left out, listed below) | 41 |
| places with an open seaward end and no mouth words ("downstream of X", "whole") | 65 places / 83 line ends. **Every one starts exactly at the tidal boundary** [ran] |
| places in the live reach run that hold any tidal section | **0 of 352** [ran] |
| proposals that put a NEW cut on top of an existing tidal boundary | **6** (Chapman, Stave x2: 0.7-3.6 m; Illiance, Kitsault, Wilauks: at the line's end) |
| curated cuts within 500 m of a tidal boundary | 5, none a duplicate (closest 56.6 m) |
| owner's pending DFO cut proposals within 500 m of a tidal boundary | 0 of 37 |

**In short:** no place in force ends at a wrong mouth point. The risk is in the **proposals**. The split hunt suggests
new cuts where a tidal boundary already exists. Accepting them adds duplicate cuts, and three of them sit on the line's
very end.

## Where the tidal boundary lives [read]

- `data/curated/waters/tidal_limits.json`: one row per ocean-entering blue line (1,170 rows). There are 33 `split`
  rows and 1 `cut` row (rank 1). Their authority is in `tidal_split_authority.json`: 23 `dfo`, 10 `owner`, 0 `none`.
  There are also 16 `whole` rows (whole tidal waters, Fraser channels) and 1,120 `mouth` rows. The `mouth` rows were
  auto-generated on 2026-10-28 and carry no authority (T678).
- The atlas build applies the owner's order of 2026-11-11 (T678, "coast/2"):
  1. a DFO-written or owner split;
  2. otherwise the coast crossing (`data/generated/atlas/full/shore_cuts.json`: 170 lines cut at `cut_m`; the rest
     "the coast crossing is the line's mouth");
  3. the polygon, for map shading only.
- Output: `tidal_sections.json`, then the reach run's `tidal.jsonl` (282 sections).
- A place's mouth end is not a named cut. "Downstream of X" leaves the end open to the line's bottom. The reach run
  then takes the tidal sections out of every DFO place (T632, `pipeline/atlas/reach/dfo_salmon.py:25-30,110-111`;
  `tidal_calls.json` `keep_tidal` is empty). So an open end lands on the tidal boundary by construction. An end only
  goes wrong if it is a cut ABOVE the boundary, which leaves a freshwater gap.
- The registry also gives each `mouth` line a boundary `<item>__mouth` (kind outlet) at m 0.
- The "2 allowed tidal waits" are Caycuse River (354151091) and Dempster Creek (360848786), at
  `pipeline/regs/review/apply.py:407-408`. Neither has a tidal row or a coast cut, and no DFO place is on either [ran].

## The 25 places

m = metres up the FWA line from its mouth. "Boundary" = the stream's tidal boundary as built (atlas
`data/generated/atlas/full`, 2026-11-13). "Proposal" = `data/generated/review/dfo-suggestions.json`.

| place (status) | words (short) | end in force | boundary (id, tier, m, lon/lat) | proposal's end | verdict | move needs |
|---|---|---|---|---|---|---|
| 1:campbell-river:from-a-fishing-boundary-sign-at-th (active) | Maple St down to cement block sign | `campbell_river__cement_block_boundary_sign` m 1630.7 | same, rank 1 owner, -125.26205 50.03759 | none | uses the boundary | none |
| 1:cowichan-river:from-the-white-triangle-boundary-s (active) | downstream to the tidal boundary | open; sections start at m 0 on the 3 sea-entering channels [ran] | coast crossing at each line's end (nothing tidal); no DFO-written Cowichan boundary is curated | none | uses the boundary. Flag: DFO's "tidal boundary" is not located anywhere; owner may know signs | none unless owner places one (new tidal split = atlas rebuild) |
| 1:nanaimo-river: cedar-road-bridge-upstream-approx (dormant), from-the-upstream-side-of-the-ceda (active), from-the-cedar-road-bridge-crossin (active) | from Cedar Road Bridge up | `nanaimo_river__cedar_road_bridge` m 2979.5 | same, rank 1 dfo, -123.88045 49.11625 | same cut | uses the boundary | none |
| 1:nanaimo-river:in-subarea-17-14-tidal-portions-of (+ ~2) (active) | tidal portions from Cedar Bridge down | wholly tidal (`tidal_water`), binds nothing | Cedar Road Bridge is its upper end | ~2: new cut 400 m below the bridge, inside tidal water | tidal place; proposal moot | none |
| 1:qualicum-river:waters-below-the-qfn-boundary-fenc (active) | QFN fence down to Hwy 19A bridge | `qualicum_river__hwy_19a_bridge` m 108.3 | same, rank 1 owner, -124.6118 49.39774 | none | uses the boundary | none |
| 1:somass-river: from-the-tidal-boundary-at-the-pap (dormant), somass-river-from-the-tidal-bounda (dormant), from-the-tidal-boundary-signs-at-t (active) | from the tidal boundary at Paper Mill Dam | `somass_river__tidal_boundary_at_papermill_dam` m 5520.9 | same, rank 1 dfo, -124.8538 49.27476 | none | uses the boundary | none |
| 2:chapman-creek:upstream-of-tidal-water-fishing-bo (active) | upstream of tidal signs below Hwy 101 bridge | `chapman_creek__tidal_boundary` m 180.5 | same, rank 1 dfo, -123.722071 49.440626 | **NEW cut `chapman_hwy101` m 176.9 (3.6 m off)** | uses the boundary; **proposal duplicates it** | none. Do not accept the proposal (a duplicate cut = atlas rebuild for nothing) |
| 2:seymour-river:from-the-seymour-river-dam-to-the (dormant) | dam to the tidal boundary | no extent | `seymour_river__cn_railway_bridge_tidal_boundary` m 211.9, rank 1 dfo, -123.024501 49.303573 | open end (lands on the boundary); its upper end is Seymour Lake, not the dam | no extent | owner re-mark, then reach-only rerun |
| 2:stave-river: downstream-of-b-c-hydro-dam-to-the (dormant), ~2 (active) | dam to the CPR Railway Bridge | `stave_river__cpr_bridge_tidal_boundary` m 384.7 | same, rank 1 dfo, -122.423613 49.17198 | **NEW cut `stave_cpr_bridge` m 384.0 (0.7 m off)** | uses the boundary; **proposal duplicates it** | none. Do not accept the proposal |
| 2:fraser-river:mainstem-waters-upstream-of-the-cp (dormant), ~2 (active) | upstream of CPR Bridge at Mission | `fraser_river__the_cpr_bridge_at_mission` m 76506.3 | same, rank 1 dfo, -122.30047 49.12606 (channels below are `whole`) | same cut | uses the boundary | none |
| 5b:dean-river:from-the-confluence-with-crag-cree (active) | Crag Creek down to the tidal boundary | open end below the canyon signs | coast crossing at m 0 (joins sea), -126.96861 52.80523; alias `dean_river__tidal_boundary` | `dean_river__mouth` (same point, 0 m) | uses the boundary. Not in the live reach run (extent added after 2026-11-13) | reach-only rerun |
| 6:nass-river:mainstem-waters-downstream-of-the (dormant), ~2 (active) | downstream of (signs near) Meziadin confluence | no extent | `nass_river__tidal_boundary_dfo_subarea_3_12` m 1805.2, rank 1 owner, -129.951193 54.979542 | downstream of the Meziadin confluence; open end lands on the boundary | no extent | owner re-mark, then reach-only rerun |
| 6:pallant-creek:upstream-of-the-bridge-located-nea (active) | upstream of the bridge near the mouth | `pallant_creek__bridge_near_mouth` m 56.6, -132.03302 53.05241 | coast crossing at m 0 (joins sea), -132.03248 53.05209; alias `pallant_creek__tidal_boundary` | none | **different point: 56.6 m above the boundary**. The words name the bridge, so the extent follows the words; the 0-56.6 m stretch is fresh water in no place. Owner's call | if the owner wants it to reach the boundary: re-mark (whole / no lower cut), then reach-only rerun |
| 6:illiance-river:upstream-of-white-triangular-fishi (active) | upstream of signs near the mouth | no extent | FWA mouth m 0 on 360867921, -129.461319 55.4752 (`illiance_river__mouth`; auto row, the coast cut never looked at this line) | **NEW cut at m 0, the line's end (0 m off)** | no extent; **proposal = a new cut on the boundary, at a line end** | owner mark "whole" (or open end): reach-only rerun. Accepting the new cut = atlas rebuild and a cut on a line's end (known to break both halves) |
| 6:kitsault-river:upstream-of-white-triangular-fishi (active) | upstream of signs near the mouth | `whole` (curated) | FWA mouth m 0 on 360881469, -129.48135 55.47998 (`kitsault_river__mouth`) | NEW cut at m 0 (stale; written before the extent) | uses the boundary. Not in the live reach run yet | reach-only rerun; ignore the proposal |
| 6:wilauks-creek:upstream-of-white-triangular-fishi (active) | upstream of signs near the mouth | no extent | FWA mouth m 0 on 360886670, -129.46232 55.47826 (`wilauks_creek__mouth`) | **NEW cut at m 0, the line's end** | no extent; same as Illiance | same as Illiance |
| 6:kincolith-river:upstream-of-white-triangular-fishi (active) | upstream of signs at the Kincolith River Bridge | `ksi_gingolx__kincolith_river_bridge` m 1562.7 | same, rank 1 dfo, -129.9601 54.9967 | none | uses the boundary | none |

Flag for the owner, not decided: no mouth-worded place sits on a stream whose DFO boundary is upstream of the
geographic mouth. Pallant, Illiance, Kitsault and Wilauks all have their boundary at m 0. "Signs near the mouth" with no
known sign position is read here as the whole river. That is the owner's call.

### Open seaward ends with no mouth words [ran]

65 places reach an ocean-entering line through an open end ("downstream of X", "whole"). That is 83 line ends: 56 at a
coast crossing at m 0, 25 at a rank-1 split, 2 at a coast cut. Every one starts exactly at the boundary in
`data/generated/reaches/full/dfo_salmon.jsonl.gz` (2026-11-13 21:36). The other 34 line ends that stop higher up are
lower ends the words name upstream: Skeena reaches, Nitinat, Somass above Collins Farm, Campbell from Maple Street (the
cement-block place covers the 290 m below), and so on. None of them is a mouth end.

### Left out: the words mean a freshwater confluence, a lake bay or an area (41)

- Freshwater confluences / mouths of tributaries (35):
  - Campbell River: Quinsam x2 (their sea end is open and starts at the boundary).
  - Nanaimo River: Boulder Creek mouth x2.
  - Nitinat River: Little Nitinat.
  - Puntledge River: Morrison Creek.
  - Stamp River: Beaver Creek mouth x2.
  - Chehalis Hatchery to the Harrison; Harrison River to the Fraser x2; Inch Creek to Nicomen Slough.
  - Chilliwack River: Slesse Creek x4.
  - Squamish River: Cheakamus (the sea end is open, at `squamish_river__tidal_boundary` m 13.7).
  - Fraser River at the Seton (R3); Dean River above Crag Creek.
  - Ecstall River: Johnston Creek. Gitnadoix River and Lakelse River x2: both run into the Skeena.
  - Skeena River: Lakelse, Kitwanga, Kitsumkalum, Babine, Kispiox and the CNR bridge reaches, x8.
  - Babine River: Nilkitkwa x2. Nechako River: the Fraser x2.
- A tidal boundary named only as a reference point (1): Somass River, "0.5 km upstream from the tidal boundary signs".
- A lake (2): Babine Lake, Pinkut Creek mouth x2.
- Area rules (3): Region 6 "all streams flowing into tidal water", Areas 5, 6 and 3-6.

## Cuts near a tidal boundary (item 4) [ran]

These are curated cuts within 500 m along the same line of a boundary. None is a duplicate; each is its own named
feature:

| cut | distance above the boundary |
|---|---|
| `pallant_creek__bridge_near_mouth` | 56.6 m |
| `hays_creek__lower_culvert_near_cannery` | 134.8 m |
| `nanaimo_river__hwy_19_bridge` | 256.7 m |
| `campbell_river__maple_street_boundary_sign` | 289.8 m |
| `keogh_river__lower_fish_counting_fence` | 346.1 m |

The owner's pending cut proposals from marks:
- None of the 37 DFO ones with a position is within 500 m.
- The 1,171 Tidal-tab proposals are the boundaries themselves. Ksi Gingolx is 3.9 m off its curated cut: the same
  bridge.

## Does the suggester prefer the tidal boundary? (item 5) [read]

No, not by design. It gets there through the open end and the reach subtraction.
- `pipeline/regs/dfo_salmon/suggest.py` never reads `tidal_limits.json`.
- Word matching against every registry cut runs first (`suggest.py:481-500`). A phrase like "the tidal boundary" lands
  on a rank-1 split only if that split's label happens to share the words. On the Dean it matched `dean_river__mouth`
  (low).
- Only after that does the mouth rule run (`suggest.py:80`, `502-508`). It returns an open end (`679-691`), which is
  right because of T632.
- When "only the mouth is named", the matcher gives up (`689-691`).
- The split hunt then replaces the matcher's answer (`1368-1371`). `_hunt_new` (`924-985`) snaps the hand-found
  lat/lon and always makes a NEW split. It never checks for an existing cut or tidal boundary at that point. That is
  where the Chapman, Stave and Alice Arm duplicates come from (`data/generated/review/dfo-split-hunt.json`, 2026-11-09).

Fix proposal (not done; regenerating suggestions spends no credits):
1. In `resolve`, before the word match: if `MOUTH_RE` matches and no other landmark is named, look up the line's
   tidal-limit row. A `split` or `cut` row returns that split (high). Otherwise return the open "mouth" end.
2. In `_hunt_new` / `apply_hunt`: if a curated cut or tidal split sits within about 25 m on the same blk, return
   `{"kind": "split", "split_id": <it>}`, not a new split.
3. Never propose a new cut at m 0 or at the line's top. Use `whole` or an open end instead.
4. Rerun `python -m pipeline.regs.dfo_salmon.suggest`, then pin it with a test on Chapman, Stave and Illiance.

Review app end picker [read]:
- It offers the water's registry boundaries that have a point: `curation-review/backend/dfo.py:595-639`,
  `DfoReview.tsx:315`, `ExtentFixEditor.tsx:250-253`.
- Rank-1 tidal splits are offered under their own label.
- Coast cuts are offered, but labelled only "shore".
- `<item>__mouth` is not offered, because it has no resolved point (`dfo.py:590-591`).
- To end a place at the tidal boundary, leave the end open ("downstream of X") or pick the rank-1 split. Small proposal:
  label the coast cuts "tidal boundary (coast)".

## What a move needs (item 6)

| place | the owner re-marks | reach-only rerun | atlas rebuild |
|---|---|---|---|
| Pallant Creek | only if he wants it to reach the boundary | yes, after the re-mark | no |
| Illiance, Wilauks | yes (no extent yet) | yes | only if he accepts the new m 0 cut (not advised: an end cut) |
| Nass x2, Seymour | yes (no extent yet) | yes | no |
| Dean, Kitsault | no (already curated) | yes, to show up | no |
| Chapman, Stave x2 | no; do not accept the stale proposal | no | no |

Inputs read: places `data/curated/dfo_salmon/places/region-*.json` (region-6 saved 11:12 today, so this is a snapshot);
atlas `data/generated/atlas/full` (COMPLETE 2026-11-13 20:38); reach `data/generated/reaches/full`; suggestions file
(written 2026-11-14 23:41). Scripts are throwaway, in the session scratchpad.
