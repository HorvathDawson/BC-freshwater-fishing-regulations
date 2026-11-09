# T523 — A cut's id keeps its upstream/downstream (and inlet/outlet) words

Status: DONE 229c464de (code), 1d58354b3 (data: 4 ids renamed, Quatse mark line diff-proven, 332 DFO rows unchanged; Squamish on OSM 5L32)

Owner 2026-11-09 (review app): `quatse_river__quatse_river_hatchery_weir_100_m` / `…_100_m_2` "should say up and
downstream instead".

## Cause
`pipeline/common/split_ids.base` slugs the cut's name to 32 characters, so the tail that tells two cuts apart is cut
off and the second one becomes `_2`. Labels are right ("(100 m upstream)" / "(100 m downstream)"). Two pairs today:
- `quatse_river__quatse_river_hatchery_weir_100_m` (downstream) / `…_2` (upstream)
- `stamp_river__robertson_creek_hatchery_lagoon` (outlet) / `…_2` (inlet)

## What
- An offset cut's id ends in the existing hand convention `_u<m>m` / `_d<m>m` (owner: "could do _u and _d") (as `puntledge_river__…_u100m`);
  the name part is not truncated mid-word into a clash. A clash suffix `_2` stays only for truly identical names.
- Atlas tidal ids use the same `base` (T326): new ids it mints must not change for existing tidal cuts — prove with
  a before/after id list.
- Rename the 4 ids above in `data/curated/waters/cuts.json` and every curated reference
  (`dfo_salmon/places/region-1.json`, …). `marks.jsonl` is never edited: the owner's verified 11-09 Quatse mark (extent
  fix points by split id) must stay verified, not orphaned/stale — carry it the way renamed splits are carried
  (`regs/review/carry.py` `renamed_splits`, `curation-review/backend/dfo.py`), and prove orphan/stale counts unchanged.
- Data change lands after the running T517 rebuild, never mid-build.

## Also: Squamish "powerline crossing at 9 mile" sits too far upstream (owner 11-09)
`squamish_river__powerline_crossing_at_9_mile` is anchored as the Cheakamus confluence + 1,500 m upstream (DFO's
"approximately 1.5 km"), which lands a little above the real crossing. OSM + Esri imagery: two 500 kV lines (5L30
49.786527,-123.186332; 5L32 49.785773,-123.185859) cross the Squamish under one cleared corridor. Fix: re-anchor as a
point on the DOWNSTREAM line (5L32), since the DFO area is "downstream of the powerline crossing"; keep the id and
label; note the source. Applied with the T523 data change, after the T517 rebuild.
