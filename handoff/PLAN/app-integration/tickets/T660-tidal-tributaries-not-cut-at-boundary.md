# T660 — Tidal: streams entering a removed tidal mainstem are not cut at the tidal boundary

Status: PARTIAL — 12358ece6, cb41409bb, 7a9a796d0; left (Claude): reach rerun on full_next, binding diff, restamp/promote (the coordinator's build-27 chain)

Source: owner 2026-11-11 on the T632 tidal sheets (3 and 4 good, but): "it is weird how all the streams that go into
the removed mainstem dont also get collapsed to the boundary (2 furthers points of land or what every the tidal
boundary for unmarked ones generic method is...)".

## Seen on the sheets (`handoff/PLAN/app-integration/T632-shots/`, B15–B18, B20, B21)
Below the Skeena tidal limit the mainstem is removed (red), but the tributaries and the many straight connector
lines that meet it across the estuary stay in the place (blue), drawn over tidal water. Same at the Nass, Chambers
Creek and Ksi Gingolx panels.

## How tidal water is decided today (measured 2026-11-11)
- **Curated limits** `data/curated/waters/tidal_limits.json` (T326), read only by the atlas (`atlas/splits/tidal.py`):
  1,126 `mouth`, 33 `split`, 10 `whole`, 0 `cut`. The atlas writes `tidal.resolved.json`.
- **Reach step** (`reach/outside.py::tidal_water`, called by `build_reaches`): tidal = the rows marked tidal (Nitinat
  Lake) + `downstream_of` every `split`/`cut` limit on the items that carry it + every section of a `whole` water. Taken
  out of every binding after the tributary walk (`_without_tidal`), and since T632 out of every DFO place
  (`reach/dfo_salmon.py::resolve_all`). Written as `tidal.jsonl` (101 sections on the T632 run).
- **Tiles** (`deliver/tiles/tidal.py`): the same pieces are not drawn; the `tidal_water` layer is the DFO polygon
  (`tidal_boundary` in the gpkg) plus those pieces. The bundle proves tiles and run agree (`bundle/undrawn.py`).
- **The generic rule for an unmarked mouth**: a `mouth` row means "the water meets tidal water at its mouth: nothing
  is tidal, nothing is cut" (an alias at the FWA line's lower end). A stream with no row is not tidal anywhere. The
  proposals behind the rows came from `tools/audit/tidal_candidates.py` ("where the line leaves the DFO polygon going
  upstream"). The reach run never reads the polygon.
- **Why the creeks stayed**: FWA draws each creek on from its shore straight across the river polygon or estuary to the
  river's line (edge type 1450, named `fwa_edge_types.WATER_CROSSING` here). The creek's mouth section runs from its first
  confluence on land, across the water, to the Skeena line. Its FWA mouth (where the `mouth` row puts the boundary) is
  in mid-estuary. Nothing cut it at the shore.

## Measured, per place (the 21 T632 places, T632 run)
Kept length inside the DFO tidal polygon / sections touching it / sections joining a removed tidal piece:
B03 Region 1: 340.9 km / 5,205 / 73 · B04 Nitinat: 3.3 km / 2 / 1 · B05–B07 Somass: 0.2 km / 1 / 1 · B08 Coquitlam:
10 m / 1 / 1 · B09 Areas 28–29: 74.9 km / 439 / 141 · B10 Kanaka 24 m · B11 Little Campbell 16 m · B12 Nicomekl 14 m ·
B13/B14 Squamish: 4.3 km / 3 / 1 · B15 Region 6: 534.8 km / 7,710 / 49 · B16/B17 Skeena B(ii)/B: 84.8 km / 39 / 39 ·
B18 Nass C: 51.2 km / 20 / 7 · B19 Haida Gwaii D: 114.3 km / 2,485 / 3 · B20/B21 Skeena mainstem: 17 m / 1 / 1 ·
B01, B02: 0.
Province-wide, outside the run's tidal water: 17,575 sections touch the polygon, 1,366.9 km inside it, every one bound by
at least one rule (zone rules). Of that, crossing lines (1450) are 384.8 km; the rest is streams' own lines (1000:
583 km), river construction lines (1250: 230 km) and side channels. Inland the same 1450 lines are everywhere (329,452
lines, 53,877 km; 476 km in the polygon), so the rule cannot be "1450 is tidal".

## The rule
**A stream entering tidal water ends at its own shore: the line FWA draws across the tidal water is tidal.**
1. Atlas (`atlas/splits/shore.py`, new sub-phase `build.cut_shores` after the length cap): on every blue line whose mouth
   piece starts with a run of crossing lines lying at least 50 % inside the DFO tidal polygon (sampled every 10 m) and
   continues above it, cut at the top of the run (the exact FWA segment boundary where the creek's own line begins).
   Id `shore:<blk>:<m>`, source `shore` (minted, like `length:`), label "shore". Every run touching the polygon is
   listed in `shore_cuts.json`, cut or not and why. Needs an ATLAS REBUILD.
2. Reach (`reach/outside.py`, committed): `crossing_pieces` = pieces at or below a shore cut, plus any piece whose every
   line is a crossing. `tidal_crossing_owner` makes one tidal (owner `crossing:<section>`) exactly when every outlet is
   off-map tidal water (a tidal piece, a whole tidal water, or another such crossing). A crossing into a river the
   curated limit keeps freshwater stays freshwater (owner list). Rule diagnostics count them (`crossings: n`). Tiles take
   the same set. No regulation value changes; only where rules apply.
On the current atlas the committed reach change moves nothing (10 crossing pieces exist, 0 end in tidal water: checked
on `data/generated/atlas/full`).

## Before / after (simulated: the patch's cuts made by the real sectionizer on an in-memory copy of the live graph,
then the committed reach rule against the T632 run's tidal water)
629 shore cuts; 163 crossings become tidal (120.7 km); 436 stay freshwater (79.3 km, into 191 rivers the limits keep
fresh); 32 runs touching the polygon not cut (25 under 50 %, 5 whole-line crossings, 2 slivers).
Sections / metres after T632 -> after T660:
B03 107,909 -> 108,066 / 87,402,475 -> 87,400,767 (14 crossings, 1.7 km) · B09 38,015 -> 38,037 / 39,014,409 ->
38,984,951 (107, 29.5 km) · B15 612,940 -> 613,098 / 432,440,243 -> 432,350,690 (42, 89.6 km) · B16 15,649 -> 15,649 /
15,115,841 -> 15,031,500 (38, 84.3 km) · B17 87,260 -> 87,260 / 89,106,408 -> 89,022,067 (38, 84.3 km) · B18 40,930 ->
40,939 / 38,960,939 -> 38,955,727 (4, 5.2 km) · B04, B13, B14 +1 section, B19 +13 (freshwater crossings now their own
section), same metres · others unchanged. Two more DFO places lose one crossing each: `2:mclennan-creek:whole`,
`2:silverdale-creek:whole` (their creeks keep the part above the shore). Rules: 223 rules (83 entries) bind a cut mouth
section, so their section ids change; 154 of them lose a crossing to tidal water.
Maps (`T660-shots/`, before | after, T632 style): `skeena-estuary`, `chambers-creek`, `fraser-lower`,
`fraser-new-westminster`, `fraser-mission`, `fraser-close`, `alberni`, `campbell-river`, `nitinat-narrows`.

## Blue left over tidal water after the change (explained, owner calls in `T660-owner-list.json`)
- Crossings into a river whose curated limit keeps it freshwater there (Nass above its limit, Bear, Kemano, Kitimat, …):
  `fresh_receivers`, 191 rivers.
- A limit's own water above its curated limit inside the polygon (Squamish 3.4 km, Nitinat 2.9 km, Somass 0.2 km, Skeena
  17 m): the owner placed those limits.
- Streams' own lines (not crossings) inside the polygon on a `mouth` limit: Haida Gwaii beaches (B19, 114 km), the
  Campbell and Somass marsh creeks.
- The polygon over land: Barnston Island (Fraser).

## Final run (Claude, after T637 lands; nothing promoted here)
```
git apply handoff/PLAN/app-integration/T660-pending/shore-cuts.patch
PYTHONPYCACHEPREFIX=$(mktemp -d) rtk proxy .venv/bin/python -m pytest -q -p no:cacheprovider pipeline/tests/test_shore_cuts.py pipeline/tests/test_tidal_cut.py pipeline/tests/test_sectionizer.py pipeline/tests/test_length_splits.py pipeline/tests/test_smoke_coverage.py
git add pipeline/atlas/splits/shore.py pipeline/tests/test_shore_cuts.py && git commit -- pipeline/atlas/build.py pipeline/atlas/sidecars.py pipeline/atlas/splits/sectionizer.py pipeline/atlas/splits/shore.py pipeline/deliver/bundle/spans.py pipeline/deliver/bundle/topology.py pipeline/docs/MAP.md pipeline/tests/smoke.txt pipeline/tests/test_shore_cuts.py
scripts/heavy.sh t660-atlas 50 -- .venv/bin/python -m pipeline build --atlas
scripts/heavy.sh t660-reach 50 -- .venv/bin/python -u -m pipeline.atlas.reach.cli --build data/generated/atlas/full_next --out <scratch>/reach660 --against <T637 reach_after>
.venv/bin/python -m pipeline.atlas.reach.binding_diff <T637 reach_after> <scratch>/reach660 --json <scratch>/diff660.json
```
Expect (on the 2026-11-10 atlas; T637's changes may move these a little): atlas log `shore cuts: ~629 crossing(s) …
~629 new section(s); ~32 run(s) … not cut`; reach `tidal` list a row `crossings into tidal water ~163`; `dfo tidal` still
21 places, more sections; 8 DFO places change (table above); about 223 rules change section ids, 154 lose a crossing; no
rule newly unresolved `tidal`, no DFO place drowned (not verified: stop and look if either appears).

## Not verified
- No atlas build was run with the patch: the stage ran end to end only on synthetic lines (`test_shore_cuts.py`), and
  the numbers above come from a simulation (all crossing fids' measures, not every fid; split pieces keep their parent's
  member fids, so order/magnitude were not repartitioned there).
- That 1450 is FWA's "connector across water" by definition: inferred from geometry (the Skeena creeks), not from the
  FWA code table.
- Whether any rule ends up bound to nothing, or any DFO place drowned, after the real run.
- The tiles and bundle on a rebuilt atlas (the shore cut's split name "shore" in the bundle, `spans`/`topology` treat
  `shore:` as minted).

## Leftovers (no ticket made)
- Owner: the 191 `fresh_receivers` (move a river's tidal limit, or keep it); add the rule to RULINGS.md once accepted.
- A `mouth` row's "tidal boundary" alias still sits at the FWA mouth (mid-estuary), not on the shore cut.
- The 25 runs under 50 % in the polygon, the 5 whole-line crossings and the 2 slivers stay as they are.

## Build 27 (2026-11-11): the shore stage stopped, fixed, rebuilt
- The patch (cb41409bb) stopped the full atlas build at `cut_shores`: the braid sweep wanted 403 braid cuts. Reproduced
  in memory on the live graph with the same numbers (not a T637 effect; the simulation had skipped the sweep). Cause:
  97 shores are SIDE CHANNELS crossing back into their own river (Fraser, Kemano, Bella Coola, San Juan, Zeballos …), so
  the river is a braid of the shore's own water; the sweep would cut the river across its width. None of them is one of
  the crossings that end in tidal water.
- Fix (7a9a796d0, the length cap's precedent): `shore_cuts` asks `braid_sweep.spans_a_braid`; a spanned shore is not cut
  and is listed `not_cut: "a braid of its own water spans the shore"` (stays whole and freshwater, never widened). The
  sweep must then find nothing to cut (`length_cut_failures`), junction re-homes are applied, residual straddles stop
  the build, and `shore_cuts.json` + `braid_sweep_shore.json` are written before any refusal. Pinned in
  `test_shore_cuts.py` (a braided side channel; mutation: the stage refuses).
- Full rebuild (`b27-atlas`, 28 min, rc 0, COMPLETE.json): `shore cuts: 532 … -> 532 new section(s); 129 run(s)
  touching it not cut, 97 of them a braid of its own water spans; sweep cases {refused: another water 24, refused: one
  side 4, rehomed: junction <10 m 2}, 4 edge(s) re-homed, 0 network(s) straddle`; sliver gate clean.
- Against the simulation: 629 -> 532 cuts (the 97 spanned shores); not cut 32 -> 129 (same 32 + 97). Tidal crossings,
  computed in memory with the same stage on the live graph: 163 -> 161 (120.7 -> 120.0 km). The two lost
  (`356235964:0`, `356341234:0`, Fraser delta, 706 m) are the braid sweep's geometry re-home (`rehome_host_edges`):
  FWA's confluence point with the Fraser channel lies at the creek's shore, so the outlet edge moved to the creek above
  the shore and the crossing piece has no outlet; it stays freshwater (listed in `braid_sweep_shore.json`
  `edges_rehomed_by_geometry`). Expect about 161 in the reach run's "crossings into tidal water" row.

