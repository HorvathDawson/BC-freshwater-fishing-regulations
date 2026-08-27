# Added-streams DEM / resolver — handoff

Working doc for `pipeline/added_streams/`. Validated visually through `output/added_map_{burnaby,
squamish,port_moody}.html` (top-right toggle: **dem raw** teal = the DEM flow model; **resolved** =
the minted dataset; "colour dem by connected component" groups a basin). ALL sources are DEM-oriented
now (`RELIABLE_SOURCES = set()` — burnaby moved off trust_source once the fixes below made DEM correct).

## Run / validate (safe for the assistant — no LLM credits)
```bash
PYTHONPATH=. .venv/bin/python -m pytest pipeline/added_streams/tests -q       # 92 pass (~1 s)
PYTHONPATH=. .venv/bin/python -m pipeline.added_streams.mapcheck burnaby      # regen one map (~50 s)
```
Tests live IN the module (`pipeline/added_streams/tests/`), separate from the main pipeline suite —
a bare `pytest` (testpaths = pipeline/tests) does NOT collect them; run the explicit path above.
- gpkg: `data/bc_fisheries_data.gpkg`; elevation: `dem.ElevationSampler` (AWS Terrarium tiles, cached).
- **⛔ Never run the LLM parser** (`run_parse.sh`, `dispatch`) — spends the user's credits; human-only.
- `dem_flow` on burnaby is ~20–25 s (the FWA-overshoot clip + noding). mapcheck calls it twice/source.

## DEM flow model (`dem.py::dem_flow`) — the pipeline, in order
1. **`_clip_fwa_overshoot`** — a municipal line drawn a few m PAST the river it drains into is clipped at
   the crossing (Buena Vista 359 ended 5.8 m past the Brunette → mouth on the river, no doubling-back
   connector). STRtree-gated on endpoints so it stays fast. (skipped under trust_source)
2. **`_node_pieces`** — planarizes T-junctions: split a piece where another piece's endpoint lands on its
   INTERIOR (Kyle Creek 204.1 apex loop; a trib on a mainstem body). One feature can yield >1 reach, all
   tagged with the origin feat_idx. A split feature's `out[i]` gains a `reaches` list. (skipped under trust)
3. Node graph: coincident endpoints = nodes, wider joins = visible bridges. **loop-closers** are dropped
   when both ends are HEADWATERS (local elevation maxima) — that hid two real source markers and drew a
   false Y (Noble 174/175, Hutchinson 205/206). A genuine low-mouth-on-body link is still kept.
4. **Lake hub** (approved lakes only): a lake that touches/nears an FWA/tidal river gets that as its
   OUTFLOW (`lake_outflow_node`) and WINS sink-selection over a DEM noise pit (Still Creek → Burnaby Lake
   → Brunette). ALL stream mouths within 40 m of a lake shoreline drain into it. Deer-Lake-Brook dual
   attach: a reach can drain INTO lake B at its mouth and be lake A's outflow at its source.
5. **Multi-outlet BFS**: a river has MANY tributary mouths, so the flow BFS seeds from EVERY node that
   reaches the river (a terminus/local-low within `outlet_seed_tol`=60 m, or a lake outflow) + lakes. Each
   creek flows to its OWN nearest river contact (Rudolph/Ancient Grove → Brunette, not up into Trolley).
   A seed must be a LOCAL LOW (a mouth) — NOT a high headwater near a stray FWA reach (that reversed
   Squatters). Stranded sinks (mouth up to ~800 m from the river) fall back to the single-sink + outlet.
   `out[i]`: `{coords (mouth-first), comp, down (feat idx | None), outlet (FWA/tidal lonlat | None), reaches?}`

## Curation filters (`build_dataset.py`, general — all sources)
- **Short-tributary prune** (`_prune_short_leaf_tribs`, `_SHORT_TRIB_M = 50 m`): drop a stream that flows into
  ANOTHER added stream, is <= 50 m, and has nothing (kept) flowing into it. Iterated, so a short stream left
  with only pruned inflows becomes a leaf and goes too (Buena Vista Trib.3, 48 m). ONLY <= 50 m streams are
  ever dropped — a longer creek is never cascaded out. FWA/tidal-attached streams and mainstems are untouched.
- **Receiver-overshoot clip** (`_clip_receiver_overshoot`, `_OVERSHOOT_TOL = 40 m`): a municipal line drawn a
  few m PAST the river it drains into crossed the receiver and dangled beyond it, so the connector doubled
  BACK. Trim the mouth-side overshoot so the mouth lands ON the crossing (zero-gap confluence, no connector).

## Classification (`fwa_match.py::classify`)
- duplicate (drop, favour FWA) / extension (keep the novel tail) / novel (keep whole). A **differently-named**
  municipal creek that only PARTIALLY hugs an FWA line (covb in [ext, dup)) is kept as a novel, not dropped as
  a duplicate of the FWA it runs beside — Brackendale Creek hugs Dryden Creek ~40 %, 59 % unique. High (>=dup)
  coverage or a name match still classifies as duplicate.

## Resolver (`build_dataset.py::resolve_and_mint`, `_resolve_topology`)
- Derives receivers from the dem tree. **`dem_outlet` is checked BEFORE `dem_down`** — a channel that
  drains to a real river roots THERE, not on a spurious cross-channel `dem_down` cycle (Eagle Creek 154).
- **A trib joins the stream it TOUCHES** (`_touched_receiver_channel`): if the dem routed a mouth into a
  channel it does NOT touch (a connector) while the mouth sits ON another added channel (~0), prefer the
  touched one — Magnolia Trib 1 → Magnolia Creek, not the Little Stawamus its mouth got bridged to.
- **Novels the dem eliminated are dropped** (`_keep_novel`): a channel is minted only if the dem kept a
  member, so a NAMED braid/loop gone in dem-raw is gone in resolved too (Little Stawamus side channels).
- `_anchor` is **coastal-aware**: a 900- coastal FWA reach is open sea like the tidal boundary, so tidal
  wins over it (Dynamite/Heron → Burrard Inlet). A real inland route (100- Fraser) beats tidal (Sanctuary
  Slough → 100-). tidal is still the last resort when nothing else is within reach.

## Curation config
- `clean.py::_EXCLUDE_NAMES_BY_SOURCE` (drop by name) and `_EXCLUDE_SRC_IDS_BY_SOURCE` (drop one piece by
  src_id, incl. a single `.N` MLS part — checked AFTER the suffix). burnaby prunes: 344, 356.1, 279, 305,
  274, 268. squamish: 654/.1/.2, 608.5, 597.1/.2, 664, 658.1.
- `build_dataset.py`: `FWA_EXCLUDE_BY_SOURCE` (squamish adds 900-102882-190726), `RELIABLE_SOURCES=set()`,
  `APPROVED_LAKE_NAMES_BY_SOURCE` (burnaby: Deer + Burnaby Lake).

## RESOLVED (kept as regressions in `tests/test_build.py`)
- **Stoney Creek reversal** — `dem_mouth` picked which channel END is the mouth by nearest reach mouth over
  ALL features; a foreign trib whose mouth touched Stoney's HEADWATER flipped it. Now scoped to the channel's
  OWN reach mouths (`_mouth_end`, `own_mouths` via merge provenance). Stoney mints mouth-at-13.3 m, 3.5 m
  connector to the Brunette. `test_channel_mouth_endpoint_scoped_to_own_reaches`.
- **100- route-measure percentage** — our FWA extract is regional, so a big river's blue line is only
  partially loaded (`mouth_measure > 0`) and `length_m` understates it; proj/length_m over-counted (Sanctuary
  got 100-**567200**). Now `_blue_line_total` recovers the true length from the receiver's already-coded FWA
  children (median of route-measure / local-code) and mints `(mouth_measure+proj)/true_total`. Live: Sanctuary
  100-012900, Fraser River Trib.1 100-011200, Boundary 100-010400. `test_wsc_on_partially_loaded_river_...`.

## OPEN ISSUES
1. Residual: ~4 burnaby unresolved (one `Eagle Trib.3` sub-branch).

## Guardrails
- Prefix shell with `rtk`; `graphify query/explain` before grepping; `graphify update .` after code changes.
- Memory: `~/.claude/projects/.../memory/added-streams-*.md`.
