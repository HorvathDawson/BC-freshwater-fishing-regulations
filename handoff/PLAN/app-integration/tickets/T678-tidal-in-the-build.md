# T678 — Tidal in the build: one decision, made once, read everywhere

Status: IN PROGRESS (coast cut in the build + one tidal record: commits addbfd819, 26873ad75; candidate atlas built, not promoted; progress and what is left: `handoff/PLAN/app-integration/T678-progress.md`)

Source: owner 2026-11-11: "We need to make sure all the tidal logic and stream removal happens in the build of the
graph etc if possible. Or where other tidal logic is we want to avoid it being a weird post processing step." And:
"Also the cuts used for tidal should happen where cuts happen etc."

Measured on the live atlas `data/generated/atlas/full` and reach run `data/generated/reaches/full` (build of
r-483b295de57c, T665 promoted). Throwaway measuring script in the session scratchpad, not in the repo.

## Owner answers and rulings (2026-11-11) - current, read this first

**Cut rule, final order of sources (the owner's edits do not outrank the default).** Proposed rank 2 was "your curated
split or polygon edit on that river"; owner, verbatim: "No to this unless the curated splits are the DFO ones. Like
all the splits added in region 1 and 2 especially." So the rule is:
1. a DFO written boundary (sign, bridge, written line), which includes curated split rows that encode one (most of
   the Region 1 and 2 splits);
2. otherwise coast crossing: where the line enters the sea or a tidal river (mouth-line prototype in the session
   scratchpad `t665/mouthline/`; numbers in its `tables.md`, FWA inputs it needs: line ends, edge type 1450 water
   crossings, coastline/water polygons);
3. the DFO subarea polygon for map shading only.
Polygon hand edits (122; 73 touch a line; the candidate honours 22) and non-DFO curated splits carry no authority over
the cut. Honouring them is no longer a build defect to fix. The 1,126 "at the mouth" rows in tidal_limits.json
(2026-10-28) were auto-generated, not owner decisions, and carry no authority either. The owner's own work is 33 split
rows, 10 whole-water rows and 122 polygon hand edits. The held lists (`shore.py:69-230`) are to be re-derived from
where the new method fails, not carried over.

**A - fresh stub at a line's top:** a fresh run under 25 m at the top counts as tidal. Ewen Slough (356355111) and
356242813 become wholly tidal. Use the same 25 m as the in-line gap bridge (replace the 5 m at `shore.py:645`).

**B - lines wholly in tidal water:** keep them in the graph and mark them tidal in the build. Do not delete.
Downstream readers take the flag; none works it out again.

**C - five held lines with a build-27 cut and nothing tidal below** (354087703, 354154029, 356341234, 360445710,
360861076): still open; goes on the held-rivers page (to be rebuilt around the lines the new method leaves open).

**The tidal Fraser.** Verbatim: "We still want all channels and stuff of Fraser below it's tidal boundary as tidal".
Every channel and arm of the Fraser below the Fraser's own tidal boundary is tidal, for its length (it is not cut at
its own mouth). Tributary creeks that join the tidal Fraser (e.g. McLennan Creek, Chester Creek) keep their own
boundary at their mouth unless the owner says otherwise. **Correction, same day:** "Sloughs aren't channels. Sloughs
could be really non tidal?" Sloughs are NOT covered by the rule; each slough (and each unclear unnamed line, e.g.
Katzie Slough off the Pitt) is decided one by one by the owner, with nothing pre-filled. Build rule, decided in the
atlas where tidal is marked; no post-processing list. Fraser lines below Mission (13 once suggested tidal): the owner
ticks each on the held-rivers page.

**San Juan River (owner ruling):** the tidal boundary is the San Juan Bridge, by DFO's wording, which the owner
quoted: "In that portion of Subarea 20-2 at the mouth of the San Juan River north of the beach camp boundary marker at
the large alder tree, located at 48°33.514' N and 124°24.272' W and south of the Pacheedaht Campground and a boundary
marker located at 48°33.599' N and 124°24.233' W, at the well-established stump in the sand, shoreward in a easterly
direction of this boundary and downstream of the San Juan Bridge". Source:
https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/tidal-maree/a-s20-eng.html. This is a rank-1 DFO written boundary. **ACCEPTED by the owner on 2026-11-11** ("No just accept that bridge please"): the curated split goes in as accepted, the owner's explicit decision with DFO's wording as the source. No review step.

**Kitimat:** reading still open. DFO's line Eurocan dock - MK Bay kiosk is only the seaward edge; where tidal water
ends upstream is not written anywhere found. Owner picks: keep polygon exit / boundary at DFO line / ask the Kitimat
DFO office.

## To do from the answers
- [ ] San Juan: add a curated split on the San Juan main stem at the San Juan Bridge, tidal below, ACCEPTED (owner
  2026-11-11; no review step). The held San Juan line is a side line about 2.5 km from the markers: work
  out whether it joins below the bridge (then tidal) or above.
- [ ] Classify the 33 curated splits as DFO-written or not, from their provenance and the DFO pages. Unknowns go to
  the owner.
- [ ] Cut in the atlas build with the new order (plan step 10).
- [ ] DFO tidal pages re-scrape (below).

## DFO tidal pages re-scrape (ready)
The re-scrape of the live pac.dfo-mpo.gc.ca pages is ready in the session scratchpad `t665/rescrape/` (read
`DIFF-TABLES.md`: old 2026-10-28 copy vs live 2026-11-11, per page and per block). Not moved into the repo yet. To do:
- move `merged/` to `data/source/dfo_tidal_areas/2026-11-11/`;
- run `extract` then `propose`;
- fix the extractor so the 8 missed river texts and Nanaimo Cedar Bridge come through;
- check the Tidal review does not reopen waters already decided.

## Short answer
Mostly in the build already. The **cuts** are made in the atlas build with the same cutter as every other cut. But
**which water is tidal** is not stored by the build. The atlas decides it (`shore.polygon_rule`), writes only the
inputs (three sidecars), and then the reach run works it out again. The tiles work it out a third time. The three
answers match today (checked below) only because they share code. Lines "removed" by the polygon rule stay in the graph.
Two later stages (reach and tiles) leave them out.

## The map

| # | Place | Stage | What it decides | In the build or after |
|---|---|---|---|---|
| 1 | `atlas/build.py:727-800` `prune_graph` (braid-loop prune, leaf prune) | atlas graph | Drops unnamed braid loops and headwater leaves from the graph, before any cut. Leaf prune keeps WSC `9…` "tidal" channels over 500 m (`leaf_prune.py:65-68,144`): this means coastal drainage. It is not a tidal decision | In the build. This is the one place lines leave the graph |
| 2 | `atlas/build.py:821-834` + `splits/tidal.py` `cut_defs` | atlas cuts | Curated limit `cut` rows become ordinary `SplitDef`s appended to splits.json's list. `split` rows reuse a splits.json cut | In the build, same cut path as curated and gauge cuts |
| 3 | `atlas/build.py:927-948` `alias_tidal_mouths`; `1595-1597` `tidal.add_mouth_boundaries` | atlas cuts, registry | `mouth` rows (1,126) become an alias on the standing boundary, or a registry boundary `<item>__mouth`. No section is made | In the build |
| 4 | `atlas/build.py:1275-1344` `cut_shores` → `splits/shore.py` `polygon_rule` | atlas cuts (own stage, after the length cap) | T665: polygon-exit cuts (226), lines tidal whole (rule A, 215 lines), and which pieces below a cut go tidal (`ok` fixpoint, `shore.py:656-690`). Uses the same `place_points` / `split_graph_at` / braid sweep as the length cap. Ids `shore:<blk>:<m>` land in `splits.resolved.json` (`source: shore`, 226) | In the build. But the held lists and the 30 kept measures are Python constants (`shore.py:69-230`), not curated data |
| 5 | `atlas/build.py:1300` `water_registry(areas=False)` + `shore.reach_tidal` (`shore.py:459`) | atlas, mid-cut | Builds a throwaway registry before the shore cuts so it can ask the reach's functions what is tidal | In the build, but it is a reach-shaped answer computed inside the atlas. `build.py:1598-1613` stops the build if the final registry disagrees |
| 6 | `atlas/build.py:1657-1660`, `1317-1322` | atlas sidecars | Writes `tidal.resolved.json`, `shore_cuts.json`, `inside_tidal.json`. These are the INPUTS to tidal, not the tidal sections | In the build |
| 7 | `atlas/sidecars.py:205-256` `--tidal-only` | one-off tool on a built atlas | Rewrites `tidal.resolved.json` from the curated limits after the build. It refuses only when the shore decisions would move | **After the fact** (a post-build rewrite) |
| 8 | `atlas/reach/outside.py:334-367` `tidal_water` (called `reach/build.py:134`) | reach | THE tidal set (`tidal.jsonl`, 548 sections): catalogue rows marked `tidal` (Nitinat), piece below each split (79), whole waters (21), inside lines (224), crossings (223). Recomputes the cut pieces and the crossing fixpoint (`outside.py:392-433`) from the graph | **Recomputed after the build.** Only the catalogue `tidal` row is a true reach-stage fact |
| 9 | `deliver/tiles/tidal.py:37-60` `tidal_pieces` (`tiles/__main__.py:97-107`; `export.py:223,618`) | tiles | Which sections are not drawn. Calls the reach functions again on the atlas | **Recomputed a third time.** It does not read `tidal.jsonl` |
| 10 | `deliver/bundle/undrawn.py:47-62` | bundle | Proves tiles' undrawn == bundle's off-map tidal | Check only. Reads both |
| 11 | `deliver/bundle/derived.py:273-339`, `status_index.py:143-226`, `answers` | bundle, deliver | Read the bundle `tidal` table (from `tidal.jsonl`) | Readers. Good |
| 12 | `deliver/store/build.py:150,312` | store | `tidal` on each answers key. A section with no rule set has key 0, so an unnamed tidal section cannot be told from one outside B.C. (T672 1b) | Reader, but it loses the fact |
| 13 | `atlas/reach/dfo_salmon.py:25-30,110` | reach | Subtracts the run's tidal set from each DFO place, unless `tidal_calls.json` says not to | Reader of #8. Good |
| 14 | `curation-review/backend/reuse.py:318`, `tidal.py:266` | review app | Recomputes `tidal_cut_owner` and reads the polygon | Recomputed (dev tool) |
| 15 | `gauges/consume/tidal.py` | gauges | Whether a gauge's LEVEL moves with the tide (HYDAT remarks + `tidal_stations.json`) | Different fact (stations, not water). Leave it |
| 16 | `splits/pfma_drainage.py`, `registry/build.py:475` | registry | Which DFO tidal Area a stream drains into (DFO section E scopes) | Different fact. Leave it |
| 17 | `app/packages/data/src/regs/reads.ts:378,390`, `core/regs/edge.ts`, `core/tidal.ts` | app | Reads `akey.tidal` and `xpart.province_except` "tidal". The sheet words are a copy of the stored note, pinned by conformance | Reads stored facts. No recomputation. Feeds worker: only the DFO text pattern `tidal_boundary` (`dfo/untangle.ts:14`), not water |

Sources of tidal truth today: the DFO polygon (#4), the curated limits (#2, #3), FWA edge type 1450 (#8:
`fwa_edge_types.is_water_crossing_only` in `crossing_pieces`; adds nothing on the live atlas, since all 223 crossing
pieces sit on shore-cut lines), and catalogue rows marked `tidal` (#8, Nitinat). The atlas and the reach agree on
the live build: 226 cuts, 221 with a tidal piece below by both readings, and the 5 others are held lines that keep
their build-27 cut.

## Already meets the owner's rule
- Every tidal **cut** is made in the atlas build, through the one cutter (`sectionizer.place_points`,
  `split_graph_at`, braid sweep), with the same id shape and the same `splits.resolved.json` record as curated, gauge,
  area and length cuts. Curated tidal cuts even ride in on the curated list.
- Lines dropped from the graph are dropped in one place (`prune_graph`), before any cut.
- Bundle, status index, store, salmon subtraction and the app read a stored fact. None of them decides tidal.

## Defects
1. **Tidal sections are not a build output.** The atlas decides them and the reach and tiles recompute them (#8, #9,
   #14). Three computations agree today only because they import the same functions.
2. **Shore cuts are their own stage, after the length cap.** The length cap is meant to be the last cut, used only
   where nothing else reached (`build.py:1205-1215`). Today it also cuts water that the shore stage then makes tidal.
   The shore stage also has its own record and sweep file (`shore_cuts.json`, `braid_sweep_shore.json`).
3. **Held lines and kept cut measures are code, not curated data** (`shore.py:69-230`: 93 blks, 30 measures). A tick
   from the owner is a code edit, and `atlas.inputs` does not track it as an input.
4. **The atlas builds a throwaway registry to ask the reach** (#5). The atlas depends on reach code, and the tidal set
   is computed twice inside one build.
5. **"Removed" lines are not removed.** Rule A lines stay in the graph on purpose, so ids hold and tributary walks run
   (`shore.py:13-15`). Removal happens later: the reach takes them out of bindings, and the tiles do not draw them.
6. **Fresh slivers at a line's top** (question 2). Ewen Slough (356355111): top 1247.08 m, polygon exit 1224.41 m, so
   22.7 m stays fresh above the cut. Line 356242813: top 1248.72 m, exit 1229.34 m, so 19.4 m stays fresh. The cause
   is a threshold mismatch, not ordering. A gap inside the line is bridged up to `BRIDGE_M` = 25 m (D3), but "the
   exit is the line's top" uses `DIP_M` = 5 m (`shore.py:645`). These two lines are not on the curated all-tidal
   path at all (`joins: curated` via the Fraser's tidal piece).
7. **The 19 "unnamed Fraser delta lines with no section" are not a tidal-pass artefact.** Measured on the source
   gpkg (bbox 1180000,440000,1260000,480000): 29 FWA lines meet the polygon and have no section, 16 of them ≥95%
   inside. Most carry the Fraser's own code `100-000000-…` with edge types 1350/1450. They are unnamed anabranches
   that `prune_mainstem_loops` dropped in `prune_graph`, before any cut. A few carry WSC 999 / edge 1400 and are not
   in the network. So they are already gone in the graph build, which is what the owner wants. The defect is that
   no record says so. `polygon_inside` runs on `b.chains` (pre-prune), and `polygon_rule` skips a blk with no graph
   piece silently (`shore.py:535-537`). The "already all-tidal via a curated limit" skip (`shore.py:538`) is not the
   cause: those lines are still tidal as `water:` / `split:` owners.
8. **`sidecars --tidal-only` is a post-build rewrite of a tidal input** (#7). `atlas.inputs` already says every input
   needs a rebuild (`inputs.py:68-69`).
9. **The store drops tidal for key-0 sections** (#12, T672 1b): 121 unnamed tidal sections read "outside B.C.".

## Plan (in order)
1. **One tidal table from the atlas** (needs an atlas rebuild). At the end of the atlas build, after the final
   registry, write `tidal_sections.json`: one row per section, `{section, owner}`, with owners `split:` / `water:` /
   `inside:` / `crossing:`. Use the `ok` fixpoint already computed in `polygon_rule`, with no second fixpoint. Keep
   the existing guard (`build.py:1598`) as the proof. The reach reads it and adds only the catalogue rows marked
   `tidal` (a regs fact, rightly in the reach). The tiles read it (drop `tidal_pieces`). The review app reads it.
   `tidal_cut_owner` / `tidal_crossing_owner` stay as the atlas's own functions, in an atlas module, and the reach
   stops importing them for the answer. Pin it: the reach, tiles and bundle each read this file, and the reach does
   no graph walk for tidal.
2. **Move the tidal resolution into the atlas** (#5): the functions that resolve a limit's piece become atlas code on
   the water registry the build already makes. The reach imports from the atlas, not the other way round. No section
   changes: the existing guard proves it.
3. **Held lists to curated data** (no atlas rebuild to move them; the same values give the same build). Move
   `HELD_*` and `HELD_KEEPS_ITS_CUT` to `data/curated/waters/shore_held.json`, register it in `atlas.inputs`, and
   have a tick write it through like the tidal limits.
4. **Shore cuts in the cut sequence, before the length cap** (needs an atlas rebuild and a binding diff by place,
   T677 item 3). The order becomes border, curated (+ tidal limit cuts + gauge), braids, areas, **shore**, length.
   That restores "length is last", and length cuts stop landing in water that goes tidal. Fold `shore_cuts.json`
   into the per-cut record of `splits.resolved.json` (cut or why not), and fold the shore sweep record into the one
   sweep record by stage. Section ids on the cut lines move. This is mechanical, not a rule reading, but it gives the
   owner a diff.
5. **Removal as a graph fact** (atlas rebuild). Set the tidal owner on the graph node itself (rows of plan 1), so
   "not fresh water" is part of the graph the build writes. The reach's binding subtraction and the tiles' skip then
   read the node. Owner B: keep and mark, do not delete.
6. **Record what the prune took from the tidal polygon** (code now, appears at the next rebuild). Write a row in the
   shore record, "pruned before the shore pass (braid loop / leaf / not in network)", for every polygon-touching blk
   with no graph piece. Make the silent skip at `shore.py:535-537` a counted reason.
7. **Retire `sidecars --tidal-only`** (no rebuild). A tidal limit change waits for the next atlas build, as every
   other input does.
8. **Store fact for key-0 tidal sections** (no atlas rebuild; deliver + a release): carry the plan-1 owner as a
   per-section "not fresh: tidal | outside" fact, so T672 1b closes from stored data.
9. **Names**: rename the leaf-prune "tidal" exemption to "coastal (WSC 9)" in its words only. No behaviour change.

Atlas rebuild needed: 1, 2 (rides with 1), 4, 5, 6 (the record), and owner item A if ticked. No rebuild: 3, 7, 8, 9.
Batch 1+2+4+5+6 into the next rebuild (with T677), reach-only rerun after.
10. **Mouth-line cut in the atlas build** (atlas rebuild, with step 4). The cut rule is the order in the answers
    above: (1) a DFO written boundary, including curated splits that encode one; (2) otherwise coast crossing, where the
    line enters the sea or a tidal river (prototype `t665/mouthline/`, `mouthline.py`, `tables.md`); (3) the DFO polygon
    for map shading only. Polygon hand edits and non-DFO splits carry no authority. The 25 m stub rule (A) and the
    Fraser channels rule apply in the same pass. Re-derive the held lists from the lines where the method fails; the
    rest of the owner decisions go on the rebuilt held-rivers page.
11. **Classify the 33 curated splits** (DFO-written or not), before step 10 uses them.

Atlas rebuild needed: 1, 2 (rides with 1), 4, 5, 6 (the record), 10. No rebuild: 3, 7, 8, 9, 11.
Batch 1+2+4+5+6+10 into the next rebuild (with T677), reach-only rerun after.

## Owner items
A and B are answered (above). C stays open on the held-rivers page. Open for the owner, one by one: Fraser sloughs
and 7 unnamed lines; Kitimat upstream limit; the 46 estuaries drawn as river where the mouth methods disagree.

Depends: T665, T677. Size: L.
