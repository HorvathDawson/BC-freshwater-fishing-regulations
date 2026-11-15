# Puntledge River: the "75 m below the hatchery fence" cut is on the fence (2026-11-15)

Owner, labelling DFO places in Region 1: "75 m downstream of Puntledge River hatchery fence split is on
the hatchery fence (the split already in the river) we should fix the proposal and make sure we fix this
existing split."

**Short answer: he is right, and it affects everyone who uses the cut, not only the DFO proposal.** The
synopsis and both DFO wordings say *75 m downstream* of the fence. The cut was anchored on the hatchery
grounds, about 74 m upstream of the fence. The build then moved it 75 m downstream, which put it on the
fence itself. Everything below is a proposal. What a wording means is the owner's call.

How each fact was found: **[ran]** = I ran a read-only script on generated files (scratchpad
`proj.py`, `scan.py`). **[read]** = read from a file. **[inferred]** = my own conclusion.

## 1. Cuts on the Puntledge near the hatchery

The river line is blk `354154204` (gnis:27060). "Measure" means metres up the FWA route.

| id | wording it comes from | anchor in `data/curated/waters/cuts.json` | resolved measure (`data/generated/atlas/full/splits.resolved.json`) |
|---|---|---|---|
| `puntledge_river__signs_75_m_downstream_of_puntledge_river_hatchery_fence` (cuts.json:1680) | synopsis `r1:puntledge_river@1-6` rule r4, printed p.18: "...to signs located 75 m downstream of the Puntledge River hatchery fence (total distance approximately 500 m)" | point [-125.03217, 49.68578], offset 75 downstream. `_note`: "base=Puntledge River Hatchery OSM rel 9443986; +75m d/s -> 49.68512,-125.0323" | **4000.28** [read] |
| `puntledge_river__fishing_boundary_signs_50_m_upstream_of_bc_hydro_puntledge_generating_station_tailrace` (cuts.json:1658) | same rule r4, upstream end | point [-125.03397, 49.68837], offset 50 upstream. base = "Puntledge Generating Station OSM rel 15937988" | 4451.91 [read] |
| `..._morrison_creek_into_puntledge_river_u100m` / `_d100m` | r3 | confluence + offset 100 m | 2413.25 / 2213.25 |
| `..._base_of_stotan_falls`, `..._bc_hydro_diversion_dam` | r2 | points | 7008.61 / 11094.1 (far upstream) |
| proposal `puntledge_lower_weir` (new, not built) | DFO split hunt, `data/generated/review/dfo-split-hunt.json:697` | OSM weir way 152588484, snapped [-125.0323, 49.685118] | 4001.1 [read] |
| proposal `puntledge_lower_weir_d75` (new) | same | the weir point with offset −75 | about 3926 [read] |

There is no `data/curated/dfo_salmon/cuts.json`. The repo describes one hatchery here: the "lower
hatchery" (Puntledge River Hatchery, OSM rel 9443986). I found no upper hatchery in the repo, so whether
one exists is **not confirmed**.

## 2. Where the fence is, and where the 75 m point falls

I projected points onto the published stream line for sections 57879–57881 in `stream.geojsonl` (EPSG:3005) **[ran]**:

- **The fence/weir.** OSM weir way 152588484 crosses the river at [-125.0323, 49.685118], measure about **4001**.
  Section 57880 is the r4 closure, 451.6 m long. Its downstream end is [-125.032301, 49.685111], which is the
  weir (under 1 m away). The cut resolves at 4000.28. **So the cut is on the fence.** [ran]
- **The curated anchor** [-125.03217, 49.68578] lies 74 m upstream of the weir, at measure about 4075. It is the
  hatchery grounds, not the fence. [ran]
- **The point 75 m below the weir** is at **[-125.032159, 49.684467]**, measure about **3925.3** (about 3.93 km up the
  blk). [ran]
- **Fence = weir?** One DFO place says "hatchery fish fence" and the other says "weir at the lower hatchery",
  for the same 75 m signs. That supports reading them as one structure. **[inferred]** Not confirmed on the ground.
- **Effect on the closure length.** Fixing the cut makes r4 about 4451.9 − 3925.3 = **526.6 m** instead of 451.6 m.
  Both are close to "approximately 500 m".

## 3. Who uses the cut today, and what each wording says

| user | wording | says |
|---|---|---|
| synopsis rule `puntledge_river.r4` (synopsis/region-1.json:9754–9770), `between` the tailrace cut and this cut | "to signs located 75 m downstream of the Puntledge River hatchery fence" | **75 m below** |
| DFO `1:puntledge-river:from-signs-located-50m-upstream-of` (places/region-1.json:5035, binding :5090), active. Mark **marks.jsonl:2447**, verified 2026-11-02 | "to signs located 75m downstream of the Puntledge River hatchery fish fence (total distance approx. 500m)" | **75 m below** |
| DFO `1:puntledge-river:downstream-from-a-fishing-boundary` (places :2431, binding :2479). Mark **marks.jsonl:2687**, verified 2026-11-15, applied in commit bcbf47471 | "downstream from a fishing boundary sign located 75m downstream of the weir at the lower hatchery" | **75 m below** |
| entry mark **marks.jsonl:1263** (`r1:puntledge_river@1-6`, verified 2026-10-29). It carries a reach print for r4 | — | — |
| test `pipeline/tests/test_rules_round.py:503` | checks only the id | — |

All three wordings say 75 m below. **[read]** This is not a Seton-style case where two different cuts are
both real. **The existing cut is misplaced for everyone.** The only place that needs a cut *at* the fence is the
dormant `1:puntledge-river:from-the-weir-at-the-lower-hatcher` (places :2380, binding empty): "from the weir at
the lower hatchery to signs approximately 75m downstream of the weir". Its proposal (below) is correct.

## 4. What the proposals say, and why the suggester chose the fence cut

- **`downstream-from-a-fishing-boundary`.** The proposal is `downstream_of` the fence cut, medium confidence,
  reason "75m-down, downstream, hatchery". The suggester matches a cut by the words in its **label**
  (`score` at `pipeline/regs/dfo_salmon/suggest.py:331-365`, step 3 in `resolve` at :481-500). The label says
  "75 m downstream ... hatchery fence", so it matched. Nothing checks where the cut actually sits. The
  offset-mismatch guard at :496-499 only fires when the cut's label has no offset. **[read]**
- **`from-the-weir-at-the-lower-hatcher`.** The split hunt proposes two new cuts, the weir (4001.1) and the weir
  plus −75 m. `_hunt_new` (suggest.py:922-985) keeps the offset as a property of the split and draws the point at
  `measure + off`. This proposal is correct.
- **A misreading in the hunt's warning.** Its caveat (dfo-split-hunt.json:697) says the curated cut "sits at FWA
  measure 4075 m — 74 m ABOVE the OSM weir". 4075 is the anchor **before** the 75 m offset is applied. The built
  cut sits at 4000, which is the weir. The warning saw a misplacement but put it in the wrong place. **[ran]**
- **Offsets are already supported.** The suggester turns "N m up/down" phrases into offset tokens (`_offset`
  :391-397, `score` :341-351). It adds offsets for confluences (:542-544), falls (:575-577) and hunted cuts
  (:958-973). Curated cuts already store `offset_m`/`offset_dir`, applied by `_apply_offset` in
  `pipeline/atlas/splits/anchors.py:149-160`. No new mechanism is needed. The fault is in the data: the base
  point is wrong.
- **Proposed fix (needs owner approval):** In cuts.json:1683-1691, move the anchor coord to the weir,
  [-125.0323, 49.685118], and keep the 75 m downstream offset. The cut then resolves at about 3925, at
  [-125.032159, 49.684467]. Also correct the `_note` and the hunt caveat. For the dormant weir place: keep the
  new `puntledge_lower_weir` at 4001 as the "from" end. Use the corrected curated cut as the "to" end instead of
  `puntledge_lower_weir_d75`, which would otherwise duplicate it.

## 5. What each fix needs

- **Option A: move the existing cut (recommended).** One edit to cuts.json, with the same id. Three things move
  with it: synopsis r4 (closure 451.6 → about 527 m), DFO `from-signs-located-50m-upstream-of` (same reach), and
  DFO `downstream-from-a-fishing-boundary` (its top end moves 75 m down, and the 75 m below the weir correctly
  falls outside it). Bindings and mark extent_fixes name the id, so they stay valid.
  - It needs the **atlas rebuild**, because sections are cut at splits (57879/57880 change). This can go into the
    pending rebuild with the owner's nine new cuts, then the bundle.
  - Owner re-ticks: the entry mark :1263 (its r4 reach print will change) and the DFO marks :2447 and :2687,
    because the reach under the same id moves. **[inferred]**
  - Then add the weir cut for the dormant place when it is labelled.
- **Option B: add a new offset cut and keep the old one.** This also needs the rebuild. The old id stays at the
  fence with a "75 m downstream" label, which is the wrong name for that place. The synopsis r4 and both DFO
  bindings and marks would then need to switch ids, which means more re-marks. Not recommended.
- **Possible second case to check:** the tailrace cut's base is also an OSM *relation*, the generating-station
  polygon, not the tailrace outlet. It may share the same fault. Not checked on a map.

### Other "N m up/downstream of X" wordings that sit on the landmark [ran]

- **Scan A: proposals in the DFO queue whose cut has no offset.** Five hits: Sproat (lake end),
  Stamp ×2 (signs at Girl Guide Falls), Nanaimo~2 (Cedar bridge end), South Thompson (Little Shuswap Lake end).
  In every one the cut used is the end **without** an offset in the wording. None is wrong.
- **Scan B: active bindings.** Eight hits: Qualicum, Sproat, Stamp, Puntledge Stotan, Chapman, Chilliwack~3,
  Dean, Morice~2. In each, the offset end already uses an offset cut, and the flagged cut is the other, plain end.
  Example: Chapman uses `chapman_creek__100_m_below_lower_falls`, which has a 100 m offset. None is wrong.
- **Scan C: hunted landmarks within 15 m of a curated offset cut.** None. The Puntledge weir was missed because it
  is 0.8 m from the cut. The scan pairs "landmark" against "offset cut", and here the cut sits on the landmark
  itself.
- **Little Qualicum:** the fence is at 2917.97 and the 35 m signs at 2882.97, exactly 35 m apart. Correct.

**Result: Puntledge is the only confirmed case.** The scans cannot catch a wrong base point in general. A cut
anchored on a facility polygon instead of the structure looks correct by its label, and only a map check finds it.

## Done 2026-11-15

Owner, 2026-11-15: "please add both cut on weir and one downstream then i give you permission to edit that extent for
regulations and dfo places." Applied as Option A plus the weir cut. Measures are on blk `354154204`, projected onto the
FWA line in `data/source/bc_fisheries_data.gpkg` (scratchpad `t743w/proj.py`) **[ran]**.

| cut | anchor in `data/curated/waters/cuts.json` | where it resolves | referenced by |
|---|---|---|---|
| **new** `puntledge_river__puntledge_lower_hatchery_weir` (the weir / fish fence) | point [-125.0323, 49.685118], no offset. Promoted from the split hunt's `puntledge_lower_weir` (OSM way 152588484, hunt point 49.68512,-125.03234, 2.9 m off the line), snapped to the line | measure **~4001.0**, on the weir | nothing yet. Meant for the dormant DFO place `1:puntledge-river:from-the-weir-at-the-lower-hatcher` |
| **moved** `puntledge_river__signs_75_m_downstream_of_puntledge_river_hatchery_fence` (same id) | anchor moved from [-125.03217, 49.68578] (hatchery grounds, measure 4075) to the weir [-125.0323, 49.685118]. The 75 m downstream offset is kept | measure **~3926.0**, at [-125.03216, 49.684467]. It was 4000.3, on the weir | synopsis `puntledge_river.r4` (`between` tailrace cut and this one, ~527 m instead of 451.6 m). DFO `from-signs-located-50m-upstream-of` (same reach). DFO `downstream-from-a-fishing-boundary` (its top end, 75 m lower) |

- Both cuts have `_note` "owner 2026-11-15" and are listed in the file's ODbL `_licence.ids`, because both are now
  anchored on the OSM weir. The edit was made through `curated_write.write` (backup
  `data/generated/curated_writes/backups/waters/cuts.json/20261115T185615166470Z-8bd7c60bcf3b.json`, and a copy in
  `~/bcfish-session-backup-2026-11-12/cuts-backups/`).
- **Synopsis r4: no change.** It already names the 75 m id, so it moves with the cut.
- **DFO places: no change and no mark appended.** Both "75 m below" places already name the 75 m id. The weir place is
  dormant: its binding is empty and it has **no mark**. Its verdict and extent are for the owner: between
  `puntledge_river__puntledge_lower_hatchery_weir` and `puntledge_river__signs_75_m_downstream_of_puntledge_river_hatchery_fence`.
  Do not use the hunt's `puntledge_lower_weir_d75`, which would duplicate the 75 m cut.
- **When it takes effect:** at the next atlas rebuild, which is already queued (sections 57879/57880 are re-cut), then
  the reach run and bundle. Not built here.
- **Owner re-ticks after that rebuild:** entry mark **marks.jsonl:1263** (`r1:puntledge_river@1-6`, its r4 reach print
  changes), and DFO marks **:2447** and **:2687** (same ids, but the reach moves 75 m). The weir place also needs a first
  mark.
- Validation: `check_curated --waters` gave 0 errors, and the full `check_curated` passed. `load_split_defs` loads 468 cuts.
  `pytest test_splits test_t207_split_confidence test_splits_accuracy test_licence_records test_curated_writer
  test_atlas_inputs test_rules_round`: 178 passed. `review.apply --check` exits 1 with 49 not landed (the Fraser, Region 6
  and other cut/new-split waits) and no Puntledge row. `check-tickets` passes.

### The tailrace cut has the same fault (checked, not changed)

`puntledge_river__fishing_boundary_signs_50_m_upstream_of_bc_hydro_puntledge_generating_station_tailrace` is anchored at
[-125.03397, 49.68837]. That point is the centre of the generating-station polygon (OSM rel 15937988, bbox centre
projects to 4401.9) and resolves at **4401.9 + 50 = 4451.9**. OSM maps the tailrace as a canal, way 1178621508
(`usage=tailrace`), which meets the river at 49.6885474,-125.0342327. That outlet projects to measure **~4429.3**, 0.9 m
off the line **[ran]**. Signs "50 m upstream of the tailrace" would then sit at about **4479**, which is **~27 m** above
today's cut. If the owner approves the move, r4 would be about 553 m long and the 50 m DFO place would move with it. This
is a proposal only. It was not part of the owner's instruction.
