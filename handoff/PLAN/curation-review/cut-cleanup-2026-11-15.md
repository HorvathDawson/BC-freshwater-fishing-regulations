# Cut clean-up after the owner's labelling (2026-11-15, T743)

Owner, 2026-11-15: "i thought skeena used to have a hells gate cut? also other ones liek this dont have the confluence cut?
… can you audit everything a lot compile all results/issues into questions for me later. and make sure to fix all the
remaining items left … also we could rebuild the cuts and make sure nothing causes issues? also want to make sure we
deduplicate the new cuts from proposal …"

Tags: **[ran]** checked by running a script or command; **[read]** read in code or data; **[inferred]** my reading.
Inputs: marks.jsonl at 2,739 lines after this lane; live atlas `data/generated/atlas/full` (COMPLETE 2026-11-13);
`waters/cuts.json` (468 cuts); `dfo-split-hunt.json`; proposals index rebuilt by `proposals build` after the re-saves.
Audit: `python -m pipeline.regs.review.audit` before (313 findings) and after with `--since` (299; 0 new, 14 resolved) [ran].
Scripts: session scratchpad `cc2/` (inv.py, near.py, conf.py, remark.py, restore.py, pallant.py).

## What was done

| what | how | commit |
|---|---|---|
| Pallant Creek tidal boundary moved to the bridge near the mouth (owner: "we can make the bridge the tidal boudnary i think") | the owner's own row in `waters/tidal_limits.json` (no mark, decided 2026-11-15, the Noons Creek pattern: kept over the 10-28 auto mouth row) + an `authority: owner` row in `tidal_split_authority.json` (counts 33 → 34); both through `curated_write.write`; `tidal_limits --check` says current | fd78c0a6f |
| 5 re-saves + 1 undo, appended with `dfo.mark_for` + `marks.append`, reviewer "Claude (for Dawson)", verdict and answers unchanged; backup `~/bcfish-session-backup-2026-11-12/marks-backups/marks-before-cut-cleanup-20261115-122658.jsonl` | table below; each dry-run diffed field by field against the line it replaces (only extent_fix / new_splits / note / reviewer / seconds / extent_before / the binding print moved) | fd78c0a6f |
| landed through the write-through (`apply dfo`) | Illiance, Wilauks, Morice Bulkley confluence; the other lane's Bella Coola ×2 and Bulkley ×2 landed in the same run | 9652ef28f, e702daa70, 7ed7a57fa (+ 4c33c545f, 709424dab, dee4e2ace, 91d4ae0d1 for the other lane) |

| old line | new line | place | change | landed |
|---|---|---|---|---|
| 2665 | 2734 | Illiance River "upstream of signs near mouth" | new cut "mouth (tidal boundary)" at m 0 → `whole[gnis:4205]` (open downstream end; the stream's tidal boundary IS its FWA mouth, m 0, so the cut was that point) | yes: 0 → 181 sections |
| 2669 | 2735 | Wilauks Creek, same words | same → `whole[gnis:3787]` | yes: 0 → 20 sections |
| 2678 | 2736 | Gitnadoix "upstream of powerline crossing near river mouth" | extent was already `whole`; the unused new cut at m 0 (a line-end cut) dropped | already in the file |
| 2591 | 2737, then **2739** | Morice "from Gosnell Creek to Lamprey Creek" | 2737 re-pointed to the confluence cut; the dry run showed 131 → 1,784 sections (the confluence cut puts the Thautil mouth inside the window, so the whole Thautil / Gosnell basin walks in as tributaries). Not mechanical: 2739 puts back the owner's own extent (the signs cut 100 m below); asked as Q8 | file unchanged |
| 2592 | 2738 | Morice "from the confluence of the Bulkley and Morice upstream to the Bymac Bridge" | `…bulkley_confluence_u100m` → `…bulkley_confluence` (the point the words name) | yes: 19 → 20 sections |

`apply --check` after: 51 not landed, **all waiting** (side rebuild / new cuts, plus the 2 allowed T359 tidal waits); none
refused or stale [ran]. Tests: `test_review_marks test_review_proposals test_splits test_curated_writer test_atlas_inputs
test_tidal_limits test_coast_cut test_tidal_cut test_dfo_tidal_water`: 243 passed (unfiltered pytest) [ran].
`check_curated --waters` 0 errors; full `check_curated` ok; `check-tickets` passes [ran].

Not done (my brief does not allow it, or it is not mechanical): no cut was added to `cuts.json` by hand. The repo already
has the route that turns marks' `new_splits` into curated cuts: `proposals build` → `proposals side <ids>` (patched copies)
→ side atlas → `git apply` of each `data/generated/review/proposals/split_*.patch` → `apply dfo --side-checked …`. It
dedupes proposals itself (`marks.same_anchor`: same anchor, or within 10 m) and skips any already in cuts.json
(`place_groups` dup). Section 3 feeds it.

## 1. Inventory: every new cut asked for by a mark in force (A) and its dedupe result (B)

59 `new_splits` on 51 DFO places in force [ran]. 22 of them already resolve to a curated cut (the proposal step counts them as
applied): Campbell/Quinsam confluence 0.0 m, Sandy Pool sign 0.1 m, Stamp powerline / lagoon inlet 0.0 m, Sumas Barrow Town
0.0 m ×4, Harrison Hwy 7 0.0 m ×2, Nicomekl / Serpentine 23 m cuts 0.0 m, Nicomen Siddle / Dewdney 0.0 m, Stave CPR bridge
0.7 m ×2 (the tidal boundary cut), Thompson Goldpan / Skihist 0.0 m, Nakina Sloko ±300/1300 m (built). Result **REUSE**,
nothing to do. After the re-saves, **23 new cuts** remain; none is a duplicate of a curated cut or of another proposal
(nearest neighbours by measure on the same line and by coordinate on any line within 1.5 km, below) [ran].

| proposal id | cut id | words | coord | blk @ m | marks in force | nearest existing (distance) | result |
|---|---|---|---|---|---|---|---|
| split-fraser_river_highway_1_bridge_at_hope_bi | fraser_river__highway_1_bridge_at_hope_bill_hartley_fraser_hope | Hwy 1 bridge at Hope | -121.452186, 49.386489 | 356364114 @ 158751 | 2568, 2571, 2573, 2574, 2576 | gauge 08MF005 65.9 m (a gauge, not the bridge) | NEW (merged across 5 marks) |
| split-fraser_river_alexandra_bridge_hwy_1 | fraser_river__alexandra_bridge_hwy_1 | Alexandra Bridge (Hwy 1) | -121.409692, 49.701209 | 356364114 @ 201241.5 | 2572, 2573 | Spuzzum confluence 4.0 km | NEW (merged, 2 marks) |
| split-fraser_river_seton_river | fraser_river__seton_river | confluence with the Seton | -121.926856, 50.681805 | 356364114 @ 329127.2 | 2570 | Hwy 99 bridge 254.7 m | NEW: two real places, owner still deciding (Q4) |
| split-fraser_river_signs_4_km_below_lillooet | fraser_river__signs_4_km_below_lillooet | signs ~4 km below Lillooet | -121.904312, 50.655955 | 356364114 @ 325514.5 | 2570 | tail race 2.4 km | NEW, position in doubt (Q2) |
| split-south_thompson_river_campbell_creek_d100 | south_thompson_river__campbell_creek_d100m | 100 m below Campbell Creek | (mouth) -120.078674, 50.660036, −100 m | 356361760 @ 19741.4 −100 | 2579 | the Campbell Creek length cut 0.0 m is its reference, not the same cut (offset) | NEW (offset cut) |
| split-quesnel_river_johnson_subdivision_bridge | quesnel_river__johnson_subdivision_bridge | Johnson Subdivision Bridge | -122.489466, 52.973025 | 356363937 @ 834.2 | 2580, 2581 | none within 1.5 km | NEW (merged, 2 marks) |
| split-quesnel_river_powerlines | quesnel_river__powerlines | the powerlines | -122.496669, 52.996503 | 356363937 @ 4331.6 | 2580, 2581 | none | NEW (merged, 2 marks) |
| split-atnarko_river_signs_near_tweedsmuir_park | atnarko_river__signs_near_tweedsmuir_park_lodge_stuie | sign near Tweedsmuir Park Lodge | -126.073265, 52.370657 | 360879335 @ 2339.1 | 2584 | none; hunt point 260 m off | NEW, medium (Q5) |
| split-cowichan_river_skutz_falls | cowichan_river__skutz_falls | Skutz Falls | -123.955626, 48.783139 | 354155148 @ 34212.6 | 2600 | CNR trestle 2.1 km | NEW |
| split-zymagotitz_river_hwy_16_bridge_over_the | zymagotitz_river__hwy_16_bridge_over_the_zymagotitz | Hwy 16 bridge | -128.732915, 54.487079 | 360886024 @ 778.4 | 2656 | none | NEW |
| split-khyex_river_hwy_16_bridge_over_the_khyex | khyex_river__hwy_16_bridge_over_the_khyex | Hwy 16 bridge | -129.804703, 54.232242 | 360879315 @ 1517.2 | 2666 | none | NEW |
| split-pinkut_creek_signs_25_m_below_the_pinkut | pinkut_creek__signs_25_m_below_the_pinkut_fish_fence_d25m | signs 25 m below the fence | fence -125.459485, 54.445762, −25 m | 360886207 @ 1489.5 −25 | 2667 | `pinkut_creek__fish_fence` 0.0 m is its reference (offset) | NEW (offset cut) |
| split-shegunia_river_signs_above_the_shegunia | shegunia_river__signs_above_the_shegunia_logging_road_bridge_u50m | signs above the bridge | bridge -127.6321, 55.347395, +50 m | 360875115 @ 3402.6 +50 | 2668 | Shegunia→Skeena confluence 3.0 km | NEW |
| split-shegunia_river_signs_below_the_shegunia | shegunia_river__signs_below_the_shegunia_logging_road_bridge_d50m | signs below the bridge | same, −50 m | 360875115 @ 3402.6 −50 | 2668 | — (distinct offset of the same bridge) | NEW |
| split-ecstall_river_johnston_creek | ecstall_river__johnston_creek | signs near the Johnston Creek confluence | confluence anchor gnis:2031, -129.491288, 53.857346 | 360876170 @ 57302.8 | 2671 | none: no Ecstall confluence cut exists | NEW, and it IS a confluence-anchored cut (Q14) |
| split-exchamsiks_river_hwy_16_bridge_over_the | exchamsiks_river__hwy_16_bridge_over_the_exchamsiks | Hwy 16 bridge | -129.29687, 54.332696 | 360884603 @ 227.3 | 2728 | Exchamsiks→Skeena confluence 226 m (a different place: the mouth vs the bridge) | NEW |
| split-exstew_river_hwy_16_bridge_over_the_exst | exstew_river__hwy_16_bridge_over_the_exstew | Hwy 16 bridge | -129.056405, 54.400377 | 360882477 @ 413.1 | 2676 | none | NEW |
| split-kispiox_river_signs_near_kispiox_river_r | kispiox_river__signs_near_kispiox_river_resort | signs near the resort | -127.716805, 55.434224 | 360885864 @ 13691.1 | 2680 | gauge 08EB004 42.6 m (a gauge) | NEW |
| split-zymoetz_hwy_16_bridge_over_the_zymoetz_c | zymoetz__hwy_16_bridge_over_the_zymoetz_copper | Hwy 16 bridge | -128.476985, 54.542222 | 360881231 @ 594.6 | 2691, 2692 | Zymoetz +1500 m on the Skeena 837 m | NEW (merged, 2 marks) |
| split-lakelse_river_whitebottom_rd_bridge_lake | lakelse_river__whitebottom_rd_bridge_lakelse_near_the_mouth | logging road bridge near the mouth | -128.762699, 54.444371 | 360879877 @ 2004.8 | 2695, 2696 | power line crossing 1.74 km | NEW (merged; above/below pair share it) |
| split-nass_river_canyon_city_gitwinksihlkw_bri | nass_river__canyon_city_gitwinksihlkw_bridge | Canyon City Bridge | -129.210037, 55.195211 | 360885942 @ 66226.9 | 2717 | none | NEW |
| split-kitsumkalum_signs_below_the_kitsumkalum | kitsumkalum__signs_below_the_kitsumkalum_lower_canyon | signs below the lower canyon | -128.678234, 54.604005 | 360883243 @ 13076.2 | 2723, 2725 | none | NEW (merged; up/down pair) |
| split-kitsumkalum_cn_railway_bridge_over_the_k | kitsumkalum__cn_railway_bridge_over_the_kitsumkalum | railway bridge | -128.661401, 54.520277 | 360883243 @ 22.5 | 2724 | Kitsumkalum +1500 m on the Skeena 307 m | NEW; 22.5 m above the line's end (Q7) |

Counts: **reused 22 · merged 7 groups (17 mark-cuts into 7) · new 23 · withdrawn by re-save 3** (Illiance, Wilauks,
Gitnadoix m-0 cuts). Two ids lack the river word (`zymoetz__…`, `kitsumkalum__…`): the names the review app's namer gave
at save time; harmless, but a code item for the namer (T755) if wanted.

**Split-hunt rows (dfo-split-hunt.json) that duplicate the cuts above** [ran]. T755's hunt now reuses a cut within 25 m,
so these are caught once the cuts are promoted: `fraser_lillooet_4km` (9 m), `lakelse_whitebottom_bridge` (14 m),
`kitsumkalum_lower_canyon` (2 m), `pinkut_fence_d25` (5 m), `shegunia_bridge_u50/d50` (1 m), `cowichan_skutz_falls` (0 m),
`chapman_hwy101` (4 m from `chapman_creek__tidal_boundary`), `stave_cpr_bridge` (7 m), `nicomen_*`, `stamp_*`, `nakina_*`.
**Farther than 25 m (would come back as a duplicate proposal on the next `suggest` run; withdraw in step 9):**
`fraser_hope_bridge` (114 m), `fraser_alexandra_bridge` (77 m), `kispiox_resort_signs` (116 m), `atnarko_lodge_sign` (260 m),
`exchamsiks_hwy16` (42 m), `exstew_hwy16` (38 m), `khyex_hwy16` (47 m), `nass_canyon_city_bridge` (35 m),
`kitsumkalum_cn_bridge` (27 m), `zymagotitz_hwy16` (26 m), `zymoetz_hwy16` (27 m), `harrison_hwy7` (165 m from the curated
cut), and the three m-0 ones `gitnadoix_powerline`, `mouth_signs_illiance`, `mouth_signs_wilauks` (T755 leaves a mouth end open).

## 2. Confluence ends (C), all regions [ran]

70 DFO places have confluence / mouth / outlet words. Every one bound to a confluence end uses the standing confluence cut
(Quinsam, Boulder, Little Nitinat, Morrison ±100, Ash, Hyde, Crag, Meziadin ×3, Morice→Bulkley, Gosnell, Nilkitkwa ×2, Mill,
Zymagotitz, the Forks pair), except:

| place | words | end it uses | standing confluence cut | result |
|---|---|---|---|---|
| Skeena "from Lakelse River mouth upstream to the Skeena River Overpass (New Hwy Bridge) at Ferry Island" (dormant, **unmarked**, no extent) | Lakelse mouth | the suggestion: a NEW confluence cut on blk 360223771 (a Skeena side line, m 532) + a new Dudley Little bridge cut | **none on the Skeena main line** (no `skeena_river__lakelse_river_into_skeena_river`) | owner's point is right: it does not cut the mainstem. Q12 |
| Skeena "from the confluence with the Babine River downstream to the CNR Railway Bridge at Terrace" (dormant, **unmarked**) | Babine confluence | the suggestion: CNR bridge → `gauge__08EB005` ("above Babine River"), which is the OTHER wording's end ("to a point above the confluence") | **none** (no Babine→Skeena cut) | wrong end by the words. Q13 |
| Ecstall "upstream of signs near confluence with Johnston Creek" (2671, owner note "make sure this split uses the proper confluence splits") | Johnston confluence | a NEW confluence-anchored cut (tributary gnis:2031) | none existed | it is the proper confluence cut, created by this mark. Q14 only for "signs near" |
| Morice "from Gosnell Creek to Lamprey Creek" (2591) | Gosnell | the signs cut 100 m below | `morice_river__gosnell_creek_into_morice_river` (75.17 km) | re-point not mechanical (Thautil basin). Q8 |
| Morice "from the confluence of the Bulkley and Morice" (2592) | the Forks | was the signs cut 100 m up | `morice_river__bulkley_confluence` | **fixed**, line 2738 |
| Harrison "to the confluence with the Fraser", Alouette "to the confluence of the Pitt", Nechako ×2, Chehalis hatchery, Nicomen Siddle, Babine ~2 "to the Skeena confluence" | a mouth on another river | open end (to the line's bottom) | n/a | right: the mouth IS that confluence |
| South Thompson "100 m downstream of Campbell Creek" | offset | new −100 m cut off the Campbell Creek confluence | the length cut at the mouth is its reference | right |
| Kispiox-mouth Skeena places (3, "within signs at the confluence") | an area round the mouth | `whole` of the confluence item | `skeena_river_kispiox_river_confluence__…` | right |
| Inch Creek, Babine Lake 400 m ×2 | — | no extent, unmarked | — | unmarked places (Q-unmarked) |

## 3. "Hells Gate" on the Skeena (D) [ran + read]

- No cut id or label with "hell" has ever been on the Skeena: `git log --all -S` for `skeena_river__hell`, `hells_gate`,
  `hell_s_gate` and "hell" on the waters/DFO files finds only the Fraser's `fraser_river__hell_s_gate` (km 209.1) and its
  "signs 2800 m downstream of Hell's Gate".
- The Skeena's Hell's Gate is the Classified Waters boundary 1.5 km above the Kitsumkalum mouth. It exists, under the name
  **`skeena_river__kitsumkalum_river_into_skeena_river_u1500m`** ("Kitsumkalum River → Skeena River (1500 m upstream)",
  Skeena m 123,961). The live place "mainstem waters near the Kitsumkalum River mouth, from the confluence with the
  Zymagotitz … upstream to the Classified Waters boundary at the top of Hell's Gate" (active, unmarked) ends on it; the place's
  note says so. It was never renamed or retired. Q18 asks whether to give it a Hell's Gate label.

## 4. Bad cuts already known (E), status

| item | status |
|---|---|
| Tidal-proposal duplicates: Chapman (3.6 m), Stave ×2 (0.7 m) | already counted as the existing tidal boundary cut; nothing builds twice |
| Illiance, Wilauks m-0 "mouth (tidal boundary)" cuts | **fixed** (open end); Kitsault was already `whole` |
| Gitnadoix powerline cut at m 0, unused | **fixed** (dropped); the powerline does not cross the FWA line (hunt note), Q6 |
| Morice 2591 / 2592 | 2592 **fixed**; 2591 Q8 |
| Nanaimo 2442 carries 2443's extent (no 500 m) | Q9 (needs a new offset cut; hunt has `nanaimo_borehole_d500`) |
| Sheldens Spur 19 | Q1 (position in doubt) |
| Lillooet signs | Q2 |
| Puntledge tailrace "50 m" (~27 m off) | Q3 (not approved to move) |
| Pallant Creek | **fixed** (tidal boundary at the bridge; takes effect at the rebuild) |
| Cuts at measure 0 in the live atlas [ran] | `morice_river__bulkley_confluence` (m 0 of the Morice: its mouth), `burton_creek__hwy_6_bridge` (m 0 on 356568717: a bridge at a line's very start, worth a map look), `pennask_creek__pennask_lake` (a lake edge, fine). All three are in a built atlas that passed the sliver gate. New: `kitsumkalum__cn_railway_bridge…` at m 22.5 (Q7) |
| Audit K: K1 (distance words, no offset cut): Nanaimo 400 m / 500 m, Puntledge Stotan 450 m, Somass 200/150 m, Stamp 500 m ×2 / 250 m, Chehalis 2.4 km, Squamish 1.5 km | all but Nanaimo 2442 name the distance as a description of where a named landmark sits (the landmark cut is right) [inferred]; Q9, Q10 |
| Audit K2 (offset cut, no distance words): Little Campbell 2538 (200 m signs), Babine 2490 (80 m signs) | Q10 |
| Tatshenshini border cuts (`border:<blk>:<n>`) not in splits.resolved.json | code item, not fixed (handoff/PLAN/curation-review/tatshenshini-border-2026-11-15.md) |

## 5. REBUILD INPUT (for the side build; NOT run here)

**Cuts the side atlas must contain:**
- the 23 new cuts in §1 (from `proposals side`, never by hand);
- already in `cuts.json`, built for the first time: `puntledge_river__puntledge_lower_hatchery_weir` (new) and the moved
  `puntledge_river__signs_75_m_downstream_of_puntledge_river_hatchery_fence` (bf1a29567);
- Pallant's tidal limit now `split` at `pallant_creek__bridge_near_mouth` (fd78c0a6f): the 0–56.6 m piece becomes tidal;
- code already landed that the rebuild carries: eafa7b9f5 (DFO lake through-river, POLICY 39), T753 lake loops (POLICY 40),
  T755 channels at resolve time.

**Must NOT be applied twice:** T664/T665 inputs (promoted f5831c1ac; do not `git apply` T664-pending/*); the Illiance,
Wilauks and Gitnadoix m-0 cuts (no mark in force names them now, so `proposals build` no longer lists them; do not re-add);
the hunt duplicates in §1 (withdraw at step 9, never accept).

**Commands, in order** (paths from the repo root):
```
SIDE=$PWD/data/generated/side/dfo-cuts23; rm -rf $SIDE; mkdir -p $SIDE/logs
rtk git status; node app/tools/check-tickets.mjs; scripts/fast-fail.sh; scripts/heavy.sh --status
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.proposals build          # re-check: 23 new_split ids, as below
IDS=split-fraser_river_highway_1_bridge_at_hope_bi,split-fraser_river_alexandra_bridge_hwy_1,split-fraser_river_seton_river,split-fraser_river_signs_4_km_below_lillooet,split-south_thompson_river_campbell_creek_d100,split-quesnel_river_johnson_subdivision_bridge,split-quesnel_river_powerlines,split-atnarko_river_signs_near_tweedsmuir_park,split-cowichan_river_skutz_falls,split-zymagotitz_river_hwy_16_bridge_over_the,split-khyex_river_hwy_16_bridge_over_the_khyex,split-pinkut_creek_signs_25_m_below_the_pinkut,split-shegunia_river_signs_above_the_shegunia,split-shegunia_river_signs_below_the_shegunia,split-ecstall_river_johnston_creek,split-exchamsiks_river_hwy_16_bridge_over_the,split-exstew_river_hwy_16_bridge_over_the_exst,split-kispiox_river_signs_near_kispiox_river_r,split-zymoetz_hwy_16_bridge_over_the_zymoetz_c,split-lakelse_river_whitebottom_rd_bridge_lake,split-nass_river_canyon_city_gitwinksihlkw_bri,split-kitsumkalum_signs_below_the_kitsumkalum,split-kitsumkalum_cn_railway_bridge_over_the_k
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.proposals side $IDS --out $SIDE
export CURATED_ROOT=$SIDE/curated
.venv/bin/python -m pipeline.tools.audit.check_curated --waters                                   # 0 errors
scripts/heavy.sh atlas-side 50 -- .venv/bin/python -u -m pipeline.atlas.build --full --out $SIDE/atlas/full_next > $SIDE/logs/atlas.log 2>&1
.venv/bin/python -m pipeline.atlas.promote $SIDE/atlas/full_next --dry-run > $SIDE/logs/parity.txt
.venv/bin/python -m pipeline.atlas.inputs --build $SIDE/atlas/full_next
scripts/heavy.sh reach-side 50 -- env PYTHONPATH="$PWD" .venv/bin/python -m pipeline.atlas.reach.cli --build $SIDE/atlas/full_next --out $SIDE/reach > $SIDE/logs/reach.log 2>&1
.venv/bin/python -m pipeline.atlas.reach.binding_diff data/generated/reaches/full $SIDE/reach --json $SIDE/binding_diff.json
# STOP: read the checks below; owner go
unset CURATED_ROOT
for p in data/generated/review/proposals/split_*.patch; do git apply "$p"; done      # only the 23 ids above
diff data/curated/waters/cuts.json $SIDE/curated/waters/cuts.json                     # empty
.venv/bin/python -m pipeline.atlas.promote $SIDE/atlas/full_next
rtk git commit -m "T743: 23 DFO cuts …" -- data/curated/waters/cuts.json
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.apply dfo --dry-run
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.apply dfo --side-checked <each mark-dfo-… id the dry run lists as waiting on a side rebuild>
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.apply --check              # only the 2 T359 tidal keys
# step 9: withdraw the §1 hunt duplicates in data/generated/review/dfo-split-hunt.json (as 1975b46b3), then suggest
scripts/heavy.sh build 50 -- .venv/bin/python -m pipeline build
```

**Checks that prove nothing broke:**
1. Parity: new boundary ids are exactly the 23 cut ids + `puntledge_river__puntledge_lower_hatchery_weir` + Pallant's tidal
   cut; no boundary lost (`--accept-boundary-loss` must not be needed); changed sections only on blks 356364114, 356361760,
   356363937, 360879335, 354155148, 360886024, 360879315, 360886207, 360875115, 360876170, 360884603, 360882477, 360885864,
   360881231, 360879877, 360885942, 360883243 (cuts), 354154204 (Puntledge), 360874940 (Pallant), plus the T753 lake-loop
   moves listed in `lake_loop_report.json`.
2. Binding diff, cut by cut: each changed rule or place is on one of those blks or is a T753 / eafa7b9f5 move (Redsand and
   Kitsumkalum Lake shrink); Puntledge r4 ~452 → ~527 m; Pallant's DFO place loses nothing (it starts at the bridge).
3. End cut / sliver: `sliver_gate.json` empty; no new piece under 5 m; watch Kitsumkalum CN bridge (m 22.5) and Exchamsiks
   (m 227) pieces; `straddle_gate` and `unplaced_gate` pass.
4. `apply --check`: 0 not landed except the 2 allowed T359 tidal waits.
5. The audit again with `--since <this run's json>`: I rows go, E Pallant row goes (now the tidal boundary).

## 6. QUESTIONS FOR THE OWNER

Default if unanswered = what stays in force today.

### Cuts in doubt
1. **Sheldens Creek, Spur 19 Bridge.** DFO: "upstream of white triangular fishing boundary signs located at the Spur 19 Bridge
   site." You marked the cut wrong ("completely the wrong spot?"). Today it is the forest road crossing 290 m below the top of
   an unnamed line we assumed is Sheldens Creek. Options: (a) you point at the bridge on the map; (b) a hunt for the other
   road ("SP 19H"); (c) leave the place unplaced. Recommend (a). Default: the disputed point stays in force.
   Ref: `6:sheldens-creek:upstream-of-white-triangular-fishi`, marks 2470/2517, cut `sheldens_creek__spur_19_bridge_site`.
2. **Fraser, signs about 4 km below Lillooet.** DFO: "…to the fishing boundary signs … approximately 4 km downstream of the
   town of Lillooet." The new cut is 3.6 km below the Seton mouth. If "the town" means its south edge, the signs are ~2 km
   lower. Options: keep / move 2 km lower / you place it. Recommend keep (no sign data). Default: built as saved.
   Ref: `split-fraser_river_signs_4_km_below_lillooet`, mark 2570.
3. **Puntledge, tailrace "50 m upstream" signs.** The cut is about 27 m below where 50 m above the real tailrace outlet falls.
   Options: move it 27 m up (the closure grows to ~553 m) / keep. Recommend move. Default: keep.
   Ref: `puntledge_river__fishing_boundary_signs_50_m_upstream_of_bc_hydro_puntledge_generating_station_tailrace`.
4. **Fraser at the Seton River.** DFO: "from the confluence with the Seton River downstream …". The Seton mouth and the Hwy 99
   bridge are 254 m apart. Options: keep a separate mouth cut (the 254 m piece has no DFO place) / reuse the bridge cut.
   Recommend keep the mouth cut. Default: the mouth cut is built.
   Ref: `split-fraser_river_seton_river`, `fraser_river__hwy_99_bridge_at_lillooet`.
5. **Atnarko, sign near Tweedsmuir Park Lodge.** DFO does not say where the sign is; the cut is at the lodge's viewing
   platform, 2.3 km above the Talchako. Options: keep / you place it. Default: keep. Ref: mark 2584.
6. **Gitnadoix, "upstream of powerline crossing near river mouth".** The power line does not cross the mapped river (it passes
   ~360 m north of the mouth), so the place is the whole river. Options: keep whole / you place a point. Default: whole.
   Ref: mark 2736 (was 2678).
7. **Kitsumkalum, "downstream of railway bridge".** The bridge is 22 m above where the mapped line ends at the Skeena, so this
   place is a 22 m piece. Options: keep / the place is "whole" below the bridge to the Skeena (same thing) / you say the bridge
   is somewhere else. Default: keep. Ref: mark 2724.
8. **Morice, "from Gosnell Creek to Lamprey Creek".** Today it starts at the signs 100 m below Gosnell Creek. Starting at the
   Gosnell Creek confluence itself puts the Thautil's mouth inside the stretch, and with tributaries on, the whole Thautil and
   Gosnell basin (1,653 more pieces) joins it. Options: (a) keep the signs point; (b) the confluence, tributaries on (the big
   basin); (c) the confluence, tributaries off. Recommend (a) or (c). Default: (a). Ref: marks 2591 / 2737 / 2739.
9. **Nanaimo, "from 500 m below the lower power line at Bore Hole".** The place starts AT the power line, so the 500 m below is
   missing (it carries the other wording's stretch). Options: a new cut 500 m below the power line (needs the rebuild) / keep.
   Recommend the new cut. Default: keep. Ref: mark 2442, hunt `nanaimo_borehole_d500`.
10. **Little Campbell and Babine signs cuts with distances the words do not print.** Little Campbell "downstream of a line between
    2 fishing boundary signs … to the pedestrian bridge" uses a "200 m upstream" signs cut; Babine "from the Nilkitkwa
    confluence upstream to the fishing boundary signs below the Babine Fence" uses the "80 m below the fence" cut. Both look right
    to me (the signs are where those cuts are). Options: keep / plain landmark cut. Default: keep. Ref: marks 2538, 2490.
11. **Sproat, move the signs cut 50 m to the bottom of the pool** (your own earlier note). Not done. Options: move / keep.
    Default: keep. Ref: mark 2468.

### Readings
12. **Skeena, "from Lakelse River mouth upstream to the Skeena River Overpass (New Hwy Bridge) at Ferry Island".** You are right:
    there is no confluence cut for the Lakelse on the Skeena main line; the suggestion puts the Lakelse mouth on a side line.
    Options: (a) a new standing Lakelse → Skeena confluence cut on the main line plus the Dudley Little (Hwy 16) bridge cut, then
    you mark it; (b) leave it (the place is dormant). Recommend (a) with the next rebuild after you mark it. Default: unplaced.
    Ref: `6:skeena-river:from-lakelse-river-mouth-upstream`.
13. **Skeena, "from the confluence with the Babine River downstream to the CNR Railway Bridge at Terrace".** There is no Babine →
    Skeena confluence cut; the suggestion ends at the gauge "above the Babine", which is the other wording's "a point above the
    confluence". Options: (a) a new Babine → Skeena confluence cut, then you mark it; (b) accept the gauge point. Recommend (a).
    Default: unplaced. Ref: `6:skeena-river:from-the-confluence-with-the-babin`.
14. **Ecstall, "upstream of fishing boundary signs near confluence with Johnston Creek".** The new cut is the Johnston Creek
    confluence itself (a proper confluence cut). OK, or do the signs sit away from the mouth? Default: the confluence.
    Ref: mark 2671.
15. **Illiance and Wilauks, "upstream of signs near mouth".** Saved as the whole river from the tidal boundary (the mouth), like
    Kitsault. Say if you know the signs are higher up. Default: whole. Ref: marks 2734, 2735.
16. **Thompson below Kamloops Lake: the three year-round no-fishing areas (Deadman, Juniper, Ashcroft) are not cut out** of the
    live place; your "wrong" sits on the dormant twin. Options: carve them out (the closed-area reader lane) / keep. Recommend
    carve out. Ref: `3:thompson-river:from-kamloops-lake-outlet-downstre~2`, mark 2521.
17. **Fraser Region 3 "mainstem waters", marked wrong**: the extent is the Region 3 mainstem; your note may have been about a
    label. The side channels you named (Seabird Island) are in Region 2 and now come in through the channel lane (T755).
    Re-tick or say what is wrong. Ref: marks 2577/2578.
18. **Skeena Hell's Gate.** It is the cut "Kitsumkalum River → Skeena River (1500 m upstream)", the Classified Waters boundary;
    it has always been there under that name. Rename its label to "top of Hell's Gate (1.5 km above the Kitsumkalum)"? Default:
    no rename. Ref: `skeena_river__kitsumkalum_river_into_skeena_river_u1500m`.

### Places to re-mark
19. Morice "chinook only" species words inside the place text (mark 2596): retire the row or re-mark. Default: as is.
20. Seymour (dam to the tidal boundary) and Nass ×2 (Meziadin): no extent yet; a mark is needed. Default: unplaced.
21. Puntledge "from the weir at the lower hatchery": dormant, no mark; the weir cut and the 75 m cut are ready.
22. After the rebuild: re-tick Puntledge marks 1263, 2447, 2687 (the 75 m cut moved).

### Unmarked places
23. 37 active places are still unmarked (audit section J, by region), plus the dormant Skeena pair in Q12/Q13, Inch Creek and the
    Babine Lake 400 m pair. Mark them, or say which ride a marked place. Default: they stay as the files have them.

Reference: audit before/after JSON in the session scratchpad; T743 ticket.
