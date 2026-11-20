# Fraser side channels, Redsand, narrowest-wins checks, the nine cuts, group marks (2026-11-15, T743)

Read-only on marks and curated data: nothing here changed `marks.jsonl`, `dfo-suggestions.json` or `data/curated/**`.
Bindings are computed on the live atlas (`data/generated/atlas/full`) with the reach run's own calls
(`entries.to_reach_input` + `reach.build.build_reach`, `regional=False`; tidal subtraction not applied) on the places
files as they are now. River km = FWA route measure on the Fraser main line (blk 356364114). Scripts:
`/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/51a6025f-10ca-4232-b730-e92b1263ce2f/scratchpad/fch/`.

## 1. The Fraser's side channels (owner: "make sure that when i checked fraser as water for the mainstem items it includes all channels too. like sea bird island etc.")

**How the atlas attaches them.** FWA codes every Fraser channel with the Fraser's own watershed code (`wsc 100`):
300 stream sections on the whole river. Three kinds:
- **Unnamed channels** inherit the Fraser's name (`NameSource.side_channel`, gnis 39325) and are IN the Fraser item
  `gnis:39325` (244 sections). Mission → Spuzzum: 29 channel sections, 57.8 km, beside the 12 mainstem sections
  (120.5 km). Both "upstream of the CPR Bridge" marks (item-restricted or not) already hold all 29. Above Spuzzum
  every channel is in the item (183 sections, Regions 3/5/7A): nothing is dropped there.
- **Channels a curator named** (`name_variants.json`, source `regulation`, from the synopsis text) have no gnis, so
  `registry.build.split_distinct_names` moves each to its own item `blk:<lowest blk>`. These are in NO item of the DFO
  water record `2:fraser-river`, so **every** Fraser DFO place drops them, restricted to `gnis:39325` or not.
  This is the Seabird Island gap.
- **Gazetted channels and sloughs** have their own gnis item. The water record `2:fraser-river` lists 12 of them
  (match note: "named channels and sloughs without their own regulation entries"); Nicomen and Strawberry sloughs are
  left out on purpose (own entries). An item-restricted mark (`between[gnis:39325]`) drops the listed ones.
- `topology.order_parts` only orders an item's parts for the app (a side part goes after the main-stem part it runs
  beside); it never decides membership.

**The 41 vs 50 (19.5 km).** The 9 sections the restricted pair drops are Maria Slough (5 stream pieces + 3 lake
polygons, 14.28 km) and the gazetted Jeperson Side Channel line (gnis:11481, 5.28 km). Seabird Island North, Herrling
Island and the rest of Jeperson are not in that 9: neither binding has them.

**Mission (km 76.5) → Spuzzum (km 197.0): every channel not in `gnis:39325`**

| channel | item | sections | km long | river km | in "~2" (unrestricted) | in restricted | own DFO place / synopsis | recommendation |
|---|---|---|---|---|---|---|---|---|
| Herrling Island Side Channel | `blk:355992187` (named by curation) | 7 | 12.24 | 126.6–131.8 | no | no | none / named only in the Fraser entry's r4 closure (May 15–Jul 31) | **the Fraser's own channel: belongs with the mainstem wording** |
| Seabird Island North Side Channel | `blk:355991777` (named by curation) | 4 | 4.74 | 134.8–137.2 | no | no | none / Fraser r4 | **the Fraser's own channel: belongs with the mainstem wording** |
| Jeperson Side Channel (braids) | `blk:355994562` (named by curation) | 5 | 3.25 | 113.2–117.8 | no | no | none / Fraser r4 names only `gnis:11481` | **the Fraser's own channel: belongs with the mainstem wording** |
| Jeperson Side Channel (gazetted line) | `gnis:11481` | 1 | 5.28 | 113.1–118.0 | yes | no | none / Fraser r4 | **belongs with the mainstem wording** (same channel as the row above) |
| Maria Slough | `gnis:13499` (+3 lake polygons) | 8 | 14.28 | 126.2–138.1 | yes | no | none / none (in the Fraser water record) | **owner's call (a slough, "one by one")**: DFO has no place of its own; suggested with the mainstem wording, since the water record already counts it as the Fraser's |
| Nicomen Slough (its Fraser-coded part) | `gnis:21105` | 16 (12 Fraser-coded) | 24.14 | 86.5–104.5 | no | no | **2 DFO places** (Siddle Ck → Fraser, Hwy 7 Dewdney → Fraser) / synopsis `r2:nicomen_slough@2-8` | **separate water** (as now) |
| Strawberry Slough | `gnis:16210` | 2 | 4.20 | 86.8–89.0 | no | no | none / synopsis `r2:strawberry_slough@2-8` | **separate water** (as now; left out of the water record on purpose) |

Below Mission (tidal, owner 2026-11-11: the Fraser's own channels below its tidal boundary are tidal): Morey, Cannery,
Williamson, Gilmour, Tilbury, Annacis, Sapperton, Parsons, Bedford, Enterprise (all in the water record) and 20
unnamed Fraser item sections (km 0–78.5). No Mission–Spuzzum extent reaches them.

**What would make "Fraser as the water" hold all its channels** (proposals, not done):
1. Water record `2:fraser-river`: add `blk:355992187`, `blk:355991777`, `blk:355994562` (curated, via the review
   app's DFO water binding, owner's yes). Measured: the unrestricted extent then holds them (+20.2 km).
2. Item-restricted marks (`between[gnis:39325]`): for the review-app lane, an item restriction to a river should
   also take the items of the water record that hold the river's own channels (nodes that inherit its gnis as a
   side-channel name). Or the owner re-marks the mainstem wordings without the item restriction. Either way the
   Maria Slough call comes first, because the water record holds it.
3. Synopsis side (another finding): the Fraser entry's r4 "No Fishing in the Jesperson's Side Channel …" binds
   `gnis:11481` only and misses `blk:355994562` (3.25 km of the same channel).

**Province-wide.** I re-bound all bound DFO places with the item restriction lifted and with the river's own split-out
channel items added. 6 places on 4 waters drop sections of the river's own channels:

| water | places | channel dropped | km | verdict |
|---|---|---|---|---|
| Fraser (R2) | `…upstream-of-the-cp` (dormant, restricted), `…upstream-of-the-cp~2` (live) | Herrling, Seabird N, Jeperson (both items), Maria Slough | 39.8 restricted / 20.2 unrestricted | **real gap** (table above). Nicomen + Strawberry (28.3 km) are correctly out |
| Squamish (R2) | `squamish-river-including-powerhous:whole` (live) | Squamish Powerhouse Channel `blk:360219234` | 2.97 | **real gap**: DFO's own name says "including Powerhouse Channel"; water record holds only `gnis:25671` |
| Stave (R2) | `downstream-of-b-c-hydro-dam-to-the` (+ `~2`, live) | Ruskin Spawning Channel `blk:355993381` | 2.56 | right to drop: the live wording excepts Ruskin, which is its own place |
| Quinsam (R1) | `quinsam-river:whole` (live) | Quinsam River Diversion Headpond `blk:354088905` | 0.69 | right to drop: a diversion, not a channel |

That is every case (fewer than ten). Item restrictions elsewhere (Alouette / North Alouette, Chilliwack / Vedder,
Tatshenshini) drop other named rivers of a multi-item water record on purpose; those are not channels.

## 2. Redsand Lake (mark 2625: `whole[wbk:-1]` with tributaries, 2,639 sections / 3,086 km)

- `wbk:-1` is a hand-drawn lake (`data/curated/waters/added_lakes.geojson` feature 1, 40 ha): FWA and OSM have no
  Redsand Lake polygon. It sits on the Kitsumkalum River mainstem about 2 km below Kitsumkalum Lake. The DFO water
  matched it by exact name. **The binding and the mark are right**; nothing to re-mark.
- Cause: the tributary walk. `reach.build._through_river_with_the_lake` (T468) gives a lake row the river flowing
  through it when that river has "no row of its own". A DFO place is resolved with no table of rows, so every
  through-river looks unrowed: Redsand took the Kitsumkalum above it and, through Kitsumkalum Lake, the whole upper
  basin. The same gap D5 (T679) closed for confluence cuts.
- General? Re-bound all 261 bound DFO places: exactly two change, Redsand Lake (2,639 → 4 sections, 3.0 km: the lake +
  2 unnamed inflows) and Kitsumkalum Lake (2,626 → 983 sections, 3,070 → 992 km, loses the Kitsumkalum River above
  it, which has DFO places of its own). Synthetic ids are not the defect: the registry has 17 curated `wbk:-N` waters;
  the only DFO ones are `6:redsand-lake` and its place; synopsis rows on them (Cheam Lake `-4`, Marsh Pond `-2`,
  Minnekhada `-5`, Kootenay Lake parts `-20..-22`, Nation Arm/Davis Bay/zones `-16..-19`, Shannon Lake `-23`/`-15`,
  Hall Road Pond `-3`) are untouched by the fix.
- Fix landed, **eafa7b9f5** (unpushed): `DFO_THROUGH_RIVER_STAYS_OUT`: a DFO lake place's through-river stays out
  unless DFO's words name it; synopsis lake rows unchanged (Powell Lake keeps the Powell River). POLICY_VERSION 38 →
  39. Tests `test_lake_through_river.py` (+4, mutation pin), `test_reach_builder.py`. The 11-13 reach run had no such
  join, so **no shipped binding moves**; the two lakes come right at the next reach run.
- Owner question (batched): should a DFO lake ever take its through-river when DFO's words do not name it (a lake
  whose river has no DFO place anywhere, like Powell)? As landed: never.

## 3. "Narrowest wins" spot checks (current marks, `salmon.chain_order`)

The chain order on a section: tier, then **fewer sections first**, then id. `verdict_on` takes, per species and day,
the first place in that order with a rule for it that day; a place silent that day passes to the next (wider) one.

| water | chain on its sections (sections per place) | what a reader gets | verdict |
|---|---|---|---|
| Tahltan (25 sections) | Tahltan incl. tribs (2,594) > Stikine incl. tribs (69,100) | Jun 1–Aug 31: Tahltan's finfish closure (all salmon). Other days the Tahltan says nothing, so the Stikine's rows speak (chinook non-retention, coho 4/day) | **right** (the Tahltan is in "Stikine including tributaries"; its own rule wins where it speaks) |
| Nakina (54) | Nakina whole (54) > Taku incl. tribs (23,360); the dormant centre-line piece (1) first on its section | Jul 20–Aug 15: Nakina's chinook + coho closures. Sockeye always, and chinook/coho on other days: Taku's (sockeye and chinook no fishing, coho 4/day) | **right**. Note: the Nakina place has no tributaries (DFO's words give none), so the Nakina's tributaries get the Taku's rows only |
| Tatsatua (18) | Tatsatua incl. tribs (886) > Taku (23,360) | Aug 20–Sep 15: no fishing for salmon; other days the Taku's | **right** |
| Nitinat parts | bridge ±50 m (1) / powerline → Little Nitinat (1) / Red Rock ±100 m (1) > downstream of Parker (10); upstream of Parker (3) alone | Each listed part's finfish closure wins on its section in its dates (bridge Aug 25–Oct 15, powerline all year, Red Rock Aug 25–Dec 31); outside those dates "downstream of Parker" speaks (its coho Oct 16–Dec 31 etc.) | **right**: "except in the areas AND TIMES described below" is exactly this fall-through. No rest extent needed |
| Little Shuswap Lake | no place today (South Thompson's mark waits on the Campbell Creek cut; the lake is not an item of the water record) | today: unlisted water, all salmon closed | **waits on the cut**: once built, the mark's `whole[wbk:329534136]` puts the lake in the South Thompson place (chinook, coho, pink, sockeye rows) |
| Atnarko (42) | Atnarko whole incl. tribs (5,006) > Bella Coola "all tributaries" (10,048) > Bella Coola whole (10,048) | Atnarko's own rows every day: its table covers all five species all year, so the Bella Coola never answers on the Atnarko | **right today**, by the cascade, not by an exclusion. The "not including Atnarko" exclusion (review-app lane) makes it clean if the Atnarko table ever leaves a gap. The lower Atnarko's coho opening (2584) **waits on the Tweedsmuir signs cut** |

**Wrong, found on the way: Bella Coola mainstem chinook.** "All tributaries to the Bella Coola River" (2583, his note
"this one should be tributaries only?") is bound `whole[gnis:11611]` with tributaries, so it covers the mainstem too.
It ties the "whole" place at 10,048 sections and wins the tie by id ("all-…" sorts before "whole"). On the Bella
Coola mainstem, **chinook reads "no fishing"** where DFO's whole-river row gives 4 a day (1 over 65 cm). Fix: that
place `tributaries_only: true` (a re-mark / the review-app lane). Not in the shipped release (the mark is from last night).

## 4. The nine new cuts (waiting on an atlas rebuild)

OSM by Overpass (nearest way, to the segment); river km by snapping to the FWA streams layer. Proposal ids are as the
triage lists them (no correction needed). T664/T665 are already promoted (f5831c1ac): do NOT re-apply
`T664-pending/*` patches.

| proposal | cut | coords | river km | OSM check | water / side of cuts | verdict |
|---|---|---|---|---|---|---|
| `split-fraser_river_highway_1_bridge_at_hope_bi` | `fraser_river__highway_1_bridge_at_hope_bill_hartley_fraser_hope` | -121.452186, 49.386489 | 158.75 | 32 m from Trans-Canada Hwy bridge, way 30689321 | Fraser; between CPR Mission (76.5) and Spuzzum (197.0) | ok |
| `split-fraser_river_alexandra_bridge_hwy_1` | `fraser_river__alexandra_bridge_hwy_1` | -121.409692, 49.701209 | 201.24 | 36 m from the Hwy 1 bridge, way 30689318; the old suspension bridge (way 132578983) is 826 m upstream | 4.27 km above Spuzzum; 7.9 km below the Hell's Gate cut (209.1) | ok: owner 11-15 "this is fine" for the 4.3 km; reads "the Alexandra Bridge" as the Hwy 1 bridge |
| `split-fraser_river_seton_river` | `fraser_river__seton_river` | -121.926856, 50.681805 | 329.13 | the FWA Seton mouth; Hwy 99 bridge (way 30492346) 263 m away | 255 m below the curated Hwy 99 cut (329.38) | ok, keep it: DFO says "from the confluence"; reusing the bridge cut widens the place 255 m. The piece is real water no DFO place covers (recommendation in the ticket, awaiting his yes) |
| `split-fraser_river_signs_4_km_below_lillooet` | `fraser_river__signs_4_km_below_lillooet` | -121.904312, 50.655955 | 325.51 | no OSM feature for signs; 3.6 km below the Seton mouth | the tail-race cut (327.9) falls inside the place | **second look**: which edge of town is "the town of Lillooet" (south edge would put it ~2 km lower) |
| `split-south_thompson_river_campbell_creek_d100` | `south_thompson_river__campbell_creek_d100m` | -120.078674, 50.660036 (the mouth) | mouth 19.74, cut ~19.64 | 28 m from Campbell Creek, way 102241422 | South Thompson; offset on the downstream side, as worded | ok |
| `split-quesnel_river_johnson_subdivision_bridge` | `quesnel_river__johnson_subdivision_bridge` | -122.489466, 52.973025 | 0.83 | 4 m from OSM "Johnston Bridge", way 42469393; 26 m from DFO's own coordinate | Quesnel; nothing below Morehead Ck cut (85.7) | ok |
| `split-quesnel_river_powerlines` | `quesnel_river__powerlines` | -122.496669, 52.996503 | 4.33 | 36 m from power line, way 204959015; 29 m from DFO's coordinate | 3.5 km above the Johnson bridge | ok (DFO's "just upstream of Hwy 97 bridge" does not fit, nearest Hwy 97 is 1.75 km off; DFO's coordinates rule) |
| `split-atnarko_river_signs_near_tweedsmuir_park` | `atnarko_river__signs_near_tweedsmuir_park_lodge_stuie` | -126.073265, 52.370657 | 2.34 (km 0 = Talchako junction) | 61 m from the lodge viewing platform, way 1213126424 | Atnarko; no other Atnarko cut | ok, medium: DFO does not publish the sign's place; the hunt's point is 259 m off |
| `split-cowichan_river_skutz_falls` | `cowichan_river__skutz_falls` | -123.955626, 48.783139 | 34.21 | 0 m from the OSM waterfall node 667290278 | between Mile 66 trestle (30.5) and CNR mile 70.2 (41.4) | ok |

**Second looks outside the nine:** Morice 2591 ("from Gosnell Creek to Lamprey Creek") uses
`morice_river__gosnell_creek_into_morice_river_d100m` (km 75.07, the signs cut of the other wording 2595/2596); the
confluence cut `…gosnell_creek_into_morice_river` (75.17) fits its words. Morice 2592 ("from the confluence of the
Bulkley and Morice") uses `…bulkley_confluence_u100m` (0.1, the Forks signs); `…bulkley_confluence` (0.0) fits. Both
are re-marks on existing cuts, no rebuild needed. Withdraw a third hunt duplicate too: `fraser_lillooet_4km` (9 m
from the Lillooet proposal).

### Ready-to-run sequence (NOT run; after the owner says he is done labelling)
Times: atlas 32 min, reach 12–13 min, full build ~31 min (reach 12, tiles 4, deliver 15), from `next-rebuild-results.md`
and the 11-13 build.
```
SIDE=$PWD/data/generated/side/dfo-cuts9; rm -rf $SIDE; mkdir -p $SIDE/logs
# 0 (2 min) checks
rtk git status; node app/tools/check-tickets.mjs; scripts/fast-fail.sh; scripts/heavy.sh --status
# 1 (1 min) proposals from the final marks, then ONE side copy with all nine
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.proposals build
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.proposals side split-fraser_river_highway_1_bridge_at_hope_bi,split-fraser_river_alexandra_bridge_hwy_1,split-fraser_river_seton_river,split-fraser_river_signs_4_km_below_lillooet,split-south_thompson_river_campbell_creek_d100,split-quesnel_river_johnson_subdivision_bridge,split-quesnel_river_powerlines,split-atnarko_river_signs_near_tweedsmuir_park,split-cowichan_river_skutz_falls --out $SIDE
export CURATED_ROOT=$SIDE/curated          # the root the side command printed
# 2 (<1 min)
.venv/bin/python -m pipeline.tools.audit.check_curated --waters                    # 0 errors
# 3 (~32 min, HEAVY)
scripts/heavy.sh atlas-side 50 -- .venv/bin/python -u -m pipeline.atlas.build --full --out $SIDE/atlas/full_next > $SIDE/logs/atlas.log 2>&1
# 4 (~3 min) parity: new boundary ids = the 9 cut ids only; changed sections only on blks 356364114, 356361760,
#   356363937, 360879335, 354155148; no lost boundary (--accept-boundary-loss must NOT be needed)
.venv/bin/python -m pipeline.atlas.promote $SIDE/atlas/full_next --dry-run > $SIDE/logs/parity.txt
.venv/bin/python -m pipeline.atlas.inputs --build $SIDE/atlas/full_next
# 5 (~13 min, HEAVY) rules keep their water, new ids where a cut split a section; read by place. Also expect the
#   eafa7b9f5 change: 6:redsand-lake and 6:kitsumkalum-lake shrink (section 2 above), POLICY_VERSION 39
scripts/heavy.sh reach-side 50 -- env PYTHONPATH="$PWD" .venv/bin/python -m pipeline.atlas.reach.cli --build $SIDE/atlas/full_next --out $SIDE/reach > $SIDE/logs/reach.log 2>&1
.venv/bin/python -m pipeline.atlas.reach.binding_diff data/generated/reaches/full $SIDE/reach --json $SIDE/binding_diff.json
# 6 STOP: owner go
# 7 (~3 min) promote, WITHOUT CURATED_ROOT
unset CURATED_ROOT; SC=<scratch>; mkdir -p $SC/backup; cp data/curated/waters/cuts.json $SC/backup/
for p in data/generated/review/proposals/split_{fraser_river_highway_1_bridge_at_hope_bi,fraser_river_alexandra_bridge_hwy_1,fraser_river_seton_river,fraser_river_signs_4_km_below_lillooet,south_thompson_river_campbell_creek_d100,quesnel_river_johnson_subdivision_bridge,quesnel_river_powerlines,atnarko_river_signs_near_tweedsmuir_park,cowichan_river_skutz_falls}.patch; do git apply "$p"; done
diff data/curated/waters/cuts.json $SIDE/curated/waters/cuts.json        # empty
.venv/bin/python -m pipeline.atlas.inputs --build $SIDE/atlas/full_next
.venv/bin/python -m pipeline.atlas.promote $SIDE/atlas/full_next
rtk git commit -m "…nine DFO cuts…" -- data/curated/waters/cuts.json
# 8 (~5 min) land the 12 waiting DFO marks (one commit each), then the check
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.apply dfo --dry-run
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.apply dfo --side-checked mark-dfo-2_fraser_river_mainstem_waters_downstream_of_the mark-dfo-2_fraser_river_from_the_downstream_side_of_the_cp mark-dfo-2_fraser_river_from_the_downstream_side_of_the_cp_2 mark-dfo-2_fraser_river_mainstem_waters_from_the_downstrea mark-dfo-2_fraser_river_upstream_from_the_highway_1_bridge mark-dfo-2_fraser_river_from_the_highway_1_bridge_at_hope mark-dfo-3_fraser_river_in_that_portion_of_the_fraser_rive mark-dfo-3_south_thompson_river_from_the_green_can_buoy_near_outle mark-dfo-5a_quesnel_river_that_portion_of_the_quesnel_river mark-dfo-5a_quesnel_river_between_the_johnson_subdivision_br mark-dfo-5b_atnarko_river_downstream_of_the_fishing_boundary mark-dfo-1_cowichan_river_from_cnr_trestle_at_mile_70_2_to_s
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.apply --check                           # only the 2 T359 tidal keys
# 9 (~3 min) withdraw the hunt duplicates (needs the promoted cut ids), then regenerate suggestions — only after the
#   suggest.py lane has landed. Edit data/generated/review/dfo-split-hunt.json as T532 did (1975b46b3): delete cuts
#   fraser_hope_bridge, fraser_alexandra_bridge, fraser_lillooet_4km; in their Fraser locations replace {"cut": X}
#   with {"split": "<new curated id>", "confidence": "high", "note": "…"}
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.dfo_salmon.suggest
# 10 (~31 min, HEAVY) then the release steps as usual
scripts/heavy.sh build 50 -- .venv/bin/python -m pipeline build
```
Ids to confirm at step 1: the proposal list is from a marks snapshot of 2026-11-15; re-run `proposals build` on the
final marks and use its ids if any changed. The Qualicum mark (`mark-dfo-1_qualicum_river_all_open_portions_of_the_qualicum`)
was refused and waits on the app fix, not on a cut.

## 5. The 12 older group-written marks (8 places)

Each pair of lines in a group has one timestamp (one click). Extents read against the places files and cuts as they are.

| line | place | wording (short) | extent saved | written from | verdict | reason |
|---|---|---|---|---|---|---|
| 2442 | 1:nanaimo `from-500-m-below-the-lower-power-l` (live) | from **500 m below** the lower power line at "Bore Hole" up to the Boulder Creek mouth signs | between(power line at Bore Hole → Boulder Ck) on gnis:16505 | itself (head) | **looks wrong** | the power-line cut is a plain point with no offset, so the place stops at the power line and misses the 500 m below it that DFO includes; it carries the member wording's (2443) extent. Needs a cut 500 m below the power line |
| 2443 | 1:nanaimo `from-the-power-line-crossing-at-bo` (dormant) | from the power line at "Bore Hole" up to Boulder Creek | same | head 2442 | looks right | both ends match |
| 2479 | 2:chilliwack `…fishing-boun~3` (live) | 100 m below Slesse downstream, incl. the Sumas from Barrow Town Pump Station | Chilliwack + Vedder below the Slesse signs, no Sumas | itself | wrong, superseded by 2500 | Sumas missing |
| 2480 | 2:chilliwack `…fishing-boun` | same reach ("100 m from") | as 2479 | head ~3 | wrong, superseded by 2501 | Sumas missing |
| 2481 | 2:chilliwack `…fishing-boun~2` | same + "except the closure area identified above" | as 2479 | head ~3 | wrong, superseded by 2502 | Sumas missing |
| 2482 | 2:chilliwack `upstream-from-a-line-between-2-fis~2` | "upstream from … downstream including the Sumas" | as 2479 | head ~3 | wrong, superseded by 2503 | Sumas missing |
| 2500 | ~3 | as 2479 | + Sumas below `sumas_river__barrow_town_pump_station` (built) | itself | looks right | both parts drawn (the Sumas runs to its mouth, not to "signs near the confluence": small) |
| 2501 | `…fishing-boun` | as 2480 | as 2500 | head ~3 | looks right | same reach |
| 2502 | `…fishing-boun~2` | as 2481 | as 2500 | head ~3 | looks right, caveat | the excepted Vedder Crossing closure is not subtracted; right only while the narrower Vedder Crossing place (dormant) wins on its sections, which `chain_order` does |
| 2503 | `upstream-from-a-line-between-2-fis~2` | as 2482 | as 2500 | head ~3 | looks right | "upstream" is DFO's slip: same open rows and Sumas part as the downstream reach (2024-09-09 page); the truly upstream wording (no ~2) is separate and marked by hand (2483) |
| 2525 | 2:stave `the-ruskin-spawning-channel-on-the` | Ruskin Spawning Channel, east bank, inlet to boat ramp | whole(blk:355993381) | itself | looks right | the registry's Ruskin Spawning Channel item (4 sections) |
| 2526 | 2:stave `that-portion-of-the-stave-river-kn` | "that portion of the Stave River known as the Ruskin Spawning Channel" | same | head 2525 | looks right | same place |

Per place: Nanaimo "500 m below the power line" **wrong** (the only group save that put one wording's stretch on
another); Nanaimo "from the power line" right; the four Chilliwack-Vedder places right now (2500–2503 replaced the
Sumas-less 2479–2482); both Stave places right. Not checked: whether DFO's "lower power line" is the crossing the
`power_line_at_bore_hole` cut sits on (no OSM check).
