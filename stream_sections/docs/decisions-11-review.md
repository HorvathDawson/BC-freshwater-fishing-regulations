# decisions-11 — review plan (subagent pass, 2026-08-04)

Companion to `decisions-11-likely-na.json`. The 165 human-review verdicts were folded back into
`14-locators-to-curate.json`. **57 clean ones are already applied** (commit — 49 not_a_split→n/a,
6 defer→deferred, +2 lake reaches held back). The remaining **110** were reviewed by 4 subagents
(propose-only). Their per-row proposals are in **`decisions-11-plan.json`**; this memo is the
human view. `apply_now_safe` marks the high-confidence, no-judgment ones.

## 1. Ready to apply now — 51 rows (apply_now_safe)

**→ not_applicable (44)** — verified lake-edge auto-splits (bucket C) + confirmed whole-stream/note rows (bucket E). None needed an authored split.

- `alouette-river-b5a707` — ALOUETTE RIVER · E/na-with-siblings
- `ash-river-f11aa9` — ASH RIVER · C/auto_lake_edge
- `atnarko-bella-coola-rivers-includes-tributari-250ae0` — ATNARKO/BELLA COOLA RIVERS [Includes Tributaries] EXCE · E/na-with-siblings
- `big-bar-creek-ce0182` — BIG BAR CREEK · C/auto_lake_edge
- `bonaparte-river-98b62b` — BONAPARTE RIVER · C/auto_lake_edge
- `chilliwack-vedder-rivers-does-not-include-sum-3ab919` — CHILLIWACK / VEDDER RIVERS (does not include Sumas Riv · E/na-with-siblings
- `chimney-creek-04b2dc` — CHIMNEY CREEK · C/auto_lake_edge
- `chuckwalla-river-a0316a` — CHUCKWALLA RIVER · E/na-with-siblings
- `columbia-river-73fe32` — COLUMBIA RIVER · C/auto_lake_edge
- `cowichan-river-see-map-below-337c0e` — COWICHAN RIVER (see map below) · E/na-with-siblings
- `cowichan-river-see-map-below-bc238a` — COWICHAN RIVER (see map below) · E/na-with-siblings
- `davie-river-f757c8` — DAVIE RIVER · C/auto_lake_edge
- `deadman-river-aa98fb` — DEADMAN RIVER · C/auto_lake_edge
- `dean-river-65ebf2` — DEAN RIVER · E/na-with-siblings
- `eagle-river-a52a4f` — EAGLE RIVER · C/auto_lake_edge
- `findlay-creek-1b3bee` — FINDLAY CREEK · E/na-with-siblings
- `fraser-river-7c032d` — FRASER RIVER · E/na-with-siblings
- `fraser-river-upstream-of-the-cpr-bridge-at-mi-4460d2` — FRASER RIVER (upstream of the CPR Bridge at Mission) · E/na-with-siblings
- `guichon-creek-9a81b6` — GUICHON CREEK · C/auto_lake_edge
- `harrison-river-from-the-fraser-river-upstream-f8ecc6` — HARRISON RIVER (from the Fraser River upstream to Harr · C/auto_lake_edge
- `kitsumkalum-kalum-river-443a33` — KITSUMKALUM (Kalum) RIVER · E/na-with-siblings
- `kootenay-lake-main-body-for-location-see-map--ba2e51` — KOOTENAY LAKE - MAIN BODY (for location see map on pag · E/na-with-siblings
- `kootenay-lake-upper-west-arm-for-location-see-f597ff` — KOOTENAY LAKE - UPPER WEST ARM (for location see map o · E/na-with-siblings
- `liard-river-watershed-see-map-on-page-63-5f8021` — LIARD RIVER WATERSHED (see map on page 63) · E/na-with-siblings
- `mckinley-creek-9d6931` — MCKINLEY CREEK · C/auto_lake_edge
- `moberly-river-b2a8ec` — MOBERLY RIVER · C/auto_lake_edge
- `moyie-river-a098eb` — MOYIE RIVER · E/na-with-siblings
- `nahatlatch-river-0bb710` — NAHATLATCH RIVER · C/auto_lake_edge
- `nahmint-river-66b328` — NAHMINT RIVER · C/auto_lake_edge
- `nahmint-river-786f4c` — NAHMINT RIVER · C/auto_lake_edge
- `nanaimo-river-bd9e20` — NANAIMO RIVER · E/na-with-siblings
- `nicola-river-4b9f64` — NICOLA RIVER · C/auto_lake_edge
- `nicola-river-aba357` — NICOLA RIVER · C/auto_lake_edge
- `paul-creek-downstream-of-paul-lake-b809c0` — PAUL CREEK (downstream of Paul Lake) · C/auto_lake_edge
- `pennask-creek-c98c41` — PENNASK CREEK · C/auto_lake_edge
- `pitt-river-564cfe` — PITT RIVER · C/auto_lake_edge
- `seton-river-includes-bc-hydro-power-canal-ups-5f4cbe` — SETON RIVER (includes BC Hydro Power Canal upstream of · C/auto_lake_edge
- `seton-river-includes-bc-hydro-power-canal-ups-8eb4d1` — SETON RIVER (includes BC Hydro Power Canal upstream of · C/auto_lake_edge
- `seymour-river-db4eb4` — SEYMOUR RIVER · C/auto_lake_edge
- `shuswap-river-521556` — SHUSWAP RIVER · C/auto_lake_edge
- `silverhope-silver-creek-015132` — SILVERHOPE (Silver) CREEK · C/auto_lake_edge
- `thompson-river-upstream-of-kamloops-lake-e0c70b` — THOMPSON RIVER (upstream of Kamloops Lake) · C/auto_lake_edge
- `wap-creek-7d3df8` — WAP CREEK · E/na-with-siblings
- `whatshan-river-bcc368` — WHATSHAN RIVER · C/auto_lake_edge

**→ deferred (7)** — verified real within-lake cuts, unbuilt (docs/15):

- `dinosaur-lake-reservoir-downstream-of-w-a-c-b-4fc5f6` — DINOSAUR LAKE (Reservoir Downstream of W.A.C. Bennett: Dinosaur Lake reservoir reach (W.A.C. Bennett Dam <-> Peace Canyon Dam)
- `mara-lake-70c173` — MARA LAKE: south of the CPR bridge (Mara Lake)
- `pitt-lake-47a388` — PITT LAKE: north of the boundary-sign line (east/west shores) near the head of Pitt Lake
- `prudhomme-lake-south-of-the-hwy-16-bridge-eccc4b` — PRUDHOMME LAKE (south of the Hwy 16 bridge): Prudhomme Lake - south of the Hwy 16 bridge
- `rosemond-lake-061373` — ROSEMOND LAKE: south of the CPR Bridge (Rosemond Lake)
- `shumway-lake-02ae86` — SHUMWAY LAKE: north of the boundary-sign line at the south end of Shumway Lake
- `upper-arrow-lake-drawdown-area-f15974` — UPPER ARROW LAKE (drawdown area): Upper Arrow Lake - drawdown area (Hwy 1 bridge <-> Akolkolex Narrows power line)

## 2. Lake-internal splits — labeled (bucket D)

All deferred (lake subdivision unbuilt, docs/15). Method + label proposed; ⚑ = needs your call.

- ⚑ `fran-ois-lake-9c8274` [NA] — outlet of François Lake
- ⚑ `dinosaur-lake-reservoir-downstream-of-w-a-c-b-50210f` [NA] — Dinosaur Lake (Reservoir Downstream of W.A.C. Bennett Dam)
- ⚑ `great-central-lake-4ff0ab` [NA] — Stamp River, dam to signs ~50 m SW of Ash Main Bridge (Great Central Lake outlet)
- ⚑ `williston-lake-in-zone-a-includes-waters-500--456568` [buffer] — within 500 m east/upstream of Causeway Road (Williston Lake, Zone A)
- ⚑ `williston-lake-in-zone-a-includes-waters-500--7972e6` [buffer] — within 500 m east/upstream of Causeway Road (Williston Lake, Zone A)
- ⚑ `williston-lake-in-zone-a-includes-waters-500--3be97e` [buffer] — within 500 m upstream and downstream of Causeway Road (Williston Lake, Zone A)
-    `mara-lake-70c173` [line] — south of the CPR bridge (Mara Lake)
-    `rosemond-lake-061373` [line] — south of the CPR Bridge (Rosemond Lake)
-    `pitt-lake-47a388` [line] — north of the boundary-sign line (east/west shores) near the head of Pitt Lake
- ⚑ `premier-lake-895b0d` [line] — south of the boundary-sign line (Premier Lake)
-    `shumway-lake-02ae86` [line] — north of the boundary-sign line at the south end of Shumway Lake
- ⚑ `sakinaw-lake-df4750` [line] — easterly of the line from the Sakinaw Lake boat-launch sign
- ⚑ `chilko-lake-83dc6f` [polygon] — Big Lagoon, west side of Chilko Lake
- ⚑ `mahood-lake-see-map-on-page-28-for-area-closu-79ec2d` [polygon] — western tip of Mahood Lake, within fishing boundary signs (see map p.28)
- ⚑ `shannon-lake-netted-off-portion-on-the-south--c375a0` [polygon] — netted-off portion, south end of Shannon Lake
- ⚑ `shuswap-lake-see-maps-on-page-28-includes-lit-80ee82` [polygon] — Shuswap Lake named arms/portions (Seymour, Anstey, etc.) — multi-basin, see maps p.28
- ⚑ `alouette-lake-396262` [polygon] — swimming areas, Alouette Lake
- ⚑ `beaver-lake-6e12af` [polygon] — vague portion(s), Beaver Lake ('on parts')
- ⚑ `cowichan-lake-including-bear-lake-5a1513` [polygon] — vague portion(s), Cowichan Lake ('on parts')
- ⚑ `long-lake-nanaimo-5c3300` [polygon] — vague portion(s), Long Lake (Nanaimo) ('on parts')
- ⚑ `osoyoos-lake-29ce8a` [polygon] — 5 signed swimming areas, Osoyoos Lake
- ⚑ `brannen-lake-1539e6` [polygon] — vague portion(s), Brannen Lake ('on parts')

## 3. Split proposals — bucket F (29) — PREPARE-ONLY, nothing authored

**Ready-to-author (16)** — clean anchors, coords resolve in the joint pass:

- `lower-campbell-lake-s-tributaries-a724e1` [point] — Tributary-SET row itself is not-a-split. The real split lives on CAMPBELL RIVER mainstem: reg 'Campbell River between Strathcona Dam and (Lower) Campbell Lake' 
- `brunette-river-s-tributaries-aa1996` [two_boundary] — Trib-SET row not-a-split; two mainstem BRUNETTE RIVER regs: (1) 'No Fishing upstream of Burnaby Lake' => lake anchor/auto lake edge at Burnaby Lake; (2) 'from C
- `chemainus-river-79cc9a` [confluence] — Single confluence split. 'downstream of Bannon Creek' = one boundary at the Bannon Creek mouth; a confluence anchor cuts once and yields both the downstream and
- `granby-river-s-tributaries-465204` [confluence] — Trib-SET row not-a-split; real split is on GRANBY RIVER mainstem: 'Upstream of Burrell Creek' => single confluence split at Burrell Creek mouth. Set target:{blk
- `kitsumkalum-kalum-river-b6b706-a` [confluence] — 'from the outlet of Kitsumkalum Lake to Glacier Creek confluence': -a boundary = lake outlet (auto lake edge, no authored split). -b = Glacier Creek confluence 
- `kootenay-lake-s-tributaries-6b7979` [unclear] [reuse splits.json:kootenay_idaho_border] — Trib-SET row; USER_NOTE is an EXCLUSION not a new split: 'Does not include the Kootenay River upstream from Kootenay Lake to the U.S. border near Creston'. That
- `lardeau-river-e666ed-a` [offset] — 'downstream of fishing boundary signs at Trout Lake outlet, to fishing boundary signs approximately 600m downstream near ...': -a = Trout Lake outlet (auto lake
- `little-qualicum-river-496f12` [two_boundary] — 'All tributaries' is not-a-split (trib-set). Real split: 'No Fishing from the falls in Little Qualicum Falls Provincial Park downstream to the hatchery fence' =
- `meziadin-river-09af13-a` [two_boundary] — 'from fishing boundary signs at outlet of Meziadin Lake to Nass River' = the entire Meziadin River below the lake: bounded ONLY by the lake edge (up) and the na
- `salmo-river-s-tributaries-d8cd97` [two_boundary] — Trib-SET row not-a-split; real split on SALMO RIVER mainstem: 'Sheep Creek to South Salmo River' (C&R rainbow/bull trout) => two-boundary -a Sheep Creek conflue
- `salmon-river-4a0073-a` [two_boundary] — Multiple regs on SALMON RIVER (MU 1-10): (1) 'No Fishing upstream of Kay Creek' => single confluence split at Kay Creek mouth; (2) 'no powered boats upstream of
- `weaver-lake-and-weaver-creek-47d5a0-a` [two_boundary] — 'from fishing boundary signs at log booms on Weaver Lake downstream to where Sakwi Creek enters Weaver Creek': -a = Weaver Lake outlet at the log booms (lake ed
- `kootenay-river-downstream-of-idaho-border-c2529b` [two_boundary] [reuse splits.json:kootenay_idaho_border] — 'from Idaho border near Creston to Kootenay Lake': both boundaries already handled — authored split kootenay_idaho_border covers the border point, and Kootenay 
- `mcleod-river-c29823` [two_boundary] — 'excluding War Lake' is a not-a-split exclusion. Real split: 'Carp Lake to War Falls' => two-boundary -a Carp Lake outlet (lake edge, auto) + -b War Falls (fall
- `salmo-river-57c52d` [unclear] — 'Remainder of mainstem' is NOT a new boundary — it is the COMPLEMENT reach left over after the other Salmo River splits (e.g. Sheep Creek -> South Salmo, see sa
- `john-hart-lake-s-tributaries-36c501` [point] — 'channel downstream of Ladore Dam' => single point split at Ladore Dam (dam_weir_fence). The 'includes' clause is not-a-split. Auto-proposal has a rough candida

**Deferred to you (13)** — hard / ambiguous / parsing errors:

- ⚑ `elk-river-s-tributaries-see-exceptions-9149a8` — USER_NOTE only says 'exceptions have splits in them' without naming boundaries. Which tributary exceptions and their split wording are unspecified; ne
- ⚑ `fraser-river-upstream-of-the-cpr-bridge-at-mi-af0798` — Area/line bounded by fishing boundary signs ('area bounded by a line commencing at a fishing boundary sign at the eastern end of Landstrom Bar (Scale 
- ⚑ `john-hart-lake-s-tributaries-4e8025` — USER_NOTE 'need to look what channel is might be a split' — no boundary named. Likely overlaps the sibling row john-hart-lake-s-tributaries-36c501 ('c
- ⚑ `kitimat-river-angling-regulations-for-the-kit-eb7a2e` — Multiple splits AND regs are 'currently under review' per locator_text. USER_NOTE lists: west-half-of-river closure between boundary signs near Kitima
- ⚑ `kokish-river-02cef0` — Between-signs reach: upper sign at the IPP (independent power project) tail-race confluence, lower sign ~500 m downstream. Anchor A is a powerhouse ta
- ⚑ `lake-revelstoke-s-tributaries-96d2a3` — 'No Fishing from Mica Dam to fishing boundary signs at the narrows immediately downstream of the mouth of Bigmouth Creek': two-boundary -a Mica Dam (d
- ⚑ `premier-lake-s-tributaries-99ac9d` — USER_NOTE is a question: 'No Fishing south of fishing boundary signs on the lake shore — is this a trib or lake reg?'. Ambiguous whether it's a lake-s
- ⚑ `stave-river-c74d9d` — 'in the Northrop Spawning Channel, from the intake downstream to where the channel joins the Stave River main': a side/spawning channel (likely UNNAME
- ⚑ `trout-lake-s-tributaries-d2d6c5` — USER_NOTE 'this is awkward. split the lake node so tribs can be separated?' — the desired split is a division of the LAKE node itself so tributaries a
- ⚑ `upper-arrow-lake-s-tributaries-330f07` — Marked 'wrong' but USER_NOTE is EMPTY — no reg wording provided for the claimed split. Cannot name any anchor; needs Dawson to supply the split text.
- ⚑ `nahatlatch-river-1d1eaf` — 'Downstream of Nahatlatch Lake (including Hannah and Frances lakes; except as noted upstream of )': USER_NOTE says complex, 'we will need to go throug
- ⚑ `adam-river-except-eve-river-ca05b3` — 'except Eve River' normally = a tributary EXCLUSION (not a split). USER_NOTE questions whether it is actually 'upstream of Eve River (but not includin
- ⚑ `nahatlatch-river-a0ebb8` — locator_text 'except as noted upstream of' is the SAME truncated/broken clause as nahatlatch-river-1d1eaf. USER_NOTE: 'not sure what this means ... co

## 4. Everything else flagged needs_dawson

- ⚑ `canim-lake-see-map-on-page-42-de63b0` (C) — [human-review 2026-08-04 lake-verify] UNCONFIRMED: 'see map on page 42' is map-only. If page 42 shows the WHOLE lake -> not_applicable; if it shows a sub-region closure -
- ⚑ `williston-lake-in-zone-a-includes-waters-500--456568` (D) — [human-review 2026-08-04 lake-internal] buffer (line-buffer hybrid) per docs/15 §5b: within 500 m of Causeway Road, Williston Lake Zone A. Not a clean point-buffer — the 
- ⚑ `williston-lake-in-zone-a-includes-waters-500--7972e6` (D) — [human-review 2026-08-04 lake-internal] buffer (line-buffer hybrid) per docs/15 §5b: within 500 m of Causeway Road, Williston Lake Zone A. Duplicate of williston-lake-in-
- ⚑ `williston-lake-in-zone-a-includes-waters-500--3be97e` (D) — [human-review 2026-08-04 lake-internal] buffer (line-buffer hybrid) per docs/15 §5b: 500 m upstream AND downstream of Causeway Road, Williston Lake Zone A — this phrasing
- ⚑ `chilko-lake-83dc6f` (D) — [human-review 2026-08-04 lake-internal] polygon (Tier-2, named bay) per docs/15 §5c: Big Lagoon, west side of Chilko Lake. 'West side of lake' reads as descriptive of the
- ⚑ `fran-ois-lake-9c8274` (D) — [human-review 2026-08-04 lake-internal] NOT lake-internal. Existing row note already says '[auto-lake-split] Bounded only by lake edge(s)/mouth ... NO authored split need
- ⚑ `mahood-lake-see-map-on-page-28-for-area-closu-79ec2d` (D) — [human-review 2026-08-04 lake-internal] polygon (Tier-2, map-only) per docs/15 §5c/§7: western tip of Mahood Lake, within fishing boundary signs; name_verbatim itself say
- ⚑ `premier-lake-895b0d` (D) — [human-review 2026-08-04 lake-internal] line per docs/15 §5a: south of the boundary-sign line on the lake shore (Premier Lake). Locator text names only one shore ('on the
- ⚑ `shannon-lake-netted-off-portion-on-the-south--c375a0` (D) — [human-review 2026-08-04 lake-internal] polygon (Tier-2, authored enclosure) per docs/15 §5c: netted-off portion, south end of Shannon Lake. Boundary is a physical net, n
- ⚑ `shuswap-lake-see-maps-on-page-28-includes-lit-80ee82` (D) — [human-review 2026-08-04 lake-internal] MIXED, needs splitting into multiple rows before authoring: (1) the part of South Thompson River BETWEEN Shuswap and Little Shuswa
- ⚑ `alouette-lake-396262` (D) — [human-review 2026-08-04 lake-internal] polygon (Tier-2, vague/map-only) per docs/15 §1/§7 (swimming areas are explicitly listed as a lake-internal example). Row was auto
- ⚑ `beaver-lake-6e12af` (D) — [human-review 2026-08-04 lake-internal] polygon (Tier-2, vague) per docs/15 §7: 'on parts' gives no derivable geometry — stays map-only until the full reg text/map is che
- ⚑ `cowichan-lake-including-bear-lake-5a1513` (D) — [human-review 2026-08-04 lake-internal] polygon (Tier-2, vague) per docs/15 §7: 'on parts' gives no derivable geometry — stays map-only. Also note name_verbatim includes 
- ⚑ `long-lake-nanaimo-5c3300` (D) — [human-review 2026-08-04 lake-internal] polygon (Tier-2, vague) per docs/15 §7: 'on parts' gives no derivable geometry — stays map-only.
- ⚑ `osoyoos-lake-29ce8a` (D) — [human-review 2026-08-04 lake-internal] polygon (Tier-2, named/signed but multiple) per docs/15 §5c: 5 signed swimming areas, Osoyoos Lake. More concrete than the plain '
- ⚑ `brannen-lake-1539e6` (D) — [human-review 2026-08-04 lake-internal] polygon (Tier-2, vague) per docs/15 §7: 'on parts' gives no derivable geometry — stays map-only.
- ⚑ `sakinaw-lake-df4750` (D) — [human-review 2026-08-04 lake-internal] line per docs/15 §5a: 'easterly of a line drawn from a boundary sign located at the north side of the Sakinaw Lake boat launch sou
- ⚑ `dinosaur-lake-reservoir-downstream-of-w-a-c-b-50210f` (D) — [human-review 2026-08-04 lake-internal] NOT a lake-internal subdivision. The existing auto-proposal note already concludes: 'Better modelled as a lake polygon (Dinosaur L
- ⚑ `great-central-lake-4ff0ab` (D) — [human-review 2026-08-04 lake-internal] NOT lake-internal. 'From the dam to fishing boundary signs … upstream of the Ash Main Bridge' describes a STREAM reach on the Stam
- ⚑ `chemainus-river-688d54` (E) — [human-review 2026-08-04 na-with-siblings] ANOMALY — USER_NOTE 'requires split: upstream of Bannon Creek' names the exact same locator_text as this row. full_regulation s
- ⚑ `west-road-blackwater-river-fbb33d` (E) — [human-review 2026-08-04 na-with-siblings] confirmed — 'in mainstem (only)' descriptor itself is not a split. However, full_regulation for this Region 5 (MU 5-12/5-13) en

## 5. Gaps & anomalies (bucket E)

- **GAP** `west-road-blackwater-river-fbb33d` — WEST ROAD ("Blackwater") RIVER: no existing row for '(no named boundary found in this entry's full_regulation)'. [human-review 2026-08-04 na-with-siblings] confirmed — 'in mainstem (only)' descriptor itself is not a split. However, f
- ⚑ `chemainus-river-688d54` — flagged by bucket E as really the `-b` half of a Bannon Creek confluence split (pairs with `chemainus-river-79cc9a` in §3), NOT n/a. Handle both together.

## How to apply

`decisions-11-plan.json` holds every row's `action` (`not_applicable|deferred|todo`),
`reclassify_anchor_kind`, `label`, `note_to_append`, `confidence`, `needs_dawson`, and
`apply_now_safe`. Apply the 51 `apply_now_safe` rows first (mechanical), then walk the ⚑ list.

