# MEMO — Regulation Weirdness & Corrections Catalog

*Generated from `pipeline/matching/overrides.json` (480 corrections). This is the "what does the
legacy override layer actually fix" reference — every deviation between the printed BC freshwater
synopsis and the FWA/GNIS ground truth. Use it to check Phase-5 output represents all known errors.*

## Category summary

| # | Category | Count | What it means |
|---|---|------:|---------------|
| 1 | `wrong_mu_boundary` | 47 | Reg prints a Management-Unit that disagrees with where FWA places the water (MU boundary error / water on a boundary). |
| 2 | `name_or_spelling` | 50 | Printed name ≠ gazetted/GNIS name: typo, quoting, word order, simplification, renamed, or an alternate name. |
| 3 | `reach_segment` | 33 | Reg scopes to a sub-reach ("upstream/downstream of X", km markers) whose boundary is NOT in FWA — needs a curated split. |
| 4 | `tributary_link` | 35 | Reg names "tributaries of X" — linked to the parent waterbody + closure. |
| 5 | `ungazetted_from_map` | 74 | Water is unnamed / not in FWA / hand-located from the reg map (ungazetted). |
| 6 | `combined_multi` | 6 | One printed entry covers multiple waters / a multipolygon GIS feature. |
| 7 | `out_of_region` | 8 | Reg files the water under the wrong region (e.g. Haida Gwaii). |
| 8 | `uncertain_verify` | 1 | Manual match is UNCERTAIN — flagged for human verification. |
| 9 | `federal_closure` | 1 | Federal Fisheries Act schedule / national-park / Parks-Canada closure. |
| 10 | `gnis_relink_other` | 150 | Re-pointed to explicit GNIS id(s), reason not otherwise classified. |
| 11 | `waterbody_relink` | 18 | Re-pointed to explicit waterbody key/polygon id(s). |
| 12 | `wsc_relink` | 9 | Re-pointed to explicit FWA watershed code(s). |
| 13 | `skip_crosslist_variant` | 17 | Skipped: a cross-listing / name-variant of another entry (dedupe). |
| 14 | `skip_not_found` | 7 | Skipped: intentionally left not_found. |
| 15 | `skip_other` | 14 | Skipped for another reason. |
| 16 | `other_uncategorized` | 10 | Unclassified — inspect individually. |

## wrong_mu_boundary  (47)

| Name (verbatim) | Region/MU | Note |
|---|---|---|
| "ANDERSON" LAKE | REGION 1 1-3 | Lake in MU 1-3. FWA has MU 1-6, regulation has MU 1-3 (boundary issue). Piscivorous rainbow trout and kokanee population. BC Lakes Database: survey_id 1129, WBID 00105SANJ, watershed code 930-06320… |
| "WHALE" LAKE (Gustafsen Lake area) | REGION 5 5-2 | Whale Lake in Gustafsen Lake area; found via BC Lakes Database (survey_id from 1981: 'A Reconnaissance Survey of Whale Lake 00389DOGC, 1981'). FWA has MU 5-15, regulation has MU 5-2 (boundary issue). |
| BEAR LAKE (Crooked River Provincial Park) | REGION 7A 7-16 | FWA has MU 7-24, regulation has MU 7-16 (boundary issue). Crooked River Provincial Park is mostly in 7-24 but extends into 7-16; the lake is in 7-24. |
| BRIDAL LAKE | REGION 4 4-7 | FWA has MU 4-8, regulation has MU 4-7 (boundary issue) |
| BRIDGE LAKE | REGION 5 5-2 | FWA has MU 5-1, regulation has MU 5-2 (boundary issue) |
| CEDAR LAKE | REGION 2 2-2 | Lake on boundary with MU 2-2. FWA has MU 2-17, regulation has MU 2-2 (boundary issue). Location found from map. |
| CHILKO LAKE | REGION 5 5-4 | GNIS 33890 - Tŝilhqox Biny (current name) / Chilko Lake (former name). FWA has MU 5-4, regulation has MU 5-4 (correct MU). |
| CHILKO LAKE'S tributary streams | REGION 5 5-4 | Tributaries of Chilko Lake - links to parent waterbody (GNIS 33890 - Tŝilhqox Biny / Chilko Lake). FWA has MU 5-4, regulation has MU 5-4. |
| CHILLIWACK LAKE | REGION 2 2-4 | FWA has MU 2-3, regulation has MU 2-4 (boundary issue) |
| CHUBB LAKE | REGION 7A 7-10 | FWA has MU 7-8, regulation has MU 7-10 (boundary issue - neighboring MUs but lake not close to border, likely mislabeled MU) |
| DEM LAKE | REGION 7A 7-25 | FWA has MU 7-26, regulation has MU 7-25 (boundary issue - lake is approximately 2.5km from border) |
| DUNALTER LAKE (Irrigation Lake) | REGION 6 6-9 | FWA has MU 6-8, regulation has MU 6-9 (boundary issue). Alternate name 'Irrigation Lake' confirmed by Irrigation Lake Park near the lake. |
| ELLISON LAKE | REGION 8 8-8 | FWA has MU 8-10, regulation has MU 8-8 (boundary issue - lake likely mislabeled MU) |
| EMERALD LAKE | REGION 7A 7-15 | FWA has MU 7-16, regulation has MU 7-15 (boundary issue). Confirmed correct location via stocked lake maps - lake is in MU 7-16. |
| FISHER MAIDEN LAKE | REGION 4 4-26 | Found via fish stocking records and ACAT reports (https://a100.gov.bc.ca/pub/acat/public/viewReport.do?reportId=51502). FWA has MU 4-17, regulation has MU 4-26 (boundary issue). |
| FRAZER LAKE | REGION 8 8-9 | FWA has MU 8-10, regulation has MU 8-9 (boundary issue - lake is 700m from border) |
| HALL LAKE | REGION 4 4-34 | FWA has MU 4-20, regulation has MU 4-34 (boundary issue) |
| HART LAKE (Fort St. James) | REGION 7A 7-25 | Unnamed lake near Fort St. James; found via BC Lakes Database (survey_id 5255: 'A RECONNAISSANCE SURVEY OF UNNAMED "HART" LAKE'). FWA has no GNIS ID, regulation has MU 7-25. |
| HIAWATHA LAKE | REGION 4 4-3 | FWA has MU 4-4, regulation has MU 4-3 (boundary issue) |
| HOPE SLOUGH | REGION 2 2-8 | FWA has MUs 2-3, 2-4, regulation has MU 2-8 (boundary issue) |
| HUSH LAKE | REGION 5 5-15 | FWA has MU 5-2, regulation has MU 5-15 (boundary issue - lake is on the border) |
| JACK OF CLUBS LAKE | REGION 5 5-2 | FWA has MU 5-15, regulation has MU 5-2 (boundary issue) |
| KATHERINE LAKE | REGION 5 5-15 | FWA has MU 5-2, regulation has MU 5-15 (boundary issue - lake is close to border) |
| LA SALLE LAKES | REGION 7A 7-3 | FWA has MU 7-5, regulation has MU 7-3 (boundary issue). GNIS ID matches both polygons. |
| LAKELSE RIVER | REGION 6 6-10 | FWA has MU 6-11, regulation has MU 6-10 (boundary issue) |
| LANGFORD LAKE | REGION 1 1-2 | FWA has MU 1-2, regulation has MU 1-21 (boundary issue) |
| LLOYD LAKE | REGION 3 3-30 | FWA has MU 3-29, regulation has MU 3-30 (boundary issue) |
| LOST LAKE | REGION 6 6-15 | FWA has MU 6-21, regulation has MU 6-15 (boundary issue). Lake near Terrace; incorrect GNIS_ID 39158 candidate found in MU 6-21. Using specific waterbody_key for correct lake. Location: https://www… |
| LUCILLE LAKE | REGION 2 2-9 | FWA has MU 2-6, regulation has MU 2-9 (boundary issue); name order: 'lake lucille' in gazetteer |
| LYNX LAKE | REGION 7A 7-15 | FWA has MU 7-10, regulation has MU 7-15 (boundary issue - neighboring MUs but lake is 21km from border, likely mislabeled MU in regulations) |
| MIAMI CREEK | REGION 2 2-19 | FWA has MU 2-18, regulation has MU 2-19 (boundary issue) |
| NATION RIVER | REGION 7A 7-30 | FWA has MUs 7-28, 7-29, regulation has MU 7-30 (boundary issue) |
| NIMPO LAKE | REGION 5 5-12 | FWA has MU 5-6, regulation has MU 5-12 (boundary issue) |
| NOLA LAKE | REGION 1 1-9 | FWA has MU 1-10, regulation has MU 1-9 (boundary issue) |
| OWEEGEE LAKE | REGION 6 6-16 | FWA has MU 6-17, regulation has MU 6-16 (boundary issue - lake is less than 1.5km from border) |
| PANTHER LAKE | REGION 1 1-5 | FWA has MU 1-6, regulation has MU 1-5 (boundary issue) |
| RANCHERIA RIVER'S TRIBUTARIES | REGION 6 6-25 | Tributaries of Little Rancheria River - links to parent waterbody. FWA has MU 6-24, regulation has MU 6-25 (boundary issue). |
| SQUARE LAKE (located in Crooked River Provincial Park) | REGION 7A 7-16 | FWA has MU 7-12, regulation has MU 7-16 (boundary issue). Crooked River Provincial Park is partly in both 7-24 and 7-16; the lake is in 7-24 which is very close to 7-16. |
| STAWAMUS RIVER | REGION 2 2-9 | FWA has MU 2-8, regulation has MU 2-9 (boundary issue) |
| STELLAKO RIVER | REGION 7A 7-12 | FWA has MU 7-13, regulation has MU 7-12 (boundary issue) |
| SWAN LAKE | REGION 7B 7-20 | Disambiguate from GNIS 20912; FWA has MU 7-33, regulation has MU 7-20 (boundary issue - lake is about 800m from boundary with MU 7-20) |
| SWAN LAKE | REGION 8 8-26 | FWA has MU 8-22, regulation has MU 8-26 (boundary issue). Confirmed correct lake via stocked lakes map. Other Swan Lake (GNIS 37740) is in MU 8-6. |
| TANYA LAKE'S TRIBUTARIES | REGION 5 5-10 | Tributaries of Tanya Lakes - links to parent waterbody (GNIS 23919 - Tanya Lakes). FWA has MU 5-10, regulation has MU 5-10. |
| TSITNIZ LAKE | REGION 7A 7-9 | FWA has MU 7-8, regulation has MU 7-9 (boundary issue - lake is about 3km from border) |
| TUPPER RIVER | REGION 7B 7-20 | FWA has MU 7-33, regulation has MU 7-20 (boundary issue - regulation refers to outlet weir at Swan Lake which is in 7-33) |
| TWIN LAKES | REGION 4 4-34 | 2 polygons near Premier Lake (accessed via 15min hike from overflow camping area); first lake encountered is Twin Lakes (Yankee); FWA MU 4-21, regulation MU 4-34 (boundary issue); Found via stocked… |
| WAHLA LAKE | REGION 6 6-2 | FWA has MU 6-1, regulation has MU 6-2 (boundary issue - neighboring MUs but lake is 7km from boundary) |

## name_or_spelling  (50)

| Name (verbatim) | Region/MU | Note |
|---|---|---|
| "AGNUS" LAKE | REGION 5 5-6 | GNIS name: Agnus Lake. Remove quotes. |
| "BIG QUALICUM" RIVER | REGION 1 1-6 | GNIS name: Qualicum River. Duplicate entry with 'QUALICUM RIVER' in same region. |
| "ERROCK" ("Squakum") LAKE | REGION 2 2-8 | GNIS name: Lake Errock. Name order differs in gazetteer. |
| "JONES" LAKE | REGION 2 2-3 | GNIS name: Wahleach Lake. Labelled as Wahleach Lake in GIS (https://www.bchydro.com/community/recreation_areas/jones_lake.html). |
| "MARSHALL" CREEK | REGION 2 2-4 | GNIS name: Marshall Creek. Remove quotes. |
| "MAXWELL LAKE" (Lake Maxwell) | REGION 1 1-1 | GNIS name: Lake Maxwell. Name order differs in gazetteer. |
| "MORGAN" LAKE | REGION 3 3-19 | GNIS name: Morgan Lake. Remove quotes. |
| "PAQ" LAKE | REGION 2 2-5 | GNIS name: Lily Lake. Known locally as Lily Lake. |
| "STOWELL LAKE" (Lake Stowell) | REGION 1 1-1 | GNIS name: Lake Stowell. Name order differs in gazetteer. |
| "WESTON LAKE" | REGION 1 1-1 | GNIS name: Lake Weston. Name order differs in gazetteer. |
| ARROW PARK (Mosquito) CREEK | REGION 4 4-32 | GNIS name: Mosquito Creek. MU 4-18 near Arrow Park community. GNIS 7335 (also Mosquito Creek) is in MU 4-32. |
| BALLON LAKE | REGION 5 5-2 | GNIS name: Baillon Lake. Spelling correction. |
| BEAR (Mahood) CREEK | REGION 2 2-4 | GNIS name: Mahood Creek. Two GNIS entries for same waterbody. |
| BIGHORN RESERVOIR (Lakeview Irrigation District) | REGION 8 8-11 | GNIS name: Big Horn Reservoir. Spacing correction. |
| BOBTAIL (Naltesby) LAKE | REGION 7A 7-12 | GNIS name: Naltesby Lake. Alternate name in gazetteer. |
| BURTON CREEK | REGION 4 4-15 | GNIS name: Burton (Trout) Creek. Full name includes parenthetical. |
| CHUNAMUN LAKE | REGION 7B 7-35 | Lake in Region 7B MU 7-35 (same waterbody as 'CHINAMAN' LAKE - alternate name); found via BC Lakes Database (Report ID 52574: 'Chunamun Lake Gillnet Survey - 1989', WBID: 00552UPCE). Survey date: O… |
| EAST HAUTETE LAKE | REGION 7A 7-27 | GNIS name: East Hautête Lake. Accent correction. |
| EDWARDS LAKE | REGION 4 4-2 | GNIS name: Edwards Lakes (plural). Plural variation. |
| GARBUTT LAKE | REGION 4 4-22 | GNIS name: Norbury Lake. Official name is Norbury (Garbutt) Lake. |
| HAUTETE LAKE | REGION 7A 7-27 | GNIS name: Hautête Lake. Accent correction. |
| HEVENOR ("McQueen") CREEK | REGION 6 6-30 | GNIS name: Hevenor Creek. Primary name without parenthetical. |
| KINBASKET (McNaughton) LAKE | REGION 4 4-36 | Kinbasket Lake (alternate name: McNaughton Lake). GNIS 3133 - 2 polygons spanning MUs 4-34, 4-36, 4-37, 4-38, 4-39, 4-40, 7-2. Regulation MU 4-36. |
| KLWALI LAKE | REGION 7A 7-28 | GNIS name: Klawli Lake. Spelling correction. |
| KOOCANUSA RESERVOIR | REGION 4 4-2,4-3,4-22 | GNIS name: Lake Koocanusa. Name variation. |
| KSI HLGINX RIVER (formerly Ishkheenickh River) | REGION 6 6-14 | GNIS name: Ksi Hlginx. GNIS drops 'River' suffix. |
| KSI SGASGINIST CREEK (formerly Seaskinnish Creek) | REGION 6 6-15 | GNIS name: Ksi Sgasginist. GNIS drops 'Creek' suffix. |
| KSI SII AKS RIVER (formerly Tseax River) | REGION 6 6-14 | GNIS name: Ksi Sii Aks. GNIS drops 'River' suffix. |
| KSI X'ANMAS RIVER (formerly Kwinamass River) | REGION 6 6-14 | GNIS name: Ksi X'anmas. GNIS drops 'River' suffix. |
| KWOTLENEMO (Fountain) LAKE | REGION 3 3-17 | GNIS name: Kwotlenemo (Fountain) Lake. Parenthetical included in GNIS. |
| LAKE REVELSTOKE | REGION 4 4-38,4-39 | GNIS 39145 - Revelstoke Lake. Regulation uses 'Lake Revelstoke' name order. |
| LONZO ("Marshall") CREEK | REGION 2 2-4 | FWA name is Marshall Creek (GNIS 1860) in Region 2 MU 2-4. Lonzo is alternate name. |
| MAGGIE LAKE | REGION 1 1-8 | GNIS name: Makii Lake. Renamed in gazette (https://apps.gov.bc.ca/pub/bcgnws/names/62541.html). |
| MAHATTA RIVER | REGION 1 1-13 | GNIS name: Mahatta Creek. Gazetteer lists as creek. |
| MCDONNEL LAKE | REGION 6 6-9 | GNIS name: McDonell Lake. Two lakes with same name in zone 6 (GNIS 22882 and 30846). Spelling correction. |
| MCKAY CREEK | REGION 2 2-8 | GNIS name: Mackay Creek. Spelling correction. |
| MORFEE LAKE (south) | REGION 7A 7-30 | GNIS name: Morfee Lakes (plural). Plural variation. |
| PEND D'OREILLE RIVER (Includes the reservoirs behind Waneta  | REGION 4 4-8 | GNIS name: Pend-D'Oreille River. Hyphenation differs in gazetteer. |
| QUINN CREEK | REGION 4 4-22 | GNIS name: Quinn (Queen) Creek. Full name includes parenthetical. |
| SARDIS PARK POND | REGION 2 2-4 | GNIS name: Sardis Pond. Name simplification. |
| SUNSHINE ("Ant") LAKE | REGION 5 5-11 | GNIS 10522 - Ant Lake. Regulation uses alternate name 'Sunshine' with 'Ant' in parentheses. Regulation MU 5-11. |
| SUSTUT LAKES | REGION 6 6-18 | GNIS name: Sustut Lake (singular). Plural/singular variation. |
| SWELTZER CREEK | REGION 2 2-3 | GNIS name: Sweltzer River. Labelled as Sweltzer River in GIS. |
| TEE PEE LAKES | REGION 8 8-6 | 3 polygons with GNIS 21766 - Tepee Lakes (spelling variation in FWA) |
| THORN CREEK | REGION 7A 7-39 | GNIS name: Thorne Creek. Spelling correction. |
| TOQUART LAKE | REGION 1 1-8 | GNIS name: Toquaht Lake. Spelling mismatch in synopsis. |
| TOQUART RIVER | REGION 1 1-8 | GNIS name: Toquaht River. Spelling mismatch in synopsis. |
| TUC-EL-NUIT LAKE | REGION 8 8-1 | GNIS name: Tugulnuit Lake. Spelling mismatch. |
| WHALE LAKE (Canim Lake area) | REGION 5 5-15 | GNIS name: Whale Lake. Parenthetical area qualifier in regulation name. |
| WITCH LAKE | REGION 7A 7-28 | Lake in Region 7 MU 7-28; FWA name is 'Onjo Lake' (gazette name). Found via BC Lakes Database (Report ID 34828: 'A Reconnaissance Survey of Witch Lake, 1977 01386NATR', WBID: 01386NATR). Survey dat… |

## reach_segment  (33)

| Name (verbatim) | Region/MU | Note |
|---|---|---|
| "RYE" LAKE | REGION 5 5-2 | Unnamed lake approximately 1.6 km downstream of Joan Lake in MU 5-2; found via ACAT report (https://a100.gov.bc.ca/pub/acat/public/viewReport.do?reportId=23024). FWA has no GNIS ID. |
| ADAMS RIVER (downstream of Adams Lake) | REGION 3 3-37 | Matched via base name: ADAMS RIVER |
| ADAMS RIVER (upstream of Adams Lake) | REGION 3 3-37 | Matched via base name: ADAMS RIVER |
| ALEXANDER CREEK (downstream of the easternmost Hwy 3 bridge) | REGION 4 4-23 | Matched via base name: ALEXANDER CREEK |
| ALEXANDER CREEK (upstream of the easternmost Hwy 3 bridge) | REGION 4 4-23 | Matched via base name: ALEXANDER CREEK |
| BURNT BRIDGE CREEK (upstream of Sitkatapa Creek) | REGION 5 5-11 | Matched via base name: BURNT BRIDGE CREEK |
| CHESLATTA RIVER (downstream of falls) | REGION 6 6-4 | Matched via base name: CHESLATTA RIVER |
| COAL CREEK (downstream of Old MF&M Railway bridge 7 km upstr | REGION 4 4-23 | Matched via base name: COAL CREEK |
| DAVIS BAY (in Finlay Reach of Williston Lake) | REGION 7A 7-37 | Davis Bay in Finlay Reach of Williston Lake. Links to full Williston Lake (GNIS 28522) plus ungazetted point at bay location. Finlay Reach is the northern arm where Finlay River (GNIS 12355) enters… |
| ELK RIVER (downstream of Elko Dam) | REGION 4 4-2 | Matched via base name: ELK RIVER |
| ELK RIVER (upstream of Elko Dam) | REGION 4 4-2,4-23 | Matched via base name: ELK RIVER |
| FORDING RIVER (downstream of Josephine Falls) | REGION 4 4-23 | Matched via base name: FORDING RIVER |
| FORDING RIVER (upstream of Josephine Falls) | REGION 4 4-23 | Matched via base name: FORDING RIVER |
| FRASER RIVER (upstream of the CPR Bridge at Mission) | REGION 2 2-4 | Fraser River (GNIS 39325) upstream of the CPR Bridge at Mission, plus all named channels and sloughs without their own regulation entries. Region 2. Nicomen Slough and Strawberry Slough excluded — … |
| GLACIER (Redslide) CREEK (unnamed tributary to Nanika River) | REGION 6 6-9 | Medium confidence. Historic Alcan water diversion boundary (1950): diverted Nanika watershed waters upstream of Glacier Creek, ~4km below Kidprice Lake |
| HUNLEN CREEK (upstream of Hunlen Falls) | REGION 5 5-11 | Matched via base name: HUNLEN CREEK |
| KOOTENAY RIVER (downstream of Idaho border) | REGION 4 4-7,4-8 | Kootenay River and all named branches/channels sharing the same watershed code in Zone 4. |
| KOOTENAY RIVER (upstream of Koocanusa Reservoir) | REGION 4 4-2,4-21,4-22,4-24,4-25,4-35 | Matched via base name: KOOTENAY RIVER |
| LODGEPOLE CREEK (downstream of falls near the km 26 post on  | REGION 4 4-2 | Matched via base name: LODGEPOLE CREEK |
| LODGEPOLE CREEK (upstream of falls) | REGION 4 4-2 | Matched via base name: LODGEPOLE CREEK |
| MICHEL CREEK (downstream of the easternmost Hwy 3 bridge) | REGION 4 4-23 | Matched via base name: MICHEL CREEK |
| MICHEL CREEK (upstream of the easternmost Hwy 3 bridge) | REGION 4 4-23 | Matched via base name: MICHEL CREEK |
| PAUL CREEK (downstream of Paul Lake) | REGION 3 3-27 | Matched via base name: PAUL CREEK |
| PEACE RIVER (Downstream of boundary signs 1,200m downstream  | REGION 7B 7-31 | Matched via base name: PEACE RIVER |
| PREACHER LAKE (east of Bowers Lake) | REGION 5 5-1 | Matched via base name: PREACHER LAKE |
| SAND CREEK (downstream of Hwy 3) | REGION 4 4-22 | Matched via base name: SAND CREEK |
| THOMPSON RIVER (downstream of signs at Kamloops Lake outlet  | REGION 3 3-13,3-14,3-18 | Matched via base name: THOMPSON RIVER |
| THOMPSON RIVER (upstream of Kamloops Lake) | REGION 3 3-28 | Matched via base name: THOMPSON RIVER |
| WIGWAM RIVER (downstream of the access road adjacent to km 4 | REGION 4 4-2 | Wigwam River in MU 4-2. Regulation specifies specific reach: 'downstream of the access road adjacent to km 42 on the Bighorn (Ram) Forest Service Road'. Links to entire river (GNIS 2311 - Wigwam Ri… |
| WIGWAM RIVER (upstream of the Forest Service recreation site | REGION 4 4-2 | Wigwam River in MU 4-2. Regulation specifies specific reach: 'upstream of the Forest Service recreation site adjacent to km 42 on the Bighorn (Ram) Forest Service Road'. Links to entire river (GNIS… |
| WILLISTON LAKE (in Zone A) (includes waters 500 m east/upstr | REGION 7A 7-30,7-37,7-38 | Williston Lake Zone A. GNIS 28522 (Williston Lake/Reservoir). Zone A includes waters 500m east/upstream of the Causeway Road. TODO: Needs custom polygon subdivision to separate Zone A from Zone B. … |
| WILLISTON LAKE (in Zone B) | REGION 7B 7-31,7-36 | Williston Lake Zone B. GNIS 28522 (Williston Lake/Reservoir). Zone B is the remainder of Williston Lake excluding Zone A (500m east/upstream of Causeway Road). TODO: Needs custom polygon subdivisio… |
| YOUNG CREEK (upstream of Hwy 20) | REGION 5 5-11 | Matched via base name: YOUNG CREEK |

## tributary_link  (35)

| Name (verbatim) | Region/MU | Note |
|---|---|---|
| "OLDFIELD" CREEK | REGION 6 6-14 | Oldfield Creek in MU 6-14, tributary of Hays Creek. Oldfield Creek Fish Hatchery located here. |
| (Lower) CAMPBELL LAKE'S TRIBUTARIES | REGION 1 1-6 | Tributaries of Campbell Lake - links to parent waterbody (GNIS 17768 - Campbell Lake) |
| ATNARKO/BELLA COOLA RIVERS [Includes Tributaries] EXCEPT: Bu | REGION 5 5-6,5-8,5-11 | Links to both Bella Coola River (GNIS 11611) and Atnarko River (GNIS 17209). Bella Coola River flows from the coast (ocean) upstream to confluence of Atnarko and Talchako Rivers. Regulation MUs 5-6… |
| BRUNETTE RIVER'S TRIBUTARIES | REGION 2 2-8 | Tributaries of Brunette River - links to parent waterbody (GNIS 10070 - Brunette River). Regulation MU 2-8. |
| BURNABY LAKE'S TRIBUTARIES | REGION 2 2-8 | Tributaries of Burnaby Lake - links to parent waterbody (GNIS 10633) |
| BUTTLE LAKE'S TRIBUTARIES | REGION 1 1-9 | Tributaries of Buttle Lake - links to parent waterbody (GNIS 16747) |
| CHEHALIS LAKE'S TRIBUTARIES | REGION 2 2-19 | Tributaries of Chehalis Lake - links to parent waterbody (GNIS 13012 - Chehalis Lake). Regulation MU 2-19. |
| COLDWATER RIVER'S TRIBUTARIES | REGION 3 3-13 | Tributaries of Coldwater River - links to parent waterbody (GNIS 18066 - Coldwater River). Regulation MU 3-13. |
| COLUMBIA LAKE'S TRIBUTARIES | REGION 4 4-25 | Tributaries of Columbia Lake - links to parent waterbody (GNIS 18123 - Columbia Lake). Regulation MU 4-25. |
| CONNOR LAKE'S TRIBUTARIES | REGION 4 4-23 | Tributaries of Connor Lakes - links to parent waterbody (3 polygons with GNIS 19304) |
| DUNCAN LAKE'S TRIBUTARIES | REGION 4 4-27 | Tributaries of Duncan Lake - links to parent waterbody (GNIS 8109 - Duncan Lake). Regulation MU 4-27. |
| ELK RIVER'S TRIBUTARIES (see exceptions) | REGION 4 4-2,4-23 | Tributaries of Elk River - links to parent waterbody (GNIS 16880 - Elk River). Regulation MUs 4-2, 4-23. |
| FLATHEAD RIVER'S TRIBUTARIES | REGION 4 4-1 | Tributaries of Flathead River - links to parent waterbody (GNIS 19843 - Flathead River). Regulation MU 4-1. |
| GRANBY RIVER'S TRIBUTARIES | REGION 8 8-15 | Tributaries of Granby River - links to parent waterbody (GNIS 18775 - Granby River). Regulation MU 8-15. |
| JOHN HART LAKE'S TRIBUTARIES | REGION 1 1-10 | Tributaries of John Hart Lake - links to parent waterbody (GNIS 18905) |
| KETTLE RIVER'S TRIBUTARIES | REGION 8 8-14 | Tributaries of Kettle River - links to parent waterbody (GNIS 11984 - Kettle River). Regulation MU 8-14. |
| KINBASKET (McNaughton) LAKE'S TRIBUTARIES | REGION 4 4-36 | Tributaries of Kinbasket Lake - links to parent waterbody (GNIS 3133 - Kinbasket Lake). Regulation MU 4-36. |
| KOOTENAY LAKE'S TRIBUTARIES | REGION 4 4-7,4-19 | Tributaries of Kootenay Lake - links to parent waterbody (GNIS 14091 - Kootenay Lake). Regulation MUs 4-7, 4-19. |
| LAKE REVELSTOKE'S TRIBUTARIES | REGION 4 4-38 | GNIS name: Revelstoke Lake. Tributary entry - links to parent waterbody. |
| LITTLE SLOCAN LAKE'S TRIBUTARIES | REGION 4 4-16 | GNIS names: Upper Little Slocan Lake (30277) and Lower Little Slocan Lake (18652). Tributary entry - links to both parent waterbodies. |
| LOWER ARROW LAKE'S TRIBUTARIES | REGION 4 4-14 | Tributaries of Lower Arrow Lake - links to parent waterbody (GNIS 18644 - Lower Arrow Lake). Regulation MU 4-14. |
| MCARTHUR ISLAND SLOUGH | REGION 3 3-28 | Slough polygon + tributary stream segments |
| PEND D'OREILLE RIVER'S TRIBUTARIES (except Salmo River[Inclu | REGION 4 4-8 | GNIS name: Pend-D'Oreille River. Tributary entry - links to parent waterbody (hyphenated form). |
| PREMIER LAKE'S TRIBUTARIES | REGION 4 4-21 | Tributaries of Premier Lake - links to parent waterbody (GNIS 25274 - Premier Lake). Regulation MU 4-21. |
| REVELSTOKE LAKE'S TRIBUTARIES | REGION 4 4-38 | Tributaries of Revelstoke Lake - links to parent waterbody (GNIS 39145 - Revelstoke Lake). Regulation MU 4-38. |
| SALMO RIVER'S TRIBUTARIES | REGION 4 4-8 | Tributaries of Salmo River - links to parent waterbody (GNIS 20528 - Salmo River). Regulation MU 4-8. |
| SLEWISKIN (Macdonald) CREEK | REGION 4 4-15 | FWA name is McDonald Creek (GNIS 6159). Regulation uses 'SLEWISKIN (Macdonald) CREEK' with alternate name 'Macdonald'. Note spelling variation: Macdonald vs McDonald. Left tributary. MU 4-15. |
| SLOCAN LAKE'S TRIBUTARIES | REGION 4 4-17 | Tributaries of Slocan Lake - links to parent waterbody (GNIS 27954 - Slocan Lake). Regulation MU 4-17. |
| SQUAMISH RIVER'S TRIBUTARIES | REGION 2 2-6 | Tributaries of Squamish River - links to parent waterbody (GNIS 25671 - Squamish River). Regulation MU 2-6. |
| TROUT LAKE'S TRIBUTARIES | REGION 4 4-30 | Tributaries of Trout Lake - links to parent waterbody (GNIS 28481 - Trout Lake). Regulation MU 4-30. |
| UPPER ARROW LAKE'S TRIBUTARIES | REGION 4 4-31 | Tributaries of Upper Arrow Lake - links to parent waterbody (GNIS 8405 - Upper Arrow Lake). |
| WAHLEACH ("Jones") LAKE'S TRIBUTARIES | REGION 2 2-3 | Tributaries of Wahleach Lake - links to parent waterbody (GNIS 22474 - Wahleach Lake). Alternate name: "Jones" Lake. Regulation MU 2-3. |
| WEST KETTLE RIVER'S tributaries | REGION 8 8-12 | Tributaries of West Kettle River - links to parent waterbody (GNIS 6358 - West Kettle River). Regulation MU 8-12. |
| WEST ROAD ("Blackwater") RIVER'S TRIBUTARIES | REGION 6 6-1 | Tributaries of West Road River - links to parent waterbody. Regulation MU 6-1. NOTE: Different regulations apply to Region 6 vs Region 7 - regulations must be applied separately for each region. |
| WEST ROAD ("Blackwater") RIVER'S TRIBUTARIES | REGION 7A 7-10 | Tributaries of West Road River - links to parent waterbody. Regulation MU 7-10. NOTE: Different regulations apply to Region 6 vs Region 7 - regulations must be applied separately for each region. |

## ungazetted_from_map  (74)

| Name (verbatim) | Region/MU | Note |
|---|---|---|
| "ALCES" LAKE | REGION 4 4-24 | Lake in MU 4-24. BC Lakes Database: WBID 00374KOTR, ACAT 280743. Gazetted name: MOOSE LAKE. Location found from map. |
| "ALTA" LAKE | REGION 4 4-3 | Unnamed lake in MU 4-3. Location found from map. Low confidence - Alta Lake GNIS 7915 is in Region 2 MUs 2-11, 2-9. |
| "BLUFF" LAKE | REGION 5 5-2 | Unnamed lake approximately 25 km NE of Lac La Hache in Region 5; found via BC Lakes Database (survey_id 20677: 'A Reconnaissance Survey of Bluff Lake'). FWA has no GNIS ID. |
| "CRUISE" LAKE | REGION 5 5-6 | Unnamed lake in MU 5-6, approximately 500 m south of Stewart Lake. BC Lakes Database: WBID 01304ATNA, ACAT 32593. Location found from map. |
| "DOG" LAKE | REGION 5 5-2 | Unnamed lake in MU 5-2, approximately 6 km south/southwest of the confluence of Dog and Pigeon Creeks. BC Lakes Database: WBID 00289DOGC. ACAT 6700. Other Dog Lakes exist in Region 4 (GNIS 24393, M… |
| "EAST GRIBBELL" CREEK | REGION 6 6-3 | East Gribbell Creek in MU 6-3, on Ursula Channel. Ungazetted stream. Waterbody ID 00000KHTZ. Cutthroat Trout observed 1994-01-01. Source: FISS Database survey_id 77237 '01-JAN-94 Fisheries Assessme… |
| "GEESE" LAKE (2 km northeast of Eliguk Lake) | REGION 5 5-12 | Lake in MU 5-12, 2 km northeast of Eliguk Lake. Medium confidence. Location found from map. |
| "GRASSY" LAKE (unnamed lake approx. 1 km southwest of West K | REGION 5 5-1 | Unnamed lake in MU 5-1, approximately 1 km southwest of West King Lake. BC Lakes Database: WBID 00707BRID, watershed code 129-360400-23900-98400-9950-9850-283. ACAT 54219. Other Grassy Lakes exist … |
| "GRIZZLY" LAKE (unnamed lake approx. 4.5 km upstream of Maef | REGION 5 5-15 | Lake in MU 5-15. BC Lakes Database: WBID 00514CARR, watershed code 160-466100-28200-84200. ACAT 54245. Location found from map. Low confidence. |
| "HIGH" LAKE (unnamed lake approx. 4 km north of Bridge Lake) | REGION 5 5-1 | Unnamed lake in MU 5-1, approximately 4 km north of Bridge Lake. BC Lakes Database: WBID 00697BRID. ACAT 24310. Other High Lakes exist in Region 4 (GNIS 21399, MU 4-35) and Region 8 (GNIS 21400, MU… |
| "KESTREL" LAKE | REGION 5 5-2 | Lake in MU 5-2. BC Lakes Database: WBID 00238TWAC. Gazetted name: KESTREL LAKE. Location found from map. |
| "LITTLE BISHOP" LAKE (approx. 1.7 km northeast of Bishop Lak | REGION 5 5-13 | Lake in MU 5-13, approximately 1.7 km northeast of Bishop Lake. Location found from map. |
| "LITTLE JONES" LAKE | REGION 5 5-2 | Unnamed lake in MU 5-2, approximately 13 km east/southeast of 150 Mile House on the north side of Jones Creek. Confirmed via stocked lakes map. Location found from map. |
| "LITTLE MITTEN" LAKE (approx. 400 m west of Mitten Lake) | REGION 4 4-34 | Unnamed lake approximately 400m west of Mitten Lake in MU 4-34; found via BC Lakes Database ('Kootenay Fisheries Field Report Little Mitten Lake 00753KHOR', located approx. 13 km SW of Parsons). FW… |
| "LITTLE PETER HOPE" LAKE (unnamed lake approximately 200 m s | REGION 3 3-20 | Unnamed lake in MU 3-20, approximately 200 m southwest of Peter Hope Lake. BC Lakes Database: WBID 00497LNIC. ACAT ObjectID 10044. FWA Watershed Code: 120-246600-53700-23700-5090-0000-000-000-000-0… |
| "LITTLE TOMAS" LAKE | REGION 7A 7-25 | Unnamed lake in Region 7 MU 7-25; found via BC Lakes Database (Report ID 3676: 'FORT ST. JAMES LAKE INVENTORY 1996 RECONNAISSANCE SURVEY OF UNNAMED LAKE 73 K113 (Little Tomas)', WBID: 01199STUL). |
| "LOWER BEAVERPOND" LAKE (lowermost of the two Beaverpond lak | REGION 7A 7-38 | Unnamed lake in Region 7 MU 7-38 (lowermost of the two Beaverpond lakes); found via BC Lakes Database (Report ID 6399: 'A Reconnaissance Survey of Lower Beaver Pond Lake 00849UOMI'). Survey date: M… |
| "MCCLAIN" LAKE | REGION 4 4-34 | Lake in MU 4-34, approximately 750 m south of Mitten Lake. BC Lakes Database: WBID 00799KHOR, ACAT 22239. Spelling variations: McClain/McLain/McLean. Location found from map. |
| "MOSS POTHOLE" LAKES | REGION 2 2-18 | Group of lakes in MU 2-18. Multiple polygons. Location found from map. |
| "MT. MILLIGAN" LAKE | REGION 7A 7-28 | Unnamed lake in Region 7 MU 7-28 (located approximately 7.5 km south/southeast of Mt. Milligan); found via BC Lakes Database (Report ID 4121: 'Mount Milligan Lake 2004 Fish Stocking Assessment 0147… |
| "NORMAN" LAKE (unnamed lake approximately 600 m southeast of | REGION 3 3-19 | Unnamed lake in MU 3-19, approximately 600 m southeast of Durand Lake. BC Lakes Database: WBID 00719THOM. ACAT 15541. Other Norman Lakes exist in Region 7 and Region 8. Location found from map. |
| "PETE'S POND" Unnamed lake at the head of San Juan River | REGION 1 1-3 | Unnamed lake at the head of San Juan River. BC Lakes Database survey_id 1155: 'A RECONNAISSANCE SURVEY OF PETE'S POND'. Regulation MU 1-3. |
| "PIGEON LAKE #1" | REGION 5 5-2 | Unnamed lake adjacent to Dog Creek Road, approximately 9 km west of Gustafsen Lake and 19 km north of Meadow Lake Road in MU 5-2; found via BC Lakes Database ('A Reconnaissance Survey of Pigeon #1 … |
| "SANDY" LAKE | REGION 5 5-2 | Unnamed lake approximately 3.2 km south of Le Bourdais Lake in MU 5-2. Confirmed via FDIS fish observation (Waterbody ID 48340, Project: Inventory, Rudy, Maud Creek, Sandy Lake; 2019). Reference: h… |
| "SINKHOLE" LAKE | REGION 5 5-2 | Unnamed lake approximately 100 m east of Sneezie Lake in MU 5-2; found via BC Lakes Database ('A Reconnaissance Survey of Unnamed Lake 5964', WSC 100-385000-98600-98900-5160-5530, located approx. 1… |
| "SLIM" LAKE | REGION 5 5-4 | Unnamed lake in Taseko River drainage approximately 4 km north of Cone Hill in MU 5-4; found via BC Lakes Database (survey_id 5210: 'Lake Survey: Slim Lake 00811TASR', associated with Taseko Mines … |
| "SNAG" LAKE | REGION 5 5-1 | Unnamed lake approximately 60 km ESE of 100 Mile House (West King Area) in MU 5-1; found via BC Lakes Database (survey_id 20655: 'A Reconnaissance Survey of Snag Lake'). FWA has no GNIS ID. |
| "SPRING" LAKE | REGION 4 4-22 | Unnamed lake approximately 1.5 km west/northwest of the west end of Tie Lake in MU 4-22. Medium confidence. |
| BABY CHARLOTTE LAKE | REGION 5 5-6 | Lake in MU 5-6. Location found from map. |
| CAP SHEAF LAKES | REGION 2 2-16 | Lake in MU 2-16. Location found from map (https://www.alltrails.com/explore/recording/afternoon-hike-at-placer-mountain-0d770c4?p=-1&sh=li9ufv). |
| CHAMPION LAKE NO. 3 | REGION 4 4-8 | Lake in MU 4-8. BC Lakes Database: WBID 00352LARL, ACAT 4926. Location found from map. |
| CHAMPION LAKES NO. 1 & 2 | REGION 4 4-8 | MU 4-8. BC Lakes Database: WBID 00339LARL (Champion Lake #1 Lower, survey_id 2030) and WBID 00346LARL (Champion Lake #2 Middle, survey_id). ACAT ObjectID 4923. Gazetted name: CHAMPION LAKES. Map re… |
| CHEAM LAKE | REGION 2 2-3 | Wetland in MU 2-3. Polygon found in FWA wetlands layer. Location found from map. |
| CHIPMUNK LAKE | REGION 6 6-1 | Unnamed lake in Region 6; found via BC Lakes Database (survey_id 65: 'UNTITLED REPORT: LIMNOLOGY DATA FOR CHIPMUNK LAKE'). FWA has no GNIS ID. |
| CLANWILLIAM LAKE | REGION 3 3-34 | Unnamed in FWA lakes layer. MU 3-34. BC Lakes Database: WBID 523394. ACAT ObjectID 1122331271. Gazetted name: CLANWILLIAM LAKE. Location found from map. |
| DEEP LAKE | REGION 3 3-28 | Unnamed lake in MU 3-28. Location found from stocked lake map. |
| DINA CREEK | REGION 7A 7-30 | Dina Creek in MU 7-30. Unnamed creek that flows to Dina Lakes. Medium confidence. |
| EYE LAKE | REGION 7A 7-26 | Lake in MU 7-26. BC Lakes Database: WBID 00041MIDR, ACAT 3257. Location found from map. |
| FISH LAKE (unnamed lake approx. 2 km northwest of McClinchy  | REGION 5 5-6 | Unnamed lake in Region 5; found via BC Lakes Database (survey_id 315: 'UNTITLED REPORT: WINTER LIMNOLOGY DATA FOR FISH LAKE'). FWA has no GNIS ID. |
| GREEN TIMBERS LAKE | REGION 2 2-4 | Lake in MU 2-4. Polygon found in FWA manmade waterbodies layer. Location found from map. |
| IDLEWILD LAKE (old Cranbrook Reservoir) | REGION 4 4-3 | Lake in MU 4-3. BC Lakes Database: WBID 01249SMAR, ACAT 52677. Alternate name: old Cranbrook Reservoir. Location found from map. |
| JACKPINE LAKE | REGION 3 3-28 | Unnamed lake in MU 3-28. Location found from map. Other Jackpine Lakes exist in Region 5 (GNIS 16938, MU 5-2) and Region 8 (GNIS 16941, MU 8-11). |
| JERRY SULINA PARK POND | REGION 2 2-8 | Unnamed pond in MU 2-8. Location found from map. |
| LARRY LAKE (unnamed lake located about 400 m west of Thalia  | REGION 8 8-5 | Unnamed lake west of Thalia Lake in Region 8 MU 8-5. BC Lakes Database: Report ID 13491 ('Memo to File: Larry Lake Investigation - June 11 & 12 1984 00470SIML', WBID 00470SIML), survey date Apr 1, … |
| LITTLE LAC DES ROCHES (at west end of Lac Des Roches) | REGION 3 3-30 | Lake in MU 3-30, at west end of Lac Des Roches. Location found from map. |
| LITTLE LOST LAKE | REGION 7A 7-3 | Unnamed lake in Region 7 MU 7-3; found via BC Lakes Database (survey_id 6447: 'A RECONNAISSANCE SURVEY OF UNNAMED (LITTLE LOST) LAKE'). FWA has no GNIS ID. Note: GNIS 10869 exists for 'Little Lost … |
| LORENZO LAKE | REGION 3 3-39 | Lake in MU 3-39. BC Lakes Database: WBID 02102MAHD. ACAT ObjectID 7147. FWA Watershed Code: 129-360400-23900-98400-4800-9150-000-000-000-000-000-000. Location found from map. |
| LOWER KANE LAKE | REGION 3 3-13 | Lake in MU 3-13. BC Lakes Database: WBID 01088LNIC. ACAT ObjectID 31446. FWA Watershed Code: 120-246600-33700-41300-7100-0000-000-000-000-000-000-000. Same regulations as Upper Kane Lake. Location … |
| MACLEAN PONDS | REGION 2 2-4 | Unnamed in FWA lakes layer. ID 070111626. Location found from map. |
| MARSH POND | REGION 2 2-4 | No polygon in FWA. Using custom ungazetted waterbody with coordinates from KML point in Aldergrove Regional Park. |
| MERIDIAN LAKE | REGION 5 5-1 | Unnamed lake in MU 5-1, in Jim Creek system (North Thompson River watershed), approximately 55 km east of 100 Mile House. BC Lakes Database: ACAT 54214. FWA watershed code: 129-360400-23900-98400-4… |
| MINE LAKE | REGION 1 1-15 | Lake in MU 1-15. Also labelled as 'Main Lake' in gazette (https://www.canoevancouverisland.com/canoe-kayak-vancouver-island-directory/main-lake-canoe-chain-quadra-island/). Location found from map. |
| NATION ARM (Williston Lake) | REGION 7A 7-30 | Nation Arm of Williston Lake. Links to full Williston Lake (GNIS 28522) plus ungazetted point at arm location. Nation River (GNIS 16593) flows into this arm. MU 7-58. TODO: Needs custom polygon sub… |
| PADDY LAKE | REGION 5 5-1 | Lake in MU 5-1, also known as Squirrel Lake. Paddy Lake Recreation Site located here (REC5960). Referred to as 'Paddy Squirrel Lake' in BC Lakes bathymetric maps. Another Paddy Lake (GNIS 21895) ex… |
| RAINBOW LAKE | REGION 3 3-12 | Unnamed lake in Region 3 MU 3-12; found via BC Lakes Database (survey_id 4782: 'A RECONNAISSANCE SURVEY OF RAINBOW LAKE'). FWA has no GNIS ID. |
| ROSE LAKE | REGION 3 3-20 | Lake in MU 3-20. BC Lakes Database: WBID 00776STHM. ACAT 15585. Other Rose Lakes exist in Region 2, 5, and 6. Location found from map. |
| SAM'S FOLLY LAKE | REGION 4 4-34 | Lake in MU 4-34. BC Lakes Database: WBID 00398COLR, ACAT 2716. Location found from map. |
| TOMS LAKE | REGION 6 6-1 | Unnamed lake in Region 6 MU 6-1; found via BC Lakes Database (survey_id 73: 'UNTITLED REPORT: LIMNOLOGY DATA FOR TOMS LAKE'). FWA has no GNIS ID. |
| TULIP LAKE | REGION 3 3-20 | Lake in MU 3-20. BC Lakes Database: WBID 00762STHM. ACAT ObjectID 49860. FWA Watershed Code: 128-123700-73700-41000-0000-0000-000-000-000-000-000-000. Location found from map. |
| UNNAMED LAKE "A" - MAP A (below) | REGION 1 1-10 | Unnamed lake in MU 1-10. Location found from map in regulations. |
| UNNAMED LAKE "B" - MAP A (below) | REGION 1 1-10 | Unnamed lake in MU 1-10. Location found from map in regulations. |
| UNNAMED LAKE "C" - MAP B (below) | REGION 1 1-10 | Unnamed lake in MU 1-10. Location found from map in regulations. |
| UNNAMED LAKE "D" - MAP B (below) | REGION 1 1-10 | Unnamed lake in MU 1-10. Location found from map in regulations. |
| UNNAMED LAKE "E" - MAP B (below) | REGION 1 1-10 | Unnamed lake in MU 1-10. Location found from map in regulations. |
| UNNAMED LAKE "F" - MAP B (below) | REGION 1 1-10 | Unnamed lake in MU 1-10. Location found from map in regulations. |
| UNNAMED LAKE "G" - MAP B (below) | REGION 1 1-10 | Unnamed lake in MU 1-10. Location found from map in regulations. |
| UNNAMED LAKE "H" - MAP B (below) | REGION 1 1-10 | Unnamed lake in MU 1-10. Location found from map in regulations. |
| UNNAMED LAKE "I" - MAP B (below) | REGION 1 1-10 | Unnamed lake in MU 1-10 ("Elmer Lake" on Google Maps). Location found from map in regulations. |
| UNNAMED LAKE ("Kinglet Lake") located 100 m west of Butterfl | REGION 7A 7-15 | Unnamed lake locally known as "Kinglet Lake", 100m west of Butterfly Lake in MU 7-15. Identified via BC stocking records. Reference: https://www.env.gov.bc.ca/omineca/esd/faw/stocking/kinglet/kingl… |
| UNNAMED LAKE ("Redstart Lake") located approx. 200 m southwe | REGION 7A 7-15 | Unnamed lake locally known as "Redstart Lake", approximately 200m southwest of Butterfly Lake in MU 7-15. Two polygons identified via BC stocking records. Reference: https://www.env.gov.bc.ca/omine… |
| UNNAMED LAKE (approx. 500 m south of Natalkuz Lake) | REGION 6 6-1 | Unnamed lake in MU 6-1, approximately 500 m south of Natalkuz Lake. Location found from map. |
| UNNAMED LAKES (located immediately north and south of Bluey  | REGION 8 8-6 | 9 unnamed lakes located immediately north and south of Bluey Lake in Region 8 MU 8-6. All waterbodies matching the location criteria are included. |
| UPPER KANE LAKE | REGION 3 3-13 | Lake in MU 3-13. BC Lakes Database: WBID 01083LNIC. ACAT ObjectID 4123. FWA Watershed Code: 120-246600-33700-41300-7100-0000-000-000-000-000-000-000. Same regulations as Lower Kane Lake. Location f… |
| WENTWORTH LAKES | REGION 5 5-13 | 2 polygons found via BC Lakes Database ('A Reconnaissance Survey of Unnamed Lake (Upper Wentworth)', WBID: 00628NAZR and 'A Reconnaissance Survey of Wentworth Lake'). Regulation MU 5-13. |

## combined_multi  (6)

| Name (verbatim) | Region/MU | Note |
|---|---|---|
| CHILLIWACK / VEDDER RIVERS (does not include Sumas River) (s | REGION 2 2-4 | Combined entry for Chilliwack River (GNIS 8634) and Vedder River (GNIS 3062) and Vedder Canal (GNIS 29662). Regulation MU 2-4. |
| CHILLIWACK LAKE, UPPER PITT RIVER | REGION 2  | In-season combined entry: Chilliwack Lake (GNIS 13745) + Pitt River (GNIS 7551) |
| HALL ROAD (Mission) POND | REGION 8 8-10 | Region 8 MU 8-10. Links to both Mission Creek Regional Park Children's Fishing Pond ungazetted waterbody (49.87084°N, 119.42958°W) AND adjacent FWA waterbody 329460964. Both locations provided to e… |
| LILLOOET LAKE, LILLOOET RIVER | REGION 2 2-9 | Combined entry for Lillooet Lake (GNIS 19926) and Lillooet River (GNIS 10313). Regulation MU 2-9. |
| ROCK ISLAND LAKE | REGION 4 4-25 | Named 'Rock Isle Lake' in FWA; has both polygon and KML point with same name/MU |
| VEDDER RIVER | REGION 2 2-4 | Links to both Vedder River (stream) and Vedder Canal (polygon with GNIS_NAME_2: Vedder River). Canal is irrigation diversion from the river system. |

## out_of_region  (8)

| Name (verbatim) | Region/MU | Note |
|---|---|---|
| COPPER CREEK | REGION 1 6-12 | Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice |
| DATLAMEN CREEK | REGION 1 6-13 | Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice |
| DEENA CREEK | REGION 1 6-12 | Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice |
| HONNA RIVER | REGION 1 6-13 | Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice |
| MAMIN RIVER | REGION 1 6-13 | Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice |
| PALLANT CREEK | REGION 1 6-12 | Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice |
| TLELL RIVER | REGION 1 6-13 | Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice |
| YAKOUN RIVER | REGION 1 6-13 | Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice |

## uncertain_verify  (1)

| Name (verbatim) | Region/MU | Note |
|---|---|---|
| MCLENNAN CREEK | REGION 2 2-8 | UNCERTAIN: Could be McLennan Creek (100-052188, MU 2-4) OR McLean Creek (100-025956-073866, MU 2-8). Currently matched to McLennan Creek but regulation specifies MU 2-8 which matches McLean Creek. … |

## federal_closure  (1)

| Name (verbatim) | Region/MU | Note |
|---|---|---|
| PROVOST DAM | REGION 1 1-5 | Unnamed lake/reservoir in FWA. Federal Fisheries Act Schedule: https://laws-lois.justice.gc.ca/eng/regulations/SOR-2008-120/section-sched743254-20220221.html |

## gnis_relink_other  (150)

| Name (verbatim) | Region/MU | Note |
|---|---|---|
| ADAM RIVER (except Eve River) | REGION 1 1-10 | Matched via base name: ADAM RIVER |
| ALEXIS LAKE | REGION 5 5-13 | GNIS 9356 - Tigulhdzin (current name) / Alexis Lake (former name). Name was officially changed. |
| ANDY BAILEY (Jackfish) LAKE | REGION 7B 7-48 | Matched via base name: ANDY BAILEY LAKE |
| ARLINGTON LAKES | REGION 8 8-12 | 6 polygons with GNIS 16647 - Arlington Lakes |
| ASP (China) CREEK | REGION 8 8-5 | Matched via base name: ASP CREEK |
| AYLMER (Star) LAKE | REGION 3 3-27 | Matched via base name: AYLMER LAKE |
| BEAR RIVER (Sustut Watershed) | REGION 6 6-18 | Matched via base name: BEAR RIVER |
| BEAVER CREEK chain of lakes | REGION 5 5-2 | Beaver Creek chain of lakes in MU 5-2. FWA has name 'Beaver Creek' (GNIS 11119). |
| BIG FISH (Dunbar) LAKE | REGION 4 4-34 | Matched via base name: BIG FISH LAKE |
| BIG LAKE (approx. 10 km west of 100 Mile House) | REGION 5 5-2 | Disambiguate using GNIS ID |
| BIG LAKE (approx. 30 km west of Likely) | REGION 5 5-2 | Disambiguate using GNIS ID |
| BIG O.K. ("Island") LAKE | REGION 3 3-18 | Matched via base name: BIG O.K. LAKE |
| BIGHORN (Ram) CREEK | REGION 4 4-2 | Matched via base name: BIGHORN CREEK |
| BISHOP ("Brown") LAKE | REGION 5 5-13 | Matched via base name: BISHOP LAKE |
| BLUE LAKE (Soda Creek area) | REGION 5 5-2 | Disambiguate using GNIS ID |
| BLUE LAKE (near Alexandria) | REGION 5 5-2 | Disambiguate using GNIS ID |
| BOAR LAKE (Dog Creek drainage) | REGION 5 5-2 | Matched via base name: BOAR LAKE |
| BURNELL (Sawmill) LAKE | REGION 8 8-1 | Matched via base name: BURNELL LAKE |
| BUTLER LAKE (east of Allison Lake) | REGION 8 8-6 | Matched via base name: BUTLER LAKE |
| CAMERON LAKES | REGION 7B 7-31 | 3 polygons with GNIS 38712 - Cameron Lakes |
| CANIM LAKE (see map on page 42) | REGION 5 5-1 | Matched via base name: CANIM LAKE |
| CANIM RIVER (also in M.U. 3-46) | REGION 5 5-15 | Matched via base name: CANIM RIVER |
| CANIM RIVER (also in M.U. 5-15) | REGION 3 3-46 | Matched via base name: CANIM RIVER |
| CARIBOU LAKES | REGION 4 4-32 | GNIS names: North Caribou Lake (37054) and South Caribou Lake (37055). Split into two lakes in FWA. |
| CEDAR LAKE (near Golden) | REGION 4 4-34 | Matched via base name: CEDAR LAKE |
| CHRISTOPHER LAKE (Canim Lake area) | REGION 5 5-15 | Matched via base name: CHRISTOPHER LAKE |
| CLEAR LAKE (Quadra Island) | REGION 1 1-15 | Matched via base name: CLEAR LAKE |
| COLUMBIA RIVER | REGION 4 4-8,4-15,4-26,4-34,4-38 | Columbia River and all named channels sharing the same watershed code in Zone 4. |
| COMO (Welcome) LAKE | REGION 2 2-8 | Matched via base name: COMO LAKE |
| CONNOR LAKE | REGION 4 4-23 | 3 polygons with GNIS 19304 - Connor Lakes (plural in FWA) |
| COOK LAKE (Solomon Lake area) | REGION 5 5-2 | Matched via base name: COOK LAKE |
| COWICHAN LAKE (including Bear Lake) | REGION 1 1-4 | Matched via base name: COWICHAN LAKE |
| COWICHAN RIVER (see map below) | REGION 1 1-4 | Matched via base name: COWICHAN RIVER |
| CRAZY BEAR (Ginny) LAKE | REGION 5 5-6 | Matched via base name: CRAZY BEAR LAKE |
| DEER LAKE (Burnaby) | REGION 2 2-8 | Matched via base name: DEER LAKE |
| DEER LAKE (Sasquatch Park) | REGION 2 2-18 | Matched via base name: DEER LAKE |
| DINOSAUR LAKE (Reservoir Downstream of W.A.C. Bennett Dam) | REGION 7B 7-31 | Matched via base name: DINOSAUR LAKE |
| DOWNTON LAKE (Reservoir) | REGION 3 3-33 | Matched via base name: DOWNTON LAKE |
| DUCK LAKE (permit required see Note on page 34) | REGION 4 4-6 | Matched via base name: DUCK LAKE |
| EAST (Fork) WHITE RIVER | REGION 4 4-24 | Matched via base name: EAST WHITE RIVER |
| ECHOES LAKE (near Kimberley) | REGION 4 4-20 | 2 polygons with GNIS 7369 - Echoes Lakes (plural in FWA) |
| ELK RIVER (also see Buttle Lake's Tributaries) | REGION 1 1-9 | Matched via base name: ELK RIVER |
| EMILY ("TURTLE") LAKE | REGION 2 2-16 | Matched via base name: EMILY LAKE |
| FISH LAKE (Taseko Lake area) | REGION 5 5-4 | Matched via base name: FISH LAKE |
| FRASER RIVER | REGION 3 3-14 | Fraser River in Region 3. Channels and sloughs are in Zone 2 only. |
| FRASER RIVER | REGION 5 5-2 | Fraser River in Region 5. Channels and sloughs are in Zone 2 only. |
| FRASER RIVER | REGION 7A 7-9 | Fraser River in Region 7A. Channels and sloughs are in Zone 2 only. |
| HAREWOOD (Extension) LAKE | REGION 1 1-5 | Matched via base name: HAREWOOD LAKE |
| HARRISON RIVER (from the Fraser River upstream to Harrison L | REGION 2 2-18 | Matched via base name: HARRISON RIVER |
| HART LAKE (Crooked River Provincial Park) | REGION 7A 7-16 | Matched via base name: HART LAKE |
| HATZIC LAKE AND SLOUGH | REGION 2 2-8 | MU 2-8. GNIS 16118 (Hatzic Lake), GNIS 16120 (Hatzic Slough) and GNIS 37373 (Lower Hatzic Slough). |
| HAYS CREEK (in Prince Rupert) | REGION 6 6-14 | Matched via base name: HAYS CREEK |
| HEALY (Panther) LAKE | REGION 1 1-5 | Matched via base name: HEALY LAKE |
| HEALY LAKE'S OUTLET STREAM | REGION 1 1-5 | Healy Lake's outlet stream is the South Englishman River (GNIS 26468) in Region 1 MU 1-5. |
| HEFFLEY LAKE (parts of ) | REGION 3 3-27 | Matched via base name: HEFFLEY LAKE |
| HORSEFLY RIVER (from Quesnel Lake to Horsefly River Falls) | REGION 5 5-2 | Matched via base name: HORSEFLY RIVER |
| ILLUSION LAKES | REGION 1 1-6 | 7 polygons with GNIS 9801 - Illusion Lakes |
| ISLAHT (Horseshoe) LAKE | REGION 8 8-11 | Matched via base name: ISLAHT LAKE |
| IVEY (Horseshoe) LAKE | REGION 2 2-11 | Matched via base name: IVEY LAKE |
| KITCHENER (Meadow) CREEK | REGION 4 4-6 | Matched via base name: KITCHENER CREEK |
| KITIMAT RIVER (Angling regulations for the Kitimat River are | REGION 6 6-3 | Matched via base name: KITIMAT RIVER |
| KITSUMKALUM (Kalum) RIVER | REGION 6 6-15 | Matched via base name: KITSUMKALUM RIVER |
| KOOTENAY LAKE - LOWER WEST ARM (for location see map on page | REGION 4 4-7 | TODO: Currently links to full Kootenay Lake (GNIS 14091). Needs custom polygon subdivision to isolate Lower West Arm. MU 4-4. See regulation map page 34. |
| KOOTENAY LAKE - MAIN BODY (for location see map on page 34) | REGION 4 4-19 | TODO: Currently links to full Kootenay Lake (GNIS 14091). Needs custom polygon subdivision to isolate Main Body (excluding West Arm zones). MU 4-4. See regulation map page 34. |
| KOOTENAY LAKE - UPPER WEST ARM (for location see map on page | REGION 4 4-7 | TODO: Currently links to full Kootenay Lake (GNIS 14091). Needs custom polygon subdivision to isolate Upper West Arm. MU 4-4. See regulation map page 34. |
| KOOTENAY LAKE, ALL PARTS (Main Body, Upper West Arm and Lowe | REGION 4 4-19 | Kootenay Lake - Main Body, Upper West Arm and Lower West Arm. GNIS 14091 - Kootenay Lake. Regulation MU 4-19. |
| KUMP (Lost) LAKE | REGION 8 8-5 | Matched via base name: KUMP LAKE |
| KWITZIL LAKE (also known as Gravelpit Lake) | REGION 7A 7-12 | Matched via base name: KWITZIL LAKE |
| LAFARGE (Pinetree Gravel Pit) LAKE | REGION 2 2-8 | Matched via base name: LAFARGE LAKE |
| LAJOIE (Little Gun) LAKE | REGION 3 3-32 | Matched via base name: LAJOIE LAKE |
| LAKE WESTON ("Weston Lake") | REGION 1 1-1 | Matched via base name: LAKE WESTON |
| LAMBLY (Bear) LAKE | REGION 8 8-11 | Matched via base name: LAMBLY LAKE |
| LEMON LAKE (in Gibbons Creek drainage) | REGION 5 5-2 | Matched via base name: LEMON LAKE |
| LIGHTNING LAKE (Manning Park) | REGION 2 2-1 | Matched via base name: LIGHTNING LAKE |
| LILY ("Paq") LAKE | REGION 2 2-5 | Matched via base name: LILY LAKE |
| LITTLE MAIN LAKE (Quadra Island) | REGION 1 1-15 | Matched via base name: LITTLE MAIN LAKE |
| LONG LAKE (Nanaimo) | REGION 1 1-5 | Disambiguate using GNIS ID |
| LOST LAKE (near Taweel Lake) | REGION 3 3-39 | Matched via base name: LOST LAKE |
| LOST LAKE (near Whistler) | REGION 2 2-8 | Matched via base name: LOST LAKE |
| MACHETE LAKE (including that portion known as "Bear"Lake) | REGION 3 3-30 | Matched via base name: MACHETE LAKE |
| MAHOOD LAKE (see map on page 28 for area closure) | REGION 3 3-46 | Matched via base name: MAHOOD LAKE |
| MAIN LAKE (Quadra Island) | REGION 1 1-15 | 2 polygons with GNIS 12126 - Main Lake on Quadra Island |
| MARBLE ("Link") RIVER (only between Victoria and Alice lakes | REGION 1 1-13 | Matched via base name: MARBLE RIVER |
| MCCULLOCH RESERVOIR | REGION 8 8-10 | 3 polygons with GNIS 15973 - McCulloch Reservoir |
| MELLIN (Jerry) LAKE | REGION 3 3-12 | Matched via base name: MELLIN LAKE |
| MILL LAKE (Abbotsford) | REGION 2 2-4 | Matched via base name: MILL LAKE |
| MIXAL (Bear) LAKE | REGION 2 2-5 | Matched via base name: MIXAL LAKE |
| MOOSE ("Alces") LAKE | REGION 4 4-24 | Matched via base name: MOOSE LAKE |
| MOYIE LAKE | REGION 4 4-5 | 2 polygons with GNIS 15779 - Moyie Lake spans MUs 4-4 and 4-5 |
| NAHATLATCH LAKE (east and west) | REGION 3 3-15 | 2 polygons with GNIS 16470 - Nahatlatch Lake (east and west) |
| NALTESBY LAKE (Bobtail Lake) | REGION 7A 7-12 | Matched via base name: NALTESBY LAKE |
| NANCY GREENE (Sheep) LAKE | REGION 4 4-9 | Matched via base name: NANCY GREENE LAKE |
| NATHAN (Beaver) CREEK | REGION 2 2-4 | Matched via base name: NATHAN CREEK |
| NICOMEN SLOUGH | REGION 2 2-8 | Nicomen Slough (GNIS 21105) in Region 2. Includes 7 specific waterbody polygons plus the GNIS stream match. |
| NORBURY (Garbutt) LAKE | REGION 4 4-22 | Matched via base name: NORBURY LAKE |
| NORBURY (Little Bull) CREEK | REGION 4 4-22 | Matched via base name: NORBURY CREEK |
| NORNS (Pass) CREEK | REGION 4 4-15 | Matched via base name: NORNS CREEK |
| NORRISH (Suicide) CREEK | REGION 2 2-8 | Matched via base name: NORRISH CREEK |
| NORTH (Fork) WHITE RIVER | REGION 4 4-24 | Matched via base name: NORTH WHITE RIVER |
| PAT ("Six Mile") LAKE | REGION 3 3-19 | Matched via base name: PAT LAKE |
| PEACE RIVER (From Hwy. 29 bridge to the Site C dam) | REGION 7B 7-31 | Matched via base name: PEACE RIVER |
| PEACE RIVER (From Site C dam to boundary signs 1,200m downst | REGION 7B 7-31 | Matched via base name: PEACE RIVER |
| PRIOR LAKE | REGION 1 1-2 | Direct GNIS ID match |
| PROSPECT LAKE | REGION 1 1-2 | Direct GNIS ID match |
| PRUDHOMME LAKE (south of the Hwy 16 bridge) | REGION 6 6-14 | Matched via base name: PRUDHOMME LAKE |
| RAINBOW LAKES | REGION 7B 7-52 | 2 polygons with GNIS 30756 - Rainbow Lakes |
| RICE LAKE (North Vancouver) | REGION 2 2-8 | Matched via base name: RICE LAKE |
| ROSEN LAKE (Read Island) | REGION 1 1-15 | Matched via base name: ROSEN LAKE |
| ROSS (Six Mile) LAKE | REGION 6 6-9 | Matched via base name: ROSS LAKE |
| ROSS LAKE (Boundary between Ross Lake and Skagit River is ma | REGION 2 2-2 | Matched via base name: ROSS LAKE |
| RYKERTS ("Vic Mawson") LAKE | REGION 4 4-6 | Matched via base name: RYKERTS LAKE |
| SAYRES (Cedar) LAKE | REGION 2 2-8 | Matched via base name: SAYRES LAKE |
| SCHKAM (Lake of the Woods) LAKE | REGION 2 2-18 | Matched via base name: SCHKAM LAKE |
| SCOTT (Hoy) CREEK | REGION 2 2-8 | Matched via base name: SCOTT CREEK |
| SETON RIVER (includes BC Hydro Power Canal upstream of the d | REGION 3 3-16 | Matched via base name: SETON RIVER |
| SHANNON LAKE (netted off portion on the south end of the lak | REGION 8 8-11 | Matched via base name: SHANNON LAKE |
| SHUSWAP LAKE (see maps on page 28) (includes Little Shuswap  | REGION 3 3-26 | Matched via base name: SHUSWAP LAKE |
| SILVER (Silverhope) LAKE | REGION 2 2-2 | Matched via base name: SILVER LAKE |
| SILVERHOPE (Silver) CREEK | REGION 2 2-2 | Matched via base name: SILVERHOPE CREEK |
| SILVERTHORNE (Erickson) LAKE | REGION 6 6-9 | Matched via base name: SILVERTHORNE LAKE |
| SKAGIT RIVER (boundary between Skagit River and Ross Lake is | REGION 2 2-2 | Matched via base name: SKAGIT RIVER |
| SKEENA RIVER (mainstem only) | REGION 6 6-10 | Matched via base name: SKEENA RIVER |
| SNEEZIE LAKE (near Timothy Lake) | REGION 5 5-2 | Matched via base name: SNEEZIE LAKE |
| SOWERBY ("Grundy") LAKE | REGION 4 4-21 | Matched via base name: SOWERBY LAKE |
| STUM LAKE | REGION 5 5-13 | GNIS 16247 - Tegunlin (current name) / Stum Lake (former name). Name was officially changed. |
| SUMALLO RIVER (includes "Cedar" Lake, at Sunshine Valley) | REGION 2 2-2 | Matched via base name: SUMALLO RIVER |
| SUNDANCE LAKE | REGION 7B 7-32 | 2 polygons with GNIS 20296 - Sundance Lakes (plural in FWA) |
| SUSKWA (Bear) RIVER | REGION 6 6-8 | Matched via base name: SUSKWA RIVER |
| TACHEEDA LAKES (north and south) | REGION 7A 7-16 | 2 polygons with GNIS 3956 - Tacheeda Lakes (north and south) |
| TAGISH LAKE | REGION 6 6-27 | 2 polygons with GNIS 23158 - Tagish Lake |
| TATLATUI LAKE | REGION 7A 7-39 | 2 polygons with GNIS 25404 - Tatlatui Lake |
| TATSATUA CREEK (formerly known as Tatsamenie Lake's outlet s | REGION 6 6-26 | Matched via base name: TATSATUA CREEK |
| TEEPEE LAKE (adjacent to West Road River) | REGION 5 5-13 | Matched via base name: TEEPEE LAKE |
| THETIS LAKE | REGION 1 1-1 | 2 polygons with GNIS 21695 - Thetis Lake |
| TONKAWATLA (Tum Tum) CREEK | REGION 4 4-32 | Matched via base name: TONKAWATLA CREEK |
| TREPANIER RIVER | REGION 8 8-8 | Trepanier River in MU 8-8. FWA has name 'Trépanier Creek' (GNIS 27648). |
| TROUT CREEK (Wells Gray Park) | REGION 3 3-46 | Matched via base name: TROUT CREEK |
| TROUT LAKE (Sasquatch Park) | REGION 2 2-18 | Matched via base name: TROUT LAKE |
| TROUT LAKE (Sechelt) | REGION 2 2-5 | Matched via base name: TROUT LAKE |
| TWIN LAKES | REGION 2 2-8 | 2 polygons with GNIS 30212 - Twin Lakes |
| TWIN LAKES | REGION 8 8-1 | 3 polygons with GNIS 3086 - Twin Lakes span MUs 8-1 and 8-2 |
| UPPER ARROW LAKE (drawdown area) | REGION 4 4-31,4-32 | Matched via base name: UPPER ARROW LAKE |
| VASEUX LAKE (including two lagoons on the west side of Okana | REGION 8 8-1 | Matched via base name: VASEUX LAKE |
| WAHLEACH ("Jones") LAKE | REGION 2 2-3 | Matched via base name: WAHLEACH LAKE |
| WAUGH (Worm) LAKE | REGION 2 2-5 | Matched via base name: WAUGH LAKE |
| WEAVER LAKE and WEAVER CREEK | REGION 2 2-19 | MU 2-19. GNIS 25954 (Weaver Lake) and GNIS 25951 (Weaver Creek). |
| WHITE RIVER (see also east White & North White Rivers) | REGION 4 4-24 | Matched via base name: WHITE RIVER |
| WOLF LAKE (situated approx. 2.3 km northeast of Lorin Lake) | REGION 5 5-1 | Matched via base name: WOLF LAKE |
| YELLOWHEAD LAKE | REGION 7A 7-1 | 2 polygons with GNIS 30397 - Yellowhead Lake |
| ZYMOETZ (Copper) RIVER | REGION 6 6-9 | Matched via base name: ZYMOETZ RIVER |

## waterbody_relink  (18)

| Name (verbatim) | Region/MU | Note |
|---|---|---|
| "BLUEY LAKE POTHOLES" | REGION 8 8-6 | Bluey Lake Potholes in Region 8 MU 8-6. 10 specific waterbody polygons. |
| "LOST" LAKE | REGION 4 4-23 | Near Elkford; found on AllTrails: https://www.alltrails.com/poi/canada/british-columbia/elkford/lost-lake; Using specific waterbody_key (GNIS 18009 is a different Lost Lake) |
| CLIFFORD (Cliff ) LAKE | REGION 8 8-5 | FWA incorrectly has GNIS_ID 31706 in Region 1 MU 1-14; correct lake is in Region 8 MU 8-5. BC Lakes Database: survey_id 3834 ('CLIFF AND RICK LAKES INVESTIGATION, JULY 24 & 25 1989', WBID 174079), … |
| DINA LAKE #1 | REGION 7A 7-30 | Found via BC Lakes Database ('Dina Lake #1 Pygmy Whitefish Study', WBID: 00357PARA, located north of Mackenzie). Regulation MU 7-30. |
| DINA LAKE #2 | REGION 7A 7-30 | Found via BC Lakes Database ('A Fisheries Evaluation of Dina Lake #2', WBID: 00346PARA). Regulation MU 7-30. |
| FIVE O'CLOCK LAKE (approx. 800 m southeast of Cup Lake) | REGION 8 8-14 | Lake in Region 8 MU 8-14 (approx. 800 m southeast of Cup Lake); found via BC Lakes Database (Report ID 20691: 'Memo to File - Five O'Clock Lake Fish 00796KETL', WBID: 00796KETL). Survey date: May 1… |
| HEADWATER LAKE #1 | REGION 8 8-8 | Lake in Region 8 MU 8-8; found via BC Lakes Database (survey_id 5824: 'LAKE OVERVIEW DATA - HEADWATER LAKES;LAKE #1', WBID 175465). Survey date: Sep 1, 1972. |
| LEWIS ("Cameron") SLOUGH | REGION 4 4-21 | Found in EAUBC Lakes dataset |
| LITTLE DUM LAKE | REGION 3 3-28 | Lake in MU 3-28. BC Lakes Database: WBID 00618LNTH. ACAT ObjectID 15306 ('A Reconnaissance Survey Of Dum 2'). Found in stocked lakes map. Part of Dum Lake group (gazette: https://apps.gov.bc.ca/pub… |
| MAYDOE LAKE | REGION 5 5-6 | Known as Cowboy (Maydoe) Lakes in FWA; 2 polygons found via ACAT report ('Reconnaissance Survey of Cowboy (Maydoe) Lakes - 1997', WBIDs: 01344ATNA and 01372ATNA). Regulation MU 5-6. |
| MINNEKHADA MARSH | REGION 2 2-8 | Wetland in Minnekhada Regional Park. BC Lakes Database surveys: 'Minnekhada Regional Park Inventory - 2017; SU17-270318' and 'Minnekhada Regional Park Invasives - 2016; SU16-235849'. Regulation MU … |
| OKANAGAN RIVER OXBOWS | REGION 8 8-1 | Okanagan River Oxbows in MU 8-1. Multiple oxbow waterbodies and stream segments along the Okanagan River. Includes 32 watershed codes, 40 waterbody keys, and 1 linear feature ID. |
| RADAR LAKE | REGION 7B 7-20 | Lake in Region 7B MU 7-20; found via BC Lakes Database (Report ID 6880: 'Peace Fisheries Field Report: Radar Lake (230-690000-56100-63800-6807, 00680LPCE), 2004', WBID: 00680LPCE). Survey date: Jul… |
| RICKEY LAKE | REGION 8 8-5 | Lake in Region 8 MU 8-5 (WBID: 00472SIML). Shares BC Lakes Database survey with CLIFFORD (Cliff) LAKE: survey_id 3834 ('CLIFF AND RICK LAKES INVESTIGATION, JULY 24 & 25 1989'), survey date Jul 25, … |
| ROSE VALLEY RESERVOIR (Lakeview Irrigation District) | REGION 8 8-11 | Lake in Region 8 MU 8-11 (Lakeview Irrigation District); found via BC Lakes Database (Report ID 43772: 'Survey of Rose Valley (Lake) Reservoir 1977', WBID: 00867OKAN). Survey date: May 1, 1977. |
| SARDINE LAKE | REGION 5 5-2 | Found via BC Lakes Database ('Fish Tissue Sample for Sardine lake', WBID: 00444QUES, December 1992). Regulation MU 5-2. |
| SHANDY LAKE | REGION 7A 7-5 | Lake in Region 7 MU 7-5; found via BC Lakes Database (survey_id 6484: 'BATHYMETRIC OF SHANDY LAKES', WBID 18411). Survey date: Aug 1, 1974. |
| TEBBUTT LAKE | REGION 7A 7-13 | Lake in Region 7 MU 7-13; found via BC Lakes Database (Report ID 9703: 'Tebbutt Lake - Reconnaissance Survey 1987 02283STUL', WBID: 02283STUL). Survey date: Jul 1, 1987. |

## wsc_relink  (9)

| Name (verbatim) | Region/MU | Note |
|---|---|---|
| "SEELEY" CREEK (outlet of Seeley Lake) | REGION 6 6-9 | Seeley Creek (outlet of Seeley Lake) in MU 6-9. |
| MAKA CREEK | REGION 3 3-13 | Two candidates in MU 3-13: (1) WSC 100-190442-244975-232973-504304 (103 segments) - MOST LIKELY match as it is found inside fisheries sensitive wildlife area, or (2) WSC 100-190442-244975-119256-79… |
| MOSES CREEK | REGION 4 4-39 | Moses Creek in MU 4-39. No GNIS name in FWA. Reference: Fish Collection Permit CB12-80431 Moses Creek Hydro Project Fisheries Impact Assessment (ACAT Report ID 37080) - proposed hydroelectric proje… |
| MUCHALAT RIVER | REGION 1 1-12 | Direct watershed code match |
| NAUTLEY RIVER | REGION 7A 7-13 | Nautley River in MU 7-13. |
| NELSON CREEK | REGION 2 2-8 | Two candidates in MU 2-8: (1) Nelson Creek in West Vancouver (WSC 900-088087) - MOST LIKELY match, or (2) Nelson Creek (ditch) near Maillardville Coquitlam (WSC 100-019698-194371). Linked to both f… |
| SOUTH ALOUETTE RIVER | REGION 2 2-8 | South Alouette River. Regulation MU 2-8. |
| WEST ROAD ("Blackwater") RIVER | REGION 5 5-12,5-13 | West Road River (Blackwater River). Regulation MUs 5-12, 5-13. |
| WEST ROAD ("Blackwater") RIVER | REGION 6  | West Road River (Blackwater River). Regulation MU 6-1. |

## skip_crosslist_variant  (17)

| Name (verbatim) | Region/MU | Note |
|---|---|---|
| "BLACKWATER" RIVER | REGION 5 5-13 | Alternate name for WEST ROAD ("Blackwater") RIVER. Same waterbody, regulation MU 5-13. Primary entry is in Region 6. |
| "BROWN" LAKE | REGION 5 5-13 | Alternate name for BISHOP ("Brown") LAKE. Same waterbody. |
| "CHINAMAN" LAKE | REGION 7B 7-35 | Same waterbody as CHUNAMUN LAKE (waterbody_key 328995585, WBID: 00552UPCE). Both names appear in BC Lakes Database surveys. |
| "LINK" RIVER | REGION 1 1-13 | Listed as 'Marble (Link) River' in gazetteer. 'Link River' is an alternate name. |
| BEAR RIVER | REGION 1 1-10 | Regulation says 'See Amor de Cosmos Creek'. Bear River is historical/alternate name for Amor de Cosmos Creek. Evidence: https://www.facebook.com/aboriginal.journeys/videos/819108737814861/ |
| BLACKWATER RIVER | REGION 7A 7-10 | Regulation says 'See West Road River'. Alternate name for WEST ROAD ('Blackwater') RIVER. Regulation MU 7-10. |
| CAMERON SLOUGH | REGION 4 4-21 | Alternate name for LEWIS ("Cameron") SLOUGH. Same waterbody in regulation MU 4-21. |
| COPPER RIVER | REGION 6 6-9 | Alternate name for ZYMOETZ (Copper) RIVER. Same waterbody, regulation MU 6-9. |
| HEBER CREEK | REGION 1  |  |
| ISHKHEENICKH RIVER | REGION 6 6-14 | Regulation says 'See Ksi Hlginx River'. River has been renamed to KSI HLGINX (GNIS 4069). Regulation MU 6-14. |
| KWINAMASS RIVER | REGION 6 6-14 | Regulation says 'See Ksi X'anmas River'. River has been renamed to KSI X'ANMAS (GNIS 3815). Regulation MU 6-14. |
| LITTLE CAMPBELL RIVER | REGION 2 2-4 | Alternate name for Campbell River in MU 2-4. Regulations already covered under Campbell River entry. |
| MCNAUGHTON LAKE | REGION 4 4-36 | Regulation says 'See Kinbasket Lake'. McNaughton Lake is an alternate name for part of Kinbasket Lake. |
| MCQUEEN CREEK | REGION 6 6-30 | Alternate name for HEVENOR ("McQueen") CREEK. Same waterbody. |
| SAWMILL LAKE | REGION 8 8-1 | Alternative name for Burnell Lake. Same waterbody. |
| SEASKINNISH CREEK | REGION 6 6-15 | Regulation says 'See Ksi Sgasginist Creek'. Creek has been renamed to KSI SGASGINIST CREEK. Regulation MU 6-15. |
| TSEAX RIVER | REGION 6 6-14 | River has been renamed to KSI SII AKS RIVER. Regulation MU 6-14. |

## skip_not_found  (7)

| Name (verbatim) | Region/MU | Note |
|---|---|---|
| FROG LAKE | REGION 5 5-6 | Searched but only found in Region 3 (MU 3-29, GNIS 12556, waterbody_key 330954838), Region 1 (MU 1-10, GNIS 12554, waterbody_key 329173098), and Region 7 (MU 7-30, GNIS 56145, waterbody_key 3293833… |
| HIDDEN LAKE | REGION 5 5-6 | Searched but only found in Region 5 MU 5-15 (GNIS 21382, waterbody_key 329635892), not in regulation MU 5-6. May be MU boundary issue or typo in regulations. Reference: BC Freshwater Fishing Regula… |
| LITTLE STAWAMUS CREEK | REGION 2 2-8 | Known location in MU 2-8 near Squamish but stream does not exist in FWA mapping data. Would require custom stream segment creation. Reference: DFO Stream Summary Catalogue 'Little Stawamus Creek' h… |
| REDFERN LAKE | REGION 5 5-15 | Searched but only found in Region 7 (MU 7-42, GNIS 38145, waterbody_key 329423854), not Region 5 (regulation MU 5-15). May be typo in regulations or require duplicate feature creation. Reference: B… |
| SECRET LAKE | REGION 5 5-6 | Searched but only found in Region 8 (MU 8-7, GNIS 37609, waterbody_key 329220075) and Region 3 (MU 3-30, GNIS 38166, waterbody_key 331154867), not Region 5 (regulation MU 5-6). May be typo in regul… |
| SQUARE LAKE | REGION 5 5-6 | Searched but only found in Region 5 MU 5-3 (GNIS 29739, waterbody_key 329677163), not in regulation MU 5-6. May be MU boundary issue or typo in regulations. Reference: BC Freshwater Fishing Regulat… |
| SQUIRREL LAKE | REGION 6 6-1 | Searched but only found in Region 5 (MU 5-1, GNIS 38786, waterbody_key 329642030). Should be close to border with Region 5 MUs 5-10, 5-12, or 5-13 (neighboring MU 6-1). Lake likely exists near regi… |

## skip_other  (14)

| Name (verbatim) | Region/MU | Note |
|---|---|---|
| ARROW LAKES | REGION 4 4-15,4-32 | Regulation refers to Upper/Lower Arrow Lake details |
| ARROW LAKES' TRIBUTARIES | REGION 4  | Likely covered by Upper/Lower tributaries |
| BASALT LAKE | REGION 5 6-1 | Duplicate entry - already covered in Region 6 (MU 6-1, GNIS 18842) |
| CHIPMUNK LAKE | REGION 5 6-1 | Cross-listed entry - already covered in Region 6 (MU 6-1) |
| ENDAKO RIVER | REGION 7A 7-12 | Cross-listed entry - already covered in Region 6 (MUs 6-4, 6-5) |
| GATCHO LAKE | REGION 5 6-1 | Duplicate entry - already covered in Region 6 (MU 6-1, GNIS 13318) |
| NAGLICO LAKE | REGION 5 6-1 | Duplicate entry - already covered in Region 6 (MU 6-1, GNIS 16467) |
| PETTRY LAKE | REGION 5 6-1 | Duplicate entry - already covered in Region 6 (MU 6-1, GNIS 20981) |
| SEVEN MILE RESERVOIR | REGION 4 4-8 | Dammed portion of Pend d'Oreille River - uses same regulations as Pend d'Oreille River. This may change in future. Polygons are in unnamed manmade lakes. |
| SEVEN MILE RESERVOIR'S TRIBUTARIES | REGION 4 4-8 | Covered by Pend d'Oreille River tributary regulations |
| SQUIRREL LAKE | REGION 5 6-1 | Cross-listed entry - already covered in Region 6 (MU 6-1) |
| TOMS LAKE | REGION 5 6-1 | Cross-listed entry - already covered in Region 6 (MU 6-1) |
| WANETA RESERVOIR | REGION 4 4-8 | Dammed portion of Pend d'Oreille River - uses same regulations as Pend d'Oreille River. This may change in future. Polygons are in unnamed manmade lakes. |
| WANETA RESERVOIR'S TRIBUTARIES | REGION 4 4-8 | Covered by Pend d'Oreille River tributary regulations |

## other_uncategorized  (10)

| Name (verbatim) | Region/MU | Note |
|---|---|---|
| "DIANA" CREEK | REGION 6 6-14 | Diana Creek in MU 6-14. Specific stream segments identified by linear feature IDs. |
| BOWRON LAKE Park waters other than Bowron Lake | REGION 5 5-16 | Synopsis lists 'BOWRON LAKE Park waters other than Bowron Lake' in Region 5 MU 5-16. Applies to all streams and lakes within Bowron Lake Provincial Park, excluding Bowron Lake itself. Layer: TA_PAR… |
| CHILKOOT TRAIL NATIONAL HISTORIC PARK WATERS | REGION 6 6-28 | Synopsis lists 'CHILKOOT TRAIL NATIONAL HISTORIC PARK WATERS' in Region 6 MU 6-28. Applies to all streams and lakes within the Chilkoot Trail National Historic Site. Layer: HIST_HERITAGE_WRECK_SVW,… |
| CRESTON VALLEY WILDLIFE MANAGEMENT AREA (CVWMA) WATERS | REGION 4 4-6 | Synopsis lists 'CRESTON VALLEY WILDLIFE MANAGEMENT AREA (CVWMA) WATERS' in Region 4 MU 4-6. Applies to all streams and lakes within Creston Valley Wildlife Management Area. Layer: WLS_WILDLIFE_MGMT… |
| KIKOMUN CREEK PARK (all lakes in the park) | REGION 4 4-22 | Synopsis lists 'KIKOMUN CREEK PARK (all lakes in the park)' in Region 4 MU 4-22. Regulations apply specifically to lakes within Kikomun Creek Provincial Park. Layer: TA_PARK_ECORES_PA_SVW, ID field… |
| LIARD RIVER WATERSHED (see map on page 63) | REGION 7B 7-53 | Regulation specifies 'LIARD RIVER WATERSHED (see map on page 63)' in MU 7-53. FWA NAMED WATERSHED: Named Watershed ID 5, Object ID 6422089. Layer: FWA_NAMED_WORKSHEDS_POLY, ID field: NAMED_WATERSHE… |
| SICAMOUS NARROWS | REGION 3 3-26 | Specific segment of Shuswap River in MU 3-26. Linear feature ID 703030326 represents the Sicamous Narrows portion. Also associated with river polygon 700189163 (will match in future when river poly… |
| SKEENA RIVER/KISPIOX RIVER CONFLUENCE | REGION 6 6-8 | Confluence of Skeena River and Kispiox River in Region 6 MU 6-8. No FWA polygon or stream feature at the exact confluence point. Coordinates from BC Albers projection. |
| SQUAMISH POWERHOUSE CHANNEL | REGION 2 2-6 | Squamish Powerhouse Channel in Region 2 MU 2-6. Links to 5 specific stream segments representing the channel. Reference: https://a100.gov.bc.ca/pub/acat/documents/r40717/09_CMS_05_powerhouse_138868… |
| STRATHCONA PARK WATERS | REGION 1 1-9 | Synopsis lists 'STRATHCONA PARK WATERS' in Region 1. Applies to all streams and lakes within Strathcona Provincial Park. Layer: TA_PARK_ECORES_PA_SVW, ID field: ADMIN_AREA_SID. |