# T1118 — name bindings whose target lies in another MU than the printed row (lane L21, 2026-11-25, first pass)

Read-only listing from the curated files on branch feat/T1053-walk-climbs-head-lakes (after the T1117 Anderson fix) and the
live registry's item MUs. A row is listed when its own note records an MU mismatch ("boundary issue", "FWA has MU"), or
when none of the printed MUs (the override's `criteria.mus`, or "MU x-y" in a variant's note) is among the target item's
MUs. A water straddling MUs, or an MU line drawn differently from the book's, can be listed and still be right: each row
needs a look, not a fix.

98 rows.

| File | Printed name | Bound to | Printed MU | Target's MUs | Why listed | Note (start) |
|---|---|---|---|---|---|---|
| book_name_links | "ANDERSON" LAKE | wbk:329504289 | 1-3 |  | mismatch noted | Lake in MU 1-3. FWA has MU 1-6, regulation has MU 1-3 (boundary issue). Piscivorous rainbow trout and kokanee population. BC Lakes Database: survey_id 1129, WBI |
| book_name_links | "WHALE" LAKE (Gustafsen Lake area) | wbk:329116791 | 5-2 | 5-2 | mismatch noted | Whale Lake in Gustafsen Lake area; found via BC Lakes Database (survey_id from 1981: 'A Reconnaissance Survey of Whale Lake 00389DOGC, 1981'). FWA has MU 5-15,  |
| book_name_links | ARROW PARK (Mosquito) CREEK | gnis:14970 | 4-32 | 4-18 | target MU differs | GNIS name: Mosquito Creek. MU 4-18 near Arrow Park community. GNIS 7335 (also Mosquito Creek) is in MU 4-32. |
| book_name_links | BEAR LAKE (Crooked River Provincial Park) | gnis:11062 | 7-16 | 7-24 | mismatch noted; target MU differs | FWA has MU 7-24, regulation has MU 7-16 (boundary issue). Crooked River Provincial Park is mostly in 7-24 but extends into 7-16; the lake is in 7-24. |
| book_name_links | BRIDAL LAKE | gnis:37990 | 4-7 | 4-8 | mismatch noted; target MU differs | FWA has MU 4-8, regulation has MU 4-7 (boundary issue) |
| book_name_links | BRIDGE LAKE | gnis:33293 | 5-2 | 5-1 | mismatch noted; target MU differs | FWA has MU 5-1, regulation has MU 5-2 (boundary issue) |
| book_name_links | CEDAR LAKE | wbk:329522925 | 2-2 | 2-17 | mismatch noted; target MU differs | Lake on boundary with MU 2-2. FWA has MU 2-17, regulation has MU 2-2 (boundary issue). Location found from map. |
| book_name_links | CHILKO LAKE | gnis:33890 | 5-4 | 5-4, 5-5 | mismatch noted | GNIS 33890 - Tŝilhqox Biny (current name) / Chilko Lake (former name). FWA has MU 5-4, regulation has MU 5-4 (correct MU). |
| book_name_links | CHILKO LAKE'S tributary streams | gnis:33890 | 5-4 | 5-4, 5-5 | mismatch noted | Tributaries of Chilko Lake - links to parent waterbody (GNIS 33890 - Tŝilhqox Biny / Chilko Lake). FWA has MU 5-4, regulation has MU 5-4. |
| book_name_links | CHILLIWACK LAKE | gnis:13745 | 2-4 | 2-3 | mismatch noted; target MU differs | FWA has MU 2-3, regulation has MU 2-4 (boundary issue) |
| book_name_links | CHUBB LAKE | gnis:13840 | 7-10 | 7-8 | mismatch noted; target MU differs | FWA has MU 7-8, regulation has MU 7-10 (boundary issue - neighboring MUs but lake not close to border, likely mislabeled MU) |
| book_name_links | DEM LAKE | gnis:38071 | 7-25 | 7-26 | mismatch noted; target MU differs | FWA has MU 7-26, regulation has MU 7-25 (boundary issue - lake is approximately 2.5km from border) |
| book_name_links | DUNALTER LAKE (Irrigation Lake) | gnis:2687 | 6-9 | 6-8 | mismatch noted; target MU differs | FWA has MU 6-8, regulation has MU 6-9 (boundary issue). Alternate name 'Irrigation Lake' confirmed by Irrigation Lake Park near the lake. |
| book_name_links | ELLISON LAKE | gnis:16910 | 8-8 | 8-10 | mismatch noted; target MU differs | FWA has MU 8-10, regulation has MU 8-8 (boundary issue - lake likely mislabeled MU) |
| book_name_links | EMERALD LAKE | gnis:31097 | 7-15 | 7-16 | mismatch noted; target MU differs | FWA has MU 7-16, regulation has MU 7-15 (boundary issue). Confirmed correct location via stocked lake maps - lake is in MU 7-16. |
| book_name_links | FISHER MAIDEN LAKE | wbk:329524272 | 4-26 | 4-26 | mismatch noted | Found via fish stocking records and ACAT reports (https://a100.gov.bc.ca/pub/acat/public/viewReport.do?reportId=51502). FWA has MU 4-17, regulation has MU 4-26  |
| book_name_links | FRAZER LAKE | gnis:12484 | 8-9 | 8-10 | mismatch noted; target MU differs | FWA has MU 8-10, regulation has MU 8-9 (boundary issue - lake is 700m from border) |
| book_name_links | HALL LAKE | gnis:36134 | 4-34 | 4-20 | mismatch noted; target MU differs | FWA has MU 4-20, regulation has MU 4-34 (boundary issue) |
| book_name_links | HIAWATHA LAKE | gnis:21359 | 4-3 | 4-4 | mismatch noted; target MU differs | FWA has MU 4-4, regulation has MU 4-3 (boundary issue) |
| book_name_links | HOPE SLOUGH |  | 2-8 |  | mismatch noted | FWA has MUs 2-3, 2-4, regulation has MU 2-8 (boundary issue) |
| book_name_links | HUSH LAKE | gnis:22322 | 5-15 | 5-2 | mismatch noted; target MU differs | FWA has MU 5-2, regulation has MU 5-15 (boundary issue - lake is on the border) |
| book_name_links | JACK OF CLUBS LAKE | gnis:16935 | 5-2 | 5-15 | mismatch noted; target MU differs | FWA has MU 5-15, regulation has MU 5-2 (boundary issue) |
| book_name_links | KATHERINE LAKE | gnis:11358 | 5-15 | 5-2 | mismatch noted; target MU differs | FWA has MU 5-2, regulation has MU 5-15 (boundary issue - lake is close to border) |
| book_name_links | LA SALLE LAKES | gnis:2482 | 7-3 | 7-5 | mismatch noted; target MU differs | FWA has MU 7-5, regulation has MU 7-3 (boundary issue). GNIS ID matches both polygons. |
| book_name_links | LAKELSE RIVER |  | 6-10 |  | mismatch noted | FWA has MU 6-11, regulation has MU 6-10 (boundary issue) |
| book_name_links | LANGFORD LAKE | gnis:16325 | 1-2 | 1-1 | mismatch noted; target MU differs | FWA has MU 1-2, regulation has MU 1-21 (boundary issue) |
| book_name_links | LLOYD LAKE | gnis:33438 | 3-30 | 3-29 | mismatch noted; target MU differs | FWA has MU 3-29, regulation has MU 3-30 (boundary issue) |
| book_name_links | LOST LAKE | wbk:329242402 | 6-15 | 6-15 | mismatch noted | FWA has MU 6-21, regulation has MU 6-15 (boundary issue). Lake near Terrace; incorrect GNIS_ID 39158 candidate found in MU 6-21. Using specific waterbody_key fo |
| book_name_links | LUCILLE LAKE | gnis:37388 | 2-9 | 2-6 | mismatch noted; target MU differs | FWA has MU 2-6, regulation has MU 2-9 (boundary issue); name order: 'lake lucille' in gazetteer |
| book_name_links | LYNX LAKE | gnis:19238 | 7-15 | 7-10 | mismatch noted; target MU differs | FWA has MU 7-10, regulation has MU 7-15 (boundary issue - neighboring MUs but lake is 21km from border, likely mislabeled MU in regulations) |
| book_name_links | MACKENZIE CREEK | gnis:12042 | 4-21 | 4-31 | target MU differs | BOOK ERRATUM (owner 2026-10-26): the synopsis prints MACKENZIE CREEK under MU 4-21 (Region 4, printed p.38); that is a typo for MU 4-31 — it is the Mackenzie Cr |
| book_name_links | MIAMI CREEK |  | 2-19 |  | mismatch noted | FWA has MU 2-18, regulation has MU 2-19 (boundary issue) |
| book_name_links | NATION ARM (Williston Lake) | wbk:-16 | 7-30 | 7-29 | target MU differs | Bound to the curated polygon(s) cut in the lake splitter, 2026-09-11. FWA carries one polygon for this lake and the book writes it as several waters; `added_lak |
| book_name_links | NATION RIVER |  | 7-30 |  | mismatch noted | FWA has MUs 7-28, 7-29, regulation has MU 7-30 (boundary issue) |
| book_name_links | NIMPO LAKE | gnis:21146 | 5-12 | 5-6 | mismatch noted; target MU differs | FWA has MU 5-6, regulation has MU 5-12 (boundary issue) |
| book_name_links | NOLA LAKE | gnis:1816 | 1-9 | 1-10 | mismatch noted; target MU differs | FWA has MU 1-10, regulation has MU 1-9 (boundary issue) |
| book_name_links | OWEEGEE LAKE | gnis:21851 | 6-16 | 6-17 | mismatch noted; target MU differs | FWA has MU 6-17, regulation has MU 6-16 (boundary issue - lake is less than 1.5km from border) |
| book_name_links | PANTHER LAKE | gnis:21965 | 1-5 | 1-6 | mismatch noted; target MU differs | FWA has MU 1-6, regulation has MU 1-5 (boundary issue) |
| book_name_links | PRIOR LAKE | gnis:26972 | 1-2 | 1-1 | target MU differs | Direct GNIS ID match |
| book_name_links | PROSPECT LAKE | gnis:26995 | 1-2 | 1-1 | target MU differs | Direct GNIS ID match |
| book_name_links | RANCHERIA RIVER'S TRIBUTARIES |  | 6-25 |  | mismatch noted | Tributaries of Little Rancheria River - links to parent waterbody. FWA has MU 6-24, regulation has MU 6-25 (boundary issue). |
| book_name_links | SQUARE LAKE (located in Crooked River Provincial Park) | wbk:329102648 | 7-16 | 7-24 | mismatch noted; target MU differs | FWA has MU 7-12, regulation has MU 7-16 (boundary issue). Crooked River Provincial Park is partly in both 7-24 and 7-16; the lake is in 7-24 which is very close |
| book_name_links | STAWAMUS RIVER |  | 2-9 |  | mismatch noted | FWA has MU 2-8, regulation has MU 2-9 (boundary issue) |
| book_name_links | STELLAKO RIVER |  | 7-12 |  | mismatch noted | FWA has MU 7-13, regulation has MU 7-12 (boundary issue) |
| book_name_links | SWAN LAKE | gnis:20911 | 7-20 | 7-33 | mismatch noted; target MU differs | Disambiguate from GNIS 20912; FWA has MU 7-33, regulation has MU 7-20 (boundary issue - lake is about 800m from boundary with MU 7-20) |
| book_name_links | SWAN LAKE | gnis:20909 | 8-26 | 8-22 | mismatch noted; target MU differs | FWA has MU 8-22, regulation has MU 8-26 (boundary issue). Confirmed correct lake via stocked lakes map. Other Swan Lake (GNIS 37740) is in MU 8-6. |
| book_name_links | TANYA LAKE'S TRIBUTARIES | gnis:23919 | 5-10 | 5-10 | mismatch noted | Tributaries of Tanya Lakes - links to parent waterbody (GNIS 23919 - Tanya Lakes). FWA has MU 5-10, regulation has MU 5-10. |
| book_name_links | TSITNIZ LAKE | gnis:29262 | 7-9 | 7-8 | mismatch noted; target MU differs | FWA has MU 7-8, regulation has MU 7-9 (boundary issue - lake is about 3km from border) |
| book_name_links | TUPPER RIVER |  | 7-20 |  | mismatch noted | FWA has MU 7-33, regulation has MU 7-20 (boundary issue - regulation refers to outlet weir at Swan Lake which is in 7-33) |
| book_name_links | TWIN LAKES | wbk:329524100, wbk:328989162 | 4-34 | 4-21 | mismatch noted; target MU differs | 2 polygons near Premier Lake (accessed via 15min hike from overflow camping area); first lake encountered is Twin Lakes (Yankee); FWA MU 4-21, regulation MU 4-3 |
| book_name_links | WAHLA LAKE | gnis:22471 | 6-2 | 6-1 | mismatch noted; target MU differs | FWA has MU 6-1, regulation has MU 6-2 (boundary issue - neighboring MUs but lake is 7km from boundary) |
| name_variants | ANDERSON LAKE | wbk:329504289 | 1-3, 1-6 |  | mismatch noted | Lake in MU 1-3. FWA has MU 1-6, regulation has MU 1-3 (boundary issue). Piscivorous rainbow trout and kokanee population. BC Lakes Database: survey_id 1129, WBI |
| name_variants | BEAR LAKE | wbk:329102601, gnis:11062 | 7-16, 7-24 | 7-24 | mismatch noted | FWA has MU 7-24, regulation has MU 7-16 (boundary issue). Crooked River Provincial Park is mostly in 7-24 but extends into 7-16; the lake is in 7-24.; REGION 7A |
| name_variants | BRIDAL LAKE | wbk:329262703, gnis:37990 | 4-7, 4-8 | 4-8 | mismatch noted | FWA has MU 4-8, regulation has MU 4-7 (boundary issue); REGION 4 - Kootenay; MU 4-7 |
| name_variants | BRIDGE LAKE | wbk:329060770, gnis:33293 | 5-1, 5-2 | 5-1 | mismatch noted | FWA has MU 5-1, regulation has MU 5-2 (boundary issue); REGION 5 - Cariboo; MU 5-2 |
| name_variants | CEDAR LAKE | wbk:329522925 | 2-17, 2-2 | 2-17 | mismatch noted | Lake on boundary with MU 2-2. FWA has MU 2-17, regulation has MU 2-2 (boundary issue). Location found from map.; REGION 2 - Lower Mainland; MU 2-2 |
| name_variants | CHILLIWACK LAKE | wbk:329083341, gnis:13745 | 2-3, 2-4 | 2-3 | mismatch noted | FWA has MU 2-3, regulation has MU 2-4 (boundary issue); REGION 2 - Lower Mainland; MU 2-4 |
| name_variants | CHUBB LAKE | wbk:329097617, gnis:13840 | 7-10, 7-8 | 7-8 | mismatch noted | FWA has MU 7-8, regulation has MU 7-10 (boundary issue - neighboring MUs but lake not close to border, likely mislabeled MU); REGION 7A - Omineca; MU 7-10 |
| name_variants | Chilko Lake | wbk:329079684, gnis:33890 | 5-4 | 5-4, 5-5 | mismatch noted | GNIS 33890 - Tŝilhqox Biny (current name) / Chilko Lake (former name). FWA has MU 5-4, regulation has MU 5-4 (correct MU).; REGION 5 - Cariboo; MU 5-4; Tributar |
| name_variants | DEM LAKE | wbk:329353173, gnis:38071 | 7-25, 7-26 | 7-26 | mismatch noted | FWA has MU 7-26, regulation has MU 7-25 (boundary issue - lake is approximately 2.5km from border); REGION 7A - Omineca; MU 7-25 |
| name_variants | DUNALTER LAKE | wbk:329064543, gnis:2687 | 6-8, 6-9 | 6-8 | mismatch noted | FWA has MU 6-8, regulation has MU 6-9 (boundary issue). Alternate name 'Irrigation Lake' confirmed by Irrigation Lake Park near the lake.; REGION 6 - Skeena; MU |
| name_variants | ELLISON LAKE | wbk:329459126, gnis:16910 | 8-10, 8-8 | 8-10 | mismatch noted | FWA has MU 8-10, regulation has MU 8-8 (boundary issue - lake likely mislabeled MU); REGION 8 - Okanagan; MU 8-8 |
| name_variants | EMERALD LAKE | wbk:329102641, gnis:31097 | 7-15, 7-16 | 7-16 | mismatch noted | FWA has MU 7-16, regulation has MU 7-15 (boundary issue). Confirmed correct location via stocked lake maps - lake is in MU 7-16.; REGION 7A - Omineca; MU 7-15 |
| name_variants | FISHER MAIDEN LAKE | wbk:329524272 | 4-17, 4-26 | 4-26 | mismatch noted | Found via fish stocking records and ACAT reports (https://a100.gov.bc.ca/pub/acat/public/viewReport.do?reportId=51502). FWA has MU 4-17, regulation has MU 4-26  |
| name_variants | FRAZER LAKE | wbk:329459820, gnis:12484 | 8-10, 8-9 | 8-10 | mismatch noted | FWA has MU 8-10, regulation has MU 8-9 (boundary issue - lake is 700m from border); REGION 8 - Okanagan; MU 8-9 |
| name_variants | HALL LAKE | wbk:329523986, gnis:36134 | 4-20, 4-34 | 4-20 | mismatch noted | FWA has MU 4-20, regulation has MU 4-34 (boundary issue); REGION 4 - Kootenay; MU 4-34 |
| name_variants | HIAWATHA LAKE | wbk:329246408, gnis:21359 | 4-3, 4-4 | 4-4 | mismatch noted | FWA has MU 4-4, regulation has MU 4-3 (boundary issue); REGION 4 - Kootenay; MU 4-3 |
| name_variants | HOPE SLOUGH | {"wscs": ["100-072260-716322"]} | 2-8 |  | mismatch noted | FWA has MUs 2-3, 2-4, regulation has MU 2-8 (boundary issue); REGION 2 - Lower Mainland; MU 2-8 |
| name_variants | HUSH LAKE | wbk:329097899, gnis:22322 | 5-15, 5-2 | 5-2 | mismatch noted | FWA has MU 5-2, regulation has MU 5-15 (boundary issue - lake is on the border); REGION 5 - Cariboo; MU 5-15 |
| name_variants | JACK OF CLUBS LAKE | wbk:329677094, gnis:16935 | 5-15, 5-2 | 5-15 | mismatch noted | FWA has MU 5-15, regulation has MU 5-2 (boundary issue); REGION 5 - Cariboo; MU 5-2 |
| name_variants | KATHERINE LAKE | wbk:329098146, gnis:11358 | 5-15, 5-2 | 5-2 | mismatch noted | FWA has MU 5-2, regulation has MU 5-15 (boundary issue - lake is close to border); REGION 5 - Cariboo; MU 5-15 |
| name_variants | LA SALLE LAKES | wbk:328981666, wbk:328981670, gnis:2482 | 7-3, 7-5 | 7-5 | mismatch noted | FWA has MU 7-5, regulation has MU 7-3 (boundary issue). GNIS ID matches both polygons.; REGION 7A - Omineca; MU 7-3 |
| name_variants | LAKELSE RIVER | {"wscs": ["400-174068"]} | 6-10, 6-11 |  | mismatch noted | FWA has MU 6-11, regulation has MU 6-10 (boundary issue); REGION 6 - Skeena; MU 6-10 |
| name_variants | LANGFORD LAKE | wbk:329676318, gnis:16325 | 1-2, 1-21 | 1-1 | mismatch noted; target MU differs | FWA has MU 1-2, regulation has MU 1-21 (boundary issue); REGION 1 - Vancouver Island; MU 1-2 |
| name_variants | LLOYD LAKE | wbk:328966812, gnis:33438 | 3-29, 3-30 | 3-29 | mismatch noted | FWA has MU 3-29, regulation has MU 3-30 (boundary issue); REGION 3 - Thompson-Nicola; MU 3-30 |
| name_variants | LOST LAKE | wbk:329242402 | 6-15, 6-21 | 6-15 | mismatch noted | FWA has MU 6-21, regulation has MU 6-15 (boundary issue). Lake near Terrace; incorrect GNIS_ID 39158 candidate found in MU 6-21. Using specific waterbody_key fo |
| name_variants | LUCILLE LAKE | wbk:329532512, gnis:37388 | 2-6, 2-9 | 2-6 | mismatch noted | FWA has MU 2-6, regulation has MU 2-9 (boundary issue); name order: 'lake lucille' in gazetteer; REGION 2 - Lower Mainland; MU 2-9 |
| name_variants | LYNX LAKE | wbk:329547767, gnis:19238 | 7-10, 7-15 | 7-10 | mismatch noted | FWA has MU 7-10, regulation has MU 7-15 (boundary issue - neighboring MUs but lake is 21km from border, likely mislabeled MU in regulations); REGION 7A - Ominec |
| name_variants | MIAMI CREEK | {"wscs": ["100-077501-243752"]} | 2-18, 2-19 |  | mismatch noted | FWA has MU 2-18, regulation has MU 2-19 (boundary issue); REGION 2 - Lower Mainland; MU 2-19 |
| name_variants | McArthur Island Slough | blk:355994157 | 3-12, 3-28 | 3-29 | target MU differs | Unnamed slough near McArthur Island in Region 3 MU 3-12.; Slough polygon + tributary stream segments; REGION 3 - Thompson-Nicola; MU 3-28 |
| name_variants | NATION RIVER | {"wscs": ["200-948755-937012"]} | 7-30 |  | mismatch noted | FWA has MUs 7-28, 7-29, regulation has MU 7-30 (boundary issue); REGION 7A - Omineca; MU 7-30 |
| name_variants | NIMPO LAKE | wbk:329601581, gnis:21146 | 5-12, 5-6 | 5-6 | mismatch noted | FWA has MU 5-6, regulation has MU 5-12 (boundary issue); REGION 5 - Cariboo; MU 5-12 |
| name_variants | NOLA LAKE | wbk:328988105, gnis:1816 | 1-10, 1-9 | 1-10 | mismatch noted | FWA has MU 1-10, regulation has MU 1-9 (boundary issue); REGION 1 - Vancouver Island; MU 1-9 |
| name_variants | OWEEGEE LAKE | wbk:329591352, gnis:21851 | 6-16, 6-17 | 6-17 | mismatch noted | FWA has MU 6-17, regulation has MU 6-16 (boundary issue - lake is less than 1.5km from border); REGION 6 - Skeena; MU 6-16 |
| name_variants | PANTHER LAKE | wbk:328966275, gnis:21965 | 1-5, 1-6 | 1-6 | mismatch noted | FWA has MU 1-6, regulation has MU 1-5 (boundary issue); REGION 1 - Vancouver Island; MU 1-5 |
| name_variants | PRIOR LAKE | wbk:329676437, gnis:26972 | 1-2 | 1-1 | target MU differs | Direct GNIS ID match; REGION 1 - Vancouver Island; MU 1-2 |
| name_variants | PROSPECT LAKE | wbk:329676317, gnis:26995 | 1-2 | 1-1 | target MU differs | Direct GNIS ID match; REGION 1 - Vancouver Island; MU 1-2 |
| name_variants | Rancheria River | {"wscs": ["200-692231-770914-177748"]} | 6-24, 6-25 |  | mismatch noted | Tributaries of Little Rancheria River - links to parent waterbody. FWA has MU 6-24, regulation has MU 6-25 (boundary issue).; REGION 6 - Skeena; MU 6-25 |
| name_variants | SQUARE LAKE | wbk:329102648 | 7-12, 7-16 | 7-24 | mismatch noted; target MU differs | FWA has MU 7-12, regulation has MU 7-16 (boundary issue). Crooked River Provincial Park is partly in both 7-24 and 7-16; the lake is in 7-24 which is very close |
| name_variants | STAWAMUS RIVER | {"wscs": ["900-102882"]} | 2-8, 2-9 |  | mismatch noted | FWA has MU 2-8, regulation has MU 2-9 (boundary issue); REGION 2 - Lower Mainland; MU 2-9 |
| name_variants | STELLAKO RIVER | {"wscs": ["100-567134-374775-948201"]} | 7-12, 7-13 |  | mismatch noted | FWA has MU 7-13, regulation has MU 7-12 (boundary issue); REGION 7A - Omineca; MU 7-12 |
| name_variants | SWAN LAKE | wbk:329328382, gnis:20911 | 7-20, 7-33 | 7-33 | mismatch noted | Disambiguate from GNIS 20912; FWA has MU 7-33, regulation has MU 7-20 (boundary issue - lake is about 800m from boundary with MU 7-20); REGION 7B - Peace; MU 7- |
| name_variants | SWAN LAKE | wbk:329459120, gnis:20909 | 8-22, 8-26, 8-6 | 8-22 | mismatch noted | FWA has MU 8-22, regulation has MU 8-26 (boundary issue). Confirmed correct lake via stocked lakes map. Other Swan Lake (GNIS 37740) is in MU 8-6.; REGION 8 - O |
| name_variants | TSITNIZ LAKE | wbk:329677206, gnis:29262 | 7-8, 7-9 | 7-8 | mismatch noted | FWA has MU 7-8, regulation has MU 7-9 (boundary issue - lake is about 3km from border); REGION 7A - Omineca; MU 7-9 |
| name_variants | TUPPER RIVER | {"wscs": ["200-948755-780133-471255"]} | 7-20, 7-33 |  | mismatch noted | FWA has MU 7-33, regulation has MU 7-20 (boundary issue - regulation refers to outlet weir at Swan Lake which is in 7-33); REGION 7B - Peace; MU 7-20 |
| name_variants | Tanya Lake | wbk:329283053, wbk:329283058, gnis:23919 | 5-10 | 5-10 | mismatch noted | Tributaries of Tanya Lakes - links to parent waterbody (GNIS 23919 - Tanya Lakes). FWA has MU 5-10, regulation has MU 5-10.; REGION 5 - Cariboo; MU 5-10 |
| name_variants | WAHLA LAKE | wbk:329613022, gnis:22471 | 6-1, 6-2 | 6-1 | mismatch noted | FWA has MU 6-1, regulation has MU 6-2 (boundary issue - neighboring MUs but lake is 7km from boundary); REGION 6 - Skeena; MU 6-2 |
| name_variants | WHALE LAKE | wbk:329116791 | 5-15, 5-2 | 5-2 | mismatch noted | Whale Lake in Gustafsen Lake area; found via BC Lakes Database (survey_id from 1981: 'A Reconnaissance Survey of Whale Lake 00389DOGC, 1981'). FWA has MU 5-15,  |
