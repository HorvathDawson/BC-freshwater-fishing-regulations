# Proposed name_variants additions — REVIEW BEFORE APPLYING

Two sources. Nothing applied yet. Approve sections and I'll merge with dedupe.


## A. name_variants_more.csv (source: Gazetteer / SOR-2008-120)

Matched by **point-in-polygon** (lakes+manmade). 8 additions after dropping names equal to the polygon's GNIS name, 'Unnamed lake' placeholders, and descriptions.


| wbk | polygon GNIS name(s) | ADD (source=gazetteer) |
|---|---|---|
| 328974967 | ['Nancy Greene Lake'] | Sheep Lake |
| 329524047 | ['Norbury Lakes', 'Norbury Lake'] | Peckhams Lake and Garbutts Lake |
| 329102648 | — | Squaw Lake |
| 329069402 | — | Hidden Lake |
| 329069452 | — | Stink Lake |
| 329069342 | — | Fisher Lake |
| 329069403 | — | Muskrat Lake |
| 329101355 | — | Provost Dam |

**Skipped:** 12 rows are **rivers** (no lake polygon contains the point — need stream matching or skip): items 57, 62, 63, 64, 65, 68, 74, 76, 79, 81, 87, 88. 1 **bad coord** (item 4 Jim Smith Lake: lat==lon typo `49°28′ 49°28′`).


## B. ettt.csv (lake surveys) — 31 CLEAN + 9 SUSPECT

GAZETTED_NAME kept where it differs (normalized) from GNIS_NAME_1/2/3. Match by WATERBODY_KEY.


### B1. CLEAN — recommend adding (source=lake_survey)

Mostly ASCII-for-accented (Brûlé→Brule) and English names for renamed lakes (Chilko→Tŝilhqox Biny).


| wbk | GNIS name(s) | ADD gazetted |
|---|---|---|
| 329424460 | Onjo Lake | WITCH LAKE |
| 329223897 | Niska Lakes | NISGA'A LAKES |
| 329193768 | Désirée Lake | DESIREE LAKE |
| 329016197 | Hucuktlis Lake | HENDERSON LAKE |
| 329016216 | Little Toquaht Lake | LITTLE TOQUART LAKE |
| 329016202 | Makii Lake | MAGGIE LAKE |
| 329676326 | Magic Lake Estates Water Reservoir No. 1 | MAGIC LAKE ESTATES RESERVIOR 1 |
| 329016206 | Toquaht Lake | TOQUART LAKE |
| 329595984 | Bendziny, Puntzi Lake(formerly) | PUNTZI LAKE |
| 329071811 | Lho Lakes, Sucker Lake | DACE LAKE |
| 329182794 | Telhiqox Biny, Tatlayoko Lake(formerly) | TATLAYOKO LAKE |
| 329320864 | East Barrière Lake | EAST BARRIERE LAKE |
| 329552301 | East Hautête Lake | EAST HAUTETE LAKE |
| 329276268 | Lhuy Nachasgwen Gunlin, Eagle Lake (formerly) | EAGLE LAKE |
| 329079709 | Rossé Lake | ROSSE LAKE |
| 329079684 | Tŝilhqox Biny, Chilko Lake (formerly) | CHILKO LAKE |
| 329353175 | Nendatoo Lake | CRIPPLE LAKE |
| 329343274 | Brûlé Lake | BRULE LAKE |
| 328974352 | OgLake | OG LAKE |
| 329148307 | François Lake | FRANCOIS LAKE |
| 329021788 | Crazy Bear (Ginny) Lake | CRAZY BEAR LAKE |
| 329657984 | Cameron Lakes | NORTH CAMERON LAKE |
| 329276269 | Tegunlin, Stum Lake(formerly) | STUM LAKE |
| 329276278 | Benchuny, Anah Lake (formerly) | ANAH LAKE |
| 329276293 | Tigulhdzin, Alexis Lake(formerly) | ALEXIS LAKE |
| 329552303 | Hautête Lake | HAUTETE LAKE |
| 329079706 | Little Lagoon | LITTLE LAGOON LAKE |
| 329320866 | North Barrière Lake | NORTH BARRIERE LAKE |
| 329320882 | South Barrière Lake | SOUTH BARRIERE LAKE |
| 329320950 | Upper South Barrière Lake | UPPER SOUTH BARRIERE LAKE |
| 329595989 | Cheẑich'ed Biny, Chilcotin Lake(formerly) | CHILCOTIN LAKE |

### B2. SUSPECT — GAZ equals a DIFFERENT lake's own name (review each)


| wbk (this lake) | this GNIS name | GAZ (suspect) | collides w/ wbk | verdict? |
|---|---|---|---|---|
| 329494828 | Yellow Lake | MCGUCKIN LAKE | 329494947 | ? |
| 329532487 | McCombe Lake | BUNTZEN LAKE | 329532437 | ? |
| 329018549 | Koosawu Áa | SURPRISE LAKE | 329086738, 329292818, 329500350, 329590100, 329650857 | ? |
| 329518151 | Rosemond Lake | MARA LAKE | 329518146 | ? |
| 329682391 | Lower Burnie Lake | BURNIE LAKES | 329682392 | ? |
| 329520358 | Cathedral Lakes, Woods, Lake of the | LAKE OF THE WOODS | 329170829, 329451962 | ? |
| 329178029 | Wolf Lake | WOLFE LAKE | 329457562, 329520006 | ? |
| 328975711 | One Lake | TWO LAKE | 329276383 | ? |
| 328961766 | Intata Reach, Ootsa Lake, Nechako Reservoir | NATALKUZ LAKE | 328961715 | ? |
