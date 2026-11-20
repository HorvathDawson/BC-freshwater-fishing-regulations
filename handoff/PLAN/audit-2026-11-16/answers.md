# General audit, checker "answers": the app's answers against the printed sources (release r-46e4355a7277)

Saved by the main chat from the checker's hand-back (the checker could not write files). Evidence: the session scratch folder, under audit-general/answers.

## Answers audit: 7 things the app allows that the sources forbid, 6 it forbids that they allow, no wrong numbers or dates

**`report.md` was not written.** The harness refused the file ("Subagents should return findings as text"), so the full report is the second half of this message, ready to save as `…/audit-general/answers/report.md`. Nothing in the repo was changed; `git status` is clean and my temp files are removed.

Release checked: `data/release/r-46e4355a7277`. About 98 waters and about 1,060 (part-group × day) answers were compared (Jul 15, Oct 10, Jan 15, plus extra days on salmon rivers), across Regions 1 to 8 with 7A and 7B. Five Opus sub-checkers read the printed rows first, then the app's own page (`buildRegsPage` + `salmonCard` on the release). I re-checked every (a) and (b) finding against the page text or image and the app's page, and ran four whole-release sweeps.

### Allowed by the app, forbidden by the source
1. **A1. Wild-trout limits are missing from the keep card on at least 15 lakes.** Comox Lake prints "No wild rainbow trout over 50 cm" (p.15); the card says "Up to 4 … any size; up to 1 over 50 cm". Same on Sproat, Great Central, Makii, Cultus, Harrison, Pitt, Chilliwack, Chehalis, Lillooet, Lois, Ruby, Windsor; Dodd Lake's "wild trout/char quota = 2" is also missing. The parse and the stored row are right; the card's tree drops it.
2. **A2. Kitimat River and 23 streams under it, Jul 1 to Oct 31:** "Up to 5 rainbow trout" without "hatchery" (p.51). Wild rainbow are limited to 1 from streams, none under 30 cm. Same display loss as A1.
3. **A3. Peace River below W.A.C. Bennett Dam (part 9) shows open.** The Dinosaur Lake row closes "from W.A.C. Bennett Dam to 100 m south of Gething Creek" (p.65). That part 9 lies inside the closure is inferred from its ends, not measured.
4. **A4. Coal Creek above the old MF&M bridge asks for no Classified Waters Licence.** p.36 makes every Elk tributary Class II except Coal Creek below that bridge. Ruling Li5 is the cause.
5. **A5. A stretch the book closes shows a salmon card that says open.** p.77 says closed fresh waters are closed for salmon too. Seen on the Skeena Ecological Reserve (sockeye 4 a day), the Atnarko's closed stretch (chinook) and the Cowichan below Mile 66 (release only). This is core's card; I did not see it on a device.
6. **A6. DFO "Finfish closure" rows close only the salmon card.** The Stamp hatchery-lagoon stretch on Oct 10 shows open with a trout card. Ten such DFO rows (Stamp, Somass, Sproat, Nitinat, Tahltan).
7. **A7. Williston Lake's "No rainbow trout under 30 cm" (p.61) is missing on Nation Arm and Davis Bay**, which show "any size".

One more with the same effect: **D1. "No angling from powered boats" is never shown**, for all 7 rules of that kind (Atnarko/Bella Coola, Horsefly, Chilcotin, Chilko, Clearwater, Nicomekl, Serpentine).

### Forbidden by the app, allowed by the source
- **B1. Fraser Lake is closed all year** because Ellis Island Ecological Reserve covers 0.1 % of the lake polygon; the book gives it a lake trout quota (p.59). My overlap measurement finds the same for **Kotcho Lake** (0.1 %), **Cold Fish Lake** (0.1 %) and 15 Mile Swamp (3 %). Westwick Lakes (26 %) and Priest Lake (59 %) are partly inside.
- **B2. Dead fin fish as sturgeon bait is not shown** on the Region 2 Fraser and lower Harrison (p.8 and p.23); the page says fish parts are not allowed.
- **B3 to B6, minor:** the Shuswap Lake community-pier exemptions are missing; the Pitt Lake head caveat says "release all" where the book says wild only; four Bella Coola side channels carry the Atnarko's "No powered boats"; salmon is closed on both banks at the Kitimat hatchery signs where DFO closes the west bank.

### Decisions for you
1. **Closed stretch vs salmon card (A5).** Recommend the salmon card reads closed when the page is closed by a full closure, with DFO's row kept as text.
2. **DFO "Finfish closure" (A6).** Recommend it closes the whole page, trout included, on its dates.
3. **Coal Creek above the bridge (A4).** Recommend Class II when open, with an Li5 exception for a row that un-classifies only "the part described".
4. **Williston's 30 cm minimum (A7).** Recommend binding it to all three Zone A parts.
5. **Puntledge above Comox Lake.** p.18 says "fly fishing only upstream of the diversion dam, including tributaries" and exempts the river from the summer closure; the repo stops both at the lake. Recommend carrying both above the lake, or at least a caveat.
6. **Westwick and Priest lakes (B1):** open with a part-closed caveat, or closed? The four barely-touched waters are a plain fix.
7. **Skeena "Section 4"** has no printed upper end; the app classifies the river to its source.
8. **DFO "non-retention" reads Closed** (your T349 ruling; 20 rows). Keep, or show release only? If kept, RULINGS.md should say it covers dated rows.

### Not verified
- **Geometry.** Where cuts sit on the ground was checked by labels and km only; the atlas was not loaded.
- **Device and live feed.** No screen was looked at and the live DFO feed was not compared.
- **Coverage.** About 98 of roughly 19,700 shown waters, three days each. The A1 sweep is a floor: it misses cards that say "hatchery" for another fish, as Dodd's does.
- **Sub-checker items I did not re-run:** E3, E6, E7, F5 to F7, and the page-image reads for A2, A3, A4, A7 (I read those rows in the page text).

Everything is in `/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/64288e7d-44dc-46e9-9576-a31a32c1991a/scratchpad/audit-general/answers/` (6.6 MB): `tool/` (the dump tools), `pdf/` and `dfo/` (sources as text), `png/` (page crops), and `r1r8/`, `r2/`, `r3r5/`, `r4r7b/`, `r6r7a/` (per-water app outputs and source notes).

---

---

# Full report

## How it was done
- **The app's side.** `tool/adump.ts` (bundled with the repo's esbuild) opens the release with `openRegsStore` / `loadRegsInputs`, builds the page with core `buildRegsPage` and the salmon card with core `salmonCard`, and prints it as text. `tool/water.py` runs it for every part of a water on the three days (2026 weekdays). The salmon card is the release's own file, no live feed.
- **The printed side.** The Synopsis PDF (`data/source/fishing_synopsis.pdf`) as page text (`pdf/pNNN.txt`) and, where a row mattered or the text was garbled (PDF p.17 has two text layers), as a page image. Printed page = PDF page − 2 up to p.40, − 6 after. DFO: the cached pages the release was built from (`cache/dfo_salmon/raw/`; Regions 1 and 2 dated 2026-11-06, Region 6 2026-09-02, Region 3 2026-08-14).
- **Answers compared.** Regions 1+8: 233. Region 2: 154. Regions 3+5: 194. Regions 4+7B: 162. Regions 6+7A: 318.

## Waters checked
| Region | Water (item) | Days | Verdict |
|---|---|---|---|
| 1 | Cowichan River (gnis:12227) | Jul 10, Jul 15, Oct 10, Jan 15 | A5, E6, F2 |
| 1 | Nanaimo River (gnis:16505); Somass River (gnis:25707) | 3 | clean (F1) |
| 1 | Stamp River (gnis:14533) | 3 | A6 |
| 1 | Puntledge River (gnis:27060) | 3 | F3 |
| 1 | Cowichan Lake; Farewell Lake; Holt Creek | 3 / 3 / Oct 10 | clean |
| 1 | Comox Lake (wbk:329095500); Sproat Lake (wbk:329016196) | 3 / Jul 15 | A1 |
| 1 | no row: Mill Stream (gnis:3051), Victoria Lake (wbk:328964730) | 3 | clean |
| 2 | Fraser River parts 0-4 (gnis:39325) | Jul 15, Oct 10, Jan 15, Nov 10 | B2, F4 |
| 2 | Chilliwack (gnis:8634), Vedder (gnis:3062), Sumas (gnis:26216) | up to 7 | clean (F1) |
| 2 | Harrison River (gnis:15333) | 4 | B2 |
| 2 | Alouette (gnis:9630); Squamish (gnis:25671) + Turbid Creek | 4-5 | clean (F1) |
| 2 | Cultus, Pitt, Harrison, Dodd lakes | 3 / 3 / Jul 15 / Jul 15 | A1 (Pitt also B4) |
| 2 | Lonzo Creek (gnis:1860); Herrling and Seabird side channels | 4-5 | clean |
| 2 | no row: Camp Slough (gnis:17795), Judson Lake (wbk:328961745) | 3 | clean |
| 3 | Fraser River parts 5-18 | 3 | E3 |
| 3 | Thompson River (gnis:39492), 12 parts | 3 | clean |
| 3 | Shuswap Lake (wbk:329518145) | 3 | B3 |
| 3 | Kamloops Lake; Nicola River; Coldwater River + Juliet and Mine creeks; Hamilton Lake | 3 | clean |
| 3 | no row: Eleven Mile Creek (gnis:8623), Kelly Lake (wbk:328963516) | 3 | clean |
| 5 | Fraser River parts 18-22 | 3 (+Aug 1) | clean |
| 5 | Dean River (gnis:16075), 10 parts | 3 | clean |
| 5 | Bella Coola River (gnis:11611) | 3 | D1, B5 |
| 5 | Atnarko River (gnis:17209) | 3 | D1, A5 |
| 5 | Horsefly River (gnis:24531) | 3 | D1 |
| 5 | Quesnel Lake; Chilko Lake (wbk:329079684); Edmond Creek | 3 | clean |
| 5 | no row: Nazko River, Horsefly Lake | 3 (+May 15) | clean |
| 4 | Kootenay Lake, 3 parts (wbk:-20, -21, -22) | 3 | clean |
| 4 | Elk River (gnis:16880), 8 parts | 3 | clean |
| 4 | Kootenay River (gnis:14097); Columbia Lake (wbk:329093897) | 3 | clean (E5, F7) |
| 4 | Lizard Creek, Michel Creek (Elk tributaries) | 3 | clean |
| 4 | Coal Creek (gnis:17550) | 3 | A4 |
| 4 | no row: Toby Creek (gnis:25934), Lake Eileen (wbk:328966153) | 3 | clean (E5) |
| 7B | Peace River (gnis:14619), 10 parts | 3 | A3 |
| 7B | Dinosaur Lake; Williston Zone B (wbk:-19); Halfway River; Moberly Lake; Charlie Lake | 3 | clean (E5, E7) |
| 7B | Pine River + Le Moray Creek; Burnt River; Murray River | 3 | clean |
| 7B | no row: Sukunka River (gnis:2479), Quality Lake (wbk:328982677) | 3 | clean (E5) |
| 6 | Skeena River (gnis:2936), 22 parts | Jul 15, Aug 15, Oct 10, Jan 15, May 20, Jun 10 | A5, F5 |
| 6 | Zymoetz (gnis:3137); Lakelse River; Lakelse Lake; Babine Lake; Pinkut Creek; Endako River | 3-4 | clean |
| 6 | Kitimat River (gnis:3225) | 3 | A2, B6 |
| 6 | no row: Kitlope River, Nanika Lake | 3 | clean (F1) |
| 7A | Williston Lake: Zone A, Nation Arm, Davis Bay (wbk:-18, -16, -17) | 3 | A7 |
| 7A | Fraser Lake (wbk:329148308) | 3 | B1 |
| 7A | Nechako; Fraser parts 22-23; Stellako; Crooked; Stuart | 3 | clean (E7) |
| 7A | no row: Ingenika River, Takla Lake | 3 | clean |
| 8 | Similkameen (gnis:26324) + Tulameen, Ashnola; Okanagan River; Okanagan Lake; Shuswap River parts 1-11; Skaha, Mabel, Meadow lakes | 3-6 | clean |
| 8 | no row: Vernon Creek (gnis:27995), Naramata Lake (wbk:328985741) | 3-4 | clean |

**Province-wide rules on the 18 no-row waters:** regional daily quotas and sizes, single barbless hook on streams, bait rules, steelhead rules (Regions 1, 2, 3, 5, 6), spring stream closures and winter trout releases all matched. I also read printed p.8 against the pages myself: lines, hooks, weight, bait, spear by region, set lines in Region 6 and Zone 7A lakes only. All as printed, except B2.

## (a) Allowed by the app, forbidden by the source

**A1. Wild-trout limits missing from the keep card (at least 15 lakes).**
- Source: Comox Lake p.15 (page image): "No wild rainbow trout over 50 cm". Sproat Lake p.18: "…no wild rainbow trout over 50 cm". Cultus and Harrison p.23, Pitt p.25: "No wild trout over 50 cm, 1 bull trout over 60 cm". Dodd Lake p.23: "Wild trout/char daily quota = 2 (no wild trout over 40 cm); single barbless hook".
- App, every day, Comox: `kind Brown trout, cutthroat trout and rainbow trout: Up to 4 … combined ; SIZE: any size ; also: Up to 1 over 50 cm (20 in)`. Cultus and Dodd: `Up to 4 brook, brown, cutthroat or rainbow trout combined ; SIZE: any size`. "Wild" is nowhere in the page's keep section.
- Fault: display tree, not parse. `comox_lake.r1` is parsed right (RB, `lengths [{min_cm 50, take 0}]`, `origin: wild`) and is in the release's rule set. The stored row has it (`conds: origin2 … wild … max 50`; `caps: "No wild rainbow trout over 50 cm (20 in) may be kept."`), but `group.buckets`, which the card draws, holds only "any size" and "only 1 over 50 cm". It appears only under "See every regulation line ›".
- Width (my sweep of stored rows with an origin condition whose card tree says neither "wild" nor "hatchery"): Comox, Great Central, Makii (Maggie), Sproat; Chehalis, Chilliwack, Cultus, Harrison, Lillooet, Pitt, Lois, Ruby, Windsor; plus Dodd by hand.
- Verified: Comox on the page image; Cultus, Harrison, Pitt, Dodd on page text; page JSON and store rows by me. No ruling drops this rule.

**A2. Kitimat River and tributaries, Jul 1 to Oct 31: "Up to 5 rainbow trout" without "hatchery" (24 waters).**
- Source: p.51: "Hatchery rainbow trout (adipose clipped, <50 cm) daily quota = 5, no minimum size, all year". Region 6 p.49: "1 trout from streams July 1-Oct 31"; release "Trout under 30 cm from any stream".
- App, Jul 15 and Oct 10: `kind Rainbow trout [RB]: Up to 5 rainbow trout ; SIZE: 50 cm (20 in) or less`. On Jan 15 it reads `Up to 5 hatchery rainbow trout`.
- Fault: same display loss as A1 (stored condition `z6:daily_quotas … wild, 1, min 30`).
- Verified: page text and app by me; the page image by the checker.

**A3. Peace River below W.A.C. Bennett Dam shown open (part 9).**
- Source: Dinosaur Lake row, p.65: "No Fishing from W.A.C. Bennett Dam to 100 m south of Gething Creek and between the anti-vortex dyke and Peace Canyon Dam✱".
- App, part 9 ("From Dinosaur Lake up to the Williston Lake — Zone B outlet"), all days: `STATUS open`, Zone B keep cards, no caveat. The Dinosaur Lake page carries the closure as a part-closed caveat.
- Fault: rule binding. `dinosaur_lake.r1` (region-7.json) is held on the lake only, as an undrawn part.
- Verified: row text and both pages by me; image by the checker; geometry inferred.

**A4. Coal Creek above the old MF&M Railway bridge: no Classified Waters Licence.**
- Source: p.36: "ALL tributaries (EXCEPT Coal Creek downstream of old MF&M Railway Bridge 7 km upstream of Elk River) are Class II waters when open". Coal Creek row, p.35: "Part described is NOT a Classified Water".
- App, part 1 (bridge to source): `LIC needs: Basic angling licence`, no classified box. Lizard Creek shows the Class II box.
- Fault: ruling Li5 applied to a row that un-classifies only its own part.
- Verified: page text and app by me; images by the checker.

**A5. Closed stretch, open salmon card.**
- Source: p.77 (page image): "Please note that when fresh waters are closed to fishing, or have gear restrictions outlined in this Synopsis, the regulations apply to fishing for salmon as well as for non-salmon species." p.9: "Fishing is prohibited in Ecological Reserves in B.C."
- App:
  - Skeena parts 2 and 4 (ecological reserve), Aug 15: `Closed all year` beside `SALMON headline: Open today`.
  - Atnarko parts 2-4, Jul 15: `Closed all year` beside `SALMON headline: Open today`.
  - Cowichan part 1, Oct 10: `STATUS closed` beside `SALMON headline: Release only today` (DFO row: "66 Mile Trestle … | All | Sep 1 to July 14 | Open for salmon catch and release").
- Fault: by design. The page's gear rules reach the salmon card (`gearProvincial`); its closures do not. No ruling covers it.
- Verified: p.77 image and the three pages by me. Model only, no screen.

**A6. DFO "Finfish closure" closes only the salmon card.**
- Source (DFO Region 1): "Stamp River | From the inlet upstream of Robertson Creek Hatchery Lagoon downstream to the outlet … | All | Aug 25 to Oct 31 | Finfish closure"; "… powerline crossing downstream approximately 500m … | All | Oct 1 to Oct 14 | Finfish closure". Ten rows in all: Nitinat 3, Somass, Sproat, Stamp 4, Tahltan.
- App, Stamp parts 5 and 6, Oct 10: `STATUS open` with the trout card; the salmon card says `Finfish closure (Aug 25–Oct 31)`.
- Fault: policy. `typed.py` reads the row as a salmon closure.
- Verified: DFO rows and app lines by me.

**A7. Williston Lake: "No rainbow trout under 30 cm" missing on Nation Arm and Davis Bay.**
- Source: "WILLISTON LAKE (in Zone A) 7-30, 7-37, 7-38 … No rainbow trout under 30 cm … See Nation Arm", p.61. The extracted text misplaces the line under Wicheeda; the checker confirmed it on the image. Nation Arm "(Williston Lake)" and Davis Bay "(in Finlay Reach of Williston Lake)" are parts of that lake.
- App: Zone A piece `must be 30 cm (12 in) or longer`; Nation Arm and Davis Bay `SIZE: any size`.
- Fault: rule binding. `williston_lake.r1` matches `wbk:-18` only.

## (b) Forbidden by the app, allowed by the source

**B1. Fraser Lake closed all year (also Kotcho Lake, Cold Fish Lake).**
- Source: "FRASER LAKE 7-13 Lake trout daily quota = 1 (none under 60 cm)", p.59.
- App: `Closed all year`, chain `<zp:superior_closures::superior_closures.r3>` (ecological reserves).
- Measured (lake polygon ∩ reserve polygons, `bc_fisheries_data.gpkg`, read-only):

| Water | Area | Inside a reserve |
|---|---|---|
| Fraser Lake | 5,435 ha | 0.1 % (Ellis Island ER, 6.1 ha) |
| Kotcho Lake | 8,167 ha | 0.1 % |
| Cold Fish Lake | 827 ha | 0.1 % |
| 15 Mile Swamp | 2,343 ha | 3 % |
| Westwick Lakes | 50 ha | 26 % |
| Priest Lake | 2.6 ha | 59 % |

- The other 16 named lakes this rule closes are 97 to 100 % inside. All 77 named lakes closed by the national-park rule are at least 90 % inside a park.
- Fault: rule binding. A `within(area)` holds every lake the polygon touches; AGENTS rule 13 says to take such a lake out with `outside_items`.

**B2. Dead fin fish as sturgeon bait not shown.**
- Source: p.8: "You may use the head of fin fish or the headless body of fin fish as bait, only: (a) when sport fishing for sturgeon in Region 2 only on the Fraser River, Lower Pitt River (CPR Bridge upstream to Pitt Lake), Lower Harrison River (Fraser River upstream to Harrison Lake)". Fraser and Harrison rows, p.23: "Dead fin fish may be used for bait only when fishing for sturgeon".
- App, Fraser parts 1-4 and Harrison parts 0-1: `GEAR bait: … Not allowed: Fish or fish parts`, no note.
- Fault: display. Parsed (`zp:bait::bait.r3` and the rows); rulings W2 and G1 require it. The lower Pitt was not run.

**B3. Shuswap Lake pier exemptions not shown.**
- Source, p.32: "(anglers fishing from the community pier in the city of Salmon Arm are exempt from the closure)" and "…exempt from the bait ban".
- App: the Salmon Arm Bay closure caveat and `No natural bait — all year`; "pier" is not in the page.
- Fault: display. `shuswap_lake.r5x` is an undrawn part "held as a note" that is never drawn.

**B4. Pitt Lake head.** Source p.25: "Wild trout/char catch and release, single barbless hook north of fishing boundary signs". App: `Trout and char — release all @ North of fishing boundary signs`. `pitt_lake.r3` keeps `origin: wild`; the caveat's words drop it.

**B5. Bella Coola side channels.** Source p.43: "No powered boats on Atnarko River, from Goat Creek to the confluence with Talchako River". App, Bella Coola part 0 (side channels joining 55 to 59.1 km; the Talchako joins at km 62.6): `BOATS: No powered boats`. Rule binding; geometry not checked.

**B6. Kitimat hatchery signs.** DFO: "on west bank between signs at Kitimat hatchery outfall. | All | Apr 1 to Mar 31 | No fishing for salmon". App, part 1: salmon closed for every species on the whole part. The book's own west-half rule is shown as a one-side caveat.

## (c) Number, size or date differs
None found.

## (d) Rule missing or on the wrong part

**D1. "No angling from powered boats" never shown (7 rules).**
- Source: p.43: "No angling from powered boats on mainstems of Atnarko River and Bella Coola River". p.45: "No angling from powered boats from Woodjam Bridge to Quesnel Lake". Also printed for the Chilcotin, the Chilko above Henry's Crossing (Sept 11-30), the Clearwater below Falls Creek, and the Nicomekl and Serpentine above the dyke gates.
- App: "powered" does not occur on Bella Coola part 1, Atnarko parts 5-6, Horsefly part 0, Chilcotin part 0, Clearwater part 1 or Nicomekl part 3. The Thompson's `BOATS: No angling (from a boat)` does show.
- Fault: display. The rules are parsed (`method:angling@angler=in_powered_boat`) and the store words them (`display_rule` "No angling (from a powered boat) — downstream of Woodjam Bridge"); the page draws `in_boat`, not `in_powered_boat`.

A3, A7 and B5 are also wrong-part findings.

## (e) Wording that would mislead
- **E3. Fraser part 5 (Spuzzum Creek to Alexandra Bridge).** The salmon card shows the Region 2 page's details (Nov 6, 2026; coho 25 cm; adult chinook over 62 cm) under "from: Region 3 · All Region 3 waters". Every species is correctly closed. Checker's finding, not re-run.
- **E5. Other-fish rows with a size sub-limit show no "size limit" hint.** Region 4 kokanee (p.34: "no more than 5 over 30 cm") reads `15 a day`; Zone B pike "3 (only 1 over 90 cm)" and walleye "3 (only 1 over 70 cm)" (p.64) read `3 a day`. The tap-through sheet does say "Up to 5 over 30 cm" and so on; I checked. One tap away, not missing.
- **E6. Cowichan boats line.** Book p.15: "Speed restriction (8 km/h) and engine power restriction - 7.5 kW (10 hp) on parts". App: `Engine limit 7.5 kW (10 hp) on parts; Speed limit 8 km/h`.
- **E7. Part labels.** Halfway River parts 0-1 carry the Peace row's name; Stellako parts 2-4 read "Where Region 6 and Region 7 meet" though they lie in MU 6-4. Rules shown are right. Not re-checked.

## (f) Source unclear, or an owner ruling stricter than the words
- **F1. DFO "Non-retention" / "No retention of coho" shown as "Closed"** (20 rows: Chilliwack/Vedder coho Jul 1-Aug 31, Alouette coho Sep 1-30, Area 6 streams coho Jan 1-Oct 31, Sproat). Owner ruling T349 (`deliver/salmon.py`); RULINGS D5 names only the "until further notice" case.
- **F2. Cowichan below Mile 66.** The book closes it Sep 1-Nov 15; DFO opens salmon catch and release Sep 1-Jul 14 there.
- **F3. Puntledge above Comox Lake.** p.18: "Fly fishing only upstream of the BC Hydro diversion dam (approximately 3.5 km downstream of Comox Lake), including tributaries" and "Exempt from July 15-August 31 summer closure". The repo ends both at the lake outlet (`puntledge_river.r1`, `r6`), so parts 8-9 are closed Jul 15-Aug 31 and not fly only.
- **F4. DFO Fraser rows (2026-11-06).** "upstream of the CPR Bridge at Mission | All | Apr 1 until further notice | No fishing for salmon", then coho and chum openings on "downstream of the CPR Bridge at Mission to the Highway 1 Bridge at Hope". The release binds them Mission to Hope (owner's note of 2026-11-03): on Nov 10 parts 1-3 show coho and chum open.
- **F5. Skeena "Section 4"** (p.53: "1.5 km upstream of Zymoetz River … July 1-Dec 31") has no printed upper end.
- **F6. Region 6 "1 trout from streams" and the winter stream release** are read as excluding char (rulings C2/C4), so brook and lake trout show as keepable from streams in winter.
- **F7. Smaller:** Kootenay River above Koocanusa, "no trout under 30 cm" on brown, cutthroat and rainbow only (C2(b)); Kootenay part 6 may lie under the reservoir; Kitimat hatchery steelhead released Nov 1-Jun 30 (Q6); Region 5 Fraser sturgeon "closed … (sturgeon catch and release)"; Region 3 p.28 third column has no heading.

## Not findings
- Tidal parts showing a closed salmon card in my tool: the app asks no salmon on a tidal page (ruling P52).
- White sturgeon closed outside the licensed Fraser watershed: ruling W1.
- A water DFO does not list reads closed for salmon: owner ruling.

## What I could not check
- Geometry on the ground (Hell's Gate and Boston Bar signs, Bridge River Rapids ladder, the Landstrom Bar zone drawn as 140 m, Peace part 9, Kootenay part 6, Bella Coola side channels).
- The device and the live DFO feed.
- Days other than the three per part, beyond spot checks.
- Not run: Bulkley, Babine River, Lower Pitt River, the Squamish exception rivers, Liard watershed waters, non-resident licence lines. Chilliwack and Chehalis lakes are in the A1 sweep by store row only.
