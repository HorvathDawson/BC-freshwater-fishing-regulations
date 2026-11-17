# Skeena cascade review (2026-11-15 night, by a Fable reviewer; read-only)

Part 1 was written before the rebuild (on the side build); Part 2 checks the BUILT release r-46e4355a7277.
The owner's questions with recommended answers are in Part 2 section 4; tickets T770, T771, T781.

---

# PART 2 (the built result)

# Skeena cascade of rules: independent review, PART 2 (the built result)

Read-only. Nothing in the repo was changed. Part 1 is `part1.md` in the same folder.

Tags: **[ran]** a script I ran on the built files, **[read]** read in data or code, **[inferred]** my reading.

What was checked
- Release `data/release/r-46e4355a7277`: the 11 `salmon*.bin.gz` files, decoded with the repo's own `deliver.salmon.decode_files` (348 chains, 1,949,507 sections) [ran].
- Bundle `data/generated/bundle/bundle.sqlite` (built 00:38): its section table is the release's, same handle digest `55032563b98ee973`, and its `salmon_section` equals the release's shards row for row [ran]. Rule-book answers come from the reference reader `effective_rules` on this bundle.
- Reach tables `data/generated/reaches/full` (DFO placements, `rule_section`), atlas sidecars in `data/generated/atlas/full` (`registry.json`, `splits.resolved.json`, `section_handles.txt`). `graph.pkl` and `geometries.pkl` were not loaded.
- A cross-check: every Skeena chain's status, recomputed from the typed DFO rows with `verdict_on`, equals the release on 10 sample days for all five fish: 0 mismatches over 73 chains [ran `p2_pairs.py`].

Scripts and outputs (same folder): `p2.py`, `p2_main.py` / `p2_main.txt` (mainstem), `p2_tribs.py` / `p2_tribs_a.txt`, `p2_tribs_b.txt` (tributaries), `p2_checks.py`, `p2_rulebook.py` / `p2_rulebook.txt`, `p2_pairs.py` / `p2_pairs.txt`, `p2_scan_rule_section.py`.

---

## 1. The 13 Part 1 checks

| # | Check | Result | Evidence |
|---|---|---|---|
| 1 | Sockeye pair order, mainstem between the CNR bridge and Cedarvale | **PASS** | Section `360887278:132164`, chain 218. Jul 15: sockeye open, "2 per day (Jul 10–28)", from "mainstem waters only, upstream of CNR bridge … to a point above the Babine". Jul 1: closed, "No fishing for sockeye (Apr 1–Mar 31)", from the rest-of place. Order is by structure, see 2(a). [ran] |
| 2 | Pink place after the fix | **PASS** | `6:skeena-river:mainstem-waters-only` binds 94 sections, all above the bridge; it is in no chain below the bridge. Lower mainstem pink: open, "2 per day", all year, from "mainstem waters downstream of CNR". Above the bridge: closed Jan 1 to Jun 15 (B(i)), open Jun 16 to Dec 31. [ran] |
| 3 | Every stretch and tributary of Part 1 section A against the release | **PASS on the mainstem; tributaries pass with 3 known exceptions** | Mainstem: 14 stretches, every fish and date as Part 1 A1 (`p2_main.txt`). Tributaries: all 21 waters as A2 (`p2_tribs_*.txt`) except Gitnadoix below the powerline (coho open, DFO closed), the 100 m of the Morice between Gosnell Creek and the signs (coho Sep 1-30 closed, DFO open), and the Babine Lake closed circles (see 2(c)). [ran] |
| 4 | Kasiks and Babine Lake: bound or still notes | **PASS (Kasiks); CHANGED (Babine Lake)** | See 2(c), 2(d). |
| 5 | Morice top 2 km after the fix | **PASS** | "Outlet signs to Gosnell" now binds 72 sections, exactly the province's two closures together (11 + 61) [ran]. `360885316:89754`, Jul 1, coho: closed, "No fishing for salmon (Jan 1–Sep 30)", from that place; Oct 10: closed, "No fishing for coho", from B(i). |
| 6 | The 15 Skeena-basin sections in Region 7A | **PASS** | 15 sections on chains 264, 265, 298. No Region 6 "A" place; Region 7's page sentence is last. All five fish closed all year; pink after Jun 15 reads "There is no fishing for salmon in Region 7 until further notice." One-region sections, so no two-region grouping. [ran] |
| 7 | The 27 Skeena side channels with the "above the gauge" chain | **PASS** | Walked down `section_down` to the main line: the lowest joins at m 359,862, above the gauge (337,743). No side channel entering between the bridge and the gauge lacks the sockeye place, and none outside has it. [ran] |
| 8 | Tidal section `360887278:0` | **PASS** | Chain 0 ("Not opened by DFO: closed to salmon fishing") and a row in the bundle's `tidal` table (`split:skeena_river__tidal_boundary`). No basin section other than tidal water is on chain 0. [ran] |
| 9 | Gear lines | **FAIL (as predicted, not a salmon answer)** | "No fishing for eulachon" is missing on the Lakelse all year and on the Kitsumkalum Jan 1 to Jun 15 (a bait line takes its place). Kitsumkalum Lake itself (`lake:329241962`) has no "No natural bait" line Jan 1 to Jun 15; its inflow streams do. [ran] |
| 10 | Rule book with the reference reader | **PASS, one finding confirmed** | Steelhead: mainstem below Cedarvale May 20: no closure speaks (exempt). Mainstem above Cedarvale May 20 and May 31: closed by the winter closure; Jun 1: open. Kispiox May 20: both closures speak; Jun 16 open. Exstew May 20: steelhead closure. Fulton Feb 10 open, May 10 closed. Rainbow Alley Feb 10: no closure speaks. Classified on Aug 1: Skeena River 2 at m 108,507 and in the Kitsumkalum zone; none at m 132,164 (the gap); Section 4 from m 157,258 up; Zymoetz 47 sections unit A, 59 unit B, none in both. **Gosnell Creek, Thautil River and Lamprey Creek on Jul 1: closed by "No Fishing from Gosnell Creek to Lamprey Creek, Jan 1-Aug 31"** (the Morice row). See 2(b). [ran] |
| 11 | Base plus named rows on sample sections | **PASS** | Seven mainstem sections carry the Region 6 base (12 entries below Cedarvale, 13 above: the winter closure) plus only SKEENA RIVER; the Kispiox zone adds the confluence row and the mainstem-only row. Kispiox: its row + SKEENA RIVER. Zymoetz: its row + SKEENA RIVER. Gosnell Creek: Bulkley, Gosnell, Morice, Skeena, Thautil (the Morice one is the open question). Rule-book bindings in the basin are identical to the side run I read in Part 1: 0 of 177 rules differ. [ran] |
| 12 | The Lakelse channel cut by the Lakelse-mouth cut | **PASS** | `360866092:0` (0 to 168 m) and `360866092:168`: both carry the Lakelse row, the Lakelse licence and the Lakelse DFO places; neither carries a Skeena mainstem rule. [ran] |
| 13 | Stream sections the SKEENA RIVER bait row misses | **PASS (attribution only)** | 25 sections have the base bait ban and not the row's (23 unnamed side pieces, 1 on MacMillan Creek, 1 on the Shelagyote); 8 have the row's and not the base's (the Region 7A streams). Every one has a bait ban. [ran] |

Score: 11 pass, 1 pass with exceptions (3), 1 fail (9, gear line only).

---

## 2. The specific confirmations

### (a) Skeena sockeye, Terrace to above the Babine [ran]

The rest-of place `6:skeena-river:upstream-of-cnr-bridge-at-terrace~2` now has extent `rest`, bounds CNR bridge to gauge 08EB005, and three listed places: the mainstem openings, the Kispiox sign zone, the Kitwanga mouth stretch. Its reach row binds 0 sections of its own and carries `behind` (54, 2 and 3 sections).

| Where (section) | Jul 1 | Jul 10 | Jul 20 | Aug 27 | Sep 15 |
|---|---|---|---|---|---|
| Mainstem, bridge to Cedarvale (`…:132164`) | closed, rest-of | open 2/day, listed | open 2/day, listed | open 2/day, listed | closed "FN0919 (Aug 28–Mar 31)", listed |
| Mainstem above Cedarvale (`…:208304`) | closed, rest-of | open 2/day, listed | open 2/day, listed | open 2/day, listed | closed, listed |
| Babine mouth to the gauge (`…:335399`) | closed, rest-of | open 2/day, listed | open 2/day, listed | open 2/day, listed | closed, listed |
| Kitwanga mouth stretch (`…:227042`) | closed all five days, "No fishing for salmon (Apr 1–Mar 31)", from the Kitwanga place | | | | |
| Kispiox sign zone (`…:284309`) | closed all five days, from the Kispiox zone place | | | | |
| Above the gauge (`…:337742`) | closed all five days, "No fishing for sockeye", from B(i) | | | | |

"listed" = `mainstem-waters-only-upstream-of-c~2`. It answers on the days it has a sockeye row (Jul 10 to Mar 31); the rest-of row answers Apr 1 to Jul 9.

By structure, not by name: with 0 sections of its own the rest-of place would sort last in its tier; in every chain it sits directly after the listed mainstem place (and its dormant twin after it), which is what `rest_after` does. In the two zones the zone place is first (2 and 3 sections), then the mainstem place, then the rest-of. The rest-of place is in no `u` list and not in `off`.

### (b) Rule book: Morice "No Fishing from Gosnell Creek to Lamprey Creek, Jan 1-Aug 31" [ran]

- Binds 1,958 sections (and so does "Fly fishing only … Sept 1-Sept 30"). Unchanged by the rebuild.
- On the Morice main line: m 54,533 (Lamprey Creek) to m 75,169 (Gosnell Creek), three sections.
- It takes **all 20 Gosnell Creek sections, all 6 Thautil River sections and all 5 Lamprey Creek sections**, with their side streams: 1,827 sections beyond the stretch. Gosnell and the Thautil have their own rows ("No Fishing Aug 1-Sept 30; bait ban"); ruling T9 says a joining water with a row of its own is left out. Lamprey Creek has no row, so T9 puts it in.
- Effect, from the reference reader: Gosnell Creek, rainbow trout, Jun 20 and Jul 1: closed by the Morice row. Aug 15: closed by its own row (and the Morice row). Oct 10: open, trout quota. Without the Morice row it would be open Jun 16 to Jul 31.
- Against the DFO Morice places: DFO's "Gosnell Creek to Lamprey Creek" binds 131 sections, all inside the province's 1,958; none of Gosnell, Thautil or Lamprey Creek. DFO's stretch starts at the signs (m 75,069), the province's at the creek (m 75,169). DFO's top place equals the province's two top closures exactly (72 sections). No salmon answer depends on the difference: those creeks are closed to salmon all year either way.

### (c) Babine Lake [ran; app behaviour read in `app/packages/core/src/salmon.ts`]

- The sockeye place `…excluding-tributaries~3` binds one section, the lake (`lake:329026676`), no tributary. Lake chain 299: the place, then "Babine Lake including tributaries", B(i), A. Aug 10: sockeye open, "2 per day (Aug 1–27)". Jul 20: closed, "No fishing for sockeye (Apr 1–Jul 31)". Other fish closed all year.
- The 12 circles and the Gullwing line: 13 pieces, each `closure_carried`, "not confirmed (no verified closed_area mark): carried on its water", 0 sections. Each ships as an undrawn place on the lake with its own year: closed every day, "No fishing for salmon — within a 400 m radius of the mouth of Twain Creek" (and so on), in force every day.
- What the app shows: the map paints the whole lake by the drawn answer, so the whole lake reads open for sockeye Aug 1 to 27, including inside the circles and east of the Gullwing line. The card's sockeye row says open. The 13 closed areas appear in the card's "part of this water" block, one entry each, in DFO's words, marked in force today. The card's headline changes for an undrawn part only when the page reads closed and a part is open; there is no "open, closed in part" headline. [read, not run in the app]
- This is the reverse of Part 1's state (lake closed, opening as a note). It now reads more open than DFO inside the 13 areas for 27 days a year.
- A circle that covers only part of the lake section can never bind (it would close the lake), so confirming the 13 pieces will not change the map; only cutting the lake would.

### (d) Kasiks [ran]

Cuts built: Hwy 16 bridge m 194, signs below the Upper Pool m 17,653.

| Place | Sections | Answer |
|---|---|---|
| upstream of the signs below the Upper Pool | 233 | coho closed all year, "No fishing for coho" |
| downstream of those signs | 193 | **coho open Sep 1 to Oct 31, "4 per day, only 1 over 50 cm"**; rest of the year closed by B(ii) |
| upstream of the Hwy 16 bridge | 425 | chinook closed all year |
| whole (dormant) | 426 | no rules |

Three chains: above the signs (233), below the signs and above the bridge (192), the mouth below the bridge (1: coho open in season, chinook closed by B(ii)). Sockeye, pink closed by B(ii); chum by A. Matches DFO.

### (e) Kitimat west bank [ran]

`6:kitimat-river:on-west-bank-between-signs-at-kiti` binds one section, `360885824:4295` (m 4,296 to m 5,046, about 750 m), the full width. An angler on the east bank there sees every salmon closed all year, "No fishing for salmon (Apr 1–Mar 31)", attributed to the west-bank place. The section just below (`360885824:0`) on Jul 1 reads chinook open "1 per day, 80 cm or less", coho open "4 per day, only 2 over 50 cm", pink and chum open "2 per day", from "downstream of Highway #37 bridge". So the east bank is shown closed where DFO opens it. It errs closed.

### (f) Same-tier pairs in one chain that disagree [ran `p2_pairs.py`]

73 chains serve the Skeena basin. 14 pairs of same-tier places share a chain and give a different answer for some fish on some day. None is decided by the id.

| Wins | Over | Sections | Fish and days they differ | Why it wins |
|---|---|---|---|---|
| Kitsumkalum mouth zone | lower mainstem | 4 | coho Jul 15 to Aug 31, sockeye Jul 10 to Aug 27, pink Jul 1 to Aug 31 (zone closed); sockeye Sep 1-15 (zone open) | fewer sections (4 < 128) |
| Kitwanga mouth stretch | mainstem sockeye openings | 3 | sockeye Jul 10 to Aug 27 | fewer (3 < 54) |
| Kitwanga mouth stretch | mainstem pink | 3 | pink Jun 16 to Dec 31 | fewer (3 < 94) |
| Kitwanga mouth stretch | mainstem coho | 3 | coho Jul 15 to Oct 15 | fewer (3 < 94) |
| Kispiox sign zone | mainstem sockeye openings | 2 | sockeye Jul 10 to Aug 27 | fewer (2 < 54) |
| Kispiox sign zone | mainstem pink | 2 | pink Jun 16 to Dec 31 | fewer (2 < 94) |
| Kispiox sign zone | mainstem coho | 2 | coho Jul 15 to Oct 15 | fewer (2 < 94) |
| The Forks (Bulkley wording) | Bulkley below the Morice | 1 | coho Jul 15 to Aug 15, pink Jun 16 to Aug 15 | fewer (3 < 36) |
| The Forks (Bulkley wording) | Morice below Lamprey | 1 | coho Jul 15 to Aug 15 | fewer (3 < 887) |
| The Forks (Bulkley wording) | Morice, confluence to Bymac | 1 | pink Jun 16 to Aug 15 | fewer (3 < 20) |
| The Forks (Morice wording) | the same three | 1 each | the same | fewer |
| **Mainstem sockeye openings** | **rest-of ("excluding the areas below")** | 54 | sockeye Jul 10 to Aug 27 (open over closed) | **rest-of stands behind its listed place** |

In every size-decided pair the closed place is the smaller one and wins, which is DFO's meaning. A T771 build check for "equal tier, equal size, different answer" would be green on the Skeena today. Equal-size pairs still ordered by id exist (the two Forks wordings; "mainstem waters only" pink and "upstream of the CNR bridge" coho, both 94 now; dormant twins) but none disagrees on any fish or day.

---

## 3. What is wrong, or worth knowing, in what the app would show

1. **Babine Lake, Aug 1 to 27:** the whole lake reads open for sockeye, including within 400 m of the 12 creek mouths and east of the Gullwing line. The closures are on the card as 13 part entries; the map and the sockeye row do not show them.
2. **Gosnell Creek and Thautil River, Jun 16 to Jul 31:** shown closed to all fishing by the Morice row. Likely more closed than the book (ruling T9).
3. **Kitimat east bank, m 4,296 to 5,046:** shown closed to salmon all year; DFO closes only the west bank.
4. **Gitnadoix below the powerline:** coho shown open Sep 1 to Oct 31; DFO opens only above the powerline. No powerline cut exists.
5. **Eulachon line** missing on the Lakelse and, to Jun 15, the Kitsumkalum. **Kitsumkalum Lake** has no DFO bait line Jan 1 to Jun 15 (the province's bait ban still holds there).
6. **Morice, 100 m between Gosnell Creek and the signs:** coho Sep 1-30 closed; DFO's "Gosnell Creek to Lamprey Creek" row would open it. The owner kept the signs point.

Everything else on the Skeena mainstem and the 21 tributaries reads as DFO's page says.

---

## 4. The nine Part 1 owner questions

| # | Question | Status |
|---|---|---|
| 1 | Skeena above Terrace, sockeye: the two rows | **Answered and built.** Owner 2026-11-15 22:12 "this iss handled by rest of option in extents". The build orders them by structure (2(a)). The build check for leftover ties is T771. |
| 2 | Kasiks coho | **Answered by the build.** Lower river open Sep 1 to Oct 31 (2(d)). The signs point is a medium-confidence one. |
| 3 | Babine Lake sockeye | **Changed.** Part 1 recommended keeping the lake closed until the circles were confirmed. The lake is now open with the circles as notes. New question N1. |
| 4 | Gitnadoix coho near the mouth | **Still open.** Recommended: add the powerline cut if the crossing is more than a few hundred metres above the mouth; otherwise leave. |
| 5 | Gosnell Creek and Thautil River inside the Morice closure | **Still open, confirmed on the built bundle.** Recommended: leave both creeks out of the Morice "Gosnell Creek to Lamprey Creek" rules, as ruling T9 says. |
| 6 | Tributaries inside a salmon stretch (Kispiox, Morice, Lakelse) | **Still open.** Recommended: keep. |
| 7 | Skeena near the Kitsumkalum mouth, sockeye Sep 1 to 15 | **Still open.** Built as printed (open, 2/day, FN0679). Recommended: keep as printed. |
| 8 | Eulachon line | **Still open, confirmed.** Recommended: always show it below Terrace. |
| 9 | "A point above the Babine" at the gauge | **Still open.** The rest-of place uses the same gauge as its upper bound. Recommended: keep. |

### New questions

- **N1. Babine Lake in August.** Sockeye is open on the lake Aug 1 to 27, but not within 400 m of 12 creek mouths and not east of the Gullwing Creek line. We now show the whole lake open and list those 13 closed areas only as notes on the card. Recommended: keep the lake open, and add a headline on the card for those days, "open, closed in part", the mirror of the "closed, open in part" headline that already exists.
- **N2. Kitimat River at the hatchery outfall, east bank.** DFO closes only the west bank between the signs; we close both banks for about 750 m until the one-bank work is built. Recommended: leave it closed until then; it errs on the safe side.
- **N3. Lamprey Creek.** With question 5 answered as recommended, Lamprey Creek (no row of its own) stays inside the Morice closure and is closed Jan 1 to Aug 31. Recommended: yes, that is what the ruling says for a joining creek with no line of its own.

## 5. Notes for the tickets

- T771: on the Skeena the check has nothing to report today. The id still orders several equal-size pairs that agree; the check should compare answers, not just sizes.
- The rest-of place's reach row reads `outcome: unresolved, reason: no_sections`. The bundle handles it (not in `u`, not in `off`), but any audit that counts "unresolved" DFO rows will count it and its dormant twin.
- The dormant "from the confluence with the Babine River downstream to the CNR Railway Bridge" place (53 sections) sits ahead of the sockeye openings (54) in the chain. No rules today; if DFO re-lists that wording, it answers first.

---

# PART 1 (before the rebuild)

# Skeena cascade of rules: independent review, PART 1 (2026-11-15, before the rebuild finishes)

Read-only. Nothing in the repo was changed. Everything below is in
`/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/64288e7d-44dc-46e9-9576-a31a32c1991a/scratchpad/cascade/`.

Tags: **[ran]** a script I ran, **[read]** read in a source or in code, **[inferred]** my reading.

Sources used
- DFO page: `cache/dfo_salmon/raw/region6-eng.html` (fetched 2026-11-09, DFO "date modified" 2026-09-02; manifest says unchanged since the previous fetch) [read]. Plain-text dump: `region6_page.txt` (line numbers below are lines of that dump).
- Typed rows: `data/generated/regs/dfo_salmon/typed/region-6.json` (typed_format 3, same page date) [read].
- Places: `data/generated/side/rebuild-input-2/curated/dfo_salmon/places/region-6.json`, identical to `data/curated/dfo_salmon/places/region-6.json` at the time I read it [ran].
- Side run: `data/generated/side/rebuild-input-2/reach/dfo_salmon.jsonl.gz`, `rule_section.jsonl.gz`, `rule_extent.jsonl`, `report.json`; `binding_diff.json` (in the run's top folder, not in `logs/`); atlas sidecars `registry.json`, `splits.resolved.json`, `tidal_sections.json`. `graph.pkl` was not loaded.
- Code: `pipeline/deliver/salmon.py` (whole), `pipeline/regs/dfo_salmon/live.py` `compose`, `entries.py`, `closures.py` (parts).
- Rule book: side copy of `synopsis/region-6.json`, `data/curated/regulations/reference/region-6.md`, `pipeline/docs/RULINGS.md` (Q17, L3, L4, T2, T9, T13, S8, S9, S11, Li5), the memory notes named in the brief. The PDF could not be opened here (no PDF renderer), so printed words are quoted from the entry's `regs_verbatim` and the reference transcription, with the printed page the entry records.

Limits of Part 1
- My chain simulation (`sim_chain.py`) uses the repo's own `chain_order`, `location_rules`, `verdict_on`, `gear_on` on the side run's placements. It does NOT simulate the region-membership step (T668) or the closed-area pieces. Both are listed for Part 2.
- The side run still has the pink place on the whole river (222 sections) and the Morice top 2 km uncovered; both fixes were still landing.

---

## A. DFO: expected answers from the page alone

### A0. How DFO's page is built [read]

- Page sentence (line 13): "Unless otherwise stated in the table below, the daily limit for all waters in Region 6 is zero (0)".
- A (lines 16-18): "All lakes or streams or parts thereof in the aggregate (combined total), in Region 6, unless otherwise stated below. Please review sections B, C, D, E and F carefully." Chinook "4 per day, only 1 over 65 cm."; Coho "4 per day, only 1 over 50 cm."; "Sockeye, pink and chum | Apr 1 to Mar 31 | No fishing for sockeye, pink or chum salmon, unless otherwise stated below".
- B heading (line 19): "Section "A" applies if stream, specific area, time period, quotas or other species restrictions are not listed in the following sections."
- B(i) default (lines 21-25), "All waters in section "B(i)" – Skeena River Watershed upstream of the CNR Railway Bridge at Terrace, unless otherwise stated below": "All | Jan 1 to Jun 15 | No fishing for salmon"; coho, sockeye, chinook, chum each "Apr 1 to Mar 31 | No fishing". No pink row.
- B(ii) default (lines 98-102), "... downstream of the CNR Railway Bridge at Terrace, unless otherwise stated below": "No fishing for eulachon"; chinook, sockeye, coho, pink each "Apr 1 to Mar 31 | No fishing". No chum row.

So the cascade DFO states is: a water's own row for that fish and date, else the section default, else A, else the page sentence. Per fish, per date. [inferred from lines 13, 16, 19, 21, 98]

Consequences used everywhere below:
- Plain tributary ABOVE the CNR bridge (B(i) only): all five closed all year. Chinook, coho, sockeye, chum by B(i); pink by B(i) "All" Jan 1 to Jun 15 and by A the rest of the year.
- Plain tributary BELOW the CNR bridge (B(ii) only): all five closed all year. Chinook, sockeye, coho, pink by B(ii); chum by A.
- A's chinook and coho openings never reach the Skeena watershed: both section defaults close them all year.
- E's "All streams and lakes flowing into tidal waters of Areas 3, 4, 5, and 6" (line 184) is printed under E, "Other Mainland Watersheds", so it does not speak for the Skeena although the Skeena drains to Area 4.

### A1. Skeena mainstem, by stretch (metres from the mouth on the main line)

DFO rows: B(i) lines 77-88, B(ii) lines 132-143.

| # | Stretch | Chinook | Coho | Sockeye | Pink | Chum | Row that answers; what it overrides |
|---|---|---|---|---|---|---|---|
| 0 | below the tidal boundary (0 to 6,405) | tidal water: not this page | | | | | |
| 1 | tidal boundary to the Zymagotitz mouth (6,405 to 115,773) | closed all year | Jul 15 to Aug 31: 4/day, 2 over 50 cm. Sep 1 to Nov 30: 4/day, 1 over 50 cm. Dec 1 to Jul 14: closed | Apr 1 to Jul 9 closed; Jul 10-28: 2/day; Jul 29 to Aug 16: 4/day; Aug 17-27: 2/day; Aug 28 to Mar 31 closed | 2/day all year | closed all year | "mainstem waters downstream of CNR Railway Bridge at Terrace" (lines 132-141) over B(ii). Coho Dec 1 to Jul 14 falls to B(ii). |
| 2 | Kitsumkalum mouth zone, Zymagotitz mouth to "top of Hell's Gate" (115,773 to 123,961) | closed all year | Jul 1 to Aug 31 closed; Sep 1 to Nov 30: 4/day, 1 over 50; rest closed | Jul 1 to Aug 31 closed; **Sep 1-15: 2/day**; rest closed | 2/day except Jul 1 to Aug 31 | closed | Zone rows (lines 142-143): "All | Jul 1 to Aug 31 | No fishing for salmon", "Sockeye | Sep 1 to Sep 15 | 2 per day FN0679", over stretch 1's rows. |
| 3 | top of Hell's Gate to the CNR bridge (123,961 to 132,165) | as stretch 1 | | | | | |
| 4 | CNR bridge to Cedarvale (132,165 to 208,305) | closed all year | Jul 15 to Oct 15: 4/day, 2 over 50; rest closed | Jul 10-28: 2/day; Jul 29 to Aug 16: 4/day; Aug 17-27: 2/day; rest closed | Jun 16 to Dec 31: 2/day; Jan 1 to Jun 15 closed | closed | Lines 77, 80, 81, 82-86 over B(i). |
| 5, 7, 9 | Cedarvale to the Babine mouth, outside the two closed zones (208,305 to 335,400) | closed all year | as 4 | as 4 | as 4 | closed | As 4, plus "mainstem waters only, upstream of Cedarvale | All | Jan 1 to May 31 | No fishing for salmon" (line 78). |
| 6 | Kitwanga mouth zone, Mill Creek to the Highway 37 bridge (227,042 to 229,500) | closed all year, every fish | | | | | Line 88, "All | Apr 1 to Mar 31 | No fishing for salmon". |
| 8 | Kispiox confluence, inside the 4 signs (about 284,310 to 284,780) | closed all year, every fish | | | | | Line 87. |
| 10 | above "a point above the confluence with the Babine River" to the top | closed | as 4 | **closed all year** (B(i)) | as 4 | closed | The sockeye rows stop at "a point above the confluence with the Babine River"; DFO does not say where that point is. |

### A2. Tributaries with their own rows

| Water (section) | DFO rows (dump lines) | Expected answer |
|---|---|---|
| Kispiox, including tributaries (B(i)) | 46-51 | Chinook closed. Coho: Aug 24 to Sep 3 closed (FN0897), **Sep 4 to Oct 15: 4/day, 2 over 50 (FN0951)**, rest closed by B(i). Pink: "downstream of fishing boundary signs near Kispiox River Resort" **Jun 16 to Aug 23: 2/day**; Aug 24 to Mar 31 closed (FN0897); everywhere else and every other date closed. Sockeye, chum closed. Gear: "No natural bait allowed" all year. |
| Kitsumkalum River, including tributaries (B(ii)) | 116-120 | Chinook closed (both halves printed). Coho Sep 1 to Oct 31: 4/day, 1 over 50; rest closed. Pink: "downstream of railway bridge" 2/day all year; elsewhere closed. Sockeye closed (B(ii)), chum closed (A). Gear: no bait Jan 1 to June 15. |
| Kitsumkalum Lake, including tributaries (B(ii)) | 121-122 | Coho Sep 1 to Oct 31: 4/day, 1 over 50. Everything else closed. |
| Kitwanga, including tributaries (B(i)) | 54-55 | Everything closed all year. Gear: no bait Sep 1 to Dec 31. |
| Zymoetz (B(i)) | 94-96 | Everything closed all year. Gear: no bait all year. No "including tributaries": tributaries are plain B(i). |
| Zymagotitz, including tributaries (B(ii)) | 146-147 | Coho Sep 1 to Oct 31: **1 per day**. Everything else closed. |
| Lakelse River, including tributaries (B(ii)) | 123-127 | Pink closed all year. Coho: "downstream of the CNR bridge" Sep 1 to Oct 31: 4/day, 1 over 50; above it closed. Chinook closed. Gear: no bait all year. |
| Exchamsiks, Exstew, including tributaries (B(ii)) | 106-109 | Coho Sep 1 to Oct 31: 4/day, 1 over 50. Everything else closed. |
| Kasiks, including tributaries (B(ii)) | 112-114 | Coho: **upstream of the signs below the Upper Pool closed all year; downstream of them Sep 1 to Oct 31: 4/day, 1 over 50**. Everything else closed. |
| Gitnadoix, including tributaries (B(ii)) | 110-111 | "upstream of powerline crossing near river mouth": coho Sep 1 to Oct 31: 4/day, 1 over 50; chinook closed. Below the powerline: plain B(ii), all closed. |
| Khyex (B(ii)) | 115 | Everything closed all year. |
| Ecstall, including tributaries (B(ii)) | 103-105 | Coho Sep 1 to Oct 31: 4/day, 1 over 50. Everything else closed. |
| Bulkley (B(i)) | 38-43 | "downstream of the Morice River confluence excluding tributaries": pink Jun 16 to Oct 15: 2/day; coho Jul 15 to Oct 15: 4/day, 2 over 50; chinook closed. The Forks (100 m above the confluence to about 1 km below): all closed Jun 16 to Aug 15. Bulkley above the Morice confluence: no row, plain B(i), all closed. Tributaries: chinook closed; no bait Aug 1 to Dec 31 (not the Morice, Suskwa, Two Mile). |
| Morice, including tributaries (B(i)) | 57-70 | Outlet signs to Gosnell Creek: all closed Jan 1 to Sep 30, then closed by B(i): closed all year. Gosnell to Lamprey: all closed Jan 1 to Aug 31; coho Sep 1-30: 4/day, 2 over 50, fly-fishing only. Below Lamprey: coho Jul 15 to Oct 15: 4/day, 2 over 50. Pink: confluence up to the Bymac bridge Jun 16 to Aug 31: 2/day; above the bridge closed. Chinook closed. The Forks arm: all closed Jun 16 to Aug 15. Gear: no bait all year. |
| Telkwa (B(i)) | 93 | "downstream of Howson Creek": coho Jul 15 to Oct 15: 4/day, 2 over 50. Everything else closed. |
| Suskwa (B(i)) | 89-90 | Everything closed. No bait all year, tributaries included. |
| Babine River (B(i)) | 30-35 | Coho Jul 15 to Oct 15: 4/day, 2 over 50. Sockeye Aug 1-16: 2/day (FN0793), rest closed. Chinook, pink, chum closed. No bait all year. |
| Babine Lake (B(i)) | 26-29 | Chinook closed (tributaries too). Sockeye on the lake itself **Aug 1-27: 2/day (FN0679)**, rest closed; closed all year within 400 m of the 12 named creek mouths and east of the Gullwing Creek line. Coho, pink, chum closed. |
| Fulton (B(i)) | 44-45 | Sockeye Aug 1-14: 2/day; rest closed. |
| Pinkut Creek (B(i)) | 73-75 | Below the signs 25 m below the fence: sockeye Aug 1-14: 2/day; all closed Aug 15 to Sep 15. Rest closed. |
| Shegunia (B(i)) | 76 | Everything closed. |

### A3. Where DFO's own structure is ambiguous [inferred]

1. **Two sockeye rows on one stretch** (lines 82 and 83-86). Line 82: "upstream of CNR bridge at Terrace, B.C. to a point above the confluence with the Babine River, excluding the areas below | Sockeye | Apr 1 to Mar 31 | No fishing for sockeye". Lines 83-86: "mainstem waters only, upstream of CNR bridge at Terrace, B.C. to a point above the confluence with the Babine River" with the in-season openings. "The areas below" are the mainstem rows printed under it, so the openings must answer on the mainstem and line 82 is the leftover.
2. "a point above the confluence with the Babine River": no landmark.
3. Fulton: "Sockeye | Apr 1 to Mar 31 | No fishing for sockeye" and "Sockeye | Aug 1 to Aug 14 | 2 per day FN0679" in one place. The dated row is the exception.
4. Babine River line 34: species cell "Sockeye", limit "No fishing for salmon FN0869". Read by the cell (sockeye only), coho stays open to Oct 15. The typed feed reads it that way [read].
5. Kitsumkalum zone "Sockeye | Sep 1 to Sep 15 | 2 per day FN0679" while the river around it is closed from Aug 28 (FN0919). Looks like a stale row, but it is printed.
6. "Upstream of Cedarvale: All, Jan 1 to May 31" against B(i)'s "All, Jan 1 to Jun 15". No fish opens before Jun 16, so nothing depends on it.
7. A stretch printed under a water named "(including tributaries)" (Kispiox below the resort signs, Morice below Lamprey Creek, Lakelse below the CNR bridge): does the stretch's opening reach tributaries that join inside it?
8. Morice has two rows with different top ends: "from Gosnell Creek to Lamprey Creek" and "from fishing boundary signs located approximately 100 m downstream of Gosnell Creek to Lamprey Creek".
9. Notes that are not rules: the Kitsumkalum mouth note, the Zymoetz note, "Note: Barbed hooks are authorized in Babine Lake", the Tlell tidal note.
10. "Skeena River | upstream of the CNR Bridge located at Terrace | Coho" has no "mainstem waters only". Read as the river itself.

---

## B. The mechanism, from code

### B1. How a chain is built and read [read `pipeline/deliver/salmon.py`]

- A section's chain is every place whose extent covers it, sorted by `chain_order` (line 359): tier descending, then fewer sections, then the "other places" flag, then the location id. On a section in one region, that region's places come first within a tier (`plan_at`, line 750).
- `verdict_on` (line 242): per fish and day, walk the chain; the first place with a limit row for that fish that holds that day answers. Inside one place: the fish's own row beats an "All" row, except an "All" closure the fish's row is not dated inside (`_all_closure_wins`); if an opening and a closure both hold, the opening answers. A row whose date did not read makes the fish "check" at that place every day. If nothing speaks: closed (`NOT_LISTED`).
- `gear_on` (line 306): the first place with ANY gear line that day gives all the gear lines; a row about another fish (eulachon) is treated as a gear line.
- A place with no extent rides every section of its water as a note (`u`), never painted. A dormant place is in the chain with no rules. A closed-area piece is a place of its own with its parent's tier.
- Region 6's page sentence is not a separate tier -1 place: it is a second fingerprint (`page:6:region`) on `6:a:region` [read places file].

### B2. Where the order or the answer comes from something other than DFO's structure

| Mechanism | Skeena example | Right by A? |
|---|---|---|
| **Id tie at equal tier and size** | `6:skeena-river:mainstem-waters-only-upstream-of-c~2` (openings) and `6:skeena-river:upstream-of-cnr-bridge-at-terrace~2` (closed all year): both 54 sections on the side run. "mainstem…" sorts before "upstream…". They disagree on sockeye on 49 days, Jul 10 to Aug 27 [ran `scan_mech.py`]. | Right, by alphabet only. Reversed, the mainstem sockeye season would read closed. |
| Id tie, same rule both sides | The Forks: `6:bulkley-river:bulkley-river-and-morice-river-wat` and `6:morice-river:…`, 3 sections each. Also the pairs "whole" / "including tributaries" on Kitseguecla and Sustut. | No effect: 0 fish-days differ [ran]. |
| Size tie-break among same-tier places | Kitsumkalum zone (4 sections) before the lower mainstem (128); Kitwanga zone (3) and Kispiox zone (2) before everything; "below the railway bridge" (1) before "below the canyon signs" (117). | Right. This is what makes "excluding the areas below" work for the two closed zones. |
| Dormant places | `6:skeena-river:from-the-confluence-with-the-babin` (53 sections, newly bound tonight) sorts BEFORE the sockeye pair (54). Dormant twins at the Kispiox zone, Lakelse mouth to Ferry Island, Babine River, Morice, Zymoetz note, `6:b:section`. None has rules today [ran]. | No effect today. If DFO re-lists the Babine-to-CNR wording, the live feed will let it answer ahead of the current pair. |
| "Other places" riders | None in Region 6 [ran]. | n/a |
| Region straddlers | 15 Skeena-basin sections lie in Region 7A by the region outline (8 streams, 7 lakes) [ran `rule_section` counts]; 8 of them show in my samples (7 Sustut tributaries, 1 Babine Lake tributary) with `7:default:region` and without `6:a:region`. Named-water and B(i) places still bind them. | Cannot tell without the bundle (Part 2). All fish are closed by B(i) except pink after Jun 15, which falls to Region 7's sentence. |
| Undrawn places as notes | Kasiks: 3 live places with no extent. Babine Lake: the sockeye place and 13 closed pieces. | Wrong on the map, said on the card. See C. |
| Closed-area pieces | Babine Lake's 400 m circles and the Gullwing line: all "carried, not confirmed" [ran]. | They close nothing on the map; harmless only because the lake opening is not drawn either. |
| Gear: narrowest place with any line that day | B(ii)'s "No fishing for eulachon" shows on the lower mainstem all year, but on the Kitsumkalum only Jun 16 to Dec 31 (the bait line hides it Jan 1 to Jun 15) and never on the Lakelse (bait line all year) [ran]. Morice, Gosnell to Lamprey, Sep 1-30: "Fly-fishing only" replaces "No natural bait allowed". | Eulachon line: not DFO's structure, it applies to all of B(ii). Not a salmon answer. Fly-only replacing no-bait: harmless. |
| Opening beats closure inside one place | Fulton sockeye Aug 1-14 [ran]. Also A: chinook and coho rows against the page sentence. | Right in both. The rule that happens to hold is "the row with the shorter dates is the exception". |
| A row whose date did not read | None in sections A, B, B(i), B(ii) [ran]. | n/a |

### B3. Recommendation for the equal-tier, equal-size tie

A rule that does not depend on ids, in two parts:

1. **A place whose printed words make it the leftover answers after its siblings.** "excluding the areas below", "unless stated below", "all other" are DFO saying "the rest". Such a place joins the existing "rides" slot of the sort key (between size and id), exactly as the "other places" places already do. The sockeye pair then orders by DFO's words. The place file already marks it (`source_text.op: whole_water_excluding`, and the binding note calls it a residual) [read].
2. **A build check for what is left.** After (1), any two live places of equal tier and equal size on a shared section that give a different answer for some fish on some day stop the build (or are reported by name). Today that check would name one Skeena pair (this one) and one pair outside it: the two Tatshenshini "upstream of the border" places, 984 sections, coho differs on 344 days [ran]; that is the Tatshenshini mis-binding the second look already flagged.

I do not recommend "the stricter answer wins" as the tie rule: here it would close the sockeye season DFO opens.

---

## C. Expected (A) against the side run's bindings

Outputs: `sim_skeena_main.txt`, `sim_tribs_1.txt` to `sim_tribs_4.txt` [ran].

**Mainstem: every stretch and fish matches A** on the side run, including the Kitsumkalum zone, both closed zones, the sockeye season and its end above the Babine. Tributaries match A except the rows below.

### C1. Differences

| # | Water, stretch | Fish, dates | Expected (A) | Chain's likely answer | Why |
|---|---|---|---|---|---|
| 1 | Kasiks, below the signs below the Upper Pool | coho, Sep 1 to Oct 31 | open, 4/day, 1 over 50 | closed all year (B(ii)) | The three live Kasiks places have no extent; only the dormant "whole" is drawn. The two cuts exist in the side atlas (m 194 and m 17,653). Pending proposals. |
| 2 | Babine Lake, the lake itself | sockeye, Aug 1-27 | open, 2/day | closed all year (B(i)) | The live lake place has no extent; it rides the lake as a note. |
| 3 | Gitnadoix, mouth to the powerline crossing | coho, Sep 1 to Oct 31 | closed (B(ii)) | open | "upstream of powerline crossing" is bound to the whole river (1,000 sections). The second look records this as kept. Reads more open than DFO on a short stretch. |
| 4 | Morice, the 100 m between Gosnell Creek and the signs below it | coho, Sep 1-30 | open (row "from Gosnell Creek to Lamprey Creek") | closed | Both Gosnell-to-Lamprey places start at the signs; the upper place ends at the creek. The owner kept the signs point. |
| 5 | Kitsumkalum Lake (the lake polygon) | gear, Jan 1 to Jun 15 | no natural bait | no bait line | The river's "including tributaries" reaches the lake's inflow streams but not the lake polygon. Salmon answers unaffected. |
| 6 | B(ii) waters with a bait line | "No fishing for eulachon" | shown all year | hidden while a narrower gear line holds | B2. |

### C2. Same answer, different reason (no fish changes)

- Skeena "mainstem waters only" (pink) drawn on the whole river: below the bridge the lower-mainstem place answers pink first every day [ran]. After the fix nothing changes for the angler.
- Morice top 2 km (m 89,755 to the lake): every fish is closed all year with or without the fix, because "upstream of the Bymac Bridge" closes pink and B(i) closes the rest [ran]. The second look's line that pink is left open there Jun 16 to Sep 30 is not what the chain gives. The fix changes only the words shown.
- Between the Babine mouth (335,400) and gauge 08EB005 (337,743) the sockeye season applies: that is the chosen reading of "a point above".

### C3. Cannot tell without the bundle (Part 2)

- The order of the sockeye pair in the built chain, and whether the fix to the pink place changed any size and so any order.
- The 15 Region 7A sections of the Skeena basin.
- 27 Skeena side-channel sections carry the "above the gauge" chain. If any lies between the CNR bridge and the gauge, it loses the sockeye season. Needs positions.
- The tidal section `360887278:0`: no DFO place covers it, so it should be chain 0.
- How the card shows Kasiks and Babine Lake (the undrawn places and pieces).

### C4. Reading questions (A is ambiguous, the chain picked one)

- Tributaries joining inside a stretch get the stretch's opening: Kispiox below the resort signs (288 tributary sections, pink), Morice below Lamprey Creek (844, coho), Lakelse below the CNR bridge (about 100, coho) [ran].
- Kitsumkalum zone sockeye Sep 1-15: shown open as printed.

---

## D. The rule book layer on the Skeena

### D1. Region 6 base, printed p.49 [read entries + reference transcription; counts ran on `rule_section`]

| Rule (printed words) | Binds in the Skeena basin (87,279 sections) |
|---|---|
| "Bait ban: In the Skeena River, including tributaries, and Nass River, including tributaries, year-round." | 70,409 stream sections: every stream section of the basin inside Region 6. |
| "Single barbless hook: must be used in all streams of Region 6, all year." | 70,409 |
| "No fishing: in all rivers and streams for steelhead, May 15 – June 15. Exemptions include mainstem portions of the Skeena, Nass, Iskut, Stikine and Taku Rivers, not currently closed under the existing winter/spring closure regulation." | Closure 70,409; the exemption binds the 222 freshwater Skeena mainstem sections. |
| "No fishing: in any stream in the watersheds of the Skeena River upstream of Cedarvale … Jan 1-June 15 (NOTE: … Skeena River mainstem upstream of Cedarvale is only closed Jan 1-May 31). See tables for other exceptions." | Tributary streams above Cedarvale 48,328 (Jan 1 to Jun 15); mainstem above Cedarvale 72 (Jan 1 to May 31). The 72 are the same count as DFO's "upstream of Cedarvale". |
| Trout/char 5, 1 over 50 cm (hatchery steelhead counted), 3 Dolly Varden/bull/lake trout, 1 trout from streams Jul 1 to Oct 31; releases: Dolly Varden/bull trout from streams, trout under 30 cm from streams, all trout from streams Nov 1 to Jun 30, lake trout from the Fraser and Skeena watersheds Sep 15 to Nov 30, all wild steelhead | Region-wide rows 87,264; stream rows 70,409. |
| Kokanee 10 (none from streams), grayling 3, burbot 5, whitefish 15, sturgeon closed, possession = 2 daily quotas, set lining in lakes only | 87,264 / lakes 16,855 |

The base matches the reference transcription line for line [read]. The four rules the reference file lists as "missing" are all present now.

**15 basin sections get no Region 6 base at all** (87,279 minus 87,264): they lie in Region 7A by the outline and take 7A's base (its stream bait ban, spring closure and quotas bind 8 streams and 7 lakes there) [ran]. Water rows still reach them.

### D2. Skeena-watershed rows and their extents [read; bound counts ran]

52 Region 6 water rows bind in the basin. The ones that move an answer:

| Row (printed page) | What it adds to the base | Extent, sections bound |
|---|---|---|
| SKEENA RIVER ✱ (p.53) "Bait ban" | Same as the base | Whole river with tributaries, 70,392. 17 fewer than the base's bait ban. |
| SKEENA RIVER (mainstem only) (p.53) | Class II "Skeena River 2", Jul 1 to Sep 30, Exchamsiks mouth (67,917) to 1.5 km above the Kitsumkalum (123,961); Class II "Skeena River Section 4", Jul 1 to Dec 31, from 1.5 km above the Zymoetz (139,970) up. Non-guided non-resident aliens, Sat/Sun Jul 1 to Dec 31: Shegunia mouth to Sedan Creek (19 sections), Chimdemash Creek to 1.5 km above the Zymoetz (7). | Cuts as printed. |
| SKEENA RIVER/KISPIOX RIVER CONFLUENCE (p.53) | No angling from boats inside the 3 signs | 2 sections, the same zone DFO closes. |
| KISPIOX ✱ (p.51) | Bait ban, no boats, no powered boats, alien weekend closure, Class II Sep 1 to Oct 31 | Whole with tributaries. |
| KITSUMKALUM ✱ (p.52) | Bait ban, Class II all year, alien Saturday (lake outlet to Glacier Creek, 396) and Sunday (all, 3,317) closures | 3,317 = DFO's "whole" count. |
| KITWANGA ✱ (p.52) | Bait ban, Class II all year | 921 = DFO's count. |
| KITSEGUECLA (p.52), no ✱ on the name | Bait ban, alien closure on the river only (5); the Class II clause prints its own "[Includes Tributaries]" | Consistent with ruling T2. |
| ZYMOETZ ✱ (p.54) | No fishing: McDonell Lake down 3 km (215); in the canyon (13); above the transmission-line sign Jan 1 to Jun 15 (4,972). Two classified units split at Limonite Creek: A above (Class I, Jul 24 to Dec 31), B below (Class II, Jul 24 to May 31), each with its own alien weekend rule. Bait ban (5,309). | As printed. |
| BULKLEY ✱ (p.50) | "No Fishing upstream of Morice/Bulkley River confluence" (2,188); bait ban and alien closure on the whole watershed (14,424); no boats Morice to Barrett Aug 15 to Dec 31 (60), Moricetown Canyon (4); Class II Sep 1 to Oct 31 | As printed. |
| MORICE ✱ (p.52) | No fishing outlet signs to the Dunes all year (11); Dunes to Gosnell Jan 1 to Sep 30 (61, the same 61 as DFO's place); **Gosnell to Lamprey Jan 1 to Aug 31 and fly only Sep 1-30 (1,958)**; bait ban, no boats Aug 15 to Dec 31, Class II Sep 1 to Oct 31 (2,917) | See D4 item 1. |
| BABINE RIVER ✱ (p.50) | No fishing between the signs at the fence (4); "Open all year" in Rainbow Alley (6, lifts the winter closure); fly only stretches; bait ban; no boats below the fence; Class I Sep 1 to Oct 31 below Nilkitkwa Lake | As printed. |
| BABINE LAKE (p.50) | No fishing east of the Gullwing line; within 400 m of Pinkut Creek Aug 15 to Sep 15 | Both are undrawn parts carried on the lake as notes (1 section). |
| LAKELSE RIVER ✱ (p.52) | Alien closure lake outlet to the powerline; fly only lake to CNR bridge Mar 1 to May 31 (179); bait ban, no powered boats (288); Class I all year | 288 = DFO's "whole" count. |
| FULTON ✱ (p.51) | "Open June 16-Apr 30 each year": closed May 1 to Jun 15, winter closure lifted Jan 1 to Apr 30 (1,194) | As printed. |
| GOSNELL ✱ and THAUTIL ✱ (p.51, p.54) | "No Fishing Aug 1-Sept 30; bait ban" (806 and 1,653) | See D4 item 1. |
| SUSTUT ✱, BEAR ✱ (p.53, p.50) | No fishing above the BCR bridge at the Bear mouth (3,229); Bear: "No Fishing" (476) | As printed. |
| TELKWA, SUSKWA, ECSTALL, GITNADOIX, KLUATANTAN | Bait ban and classified designations | Whole with tributaries. |
| PINKUT (p.53) | No fishing below the fish fence Aug 15 to Sep 15 (2) | Province cuts at the fence, DFO at the signs 25 m below: 2 sections against 1. |
| TWO MILE CREEK (p.54) | Open all year, trout quota 1, no minimum size | 3 |

### D3. Tonight's cuts and the binding diff: 184 rules moved, attributed [ran `binding_diff.json`]

No Skeena rule changed its extent. Every move is a new, smaller section inside an extent the rule already had.

- New Region 6 sections (8): Skeena main line at 108,507 (Lakelse mouth), 131,651 (Ferry Island bridge), 335,400 (Babine mouth); two Skeena side channels cut by the Lakelse-mouth cut (`360217743:684`, `360223771:502`); one channel in the Lakelse set, `360866092:168` (the resolved cut says "tributary of a braid cut"); Kasiks at 194 and 17,653. One section gone: the Kasiks automatic length cut at 22,598.
- 68 provincial rules +16 −3: the 8 above plus the Shuswap, Thompson, Nanaimo, Puntledge and Sproat cuts.
- 30 Region 6 base rules and `skeena_river.r1` (bait ban) +8 −1: exactly the 8 and the 1.
- `steelhead_stream_closure.r2` (the mainstem exemption) +5: the 3 main-line pieces and the 2 side-channel pieces. Correct: all are Skeena mainstem.
- `skeena_nass_winter_closure.r3` (mainstem above Cedarvale) +1: the Babine piece. Correct.
- `lakelse_river.r3, r4, r5` and the Lakelse licence +1: `360866092:168`.
- Licences: "Skeena River 2" +3 (the Lakelse-mouth pieces, inside Exchamsiks 67,917 to 123,961); "Skeena River Section 4" +1 (Babine piece).
- The rest are other regions' base rules and rows (Region 1: 22, Region 3: 25, Region 8: 22, Fraser sturgeon rows).

One thing to look at in Part 2: the Lakelse-mouth cut is a confluence cut with a 428 m offset, and it cut a channel of the Lakelse system 169 m up. Which side of the cut that 169 m piece falls on matters to nothing live today (only the dormant DFO place uses the cut), but it should be on the Lakelse side for every Lakelse rule.

### D4. Base-plus-deltas questions

1. **Gosnell Creek and the Thautil River are inside the Morice's "Gosnell Creek to Lamprey Creek" closure** [ran]. `morice_river.r4` ("No Fishing from Gosnell Creek to Lamprey Creek, Jan 1-Aug 31") and `r5` bind 1,958 sections: 131 on the stretch (the same 131 DFO binds) and 1,827 more, among them all 20 Gosnell Creek sections, all 6 Thautil River sections and all 5 Lamprey Creek sections with their tributaries. Gosnell and Thautil each have their own row, "No Fishing Aug 1-Sept 30; bait ban". Ruling T9 says a joining water at a confluence cut with a row of its own is left out. With r4 on them they read closed Jan 1 to Sep 30; by their own rows plus the base they would be closed Jan 1 to Jun 15 and Aug 1 to Sep 30, open Jun 16 to Jul 31 [inferred]. Their own printed dates would be half redundant if the Morice closure applied. Not caused by tonight's cuts (not in the diff). Lamprey Creek has no row, so T9 puts it in.
2. The same words bind 1,958 sections in the rule book and 131 in the DFO layer. No salmon answer differs (all closed on those creeks), but the two layers disagree on what "Gosnell Creek to Lamprey Creek" covers.
3. The SKEENA RIVER ✱ bait-ban row reaches 70,392 sections, the base bait ban 70,409, and the basin has 8 more stream sections in Region 7A. Every one still has a bait ban from one source or another; the attribution differs on at least 25 sections.
4. The 15 sections in Region 7A: their base is 7A's, not Region 6's. No Region 6 steelhead closure, no winter closure there.
5. Steelhead on the mainstem above Cedarvale: closed Jan 1 to May 31 (winter closure), open from Jun 1 (exemption, ruling L4). Tributaries closed to Jun 15. DFO's "upstream of Cedarvale, All, Jan 1 to May 31" matches the province's date.
6. Classified waters, one unit per section (Li5): the mainstem between 123,961 and 139,970 is in neither Skeena unit, as printed. Zymoetz A and B split at Limonite Creek. Gosnell takes the Morice's unit.
7. Cross-layer: the province bans bait on every Skeena stream all year, so DFO's dated bait lines (Kitwanga Sep 1 to Dec 31, Kitsumkalum Jan 1 to Jun 15, Bulkley tributaries Aug 1 to Dec 31) never change what an angler may do; they only change which line the salmon card prints.

---

## E. Questions for the owner

1. **Skeena above Terrace, sockeye.** DFO prints two rows for the river from the CNR bridge to above the Babine: one says no sockeye all year "excluding the areas below", the other gives the summer openings for the mainstem. Today the openings win only because of how the two are named inside our files. Recommended: make it a rule that a row saying "excluding the areas below" always answers after the rows below it, and have the build stop if two equal places ever disagree.
2. **Kasiks River, coho.** DFO opens coho below the signs below the Upper Pool from Sep 1 to Oct 31. Our map shows the whole Kasiks closed for coho until the two new cuts are accepted. Recommended: accept the pending Kasiks cuts so the lower river reads open.
3. **Babine Lake, sockeye.** DFO opens sockeye on the lake Aug 1 to Aug 27 (not within 400 m of 12 creek mouths, not east of the Gullwing Creek line). Our map shows the lake closed and the opening only as a note. Recommended: keep it closed on the map with the note until the circles and the line are confirmed, since drawing the opening without them would open water DFO closes.
4. **Gitnadoix River, coho near the mouth.** DFO opens coho only upstream of the powerline crossing near the mouth; we open the whole river. Recommended: leave as is only if the powerline is at the mouth for practical purposes; otherwise add the cut.
5. **Gosnell Creek and Thautil River, trout fishing in early summer.** The book gives each its own line, "No Fishing Aug 1-Sept 30". We also apply the Morice's "No Fishing from Gosnell Creek to Lamprey Creek, Jan 1-Aug 31" to both creeks, which closes them from Jun 16 to Jul 31 as well. Recommended: leave both creeks out of the Morice closure, as the ruling for a joining creek with its own line says.
6. **Tributaries inside a salmon stretch.** On the Kispiox below the resort signs (pink), the Morice below Lamprey Creek (coho) and the Lakelse below the CNR bridge (coho), we give the opening to the side creeks that join inside the stretch, because DFO heads each river "(including tributaries)". Recommended: keep it.
7. **Skeena near the Kitsumkalum mouth, sockeye Sep 1 to Sep 15.** DFO still prints "2 per day" for this short stretch although the river around it closed Aug 28. Recommended: show it as printed with its notice number.
8. **Eulachon line.** "No fishing for eulachon" below Terrace disappears from the card on the Lakelse all year and on the Kitsumkalum until Jun 15, because a bait line takes its place. Recommended: always show it below Terrace.
9. **"A point above the Babine".** We end the sockeye season at the water gauge 2.3 km above the Babine mouth. Recommended: keep.

## To verify in Part 2 (on the built release)

1. The built chain for a mainstem section between the CNR bridge and Cedarvale: order of the sockeye pair, and sockeye on Jul 15 (expected open, 2/day).
2. The pink place after the fix: about 94 sections, none below the bridge; pink on the lower mainstem still 2/day all year.
3. Every stretch of A1 and every row of A2, per fish across the year, against the release's `salmon` file, with the answering place.
4. Kasiks and Babine Lake: bound or still notes; what the card says.
5. The Morice top 2 km after the fix: still closed all year, now with the Jan 1 to Sep 30 words.
6. The 15 Region 7A sections: regions on the chain, pink after Jun 15.
7. The 27 Skeena side channels with the "above the gauge" chain: all above 337,743?
8. The tidal section `360887278:0`: chain 0.
9. Gear lines: eulachon on the Lakelse and Kitsumkalum; Kitsumkalum Lake polygon.
10. Rule book, with `effective_rules`: Gosnell Creek and the Thautil on Jul 1 (which rule closes them); a Skeena tributary and the mainstem on May 20 and Jun 5 for steelhead; mainstem above Cedarvale on May 31 and Jun 1; Rainbow Alley and the Fulton in February; one section in each of Skeena River 2, Section 4, Zymoetz A, Zymoetz B and the gap between the two Skeena units.
11. Base plus deltas on one section per mainstem stretch and per tributary row: every difference from the Region 6 base attributable to a named row.
12. `360866092:168` and its 169 m lower piece: which Lakelse rules each carries.
13. The 17-plus stream sections the Skeena bait-ban row misses.

## Files

- `part1.md` (this report)
- `region6_page.txt` (DFO page as text), `places_r6.txt` (places and extents)
- `sim_chain.py`, `sim_skeena_main.py`, `sim_tribs.py`, `scan_mech.py`, `scan_rule_section.py`, `dump_page.py`
- `sim_skeena_main.txt`, `sim_tribs_1.txt` to `sim_tribs_4.txt` (per-stretch answers with the answering place)
- `synopsis_r6_zone.txt`, `synopsis_skeena_rows.txt` (rule book entries with bound counts)
- `registry_slim.pkl`, `rule_section_skeena.pkl` (working data for Part 2)
