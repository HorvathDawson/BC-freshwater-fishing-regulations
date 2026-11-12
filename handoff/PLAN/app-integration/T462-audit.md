# T462 audit — does every screen show the regulations the release holds, and the synopsis says?

Read-only, 2026-11-11. Release `r-303b4007a70a` (LATEST; staging serves the same one: `site-release.json`). No code and
no data changed. Findings only; each fix is one line and is a later lane. Printed page numbers are the book's own.

## Summary

- **30 findings: 8 wrong regulation shown, 9 regulation missing, 10 misleading wording, 3 cosmetic.**
- The status of a part on a day is right everywhere it was measured: map colour, bait/fly marks, year strip, banner and
  page status agree on every keyed section of the province (0 disagreements). The numbers the Keep tab prints equal the
  release's answers on 124,578 pages (0 differences). The salmon card prints the release's verdict on 308,805 cards (0).
- What is wrong is **what the screens leave out**: whole kinds of rule the release holds and no screen draws (boat
  angling bans, rules held "in part", a water's own possession limit, the chinook annual limit, Region 8's brook
  trout), and a short line that says "keep" over a fish that must be released.
- Earlier audits are not repeated (T608A F1–F21, T608B G1–G28, T668 straddlers, T369). Where a finding touches one, it
  says so.

| severity | count | ids |
|---|---|---|
| wrong regulation shown | 8 | K1 K2 G2 G6 D1 D2 W1 W2 |
| regulation missing | 9 | K3 K4 K5 G1 G3 G4 G5 G7 D3 |
| misleading wording | 10 | K6 K7 K8 G8 G9 G10 D4 D5 D6 W3 |
| cosmetic | 3 | K9 G11 W4 |

## Method

- The app's own readers and presenters were run over the live release in node (`loadRegsInputs` → `buildRegsPage`,
  every sheet; `salmonCard` / `salmonTable`; the status index and method file readers; `joinInSeason` /
  `inSeasonCover`). Nothing was re-derived. "Page" below is the view model the Regulations screen draws.
- Four reading lanes (Keep tab; Gear / Methods / boats / Licence; salmon + in-season; map, search, parts, day picker,
  web pages), each with a province-wide mechanical pass and a deliberate sample against the printed book (PDF text
  per page) or DFO's pages (fetched 2026-11-11; their dates equal the release's `asof`).
- Every finding marked **re-run** below was run again by the lead after the lanes reported, through a separately fixed
  harness, and its book line re-read. The others are the lane's own proof (scripts in the session scratch area; they
  are not in the repo).
- A harness fault was caught and fixed during the audit (weekday passed as "wed", the app uses "Wednesday"); every
  mechanical pass quoted here ran on the corrected form.

## Findings — wrong regulation shown

| # | Water, part, day | Release holds | Screen says | Source | Where; how many |
|---|---|---|---|---|---|
| **K1** re-run | Charlie Lake `wbk:329264876`, whole water, Jul 15 | `r7:charlie_lake@7-33::charlie_lake.r1p` "Walleye — 3 in possession" (and `.r2p` pike); the fish's role is `possession_cap` | `keep.possession.says`: "Possession: 2 days’ quota. … Possession: the daily quota for Arctic grayling … Dolly Varden or bull trout … lake trout." No walleye, no pike: reads as 6 in possession. The cap is only inside the walleye sheet | p.65: "CHARLIE LAKE 7-33 Walleye daily and possession quotas = 3; northern pike daily and possession quotas = 3" | `core/src/regs/standing.ts:87` keeps role `possession`, drops `possession_cap`. **51 caps on 31 waters** (lower bound: own rows, Jul 15), e.g. Bowron Lake Park waters "Trout/char daily and possession quotas = 1" (p.43) on the Cariboo River, Teslin, Tagish, Bennett |
| **K2** re-run | Peace River `gnis:14619`, part "From Hwy. 29 bridge to the Site C dam", any day | `fish.MW` = release, by `peace_river.r3` "Mountain whitefish catch and release, including tributaries" | Other fish list: "Whitefish · 15 a day", tone keep. "Release every mountain whitefish." is one tap down, in the sheet | p.66: "Arctic grayling catch and release, including tributaries; Mountain whitefish catch and release, including tributaries" | `core/src/regs/others.ts:56` words the row from its lead member (lake whitefish) only. **91 parts, about 86 waters** (Peace tributaries, Halfway, Moberly) |
| **G2** | Cowichan River `gnis:12227`, "Mouth → CNR Mile 66 Trestle", Jul 15 (game fish closed, gear tab drawn for salmon) | frame `bait_ban: true` by `z1:bait_ban_streams.r1` "Bait ban: applies to all streams of Region 1, all year" | Bait heading: "No natural bait Jan 1–Jul 3, Nov 16–Dec 31 · not today" above a list that is all ✕ | p.13 Region 1 bait ban, all streams, all year | `core/src/regs/bait.ts:66` `datedBans` counts open days only; `gearIfFished` draws it on a closed day. **365 rule-set keys; 11,634 part-days of 12 sampled days.** Seen only where the gear tab is drawn on a closed day (salmon open); that subset was not counted |
| **G6** | Pend d'Oreille River `gnis:4927`, part 0 | `pend_doreille_river.r1` "EXEMPT from single barbless hooks" lifts only `z4:single_barbless_hook.r1` | Hooks: "Single barbless hook, no trebles" (the provincial stream rule still decides) | p.39 row: "EXEMPT from single barbless hooks" | Release (lift target), not the app. 1 water. Owner reading: no ruling covers it |
| **D1** | Tatshenshini River `gnis:25410` and tributaries (chain 143), Jul 24–Aug 14 | coho verdict "2 per day (Apr 1–Mar 31)" every day | Aug 1: "Open today", "Coho: Open (2 a day)", beside "Pink · chum: No fishing for salmon (Jul 24–Aug 14)" | DFO Region 6: "Tatshenshini River (downstream of the BC/Yukon border) / All / Jul 24 to Aug 14 / No fishing for salmon" (page re-read) | `pipeline/deliver/salmon.py:189` a species' own row beats an "All" row even when that row is a closure. **2 places, 6,254 sections**; also Koksilah River (undrawn, caveat only) |
| **D2** | Courtenay River `gnis:12206`, Puntledge River `gnis:27060` parts 0/2/4, Jun 1 | the "To be determined" row is typed with `species: null` | All five species "Check DFO — To be determined" | DFO lists "Chum / To be determined" only; unlisted species are closed | Same class as T608A F3, **new waters**: all 8 TBD rows are typed without a species. With real dates it goes the other way: Kamloops Lake `wbk:329563838` sockeye reads "Closed" where DFO prints "Sockeye / Apr 1 to Mar 31 / To be determined" |
| **W1** | Fraser River `gnis:39325`, part 0 (tidal, below Mission); 81 named waters' tidal parts | status index `tidal`; section key 0; part has no key | Page status "outside"; "This part of the water is outside B.C.: no B.C. rules apply." (re-run: the part has no key) | It is tidal water in B.C. under DFO's tidal rules; the app has tidal words (T166) and does not use them here | `data/src/regs/inputs.ts:76-79,101-103`, `core/src/regs/edge.ts:28`. **285 sections** (164 named, 121 unnamed). That a map tap reaches this page is inferred, not driven |
| **W2** | McLennan Creek `gnis:26380`, Cougar Canyon Creek `gnis:8664` (all year); Nanaimo River `gnis:16505` Jul 15 | every freshwater part closed | Search row dot: "Has its own regulations" (amber); the map and the page say closed | — | `pipeline/deliver/status_index.py:211-222` the roll-up drops outside parts but not tidal ones. **14 waters, 1,846 water-days** |

## Findings — regulation missing

| # | Water, part, day | Release holds | Screen says | Source | Where; how many |
|---|---|---|---|---|---|
| **G1** re-run | Kispiox River `gnis:2308`, part 0, Aug 15; Thompson River below Kamloops Lake `gnis:39492` | `r3:thompson_river…r6` "No angling (from a boat)", `gear [{ban:[angling], when:{angler:in_boat}}]`; held only in `frames.gear.circumstantial` | Kispiox: boats line and Methods say only "No powered boats". Thompson: no boats line. No "from a boat" words on any tab or sheet | p.51: "KISPIOX RIVER … No angling from boats, no powered boats"; p.32 Thompson "No angling from boats"; also p.50 Babine, Bulkley; p.43 Atnarko / Bella Coola, Chilcotin; p.44 Chilko; p.53 Skeena | No app file reads `circumstantial`. **15 rules, 142 parts, about 110 waters** |
| **G3** | Mahood Lake `wbk:329354708`; Cowichan Lake; Okanagan Lake; 30 more | `frames.gear.in_part`: e.g. Mahood "Bait ban, Jul 1-Dec 31", "Single barbless hook, Jul 1-Dec 31" within the signs; Cowichan "overall 10 km/h … within 60 m of shore" (p.15) | Nothing: the Keep caveat carries the release rule only, never the gear or vessel rule | the rows' own lines | No app code reads `in_part`. **26 rules, 33 parts, 0 shown.** T608A listed three as "note carried, look not checked": they are not on the screen |
| **K3** re-run | Shuswap Lake `wbk:329518145`, Nov 11 | `fish.LT` = release by `z3:daily_quotas::trout_char_quota.r7` "Lake trout — release all, Oct 15–Jan 31" | Lake trout is nowhere on the Keep tab; the only lake-trout words are the stamp line "only if you keep lake trout and Dolly Varden/bull trout over 60 cm" | p.28 Region 3: "you must release … Lake trout from Oct 15-Jan 31" | Inferred cause `pipeline/deliver/answers/card.py:970` (a child row's releases never reach `not_kept`). **8 waters on 6 sampled days** (lower bound): Mara, Little Shuswap, Adams, François lakes, Little River; steelhead release on File Creek and Adams River |
| **K4** re-run | Bruer Creek `gnis:10061`, Mouth → Source, Jul 15 | `z8:daily_quotas::trout_char_quota.r5`; frame EB keep 20 | No brook trout on the tab or any sheet | p.68: "20 brook trout from streams" | `core/src/regs/keep.ts:130` a pooled row with no headline is not drawn. **1,046 parts, 1,028 waters** (every open Region 8 stream). Known (T80 3a / T240), still live |
| **K5** | Mat Creek `gnis:14254` (Duncan River tributary), Jan 15 | DV keep 2 by `duncan_river.r5` "bull trout daily quota = 2 (any size)" | Only "Dolly Varden/bull trout may be kept Nov 1–Mar 31…": no number | p.36 Duncan River | Same mechanism as K4. **14 waters**, Nov–Mar. Known (T80 3b) |
| **G4** | Qualicum River `gnis:27804` part 3; Chilcotin `gnis:13744` part 1 (Oct 15); Fraser part 8 (Oct–Dec) | `counts.hook_gap_mm` = 15 | Hooks: "Single barbless hook, no trebles" | p.18: "Single barbless hook, no hooks greater than 15 mm from point to shank"; p.43; p.30 | The pipeline's gear summary never words the slot. **3 waters** |
| **G5** | Somass River `gnis:25707` part 1, Nov 11 | licence frame present | No Licence tab on any day game fish are closed, though Gear / Methods are drawn for salmon | — | `core/src/regs/licence.ts:257`, `page.ts:64`. Every closed day with salmon open; days not counted |
| **G7** | Dean River parts 2–3, non-resident alien; Wigwam River `gnis:27703`, non-resident | licence terms 93–95, 101–105, 108–109 are in no frame | Tab shows the Classified Waters Licence and price only: no limited entry draw, no 8-day cap, no booking | p.7 classified waters terms | Release (terms never reach a frame). 10 terms |
| **D3** | Every water where chinook opens (Conuma, Vedder, Tatshenshini checked) | the bucket's `period` tiers; notes `fold: "bucket"` | "Up to 4 salmon a day in total … · 8 in possession" and nothing more | DFO headers (R1, R2, R3, 5B, 6, 8): "The total annual limit for chinook salmon over 50 cm is 10"; R8: "monthly limit of 4 chinook over 50 cm" | `core/src/salmon.ts:1232` draws the total tier only; `:791` hides the folded note. **433,290 sections** |

## Findings — misleading wording

| # | What | Proof and count |
|---|---|---|
| **D4** | Region 6 section A's "in the aggregate (combined total), in Region 6" is lost: Inhini River `gnis:10225` reads "Chinook: Open (4 a day · only 1 over 65 cm)", as if per water | Curated place `6:a:region` holds the section title, not DFO's cell; `quota.pool_of` reads only that. 30 chains, 393,985 sections. Region 5B's same wording does print as a pool |
| **D5** | An undrawn DFO place reads as a footnote: title "Fishing is different in some spots today", "Everywhere else, follow this page", under a headline "Closed today" | 98 waters carry undrawn places; 169 parts carry the caveat; on **554 part-days across 31 waters** every row is Closed while the caveat place is open (Harrison Nov 11: coho 4 hatchery, chum 2). On 2,141 part-days the caveat says "different" and is not |
| **D6** | Region 6 section D gear line cut mid-word: "Single, barbless hook in tidal and non (Apr 1–Mar 31)" (Ain River `gnis:32409`) | DFO: "…in tidal and non-tidal portions of all streams". Typed verbatim is cut. 6 chains, 25,787 sections; shows on lakes too |
| **K6** | Skeena River `gnis:2936`, Mon Jul 20, parts 10–14: "…on Saturdays and Sundays in two zones:" ends at the colon; part 7 never says who is closed | `core/src/regs/caveats.ts` `describeShort` cuts at the first " (". 2 rules, 6 parts (p.53). Saturday's block is whole |
| **K7** | "Open, except some parts" when no part is closed (Shuswap Lake Jul 15: only a catch-and-release holds) | `core/src/regs/strip.ts:148` counts any in-force rule of an undrawn part. 37 page-days, 7 waters. Errs cautious |
| **K8** | Shuswap Lake's Salmon Arm Bay closure note drops "(anglers fishing from the community pier … are exempt)" (p.32) | lift `shuswap_lake.r5x` has no verdict on an undrawn part. 1 water. Errs closed |
| **G8** | Non-resident-alien weekend closures (Skeena, Kispiox, Zymoetz, Babine, Stellako; p.53–54) are not on the Licence tab even with "From outside Canada" picked | They are a Keep caveat only |
| **G9** | Artificial-fly-only waters say Hooks "One hook or fly" (Pennask Lake, p.31) | p.80: an artificial fly is a dressed hook. 66 parts |
| **G10** | Under-16 non-resident "for your own limit" line prices three documents at the basic licence's price | `core/src/regs/licence.ts:178`. 198 keys, 941 parts |
| **W3** | Web hubs "Fly fishing only / Bait ban waters in Region N" list a fraction: Region 2 fly-only lists 4 of 49 waters (Eaton Creek's "Fly fishing only, Jul 1-Oct 31" is absent); "Fishing closures in Region 2" lists all 2,030 | `tools/seo/topics.ts` matches the build day's sentences. R1 8 of 76, R5 7 of 13 |

## Cosmetic

- **K9** Region 8 streams: "Up to 4 brown, cutthroat or lake trout of the 5 from streams" (book: 5, not more than 4 from streams). Not counted.
- **G11** Two identical "Classified (Class II)" boxes on one part (Elk tributaries, Stellako): 59 parts.
- **W4** A water page's description names one region for a water in several ("Fraser River (near Quesnel, Region 2)"): 59 waters.

Smaller drops on the Gear / Methods tabs, each verified, none changing an answer: ice fishing never listed on streams
(Little River's "No Ice Fishing" not shown); the 1 kg roe possession limit (p.8); "dead fin fish may be used" for
sturgeon and set lines; the boats chip drops "max 7.5 kW" (the Methods list keeps it); the set-line note repeats a line.

One release-internal note, no screen effect today: on the Bella Coola / Atnarko (38 parts) `fish.CT.lead` says keep 1
while the tree, correctly, says no cutthroat may be kept (`atnarko_bella_coola_rivers.r4`, p.43).

## One line each: the fix

K1 read `possession_cap` into the Possession row · K2 word the row from every member, worst first · K3 carry a child
row's releases into `not_kept` · K4/K5 draw a pooled row with no headline · K6 cut only a trailing bracket · K7 count
closures only · K8 give the undrawn lift a verdict · G1 draw `circumstantial` boat bans as a chip and a Methods line ·
G2 date the bait heading from the rule, not from open days · G3 draw `in_part` gear and vessel rules in the caveat ·
G4 word `hook_gap_mm` · G5 keep the Licence tab when gear is drawn · G6 owner reading, then the lift target · G7 owner
call: which terms the tab owes · G8 show the caveat on the Licence tab for that profile · G9 "one fly" · G10 price the
bundle · D1 an "All" closure beats a species row · D2 type the TBD row's species · D3 draw the bucket's period tiers ·
D4 read DFO's cell for the pool · D5 name it "not mapped yet" and lead with it when it is looser than the page · D6
re-type the line · W1 tidal words for a no-key tidal part · W2 drop tidal parts from the roll-up · W3 pick hub waters
from the year's rules, not the day's words · W4 name every region.

## Checked and clean

- **Map vs page:** all 1.96M section handles (2,441 keys), 14 days incl. three weekend pairs: closed ⇔ closed, own/base
  ⇔ open, tidal ⇔ tidal, 0 disagreements. Bait and fly marks vs the strip, every section, 366 days: 0. Own/base
  legend: 0.
- **Keep tab:** 20,763 keyed parts × 6 days: 0 number differences, 0 contradictory statuses, 0 strip or status
  mismatches; 15,712 closed page-days: banner present, reopen day right. 77 parts' undrawn retention and access rules
  are all carried with their dates.
- **Gear:** 12,732 pages: 0 verdicts printed the other way, 0 count mismatches (the barbless inversion and the lost
  "single" do not recur), 0 designations without a box. Dated bait bans replace Region 1's on the right days.
- **Salmon card:** 20,587 parts × 15 days: 0 status, quota, species or source differences; 2,463 DFO rows of drawn
  places match except D2. Date edges, Feb 29, jack sizes, hours lines right.
- **In-season:** 32 notices, 441 notice × water pairs, 0 outside the notice's region (T608A F7 is fixed in the current
  links); the day picker drops expired ones; words are the province's.
- **Parts and day picker:** all 239 merged groups share one key; `closed_all_year` agrees on 365 days for every part;
  the picker stays in the current year; Feb 29 and the B.C. midnight are right.
- **Web pages:** 41 water pages on staging: the status word equals the app's for the part shown; descriptions and
  JSON-LD carry no limits or seasons. Gauge pages state no regulation.
- **Against the book, row by row:** Thompson (closure union and edges), Nicola, Vedder, Kootenay Lake arms, Elk,
  Quesnel and Shuswap lakes, Kettle, Similkameen, Okanagan Lake, Charlie Lake, Skeena, Campbell, Babine, Stellako,
  Chilko, Dean, Cowichan, Somass, Stamp and others (about 45 waters across the lanes), fees p.5.

## Not covered

- No device, emulator or browser was driven: every "screen" is the view model the screen draws, plus staging HTML for
  the web pages. Layout, truncation on a phone, and whether a tap reaches W1's page are unseen.
- The live DFO feed overlay (`withSalmonFeed`) and the salmon card on tidal or no-key parts.
- Dated releases on days other than the six sampled (K3 is a lower bound); `multi:` / `share:` / species sheets' text;
  worked examples beyond their numbers.
- Whether salmon is open on the closed days of G2 and G5 (how often an angler meets them).
- Region 7 topic hubs and the hubs capped at 600 rows.
- Positions on the ground (signs, bridges, cut placement): T608A's open items stand.
- Waters on regional rules only were checked mechanically, not row by row against the book.
- The lead re-ran K1–K4, G1, W1 and re-read D1's DFO rows; the other findings rest on their lane's proof.
