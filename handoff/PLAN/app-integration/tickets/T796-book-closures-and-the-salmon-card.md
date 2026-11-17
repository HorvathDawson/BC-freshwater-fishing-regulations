# T796 — A stretch the book closes still shows an open salmon card; a DFO "Finfish closure" closes only the salmon card

Status: OPEN (owner: OWNER-TODO "2026-11-16 morning list" questions 29 and 30; app core salmon card + pipeline).

Source: general audit 2026-11-16, answers checker A5 and A6 (`handoff/PLAN/audit-2026-11-16/answers.md`).

## A5. Closed stretch, open salmon card (errs OPEN)
Rule book p.77: "when fresh waters are closed to fishing, or have gear restrictions outlined in this Synopsis, the
regulations apply to fishing for salmon as well". Seen: Skeena parts 2 and 4 (ecological reserve) on Aug 15, "Closed
all year" beside "Open today" for sockeye; Atnarko parts 2 to 4 on Jul 15 (chinook); Cowichan below Mile 66 on Oct 10
(closed beside "Release only today"). The page's gear rules reach the salmon card (`gearProvincial`); its closures do
not. Read from the app's view model, not seen on a device.
Recommended: the salmon card reads closed when the page is closed by a full closure, with DFO's row kept as text.

## A6. DFO "Finfish closure" (errs OPEN for trout)
Ten DFO rows (Stamp 4, Nitinat 3, Somass, Sproat, Tahltan) say "Finfish closure"; the app closes salmon only. The
Stamp hatchery-lagoon stretch on Oct 10 shows open with a trout card. `typed.py` reads the row as a salmon closure.
Recommended: a finfish closure closes the whole page, trout included, on its dates.

## Owner, 2026-11-16 (question dialog; verbatim in `handoff/PLAN/OWNER-TODO.md`, "Answers by batch")
Question 29, typed: "yes we want to do this but can you bring the example changes to me so i can see why it happened etc? but yes we want it to do this". Question 30: "Closes the whole page (Recommended)". Split agreed with the app chat (11:55): A5 = pipeline adds the book's full closure as the first member of the stretch's salmon chains (a release-side location marked static), the app keeps a static location under a live feed and words the card; A6 = pipeline `typed.py` + deliver + vectors first, then the worker's twin, then core closes the page from the feed. The before / after examples (Skeena reserve sockeye Aug 15, Atnarko chinook Jul 15, Cowichan below the trestle Oct 10) go to him with the why.

## Lane result A6 (lane/salmon-rules-1116, 2026-11-16)
- `typed.typed_rules` (the one way a row is typed: `feed.type_rows` and the vectors' hand rows): a row whose limits
  cell (less a part-closure sentence) says "Finfish closure" keeps its salmon closure (retention_limit, take 0,
  may_target false, species SA) and that rule carries `finfish: true`. `deliver.salmon.verdict_on`: a closed verdict
  answered by such a rule carries `f: 1`. `typed.as_catalogue` strips the DFO marks (`hidden`, `finfish`) where a
  reader validates against `CatalogueRule` (splitwork, the review detail).
- THE FIELD FOR THE APP CHAT: `f: 1` on a verdict object (payload `verdicts[i]`, and the live feed's `verdicts`),
  present only on `s: "closed"` verdicts from a finfish closure; absent otherwise. Meaning: on that day ALL fishing is
  closed on the chain's stretch, trout included. Read it from the same per-species verdict runs the salmon card reads:
  the page is closed on a day when any species' verdict of the page's drawn chain carries `f`.
- Worker twin: `typed.ts` adds `finfish: true` on the retention_limit closure (may_target false, no `within`) of a
  row matching /\bfinfish\s+closure\b/i outside a part-closure sentence; `salmon.ts` `verdictOn` adds `f: 1` when the
  verdict is closed and a speaking rule has `finfish`.
- Vectors moved: pages 1 (9 locators) and 6 (1 locator): `rules[0].finfish: true`, `calendar.verdicts[0].f: 1`;
  `live.verdicts` 31, 37, 38, 50, 51, 56, 57, 58, 218 gain `f: 1`. Nothing else moved.
- Release (salmon step rebuilt): 11 chains, 2,638 sections gain `f` on the closure's days, no other verdict changes:
  Tahltan including tributaries (2,595 sections, Jun 1-Aug 31), Stamp Ash River to the Great Central Lake dam (33,
  Jan 1-Apr 30), Stamp above Girl Guide Falls (2, Jan 1-Apr 30), Stamp hatchery lagoon (1, Jan 1-Apr 30 and
  Aug 25-Oct 31), Stamp powerline to the lagoon (1, Jan 1-Apr 30 and Oct 1-14), Stamp Falls (1, Jun 15-Nov 15),
  Somass below the gravel pit signs (1, Aug 25-Nov 15), Sproat below the lake (1, Jun 15-Nov 15), Nitinat Red Rock
  Pool (1, Aug 25-Dec 31), hatchery intake to the Little Nitinat (1, all year), bridge (1, Aug 25-Oct 15).
- Tests: `pipeline/tests/test_t796_finfish_closure.py` (3; mutations: no `f` fails 2, no `finfish` fails 3).

## A5 proposal, written before the code (lane/salmon-rules-1116, 2026-11-16)
THE MARKER (the `piece` / `bound` pattern: a flag on the place's entry in payload `p`, read without parsing the id):
- A book closure is a salmon place of its own, id `book:<entry_id>::<rule_id>` (a `~2`, `~3` suffix when the same rule
  closes different days on different sections, e.g. lifted on some), put FIRST in the chain of every section it
  closes. Its entry in `p`: `{"r": <region slug, absent for a provincial row>, "water": <the book row's name>,
  "static": 1, "book": {"rule": "<entry_id>::<rule_id>", "pages": [<printed pages>]}}`. `static: 1` = the release's own
  place: DFO's feed never carries it, so under a live feed the app keeps it from the release file.
- Its year, for the app to compose under a live feed: payload `static`: `{<id>: {"sp": [5 runs of release verdict
  indexes, -1 = says nothing that day], "g": [0, -1]}}` (the live feed's place shape, `live.location_calendar`).
- Its verdict: `{"s": "closed", "t": "Closed to all fishing by the rule book (p.N): <the book's line> (<dates>)"}`
  on every salmon species on the days it closes; nothing on other days, where the rest of the chain answers as before.
  The chain's `a` names it (index 0) on those days; `s` groups each gain it as their first member.
WHICH CLOSURES: exactly the status index's CLOSED test, taken from the stored verdicts (`verdicts.project.closing`), and
only a rule that BY ITSELF closes every game fish: a full closure ("No Fishing", a park or ecological reserve closure,
`closure_grade` full: take 0, may not fish, no length / origin / while / target / side condition, drawn), in force that
day after every lift the reference reader applies, and at every moment of the day (an hours or weekday closure never
closes the day). Never a gear rule, a species closure (steelhead), an angler closure or a mix of species closures.
Only on sections DFO lists (a section in no DFO chain is closed to salmon already); never tidal or outside B.C.
WHERE: release side (`deliver.salmon.book_closures` + `with_book_closures`, called by the release build before it
encodes the salmon files), because the stored verdicts are made after the bundle.

## Lane result A5 (lane/salmon-rules-1116, 2026-11-16)
- Built as proposed: `deliver.salmon.book_closures` (the stored verdicts: per section, each rule that by itself
  closes every game fish, and its days), `book_inputs` (the release's listed sections, tidal / outside B.C. left out,
  each rule's words and pages from the bundle), `with_book_closures` (the book places first; `p[id]` with `static: 1`
  and `book: {rule, pages}`; payload `static[id]` = `{sp, g}`), called in `release.build` before the salmon files are
  encoded. RULINGS D9.
- ONE CHANGE TO THE PROPOSAL, for size: `BOOK_ONLY_WHERE_DFO_LETS_FISH`. Carried on every DFO-listed section the book
  closes (620,930 sections, 543 rules) the salmon head is 92 kB gzipped against its 60 kB gate (736 new chains, 547
  places). Carried only on chains where DFO lets some salmon be fished on some day: 50 kB (139 new chains, 106 places,
  76,924 sections). Elsewhere DFO closes every salmon every day, so the answer is closed either way and only the words
  differ. The cost: a chain DFO closes all year in the release and opens in season through the live feed carries no
  book place under that feed. Raising the gate (owner) and setting the constant False carries it everywhere.
- What changes (salmon step of r-46e4355a7277 rebuilt with fixes 1-4 under it): 70 stretches (book rule x DFO
  chain), 71,046 sections, 55 book rules; on those days DFO had said open (16,932 species-days summed over stretches),
  check (1,905) or release (408). The full list: the lane's scratch file `t796-a5-changes.txt` (path in the lane
  report). Largest: the Skeena/Nass winter closure on DFO's Nass section C (23,338 sections, coho Jan 1-Jun 15), the
  Iskut closure on the Stikine (11,870, coho Apr 1-Jun 30), the Klinaklini (7,672, chinook and coho Apr 1-Jun 30), the
  Iskut below Forest Kerr Canyon (4,322), the national park reserves on Haida Gwaii (2,795, coho all year), the
  Kitimat tributaries Mar 16-May 31 (2,594), the Atnarko and Bella Coola closures (1,757 + 1,718 + 149), ecological
  reserves (section E 1,032, Stikine 1,024, section D 342, Areas 3-6 77, Skeena below the CNR bridge 7).
- The three examples for the owner (each [ran] on the rebuilt salmon step):
  1. Skeena below the CNR bridge at Terrace, inside an ecological reserve (7 sections, e.g.
     `360216952:0`), Aug 15, sockeye. Book p.9: "Fishing is prohibited in Ecological Reserves in B.C." DFO's row
     (Skeena mainstem waters downstream of CNR Railway Bridge): "4 per day (Jul 29-Aug 16), FN0787". Before: open, 4 a
     day. After: closed, "Closed to all fishing by the rule book (p.9): Fishing is prohibited in Ecological Reserves in
     B.C." The page said "Closed all year" and the salmon card "Open today"; now both say closed.
  2. Atnarko River from Tenas Lake to the signs near the Atnarko Park campsite (1,718 sections with the tributaries the
     row includes), Jul 15, chinook. Book p.43: "No Fishing from Tenas Lake to fishing boundary signs near Atnarko Park
     campsite" (all year). DFO's row (Atnarko River, whole): "4 per day, only 1 over 65 cm (Jan 1-Jul 22)". Before:
     open. After: closed by the book; chum and pink (DFO 2 and 1 a day all year) closed all year there too.
  3. Cowichan below the CNR Mile 66 Trestle (2 sections, `354088123:0`, `354155148:20036`), Oct 10, coho. Book p.15:
     "No Fishing downstream of the CNR Mile 66 Trestle between Sept 1-Nov 15". DFO's row (66 Mile Trestle downstream
     to Sandy Pool): "All | Sep 1 to July 14 | Open for salmon catch and release". Before: release only. After: closed
     by the book Sep 1-Nov 15 (and Jul 4-14 by "No Fishing July 4-July 14"); from Nov 16 DFO's answer is back.
- THE NOTE FOR THE APP CHAT: payload `p[id].static: 1` (+ `book: {rule: "<entry_id>::<rule_id>", pages: [n]}`, `r`,
  `water`); payload `static: {id: {sp: [5 runs, release verdict indexes, -1 = nothing], g: [0, -1]}}`. Under a live
  feed, `composeChain` must take a static place's year from `static` (its verdicts copied from the release's
  `verdicts` into the composed tables), never drop it for missing from the feed; a two-region chain's `s` already
  lists the book places in every group. The card can name the row from `p[id].book` (the book's page) instead of a
  DFO region / section. The contract line in `release/contract.py` (`pack.generated.ts`) does not yet mention
  `static` or the verdict's `f` (left for the app chat's regeneration, so this lane did not touch `app/`).
- Tests: `pipeline/tests/test_t796_book_closures.py` (5: first on its days, the policy, the live composition, only a
  rule closing every game fish and never a closure open at some moment, the date ranges). Mutations: the book place never
  answering fails 2; the "every moment" check alone is backed by the per-moment rule intersection (no single
  mutation of it fails a test).

## Follow-up (lane/salmon-rules-1116, 2026-11-16, owner: "Raise to 100 kB (Recommended)")
- `BOOK_ONLY_WHERE_DFO_LETS_FISH` False: the book place joins every DFO-listed chain it closes (620,930 sections,
  547 places, 1,084 chains). The ANSWERS are the same 70 stretches / 71,046 sections; everywhere else only the words
  change (DFO's closure -> the book's). `SIZE_GATES["salmon"]` 60,000 -> 100,000 (head 91,788 B gz). RULINGS O7.
- STILL OVER on the live release's numbers (deltas measured on the rebuilt salmon step, added to r-46e4355a7277's
  manifest): `salmon-6` 379,396 -> ~432,600 (gate 420,000); web `first_tap` 4,198,757 -> ~4,311,950 (gate 4,200,000,
  1,243 B of headroom today); `first_tap_low` 3,359,382 -> ~3,472,575 (gate 3,400,000). `first_tap_unnamed` holds
  (~6,131,200 of 6,200,000). Not raised here: owner decisions before the next release.
- The Salmon map paints from the chain's `m` (core `salmonChainStatus` / `salmonDayStatus`, label `s<chain>` per
  section from the shard): on the three examples it reads closed on their days; province-wide 11,113,487 section-days
  change colour (open/check/release -> closed), and on 0 section-days does `m` disagree with the chain's verdicts.

## Hard cases, the data side (lane/salmon-hardcases-1116, 2026-11-16)
The break-it review (`handoff/PLAN/curation-review/t796-book-closures-hard-cases/HARD-CASES.md`, dev ba07edad7). Measured
on r-685b700b2802's own inputs (bundle + stored verdicts), the salmon step alone.
- FIXED (words only; 0 status changes, map and `c` unchanged, head 91,788 -> 91,927 B gz): the card quotes THE BOOK'S
  LINE (`deliver.salmon.book_line`, read back from the row's own text): S2 the dates past the ✱ (Iskut ", Apr 1-Apr 30",
  Deena ", Feb 1-May 31", Stamp, Englishman), S4 the PDF line breaks joined (Quinsam r1/r2, Tahltan r1 and 17 more), X3
  a fragment from the start of its line (Buttle "Fly fishing only; except Thelwood Creek is closed all year"; the
  Iskut/Fraser zone row's "and in the Fraser River watershed …"), also r7's "No Fishing"-only rules (Chowade, Halfway,
  six lakes) given their printed place and dates. S1's brackets: a closure closing fewer days than it prints names its
  lift after the line (`book_lifts`): Thompson below Kamloops Lake "…; lifted May 1–31 by Thompson River (p.32):
  Additional opening …" (and Fulton, Nicola); a gap nothing lifts stops the build. 43 of 543 lines, 40 word groups.
  `pipeline/tests/test_t796_book_lines.py` (6).
- NOT FAULTS: S3's "0% inside" Cormier Creek 1912509 is a POND (lake:329638447, 2,757 m², a widening folded into the
  creek): 28% of its area lies in the Blue/Dease Rivers ER (the review measured the 65 m route line). Skeena 765427 (a
  672 m side channel, 10%) and Ningunsaw 765104 (26%) are E8 (a straddle is inside, 1 km rejoin). S1's June closure is
  L1 (strict lift; L20 names the Thompson). X2 is Z12 (national parks closed by default). X1 is the app's composition
  (below).
- X1, verdict APP: `c` means every day of the chain is the header's closure (`closed_by_header`, T356); the book place
  answers some days, so `c` goes — and the app's live composition recomputes it the same way (`salmonFeed.ts`
  `composeChain` -> `closedByHeader` over the composed runs, statics included), so a data-side `c` would not survive
  the feed. All 320,369 sections stay closed every day. The region-wide shape (T227) is the card's call: e.g. treat a
  chain closed every day whose non-book days are the header's as region-closed, the book line as a note.
- OWNER QUESTIONS (none changes an answer until answered; counts = salmon status changes on r-685b700b2802's inputs):
  - W1 Campbell River (R1). p.13 "Summer closure: No Fishing in any stream in Management Units 1-1 to 1-6 from July
    15-Aug 31 (see water specific regulations table for exceptions)."; p.14 lists "CAMPBELL RIVER 1-10". The 1-6/1-10
    line runs down the lower river, so its 10 lower sections are in both MUs (`mu_set` "1-10,1-6"). (1, rec.) follow the
    row's MU: the summer closure does not reach the Campbell — salmon 3 sections (pink closed->open 113 species-days,
    chinook closed->check 96, Jul 15-Aug 31), 5 words only; trout 10 sections lose Jul 15-Aug 31. The only water this
    moves (every MU-group zone rule checked). Regs rebuild (reach + bundle). (2) keep: nothing moves.
  - S1 Thompson below Kamloops Lake, June. p.28 "Spring closure: No Fishing in any stream in Region 3 from Jan 1-June 30
    (see tables for exceptions)."; p.32 "No Fishing Oct 1-May 31 … Additional opening from the CNR Bridge downstream of
    Deadman River to CNR Bridge upstream of Bonaparte River, May 1-31" (no "Exempt from spring closure"; the upstream row
    prints one). (1, rec.) keep, L1/L20: closed in June. (2) the row's dates replace the spring closure there: salmon 22
    sections chinook/sockeye closed->check Jun 1-30 (1,290 species-days), 4 words only; trout 26 sections open in June.
  - S3 partly inside a reserve or park. p.9 "Fishing is prohibited in Ecological Reserves in B.C."; Z10 "A lake that lies
    mostly outside the area is taken out by name". Of the 5,750 sections whose salmon answer an area closure decides:
    5,566 >=90% inside, 22 at 50-90%, 13 under 50% (tiles' polygons). (1, rec.) Z10 as written: take out by name the 9
    waterbodies mostly outside — Cold Fish Lake (0.1% inside Gladys Lake ER, 827 ha), Tow Hill ER ponds 19%/34%, Gwaii
    Haanas 22%/38%, Chilkoot 27%, Drizzle Lake ER 45%, Gladys ER pond 47%, Cormier Creek pond 28%: salmon 9 sections
    (coho 3,111 + chinook 1,098 species-days closed->open; Cold Fish 671), trout open there too; streams keep E8.
    `outside_items` + atlas rebuild. (2) keep: nothing moves. (3) also 50% for stream sections (overrides E8): +4
    sections (Ningunsaw 26%, Skeena side channel 10%, Gwaii Haanas 38%, Gladys 50%), 1,561 species-days.
  - X2 national parks. p.9 "Freshwater fishing is prohibited in National Parks unless opened under the National Parks
    Fishing Regulations. Where open, anglers require a National Park Fishing Permit." (Z12: closed by default.) Today
    "Closed to all fishing by the rule book (p.9): Freshwater fishing is prohibited … Regulations. (all year)" on up to
    6,934 sections; 0 salmon answers depend on it. (1, rec.) keep closed, word it as the book: "Closed by the rule book
    unless opened under the National Parks Fishing Regulations (p.9): …  Where open, anglers require a National Park
    Fishing Permit." — words only. (2) keep as now. (3) not on the salmon card: DFO's words return (0 answers today; an
    in-season DFO opening in a park would read open beside a closed page).
