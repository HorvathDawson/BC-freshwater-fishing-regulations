# Gotchas — things that look like a bug, a change, or a duplicate, and are not

Short entries, newest first. One per trap that has already cost someone an hour. A gotcha earns a
place here when the *evidence* is misleading: the data says one thing and means another. Things the
code now handles belong in the code's comments, not here.

---

## An "All" closure against a species row on the same DFO place (the Tatshenshini, the Cowichan)

**2026-11-12 · owner ruling T671 D1 · DFO salmon · `pipeline/deliver/salmon.py` `_all_closure_wins`, RULINGS L24**

DFO's table puts an `All` row and a species row on one place with no words saying which wins, and the two shapes
look alike but read opposite ways:

- Tatshenshini below the border: `All | Jul 24 to Aug 14 | No fishing for salmon` beside `Coho | Apr 1 to Mar 31 |
  2 per day`. The coho row is NOT the exception — it is DFO's all-year default written loosely (above the border the
  same closure sits beside `Coho | Aug 15 to Jul 23`, dated around it). The closure wins: coho closed Jul 24-Aug 14.
- Cowichan, 66 Mile Trestle to the Sandy Pool: `All | Sep 1 to July 14 | Open for salmon catch and release` beside
  `Coho | Nov 1 to Dec 31 | 1 per day`. The coho row IS the exception: dated inside the All row, it speaks.

The rule (owner): the All CLOSURE wins unless the species row is dated inside the All row's dates. Do not "fix" the
Tatshenshini's coho back to open because its own row says all year, and do not let an All row that is no closure
swallow a short species opening. On the pages of 2026-11 only the Tatshenshini below the border and the Koksilah
(undrawn) are changed by it; the Region 6 B(i) places now quote the All closure on Jan 1-Jun 15 instead of the
species' own closure (closed either way).

## Kept city-layer names that are not real names (and labels that look wrong)

**2026-10-29 · owner review · city layers (Burnaby, Port Moody, Squamish) · `handoff/PLAN/audit/c1029-owner-review.md`**

Names are shown **as sourced** (owner rule). City layers keep any feature that carries a name, so some kept
"names" are asset codes or design designations, and some labels land on the wrong FWA line. This is a
known-issue note, not a filter: do not "fix" these by dropping or renaming them.

- **Designations, not names:** 154 Burnaby "X Trib.N" (e.g. "Stoney Trib.3-1-1-2-2-1"); 17 Port Moody and
  16 Squamish "Trib" names.
- **Codes as names (Burnaby):** Slvt2; Jr 1-1, Jr 4-1, Jr 4-1-1, Jr 4-1-2, JR Trib. 4; Crabtown Creek T1.
- **Not streams / odd spelling (Squamish):** Mamquam Blind Channel Wetland (a wetland); Logger's Lane
  Connector and Loggers Lane Creek (two spellings, one a "Connector"); Mashiter Side Channel.
- **Labels that look wrong on an FWA line:** Evans Creek on the Squamish River; Dragonfly Creek on Still
  Creek; Lost Trib.1 on Austin Creek; Stoney Trib.4 on Stoney Creek.
- **Several names on one unnamed FWA line:** Thunderbird + Newport on `360305166`; Beaver Creek + Beaver
  Creek BioPond North + Deer Lake Brook on `356297118`; Beecher + Sunken Engine on `356093488`.
- **Already there before:** the Seymour River item holds the city's Tunnel Creek (`-2000000390`).

Seeing one of these on the map is the data as sourced, not a regression.

---

## Book errata: an MU printed wrong (Mackenzie Creek "4-21" is 4-31)

**2026-10-26 · owner ruling · Region 4, `r4:mackenzie_creek@4-21` · `data/curated/regulations/book_name_links.json`**

The synopsis prints MACKENZIE CREEK ("No Fishing", printed p.38) under **MU 4-21**. There is no Mackenzie
Creek in 4-21; the book means the one in **MU 4-31** (`gnis:12042`). It reads as a water the registry is
missing — it is a typo in the book.

The fix is an ordinary override keyed on the row AS PRINTED (`name_verbatim` + the printed MU 4-21)
naming the right item; the entry id keeps the printed MU (`@4-21`) because it is minted from the row,
not the item. Do not "correct" the id or the MU in the corpus.

Known book errata (add new ones here): **Mackenzie Creek MU 4-21 → 4-31.** The Region 5 placeholders
(Redfern, Secret, Frog, Hidden, Square, Chipmunk lakes) are `not_found` overrides whose notes say "may
be a typo" — unconfirmed, not errata.

---

## A water's undated hatchery-steelhead quota does not lift the zone's Nov-Jun release (the Kitimat)

**2026-10-21 · owner ruling · Region 6, `kitimat_river.r5` vs `z6 trout_char_quota.r7` · test `test_answers_kitimat_steelhead_lift.py`**

The Kitimat row prints "Hatchery steelhead (adipose clipped, >50 cm) daily quota = 2" with **no dates**, right under
"Hatchery rainbow trout … daily quota = 5, … **all year**" (printed p.51). Region 6 prints "And you must release: …
Trout of any size from streams, Nov 1-June 30" (printed p.49). It reads as if the row's quota is a year-round exception
(and the Kitimat hatchery steelhead run is a winter one), so the curated `exempts` on r5 named r7 and the app let you
keep 2 in Nov-June. **The owner ruled it is NOT lifted:** hatchery steelhead on the Kitimat are catch-and-release
Nov 1-June 30, and the quota of 2 applies outside that window. Only "all year" lifts a dated zone release; an undated
row quota does not. Separately (same day), keeping hatchery steelhead counts toward the 1-over-50 cm trout limit — a
"+1 for steelhead", as on the Vedder — so it does not free up another trout over 50 cm.

## A row's closed season does not shorten the zone's (the Stein, the Nahatlatch)

**2026-10-05 · zone closures, all regions · `pipeline/deliver/bundle/rules/lifts.py`, export `gotchas.closures_combine`**

The Stein's row prints "**No Fishing** Jan 1-May 31"; Region 3 prints "No Fishing in any stream in
Region 3 from Jan 1-June 30". It reads as if the row's dates replace the zone's, so the Stein
opens June 1. They do not: **both hold, closed on the union** (Jan 1-June 30). The p.28 line that
lists "Nahatlatch River downstream of Nahatlatch Lake and Stein River: from Jan 1-May 31" looks
like an exception list and is not one — it sits under *Steelhead Management Changes*, a list of
steelhead closures (the Thompson heads it). Two lifts built on it were removed.

A row beats a zone closure only where the book PRINTS an exemption ("exempt from spring
closure", or the zone page names the water), or prints a dated catch and release, opening or
quota INSIDE the closure — then on exactly those dates, for exactly those fish (the Nicola below
its lake: trout catch and release Jan 1-Feb 28; whitefish stay closed). Every entry where the
two overlap is listed, generated, in the export's `guide.gotchas.closures_combine` — and the
READER decides what each note claims: "both hold" only on the days and for the fish the zone
closure actually speaks outside the row's dates (`zone_holds`), with any lift in part named
(`zone_lifted`: the Nicola's trout, the Thompson's May opening on 2 of its 20 sections). The
Fulton's "Open June 16-Apr 30" closes it May 1-June 15 and lifts the Skeena winter closure on
every other day, so it is no such case; read off printed dates alone it looked like one. Before
adding a lift, find the printed words; a row's own season is never them.

---

## A dated bait ban DOES replace the zone's all-year one (Quatse, Somass, Sproat, Stamp)

**2026-10-05 · Region 1 bait ban · export `gotchas.dated_bait_ban_replaces_zone`**

The opposite of the entry above, and easy to "fix" by analogy. Region 1 prints "Bait ban: applies
to all streams of Region 1, all year, with some important exceptions. Check the tables." The
Quatse prints "Bait ban, May 1-Nov 30". Here the row's dates DO replace the zone's: bait is
allowed on the Quatse Dec 1-Apr 30 (`quatse_river.r4x` lifts the zone ban on those dates). A
closure never works this way; a bait ban with "check the tables" does. As of 2026-10-05 only
Region 1 prints this shape (the export's gotcha says where, read off the rules, not a fixed
sentence): Regions 6, 7A and 7B ban bait all year on streams too, but no row there prints a
dated ban on the same water (the one Region 5 Fraser section under Zone 7A's ban is lifted by the
Region 7 Fraser row's "EXEMPT from bait ban").

---

## The same closure appears twice, under two sign counts (Skeena @ Kispiox)

**2026-09-21 · DFO salmon, Region 6 · `pipeline/regs/dfo_salmon/`**

The Skeena carries **three** locators for the fishing-boundary-sign closure at the Kispiox
confluence:

- `6:skeena-river:all-waters-within-the-4-triangular` — *"all waters within the **4** triangular
  fishing boundary signs located at the confluence of the Kispiox River with the Skeena River"*
- `6:skeena-river:mainstem-waters-within-3-white-tri` — *"mainstem waters within **3** white
  triangular fishing boundary signs located at the confluence of the Kispiox River…"*
- `6:skeena-river:mainstem-waters-within-three-white` — *"mainstem waters within **three** white
  triangular fishing boundary signs located at the confluence with the Kispiox River"*

The duplicate grouper caught the numeral one (score 0.934) and missed the spelled-out one — the
gap between `3` and `three` is wider than its threshold. Both are now
`duplicate_confirmed` against the four-sign primary.

**They are one zone.** DFO re-worded it across years and every wording survives in the corpus, so
the sign count is not a distinguishing fact — do not go looking for two or three closures, and do
not bind them to different water. All three bind to the same pair of cuts
(`skeena_river__kispiox_sign_zone_lower` / `_upper`).

The sign count is the trap: a wording can change 4 -> 3 -> three without a single fish being
affected, and each change mints a locator that looks new.

Why it reads as several: the locator id is a fingerprint of `section|water|specific_area`, so re-worded
scope text mints a *new* locator rather than updating the old one. That is the design — the old one
goes dormant and revives if the wording returns — but it means **wording drift looks exactly like a
new regulation**. `whatchanged` (archived under `archive/pipeline/regs/dfo_salmon/`) diffs rules on structure for this reason; locators have no such
defence, and a curator is the only thing that spots it.

Check before assuming two: same water, same section, same rules, landmark in the same place. If all
four match, it is one zone — record it with `duplicate_of` + `duplicate_confirmed`, not by binding
the copies to different geometry.

> The 400 m either side of the confluence is an **assumption**, not a survey. DFO gives no distance;
> 400 m is the radius it uses for the comparable Region 6 mouth closures (Babine Lake, Pinkut Creek).
> `spatial_caveat` stays set until someone pins the real signs.

---

## A locator that matches everything but a trailing period

**2026-09-21 · DFO salmon · `pipeline/regs/dfo_salmon/`**

Four of the ten live Skeena locators matched their stored binding only after normalising a trailing
`.`. The page's scope text gains and loses sentence-ending punctuation between scrapes, and because
the fingerprint is taken over the raw text, one character retires a locator and mints its twin.

Same family as the entry above, smaller blast radius: it shows up as a locator that has "vanished"
alongside an identical one that has "appeared", on the same water, in the same run.

---

## Sheldens Creek is a name we assumed

**2026-10-29 · names / DFO salmon · `data/curated/waters/found_name_calls.json` (was `names.OWNER`, T327), `handoff/PLAN/curation-review/name-variants/mine.py` (`DFO_UNMATCHED_LEADS`, a one-off miner; T437 repointed)**

DFO's Region 6 rules name "Sheldens Creek" (Moresby Island, Haida Gwaii), but no Freshwater Atlas water carries that
name. We named the unnamed FWA line blk 360846083 (code 950-016367) into Sheldens Bay "Sheldens Creek" because the
forest roads around it are named for it ("SP 19(1) TO SHELDONS", "SP 19H TO SHELDONS", SHEL100–SHEL103). The owner
accepted it on 2026-10-29 knowing it is **low confidence and an assumption** — no map, gazetteer or DFO source names
this line. The DFO salmon rule and its cut (Spur 19 road crossing, 53.12968,-131.78224) bind to it through that name.

If a source ever names a different stream "Sheldens Creek" (BC Geographical Names, a DFO map, a local), or DFO's
wording points elsewhere, this is the first thing to recheck: drop its `found_name_calls.json` row (and rerun `pipeline.tools.mined_names`) and the rule unbinds on the next
build. Don't treat it as a confirmed name in audits or when matching other sources.
