# Can a place missing from the live DFO feed make a section read MORE open? (2026-11-15)

Read-only check. Nothing in the repo was changed except this file. The experiments ran in memory or in the
session scratchpad: Python `live.run(..., dry_run=True)` and the worker's `runDfo` on `MemStore`s, fed doctored
copies of `cache/dfo_salmon/raw/region1-eng.html`.

**Answer: yes.** When a narrow place that restricts fishing drops out of the feed, `composeChain` skips it and
the wider place's opening shows. This happens in the worker in two of the four ways a place can go missing: the
reword the worker does not recognise, and a row DFO really dropped. In the Python live feed it happens on every
reword. All of these are reported to the owner. None of them is shown to the angler as "not known".

## The mechanism (app)

- `app/packages/core/src/salmonFeed.ts:279`: a chain location with no `feed.locs[...]` entry is filtered out.
- `:288`: `first(..., 0)`. The first remaining place with an answer speaks. If none answers, the result is verdict 0,
  NOT_LISTED, "closed".
- The Python reference does the same: `pipeline/regs/dfo_salmon/live.py:126-131`.
- So for a chain [narrow closure, wider opening], losing only the narrow place gives the wider opening.
- The app never reads the worker's `regions[].gone` (no reader in `app/packages`).
- `held` (`salmonFeed.ts:413`) lists only `kept === "carried"`.
- An unplaced row is shown only as a caveat on its water's page (`salmon.ts:771`). The map and status still show the wider answer.

**Scale (verified).** I ran `failopen.py` (scratch) over release `r-f749475d0962` and the local Python feed
(2026-11-09). In 68 of 435 chains, removing only the narrowest place makes some species and day read looser.
Examples:

- Quatse 100 m weir closure: coho closed → open "2 per day".
- Campbell R. Maple St–cement block: coho closed → open "4 hatchery-marked" (Sep 1).
- Nitinat closures: chinook closed → open.
- Little Campbell: chinook closed → open.
- `6:f:closure` and `6:a:region`: chinook closed → open.
- `5b:kilbella-river:whole`: chinook closed → open.

## Per case. Example chain: `[1:quatse-river:100m-upstream-and-downstream-of-th, 1:quatse-river:whole, 1:default:region]`

| Case | Python feed (`live.py`) | Worker (`dfo/live.ts`, robust) | App shows | Verdict |
|---|---|---|---|---|
| (a) DFO drops the row | `reconcile` makes it `dormant` (`entries.py:819-825`). No `locs` entry (`live.py:508-511`). **No notice**: shown only in the dry-run preview "dropped". VERIFIED | Place goes into `regions[].gone`. Urgent `dfo-gone` notice (`live.ts:583-584, 588`). No `locs` entry. VERIFIED | Wider opening (coho "2 per day"). Correct if DFO really removed the row; wrong if the page was misread | **FAILS OPEN when misread** (told in the worker, silent in Python) |
| (b1) reword close enough to match (top candidate = this place) | Unplaced `drift` plus urgent notice. Place **leaves `locs`** (`live.py:219-235`). VERIFIED. Pinned as intended by `test_dfo_live.py:114-115` | `REWORDED_VERDICT` "check" `locs` entry, plus `holds` (`live.ts:570-582`). VERIFIED | Python: open. Worker: "check" | Python **FAILS OPEN**; worker reads not-known |
| (b2) reword with no candidate scoring 0.5 or more (`entries.py:766, 812`) | `new` unplaced, place dormant, no `locs` entry. VERIFIED | `new` unplaced, place in `gone`; `dfo-gone` and `dfo-new` notices. **No check.** VERIFIED (`b2_far`) | Wider opening | **FAILS OPEN** (told) |
| (b3) reword whose top candidate is a sibling place | Same as b2 | Only `candidates[0]` counts (`live.ts:572`). The sibling `~2` is still matched, so the real place goes to `gone`. VERIFIED: Campbell `b3_sibling` reads open "4 hatchery-marked" on Sep 1 instead of closed | Wider opening | **FAILS OPEN** (told) |
| (c) region page fetch fails | Last feed carried, `stale` (`live.py:467-478`). With no last feed: no `locs` at all. VERIFIED | Same (`live.ts:461-476`); read from code | Last answers, labelled stale. With no last feed: everything NOT_LISTED, closed | **FAILS CLOSED** / last known |
| (d) row joins but will not parse | Unreadable dates or limits give a "check" verdict (T676). VERIFIED. A catalogue refusal raises (`type_page` robust=False, `live.py:192`) and holds the region (`:486-489`) | Per place: last answers carried, or `ROW_HELD_VERDICT` "check" (`live.ts:204-207, 546-563`) | "check", or last known | **not known** |
| Page does not parse; structural change (more than 25% unbound or ±20% count, `entries.py:53-55, 827-832`) | Region held at the last feed | Same (`live.ts:512-522`) | Last known | FAILS CLOSED / last known. A single reword never trips this threshold |

## (2) A section in two regions

Each region's places form their own group (`salmonFeed.ts:279`). A group with no answer reads verdict 0, closed.
`strictestIx` ranks closed as the strictest (`:180, :206`), so a region that is missing entirely closes the section.

VERIFIED (`tworegion.py`): in 17 two-region chains I dropped one region's whole group 35 ways, and none read looser.
The answer is **FAILS CLOSED**. Inside one region's group, losing only the narrow place is case (1) again.

## Third ways

- **Location id renamed between release and feed, or the release is newer than the worker's entry files**
  (`priv entries/`, synced by `sync_inputs`). The release names an id the feed does not carry. The worker never
  lists that id as `gone`, because its own entries do not know it. The row is unplaced or listed under a different
  id (`feedOnly`). The wider place answers. **FAILS OPEN, silently on the map.** Inferred from code:
  `salmonFeed.ts:279, 405-411`. `check_release` (`live.py:629`, `release_only`) reports it only when run by hand.
- **Stale cached feed**: it keeps the last answers, labelled after 30 h (`staleness.ts:41, 77`). It can be looser
  than today's DFO, but it is disclosed. Last known, not a missing-place case.
- **T573 closed pieces**: handled by T757 (branches, not merged).
- **Release build** (`deliver.salmon`): a dormant place says nothing, so it has the same fall-through
  (`salmon.py:720, 858`). Inferred.

## Worst case

DFO rewords a closure stretch so that it matches no curated text, or matches a sibling place better. The worker
then lists it as `gone`, and the section paints open with the wider river's limit. The card adds only an
"unplaced row" caveat. One reword stays under the 25% structural-hold threshold.

## Smallest fix (proposal)

1. **Worker** (`live.ts:576-585`). A `dormant` curated place that the last feed published, on a page that also
   has any unplaced row on the **same water** (`key(o.water)`), reads `REWORDED_VERDICT` "check". Today only the
   top candidate gets that.
   - Better still, any `gone` place whose last feed answer was stricter than its chain's next place reads "check"
     until the owner confirms the drop. That matches "a place DFO dropped is reviewed, not assumed".
   - Port the same to `live.py`, or retire the Python feed as a publish path.
2. **App**, as a backstop against id or vintage mismatch. Chain ids the release lists that are missing from a feed
   region the feed carries (`feed.regions[r]` present, not held) read "not known" when the release had an answer
   for them (non-dormant, no `d`). They do not fall through. Report them once (`reportFailure`). This mirrors T757's
   rule for pieces.

## Tests

- Exist:
  - `app/workers/feeds/test/dfo.worker.test.ts:292` (reworded, matched as top candidate → check).
  - `:315` (gone → listed and told; asserts that the wider rule answers).
  - `pipeline/tests/test_dfo_live.py:114-115` asserts that a reworded place leaves `locs`, which pins the Python fail-open.
- Missing:
  - A reword with no candidate.
  - A reword whose top candidate is a sibling.
  - An app test that a release id the feed lacks never reads looser.
  - A property test over the release: for every chain, "remove the narrowest place, it must not read looser"
    (`failopen.py` is that check).

Scratch scripts, not committed: `failopen.py`, `live_doctor*.py`, `tworegion.py` and `wt/gone.test.ts` in the
session scratchpad.
