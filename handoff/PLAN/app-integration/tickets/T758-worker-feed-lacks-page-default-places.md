# T758 — The feeds worker's live salmon feed has no answer for the region-default places the Python feed publishes

Status: DONE — 179c6cba5, c7a09348a. The feeds worker publishes the page-default places from the page's own sentence, equal to the Python on all 435 chains; staging since 2026-11-15, production worker deployed 2026-11-19.

Closed by the 2026-11-20 audit: worker fix 179c6cba5 (equal to the Python on all 435 chains, 99dfae816), merged via c7a09348a 2026-11-16. The only item left, the production worker, was deployed 2026-11-19 with 0.8.3 (OWNER-TODO: 'Done by Claude: step 1 (feeds worker, version 0a3205f6 ...)').

Source: found 2026-11-15 by the T757 worker lane when the worker's test copies of the curated DFO entries were
refreshed; confirmed on the live staging feed the same day. The Android chat (DFO is its area) has no ticket or owner
decision on it and asked for this ticket, first step as written below.

## What is known
- The Python feed (`pipeline/regs/dfo_salmon/feed.py` `build`, T349) makes one rule per PAGE-DEFAULT place from the
  page's own sentence ("There is no fishing for salmon in Region 3 except …"), through
  `cascade.statement_locators` (keyed `page:<slug>:<scope>`). Curated places of kind `page_default`:
  `1:default:region`, `2:default:areas-13-15-16`, `2:default:areas-28-29`, `3:default:region`, `5a:default:region`,
  `7:default:region`, `8:default:region`.
- The worker has no port of it: `app/workers/feeds/src/dfo/entries.ts` only leaves `page_default` places out of
  "missing". On the worker's saved pages with today's committed entries the Python publishes 274 locations, the
  worker 267; the 7 are exactly those ids (pinned as a named exception in `dfo.vectors.test.ts` on branch
  `feeds/t757-worker-pieces`, so closing the gap fails that test until the exception is dropped).
- Live staging feed, 2026-11-15 22:14Z (worker version f2b98bd4): 268 locations, none of them a `:default:` place.
- Older than T757 and not about closed areas.

## Not known (step 1 answers it)
What a water covered only by its region's default sentence reads in the app today, per species and day:
(a) from the release alone (offline), (b) from the release + the live worker feed, (c) from the release + what the
Python feed publishes. And whether the release's chains name the `:default:` ids at all.

## Work
1. **Investigation on real data** (release r-f749475d0962 on disk + the live staging feed + the Python's own output):
   one Region 3 water, one Region 8 water, one Region 2 tidal-area default; today and one summer day; the app's own
   card function. Classify: same / the release's baked default shows / "not listed" or nothing / OPEN or a wrong
   reading where the Python is closed or limited.
2. **Priority by the answer** (the Android chat's rule): the baked default shown correctly = a parity gap, after
   build 33. "Not listed", open, or a stale reading = a correctness bug: fixed before closed testing, and on the
   owner's list the same day.
3. **Fix:** TS port of `cascade.statement_locators` + the `feed.build` step into the worker (`src/dfo/feed.ts`), the
   vectors emitter writes the page-default locs' calendars, the named exception in `dfo.vectors.test.ts` is dropped,
   mutation-checked tests. Deploy to staging with a word to the Android chat; production is the owner's.

## Acceptance
- The worker's loc-id set over the saved pages equals the Python's with no exception list.
- For the three sample waters the app reads the same under the live worker feed as under the Python feed.

Depends: T757's worker branch (the refreshed entry copies and the `live` pin). Size: S to M (decided by step 1).

## Step 1, answered 2026-11-15 15:27: the status is right everywhere, the words are wrong; no case shows open
Real data: release r-f749475d0962 (on disk), the live staging worker feed of 22:14Z, and the Python's page-default
locations from a `live.run` dry run over the saved pages (regions 3 to 8 are byte-identical to today's pages; regions
1 and 2 changed on Nov 6, but the default sentences in the feed's own texts are word for word the ones the Python
read). Cards from the app's own path (`pickSalmon` -> `withSalmonFeed`, `salmonCardLive`).
- **The release uses the ids:** 153 chains name a `:default:` place, always last in `l`. Chains that are the default
  alone cover about 1.1 M sections (R1 106,817; R2 62,978; R3 134,365; R5a 167,128; R7 598,878; R8 41,416).
- **What the app shows under the live worker feed:** "Salmon: closed — Not opened by DFO: closed to salmon fishing"
  (verdict 0, NOT_LISTED; no "from" line). **Offline, and under the Python feed:** DFO's own sentence, e.g. "There is
  no fishing for salmon in Region 3 except for the opportunities listed in the table below." from "Region 3 · All
  Region 3 waters"; for Region 2 Areas 13, 15, 16: "…the daily limit … is zero (0)."
- **Samples** (Nov 15 and Jul 15 alike): North Thompson River (R3), Okanagan River (R8), Lillooet River (R2, Areas
  28/29), Southgate River (R2, Areas 13/15/16). Headline "Closed today", map closed, in all three readings.
- **Full sweep, worker feed against Python feed:** 153 chains x 5 species x 366 days: verdict status and map code are
  identical everywhere; only the text differs (about 205k species-days).
- **Why:** `composeChain` drops a chain location the feed lacks and falls to verdict 0, which is always closed. On a
  mixed chain NOT_LISTED replaces the default on the days the narrower places say nothing.
- **Rank:** not a wrong answer today (all 7 sentences are closures or zero limits). It becomes one the day a default
  sentence opens something: live would read closed, offline open. By the Android chat's rule ("not listed" = fix
  before closed testing) the fix started the same day. Worker only; no app build; production's worker has the same gap
  and its deploy is the owner's.

## What the Android chat wants before the staging deploy (2026-11-15)
The vectors test holding worker == Python on the defaults' calendars with the 7-id exception gone, and the sweep
(153 chains x 5 species x 366 days) re-run with the TEXT identical too.

## Open question for the owner's review (asked by the Android chat; list, do not change)
A chain location missing from the feed reads as verdict 0, "not opened, closed" (fail closed). T757 adds "not known"
for a bound closed-area piece. Is there another kind of location where "missing from the feed" should read "not known"
instead? Candidates to check in the code before they go to the owner (NOT verified, the iOS chat's reading):
1. A narrower place that RESTRICTS inside a wider opening (a closed or reduced stretch that is an ordinary place, not
   a T573 piece): if it is missing because its wording drifted and no longer joins, rather than because DFO removed
   it, the chain falls to the wider place and reads more open than DFO says. What the feed does today with a curated
   place that no longer joins (held row, "missing" report, unplaced list) decides whether this can happen.
2. A two-region section (`s` groups): one region's places missing from the feed leaves the other region's answer
   alone, where the strictest of both was meant.

## Results (worker lane), 2026-11-15
Branch `feeds/t758-page-defaults` (179c6cba5, off the T757 worker branch). `src/dfo/cascade.ts` ports
`cascade.statement_locators` (and `statements`, `page_scope`, `page_fingerprint`); `feed.ts build` appends the page's
default-sentence locators; `live.ts join` skips a `page:` fingerprint no place carries. The vectors' `live` is the
Python `live.run` index over the saved pages plus two edited-page cases (`no_default_sentence`,
`sentence_without_a_place`). `dfo.vectors.test.ts` holds the worker's `runDfo` equal to it, every place with its year,
274 places, no exception list. End to end on release r-f749475d0962: the 153 default chains (279,990 species-days) and
all 435 chains: 0 differences between the worker feed and the Python feed in status, words, map code or gear. 487
worker tests, emitter `--check`, `check:fast` rc 0; six mutation checks, each fails a test. The Android chat read the
two pipeline files the lane changed (`pipeline/tools/emit/dfo_vectors.py`, `pipeline/tests/test_dfo_vectors.py`).

## Staging, 2026-11-15 15:57
Deployed with the Android chat's word: `ios/staging-worker` 173867c4c (push + crash limit + T757 + this), version
92b74def; gauges, dfo, inseason answered 200. Live feed of 22:58:18Z: 275 locations = 251 places + 17 closed-area
pieces + the 7 defaults, each default with DFO's own sentence ("There is no fishing for salmon in Region 3 except for
the opportunities listed in the table below."; Region 1 "…the daily limit for all waters in Region 1 is zero (0)."; …).
Row-digest notices: by the code (`rowDigest`) a region is compared only when its page's sha differs from the stored
baseline, so the deploy itself sends none; the first time a DFO page changes, that region's default sentence will
show as an "added" row once. Not checked on a device: the staging app reading the sentence on a card.

## Merged, 2026-11-16
On `redesign/stream-sections` with T757's worker half (one merge of `feeds/t758-page-defaults`); the checks are in the
T757 ticket's "Merged" section. Left: the production worker deploy (owner).
