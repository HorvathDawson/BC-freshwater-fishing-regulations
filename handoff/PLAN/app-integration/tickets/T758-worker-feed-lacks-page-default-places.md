# T758 — The feeds worker's live salmon feed has no answer for the region-default places the Python feed publishes

Status: IN PROGRESS (iOS chat; step 1, the investigation on real data, started 2026-11-15 15:25; the fix waits for its answer).

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
