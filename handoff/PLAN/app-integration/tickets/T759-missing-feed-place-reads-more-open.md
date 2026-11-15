# T759 — A DFO place missing from the live feed can make a water read MORE OPEN than DFO says

Status: OPEN (before closed testing; found 2026-11-15; fix proposed, owner to confirm the reading)

Source: main chat 2026-11-15, after T758 (the worker feed lacked the region-default places). Read-only trace with real
data: `handoff/PLAN/curation-review/feed-missing-location-2026-11-15.md` (file:line for every case).

## The defect
The app composes, per section, a chain of DFO places (narrowest first) with today's live feed. `composeChain`
(app/packages/core/src/salmonFeed.ts:279) drops a place the feed lacks and lets the next, wider place answer (:288). In
68 of the release's 435 chains, removing the narrowest place makes some species or day read looser (verified on release
r-f749475d0962).

When a place goes missing:
- DFO drops its row: Python feed silent, worker "gone" + notice; the wider place answers. Right if DFO really lifted the
  restriction; FAILS OPEN if the page was misread.
- DFO rewords the row and the best match is still this place: Python feed drops it from `locs` (FAILS OPEN; a test,
  test_dfo_live.py:115, asserts this as intended); the worker reads "check" (live.ts:570-582).
- DFO rewords the row and nothing matches, or a sibling place matches better: both feeds let the wider place answer.
  FAILS OPEN; the card shows only an "unplaced" caveat.
- A place id renamed between release and feed, or a release newer than the worker's entry copies: FAILS OPEN, silent
  (inferred from code, not run).
- Page fetch fails / row unparseable / one region's places all missing: fails closed or reads "not known". Fine.

Worst case found: DFO rewords the Campbell River "Maple Street to the cement block" closure, a sibling place matches
better, and the section reads open ("4 hatchery-marked") on Sep 1 instead of closed.

## Proposed fix (proposal; the owner confirms the reading)
1. Feeds (worker live.ts:576-585 and Python live.py, same rule): a curated place the feed would list as gone reads
   "check" (not known) when the same water has an unplaced live row, or when the place's last answer was stricter than
   what would answer in its stead. A place DFO plainly removed with no unplaced row on its water still goes.
2. App backstop (salmonFeed.ts): a release-named place absent from a feed region that was carried and not held reads
   "not known, check the DFO notice", reported once (reportFailure). Never silently the wider place.
3. Tests: no-candidate reword, sibling reword, renamed id, release newer than the feed; the Python test that asserts
   the fail-open is rewritten.

## Owner question
When DFO's page no longer shows a restricting place and our reader cannot tell "removed" from "reworded", should the
app say "not known, check the DFO notice" for that stretch (recommended) or follow the wider rule?

## Who
Python feed: main chat. Worker and app: the iOS chat (with T757/T758). Not part of Play build 33.

## Python half 2026-11-15 (branch `data/t759-missing-place-check`; a PROPOSAL until the owner answers the question above)

Code: `pipeline/regs/dfo_salmon/live.py` (`missing_places`, `stricter_where`, `_looser`, `known`). Tests:
`pipeline/tests/test_dfo_live.py` (`test_t759_*`).

### The rule (the worker port copies it; `live.ts:565-589` today has only "reworded" and "gone")
Run per region read today (not held, not failed), AFTER every region's answers are in (rule ii composes release
chains, which may cross regions) and against no check this step writes. For each `reconcile` outcome with
`status == "dormant"` and a `location_id` not already in `locs`, in this order:

1. **reworded**: it is `candidates[0].location_id` of a `drift` outcome (first drift wins) -> `REWORDED_VERDICT`, hold
   `fingerprint` = that drift row's fingerprint, `why` = `REWORDED_WHY` (unchanged from the worker).
2. **(i) unplaced water**: some unplaced row of THIS run's page has `names.key(u.water) == names.key(o.water)` (non-empty)
   -> `MISSING_VERDICT`, `fingerprint` = the smallest such unplaced fingerprint, `why` = `MISSING_UNPLACED`.
3. **(ii) stricter**: the place has a LAST answer and, on some release chain whose `l` names it, `compose(l, locs + {lid:
   last})` vs `compose(l, locs)` (with `s` groups, merged verdicts interned into a scratch copy of the tables) differ on
   some species/day so that `_looser(instead, was)`:
   `was != instead and not (was.s == instead.s != "open") and strictest([was, instead]) != instead`
   (`deliver.salmon.STRICT` closed < release < check < open; two openings by `stricter_opening` — a mixed merge counts
   as stricter). First hit in chain order, species order, day order -> `MISSING_VERDICT`, `fingerprint` = the outcome's
   fingerprint, `why` = `MISSING_STRICTER + " (<was.s> -> <instead.s>, <SP> on MM-DD, chain a > b > c)"`.
   LAST answer = the last feed's hold for lid if it was `kept: "check"` (its `last`, or none when it has none);
   otherwise the last feed's `locs[lid]` when `r == slug`; otherwise none (rule ii cannot fire).
4. **gone**: else, as today — listed in `gone` (`{location_id, water, place, page}`, the earlier entry kept) and told,
   only when the last feed published it or already listed it. Without chains the notice adds "(the stricter-answer
   check did not run: no release chains were given)".

A check calendar is `{r, sp: [[0, v]] * 5, g: [0, -1]}`. Every check gets a hold `{location_id, fingerprint, why,
kept: "check", last?}` (`last` = `{sp, g}` in THIS feed's tables, the last answer; re-indexed on every carry, including
a held / failed / unasked region) and an urgent notice `dfo-missing:<slug>:<lid>:<fingerprint>` naming the place, its
water and scope, and the `why`. A check stays until the row is placed (fingerprint joins) or the owner makes the place
dormant in the entry file (then `reconcile` no longer lists it). Verdict texts:
- `REWORDED_VERDICT` = `{s: "check", t: "DFO reworded this place and the new wording is not confirmed yet — check DFO"}`
- `MISSING_VERDICT` = `{s: "check", t: "DFO's page no longer shows this place as we read it, and the rule that would answer in its stead may be wrong — check DFO"}`

**What (ii) needs**: the place's last answer (the feed already has it: `prev.locs`, and the new hold `last`) and the
release CHAINS (`chains[].l`, `s`). The Python reads them from the live release (`main`: `--release` or LATEST; raises
when none). The worker has no release today: the port needs the chains synced to it (e.g. `sync_inputs` writing
`private/feeds/dfo/chains.json` = `[{l, s?}]` from the release it ships with) — until then the worker can do 1, 2, 4.
Worker note: `canonical()` renumbers verdicts by first use over `locs`; hold `last` indices must be renumbered with them.

### `known` (top-level index field, agreed with the worker/app chat)
`known: {slug: sorted ids}` = every place id the run had an entry for in that region (`ef.locations`: active, dormant,
page defaults) plus the derived closed pieces (`#closed:`); only regions read this run, or carried unasked from the run
that read them (that run's list). A held or failed region has NO key. **Contract: the app's backstop fires only for a
release-named id absent from `known[slug]` of a region that HAS the key** (renamed id, or a release newer than the
feed's entries) — never for a dormant or deliberately-left-out place.

Vectors emitter (`pipeline/tools/emit/dfo_vectors.py`, being changed on the T758 branch — NOT edited here): in its
`live` output, beside the per-region fields it already copies, add
```python
"known": got["index"]["known"],
"holds": {s: r.get("holds", []) for s, r in got["index"]["regions"].items()},
"gone": {s: r.get("gone", []) for s, r in got["index"]["regions"].items()},
```
then regenerate `python-vectors.generated.json` (the other chat).

### Fixtures a port should pass (all in `test_dfo_live.py`, on doctored copies of `fixtures/dfo_salmon/region1-eng.html`)
- close reword (Quatse weir "100 m … hatchery weir"): top candidate itself -> REWORDED check, hold fields, `last` = base.
- far reword ("Within signed boundaries at the hatchery fence"): `new` row on the Quatse -> (i) check, no `last`.
- Campbell Maple St reworded "from the end of Maple Street downstream to the confluence with the Quinsam River."
  (top candidate the sibling `~2`) -> (i) check; chain never reads open where the full page does not.
- Maple St row dropped (rowspan moved): (ii) check `closed -> open`; a third run keeps it from `last`, nobody told
  again; without chains it goes and the notice says the check did not run.
- Cayeghle row dropped -> plainly removed: `gone`, one `dfo-gone` notice, no hold.
- `known`: lists region 2's dormant places and the closed pieces; held / failed region 6 has no key; unasked region
  keeps the earlier list.

### Sweep (read-only, release r-f749475d0962 + local feed 2026-11-09)
72 chains read looser when their narrowest place is removed (68 by verdict kind, the trace's measure, + 4 by a looser
limit only: Skeena mainstem below CNR, Bella Coola whole x2, Kincolith). Fed through `missing_places` as a plain drop
(rule ii only) and as a far reword (rule i): 72/72 read check both ways, 0 still read looser. Not covered: a place
with no last answer and no unplaced row (first run, or a feed reset) still goes.
