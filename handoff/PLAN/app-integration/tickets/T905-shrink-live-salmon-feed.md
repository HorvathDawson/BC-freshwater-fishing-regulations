# T905 — Shrink the live salmon feed back under its old size

Status: IN PROGRESS (option 2 built in 6ba240568, d163d5416, 3e368e516: lane L14, branch perf/T905-worker-state-out-of-feed, waits for review + merge); left (Claude): the STAGING feeds-worker deploy (deploy/staging/feeds.sh) and a read of its first DFO run (seeded once, no wake, state.json in R2 private); then PRODUCTION deploy/prod/feeds.sh, the owner's. The salmon head gate (110,000) is not part of option 2.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T905 | Shrink the live salmon feed back under its old size (owner) | IN PROGRESS | 6ba240568, d163d5416, 3e368e516 | owner 2026-11-24 option 2 "Move worker state out": built, the public index 132,987 -> 121,688 B (check 124,000); left (Claude): review, merge, staging feeds deploy; production deploy the owner's |

T870 (2026-11-19) added each place's printed DFO rows (`pr`) to the live salmon feed: index.json grew to ~133 kB
(15.7 kB gzipped) and its size check was raised from 112 kB to 135 kB. Owner 2026-11-19 ~06:40, on that question,
verbatim: "okay we will work to shrink it down later."

Owner 2026-11-24, verbatim: "Move worker state out (Recommended)" — option 2 (A: worker state out of the public index,
backward-safe; no `dfo-feed` bump, no `FEED_MIN_APP` move). Not chosen: B (`pr` rows interned, `dfo-feed/4` + `FEED_MIN_APP`
0.9.1: a 0.9.0 app refuses an index whose `pr` is not a list of strings) and `salmon/5`.

Done (6ba240568, d163d5416, 3e368e516; lane L14, branch perf/T905-worker-state-out-of-feed): `STATE_KEYS` = `sha256`,
`changed`, `failures`, `header`, `unresolved`, `texts` (`pipeline/common/feed_formats.py DFO_STATE_KEYS` →
`feedFormats.generated.ts` → worker `dfo/state.ts`) moved into a private `{format: "dfo-state/1", regions: {slug: {…}}}`:
worker R2 `private/feeds/dfo/state.json`, Python `<feed dir>.state.json`. `test_dfo_live` fixture: 132,987 → 121,688 B
raw (15,763 → 14,334 gz), state 11,392 B; check 135,000 -> 124,000 (`test_todays_pages_join_every_location`). No app
reads a moved key (`salmonFeed.test.ts` "(T905)" reads both shapes to the same payload); `push.ts dfoAnswersChanged`
skips every STATE_KEY (`REGION_BOOKKEEPING`), so the cutover wakes none. Cutover: the first run with no state file seeds
it ONCE from the last index (state written first at the cutover; after it index, then state; notices after both).
Rollback to a pre-T905 worker: every page a first read, keys back on the index; redeploying T905 reads the state file
(pinned "after a rollback the state wins over keys left on the index").

## Work (left)
1. Review + merge.
2. STAGING `deploy/staging/feeds.sh` (its worker tests gate it, then it runs every job at once: that run is the cutover,
   read its log for "seeded once" and `answersChanged: 0`, and R2 `private/feeds/dfo/state.json` appears).
3. PRODUCTION `deploy/prod/feeds.sh` (the owner's, CONFIRM_PROD) the same way. No app build, no data release needed.
   The worker is the publisher of `feeds/dfo/` (the Python's `data/generated/regs/dfo_salmon/feed/` reaches R2 only via
   `upload.sh --ignore-existing`); Python updated, worker not: the public feed keeps the keys until the worker is deployed.

Release head `salmon` (note, not part of option 2): over its 100,000 B gz gate on the 0.9.0 build (101,480; live
97,723); owner raised it to 110,000 (2026-11-24, "Raise to 110,000 (Recommended)", T796). Measured r-741815c6090d vs
r-0aec88ed3edb: the growth is `ca` (T812 Babine circles: 11 circles, 48-point rings at 6 decimals, +3,559 gz); T754 adds
~27 B. Lossless = `salmon/5` (C1 ring as microdegree offsets 99,987 gz; + `pr` interned + chain `l` as place indices
97,563) with `minAppVersion` 0.9.1; inside salmon/4 only fewer ring points (32 pts 100,177, 24 pts 99,722 gz; a look
call); no `ca` at all (app draws from `c` + `radius`) 97,747.
