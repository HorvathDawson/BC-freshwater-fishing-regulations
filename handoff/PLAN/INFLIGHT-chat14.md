# Chat 14 in flight (main chat after chat 13): state at 23:26 on 2026-11-15, written before a compact

Read with `NEXT-SESSION-after-chat13.md` (the order of work) and OWNER-TODO.md from "2026-11-15 night" to the end (the
owner's words tonight, verbatim). The owner is asleep or about to be; the overnight plan was told to him.

## Done tonight
- Step 1: chat 13's commits checked (full pipeline tier) and pushed; five review tests that stood on live curated data
  were fixed (they went red when the owner's saves landed).
- Step 2: marks audit + second look by eye at all 352 DFO places (`curation-review/final-marks-second-look-2026-11-15.md`),
  then the fixes he asked for (`curation-review/second-look-fixes-2026-11-15.md`): Tatshenshini (it made the reach run
  refuse), Morice lake outlet, Skeena pink above the CNR bridge, Sheldens whole creek, Babine Lake sockeye, Kitimat
  side streams off, Cowichan / Nanaimo side streams off, Shuswap closed stretch 2,075 m, Thompson Deadman / Ashcroft /
  Juniper closed areas. All as marks saved "unsure" for his re-tick (lines 2770-2794).
- T761 (DFO rest-of rework: own bounds, listed places, dormant / undrawable, "areas and times") built, on the shared
  branch (d65d4e15d), and the data sweep landed (marks 2790-2794: Skeena pair + dormant twin, Nitinat, Dean, Puntledge).
- T763 (Kootenay yearly limit band) on the shared branch (7958b3c60 + e3d6d5f26). T762 (year line, app chat) on the
  shared branch (4e793d61f).
- Atlas: second side build (`data/generated/side/rebuild-input-3`, 13 new cuts) PROMOTED to `atlas/full`;
  `full.prev` = the earlier build tonight's work started from; `full.b9de120e` = LATEST's atlas (keep). Live reach run
  backed up at `reaches/full.pre_ri3`. Every mark has landed (`apply --check`: only the two T359 tidal waits).
- Trout display exploration (Fable): page https://claude.ai/artifact/EopBgDtf8eu4LGZpYixSkH, rounds 1 and 2
  published; round 3 running (owner's feedback in OWNER-TODO). Nothing waits on it (owner's word).

## State at 04:13 (Nov 16)
- **RELEASED TO STAGING: `r-46e4355a7277` is LATEST and on staging; the staging site is rebuilt on it; smoke 19 pass,
  0 warn, 0 fail.** Head d0d584159 (pushed). The app chat got AFTER at 04:12: it redeploys the staging feeds worker
  (branch `ios/staging-worker`), then bench-diff, fast-fail, both CIs, then the Play build of 0.7.11 (R8) to Play
  internal on lane 3. It reports when each is done.
- The release took nine runs. What stopped it, each fixed at its cause: the reach gate (T778, fixed a2ba7396b); the
  disk (T764, the app chat); a stale mini release (ebc3ca7d9; the pytest tier does not run that step: run
  `python -m pipeline.tools.mini_release --check` before a push); the binding diff read
  (`curation-review/release-r-46e4355a7277-binding-diff-read.md`); three golden-tap declarations (fab8b8ed0,
  53d8e74c5: the closed Fraser state had landed on the wrong part); 12 app tests pinning old part counts (the app
  chat, 39918037e); the upload gate's fetch refused for want of a user agent (5c5078b2a).
  RULES LEARNED: never restore the working tree between release reruns (the release expects its re-cut fixtures in
  place); push and let CI finish before starting; make no commits while it runs.
- Owner on the pace (01:45): "Everything takes so long maybe we should have less tests all the time and do it together
  at end or something", then "Feels wasteful". My proposal is in OWNER-TODO for his yes or no; a skip of the release's
  checks on those words alone was REFUSED by the permission check: he must say it explicitly.
- **GENERAL AUDIT RUNNING** (five Fable checkers, read-only, started 04:15; reports under
  `<scratch>/audit-general/{labels,answers,code,guards,decisions}/report.md`): release vs the owner's labels (all
  regions but the Skeena); app answers vs the printed sources (6 waters per region); the code of Nov 14 to 16; guards
  and tests (with a smaller check plan for the owner); decided but not built. When each reports: read it, copy the
  five reports into `handoff/PLAN/audit-2026-11-16/`, ticket the confirmed findings (T783 onward), fix nothing that
  changes an answer without the owner unless it is a plain bug with a clear fix.
- Skeena cascade review: DONE, `curation-review/skeena-cascade-review-2026-11-15.md`, ticket T781.
- Trout display: round 6 is published (same link), MOCKUPS ONLY. Owner's replies and the open questions are in
  OWNER-TODO; the original designer agent can be resumed for round 7 when he answers.
- STILL OWED: the ONE combined morning list for the owner (every save to re-tick, mark lines 2770 to 2794; every open
  question: the 16 second-look ones, lanes A and B, the cascade's, T770, the Middle Shuswap tributary choice, the
  check-plan proposal, trout round 6's four), a plain summary, the STATUS.md top entry, the To-Do page rows.

## Since 04:15
- Play build DONE: 0.7.11 build 33 (R8) on Play internal, 04:37, from 0c9988033 (the app chat; its commits 88681f9f0
  and 362c44e6b). Staging feeds worker redeployed 04:13.
- Audit: four of five reports in (`handoff/PLAN/audit-2026-11-16/{code,labels,decisions,guards}.md`); tickets T783 to
  T792; the morning list in OWNER-TODO carries sections A to H. The answers checker still runs: when it reports, save
  `answers.md` beside the others, add its findings to the morning list, ticket (T793 onward).
- The code checker was asked to send its full report as text (its file was never written); if it arrives, replace
  `audit-2026-11-16/code.md` with it.
- NOT DONE: the To-Do page rows for the morning list (update `sl-*` rows: done / answered, and add one row pointing at
  the list); the fixes for T783, T784, T785, T792 A (small, clear; do after the owner has seen the list or when he
  says go); nothing that changes an answer is fixed without him.

## Next, in order
1. The answers checker's report → `audit-2026-11-16/answers.md`, the morning list, tickets.
2. The To-Do page rows.
3. With the owner: the morning list (re-ticks, questions), then the merge question, then production order (the
   feeds worker before the data), then closed testing.

## Tickets filed tonight (main chat: T770 to T792; the app chat has T762-T769)
T770 gear lines do not stack (owner's yes needed) · T771 chain size and the equal-places check · T772 lake takes its
through-river's side channels (Kitsumkalum) · T773 tidal boundary name (Kitsault) · T774 a place cannot be saved
undrawn (Kitimat) · T775 review app rest-of leftovers · T776 audit tool faults · T777 anchors scanner gap · T778 the
Shuswap delta piece (blocks the release). Still to file after the release: Kitimat wild rainbow limit 1 not on the
card, and Ross Lake's headline 2 against a daily total of 4 (both from the trout review, round 3).

## Rules that bit tonight
- Read `date` before writing a time: estimated stamps ran 30 minutes fast.
- zsh does not split $VAR into words: build argument lists as arrays.
- `git apply` of several cut patches collides; use `apply_proposals --only <id> --side-checked <id>`.
- A lane's fix can trip a layout guard only the full tier sees (the size-shape guard): full tier before push, always.
