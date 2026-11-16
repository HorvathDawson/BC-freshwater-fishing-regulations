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

## Running at 00:15 (Nov 16)
- **T778 FIXED and merged (a2ba7396b, POLICY_VERSION 44)**: whole-province binding diff = the two Middle Shuswap
  places gain piece `355998688:691`, nothing else moves (`<scratch>/t778/binding_diff.txt`). Side-check gap = T779.
- `pipeline build --hold-latest` restarted 00:14 (lane 1, log `<scratch>/logs/build-3.log`).
- Full pipeline CI tier on a2ba7396b (lane 3, log `<scratch>/logs/ci-tier-6.log`). The branch was pushed at
  1855ecf39 (tier + whole app check green); T778 and later commits are not pushed yet.
- Disk: filled at 23:38 (151 GB of app-test `release-open-*` temp folders); cleared; app chat fixed the cause (T764).
- Trout display round 4 (design agent; owner 23:58: one design from the picks, one size wording, inches and cm on the
  row, group size limits clearer, Dolly Varden named). ITERATION ONLY (owner 23:59), nothing built. Republish the
  same scratch path when it reports.
- App chat: R8 benches (one install bench rerun after the disk filled); it sends "benches done"; the staging release
  waits for that and for the build.

## Next, in order
1. Build done → read it (deliver gates, size gates, part names); if it stops, fix the cause.
2. Full pipeline CI tier on the head (T761 sweep commits + e3d6d5f26 are NOT yet covered by a green tier: the last
   tier ran before e3d6d5f26 and had the one size-shape failure it fixes), then the data-tier tests T761 / T763 listed
   (`test_curation_review_*.py`, `test_review_closed_areas`, `test_review_dfo`, `test_review_marks`,
   `test_review_queue`, `test_dfo_salmon.py`, `test_dfo_header.py`, `test_export_ui_rules.py`, `test_answers_28_live.py`),
   then push.
3. Marks audit again with `--since <scratch>/audit/audit-final.json`.
4. Message the app chat BEFORE the staging release and wait for its "benches done"; then
   `python -m pipeline release --staging` (expect `--accept-fixtures` for: T762's 103 year lines on 98 answer keys,
   T763's one Kootenay card, tonight's data changes; read the fixture diff first), a FULL staging site build from a
   clean worktree, `node app/tools/smoke-staging.mjs`; then the "after" message with the head: the app chat redeploys
   the staging worker from `ios/staging-worker` and runs the Play build of 0.7.11 (R8) to Play internal.
5. Skeena cascade review part 2: resume the Fable agent (part 1: `<scratch>/cascade/part1.md`, 13 checks listed at its
   end; to confirm: the rule-book Morice "No Fishing from Gosnell Creek to Lamprey Creek" binding Gosnell Creek and the
   Thautil, ruling T9). Write its report into `handoff/PLAN/curation-review/`.
6. General audit (Fable, about six checkers; scope told to the owner: release vs labels; app answers vs sources on a
   sample per region + the Skeena; every code commit of Nov 14-15 with the flagged list; guards and tests; decided but
   not built). Include the app chat's `pipeline/tools/emit/dfo_vectors.py` and `pieces_gate.py` branch changes.
7. Morning: ONE combined list for the owner (he said "ill review all regions together"): every save to re-tick and
   every open question, all regions; update the To-Do page rows (`sl-*`, `trout-display-options`), STATUS.md top
   entry, START-HERE pointer.

## Tickets filed tonight (main chat: T770 to T778; the app chat has T762-T769)
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
