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

## At 05:00: all five audit reports are in
`handoff/PLAN/audit-2026-11-16/{code,labels,decisions,guards,answers}.md`; tickets T783 to T798; the owner's morning
list has sections A to I (36 questions); To-Do page rows `ml-*`. Nothing is running.

## At 10:12: T793's fix is ready, not merged
Branch `data/t793-wild-limits-on-card`, commit f672f4444 (22 answer keys change; the build now refuses a card that
drops a wild or hatchery condition). The owner's list now has 38 questions: 37 = the new card sentences, go as worded?
38 = does "no wild trout over 40 cm" cover char on Dodd and the six lakes like it? Head 13928d2d0, pushed, clean.
Nothing is running. On his go for 37: cherry-pick f672f4444, `mini_release --check`, the changed tests, push, CI
green, `pipeline build --hold-latest`, the rehearsal, BEFORE to the app chat, the release, the site, AFTER.

## State at 12:14 on Nov 16: the owner answered EVERY question (13 dialog batches, 10:27 to 12:09)
His answers, verbatim: `OWNER-TODO.md`, "Answers by batch" (end of the file). He wants questions asked IN CHAT, in
multiple-choice batches ("i want questions in batches from you directly instead"); the page
https://claude.ai/artifact/H8gtvpptCZLGLaPGCyNfaJ is only a reference now. Tickets T799 to T806 hold the new work; ten
older tickets carry his answer in a section "Owner, 2026-11-16".

### State at 13:31 (read this first after a compact)
- **Pushed:** T793 (wild-trout card sentences) is on the shared branch, CI should be green. Head is on origin.
- **Two fix lanes are DONE in worktrees, NOT merged** (cherry-pick each commit into the main checkout, then the full
  pipeline tier, `mini_release --check`, `dfo_vectors --check`):
  - `lane/salmon-rules-1116` in `.claude/worktrees/agent-a2becf9b4bea93daf`: T783 3f90138cf, T770 4e0feaca9 (gear
    lines stack; eulachon never displaced; 13 chains gain a line), T787 178d52320 (hidden rows kept, marked, ignored;
    `typed.FORMAT` 4: BEFORE the next build run `python -m pipeline.regs.dfo_salmon.locations && python -m
    pipeline.regs.dfo_salmon.feed` in the main checkout, cached pages only), T796 A6 25f11159e (verdict field `f: 1`),
    T796 A5 a4a8cc070 + cae652cba (book place `book:<entry>::<rule>`, payload `static`, 70 stretches change; the
    full list is `<scratch>/t796-a5-changes.txt`; size gate `BOOK_ONLY_WHERE_DFO_LETS_FISH` keeps the salmon head
    at 50 kB instead of 92 kB: my call or the app chat's, with an errs-open edge under a live feed). The notes for
    the app chat (worker twins for T787, A6, T770; core for A5 static places and A6 page closing) are in each
    ticket's "Lane result" section on that branch. The owner said GO on A5 and wants the full list of the 70 with a
    map picture before and after, BEFORE it ships.
  - `lane/book-rule-fixes-1116` in `.claude/worktrees/agent-aaab7188849edc942`: T795 07db8a6a8 (six lakes open with
    the note), T794 56f4e4e91 (Williston) + 16f92dfcc (Peace part 9 closed, no new cut) + eae8241a3 (Bella Coola side
    channels), T798 20b902f56 (Coal Creek; reach POLICY_VERSION 45) + b79a4513d (Puntledge: owner said "Yes, as
    printed"), T801.1 186388445, rulings 865cbac37, ticket notes 8b2d83c17. After merging: a reach-only rerun, then
    deliver, then the release.
- **The app chat changed**: the new one is session `bc-freshwater-fishing-regulations-9c` (took over 13:09; lane 2).
  Same protocol: it says "ready to land", I answer "clear" / "wait"; I tell it BEFORE a release and when the T796
  marker note is ready (send it the lane's notes).
- **Re-check of saves with pictures (T806)**: answered so far: 1 to 27 and 30 (see OWNER-TODO "Re-check of saves").
  STILL TO SHOW HIM: 28 Kitimat hatchery, 29 Skeena sockeye rest-of, 31 to 34 the closed areas (pictures are JPGs in
  `<scratch>/recheck-pics/`; `captions.json` has what the drawer noticed). Then save each answer through the review
  app with his words. Follow-ups he asked for: Lamprey Creek confluence onto the Morice main channel; Sheldens: a
  low-confidence cut at the road crossing near the mouth, also the tidal boundary; gotchas (Kanaka, Kispiox, Shuswap
  one km, Stamp Falls Deer Creek, Somass Collins Farm, Bulkley / Suskwa, Sheldens); Babine: confirm the 7 matching
  circles, lake open with note, the missing five + Gullwing line as a LATER item for him; Stamp Falls cut stays.
- **Heading audit** (`curation-review/dfo-side-streams-against-exact-headings-2026-11-16.md`): 14 places disagree
  with his rule; they go to him as questions (not asked yet), then the "derive from the exact heading" build.
- **Trout** (mockups only): round 8 is published; the third attack says still "not yet" (design review file, last
  section). The designer's three round 8 questions and the attack's findings are NOT yet put to him; round 9 would
  fix: wild char rows at Dodd / Lois, dates for zero-fish / wild changes / closures, "no limit of its own", the grey
  L. Designer agent `a0e6f69598b62e7f2`, reviewer `ab3f19d1c5cf2f5cf` (resume by SendMessage).
- T804 (the universal "own row" rule) and the small code lane (T784, T785, T792 A, T799, T797 items 2 to 5, T801
  items 2 to 4) are NOT started.

### Added 13:48: the owner's later answers (all in OWNER-TODO, end of file)
- ALL 34 re-check pictures are answered. Right as drawn: 1, 3 to 21, 23 to 26, 29, 30, 32, 33 (keep 4 km), 34 (keep
  the printed points). Changes he asked for: Stamp Falls cut STAYS (500 m below the fishway; gotcha); Lamprey Creek
  confluence onto the Morice main channel (22); Sheldens: low-confidence cut at the road crossing near the mouth,
  also the tidal boundary (26); Babine: confirm the 7 matching circles, lake open with note, the rest a later item
  for him (27); Kitimat outfall strip TAKES its side streams ("Heading counts here too") (28); Deadman START: find
  out why it is 495 m below the bridge, then show him (31). The Blanchard place is right (his "wrong" withdrawn).
  NOT DONE YET: saving each answer through the review app with his words.
- Heading rule: "Yes, river only for all ten" (Tatshenshini whole-river row, Kitseguecla, Sustut, Cranberry, Kiteen,
  Meziadin, three Nanaimo places, Little Campbell, the Morice Forks); "A named creek keeps its side streams" (the
  three creeks and the Blanchard stay as saved; an exception to write into RULINGS). Build: derive the setting from
  the exact heading + the place's words, a saved value only as a stated exception (design note in the audit file);
  changed answers to him before it ships. No ticket yet: file it (next free number T807).
- Salmon head size gate: "Raise to 100 kB". The salmon lane agent (`a2becf9b4bea93daf`, resumed 13:43) is adding
  commits on `lane/salmon-rules-1116`: carry the book place everywhere, gate 100 kB, the full list for the owner
  (`<scratch>/t796/book-closures-list.md`), proof that the map's status data reads closed. The app chat (-9c) builds
  its twins FROM that branch (never rebase it) and we land the vector-moving parts TOGETHER; T783 alone may land any
  time (tell it first).
- Trout: "up to the 5" in the grey band; Cunningham's possession line is the lake trout's only; "round 3 then attack
  again but use opus": round 9 is running with an OPUS designer (agent `a2f408891ce214d38`); when it hands back:
  publish the same page, then spawn an OPUS attacker (brief like the Fable one's: the review file's three sections +
  `trout-adversarial/enum.json`, `rules.json`), report both to him. From now on trout agents are Opus.
- The book-rule lane (`lane/book-rule-fixes-1116`) can land now: nothing in it waits on anyone (Puntledge: "Yes, as
  printed").

### Added 15:00
- The book-rule lane is LANDED and pushed (13:54; full tier 6,499 pass). A reach-only rerun into
  `<scratch>/reaches-1116/full` finished clean: exactly 7 of 3,404 rules changed (Puntledge r6 +1,059 and r1 +6,
  Atnarko r12 -50, ecological reserves r3 -6 and the new r3p, Williston r1 +2, the new Dinosaur r1b). NOT promoted:
  more reach changes are coming (T807, T803); delete that folder (15.6 GB) once a later run replaces it.
- The app chat landed merge 3 (T756 + T760) at 14:57; next from it: T797, then the deploy gate; its salmon twins are
  on `app/salmon-twins-1116`.
- The owner read the book-closure list (about 14:55): yes to raising three more limits (salmon-6 460,000; web
  first_tap 4,400,000; first_tap_low 3,600,000); closures must cover only the water and days the book closes (to be
  PROVEN per row); the Nass side streams' coho closure Jan 1 to Jun 15 needs its real consequence looked at before it
  ships; and he wants ONE artifact page of all 70 closures with maps, built by a SMALLER model (Sonnet). The salmon
  lane agent (`a2becf9b4bea93daf`) is doing the limits, the per-row proof and `scratchpad/t796/nass-consequence.md`,
  and making `scratchpad/t796/book-closures-raw.json` regular with full section ids. WHEN IT RETURNS: tell the app
  chat the commit; spawn a SONNET worker to build the page (`<scratch>/book-closures-page/`, images as files; the map
  drawing tool to reuse is `<scratch>/recheck-pics/tool/`), publish it as a new artifact, give him the link in chat.
- Trout round 9 (Opus designer) is published on the same page; an OPUS attacker (`a402ba0a063c3ee4e`) is on it. When
  it returns: append its report to the design review file, then put the verdict and the designer's three questions
  to him in one dialog (one line above the table when every fish closes the same day: yes; a limit that only gets
  looser stays undated: yes; Adams River shows the stricter 80 cm with the one-day rule in the sheet: yes).
- No worker runs on Fable any more (his word 13:49): Opus or Sonnet; name the task and reason to him first otherwise.

### Added 15:07
- Owner on trout round 9 (15:01): "for the trout view i liked the old look more but with the words of round 9 buyt
  the frame look is a lot cleaner". Round 10 is being built by an Opus designer (`ae176f25b522ddc16`): round 8 look +
  round 9 words + round 9 outline, as a three-way switch (A no outline, B with outline = default, Round 9). It writes
  `<scratch>/trout-display/trout-display.r10.html` and `shots/r10/`, NOT the live file (the attacker is reading round
  9). WHEN IT RETURNS: copy it over `trout-display.html`, publish to the same artifact url, send him
  `shots/r10/skeena-three.png` with one question (A or B, or back to round 9). The attacker's word findings then go
  to the same designer by message (round 11), not a fresh worker.
- Gotchas written and pushed: eight DFO place wordings kept on his word (`pipeline/docs/GOTCHAS.md`, last section).
- T806's last step is running (Opus, `aa310335a7fa07266`, in the MAIN checkout, no commits): each of his picture
  answers saved through the review app's own save path with his words; Kitimat strip (2786) side streams on. WHEN IT
  RETURNS: check marks.jsonl gained lines only, commit `data/curated/review/marks.jsonl` and the places files by
  explicit path, tick T806. Do not merge anything into the main checkout that touches those files until then.

### The fix queue for the next staging release (all with his go; each lands with the full tier, then ONE release)
1. T793 (landing now). Tell him with the changed-answers list that Lois and Khartoum read 2 wild + 2 wild (T805.1).
2. Printed rules back on the page: T794 (Peace below Bennett Dam: map check, maybe a cut = next rebuild; Williston
   all three parts; Bella Coola side channels), T795 (Fraser, Kotcho, Cold Fish lakes, 15 Mile Swamp, Westwick,
   Priest: open with the note), T797 items 2 to 5, T798 (Coal Creek Class II above the bridge; Puntledge above Comox
   Lake carries both), T801 (Section 4 wording).
3. Salmon rules: T770 (show both gear lines; eulachon on every Region 6 water), T787 (hidden tables kept, marked
   hidden, ignored: parser + the worker's twin), T796 (A5 static closed location; A6 finfish closes the page; examples
   to him), T783.
4. Small code: T784, T785, T792 A, T799.
5. T804: the universal "a joining water with its own row is out" rule: the trigger sentence + the province-wide list
   of changed answers go to HIM before it ships.
6. T803: the label follow-ups (Cowichan mark off, Middle Shuswap, re-saves, gotchas, Mara cut name, Babine areas,
   Gitnadoix, side channel name; Stamp Falls cut 60 m down waits for a map rebuild).
7. T791 (fewer, later checks: yes as proposed), T790 (records), fresh production commands (T404).
Release rule: rehearse first (`python -m pipeline release --staging --dry-run --rehearse-follows`), BEFORE to the app
chat, release, site, AFTER.

### Not asked / still his
The 34 pictures (above); the build 33 phone walk; iPhone build 36 + push; T786 (ship DFO's words undrawn) unanswered;
T802 (classified-water indicator) is an idea, looks first. No Firebase now ("we dont want this right now").

## Tickets filed tonight (main chat: T770 to T798; the app chat has T762-T769)
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
