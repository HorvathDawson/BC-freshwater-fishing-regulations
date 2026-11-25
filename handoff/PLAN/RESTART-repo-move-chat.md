# Restart page: repo move chat (roadmap row 3; T1041's move, T1120, T1062)

Goal: the repo moves from the owner's personal account to the free org, as **`canifishthis/canifishthis`**, with the
worker token, rulesets and merge queue working after it; then a fresh clone proves the docs (T1120) and the
release-candidate plan is written (T1062). Scope is frozen to those three.

Rules of this chat: Fable orchestrates; one lane at a time (Sonnet for docs and the slug flip, Opus only for the
worker token path); one review round; work only in scratchpad worktrees (this chat's: `scratchpad/repo-move`, branch
`chat/repo-move`), never the main checkout; heavy jobs through `scripts/heavy.sh`, never while a release build runs;
no production step, no parser run; CONFIRM_PROD is the owner's; finds go to the planning chat
(`bc-freshwater-fishing-regulations-8e`), which numbers tickets.

The prepared material is `handoff/PLAN/repo-move/PREP.md` (+ `snapshot-before/`).

## Owner's answers

- 2026-11-25 ~13:20, "is the repo renamed in the same cutover?": **"Rename now (Recommended)"**:
  `canifishthis/canifishthis`.
- 2026-11-25 ~13:25, he asked "would it be worth splitting up into multiple repos?": answered no (the parts change
  together, one release cut, CI already path-split, everything else would double); the only seam is `handoff/PLAN`,
  a docs lean-out (T1081) question. No decision asked of him.

- 2026-11-25 ~13:38, "okay i did org setup". Read back with `gh api`: two-factor required = true, base permission
  = read. The Actions settings and the token policy cannot be read with his `gh` scopes; asked how to confirm:
  **"I set them as listed"** (all actions allowed, workflow permissions read-only, workflows cannot approve PRs,
  fine-grained tokens allowed). Recorded as his word; the first CI run and the token step after the transfer prove it.
- Same dialog, the second collaborator account (write access): **"Drop it"**. He removes it (Settings ->
  Collaborators), or it falls off at the transfer if it has no two-factor. Check after the transfer that only the
  owner is listed.

- 2026-11-25 ~13:42 he granted `gh` the `admin:org` scope. Read back: org Actions enabled for all repos, all
  actions allowed, default workflow token read, workflows cannot approve PRs: **the same as the repo today**. No
  member without two-factor, no outside collaborators. The fine-grained token policy has no API a user login can
  read: still his word, proved at the token step. The collaborator account was still on the repo at 13:42.

- 2026-11-25 ~13:58, the release-candidate plan (T1062), dialog, his picks: rc builds show the **plain version**;
  the rc is tagged **at the staging ship**; `release --final` is run by **Claude, on his word**; the tag ruleset
  (`v*`, `data/*`: no update, no delete) comes **after the first rc release**; data-only releases get **no rc**.
  "Do you agree the plan as written?": **"Agreed"**. Step 5 of this chat is done; the build half is for a later chat.
- Same dialog, T1131 added to this chat after T1120 (the planning chat relayed his approval; asked him directly):
  **"yes and it should use the android or webapp images to generate apple aspect ratio images too"**.
- Times on this page before 14:00 were first written from memory and ran about an hour fast; corrected against the
  clock (the Mac's time zone, MST).

## From the planning chat (2026-11-25 ~13:40)

- Find 1 is **T1130** (the worker accepts 200 as well as 204; 0.10.1), its PR #219. Finds 2 and 3 are on the owner
  item "repo move, the org set up in the background". Send it the token's expiry date when the owner makes it.
- Release timing: staging about 15:45, the owner's production commands about 16:00 or later, then the phone builds.
  The planning chat says when production is done and `release/0.10.0` is merged back.

## State (2026-11-25 14:00)

Step 1 (prepare, nothing live changes): **done 2026-11-25 ~13:50.** Waiting for the planning chat's word on 0.10.0.
1. Snapshot: done (`repo-move/snapshot-before/`, PREP section 1).
2. The list of what names the repo or the account: done (PREP section 2).
3. Slug-flip draft PR: **#217** (branch `chore/T1041-slug-flip`, worktree `scratchpad/slug-flip`), draft, no
   auto-merge; 5 files; targeted tests green; `before_pr.sh` not run (heavy, a release was building).
4. Token and wrangler commands: done (PREP section 4, with the order for step 3 and the two proofs).
5. Ruleset and merge-queue change: written (PREP section 5); the timeout value to confirm against recent runs.
6. Worktrees: done; one `git remote set-url` covers all (PREP section 3).
7. T1120's outside-git list: done (`repo-move/OUTSIDE-GIT.md`, 17 finds; they are step 4's work list, T1120 step 2).
8. Standard runners: checked, all `ubuntu-latest` (PREP section 6).
9. Org settings walk-through: done by the owner (answers above).

## What happens next (written 2026-11-25 15:07 MST by the clock)

Relayed by the planning chat, the owner's decisions; **not yet confirmed to this chat by the owner himself**: ask him
before acting on the two marked (ask).

1. **The transfer click** (the planning chat's proposal to the owner, his decision open): when BOTH 0.10.0 phone
   builds are uploaded and before the merge-back PR (the planning chat says "about 18:30" and "be ready from about
   18:00"; its clock has read about 20 minutes ahead of this Mac's). An earlier window (during the iPhone compile)
   only if all four hold at that minute: every production command done including the last `upload.sh` and the
   production feeds deploy; no CI run in progress that a gate will read; the remote set straight after the click and
   the token work done at once; no versionCode push or merge-back PR about to happen. Wait for the planning chat's word.
2. At the click: the owner transfers to `canifishthis` and renames to `canifishthis`; the 0.10.0 chat sets the main
   checkout's remote (it holds that checkout); then this chat: the owner's new token -> staging `secret put` ->
   `check-github` -> snapshot compare -> auto-merge re-checked on open PRs -> the owner's production `secret put` and
   `check-github`. **Hold the staging proof run (`/run/runtiming`) until the 0.10.0 chat says staging is free.**
   #217 merges to main only, never onto `release/0.10.0`.
3. The merge queue (the owner's OK on the ruleset change, PREP section 5).
4. (ask) **The build half of T1062 comes to this chat right after the cutover**, so the T1140 patch release (a HIGH
   bug on production, its own patch) can be the first release through a release candidate. One Opus lane for
   `pipeline/release.py`, Sonnet for the docs, one review. First trace the release ledger and how 0.9.0 passed
   `tag_problems`. Only after 0.10.0's phone builds are finished (it edits the two build scripts). The fix does not
   wait: if the build is not merged when the fix is reviewed, the patch ships by the runbook as today.
5. T1120 (fresh clone; work list = the 17 finds in `repo-move/OUTSIDE-GIT.md`), then T1131 (the owner confirmed it
   to this chat; the iPhone-ratio pictures from the Android or web captures; step 1 = the Linux render proof).

Done: step 1 (prepare, PR #221), the T1062 plan (agreed, PR #229).

## For the planning chat

- (none numbered yet)
