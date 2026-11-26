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

## State (2026-11-25 17:10 MST by the clock)

**Done.**
1. Prepare (PR #221): snapshot, name list, token path, merge-queue rule, outside-git list.
2. **The move: the repo is `canifishthis/canifishthis`** (owner's click ~15:16). Every check passed; what it showed
   is `repo-move/PREP.md` section 9. Slug flip on main: a85dec393 (#217).
3. Merge queue on (ruleset 14082789; `gh pr merge --merge --auto` enqueues).
4. T1062, the plan (agreed, #229) AND the build: **on main, PR #257**, commits 5ac3d67f3 (code + tests) and
   fad6911e8 (runbooks), merge 7139d1ae2. One independent review (four findings, all fixed, real-git tests). The
   0.10.0 chat cherry-picks a85dec393, 5ac3d67f3, fad6911e8 onto `release/0.10.0`; its staging ship then tags
   `v0.10.0-rc.1`; `release --final 0.10.0` is run by a chat on the owner's word. The planning chat flips T1062's row
   when 0.10.0 has shipped through an rc.
5. PR #256 (T1134, another chat's, closed): its two video tests find `ffprobe` on PATH and skip by name where there
   is none (3192f957b); main merged into it to clear a conflict in `app/tools/store-shots.json` (9cfe92673). The
   owner said yes to this job here. Tell the planning chat its merge commit when it lands.

**Owner's later answers (dialogs and chat, all 2026-11-25):** the main ruleset: "Queue + restore bypass
(Recommended)"; Dependabot and CodeQL appearing: "I turned them on, keep", then for the runner backlog "Pause both
until 0.10.0 is out (Recommended)", and for CodeQL "you can change it" (the org configuration, PREP section 9);
the order: "yes the release candidate structure is first then 0.10.0 happens right after. with other items";
the Contributors list (two old commit authors): "it is a future thing to write. yes. like afgter 0.11" (the
planning chat's T1143); "clean up workflows after too. can delete bloat as part of cleanup" (to the planning chat);
the #256 job: "yes it can fix ti". Token: production check ok, "the expire is 1 year", old token deleted. The exact
expiry date is still owed to the planning chat (the owner reads it off the token page).

**Next, in order.**
1. **T1120**, the fresh clone. Work list: the 17 finds in `repo-move/OUTSIDE-GIT.md`. Heavy steps through
   `scripts/heavy.sh`, never while a release build runs; the disk had 31 GB free at 17:07 and `heavy.sh` refuses
   under 20 GB, the sources alone are about 16 GB: free space first (the planning chat has a disk ticket), and ask
   the 0.10.0 chat before any heavy step. The light half (clone, hooks, installs, fast tests, the doc fixes) needs
   neither. No production step, no parser run. The owner confirms the secrets list (about 10 minutes).
2. **T1131**, the store pictures by a hand-started workflow; the iPhone-ratio pictures from the Android or web
   captures (his words); step 1 = the proof that the map frames render on a Linux runner; stop and report if not.
3. After 0.10.0 is out (the planning chat keeps these on the owner's list): Dependabot security updates and CodeQL
   back on; the tag ruleset (`v*`, `data/*`) after the first rc release.

Until 0.10.0's PRs are through: batch docs pushes (few runners), one push per PR.

## For the planning chat

- All sent and answered: T1130 (the worker's 204), T1143 (Contributors list), the CI ticket (runners, five workflows
  per docs PR, the workflow clean-up), the two #257 review leftovers (the rc check decides by branch name; a
  misleading message when the clone lacks the rc commit), the Dependabot/CodeQL re-enable item.
