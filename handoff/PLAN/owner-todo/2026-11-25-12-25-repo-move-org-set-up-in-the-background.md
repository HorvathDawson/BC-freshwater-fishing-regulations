order: 780

## 2026-11-25 12:25: repo move, the org set up in the background while 0.10.0 builds (planning chat's plan)

Your words: "could we not start setting up the org and in the background moving it over? while we finish the build?"
Yes for everything except the transfer click itself. The facts are from T1041's transfer checklist (checked with
`gh api` on 2026-11-23); the repo-move chat re-checks each one.

**Part 1, now, beside the build (changes nothing the release uses):**
- You (about 10 minutes, any time): create the free GitHub organisation and tell the repo-move chat its name. In
  its settings: two-factor required, base permission "read", Actions allowed with the same workflow permissions the
  repo has today, fine-grained personal tokens allowed (with or without approval: your own request is approved
  automatically). Do not transfer yet.
- The repo-move chat (paste prompt 3 from the roadmap; it starts cheap, one lane): the slug-flip commit as a draft
  PR with the org's name in it; the exact token and `wrangler secret put` commands written out; the ruleset and
  merge-queue change written out; every open chat's worktree list, so each remote can be updated in one pass;
  T1120's list of what lives outside git; a `gh api` snapshot of today's rulesets, secrets names and settings to
  compare against after the move.

**Part 2, the cutover, about 20 minutes, only when the planning chat says 0.10.0 is on production and merged back:**
1. You: Settings -> Transfer, to the org. GitHub redirects the old web and git addresses, so nothing that is
   already running breaks on that alone.
2. The one thing that does stop at the click: the feeds workers' GitHub token (it belongs to your personal account),
   so worker-started workflows (HYDAT, run timing) fail and report until step 3. This is why the click waits for the
   release: a release in flight should not lose them.
3. You: issue the new token on the org (this repository only, Actions read and write). The chat puts it on the
   staging worker; you run the production line. The chat proves one worker-started workflow on the new repo.
4. The chat: compares the snapshot (rulesets, the four Actions secrets, required checks), merges the slug flip,
   updates every checkout's and worktree's remote, tells every open chat the new address.
5. After that, not in the 20 minutes: the merge queue (your OK on the ruleset change), the fresh-clone proof
   (T1120), the release-candidate plan (T1062).

**Why not transfer during the build (planning chat's judgment, yours to overrule):** the release reads CI results for
its gates and its scripts carry the repo's name; a redirect probably covers both, but "probably" in the middle of a
production release costs more than the 20 minutes saved. With part 1 done, the click can follow the merge-back at
once.

**13:11: the org exists.** Your words: "okay i created org ... org is https://github.com/canifishthis", and "the repo
in should be renamed too maybe?" The new owner is `canifishthis`. The rename is the repo-move chat's first question
to you (planning chat's proposal: rename to `canifishthis` in the same cutover, so the slug flip is one change and
the address reads canifishthis/canifishthis; GitHub redirects the old name too, as long as nobody makes a new repo
at the old one). Actions minutes: a public repo is not counted, in an org as on a personal account.

**13:38, from the repo-move chat (session `bc-freshwater-fishing-regulations-38`, started 13:12):**
- Answered 13:25 in its dialog, your pick: "Rename now (Recommended)". The repo becomes canifishthis/canifishthis at
  the transfer. The slug flip is draft PR #217; it merges only after the transfer and rename.
- **Do (you), before the transfer click:** the second collaborator account with write access becomes an outside
  collaborator after the transfer, and an org that requires two-factor removes an account without it. Check that
  account has two-factor on, or drop it.
- **Do (you), after the move, dated:** the new GITHUB_DISPATCH_TOKEN expires in at most 366 days (the org's cap).
  Re-issue it on both feeds workers before it does; the repo-move chat gives the exact date when you make the token,
  and it is written here then.

**Answered in the repo-move chat's dialog shortly before 13:40 (the clock at the relay; the chat wrote "~14:35"):**
- "okay i did org setup". Read back by the chat with `gh api`: two-factor required, base permission read. The
  Actions settings and the token policy cannot be read with your gh scopes; your words: "I set them as listed".
- The second collaborator account: "Drop it". You remove it, or it falls off at the transfer. Part 1 of the plan is
  done on your side; what is left is the transfer click and the new token (part 2).

**15:07: when the transfer click can come (you asked to start the move sooner; both chats gave the facts).**
- Not before or during production, and not while the Android build runs: the staging upload starts a workflow
  through the repo's hard-coded name, production data reads the release tags on the remote, each phone build starts
  with a CI gate that looks runs up by repo, and the Android build's version-code commit is pushed and must go green
  before the iPhone gate.
- **The planning chat's proposal: the click when both phone builds are uploaded (about 18:30), before the
  merge-back PR.** Then only that PR rides on the new name. The slug flip (#217) goes to main only, never onto
  release/0.10.0; the release checkout keeps the old name through GitHub's redirect until it is merged back.
- An earlier window exists, about half an hour sooner, while the iPhone build compiles, if all four hold at that
  minute: your production commands all done (the feeds worker too); no CI run in progress that a gate will read;
  the iPhone build past its gate; no push or PR about to happen. The repo-move chat calls it workable, the 0.10.0
  chat calls any move before the builds are uploaded a risk. If any of the four is unsure, wait.
- At the click, in this order: the 0.10.0 chat sets the main checkout's remote (one line, no working file); you
  make the new token; the repo-move chat puts it on staging, you run the production secret line; it compares the
  snapshot and re-checks auto-merge on open PRs. About 20 minutes, you present.
