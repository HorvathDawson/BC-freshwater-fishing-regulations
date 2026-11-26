# T1144 — CI on the new org: few runners, five workflows per pull request; clean up the workflows

Status: OPEN — the hardening release (tooling); after 0.10.0 is out

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1144 | CI on the new org: few runners, five workflows per pull request; clean up the workflows | OPEN — the hardening release (tooling); after 0.10.0 is out | | repo-move chat 2026-11-25; owner: "clean up workflows after too" |

Source: the repo-move chat, 2026-11-25, after the move to canifishthis/canifishthis: the new org runs only a few CI jobs at
a time (2 running and 32 waiting at the worst; #217 spent 32 minutes in the merge queue, all of it waiting for
runners). The planning chat's docs PRs alone filled the queue and 18 runs were cancelled by hand. Owner, 2026-11-25 15:44
(relayed by the repo-move chat), verbatim: "clean up workflows after too. can delete bloat as part of cleanup".

Facts (repo-move chat): all five workflows start on every pull request by design. app-ci, docs-ci, feeds-worker-ci
and pipeline-ci each have a bare `pull_request:` trigger and their own `changes` job, so their required "... ok"
check can report; mutation.yml also has a bare `pull_request:`. A docs-only push costs about 10 short jobs. The
merge queue (ruleset 14082789: MERGE, ALLGREEN, min 1, wait 0, timeout 60) runs the checks a second time.

Work:
1. mutation.yml is not a required check: give it a `paths:` filter (pipeline/**, scripts/*.py, app source) so it
   never starts on docs.
2. The four required workflows cannot be path-filtered at the trigger (a workflow that does not start leaves its
   required check pending for ever). ONE small gate workflow computes the changed areas once and reports all four
   "... ok" checks, calling the heavy workflows (`workflow_call`) only for the areas that changed. A docs-only PR is
   then 1 workflow and 2 jobs.
3. docs-ci also re-runs on `edited` (a PR title or body edit): keep only if the title check needs it.
4. The owner's clean-up: list every workflow with what starts it, what it costs and whether anything still needs
   it; delete what is unused. The list goes to the owner before a deletion.
5. Say in `AGENTS.md` how many docs PRs a chat may have open at once while runners are short (proposal: one).
6. Turned off until 0.10.0 is out, back on after (see T1141): Dependabot security updates (the repo switch, the
   owner's) and CodeQL default setup (the org security configuration canifishthis-org-config-1, id 303766, field
   `code_scanning_default_setup` back to enabled; the repo-move chat set it to disabled 2026-11-25 15:43 on the
   owner's "you can change it").

Acceptance: a docs-only PR starts one workflow; every required check still reports on every PR and in the merge
queue; a change under pipeline/ still runs the pipeline tier. Proven on a test PR of each kind.
Severity: medium (slow merges on every release day). Model: Opus for the gate workflow (required checks and the
merge queue are easy to break), Sonnet for the rest. Size: M. Owner: sees the deletion list.

## Measured 2026-11-25 night (read-only: 1,000 Actions runs, every job's waiting and running time, PRs #185-#268)
Notes: `tickets/archive/night-notes-2026-11-25.tar.gz` `ci-merge-queue.md` (raw table and the `gh` commands to re-run it).
- A code PR takes a median 40.1 min from last push to merge: 22.4 min of PR CI + 19.7 min in the queue; both are
  mostly the pipeline tier's `Tests` step (11-19 min, one core, `pipeline-ci.yml:126`), run twice.
- The push to main is a THIRD run: 31 of 31 push runs after the queue went on were at a commit the queue had just
  tested green (about 21% of all jobs).
- Waiting for a runner: a median of 0 min per job, except the first hour after the org move. "One docs PR at a
  time" is no longer needed; a docs PR (1.3 min to build) still waits behind any code PR ahead of it in the queue.
- Confirmed: the PR title plus " (#NNN)" is checked against 100 characters (#266 failed at 103); a docs PR starts
  12 tiny jobs and 8 docs PRs edited `APP-STORE-SUBMISSION.md`, which runs the whole app tier. Not seen in the
  window: a conflicted PR starting no workflow; vitest `onTaskUpdate` timeouts on CI (all 5 failed shards were
  real failures). Cost is $0 (public repo).
- To add to the work: pipeline tests in parallel (pytest-xdist); the queue skips an area whose files did not
  change since the PR's green run; `main` dropped from `on: push` (release branches and a cache-warm job stay);
  one required check instead of four; the title checked with its suffix before a PR. A build-ready plan is being
  written (same archive, `ci-speed-build-plan.md`).

## Build plan 2026-11-25 night (read-only; every saving is an estimate, no suite was run)
Notes: `tickets/archive/night-notes-2026-11-25.tar.gz` `ci-speed-build-plan.md` (files and lines, the YAML sketches, the CI job log).
- The pipeline tests are mostly safe on 4 cores now: each test process already gets its own temp folder and its
  own copy of the curated data; no fixed ports or /tmp paths; no writes into the checkout. Risks: three timed
  tests, memory, one test that fetches old git data; each test FILE stays in one process (order-dependent tests).
  A code PR: about 40 to about 20 min (a guess). CI only: the Mac keeps its chunked runner.
- The push-to-main run can go: main's newest commit already has a green merge-queue run for all four workflows,
  and `ci_status.py` finds runs by commit whatever the event, so release checks keep working; a small cache job
  on main keeps the caches PRs restore.
- The six release stops of 0.10.0 were literals copied from a release; four (a count, page numbers, strings) the
  existing guard cannot see (it only catches numbers above 366). ONE pins file re-cut with the fixtures turns six
  stops into one accept (T1135).
- `caffeinate` probably does not stop a closed-lid sleep (not checked): the runbook says "lid open, on power"
  (T1161).
- Order: the title check, `caffeinate`, parallel tests, then dropping the main push run.

## Finds 2026-11-25 late (from the chats)
- A docs-only PR can break a pipeline test that nothing runs on it: the planning chat's PR #272 added a notes
  archive under `tickets/archive/`, and `pipeline/tests/test_reports_cmd.py` reads every archive there as closed
  tickets; docs PRs skip the pipeline tier, so main went red unseen until PR #273's queue run (fixed there: the
  test globs `closed-*.tar.gz`). The path filter must send `tickets/archive/**` (and any docs path a pipeline
  test reads) to the pipeline tier, or that test belongs to the docs tier (the speed chat fixes it in its
  "no third run" PR).
- A fresh worktree cannot commit or push until it has `app/node_modules` and a `.venv` (the commit hook:
  "commitlint is not installed"; pre-push: build-coho needs `<worktree>/.venv`, though the hook itself falls back
  to the main checkout's): about 5 min lost per new worktree; build-coho should take `PIPELINE_PY` like the hook.
- A docs-guard test assumed a single restart page (`app/tools/check-tickets.test.ts`; fixed in PR #275).

## 2026-11-26: a new PR's title is first checked by CI
- The hook from #276 checks the title only on a push to a branch that already HAS a PR. The title is set at
  `gh pr create`, after the first push, so a new PR's title is first checked by CI (the planning chat's reading, not
  verified in code). The limit is 93 characters INCLUDING " (#NNN)", so 86 for the title; #297 sat red all night at 100.
- New item: on the first push, the pre-push hook checks the head commit's subject (it becomes the title) against 86.
  Until it lands, every agent brief says "86 or fewer". (2026-11-26, T1177 batch: the pages that still said the bare `scripts/before_pr.sh` now say `--title`; the owner's pick, "Check the commit subject on push", is this item; no separate ticket.)
- From the night (speed chat): #279 added pytest-xdist to `requirements.txt` and the stage keys hash every installed
  package, so the first build after a venv reinstall reruns reach and deliver with the same output; #286 (T1171)
  landing first avoids it. A merge-of-main push still takes 250-350 s (the fixed steps): the next cut.

## Owner

- [x] 2026-11-25 — After 0.10.0 is out: turn Dependabot security updates back on (the repo switch, yours) and CodeQL back on (one field on the org security configuration); the planning chat reminds you (T1141). → "you can do it for me" (2026-11-26; done early: Dependabot 02:07, CodeQL 11:32 MST on his pick; T1141)
- [x] 2026-11-25 — "clean up workflows after too. can delete bloat as part of cleanup" (2026-11-25 15:44).
- [x] 2026-11-25 — Picks for CI (defaults first): pytest-xdist on the same runner / shards / a larger runner; the queue skips unchanged areas / re-runs everything; drop main from on-push / keep; one required check / four; no docs-PR limit and at most two code PRs in CI / one docs PR at a time; no Team plan yet / Team; no self-hosted runner on the Mac. → "Yes, the recommended set" (2026-11-25 23:23); shards → "Measure shards, then decide (Recommended)" (2026-11-26)
- [x] 2026-11-26 — The title length, pre-checked better → "Check the commit subject on push" (2026-11-26; not a separate open-PR script)
