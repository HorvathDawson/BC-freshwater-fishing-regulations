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

## Owner

- [ ] 2026-11-25 — After 0.10.0 is out: turn Dependabot security updates back on (the repo switch, yours) and CodeQL back on (one field on the org security configuration); the planning chat reminds you (T1141).
- [x] 2026-11-25 — "clean up workflows after too. can delete bloat as part of cleanup" (2026-11-25 15:44).
- [ ] 2026-11-25 — Picks for CI (defaults first): pytest-xdist on the same runner / shards / a larger runner; the queue skips unchanged areas / re-runs everything; drop main from on-push / keep; one required check / four; no docs-PR limit and at most two code PRs in CI / one docs PR at a time; no Team plan yet / Team; no self-hosted runner on the Mac.
