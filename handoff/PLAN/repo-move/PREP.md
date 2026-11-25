# Repo move: what was prepared before the transfer (T1041; repo-move chat, 2026-11-25)

From `HorvathDawson/BC-freshwater-fishing-regulations` to **`canifishthis/canifishthis`** (owner 2026-11-25 13:25:
"Rename now": the transfer and the rename happen in the same cutover). The repo is public and stays public. Nothing
here changed anything live. The transfer waits until 0.10.0 is on production and `release/0.10.0` is merged back.

## 1. Snapshot of today (compare after the move)

`snapshot-before/` holds the raw `gh api` answers, taken 2026-11-25 13:16 MST. In short:

| What | Today |
|---|---|
| Visibility, default branch | public, `main` |
| Merge settings | merge commits only (squash and rebase off), auto-merge on, delete branch on merge on, "update branch" off; merge commit title = PR title, message = PR body |
| Features | issues, projects, wiki on; discussions, pages off |
| Security | secret scanning on, push protection on; Dependabot security updates off |
| Rulesets (3, all active) | `14082789` "main trunk (T995)": default branch; no deletion, no force push, PR required (0 approvals, merge method "merge" only), 4 required checks; bypass: repo admin, pull-request mode. `27831606` "release branches (T995)": `release/**`; no deletion, no force push. `27831608` "old dev branch read-only…": `redesign/stream-sections`; no deletion, no force push, no update |
| Required checks (GitHub Actions, integration 15368) | `docs ok`, `app ok`, `feeds worker ok`, `pipeline ok`; not strict (a PR need not be up to date) |
| Actions secrets (names) | `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_API_TOKEN`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` |
| Actions variables, environments, deploy keys, webhooks, Dependabot secrets, self-hosted runners, autolinks, releases | none |
| Actions permissions | enabled, all actions allowed, default workflow token = read, workflows cannot approve PRs |
| Workflows (7, all active) | app, docs, feeds-hydat, feeds-runtiming, feeds worker, mutation, pipeline |
| Collaborators | the owner (admin) and one second account with write. After a transfer a collaborator becomes an "outside collaborator" of the org. **If the org requires two-factor, an outside collaborator without it is removed**: check that account before the click, or drop it |
| Open PRs at snapshot | #207, #209, #212 (all auto-merge; they move with the repo) |

The org today (`gh api orgs/canifishthis`): free plan, 1 member (the owner, admin), 0 repos, base permission `read`,
**two-factor not required yet**. The name `canifishthis/canifishthis` is free. The org's Actions settings need the
`admin:org` scope to read with `gh` (the owner's `gh` has `read:org`): look in the browser, or
`gh auth refresh -h github.com -s admin:org`.

After the move, run the same calls against `repos/canifishthis/canifishthis` and diff (the rulesets keep their ids).

## 2. Everything that names the repo or the owner's account

331 lines in tracked files match the repo name, the account name or the home folder. Sorted by what to do:

**Changes in the slug flip (the draft PR):**
- `app/packages/core/src/repo.ts` `REPO` (feeds `REPO_URL` / `REPO_BLOB` in `menu/licences.ts`: the licence links in
  the app and site; and `workers/feeds/src/dispatch.ts`: the worker's workflow starts)
- `scripts/ci_paths.py` `REPO` (`ci_paths.py repo`: `deploy/staging/upload.sh`, `scripts/start-workflow.sh`)
- tests: `app/packages/core/src/menu/legal.test.ts`, `app/workers/feeds/test/dispatch.test.ts`
- `handoff/PLAN/runbooks/fresh-machine.md`: the clone URL and the `cd` line

**Takes effect only when rebuilt or redeployed (the flip alone does not reach them):**
- the two feeds workers (staging, production): `dispatch.ts` carries the address compiled in. Until each is deployed
  again it calls the old address and depends on GitHub's redirect (see section 4).
- the shipped phone apps and the live site: their licence links point at the old address and rely on GitHub's
  redirect until the next release. The redirect holds as long as nobody creates a repo at the old name.

**Stays (history, or not the GitHub address):**
- `app/tools/check-commits.test.ts`, `app/tools/commits-contract.json`: old merge subjects quoted as test data
- `app/tools/maplibre-android/archive-t420/upstream/README.md`: a fork branch on the personal account, local paths
- `pipeline/tests/fixtures/ci_status_docs_ci_runs_2026-11-24.json` (114 lines): a recorded API answer
- `pipeline/gauges/feed/forecast.py`, `app/workers/feeds/src/gauges/forecast.ts`: the User-Agent string
  `BC-freshwater-fishing-regulations/1.0`, a product name sent to Open-Meteo, not an address
- every local path (`/Users/…/BC-freshwater-fishing-regulations`, `~/dev/personal/BC-freshwater-fishing-regulations`)
  in runbooks, tickets and old scripts: **the local folder keeps its name**, so these stay true
- chat session names `bc-freshwater-fishing-regulations-NN` and the Claude memory folder: both come from the local
  folder name, unchanged
- about 190 lines in old tickets, hand-off pages and one-off scripts under `handoff/PLAN/`: history

**Outside git, names the repo:**
- `.git/config` of the main checkout: `origin` (one entry, shared by every worktree: section 3)
- `~/.ssh/config`: the host alias `github.com-personal` (names the host and key, not the repo: no change)
- the `gh` login (`HorvathDawson`, scopes `repo`, `read:org`, `gist`, `admin:public_key`): reads the repo from the remote
- GitHub itself: the personal fine-grained token behind `GITHUB_DISPATCH_TOKEN` (section 4)
- Cloudflare, Play, App Store Connect: nothing names the repo (checked in T1041, 2026-11-23)

## 3. Checkouts and worktrees: one command covers all of them

There is one clone on this Mac: the main checkout. Its 41 worktrees (21 chat and lane scratchpads, 9 `~/bcfish-*`,
10 `.claude/worktrees/agent-*`, one temporary `build-apk-wt-*` while a build runs) all share the main checkout's
`.git/config`, so **one command changes the remote for every one of them**:

```bash
git -C ~/dev/personal/BC-freshwater-fishing-regulations remote set-url origin git@github.com-personal:canifishthis/canifishthis.git
git -C ~/dev/personal/BC-freshwater-fishing-regulations fetch origin main     # proves it
```

This edits one line of `.git/config`; it touches no working file, so it is safe while chats work, but it is run only
after the transfer. `~/t416-fresh` (the old fresh-clone proof) has the main checkout's folder as its remote: no change.
No other clone was found under `~/dev/personal` or `~`.

Open chats to tell (roadmap "Who is who"; `ListAgents` gives the live list): planning `-8e`, 0.10.0 `-56`,
iOS release `-06`, store content `-a1`.

## 4. The worker token: commands

Researched read-only 2026-11-25 (Opus lane); nothing was set or started.

**What the worker does with the token** (`app/workers/feeds/src/dispatch.ts`):
- `POST api.github.com/repos/<REPO>/actions/workflows/<file>/dispatches` on `main`; only a 204 counts as started.
  Callers: a newer HYDAT seen during a gauge run (`feeds-hydat.yml`); the Monday 10:40 UTC cron or `POST /run/runtiming`
  (`feeds-runtiming.yml`: new NuSEDS, or the monthly sources refresh when `private/runtiming/src/FETCHED` is missing
  or 30+ days old).
- `GET …/actions/workflows/feeds-hydat.yml` from `POST /run/check-github`: checks the token, starts nothing.
- A failed start is stored in R2 (`private/feeds/dispatch[-refresh]/<wf>.json`), emailed to the owner ("GitHub
  answered <code>"), and retried at most every 6 hours.

**The old address in the deployed workers.** Each worker has `REPO` compiled in until it is deployed again. GitHub
redirects API calls for a moved repo with "a 301 or 307 depending on the type of request"; the worker's `fetch`
follows redirects and keeps the Authorization header; a 307 keeps the POST and its body, a 301 would not. So an
un-redeployed worker with the new token **should** work, but that is inference: the staging proof below is the test.
If `check-github` passes and the start fails with a 3xx/404/405, staging is deployed with the flip before going on.

**Production cannot be proved by a start.** It sends `target: production`, and both workflows have `production`
commented out, so GitHub refuses it today (as before the move). Production is proved with `check-github` only.

**The token** (github.com -> Settings -> Developer settings -> Fine-grained tokens -> Generate new token; only
possible after the transfer, because the repo must be in the org to be picked):
- Resource owner `canifishthis`; Repository access "Only select repositories" -> `canifishthis/canifishthis`.
- Repository permissions: **Actions: Read and write**. Nothing else (Metadata: read is added by GitHub).
- Expiry: at most 366 days in a new org; pick the longest and the planning chat adds an owner item for the date.

**Commands** (from the main checkout folder; a value is never written anywhere):

```bash
# staging: the chat runs it, the owner pastes the value at the prompt
npx --prefix app wrangler secret put GITHUB_DISPATCH_TOKEN -c app/workers/feeds/wrangler.toml
# production: the owner's only
npx --prefix app wrangler secret put GITHUB_DISPATCH_TOKEN -c app/workers/feeds/wrangler.prod.toml
```

`secret put` deploys a new worker version with the secret at once: no redeploy needed for the token.

**Proof on staging** (writes to staging only; the workflow refuses any other bucket):

```bash
T=$(tr -d '\n' < app/workers/feeds/.run-token); S=https://bc-fishing-feeds-staging.horvath-dawson.workers.dev
curl -s -X POST -H "Authorization: Bearer $T" $S/run/check-github   # want ok:true and the HYDAT workflow "active"
curl -s -X POST -H "Authorization: Bearer $T" $S/run/runtiming      # staging has no FETCHED: starts one refresh run
rclone cat r2:bc-fishing-regulations-staging/private/feeds/dispatch-refresh/feeds-runtiming.json --s3-no-check-bucket
gh run list -R canifishthis/canifishthis --workflow feeds-runtiming.yml --event workflow_dispatch --branch main --limit 3
gh run watch <id> -R canifishthis/canifishthis --exit-status
```

**Proof on production** (the owner; writes nothing):

```bash
curl -s -X POST -H "Authorization: Bearer $(tr -d '\n' < app/workers/feeds/.run-token.prod)" https://feeds.canifishthis.ca/run/check-github
```

**Order for step 3** (the owner's order holds; two additions in bold):
1. Before the click: org token policy and Actions setting confirmed; not on a Monday 10:00–11:00 UTC (the cron).
2. Owner: transfer + rename. Owner: the new token.
3. Chat: staging `secret put` (owner pastes), **then `check-github` first**, so a bad token and an unfollowed redirect
   can be told apart; then `/run/runtiming` and the run watched to green.
4. Compare the snapshot (section 1).
5. Merge #217. Deploys build from the checkout they run in, so the flip reaches staging at the next
   `deploy/staging/feeds.sh` from a checkout that has it; **`check-github` once more after that deploy** (the first
   proof ran through the redirect, this one through the new address).
6. Owner: production `secret put`, production `check-github`. Production gets the flipped address at its next
   release deploy; not urgent while the redirect holds.
7. Owner: delete the old personal token (Settings -> Developer settings -> Fine-grained tokens), after both workers
   carry the new one.

**Between the click and the new token:** only workflow starts fail (Monday cron; a new HYDAT, none pending). Every
other feed job is unaffected. The owner may get one "could not be started" email.

**Not verified:** that the POST gets a 307 today and the worker re-sends its body; that the old token stops working
at the transfer (T1041 says so). Both are settled by step 3.

## 5. The merge queue: the ruleset change (the owner's OK before it is applied)

Merge queues exist only for repos owned by an organisation, which is why this follows the move. The four CI
workflows already run on `merge_group` (`app-ci`, `docs-ci`, `feeds-worker-ci`, `pipeline-ci`), so the four required
checks report inside the queue. `mutation.yml` leaves it out on purpose (it is not a required check).

The change: one rule added to ruleset `14082789` "main trunk (T995)", nothing else in it altered:

```json
{ "type": "merge_queue",
  "parameters": {
    "merge_method": "MERGE",
    "grouping_strategy": "ALLGREEN",
    "max_entries_to_build": 5,
    "min_entries_to_merge": 1,
    "max_entries_to_merge": 5,
    "min_entries_to_merge_wait_minutes": 0,
    "check_response_timeout_minutes": 60 } }
```

- `MERGE`: merge commits, as today. `ALLGREEN`: every PR in a group must pass on its own, so one bad PR is put out
  and the rest carry on. `min 1 / wait 0`: a lone PR merges as soon as its checks pass; nothing waits for company.
- `check_response_timeout_minutes: 60`: the last 30 green runs of each (2026-11-25): pipeline median 0.9, longest
  26.3 minutes; app 6.4 / 8.3; docs 1.5 / 1.9; feeds worker 0.7 / 3.4. None has run on `merge_group` yet: the first
  queued PR is the proof.
- Applied with `gh api -X PUT repos/canifishthis/canifishthis/rulesets/14082789 --input <file>`, where the file is
  the current ruleset's `name`, `target`, `enforcement`, `conditions`, `bypass_actors` and `rules` plus the rule above.
  Undo: the same call without the rule.
- What changes for lanes: `gh pr merge --merge --auto` puts the PR in the queue; nobody merges `main` into a stacked
  PR any more. `AGENTS.md` W47 and `START-HERE.md` step 6 get one line each when it is on.
- The admin bypass (pull-request mode) stays as it is.

## 6. Actions stays free

Every job in all 7 workflows runs on `ubuntu-latest` (48 `runs-on` lines, no larger runner, no macOS or Windows, no
self-hosted runner, no container service). Standard runners on a public repo cost nothing, in an org as on a
personal account. Third-party actions used: `pnpm/action-setup`; the rest are GitHub's own (`actions/*`). The org's
Actions policy must allow them (section 7).

## 7. The org's settings (the owner's clicks, github.com/organizations/canifishthis/settings)

1. **Authentication security** -> "Require two-factor authentication for everyone": on. (Your own account must have
   it; see the collaborator note in section 1.)
2. **Member privileges** -> Base permissions: `Read` (already so). Repository creation: off for members is tidier
   (you are an owner, it does not bind you).
3. **Actions -> General**: "Allow all actions and reusable workflows" (as the repo today). Workflow permissions:
   "Read repository contents and packages permissions"; "Allow GitHub Actions to create and approve pull requests":
   off. Fork pull request workflows: leave "Require approval for first-time contributors".
4. **Personal access tokens -> Settings**: fine-grained tokens "Allow access via fine-grained personal access tokens";
   approval required or not is your choice (an owner's own request is approved at once). Classic tokens: "Restrict"
   is safer and nothing here uses one except your own `gh` login, which is OAuth, not a classic token.
5. **Code security** (optional, free on public repos): secret scanning and push protection as defaults for new repos.
   The transferred repo keeps its own settings.

## 8. T1120: what lives outside git

`OUTSIDE-GIT.md` beside this page (2026-11-25, names only): 97 rows in seven groups, each with how it comes back,
and 17 finds: things that work today only because of this Mac, or where `fresh-machine.md` / `secrets.md` are stale.
The ones most likely to stop the fresh clone (T1120 step 2): `data/source/fwa_coast.gpkg` is read by the atlas but
is neither pinned nor fetched by `bootstrap.sh`; bootstrap does no `pnpm install` and sets no hooks; no page lists
the phone toolchain (JDK, SDK, NDK, Xcode, CocoaPods); the pinned MapLibre AAR is built, not fetched. The page ends
with the secrets the owner confirms he can restore.
