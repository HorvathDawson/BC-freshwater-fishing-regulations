order: 700

## 2026-11-23 ~15:00: T1041 faster CI checks (answers, verbatim) — one item for you, AFTER 0.9.0
- Local checks: "Both (Recommended)" — related tests before each push + full app suite and pipeline tier once before
  opening the PR. Built: the pre-push hook runs `vitest related` + the pipeline tests importing a changed module;
  `scripts/before_pr.sh` runs both whole suites.
- Mutation in CI: "Report only (Recommended)" — on changed lines, Stryker for app TS, mutmut for Python; non-blocking.
  Built: `.github/workflows/mutation.yml` (not a required check).
- PR chains: "Merge queue (Recommended)". Timing: "All now". Merge queue needs an org: "Move to a free org
  (Recommended)" — the transfer is your click.
- Later the same day: "i think we should do repo move after m0.9.0" — so the move, the slug flip and the merge-queue
  ruleset are a post-0.9.0 follow-up; only the `merge_group` triggers went in now (they do nothing until a queue exists).
- **Do (you), any time, one line: keep the push connection alive** (T1041: GitHub closed git's ssh connection under a
  420 s pre-push hook; with a keepalive the same push went through). In the main checkout:
  `git config core.sshCommand "ssh -i ~/.ssh/id_ed25519_personal -o ServerAliveInterval=15 -o ServerAliveCountMax=8"`
  (every worktree shares it). Or add `ServerAliveInterval 15` under `Host github.com-personal` in ~/.ssh/config.
- **Do (you), after 0.9.0 ships: move the repo to a free organisation** (details: T1041, "Transfer checklist"):
  1. Create a free GitHub organisation (github.com/organizations/plan, Free); you are its owner.
  2. Repo Settings -> General -> Danger zone -> Transfer -> the organisation (keep the name).
  3. Re-issue the feeds worker's `GITHUB_DISPATCH_TOKEN`: a new fine-grained token, resource owner = the organisation,
     only this repo, Actions: read and write. Put it on BOTH feeds workers:
     `npx --prefix app wrangler secret put GITHUB_DISPATCH_TOKEN -c app/workers/feeds/wrangler.toml` (staging) and
     `... -c app/workers/feeds/wrangler.prod.toml` (production); then revoke the old token (runbooks/secrets.md).
  4. Check the 4 Actions secrets (CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_TOKEN, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY)
     and the 3 rulesets are still on the repo (Settings -> Secrets / Rules).
  5. On your machine: `git remote set-url origin git@github.com-personal:<org>/BC-freshwater-fishing-regulations.git`.
  6. Tell Claude the org name: the slug flip (repo.ts + ci_paths.py + 4 tests) and the merge-queue rule go in as one PR.
