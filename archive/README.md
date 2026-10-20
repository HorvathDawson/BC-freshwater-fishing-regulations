
## v1 apps (archived 2026-08-28)

`webapp/`, `mobile/`, `r2-worker/` — the v1 clients and range-serving worker. The new app is
greenfield in `app/` and inherits no code from them (see `pipeline/docs/13-build-plan.md` §4).

**Before the next deploy** the Cloudflare git integration must be repointed: it currently runs
`cd webapp && npm install && npm run build` and serves `webapp/dist/` (see `DEPLOY.md`). Until then
`main` still builds — this move is on a branch.

`cron-runner/` and `scripts/` archived too — the recurring jobs they drive
(`.github/workflows/update-*.yml`) already import `pipeline.recurring.*`, which was archived
earlier, so those workflows are legacy. The new live-feed design is 13-build-plan §5 / step 10.

**Left at root:** `DEPLOY.md` (still the record of how v1 was wired). The `.github/workflows/update-*.yml` crons
were removed 2026-10-20 (P04, owner audit decision #3): they ran code that no longer exists. Successors: the P3-CRONS
plan (`handoff/PLAN/P3-CRONS.md` in the memory project directory).
