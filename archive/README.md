
## v1 apps (archived 2026-08-28)

`webapp/`, `mobile/`, `r2-worker/` — the v1 clients and range-serving worker. The new app is
greenfield in `app/` and inherits no code from them (see `pipeline/docs/13-build-plan.md` §4).

**Before the next deploy** the Cloudflare git integration must be repointed: it currently runs
`cd webapp && npm install && npm run build` and serves `webapp/dist/` (see `DEPLOY.md`). Until then
`main` still builds — this move is on a branch.

`cron-runner/` and `scripts/` archived too — the recurring jobs they drive
(`.github/workflows/update-*.yml`) already import `pipeline.recurring.*`, which was archived
earlier, so those workflows are legacy. The new live-feed design is 13-build-plan §5 / step 10.

**Left at root:** `.github/workflows/update-*.yml` (dead until repointed — delete or rewrite when
step 10 lands) and `DEPLOY.md` (still the record of how v1 was wired).
