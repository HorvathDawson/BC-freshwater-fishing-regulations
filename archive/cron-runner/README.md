# bc-fishing-cron

A tiny Cloudflare **Worker** on a **Cron Trigger** that dispatches the project's
GitHub Actions workflows on a **reliable clock**. It's a free-tier replacement
for GitHub's own `schedule:` trigger, which drifts 20–30 min under load (GHA's
scheduler is the flaky part — the runners are fine once dispatched).

No container, no image, no Durable Object — just a Worker that makes a few HTTPS
calls to the GitHub REST API (`workflow_dispatch`) and exits. **GHA still does
all the actual work** (fetch/export/upload).

## How it works

- `src/index.ts` — the Worker. Its `scheduled()` handler POSTs a
  `workflow_dispatch` for each workflow named in `WORKFLOWS`. GHA runs the job.
- `wrangler.toml` — the `[triggers]` cron (punctual), plus `[vars]` for the
  target repo/branch/workflows. Secrets hold the GitHub token.

## Config

`[vars]` in `wrangler.toml`:

- `GITHUB_OWNER`, `GITHUB_REPO` — the repo hosting the workflows.
- `GITHUB_REF` — branch to run on (`main`).
- `WORKFLOWS` — comma-separated workflow **file names** to dispatch each tick
  (e.g. `update-hydro.yml`). The unified hydro workflow gates its own cadence
  internally, so triggering every 15 min is fine.

Secrets:

- `GITHUB_TOKEN` — a fine-grained PAT scoped to this repo with **Actions:
  read/write** (the only permission `workflow_dispatch` needs).
- `TRIGGER_TOKEN` — optional; guards the manual `GET /__run?token=…` endpoint.

## Deploy

```bash
cd cron-runner
npm install

npx wrangler secret put GITHUB_TOKEN
npx wrangler secret put TRIGGER_TOKEN   # optional

npx wrangler deploy
```

## Verify

- **Manual kick (no waiting):** `curl "https://<worker-url>/__run?token=<TRIGGER_TOKEN>"`
  → returns the dispatch results (each workflow → HTTP 204 on success). Then
  check the repo's **Actions** tab for the newly-started run.
- **Logs:** `npx wrangler tail` — look for `dispatched <workflow> … → 204`.

## Rollback

`npx wrangler delete` this Worker and GHA's own `schedule:` continues untouched —
zero-risk. (The GHA `schedule:` is left in place as a coarse fallback.)
