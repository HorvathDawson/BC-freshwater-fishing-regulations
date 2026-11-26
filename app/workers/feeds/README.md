# workers/feeds: the feeds Cloudflare Worker

Publishes the live data the app reads besides the release: gauge readings and forecasts (`src/gauges/`), DFO salmon
notices (`src/dfo/`), the province's in-season changes (`src/inseason.ts`), the run-timing source check
(`src/runtiming.ts`), plus push wake-ups (`src/push.ts`), crash and bug reports (`src/crashes.ts`, `src/reports.ts`), the
daily backup (`src/backup.ts`), status (`GET /status`, `src/status.ts`) and email notifications (`src/events.ts`). The
Python twins it is held to are `pipeline/gauges/feed/publish.py` and `pipeline/regs/dfo_salmon/live.py` (`parity/`, `test/`).

- Entry: `src/index.ts` (fetch handler: `POST /run/gauges|dfo|inseason` with the bearer run token; scheduled handler: the
  `[triggers]` crons in `wrangler.toml`). Dev: `pnpm exec wrangler dev -c wrangler.toml` (`package.json` `dev`).
- Reads: R2 (`BUCKET`, the staging or production bucket) for `private/feeds/*` state and `feeds/live/clim.json`; the upstream
  sites (ECCC, DFO, BC in-season pages).
- Writes: R2 `feeds/live/`, `feeds/dfo/`, `feeds/inseason/` (published; served by the data worker) and
  `private/feeds/`, `private/reports/` (private).
- Config: `wrangler.toml` (staging) and `wrangler.prod.toml` (production); `test/config.test.ts` holds the two to the
  same bindings and crons. Deploy: `deploy/staging/feeds.sh`; production is `deploy/prod/feeds.sh` (owner only, AGENTS A7).
- Not in the pnpm workspace (no lockfile churn): it resolves typescript and vitest from `app/node_modules`.

## Rules

Never a GitHub schedule: the worker's crons start workflows (`feeds-hydat.yml`, `feeds-runtiming.yml`) or email the
owner. `src/gauges/policy.generated.ts` and `glyphs.generated.ts` are generated (`pipeline.tools.emit.gauge_policy`,
`tools/card_glyphs.py`); never edit them.

## Tests

`pnpm exec vitest run --root workers/feeds` from `app/` (CI: `feeds-worker-ci.yml`, required check `feeds worker ok`).
