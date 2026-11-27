# deploy: uploads, site, workers and store builds

Shell, Python and Node scripts that put a built release, the site, the workers and the app builds where people read them
(Cloudflare R2 + Workers, Play, App Store / TestFlight). Production is the OWNER'S: `CONFIRM_PROD` is typed only by him (AGENTS A7); an
agent runs only the staging scripts (and only as the owner's memory notes allow).

| path | what | entry |
|---|---|---|
| `staging/upload.sh` | a release + feeds + sql.js wasm to the staging bucket in `publish.py`'s order, the manifest LAST | `deploy/staging/upload.sh [r-<id>]` (run by `python -m pipeline release --staging`) |
| `staging/site.sh`, `site-all.sh`, `check-public.mjs` | the static site and its checks | |
| `staging/feeds.sh`, `data-worker.js`, `*.wrangler.toml` | the feeds worker and data worker deploys | |
| `staging/build-apk.sh`, `build-ios-*.sh` | release builds against staging data (R8 on, version stamp); APK to the staging page | |
| `staging/play-*.py`, `appstore-screenshots-push.py`, `testflight-*.mjs` | store listing, upload and TestFlight tooling | |
| `staging/test-updates.mjs` | the test channel the Android test bed drives | |
| `prod/upload.sh` | promotes the release staging serves to the production bucket (same id, same manifest), pointer LAST | `CONFIRM_PROD=<r-id>` by the owner only |
| `prod/{site,feeds,rollback,drop-v1}.sh`, `host.wrangler.toml` | production site, feeds worker, rollback, the v1 layout drop | owner only |
| `lib/` | shared helpers: bucket names, R2 objects, input formats, build links, site stamp | sourced by the scripts above |

- Reads: `data/release/<r-id>/` (and `LATEST`), `app/` builds. Writes: the R2 buckets named in each script (a staging script hardcodes
  its staging bucket and never names a production one).
- Rules: deploy scripts `set -o pipefail` and never `|| true` (AGENTS W46); never a GitHub schedule for feeds (a worker cron starts a workflow).
  Runbook: `docs/runbooks/release.md`.
- Tests: `deploy/staging/*.test.mjs` (`node --test`, run by `pipeline/tests/test_deploy_node_scripts.py`), `pipeline/tests/test_deploy_gates.py`.
