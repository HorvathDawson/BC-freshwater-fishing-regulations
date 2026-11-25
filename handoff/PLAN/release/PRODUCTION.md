# Production releases (T404)

Production is canifishthis.ca (site), data.canifishthis.ca (bucket `bc-fishing-regulations`, worker `bc-fishing-r2`),
the feeds worker `bc-fishing-feeds` and the Play app built with `EXPO_PUBLIC_APP_ENV=production`.

**Who does what** (the runbook marks each step): everything that needs `CONFIRM_PROD` is the OWNER's: the production
data (`release --prod`, `deploy/prod/upload.sh`), the production website (`deploy/prod/site.sh`), the production feeds
and data workers, the production app builds and a rollback; so are the Play production track and the App Store. Owner
2026-11-22: "only me unless i say otherwise that time". Claude prepares each (staging, tests, smoke, the dry run) and
hands over the command; it runs one only when the owner says so, that one time. `release --prod` refuses any agent.
Every rehearsal (`--dry-run`, or `--bucket <non-prod>`) writes nothing to production and needs no confirmation.

**The one switch.** The app picks its environment from `EXPO_PUBLIC_APP_ENV` (eas.json, `build-apk.sh --production`).
Every host comes from `app/apps/mobile/src/env.ts` (data, tiles, map assets, feeds, synopsis, sql.js, `play.json`,
reports, crashes); the public site is `@app/core` `SITE_ORIGIN`. `app/tools/no-env-hosts.test.ts` fails on a host spelt
anywhere else in app code.

| piece | staging | production |
|---|---|---|
| data upload | `deploy/staging/upload.sh` | `deploy/prod/upload.sh` (promotes what staging serves) |
| data rollback | — | `deploy/prod/rollback.sh` |
| data worker | `deploy/staging/data.wrangler.toml` | `deploy/prod/data.wrangler.toml` (same `data-worker.js`) |
| feeds worker | `deploy/staging/feeds.sh` + `wrangler.toml` | `deploy/prod/feeds.sh` + `app/workers/feeds/wrangler.prod.toml` |
| worker inputs | `deploy/staging/sync-inputs.sh` | `deploy/prod/sync-inputs.sh` (called by upload) |
| site | `deploy/staging/site.sh` | `deploy/prod/site.sh` |
| Play build | `deploy/staging/build-apk.sh --play` | `deploy/prod/build-apk.sh` (= `--play --production`) |
| smoke | `node app/tools/smoke-staging.mjs` | `node app/tools/smoke-staging.mjs --production` |

## A production release

The steps, in order, with who runs each: `handoff/PLAN/runbooks/release-by-hand.md`, Step 7 (and Step 6 for the
phones). The order rules the gates enforce:

- **The data goes first**, then the website, then the feeds worker when its code changed (owner 2026-11-21).
- **The website goes before the data** when the release's minimum app is newer than the production site:
  `deploy/prod/upload.sh` asks the production site's version first (T994 #10) and refuses ("the production site is app
  X, older than Y"). `PROD_SITE_CHECK_SKIP=<reason>` skips it, printed (a staging `SITE_CHECK_SKIP` never does).
- **The feeds worker goes before the data** when the release binds closed-area pieces the production worker does not
  serve yet (T790 item 7): the upload's pieces gate asks `https://feeds.canifishthis.ca` and refuses.
- **The data goes before or with the phones** when the new app reads new data fields.

**Store pictures (T704, T1076)**, from the release checkout, before the Play listing is updated:
`scripts/heavy.sh store-shots 30 -- node app/tools/store-shots/run.mjs` (no arguments; headless Chrome, no credits). It
builds its OWN store-shots web export of the checkout (`EXPO_PUBLIC_STORE_SHOTS=1`, into `app/dist/web-store-shots`, the
staging app environment and live feeds; a few minutes), serves it on a local port, re-shoots the 8 Play phone pictures
from it into `app/dist/store-shots/play-phone/` and FAILS (exit 1, `report.json` says which scene) when a
shot's content is missing: salmon above the game-fish calendar, the forecast on the chart, more than one part on the
river. Look at them, then upload to Play Console. `--reuse` serves the export already built (no rebuild); `--site URL` points at another store-shots build and is refused at
once, with the instruction, when that site lacks the `store-shots-build.json` marker (staging and production never have it).

The one-time first transition (buckets, the data worker, the www redirect, search engines, retiring v1) was done
2026-11-18/19 and is removed from this page; it is in git: `git show a3ca419e2:handoff/PLAN/release/PRODUCTION.md`.

## Rollback

- **Data:** `CONFIRM_PROD=canifishthis.ca deploy/prod/rollback.sh` (the previous pointer back; it checks every file it
  names is still there), then `echo <previous r-id> > data/release/LATEST`. A promote never deletes the previous release
  (T865: it deletes only the releases neither pointer names; promoting the live release again leaves the previous
  pointer as it is).
- **A worker** (feeds, data, site): `npx --prefix app wrangler deployments list -c <its config>`, then
  `npx --prefix app wrangler rollback <version-id> -c <its config>`. Configs: `app/workers/feeds/wrangler.prod.toml`,
  `deploy/prod/data.wrangler.toml`, `deploy/prod/site.wrangler.toml`.
- **The app:** Play Console → the release → Halt rollout; ship a fixed build (a higher versionCode). Our `play.json`
  only adds the "What's new" bullets once Play offers that build, so a halted build announces nothing.

## Clean-up (T865, automatic)

R2's free tier is 10 GB. After every promote, `deploy/prod/upload.sh` runs `pipeline.deliver.release.prod_prune`:
- **releases**: every object under `release/r-*/`, `release/tiles/*/`, `release/basemap/*/` whose folder neither
  `release/manifest.json` nor `release/manifest.previous.json` names is deleted (an allowlist: `feeds/`, `private/`,
  `runtiming/`, `map-assets/`, `app/`, `*.wasm`, `feedback/` are never candidates). Refused, nothing deleted, exit 1:
  a pointer it cannot read or parse, a file a kept manifest names that is missing, both pointers naming one release.
  A folder no pointer names but that went up under 24 h ago (server time) is kept, and said: a web tab opened on a
  release promoted earlier the same day still reads its files. It prints each key once deleted and the bytes freed
  (a delete that stops part way says how many went); `--dry-run` prints the plan as the pointer step would leave it.
- **v1**: not automatic; `deploy/prod/drop-v1.sh` (done 2026-11-18: 20,019 objects, 8.57 GB, OWNER-TODO).
- Every `deploy/prod` script compares `--bucket` bare (`deploy/lib/bucket.sh`): `bc-fishing-regulations/` or
  `r2:bc-fishing-regulations` still needs `CONFIRM_PROD`.
`runtiming/` is not pruned: its own publisher's content-named files (a few MB a refresh, `pipeline/runtiming/feed/publish.py`).

## Not yet (tickets)

**Owner, left from the first transition:** the production option of `feeds-hydat.yml` and `feeds-runtiming.yml`
(uncomment `- production` and the prod secrets in each; add `PROD_R2_ACCESS_KEY_ID` / `PROD_R2_SECRET_ACCESS_KEY` in
GitHub). Until then the production worker's HYDAT / run-timing starts fail and say so by email.

`pipeline/runtiming/feed/publish.py` names only staging's bucket, so `feeds-runtiming.yml` cannot publish
to production yet; until it can, `deploy/prod/upload.sh` copies staging's run timing for the same tiles.
