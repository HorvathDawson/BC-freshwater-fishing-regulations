# Doing a release yourself

Written 2026-11-18 for the owner: a whole release, start to finish, with no Claude session. The same page is a
Claude doc: https://claude.ai/artifact/5r4yEqNwqygin1oKo5Zxjk (keep the two in step). The full references behind it are
`handoff/PLAN/runbooks/release.md` (the data release, every flag) and `handoff/PLAN/release/PRODUCTION.md`
(production and rollback).

## What a release is made of

A release has up to five pieces. The data always goes first; the others go only when they changed. Every command below
runs in a terminal from the repo folder (`~/dev/personal/BC-freshwater-fishing-regulations`).

| Piece | What it is | Needed when | Staging command |
| --- | --- | --- | --- |
| Data release | The rules, waters, tiles and maps the app downloads: one folder, `data/release/r-…` | The regulations or curated data changed | `.venv/bin/python -m pipeline release --staging` |
| Staging website | The web app and water pages at staging.canifishthis.ca | After every data release, and when app code changed | `deploy/staging/site-all.sh` |
| Feeds worker | The Cloudflare worker for gauges, salmon openings, closed areas and reports | Its code changed, or the release adds closed areas | `deploy/staging/feeds.sh` |
| App build | Android on Play internal testing, iPhone on TestFlight | The app code changed (a new version, like 0.8.0) | `deploy/staging/build-apk.sh --play` and `deploy/staging/build-ios-testflight.sh` |
| Production | The same data, site and worker on canifishthis.ca, plus the production app | After you have checked staging | see Step 7 |

**Build once, promote the same thing.** Production re-uploads the exact release folder staging served. Nothing is
rebuilt for production, and `--prod` refuses if staging serves anything different.

**When the data needs a newer app** (0.8.0's new trout tables are the example), the release is stamped with the oldest
app that can read it. Older apps keep the data they already have and say "Update the app to get new regulations". They
never break.

## Before you start

Everything is already set up on this Mac. A two-minute check saves a stopped run later.

1. **Up to date and clean.** `git pull` on branch `redesign/stream-sections`. Code and `data/curated/` must be committed
   and pushed (the release refuses otherwise). Use pull, never `git stash` or `git reset --hard`: your review files live
   uncommitted in this folder.
2. **CI is green.** `gh run list --branch redesign/stream-sections -L 3`. The release refuses while CI is red. A flaky
   failure: `gh run rerun <id> --failed`.
3. **Logins answer.**
   - GitHub: `gh auth status`
   - Cloudflare: `npx --prefix app wrangler whoami` (if expired: `npx --prefix app wrangler login`)
   - R2: `rclone listremotes` shows `r2:`
4. **Secrets are in `~/.canifishthis/`**: the Play service account, the upload keystore, the App Store Connect key and
   the iOS team id. They are there today. A new Mac: `handoff/PLAN/runbooks/fresh-machine.md`. Every secret, where it
   lives and how to replace it: `handoff/PLAN/runbooks/secrets.md`.
5. **The machine is free.** Big jobs wait their turn (the scripts queue themselves through `scripts/heavy.sh`), but two
   at once is slow. Don't start an app build while a release runs. Close big apps.
6. **Disk space.** Test leftovers can fill the disk (151 GB once). If space is low, delete old `release-open-*` folders
   in `$TMPDIR`.

## Steps 1–2: the data release to staging

One command builds the release, checks it, makes it the current one (LATEST) and puts it on staging. When it stops, fix
what it names and **run the same command again**: it picks up where it stopped and remembers its flags.

1. **Rehearse** (about 15 min, builds nothing):

   ```
   .venv/bin/python -m pipeline release --staging --dry-run
   ```

   It ends with every stop the real run would hit now, as one list. Clear them first.
2. **Run it** (about 1–1½ h):

   ```
   .venv/bin/python -m pipeline release --staging --push
   ```

   `--push` lets it push its own commits. Add `--min-app-version 0.8.0` (or whichever version) when the data needs a
   newer app; otherwise it keeps the live release's stamp. It prints its plan first, then seven stages: preflight,
   baseline, build, checks, follows, promote, staging upload.
3. **Done** when it prints that the staging upload and its smoke test passed. `data/release/LATEST` now names the new
   `r-…` id. Note that id: production needs it.

When it stops, the message has a `next:` line. The usual stops:

| Stop | What it means | What you do |
| --- | --- | --- |
| Preflight list | Something isn't committed or pushed, CI is red, or the quick checks (fast-fail) found a broken test | Commit and push; wait for CI. A fast-fail failure is broken code (see Step 8) |
| Binding diff (stage 4) | Some rules now apply to different waters than in the live release | Read `data/generated/release/checks/<r-id>/binding_diff.txt`. If every change is one you meant, rerun with `--accept-diff <the .json path it prints>`. If not, don't ship: find the cause |
| Golden page changed (stage 5) | What the app tells an angler changed on one of the sample pages | Read `git diff -- app/packages/ui-native/src/regs/fixtures`. If every change is intended, rerun with `--accept-fixtures <token>` (the token is printed) |
| A test fails on the new release (stage 5) | The app code doesn't agree with the new data. This happened on 2026-11-18 | Needs a code fix (Step 8) |
| Commits not pushed (stage 5) | You ran without `--push` | Run the `git push` it prints, then rerun |
| Upload failed (stage 7) | Cloudflare login, network, or GitHub Actions didn't answer | Fix the login or wait, then rerun. It skips straight to the upload |

A stopped release does no harm. It sits on disk, named in `data/release/CANDIDATE`, and nothing ships it. Staging and
the phones keep the old release until a run finishes.

## Steps 3–4: staging website and feeds worker

**Website, after every data release** (about 20 min):

```
deploy/staging/site-all.sh
```

It runs quick checks first, then builds the web app and the water pages on the new release, deploys them to
staging.canifishthis.ca and runs a smoke test that prints which release the site shows. If only app code changed and
the data didn't, `deploy/staging/site-all.sh --relink` is faster. `--dry-run` deploys nothing.

**Feeds worker, only when it changed** (5–10 min):

```
deploy/staging/feeds.sh
```

It runs the worker's own tests, deploys, then runs every feed job once so the new output is there now. It's needed when
files under `app/workers/feeds/` changed since its last deploy (`git log --oneline -5 -- app/workers/feeds`). It's also
needed when the release adds closed areas: then deploy the worker **before or together with** the data, never after.

## Step 5: check it yourself

Look at staging before anything goes further. The checks above prove the files are whole; only a look proves they read
right.

- **Web:** open staging.canifishthis.ca. Check a handful of waters you know: Vedder, Chilliwack Lake, a Region 4 lake,
  the Bowron, the Puntledge.
- **Phone (Android):** `deploy/staging/build-apk.sh` (about 30 min) builds an app that reads staging and emails you the
  link. The page is https://data-staging.canifishthis.ca/app/android/index.html. Install it over the old one and check
  the same waters, offline too.
- **Emulator test bench (optional; this is what Claude runs):**

  ```
  scripts/heavy.sh testbed 30 -- bash app/tools/testbed/session.sh --build --crash-stub -- node app/tools/testbed/run.mjs regs-open regs-switch water-nav trout-table
  ```

  The report lands in `app/tools/testbed/out/<run>/report.md`, with screenshots. Every scenario is listed in
  `app/tools/testbed/README.md`.

If something reads wrong, stop here. Production waits, and the next release fixes staging.

## Step 6: the app build (Play internal and TestFlight)

Only when the app code changed. Both builds use the committed code on GitHub, never uncommitted edits.

1. **Version.** `"version"` in `app/apps/mobile/app.json` (0.8.0 on 2026-11-18). A new app version means a new number
   here.
2. **What's new.** Add an entry for that version at the top of `app/packages/core/src/changelog.ts`: 3–5 plain bullets
   for anglers. They become the Play release notes and the in-app "What's new". Commit, push and wait for CI to go
   green.
3. **Precheck** (a minute; lists everything missing at once):

   ```
   deploy/staging/build-apk.sh --play --precheck
   ```
4. **Android to Play internal** (about 1 h including its checks):

   ```
   deploy/staging/build-apk.sh --play
   ```

   It raises the build number (versionCode) and commits only that, builds and checks the app bundle, uploads it to the
   internal track and emails you the tester link. Then install it from Play on your phone. If the upload worked but the
   email step failed: `deploy/staging/build-apk.sh --play --publish-only`.
5. **iPhone to TestFlight** (about 30 min, then Apple takes 5–30 min to process):

   ```
   deploy/staging/build-ios-testflight.sh
   ```

   It builds, signs with the App Store Connect key and uploads. The build number raises itself. `--dry-run` shows the
   steps first.

**Before the first production launch**, Play and TestFlight builds read staging data. After it, every Play build is
production: `CONFIRM_PROD=canifishthis.ca deploy/prod/build-apk.sh` (Step 7), and staging keeps the sideload app.

## Step 7: production

Production is yours alone. Every production command refuses without `CONFIRM_PROD`, and the data step also wants you to
type the release id again in your own terminal (it refuses inside a Claude session).

**The first launch is a one-time sequence.** Production hasn't been published yet (`data/release/PUBLISHED` doesn't
exist on 2026-11-18). The first time, follow "The first transition" in `handoff/PLAN/release/PRODUCTION.md`, in its
order. It covers the buckets, the data worker, the data, the feeds worker, the site, the www redirect, the app, search
engines, and retiring the old v1 setup.

**Every release after that:**

1. **Feeds worker first, if the release adds closed areas or the worker changed:**

   ```
   CONFIRM_PROD=bc-fishing-feeds deploy/prod/feeds.sh
   ```
2. **Data** (the same `r-…` that staging serves):

   ```
   deploy/prod/upload.sh <r-id> --bucket bc-fishing-regulations-staging --dry-run   # rehearse, sends nothing
   CONFIRM_PROD=<r-id> .venv/bin/python -m pipeline release --prod <r-id>
   ```
3. **Site, if pages or the web app changed:** build both halves, then deploy:

   ```
   EXPO_PUBLIC_CONTACT=support@canifishthis.ca EXPO_PUBLIC_APP_ENV=production \
     pnpm -C app --filter @app/mobile exec expo export --clear --platform web --output-dir ../../dist/web-prod
   scripts/heavy.sh seo-prod 30 -- node app/tools/build-seo.mjs --release-dir data/release/<r-id> \
     --app app/dist/web-prod --threads 4 --out app/dist/seo-prod
   CONFIRM_PROD=canifishthis.ca deploy/prod/site.sh
   ```
4. **App, if it changed:** `CONFIRM_PROD=canifishthis.ca deploy/prod/build-apk.sh`, then in Play Console move that build
   to Closed testing or Production with a staged rollout (for example 20%, then 100%).
5. **Watch for a day:** `node app/tools/smoke-staging.mjs --production`,
   `curl -s https://feeds.canifishthis.ca/status`, and the `[production]` emails (crashes, feed health).

## Step 8: when something breaks

Most stops are a login, a judgment call or a rerun. A few need a code change. In either case nothing reaches anglers
until the checks pass.

| Kind | Examples | What you do |
| --- | --- | --- |
| Fix it yourself | Expired login; flaky CI (`gh run rerun <id> --failed`); disk full; network; a binding diff or golden page you need to judge | Fix it, rerun the same command |
| Needs a code change | A fast-fail step fails; a test fails on the new release; CI is red for real; a build stage fails (`python -m pipeline build --status` shows which and its log) | Fix the code, or hold the release. Holding is always safe: the live data stays as it is |

The 0.8.0 release (2026-11-18) hit the second kind: the new data changed a few test counts, and the tests had to be
updated to match. Expect at least one of these on a release that changes a lot.

**Rollback (production):**

- **Data:** `CONFIRM_PROD=canifishthis.ca deploy/prod/rollback.sh` puts the previous release back. Then run
  `echo <previous r-id> > data/release/LATEST`. A promote never deletes the previous release.
- **A worker** (feeds, data, site): `npx --prefix app wrangler deployments list -c <its config>`, then
  `npx --prefix app wrangler rollback <version-id> -c <its config>`. The configs are
  `app/workers/feeds/wrangler.prod.toml`, `deploy/prod/data.wrangler.toml` and `deploy/prod/site.wrangler.toml`.
- **The app:** Play Console → the release → Halt rollout, then ship a fixed build with a higher number.

Staging has no rollback. The next release replaces it.

## How long it takes, and a checklist

About 3–4 hours from start to staging plus app builds, mostly waiting, when nothing needs a code fix. Production adds
about an hour, plus a day of watching.

| Step | About |
| --- | --- |
| Rehearse | 15 min |
| Data release to staging | 1–1½ h |
| Staging website | 20 min |
| Feeds worker (if needed) | 5–10 min |
| Your check (sideload app + web) | 30–60 min |
| Play internal build | 1 h |
| TestFlight build | 30 min + Apple's 5–30 min |

- [ ] `git pull`; everything committed and pushed; CI green
- [ ] Logins: `gh auth status`, `npx --prefix app wrangler whoami`, `rclone listremotes`
- [ ] `.venv/bin/python -m pipeline release --staging --dry-run`, then clear its list
- [ ] `.venv/bin/python -m pipeline release --staging --push` (+ `--min-app-version X.Y.Z` if needed); rerun until done;
      note the `r-…` id
- [ ] `deploy/staging/feeds.sh` (only if the worker changed or closed areas were added)
- [ ] `deploy/staging/site-all.sh`
- [ ] Check on web and phone (`deploy/staging/build-apk.sh` for the sideload app)
- [ ] New app version: `app.json` version + `changelog.ts` entry, pushed, CI green
- [ ] `deploy/staging/build-apk.sh --play --precheck`, then `deploy/staging/build-apk.sh --play`
- [ ] `deploy/staging/build-ios-testflight.sh`
- [ ] Production (Step 7): feeds worker if needed, data, site, app, then watch for a day
