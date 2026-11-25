# Doing a release yourself

The one release runbook (T993 plan item 11, rewritten 2026-11-22): a whole release, start to finish, in order, with no
Claude session needed. The same page is a Claude doc: https://claude.ai/artifact/5r4yEqNwqygin1oKo5Zxjk (keep the two
in step).

- **Why a step stopped, and when it is safe to go past it:** `handoff/PLAN/runbooks/release-gates-explained.md`.
- **Every flag of the release command, stage by stage:** `handoff/PLAN/runbooks/release.md`.
- **Production reference (hosts, scripts, rollback, the clean-up):** `handoff/PLAN/release/PRODUCTION.md`.

Every command runs in a terminal from the repo folder (`~/dev/personal/BC-freshwater-fishing-regulations`), the main
checkout. The data the release reads (`data/release/`, the atlas, the reach run) exists only there, so the release
never runs from a worktree.

## Who runs what

Each step is marked with who may run it.

- **[you]** — only you. Everything that needs `CONFIRM_PROD`: the production data, the production website, the
  production feeds worker, the production data worker, the production app builds, a rollback; and the Play production
  track, the App Store and your phones. Your rule (2026-11-22): "only me unless i say otherwise that time". Claude
  prepares each one (staging, tests, smoke, the dry run) and hands you the exact command; it runs one only when you say
  so, that one time. `release --prod` also refuses inside a Claude session.
- **[Claude]** — Claude may run it, and so may you. Everything on staging (data, website, workers, the staging-data
  store builds); every rehearsal (`--dry-run`, production's too: they send nothing); building the production web export
  and water pages (local files); promoting a production build already on Play to Closed testing; the production smoke
  test.
- **[you, or Claude on your word]** — the command is safe to run, but you decide (the version number, the changelog
  words).

## What a release is made of

| Piece | What it is | When it goes |
| --- | --- | --- |
| Data release | The rules, waters, tiles and maps the app downloads: one folder, `data/release/r-…` | Every release |
| Website | The web app and the water pages | After every data release, and when app code changed |
| Feeds worker | The Cloudflare worker for gauges, salmon openings, closed areas and reports | When its code changed, or the release adds closed areas |
| App builds | Android on Google Play, iPhone on TestFlight / App Store | When the app code changed (a new version) |

Three rules hold the whole way through:

- **Build once, promote the same thing.** Production re-uploads the exact folder staging served. Nothing is rebuilt
  for production, and `--prod` refuses if staging serves anything else.
- **A stop does no harm.** A stopped release sits on disk, named in `data/release/CANDIDATE`. Nothing ships it.
  Staging, production and the phones keep the old release.
- **Fix, then run the same command again.** The release command picks up where it stopped and remembers its flags.

## Before you start

1. **The hooks and packages are in place.** Once per clone: `git config core.hooksPath .githooks` (set on this Mac
   already). After a pull that changed `app/package.json`: `pnpm -C app install --frozen-lockfile --prefer-offline`
   (the commit hook needs it).
2. **The trunk is current and green.** All the release's work is merged into `main` by pull request.
   `.venv/bin/python scripts/ci_status.py` says whether CI is green on HEAD. A flaky failure: `gh run rerun <id>
   --failed`.
3. **Logins answer.**
   - GitHub: `gh auth status`
   - Cloudflare: `npx --prefix app wrangler whoami` (expired: `npx --prefix app wrangler login`)
   - R2: `rclone listremotes` shows `r2:`
4. **Secrets are in `~/.canifishthis/`**: the Play service account, the upload keystore, the App Store Connect key, the
   iOS team id. A new Mac: `handoff/PLAN/runbooks/fresh-machine.md`. Every secret and how to replace it:
   `handoff/PLAN/runbooks/secrets.md`.
5. **The machine is free.** Big jobs queue through `scripts/heavy.sh`; `scripts/heavy.sh --status` says what holds
   each lane. Don't start an app build while a release builds.
6. **Disk space.** `scripts/heavy.sh` refuses to start with less than 20 GB free. Old test folders in `$TMPDIR`
   (`release-open-*`) and merged worktrees are the usual cause.

Never use `git stash` or `git reset --hard` in this folder: your review files live here uncommitted. Use
`git pull --ff-only`.

## Step 0: the version and the release branch

**[you, or Claude on your word]**

**How the version is chosen** (T998; the code is `pipeline/release_version.py`, the rule file
`app/tools/versioning.json`):

- It is computed from the commit messages on `main` since the last `v…` tag (before the first tag: since the
  cut-over, starting from 0.8.5).
- Only commits that change the app count: a path under `app/` that a store build or the website ships. Pipeline,
  data, docs, the feeds worker and the dev tools never bump the app.
- The highest change wins: `feat` raises the middle number (0.8.5 → 0.9.0); `fix` or `perf` the last (0.8.5 →
  0.8.6); a `!` or a `BREAKING CHANGE:` line the first. Before 1.0.0, a breaking change raises only the middle number.
  Every other type (`docs`, `chore`, `ci`, `test`, `refactor`, `build`, `style`) changes nothing.
- 1.0.0 is your call, at the first open Play / App Store release: the one version you may type that the commits did
  not compute.
- Build numbers (Android versionCode, iPhone build) are separate. They only go up, one per store build.
- You confirm every version. A typed version that disagrees with the computed one is refused, naming both.

1. **See what the commits say** (makes nothing):

   ```
   .venv/bin/python -m pipeline release --cut --dry-run
   ```

   It prints the version, the commits that decided it, and how many it counted. Read the "decided by" lines. If one of
   them is not a change anglers get, stop and ask Claude: the rule needs fixing in code. You cannot type a different
   number past it.
   - It says **"no app release"**: nothing since the last version bumps the app. This is a data-only release: go to
     [A data-only release](#a-data-only-release).
2. **Cut the branch** at scope lock, when the batch is complete:

   ```
   .venv/bin/python -m pipeline release --cut
   ```

   Type the version at the prompt to confirm it. (Claude cannot answer the prompt; it runs
   `.venv/bin/python -m pipeline release --cut <x.y.z>` once you have said the number.) It makes
   `release/<x.y.z>` from a fresh `origin/main` with one commit, and pushes it. That commit:
   - sets the version in `app/apps/mobile/app.json` and `app/apps/mobile/package.json`;
   - opens an EMPTY entry for the version in `app/packages/core/src/changelog.ts`;
   - writes the developer `CHANGELOG.md` (the commits, by type).

   Done when it prints "cut from origin/main and pushed. Next: git switch release/<x.y.z>".
3. **Switch to it** in this folder: `git switch release/<x.y.z>`. Your uncommitted review files come along. The
   release builds and ships from this branch.

If it stops: "exists on origin already" means the branch was cut before (just switch to it); "is tagged already"
means that version shipped (cut the next one); "a commit not in the format" is explained in the gates page.

## Step 1: the "What's new" bullets

**[you, or Claude on your word]** — only for an app release.

Write 3–5 plain bullets for anglers in the empty entry `app/packages/core/src/changelog.ts` opened for this version.
They become the Play release notes, the TestFlight "What to Test" and the in-app "What's new". Claude drafts them; you
clear the words. Commit on the release branch and push:

```
node scripts/check-changelog.mjs
git commit -m "chore(release): What's new for <x.y.z>" -- app/packages/core/src/changelog.ts
git push origin release/<x.y.z>
```

`node scripts/check-changelog.mjs` exits 0 once the entry has a bullet and is the newest. The app builds refuse to
start until it does.

## Step 2: rehearse

**[Claude]**

The rehearsal runs every check and ends with one list of every stop the real run would make. It commits, promotes and
uploads nothing. Its fast-fail and fresh-copy passes are remembered, so the real run skips them on the same files.

```
.venv/bin/python -m pipeline release --staging --dry-run --build --rehearse-follows
```

- `--build` builds the new release first (the same build the real run makes; LATEST does not move). Without it, the
  rehearsal sees only the first stage and prints what it could not see under `NOT SEEN`.
- `--rehearse-follows` runs the golden-page re-cut and every test on the new release, then puts the fixtures back.
- It also prints the **size headroom** of each data file against its limit, tightest first, with `WARN` under 5 %.
- It prints the version check, the derived minimum app (below) and, for each check, `ok` or `WOULD STOP`.

Clear every `WOULD STOP` it lists (the gates page says what each one means), commit and push the fixes to the release
branch (a code fix goes to `main` first: [A fix during the release](#a-fix-during-the-release)), and rehearse again
until the list is empty.

**The minimum app is derived for you.** The release is stamped with the oldest app that can read it: the live
release's stamp, raised to what the code's data formats need. The rehearsal says the number and which older apps a
rise leaves behind (they keep the data they hold and say "Update the app to get new regulations"; they never break).
You do not pass `--min-app-version` unless you want a higher one.

**Two things on staging may have to go first.** Both are Step 4 commands, run before Step 3 when the rehearsal asks:

- the `site` check stops (the staging site is older than the release's minimum app): deploy the staging site from the
  release branch first (`deploy/staging/site-all.sh`), then rehearse again;
- the `pieces` check stops (the release binds closed-area pieces the staging feeds worker does not serve): deploy the
  staging feeds worker first (`deploy/staging/feeds.sh`), then rehearse again.

## Step 3: the data release to staging

**[Claude]**

```
.venv/bin/python -m pipeline release --staging --push
```

`--push` lets it push the commits it makes itself (in-season matches, re-cut fixtures) to the release branch; never
anyone else's. It prints its plan, then seven stages: preflight, baseline, build, checks, follows, promote, staging
upload. The build skips any stage whose inputs did not change, so after a `--build` rehearsal it is quick.

**Done** when it prints `release: r-… is LATEST and on staging`. At that moment it has also:

- run the staging smoke test;
- tagged the shipped commit `data/<r-id>` and, on the version's first ship, `v<x.y.z>`, and pushed the tags;
- printed the after-release list and the merge-back command (Step 8).

Note the `r-…` id: production needs it.

**When it stops**, it prints what failed and a `next:` line. Fix that and run the same command again (flags are
remembered; the escapes `--no-…` are not). The usual stops and when each accept is safe:
`handoff/PLAN/runbooks/release-gates-explained.md`. In short:

| Stop | What you do |
| --- | --- |
| Preflight list | Fix every item it lists, then rerun |
| Binding diff (stage 4) | Read the file it names. Every change meant? Rerun with `--accept-diff <the .json it prints>`. Otherwise do not ship: find the cause |
| Golden page changed (stage 5) | Read the `git diff` it prints. Every change meant? Rerun with `--accept-fixtures <token>` |
| A test fails on the new release (stage 5) | A code fix (on `main` first), then rerun |
| Upload failed (stage 7) | Fix the login or wait for GitHub, then rerun. It goes straight back to the upload |

## Step 4: the staging website and feeds worker

**[Claude]**

**Website, after every data release** (and after app changes):

```
deploy/staging/site-all.sh
```

It runs quick checks, builds the web app and the water pages on LATEST, deploys them to staging.canifishthis.ca and
runs the smoke test, which prints the release the site shows. It refuses uncommitted changes under `app/`. If only the
app changed (the water pages were already built on LATEST), `deploy/staging/site-all.sh --relink` is faster.
`--dry-run` deploys nothing.

**Feeds worker, when it changed** (`git log --oneline -5 -- app/workers/feeds`), or when the release adds closed areas:

```
deploy/staging/feeds.sh
```

It runs the worker's tests, deploys, then runs every feed job once. When the release adds closed areas it goes
**before** the data: the release's `pieces` check stops until the staging worker serves them.

**Data worker, only when `deploy/staging/data-worker.js` changed:**
`npx --prefix app wrangler deploy -c deploy/staging/data.wrangler.toml`.

## Step 5: look at staging yourself

**[you]**

The checks prove the files are whole; only a look proves they read right.

- **Web:** open https://staging.canifishthis.ca and check waters you know (the Vedder, Chilliwack Lake, a Region 4
  lake, the Bowron, the Puntledge).
- **Phone:** **[Claude]** `deploy/staging/build-apk.sh` builds a sideload app that reads staging and emails you the
  link (https://data-staging.canifishthis.ca/app/android/index.html). Install it over the old one; check the same
  waters, offline too.
- Claude also runs the emulator test bench (`app/tools/testbed/README.md` lists the scenarios).

If something reads wrong, stop here. Production waits; the next release fixes staging.

## Step 6: the phone builds

Only for an app release. Both builds use the committed code of the release branch, never uncommitted edits. Each runs
the same gates first (the precheck list, CI, fast-fail), so a missing secret or a red CI stops it before the long build.

### Android: Play Internal, then Closed testing

Closed testing gets the **production** build (your answer 2026-11-22: "Production build (Rec.)"): the production
app bundle goes to Internal, and that same build is promoted to Closed. A staging-data build on Closed (0.8.5's build
43) was the exception, not the rule.

1. **Rehearse** **[Claude]** (builds and uploads nothing, needs no confirmation): `deploy/prod/build-apk.sh --dry-run`
   prints the plan and the precheck list (every missing secret, link or changelog entry at once, `WOULD STOP` when
   one is missing).
2. **The production build to Play Internal** **[you]**:

   ```
   CONFIRM_PROD=canifishthis.ca deploy/prod/build-apk.sh
   ```

   It raises the versionCode and commits only that, builds and checks the app bundle (signature, MapLibre), uploads
   it to the Internal track with this version's bullets, writes the production version files, and emails the tester
   link. R8 is on in every build (T1003): the build stops if R8's configuration turns a pass off or the bundle's R8
   scores fall under their floors (`app/tools/r8-report.mjs`), the email carries the three scores, and R8's mapping
   file goes to Play with the bundle (a missing mapping stops the upload; Play's crash stacks need it). Then push its commit: `git push origin release/<x.y.z>`. If the upload worked but a later step failed:
   `CONFIRM_PROD=canifishthis.ca deploy/prod/build-apk.sh --publish-only`.
   A build that reads **staging** data for Internal testers is `deploy/staging/build-apk.sh --play` **[Claude]**;
   it is never the one promoted to Closed.
3. **Install it from Play** on your phone **[you]**. About shows no environment badge on a production build.
4. **Promote the same production build to Closed testing** **[Claude]** (track `alpha`; no new upload, the notes
   come from Internal):

   ```
   .venv/bin/python deploy/staging/play-upload.py --package ca.canifishthis.app --max-version-code
   .venv/bin/python deploy/staging/play-upload.py --package ca.canifishthis.app --promote <versionCode> --track alpha
   ```

   The first prints the highest versionCode Play holds (the build just uploaded). Done when it prints
   `committed: versionCode <n> on track 'alpha'`. Google reviews it before testers get it.
   Closed is the track the production-access application is judged on (T1046): Step 8's
   `play-production-access/releases.md` row and "Fixed in" updates are owed for this build.
5. **Production track** **[you]**: in Play Console, the same build to Production with a staged rollout (for example
   20 %, then 100 %). Production waits for Step 7's data.

### iPhone: TestFlight, internal then external

1. **Precheck** **[Claude]**: `deploy/staging/build-ios-testflight.sh --production --precheck` (lists every missing
   secret, tool or link; `--dry-run` prints the steps).
2. **The production build** **[you]**:

   ```
   CONFIRM_PROD=canifishthis.ca deploy/staging/build-ios-testflight.sh --production
   ```

   It builds, signs with the App Store Connect key and uploads; the build number raises itself. Apple processes it
   (the script waits, polling every 30 s). Then **the external group is automatic**: it answers export compliance,
   sets "What to Test" from the iPhone bullets, adds the build to the External group and submits it for Beta App
   Review when Apple asks (the first build of a version). Its last line says where it stands. If that step failed:

   ```
   node deploy/staging/testflight-distribute.mjs --version <x.y.z> --build <build>
   ```

   (Every step is skipped when already done; `--dry-run` only reads.) A build on **staging** data for internal
   testers is `deploy/staging/build-ios-testflight.sh` **[Claude]**.
3. **App Store submission** **[you]**, in App Store Connect, after Step 7's data.

## Step 7: production

Do it after you have checked staging. The data goes first unless a gate says otherwise. Every production deploy below
is yours: Claude gets each one ready and hands you the command (your rule, 2026-11-22: "only me unless i say otherwise
that time").

1. **Rehearse** **[Claude]** (sends nothing, needs no confirmation):

   ```
   .venv/bin/python -m pipeline release --prod <r-id> --dry-run
   deploy/prod/upload.sh <r-id> --bucket bc-fishing-regulations-staging --dry-run
   deploy/prod/feeds.sh --dry-run          # only when the feeds worker changed or closed areas were added
   ```

   The first says whether staging still serves `<r-id>` and whether its tags are right. The second walks the upload.
   The third runs the worker's tests and builds its bundle.
2. **Only if the release needs it first:**
   - **The website first** **[you]**, when the release's minimum app is newer than the production site. The data
     upload refuses with "the production site is app X, older than Y". Item 5, then come back.
   - **The feeds worker first** **[you]**, when the release adds closed-area pieces the production worker does not
     serve yet. The data upload's pieces gate refuses otherwise. `CONFIRM_PROD=bc-fishing-feeds deploy/prod/feeds.sh`.
3. **The data** **[you]**, in your own terminal (it refuses inside a Claude session):

   ```
   CONFIRM_PROD=<r-id> .venv/bin/python -m pipeline release --prod <r-id>
   ```

   Type the id again when asked. It refuses unless the release passed its checks, reached staging, is unchanged on
   disk, staging still serves it, and its `data/<r-id>` / `v<x.y.z>` tags are on GitHub at the shipped commit. Then it
   copies staging's files to production, keeps the old pointer for rollback, writes the pointer last and
   `data/release/PUBLISHED`, and prunes releases neither pointer names. Done when it prints
   `release: <r-id> published to production`.
4. **Smoke test** **[Claude]**: `node app/tools/smoke-staging.mjs --production`.
5. **The website**, with each app version so the two match (skip if item 2 did it):
   - **[Claude]** after the full test tier and the staging smoke pass, builds both halves on the same release and
     rehearses the deploy:

     ```
     (cd app/apps/mobile && EXPO_PUBLIC_CONTACT=support@canifishthis.ca EXPO_PUBLIC_APP_ENV=production \
       pnpm exec expo export --clear --platform web --output-dir ../../dist/web-prod --source-maps \
       && node ../../tools/web-shell.mjs ../../dist/web-prod && node ../../tools/sourcemaps.mjs keep-web ../../dist/web-prod)
     scripts/heavy.sh seo-prod 30 -- node app/tools/build-seo.mjs --release-dir data/release/<r-id> \
       --app app/dist/web-prod --threads 4 --out app/dist/seo-prod
     deploy/prod/site.sh --dry-run
     ```

     The web's source maps are SERVED (T1008, owner 2026-11-22): `keep-web` checks them (every bundle over 100 KB names
     its map, the debug ids agree, a map names only files the repository tracks or node_modules holds), keeps a copy in
     `data/sourcemaps/<key>/web/` for `sourcemaps.mjs symbolicate`, and leaves them in the export, so the site carries
     them under `/_expo/static/` with the bundle's cache rule.

   - **[you]** deploy it: `CONFIRM_PROD=canifishthis.ca deploy/prod/site.sh`. It refuses uncommitted changes under
     `app/`.
6. **The feeds worker** **[you]**, after the data and the matching website, when its code changed (skip if item 2 did
   it): `CONFIRM_PROD=bc-fishing-feeds deploy/prod/feeds.sh`.
7. **The data worker** **[you]**, only when `deploy/staging/data-worker.js` changed:
   `npx --prefix app wrangler deploy -c deploy/prod/data.wrangler.toml`.
8. **The phones** **[you]**: Play Production (Step 6, Android 5) and the App Store (Step 6, iPhone 3). Before you
   update the Play listing, **[Claude]** re-shoots the store pictures from a store-shots web build of the checkout (the
   tool builds and serves its own; staging is not used): `scripts/heavy.sh store-shots 30 -- node app/tools/store-shots/run.mjs`
   (it fails, naming the scene, when a shot is missing its content, and stops at once when pointed at a site that is not a
   store-shots build; details in `handoff/PLAN/release/PRODUCTION.md`).
9. **Watch for a day** **[you]**: `node app/tools/smoke-staging.mjs --production`,
   `curl -s https://feeds.canifishthis.ca/status`, and the `[production]` emails (crashes, feed health).

## Step 8: tags and the merge back

**[Claude]**

- **Tags** are made by Step 3 (`data/<r-id>`, and `v<x.y.z>` on the version's first ship). Nothing to do unless it
  stopped there; then rerun Step 3's command: everything done is skipped and only the tags are left.
- **Merge the release branch back into `main`** by a pull request, so its own commits (fixtures, matches, the
  versionCode, the changelog) reach the trunk. Step 3 prints the first command; the second turns on auto-merge:

  ```
  gh pr create --base main --head release/<x.y.z> --title "chore(release): merge release/<x.y.z> back into main" --body "The release's own commits (in-season matches, re-cut fixtures, cherry-picks) back on main."
  gh pr merge release/<x.y.z> --auto --merge
  ```

  It merges itself once the four required checks pass. Then `git switch main && git pull --ff-only`.
- **The production-access record** (T1046), on an app release: add a row to
  `handoff/PLAN/play-production-access/releases.md` (version, date, builds, tracks, what changed for testers, the
  feedback rows it answered), and in `feedback-log.md` set "Fixed in" to this version and status to "fixed" on every
  row the release closes (a row reading "0.9.0 (pending)" becomes "0.9.0"). Then bring `questionnaire-draft.md` up to
  date for the release (owner 2026-11-24: "keep the google release questions for closed with how releases do feedback
  and everything up to date"): Q3 (feedback summary and how it was collected) gets the new rows; Q7 (changes made from
  the closed test) gets this release's fixes, each citing its feedback row; Q8 (readiness) gets fresh numbers (crash and
  ANR rates from Android vitals if the owner gives them, tests, the device pass). Commit all three by PR with the merge
  back.

## A fix during the release

**[Claude]**

A fix lands on `main` first, by pull request, like any change. Then it is copied onto the release branch:

```
git switch release/<x.y.z>
git cherry-pick -x <the fix's commit on main>
git push origin release/<x.y.z>
```

Then run Step 3's command again. Nothing is committed straight to `main`; pushes to `main` are refused.

## A data-only release

When the rules or curated data changed but nothing bumps the app (Step 0 said "no app release"):

1. **[Claude]** Cut the day's data branch from `origin/main`, then switch to it:

   ```
   .venv/bin/python -m pipeline release --cut --data-only
   git switch release/data-<YYYY-MM-DD>
   ```

   It is refused if an app commit would bump the app (then it is an app release: Step 0).
2. Steps 2, 3 and 4 as above (rehearse, data to staging, staging website).
3. Step 5 (look at it), then Step 7 without the phones.
4. Step 8: only the `data/<r-id>` tag is made (the app version is unchanged); merge the data branch back the same way.

No phone build, no changelog entry.

## When something breaks

Most stops are a login, a judgment call or a rerun. A few need a code change. Either way nothing reaches anglers
until the checks pass, and holding a release is always safe.

| Kind | Examples | What you do |
| --- | --- | --- |
| Fix it yourself | An expired login; flaky CI (`gh run rerun <id> --failed`); disk full; network; a binding diff or golden page to judge | Fix it, rerun the same command |
| Needs a code change | A fast-fail step fails; a test fails on the new release; CI is red for real; a build stage fails (`.venv/bin/python -m pipeline build --status` shows which and its log) | Fix the code on `main`, cherry-pick it, rerun; or hold the release |

Every stop, what it protects and when an accept is safe: `handoff/PLAN/runbooks/release-gates-explained.md`.

**Rollback (production)** **[you]**:

- **Data:** `CONFIRM_PROD=canifishthis.ca deploy/prod/rollback.sh` puts the previous release back (`--dry-run` first
  shows it). Then `echo <previous r-id> > data/release/LATEST`. A promote never deletes the previous release.
- **A worker** (feeds, data, site): `npx --prefix app wrangler deployments list -c <its config>`, then
  `npx --prefix app wrangler rollback <version-id> -c <its config>`. The configs are
  `app/workers/feeds/wrangler.prod.toml`, `deploy/prod/data.wrangler.toml` and `deploy/prod/site.wrangler.toml`.
- **The app:** Play Console → the release → Halt rollout, then ship a fixed build with a higher number.

Staging has no rollback. The next release replaces it.

## Checklist

- [ ] Before you start: `main` green (`.venv/bin/python scripts/ci_status.py`); logins answer
- [ ] Step 0: `.venv/bin/python -m pipeline release --cut --dry-run`; read the version; `--cut`; `git switch release/<x.y.z>`
- [ ] Step 1: the changelog bullets, cleared by you, committed and pushed
- [ ] Step 2: `.venv/bin/python -m pipeline release --staging --dry-run --build --rehearse-follows` until no stop
- [ ] Step 3: `.venv/bin/python -m pipeline release --staging --push`; rerun until done; note the `r-…` id
- [ ] Step 4: `deploy/staging/site-all.sh`; `deploy/staging/feeds.sh` if the worker changed or closed areas were added
- [ ] Step 5: check web and phone on staging
- [ ] Step 6: Android production build → Internal → Closed (`play-upload.py --promote`); iPhone production build
      (external group automatic)
- [ ] Step 7: rehearse (Claude); data, website, feeds worker (you, with the commands Claude hands you); smoke;
      phones (you); watch a day
- [ ] Step 8: merge the release branch back by PR; `git switch main && git pull --ff-only`
- [ ] Step 8: `play-production-access/releases.md` row added; "Fixed in" set on the feedback rows it closes
- [ ] Step 8: `play-production-access/questionnaire-draft.md` Q3, Q7, Q8 updated for this release
