# Secrets: what they are, where they live, how to replace them (T240 §9, T249)

Names and places only: no value is in the repo. Replacing anything here is the owner's job, except the staging
`RUN_TOKEN` (Claude may rotate it).

**Can't be recovered if lost; keep a backup (a password manager or an encrypted disk):** everything in
`~/.canifishthis/`, `app/apps/mobile/android/app/debug.keystore`, and the `.p8` keys (Apple lets you download each one
once). Cloudflare secrets can't be read back once set, so a lost `REPORT_SALT` can only be replaced, never restored.

## Cloudflare: the feeds worker

Staging = `bc-fishing-feeds-staging` (`app/workers/feeds/wrangler.toml`). Production = `bc-fishing-feeds`
(`app/workers/feeds/wrangler.prod.toml`). Each has its own values. To set one:
`npx --prefix app wrangler secret put <NAME> -c <that config>` (it asks for the value).

| Name | Where | What it does | How it was made / how to replace |
| --- | --- | --- | --- |
| `RUN_TOKEN` | staging, prod | Lets our scripts start the worker's jobs (`/run/…`) | A random string. Staging: `deploy/staging/rotate-run-token.sh` (new value, sets it, writes the local copy `app/workers/feeds/.run-token`). Prod: `openssl rand -hex 32`, `secret put`, and save it as `app/workers/feeds/.run-token.prod` (a fresh clone has none: it is not in git) |
| `REPORT_SALT` | staging, prod | Hashes the ids of problem and crash reports. Without it `/report` and `/crash` answer 503 and `feeds.sh` won't deploy | A random string. **Never rotate**: report ids would stop matching their history. Only if lost: `openssl rand -hex 32` + `secret put` |
| `NOTIFY_EMAIL_TO` | staging, prod | Where the worker emails you (digests, DFO, crashes) | Your address. It must be verified in Cloudflare → Email Routing → Destination addresses first, then `secret put` |
| `GITHUB_DISPATCH_TOKEN` | staging, prod | Lets the worker start GitHub workflows (HYDAT, run timing) | GitHub → Settings → Developer settings → Fine-grained tokens: this repo only, Actions read and write. **It expires**: regenerate it there, then `secret put` on both workers |
| `APNS_KEY`, `APNS_KEY_ID`, `APNS_TEAM_ID` | staging only | iPhone push | `APNS_KEY` = the text of an APNs `.p8` key (developer.apple.com → Keys → + → Apple Push Notifications service); `APNS_KEY_ID` = its 10-character id; `APNS_TEAM_ID` = the team id. Set on prod when push ships |

Plain settings (not secret, in `wrangler.toml`): `NOTIFY_EMAIL_FROM`, `NOTIFY_DIGEST`, `FEEDS_ENV`, `STATUS_URL`,
`WORKER_NAME`, `BUCKET_NAME`, `BACKUP_BUCKET_NAME`, `SITE_ORIGIN`.

## Cloudflare: the other workers

| Worker | Secrets |
| --- | --- |
| Staging data worker (`deploy/staging/data.wrangler.toml`) | none |
| Prod data worker (`deploy/prod/data.wrangler.toml`) | `FEEDBACK_TO`: **nothing reads it** (left from the v1 app). Delete it with `npx --prefix app wrangler secret delete FEEDBACK_TO -c deploy/prod/data.wrangler.toml` |
| Staging and prod site workers | none |

## GitHub Actions (repo secrets)

Set with `gh secret set <NAME>` (it asks for the value), or GitHub → the repo → Settings → Secrets and variables →
Actions.

| Name | Used by | How to replace |
| --- | --- | --- |
| `CLOUDFLARE_ACCOUNT_ID` | feeds-hydat, feeds-runtiming | Not a credential. Cloudflare dashboard → the account's id |
| `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` | the staging R2 writes of feeds-hydat, feeds-runtiming | Cloudflare → R2 → Manage API tokens → Create: Object Read and Write, the staging bucket only. Set both, then delete the old token |
| `PROD_R2_ACCESS_KEY_ID`, `PROD_R2_SECRET_ACCESS_KEY` | the production option of feeds-hydat (commented out) | **Not set yet.** Made at the first production launch (`handoff/PLAN/runbooks/release-by-hand.md`), the same way, prod bucket only |
| `CLOUDFLARE_API_TOKEN` | **no workflow reads it** | Delete it (`gh secret delete CLOUDFLARE_API_TOKEN`), or keep it for production deploys from GitHub later |

## This Mac: `~/.canifishthis/` (outside the repo)

| File | What it does | How it was made / how to replace |
| --- | --- | --- |
| `upload-keystore.jks`, `upload-keystore.properties`, `upload_certificate.pem` | The Play **upload** key: `build-apk.sh --play` signs with it; Play refuses anything else | Made with `keytool` at the first Play setup; the `.pem` is its public half. Google holds the real app-signing key, so a lost upload key can be replaced: Play Console → the app → Test and release → App integrity → App signing → Request upload key reset (takes Google a day or two). Then make a new key with `keytool -genkeypair` and update the `.properties` |
| `play-service-account.json` | `deploy/staging/play-upload.py` uploads builds with it | Google Cloud Console → IAM → Service accounts → the account → Keys → Add key → JSON. Delete the old key there. The account needs access in Play Console → Users and permissions |
| `asc-api-key.json` + `AuthKey_W35H7S7X3F.p8` | `build-ios-testflight.sh` signs and uploads with it (the json names this `.p8`) | App Store Connect → Users and Access → Integrations → App Store Connect API → Team key with the **Admin** role (an App Manager key can archive but can't sign). The `.p8` downloads once: if lost, revoke it, make a new one, and update `key_id` and `key_path` in the json |
| `AuthKey_6T3DBFF92H.p8`, `AuthKey_843F3K6BSH.p8` | Not used by any script | Probably the earlier App Manager key and the APNs key (OWNER TO CONFIRM which is which). Check both ids in App Store Connect (API keys) and developer.apple.com → Keys. Keep the APNs one (its text is the worker's `APNS_KEY`); revoke the other |
| `ios-team-id` | The Apple team id (not secret) | developer.apple.com → Membership details |
| `app-store-url`, `play-test-url`, `play-last-version-code`, `testflight-last-build` | State the build scripts keep (not secret) | Written by the scripts |

## In the repo folder, gitignored

| File | What it does | How to replace |
| --- | --- | --- |
| `app/workers/feeds/.run-token` | The local copy of staging's `RUN_TOKEN` | `deploy/staging/rotate-run-token.sh` |
| `app/apps/mobile/android/app/debug.keystore` | Signs the staging sideload APK; the same key every build, so an update installs over the last one | Expo's standard debug keystore. If lost, any new debug keystore works, but every phone must uninstall the staging app once. Back it up |
| `.env` (repo root) | Exists on the owner's Mac only; never opened for this list. Nothing in the code or the docs reads a root `.env` (checked 2026-11-25), so its purpose is unknown | Owner: say what it holds and whether it can go; until then it is not needed to set up, test or build |

## Logins kept by tools

| Login | Used for | How to renew |
| --- | --- | --- |
| rclone remote `r2:` (`~/.config/rclone/rclone.conf`: an R2 access key, secret and endpoint) | `deploy/staging/upload.sh` and the production upload | Cloudflare → R2 → Manage API tokens → Create, then `rclone config` → edit `r2` |
| Cloudflare (wrangler) | Every worker and site deploy | `npx --prefix app wrangler login` |
| GitHub CLI (`gh`, in the keychain) | Pushes, CI checks, starting workflows | `gh auth login` |
| Claude CLI | The parser runs (yours only) | `claude` → log in |

## Not secrets, but accounts only you hold

Play Console, App Store Connect / Apple Developer, Cloudflare (DNS, Email Routing, R2, workers), the domain registrar,
GitHub, Google Cloud (the Play service account).

## Rule

A secret never goes into a committed file, a log, a ticket or a report. `.run-token` stays gitignored
(`app/workers/feeds/.gitignore`). If a secret turns up in git history, replace it first, then clean up.
