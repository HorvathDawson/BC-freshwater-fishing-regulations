# Secrets inventory (T240 §9, with T249)

Names and where they live only. No value is in the repo. Checked 2026-11-05 (`wrangler secret list`, `gh secret list`,
`~/.canifishthis/`). Rotate = who changes it and how.

## Cloudflare feeds workers
| name | where | rotate |
|---|---|---|
| `RUN_TOKEN` | staging `bc-fishing-feeds-staging`; prod `bc-fishing-feeds` has its OWN value (placeholder, T404) | Claude may rotate staging: `deploy/staging/rotate-run-token.sh` (local copy `app/workers/feeds/.run-token`, gitignored). Prod: owner |
| `REPORT_SALT` | staging; prod its own. REQUIRED (T431): without it `/report` and `/crash` answer 503, and `deploy/staging/feeds.sh` refuses to deploy | never, or report ids stop matching their history. Owner |
| `NOTIFY_EMAIL_TO` | staging; prod | owner (a verified Email Routing address) |
| `GITHUB_DISPATCH_TOKEN` | staging; prod | owner: a fine-grained GitHub token, this repo only, Actions read+write. It expires: renew it, then `wrangler secret put` |

Plain vars (not secret, in `wrangler.toml`): `NOTIFY_EMAIL_FROM`, `NOTIFY_DIGEST`, `FEEDS_ENV`, `STATUS_URL`.

## GitHub Actions (repo secrets)
| name | used by | rotate |
|---|---|---|
| `CLOUDFLARE_ACCOUNT_ID` | feeds-hydat, feeds-runtiming, feeds-sync-inputs, staging-* | not a credential; never |
| `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` | the staging R2 writes of the feeds workflows | owner: Cloudflare → R2 → API tokens (staging bucket only) |
| `PROD_R2_ACCESS_KEY_ID`, `PROD_R2_SECRET_ACCESS_KEY` | the prod option of `feeds-hydat.yml` (commented out until T404) | owner, at T404: not set yet |
| `CLOUDFLARE_API_TOKEN` | **no workflow reads it** (set 2026-03-18, v1 era) | owner: delete it, or keep it for T404's deploys |

## The owner's Mac (`~/.canifishthis/`, outside the repo)
| file | what | rotate |
|---|---|---|
| `upload-keystore.jks` + `upload-keystore.properties` | the Play UPLOAD key (`build-apk.sh --play`) | owner. If it is lost, Play Console → App integrity → reset the upload key (`upload_certificate.pem` is its public half) |
| `play-service-account.json` | `deploy/staging/play-upload.py` (internal track uploads) | owner: Google Cloud → the service account → keys |
| `play-test-url`, `play-last-version-code` | not secret (state for `build-apk.sh`) | — |
| rclone remote `r2:` (`~/.config/rclone/rclone.conf`) | `deploy/staging/upload.sh` | owner |
| wrangler login | worker deploys | owner (`wrangler login`) |

## Not secrets, but owner-held accounts
Play Console, Cloudflare (DNS, Email Routing), the domain registrar, GitHub, and later the Apple Developer account (T88).

## Rule
A secret never goes into a committed file, a log, a ticket or a report. `.run-token` stays gitignored
(`app/workers/feeds/.gitignore`). A secret that turns up in git history: rotate it first, then clean up.
