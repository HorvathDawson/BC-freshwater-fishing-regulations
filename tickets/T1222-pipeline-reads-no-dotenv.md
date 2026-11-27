# T1222 — the pipeline reads no .env file

Status: IN PROGRESS — lane `chore/T1222-no-dotenv-load` (PR open, not merged).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1222 | the pipeline reads no .env file | IN PROGRESS | — | PR on `chore/T1222-no-dotenv-load`: review and merge; rides the 0.10.0 re-cut build (every stage's key changes once) |

Source: the owner, 2026-11-27 (relayed verbatim by the planning chat). He ran `cut -d= -f1 .env` himself: the names are
`GOOGLE_API_KEY` and `GOOGLE_API_KEY_2` through `GOOGLE_API_KEY_7` only (the file's own comments say Gemini keys for the
old parsing step, loaded via python-dotenv), and said: "so nothing matters". Decision: remove the automatic load. Earlier:
"we might wanna fully rework the strucutre maybe we dont need a env file at all we want industry standard format for
keeping it simple stupid". The load was `pipeline/common/curated.py` `_load_env()`, called from `parse_config` (T1192
moved it there from `get_config()`; its note said "its removal is T1222").

## Work
1. Remove `_load_env()`, its call in `parse_config` and its test
   (`test_the_env_file_is_loaded_once_by_whichever_config_read_comes_first`); a new test: no config read calls
   `load_dotenv`, and importing the config modules does not import `dotenv` (a fresh process, `sys.modules`).
2. `python-dotenv` out of `requirements.txt`: nothing in the repo imports it (the review backend's requirements are the
   repo's own set; `test_curation_review_deps_t994.py` only mapped the name, now dropped). It stays pinned in
   `constraints.txt` (`python-dotenv==1.2.2`, with the reason): `uvicorn[standard]` requires it (`python-dotenv>=0.13` in
   uvicorn 0.52.4's METADATA), so it is still installed and must stay pinned.
3. Gemini / `GOOGLE_API_KEY*`: no reader anywhere in the tree (`git grep -i "GOOGLE_API_KEY\|gemini\|genai\|google.generativeai"`
   finds only two lake slugs in `app/seo/slug-map.json` and the `google-genai` pin). The Gemini parser left the code in
   `ac3476d00` (2026-08-14, "remove Gemini parser"); today's parser dispatches the `claude` CLI (AGENTS A1). No dead
   Gemini code, so no removal ticket.
4. Docs: where settings and secrets come from (`docs/runbooks/fresh-machine.md` section 10), the `.env` row in
   `docs/runbooks/secrets.md`, and the comments that said config reads load `.env`.

## The run-time variables, checked (2026-11-27, read from the readers)

| variable | reader | unset |
|---|---|---|
| `R2_ENDPOINT` | `pipeline/runtiming/feed/publish.py:70` (`--via aws`) | `SystemExit("publish --via aws: R2_ENDPOINT is not set")` |
| `R2_ENDPOINT` | `deploy/staging/check-public.mjs:37` (no rclone remote) | throws `no rclone remote r2: and no R2_ENDPOINT for the AWS CLI` |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | `.github/workflows/feeds-hydat.yml:81,128`, `feeds-runtiming.yml:117` (from GitHub secrets) | `::error::R2 keys are not set`, exit 1 |
| `FEEDS_URL` | `pipeline/deliver/release/pieces_gate.py:98,178` | a release with bound pieces is refused: "no feeds worker was named (FEEDS_URL)" |
| `FEEDS_URL` | `deploy/lib/input-formats.sh:35` | every formatted input refused: "set FEEDS_URL to the worker that reads it" |
| `FEEDS_URL` | `deploy/staging/upload.sh:26`, `sync-inputs.sh:83`, `deploy/prod/sync-inputs.sh:33`, `upload.sh:49` | set by the script itself (staging, production); a rehearsal bucket falls to the two refusals above |
| `MIN_APP_VERSION` | `pipeline/deliver/release/app_gate.py:84`, `site_gate.py:116` | optional by design: the manifest's `minAppVersion` stamp is used; a too-old floor is refused naming `MIN_APP_VERSION` (`app_gate.py:77`) |
| `NOTIFY_EMAIL_TO` | not a shell variable: `deploy/staging/feeds.sh:25` and `deploy/prod/feeds.sh:38` check the worker's `wrangler secret list` | the deploy stops, naming it and the `secret put` command |
| `FEED_NOTIFY_URL` | `pipeline/gauges/feed/notify.py:106` | off by design, one printed line `notify: off (FEED_NOTIFY_URL unset)` |

Other variables the release, deploy and build scripts read from the shell (none is a secret; each defaults or stops by
name): SITE_URL, SITE_CHECK_SKIP, PROD_SITE_CHECK_SKIP, RELEASES_DIR, REL, FEEDS_WORKER_NOT_LIVE, SITE_DIRTY_OK, SMOKE,
SMOKE_SHOTS, SYNC_INPUTS_FROM_REVIEW, SYNC_INPUTS_LOCK, PLAY_TRACK, PLAY_ABIS, JAVA_HOME, ANDROID_HOME, MLN_LOCAL_MAVEN,
EXPO_PUBLIC_*, GRADLE_*, PIPELINE_TRACE, PIPELINE_PROFILE, CURATED_ROOT, UI_EXPORT_BUNDLE, CURATION_API,
PREVERIFY_CACHE; and RUN_TOKEN / REPORT_SALT, which ARE secrets read from the environment by
`deploy/staging/check-public.mjs`: a person who kept any of these in `.env` must export it.

Found, not fixed here:
- `deploy/staging/check-public.mjs:53-56`: the secret scan looks for `NOTIFY_EMAIL_TO`, `REPORT_SALT`, `AWS_ACCESS_KEY_ID`
  and `AWS_SECRET_ACCESS_KEY` by value from the environment; an unset one is skipped without a word, so the scan is
  narrower than it says.
- `constraints.txt` still pins `google-genai==2.10.0` and `google-auth==2.55.1`, which nothing in `requirements.txt`
  asks for (left from the Gemini parser; a constraint installs nothing, so it is harmless clutter).

## Acceptance
- No code imports or calls `dotenv` (only the new test, comments and docs name it); `python-dotenv` is not in
  `requirements.txt` and is pinned in `constraints.txt` for `uvicorn[standard]`.
- The new test fails if `parse_config` (or any config read) calls `load_dotenv` again, or a config module imports `dotenv`.
- The config / curated tests, `scripts/check_data_paths.py`, `check-tickets.mjs`, the mini release `--check` and
  `scripts/before_pr.sh` pass.

Depends: none. Size: S. Owner: no. Default: n/a.

## Owner

- [x] 2026-11-25 — the root `.env`: what does it hold, and can it go? → "so nothing matters" (2026-11-27)
