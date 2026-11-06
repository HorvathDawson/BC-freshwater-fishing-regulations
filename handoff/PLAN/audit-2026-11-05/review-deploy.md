# T333 audit — review app, deploy, scripts, CI, fetch_data, archive/

Finder area: `curation-review/`, `deploy/`, `scripts/`, `.github/workflows/`, `data/fetch_data.py`, `archive/`.
Read-only; evidence is file:line as of HEAD 0152c48b6 (2026-11-05). Ranked most harmful first.

## Findings

1. **Staging upload clears the live pointer before anything is proven — the "atomic switch" is not atomic** · silent fallback / safety
   · `deploy/staging/upload.sh:37-38`, `:40-47`, `:79-90`
   · Line 38 `rclone delete "$D" …` removes every key except feeds/, private/, runtiming/, test/, app/android/ — including `release/manifest.json` and the live release's files — BEFORE the run-timing workflow is confirmed started (40-47), before any upload (49-78) and before the new pointer is written (90). Any `set -e` exit between 38 and 90 (gh not logged in, workflow never appears, an rclone error, the run-timing tile check at 86) leaves staging with NO manifest: every staging app, the SEO smoke and the restore test see 404 until the script is rerun end to end. The comment at 79-82 ("A failed run stops here without writing the pointer") is true only of the new pointer; the old one is already gone.
   · Why it matters: staging is the only pre-prod proof; a dark staging during a failed upload hides whether the failure was the release or the clear. The same shape would be fatal if ever copied to prod.
   · Fix: two-phase clear — delete only keys that are neither the live release's content-named files nor `release/manifest.json`, upload, flip the pointer, THEN prune the previous release's files (publish.py's order already exists; let it own the prune). Or at minimum move line 38 after line 47 and re-upload the old manifest on failure via a trap.
   · Confidence: high (read).

2. **sync-inputs.sh masks every rclone failure and reports "inputs in sync"** · silent fallback / deploy continues past a failed step
   · `deploy/staging/sync-inputs.sh:22` — `rclone copyto … -v 2>&1 | grep -E "Copied|Skipped|ERROR" || true`; `:25` prints `inputs in sync … (N objects)` unconditionally.
   · With `set -euo pipefail`, the `|| true` and the grep swallow rclone's exit status: a refused put (expired r2: token, a 5xx) prints one ERROR line and the script exits 0, counting the object as synced. `upload.sh:75` calls it mid-release and continues to the pointer flip. Its "Mac twin" `.github/workflows/feeds-sync-inputs.yml:93-99` uses `aws s3 cp --only-show-errors` unmasked, so the two uploaders the header (sync-inputs.sh:5-7) says "cannot drift" drift exactly in failure behaviour.
   · Why it matters: the DFO entries / wording / roster / in-season matches the feeds worker reads can silently stay stale on staging while the release says it shipped them.
   · Fix: `rclone copyto … || { echo "sync-inputs: put $key failed"; exit 1; }` (drop `-v | grep`; use `--stats-one-line` if the Copied/Skipped lines are wanted) and count only successful puts.
   · Confidence: high.

3. **Two HYDAT downloaders; the owner's command uses the unchecked one** · duplicate pathway + silent fallback
   · `data/fetch_data.py:1248-1285` (`fetch_hydat`: no size/CRC/`quick_check`, not resumable, `:1265-1267` prints "could not read the HYDAT listing; leaving what is here" and RETURNS — exit 0) vs `scripts/hydat-fetch.sh:1-45` (T301: resumable `curl -C -`, Content-Length check, `unzip -t`, `PRAGMA quick_check`, sha256 record). `scripts/update-hydat.sh:52-61` runs the unchecked one, then re-implements the listing check itself (comment at `:13-14` documents the fallback it is working around). `.github/workflows/feeds-hydat.yml` uses the checked script.
   · Why it matters: a truncated or corrupt 266 MB zip from the Mac path becomes the envelope every percentile in the app is measured against, with no checksum on record; the two paths can disagree on which release is "current".
   · Fix: `update-hydat.sh` step 1 calls `scripts/hydat-fetch.sh "$latest" data/generated/hydat_work data/source/hydat.sqlite3`; delete `fetch_hydat` from fetch_data.py (keep `hydat_latest_release`, or move it beside the script). One fetcher, one checksum file.
   · Confidence: high.

4. **Review-app frontend: ~20 `.catch(() => {})` / empty-result fallbacks** · silent fallback (owner rule 11-03: no silent fallbacks, shown failures also reported)
   · `curation-review/frontend/src/App.tsx:111, 212, 224-226, 238, 241, 298-299`; `components/DfoReview.tsx:411, 471, 494, 574, 1015-1016, 1077, 1179`; `components/EntryDetail.tsx:122, 159, 189`; `components/InSeasonReview.tsx:187, 197`; `components/ReportsView.tsx:209`; `components/RefusedSavesBanner.tsx:22`; `curatedSave.ts:36`.
   · Concrete cases: `DfoReview.tsx:1179` `.catch(() => setTribs([]))` — a 500 on `/tributaries` renders the confluence picker as "no tributaries", the reviewer places the cut elsewhere; `:494` a failed saved-extent preview draws only the points, so the saved extent LOOKS unbound; `:1015-1016` a hardcoded `OFFSET_FALLBACK` anchor list replaces the server's `marks.OFFSET_ANCHORS` on any error (a second copy of the policy); `EntryDetail.tsx:159/189` named splits and reaches become `null` (indistinguishable from "none"); `App.tsx:212/226` the pending-build poll never says it failed, so "awaiting build" can stay forever; `curatedSave.ts:36` the write-through poll goes quiet, so a background refusal is never shown unless another save happens.
   · Why it matters: a reviewer's mark is a decision made on what the screen shows; an empty list that is really an error produces a wrong curated file through the write-through.
   · Fix: one `reportFailure(where, err)` helper (NoticeStack already exists) and no empty catch; a list that failed renders a "could not load X" row, not `[]`/`null`. T333's app-wide sweep scope applies to this frontend too.
   · Confidence: high (all read; harm per site varies).

5. **Backend reads that answer "empty" on an error, one of them cached** · silent fallback
   · `curation-review/backend/reuse.py:1139-1149` `_applied_keys_for` is `@lru_cache` and returns `frozenset()` on ANY exception — the failure is cached until the key tuple changes, so every mark reads "not applied" (status/`attention` noise for the whole session) and the log line is the only trace.
   · `reuse.py:1960-1978` geometry lookup → `None` ("a missing layer/key must not 500 the map"): the item simply has no outline on the map.
   · `awaiting_water.py:33-37` `_doc()` → `{}` on an unreadable JSON (an entry's awaiting-water state silently "none"); `awaiting_water.py:70-78, 85-93` centre → `None`; `preverify.py:44-48` → `{}`; `inseason.py:90-94` label → the raw id.
   · `region_cascade.py:63-66` basins `{}` when the cache file is absent — acceptable: the response carries `basins_cached` (:253, :377).
   · Why it matters: the queue and the map are what the write-through acts on; an empty-on-error read is a wrong curated decision one click later.
   · Fix: never cache a failure (`raise` out of the lru_cache'd function, catch in the caller and put `{"error": …}` on the response); the map/awaiting responses carry an `errors: [...]` field the frontend shows (same helper as 4).
   · Confidence: high for 1146 (cached failure), medium for the rest (each is logged or documented as deliberate).

6. **Review-app `--reload` + daemon write-through: a background apply dies with the process and nothing says so** · silent fallback / concurrent lanes
   · `curation-review/run.sh:22`, `backend/run.sh:9` run uvicorn `--reload`; `write_through.py:102` the apply worker is `daemon=True`; `wait_idle` (`:129`) has no caller (no shutdown hook in `app.py`).
   · An agent lane editing any backend file restarts the owner's server; a DFO/entries/splits apply in flight (seconds warm, ~20 s cold, `:12-15`) is killed, `outcomes`/`domains` are gone, the frontend's "saving…" clears to nothing and the curated file is not written until the NEXT mark on that domain re-runs the apply. The mark itself is safe (appended first, `:20-24`).
   · Fix: `@app.on_event("shutdown")` → `WRITE_THROUGH.wait_idle(30)`; drop `--reload` from the owner's run.sh (keep it on a dev flag); on startup, if marks newer than the curated file exist, schedule the domain apply once.
   · Confidence: high (read); harm medium (self-heals on the next mark).

7. **Docstrings still say "THE ONE WRITE" while two write routes exist** · doc/code contradiction + asymmetric logging
   · `curation-review/backend/app.py:8-13`, `writes.py:2-3`, `frontend/src/api.ts:141-142`, `run.sh:7-9`, `backend/run.sh:3` all state one write (a mark). `AGENTS.md:716-722` and `pipeline/tests/test_curation_review_safety.py:190-196` know the exception: `POST /api/inseason/matches` (`app.py:1054-1062`, `inseason.py:277-334`) writes `data/curated/inseason/notice_waters.json` through `writes.commit` — outside the marks log and the write-through.
   · The mark route logs every refusal (`app.py:295-306` `_log_refused` → `writes.log_refused`); the in-season route returns 400 with no log line, so a refused in-season save leaves no trace in `data/generated/review/`.
   · Fix: the four docstrings name both writes; `inseason_save_match` logs refusals the same way (or the owner decides the matches file becomes a marks scope, which would also give it write-through provenance).
   · Confidence: high.

8. **Routes no frontend calls, kept alive by their tests** · dead code
   · Backend routes with no `curation-review/frontend/src` caller (grep of every `/api/…` string and suffix): `GET /api/answer` (`app.py:561`), `/api/entries/{id}/answer/waters` (:546), `/api/entries/{id}/bundle` (:535), `GET /api/marks` (:409), `/api/marks/splits` (:416), `/api/review/lake-splits` (:471), `/api/review/tickets` JSON (:462; the `.md` one is used), `/api/synopsis/row/{key}/page.png` (:1117), `/api/synopsis/page/{printed}.png` (:1160), `/api/entries/{id}/evidence/crop.png|page.png` (:518, :526 — `types.ts:728-731` declares `crop_url/page_url/row_image` but no component fetches them; `Evidence.tsx` does not), `/api/row-image/{filename}` (:1065; `review.py:48` emits the URL, `SynopsisLink.tsx:6,44` reads it only as a boolean). Only `pipeline/tests/test_curation_review_api.py:346-531` and `test_curation_review_screen.py:173-183, 411-414` call `/api/answer` and `/api/marks/splits`. `api.ts` also exports an unused `entries` method.
   · Why it matters: each is a reader of the bundle/reach run that must be kept in step with the data layer (T369 "one load path per screen"); a route only a test exercises is a test seeded from its own input.
   · Fix: delete the routes and their tests, or keep `/api/answer` as the explicit CLI/debug probe and document it as such in API.md. Verify with the safety test's route sweep.
   · Confidence: medium-high (grep-based; a dynamic URL could hide a caller — none found in `api.ts`).

9. **`build-ios-testflight.sh --production` builds and uploads a production store build from `deploy/staging/` with no CONFIRM guard** · safety
   · `deploy/staging/build-ios-testflight.sh:12-14, 42-43` — `--production` sets `EXPO_PUBLIC_APP_ENV=production`, no env switch, and (`:119-121, 124-126`) uploads to App Store Connect. The header says the owner runs it; nothing enforces it, unlike `deploy/prod/site.sh:9` and `deploy/prod/feeds-placeholder.sh:7` (`CONFIRM_PROD=…`). Reading prod data from a store build is a production artefact.
   · Fix: `[[ -z $PROD || ${CONFIRM_PROD:-} == canifishthis.ca ]] || exit 2`, or move the production flag into `deploy/prod/build-ios.sh`.
   · Confidence: high.

10. **`.github/copilot-instructions.md` is generic boilerplate that contradicts AGENTS.md** · dead file / contradicting rules
    · `.github/copilot-instructions.md:1-103` — "Avoid working on more than one file at a time", "Be chatting and teach", "WAIT for explicit user confirmation before making ANY edits", four named reviewer personas for >3 files, WCAG 2.1 AAA, browser polyfills. AGENTS.md:1 ("Read AGENTS.md first — it holds every rule"), owner 10-24 token-efficient tickets, 10-27 start planned tickets unasked.
    · Fix: delete (T335) or replace with one line: "See AGENTS.md".
    · Confidence: high.

11. **`curation-review/API.md` documents removed routes and omits ~25 live ones** · dead docs
    · `API.md:10, 175` describe `POST /api/check`, `GET /api/proposals` (removed, correctly said so) but there is no entry for `/api/review/closed`, `/api/review/write-through`, `/api/marks/health`, `/api/split-anchors`, `/api/dfo/*` (12 routes, `app.py:689-864`), `/api/sloughs/*`, `/api/q154/*`, `/api/reports*`, `/api/synopsis/ref`, `/api/dfo/cascade-regions*`. The documented set (40) ≈ the 2026-10-2x app, not today's 72 routes.
    · Fix: generate the route table from `app.routes` (one test asserts API.md names every route) — T334 scope.
    · Confidence: high.

12. **`check-public.mjs` shrinks its own secret list on a read error** · check seeded weaker on failure
    · `deploy/staging/check-public.mjs:41-46` `readPrivate` → `''` on any failure; `:51, :57-60, :63` `catch {}` around every secret source. A failing `rclone cat` or `rclone config dump` silently drops RUN_TOKEN / the R2 key pair / reporter emails from the by-value scan; step 3 then passes with fewer patterns. The names found are printed (`:99`), so a careful reader can notice.
    · Fix: a secret source that fails is a FAIL line, not a skip (`out(false, 'could not read …')`).
    · Confidence: high.

13. **`build-apk.sh --play` bakes a fixed Claude attribution into the owner's commit** · hygiene / contradicts attribution rule
    · `deploy/staging/build-apk.sh:77-79` — `git commit -m "app: versionCode … \n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>" -- app.json` whoever runs it, whatever model wrote it.
    · Fix: drop the trailer (the script is the author; the person running it is the committer).
    · Confidence: high.

14. **Tracked TypeScript build artefacts** · dead files
    · `curation-review/frontend/tsconfig.tsbuildinfo` (870 B), `tsconfig.node.tsbuildinfo` (9.2 KB) are in git (`git ls-files`), rewritten by every `tsc -b`; also `curation-review/frontend/package-lock.json` + `npm install` (`run.sh:28`) while the rest of the repo is pnpm.
    · Fix: `git rm --cached`, add `*.tsbuildinfo` to `.gitignore`; consider pnpm for the review frontend (one lockfile tool).
    · Confidence: high.

15. **`data-worker.js` turns the rate limit off on any binding error** · silent fallback (by design, documented)
    · `deploy/staging/data-worker.js:17-22` — `catch { return null; }` and `if (!env.RL) return null` ("never take the data down"). A broken RL binding = unlimited. Acceptable for staging data; the same file is the template for the prod worker (`:4-5` says prod "has the same defect").
    · Fix: log once per isolate (`console.warn`) so Workers Logs show the limiter is off; keep the serve-anyway policy.
    · Confidence: high (low harm).

16. **`upload.sh` `q()` evals a shell argument inside Python** · hygiene
    · `deploy/staging/upload.sh:14` `python3 -c "…print(eval(sys.argv[1]))"` — constant strings today; one copy-paste of a path into `q` runs code.
    · Fix: `python3 -c 'import json,sys;… functools.reduce(dict.get, sys.argv[2:], m)'` with the key path as separate args.
    · Confidence: high (low harm).

17. **Two spellings of the fetch_data invocation** · duplicate pathway (low)
    · `scripts/dev-build.sh:26`, `scripts/update-hydat.sh:52` run `(cd data && PYTHONPATH=.. ../.venv/bin/python fetch_data.py …)`; `scripts/bootstrap.sh:45` runs `PYTHONPATH=$PWD .venv/bin/python data/fetch_data.py …` from the root; `pipeline/atlas/freshwater_atlas.py:335-590` tells the user `python -m data.fetch_data --layers …` (`data/__init__.py:7` makes it a package). Three forms, three cwds for a script whose outputs come from `project_config.get_config()` (absolute) — they work today, but any relative path added to fetch_data breaks one of them silently.
    · Fix: one documented form (`python -m data.fetch_data`), the two scripts use it.
    · Confidence: medium.

## Verified clean (no finding)

- No GitHub workflow has `schedule:`; every write workflow is `workflow_dispatch` or a path-filtered push (`feeds-sync-inputs.yml:40-51`, staging only, guarded at `:77-83`). Crons live only in the Cloudflare worker (`app/workers/feeds/wrangler.toml:66-77`), as the owner wants.
- Every `deploy/staging/*` script hardcodes the staging bucket/worker (`upload.sh:10`, `sync-inputs.sh:12`, `backup.sh:16-17`, `restore-test.sh:17-18`, `feeds.sh:9-10`, `rotate-run-token.sh:14-15`, `build-apk.sh:46`, `check-public.mjs:21`, `test-updates.mjs`); no staging wrangler config carries a prod env. Both prod scripts have a CONFIRM guard (`deploy/prod/site.sh:9`, `feeds-placeholder.sh:7`); the workflows' production option is commented out and the bucket guard refuses it (`feeds-runtiming.yml`, `feeds-hydat.yml`, `feeds-sync-inputs.yml`).
- Secrets: no `set -x`, no token echoed; `rotate-run-token.sh` prints only the file path; `build-apk.sh:163-165, 210-212` send the token in a header only; workflows read `secrets.*` into env and never print them.
- Heavy jobs: `build-apk.sh:42-45`, `build-ios-testflight.sh:36-39`, `build-ios-sim.sh:16-19`, `update-hydat.sh:71`, `test-full.sh:6` all go through `scripts/heavy.sh`; `heavy.sh:47` kills only its own job's process group; `curation-review/run.sh:24` kills only its own backend PID. No `pkill`/`killall`, no `git add -A`/`git add .` in scripts, deploy, CI or the review app; `build-apk.sh:77` commits with `-- <path>`.
- Backend writes: `writes.py` is the only curated writer (`reuse.py:1579, 1632`, `inseason.py:333` go through `writes.commit`); `region_cascade.py:450` writes only a generated cache from `__main__`; `write_through.py` never touches marks.jsonl and is off over a sandbox store (`:205-214`).
- `test-full.sh` runs without `-e` deliberately (collects failures, exits 1 at `:42`); `test-smoke.sh` is `-e`.

## archive/ still read by live code

No live code imports or opens anything under `archive/` or `pipeline/docs/archive/` (`git grep` over tracked `.py .ts .tsx .js .mjs .sh .yml .toml .json`; the only `open`/`import` hits are inside `archive/` itself). Every reference is a comment, docstring, `$comment` or test-path exclusion that names the folder as provenance. T335 can delete both folders without a code move; these lines should be reworded (or the referenced doc moved) so they do not dangle:

| file:line | archive path named | proposed new home / action |
|---|---|---|
| `pipeline/__init__.py:15` | `archive/pipeline/` ("recurring subpackages were archived … reference only") | reword: "removed 2026-11 (T335); the feeds worker replaced them" |
| `pipeline/atlas/reach/__init__.py:20` | `pipeline/docs/archive/REACH-BUILDER.md` ("Design:") | move REACH-BUILDER.md → `pipeline/docs/REACH-BUILDER.md` (live design doc, still cited by code) |
| `pipeline/atlas/reach/classify.py:17` | `pipeline/docs/archive/REACH-BUILDER.md §"open policy"` | same move |
| `pipeline/gauges/review.py:44` | `pipeline/docs/archive/HANDOFF-curated-layout.md §8` | move §8 (the two-tier curated/generated split) into `pipeline/docs/18-how-regulations-are-stored.md` or AGENTS §curated; reword |
| `.github/workflows/pipeline-ci.yml:15` | `pipeline/docs/archive/HANDOFF-curated-layout.md §8` | same |
| `pipeline/tests/test_export_codec.py:14` | `pipeline/docs/archive/06-ui-data-contract.md Part 6` | reword to the current contract doc (`pipeline/docs/18-…` / `pipeline/deliver/export` docstring) |
| `pipeline/deliver/answers/__init__.py:21`, `answers/common.py:202`, `answers/rows/__init__.py:3`, `deliver/store/common.py:171` | `archive/pipeline/answers-reference-v35/golden.js`, `page_v35.js`, `answers-reference-v36/page_data.py` (the v35 spec the port was made from) | reword to "the v35 harness (retired, T335)"; the spec now IS `rows.py`/`answers_v35_port.py` — say so |
| `pipeline/tests/answers_v35_port.py:3-4, 13-15, 540`, `test_answers_v35_port.py:3, 339` | `archive/pipeline/answers-reference-v35/` (fixture "made by the archived harness") | keep the fixture (already in `pipeline/tests/`); reword the provenance comment; the `made_by` string at :540 is data in a golden — leave or regenerate |
| `pipeline/tests/test_dataflow_gates.py:317` | `archive/pipeline/answers-reference-v35/` (exhibit "outside these roots") | reword |
| `pipeline/tests/test_fwa_edge_types.py:54`, `test_t117_walks.py:196, 270`, `test_wsc_relation.py:103` | `"/archive/" in rel` path exclusions | harmless after deletion; drop the clause |
| `pipeline/regs/dfo_salmon/fetch.py:5` | `archive/pipeline/recurring/in_season/scraper.py` (curl_cffi impersonate precedent) | reword: "the retired in-season scraper used the same" |
| `pipeline/gauges/generate/match.py:14` | `archive/…/HANDOFF_gauge_fwa_matching.md` (434/450 measurement) | keep the number, cite "v1 pass, 2026-09" without the path |
| `app/workers/feeds/src/index.ts:2`, `src/inseason.ts:2` | `archive/pipeline/recurring/in_season/` (what the worker replaced) | reword |
| `app/packages/map/src/controls.css:10`, `hatch.ts:8`, `runtime-style.ts:468`, `style/layers.source.json:2166`, `app/tools/build-style.mjs:291, 543, 602` | `archive/webapp/src/components/Map.tsx`, `src/map/styles.ts` (v1 formulas carried over) | the formulas live in these files now; reword "(from v1, retired)" |
| `app/packages/ui-native/src/FishSpinner.native.tsx:6`, `app/tools/build-spinner.mjs:6, 18, 50, 56, 65, 72, 111`, `app/tools/build-icons.mjs:8` | `archive/webapp/src/components/FishLoader.tsx`, `archive/webapp/public/favicon.svg` (the spinner/icon provenance) | comments only: `build-icons.mjs:25-27` reads `app/apps/mobile/assets/icon/favicon.svg` (verified); the spinner's numbers are inlined. Reword "(from v1, retired)" |
| `app/tools/seo/legacy-crosswalk.mjs:5` | `archive/webapp/scripts/prerender.mjs` (v1 URL scheme) | reword |
| `deploy/staging/data-worker.js:3` | `archive/r2-worker` (the double-gzip defect) | reword |
| `NOTICE:93` | "archive/ holds retired code; it may carry dependencies under their own licences" | delete the line with the folder |
| `AGENTS.md:10, 237, 299` and `README.md`, `app/README.md`, `pipeline/docs/00-README.md`, `GOTCHAS.md`, `18-…`, `19-…`, `dfo_salmon/CURATION.md`, handoff/* (see `git grep -c archive/ -- '*.md'`) | `pipeline/docs/archive/*` as "deep context" | AGENTS.md:10 is the index of the archived docs: either keep `pipeline/docs/archive/` (then T335 deletes only `archive/`) or move the four still-cited docs (REACH-BUILDER, HANDOFF-curated-layout, 06-ui-data-contract, 13-build-plan) up to `pipeline/docs/` and drop the rest — owner decision |

Checked for T335: `app/tools/build-icons.mjs:25-27` reads `app/apps/mobile/assets/icon/favicon.svg` and `build-spinner.mjs` inlines its numbers — neither opens `archive/`. `pnpm check` (spinner/icon byte gates) and `pipeline build --graph --dry-run` are the proof after the delete, as the ticket says.
