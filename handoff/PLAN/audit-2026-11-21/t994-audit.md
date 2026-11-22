# T994 audit: production-level findings (read-only, 2026-11-21)

Branch `redesign/stream-sections` @ 0d15cc5a8. Nothing edited, built or run (no tests, no scripts, no network).
Method: AGENTS.md, both MAPs, the 2026-11-16 audit (code.md, guards.md) and the open-ticket ledger read first. Five
read-only Sonnet sub-auditors each took one area: the feeds worker, the review backend, deploy/ + scripts/, pipeline
Python, and the app data seam. I re-read the load-bearing lines of findings 1-5, 7, 8, 10, 11, 14, 16, 21-23 myself.
The rest are the sub-auditors' readings, quoted from the code; they are marked (sub) where I did not re-open them.

Already in a ticket, so left out: T558, T559, T609/T608C W1-W20, T560 (slug copies, the atomic-write copies, which
release is live), T788 (pieces_gate, site.sh dirty stamp, the --no-basemap flags, the channels id() cache,
`_region_rows` / `cut_label`, heavy.sh disk), T977, T851, T633, T555, T676, T737/T738/T756, T993/T978 (CI cleanup in
general), and the 11-16 audit's items.

Sizes: S = under an hour, M = half a day, L = more.

---

## 1. `scripts/update-hydat.sh --upload` publishes a release to any bucket, with no gate
- **Where:** `scripts/update-hydat.sh:80-84` → `pipeline/deliver/release/publish.py:93-116`.
- **What:** step 4 runs `publish --release "$REL" --yes`. The upload goes to `os.environ["R2_BUCKET"]`, whatever it
  is: `s3.upload_file(str(p.path), os.environ["R2_BUCKET"], p.key, ...)`. Nothing checks it against a bucket list or
  `CONFIRM_PROD`. None of the deploy gates run (`app_gate`, `pieces_gate`, `site_gate`, `ci_status`, the
  staging-equals-prod manifest check). `--yes` also re-stamps the local `manifest.json` (`stamp_file`, line 96) and
  writes `PUBLISHED` (line 116).
- **Why it matters:** T404 moved production onto `deploy/prod/upload.sh` ("publish.py no longer in that path"), but this
  second publish path survived. With the production key in the environment it writes production directly, ungated.
  The re-stamp changes the manifest's bytes, so the next `deploy/prod/upload.sh` byte-identity check (`:43`) fails
  against staging. `pipeline/runtiming/feed/publish.py` already has a bucket allowlist plus `CONFIRM_PROD`
  (`checked_bucket`, T404); this publisher has neither. `pipeline/docs/MAP.md:29` still says `--prod` goes through
  `publish.py`.
- **Fix:** replace step 4 with a printed `deploy/staging/upload.sh <r-id>` (or a call to it). Either remove `--yes` from
  `deliver/release/publish.py` or give it `checked_bucket` + `CONFIRM_PROD`. Fix the MAP line.
- **Size:** S. **Confidence:** high.

## 2. Review backend: any web page can start a re-resolve and a staging publish (CSRF)
- **Where:** `curation-review/backend/app.py:1364-1370` (`POST /api/inseason/resolve`, no body); CORS at `app.py:41-49`.
- **What:** the route takes no body, so a cross-site `fetch(url, {method: "POST", mode: "no-cors"})` is a "simple"
  request with no preflight. CORS stops the page reading the answer, not sending the request. The route runs
  `inseason.resolve_all()`, then `INPUTS_SYNC.request(...)`, which starts `deploy/staging/sync-inputs.sh` in the
  background. No Origin or Host check exists, so DNS rebinding also reaches `/api/marks` and `/api/inseason/matches`.
  The comment at `:41-44` claims CORS "stops another page … posting a mark"; that holds only for reads.
- **Why it matters:** any page open in the owner's browser can rewrite `data/generated/inseason/resolved.json` and push
  it to staging.
- **Fix:** one middleware that refuses every non-GET `/api` request whose `Origin` is present and is not `VITE_ORIGIN`,
  and any `Host` other than `127.0.0.1:<port>` / `localhost:<port>`. A test posts with a foreign Origin.
- **Size:** S. **Confidence:** high for the resolve route; medium for rebinding.

## 3. Feeds worker: report text reaches the 400-day monthly backups through the notify outbox
- **Where:** `app/workers/feeds/src/backup.ts:28` (`BACKUP_PREFIXES = ["private/", …]`) and `:40-48` (`personalKind`);
  `events.ts:19, 67-72` (`NOTIFY_STATE`, `pending: FeedEvent[]`); `reports.ts:490` (`words: r.text`).
- **What:** `personalKind` knows the report and crash day files, but `private/feeds/notify/state.json` returns `null`
  (not personal), so it is copied to `daily/` and, on the 1st, to `monthly/`. Untold report events sit in `pending`
  with the report's text, install id and place until the 15:00 UTC digest. The 09:25 UTC backup catches them every
  day. (sub: digest and backup times)
- **Why it matters:** T559 made the report and crash files obey the 180-day promise on the privacy page. This is a second
  route around that: a typed email address in a report can stay 400 days.
- **Fix:** classify `private/feeds/notify/` as daily-only or `never`, or keep only the report id in `pending` and
  re-read the text at send time. Make `forgetReport` scrub `pending`.
- **Size:** S. **Confidence:** medium-high.

## 4. Feeds worker: a failed read of the in-season feed resets its history and skips the W14 guard
- **Where:** `app/workers/feeds/src/inseason.ts:253-260`.
- **What:** `try { const t = await deps.feed!.get("index.json"); prev = … JSON.parse(t) } catch { prev = null; }`. An R2
  error or a corrupt file becomes "first run":
  - `buildFeed(rows, null, …)` restamps `changedAt`;
  - the `removed` history is dropped;
  - the missing-`matches.json` guard on the next line (`linksText === null && prev`) cannot fire.

  The run is then written back and recorded as ok.
- **Why it matters:** this is T558 #1's fault class in a file T558 did not touch. One R2 blip silently rewrites the
  public in-season feed's history.
- **Fix:** read through `io.readStored` (throws on a failed read or parse). Keep `null` only for an absent file or a
  different `format`.
- **Size:** S. **Confidence:** high.

## 5. Gauge publisher: a failed or corrupt history read wipes the water-temperature history (worker, and its Python twin)
- **Where:**
  - worker (production): `app/workers/feeds/src/gauges/publish.ts:972-984`, then `priv.put(TEMP_HISTORY, …)` at `:993`;
  - Python parity spec: `pipeline/gauges/feed/publish.py:622-636`.
- **What:** both do `try … JSON.parse … catch { hist = {} }` and then write the near-empty history over the file. The
  docstring says this history is the only source ("starts now rather than when it is wanted").
- **Python twin's other cases:** the same file's `previous()` (`:848-857`, a station's year-to-date daily archive
  returns `{}`) and the previous index (`:832-835`, every maintenance `since` resets) have the same shape. That twin
  only feeds the local dev feed (AGENTS 64), but it is the parity spec, so fix both together.
- **Why it matters:** one transient R2 failure deletes months of temperature samples, permanently, with nothing logged.
- **Fix:** treat only an absent file as a first run. A failed read or bad JSON fails the step, and the last good file
  stays.
- **Size:** S. **Confidence:** high.

## 6. GitHub workflows: no `permissions:` anywhere, and one input spliced into shell text
- **Where:**
  - `.github/workflows/staging-smoke.yml:39`: `--slug "${{ inputs.slug }}"`;
  - none of the six workflows declares `permissions:` (grep finds none).
- **What:** a dispatch input containing `"; …` runs commands. Each `GITHUB_TOKEN` gets the repo default (read-write on
  older repos), including the three workflows that hold R2 keys (`feeds-hydat`, `feeds-runtiming`,
  `staging-security`).
- **Why it matters:** the standard least-privilege hardening is missing. The injection needs write access to dispatch,
  so its impact is low.
- **Fix:** `env: SLUG: ${{ inputs.slug }}` with `"$SLUG"`, and a top-level `permissions: contents: read` in every
  workflow (widen per job only where needed).
- **Size:** S. **Confidence:** high (injection), medium (whether the repo default token is read-write).

## 7. The CI "no hard-coded data path" gate misses every multi-segment literal: 10 are live
- **Where:** gate at `.github/workflows/pipeline-ci.yml:140`. The pattern `'"data/[a-z_]+\.(json|…)"'` matches one path
  segment only.
- **What it misses:**
  - `pipeline/gauges/feed/hydat_daily.py:66,71`: `Path("data/source/hydat.sqlite3")`,
    `Path("data/generated/gauges/hydat_daily")`. The `--out` directory is then pruned with `unlink`, `:76-78`.
  - `gauges/feed/publish.py:194`: `ROSTER_PATH`.
  - `regs/dfo_salmon/closure_mouths.py:164`.
  - `regs/review/tidal.py:146`.
  - `regs/parsing/ingest_catalogue.py:511`: the curated corpus `--out`.
  - `cache/dfo_salmon/...` literals in `splitwork.py:35-36`, `churn.py:55`, `validate.py:202`, `entries.py:1204`.
  - `refresh.py` anchors the same cache on `__file__` instead.
- **Why it matters:** W46 and AGENTS 41 forbid these, and CI is meant to grep for them. Each one is relative to the cwd,
  so a run from another directory reads or prunes a second tree silently. The DFO cache is named two ways.
- **Fix:** widen the pattern to `"(data|cache)/[a-z_]+/`. Add `SOURCE`/`GENERATED` keys for the hydat file and the DFO
  cache, and move the ten literals onto them.
- **Size:** M. **Confidence:** high.

## 8. App: the salmon, in-season, run-timing, HYDAT-day and weather reads have no timeout
- **Where:**
  - `app/apps/mobile/src/App.tsx:232, 271, 293` pass `undefined` (the bare global `fetch`);
  - `packages/data/src/feed/salmon.ts:61, 72`, `feed/inseason.ts:62`, `spots/dayFlow.ts:60`, `spots/weather.ts`,
    `notify/conditional.ts:20` call it directly.
  - Only the gauge feed has limits (`feed/http.ts:273` `FEED_LIMITS`), as do `net/fetch.ts` `fetchResilient` and
    `lane.ts`.
- **What:** React Native `fetch` has no default timeout. A half-open socket leaves the held promise pending, and the TTL
  re-ask never fires for a promise that never settles. (sub: the held-promise path)
- **Why it matters:** on a weak rural connection, the Salmon card, the in-season banner and run timing can spin until the
  app restarts, and nothing is reported.
- **Fix:** route them through `fetchResilient`, or wrap the injected fetch once in `App.tsx` with `AbortSignal` and a
  total time.
- **Size:** M. **Confidence:** medium-high.

## 9. `deploy/lib/input-formats.sh:27`: the format gate fails open
- **What:** `fmt=$(jq -r '…' "$out/$key" 2>/dev/null || true)`, then `if [[ -n "$fmt" ]]`. A missing jq, or a corrupt
  or empty staged file, gives an empty `fmt`: "no format", so the file is put to R2 without asking the worker.
  `deploy/prod/sync-inputs.sh:59` calls jq after the puts, not before. (Line 35 of the same file is T788.)
- **Why it matters:** the gate exists because of the 2026-11-09 `matches.json` outage. W46 says never `|| true`.
- **Fix:** drop `|| true` and `2>/dev/null`, and make a jq failure a `refuse` for that key. Add `command -v jq` at the
  top of both sync-inputs scripts.
- **Size:** S. **Confidence:** high.

## 10. Production upload has no site-version gate
- **Where:** `deploy/prod/upload.sh:43-48` runs `app_gate` and `pieces_gate` only. Staging also runs `site_gate`
  (`deploy/staging/upload.sh:26`, added after the 2026-11-09 minAppVersion failure).
- **Why it matters:** a release whose `minAppVersion` is newer than the production website makes the production web app
  refuse it ("map data could not be loaded"). The staging fix never reached production.
- **Fix:** `site_gate "$M" --site https://canifishthis.ca` when `$BUCKET == $PROD_BUCKET` (`site_gate` already takes
  `--site`). Check the prod runbook's order (site before data) at the same time.
- **Size:** S. **Confidence:** medium-high.

## 11. App: a synopsis-PDF recovery failure is swallowed
- **Where:**
  - `app/packages/ui-native/src/regs/synopsisPdf.native.ts:62`: `await s.alternate().catch(() => null)`;
  - the alternate at `apps/mobile/src/App.tsx:637`: `(await (await fetch(ENDPOINTS.manifest)).json() as {…})`, with no
    `res.ok` check, no timeout and no parse.
- **What:** this is the only manifest read that skips `loadManifest`/`parseManifest`. A 404 or HTML body throws, and the
  throw is turned into null.
- **Why it matters:** the T285 recovery fails silently and the reader sees only "can't open the PDF". W46 forbids the
  bare catch; `tools/failures.test.ts` should have counted it (check its `FINE` count for this file).
- **Fix:** `loadManifest(ENDPOINTS.manifest)`, and `.catch((e) => { reportFailure(e); return null; })`.
- **Size:** S. **Confidence:** high.

## 12. App: `storedHydat` never checks its file's format
- **Where:** `app/packages/data/src/spots/dayFlow.ts:62`: `return (await res.json()) as StoredDaily;`. Lines 72-73 check
  only `Array.isArray(doc.q)`.
- **What:** `format` (`hydat-daily/1`) and `from` are never checked, and there is no feed gate. With `from` undefined,
  `y - doc.from` is NaN, so every day answers `{kind: "none"}`.
- **Why it matters:** every other feed checks its format (T676). This one would print "no record that day" for every
  gauge after a format change, silently.
- **Fix:** `parseHydatDaily(raw)` that checks the format, an integer `from` and `q` as an array of arrays, throws a named
  error and reports it.
- **Size:** S. **Confidence:** high.

## 13. App: `parseManifest` checks four fields and then casts; a non-JSON body is not a `ManifestError`
- **Where:** `app/packages/data/src/manifest.ts:47-53` (`return m as Manifest`); callers parse first:
  `release/boot.ts:289`, `manifest.ts:63`, `update/service.ts:555`, `release/install.ts:532`.
- **What:** no `files[name]` entry is checked for `url`, `bytes`, `sha256` or `format`, though `readsRelease`,
  `verify` and `diff` dereference them. `JSON.parse` of an HTML 200 page throws a raw `SyntaxError`, which
  `resilientManifest` handles differently from a `ManifestError`. (sub)
- **Why it matters:** a pipeline drift then fails deep in install as `TypeError: undefined`, not at the boundary with a
  name.
- **Fix:** validate each file entry in `parseManifest`, and wrap the parse so a non-JSON body throws
  `ManifestError("manifest: not JSON")`.
- **Size:** S-M. **Confidence:** high.

## 14. Review backend: `_fwa_lake_poly` swallows every error, caches the None, and leaks the connection
- **Where:** `curation-review/backend/reuse.py:2081` (`lru_cache(4096)`) and `:2100-2124`.
- **What:**
  - `if not src.exists(): return None`;
  - `except Exception: # noqa: BLE001 … return None`;
  - `db.close()` runs only on the success path.
- **Why it matters:** a missing gpkg or a sqlite or WKB error silently draws the through-line instead of the lake on the
  verification map. That is the exact defect the function exists to fix (Kootenay). The None stays cached for the
  life of the process. AGENTS W46 requires a 503 naming the cause.
- **Fix:** raise on a missing gpkg. Catch only a key with no row (return a typed "no polygon"). Use
  `contextlib.closing`.
- **Size:** S. **Confidence:** high.

## 15. Review backend: three more broad catches that turn failures into answers (sub)
- **Where:**
  - `awaiting_water.py:77-78` and `:91-92` (`except Exception: return None`; `_mu_centre` is `lru_cache`d);
  - `reports.py:93-112` (`_vintage` / `_bundle_items` / `_node_of`: a missing bundle gives `items: []`, `node: None`);
  - `dfo.py:467-469` (`walks_now` gives "unknown") and `:877-883` (labels fall back to the verbatim text);
  - `inseason.py:105-108` (`_derived_label` returns the raw pick id).
- **Why it matters:** in the sign-off tool, a real bug reads as a legitimate "no position", "no water" or "unknown".
- **Fix:** catch the specific refusal types only. Let everything else reach the 503 handler, or return a typed error
  the UI shows.
- **Size:** S. **Confidence:** medium-high.

## 16. Pipeline: DFO split coordinates collapse every failure to `{}`
- **Where:** `pipeline/regs/dfo_salmon/splitwork.py:322-358`. One `try` covers `splits.resolved.json`, the registry,
  the gpkg and geopandas, and ends in `except Exception: return {}`. `item_blks` and `mine` (`:333-334`) are built
  and never used.
- **Why it matters:** AGENTS 37 and W46: absent data is a bug, never an empty set. Callers see "no coordinates" for every
  split.
- **Fix:** drop the try (a `FileNotFoundError` already names the file) and delete the dead locals.
- **Size:** S. **Confidence:** high.

## 17. Pipeline forecast fetch: any error reads as "not in season"
- **Where:** `pipeline/gauges/feed/forecast.py:341-346`: `except Exception: # noqa: BLE001 — 404 = not in season`,
  then `return None`.
- **What:** timeouts, 5xx, DNS errors and format errors all look like a station with no forecast. The caller
  (`publish.py:1192`) prints only on exceptions, and none can arrive.
- **Why it matters:** an RFC outage or a format change looks like a province with no forecasts. Check the worker's
  forecast twin for the same shape; I did not open it.
- **Fix:** treat only `HTTPError` 404 as None. Count everything else and report "N stations failed".
- **Size:** S. **Confidence:** medium-high.

## 18. `deploy/staging/sync-inputs.sh:28`: `--dry-run` is read from `$1` only
- **What:** `DRY=(); [ "${1:-}" = "--dry-run" ] && DRY=(--dry-run)`. `--dry-run` in second place, or a typo, does a real
  put. `deploy/prod/sync-inputs.sh:21-27` has a loop that refuses unknown arguments, so the pair has drifted. (sub)
  The staging lock (`:32-38`) also has a take-over race where a waiter can `rm -rf` the new holder's lock.
- **Fix:** copy prod's argument loop. Take the lock with heavy.sh's mv-based take-over.
- **Size:** S. **Confidence:** high.

## 19. Deploy scripts: remaining `|| true` and `2>/dev/null` that hide tool failures (W46)
- `deploy/staging/site.sh:30`: `grep -rl … | xargs perl -pi … || true`. A failed rewrite leaves the production
  "Open the app" link on the staging hubs, and nothing checks for it afterwards.
- `deploy/staging/site.sh:12` and `deploy/prod/site.sh:26`: `js=$(grep -o … "$W/index.html" | sort -u)` exits 1 with no
  message when no bundle is named. An empty `one=$(find …)` then gives the misleading "built against another app
  export". (sub)
- `deploy/staging/upload.sh:46-49`: `RT_HAVE=$(rclone cat … 2>/dev/null | python3 … || true)`. A dead rclone or bad
  credentials read as "staging has no run timing". (sub)
- `deploy/staging/restore-test.sh:43`: `rclone check … | grep -oE … || echo "0 differences found"`. A failed check
  prints "0 differences". (sub)
- `deploy/staging/feeds.sh:14, 20` and `deploy/prod/feeds.sh:27, 30`: `wrangler secret list 2>/dev/null | grep -q
  REPORT_SALT`. An expired login reads as "not set". `grep -q` can also SIGPIPE wrangler under pipefail, which
  `site.sh:18` already warns about. `deploy/staging/feeds.sh:44` shows only `tail -4` of a failed deploy. (sub)
- **Fix:** capture output and rc first, then test. Add a post-check where a step rewrites files.
- **Size:** S (all together). **Confidence:** high.

## 20. `scripts/start-workflow.sh:43, 54`: a gh blip after an accepted dispatch exits with the wrong code
- **What:** under `set -e`, the bare `run=$(gh run list …)` and `concl=$(gh run view …)` exit with gh's rc 1, not 75.
  `deploy/staging/upload.sh:64-65` then says "did not start" while a run is going, and a rerun dispatches twice. The
  script's own header says that double dispatch must not happen.
- **Fix:** `run=$(gh …) || { echo "dispatch accepted; cannot read runs: check Actions" >&2; exit 75; }`, and the same
  for `gh run view`.
- **Size:** S. **Confidence:** medium-high.

## 21. Review backend: `requirements.txt` floors, not pins, and missing direct deps
- **Where:** `curation-review/backend/requirements.txt`: `fastapi>=0.110`, `uvicorn[standard]>=0.29`.
- **What:** pydantic and shapely are used directly and not listed; the comment says the repo .venv has them. The
  installed versions (fastapi 0.141.1 / starlette 1.6.0, per the sub-auditor) are what keeps a `text/plain` mark
  post refused; 0.110 would not.
- **Fix:** pin the tested versions, or move the backend into the repo's locked Python dependencies.
- **Size:** S. **Confidence:** high.

## 22. App: `release/previous` and `release/refused` are read with no catch and no shape check
- **Where:** `app/packages/data/src/release/install.ts:296-299` (`JSON.parse(…) as Held`); `release/boot.ts:504` and
  `:549` have no catch, while `:466` (OPENED) catches and reports.
- **Why it matters:** a corrupt helper pointer aborts every boot, although the main pointer is fine. The writes go
  through `place` (likely atomic), so this is unlikely. `sameSet` does `Object.keys(a.files)`.
- **Fix:** shape-check in `readHeld`, and give both calls OPENED's `.catch(reportFailure → null)`.
- **Size:** S. **Confidence:** medium.

## 23. Feeds worker: a new APNs token is minted on every wake (sub)
- **Where:** `app/workers/feeds/src/push.ts` `apnsJwt` (~155), called once per `sendWake` (~299).
- **What:** wakes can be 10 minutes apart (crons at :05, :15, :35 and :45, plus hand runs). Apple answers
  `TooManyProviderTokenUpdates` (429) when the token is refreshed more often than every 20 minutes. Each such failure
  counts against `RELEASE_WAKE_TRIES`, and after three the release is marked told.
- **Fix:** keep the JWT and its `iat` in R2 and reuse it for 40-50 minutes.
- **Size:** S. **Confidence:** medium (the Apple rule was not checked live).

## 24. Feeds worker: backup and prune subrequests grow without bound; a partial snapshot has no manifest (sub)
- **Where:** `app/workers/feeds/src/backup.ts` `runBackup` (~113-135) and `pruneBackups` (~172-190).
- **What:**
  - every daily run re-copies every kept report and crash kind (one get plus one put per target);
  - the prune does one get per kind per day;
  - the manifest is written last;
  - `skipped` never sets `ok` to false.
- **Why it matters:** near the Workers subrequest cap (about 4,500 reports), the run dies partway with no manifest. The
  W1 flood pace reaches that in weeks.
- **Fix:** copy reports incrementally (they are immutable), count subrequests and stop clearly before the cap, and treat
  a non-empty `skipped` as degraded.
- **Size:** M. **Confidence:** medium.

## 25. Feeds worker: `/push/register` has no daily total (sub)
- **Where:** `app/workers/feeds/src/push.ts:25, 93-120`: `PUSH_PER_DAY = 200` per address, no `dailyTotal`.
- **What:** W1 added a daily total to reports and crashes, not to push. Junk tokens from rotating addresses fill
  `private/push/tokens/` and take send slots (`PUSH_SENDS_PER_RUN` 1,500) ahead of real phones until APNs rejects them.
- **Fix:** a `dailyTotal` on register, and tell the owner once when it trips.
- **Size:** S. **Confidence:** medium.

## 26. Pipeline feed notify: an unreadable dedupe state re-sends every notice (sub)
- **Where:** `pipeline/gauges/feed/notify.py:77-82` (`except Exception: return []`); `send_due` records with a plain
  `write_text`.
- **Fix:** tolerate only `FileNotFoundError`, and write atomically.
- **Size:** S. **Confidence:** high (low impact: owner spam).

## 27. Pipeline DFO live: `_read_texts` drops a page-text parse failure silently (sub)
- **Where:** `pipeline/regs/dfo_salmon/live.py:661-664` (`except Exception: return`). Its comment says the failure "is
  told", but no notice is raised for this path, unlike `_read_header` at `:614-618`.
- **Fix:** add an urgent notice, as `_read_header` does.
- **Size:** S. **Confidence:** medium.

## 28. `deploy/prod/upload.sh:88`: a precondition checked after the uploads (sub)
- **What:** `[[ -d data/generated/gauges/hydat_daily ]] || exit 1` runs after tiles, data and feed seeds are sent (the
  pointer is still last, so the release stays correct). Staging treats the same directory as optional
  (`staging/upload.sh:91`).
- **Fix:** move the check up beside the `$M` check, and make the pair agree.
- **Size:** S. **Confidence:** high.

## 29. `deploy/staging/build-apk.sh:451, 503`: the APK email result is not enforced on the full-build path (sub)
- **What:** `code=$(curl -s … "$FEEDS/notify/apk" || true)`, then `exit 0` whatever the code. `--publish-only` (`:195`)
  exits 1 on a non-200, so the two paths disagree.
- **Fix:** pick one behaviour, using `curl --fail-with-body`.
- **Size:** S. **Confidence:** high.

## 30. Copy-paste in the worker's public routes (sub)
- **What:** the `json()` response helper and the bounded body-read + parse block are repeated in `reports.ts`,
  `crashes.ts` and `push.ts`. A future fix to the byte cap or the error shape lands in one and not the others.
- **Also:** `index.ts:926` `checkToken` and `dispatch.ts:~128` `fetch_` are uncaught on `POST /run/check-github`, so a
  GitHub network error is a bare 500, not the `{ok: false}` the sibling routes return.
- **Fix:** move the helper and the reader into `io.ts`, and wrap the dispatch route.
- **Size:** S. **Confidence:** high.

---

## Checked and sound (no finding)
- **Review backend:**
  - binds 127.0.0.1, and CORS is `VITE_ORIGIN` only;
  - the `row_image` path parameter is regex-bound;
  - no `shell=True`;
  - every `lru_cache` is bounded;
  - the write routes take pydantic models.
- **Feeds worker:**
  - the token compare is constant-time and the token is never read from the query string;
  - report, crash and push bodies are shape-checked and byte-capped;
  - mail subjects are ASCII or base64;
  - the status and email HTML is escaped;
  - CORS `*` only on the public report and crash routes.
- **App:**
  - every non-`@app` dependency has a `deps.md` line;
  - production pins are exact;
  - no secrets in config (the ArcGIS key is the documented referrer-restricted one);
  - the hooks read (prefetch, crashReport, net, imagery, dayFlow) clean up;
  - `fetchVerified` / `installAssets` check bytes and sha.
- **Pipeline:**
  - no `urlopen` without a timeout;
  - no mutable default arguments;
  - no unsorted glob feeding an output;
  - no clocks in build output beyond the known `build_ts`.
- **Deploy:**
  - the staging/prod wrangler tomls differ only in names, buckets and routes;
  - heavy.sh, pytest-chunked, test-*.sh, rollback, drop-v1 and backup.sh read sound;
  - no `pull_request_target`.

## Not verified
- No `shellcheck` (not installed) and no `bash -n` run.
- The worker's forecast twin (finding 17).
- `build.py:447/454` subprocess return codes.
- Apple's token-refresh limit, against the live service (finding 23).
- Whether the repo's default `GITHUB_TOKEN` is read-write (finding 6).
- The exact held-promise path in finding 8.
