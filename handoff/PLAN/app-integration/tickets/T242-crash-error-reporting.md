# T242 — crash and error reporting (privacy-light)

Status: PARTIAL — built and tested (960d2d8, 31c8d04); 2026-11-06 the crash TEST BED proves all three kinds on the emulator (62046a681, run.mjs crash-bed: a JS error, an unhandled promise, a native SIGSEGV each reach the worker's own POST /crash handler on a local stub, no staging write) and fixed a double report of a JS fatal; left (owner): read GET /crashes once, approve the privacy wording, T407's phone check after the 0.6.0 worker deploy; iOS is T362.

Source: owner 2026-10-28 — approved proposal P8 in handoff/PLAN/proposed-tickets-2026-10-28.md ("approved ALL 16").

## Work
1. Choose: Sentry free tier vs a tiny Cloudflare endpoint (default: Cloudflare endpoint — no third party, no SDK weight).
2. Report uncaught JS errors + native crashes (Expo's error handler) with: app build, release, platform/OS, the error,
   the stack, the route kind (map / water / menu — no water names, no coordinates, no ids that identify a person).
   Sampling + dedupe by stack hash; never sends offline (drops).
3. A switch in More → Data ("Send crash reports", default on, said in the privacy policy, T244).
4. Owner digest: new error kinds once a day (T225 outbox).
## Acceptance
A forced error on web + Android lands once; the payload test pins every field; turning the switch off sends nothing.
Depends: T225 (outbox), T249 (rate limits). Size: S.
Owner: yes — Sentry vs own endpoint (default own); default on/off (default on, anonymous).

## Built (960d2d8)
Owner rule 2026-10-29: NO third-party crash service; everything from our own servers, through the feeds worker / R2 like problem reports.
- **Worker** (`app/workers/feeds/src/crashes.ts`, routes in `index.ts`): `POST /crash` (public, CORS, <= 8 KB, `crash/1`, every key
  known else 400) -> per-IP gate (1 per 3 s, 40 a day; salted hash in `private/crashes/limits/`, same `rateLimit` as reports, now
  parameterised) -> ONE object per error kind `private/crashes/<hash>.json` {first, last, count, versions (<= 20), sample}; the hash
  is name + message + the stack's first function names with every number removed, so a kind is one object across builds. No IP /
  User-Agent / country stored. `GET /crashes?since=` (Bearer run token; RL_AUTH class). Digest: the gauge run turns a kind whose
  `first` is today/yesterday and not yet told into ONE `crash` digest line (not urgent; "New app errors" section in the email);
  repeats never. Daily purge (with the report purge): kinds not seen for 90 days are deleted.
- **App** (`@app/ui` `crashReport.ts`): `crashPayload` (pinned whole by test: format, created, kind js/promise/render, fatal, route KIND,
  app, env, platform, release, name, message, stack); `scrubMessage` removes quoted text, web/e-mail addresses, coordinates and 5+ digit
  numbers; `scrubStack` cuts paths/URLs to file names; `routeKind` (map / conditions / runs / spots / menu / water / other; never a
  water or place). `createCrashReporter`: switch off -> nothing; offline -> dropped (never queued); same error once per run; <= 5 kinds
  a run; a FATAL error is also kept in one slot on the device (`crash-pending.json` / localStorage) and sent at the next start, then
  cleared (forgotten unsent if the switch is off). `installCrashHandlers`: RN `ErrorUtils` global handler (the previous handler still
  runs, so the red box / crash is unchanged) and, on the web, `error` + `unhandledrejection`. Wired in `apps/mobile/src/App.tsx`
  (landed inside another lane's commit 57bb1144e), URL from `env.ts` `crashUrlFor` (the report worker's `/crash`; none for production
  until its worker exists; `EXPO_PUBLIC_CRASH_URL` overrides, `off` disables).
- **Switch**: More -> Data -> "Send crash reports" (default ON, switch role, hint says what goes); kept on the device
  (`openCrashSettingStore`).
- **Privacy page** (`tools/seo/html.ts`): the short version names both things the app sends, and a new "App errors (crash reports)"
  section lists every field, what is removed, the offline/fatal rule, the 90-day retention and how to turn it off.
- Tests: `workers/feeds/test/crashes.test.ts` (store + fold + no IP/UA, limits, malformed/oversized/unknown-key refusals, routes,
  digest once, purge), `ui/src/crashReport.test.ts` (payload pinned, scrub, switch, offline, dedupe, fatal kept/sent/cleared, handlers
  chain), `ui-native/menu/CrashSwitch.test.tsx`, `apps/mobile/env.test.ts`.

## Owner answers 2026-10-29 and what was done (31c8d04)
- Default ON approved. Staging worker deployed by the owner (first version).
- **Web forced error on staging web (2026-10-29)**: a throw and an unhandled rejection in the dev web app (local env -> staging worker): the throw
  POSTed `crash/1` and got **201**; the rejection 1.5 s later got 429 (the deployed gap is 3 s per IP; now 1 s in code, needs the redeploy).
  The payload on the wire was scrubbed ("forced T242 check '...' #,#"; stack `at <anonymous>:1:24`, route `map`). curl: bad key 400, GET /crashes without
  token 403. Reading `GET /crashes` needs the run token (not used by Claude): owner can check `GET /crashes?since=2026-10-29` shows TestError /
  TypeError "forced T242 check" (and app `t242-check`).
- **Native kills (Android)**: local Expo module `apps/mobile/modules/exit-info` (Kotlin, `ApplicationExitInfo`, API 30+, own process only, reason /
  importance / status / rss / description; NO trace stream read) -> `@app/ui` `reportPastExits` at start: bad exits only (CRASH, CRASH_NATIVE, ANR,
  LOW_MEMORY, EXCESSIVE_RESOURCE_USAGE, INITIALIZATION_FAILURE, SIGNALED, DEPENDENCY_DIED), newest 3, kind `native`, name `NativeExit`, no stack,
  route `other`, same switch / same scrub / same worker; a plain CRASH is skipped when the kept JS fatal was just sent (same event); the newest
  timestamp is kept (`crash-exit-seen`) only when sending was possible. Worker: `native` kind accepted, digest text says "the system ended the app".
  Privacy page lists it (and says iPhones send none yet).
- **Test bed** `crash` scenario (`am crash` -> restart -> "[crash-reports] sent N exit record(s)" -> a normal restart sends none). On the emulator
  2026-10-29 the module read the system record (1 new, CRASH, 1 bad) and the app tried to send it; the deployed worker answered 400 "bad kind" (it predates
  `native`), so step 3 is red until the redeploy. Steps 1, 2 and 4 pass.
- **iOS**: MetricKit (MXMetricManager diagnostic payloads) needs a Swift subscriber module and only delivers on the next day's launch; not cheap, not
  done. Web and iOS still report JS errors only.

## Not done / owner
- Re-run the `crash` scenario (worker already redeployed); read `GET /crashes` (run token) once.
- Privacy wording is Claude's draft (owner to approve, like the report wording).
- Review-app tab for crashes (the digest + `GET /crashes` are the v1 reading).


## Merged in (ticket audit 2026-11-05, owner)
This ticket now also carries the leftovers of: T291, T407 — read each file's open items and finish them here; each closes pointing here.

## Crash test bed (2026-11-06, 62046a681): every kind reaches the report path, nothing written to staging
- **How:** `session.sh --crash-stub` builds the test-bed APK with `EXPO_PUBLIC_CRASH_URL=http://localhost:39256/crash` (cleartext
  patched after prebuild, never a store build) and `adb reverse`s it to this machine, where `app/tools/testbed/crash-stub/server.mjs`
  runs the feeds worker's OWN `handleCrash` (workers/feeds/src/crashes.ts via Node's type stripping) over an in-memory bucket.
  A tool's build (`TEST_REPORTS`) shows "Crash test" on the Updates log: Throw a JS error (a setTimeout throw: the global handler,
  fatal) and Reject a promise (Hermes' tracker). The native crash is `kill -11` on the app's pid (`adb root`), the shape of T351's
  reports. Scenario: `run.mjs --no-r2 crash-bed` (README; `--no-r2` = no staging test-channel reset).
- **Results** (n1_pixel; `out/runs/base/crash-bed`, then the stressed emulator `out/runs/stress/crash-bed`):

  | kind | phone | worker handler | stored |
  |---|---|---|---|
  | unhandled promise | sent at once, app keeps running | 201 `promise`, fatal false, stack scrubbed to `at promise (address at index.android.bundle:1:…)`, route menu | `private/crashes-test/<hash>` |
  | uncaught JS error | app ends; sent before it died | 201 `js`, fatal true | crashes-test/ |
  | native SIGSEGV | next start: `CRASH_NATIVE while foreground, status 11` | 201 `native`, `NativeExit` | crashes-test/ |

- **Bug found and fixed:** a JS fatal sent BEFORE the app died (the usual case online) cleared the kept slot, so the next start did
  not know the system's plain CRASH record was the same death and sent it too: one crash told twice, the second as a `native` kind
  (first run: "exit records sent with it: 1"). The slot now says "sent" (`FATAL_SENT`) until the next start reads it; same run after
  the fix: "exit records sent with it: 0". Also: an exit record whose send FAILED (server unreachable) was marked seen and lost (found
  when `adb root` dropped the reverse forward: "sent 0 exit record(s); 1 new (CRASH_NATIVE), 1 bad"); the mark now stops just before
  the oldest failed one, so it goes at the next start. Both pinned in `crashReport.test.ts`.
- The `test: true` tag holds end to end (T291's leftover, for crashes): every kind landed under `crashes-test/`.
- Left (owner): `GET /crashes` read once on staging; the privacy wording; T407's phone check after the 0.6.0 worker deploy.
