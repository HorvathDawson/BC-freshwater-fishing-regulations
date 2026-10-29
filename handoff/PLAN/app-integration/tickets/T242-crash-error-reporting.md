# T242 — crash and error reporting (privacy-light)

Status: PARTIAL — built and tested (960d2d8, 31c8d04); web forced error lands on staging (201); Android native-exit path proven up to the POST; the staging worker needs ONE MORE deploy (new `native` kind + 1 s gap) before the Android exit record is accepted: `npx --prefix app wrangler deploy -c app/workers/feeds/wrangler.toml`, then `scripts/heavy.sh testbed 30 -- bash app/tools/testbed/session.sh --max-min 30 -- node app/tools/testbed/run.mjs crash` (no rebuild).

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
- Redeploy the feeds worker (command in Status), then re-run the `crash` scenario; read `GET /crashes` (run token) once.
- Privacy wording is Claude's draft (owner to approve, like the report wording).
- Review-app tab for crashes (the digest + `GET /crashes` are the v1 reading).
