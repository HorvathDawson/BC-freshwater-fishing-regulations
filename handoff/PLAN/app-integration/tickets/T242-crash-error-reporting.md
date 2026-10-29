# T242 — crash and error reporting (privacy-light)

Status: PARTIAL — built and tested (960d2d8); the staging worker is NOT deployed yet (the permission classifier refused the deploy: owner runs `npx --prefix app wrangler deploy -c app/workers/feeds/wrangler.toml`), so the end-to-end "forced error lands once" on web + Android is not run.

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

## Not done / owner
- Deploy the feeds worker (command above), then force an error on a staging web build and the Android test bed and read `GET /crashes`.
- True native process crashes (an OutOfMemoryError that kills the JVM, a native signal) never reach the JS handler: only JS errors and
  the global handler's fatal JS errors are reported. A native crash reporter (a stored "last run ended badly" marker) is a follow-up.
- Privacy wording is Claude's draft (owner to approve, like the report wording).
- Review-app tab for crashes (the digest + `GET /crashes` are the v1 reading).
