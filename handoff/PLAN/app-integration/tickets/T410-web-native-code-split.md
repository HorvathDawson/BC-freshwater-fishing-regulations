# T410 — One clear web / native split in the app code

Status: OPEN — with the launch-and-hygiene chat (owner 2026-11-05: "make it so the web and app code are split out a
little better … not sure the standard here but seems like something to clean up").

## Today (2026-11-05)
- 38 platform files (`*.web.ts(x)`, `*.native.ts(x)`, `*.ios` / `*.android`) — the Expo/Metro standard: the bundler
  picks the file for the platform, so the other platform's code is never even in its bundle.
- Beside them, 43 inline `Platform.OS` checks in 30 files and `typeof document / window` checks in 10 — web and phone
  code mixed in one file, both shipped to both.
- Packages: core (pure logic, no platform), data, map, ui (shared hooks), ui-native (screens, web + phone via
  react-native-web), apps/mobile. The SEO site tools live in app/tools/seo.

## Work
1. Write the rule down (AGENTS.md / app README): logic that differs by platform lives in a platform file
   (`foo.web.ts` + `foo.native.ts` behind one `foo.ts` type), never an inline branch — except a one-value difference
   (a size, a flag), which may use `Platform.select`. `core` stays platform-free.
2. Move each inline branch that is more than a value into platform files (list them first; one commit per area).
3. A guard test: no `Platform.OS` / `typeof document` outside an allow-list, so it cannot drift back.
4. Check both bundles: the web bundle carries no native-only module, the APK no web-only one (bundle listing diff).

## Acceptance
The rule is written; the guard passes; Android, iOS and web tests green; web and APK bundle contents listed before/after.

Links: T409 (web speed work stays in web files), T333 (hygiene sweep). Size: M. Owner: no.


## Merged in (ticket audit 2026-11-05, owner)
This ticket now also carries the leftovers of: T258, T260 — read each file's open items and finish them here; each closes pointing here.
