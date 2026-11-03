# T352 — Crash and problem reports lack the facts that would find the cause

Status: OPEN (from T346, 2026-11-02).

What the 7 crash kinds and 1 problem report held vs what was needed:
1. No water/item id, section, or open pane on a crash (only `route` = water/map/other). The Coquitlam crash shows as
   route "water" with a minified stack; add the open item id and the screen/pane state.
2. "Requiring unknown module '…'": the module id is cut to an ellipsis, and the minified `index-<hash>.js` stack cannot be
   mapped back. Keep the module id; send a release id that maps to uploaded source maps (or keep maps with the release).
3. Two hashes (b0eaeeb61c3d3a6c, f1a058176d7c1a37) are the same error, split because the stack format differs by browser.
   Normalise the stack before hashing (or hash name + message + route only).
4. No install/session id, so "owner's vs other testers'" cannot be told and distinct-user counts are impossible. Add a
   random per-install id (no personal data), or an `owner` flag set by the staging build.
5. Native exits carry no stack or tombstone; background OS kills (SIGNALED 9, LOW_MEMORY) are sent like crashes. Mark
   them as `background` kills and keep them out of the crash counts.
6. Problem report context holds only screen ("more"), mode, version, tiles; add the active layer, forecast offset,
   zoom/centre and the open water id (the colour report below needed them).
7. Dev-build crashes arrive too (env "local", `useRegsPrefetch is not defined`, 1 report); the store should drop env
   "local" (it is not a tester).
Size: S–M.

## Owner decisions (2026-11-03)
1. **Crashes carry the open water's id** (and the pane state: sheet open, Show all parts, section index). The Data hint
   ("…the kind of screen… Nothing else") and the privacy page are reworded to say so.
4. **A random per-install id** (made on the device, no personal data) on crash and problem reports; privacy page says so.
6. **Problem reports add** the active layer, forecast offset, map zoom/centre and the open water id, and the report page
   LISTS what will be sent before Send.
2/3/5/7 need no decision: keep a short numeric module id in the message; hash on a browser-neutral stack; background
OS kills sent non-fatal (`background`) and out of the crash counts; the store drops env "local".
Order: the worker (accepts old AND new payloads) deploys before the 0.5.0 APK reaches testers (batch 7).

## Implemented (2026-11-03, uncommitted; worker NOT deployed)
Payloads: **crash/2** (app) and **report/2** (app); the feeds worker takes crash/1 + crash/2 and report/1 + report/2, each
version with exactly its own keys (a crash/1 carrying a crash/2 field is refused, as is any unknown key).
1. crash/2 adds `water` (item id), `view`, `part`, `section` (tapped handle), `whole`, `sheet` (search / spot / adding) —
   all from the route the Shell last told (`crashWhere(place.route)` in the one `context()` function, App.tsx). Not sent:
   the water map's "Show all N parts" list and the Shell's own sheets (Layers, day): they are component state, not route
   data (lifting them = regs/pane files). Native exits name no water (the screen of the dead run is unknown).
2. `scrubMessage` keeps a quoted 1–6 digit number after "module" (`Requiring unknown module "1234"`). Release id: `release`
   and `app` already sent. **Source maps are NOT kept with a build**: `expo export --platform web` runs without
   `--source-maps` (no `.map` under app/dist), and the Android Hermes map is a gradle intermediate
   (build/generated/sourcemaps), not archived with the AAB. Keeping them (export with `--source-maps`, archive the
   Hermes map per release, never served) is a separate step.
3. `crashHash` = name + message (digits out) + the first 5 NAMED frames (`frameName`: Chrome/V8/Hermes "at fn (…)",
   Safari/Firefox "fn@…", qualifiers and anonymous frames dropped). Applied to crash/1 too: one kind per error going
   forward; kinds stored under the old hash age out (90 days), each live one is told once more as new.
4. `installId` (16 random hex, made once, `openInstallIdStore`: web localStorage key / native file) on crash/2 and on
   report/2 (`context.install`, its own "Install id" row). The worker keeps up to 100 distinct ids per kind (`installs`).
5. Android exit LOW_MEMORY / SIGNALED with importance not FOREGROUND/VISIBLE → sent `fatal: false, background: true`. The
   worker marks the kind `background` (crash/1 read from its "… while background" line), leaves it out of the digest and
   of GET /crashes unless `?background=1`.
6. report/2 adds "Map view" (layer · ground, forecast offset, zoom to 0.1, centre to 3 decimals) and "Install id" rows;
   the section handle is now a listed line of "Where you were"; the route's water/part/section fill in under a page's own
   where. Every field sent is a listed line above Send (pinned in ReportSend.test.tsx).
7. env "local" → 202 `{ok:true, stored:false}`, nothing stored (staging and production; a `test` tag changes nothing).
Words: CRASH_WORDS.hint and the privacy page (tools/seo/html.ts, effective 2026-11-03) say what goes, incl. the install id.
- Main session 2026-11-03: crash/2 carries NO section handle (AGENTS rule 5: section ids never leave the bundle in an
  API contract; the owner asked for the water id + pane). The problem report's listed "Section" line (sent by the reader,
  shown before Send) is unchanged. Worker deploy before the 0.5.0 APK reaches testers (batch 7).
