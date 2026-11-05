# After 0.6.0: start here

Written 2026-11-05, as 0.6.0 was being built for Play internal. It replaces `NEXT-SESSION-after-0.5.0.md`. Two chats,
in order: **Data and build** first, then **Launch and hygiene**. Each is one chat for the whole tree.

## Paste this to start the chat

> Read `handoff/PLAN/NEXT-SESSION-after-0.6.0.md`, then the STATUS.md top entry. 0.6.0 is on Play internal. Start
> with T408: walk me through the data flow one question at a time. Then do the Data and build list in order. Start
> ticket work without asking; ask me only for decisions. Keep replies short and STATUS.md current.

(For the second chat, swap the T408 lines for: "Do the Launch and hygiene list in order.")

## Where 0.6.0 left things
- **App:** 0.6.0 (`app.json`, `package.json`, changelog dated 2026-11-05). The AAB went to Play internal via
  `deploy/staging/build-apk.sh --play`, which bumped and committed `versionCode`. The STATUS top entry has the code.
- **Rollout (owner 2026-11-05):** 2–3 days of internal testing. If clean, promote the SAME build to Closed testing
  (T371, the owner's Console steps). If not, ship 0.6.1 internally first. Closed testers read staging until the prod
  switch (T404). The prod switch comes after the owner's own DFO label review curation.
- **Staging:** feeds worker deployed with 0.6.0 (version b960604d; T407 report reasons, T178 monthly skill score, version
  notices). The site has the T409 web-only wins (staging 0457d354). No new data release was needed: the salmon
  compaction is app-side only (same files).
- **Prod:** untouched, and human-only. The owner said "we don't want to deploy to prod for a while".

## First (Claude's own, before ticket work)
1. **Read the reports.** `GET /reports?since=…` and `GET /crashes?since=…&background=1` on
   https://bc-fishing-feeds-staging.horvath-dawson.workers.dev (Bearer token in `app/workers/feeds/.run-token`). Turn each real
   problem from 0.6.0 testing into a ticket. "App bug" and "Feature request" reports are the owner's own test notes.
2. **`graphify update .`** (not run after the 0.6.0 changes).

## Chat 1 — Data and build (in order)
1. **T408** — walk the owner through the data flow, ONE question at a time, write down the answers, then simplify. The
   owner wants to learn the flow as well, so explain, don't just ask. Its answers shape T324–T326, T266, T295, T297
   and T337, so those come after it.
2. **T164** — DFO salmon dossier, splits, review.
3. **T178** — the CLEVER vs COFFEE weights, once the monthly scores exist (the worker now scores, deployed 11-05).
   Propose the weights; changing them is a reviewed change.
4. **T239** — annual synopsis update runbook + dry run.
5. **T266, T295, T297** — marks/exporter, faster rebuild, build clean-up (all IN REVIEW; finish together).
6. **T324, T325, T326** — one clean data flow, write-through, tidal lock.
7. **T337** — DFO water bindings refresh inside the build.

## Chat 2 — Launch and hygiene (in order)
1. **T86, T240, T244** — production hosting, launch checklist, store readiness.
2. **T409 rest** (owner 11-05: do it) — web-only: build the map only when it is first seen on `/water/*` (local
   93·93·93; a phone's "back to map" waits ~1–2 s once). Measure as T409 did (local, 3 runs, median); no native diff.
   Before the prod SEO launch.
3. **T371** — Closed testing (the owner's Console steps; Claude keeps drafts current).
4. **T305, T306, T331, T341, T402** — pipeline and test hygiene. **The known failing tests (all from before 0.6.0):**
   - slow pytest: `test_one_water_kind` ×3, `reach_tributaries`, `unnamed_trib_sweep`, `watershed_parts` (T306);
   - release-perf A5 search p95 timing (T341);
   - `Walkthrough.test.tsx` flakes under the parallel suite and passes alone (T402).
5. **T333, T334, T335** — the audits and the archive removal (the owner scopes the audits first).
6. **T404** — one switch to production + the prod runbook (it includes the www redirect fix).
7. **T410** — one clear web/native split in the app code (38 platform files, 43 inline `Platform.OS` checks in 30
   files, 10 files checking `typeof document/window`).

## Deferred (don't start unless the owner asks)
T383 (per-water time zones), T389 (unnamed-reach links; needs a tiles rebuild), T304, T401 (pan buffer), and the
T307/T308/T310 sign-offs. T351 waits for a 0.5.0+ native crash report.

## Waiting on the owner (also in OWNER-TODO.md)
- **iOS:** an Apple Developer account, App Store Connect, the app name ("Can I Fish?" or "Can I Fish This?"), then
  `deploy/staging/build-ios-testflight.sh` and the real-iPhone run (T364, after the hygiene chat). T363/T365/T88 are
  PARTIAL.
- **Prod launch steps:** SEO (prod site, robots, Search Console + Bing, outreach) and the www redirect, both AT PROD
  LAUNCH ONLY (T404).
- **Closed testing:** promote 0.6.0 after 2–3 clean days (T371).

## Decided on 2026-11-05 (don't re-ask)
- The in-memory salmon run table at 6.3 MB is fine (T360). No lazy decode.
- No lighter z8 map (T245). The zoomed-out pan stays as is.
- Region pages keep the "When waters are open" bar and the map button as they are (T406).
- Keep Cloudflare's JS detections and Web Analytics beacon (0 ms blocking).
- Fish pictures: 240 px thumbnails in rows, the full picture only in the ID pane.
- Report a problem has "App bug" and "Feature request" (T407).
- The changelog drafts are approved. Keep 3–5 plain bullets per version.

## Rules that still hold (AGENTS.md has all of them)
- **Human-only:** the LLM parser, prod R2 and the prod deploy. Hand the owner the command instead.
- **Claude may do:** a staging upload, a staging site deploy and a staging feeds worker deploy. The worker ships with
  the app, never ahead of it.
- **The APK:** only when the owner says "build".
- **Heavy jobs:** every one goes through `scripts/heavy.sh`, one at a time.
- **Git:**
  - Commit with `git commit -- <paths>`.
  - Never `git stash`, never `git add -A`, never edit `marks.jsonl`, never pattern-kill processes.
  - Never touch `main`.
- **Data and code:**
  - No silent fallbacks (`reportFailure`).
  - One load path per screen.
  - Ask before filtering data content.
  - Self-host everything.
- **Lanes:** at most 2 agent lanes at once.
