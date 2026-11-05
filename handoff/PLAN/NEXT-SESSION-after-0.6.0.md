# After 0.6.0: start here

Written 2026-11-05, as 0.6.0 was being built for Play internal. It replaces `NEXT-SESSION-after-0.5.0.md`. Two chats,
in order: **Data and build** first, then **Launch and hygiene**. Each is one chat for the whole tree.

## Paste this to start the chat

> Read `handoff/PLAN/NEXT-SESSION-after-0.6.0.md`, then the STATUS.md top entry. 0.6.0 is on Play internal. Finish
> the Chat 1 list (T416, T412, T413), then do the Launch and hygiene list in order. Start ticket work without asking;
> ask me only for decisions. Keep replies short and STATUS.md current.

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
2. **T411** — the part highlight still zigzags after stepping (Coquitlam: Downstream to the bottom, then Upstream
   once). Seen on 0.6.0, so T399's fix does not cover it. Fix it in 0.6.1 before Closed testing.
3. **`graphify update .`** (not run after the 0.6.0 changes).

## Chat 1 — Data and build (in order)
Done 11-05: T408 walkthrough (`data-flow-decisions.md`), T239, T266/T325, T295/T297 (`pipeline release`), T324
(renames; restart the review app after pulling), T337, ticket audit (`ticket-audit-2026-11-05.md`), T411 in code
(0.6.1 batch), graphify updated. Owner-gated: T164 (179 DFO places), T326 (3 DFO items). Waits: T178 (2026-12-01).
Left, in order (a short "Chat 1b" before chat 2):
1. **T416** — a fresh clone can build. State in the ticket: the fresh clone is at `~/t416-fresh` with every source
   fetched (log `~/t416-fresh.log`; FWA moved to the gov object store, fixed). Left: fetch_data exits non-zero on
   errors; a source for the 5 inputs with no fetch path; pin the drifting live layers (mirror to R2 with sha256) or
   explain them; one bootstrap command; the timed clone→`pipeline release --staging --dry-run`→build proof;
   `handoff/PLAN/runbooks/fresh-machine.md`.
2. **T412** — one slow-feed pattern (carries T199, T225); never a GitHub schedule.
3. **T413** — the worker starts the HYDAT workflow (owner's optional token); until then its email.

## Chat 2 — Launch and hygiene (in order; ticket audit 2026-11-05 in `ticket-audit-2026-11-05.md`)
1. **0.6.1 is on Play internal** (11-05, T411; worker 50a816d9). Next APK: T417 (watershed flicker), T418 (style
   assets), **T390** (read play.json on every open/return) — build when the owner says "build". T419 (Salmon River
   simplification) is checked with this UI work; a fix there is a tiles change (rides a data release).
2. **T86, T240, T244** — production hosting, launch checklist (now carries T50, T75, T80, T87), store readiness.
3. **T409** — SEO launch pass (carries T188, T196, T405; T198 owner-blocked): incl. the map built when first seen on
   `/water/*` (owner 11-05). Measure as T409 did (local, 3 runs, median); no native diff. Before the prod SEO launch.
4. **T371** — Closed testing (the owner's Console steps; Claude keeps drafts current).
5. **Hygiene pass:** T410 (web/native split; carries T258, T260) · T242 (reports pipeline; carries T291, T407) · T250
   (Expo / dependency upgrade) · T261 (run timing v2 leftovers) · T305, T306, T331, T341, T402 (pipeline + test
   hygiene) · close the in-review queue (T252, T292, T293, T315, T354, T366, T55). Known failing tests (pre-0.6.0):
   slow pytest `test_one_water_kind` ×3, `reach_tributaries`, `unnamed_trib_sweep`, `watershed_parts` (T306);
   release-perf A5 search p95 (T341); `Walkthrough.test.tsx` under the parallel suite (T402).
6. **Back from deferred:** T304 (water-page region focus), T401 (pan buffer), T307/T308/T310 sign-offs, T351 (native
   crash, when a 0.5.0+ report exists), T319 (store screenshots).
7. **T333, T334, T335** — the audits and the archive removal (the owner scopes the audits first; T334 includes the docs).
8. **T404** — one switch to production + the prod runbook (it includes the www redirect fix).

## Soon (after chat 2)
T360 performance pass (carries T245, T287, T317) · T88 iOS (carries T363–T365; when the Apple account exists) ·
T239 next-book leftovers (T414, T415; before the next synopsis).

## Deferred (don't start unless the owner asks)
T383 (per-water time zones), T389 (unnamed-reach links; needs a tiles rebuild), T79, T281, and (owner 11-05) T218,
T201, T219 (bathymetry), T200 (stocked lakes), T234 (reader-set alerts), T223 (accounts). T259 dropped.

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
