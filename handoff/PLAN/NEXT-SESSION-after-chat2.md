# After chat 2 (launch and hygiene): start here

Written 2026-11-05 (late), as chat 2 closed. It replaces `NEXT-SESSION-after-0.6.0.md` for what is left.
**DRAFT — finished when the audit (T333–T335) and the combined heavy run land; until then the sections marked
PENDING are not final.**

## Paste this to start the chat

> Read `handoff/PLAN/NEXT-SESSION-after-chat2.md` and the STATUS.md top entry. Do the list in order. Start ticket work
> without asking; ask me only for decisions. Keep replies short and STATUS.md current.

## Where chat 2 left things
- **App:** 0.6.2 (versionCode 11) on Play internal since 11-05. Closed testing (T371) after 2–3 clean days: the owner's
  Console steps, same build.
- **CI green on both** (app + pipeline) for the first time since 10-20: Node pinned (`app/.nvmrc`), PNGs via fflate,
  tests in BC time, data tests marked, full history on checkout.
- **Done in chat 2:** T86 contact guard · T240 secrets list · T244/T371 drafts current · T252 (upper Pitt) · T304 (code)
  · T319 generator · T331 · T366 · T409 lazy map (all widths) · T410 (partial) · T414 · T415 · T421 · T422 · T425 ·
  T426 · T427 · Heber carry (reflowed). T416: causes found and fixed (build path in about.bundle, synopsis cache key,
  DFO typed inputs) — final proof in the heavy run.
- **Not deployed:** the feeds worker (T366 row digest) ships with the next app; the staging site needs a FULL SEO build
  before its next deploy (T304: site.sh refuses pages without `cift:region`).

## The heavy run (one agent, one job at a time) — DONE 11-06 (HEAD 4fa63f124)
1. **T416 DONE:** a fresh clone and a Mac `--side` build both give **r-0fd5c0e4e8b8**, all 52 files plus the tiles,
   synopsis and basemap identical (`~/t416-final.json`). Two more causes were fixed: the basemap is now pinned in the
   build-assets bucket and a missing one is refused, and `row_boxes.json`/`synopsis_raw_data.json` are now pins (a
   missing one is refused). Commits b53af411a, 1cd8e8812.
2. **The Mac's deliver** (`--hold-latest`): r-0fd5c0e4e8b8 waits in `data/release/CANDIDATE`. LATEST is still
   r-5a5999c94588. **Owner: promote it with `pipeline release`.**
3. **Suites:**
   - App: green (8,701 tests).
   - Pipeline fast tier: 11 failures from T436 (971f40575), fixed in 84e71af78; only the 5 affected files were rerun.
   - Slow tier: only the known T306 failures.
4. **T409:** Lighthouse phone 91–93, desktop 99–100, but phone LCP is 2.25–2.48 s (gate 1.8 s). Next item: each font
   is downloaded twice (~315 kB). **T410:** the bundle listings are clean; it stays PARTIAL for the phone pass.
   **T304:** all 19,635 pages tagged; Fast-3G 47.6 → 24.9 s locally; staging needs a full `build-seo` deploy.
5. **T305 DONE:** two builds, same `tile_version` and pmtiles sha.
6. `graphify update .` done.

## The audit (T333–T335) — DONE
Report: `handoff/PLAN/audit-2026-11-05/REPORT.md` (78 findings → ~57 confirmed after a second reader). Fix tickets
T429–T438, most harmful first: T429 curated file emptied · T430 report-format parity · T431 worker tests run nowhere ·
T432 golden parity passes on zero cases · T433 staging scripts fail-safe · T434 review-app silent fallbacks · T435
pipeline paths/determinism · T437 docs + tickets drift · T436 dead code · T438 stale worktree. T335 (delete archive/)
is verified safe once 4 cited docs move.

## Still running FROM CHAT 2 when chat 3 starts (do not disturb)
- **The combined heavy run** (chat 2's agent, steps above) holds `scripts/heavy.sh`'s lock until about 02:00–03:00
  11-06. Chat 3: start NO heavy job until it ends (heavy.sh would queue it anyway); don't kill its PIDs; don't edit
  the files it writes: T416, T305, T409, T410, T304 tickets, `pipeline/deliver/release/manifest.py`/basemap code,
  `handoff/PLAN/runbooks/fresh-machine.md`, STATUS.md's top entry. It writes its results into those tickets + STATUS.md
  and pushes; read them there. Chat 2's window stays open until it reports.
- **Safe to start at once:** T429–T438 (they touch none of the above), T335 prep (moving the 4 docs).
- **Ticket numbers:** another chat also files tickets (T423, T424, T428 were theirs). Take the highest number + 1 from
  `ls handoff/PLAN/app-integration/tickets` right before writing one; `node app/tools/check-tickets.mjs` must pass.
- **Never commit:** `data/generated/review/pfma_basin_areas.json`, `data/generated/review/write-through.jsonl`
  (untracked, generated); the store images (gitignored).
- **T434 note:** T421's refusal (applied_keys names a missing build) is swallowed again in `curation-review/backend/
  reuse.py:1139-1149` — fix both together.

## Next, in order (to finalise)
1. The audit's fix tickets T429–T438, in the order the owner picks (default: the table order).
2. T335: move what live code reads from archive/, delete archive/.
3. Whatever the heavy run left (see its results).
4. With the next APK ("build" from the owner): T250 Expo/dependency upgrade first, then the device checks — T420/T401
   pan, T242 crash test bed, T261, T293, T315, T354, T252 lower Pitt, T319 saved-spot shot, T410 phone pass; deploy
   the feeds worker with it.
5. T404 (prod switch) when the owner says prod is near.

## 0.6.3 (chat 3 owns it; owner 2026-11-06: "who will release 0.6.3?")
After chat 2's heavy run lands (its full check must be green) and only when the owner says **"build"**:
1. Carries: T409 lazy map · T304 region focus · T410 split · T425 rail switch · T427 leaner forecast · T421 · and from
   the audit T430 (report-format parity) + T431 (worker tests/salt) if done — they touch the worker, which ships with
   the app. Changelog: 3–5 plain bullets.
2. Staging data release from the new deliver (`pipeline release --staging`; the release refuses a missing basemap now).
3. Full SEO build against the new web export, then the staging site deploy (`deploy/staging/site.sh` refuses pages
   without `cift:region`); then T304's Fast-3G measurement on staging.
4. `deploy/staging/build-apk.sh --play` (bumps versionCode) → Play internal; `play.json` names 0.6.3.
5. The staging feeds worker deployed WITH it (`deploy/staging/feeds.sh`; T431: it runs the worker tests first and
   needs `REPORT_SALT` set — check `wrangler secret list` before).
6. Phone checks for the owner: T425 rail, T427 forecast, T409 back-to-map wait, T410 moved native halves.

## Waiting on the owner
- Look at: T425 shot (`.playwright-mcp/t425-phone-light.png`), T427 shots (`.playwright-mcp/T427-after-*-390.png`),
  the store shots + titles (T319, `app/tools/store-shots.json`; images in `store/graphics/store/light/`).
- T371 Closed testing (Console).
- OWNER-TODO has the rest.

## Decided in chat 2 (don't re-ask)
- Switches: ink fill everywhere (T260). Trout/Salmon on the map rail, fish art + word (T425).
- Map waits until first shown at every width on water pages (T409).
- CF token kept until prod; Apple install id declared as Device ID; carry ignores whitespace-only changes.
- Audits: all areas, report + fix tickets, medium depth; delete archive/.
