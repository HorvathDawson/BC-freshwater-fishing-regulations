# T495 — The near goal: Play closed testing on production, good enough for first users

Status: OPEN — owner 2026-11-07 (chat 7). This is the release goal; every chat works this list first. 2026-11-19: production serves the new site, data r-6258ffded676 and the feeds + data workers, and the gated build 39 (0.8.3, production) is on Play Closed waiting for Google's review (STATUS 2026-11-19 evening). Left: gate 2 T360 (PARTIAL), gate 3 T401 (PARTIAL, owner: not urgent), step 3 T463 (owner runs /code-review ultra), store T319 / T390 / T514, and the closed-testing clock itself (T371).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T495 | THE NEAR GOAL: Play closed testing on production, four gates (no wrong regs, no crashes, smooth map, clean first run) | OPEN | — | production live 11-19; build 39 on Closed awaiting Google; left: T360, T401, T463, T319/T390/T514, T371 clock |

## The goal
Get the app out to the first users (Play closed testing: 12 testers × 14 days) without a bad first experience,
**on production**: canifishthis.ca, the prod Cloudflare workers and R2. The website has to move from v1 to the new
version BEFORE closed testing starts (v1 replaced at launch; old URLs 301 to the new ones). iOS TestFlight work
resumes as soon as closed testing starts (Play first, iOS right after).

**The owner's part:** the salmon (DFO) extent review (T164) and every prod step (prod R2, workers, DNS, Play Console).
**Claude's part:** everything else — audits, cleanup, the prod cut-over prepared to one command per step, and these gates.

## Gates (owner 2026-11-07: all four)
1. **No wrong regs shown.** The presentation audit (T462, multi-agent, the owner opts in with "use a workflow") passes:
   every screen matches the release and the synopsis, outside gaps the app already flags. Its findings are fixed or
   ticketed with the owner's OK.
2. **No crashes or ANRs.** T351 (foreground crash) and T360 (memory, Conditions ~410 MB) closed: a long emulator session
   (fresh install, every tab, 30 min of panning) with no crash, ANR or memory growth, then the owner's phone.
3. **The map feels smooth.** T420 patched MapLibre confirmed on the owner's Pixel 9 (the A/B is the owner's); no
   visible gaps or flicker: T401, T451, T493, T494 (+ T488/T489 looks).
4. **A clean first run.** Fresh install → walkthrough → map → tap → regs, also on a bad connection, with no stale or
   dead control: T478, T423, T466, T456, T428 (and the IN REVIEW looks the owner has not confirmed).

## Order (what is left; the tickets closed since are listed once below)
1. IN REVIEW looks: all closed.
2. Gates 2–4 (Claude): T360 (memory, PARTIAL), T401 (PARTIAL; owner: not urgent), T461 (PARTIAL).
3. Audit + review: T463 (owner runs `/code-review ultra`, Claude triages), T496 cleanup sweep (PARTIAL).
4. Production cut-over (Claude preps, owner runs): T404 (one switch to prod, every origin from one setting, prod
   runbook), T240 (launch checklist: map assets, HYDAT, report purge, SEO build, privacy page), T409 (.com → .ca 301,
   HSTS, Search Console). Dry-run every prod script against staging first; nothing prod runs without `CONFIRM_PROD` (human).
5. **Store:** T319 (store screenshots; owner 2026-11-11), T371 (promote the gated build to closed testing), T390 (in-app
   update prompt, so testers get fixes), T514 (share page "Open in the app" → the store listing).
Closed since: T443 T449 T451 T454 T456 T459 T475 T479 T480 T482 T483 T484 T423 T424 T428 T315 T354 (looks); T478,
T351, T494, T493, T489, T488, T466, T448 (gates); T462 (gate 1); T244, T88; T164 (salmon); T198, T86, T453 DROPPED.

Not for this goal (stay POSTPONED): T200 T201 T218 T219 T223 T234 T281 T389 T447 T487 T492, and T261/T310 beyond
what ships.

Also in this goal (owner 2026-11-11): build and tooling hardening T533–T542, T600–T602, T638.

## Done when
The four gates pass, production serves the new site, app and feeds, and the gated build is on Play closed testing.
