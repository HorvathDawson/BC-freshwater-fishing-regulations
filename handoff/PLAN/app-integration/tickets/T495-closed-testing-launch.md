# T495 — The near goal: Play closed testing on production, good enough for first users

Status: OPEN — owner 2026-11-07 (chat 7). This is the release goal; every chat works this list first.

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

## Order
1. **Close what is IN REVIEW** (owner looks, batched into one phone check): T443 T449 T451 T454 T456 T459 T475 T479 T480
   T482 T483 T484 T423 T424 T428 T315 T354. What fails goes back on this list.
2. **Gates 2–4 work** (Claude, ticket by ticket): T478, T351/T360, T494, T493 + T489 (one tiles rebuild), T488, T466,
   T401/T448, T461.
3. **Audit + review:** T462 (gate 1), T463 (owner runs `/code-review ultra`, Claude triages), T496 cleanup sweep.
4. **Production cut-over, prepared** (Claude preps, owner runs): T404 (one switch to prod, every origin from one
   setting, prod runbook), T240 (launch checklist: map assets, HYDAT, report purge, SEO build, privacy page), T198/T409
   (.com → .ca 301, HSTS, Search Console), T86 (prod hosting), the prod feeds worker (placeholder exists). Dry-run every
   prod script against staging first; nothing prod runs without `CONFIRM_PROD` (human).
5. **Store:** T244 (listing, privacy labels), T371 (promote the gated build to closed testing), T390 (in-app update
   prompt, so testers get fixes), T514 (share page "Open in the app" → the store listing). Then iOS: T88 (TestFlight).
6. **Salmon:** T164 lands whenever the owner's review is done; a release after it (T461, T453 ride along).

Not for this goal (stay POSTPONED): T200 T201 T218 T219 T223 T234 T281 T319 T389 T447 T487 T492, and T261/T310 beyond
what ships.

## Done when
The four gates pass, production serves the new site, app and feeds, and the gated build is on Play closed testing.
