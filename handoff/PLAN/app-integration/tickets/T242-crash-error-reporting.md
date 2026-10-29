# T242 — crash and error reporting (privacy-light)

Status: OPEN — not started.

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
