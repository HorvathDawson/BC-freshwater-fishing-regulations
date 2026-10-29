# T249 — feeds / worker security

Status: OPEN — not started.

Source: owner 2026-10-28 — approved proposal P15 in handoff/PLAN/proposed-tickets-2026-10-28.md ("approved ALL 16").

## Work
Rate limits on every public worker route (feeds worker `/run/*`, `/notify/*`, data worker, future report/crash
endpoints): Cloudflare rate-limiting rules or a Durable-Object/KV counter; run-token rotation procedure (documented,
tested: old token refused); a check (CI or the feeds worker's health job) that no `private/` object is ever served by
the data worker; secrets inventory (names, where set, who rotates).
## Acceptance
A request flood gets 429; GET data-staging/private/... is 404/403 (test); rotation runbook executed once on staging.
Depends: T225. Size: S. Owner: yes — Cloudflare rules on the zone (default: Claude writes them, owner applies on prod).
