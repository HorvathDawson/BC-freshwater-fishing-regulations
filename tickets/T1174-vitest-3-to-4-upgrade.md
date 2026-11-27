# T1174 — Upgrade vitest 3 to 4 (Dependabot PR #305 closed)

Status: OPEN — the hardening release (tooling); its own lane, per the major-version rule of T1141

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1174 | Upgrade vitest 3 to 4 (Dependabot PR #305 closed) | OPEN — hardening (tooling) | — | a lane: why `tools/seo/gaugeChart.bundle.test.ts` fails with ERR_MODULE_NOT_FOUND under vitest 4; fix; the whole app tier green |

Source: Dependabot PR #305 (vitest 3 to 4), closed by the planning chat on 2026-11-26. On it, one test failed:
`app/tools/seo/gaugeChart.bundle.test.ts`, "builds when the command starts at the repo root", with
ERR_MODULE_NOT_FOUND. A major bump is never merged as it comes (T1141); this is its ticket.

## Work
1. Reproduce on a branch with vitest 4: find which module is not found and why (the test starts the command from the
   repo root, so a path or resolve rule likely changed).
2. Fix the cause, not the test; read vitest 4's migration notes for other changes we rely on (config, pools,
   timeouts, `onTaskUpdate`).
3. Run the whole app tier once; record its time before and after.

## Acceptance
vitest 4 on main; the gauge chart bundle test passes from the repo root; the app tier is green and its time is
recorded here. Severity: low. Model: Sonnet. Size: S–M. Owner: no.
