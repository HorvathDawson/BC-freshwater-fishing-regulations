# T413 — A new HYDAT starts the HYDAT workflow: from the worker, or the owner after the email

Status: DONE in code 2026-11-05 — a355ed884, d0e0e45f9 (goes live with the next worker deploy + the owner's token) — was OPEN (owner 2026-11-05). Owner: "I don't want GitHub to be the scheduler — it is flaky and unpaid, it will
disappear after a while. Either trigger it when needed from R2 [the worker] or assume a human sees the email then
triggers it manually." **No GitHub schedule, for this or any feed.**

- **Today (works, no change):** the worker detects a new HYDAT (T301) and emails the owner the exact Actions button
  ("feeds — HYDAT → climatology → R2", upload: true). The owner presses it.
- **This ticket:** the worker calls GitHub's workflow_dispatch for `feeds-hydat.yml` (staging) itself on that
  detection, once per new HYDAT version (recorded in R2), and the email then says it started the run (or why it could
  not — reported, never silent). Needs a fine-grained GitHub token with Actions: write on this repo only, as a worker
  secret — the OWNER creates it. Without the secret the worker keeps today's email.
- The workflow already stops early when HYDAT is not newer, so a repeat start costs nothing.
Ships with the worker (with an app release, never ahead). Size: S. Owner: the token (when they want it automatic).

## 2026-11-05: built (deploys with the next worker deploy, with the next APK)
- `app/workers/feeds/src/dispatch.ts`: `dispatchOnce` POSTs GitHub's workflow_dispatch for `feeds-hydat.yml` (ref
  `redesign/stream-sections`, or the `GITHUB_DISPATCH_REF` var; inputs upload=true) when the gauge run raises a new-HYDAT
  notice. One start per HYDAT version (recorded in `private/feeds/dispatch/feeds-hydat.yml.json`); a failed start is
  retried at most every 6 h. No token → nothing is called.
- The HYDAT email's steps come from the result: "Nothing to press: the worker started …", or why it could not (GitHub's
  status + text) followed by the button, or (no token) the button as today. The Mac step (`update-hydat.sh --upload`)
  is unchanged.
- Tests: `test/dispatch.test.ts` (7). Worker suite 204 pass.
- Unproven against GitHub itself: whether workflow_dispatch accepts a workflow that is only on the dev branch (the UI
  needs it on main). If GitHub refuses, the first email says so with its answer; the fix is the workflow on main, or
  the ref var.
- Owner: the token (OWNER-TODO). T412 reuses `dispatchOnce` for run timing.
