# T413 — A new HYDAT starts the HYDAT workflow: from the worker, or the owner after the email

Status: OPEN (owner 2026-11-05). Owner: "I don't want GitHub to be the scheduler — it is flaky and unpaid, it will
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
