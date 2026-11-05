# T412 — One "slow feed" pattern, run timing first (HYDAT clim and bathymetry follow it)

Status: OPEN (owner 2026-11-05, T408 walkthrough; `handoff/PLAN/data-flow-decisions.md` "Run timing").

Run timing is made two ways today (Mac deliver, T373, and the manual `feeds-runtiming.yml`, which refuses until the
owner uploads a binding base after each atlas promote). Owner: a slow feed like HYDAT and later bathymetry — "make sure
it is producing same output … deployed to R2 directly".

## Work
1. One path: the `feeds-runtiming.yml` workflow, own R2 folder + `index.json`. Deliver stops making run timing.
   **Started by a button or by the worker — never a GitHub schedule** (owner 11-05: GitHub as scheduler is flaky and
   unpaid). The worker emails when a source looks changed (or starts the run itself, T413's token).
2. `upload.sh` uploads the binding base with every release, so the workflow never waits on the owner.
3. Content-hashed files; publish only on a real change, with a diff report (runs / waters added, dropped, changed).
4. A build-twice test asserts identical bytes. Staging automatic; prod the owner's choice.
5. Write the pattern down so HYDAT clim and bathymetry follow it when they are next touched.

Depends: T297 (release command). Size: M. Owner: no (prod enable is theirs).
