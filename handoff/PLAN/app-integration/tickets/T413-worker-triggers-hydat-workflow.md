# T413 — New HYDAT is picked up without a remembered step (scheduled check, no new token)

Status: OPEN (owner 2026-11-05: "if the r2 feed worker detects hydat change could it auto trigger the hydat workflow
too?"; then: "I think the repo already does have the token for r2").

Revised 11-05: the repo's Actions secrets hold the R2 keys (`R2_ACCESS_KEY_ID` …), not a GitHub token, so the worker
cannot start a workflow without a new one. It does not need to: `feeds-hydat.yml` already asks ECCC's listing first (a
few KB) and stops unless HYDAT is newer than the staging feed. So give it a **weekly schedule** (STAGING, upload: true)
beside `workflow_dispatch`. A new HYDAT is then picked up within a week; an unchanged week costs one short run. The
worker's "new HYDAT" notice stays (it says the scheduled run will pick it up).

Note: GitHub runs a scheduled workflow only from the DEFAULT branch (`main`), like the Actions-tab listing — the
schedule takes effect when this lands on main (owner's merge). Same shape for the other slow feeds (T412).
Size: XS. Owner: no token needed.
