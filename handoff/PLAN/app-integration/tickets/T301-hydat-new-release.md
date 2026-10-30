# T301 — New HYDAT release: refresh stored daily flows and normals

Status: OPEN (owner 2026-10-30: the app says a new HYDAT release is out).

## Work
Confirm the new ECCC HYDAT release (version/date), download it (resumable, checksum), rebuild what depends on it: the
stored past-day flows (T274 `pipeline/gauges/feed/hydat_daily.py` → staging `feeds/hydat/`), normals/percentiles used by
Conditions, gauge station metadata; check the app's "new HYDAT release" notice clears. Diff station counts/periods vs the
old release; report anything that changes a gauge match (gauge-match one frozen file — never re-match). Prod upload stays human.

## Acceptance
App shows the new HYDAT version; notice gone on staging; diff summary.

Depends: T274. Size: S–M. Owner: no (prod upload).
