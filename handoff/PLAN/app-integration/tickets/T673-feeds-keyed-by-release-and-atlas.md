# T673 — Release-bound and atlas-bound feeds are keyed by what they are bound to

Status: OPEN — split from T555 item 2 (2026-11-11); nothing built. Needs the worker, the deploy scripts and the app together, and a staging publish to prove it.

Source: T555 item 2 (data pipeline review 2026-11-09, severity 3, "partly": a documented design choice, not an oversight).

## Today
- `feeds/inseason/matches.json` names ONE release (`pipeline/regs/inseason/publish.py`; staged by
  `pipeline/deliver/sync_inputs.py`). The app reads `runs` / `lakes` only when `links.release` is its own release
  (`app/packages/core/src/inseason.ts`); on another release it says so (a failure line) and places nothing.
- `runtiming/index.json` is bound to one `section_handles`; on another atlas the mode is "stale".
- Both are overwritten in place, so a phone still on the previous release (not updated yet, or held back by
  `minAppVersion`) loses the per-section placement of partial-water in-season notices until it updates. Whole-water
  (`all`) notices, the banner and the notice text are unaffected.

## Why it was not built in T555
It is one change across five places that must move together, and none of it can be proved without a staging publish:
1. `publish.py` + `sync_inputs.py`: also write `feeds/inseason/matches-<release>.json`.
2. `deploy/lib/input-formats.sh` and the feeds worker's `/status.json` `inputs` list are keyed by the exact path
   (`feeds/inseason/matches.json`): a new key is refused as "no worker reads it" until both know the pattern.
3. The feeds worker reads `matches.json` to count matched notices: unchanged, but it must be redeployed with 2.
4. The app's in-season source is made before the release is open: it needs the release in use (as run timing takes
   `handles`), asks for its own key, and says what it does when that key is not there.
5. Run timing: `runtiming/<handles16>/index.json`, written by the run-timing workflow, read by the app by its handles.
Plus the publish check: the key for the published release and for the one before it both exist.

## Decisions needed before building (owner or lead)
- An app whose keyed file is missing (a bucket not republished yet, production today): read the unversioned file and
  check its `release` as now (a stated second read), or say "not placed for this release" at once?
- How many previous releases' files are kept, and who removes older ones (the staging upload clears old releases only
  after the pointer flips).
- Whether the unversioned keys stay for apps already installed (they read nothing else) and for how long.

## Work
Build 1–5 in one change set with tests (publish writes both keys with the same bytes; the format gate takes the
pattern; the app asks for its own release and handles; a held-back phone keeps its placement after a newer publish),
deploy the staging worker, sync, and check a phone on the previous release.

## Acceptance
A phone on release N-1 still places a partial-water in-season notice after N is published; run timing on the previous
atlas is not "stale"; the publish check lists both keys.

Depends: T555 (done parts), T601. Size: M. Owner: decisions above.
