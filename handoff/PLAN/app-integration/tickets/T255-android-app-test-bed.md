# T255 — Android app test bed: downloads, sync and offline (Maestro on the emulator)

Status: IN PROGRESS — lane app-test-bed (2026-10-29)

Source: owner 2026-10-29 — a reusable APP TEST BED in the Android emulator (reuse T50's AVD/tooling) proving the app
behaves with downloads, sync and offline: new feeds, a bigger update (Wi-Fi/metered, update needed), offline launch /
mid-session / reconnect, spots offline (photos, captured weather/gauge context, refresh on reconnect; a sync-adapter
seam for T223), report-a-problem outbox (T237).

## Work
1. Native UI runner (Maestro flows), network control via adb, a scenario runner + report with screenshots per step.
2. Run every scenario at least once (heavy.sh); pass/fail per scenario; fix small app bugs, ticket big ones.
3. Document running it locally and how accounts (T223) add scenarios.
## Acceptance
Every scenario run on the emulator with a report + shots; README for the bed.
Depends: T50, T251, T237. Size: L. Owner: no.
