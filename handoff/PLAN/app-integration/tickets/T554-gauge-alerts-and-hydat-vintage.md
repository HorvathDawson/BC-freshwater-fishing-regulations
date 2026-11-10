# T554 — Gauge alerts say when their gauge is silent; alert rules stored safely; one writer for the HYDAT envelope

Status: OPEN.

Source: data pipeline review 2026-11-09 (owner-led; 14 areas + a gap check, every finding re-read against the code and release data by a second reviewer). Rank 10 of 17; highest severity 3 of 5.

Line numbers are the reviewers' and may be off by a few; "Second opinion" is what the verifier corrected. Fixes are proposals: re-read the code before building.

## Today
### 1. An alert on a gauge that stopped reporting says nothing: skip reasons are computed and dropped
Severity 3, wrong-or-missing-data, confirmed, reaches an angler; area gauges-hydat. Related ticket: T527.
- Evidence: core/alerts.ts:145-149 returns `skipped: missing | stale | no-record`, and :101-103 says it is "for the run log". app/packages/data/src/notify/check.ts:85-102 never reads `e.skipped`; the run line is "nothing to tell" (:140). The check reads only obs.json, which holds a station's rows from the last 6 h and only if it was written this run (publish.ts:457, 987-988). `recent` is every 6th CSV row counted from the head (publish.ts:85), so the newest row can trail the latest reading.
- Example: Staging 2026-11-10T02:05Z: obs.json has 429 of the index's 432 stations; 08HB022, 08KE016 and 08NM240 are absent. An alert on any of them is skipped as `missing` every run, and the list shows "not fired yet". The newest obs row trails the index's observedAt by a median 10 min, at most 145 min.
- Second opinion: Understated in one respect: a station ECCC answered 304 for (unchanged) takes `continue` at publish.ts:~906 before obs is filled, so it is absent from that run's obs.json and its alert is skipped as `missing` for that run even though the gauge is healthy; the check only evaluates on runs where the CSV changed. Staging counts (429 of 432, lag figures) not re-fetched, so unverified.
- Proposed fix: Keep the last skip reason and since-when in ruleState. Show it on the alert row ("gauge not reporting since ...") and in the run log. Optionally one notification when an alert has been unevaluable for 24 h. Append the last non-null CSV row to `recent` so obs carries the newest reading.

### 2. Gauge alert rules (user-authored) are stored with a plain overwrite; spots get an atomic write, a backup and a kept-aside corrupt copy
Severity 3, fragility, confirmed, reaches an angler; area app-update-integrity.
- Evidence: app/packages/data/src/spots/store.native.ts:147-153: fileStore.write is `file().writeSync(t)` straight onto the file; openNotifyStore (:137) uses it for notify.json. notify/settings.ts:114-124 update() rewrites the whole file on every change, including the background check's ruleState/runs writes and noteHeld on every app open (App.tsx:303-306). settings.ts:74: an unparseable file returns NOTIFY_DEFAULT (no alerts, notifications off) with reportOnce only; the next update() overwrites the damaged file. Spots: spots/core.ts:85-89 writes `.writing`, renames, keeps `.bak`; :66-70 keeps a corrupt file aside and throws SpotsCorrupt.
- Second opinion: Overstated: noteHeld does not write on every app open. settings.ts:161-162 returns the same state when the release is unchanged and update() skips the write (:119). Writes happen on rule edits, background-check runs (noteRun, ruleState) and a release change. Understated: an EMPTY notify.json (the likeliest torn-write result) hits `if (!text) return NOTIFY_DEFAULT` at settings.ts:72 with no report at all, and a read error is swallowed to null at store.native.ts:151, so alerts can vanish with nothing reported. Whether expo writeSync is itself atomic was not verified; no loss has been observed.
- Proposed fix: Make fileStore.write atomic (write `<name>.part`, then moveSync, as SpotsFs.rename does), which fixes every small store at once. For notify.json, keep an unparseable file aside (`.corrupt-<ts>`) and show a line in the alerts page that alerts could not be read, as spots do.

### 3. A percentile alert keeps the line of the HYDAT vintage it was saved on, forever
Severity 2, divergence, confirmed, reaches an angler; area gauges-hydat. Related ticket: T527.
- Evidence: app/packages/ui-native/src/AlertMe.tsx:141 saves `byDay: percentileByDay(rec.yearBand, p)`: 366 values copied from the release's gauge_clim at save time (core/alerts.ts:110-112). `thresholdAt` (alerts.ts:119-123) reads only that stored array. `percentileByDay` has no other caller, and nothing in data/src/notify/settings.ts or check.ts recomputes it when the release (`noteHeld`, settings.ts:161) or HYDAT changes.
- Example: An alert "crosses above the median" saved on release HYDAT 20260717 still uses that median after the app takes r-a178299f6005 (HYDAT 20261019). The chart and the feed's percentile have moved to the new record; the alert's line has not, and the notification still says "the median for Nov 9".
- Second opinion: Accurate. Impact is modest: HYDAT changes about yearly and the line moves little for long records; it matters for short records and corrected level records (see the level finding). The rule also stores no release id, so the drift cannot even be detected today.
- Proposed fix: Store the release id with the rule. When the app opens on a different release, recompute `byDay` for every percentile rule from the new yearBand, or refuse and say so if the station lost its record. Test: save, change release, expect the new line.

### 4. clim.json has two writers; a release upload can put an older HYDAT envelope back and nobody is told
Severity 2, divergence, confirmed, reaches an angler; area feeds-worker.
- Evidence: .github/workflows/feeds-hydat.yml:129 copies the rebuilt clim.json to <bucket>/feeds/live/clim.json. deploy/staging/upload.sh:79 and deploy/prod/upload.sh:76 copy the Mac's data/generated/gauges/feeds/clim.json over it with --checksum (replace when different), with no comparison of the `release` field. Worker: gauges/publish.ts:1038-1039 would raise the hydat notice again, but events.ts:172 drops the already-told key `hydat:<latest>` and dispatch.ts:50 returns the recorded 'started' for that version.
- Example: The local clim.json has release '20261019'. If the workflow has put a newer HYDAT in R2 and update-hydat.sh has not been run on the Mac, the next upload.sh restores 20261019.
- Second opinion: As claimed. The window is real by design: the worker starts the workflow by itself while the Mac step (scripts/update-hydat.sh) waits on the owner, and any release upload in between restores the old envelope. Effect on an angler is small (percentile bands from the previous HYDAT; the app's 'newer HYDAT is out' line would show).
- Proposed fix: In both upload scripts, read the remote clim.json `release` and refuse (or skip) when the local one is older. Include the envelope's current release in the hydat event key so a regression is told again.

### 5. A HYDAT workflow start is recorded as done on HTTP 204; a run that then fails is never retried or re-told by the worker
Severity 2, fragility, confirmed; area feeds-worker.
- Evidence: dispatch.ts:67-68 records state 'started' when GitHub answers 204 (request accepted, not run succeeded); dispatch.ts:50 returns that record for the same version on every later call, so nothing starts again. index.ts:94 keys the event `hydat:<latest>`, told once (events.ts:172). dispatch.ts:11-12 hard-codes ref 'redesign/stream-sections'. Run timing uses the same dispatchOnce (index.ts:476).
- Second opinion: As claimed. Softer than stated in two ways: the email does tell the owner to check the run in GitHub Actions (dispatch.ts:81), and the hydat notice stays in the gauge index while clim.json is behind, so the app keeps saying a newer HYDAT is out. Run timing is called from index.ts:489, not :476.
- Proposed fix: While clim.json's release is still behind the latest N hours after a 'started' record, emit a new event (key includes the day) and allow one re-dispatch. Alternatively have the workflow call back to the worker on failure.

## Work
Fix the items above in severity order, one commit per item where they are separable. Anything found and not done becomes a new ticket.

## Acceptance
An alert on a gauge with no report for the limit tells the user once (and a 304 "unchanged" is not "missing"). notify.json gets the spots file's atomic write + backup. A percentile alert records its HYDAT vintage and re-derives on a new one. A release upload cannot put an older clim.json back; a failed HYDAT workflow is retried or re-told.

Depends: —. Size: M. Owner: no.
