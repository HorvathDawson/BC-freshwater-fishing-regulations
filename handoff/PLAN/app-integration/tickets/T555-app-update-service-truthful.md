# T555 — Update service: a refused release never reads "Updated"; one byte count; every format bump is gated; saved ids follow aliases

Status: IN PROGRESS — items 1, 3, 4, 5 built (9312354, ed637de, ab6c897, 138f1a5e0, 5018a81d8, d707dcdfb); item 2 is its own ticket, T673; left (Claude): the emulator check under "Not device-tested" below.

Source: data pipeline review 2026-11-09 (owner-led; 14 areas + a gap check, every finding re-read against the code and release data by a second reviewer). Rank 11 of 17; highest severity 3 of 5.

Line numbers are the reviewers' and may be off by a few; "Second opinion" is what the verifier corrected. Fixes are proposals: re-read the code before building.

## Today
### 1. A refused (or retrying) release makes the update service say "ready" and then "Updated to ..." in a loop while the phone stays on the old set
Severity 3, wrong-or-missing-data, confirmed, reaches an angler; area app-update-integrity.
- Evidence: app/packages/data/src/release/boot.ts:420-424: Booted.stage() resolves null when the target is the refused set, the set being retried, or already staged. app/apps/mobile/src/App.tsx:343-347 awaits b.stage() and drops the result; ReleaseSpec.stage is Promise<void> (update/service.ts:49), so service.ts:361-364 sets phase "ready" on any resolve. App.tsx:380-398: the next foreground return calls apply() -> a full re-boot -> updates.applied(old release) + the "Updated to" toast (only applyError is checked, not booted.rolledBack). applied() clears relSeen (service.ts:595), so the next check stages "again". app/tools/release-boot.test.ts:514-520 pins only the boot half (update resolves null).
- Example: Code trace, not seen on a device: release-boot.test.ts:502-512 damages r2's paint on disk; r2 is refused and r1 stays in use. With the service wired as in App.tsx, each later release check marks r2 "ready" and the next foreground return re-boots onto r1 and toasts "Updated to <r1 date>".
- Second opinion: The trace holds. Overstated: it is not a tight loop; each cycle needs a release check and then a return from the background. It needs a refused set first, which only follows a proven defect (sha, format, decode failure), so it is rare. In the retry case the re-boot is a real second try and can succeed; only when it rolls back again is the toast false. The data shown is never wrong, only the 'Updated to' message and the 'ready' state. Not seen on a device.
- Proposed fix: Have ReleaseSpec.stage return staged / nothing-to-do / refused; the service stays "current" with a visible "release X could not be used on this device" state (reportFailure once). In App.tsx, skip applied() and the toast when booted.rolledBack is set or the booted release is unchanged. Add a service-plus-boot test for the refused and retry cases. Consider re-downloading once when the defect is on-disk damage rather than refusing the release.

### 2. Release-bound and atlas-bound feeds live at one unversioned key, so a publish orphans every device still on the previous release
Severity 3, fragility, partly, reaches an angler; area deliver-contract.
- Evidence: `feeds/inseason/matches.json` names ONE release (pipeline/regs/inseason/publish.py:12,83; staged by pipeline/deliver/sync_inputs.py:66-75). The app reads `runs`/`lakes` only when `links.release === release` (app/packages/core/src/inseason.ts:155, 236 returns `INSEASON_FAILURES.offRelease`, sections null). `runtiming/index.json` is bound to one `section_handles` (app/packages/core/src/runTiming.ts:8, 104-118); on another atlas the mode is 'stale' (app/packages/ui/src/runTiming.ts:118). Release files themselves are content-named and immutable (publish.py), but these two are overwritten in place.
- Example: Publish release N with matches.json resolved on N: a phone still holding N-1 (not yet updated, or held back by minAppVersion) loses the per-section placement of every partial-water in-season closure, and gets a failure line instead.
- Second opinion: Code is as described, and it is a documented design choice, not an oversight (publish.py docstring: runs/lakes read only on the release named; `all` holds on any release). Overstated: 'orphans every device'. The banner and the notice still show on the water; only the per-section placement of partial-water notices is lost, and it is said (failure line). Data updates are over the air, so the window is until the device takes the new release; it persists only for devices held back by minAppVersion. Whole-water (`all`) closures are unaffected.
- Proposed fix: Key them by what they are bound to: `feeds/inseason/matches-<release>.json` and `runtiming/<handles16>/index.json`, keep the previous one, and have the app ask for its own release / handles. Publish check: the key for PUBLISHED and the prior release both exist.

### 3. Two byte-count functions and four "did the set change" tests; the mobile-data gate uses the one that omits the rule book and map assets
Severity 2, duplication, confirmed, reaches an angler; area app-update-integrity.
- Evidence: Bytes needed: update/service.ts:236-242 releaseNeed counts changed files + tiles + mask + basemap; release/install.ts:292-296 installBytes also counts the synopsis (via bigFiles :286). App.tsx:340-348 passes no `need`, so service.ts:383-384 applies the 20 MB metered threshold to releaseNeed, while the first install's gate uses installBytes (boot.ts:296, 317). Set changed: service.ts:228 releaseMoved (release/tiles/basemap/assets ids), install.ts:247 sameSet (also synopsis + every file sha), manifest.ts:93 diff, notify/check.ts:60 (data_release only).
- Example: Live manifest: synopsis.bytes = 11,722,042. An update with 10 MB of changed data files plus a new synopsis downloads about 21.7 MB, but releaseNeed says 10 MB, so it goes on mobile data without asking, and the waiting-for-Wi-Fi size omits it too.
- Second opinion: The under-count is the synopsis only. installBytes does not count map assets either (bigFiles has no assets entry), so the title's 'and map assets' is not a difference between the two functions. The synopsis changes about once a regulation edition, so the wrong mobile-data decision can happen only on those updates, and by at most about 11.7 MB. Did not re-read notify/check.ts:60 line by line.
- Proposed fix: Delete releaseNeed and have the service use installBytes (pass it as ReleaseSpec.need, or import it). Define "moved" once as !sameSet(pointerFor(m), held) and use it in the service, boot and notify. Add a test that the three agree on a synopsis-only and an assets-only change.

### 4. The app-version gate knows only paint and salmon; every other format bump relies on someone remembering minAppVersion
Severity 2, missing-check, confirmed, reaches an angler; area deliver-contract.
- Evidence: pipeline/deliver/release/app_gate.py:21-24: `PAINT_NEEDS = {5: "0.3.0"}`, `SALMON_NEEDS = {3: "0.5.0", 4: "0.5.0"}`, comment 'Add a row whenever paint.py's FORMAT moves'; `problems` (33) loops over those two keys only. pipeline/deliver/release/site_gate.py imports the same two tables. Meanwhile pipeline/deliver/release/contract.py moved `water` to water/3 and `geo` to geo/2 (T517, 'An app reading water/1 refuses a water/2 release') with no gate row.
- Example: r-a178299f6005 ships water/3, geo/2, water-slice/4 with minAppVersion 0.7.0 set by hand; had the stamp been left at an older value the gate would have passed it.
- Second opinion: Confirmed. Impact is smaller than a wrong answer: an old app that meets an unread format keeps its held data (boot.ts) but shows a read failure in place of 'update the app'. site_gate.py's own header records the 2026-11-09 case (r-d14bdb180f55, stamp 0.7.0 against a 0.6.9 site), which shows the stamp is carried by hand.
- Proposed fix: Emit a `{app version: {file: format}}` table from pack.generated.ts at each app release (committed), and have app_gate derive the oldest app that reads ALL of a manifest's formats; refuse a stamp below it. Removes both hand tables.

### 5. core.item_alias (old id -> current id) ships in every release but no app code reads it; saved ids are looked up exactly
Severity 2, wrong-or-missing-data, partly; area app-update-integrity.
- Evidence: pipeline/deliver/bundle/build.py:228 fills item_alias; pipeline/deliver/release/build.py:99 ships it in core; pipeline/deliver/export/read.py:664 calls it "what an old id names now, for a saved link". In app/ the only hit is the generated contract (packages/data/src/pack.generated.ts:118). Lookups by id are exact: release/queries.ts:74 (`SELECT ord FROM item WHERE item_id = ?`), :80, :84, and regs/reads.ts:331. User data holds item ids: spots (spots/model.ts:216), gauge alerts (core/src/alerts.ts:62, opened at ui-native/src/Shell.tsx:402 with no existence check), recent searches.
- Example: Live r-a178299f6005 core: item_alias has 117 rows, e.g. wbk:328965685 -> gnis:17795 (Camp Slough), wbk:328968065 -> gnis:26104 (West Road (Blackwater) River). None of the 117 alias ids exist in item. Ids were otherwise stable between r-25a89ce9f8e6 and r-a178299f6005 (19806 items, 0 gone).
- Second opinion: The table is shipped and unread: confirmed. But no shipped release ever exposed any of the 117 alias ids as an item id, so no saved spot, alert or recent search can hold one today. The harm is hypothetical until a future build absorbs an id that has already shipped. I did not verify the claimed alert-tap and spot-link behaviour for a missing id. Worth a small ticket (resolve through item_alias, plus a release check for ids gone without an alias), not a current wrong answer.
- Proposed fix: Add one resolveItem(id) in @app/data (item, else item_alias) and use it in every by-id entry point (opener.item, spot water link, alert tap, recent searches, start link). Rewrite the stored id on first resolve. Add a test with a fixture alias row. Add a release check that lists ids gone from the previous release with no alias.

## Work
Fix the items above in severity order, one commit per item where they are separable. Anything found and not done becomes a new ticket.

## Acceptance
A refused set shows a failure state, not "ready/Updated" (test with a bad sha). One install-size function, used by the mobile-data gate. The app-version gate is generated from every format constant. Saved ids resolve through core.item_alias (test with an absorbed id).

Depends: —. Size: M. Owner: no.

## Done 2026-11-11 (items 4 and 5; item 2 split out)
- Item 5, callers (138f1a5e0, 5018a81d8). Every by-id read in `@app/data` follows `core.item_alias`: `water` and `regulationKeys`
  (ab6c897), now also `locate`, `waterBox`, `waterPoint` and the regulations' `itemOrd` (the alias is asked only for an id the
  release does not hold, so a current id costs no extra read) — so an alert tap, a spot's water
  link, a start link or an app link with an absorbed id opens the water it became. `WaterSource.currentItem` is the one
  resolver. A KEPT id is rewritten once per release opened (`followSavedItems`, `packages/data/src/savedIds.ts`, called
  from App.tsx for a cold start and an applied update alike): a spot's `item`, a gauge alert's `water.item`. Recent
  searches hold the text typed, never an id: nothing to follow there (the ticket's list was wrong on that one). An id
  gone with no alias is left as it is. Tests: `release/alias.test.ts`, `regs/reads.test.ts`.
- Item 5, the release check (138f1a5e0). `pipeline/deliver/release/gone_ids.py`: the live release's item ids the
  candidate neither holds nor aliases, listed by name in the checks stage (`checks/<r>/gone_ids.txt|json`, the count in
  the ledger). It lists and never stops. Read on the two releases on disk (r-a178299f6005 → r-303b4007a70a): 19,788 of
  19,806 kept, 18 absorbed and followed by an alias, 0 gone — so absorbed ids HAVE now shipped (the second opinion's
  "hypothetical until a future build" is no longer true), and the app half above is what keeps them opening.
- Item 4 (d707dcdfb). `pipeline/deliver/release/app_reads.json`: per app version, the formats it reads for every file
  of the release contract (the contract's own + the app's `ALSO_READS`). Written by `python -m
  pipeline.tools.emit.app_reads`; its `--check` is in `pnpm check` (`app-reads`) and `scripts/fast-fail.sh`: the current
  app.json version must have its row and the row must be what the code reads, so a format moved with no version bump
  fails (bump the version and record; `--replace "<why>"` only when no build of that version left the machine).
  `app_gate.problems` derives the oldest app that reads ALL of a manifest's formats and refuses a stamp below it;
  `site_gate.required` asks the same. `PAINT_NEEDS` / `SALMON_NEEDS` are gone. Backfilled from git (the contract at
  each version's bump commit): paint/5 → 0.3.0, salmon/4 → 0.5.0, water/3 and geo/2 → 0.7.0, the same answers the hand
  tables and stamps gave; the live release derives 0.7.0, its stamp. The table is Python-side because the contract's
  source is (`contract.py`), not `pack.generated.ts`. NEW STOP for every lane: a version bump in app.json needs
  `python -m pipeline.tools.emit.app_reads` (one line, like the changelog entry). Tests: `test_app_reads.py`.
- Item 2 → T673 (feeds keyed by release and atlas): one change across the publisher, the deploy format gate, the feeds
  worker, the app and the run-timing workflow, with decisions listed there; not built here.
- Item 1, the rest (e4add6e9a, with T585 F12): a re-boot onto the set already in use is never "Updated to" (`bootSaid`).

## Not device-tested (the emulator run should check)
- A spot and a gauge alert saved on an absorbed id (e.g. one of the 18 above, saved on build 22's release) open their
  water after the update, and the spot's water link and "this water's spots" list hold the new id.
- The app starts and updates as before with the new `release/opened` record (T585 F12) on a phone that already holds a set.

