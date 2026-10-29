# T231 — the update service: the app checks the data origin for new feeds and releases and downloads them

Status: DONE — 5681f6b8, 5c2b9cfb (staging e2e: the moved feed fetched, the others 304).

Source: owner 2026-10-28 — "the app must check the data origin (R2 via the data worker) for new updates and download
them: new feeds (gauges, DFO, in-season) and bigger updates (a new release manifest: bundle files, salmon slices,
synopsis PDF, tiles/basemap). Build the FRAMEWORK, wired into the existing sync button." Add-on (owner, same day): the
top bar's bell (T230) also lists sync failures — one bell, grouped sections; the sync button keeps its own red badge.

## Work (built)
1. `@app/data` `update/` — `checkObject` (conditional GET: If-None-Match, then sha256 of the body; offline vs HTTP
   failure told apart; a weak edge ETag is sent strong — the data worker compares strong only, measured on staging) and
   `createUpdateService`: feeds (gauges `feeds/live`, DFO `feeds/dfo`, in-season `feeds/inseason`) and the release
   (`release/manifest.json`). Checks on start (the boot's manifest read seeds it: no second request, T129), on return
   to the foreground, every 15 min (feeds) / 3 h (release), on the button. Backoff 30 s → 15 min, at once back online.
   A changed feed is handed whole to its loader (`GaugeFeed.offer`, `SalmonFeedSource.offer` + `version`,
   `InSeasonSource.refresh`); a refused body is not kept or "seen". State kept across launches (`openUpdateStore`).
2. Release: new when its data_release, tiles, basemap or assets moved. Staged by `Booted.stage` (boot.ts): a phone
   installs the whole set beside the one in use (`install`, verified, resumable, one atomic pointer swap; the launch's
   own install is deferred to the service, `deferUpdate`); the web warms the changed files it opened into the release
   cache. A large set on a metered connection waits for Wi-Fi unless the More tab's setting allows it
   (`Connectivity.metered`, NetInfo type/isConnectionExpensive). Applied at a SAFE MOMENT (a browse tab, nothing open)
   or on return from the background: `useReleaseBoot(opts, generation)` re-boots with the old set on screen until the
   new one is ready; "Updated to <date>" toast (`UpdateToast`). One pending release at a time.
3. Sync button (`syncVM` kinds): idle, checking (turn), downloading (progress ring `SyncRing` + dot), ready / waiting
   for Wi-Fi (dot), done (brief check), error (red `closed` badge "!" — an origin/bytes failure, or offline after a
   manual check); the error tap opens `SyncErrorSheet` (what failed, last success, Retry).
4. The bell (T230) counts in-season rows + failed sources; its sheet has a "Could not update" section (same
   `SyncFailures` body) above the in-season changes. More → Data: an Updates row (last checked, per source checked /
   updated / failure) and, on a phone, "Large updates on mobile data".

## Acceptance
- Unit (mocked fetch): `packages/data/src/update/service.test.ts` — unchanged (304 / same bytes), changed, failed,
  offline, partial, refused body (atomic), sha mismatch, metered, apply, schedule/backoff, foreground, persistence,
  weak ETag. Over the real boot: `tools/update-service.test.ts` (phone deferUpdate + stage + apply + prune; metered;
  sha mismatch leaves the pointer; web cache warm). Words: `core/src/updates.test.ts`; drawn: `SyncButton.test.tsx`.
- E2E on staging (2026-10-28, web against data-staging.canifishthis.ca): `feeds/inseason/index.json` then
  `feeds/dfo/index.json` bumped (a trailing newline) and restored; a tap fetched only the moved one (others 304), the
  DFO capture was replaced via `offer` with no second fetch, in-season re-read. Release apply exercised with an
  intercepted manifest (toast shot).
- Shots `../T231-shots/`: `{desktop,phone}-{idle,checking,downloading,done,error,error-sheet,error-bar-bell,
  bell-notices,applied-toast,more-updates}.png`.

## Not done / follow-ups
- Synopsis PDF stays fetched on first open (its url moves with the manifest); not pre-downloaded.
- The data worker compares ETags strongly; the client strips `W/`. A worker-side weak compare would be cleaner (T225).
- Native not run on a device this lane (metered path and the re-boot apply are tested over the in-memory phone).

Depends: T225 (feeds), T230 (bell). Size: L. Owner: no. Default: feeds 15 min, release 3 h, Wi-Fi threshold 20 MB.
