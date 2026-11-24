# T1016 — Web: a tab kept open across two releases (or a site deploy) meets 404s on files production no longer serves

Status: PARTIAL — built d787d0dff, review fixes 36862bbbc (lane L6), automatic refresh b38201d83 (lane L17, owner 2026-11-24); left (Claude): the feeds worker deploy with the 0.9.1 batch (its `expected` class for the two names), and a production-export check of the chunk case

Source: T1005 triage. Production keeps the live release and the previous one (`deploy/prod/upload.sh` prunes the rest
24 h on). The 404 kinds of T1005 (`3d5ded1b72ff865f`, `2c766955748940d9`, `f6d0af3f051de23a`, `bb863fcb6535d696`, …)
were all a crawler holding a cached r-fc137c4040fa; but a READER's tab opened before two releases and still open after
the prune reads the same pruned files (a region's regs / water slice, the in-season section lines, the tile folder's
mask and sprites), and one opened before a site deploy loads a chunk that is gone (`AsyncRequireError`, kind
`240b0d41d6fc2c88`). Each is reported as a defect and shown as a failed read.

## Work
1. Reproduce on the web (local server): boot on release A, publish B, then C and prune A, then read a slice not yet
   opened; and a later chunk after the site's export changes.
2. A 404 on an immutable release file of a release the live pointer no longer names is a known state ("this page's data
   was replaced"): say so with one action (reload onto the live release, or apply the staged one), report it as
   `expected`, never as a defect. A missing chunk: the same, reload. One load path (the app's in-session apply).
3. Tests for both, against a fake origin.

Depends: none. Size: M. Owner: no.

## Built (lane L6, 2026-11-24, d787d0dff)
- **One second look before a report** (core `failure.ts` `setFailureTriage`), installed by the web only
  (`apps/mobile` `replaced.web.ts`; `replaced.native.ts` installs nothing: a phone reads its installed set and its code is
  in the binary). `@app/ui` `replaced.ts` `replacedTriage`:
  - a 404 (`<url>: HTTP 404`, or a status + url: MapLibre's AJAXError, the outside mask's, now carried) under the release
    root is asked of the live pointer, read fresh (`@app/data` `release/replaced.ts`): a path only the opened manifest
    names → the notice and a report renamed `ReleaseReplaced`; a path the LIVE manifest names → the 404 as it is (a defect);
    the pointer not answering → the 404 as it is. The in-season "no lines" failure carries its read's 404 as its cause.
  - a chunk that would not load (`AsyncRequireError`) is asked again with the site's own page (`siteProbe`): a 404 and a
    page that no longer names this page's entry → the "Reload" notice and `SiteReplaced`; a 404 while it does → a defect;
    no answer → the line's (not reported).
- **The notice** (ui-native `ReplacedBanner`, the slow/offline bar's look, above the Shell): data → "This page’s data was
  replaced by a newer release. / Load the new data ›", whose action is the app's one in-session apply
  (`loadLiveRelease`: the update service stages the live release, `apply` boots onto it); code → "The app was updated since
  this page opened. / Reload ›". Words new (no existing message fits): the owner's pick is open.
- **The worker** (`crashes.ts` EXPECTED += `/^(ReleaseReplaced|SiteReplaced): /`): needs a deploy; until then the renamed
  reports arrive as `defect` under the new names. `tools/wire.test.ts` proves the app's scrubbed payloads class `expected`.
- **Reproduced on the web** (Expo dev server over a fake origin: r-0aec88ed3edb as A, aliases B and C, A and its tile folder
  pruned): the Conditions tab's first read 404s (conditions, flow-area, flow-small), the notice shows, its action applies C and
  the panel loads (`../T1016-shots/prune-{light,dark}{,-after}.png`). The chunk case is proved by tests only (the dev server
  loads chunks another way).

## Review #101 fixes (lane L6, 2026-11-24, 36862bbbc)
- No look holds a report for good: the pointer read and the site probe's asks are aborted after 10 s (`POINTER_MS`,
  `LOOK_MS`), and the triage reports the failure as it is past `LOOK_MS` whatever the probe does (a stalled read used to
  hold the report, and every later release-root 404 joining it, until the tab closed).
- A chunk asked again with no answer is reported (as on main), now as a NEW error that says so first ("no answer when
  asked again (…)": the worker's existing `network` class); the error the part shows is never written.
- The worker's name anchor is pinned (`…: ReleaseReplaced: …` inside another message stays a defect).
- A late 404 of a read made on A that lands after the apply to C is judged against A (App keeps every set the page opened,
  `openedSets`): renamed `ReleaseReplaced`, raised for A, so no notice shows on C.

## Known, left for the planning chat (not changed tonight; the pointer format stays)
- (a) A 404 on the PREVIOUS release (which production promises to keep 24 h) is classed `replaced`: a too-early prune by
  `deploy/prod/upload.sh` would be masked as expected. Telling previous from older needs a `previous` field on the pointer.
- (b) The web has no app-version gate (`appVersion: null` on the web), so the notice's button can apply in session a
  release that needs newer site code. This predates T1016: the Shell's safe-moment apply does the same.
- (c) The staged-release case can take two presses of the notice's action (review finding on #101; not reproduced by
  the lane).

## Owner's answers (2026-11-24, verbatim)
- Asked about the notice's words: **"Force refresh instead?"**
- Asked how (auto unless typing / always / keep the notice): **"Always auto refresh"**

## Built (lane L17, 0.9.1, 2026-11-24, b38201d83): the tab refreshes itself
- **At once, typing or not, no notice to press.** `ReleaseReplaced` → the live release put in use through the same one
  in-session apply (`loadLiveRelease` → the app's `apply`); `SiteReplaced` → the page reloads (`replaced.web.ts`
  `autoReload`). `@app/ui` `createReplacedState({ refresh, sent, inUse })` decides; App hands it the two refreshes.
- **No loop.** One automatic refresh per verdict (kind + release) per page life: further 404s of the same set while the
  apply runs, or a late read of the set the page left, are reported (`ReleaseReplaced`) and never refreshed again; a data
  verdict for a set no longer in use is not refreshed at all. An apply the boot refuses goes back and says so itself
  (T555's line, reported there): not said twice. A reload at most once per page CODE: `reloadOnce` keeps the entry script
  it reloaded from in `sessionStorage` (`canifishthis.t1016.reloadedFrom`, survives the reload); the page back on the
  same code (a cached page) meeting the missing chunk again does not reload; new code may reload once for a later
  deploy. Storage that throws (a blocked/private window) or does not keep the marker: no reload (it could loop).
- **Reports first.** The refresh waits for core `failuresSent()`: every second look in flight has put its report in
  the sink, and the crash reporter's new `sent()` says every POST it was asked for is issued (`keepalive: true`, so it
  outlives the page; one waiting out the 1 s gap is waited for). The reports stay `ReleaseReplaced` / `SiteReplaced`
  (class expected).
- **The banner: kept only as the after-loop fallback** (W46: a failure is shown and reported). It shows when the
  automatic refresh could not be made (the update service staged nothing, a reload that would loop, the marker
  unkept), its action the same refresh by hand, and the failure is reported as `ReplacedRefreshNotMade` (a DEFECT:
  `tools/wire.test.ts` pins its class). Words and look unchanged, so knip has nothing to remove.
- **Web only** (34a): `replaced.web.ts` `autoReload`; `replaced.native.ts` `autoReload` throws if ever called (a phone
  installs no triage, so no verdict is raised there), as its `reloadPage` does.
- **Tests** (fake origin, `ui/replaced.test.ts`): the reports are in the sink before the apply, one apply for two
  404s, the failed read made again on C; no second refresh after the apply went back; nothing staged → notice + one
  defect report, never again; two chunks → both reports, then one reload; the next page life on the same code → no
  reload, notice, defect; `reloadOnce` on new code / throwing store / unkept store / no entry. `core/failure.test.ts`
  (`failuresSent` order), `ui/crashReport.test.ts` (`sent()` across the gap). 12 fail on main; nine mutations killed.
- **Pictures**: none re-shot. No new element: the fallback is the same bar, words and look as
  `../T1016-shots/prune-{light,dark}.png`; what a reader now sees on the automatic path is the loaded panel,
  `../T1016-shots/prune-{light,dark}-after.png`.
