# T368 audit — step 1 "Find" (read-only, 2026-11-04)

Scope: `app/packages/{core,data,ui,ui-native,map}/src`, `app/apps/mobile` (no `*.test.*`, no `dist/`, no generated).
Found by grep (`catch`, `.catch(`, `console.warn/error/log`, `void <promise>`, two-argument `.then(ok, fail)`), then
each site read in context. Paths below are relative to `app/`.

Classes: **RETHROW** · **S+R** = shown to the reader AND `reportFailure` (crashReport.ts) · **FINE** (reason given) ·
**ASK** (question in the ASK list, by number). "shown" in a note = the reader already sees it today; S+R then means
"add the report". `Q1` on a row = its report half depends on ASK Q1 (environmental failures).

## Cross-cutting findings (read these first)

- **X1 — native unhandled rejections are lost.** `installCrashHandlers` (ui/src/crashReport.ts:313-333) hooks RN's
  `ErrorUtils` and, where `addEventListener` exists (web only), `unhandledrejection`. Hermes needs
  `HermesInternal.enablePromiseRejectionTracker`; nothing installs it. So every `void p` with no `.catch` below is
  reported on the web and **silent on a phone**. Proposal: install the tracker → `report(e, {kind: "promise"})`.
- **X2 — `useAsync` (ui/src/async.ts:94-101) is the choke point.** ~20 hooks surface failures only as
  `state: "failed"`; none is reported, and a failed REFRESH keeps the old value with no state change. One
  `reportFailure` in its rejection branch covers every hook below marked "via useAsync".
- **X3 — `console.error` is teed into the Updates log (ui/src/errorLog.ts); `console.warn` is not.** Neither reports.
  Every "console.error/warn instead of handling" row below is S+R or ASK.
- **X4 — "a failure is not cached" pattern** (`p.catch(() => cache.delete(k))` while the caller still gets the
  rejection) is FINE everywhere it appears; listed once per file.

## core

| file:line | what it does | class | why / note |
|---|---|---|---|
| packages/core/src/layers.ts:373 | layer pref JSON unreadable → defaults | FINE | a preference |
| packages/core/src/day.ts:103 | no Intl zone data → device clock for "today in BC" | ASK Q6 | decides open/closed dates |
| packages/core/src/paint.ts:100 | accessOf throw → `r.fail(msg)` | FINE | turned into the reader's typed failure |
| packages/core/src/recentSearches.ts:42 | recent list unreadable → [] | FINE | a preference |
| packages/core/src/runTiming.ts:349 | run species pref unreadable → default | FINE | a preference |

## data

| file:line | what it does | class | why / note |
|---|---|---|---|
| packages/data/src/release/source.ts:251 | basin grid cache reset, rethrows | FINE | X4 |
| packages/data/src/release/source.ts:519 | CONTINUE_LINKS SQLite read fails → `[]` (continuations silently dropped) | RETHROW | a failed bundle read shown as "no continuation" |
| packages/data/src/release/bgDownload.ts:122 | console.warn, re-queue, throws after N | FINE | bounded retry, final failure thrown |
| packages/data/src/release/install.ts:145 | follower hash read fails → hash off disk | FINE | falls back to the authoritative check |
| packages/data/src/release/install.ts:347 | held manifest unreadable → no asset reuse | FINE | costs a download only |
| packages/data/src/release/install.ts:354 | assets failure kept, thrown at :383 | FINE | deferred rethrow |
| packages/data/src/release/prefetch.ts:65,69 | ahead-of-need prefetch, `.catch(()=>{})` | FINE | loader does not cache failures; the real ask shows |
| packages/data/src/release/assets.ts:75,83 | asset list unreadable → re-download / VerifyError | FINE | verified either way |
| packages/data/src/release/assets.ts:121 | per-asset retry with console.warn, `failed` thrown | FINE | bounded retry |
| packages/data/src/release/loader.ts:139 | `stored()` fails → false (read slices) | FINE | routing hint, same rows |
| packages/data/src/release/loader.ts:154,169,177,186,195,206,218,227,242,279 | un-cache on failure | FINE | X4 |
| packages/data/src/release/loader.ts:310 | warm of the new set, `.catch(()=>undefined)` | FINE | warming; the real read shows |
| packages/data/src/release/offline.ts:44 | `stored()` fails → counted as not held | S+R | offline size silently under-counted |
| packages/data/src/release/background.ts:174,220 | log/mark hooks guarded | FINE | instrumentation |
| packages/data/src/release/background.ts:221-229 | prefetch failure → `say({kind:"failed"})` | FINE | goes to the Updates log as an event |
| packages/data/src/release/boot.ts:161,185,197 | typed rethrow / defect classification | FINE | rethrows |
| packages/data/src/release/boot.ts:252-253 | manifest unread → console.warn, held set opens | FINE | offline is normal; first run throws BootError |
| packages/data/src/release/boot.ts:263 | background queue failed → console.warn only | S+R | the download then waits on nothing visible |
| packages/data/src/release/boot.ts:264 | `beforeBackground` failure ignored | FINE | permission prompt nicety |
| packages/data/src/release/boot.ts:325-344 | open fails → ROLLBACK to previous set | S+R | shown (`rolledBackLine`, App.tsx:557); add report |
| packages/data/src/release/boot.ts:328,349 | manifest unreadable → prune without it | FINE | cleanup |
| packages/data/src/release/boot.ts:377 | `update.catch(()=>{})` | FINE | update service owns staging failure (service.ts:349-370) |
| packages/data/src/release/drivers/serial.ts:20 | queue tail swallow | FINE | caller holds `p` |
| packages/data/src/release/drivers/cache.web.ts:27,37,45,52 | Cache Storage refused → memory | FINE | best-effort cache; a session copy |
| packages/data/src/release/drivers/open.native.ts:37,41,47 | rethrow named | FINE | rethrows |
| packages/data/src/release/drivers/open.native.ts:43 | `void close` | FINE | closing |
| packages/data/src/release/drivers/install.native.ts:146,151 | retry with warn / release | FINE | bounded retry, thrown |
| packages/data/src/release/drivers/source.web.ts:29 | cached bytes fail verify → refetch | FINE | verified |
| packages/data/src/release/drivers/source.web.ts:52 | priority raise `.catch` | FINE | original promise carries outcome |
| packages/data/src/release/drivers/open.web.ts:55 | wasm engine un-cache | FINE | X4 |
| packages/data/src/openProbe.ts:24,78,86 | test-bed probe console.log | FINE | probe only |
| packages/data/src/net/verified.ts:37,66 | VerifyError → non-resumable retry | FINE | rethrows otherwise |
| packages/data/src/net/fetch.ts:153,200 | retry loop | FINE | final NetError thrown |
| packages/data/src/update/log.ts:25,28 | log store read/write | FINE | log storage |
| packages/data/src/update/check.ts:33,41 | fetch fails → `{kind:"failed"}` | FINE | a value; shown by SyncErrorSheet (Q1 for report) |
| packages/data/src/update/service.ts:257,268,269 | event emit guard / validator cache | FINE | cache |
| packages/data/src/update/service.ts:330 | feed refused by its loader → `failSource` | S+R | shown (sync badge); a refused feed is a defect, report |
| packages/data/src/update/service.ts:411 | manifest unparsable → `failSource` | S+R | shown; defect, report |
| packages/data/src/update/service.ts:453 | outbox flush `.catch` | FINE | outbox keeps + shows queued |
| packages/data/src/update/service.ts:489,500,507,511 | `void service.check` | FINE | check reports by events; verify it never rejects (step 2) |
| packages/data/src/regs/blob.ts:33,39 | typed rethrow | FINE | rethrows |
| packages/data/src/regs/reads.ts:207,234 | un-cache / reject batch | FINE | X4 |
| packages/data/src/regs/reads.ts:188,238,241 | `void p.then(f)` / `void flush` | FINE | flush rejects every waiter itself |
| packages/data/src/regs/place.ts:38,47 | un-cache / rethrow damaged | FINE | rethrows |
| packages/data/src/regs/store.ts:95,96,115,161,172 | refusals rethrown | FINE | rethrows |
| packages/data/src/feed/history.ts:86,99,210 | station-week cache read/write | FINE | cache |
| packages/data/src/feed/inseason.ts:36,50 | capture read/write | FINE | cache |
| packages/data/src/feed/inseason.ts:42 | network / HTTP / bad JSON → null → capture or nothing | S+R | no capture: the bell reads "no in-season changes" — a failure looks like none |
| packages/data/src/feed/http.ts:309,344,353 | history notes / obs sync | FINE | cache |
| packages/data/src/feed/http.ts:311 | offline → last good copy | FINE | aged copy, age shown (P02) |
| packages/data/src/feed/runtiming.ts:47,58,68,70 | index/runs fail → capture (`from:"capture"`) or null → "failed" | FINE | shown as captured / failed (MapLegend) — Q1 for report |
| packages/data/src/feed/salmon.ts:35,45,61 | capture read/write | FINE | cache |
| packages/data/src/feed/salmon.ts:49 | fetch fails → capture | FINE | capture carries `at`; stale regions said (core/salmonFeed.ts:224) |
| packages/data/src/feed/salmon.ts:85 | `feed.latest().catch(()=>null)` | FINE | latest() never rejects (defensive) |
| packages/data/src/report/outbox.ts:50,53,72 | queue store / error body | FINE | queue storage |
| packages/data/src/report/outbox.ts:63 | offline → queued | FINE | shown ("saved on this device") |
| packages/data/src/spots/likeVisit.ts:176,177 | weather/feed → null, `offline` flag | FINE | shown as offline (Q1) |
| packages/data/src/spots/likeVisit.ts:203,212,215 | like cache | FINE | cache |
| packages/data/src/spots/runTimingStore.native.ts:9, runTimingStore.web.ts:11 | capture read → null | FINE | cache |
| packages/data/src/spots/visits.ts:178,185,206 | panel / weather / reading → null ("a visit must save") | ASK Q7 | |
| packages/data/src/spots/capture.ts:91,165 | panel → null ("a spot must save") | ASK Q7 | |
| packages/data/src/spots/weather.ts:74,114,193 | Open-Meteo fails → null ("we do not know yet") | ASK Q7 | |
| packages/data/src/spots/leadup.ts:57,70,106,114,134 | lead-up reads → null / not carried | ASK Q7 | "not read" = asked again on refresh (T314) |
| packages/data/src/spots/store.web.ts:26 | `exists` throws → false | RETHROW | storage refused reads as "no spots file" → empty list, no error |
| packages/data/src/spots/store.web.ts:133, store.native.ts:134 | key store read → null | FINE | preference |
| packages/data/src/spots/photoStore.web.ts:37 | IndexedDB open un-cache | FINE | X4 |
| packages/data/src/spots/photoStore.web.ts:106 | downscale fails → original blob | FINE | the photo is kept, larger |
| packages/data/src/spots/core.ts:61 | photo remove fails | FINE | an orphan, not a lost record |
| packages/data/src/spots/core.ts:66,70 | torn write → old file; corrupt → `SpotsCorrupt` | FINE | recovers / rethrows |
| packages/data/src/spots/dayFlow.ts:64 | HYDAT un-cache | FINE | X4 |
| packages/data/src/spots/dayFlow.ts:106,121 | ANY error → `{kind:"offline"}` | ASK Q8 | |
| packages/data/src/spots/dayFlow.ts:108 | year series → null (no envelope) | S+R | failure reads as "no record" |
| packages/data/src/spots/dayFlow.ts:326,332 | live fill → stays pending | FINE | retried by the next refresh |
| packages/data/src/spots/queue.ts:13 | queue tail swallow | FINE | caller holds `run` |

## ui

| file:line | what it does | class | why / note |
|---|---|---|---|
| packages/ui/src/async.ts:94-101 | `useAsync` failure → state only; failed refresh keeps old value | S+R | X2 — the one place to report |
| packages/ui/src/openWater.ts:129 | group lookup fails → opened without its group | S+R | pager silently missing |
| packages/ui/src/openWater.ts:145 | solo check fails → map instead of regs | S+R | wrong pane, no word |
| packages/ui/src/inseason.ts:38 | `source.latest()` rejects → nothing | S+R | with data/feed/inseason.ts:42 |
| packages/ui/src/inseason.ts:94,114 | seen-list store | FINE | preference |
| packages/ui/src/visits.ts:104 | save fails → `setError` | S+R | shown; add report |
| packages/ui/src/imagery.ts:77 | probe throws → "failed" | S+R | shown (imagery state); report once with ui-native/imageryProbe.ts:33 (Q1) |
| packages/ui/src/photos.ts:51 | orphan remove | FINE | orphan |
| packages/ui/src/photos.ts:74 | add fails → `setError` | S+R | shown; add report |
| packages/ui/src/photos.ts:115 | photo uri fails → null (blank tile) | S+R | |
| packages/ui/src/search.ts:136 | near read fails → console.error + rethrow | S+R | shown (`nearFailed`); add report |
| packages/ui/src/prefetch.ts:63,144,164 | perf marks / battery API | FINE | instrumentation / capability |
| packages/ui/src/layerChoice.ts:25,30 | pref read/write | FINE | preference |
| packages/ui/src/regs.ts:61,76,79 | prewarm `.catch` | FINE | warming; the real read shows |
| packages/ui/src/regs.ts:164 | access file fails → gentle note dropped, console.warn | ASK Q4 | |
| packages/ui/src/regs.ts:219 | method file fails → salmon card gear marks empty | S+R | rule content silently missing |
| packages/ui/src/regs.ts:316 | page prefetch `.catch` | FINE | prefetch |
| packages/ui/src/regs.ts:348 | probe console.log | FINE | probe |
| packages/ui/src/capture.ts:88,99 | spot save fails → `setError` | S+R | shown; add report |
| packages/ui/src/errorLog.ts:25,56 | log guards | FINE | logging never breaks logging |
| packages/ui/src/chart.ts:254,256 | series / reading → null | S+R | a failed chart reads as no data |
| packages/ui/src/chart.ts:258 | `void Promise.all` | FINE | members already caught |
| packages/ui/src/net.ts:73 | data setting write | FINE | preference |
| packages/ui/src/spots.ts:40 | write fails → `setError` + rethrow | S+R | shown; add report |
| packages/ui/src/spots.ts:55 | one spot's fill fails → kept | FINE | T314: asked again next refresh |
| packages/ui/src/spots.ts:95 | retry runner `.catch` | FINE | write() already shows |
| packages/ui/src/boot.ts:74 | start fails → console.error + boot-failed screen | S+R | shown; add report |
| packages/ui/src/crashReport.ts:133,161,164,181,188,236,243,255,258,269,278,284,383,386 | the reporter's own guards | FINE | reporting cannot report itself |
| packages/ui/src/recentSearches.ts:16,20 | pref | FINE | preference |
| packages/ui/src/release.ts:29 | un-cache | FINE | X4 |
| packages/ui/src/release.ts:102 | tile sidecar unreadable → null version | ASK Q5 | |
| packages/ui/src/runTiming.ts:72,81,87 | species pref | FINE | preference |
| packages/ui/src/mapRail.ts:218 | permission read fails → "unavailable" | FINE | shown as a state |
| packages/ui/src/mapRail.ts:254 | locate fails → `noLocation` | S+R | shown (Q1) |
| packages/ui/src/dayFlow.ts:58 | any error → "offline" | ASK Q8 | |
| packages/ui/src/place.ts:98 | `onMissing` → reportFailure (systemBack.web.ts:45) | FINE | already reported |
| packages/ui/src/sync.ts:145,148 | feed index / release ask → null stamps | FINE | the update service shows these failures (SyncErrorSheet) |
| packages/ui/src/sync.ts:158 | `void updates.check` | FINE | never rejects |
| packages/ui/src/panel/feed.ts:54 | last report → undefined | S+R | a failure reads as "no report" |
| packages/ui/src/hooks/water.ts:82,88 | un-cache / warm | FINE | X4 |
| packages/ui/src/hooks/water.ts:131 | search index fails → exact/substring fallback | ASK Q3 | |
| packages/ui/src/hooks/facts.ts:41,42 | counts / feed index → null | S+R | bundle counts failing read as blank |
| packages/ui/src/offline.ts:37 | regsKept fails → nothing | S+R | low |
| packages/ui/src/likeVisit.ts:161 | `void fetchLike` | FINE | fetchLike catches inside |
| packages/ui/src/waterMap.ts:324-332 | `waterMapStartIx`: unlisted part → 0 silently (known, T345) | S+R | |

## ui-native

| file:line | what it does | class | why / note |
|---|---|---|---|
| packages/ui-native/src/Shell.tsx:88,91 | tour gauge basin → km2 1000 / null | FINE | tour camera nicety |
| packages/ui-native/src/Shell.tsx:111 | guide water search fails → no guide water | FINE | tour degrades; search shows its own |
| packages/ui-native/src/Shell.tsx:359 | waterPoint fails → no pin | FINE | spot still starts |
| packages/ui-native/src/Shell.tsx:63-69 (+BrowseHost:26-28, WaterModeHost:20-22, ShellSheets:20) | lazy `import("./later")` | FINE | already S+R via part.tsx:41 (known item, done) |
| packages/ui-native/src/LeadUpStrip.tsx:87, CoachMarks.tsx:99, useAnchors.ts:138 | layout/measure guards | FINE | not laid out yet |
| packages/ui-native/src/chartInput.web.ts:57, motion.ts:43, regs/RegsScreen.tsx:155 | capability probes | FINE | capability |
| packages/ui-native/src/netInfoConnectivity.ts:49,59,72 | NetInfo missing → never metered | FINE | capability |
| packages/ui-native/src/imageryProbe.ts:33 | probe fails twice → console.error, false | S+R | shown (imagery); Q1 |
| packages/ui-native/src/Walkthrough.tsx:194,200,210,426,430 | seen-state store | FINE | preference |
| packages/ui-native/src/FirstRunNotice.tsx:25,28 | accepted store | FINE | preference |
| packages/ui-native/src/part.tsx:41,43,55 | chunk load → reportFailure + "Try again" | FINE | already S+R |
| packages/ui-native/src/shell/BrowseHost.tsx:257 | map `onError` → console.error("map:") | ASK Q2 | |
| packages/ui-native/src/shell/WaterModeHost.tsx:65 | map `onError` → console.error("map:") | ASK Q2 | |
| packages/ui-native/src/shell/useShellNav.ts:85 | prewarm `.catch` | FINE | warming |
| packages/ui-native/src/shell/SpotHost.tsx:62,141,148 | save/delete fails → `setSpotError` | S+R | shown; add report |
| packages/ui-native/src/shell/useShellSearch.ts:97,113 | `void source.waterBox(id).then` (no catch) | S+R | X1: silent on a phone; the search arrival just doesn't frame |
| packages/ui-native/src/regs/synopsisPdf.native.ts:19,53,57,60 | held check / retry / alternate | FINE | bounded retry, thrown |
| packages/ui-native/src/regs/synopsisPdf.web.ts:44,55 | label write / close + rethrow | FINE | rethrows |
| packages/ui-native/src/regs/SynopsisLoading.tsx:62 | retry `.catch` | FINE | openBook shows + reports |
| packages/ui-native/src/regs/PdfViewer.native.tsx:58 | warn + reportFailure + failPdf | FINE | already S+R |
| packages/ui-native/src/regs/openBook.ts:24 | gov.bc.ca `Linking.openURL` fails → console.warn only | S+R | the tap does nothing |
| packages/ui-native/src/regs/openBook.ts:35 | highlighted open → warn + report + card | FINE | already S+R |
| packages/ui-native/src/menu/ReportPage.tsx:57 | share fails → `noShare` | S+R | shown (Q1) |
| packages/ui-native/src/menu/UpdatesLogPage.tsx:108 | copy → `copyFailed` | S+R | shown via App.tsx:403 (Q1) |
| `void Linking.openURL` (InSeasonNotice:86, WebSpotsNotice:41, InSeasonBanner:34, Licences:13, InfoPages:16, MoreScreen:182,430, ReportPage:53,149, SynopsisLoading:64, Shell:302) | external link opens, no catch | S+R | X1: a failed open does nothing, silently on a phone; one shared `openLink` |

## map

| file:line | what it does | class | why / note |
|---|---|---|---|
| packages/map/src/Map.native.tsx:144,149,175-177,248,284,301,302,337,356,375,510 | feature-state / data / visible writes `.catch(report)` → `onError` | ASK Q2 | ends at console.error |
| packages/map/src/Map.native.tsx:354 | atlas getFeatureState → null → `{}` | FINE | copy starts clean, mirrored after |
| packages/map/src/Map.native.tsx:461,463,467 | test-bed target finder | FINE | probe |
| packages/map/src/Map.native.tsx:492 | mark query → [] (tap falls through to the water) | ASK Q2 | |
| packages/map/src/Map.native.tsx:540,541 | project/unproject → null | FINE | label placement |
| packages/map/src/adapters/native.ts:64,74 | setFeatureState `.catch(fail)` | ASK Q2 | |
| packages/map/src/look.ts:38,48,60 | applyView / mode / group → report | ASK Q2 | |
| packages/map/src/native/useColouring.ts:106,133,164 | state writes `.catch(report)` | ASK Q2 | |
| packages/map/src/native/useColouring.ts:147,163 | nativeTag before mount | FINE | not mounted |
| packages/map/src/web/useWaterCopy.ts:87, web/useOverlays.ts:94 | → `onError` | ASK Q2 | |
| packages/map/src/web/useInput.ts:135 | a tap's handler fails → `onError` | S+R | the reader tapped and nothing opened |
| packages/map/src/web/useVisible.ts:49, web/markers.ts:104 | `void` without catch | FINE | web: unhandledrejection reports it |
| packages/map/src/gaugeDots.ts:28 | gauge GeoJSON unparsable → empty collection | S+R | gauge dots vanish with no word |
| packages/map/src/trace-geometry.ts:334 | tile fails → empty shapes, **cached** | S+R | trace stays short all session; also un-cache |
| packages/map/src/web/outsideMask.ts:28 | outside-BC mask fetch fails → no mask | ASK Q2 | cosmetic |
| packages/map/src/tile-ids.web.ts:33 | VT layers unreadable → null | FINE | next tile event retries |
| packages/map/src/native/probe.ts:28,48, native/panProbe.ts:34 | test-bed probes | FINE | probe |

## apps/mobile

| file:line | what it does | class | why / note |
|---|---|---|---|
| apps/mobile/index.js:34,42 | stall / heap probe logs | FINE | test-bed switch |
| apps/mobile/version-stamp.js:17 | `git rev-parse` fails → "" | FINE | build-time tool |
| apps/mobile/src/splash.ts:19 | preventAutoHide fails | FINE | splash hides early, nothing lost |
| apps/mobile/src/App.tsx:104 | install id not stored → console.warn | FINE | a per-run id still sent |
| apps/mobile/src/App.tsx:113 | `void reporter.flush().then(reportPastExits)` | FINE | the reporter's own path |
| apps/mobile/src/App.tsx:213,308 | `void feed.syncObservations?.()` | FINE | catches inside (http.ts:344) |
| apps/mobile/src/App.tsx:348,368 | swap probe logs | FINE | probe |
| apps/mobile/src/App.tsx:391 | cache delete → false | FINE | outcome written to the Updates log |
| apps/mobile/src/App.tsx:403 | copy/share → false | S+R | shown (copyFailed); Q1 |
| apps/mobile/src/App.tsx:431 | regs store fails → `routeLinks(null,null)` silently | S+R | the same failure shows on a water page; report here |
| apps/mobile/src/App.tsx:436 | start link unresolved → reportFailure + "place-gone" | FINE | already S+R |
| apps/mobile/src/App.tsx:454 | spots refresh catch | FINE | shown by spotsHook.error; report lives in ui/spots.ts:40 |
| apps/mobile/src/App.tsx:441, memoryTrim.ts:17-21 | trim failure → reportFailure | FINE | already reported |
| apps/mobile/src/startLink.web.ts:40 | bad start link → console.error, plain map | ASK Q9 | |
| apps/mobile/src/locate.web.ts:20 | no Permissions API → "prompt" | FINE | capability |
| apps/mobile/src/locate.native.ts:20 | permission read throws → "unavailable" | S+R | shown as a state; Q1 |
| apps/mobile/src/envStore.web.ts:5,6,12,13, envStore.native.ts:13,15,22,24 | env / channel prefs | FINE | preference |
| apps/mobile/src/waterSlugs.ts:25 | slug shard fails → null (cached for the session) | FINE | URL cosmetics; the water still opens |
| apps/mobile/src/systemBack.web.ts:45,84 | reportFailure once / slug then | FINE | already reported / slugOf catches |

## Counts (rows above; a grouped row counts once)

- **RETHROW: 2** (release/source.ts:519, spots/store.web.ts:26)
- **S+R: 39** (about half already shown and only need the report; 8 of them also hang on Q1)
- **FINE: 124** (preferences/caches, X4 un-cache, bounded retries, typed rethrows, probes, already reported)
- **ASK: 20 rows** under 9 questions (Q2 alone covers ~25 call sites in packages/map)

## ASK the owner

1. **Q1 — environmental failures.** Offline, no GPS fix, location denied, a share sheet that fails, a satellite tile
   down: shown to the reader today. Report them too, or report only failures that are not the network / the device
   saying no? (Proposal: shown always, reported only when not offline-class — else the crash inbox fills with offline.)
2. **Q2 — map engine errors** (~16 sites in packages/map: feature-state / setData / applyView / mark query / outside
   mask) end at `onError → console.error("map:")`, no face. Report only (the map redraws next frame), or also a face?
3. **Q3 — search fallback** (ui/src/hooks/water.ts:131): an unreadable search index silently falls back to the
   source's exact/substring search. Keep the fallback (and report), or fail the search with the one failure face?
4. **Q4 — access gentle notes** (ui/src/regs.ts:164): access file unreadable → the gentle note is dropped (console.warn);
   the no-access banner still shows. Say "access notes could not be read" on the page, or report only?
5. **Q5 — tile version** (ui/src/release.ts:102): an unreadable tile sidecar reads as "no version", which skips the
   tiles/bundle vintage check. Treat it as a failure (say + report), or keep null = unknown?
6. **Q6 — today in BC** (core/src/day.ts:103): without Intl zone data the device clock decides the day (open/closed
   dates). Keep silent, or report once per run?
7. **Q7 — "a spot must save"** (data/spots capture.ts, visits.ts, weather.ts, leadup.ts): estimate / weather / gauge
   failures are recorded as "not known" so the save succeeds. Keep, and report only the non-offline failures?
8. **Q8 — day flow says "offline" for any error** (data/spots/dayFlow.ts:106,121; ui/src/dayFlow.ts:58). Split offline
   from a real failure (shown as failed + reported)?
9. **Q9 — bad start link on the web** (apps/mobile/src/startLink.web.ts:40): console.error, opens the plain map. Say
   "this link could not be opened" (like place-gone) + report?

Not a question, a proposal for step 2: **X1** (install Hermes's promise-rejection tracker so phone rejections are
reported) and **X2** (report in `useAsync`).

## Failure faces today (for step 3 "one failure face")

1. **`ErrorState`** (ui-native/src/ErrorState.tsx, T257: red alert line + optional "Try again" link) — WaterScreen.tsx:215
   (sheet failed / no such water), :278 (regs unavailable), SpotsScreen.tsx:50,65, SpotScreen.tsx:229, SpotCapture.tsx:231,
   spot/SpotPhotos.tsx:72, menu/ReportPage.tsx:146,160, part.tsx:63 (part failed + Try again), App.tsx:539 (boot failed).
   Already the closest thing to the one face; `tools/one-button.test.ts` refuses retyped copies.
2. **`NetWaiting` / `PartPending` / `RegsWaiting`** (ui-native/src/regs/NetWaiting.tsx:53-63) — `partLine`
   "X: couldn't load" + Try again (core/net.ts:49-53); its own text, not ErrorState.
3. **`Note tone="closed"`** — menu/GlossaryPage.tsx:47 ("The glossary could not be read: …").
4. **Search rows note** — search/rows.tsx:31 (`nearFailed`); search failed words (core/search.ts:258, ui/search.ts:364).
5. **Sync badge + `SyncErrorSheet`** — Shell.tsx:545 (bell count), :637 (sheet with Retry); `SyncFailures` reused in
   InSeasonNotice.tsx; More → Data rows "could not update X" (core/more.ts:216-274).
6. **Synopsis card** — regs/SynopsisLoading.tsx:52-64 (failed + Retry + Close + gov.bc.ca); PdfViewer `failPdf`
   (regs/pdfView.ts).
7. **Map legend runs line** — shell/MapLegend.tsx:255 (`RUN_WORDS.failed` / `stale`).
8. **Water map status** — water/WaterMapView.tsx:68 (`WATER_MAP_WORDS.unavailable`).
9. **Map rail error** — shell/BrowseHost.tsx:143 (`rail.error`: noFix / noLocation, core/mapRail.ts:28,48).
10. **Imagery down** — ui/imagery.ts state → MapScreen `onImageryError` (satellite basemap state, no text line).
11. **Boot** — App.tsx:534-539 boot-failed page (ErrorState + install words core/install.ts:7-12), App.tsx:557
    `rolledBackLine` banner, `applyError` (ui/boot.ts:76).
12. **Updates log** — menu/UpdatesLogPage.tsx:112 (`copyFailed`), :134 (`row.problem` per event).
13. **Report page** — `queuedRetry` / refused / noShare (core/menu/words.ts:171; ReportPage.tsx).
14. **In-content notes** — Conditions "N of these gauges could not be placed" (core/conditions.ts:598); salmon stale
    regions (core/salmonFeed.ts:224); day flow "offline" phase (ui/dayFlow.ts); like-visit offline flag.

Six different shapes (ErrorState, partLine, Note, search note, sheet, inline words) for the same "what went wrong +
Retry"; 1 and 2 already carry Retry and could merge first.

## Owner answers (2026-11-04)
1. Environment (offline, no GPS, permission refused, share sheet dismissed): NOT reported ("we expect the app to be
   offline a lot"); shown where it matters.
2. Map engine errors: report; most need no face (show only what the reader can act on).
3. Search index unreadable: NO fallback to exact/substring — show the search as failed (error face) and report.
4. Access notes unreadable: no silent drop — report it (no fallback).
5. Tile sidecar unreadable: a FAILURE (cannot tell the tiles match), reported.
6. No Intl zone data: keep BC time for "today" (regs days are BC days); report once where the device lacks the zone
   (owner asked "shouldn't we always use phone clock" — answered: no, an Alberta reader at 23:30 would get tomorrow).
7. Spots: offline fallbacks are acceptable (save offline, resync on reconnect); not reported.
8. Day flow: separate real failures from offline; OFFLINE IS THE ONLY STATE WITH ACCEPTABLE FALLBACKS.
9. Bad web start link: opening the plain map is acceptable.
X1 (phone never reports unhandled rejections): fix, part of T368.
