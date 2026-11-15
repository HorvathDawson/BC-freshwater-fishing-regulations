# iOS integration branch, 2026-11-14 (`ios/integration`)

Not merged into `redesign/stream-sections`. Base: `origin/redesign/stream-sections` 053480a7e.

## Merges (`git merge --no-ff`, in this order)
| branch | head | conflicts | resolution |
|---|---|---|---|
| `ios/t722-install` | b2c4d51af | T722 ticket (Owner answers vs Code half) | both sections kept, base's first |
| `ios/t723-keyboard-back` | 97f3523ca | `run-ios.mjs` (imports, SCENARIOS), T723 ticket | both scenario sets kept; base's T734 section, then the branch's |
| `ios/audit-fixes` | e7b3df8a8 | `appUpdate.test.ts` import, `run-ios.mjs`, T723 ticket | the branch's import (`ChangelogEntry` used); all scenarios; both ticket sections |
| `ios/t719-tapup` | d6afcf124 | none | patch check below |

Auto-merged and read: `App.tsx` (t722 noBackup / space line / iOS words + audit-fixes G2 / link sink: both kept), `app.json`,
`changelog.ts`, `deps.md`. Conflict markers grepped across the tree before each commit: none.

t719 patch check: `app/patches/@maplibre__maplibre-react-native@11.5.0.patch` split per file and compared with the base's:
every Android/Kotlin/Java and JS/TS section byte-identical; only `ios/components/map-view/MLRNMapView.{h,m}` and
`MLRNMapViewComponentView.mm` differ. Its JS changes (`Map.native.tsx`, `tapLift.ts`, `BrowseHost.tsx`, `regs-open.mjs`)
are comments only. `pnpm -C app install --frozen-lockfile`: exit 0; `MLRNTapUpRecognizer` present in the installed binding.

## Step 1: no simulator
- `pnpm -C app install --frozen-lockfile`: exit 0.
- `pnpm -C app check:fast` (unfiltered): exit 0 (after the three merges, and again after t719).
- Whole app vitest (`pnpm -C app test` = `vitest run`, as `pnpm check` in app CI), after three merges: **6 failed** / 7701
  passed / 27 skipped, 650 files. Fixed at the cause in 08f892abf:
  - `tools/back-layers.test.ts` (2): `ModalBack` (**t723-keyboard-back**) is the edge strip inside a Modal that registers;
    listed in COVERED with why, and in the "found today" list.
  - `tools/failures.test.ts`: `mapRail.ts` FINE 2 -> 1 (**audit-fixes** G6 now reports the locate failure: one fewer
    silent catch, the allowance tightened).
  - `tools/no-silent-skips.test.ts`: the new iOS scenarios' named SKIPs listed with counts (**t723-keyboard-back**
    keyboard 1, modal-back 2; **audit-fixes** 4).
  - `tools/testbed-static.test.ts`: audit-fixes scenario tapped a stale label ("Open this app's settings", which the app
    no longer has): dropped (**audit-fixes**); "Audit spot" / "kb probe" are titles the scenarios type themselves:
    allowed with why.
  - `packages/ui/src/prefetch.render.test.tsx`: red on the BASE too (T737, a43e750b9, changed `runPrefetch` to fetch core
    ahead then open it; this test was not updated). Test now asserts fetch, then open after the fetch resolves. **Belongs
    to the base / T737**, not to an iOS branch.
- Feeds worker suite (`npx vitest run --root .` in `app/workers/feeds`): 447 passed.
- After the t719 merge (head below): `install --frozen-lockfile` exit 0, `check:fast` exit 0, whole vitest **exit 0: 645
  files passed, 5 skipped; 7707 tests passed, 27 skipped, 0 failed**.

## Later commits
- 90eb55685 audit G12 (`expo-notifications` `enableRemoteNotifications: false`): `expo prebuild -p ios` showed the
  entitlements without `aps-environment`, associated-domains kept; `expo config --type introspect`: Android modResults
  identical. Then **HELD by the main iOS chat** (the owner is weighing real push): reverted with a plain `git revert`
  (3212f4936). Tree at 3212f4936 = tree at 107207143 (the iOS simulator build below), plus these notes.

## Step 2: app CI on the branch
- Run 43674353270 (107207143): **failed**, 2 tests: `WaterMode.test.tsx` ("▲ on the Vedder opens the Chilliwack's
  map…", expected false to be true) and `WaterScreen.unnamed.test.tsx` (timed out at 30 s). Both pass locally alone and
  in the whole local suite; no merged branch touches them. Re-run of the failed job was cancelled by the next push.
- Run 43676645443 (90eb55685, G12): superseded.
- Run 43677144021 (3212f4936, same tree as 107207143): **success**, every step (pnpm check, feeds worker, web bundle,
  native bundles). So the two reds read as CI-load flakes, not fixed; worth watching.

## Step 3: iOS simulator (build of 107207143 `--testbed`, same app code as the head; device integ-17, iPhone 17, iOS 26.5)
Build: `build-ios-sim.sh --testbed` via heavy.sh lane 2, 146 s, signed to run locally.

### Session 1 (fresh install, lane 3, 21:46-22:31, cap 45 min; another lane's simulator session ios-shared3 ran at the start)
| scenario / step | result | baseline (T718) | note |
|---|---|---|---|
| first-open: up | false PASS "67 s" | PASS 397 s | its own shot shows the download screen (38.8 of 479.3 MB). **Test-bed fault, fixed in 51349c3bf** (appUp needs the tabs on two looks and no install screen). Why one tree read held "Map"/"More" is unproven (guess: cross-talk with the other simulator's Maestro session) |
| first-open: About says staging | FAIL | PASS | "More" not found: the app was still downloading |
| mid-download shot | the bar moving, "93.6 of 479.3 MB downloaded", iOS line "You can leave the app or lock the phone… Swiping the app away … stops it" | | `scratchpad/mid-download.png` (not committed) |
| download speed | ~0.5-0.8 MB/s on the simulator | 1.7 MB/s | the owner's line was bad tonight (dropped connections; simulator DNS errors for clients3.google.com). Not judged as an app fault |
| tap-water: search Cultus → page | FAIL (app not up in 180 s, still installing) | PASS | consequence of the slow install |
| tap-water: edge-back | PASS (50 % / 400 ms went back) | PASS | |
| tap-water: map tap on the lake → page | PASS | PASS | |
| offline (3 steps) | 3/3 PASS | 2 of 3 | step 1 now passes |
| satellite | PASS (light grey 9.4 % → 5.6 %) | PASS (10.6 → 5.6) | |
| walkthrough | started (1 of 41), cut by the cap | 41/41 | re-run in session 2 |
| pdf | not reached | PASS | session 2 |

### Session 2 (not fresh, lane 2, 22:44-23:29, cap 45 min; functional only, other lanes ran jobs meanwhile)
| scenario / step | result | branch alone / baseline | note |
|---|---|---|---|
| walkthrough | PASS 41 of 41 | 41/41 (T718) | |
| pdf | PASS "Page 11 of 88" in PDFKit | PASS (T718) | |
| keyboard: software keyboard on | PASS | | |
| keyboard: report field | PASS, field 414-523 pt, keyboard top 540 → clear | clear (T723 after) | |
| keyboard: glossary search | PASS, 173-215 pt → clear | clear | |
| keyboard: notifications gauge search + pick | PASS, 497-538 pt → clear; one tap picked the gauge | clear | |
| keyboard: alert value (decimal pad) | PASS, 472-513 pt, pad top 566 → clear; Done key present, closes the pad | as T723 | |
| keyboard: spot title and notes | PASS, both clear; Save spot (774 pt) under the keyboard, not tappable with it up | same as T723 (the known "Save row" item, owner decision 15) | |
| keyboard: Conditions → Alert me | PASS, "no keyboard drawn" | T723: not proven | as on the branch |
| modal-back, audit-fixes | not reached (cap) | | session 3 |

## Step 4: Android (test-bed APK of 3212f4936, patched MapLibre AAR fslean 13.6.1; emulator n1_pixel)
APK built by `session.sh --build` through heavy.sh lane 2. Later sessions reuse it with `--allow-stale` (HEAD 51349c3bf differs only
in `app/tools/testbed/run-ios.mjs`).

### Run 1 (lane 2, 21:51-22:32; regs-open overlapped a TestFlight archive from 22:07, load avg 13.3 at 22:08)
- spots: **FAIL, test bed**: setup "network did not reach WIFI" (the emulator's Wi-Fi stayed CONNECTING, only 3G), then the
  quick-settings shade stayed open over the app and every read came back empty. Not an app result.
- bg-install: its first step re-pointed the STAGING `test/` channel to the live release (`test/ = r-f749475d0962`; the same
  release the channel already named when the run began, as far as the earlier runs read it). Then FAIL on every step (the same
  broken Wi-Fi: "no progress on Wi-Fi in 120 s"). Test bed. Later runs use `--no-r2` (no bucket writes).
- regs-open (timings taken under load: NOT counted; quiet re-run below). Android words on the first-install screen unchanged:
  "You can close the app: the download carries on." (seen in run 2's screenshot).

### Run 2 (lane 1, 23:46-23:52): stopped by me
`run.mjs` runs scenarios in its own list order (spots before bg-install), so spots started on an app still in the first install
left by run 1 (203 of 479.3 MB on screen): invalid. Wi-Fi this boot: CONNECTED.

### Run 3 (lane 1, 23:54-00:10, `--no-r2`, bg-install alone; load avg 13.4 from other lanes): stopped by me
- `test/` channel check (no write): `test/ = r-f749475d0962` before and after; nothing written to the bucket.
- Android 13 asked "Allow Can I Fish? to send you notifications?" on the clean app. **That is expected**: bg-install answers
  it on purpose (the system download's notification), and its own `expand-notifications` is the open shade seen in run 1.
- The scenario's `tap-text "Allow"` flows started and ran **no command at all** (empty Maestro logs, no error). The same
  failure as the "Maestro Android driver did not start" in T734, and the cause of the empty screen reads ("saw: ") in
  runs 1 and 2. So the download never started and the remaining steps could only time out. **Test bed, not the app.**
- Wi-Fi on this boot: CONNECTED (in run 1 it stayed CONNECTING: not seen again).
- The Android first-install words are unchanged (run 2's screenshot): "You can close the app: the download carries on."

## Summary tables (state at 00:17; the timed rows wait for the quiet window)

### iOS (integ-17, iPhone 17, iOS 26.5; build of 107207143 = the head's app code)
| scenario | integration | baseline T718 / branch alone | verdict |
|---|---|---|---|
| first-open | false PASS, then About step FAIL (still downloading) | PASS 397 s | test bed (fixed 51349c3bf); slow line tonight; mid-download bar moving (93.6 of 479.3 MB) |
| tap-water | search → page FAIL (installing); edge-back PASS; map tap → page PASS | PASS | the FAIL follows the slow install |
| offline | 3/3 PASS | 2 of 3 | better |
| satellite | PASS 9.4 → 5.6 % | PASS 10.6 → 5.6 % | same |
| walkthrough | 41/41 | 41/41 | same |
| pdf | PASS Page 11 of 88 | PASS | same |
| keyboard | 7/7 PASS | t723: clear on every field | same as the branch |
| modal-back | session 3, running | t723: About/Layers/viewers close | pending |
| audit-fixes | session 3, running | 5 of 8 proven on the branch | pending |
| regs-open + t719 cue (≤ 150 ms every tap; double tap zooms, no page) | quiet window | T719 before: cue 371-495 ms; t719 branch: 8-121 ms | pending |
| tap-stalls (worst stall < 300 ms) | quiet window | T720: worst 227 ms | pending |

### Android (n1_pixel; test-bed APK of 3212f4936, patched MapLibre)
| scenario | integration | base (T716 round 2 / T734 build 32) | verdict |
|---|---|---|---|
| bg-install | runs 1 and 3: FAIL in the test bed | PASS | test bed: Maestro driver did not start under load (and Wi-Fi stuck CONNECTING in run 1); re-run in the quiet window |
| spots | runs 1 and 2: FAIL in the test bed | PASS | test bed (run 1: shade + Wi-Fi; run 2: started on an app mid-install); re-run in the quiet window |
| regs-open | run 1 under a TestFlight archive (load 13.3): Norrish cue 56/40/60 page 357/349/351; unnamed cue 39/**202/223** page 332/414/363; Coquitlam cue 49/72/75 page 343/350/355; Fraser cue 58/71/62 page 347/331/358; Skeena cue 131/67/77 page 586/375/373; Cultus cue 108/81/75 page 367/337/361; double tap PASS | cue 22-112, page 352-402 | not judged (taken under load); quiet re-run pending |
| tap-stalls | not run | cold start worst 125 ms, Run timing 262 ms | quiet window |

### Session 3 (not fresh, lane 2, 00:14-00:50; functional; other lanes ran jobs meanwhile)
| step | result | branch alone (T723) | note |
|---|---|---|---|
| modal-back: About | PASS, closed at 300 ms | closes | |
| modal-back: Help | PASS, closed | closes | |
| modal-back: Layers sheet | PASS, closed | closes | |
| modal-back: PDF viewer | PASS, closed | closes | |
| modal-back: photo viewer | FAIL "the modal is not up before the swipe" | closes (300 ms) | the step's own shot shows the viewer up ("Photo 1 of 1", ×). The scenario reads ids `photo-viewer`/`photo-image`, both in the app: **test-bed misread**, not judged as app |
| audit: arrow ↗ | PASS, U+FE0E present, crop 25x25 fill 0.16, colour gap 9 to the link text | PASS | |
| audit: ⚠ / ↩ titles | SKIP (not on this water) | SKIP | |
| audit: G7 location prompt | PASS, "…to centre the map on where you are and to pin a spot where you are standing…" | PASS | |
| audit: G6 Spots line + Open Settings | PASS | PASS | |
| audit: G6 map My-location line + Open Settings | PASS | PASS | |
| audit: G6 Open Settings opens Settings | PASS | PASS | |
| audit: G5 rough fix | SKIP (Settings gives no way to Precise Location) | SKIP | unit-tested |
| audit: G8 Directions → Apple Maps | PASS | PASS | |
| audit: G9 refused link | SKIP (no refusable link on the simulator) | SKIP | unit-tested |
| audit: G11 What's new | PASS, 18 rows, none says Google Play/Android/APK; 0.6.5, 0.6.4, 0.6.0 not listed on iOS | PASS (17 rows then) | |
| audit: G2 staged update | FAIL, apply lines 0 / 0 / 0 | not proven (0, 0, 0) | same as on the branch: no positive control, so neither proven nor refuted |

**Found on a screen (not from the merge):** the photo viewer's "Photo 1 of 1" label lies under the iOS status-bar clock (no
top safe-area inset in its header: `padding: 12` only, in the base too; t723 only wraps it in `ModalBack`). An iOS look
fix for a later ticket.

**Staging writes by scenarios (their own design):** audit-fixes G2 ran `deploy/staging/test-updates.mjs` reset → a
synthetic release → reset on the STAGING `test/` channel; Android run 1's bg-install ran `resetChannel`. `test/` read
after session 3 (`test-updates.mjs status`): `data_release r-f749475d0962`, gauges a live copy, i.e. the live release,
as before.
