# T826 — iOS: the Conditions panel and map differ from Android; the tap highlight stays after going back

Status: DONE (f7b41c208; landed 2026-11-17 in land/app-1117: bar listener + highlight unset; Chehalis is data (08MG001 silent); owner re-tests on the 0.7.12 build).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T826 | iOS: the Conditions panel and map differ from Android (no gauge markers, no bar, Chehalis empty); the tap highlight stays after going back | DONE | f7b41c208 | owner iPhone build 36, 2026-11-16 |

Source: owner 2026-11-16 ~21:50, in the app chat, on iPhone TestFlight build 36, verbatim: "also the condition panel is
very glitchy on apple. it doesnt show the info the same as android the mao doesnt have gauge markers the current
conditions view doesnt show the bar and when i click on chehalis it doesnt show conditions for it? also the highlights
dont let go when you go back the highlight of where you clicked stays and doesnt disspear".

## The five parts
1. The Conditions panel is glitchy on iOS and does not show the info the way Android does.
2. The Conditions map has no gauge markers on iOS.
3. The current-conditions view does not show its bar.
4. Tapping the Chehalis shows no conditions for it.
5. After going back, the highlight of the tapped water stays on the map instead of clearing.
Build 36 is older than the head (made from ios/next on 11-15); each part is first checked on the head on the iOS
simulator, then fixed in the shared code, with Android and the web unchanged.

## Work notes, 2026-11-17 00:45 (app-fix/t826-ios-conditions)
Checked on the iPhone 17 simulator (iOS 26.5, staging, release r-46e4355a7277) on three apps: build 36's code
(d658f88ac), the head (c25823a89) and the branch (fc58204bf). The walk is `node app/tools/testbed/run-ios.mjs conditions`
(`scenarios-ios/conditions.mjs`). Shots: the lane's scratchpad `t826-shots/` (names start with the part).
1. **The panel's info.** Not an iOS fault on the head: the Conditions view draws the same card as Android (estimate with
   its bar, the gauge card, reading, chart) where there is an estimate (Vedder). The Chehalis today shows NO ESTIMATE
   (see 4). Found on the way, phones only, not iOS: the route map in the Conditions view draws no gauge dots and no "you
   are here" pin, though its key names them: `Map.native.tsx` ignores `trace` (`trace: _noTrace`, since T621); the web
   draws them (`trace-overlay.ts`). Android has the same gap; a ticket if the owner wants it.
2. **Gauge markers on the Conditions map.** Present on build 36 and on the head (the dots, and the ring of a level-only
   gauge). Not reproduced. If the owner meant the route map in a water's Conditions view, that is the gap in 1.
3. **The bar.** On the head the ramp under the Conditions map came with no marks and no count after a switch from the
   Map tab (web: "30 reaches coloured" and the ticks). Cause: the native map re-asked what is on screen only when
   SOMEONE started listening; the Map tab zoomed in listens too (its read-ahead), so the legend's listener was never
   told until a pan. Fixed in `Map.native.tsx` (keyed on the listener), fc58204bf; test in adapter-contract.
4. **The Chehalis.** Data, not the app. The match holds (08MG001 Chehalis River Near Harrison Mills leads the Chehalis
   sections, 634 per the main chat); the gauge has sent nothing since 2026-11-15 13:35 PST (staging feed fetched
   2026-11-17T07:05Z: `observedAt` 2026-11-15T13:35-08:00, percentile null; ECCC's CSV ends there too, per the app
   chat). The panel says so the same on build 36, head and branch: "NO ESTIMATE · The gauges here report water level
   only today, not flow. Show Level to see their reading." · "No gauge on this water reporting · 2 not reporting or not
   ranked" · "Not reporting: Chehalis River Near Harrison Mills (last 15 Nov, 14:35 PT)", and the gauge card falls to
   Harrison River at Harrison Mills (level only). Android renders the same shared component from the same view model;
   not run on the emulator in this lane. Flow ↔ Level jumping to Harrison is T817 (other lane).
5. **The highlight after back.** Reproduced on the head: after ‹ the Chehalis kept its outline on the Conditions map.
   Cause: React Native's iOS TurboModule bridge drops a null-valued key (`convertJSIObjectToNSDictionary`;
   `enableModuleArgumentNSNullConversionIOS` off in RN 0.88), so the phone's unset write `{ selected: null }` (T345)
   reached MapLibre as `{}`. Fixed by writing `false` for the highlight's boolean keys (`sections.ts` `unsetValue`),
   427e661ad. ‹ and the edge swipe call the same `back()`; the walk's left-edge swipe did not go back over a water's
   views on the simulator (the maps and the view swipe take it), so the proof is by ‹.
Left, found, not fixed (same bridge, iOS only): the colouring engine's clears are also null writes (`StateQueue.clear`,
and `standing: null` for a class with nothing to say), so on iOS a section can keep an old standing colour after a
horizon change or a feed refresh until relabelled. Unset needs a value the paint reads as missing (the standing paint
tests `== null`), a style change; not done here.
Overlap: none with T814/T815/T816/T821 (app-fix/ios-1116) or T817 (app-fix/t817-level-only).
