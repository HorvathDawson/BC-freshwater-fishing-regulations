# T1212 — The phone map's controls as one set: the Today chip, no + / −, and the rest reworked (pictures first)

Status: IN PROGRESS — option A (Tidy) being built, lane T1212b (worktree wt-T1212b, branch
`feat/T1212-phone-map-controls-tidy`, on look A's branch); release 0.10.0 re-cut

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1212 | The phone map's controls as one set: the Today chip, no + / −, and the rest reworked (pictures first) | IN PROGRESS — 0.10.0 re-cut | 2c32f0b12, de0879941, b3f88f6ad, + review fixes | review approved; next: merge main after look A, the whole suite and check:fast, the PR; the Zoom buttons switch is T1214 |

Source: the owner, 2026-11-26, relayed by the planning chat, on T1200's round-3 pictures (07-day-selector, 08-other-ideas):
on the day selector ("still seem clunky") he picked "One \"Today\" chip that opens the calendar (Recommended)"; on the
+ / − buttons: "definitely drop +/- then we need to maybe rework all the rest of the controls?". His standing way for a
design call: a few WHOLE options, everything on one sheet, one question. Decided elsewhere and drawn here as given: look A
(T1208), towns in three tiers (T1209), the floating "Map key" with "Its own rules / Region rules only" (T1211). Kept: the
rotating search hints. Not proposed: the day in a water page's tab bar (withdrawn), a Today / Tomorrow row ("really
ugly", T367).

## The controls on the phone map today

Measured on the web app at 412 x 915 (light, Trout, Prince George at z6.3; `origin/main`, the key row from T1211's branch).

| control | size (px) | where | code |
|---|---|---|---|
| search field, with bell + refresh inside (phone; the website draws neither) | 384 x 46, two 40 px slots | top, 10 down, 14 in | `TopBar.tsx:94`, `barParts.tsx:22`, `barParts.tsx:139` |
| day bar ‹ 📅 Today › | 172 x 35 | top-left under the bar (y 68) | `DayCalendar.tsx:44`, `MapScreen.tsx:168` |
| + / − | 30 x 58 (two 28 px) | top-right (y 70). WEB ONLY: the phone app's main map draws none | `useMapInstance.ts:105`, `controls.css:43`, `MapRail.tsx:30` |
| rail: locate, layers | 46 x 90 (44 each) | right; y 144 on the web (under + / −), y 68 on the phone | `MapRail.tsx:23` |
| Trout / Salmon | 46 x 90 | under the rail (y 242) | `RegsLayerToggle.tsx:70`, `BrowseHost.tsx:171` |
| scale bar | up to 92 x 16 | bottom-left, 10 in, over the foot | `chrome.ts:77`, `Map.native.tsx:1091` |
| © credits | 28 x 28 | bottom-right, 8 in, over the foot | `MapCredits.tsx:94` |
| legend bar (going) | 412 x 66 | across the map's foot | `MapLegend.tsx:182`, `Shell.tsx:755` |
| Map key chip (T1211, being built) | 148 x 44 | bottom-left over the map | branch `feat/T1208-map-look-A` `shell/MapKey.tsx` |
| tab bar | 412 x 54, five tabs | screen foot | `TabBar.tsx:41` |
| Conditions: Now · Forecast ▾ · Change | 212 x 35 | top-left | `HorizonPicker.tsx:24` |
| Conditions: ramp + watershed days | 412 x 182 | across the foot | `MapLegend.tsx` (`LegendStrip`) |
| Conditions: "Gauge reports level only" | 199 x 34 | over the foot, left | `MapLegend.tsx:203` |

Flow · Level · Temperature is not on the map: it is the Layers sheet's Show (owner 2026-10-22, `MapLegend.tsx`). With a
water tapped, its page covers the map on a phone and none of these controls show; Back returns them as they were.

## What established phone maps do (what was read)

- One hand: Hoober's street study (1,333 people) found 49% one-handed, 36% cradled, 15% two-handed, and warns against
  putting things in "the hard to reach area in the upper-left corner"; he later says people touch the centre most
  (UXmatters, 2013-02, "How Do Users Really Hold Mobile Devices?", read).
- Zoom buttons: Google's Maps SDK for Android ships zoom controls OFF by default; its gestures are pinch, double tap in,
  two-finger tap out, and double-tap-hold-slide (developers.google.com/maps/documentation/android-sdk/controls, read via
  search summary). MapLibre Native's UiSettings has no zoom buttons at all; it has double tap and "quick zoom" (tap twice,
  hold, move) (maplibre.org UiSettings reference, read).
- What this app's maps already allow: the policy `MAP_GESTURES` (`core/src/gestures.ts:110`) turns on pan, pinch and
  double-tap zoom on both maps. Web (maplibre-gl 5.24.0, read in `node_modules`): double tap zooms in and a two-finger tap
  zooms out (`TapZoomHandler`, on with `doubleClickZoom: g.doubleTapZoom`, `map/src/gestures.ts:30`); one finger, double
  tap then drag, zooms both ways (`TapDragZoomHandler`: drag up out, down in; it is switched on with the pinch, by
  `touchZoomRotate: g.pinch`, `map/src/gestures.ts:32`, maplibre's `TwoFingersTouchZoomRotateHandler.enable`); keys + / −.
  Phone (MapLibre Native via `nativeMapGestures`, `map/src/gestures.ts:85`, spread at `native/maplibre/View.tsx:50`):
  double tap zooms in; double tap, hold and move zooms both ways (`doubleTapHoldZoom: g.doubleTapZoom`, MapLibre's "quick
  zoom"); a two-finger tap out is not in its documented settings and was not tried on a device.
- Accessibility: WCAG 2.2 SC 2.5.1 Pointer Gestures (Level A) uses exactly this case: a map that zooms by pinch needs a
  single-pointer way, "the map also includes plus/minus buttons"; double tap counts as single-pointer (w3.org Understanding
  2.5.1, read). So: one finger CAN zoom out on both (double tap, then drag), but only along a path, and 2.5.1 asks for a
  way without one; double tap only zooms IN. A tap-only zoom out needs a button: T1214 (owner). Phone-width web had + / −
  until this ticket, so there it is a step back (owner 2026-11-27: the switch, T1214). Proposed: a "Zoom buttons" switch (More → Display, off by default; on wide screens and with a mouse
  the buttons stay). Note: the phone app has had no + / − on its main map since the rail (`MapRail.tsx:30`), so the gap is
  already there on Android; the switch closes it.
- Material: one floating action button per screen for the most common action (m1.material.io, via a search summary);
  Android's FAB guide names centring a map on the user's location as a FAB use (developer.android.com, via a search
  summary). No web search result covered map control placement in Apple's HIG (the page did not load); not cited.

## The options (pictures sent to the owner in the chat: an index and one sheet each for Today, A, B, C)

All three: the one "Today" chip (calendar icon, the day, ▾; a press opens the date sheet; another day fills it purple),
no + / − on a phone, the floating Map key (T1211), the rotating search hints, bell + refresh in the search field. Map
height = from under the top controls to the tab bar (T1200's measure); "covered" = share of the map under a control.

| | A — Tidy | B — Thumb | C — Most map |
|---|---|---|---|
| top | search; Today chip + rail on one line under it | search alone | search alone |
| bottom | key chip | Today chip + key chip; rail and Trout / Salmon stacked bottom right over © | Today chip + key chip; ONE map button over © (opens locate, layers, Trout / Salmon) |
| taps | all 1 | all 1 | locate, layers, Trout / Salmon: 2 |
| hidden | nothing | nothing | locate, layers, Trout / Salmon behind the map button |
| map 412 x 915 | 749 (81.9%) | 805 (88.0%) | 805 (88.0%) |
| map 360 x 740 | 574 (77.6%) | 630 (85.1%) | 630 (85.1%) |
| covered (412) | 11.1% | 11.1% | 9.2% |
| T367 (top = search + day) | kept | broken: the day is at the foot | broken: the day is at the foot; and T425 (Trout / Salmon visible on the rail) |

Today (0.9 look): 692 px (75.6%) at 412 x 915 and 491 (66.4%) at 360 x 740, where the legend bar takes three lines;
with T1211's key alone: 758 px (82.8%). A gives back 9 px of that: its Today chip is 44 tall to match the rail (the day
bar is 35).

Checks for each option (in the pictures): text 1.3 at 360 wide (each label one line, its box grows: `type.ts`
`LABEL_PROPS`; as today the Trout / Salmon cells grow about 11 px wider than the rail, T1201); dark; the Conditions tab
(its horizons follow the day: top-left in A, over the legend bar in B and C; the rail or the map button over ©); a water
tapped (its page covers the map, with its own "Today ▾" date pill, which the chip now matches). The key opened: in B and
C the day chip keeps the corner and the key's card opens over the key chip; the ©, scale and rail rise with the foot.

What every option costs (decided, said once): without ‹ ›, the next day is two taps (chip, then "Day after"), where T367
kept it one; the owner chose the chip over that. Search hints at text 1.3 on 360: the four drawn fit the field ("Hook a
water by name" the longest of them); the longest of all, "Any creek worth a cast?", was not drawn there: unchecked.

## The option pictures, how they were made

Real renders of the web app at phone width, no composites: headless Chrome, 412 x 915 and 360 x 740 at 2x, Prince
George at z6.3, Trout, light unless "dark". The phone's bell and refresh are drawn by the store-shots web build
(`EXPO_PUBLIC_STORE_SHOTS=1`, as the store pictures are). A, B and C: the design branch's dev-only switch (`feat/T1212-phone-map-controls`, 34a7c7cc6, never merged)
on top of T1208's branch, so look A and T1211's key are in them; Today: `origin/main` with no switch. Text 1.3 is the
switch's `t13`: the app's Text and the search field drawn at 1.3 under each one's own cap, as `system-text-sweep` does;
the map's own scale and © words are not scaled. On `main` alone, b and c put the old legend bar beside the day chip
(they are meant for T1211's key).

## Built (option A, "Tidy", with © in the corner: owner 2026-11-26)

1. The day chip (`DayCalendar.tsx` `MapDayStrip`): calendar icon, the day, ▾; a press opens the existing date sheet. The
   arrows are gone. Another day (picked or from a link) fills the chip and adds a ✕ "Back to today" segment: one tap
   home. 44 px (`CornerStrip.tsx` `MAP_CELL`), as are the Conditions horizons on the map (`HorizonPicker` `cellHeight`).
2. No + / − on a phone-wide window (`MapRail.tsx` `zoomButtonsShown`: tablet and desktop keep them; `MapProps.zoomButtons`,
   hidden by `controls.css` `[data-zoom-buttons="off"]`, keys still zoom). The phone app never drew them. Double tap
   still zooms in on both (`MAP_GESTURES`, untouched). "Phone" is the window's width (`useLayoutClass()`, < 900 px): a narrow
   desktop window or small touch laptop loses them too (keys still zoom). Not built: keep them for a mouse, `zoomButtonsShown(
   layout, fine)` = `layout !== "phone" || fine`, `fine` from `matchMedia("(pointer: fine)")` in a `.web.ts`.
3. One grid: the rail 8 px under the bar and 14 px in, level with the day chip (84 / 10 under a wide window's zoom
   stack); the key chip 14 px in (`shell/MapKey.tsx` `KEY_EDGE`, its card's width follows); the scale 14 px in
   (`chrome.ts` `GEOMETRY.scaleLeft`, `controls.css`).
4. The © in the bottom-right corner (`MapCredits.tsx`, `GEOMETRY.right/bottom/target`): the 28 px chip 14 px in and 10 px
   up, in a 44 x 44 press, level with the key chip; the opening "© OpenStreetMap contributors" line stands over the foot
   block (`footBottom`) and stops short of the scale (`startupMaxWidth`); beside the floating key it takes the map's own corner, over a bar
   (Conditions, Runs) it stands above it (`shell/mapCorner.ts`, `MapProps.insetCredits`); Esri's badge stands over it.
   Same component and place on web and phone; the desktop's keys are cards, so its © is in the corner already.
5. The walkthrough's day step: same target ("days", the chip), words "Tap the day to choose another; …" (`help.ts`).
   Help had no words about the arrows. The date sheet's back is unchanged (BACK.md untouched).

Tests: `T1212.tidy.test.tsx`, `DayCalendar.test.tsx` (the chip), `MapCredits.render.test.tsx`, `tools/map-credits.test.ts`.
App suite 2026-11-27: 6 failures, all failing on look A's tip without this change (its gates and `MapDay.shell.test`).
Pictures of the built app against today (412, 360, 320; text 1.3 emulated): the lane's `T1212-pictures/built-sheet.png`.
Zoom buttons for those who cannot pinch: not built (## Owner).

## Acceptance

Pictures of the built app at 412 and 360 (Map, calendar open, key open, Conditions, dark, text 1.3) against today; the
full check; an independent review.

Depends: T1211 (the key; this branch is on look A's). Size: M.

## Owner

- [x] 2026-11-26 — the day selector at the top of the map → "One \"Today\" chip that opens the calendar (Recommended)" (2026-11-26)
- [x] 2026-11-26 — the + / − zoom buttons → "definitely drop +/- then we need to maybe rework all the rest of the controls?" (2026-11-26)
- [x] 2026-11-26 — pick one whole set of map controls from the sheet: A Tidy, B Thumb or C Most map (or none) → "do tiday
  with the attribution moved to botton corner now" (2026-11-26)
- [x] 2026-11-26 — a "Zoom buttons" switch for anyone who cannot pinch (zooming OUT by one finger is only a drag, WCAG 2.2
  SC 2.5.1) → "A switch under More > Display, off by default (Recommended)" (2026-11-27): built as T1214.
