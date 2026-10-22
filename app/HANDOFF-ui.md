# Handoff — the app

Written 2026-09-02, at the end of a long session. Status, what was learned, and what is
still open. Nothing here is a plan.

---

## Where things stand

325 tests, `pnpm check` green (boundaries → platform → style → spinner → fixture → deps →
typecheck → test).

**Conditions.** One implementation: `ConditionsPanel`, rendered only by the water screen's
Conditions view (`WaterConditions`). One water screen, whichever map you tapped (owner rule
2026-10-21): a tap on the Conditions map and on the Map tab both go through `@app/ui`
`openWater`/`openTap` and open the same frame, differing only in the view in front; back leaves
the water for the map it was tapped on. An unnamed reach opens on its Conditions view alone, from either map.

**One frame for the water screen's views (T44, owner 2026-10-21: "switching between regulation and condition tab makes
the margin on the left change").** Every view draws inside `WaterFrame` (ui-native): the frame owns the background, the
column (`WATER_FRAME.maxWidth` 1,280, centred), the header (FaceBar — drawn there and nowhere else), the header's swipe
and the gutter; the name row is `WaterTitle` on every view (the regs header's own first row on Regulations); an inset
block is `WaterInset`. A view sets no outer gutter, width or header of its own (`tools/water-frame.test.ts` lists every
horizontal spacing a view may set); `tools/water-frame-layout.test.ts` mounts the Shell, switches views and measures the
shared elements in headless Chrome at 390 / 768 / 1440 (whole-pixel equal). The Map canvas alone goes full-bleed. A
sideways swipe moves only the view's body, wherever it starts: the headers' detectors are `still` and drive the frame's
one offset (`SwipeOffsetContext`; `tools/water-frame-swipe.test.ts`).

**Wide screens (T45).** ONE layout class, `useLayoutClass(): "phone" | "tablet" | "desktop"` (@app/ui `layout.ts`,
breakpoints `LAYOUT_BREAKPOINTS` 900 / 1280), from the window width the app shell provides once (`LayoutWidthContext`,
`useWindowDimensions` in apps/mobile App.tsx); no provider is a phone. Components take the class (a prop, or the hook);
none reads the window. A phone is drawn exactly as before. Regulations (`regs/RegsWide.tsx`): a reading column of
`REGS_WIDE.measure` (560 px, ~70 characters of body, ≤ 80 measured by `tools/regs-wide-layout.test.ts`) centred in the
room the page has — a measure INSIDE the frame, which stays 1,280 so the three views keep one header and one left edge
(the owner's "frame ~720 for regs reading?" is answered here for Regulations without changing `WATER_FRAME`); on a desktop
a side column at the frame's left holds the date pill, the sections as a list and the pinned lines. `Sheet` on a tablet
or desktop is a centred dialog (`SHEET_DIALOG`, 640 wide at most, sized to its content; Done, the scrim, back and Escape
close it — no drag).

**Fish pictures (T46).** A fish's looks (an sp: sheet's consecutive `image` blocks) are one `FishLooks` group: side by
side on a tablet / desktop (a wrapping row, each look between `FISH_PICTURE.minWidth` 180 and `maxWidth` 360), stacked on
a phone; every picture at most 360 px across, so none scales with the window. Captions are the blocks' `credit` (core
`fishSheet` from the manifest), never written in ui-native. No ground behind a registered picture (T39), light and dark.
Measured in Chrome by `tools/fish-looks-layout.test.ts` (mutation: no max width fails).

The panel carries: the reading and its standing, quantity and span controls (flow/level,
72 h/this year), a forecast-model picker when more than one BC River Forecast Centre run is
live, the hydrograph with envelope + prior years + a TODAY rule, the model's disclaimer
verbatim, and the route panel with a mini-map showing both ends of the trace.

**The layout: one place for each kind of thing (T48, owner 2026-10-22).** The TOP BAR over both browse maps is the
search field alone, always open, with the sync status line at its top (`TopBar`; `syncLine`: the regulations' last day
and when the flows were last checked). Tapping it focuses it in place and raises `SearchSheet` over the SAME map (route
`search`; no second map): FULL while typing (the map dimmed), HALF once something is picked — a town flies the map there
with its pin and the waters near it lit, a water row frames it; the selected water row's › opens it through
`openWater`. Closing (‹, Escape, back, a tab) restores the camera exactly (`fit.camera`). The SIDE RAIL is Layers
alone (`MapRail`). The BOTTOM BAR, smaller, is Map · Conditions · Spots · More: **Spots** is a tab again
(`SpotsScreen`), **More** lists the menu's pages (`MoreScreen`; the hamburger and its drawer are gone).
`tools/one-menu.test.ts` holds the "one": the menu's list, the spots list, the capture flow and the search sheet each
exist once.

**Spots (owner 2026-10-21; a tab again since T48).** The Spots tab (`SpotsScreen`) — your spots (tap opens one over the
tab, back returns to it; "Show" goes to the Map tab framed on it with a pin — the spot screen's own Show is the same
function), and exactly TWO ways to add one (owner 2026-10-22; the map's-middle "Add a spot here" is gone): "At my
location" (web: offered when the browser says granted, or labelled "Asks for your location first" when the tap itself
will prompt; refused at the tap: said, nothing starts; refused in settings, or native — no location module yet,
`apps/mobile/src/locate.native.ts` — not offered, and the tab says why) and "Pick on the map" (the Map tab in PICK MODE:
`SpotPickBanner` "Tap where your spot is" + Cancel in the search field's place, the map pans and zooms as usual; the
first tap — `onMapPoint`; `onPressFeature` is withheld so a water under it does NOT open — drops the provisional pin
and raises the same `SpotDropSheet`; Cancel, back, Escape or another tab leave pick mode with nothing made). A **long press** on a browse map (`MapProps.onLongPress`:
MapLibre Native's own on a phone; on the web a held finger timed by `@app/core` `LONG_PRESS`, or `contextmenu` — right
click / Chrome's long press — whichever first, once; the click after it is not a tap, and the finger's lift is
cancelled so the browser makes no click of it on the sheet now under the finger) drops a provisional pin and
raises `SpotDropSheet`: Cancel, back, Done, the scrim or a drag removes it and saves nothing; Continue goes on
into the ONE capture (`SpotCapture`, now able to start `at` a point: the map opens on the pin, step 1 picks the water
near it, then straight to "when"). Every add is the route's `addSpot` with `at` (`@app/ui` `useMapRail`, `spotsMenu`);
words in `@app/core` `mapRail.ts`. Each new sheet is a back layer, so Android's back closes it first. The menu, the
provisional pin and Show's pin + framing belong to the map they were made on: the route moving anywhere forgets them
on that render (`railPlace`), so a map remounted on the way back is never framed on an old spot again — except
Show's own move to the Map tab, which is what its framing was made for.

**The Layers menu, direction B (owner 2026-10-21 late).** Three sections: **Map** (Map · Satellite · Corridors
[coming soon]), **Colour the water by** (Regulations · Stocked · Depth) and **On the map** (Depth contours, Private land,
then Pools and runs and Relief [coming soon]), then one "Always shown:" line (parks and ecological reserves, Indigenous
land (reserves), no-access and permit-only land, region and management-unit lines). Every row comes from the LAYER
REGISTRY, `@app/core` `layers.ts` (`LAYERS`: id, kind ground/water/overlay/always, views, default, `sets` — only
`mapModes` inputs — `needs` built/release/network, words). `availability` derives ok / no-data / online-only /
coming-soon, each with its reason; `layersVM` is the whole sheet plus `effective` (what the maps are handed: an
unavailable choice falls back, the choice is kept) and `changed` (the rail's numbered badge, said aloud as "Choose
layers, 2 changed"). Adding a layer is one entry. Rules:
- ONE water colouring for every feature. There is no lake-only choice. `mapModes` takes `water`. Under Stocked or
  Depth the lakes wear the measure and streams, river polygons and wetlands draw plain. The legend (`waterLegend`,
  shown under the chosen tile and in the Map-tab strip) ends with a line saying which waters draw neutral and why.
  Stocked and Depth stay greyed "No data yet" until a release carries lake values (`lakeModesReady` →
  `lakeValuesCarried`).
- Satellite offline: Map is tagged SHOWING. Satellite stays chosen, greyed and dashed, tagged NEEDS A CONNECTION, with
  the reason line under the row.
- Private land (`ownership`) finally has a switch. A test holds that every toggleable style group has one, that no
  entry sets protected/access/admin, and that each always-shown entry names real non-toggleable layers.
- Data sources, live data and credits LEFT the sheet. They are on the menu's Data sources & dates page and in the
  map's own attribution. The palette moved to the menu's new **Display** page (`MENU_WORDS.display`, `DisplayPage`).
- The sheet still opens from the Map tab only (`layersVM("map", …)`). The registry's `views` is the slot if Layers
  ever goes on the Conditions map.
- The Map-tab legend strip WRAPS (`LegendStrip wrap`), so nothing overflows at 390 px.
Shots: `handoff/PLAN/app-data/layers-menu/built/`.

**Map.** Conditions colours by Flow, Level or Both. Gauge dots carry pill labels on the same
ramp as the water; below z7 the water fades and each station becomes a soft disc. Route
reaches paint in `color.highlight` with downstream arrowheads.

**Satellite and the phone's connection (owner 2026-10-21, late).** Esri World Imagery ships KEYLESS, as the original
app did ("this is a non profit app"); `EXPO_PUBLIC_ARCGIS_API_KEY` is optional config (`apps/mobile/src/config.ts`,
`handoff/PLAN/app-data/satellite/esri/README.md`). A phone hears itself go offline through
`@react-native-community/netinfo` 12.0.1 (Expo SDK 57's version; why it is worth a native module: `deps.md`): one file
names it (`ui-native/src/connectivity.native.ts`, loaded lazily), `netInfoConnectivity.ts` reads it (offline only on a
definite no; each change said once). The one-tile probe stays as the fallback (`useImageryReach`: on choosing, on
coming back, and every 120 s while up on a phone, whose map reports no failed tile — `Connectivity.probeWhileUp`), and
it is the only signal where the module is not linked. The "minimal prod packages" rule is about the release's DATA
files; this adds none. Needs a rebuilt development build (`expo prebuild`) to take effect on a device.

**Water map crossing (owner 2026-10-21, late).** ▲ / ▼ that cross into another water (`water_continue`) SAY where:
"▲ Upstream into Chilliwack River" on the button itself, two lines at most, centred, then an ellipsis (the full words
stay the accessible name). Words: `@app/core` `WATER_MAP_WORDS.upInto` / `downInto`, composed by `crossLabel`.

---

## Back, everywhere

Owner 2026-10-21: "make sure android back button works for entire app where a user expects it to." One rule:
**the innermost thing closes first** — sheet/menu/dialog > overlay or in-screen step > water (its views are lateral)
> tab > the app. Every input is the same back: the ‹ links, Android's system back (button and edge gesture),
the browser's back button and a phone browser's swipe-back, and the iOS edge swipe.

**One mechanism.** `@app/ui` `useBack` (gestures.ts) is the one back: it closes the top **back layer**, else pops
the route (`route.ts` `reduce(r, {type: "back"})`), else returns `false`. The platform glue is attached once, by the
app shell (`apps/mobile/src/App.tsx` → `<Shell systemBack>`): `systemBack.native.ts` is Android's `BackHandler`
(`hardwareBackPress` → back; `false` hands the press to Android, which leaves the app), `systemBack.web.ts` keeps
one browser history entry per back step (`historySync`), so the browser's back pops exactly one step and the ‹
leaves no stranded entry. iOS has no system back: `<EdgeBack>` (`useEdgeBack`) is the left-edge swipe.
A `<Sheet>`'s Modal `onRequestClose` (Android delivers back there while a Modal is up) is that same back.
`tools/back-layers.test.ts` holds this: `BackHandler`, the History API and `onRequestClose` are named nowhere else.

| Surface | One back does | Where | Pinned by |
|---|---|---|---|
| Map tab, nothing open | **not handled** — Android leaves the app; the browser leaves the page | route depth 0 | back.test "Map tab's root" |
| Conditions / Spots / More tab root | goes to the **Map tab** (the start tab — Android's fixed start destination, as Google's own apps do); the next back leaves. **iOS:** the edge swipe does not take this step (tabs are not history on iOS; `edgeBackDepth`) | route.ts, gestures.ts | route.test, back.test, gestures.test "edgeBackDepth" |
| Search open over a map (T48) | closes search (keyboard down, camera restored); the next back is the tab rule | route `search` | back.test "search", Search.shell |
| Search: a town's "Water near …" list | back to all results (its "← All results") | SearchSheet `useBackLayer` | back.test "search" |
| A water opened from search | back to search, query and selection kept | route `from` keeps `search` | route.test, Search.shell |
| A More page (Glossary, Data sources, …) | closes the page to the More tab; a glossary term first | MenuPage `useBackLayer` | back.test "More page", Menu.test |
| Water screen, any view (Regulations · Conditions · Map) | leaves the water for the map it was tapped on (views are lateral: back never walks the strip) | route | back.test "water screen" |
| A water flowed into (stack > 1) | back to the water it came from, at its part | route | back.test "flowed into" |
| Moves inside one water (a part step, ▲ / ▼, a view, "Show regulations for this part", a split lake's other area) | none of them is a back step: they REPLACE the frame, so back still leaves the water; a water already held (A → B → A, ▲ then ▼ across a name change) is gone back to, not stacked | route (`enter`) | back-layers "back steps out one layer", back.test "N moves", "the same walks" |
| The water header's × (every water view) | leaves EVERY water at once, for where the first was opened from | route `close` | back.test "tributary chain", "the same walks" |
| Water map: a boundary mark open | closes the mark (the map stays) | WaterMapView `MarkPop` `useBackLayer` | WaterMapView.test |
| Water map: the fork chooser | closes it, no way taken | WaterMapView `Chooser` `useBackLayer` | WaterMapView.test |
| Pick on the map (pick mode's banner, owner 2026-10-22) | leaves pick mode, nothing made (= its Cancel; Escape on the web); the next back is the tab rule | SpotPickBanner `useBackLayer` | MapRail.shell "pick mode is a back layer" |
| Layers sheet (and every `<Sheet>`) | closes the sheet | Sheet `useBackLayer` | back.test "Layers", gestures.render |
| Regulations page: "Which day?" (the date pill's DaySheet) | closes the sheet, the day kept; the next back leaves the water | DaySheet → `<Sheet>` | back.test "day sheet" |
| A spot (viewed) | back to the tab it was opened over (the Spots tab, or a map) | route | back.test "a spot", MapRail.shell |
| A spot being edited, nothing changed | ends the edit, stays on the spot (= Cancel) | SpotScreen `useBackLayer` | back.test |
| A spot being edited, with changes | **asks** ("You have unsaved changes"); back again answers "Keep editing" — never drops an edit unasked | SpotScreen | back.test |
| "Delete this spot?" | answers "Keep it" — nothing deleted | SpotScreen | back.test |
| Adding a spot: details → hour → point → water | one step back each (= that step's ‹ Back) | SpotCapture `useBackLayer` | back.test "adding a spot" |
| Adding a spot, first step, nothing put in | leaves the capture | route `adding` | back.test |
| Adding a spot, first step, a name/notes/photos put in | **asks** ("Discard this spot?", `DRAFT_WORDS`); back again = "Keep adding" | SpotCapture | back.test "notes typed" |
| A zoomed or panned map; a zoomed chart; "Larger map" | **nothing is reset** — back is navigation, not undo (Android: Maps and Photos do not un-zoom on back); the map's ⤢ and the chart's own reset do that | — | (decision) |
| Folds, expanders, the regs page's section tabs / pin bar | not back layers (they are the page's state, not something opened over it); back leaves the water | — | (decision) |
| Keyboard up | the OS closes it first (Android's own) | — | — |

Not built yet, and covered when they land: the rule, glossary, species and "how this was determined" sheets
(T36/T37, `onSheet` is a no-op today), the Regulations header's Parts sheet (`onParts` unwired), the map rail's spots
menu and long-press "add a spot here" card, the hamburger menu and its pages (glossary, about, contact, bug report,
source dates), a photo viewer. Each is a surface in the table's sense: back closes it first. Expected, so the lanes
building them can test against it: the rail's menu, the hamburger menu and each of its pages close one per back
(a page opened from the menu goes back to the menu, then the menu closes); the long-press card is cancelled by back
(nothing saved, nothing asked — it holds nothing yet); once its "make a spot here" is taken, the capture's own rows
above apply.

**How a new surface registers back** (the map-rail and menu lanes, and anyone after):

1. A bottom sheet: draw it in `<Sheet open onClose title palette>` (`@app/ui-native` Sheet.tsx). Done — the
   sheet registers itself, Android's Modal back and the browser's back both close it, and it holds one history entry.
2. Anything else that opens over the screen or steps inside it (a menu, a card, a confirm, a viewer, a step):
   call `useBackLayer(open, closeOneStep)` from `@app/ui` in the component that draws it. `closeOneStep` is what one
   back does — usually close; for a guarded surface, *ask* (set a confirm) and leave it open: a layer still open after
   its back is registered again on top, so the next back reaches it again (gestures.render "takes one step").
3. Draw it inside the Shell (everything the Shell returns is under `BackLayerContext`). A surface outside it has
   no one back and falls back to its own close.
4. Do not add a `BackHandler`, `popstate` or `onRequestClose` of your own: the scan fails, and two listeners are a
   double back.
5. Add a row to this table and a case to `apps/mobile/src/back.test.tsx` (mount the Shell, open it, `hardwareBack()`).

`tools/back-layers.test.ts` checks **each component** (not each file — a registered neighbour clears nothing) that
draws a Modal or a `role="dialog"`/`aria-modal`; takes a close or back prop (`onClose`/`onDismiss`/`onCancel`/
`onBack`/`onHide`/`onDiscard`/`onExit`, as `onX:` or `onX(): void`, on itself or on a props interface it names, or a
bare `close`/`hide`/`dismiss`/`cancel` function); is named `…Sheet|Menu|Drawer|Dialog|Popover|Popup|Overlay|Viewer|
Lightbox|Gallery|Picker|Pop|Confirm|Prompt|Page`; or keeps open state (`const [menuOpen, setMenuOpen] = useState`)
for something drawn inline. Each must call `useBackLayer(` or draw a `<Sheet>`; open state must be handed to
`useBackLayer(` or to a registered surface's element (`<LayersSheet open={layersOpen}>`). So a hamburger page
`AboutPage({ onBack })`, a `SpotsPanel({ onHide })`, an inline `menuOpen` menu in the Shell all fail until they
register. A component whose back another layer already takes (an on-screen ‹ the Shell's route layer answers; a
step or confirm its parent's layer steps back from) goes in the test's `COVERED` map, keyed `path#Component: why`,
with the reason — and a stale entry fails too. The test also lists every surface it finds today by name: a new one
is added there, on purpose.

---

## What was learned

**Assertions on expressions we build prove nothing about what the renderer accepts.** A
`line-width` shipped as a `case` over two zoom curves. MapLibre allows one zoom-based
`interpolate` per expression and requires it outermost, so the layer failed to *parse* — the
map came up unstyled with a single console line, and every test passed.
`tools/style-valid.test.ts` now runs the spec's own validator over the composed style
(basemap + generated layers + runtime layers + per-mode per-theme paint) for every theme and
mode.

**An effect declared above the one that creates the map never runs.** The gauge label's pill
was registered in an effect whose only dependency was the theme; on first render
`map.current` was null, it returned, and never fired again. MapLibre answers a missing
`icon-image` by drawing the text and logging, so labels rendered bare and nothing looked
broken.

**Feature-state that nothing reads is a no-op that looks like a feature.** `highlight()` had
always written `{selected: true}`; no expression consumed it, so every route highlight set a
flag into the void.

**`once("load")` on an already-loaded map never fires.** Gauge dots appeared only after
opening a reach and coming back — the remount was the only path that baked the data into the
style.

**A ramp for a map may not contain the ground's own colour.** The flow ramp's midpoint was a
sandy beige one shade off the basemap paper, so the middle of the scale — where most rivers
sit most of the time — was invisible over land.

**Sharing a component is not the same as having one surface.** The sheet's conditions face
and the Conditions tab shared `ConditionsPanel` and still diverged: only one of them can
carry the reach you tapped, the coordinate behind "you are here", and the map's quantity.

---

## Open

**~~Nothing in this session was verified visually by me.~~ — 2026-09-03: it has been.** The
app was run against the province bundle, the live feed and the full atlas, and walked with
Playwright. Seven defects, none of which any of the 323 passing tests saw; all fixed, and
the walk is written up in the *What the Browser Said* artifact. What is still unseen: the
native app. Everything here was verified through react-native-web in a desktop browser, so
gesture feel, font metrics and the native map SDK remain unproven.

**Riffle parity: audited 2026-09-03, two gaps found and closed.** The date pill was inert —
`MapScreen` declared `onDate` and `Shell` never passed one — so the app showed the qualifier
on every seasonal answer with no way to change it; `DateSheet` is riffle's "Which day?"
month grid and day stepper. And the legend carried no counts, though `LegendCount` had an
`n` prop since it was written, with a comment reading "the count is the point". Both are in.

Riffle's remaining differences are deliberate and should stay: it says "limited" where the
app says "restricted" (core's `statusWord` is the one vocabulary, and riffle predates it),
and it puts a satellite toggle on the map where the app keeps that choice in the Layers
sheet beside the other things that change what is drawn.

**The fixture bundle is still load-bearing for tests.** The app itself reads the province
bundle, but `tools/bundle-schema.test.ts`, `packages/data/src/bundle/source.test.ts` and
`tools/feed-contract.test.ts` run against `packages/data/dev/bundle.sqlite`. Deleting it
would make the suite depend on a 44 MB uncommitted build artifact. Removing it was asked for
and not done.

**Map → Conditions flash on tab switch.** A fresh mount is clean; switching tabs shows a
frame of the wrong colouring.

**Desktop is scaffolding.** `apps/web/src/desktop/` exists; the DOM component set does not.

**Contract gaps.** `app/design/data-contract.html` still names things the source cannot
answer: `entry`, `rule`, `rule_section`, `chart`, `release`/`stock_water`, `edition`,
`base_reg`, `rule_unplaced`, `tombstone`. Those are bundle-side (see the data handoff).

**The seasonal chart's prior years come from HYDAT** and are therefore two complete years
behind; the current year's line is accumulated by the feed and starts a month long. Both are
captioned, but a reader in January sees very little.

**Gauge representativeness is unsettled** and it is the app's problem too: the panel says
"no gauge is entitled to speak for this water" from a hard rule that is known to be blunt.
See the data handoff.
