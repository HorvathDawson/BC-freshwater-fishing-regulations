# Back, everywhere (the one back)

Cited from code and tests as "app/BACK.md". Android's back works everywhere a user expects it to. One rule:
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
| A water opened from a search result | the map it was searched over, search closed and its text gone, the camera framed on the water (× the same) | route `fromSearch`: `from` drops `search`; Shell `arrive` fit | route.test, Search.shell, back.test |
| A water opened by a map tap beside the search sheet | back to search, query and selection kept (‹ says "Search") | route `from` keeps `search` | route.test, Search.shell |
| A More page (Glossary, Data sources, …) | closes the page to the More tab; a glossary term first | MenuPage `useBackLayer` | back.test "More page", Menu.test |
| Water screen, any view (Regulations · Conditions · Map) | leaves the water for the map it was tapped on (views are lateral: back never walks the strip) | route | back.test "water screen" |
| A water flowed into (stack > 1) | back to the water it came from, at its part | route | back.test "flowed into" |
| Moves inside one water (a part step, ▲ / ▼, a view, "Show regulations for this part", a split lake's other area) | none of them is a back step: they REPLACE the frame, so back still leaves the water; a water already held (A → B → A) is gone back to, not stacked | route (`enter`) | back-layers "back steps out one layer", back.test "N moves", "the same walks" |
| ▲ / ▼ across a name change into the water this one continues as / from (`water_continue`: Vedder ↔ Chilliwack) | ONE system: the cross REPLACES the water on screen, so one back leaves the system for where it was opened from; "Flows into" from a tributary stays a layer | route `cross` (`crossAction`) | back-layers "cross", "system"; back.test "one system" (Android + browser); WaterMode "across waters" |
| The water header's × (every water view) | leaves EVERY water at once, for where the first was opened from | route `close` | back.test "tributary chain", "the same walks" |
| Water map: a boundary mark open | closes the mark (the map stays) | WaterMapView `MarkPop` `useBackLayer` | WaterMapView.test |
| Water map: the fork chooser | closes it, no way taken | WaterMapView `Chooser` `useBackLayer` | WaterMapView.test |
| Pick on the map (pick mode's banner) | leaves pick mode, nothing made (= its Cancel; Escape on the web); the next back is the tab rule | SpotPickBanner `useBackLayer` | MapRail.shell "pick mode is a back layer" |
| Layers sheet (and every `<Sheet>`) | closes the sheet | Sheet `useBackLayer` | back.test "Layers", gestures.render |
| Regulations page: "Which day?" (the date pill's DaySheet) | closes the sheet, the day kept; the next back leaves the water | DaySheet → `<Sheet>` | back.test "day sheet" |
| A spot (viewed) | back to the tab it was opened over (the Spots tab, or a map) | route | back.test "a spot", MapRail.shell |
| A spot being edited, nothing changed | ends the edit, stays on the spot (= Cancel) | SpotScreen `useBackLayer` | back.test |
| A spot being edited, with changes | **asks** ("You have unsaved changes"); back again answers "Keep editing" — never drops an edit unasked | SpotScreen | back.test |
| "Delete this spot?" | answers "Keep it" — nothing deleted | SpotScreen | back.test |
| Adding a spot: details → hour → point → water | one step back each (= that step's ‹ Back) | SpotCapture `useBackLayer` | back.test "adding a spot" |
| Adding a spot, first step, nothing put in | leaves the capture | route `adding` | back.test |
| Adding a spot, first step, a name/notes/photos put in | **asks** ("Discard this spot?", `DRAFT_WORDS`); back again = "Keep adding" | SpotCapture | back.test "notes typed" |
| A zoomed or panned map; a zoomed chart | **nothing is reset** — back is navigation, not undo (Android: Maps and Photos do not un-zoom on back); the map's ⤢ and the chart's own reset do that | — | (decision) |
| Folds, expanders, the regs page's section tabs / pin bar | not back layers (they are the page's state, not something opened over it); back leaves the water | — | (decision) |
| Keyboard up | the OS closes it first (Android's own) | — | — |

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

`tools/back-layers.test.ts` checks each component (not each file) that draws a Modal or a `role="dialog"`, takes a
close or back prop, has a surface-like name (`...Sheet`, `...Menu`, `...Page`, ...) or keeps inline open state: it must
call `useBackLayer(` or draw a `<Sheet>`, and open state must reach one. A component whose back another layer already
takes goes in the test's `COVERED` map (`path#Component: why`; a stale entry fails). The test also lists every surface
it finds by name: a new one is added there, on purpose.
