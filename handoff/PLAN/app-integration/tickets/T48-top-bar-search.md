# T48 — search lives in the top bar (menu · search field · sync), opening over the same map

> **BUILT 2026-10-22 with the owner's changes (decisions.md "OWNER 2026-10-22 — answers" + "T48 BUILT"):** NO menu
> button — the top bar is the field alone with the sync line at its top; the bottom bar is Map · Conditions · Spots ·
> More (smaller); the rail is Layers alone. Where this ticket's Design says otherwise (the menu button, Spots on the
> rail, the bottom-bar options), the decisions win. Shots: `../T48-shots/`.
Phase 4 · Depends on the map-chrome lanes (wf_5f5e25c3-271: menu-info's top-left menu button, map-rail-spots' side rail),
search placement + preview (merged), G1 gestures, Android back lane · Size 1–2 days

## Owner (2026-10-21)
"after the placeholder sync and menu button goes into the topbar like discussed before maybe we should think about moving
the search up there instead of being a button? then when used it opens the menu like the search currently is? also we
could have quirky search statements like 'cast your search' 'hook into a new spot' … just make it so when we click the
search icon the text box for search opens up in top bar and the screen turns into the current search screen (maybe
blend the transition so it looks like the map is being reused or something?) … we should think about it and if thought
of as a good idea then we can roll with it."
Recommendation given (accepted pattern: Google Maps, Apple Maps, onX, Fishbrain): yes. Owner follow-up: "or the search
bar can be always open" — agreed: the field is ALWAYS VISIBLE in the top bar (never collapsed to an icon); tapping it
focuses it and opens the results over the map.

## Design
- TOP BAR on the browse screens: menu button (left) · a rounded search FIELD filling the middle · the reserved sync slot
  (right, empty until sync exists). Layers + Spots stay on the side rail. Heights and insets from tokens. It sits over the map.
- ONE PLACEHOLDER PER OPENING, chosen from a words-table list (never cycling while visible). Each line still says what the
  field searches, e.g. "Cast your search — any lake or river", "Drop a line: river, lake or creek…", "Hook a water by
  name", "Reel in a river…", "Where to next? Search waters", "Find your next water". Nothing that promises fish
  ("where they're biting"). accessibilityLabel is always the plain "Search waters".
- The field is always drawn (not an icon). TAP → it takes focus IN PLACE (no jump), the keyboard comes up, and the map behind stays MOUNTED, dimmed and slightly
  blurred. Today's SearchScreen becomes a results sheet sliding up over it. Its preview map is the SAME map (no second
  MapLibre instance): previewing a result frames the main map. Picking a result settles the sheet and flies the map to the
  water (or opens it through openWater — the one opener — when that is the action). Cancel / back / Escape reverse the
  whole transition and restore the camera exactly.
- A PLACE IS SHOWN WHEN TAPPED (owner 2026-10-21: "in search when a place is clicked it should automatically view it and
  the nearbys like the streams instead of having to use eye"): tapping a town/place result immediately flies the map
  (the same map behind the sheet) to it, drops its pin, and lights the nearby waters on the map. At the same time the
  sheet lowers to a half height showing the "Water near <place>" list, so map and list are both visible. Tapping a
  nearby water in that list frames it on the map; its row's action opens it through openWater(). The eye stops being
  needed for places; keep it only where a row has two actions (show vs open), or remove it if every row's tap already
  shows.
- Reduced motion: a crossfade, no scale or slide. Native driver for transforms (motion.ts NATIVE_DRIVER).
- BOTTOM BAR: Search and Spots have left it (Spots → side rail). Mock the two remaining options for the owner: (a) keep a
  2-item bottom bar Map · Conditions, or (b) a compact Map/Conditions switch in or under the top bar with no bottom bar.
  The owner picks before this is built.

## Acceptance
- A render test: the search field's frame is the same before and after focus (no jump); the results sheet mounts over the
  map without remounting the map (one Map instance, camera unchanged after cancel); back closes search before anything
  else (the Android back lane's registry); the placeholder is from the words list and stable while open.
- A render test: tapping a place result frames the map on it, its pin is drawn, the nearby waters are lit, and the sheet
  is at half height with the nearby list — with NO eye press.
- A house check: no second MapLibre map is created by search.
- Web check at 390x844 and desktop: a frame recording of open/cancel/pick, saved to `T48-shots/`, with frame-time under
  16 ms on the phone profile for the transition.
