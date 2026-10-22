# Layers menu: inventory (read-only, 2026-10-21)

Owner, 2026-10-21: *"we could also update and improve the layer menu. it feels a little bit clunky. we should update
it with the future additions in mind."*

This is an inventory only. Nothing in the app was changed. The screenshots in `current/` come from the MAIN tree at
`14dbc071` (Expo web, 390x844, on my own ports 39717/39718, both stopped afterwards). They show the menu **before** the
map-rail-spots lane (`wip/map-rail-spots`, worktree `wf_5f5e25c3-271-1`), which moves the Layers button onto a
right-hand side rail next to a new Spots button and removes the Spots tab.

`app/design/riffle.html` is OBSOLETE (owner 2026-10-21). It is not a reference for this work. The comments in
`LayersSheet.tsx`, `OptionRow.tsx`, `SearchScreen.tsx` and the WIP `MapRail.tsx` that cite it are history, not spec.

---

## 1. What the menu controls today

Sources: `app/packages/ui-native/src/LayersSheet.tsx` (+ `LayersSheet.test.tsx`), `Shell.tsx:164,229-266,436-494,
645-652`, `MapScreen.tsx:109-133`, `app/packages/ui/src/mapModes.ts`, `app/packages/map/style/layers.source.json`
(groups, `$toggleable_groups`, views), `app/packages/map/src/colouring.ts` (the one colouring engine).

### 1.1 Controls in the sheet

| # | Section / control | Kind | State it sets | What it does | Map tab | Conditions map | Water's Map view | Other maps (Search, MiniMap, DonorMap, SpotCapture) |
|---|---|---|---|---|---|---|---|---|
| 1 | **Streams**: no control, only a paragraph ("Flow, depth and temperature live in the Conditions tab…") and a count, "63,263 REACHES" | text | — | nothing | — | — | — | — |
| 2 | **Lakes**: "Colour lakes by" Depth / Stocked / Plain (3 swatch tiles) | lake colour mode (radio) | `layers.lake` → `lakeChoices()` → `mapModes.lake` | Depth = lake mode `surveyed` + group `depth` (contours) on. Stocked = lake mode `stocked`. Plain = lake mode `plain`, but on the Map tab `mapModes` turns it into the day's status (`profile`/`status`), so "Plain" is really "regulation status". | yes | **no**: Conditions forces `lake: standing` (or `plain` under Temp). The `depth` group still follows `activeGroups`. | yes (same `modes` and `activeGroups`) | no |
| 3 | Stocked: legend of 5 bands, inside the sheet | legend | — | shown only while Stocked is chosen | — | — | — | — |
| 4 | **Basemap**: Map / Satellite | ground (radio) | `layers.basemap` → `MapProps.basemap` → adapter `setBasemap` (in `applyLook`) | Satellite = the `imagery` raster (today EOX Sentinel-2 cloudless 2016, 10 m, `maxzoom 13`, online only) | yes | **yes, inherited, with no control there** | yes | Search map yes; MiniMap, DonorMap and SpotCapture no |
| 5 | **Live data**: "Stream gauges · 425 stations · 9h ago", "Stocking releases · per season" | info rows | — | none. The stocking row describes a feed that does not exist (prepared, not fed). | — | — | — | — |
| 6 | **Palette**: Light / Dark / Colour-blind | app theme (pills) | `themeName` / `onTheme` (App) | repaints the whole app and every map. `cvd` is light-ground only; there is no dark colour-blind theme. | all | all | all | all |
| 7 | Attribution block (6 credit lines) | text | — | credits | — | — | — | — |

### 1.2 Toggles the style defines that the menu does not show

`layers.source.json` groups (`toggleableGroups()` = `toggleable: true`):

| group | layers | toggleable | default | menu | note |
|---|---|---|---|---|---|
| `water` | stream, lake, lake_flowing, wetland, wetland_flowing, under_lake, basin | no | on | — | coloured by modes |
| `depth` | contour | **yes** | off | only through Lakes → Depth | |
| `ownership` | parcel_private (minzoom 12) | **yes** | off | **NOT EXPOSED.** Shell keeps `groups` state seeded from `toggleableGroups()`, but has no setter in any UI, so private land can never be turned on. | `$toggleable_groups`: "title is context… worth looking up deliberately" |
| `protected` | park, park_closed, eco_reserve | no (owner rule: safety) | on | — | |
| `access` | no_access, no_access_permit, indigenous_land | no (safety) | on | — | |
| `admin` | region, mu (minzoom 7) | no | on | — | the Conditions view hides it (`views[].hide`) |
| `imagery` | imagery raster | no (switched as `basemap`) | off | Basemap | |

Per-view, decided by the style and Shell rather than the menu: Conditions hides `mu`/`region` and shows
`stream__arrows` (flow arrows); the `gauges` runtime layer is shown on Conditions only; the horizon chips
(Now/+1d/+3d/+5d/+10d) and the **Showing** control (Flow / Level / Temp, in the legend strip) are Conditions' own
"mode" controls. The Map tab's mode controls are the **day stepper** (`MapDay`, in the legend strip) and the Lakes
choice in the sheet.

### 1.3 Where the button is

- Today (main): a "Layers" pill at the bottom left of the map (`MapScreen.tsx:128`), on the **Map tab only**
  (`onLayers` is undefined on Conditions, `Shell.tsx:491`). It is absent from the water's Map view, Search and the
  minimaps.
- In the lane (`wip/map-rail-spots`, not merged): `MapRail.tsx` is a right-hand stack under MapLibre's zoom buttons
  (top 84 on web, 14 on native), 40 px square buttons: **Layers** (Map tab only, still) and **Spots** (both browse
  maps). The Spots tab is gone (3 tabs). Words come from `@app/core` `MAP_RAIL`.

### 1.4 How a choice reaches the map (one pathway)

Sheet → `Shell` state (`layers`, `groups`) → `useMapModes` (`@app/ui` mapModes.ts: `modes`, `activeGroups`) →
`<Map modes groups basemap>` → adapter `applyLook` (`setLayerMode`, group visibility, `setBasemap`) → colouring engine
(`colouring.ts`) channels: status/profile, standing, and (lane `wip/lake-values`) stocked/survey. Owner rule: ONE
colouring pathway for all map views and modes; values never baked into tiles. A redesigned menu must only change
**inputs** to `mapModes` and must not grow a second path.

---

## 2. Future additions already planned or discussed

| Addition | Kind (for the menu) | Source | State |
|---|---|---|---|
| Lake **Stocked** mode | lake colouring | decisions.md "OWNER 2026-10-21 — lake Stocked / Depth"; QUESTIONS Q15; APP-PREP P17 #6 | **Prepared, not fed.** Engine channel + release slot are on `wip/lake-values` (worktree `wf_3d498e10-baf-2`), not merged. The toggle must be DISABLED with a "no data yet" note while the release has no lake values, and enable itself when a release has them. On main the toggle is still ENABLED and paints every lake plain blue. |
| Lake **Depth** (surveyed colouring + contour lines) | lake colouring + overlay | same | Prepared, not fed (same rule). The contour tiles exist (`contour`, group `depth`); survey values have no producer. |
| Lake depth **contours** as their own overlay | overlay | style group `depth` (toggleable) | built in the style; today bundled inside the Depth choice |
| **Satellite**: Esri World Imagery, online only, overzooms past the tiles | ground | decisions.md "OWNER 2026-10-21 — satellite imagery is ONLINE-ONLY"; app-data/satellite/README.md; app-data/overzoom/ | Owner picked Esri, **queued** (not built; style still EOX 2016 10 m). Overzoom to camera z18 MERGED. Offline: the drawn basemap shows and "the Layers toggle says why satellite is unavailable": **not built**. Owner still has to set up the Esri client key/billing. |
| 10 m Sentinel-2 offline base (2-3 GB) | ground (offline) | satellite README; implement_progress | open owner question |
| **Stream imagery corridor download** (buffer 100/250/500 m around a stream or section; Sentinel 10 m, lidar hillshade 1-2 m, municipal orthos) | download + ground layers inside a corridor | app-data/stream-imagery/SPEC.md | **Spec only, do not build.** 8 owner questions (§12). Its pack layers draw in the satellite ground slot. It wants a "Downloaded imagery" manager in Settings and a chip on the water screen, so the menu may only need a pointer to it. |
| **Lidar relief / hillshade** | ground or overlay | stream-imagery SPEC T1.5 | spec only |
| **Pools / runs** (slack water / fast water from lidar, "lidar YYYY") | stream overlay (points) | app-data/pools/README.md | spike: feasible, **not validated** (needs ~10 hand-checked pools); idea for a frozen `habitat_units` file |
| **Spots** on the map (saved pins) | overlay / own button | map-rail-spots lane, HANDOFF-ui (WIP) | WIP (rail Spots button, long-press to add). Whether saved spots draw on the browse map as a toggleable layer is not decided. |
| **Gauges** (stations, pill labels) | overlay | `$runtimeLayers: gauges`; Shell | built, Conditions only, not a toggle |
| **Flow arrows** | overlay | conditions-map, implement_progress (merged) | built, Conditions only (`views.conditions.show`), not a toggle |
| **Conditions quantity** Flow / Level / Temp | stream colouring (Conditions mode) | Shell `flowParam`, ChartControls in the legend | built, lives in the Conditions legend |
| **Forecast horizon** Now/+1/+3/+5/+10 d | time | owner 2026-10-20; horizons/README | built (corner chips + panel header, one state) |
| **Day** of the regulations colouring (« ‹ Today › ») | time | MapDay; D7 | built (Map tab legend; regs header pill shares the state) |
| **Basemap toggle** Map / Satellite | ground | built | built (see 1.1 #4) |
| **Colour-blind palette** | app theme | themes/cvd.json; tools/cvd.test.ts | built (light ground only) |
| **Dark** theme | app theme | themes/dark.json | built |
| **Region / MU boundaries** | overlay | style group `admin` | built, always on (not toggleable; hidden on Conditions by the view) |
| **Parks, ecological reserves, closed parks** | overlay | group `protected` | built, always on, **not toggleable by owner rule** |
| **No access / permit / Indigenous land** | overlay | group `access` | built, always on, **not toggleable by owner rule** |
| **Private land (parcels)** | overlay | group `ownership` | built in tiles + style, toggleable, **no UI** |
| **Wetlands** | water | `wetland`, `wetland_flowing` | built, always drawn, not toggleable |
| **Offline region / whole-app downloads** | data state | QUESTIONS Q5, MINIMAL-PACKAGING §5 | native is always offline (whole pmtiles installed); no per-region map download is planned. Web is lazy. The menu could show offline/online state (needed for satellite). |
| Map **data sources and dates** | info | menu-info lane (`wip/menu-info`, hamburger: glossary, about, data sources + dates from the manifest) | WIP. The Live data rows and the attribution block in Layers overlap with it. |
| Water temperature carried to rivers | stream colouring | style view `temperature` comment | idea (needs a donor panel). Today temperature colours the dots only. |

---

## 3. What makes today's menu clunky

Screenshots: `current/01…09`.

1. **The Lakes "Plain" choice is not plain.** On the Map tab it means "regulation status colour"
   (`mapModes`: `lakeMode === "plain" && statusOk ? status`). Choosing Depth or Stocked swaps the lakes' **regulation
   answer** for a different question with no warning (`04`, `06`: lakes go uniform blue).
2. **The prepared-not-fed modes are live and silently empty.** Stocked and Depth are enabled on main. Choosing them
   paints every lake `missing` blue (`05`, `06`). This breaks the owner's 2026-10-21 rule (disabled + "no data yet").
3. **Stocked hijacks the legend.** The Map-tab legend strip becomes the 5 stocking bands. The streams' status key
   **and the day stepper disappear**, though the streams are still coloured by status (`05`). Depth shows no legend
   at all (`06`). The strip is a single either/or slot, not one key per layer.
4. **Legend strip overflows the phone.** It measured 613 px wide in a 390 px viewport. "Has its own regulations" and
   "Base regulations only" are cut off (`01`); the stocked bands are cut off too ("over 10 y", `05`).
5. **Mode vs overlay is muddled.** Depth is a colouring AND a layer (contours) in one tile. `ownership` is a real
   toggle with no switch. Basemap is a radio that looks like the colour choices. Palette is an app-wide setting
   sitting among map layers. Nothing in the sheet is a plain on/off overlay switch.
6. **Most of the sheet controls nothing.** Of 7 blocks, only Lakes, Basemap and Palette are controls. Streams is a
   paragraph explaining why there is no control. Live data is two info rows, one of them for a feed that does not
   exist. The attribution block is 6 credit lines at the bottom (`03`). That is about 787 px of content for 3 decisions.
7. **Reach and scroll.** The sheet's ScrollView is capped at 520 px (`maxHeight: 520`), so on 390x844 Palette sits
   below the fold (`02`). The sheet covers 73% of the screen (top at y=231), so most of the map you are changing is
   hidden. The cap also leaves dead space under the scroll area (`03`). The Layers button sat bottom-left, far from
   the right-hand zoom stack (`01`); the lane moves it to the rail.
8. **No state on the closed map.** The button shows nothing when a choice is not the default (satellite, Stocked,
   Depth, colour-blind), so after Done there is no cue that lakes are no longer showing regulations (`05`, `06`, `08`).
9. **Per-view relevance is hidden.** The sheet opens only from the Map tab, but its Basemap choice silently carries
   into the Conditions map and the water's Map view, where there is no control to change it back (`09`: Conditions
   on satellite). Its Lakes choice is ignored on Conditions, while its `depth` group still follows. Conditions' own
   modes (Flow/Level/Temp, horizon) live in the legend and corner chips, so "what is this map coloured by" has 3
   different homes depending on the tab.
10. **Inconsistent controls.** Lakes/Basemap use swatch tiles (`OptionRow`). Palette uses filled pills (`Choice`)
    with a different inset (x=38 vs 20) and a filled accent (`02`, `03`). Section notes ("63,263 REACHES", "EVERY
    VALUE HAS AN AGE") read as headings and compete with the titles.
11. **Satellite has no availability state.** There is no offline note or disabled state (owner: "the Layers toggle
    says why satellite is unavailable"), and no vintage/resolution label ("10 m, 2016"; later Esri).
12. **No room to grow.** Every planned addition (contours as an overlay, private land, spots, pools/runs, lidar
    relief, imagery packs, Esri, gauges/arrows on other views) has no slot. Each would add another full-width
    section to an already scrolling sheet.

Not verified: the water's Map view and native (the shared Playwright browser was taken over by another agent's
session mid-walk, so I stopped). Per the code, the water's Map view has no Layers button and inherits `basemap`,
`modes` and `activeGroups` from Shell.

---

## 4. Constraints for a redesign

Owner rules (handoff/PLAN/app-integration/decisions.md, memory `app-port-direction`, `riffle-is-obsolete`):
- **ONE colouring pathway** for every map view and mode (`colouring.ts`, `mapModes` → `applyLook`). Values come from
  the release or feeds, never baked into tiles. A menu change is a change to `mapModes` inputs only.
- **Stocked / Depth: prepared, not fed.** Keep the plumbing. The toggles are disabled and greyed with a short
  "no data yet" note while the release has no lake values, and enable themselves (no app change) when it has them.
- **Satellite is online-only** (Esri World Imagery chosen), never cached. Offline, the drawn basemap is the ground,
  and the Layers toggle says why satellite is unavailable. Overzoom to z18 is in place.
- **Safety layers are not toggleable**: `protected` and `access` stay on (`$toggleable_groups`). `admin` is not
  toggleable either; views hide it by `views[].hide`, never through `groups` (the adapter refuses that).
- `ownership` is toggleable and off by default, on purpose ("title is context, not an instruction").
- The **Conditions map decides its own colouring** (no Layers button there today). If the rail shows Layers on
  Conditions, the sheet must show only what applies there (basemap, overlays), not lake modes it overrides.
- The **map rail** (Fish Washington style, lane map-rail-spots): Layers + Spots buttons stacked on the right; every
  button says its name aloud (`MAP_RAIL` words in `@app/core`).
- **Native-first** (Android + web from the same tree; iOS later). An open sheet is a back layer (G1: Android back
  closes it). Sheets drag to dismiss (`Sheet`, `useSheetDrag`).
- **Components draw view models** (AGENTS 25): decisions in `@app/ui` / `@app/core`, words in `@app/core`, no
  regulation logic in the app (owner rule 2026-10-21, backend answers).
- **Riffle is obsolete.** Design from the app's own tokens and components (`Sheet`, `OptionRow`, `Choice`,
  `MapRail`) plus owner direction.
- Counts come from `source.counts()` (APP-PREP P17 #6, AGENTS 32). Legend colours are read from the style tokens,
  never restated.

**Tokens (T22):**
- Map/theme tokens: `app/packages/map/style/tokens.json` and `app/packages/map/style/themes/{light,dark,cvd}.json`
  (built by `pnpm style:build`; `color.ui.*`, `color.stock.*`, `color.survey.*`, `color.ui.imagery.*`).
- App palette, `SPACE`, `GUTTER`: `app/packages/ui-native/src/theme.ts`. Type scale `TYPE`: `app/packages/ui-native/src/type.ts`.
- Ticket and mapping: `handoff/PLAN/app-integration/tickets/T22-theme-type-tokens.md`, `handoff/PLAN/app-integration/tokens.md`
  (values from `app/design/regs-v11/port/golden/tokens.json`).
