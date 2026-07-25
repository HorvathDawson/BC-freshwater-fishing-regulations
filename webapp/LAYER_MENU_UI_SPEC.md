# Layer Menu UI/UX Redesign — Implementation Spec

Status: design contract (draft). Design only — no application code in this document.

Scope: the map layer menu control in `webapp/src/components/Map.tsx`
(`layerMenuControl` IControl, markup class prefix `layer-menu-*`), its styles in
`webapp/src/components/Map.css`, the appearance pipeline in
`webapp/src/map/styles.ts` (`LAYER_STYLE_TARGETS`, `applyManifestStyle`,
`opacityLayerIds`), the manifest type in
`webapp/src/services/waterbodyDataService.ts` (`LayerManifestEntry` /
`LayerManifestStyle`), and the Python manifest source
`pipeline/tiles/layer_manifest.py` (`LAYER_MANIFEST`).

The tile/pipeline backend is NOT redesigned. This spec adds *menu-config* and
*documentation* fields to the manifest and defines a client-side override layer;
it does not change how tiles are generated or how data-driven expressions work.

---

## 0. Current state (baseline being replaced)

- One flat popup (`.layer-menu-popup`) opening to the side of a 29px button.
  Desktop position bottom-left, mobile top-right (`satPosition` in `Map.tsx`);
  popup opens right on desktop, left on mobile (`Map.css` `@media 768px`).
- Rows: a fixed **Satellite** checkbox row, a **global "Overlay opacity"**
  slider row shown *only* in satellite mode (multiplies every `regulations`-source
  layer's opacity via `baseOpacitiesRef` / `scaleOpacity`), then generated
  per-layer checkbox rows.
- Toggleable layers today (manifest `toggleable: true` **and** a
  `LAYER_STYLE_TARGETS` entry): `streams`, `lakes`, `wetlands`, `manmade`,
  `land_parcels_private`. Rows are built imperatively via `innerHTML` in the
  `[layerManifest, mapReady]` effect (~L1015–1033).
- Accent color throughout: `#7C3AED` (matches `SELECTION_COLOR` in `styles.ts`).

Problems this redesign fixes: (1) opacity is global, not per-layer; (2) no
per-layer info/source/color controls; (3) satellite mode overloads meaning
(basemap swap **and** overlay dimming); (4) no scroll model as the list grows;
(5) no persistence of appearance changes.

---

## 1. Design decisions (summary)

1. **Kill the global overlay-opacity multiplier entirely.** Delete the
   `overlayOpacity` state, the `.layer-menu-opacity-row`, `baseOpacitiesRef`,
   `scaleOpacity`, `OPACITY_PAINT_PROPS` multiply path, and
   `lastSatelliteOpacityRef`. Satellite becomes a **pure basemap swap**.
   Opacity becomes a **per-layer** property instead (Req 1).
2. **Basemap is its own control section**, not a layer row — a 2-option
   segmented switch **Map / Satellite** pinned at the top of the menu.
3. **Base water layers become individually toggleable** (`streams`, `lakes`,
   `wetlands`, `manmade`, plus `under_lake_streams`) so users control them
   directly regardless of basemap (Req 3).
4. **Two-view panel, one surface**: a scrollable **List view** ↔ a per-layer
   **Settings view**, with an in-panel **Back** button and **browser-back /
   `popstate`** integration (Req 2, 4).
5. **Manifest gains documentation fields** `description`, `sources`, and
   `group`; appearance defaults stay in `style` (Req 2 data model).
6. **User appearance/visibility edits persist to `localStorage`** as sparse
   overrides layered on top of manifest defaults, with per-layer and global
   **Reset to default** (Req 5).
7. **OSM trails / BC Forest Service Roads** slot into the same IA via a
   "Trails & Roads" group; `forest_service_roads` already exists in the
   manifest and `LAYER_STYLE_TARGETS` (Req 6).

---

## 2. Information architecture

### 2.1 Menu is a two-view panel

```
LayerMenu (panel)
├─ View: LIST  (default)
│   ├─ Header: "Map layers"                    [✕ close]
│   ├─ Basemap segmented control  [ Map | Satellite ]
│   ├─ Toolbar: "Reset all" (only if any override exists)
│   └─ Scroll region
│       ├─ Group "Water"          streams, under_lake_streams, lakes, wetlands, manmade
│       ├─ Group "Boundaries"     regions, wmu_boundary
│       ├─ Group "Land & Access"  parks_nat, eco_reserves, wma, historic_sites,
│       │                         watersheds, land_access, aboriginal_lands,
│       │                         land_parcels_private
│       ├─ Group "Trails & Roads" osm_trails(future), forest_service_roads
│       └─ Group "Points"         water_access_points, waterfalls (future-adjustable)
└─ View: SETTINGS  (one layer)
    ├─ Header: [‹ Back]  <Layer label>
    └─ Scroll region: Info · Sources · Appearance controls
```

Only entries with `toggleable: true` **and** a `LAYER_STYLE_TARGETS` mapping
appear as rows (unchanged gating rule from the current effect). Group order and
membership come from the new manifest `group` field; a layer with no `group`
falls into an "Other" group rendered last. Empty groups are not rendered.

### 2.2 Which layers become toggleable (manifest change)

Set `toggleable: true` on: `under_lake_streams`, `regions`, `wmu_boundary`,
`parks_nat`, `eco_reserves`, `wma`, `historic_sites`, `watersheds`,
`land_access`, `aboriginal_lands`, `forest_service_roads`. (`streams`, `lakes`,
`wetlands`, `manmade`, `land_parcels_private` already are.) Points
(`water_access_points`, `waterfalls`) and the future `osm_trails` are listed in
IA but are **out of scope for opacity/color** until they get `LAYER_STYLE_TARGETS`
entries — see §7 gating and §9 phases.

### 2.3 Interaction flows

**Open menu**: click layer button → panel opens in LIST view. Push history state
`{ layerMenu: 'list' }` (see §6).

**Toggle a layer**: checkbox on the row → `toggleLayerRef.current(key)` (existing
path) → also write `visible` override to `localStorage` (§5).

**Change inline opacity**: each row has a compact opacity slider. Dragging it
writes the appropriate opacity paint prop (`fill-opacity` for the layer's fill
ids, `line-opacity` for line ids — from `opacityLayerIds(key)`) and persists the
override. For polygons, the inline slider controls **fill** opacity only; border
opacity is edited in Settings (keeps the row compact; power controls live one
level down).

**Open per-layer settings**: click the cog on a row → panel swaps to SETTINGS
view for that layer. Push history state `{ layerMenu: 'settings', key }`. Move
focus to the Back button.

**Back out of settings**: in-panel **‹ Back** OR browser/hardware Back OR `Esc`
→ return to LIST view, restore scroll position, return focus to the cog that
opened it.

**Close menu**: header ✕, click-outside, or `Esc` while in LIST view → close
panel and clear the pushed history entry (`history.back()` if our state is on
top; see §6 for the guard).

---

## 3. Visual layout (wireframes)

Neutral palette matches the existing control (translucent white surface, subtle
shadow) rather than the neo-brutalist `.map-layer-menu`. Accent `#7C3AED`.

### 3.1 Expanded, scrollable LIST view

```
┌───────────────────────────────────────┐
│  Map layers                        ✕   │  ← sticky header
├───────────────────────────────────────┤
│  Basemap                               │  ← sticky sub-header
│  ┌──────────────┬──────────────────┐   │
│  │   ◉ Map      │   ○ Satellite    │   │  segmented control
│  └──────────────┴──────────────────┘   │
│                          [ Reset all ] │  ← only if overrides exist
├───────────────────────────────────────┤ ┐
│  WATER                                 │ │
│  ▣ ~ Streams        [====o----] ⚙     │ │
│  ▣ ~ Under-lake Str [==o------] ⚙     │ │
│  ▣ ▭ Lakes          [======o--] ⚙     │ │  scroll region
│  ▢ ▭ Wetlands       [====o----] ⚙     │ │  (flex:1; overflow-y:auto)
│  ▢ ▭ Manmade Water  [===o-----] ⚙     │ │
│  BOUNDARIES                            │ │
│  ▢ ~ Region Bounds  [======o--] ⚙     │ │
│  ▢ ~ Mgmt Unit Bnds [====o----] ⚙     │ │
│  LAND & ACCESS                         │ │
│  ▢ ▭ National Parks [==o------] ⚙     │ │
│  … (scrolls) …                         │ │
└───────────────────────────────────────┘ ┘
  ▣ = checked toggle · ▢ = unchecked
  ~  = line-geometry glyph · ▭ = polygon glyph · • = point glyph
  [==o--] = inline opacity slider · ⚙ = settings cog
```

### 3.2 A single layer row (anatomy)

```
┌─ .layer-menu-row ─────────────────────────────────┐
│ [✔] │ glyph  Label            │ [====o----] │ ⚙   │
│  ^toggle   ^type icon+label     ^opacity      ^cog │
└────────────────────────────────────────────────────┘
```

- **Toggle** (checkbox, `accent-color:#7C3AED`, ≥16px hit area, ≥44px row height
  on touch). Controls `visible`.
- **Type glyph + label**: line/polygon/point icon derived from manifest `type`
  (reuse the existing `LAYERS_SVG`; add `LINE_SVG`, `POLY_SVG`, `POINT_SVG`).
- **Inline opacity slider**: `min=0 max=1 step=0.05`; polygons → fill opacity,
  lines → line opacity. `aria-label="<Label> opacity"`, value announced as %.
- **Cog** (`⚙`, ≥44×44 touch target): opens Settings. `aria-haspopup="dialog"`,
  `aria-label="<Label> settings"`.

### 3.3 Per-layer SETTINGS view (polygon example — Lakes)

```
┌───────────────────────────────────────┐
│  ‹ Back          Lakes             ✕   │  header; Back is first focusable
├───────────────────────────────────────┤
│  ▭ Polygon layer                       │
│                                        │
│  About                                 │
│  Freshwater lakes and their shorelines │  ← manifest.description
│  from the BC Freshwater Atlas.         │
│                                        │
│  Data sources                          │
│  → BC Freshwater Atlas — Lakes    ↗    │  ← manifest.sources[]
│  → Named waterbodies dataset      ↗    │
│                                        │
│  Appearance                            │
│  Fill                                  │
│   Opacity   [======o--]        60%     │  → fill-opacity
│   Color     ■ #64B5F6   [swatches ▾]   │  → fill-color
│  Border                                │
│   Opacity   [========o]        80%     │  → line-opacity
│   Color     ■ #4A90E2   [swatches ▾]   │  → line-color
│                                        │
│  [ Reset this layer to default ]       │  ← only if this layer has overrides
└───────────────────────────────────────┘
```

Line layer (e.g. Streams): a single **Line** section — Opacity (`line-opacity`)
+ Color (`line-color`). No fill controls. Point layer (future): single Opacity +
Color once a `LAYER_STYLE_TARGETS` point entry exists.

**Zoom is intentionally NOT exposed.** `border_minzoom` stays manifest/code-owned;
users never see or set the zoom at which the border cuts in before the fill.

### 3.4 Color control detail

```
Color   ■ #64B5F6   [ ▾ ]
        └ opens palette popover:
        ┌──────────────────────────────┐
        │ ■ ■ ■ ■ ■ ■   (preset swatches)│  8–10 curated hues
        │ ■ ■ ■ ■ ■ ■                    │
        │ ───────────────────────────── │
        │ Custom  [ #64B5F6 ] [🎨 native]│  hex field + <input type=color>
        └──────────────────────────────┘
```

- Swatches are `button`s in a roving-tabindex grid; arrow keys move, Enter/Space
  select, Esc closes popover. Selected swatch shows a check + `aria-pressed`.
- Each swatch has a 1px `rgba(0,0,0,.25)` ring so light colors stay visible on
  the translucent surface (contrast, §8).
- "Custom" = hex `input` (validated `^#[0-9a-fA-F]{6}$`) + native color picker.

---

## 4. Data model — manifest additions

### 4.1 New per-entry fields (Python `LAYER_MANIFEST` + TS `LayerManifestEntry`)

Add three fields. Names are chosen to read cleanly in both Python and TS and to
avoid collision with existing keys:

| Field         | Type                              | Purpose |
|---------------|-----------------------------------|---------|
| `description` | `str` / `string`                  | 1–3 sentence plain-language blurb shown in Settings → About. |
| `sources`     | `list[{"name","url"}]` / array    | Ordered data-source links (Settings → Data sources). `name` = link text, `url` = https target. |
| `group`       | `str` / `string`                  | List-view grouping + order key. Allowed: `"water"`, `"boundaries"`, `"land"`, `"roads"`, `"points"`. Omitted → "Other". |

TypeScript (`webapp/src/services/waterbodyDataService.ts`,
`LayerManifestEntry`), add:

```ts
export interface LayerManifestSource { name: string; url: string; }
export interface LayerManifestEntry {
  // …existing: label, type, visible, toggleable, style, min_tile_zoom
  description?: string;
  sources?: LayerManifestSource[];
  group?: 'water' | 'boundaries' | 'land' | 'roads' | 'points';
}
```

Python (`pipeline/tiles/layer_manifest.py`) — example enrichment for `lakes`:

```python
"lakes": {
    "visible": True, "toggleable": True, "label": "Lakes", "type": "polygon",
    "group": "water",
    "description": "Freshwater lakes and their shorelines from the BC "
                   "Freshwater Atlas.",
    "sources": [
        {"name": "BC Freshwater Atlas — Lakes",
         "url": "https://catalogue.data.gov.bc.ca/dataset/freshwater-atlas-lakes"},
    ],
    "style": {"fill_color": "#64B5F6", "fill_opacity": 0.4,
              "line_color": "#4A90E2", "line_opacity": 0.8},
},
```

`description`/`sources`/`group` are **documentation/menu config** — like `label`,
they are ignored by tile generation and safe to add without a pipeline run
(regenerate JSON via `python -m pipeline.tiles.layer_manifest …`). Missing
`description`/`sources` render graceful fallbacks (see §7).

### 4.2 No new `style` sub-keys required

Fill vs border are already fully separable via existing
`fill_color`/`fill_opacity` and `line_color`/`line_opacity`, and
`applyManifestStyle` already writes them onto the fill/line ids in
`LAYER_STYLE_TARGETS`. The Settings view maps 1:1 onto these. Nothing new needed
in `LayerManifestStyle`.

---

## 5. Data model — client-side overrides (localStorage)

### 5.1 Storage key & schema

```
Key: "bcfish.layerOverrides.v1"
```

```jsonc
{
  "version": 1,
  "layers": {
    "lakes":   { "fill_opacity": 0.6, "fill_color": "#3E8FD8" },
    "streams": { "visible": false },
    "manmade": { "line_opacity": 0.5 }
  }
}
```

Per-layer override object (all keys optional / sparse — only edited fields are
stored):

| Key            | Type    | Applies to        | Maps to paint prop on `LAYER_STYLE_TARGETS[key]` |
|----------------|---------|-------------------|--------------------------------------------------|
| `visible`      | boolean | all               | menu toggle / layer visibility (existing path)   |
| `fill_opacity` | 0..1    | polygon fill ids  | `fill-opacity`                                   |
| `fill_color`   | hex     | polygon fill ids  | `fill-color`                                     |
| `line_opacity` | 0..1    | line/border ids   | `line-opacity`                                   |
| `line_color`   | hex     | line/border ids   | `line-color`                                     |

Mirrors `LayerManifestStyle` sub-keys plus `visible`, so the merge is trivial.

### 5.2 Precedence & merge rules

Effective value, resolved per layer per property:

```
code base style-layer default   (lowest — createRegulationLayers, expressions)
   └► manifest.style[key]        (applyManifestStyle writes flat keys)
        └► localStorage override (highest — user edit)
```

- A property **present** in the override wins. A property **absent** falls
  through to the manifest default, then to the coded default. Overrides are
  sparse: never store a value equal to the manifest default (store = set;
  reset = delete the key).
- **Apply order at runtime**: keep the existing `applyManifestStyle(map,
  manifest)` call, then run a new `applyUserOverrides(map, overrides)` pass
  immediately after (same effect that currently runs on `[layerManifest,
  mapReady]`). Override application uses `opacityLayerIds(key)` for opacity and
  the fill/line id groups for color, calling `map.setPaintProperty` — the same
  primitive `applyManifestStyle` already uses.
- **Data-driven-color layers** (manifest omits `fill_color`/`line_color`
  because the value is a `match`/`interpolate` expression — e.g. `regions`,
  `eco_reserves`, `land_access`, `forest_service_roads` dash): a user color
  override **replaces the whole expression with the chosen flat color** via
  `setPaintProperty`. This is acceptable and reversible — **Reset** removes the
  override and the code re-applies its default expression on the next
  style/override pass. In Settings, such layers show an inline note: "Setting a
  color replaces this layer's automatic colouring." Opacity overrides on these
  layers are always safe (flat prop).

### 5.3 Reset affordances

- **Per layer**: Settings → "Reset this layer to default" deletes
  `layers[key]`, re-applies manifest defaults to that layer's ids, re-renders
  controls to manifest values. Shown only when `layers[key]` is non-empty.
- **Global**: List header → "Reset all" clears the whole `layers` map. Shown
  only when at least one override exists.
- On reset, for data-driven layers, restore the coded default by re-running the
  layer's base spec (simplest: re-run `applyManifestStyle` + re-init that
  layer's style spec) rather than trying to reconstruct the expression from the
  override.

### 5.4 Robustness

- Wrap read/write in try/catch; on parse error or `version` mismatch, ignore
  and start clean (defaults render — never block the map on bad storage).
- Debounce writes (~150ms) while dragging sliders.
- Unknown layer keys in storage (e.g. a layer removed from the manifest) are
  ignored on apply and pruned on next write.

---

## 6. Browser-history / back-button integration

Goal (Req 2): browser/hardware **Back** closes the Settings popup (and, on
mobile, the whole menu) — matching native app expectations without trapping the
user on the page.

State machine driven by `history.pushState` + a `popstate` listener:

```
closed ──open menu──►  push {layerMenu:'list'}   ── LIST
LIST   ──open cog──►   push {layerMenu:'settings',key} ── SETTINGS
SETTINGS ─Back/‹/Esc─► history.back()  ─popstate─► LIST
LIST   ─✕/outside/Esc► history.back()  ─popstate─► closed
```

Rules:

- Maintain a `menuHistoryDepthRef` counter of how many entries WE pushed. On UI
  close actions, call `history.back()` only if `menuHistoryDepthRef > 0` (so we
  never pop the user off the app); otherwise just set React state closed.
- `popstate` handler reads `event.state?.layerMenu` and reconciles the panel:
  `'settings'` → SETTINGS(key); `'list'` → LIST; absent → closed. This makes the
  hardware Back button collapse SETTINGS→LIST→closed one step at a time.
- Guard against double-push: only push when transitioning *into* a deeper view,
  never on re-render.
- On component unmount / map teardown, remove the `popstate` listener and, if
  `menuHistoryDepthRef > 0`, pop our states.
- This does **not** touch the existing `urlState.ts` feature-selection history;
  keep the layer-menu state keyed under its own `layerMenu` field so the two
  don't collide.

Because the current menu is imperative DOM inside an IControl, implementing the
two-view swap + history cleanly is easier if the popup body is rendered by a
small React component (e.g. `LayerMenuPanel`) mounted via `createPortal` into the
control's `wrapper` element from `onAdd`. Recommended, but an
`innerHTML`-per-view swap is acceptable if kept behind the same state machine.

---

## 7. Rendering / gating rules (edge cases)

- **Row gating** (unchanged): render a row only for entries with
  `toggleable === true` **and** `LAYER_STYLE_TARGETS[key]` present. This keeps
  points and future `osm_trails` out until they get style targets.
- **Which appearance controls to show**, by `type`:
  - `polygon` → Fill (opacity+color) **and** Border (opacity+color), only for
    the sub-ids that exist in `LAYER_STYLE_TARGETS[key]` (`fill` group → Fill,
    `line` group → Border). If a polygon has no `fill` ids, hide the Fill block.
  - `line` → single Line section (opacity+color).
  - `point` → single section (future; needs a point `LAYER_STYLE_TARGETS`
    entry + point opacity prop).
- **Color control disabled/annotated** when the manifest omits the color key
  because it is data-driven — still editable (override replaces expression, §5.2)
  but annotated.
- **Missing `description`** → hide the About block (no empty heading).
- **Missing/empty `sources`** → hide the Data sources block.
- **Inline row opacity** reflects the *effective* value (override → manifest →
  code default). If the layer is toggled off, the slider is disabled/dimmed.
- **External links** (`sources[].url`) render as `<a target="_blank"
  rel="noopener noreferrer">` with a trailing ↗ glyph. Treat `url`/`name` as
  untrusted content from the manifest: render as text/href only, never as HTML;
  enforce `https:` scheme (drop/omit any non-http(s) link).

---

## 8. Accessibility

- **Roles**: panel is `role="dialog"` `aria-modal="false"` (it's a
  non-blocking control popover) with `aria-label="Map layers"`. Settings view
  updates the dialog's `aria-label` to the layer name.
- **Keyboard**:
  - Layer button: `Enter`/`Space` toggles the menu.
  - `Tab` order in LIST: basemap segmented control → (reset all) → each row's
    toggle → inline slider → cog, group by group.
  - Row toggle = native checkbox. Inline slider = native `range` (arrow keys
    ±step, Home/End). Cog = `button`.
  - Settings: first focusable is **‹ Back**. Color swatch grid uses roving
    tabindex (arrows to move, Enter/Space to pick).
  - `Esc`: in SETTINGS → back to LIST; in LIST → close menu. In an open color
    popover → close popover only.
- **Focus management**:
  - Opening Settings moves focus to Back.
  - Returning from Settings moves focus back to the **cog** that opened it
    (store the trigger element ref).
  - Closing the menu returns focus to the layer button.
  - Focus is *contained* within the panel while open (wrap Tab at the ends) but
    not hard-trapped from the browser chrome.
- **Announcements**: sliders expose `aria-valuetext="60%"`; toggles use the
  layer label as accessible name; live-region toast (polite) on "Reset" actions.
- **Color contrast**:
  - Text/label/percent readouts: keep `#333` on the translucent white surface
    (≥4.5:1). Do not go lighter than `#595959` for any text.
  - Swatches carry a 1px `rgba(0,0,0,.25)` border + focus ring so light hues
    (e.g. `#64B5F6`, `#81C784`) remain distinguishable and focus is visible at
    ≥3:1 against the surface.
  - Selected swatch state is conveyed by a check icon **and** `aria-pressed`,
    not color alone.
  - The accent `#7C3AED` is used only for controls (thumbs, checks), never as
    the sole carrier of meaning.
- **Touch targets**: rows ≥44px tall on touch; cog and swatches ≥44×44 hit area
  (visual can be smaller with padding).

---

## 9. Mobile vs desktop behavior

- **Control position** (unchanged): desktop **bottom-left**, mobile **top-right**
  (`satPosition = isMobileViewport() ? 'top-right' : 'bottom-left'`).
- **Panel presentation**:
  - **Desktop**: floating panel anchored to the button (opens right, per current
    `left: calc(100% + 6px)`), `max-height: min(70vh, 560px)`, internal scroll,
    `min-width: 260px` `max-width: 320px`.
  - **Mobile**: because the button is top-right and the list is now tall, the
    panel becomes a **bottom sheet** (full width, rounded top, `max-height:
    80vh`, momentum scroll, safe-area inset padding reusing the existing
    `env(safe-area-inset-bottom)` handling). This avoids the tiny left-opening
    popup and gives room for Settings. The hardware Back button closing the
    sheet (§6) is the key mobile affordance the user asked for.
- **Scroll**: only the group scroll-region scrolls; header + basemap control are
  sticky. Inline sliders must not hijack vertical scroll on touch — the slider
  gets `touch-action: none` only while the thumb is actively dragged.
- Respect `prefers-reduced-motion` for the view-swap and sheet transitions.

---

## 10. Satellite / clunkiness recommendation (chosen model + rationale)

**Chosen model: Basemap is a mutually-exclusive segmented switch, decoupled from
all overlays. Delete the global overlay-opacity feature.**

- Top of the LIST view: a 2-option segmented control **[ Map | Satellite ]**
  that only swaps the basemap raster/vector base. It does **not** touch any
  overlay opacity.
- Every water/overlay layer is an ordinary toggleable row with its own opacity
  (Req 1 + Req 3). If a user wants faint overlays over satellite, they dim the
  individual layers — persisted like any other override.

**Why this over the alternatives:**

1. *One meaning per control.* Today "Satellite" secretly also means "dim
   everything," and the opacity slider only exists in satellite mode — a hidden
   modal coupling that surprises users ("why did my layers fade?"). Splitting
   basemap (a choice) from layer opacity (per-layer properties) removes the
   coupling the user flagged as "clunky."
2. *Matches mental model of every mapping app* (Google/Apple/Mapbox): base map
   type is a switch; overlays are independent toggles. Low learning cost.
3. *Simpler code + state.* Removes `overlayOpacity`, `baseOpacitiesRef`,
   `scaleOpacity`, `OPACITY_PAINT_PROPS` multiply path, and the show/hide
   `.layer-menu-opacity-row` effects. Per-layer opacity reuses the already-clean
   `opacityLayerIds(key)` + `setPaintProperty` path.
4. *Persistence falls out naturally.* Per-layer opacity is a first-class
   override (§5); there is no awkward "global multiplier" to persist or reconcile
   against per-layer values.

Rejected: (a) keep a global overlay-opacity slider always visible — still couples
unrelated layers and fights per-layer opacity; (b) make Satellite a normal layer
row with opacity — a basemap isn't an overlay and shouldn't be dimmed/reordered
like one; the segmented control communicates exclusivity better than a checkbox.

---

## 11. Phased implementation checklist (dependency-ordered)

**Phase A — Data model foundations (no visible UI change)**
1. Add `description`, `sources`, `group` to Python `LAYER_MANIFEST`
   (`pipeline/tiles/layer_manifest.py`) for all currently-toggleable layers;
   set `toggleable: true` on the new water/boundary/land/road layers (§2.2).
2. Mirror the fields in `LayerManifestEntry` + add `LayerManifestSource`
   (`waterbodyDataService.ts`).
3. Regenerate `layer_manifest.json`
   (`python -m pipeline.tiles.layer_manifest`), verify frontend still loads.
4. Add the localStorage module: read/merge/write for
   `bcfish.layerOverrides.v1` with the precedence rules (§5). Add
   `applyUserOverrides(map, overrides)` in `styles.ts` alongside
   `applyManifestStyle`; call it in the existing apply effect.

**Phase B — Remove global overlay-opacity, add basemap segmented control**
5. Delete `overlayOpacity` state, `.layer-menu-opacity-row`, `baseOpacitiesRef`,
   `scaleOpacity`, `OPACITY_PAINT_PROPS` multiply path,
   `lastSatelliteOpacityRef`, and their effects in `Map.tsx`.
6. Replace the fixed Satellite checkbox row with the **[ Map | Satellite ]**
   segmented control at the top of the panel; `toggleSatellite` becomes a pure
   basemap swap.

**Phase C — List view: rows with inline opacity + cog + grouping**
7. Rebuild the row template: toggle + type glyph + label + inline opacity slider
   + cog. Group rows by manifest `group` with sticky sub-headers.
8. Wire inline opacity → `opacityLayerIds(key)` + `setPaintProperty` → persist
   override (debounced). Wire toggle → existing `toggleLayer` + persist
   `visible`.
9. Make the panel scrollable (sticky header/basemap; `overflow-y:auto` region);
   desktop floating panel + mobile bottom sheet (§9).

**Phase D — Settings view + history integration**
10. Introduce the two-view state machine (LIST ↔ SETTINGS) — recommended as a
    portaled `LayerMenuPanel` React component in the IControl `wrapper`.
11. Build the Settings view: About (`description`), Data sources (`sources`),
    Appearance — fill vs border sections per §3.3/§7, color popover per §3.4.
12. Add `pushState`/`popstate` back-navigation with the depth guard (§6);
    in-panel Back, `Esc`, ✕, click-outside.

**Phase E — Reset, a11y, polish**
13. Per-layer "Reset this layer" + global "Reset all" (§5.3), with
    data-driven-layer restore path.
14. Accessibility pass: roles, focus management, roving-tabindex swatches,
    contrast rings, `aria-valuetext`, reduced-motion (§8).
15. CSS cleanup in `Map.css`: retire `.layer-menu-opacity-*`; add
    `.layer-menu-panel`, `.layer-menu-group`, `.layer-menu-row` (revised),
    `.layer-menu-settings`, `.layer-menu-swatch*`, bottom-sheet media query.

**Phase F — Future layers (separate work, unblocked by A–E)**
16. Add `osm_trails` (default `visible: true`) and confirm
    `forest_service_roads` (default off per Req 6) manifest entries + a
    `LAYER_STYLE_TARGETS` entry for `osm_trails`; they render into "Trails &
    Roads" automatically.
17. (Optional) Give `water_access_points` / `waterfalls` point
    `LAYER_STYLE_TARGETS` entries + a point opacity prop to make them
    adjustable in the same menu.

---

## 12. File touch-list (for the implementer)

| File | Change |
|------|--------|
| `pipeline/tiles/layer_manifest.py` | Add `description`/`sources`/`group`; expand `toggleable`. |
| `webapp/src/services/waterbodyDataService.ts` | Extend `LayerManifestEntry`; add `LayerManifestSource`. |
| `webapp/src/map/styles.ts` | Add `applyUserOverrides`; reuse `opacityLayerIds`/`LAYER_STYLE_TARGETS`; add `osm_trails` target (Phase F). |
| `webapp/src/components/Map.tsx` | Remove overlay-opacity machinery; basemap segmented control; two-view panel + history; row rebuild; overrides wiring. |
| `webapp/src/components/Map.css` | Retire `.layer-menu-opacity-*`; add panel/group/settings/swatch/bottom-sheet styles. |
| `webapp/src/utils/` (new) | `layerOverrides.ts` — localStorage read/merge/write. |
```
