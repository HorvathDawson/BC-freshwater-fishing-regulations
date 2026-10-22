# Layers menu: recommendation (2026-10-21)

Judged from the three mockups in this folder (`a-question-first`, `b-base-colour-overlays`, `c-compact-rail-popover`,
HTML and PNG, light and dark, five states each) against `inventory.md`, today's `LayersSheet.tsx`, `ui/src/mapModes.ts`,
`ui/src/lakeColour.ts` and the `wip/map-rail-spots` lane (`core/src/mapRail.ts`, `ui-native/src/MapRail.tsx`).
riffle.html is obsolete and was not used. The only design references are the app's tokens and components.

Note: the branch already greys Stocked and Depth (`f13f623a`, `lakeModesReady`, `lakeModesNote`). The inventory's
"Stocked/Depth enabled on main" item is fixed on the branch, but not yet in the sheet's structure.

## Scores (1 to 5)

| Criterion | A: by question | B: base / colour / overlays | C: compact + More layers |
|---|---|---|---|
| Fixes the clunky list | 5 | 4 | 3 |
| Phone reach (thumb, fewest taps for the common change) | 2 | 3 | 5 |
| Mode vs overlay clarity | 3 | 5 | 4 |
| Per-view relevance | 5 | 4 | 4 |
| Honest disabled / online-only states | 5 | 5 | 4 |
| Future layers slot in | 5 | 4 | 5 |
| Fits the app's current look | 4 | 4 | 5 |
| Implementation cost (5 = cheapest) | 2 | 3 | 4 |
| **Total / 40** | **31** | **32** | **34** |

Why:

- **A** fixes the most items. Its legend sits inside the chosen tile, which solves both the 613 px strip overflow and
  Stocked taking over the strip. It drops "Plain", gives every non-default layer a chip on the closed map, and orders
  questions per view. It is also the tallest sheet: Display falls below the fold. Lake colour is split across two
  sections, which is why it needs the "INSTEAD OF REGULATIONS" and "Streams only, lakes show Stocked" labels. It also
  needs the most new pieces: question sections, link rows, map chips, and Conditions modes moved into the sheet, which
  would give flow two controls.
- **B** has the clearest model: Map, then Colour the water, then On the map. Splitting contours out of Depth into
  their own switch is right. The Conditions colour line is read-only. Its "SHOWING" tag on the Map card plus a reason
  callout is the best offline state of the three. Against it: about 1000 px tall, it adds a stream "Plain" mode the app
  does not have, and it shows two dead hatched tiles in the most-used row. The preview cards are a new component.
- **C** is the only design that answers "it feels clunky" on a phone. It is a half-sheet, the common change is one tap
  away, and everything else sits on one More layers page that can grow without a redesign. It reuses OptionRow,
  Choice and Sheet as they are. Its weak points are what it leaves unsolved: no legend for Stocked or Depth, the
  strip overflow untouched, an 8 px rail mark that is easy to miss, and the rank rule moving contours between pages
  depending on the view.

## Recommendation: C (compact front page + More layers), with grafts

C is the base. The front page covers about half the screen, the map stays readable, and future layers go onto More
layers as registry rows without new layout. The grafts below cover the problems C leaves open.

### Grafts

1. **Legend inside the chosen tile (from A).** The chosen lake-colour tile (Regulations, Stocked, Depth) shows its
   key wrapped under the tile row at sheet width, with colours read from style tokens. The Map-tab legend strip keeps
   only the streams' status key and the day stepper, and Stocked no longer replaces it. This fixes the 613 px
   overflow and the strip takeover. The strip itself must also wrap at 390 px, as a separate small fix.
2. **Offline satellite: "SHOWING" plus a reason line (from B, with A's dashed "kept" frame).** The reader's choice
   stays recorded. The drawn-map tile is tagged SHOWING, the satellite tile is greyed and outlined with a dashed accent
   frame and the label NEEDS A CONNECTION, and one line says "Esri imagery streams online only; your choice comes back
   by itself." `mapModes` receives the basemap actually in use, not the reader's choice.
3. **Contours become their own overlay (from B), and Depth still switches them on.** `lake.depth` keeps
   `groups: ["depth"]` (today's behaviour, and Depth with no lines would look empty). A separate `lake.contours` switch on
   More layers covers "contours over the regulation colours". This resolves the mode-vs-overlay mix while keeping the
   current behaviour.
4. **Rail badge with a number, plus one state line on the closed map (from A, toned down).** Replace C's 8 px
   square with a numbered badge on Layers (spoken: "Choose layers, 2 changed"). Satellite showing and lakes not on
   Regulations each get a single small chip under the rail ("Satellite", "Lakes: Stocked"). Chips appear only for slot
   choices (ground, lake colour), not for overlays, to keep the map quiet.
5. **Fixed pages, not rank promotion.** Use a per-entry `front` view list instead of C's "top 3 by rank". A control
   then stays where it was on every view, and only entries that do not apply drop out. On Conditions the front page is
   Ground plus Gauges and Flow arrows (B's Conditions switches, with gauge age in the Gauges row's note). Lake colour
   is hidden there and one line says "Colour follows Showing" (C and B agree on this).
6. **"Always shown" locked line and a single Sources row (B and C agree).** The Streams paragraph, the Live data rows
   (including the stocking feed that does not exist) and the six credit lines leave the sheet. Sources and credits
   becomes one row on More layers that opens the menu-info surface once that lane lands. Until it does, the row
   opens a plain list sheet holding the existing `attribution` lines.
7. **No stream "Plain" (rejects B).** Streams have no choice on the Map tab today, and none is added. The default
   lake choice is renamed from "Plain" to what it paints. The word is an owner question (Q1).

### What was not taken

- A's question sections and moving Flow/Level/Temp into the sheet. That would give flow two controls, and the strip
  chips already work.
- B's preview cards. OptionRow swatch tiles are enough, and the cards are a new component for the same choice.
- Showing Palette on the front page. It is a whole-app setting, so it goes on More layers under "Whole app" for now.
  Whether it moves to the hamburger menu is Q4.

## Final layer registry shape (`@app/core/src/layers.ts`)

Words go in `LAYER_WORDS` in @app/core, and the component holds none. Swatches are token names. An entry may set only
`mapModes` inputs, so there is still one colouring pathway (`colouring.ts` via `applyLook`).

```ts
export type ViewId = "map" | "water" | "conditions" | "search";
export type LayerKind = "base" | "colour-mode" | "overlay" | "setting";
export type LayerGroup = "ground" | "lakes" | "streams" | "land" | "conditions" | "display";
/** Entries sharing a set form one radio group (tiles). No set = a switch. */
export type LayerSet = "ground" | "lakeColour" | "palette";

export interface LayerEntry {
  id: string;
  group: LayerGroup;                       // heading on More layers
  kind: LayerKind;
  set?: LayerSet;
  views: readonly ViewId[];                // hidden (not greyed) elsewhere
  front: readonly ViewId[];                // on the front page for these views; otherwise on More layers
  default: boolean;
  sets: {                                  // ONLY mapModes / Map inputs
    basemap?: "map" | "satellite" | "corridors";
    lakeMode?: string;                     // must exist in the style's lake colour modes
    groups?: readonly string[];            // $toggleable_groups only (ownership, depth, ...); never protected/access/admin
    runtime?: "gauges" | "arrows" | "spots" | "pools";
    theme?: ThemeName;
  };
  needs?: {                                // availability is DERIVED, never set by hand
    release?: keyof LakeModesReady | string;  // the release must carry it  -> no-data
    network?: true;                        // streamed                     -> online-only (offline)
    download?: string;                     // a pack the reader must hold  -> no-data ("none downloaded")
    built?: false;                         // declared before it ships     -> coming-soon
  };
  swatch?: readonly [TokenRef, TokenRef, TokenRef];
  legend?: "status" | "stock" | "survey";  // drawn under the set when chosen (graft 1)
  count?: CountKey;                        // source.counts() key for the note
  chip?: true;                             // a non-default choice shows a closed-map chip (graft 4)
  words: LayerWordsKey;
}

export type Availability =
  | { state: "ok" }
  | { state: "no-data"; reason: LayerReason }
  | { state: "online-only"; reason: LayerReason }
  | { state: "coming-soon"; reason: LayerReason };

export function availability(e: LayerEntry, f: {
  carries(k: string): boolean; online: boolean; downloaded(k: string): number;
}): Availability;   // order: built -> release -> download -> network

/** The sheet's view model: pure, tested whole. */
export function layersVM(view: ViewId, choice: LayerChoice, f: Facts, build: "release" | "dev"): {
  front: SectionVM[]; more: SectionVM[]; changed: number; chips: string[];
  effective: { basemap: Basemap; lake: { mode: string; group?: string }; groups: Record<string, boolean>;
               runtime: Record<string, boolean> };   // what mapModes gets: unavailable choices fall back
};

export const ALWAYS_ON = ["protected", "access", "admin"] as const;  // named once, never entries
```

Entries for day one: `ground.map`, `ground.satellite` (needs network), `lake.status` (default), `lake.stocked`
(needs release `stocked`), `lake.depth` (needs release `surveyed`, sets groups depth), `land.private` (groups
ownership, default off, front on map/water), `lake.contours` (groups depth, More), `cond.gauges` and `cond.arrows`
(conditions only, front), `app.light`/`app.dark`/`app.cvd` (More, Whole app). Future entries are declared with
`built: false`: `ground.corridors`, `stream.pools`, `ground.relief`, and `spots.mine` if Q3 says spots are a layer.
None of them needs a sheet change.

The reader's choice (`LayerChoice`: one id per set plus the on/off switches) is the only state the Shell keeps. It
replaces `LayersState` and the separate `groups` useState.

## Implementation plan (after `wip/map-rail-spots` merges)

The rail lane owns the Layers button's position and its MAP_RAIL words. This work starts on top of it, so it never
touches MapScreen's old pill.

1. **`@app/core/src/layers.ts` + `layers.test.ts`.** Registry, `availability`, `layersVM`, `LAYER_WORDS`. Tests:
   every `sets.groups` is a `$toggleable_groups` group in `layers.source.json`, so protected/access/admin can never
   be an entry. Every `lakeMode` exists in the style. Every swatch and legend token resolves in light, dark and cvd.
   Each availability branch is tested. `changed` counts only differences from `default`. An unavailable choice gives
   its fallback in `effective` and keeps the choice. Coming-soon entries are dropped in `build: "release"`.
   Mutation-pin the derivation: a test must fail if `needs.release` is ignored (memory: checks seeded from their own
   input launder).
2. **`ui/src/mapModes.ts`.** No new rules. `MapModeInput.lake` / `groups` / basemap come from
   `layersVM(...).effective`. Delete `lakeModeOn` from the call path, since availability now covers it. Keep
   `mapModes.test.ts` green and add one case per entry that sets a mode, so the one-pathway audit stays true.
3. **`ui/src/online.ts`.** An `online` fact: `navigator.onLine` plus events on web. On native, either the
   imagery source's tile-error signal or NetInfo (a new dependency, so ask first, given "minimal prod packages").
   Until Esri lands, `ground.satellite` reports ok online and draws the current EOX source.
4. **`ui-native/src/LayersSheet.tsx` rewrite.** It draws the VM only: tile rows (OptionRow), switch rows, a "More
   layers" row, a second page with a back chevron in the same Sheet, the legend under a chosen tile, NO DATA YET /
   NEEDS A CONNECTION / COMING tags, the locked line, and the Sources row. Remove the fixed 520 px ScrollView. The
   front page sizes to content, and More scrolls. Remove the riffle.html header comment and `STOCK_BANDS` (it moves
   to a legend from tokens). `Sheet.tsx` may need a `back` prop for the sub-page (title chevron). That is a small
   change to the existing component, not a new one.
5. **`ui-native/src/MapRail.tsx`.** Badge with `changed` and the spoken label; chips under the rail from
   `chips`. The Layers button shows on Conditions too (Q2).
6. **`ui-native/src/Shell.tsx`.** Replace `layers` and `groups` state with `LayerChoice`, and persist it as today.
   Pass `effective` to every map (Map tab, water map, search map, Conditions), so the basemap no longer carries over
   with no control. Remove the old Map-tab strip takeover when Stocked is chosen. The strip wraps at 390 px.
7. **Tests.** `LayersSheet.test.tsx` rewritten against the VM (front/more split per view, disabled states, legend
   under the chosen tile, no Streams paragraph, no Live data rows). `MapRail.shell.test.tsx`: badge count and label.
   A Shell render test: choosing Satellite while offline draws the map basemap and keeps the choice. Conditions
   shows no lake tiles.
8. **House checks, once at the end** (memory: suites once): `pnpm run check` in `app/` (boundaries, platform, style,
   colours, deps, typecheck, vitest), run unfiltered and not through rtk (memory: rtk pytest false pass). Then
   `graphify update .`. Screenshot the sheet at 390x844 in light, dark and cvd on web, and on Android if available,
   and compare against `c-compact-rail-popover.png` with the grafts applied.

Out of scope here: Esri source swap (its own queued lane), corridor packs, pools, relief, the menu-info lane's
Sources page. Each arrives later as a registry entry or a link target.

## Owner questions (taste)

1. **What is the default lake choice called?** "Regulations" (B), "Status" (C), or "Today's rules"? It is the
   word a reader sees most.
2. **Should Layers show on the Conditions map?** All three put it on the rail there, which changes today's
   Map-tab-only rule. If yes, the Conditions sheet offers Ground, Gauges and Flow arrows. If no, satellite needs
   another off switch there, or Conditions always draws the map basemap.
3. **Do saved spots draw as a layer with a switch?** Or do they stay always on, managed only from the rail's Spots
   button?
4. **Palette: keep it on More layers under "Whole app", or move it to the hamburger with Sources and credits?**
   Moving it would leave the Layers menu map-only.
5. **Coming-soon entries: show them dimmed in release builds (A, C), or only in dev builds (B)?** Stocked and Depth
   stay visible greyed either way, by the existing owner rule.
