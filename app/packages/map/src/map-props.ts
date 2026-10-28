/**
 * What an app may ask a map for. Identical on both platforms by construction — a prop that
 * only one renderer honours is how the two maps start diverging.
 */
// A feature id on our own layers IS a section handle — an integer. `SectionKey` keeps
// this honest without `map` having to know which artifact minted it.
import type { SectionKey, SpotMarker, SpotPoint } from "@app/core";
import type { ReactNode } from "react";
import type { ViewStyle } from "react-native";
import type { TileEndpoints } from "./endpoints";
import type { TraceProp } from "./trace-overlay";
import type { LakeFeed, MethodFeed, ProfileStatus, SalmonFeed, StandingFeed } from "./colouring";
import type { SelectionCue } from "./paint";

/** A boundary mark on a water (`MapProps.marks`). `a11y`: what a screen reader says for it. */
export interface MapMark {
  lon: number;
  lat: number;
  label: string | null;
  a11y: string;
  on: boolean;
}

/** (T131) The reader's saved spots on a map (`MapProps.spots`). */
export interface MapSpots {
  points: readonly SpotPoint[];
  /** The spot drawn selected (last opened or shown), or null. */
  selected: string | null;
  /** A spot's mark was tapped. */
  onOpen(id: string): void;
  /** A group no zoom parts (spots on one point) was tapped: its spots. */
  onList(ids: readonly string[]): void;
  /**
   * Draws one mark's face, inside the map's 44 px target (centred on the point). The app's: the icon set lives above
   * the map. `press` is the map's (`pressSpot`), to be called by the face's one button.
   */
  face(m: SpotMarker, press: () => void): ReactNode;
}

export interface Camera {
  lon: number;
  lat: number;
  zoom: number;
}

/**
 * The few colours MapLibre's own controls need, from the app's palette.
 *
 * `controls.css` styles the zoom stack, compass and scale bar through CSS variables, and
 * NOTHING HAD EVER SET THEM — every rule fell through to a light-theme literal, so the dark
 * map wore a white control stack. A `var()` fallback is indistinguishable from a value that
 * was supplied, which is why it went unnoticed.
 *
 * Derived from the theme's `color.ui.*` tokens (`mapChrome` in ./chrome.ts) — the same tokens
 * `palette` in @app/ui-native is built from, so the controls and the app around them are one
 * palette. The prop remains only as an override.
 */
export interface MapChrome {
  /** Frame, dividers and glyphs. */
  ink: string;
  /** The control's own ground. */
  card: string;
  /** Pressed state. */
  tint: string;
  /** The scale bar's bracket and label. */
  sub: string;
  /**
   * The "you are here" pin, and the default for a panel's pins.
   *
   * WAS A LITERAL. `Map.web.tsx` built both markers with `"#5F26E0"` typed in, which is the
   * LIGHT theme's accent — so the marker for the spot you tapped stayed light-purple on the
   * dark theme, where the accent is #A97CFF, and ignored the colour-blind theme entirely.
   * A colour that only one theme agrees with is not a theme's colour.
   */
  accent: string;
  /** The hard offset slab under the stack. Not a blur — see controls.css. */
  shadow: string;
  /**
   * A CSS `filter` for MapLibre's own glyphs.
   *
   * Its control icons are background-image SVGs with near-black strokes baked in, so on a
   * dark control they are black on near-black and the zoom buttons cannot be read. There is
   * no colour to set — the image has to be inverted. `"none"` on light.
   */
  iconFilter: string;
}

export interface MapProps {
  /** Colours for MapLibre's own controls. Omitted -> `mapChrome(theme)`, from the tokens. */
  chrome?: MapChrome;
  /**
   * Drop the renderer's own furniture: zoom stack, compass, scale bar, attribution.
   *
   * For a small map that ILLUSTRATES rather than one you navigate. The route map under an
   * estimate is 210 px tall and fixed on one camera; a zoom stack and a compass over it are
   * controls for a map nobody is driving, and they cover the pins that are the point of it.
   *
   * ATTRIBUTION DOES NOT DISAPPEAR WITH THEM — it moves. A page carrying a bare map has to
   * credit the data somewhere on that page, and doing it once at the foot of the screen is
   * both better reading and the only sensible place for the hydrometric and forecast
   * credits, which are not the basemap's and were never in that control anyway.
   */
  bare?: boolean;
  /**
   * The zoom buttons, even on a `bare` map. For a small map a reader is invited to EXPLORE
   * — the Conditions route map (owner 2026-10-21: "the gauge map should have the ability to
   * zoom and look at data better") — where pinch and wheel alone are undiscoverable on a
   * desktop and a sheet's scroll competes with them on a phone. No compass: north is up.
   */
  zoomControls?: boolean;
  /**
   * THE ROUTE OVERLAY: how the gauges reach the place being read — routes copied out of the
   * tiles so they draw at every zoom, upstream and downstream drawn apart, a pin for the spot
   * and a labelled dot per gauge. See `trace-overlay.ts`. Refetched when `key` changes.
   */
  trace?: TraceProp | null;
  at: TileEndpoints;
  /**
   * Any theme the style defines, not just light/dark. It used to be the pair, so the app
   * had to launder `cvd` into `light` before calling — which meant choosing the
   * colour-blind palette changed the chrome and left the MAP red/green, for the one person
   * who chose it because they cannot tell those apart. A control that silently does half
   * of what it says is worse than one that is greyed out.
   */
  theme: string;
  /** A preset that colours every layer at once. Applied first. */
  view: string;
  /**
   * Per-layer colouring, applied over the view.
   *
   * The design colours streams and lakes independently — Rules on the rivers while the
   * lakes show Stocked — because they answer different questions. A view is the preset;
   * this is what the Layers panel actually sets.
   */
  modes?: Record<string, string>;
  /**
   * Layers this screen does not draw at all — from `hiddenLayers(viewId)`.
   *
   * SEPARATE FROM `groups`, which is a user's choice among what a view offers. This is the
   * view's own decision and it is not the user's to make. Passing it through `groups`
   * instead is what produced "layer group admin is not toggleable" seven times a render,
   * with the boundaries still drawing: `admin` is deliberately fixed, so the adapter
   * refused the call and the screen quietly kept the layers it meant to hide.
   */
  hide?: readonly string[];
  /**
   * Layers only this screen's view draws — from `shownLayers(viewId)` (the Conditions flow
   * arrows). The mirror of `hide`: absent, every view-only layer stays hidden, so a map that
   * asks for nothing (the mini map, the spot picker) draws none of them.
   */
  show?: readonly string[];
  /** Which toggleable layer groups are on. Absent means "leave the style's default". */
  groups?: Record<string, boolean>;
  initial: Camera;
  /**
   * Per-feature values the active colour mode reads, by layer then feature id. The key `"sections"` (`SECTION_DATA`,
   * sections.ts) means EVERY per-section source-layer (`perSectionLayers()`): the map fans it out (`spreadSections`),
   * so a caller hands a section's values once and never names the layers (T124).
   */
  data?: Record<string, Record<string, Record<string, unknown>>>;
  /**
   * THE DAY'S STATUS, for every layer in the `profile` colour mode (colour-latency study, AP).
   *
   * Not through `data`: that waited for the map's `idle` and a React round trip, so every newly loaded tile drew grey
   * for about half a second after a pan (owner 2026-10-20: "when we pan it takes a second to color from default").
   * The renderer is handed the index itself and labels each section with its profile as its tile arrives, and carries
   * the day in the style — a day change sets no per-section state.
   *
   * GIVE IT ONLY ON A PROVEN PAIR (P11, `usePairing` ok === true) with an index that passed `decodeStatusIndex`'s
   * digest checks; null or absent otherwise. Colouring now happens outside React, so this prop IS the pairing as far
   * as the map knows: with it absent every profile layer paints plain, and taking it away (or handing a different
   * index) clears every label the map set, so another release's profile numbers can never colour this one's water.
   */
  status?: ProfileStatus | null;
  /**
   * THE CONDITIONS MAP'S STANDINGS, for every layer in the `standing` colour mode — the same engine as `status`
   * (`colouring.ts`, the `standing` channel). `labels` is the release's static half: each section's standing CLASS (a
   * donor panel at one target area, or a lake's own station). `values` is the changing half: each class's (and basin's)
   * standing for the live feed at the chosen horizon. The engine writes a section's value into feature-state `standing`
   * as its tile arrives (no `idle`, no React); new `values` — a feed refresh, a horizon or quantity change — are ONE
   * engine call that relabels the sections of the tiles already loaded (feature-state only: no paint change, no tile
   * reload; the style-table alternative re-lays out every loaded tile and measured slower, colour-latency README).
   * Null `values` while the feed has not answered: nothing is labelled, the water draws as unmeasured.
   *
   * ONLY ON A PROVEN PAIR (P11), like `status`: absent or null otherwise, and taking it away (or handing other labels —
   * a release swap) clears every label the map set.
   */
  standing?: StandingFeed | null;
  /**
   * THE LAKE MODES' VALUES, for the lakes in the `stocked` and `surveyed` colour modes (Stocked, Depth) — the same engine
   * again (`colouring.ts`, the `stocked` and `survey` channels): the release's lake facts and today. Each lake section is
   * labelled with its survey state and its stocked band as its tile arrives; a new release (new facts) or a new day
   * relabels the loaded lakes (feature-state only). Nothing about a lake's survey or stocking is in the tiles, so a new
   * release alone recolours them. Handed over in every lake mode, so a mode switch is one paint change.
   *
   * ONLY ON A PROVEN PAIR (P11), like `status`: absent or null otherwise, and taking it away clears every label set.
   */
  lakes?: LakeFeed | null;
  /**
   * (T211) THE SALMON COLOURING, for every layer in the `salmon` mode — the same engine again (`colouring.ts`, the
   * `salmon` channel): each section's DFO chain label as its tile arrives, the day in the style. Handed over on the Map
   * tab whichever colouring is chosen, so the Standard / Salmon toggle is one paint change. ONLY ON A PROVEN PAIR.
   */
  salmon?: SalmonFeed | null;
  /**
   * (T213) BAIT BAN / FLY ONLY, marked on the Standard map — the same engine (`colouring.ts`, the `method` channel):
   * each section's method profile label as its tile arrives, the day in the gear layers' paint (`gearPaint`: an ink edge,
   * solid fly only, dashed bait ban), none where `status` closes the section. ONLY ON A PROVEN PAIR.
   */
  method?: MethodFeed | null;
  /**
   * A tapped feature, and WHERE on it the tap landed.
   *
   * The coordinate is the second half of the answer on a long river: "the Fraser" is 1,375
   * km and "how does this spot reach the gauge" is a question about a point on it.
   */
  onPressFeature?: (layerId: string, featureId: SectionKey,
                    lat?: number, lon?: number) => void;
  /**
   * (T173) A tapped FISH OBSTACLE mark (the `obstacle` layer, drawn only while its switch is on): its tile properties,
   * which `@app/core` `obstacleSheet` turns into words. Asked before the water: a tap on a mark is the mark's, and opens
   * no water under it. Absent, a mark is not tappable and the water under it answers as before.
   */
  onPressObstacle?: (props: Readonly<Record<string, unknown>>) => void;
  /**
   * Renderer failures. Not optional in spirit: an empty map and a map whose tiles failed
   * to load look identical, and this app's whole argument is that "we do not know" must
   * never be drawn as anything else.
   */
  onError?: (e: Error) => void;
  /**
   * A raw tapped coordinate, whether or not it hit a feature.
   *
   * Separate from `onPressFeature` because they answer different questions: picking a
   * WATER needs the feature under the finger, picking a POINT needs the place on the
   * ground. Step two of adding a spot is the latter — a gravel bar is not a feature.
   */
  onMapPoint?: (lat: number, lon: number) => void;
  /**
   * A LONG PRESS on the map, and where (owner 2026-10-21: press and hold the browse map to drop a spot). One finger held
   * still (`@app/core` `LONG_PRESS`): MapLibre Native's own long press on a phone; on the web a held touch timed by the
   * same policy, or the browser's own long press / a right click (`contextmenu`). It never fights a pan or a pinch, and
   * the tap that may follow it is not also a tap.
   */
  onLongPress?: (lat: number, lon: number) => void;
  /** Feature ids to draw as selected. Applied by feature-state, never by mutating paint. */
  highlight?: readonly SectionKey[];
  /**
   * THE REST OF THE WATER whose part is `highlight` (P15, the water's Map view): feature-state `water`, drawn in its own
   * colour at a width floor so the river reads as one water around the selected part. Set and cleared with
   * `highlight`, by the adapter's one `highlight` call.
   */
  water?: readonly SectionKey[];
  /**
   * (T175) THE WATER'S OWN LINE, AT EVERY ZOOM. Every map keeps a copy of what it lights (`highlight`, `water`) the
   * first time a zoom's tiles carry it, and draws it below that zoom (`water-copy.ts`). Given, this map also keeps the
   * water's own `sections` (the water a Conditions map is of, lit or not), and — where no zoom has shown any of them
   * yet — steps in about `at` (a point on the water; the centre without one) until its tiles do: it opens no further
   * out than the water's own first zoom. Geometry from the tiles only.
   */
  waterLine?: { sections: readonly SectionKey[]; at?: { lat: number; lon: number } | null } | null;
  /**
   * (T151) How `highlight` is marked (`SelectionCue`): "outline" (default — the browse map's tapped or searched water,
   * cased and floored wider) or "dim" (a water's Map view: the part at the same width as the rest, the rest dimmed).
   */
  selection?: SelectionCue;
  /**
   * BOUNDARY MARKS (P15): the release's `part_mark` places between a water's parts — a finger-sized ring, labelled
   * where `label` is given (the current part's two ends, or the mark that is open). `on`: the mark that is open.
   */
  marks?: readonly MapMark[];
  /** A mark was tapped: its index in `marks`. */
  onPressMark?: (index: number) => void;
  /**
   * Take the camera to a subject — a searched water, a town.
   *
   * `initial` is where a map STARTS; this is where it is SENT, after it exists. Keyed:
   * the camera moves when `key` changes and at no other time, so a re-render with the same
   * subject never drags back a map the reader has panned since.
   *
   * `refine` says the box is evidence rather than an outline (the bundle holds no
   * geometry — see `fixOf` in @app/core). The map then measures the `highlight` features it
   * has actually loaded and re-fits to them, a few rounds at most (`refineFit`), so a
   * searched river ends up framed by its own shape and not by where its gauges happen to be.
   */
  fit?: {
    key: string;
    bbox: readonly [number, number, number, number] | null;
    refine?: boolean;
    /**
     * (T48) Instead of a box, an exact CAMERA to go back to: search's cancel restores the camera the reader had before
     * search moved it. Wins over `bbox`; never refined.
     */
    camera?: Camera | null;
  } | null;
  /**
   * (T48) How much of the map's top is covered by the app's own bar (the search field over the browse maps), in px: the
   * web map's own controls (zoom, compass — top-right) start under it. 0 when absent. A phone's map draws none there.
   */
  insetTop?: number;
  /**
   * The ground under the water: the drawn basemap, or satellite imagery.
   *
   * A property of the MAP, not a layer group a screen may address: the imagery is an
   * external raster declared in `layers.source.json` (group `imagery`) and this is the one
   * switch for it, so a screen cannot turn it on while the drawn ground stays underneath.
   */
  basemap?: "map" | "satellite";
  /**
   * AN IMAGERY TILE FAILED, with its HTTP status (undefined: no response — the network). Satellite is online-only
   * (owner 2026-10-21): the caller decides whether that is an outage (`isImageryOutage`; GL JS never reports a 404)
   * and hands the map the drawn ground instead. Routed here INSTEAD of `onError` when given, because an offline
   * device fails every imagery tile and that is handled, not a renderer fault.
   *
   * THE WEB MAP ONLY reports it: MapLibre Native's binding has no per-tile error event, so on a phone the same answer
   * comes from `useImageryReach`'s probe. Accepted on both, so the two maps take the same props.
   */
  onImageryError?: (status: number | undefined) => void;
  /** Gauge points as GeoJSON, drawn as a dot plus its reading. Replaced per feed tick. */
  gauges?: string;
  /**
   * The section ids currently rendered, so a caller can fetch exactly what is on screen.
   *
   * Debounced by the map — it fires after movement settles, not on every frame of a pan.
   */
  onVisible?: (sections: readonly SectionKey[]) => void;
  /**
   * A point to mark, e.g. the spot a person just tapped.
   *
   * They are choosing a place they intend to find again, so they have to be able to SEE
   * where the tap landed before it becomes a permanent record — a coordinate printed in a
   * banner is not the same as a dot on the water.
   */
  marker?: { lat: number; lon: number } | null;
  /**
   * Several marked points at once, each with its own colour and label.
   *
   * `marker` answers "where did I tap"; this answers "where are the ENDS of the thing I am
   * looking at". The route panel needs both ends on screen — the reach you are standing on
   * and the station that speaks for it — and one marker cannot say which of two dots is
   * which. Separate from `marker` because they have different lifetimes: a tap marker is
   * cleared when you pan away, these belong to whatever the panel is describing.
   */
  pins?: readonly { lat: number; lon: number; tone?: string; title?: string }[];
  /**
   * (T131) THE READER'S SAVED SPOTS — not `pins`: each is a mark of its own the map places at its zoom (`@app/core`
   * `spotMarkers`: alone and named closer in, grouped further out, the selected one marked), a 44 px target that wins
   * over the water under it. Both renderers place them and carry out a tap the same way (`./spots` `pressSpot`).
   */
  spots?: MapSpots;
  /**
   * Where the camera ended up, whenever the user stops moving it.
   *
   * The caller keeps this and hands it back as `initial` on the next mount, so opening a
   * water's sheet and coming back does not throw you across the province. Reported rather
   * than controlled: a controlled camera fights every pan, because each frame of a drag
   * would round-trip through React.
   */
  onMoved?: (at: Camera) => void;
  /**
   * (T186) THE REGION UNDER THE MAP'S CENTRE, each time the map settles (`idle`): the `region` layer's `region_id` at
   * the centre ("1" … "7A" … "8"), or null over no region (the sea, outside B.C., a view that does not draw the
   * layer). The first report is the first painted map. The web prefetch reads it (`@app/ui` `useReleasePrefetch`).
   *
   * THE WEB MAP ONLY reports it: a phone holds the whole release, there is nothing to fetch ahead. Accepted on both,
   * so the two maps take the same props.
   */
  onRegion?: (regionId: string | null) => void;
  style?: ViewStyle;
}
