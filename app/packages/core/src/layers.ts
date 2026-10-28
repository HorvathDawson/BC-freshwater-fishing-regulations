/**
 * THE LAYER REGISTRY (owner 2026-10-21 late: layers menu, direction B — "map · colour the water · on the map").
 *
 * Every row of the Layers sheet is one entry here, so adding a layer is ONE ENTRY: its kind (a ground tile, a way to
 * colour the water, an overlay switch, or a layer that is always drawn), the views it belongs to, what it needs before
 * it can be offered, and its words. The sheet (`@app/ui-native` LayersSheet) draws `layersVM` and decides nothing; the
 * map is handed `effective`, through `mapModes` (`@app/ui`) and the one colouring engine — an entry only ever sets
 * `mapModes` inputs (a basemap, a water colouring, a style group), never a colour of its own.
 *
 * ONE WATER COLOURING FOR ALL FEATURES (owner 2026-10-21 late: "it is dangerous to have the divergent layers for
 * features"): there is no separate lake choice. "Colour the water by" colours streams, lakes and wetlands together; a
 * feature the chosen measure does not cover draws neutral and the legend says so.
 *
 * ONE QUANTITY ON THE CONDITIONS MAP (owner 2026-10-22: "we care about flow. so we dont want to mix flow and level for
 * now. we could have a way to separately view level. (a toggle in layer maybe?)"): its "Show" choice — Flow (the
 * default), Level, Temperature — is ONE choice here, and it drives the one colouring engine's standing channel, the gauge
 * roster and the panel's chart together. Flow is answered from flow gauges only and Level from level gauges only; the
 * legend strip carries no second switch.
 *
 * WHAT A VIEW LISTS IS WHAT IT DRAWS: each entry names the views it belongs to, the sheet on a view lists only those,
 * and a switch from another view is never drawn on it (private land does not follow a reader onto the Conditions map
 * with no way to turn it off there). The rail's badge counts the choices that view lists.
 *
 * AVAILABILITY IS DERIVED, never set by hand (`availability`): an entry not built yet is "coming-soon"; one whose
 * values the loaded release does not carry is "no-data" (Stocked, Depth: prepared, not fed — owner 2026-10-21); one
 * that streams and cannot be reached is "online-only" (Satellite). Each says why in the reader's words.
 */
import { SATELLITE_OFFLINE, type Basemap } from "./imagery";
import { STOCK_RECENCY, type StockRecency } from "./lakeFacts";
import { MAP_RAIL } from "./mapRail";
import { WATER_STATUS, WATER_STATUSES, type WaterStatus } from "./status";

/** The map views a layer can belong to: the sheet opens from the Map tab ("map") and the Conditions map ("conditions"); "water"
 *  (a water's own map view) is declared, not opened yet. */
export type LayerView = "map" | "water" | "conditions";

/** How the water is coloured: ONE choice, for every water feature at once. */
export type WaterColour = "regulations" | "stocked" | "depth";

/**
 * What the Conditions map shows: ONE quantity at a time (owner 2026-10-22, NEVER MIX QUANTITIES). The names are the
 * data layer's (`Parameter` "discharge" | "level", plus "temperature"), so the choice goes to the panel unchanged.
 */
export type ConditionsShow = "discharge" | "level" | "temperature";

/**
 *   show      one tile of "Show" (the Conditions map's quantity): a radio
 *   ground    one tile of the "Map" set (what is under the water): a radio
 *   water     one tile of "Colour the water by": a radio, for streams, lakes and wetlands together
 *   overlay   an "On the map" switch: a style group drawn over the water
 *   always    drawn on every map and never switched off (safety and orientation); listed, not offered
 */
export type LayerKind = "show" | "ground" | "water" | "overlay" | "always";

/** The marker sets an entry can switch: the reader's saved spots, drawn as pins on every map (owner 2026-10-22). */
export type PinSet = "spots";

/** What the release must carry for an entry to be offered (`LakeFacts.surveyed` / `.stocked` > 0). */
export type ReleaseValues = "stocked" | "surveyed";

export interface LayerEntry {
  readonly id: string;
  readonly kind: LayerKind;
  readonly views: readonly LayerView[];
  /** The default choice of its set (ground, water), or a switch's default state. */
  readonly default: boolean;
  /**
   * What it sets — ONLY `mapModes` inputs. A style group must be a toggleable one; a layer must be one a view alone
   * draws (`views[].show`) or a runtime layer (`$runtimeLayers`) — tests hold both.
   */
  readonly sets?: {
    readonly show?: ConditionsShow;
    readonly basemap?: Basemap;
    readonly water?: WaterColour;
    readonly groups?: readonly string[];
    readonly layers?: readonly string[];
    /** Markers the app places itself (not style layers): "spots", the reader's saved spots. */
    readonly pins?: readonly PinSet[];
  };
  /** Style groups / layers an `always` entry names (tests hold that they exist and are not toggleable). */
  readonly draws?: { readonly groups?: readonly string[]; readonly layers?: readonly string[] };
  /** What it needs before it is offered (availability is derived from these, in this order). */
  readonly needs?: { readonly built?: false; readonly release?: ReleaseValues; readonly network?: true };
  /** Its words. */
  readonly label: string;
  readonly note?: string;
}

export const LAYER_WORDS = {
  title: "Layers",
  sections: { show: "Show", ground: "Map", water: "Colour the water by", overlay: "On the map" },
  /** The rail button's spoken name, with how many choices differ from the defaults. */
  railLabel: (changed: number) => (changed > 0 ? `${MAP_RAIL.layersLabel}, ${changed} changed` : MAP_RAIL.layersLabel),
  tags: {
    showing: "Showing",
    "no-data": "No data yet",
    "online-only": "Needs a connection",
    "coming-soon": "Coming soon",
  },
  reasons: {
    "no-data": "No data yet: the release carries no values for it. It turns on by itself when one does.",
    "online-only": SATELLITE_OFFLINE,
    "coming-soon": "Coming soon.",
  },
  always: { lead: "Always shown:", tail: "They are never switched off." },
  /** The line under the water tiles naming the colourings that wait for data (one sentence, whole). */
  waiting: (names: readonly string[]) => names.length > 1
    ? `${names.slice(0, -1).join(", ")} and ${names[names.length - 1]} have no data yet. They turn on by themselves `
      + "when a release carries their values."
    : `${names[0]} has no data yet. It turns on by itself when a release carries its values.`,
  /** The legend under the chosen water colouring: what a feature it does not cover looks like, said once. */
  legend: {
    regulations: "Coloured by the day's regulations.",
    stockedNeutral: "Not stocked, or no record. Streams and wetlands draw plain: stocking is recorded for lakes only.",
    depthNeutral: "Not surveyed. Streams and wetlands draw plain: depth surveys cover lakes only.",
    surveyDigitised: "Contours traced",
    surveySheet: "Surveyed, sheet only",
    stocked: {
      season: "Stocked this season", year: "Stocked this year", recent: "Stocked 1–3 years ago",
      decade: "Stocked 3–10 years ago", old: "Stocked over 10 years ago",
    } satisfies Record<StockRecency, string>,
  },
} as const;

export type LayerReason = keyof typeof LAYER_WORDS.reasons;
export type Availability = { readonly state: "ok" } | { readonly state: LayerReason; readonly reason: string };

/**
 * THE REGISTRY, in the order the sheet lists it. Coming-soon entries are listed (greyed, "Coming soon") in every
 * build — owner 2026-10-21 late.
 */
export const LAYERS: readonly LayerEntry[] = [
  // ── Show (the Conditions map's one quantity: never two on one ramp) ──
  { id: "show.flow", kind: "show", views: ["conditions"], default: true,
    sets: { show: "discharge" }, label: "Flow", note: "How much water, against its record for the date" },
  { id: "show.level", kind: "show", views: ["conditions"], default: false,
    sets: { show: "level" }, label: "Level", note: "How high the water stands, from level gauges only" },
  { id: "show.temperature", kind: "show", views: ["conditions"], default: false,
    sets: { show: "temperature" }, label: "Temperature", note: "At the gauges that measure it; the rivers stay plain" },
  // ── Map (the ground) ──
  { id: "ground.map", kind: "ground", views: ["map", "water", "conditions"], default: true,
    sets: { basemap: "map" }, label: "Map", note: "Drawn · works offline" },
  { id: "ground.satellite", kind: "ground", views: ["map", "water", "conditions"], default: false,
    sets: { basemap: "satellite" }, needs: { network: true }, label: "Satellite", note: "Esri imagery · online only" },
  { id: "ground.corridors", kind: "ground", views: ["map", "water"], default: false,
    needs: { built: false }, label: "Corridors", note: "Detail imagery along a stream" },
  // ── Colour the water by (streams, lakes and wetlands together) ──
  { id: "water.regulations", kind: "water", views: ["map", "water"], default: true,
    sets: { water: "regulations" }, label: "Regulations", note: "Open, closed or its own rules, on the day" },
  { id: "water.stocked", kind: "water", views: ["map", "water"], default: false,
    sets: { water: "stocked" }, needs: { release: "stocked" }, label: "Stocked", note: "When a lake was last stocked" },
  { id: "water.depth", kind: "water", views: ["map", "water"], default: false,
    sets: { water: "depth", groups: ["depth"] }, needs: { release: "surveyed" },
    label: "Depth", note: "Which lakes were surveyed" },
  // ── On the map (switches) ──
  // YOUR SPOTS, on every map (owner 2026-10-22: "toggle our spots on and off in the layer menu"): on by default
  { id: "overlay.spots", kind: "overlay", views: ["map", "water", "conditions"], default: true,
    sets: { pins: ["spots"] }, label: "Spots", note: "Your saved spots" },
  { id: "overlay.contours", kind: "overlay", views: ["map", "water"], default: false,
    sets: { groups: ["depth"] }, label: "Depth contours", note: "Traced lines on surveyed lakes" },
  { id: "overlay.private", kind: "overlay", views: ["map", "water"], default: false,
    sets: { groups: ["ownership"] }, label: "Private land", note: "Title, not access. Off by default" },
  // (T173) FISH OBSTACLES: falls, cascades, canyons, velocity barriers, culverts and dams (FISS + WRIS), off by default;
  // a tap on a mark names it (`obstacleSheet`)
  { id: "overlay.obstacles", kind: "overlay", views: ["map", "water"], default: false,
    sets: { groups: ["obstacles"] }, label: "Fish obstacles", note: "Falls, cascades, culverts and dams. From zoom 10" },
  { id: "overlay.pools", kind: "overlay", views: ["map", "water"], default: false,
    needs: { built: false }, label: "Pools and runs", note: "Slow and fast water, read from lidar" },
  { id: "overlay.relief", kind: "overlay", views: ["map", "water"], default: false,
    needs: { built: false }, label: "Relief", note: "Lidar hillshade" },
  { id: "overlay.gauges", kind: "overlay", views: ["conditions"], default: true,
    sets: { layers: ["gauges"] }, label: "Gauges", note: "The stations, coloured by what they read" },
  { id: "overlay.arrows", kind: "overlay", views: ["conditions"], default: true,
    sets: { layers: ["stream__arrows"] }, label: "Flow arrows", note: "Which way the water runs" },
  // ── Always shown (never switched off): the safety layers, and the lines that say whose regulations apply ──
  { id: "always.protected", kind: "always", views: ["map", "water", "conditions"], default: true,
    draws: { groups: ["protected"], layers: ["park", "park_closed", "eco_reserve"] },
    label: "parks and ecological reserves" },
  { id: "always.indigenous", kind: "always", views: ["map", "water", "conditions"], default: true,
    draws: { groups: ["access"], layers: ["indigenous_land"] }, label: "Indigenous land (reserves)" },
  { id: "always.noaccess", kind: "always", views: ["map", "water", "conditions"], default: true,
    draws: { groups: ["access"], layers: ["no_access", "no_access_permit"] }, label: "no-access and permit-only land" },
  { id: "always.admin", kind: "always", views: ["map", "water"], default: true,
    draws: { groups: ["admin"], layers: ["region", "mu"] }, label: "region and management-unit lines" },
];

/** What availability is derived from. */
export interface LayerFacts {
  /** Does the loaded release carry values of this kind (`lakeModesReady`)? */
  readonly carries: Readonly<Record<ReleaseValues, boolean>>;
  /** Can streamed imagery be reached (`useImageryReach().up`)? */
  readonly online: boolean;
}

/** Built → release → network: the first that fails says why. */
export function availability(e: LayerEntry, f: LayerFacts): Availability {
  const n = e.needs;
  const no = (state: LayerReason): Availability => ({ state, reason: LAYER_WORDS.reasons[state] });
  if (n?.built === false) return no("coming-soon");
  if (n?.release && !f.carries[n.release]) return no("no-data");
  if (n?.network && !f.online) return no("online-only");
  return { state: "ok" };
}

/** The reader's choice: one ground, one water colouring, and the switches. The only layer state the Shell keeps. */
export interface LayerChoice {
  /** The Conditions map's quantity (a `show` entry). */
  readonly show: string;
  readonly ground: string;
  readonly water: string;
  readonly on: Readonly<Record<string, boolean>>;
}

const of = (kind: LayerKind) => LAYERS.filter((e) => e.kind === kind);
const byId = (id: string) => LAYERS.find((e) => e.id === id);
const defaultOf = (kind: LayerKind) => of(kind).find((e) => e.default)!.id;

export const LAYER_DEFAULTS: LayerChoice = Object.freeze({
  show: defaultOf("show"),
  ground: defaultOf("ground"),
  water: defaultOf("water"),
  on: Object.freeze(Object.fromEntries(of("overlay").map((e) => [e.id, e.default]))),
});

/** What the maps are handed: an unavailable choice falls back to its set's default; the CHOICE is kept. */
export interface LayerEffective {
  /** The ground the reader chose (`mapModes` turns it into the ground drawn, with `imageryUp`). */
  readonly basemap: Basemap;
  readonly water: WaterColour;
  /** The Conditions map's one quantity: the standing channel, the gauge roster and the panel's chart all ask it. */
  readonly show: ConditionsShow;
  /** Every style group an entry sets, on or off — on only where the view lists the switch. */
  readonly groups: Readonly<Record<string, boolean>>;
  /** Every view-drawn or runtime layer an entry sets (the gauges, the flow arrows), on or off, the same way. */
  readonly layers: Readonly<Record<string, boolean>>;
  /** Every marker set an entry switches (the saved spots), on or off, the same way. */
  readonly pins: Readonly<Record<PinSet, boolean>>;
}

export interface LayerTileVM {
  readonly id: string;
  readonly label: string;
  readonly note: string | null;
  /** Chosen by the reader (kept even while unavailable). */
  readonly chosen: boolean;
  /** Can be pressed: available (a chosen online-only tile stays chosen, greyed, until it is reachable again). */
  readonly enabled: boolean;
  /** The upper-case tag in the tile, if any: "Showing" on the ground drawn in place of an unreachable choice. */
  readonly tag: string | null;
  readonly availability: Availability["state"];
}

export interface LayersVM {
  readonly title: string;
  /** The Conditions map's quantity (empty on the Map tab). */
  readonly show: { readonly title: string; readonly tiles: readonly LayerTileVM[] };
  readonly ground: { readonly title: string; readonly tiles: readonly LayerTileVM[]; readonly note: string | null };
  readonly water: { readonly title: string; readonly tiles: readonly LayerTileVM[]; readonly note: string | null;
                    /** The colouring the map draws (an unavailable choice falls back to Regulations). */
                    readonly drawn: WaterColour };
  readonly overlays: { readonly title: string; readonly rows: readonly LayerTileVM[] };
  /** "Always shown: …. They are never switched off." */
  readonly always: string;
  /** How many choices THIS VIEW LISTS differ from the defaults (the rail's badge). */
  readonly changed: number;
  readonly effective: LayerEffective;
}

const DEFAULT_WATER: WaterColour = "regulations";
const DEFAULT_SHOW: ConditionsShow = "discharge";

/** The sheet's view model, pure: the sections for `view`, each tile's state and words, and what the map is handed. */
export function layersVM(view: LayerView, choice: LayerChoice, f: LayerFacts): LayersVM {
  const here = (k: LayerKind) => of(k).filter((e) => e.views.includes(view));
  const avail = (e: LayerEntry) => availability(e, f);
  const tile = (e: LayerEntry, chosen: boolean, tag: string | null = null): LayerTileVM => {
    const a = avail(e);
    return { id: e.id, label: e.label, note: e.note ?? null, chosen, enabled: a.state === "ok", tag,
             availability: a.state };
  };
  // the ground: the choice, and the one drawn in its place while it cannot be reached
  const groundChosen = byId(choice.ground)?.kind === "ground" ? byId(choice.ground)! : byId(LAYER_DEFAULTS.ground)!;
  const groundOk = avail(groundChosen).state === "ok";
  const groundDrawn = groundOk ? groundChosen.id : LAYER_DEFAULTS.ground;
  const groundTiles = here("ground").map((e) => {
    const a = avail(e);
    const tag = !groundOk && e.id === groundDrawn ? LAYER_WORDS.tags.showing
      : a.state !== "ok" ? LAYER_WORDS.tags[a.state] : null;
    return tile(e, e.id === groundChosen.id, tag);
  });
  const groundA = avail(groundChosen);
  // the water: one colouring for every feature
  const waterChosen = byId(choice.water)?.kind === "water" ? byId(choice.water)! : byId(LAYER_DEFAULTS.water)!;
  const waterOk = avail(waterChosen).state === "ok";
  const drawn: WaterColour = waterOk ? waterChosen.sets?.water ?? DEFAULT_WATER : DEFAULT_WATER;
  const waterTiles = here("water").map((e) => {
    const a = avail(e);
    return tile(e, waterOk ? e.id === waterChosen.id : e.id === LAYER_DEFAULTS.water,
                a.state === "ok" ? null : LAYER_WORDS.tags[a.state]);
  });
  // the Conditions map's quantity: always available (every quantity is answered, or refused, by the panel)
  const showChosen = byId(choice.show)?.kind === "show" ? byId(choice.show)! : byId(LAYER_DEFAULTS.show)!;
  const showTiles = here("show").map((e) => tile(e, e.id === showChosen.id));
  const waiting = here("water").filter((e) => avail(e).state === "no-data");
  const overlayRows = here("overlay").map((e) => {
    const a = avail(e);
    return tile(e, a.state === "ok" && (choice.on[e.id] ?? e.default), a.state === "ok" ? null : LAYER_WORDS.tags[a.state]);
  });
  // the groups and layers the map is handed: every one an entry names, on when an available choice IN THIS VIEW sets it
  const groups: Record<string, boolean> = {};
  const layers: Record<string, boolean> = {};
  const pins = { spots: false } as Record<PinSet, boolean>;
  for (const e of LAYERS) {
    for (const g of e.sets?.groups ?? []) groups[g] = false;
    for (const l of e.sets?.layers ?? []) layers[l] = false;
  }
  for (const r of overlayRows) if (r.chosen) {
    for (const g of byId(r.id)!.sets?.groups ?? []) groups[g] = true;
    for (const l of byId(r.id)!.sets?.layers ?? []) layers[l] = true;
    for (const p of byId(r.id)!.sets?.pins ?? []) pins[p] = true;
  }
  if (waterOk && waterChosen.views.includes(view)) for (const g of waterChosen.sets?.groups ?? []) groups[g] = true;
  const always = here("always").map((e) => e.label);
  const changed = (choice.ground !== LAYER_DEFAULTS.ground ? 1 : 0)
    + (showTiles.length > 0 && showChosen.id !== LAYER_DEFAULTS.show ? 1 : 0)
    + (waterTiles.length > 0 && choice.water !== LAYER_DEFAULTS.water && waterOk ? 1 : 0)
    + overlayRows.filter((r) => r.enabled && r.chosen !== (byId(r.id)!.default)).length;
  return {
    title: LAYER_WORDS.title,
    show: { title: LAYER_WORDS.sections.show, tiles: showTiles },
    ground: { title: LAYER_WORDS.sections.ground, tiles: groundTiles,
              note: groundA.state === "ok" ? null : groundA.reason },
    water: { title: LAYER_WORDS.sections.water, tiles: waterTiles, drawn,
             note: waiting.length ? LAYER_WORDS.waiting(waiting.map((e) => e.label))
               : null },
    overlays: { title: LAYER_WORDS.sections.overlay, rows: overlayRows },
    always: `${LAYER_WORDS.always.lead} ${listWords(always)}. ${LAYER_WORDS.always.tail}`,
    changed,
    effective: { basemap: groundChosen.sets?.basemap ?? "map", water: drawn,
                 show: showChosen.sets?.show ?? DEFAULT_SHOW, groups, layers, pins },
  };
}

/** "a and b"; "a, b, and c" (the series comma: the labels hold "and" themselves). */
function listWords(xs: readonly string[]): string {
  if (xs.length <= 2) return xs.join(" and ");
  return `${xs.slice(0, -1).join(", ")}, and ${xs[xs.length - 1]}`;
}

/** A tap on a tile or a switch: the new choice (an unavailable entry is never chosen). */
export function chooseLayer(choice: LayerChoice, id: string, f: LayerFacts): LayerChoice {
  const e = byId(id);
  if (!e || availability(e, f).state !== "ok") return choice;
  if (e.kind === "show") return { ...choice, show: id };
  if (e.kind === "ground") return { ...choice, ground: id };
  if (e.kind === "water") return { ...choice, water: id };
  if (e.kind === "overlay") return { ...choice, on: { ...choice.on, [id]: !(choice.on[id] ?? e.default) } };
  return choice;
}

/**
 * THE CHOICE, KEPT (owner 2026-10-22: the Spots switch "persists like the other layer toggles"): the reader's whole
 * layer choice as text for the app's own store, and back. Restoring trusts nothing it reads: an id the registry no
 * longer has, or one of the wrong kind, falls back to its set's default, and a switch keeps only a boolean for an
 * overlay that still exists — so a stored choice from an older app can never name a layer this one does not draw.
 */
export function saveLayers(c: LayerChoice): string {
  return JSON.stringify({ show: c.show, ground: c.ground, water: c.water, on: c.on });
}

export function restoreLayers(text: string | null): LayerChoice {
  let v: unknown = null;
  try { v = text === null ? null : JSON.parse(text); } catch { v = null; }
  if (typeof v !== "object" || v === null || Array.isArray(v)) return LAYER_DEFAULTS;
  const o = v as Record<string, unknown>;
  const pick = (k: "show" | "ground" | "water"): string => {
    const id = o[k];
    return typeof id === "string" && byId(id)?.kind === k ? id : LAYER_DEFAULTS[k];
  };
  const on: Record<string, boolean> = { ...LAYER_DEFAULTS.on };
  const stored = typeof o.on === "object" && o.on !== null ? o.on as Record<string, unknown> : {};
  for (const id of Object.keys(on)) if (typeof stored[id] === "boolean") on[id] = stored[id] as boolean;
  return { show: pick("show"), ground: pick("ground"), water: pick("water"), on };
}

/**
 * The Conditions panel's own chart quantity sets the SAME one choice (it is the map's "Show", seen from the panel): a
 * pick there recolours the map, and the map's pick is the chart's.
 */
export function chooseShow(choice: LayerChoice, q: ConditionsShow): LayerChoice {
  const e = LAYERS.find((x) => x.kind === "show" && x.sets?.show === q);
  return e ? { ...choice, show: e.id } : choice;
}

/** A legend swatch, named by what it colours — the component reads the colour from the theme's map tokens. */
export type LegendSwatch =
  | { readonly kind: "status"; readonly status: WaterStatus }
  | { readonly kind: "stock"; readonly band: StockRecency }
  | { readonly kind: "survey"; readonly state: "digitised" | "sheet" }
  /** A water the chosen measure does not cover (the lake fill and the plain stream). */
  | { readonly kind: "neutral" };

export interface LegendRowVM { readonly swatch: LegendSwatch; readonly label: string }

/**
 * THE KEY TO THE WATER COLOURING DRAWN — under the chosen tile in the sheet and in the Map tab's strip. A lake measure
 * ends with the neutral row, which says what draws plain and why: one colouring for all water, so the key must say
 * which waters it does not reach.
 */
export function waterLegend(drawn: WaterColour): readonly LegendRowVM[] {
  const L = LAYER_WORDS.legend;
  if (drawn === "stocked")
    return [...STOCK_RECENCY.map((b) => ({ swatch: { kind: "stock", band: b.band } as const, label: L.stocked[b.band] })),
            { swatch: { kind: "neutral" }, label: L.stockedNeutral }];
  if (drawn === "depth")
    return [{ swatch: { kind: "survey", state: "digitised" }, label: L.surveyDigitised },
            { swatch: { kind: "survey", state: "sheet" }, label: L.surveySheet },
            { swatch: { kind: "neutral" }, label: L.depthNeutral }];
  return WATER_STATUSES.map((st) => ({ swatch: { kind: "status", status: st } as const, label: WATER_STATUS[st].label }));
}
