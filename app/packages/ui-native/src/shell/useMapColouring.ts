/**
 * WHAT EVERY MAP IS COLOURED BY: the layers choice, the tiles/data pair, the day's status, the lake values, the
 * Conditions colouring and its horizon — decided in `@app/ui` hooks and only gathered here for the Shell (T62 3.4).
 */
import { chooseLayer, chooseShow, layersVM, withImageryCredit, type SectionKey } from "@app/core";
import { useCallback, useMemo, useState } from "react";
import { Platform } from "react-native";
import type { GaugeFeed, WaterSource, SectionId } from "@app/data";
import { useMapModes, useLayerChoice, type BrowseTab, type FlowParam, type LayerStore } from "@app/ui";
import { useConditionsColour, useGaugeGeoJSON, useLakeColour, lakeModesReady, lakeValuesCarried, usePairing,
         useStatusIndex, statusFeed, useHorizon, useHorizonPick, flowHorizon, horizonChips, useImageryReach,
         type PairingInput, type StatusSource } from "@app/ui";
import { FIELD_HANDOVER_Z, type Camera, type TileEndpoints, coloursEachTile, imageryTiles } from "@app/map";
import { connectivity } from "../connectivity";
import { imageryProbe } from "../imageryProbe";

/** Where the map starts the FIRST time. After that the camera is whatever the user left. */
// z10, above the field's handover: the app opens on rivers you can tap on the Conditions map too
export const HOME: Camera = { lon: -121.85, lat: 49.15, zoom: 10 };

/**
 * WHERE THE FIELD HANDS OVER TO THE RIVERS: the style's one number (`FIELD_HANDOVER_Z`, the basin layer's maxzoom),
 * read, never typed here (T130, owner 2026-10-22: "should be able to zoom out quite a bit more first").
 *
 * Below it the Conditions map is catchments and NOTHING IS TAPPABLE: a tap would open a
 * reach sheet for a shape the size of a valley, answering a question nobody asked. Above it
 * the rivers are back and they are what you tap.
 */
export const HANDOVER_Z = FIELD_HANDOVER_Z;

export function useMapColouring({ source, feed, status, pairing, tiles, layerStore, attribution, mapTab, mapDay,
                                  epoch }: {
  source: WaterSource; feed?: GaugeFeed; status: StatusSource | null; pairing: PairingInput | null;
  tiles: TileEndpoints; layerStore?: LayerStore; attribution?: readonly string[];
  /** Which browse map's questions are asked (a water's map view asks the Map tab's). */
  mapTab: BrowseTab;
  /** The day the map's status colours answer for; null is today. */
  mapDay: Date | null;
  /** The sync state's epoch: a feed refresh re-asks the Conditions colouring. */
  epoch: number;
}) {
  /**
   * The reaches the map currently has rendered — what the Conditions view asks the panel
   * about. Viewport-scoped because the whole table is far too big to hold for a question
   * about the few hundred reaches actually on screen.
   */
  const [visible, setVisible] = useState<readonly SectionId[]>([]);
  // Percentiles for what is on screen. Empty while the feed is unreachable, which paints
  // nothing rather than painting every reach as a drought.
  // STABLE IDENTITY MATTERS HERE. As an inline arrow this changed every render, so the
  // effect holding the map's `idle` listener tore itself down and re-registered constantly
  // — and `idle` fires once when movement settles, so the listener was frequently absent
  // at the only moment it had anything to hear. The map reported nothing, `visible` stayed
  // empty, and every reach painted as `missing`.
  const noteVisible = useCallback(
    // CONVERTED, NOT CAST. This was `ids as readonly SectionId[]`, which asserted a fact
    // rather than establishing one — and when the map began reporting stringified ids the
    // cast said nothing while every lookup missed. A section handle is an integer; make it
    // one here, at the single point where map ids become bundle keys.
    (ids: readonly SectionKey[]) =>
      setVisible(ids.map((i) => Number(i) as SectionId)), []);
  /*
   * HOW FAR AHEAD the Conditions map is painted and the panel answers — ONE state, two
   * controls (owner 2026-10-20): the map's corner chips and the panel header's both draw
   * `hz.chips` and call `hz.pick`. 0 is now, and is where it opens. NOT reset when the panel
   * closes (P16 did, because the map then had no control saying which horizon it painted;
   * it has one again, showing the same choice).
   */
  const hz = useHorizon();
  // Which side of the handover zoom the camera is on — the one bit of the camera that is render state (see onMoved).
  const [zoomedOut, setZoomedOut] = useState(HOME.zoom < HANDOVER_Z);
  /*
   * THE LAYERS CHOICE (owner 2026-10-21 late, layers menu B): one ground, ONE water colouring for every feature, the
   * Conditions map's ONE quantity (Show: Flow · Level · Temperature — owner 2026-10-22, never two on one ramp) and the
   * switches — the only layer state kept here. The quantity is held here, not in the map, because the panel a tap opens
   * has to be about the same thing the map is showing: its chart's own quantity control sets this same choice. What the map is handed is the registry's (`layersVM(...).effective`,
   * below): an unavailable choice falls back while the choice itself is kept.
   */
  const [layerChoice, setLayerChoice] = useLayerChoice(layerStore);
  /*
   * TILES AND DATA FROM ONE ATLAS, or nothing is coloured.
   *
   * A section is an integer handle into the atlas's table, and both artifacts carry it. A
   * mixed pair does not fail to match — it matches the WRONG SECTION, so every colour on
   * screen would be a real reading about a different river. That is the one outcome this
   * app may never produce, and it has happened once already: the tiles were rebuilt and
   * the data was left behind, and the map went quietly grey.
   *
   * COLOUR ONLY ON A PROVEN PAIR (P11, `usePairing`): `ok === true` colours. `false` is a mixed
   * pair, refused and SAID (the banner, `PairBanner`); `null` — not established yet, or no tile version
   * known — colours nothing and raises no alarm, which is not evidence of a mismatch.
   */
  const vintage = usePairing(pairing);
  const paired = vintage.ok === true;
  const mixedPair = vintage.ok === false;
  // checked, and no tile version to prove the pair with: nothing is coloured, and one quiet line says why
  const unproven = vintage.ok === null && vintage.unproven;
  /*
   * THE DAY'S STATUS, on the Map tab, from the release's paint (`useStatusIndex`; checked against
   * the release's handles when decoded). Until it has loaded the water stays plain: "not asked" is
   * never drawn as "base".
   */
  const statusIndex = useStatusIndex(status);
  const statusOk = statusIndex !== null && paired;
  /** Whether this platform's map colours each tile itself (`profile`, web) or takes pushed data (`status`, native):
   *  the split is described once, in @app/map `colouring.ts`. */
  const perTile = coloursEachTile(Platform.OS);
  // THE LAKE MODES' VALUES (Stocked, Depth): the release's lake facts, labelled per tile by the same engine
  const lakeFeed = useLakeColour({ source, perTile, paired, today: new Date() });
  // PREPARED, NOT FED (owner 2026-10-21): a lake mode with no values in the release is greyed in Layers and drawn plain
  const lakeReady = useMemo(() => lakeModesReady(lakeFeed), [lakeFeed]);
  /*
   * WHAT EACH LAYER IS COLOURED BY (P12): the quantity, the percentile asked of the panel, the field and every layer's
   * mode, decided in `@app/ui` (`useMapModes`) and only drawn here.
   */
  /*
   * SATELLITE IS ONLINE-ONLY (owner 2026-10-21): can Esri's imagery be reached — the device's word (web), a failed
   * imagery tile (web map), and one probed tile (both; a phone's only signal). `mapModes` turns it and the reader's
   * choice into the ground every map is handed (`mm.basemap`) and the Layers sheet's reason line; the choice is kept.
   */
  const probe = useMemo(() => imageryProbe(tiles.imagery ?? imageryTiles(null)), [tiles.imagery]);
  const imagery = useImageryReach({ wanted: layerChoice.ground === "ground.satellite", connectivity, probe });
  /*
   * THE LAYERS SHEET'S VIEW MODEL, and what every map is handed (`effective`): from the layer registry over what the
   * release carries (Stocked, Depth: prepared, not fed) and whether streamed imagery can be reached. The sheet opens on
   * both browse maps, each with ITS entries (owner 2026-10-22): the Conditions map's is Show, the ground and its gauges
   * and arrows; the Map tab's (and a water's map view's) the ground, the water colouring and its switches.
   */
  const carried = lakeValuesCarried(lakeReady);
  const layersView = mapTab === "conditions" ? "conditions" : "map";
  const layers = useMemo(() => layersVM(layersView, layerChoice, { carries: carried, online: imagery.up }),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- `carried` is rebuilt each render; its two facts are the deps
    [layersView, layerChoice, carried.stocked, carried.surveyed, imagery.up]);
  /** What the Conditions view shows — ONE quantity, the registry's Show (`effective.show`): map, gauges and chart. */
  const flowParam: FlowParam = layers.effective.show;
  // THE HORIZON THIS QUANTITY CAN ANSWER (T52): Change is flow only, so under Level it answers as Now while the choice is
  // kept; its chip stays under Level greyed "Flow only", and a tap on it moves to Flow (T130, `pickDay`).
  const horizon = flowHorizon(hz.horizon, flowParam);
  const horizonChipsShown = useMemo(() => horizonChips(hz.horizon, flowParam), [hz.horizon, flowParam]);
  // the panel's chart quantity control sets the same choice (a Level chart is the Level map)
  const setFlowParam = useCallback((q: FlowParam) => setLayerChoice((c) => chooseShow(c, q)), []);
  // EVERY horizon control picks through this: Change under Level moves the map to Flow first (`pickHorizon`)
  const pickDay = useHorizonPick(hz.pick, flowParam, setFlowParam);
  const onChooseLayer = useCallback((id: string) =>
    setLayerChoice((c) => chooseLayer(c, id, { carries: carried, online: imagery.up })),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [carried.stocked, carried.surveyed, imagery.up]);
  const mm = useMapModes({ tab: mapTab, flowParam, horizon, zoomedOut, paired, statusOk, perTile,
                           water: layers.effective.water, groups: layers.effective.groups,
                           basemap: layers.effective.basemap, imageryUp: imagery.up });
  // Esri's credit is listed exactly while its imagery is drawn (the map's own control does the same from the style)
  const credits = useMemo(() => attribution && withImageryCredit(attribution, mm.basemap), [attribution, mm.basemap]);
  /*
   * THE CONDITIONS COLOURING, per renderer (`useConditionsColour`): on a map that labels each tile, the release's
   * classes and the feed's values at the horizon (`standing` — one engine call per feed refresh or horizon change);
   * otherwise the visible sections' standings, pushed (`data`). Both FROM THE PANEL, through one arithmetic.
   */
  const cond = useConditionsColour({ source, feed, perTile, paired, onConditions: mm.onConditions,
                                     quantity: mm.standingParam, horizon: mm.standingHorizon, fieldOn: mm.fieldOn,
                                     visible, epoch });
  // the dots: the Conditions map's, while its Gauges switch is on (off, the roster is not even asked), at the horizon
  // the rivers are coloured for (T126: a dot under +5 says +5, not now)
  const gauges = useGaugeGeoJSON(source, feed, mm.onConditions && layers.effective.layers.gauges === true, mm.quantity,
                                 mm.standingHorizon);
  /*
   * THE DAY'S STATUS, ONLY on a proven pair with an index that matched (`statusOk`), and the way this platform's map
   * takes it (`statusFeed`): the web map is handed the index and the day and labels each tile as it arrives
   * (colour-latency AP — no `idle`, no round trip through here per pan); the native map is pushed the visible
   * sections' status as `data` (N2 reads only that, in the `status` mode — colour-ap review).
   */
  const { status: mapStatus, data: statusMapData } = useMemo(
    () => statusFeed({ perTile, statusOk, index: statusIndex, tab: mapTab, visible, day: mapDay ?? new Date() }),
    [perTile, statusOk, statusIndex, mapTab, visible, mapDay]);
  return { noteVisible, setZoomedOut, vintage, mixedPair, unproven, statusIndex, statusOk, lakeFeed, imagery, layers,
           flowParam, horizon, horizonChipsShown, setFlowParam, pickDay, onChooseLayer, mm, credits, cond, gauges,
           mapStatus, statusMapData };
}

export type MapColouring = ReturnType<typeof useMapColouring>;
