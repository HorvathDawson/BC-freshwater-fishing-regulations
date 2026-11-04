/**
 * WHAT EVERY MAP IS COLOURED BY: the layers choice, the tiles/data pair, the day's status, the lake values, the
 * Conditions colouring and its horizon — decided in `@app/ui` hooks and only gathered here for the Shell (T62 3.4).
 */
import { chooseLayer, chooseShow, layersVM, withImageryCredit, type Horizon, type SectionKey } from "@app/core";
import { useCallback, useMemo, useState } from "react";
import type { GaugeFeed, RunTimingSource, WaterSource, SectionId } from "@app/data";
import { useMapModes, useLayerChoice, type BrowseTab, type FlowParam, type LayerStore } from "@app/ui";
import { useCentreBasin, useConditionsColour, useGaugeGeoJSON, useLakeColour, lakeModesReady, lakeValuesCarried, usePairing,
         useStatusIndex, statusFeed, useHorizon, useHorizonPick, flowHorizon, horizonChips, useImageryReach,
         useMethodColour, useRegsLayer, useRunTiming, useSalmonColour, type PairingInput, type RegsLayer, type RunSpeciesStore,
         type StatusSource } from "@app/ui";
import type { Camera, TileEndpoints } from "@app/map";
import { FIELD_HANDOVER_Z, imageryTiles } from "@app/map/colours";
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
                                  epoch, settled = true, gearSettled = settled, salmonOnDemand = false, runTiming = null,
                                  runSpeciesStore = null, initialHorizon = 0 }: {
  /** (T369 D10) the Conditions horizon it opens on (the address's, a recreated activity's); Now when absent. */
  initialHorizon?: Horizon;
  source: WaterSource; feed?: GaugeFeed; status: StatusSource | null; pairing: PairingInput | null;
  tiles: TileEndpoints; layerStore?: LayerStore; attribution?: readonly string[];
  /** Which browse map's questions are asked (a water's map view asks the Map tab's). */
  mapTab: BrowseTab;
  /** The day the map's status colours answer for; null is today. */
  mapDay: Date | null;
  /** The sync state's epoch: a feed refresh re-asks the Conditions colouring. */
  epoch: number;
  /** (T182) False until the web's idle prefetch has started: the Salmon / Bait & fly files wait for it (a phone: true). */
  settled?: boolean;
  /** (T254) The bait & fly marks' file waits for this too (low-bandwidth mode: a quiet line); default `settled`. */
  gearSettled?: boolean;
  /** (T182, owner 2026-10-28) The web: the salmon file is read on the first press of Salmon, not up front (a phone: false). */
  salmonOnDemand?: boolean;
  /** (T199) The run-timing feed (its own stream, not the release): read the first time the Runs tab is opened. */
  runTiming?: RunTimingSource | null;
  /** (T261) Where the Runs tab's species are kept between visits (`openRunSpeciesStore`). */
  runSpeciesStore?: RunSpeciesStore | null;
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
  const hz = useHorizon(initialHorizon);
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
  // THE LAKE MODES' VALUES (Stocked, Depth): the release's lake facts, labelled per tile by the same engine
  const lakeFeed = useLakeColour({ source, paired, today: new Date() });
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
  /*
   * (T211) STANDARD / SALMON: the Map tab's regulations, painted from the province's day or from DFO's salmon. The
   * release's salmon is read and handed to the map whatever the toggle says (`useSalmonColour`), so the toggle is a mode
   * change and nothing else — no fetch, no relabel. Offered only under the Regulations colouring, once the file is in.
   */
  const [regsLayer, setRegsLayer] = useRegsLayer();
  const salmon = useSalmonColour({ source, paired, statusOk, mapTab: mapTab === "map", day: mapDay, settled,
                                   onDemand: salmonOnDemand, wanted: regsLayer === "salmon" });
  const salmonOffered = salmon.offered && layers.effective.water === "regulations";
  // (T182) on the web the first Salmon press reads the file: the map stays Standard (the toggle says Loading) until it is in
  /*
   * (T199, T261) RUN TIMING — ITS OWN TAB, Runs (owner 2026-10-29; it was a third choice beside Standard / Salmon). The
   * feed is read the first time the tab is opened and painted only on the atlas it was bound to; the species chosen and
   * the date slider's pentad are what it paints (`useRunTiming`).
   */
  const runsOffered = !!runTiming;
  const runs = useRunTiming({ source: runTiming, wanted: mapTab === "runs", paired, tiles: vintage.tiles,
                              store: runSpeciesStore });
  const runsShown = mapTab === "runs" && runs.feed !== null;
  const regsShown: RegsLayer = regsLayer === "salmon" && salmonOffered && salmon.feed !== null ? "salmon" : "standard";
  const salmonLoading = regsLayer === "salmon" && salmonOffered && salmon.loading;
  // (T213) BAIT BAN / FLY ONLY: marks on the Standard map — the release's `method` file handed over the same way; the
  // legend lists the marks while the Standard regulations are drawn
  const method = useMethodColour({ source, paired, statusOk, mapTab: mapTab === "map", day: mapDay, settled: gearSettled });
  // (owner 2026-10-28) the marks ride the Salmon map too: bait and fly rules also apply to salmon
  const gearMarked = method.feed !== null && layers.effective.water === "regulations";
  const mm = useMapModes({ tab: mapTab, flowParam, horizon, zoomedOut, paired, statusOk,
                           water: layers.effective.water, groups: layers.effective.groups,
                           basemap: layers.effective.basemap, imageryUp: imagery.up,
                           salmon: regsShown === "salmon", runs: runsShown, runsField: runs.facts?.hasField === true });
  // Esri's credit is listed exactly while its imagery is drawn (the map's own control does the same from the style)
  const credits = useMemo(() => attribution && withImageryCredit(attribution, mm.basemap), [attribution, mm.basemap]);
  /*
   * THE CONDITIONS COLOURING (`useConditionsColour`): the release's classes and the feed's values at the horizon
   * (`standing` — one engine call per feed refresh or horizon change), labelled per tile by both renderers.
   */
  const cond = useConditionsColour({ source, feed, paired, onConditions: mm.onConditions,
                                     quantity: mm.standingParam, horizon: mm.standingHorizon, visible, epoch });
  // the dots: the Conditions map's, while its Gauges switch is on (off, the roster is not even asked), at the horizon
  // the rivers are coloured for (T126: a dot under +5 says +5, not now)
  const gauges = useGaugeGeoJSON(source, feed, mm.onConditions && layers.effective.layers.gauges === true, mm.quantity,
                                 mm.standingHorizon);
  /*
   * (T376) THE WATERSHED GROUP UNDER THE CONDITIONS MAP'S CENTRE (`centreBasin`: `{ id, name }`, null in none; the id is
   * `basinStandings`' key), asked when the map comes to rest (`noteCentre`, from `onMoved`) and only on Conditions. A
   * water's Conditions view asks its OWN group (`useWaterBasin`), never this one. Asked on a proven pair only, as the
   * standings are: the conditions file it reads is then already being opened for them, so it adds no fetch of its own.
   */
  const centre = useCentreBasin(source, mm.onConditions && paired, HOME);
  /*
   * THE DAY'S STATUS, ONLY on a proven pair with an index that matched (`statusOk`) (`statusFeed`): the map is handed
   * the index and the day and labels each tile as it arrives (colour-latency AP — no `idle`, no round trip through
   * here per pan).
   */
  const mapStatus = useMemo(() => statusFeed({ statusOk, index: statusIndex, day: mapDay ?? new Date() }),
                            [statusOk, statusIndex, mapDay]);
  // the map labels each tile with the salmon chains on the Map tab whatever the toggle says (instant toggle)
  const salmonFeed = mapTab === "map" ? salmon.feed : null;
  const methodFeed = mapTab === "map" ? method.feed : null;
  return { pickedHorizon: hz.horizon, noteVisible, setZoomedOut, zoomedOut, vintage, mixedPair, unproven, statusIndex, statusOk, lakeFeed, imagery, layers,
           flowParam, horizon, horizonChipsShown, setFlowParam, pickDay, onChooseLayer, mm, credits, cond, gauges,
           mapStatus, salmonFeed, salmonOffered, salmonLoading, methodFeed, gearMarked,
           regsLayer: salmonLoading ? "salmon" as RegsLayer : regsShown,
           setRegsLayer, salmonFacts: salmon.facts,
           // (T199, T261) the run timing: offered (the Runs tab), its state, and what the map paints (on Runs only)
           runsOffered, runs, runsFeed: runsShown ? runs.feed : null,
           // (T367) on the Runs tab, why nothing is painted: the bar over the map (`PairBanner`)
           runsProblem: mapTab === "runs" ? runs.problem : null,
           centreBasin: centre.centreBasin, centreBasinError: centre.centreBasinError, noteCentre: centre.noteCentre };
}

export type MapColouring = ReturnType<typeof useMapColouring>;
