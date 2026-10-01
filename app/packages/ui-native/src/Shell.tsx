/**
 * The app: four tabs, the map as home, and a legend that always says what the colours mean.
 *
 * Drawn to `design/riffle.html`. The shell owns the state that more than one screen needs —
 * which tab, which water, which colouring, which layers, which palette — and hands it down.
 * Every answer on every screen comes from a hook (rule 25). The Map tab colours water by the
 * day's status from the status index (closed / own regulations / base); the regulation records
 * themselves are not integrated, so a water's sheet shows a placeholder where they will go.
 */
import { WATER_STATUS, WATER_STATUSES, type SectionKey } from "@app/core";
import { useCallback, useMemo, useRef, useState } from "react";
import { Text, View } from "react-native";
import type { ItemId, Parameter, RegsSource, SectionId } from "@app/data";
import { HORIZONS, useBasinStandings, useDataFacts, useGaugeGeoJSON, usePanelStandings,
         usePickFocus, useStatusIndex, useVintage, statusData,
         type GaugeQuantity, type Horizon } from "@app/ui";

/** What the Conditions view is showing. `both` colours the water by either percentile. */
/**
 * What the Conditions map is coloured by. ONE quantity plus the temperature field — see
 * `Quantity` in @app/data for why "both" is gone: it put stage and discharge percentiles on
 * the same ramp, and they disagree by more than 25 points at 84 stations.
 */
type FlowParam = Parameter | "temperature";
import type { Spot, WeatherSource } from "@app/data/spots";
import { hiddenLayers, toggleableGroups,
         type Camera, type TileEndpoints } from "@app/map";
import { ChartControls } from "./ChartControls";
import { LegendCount, LegendRamp, LegendStrip } from "./Chrome";
import { LayersSheet, STOCK_BANDS, lakeChoices, type LayersState } from "./LayersSheet";
import { MapScreen } from "./MapScreen";
import { SearchScreen } from "./SearchScreen";
import { SpotsScreen } from "./SpotsScreen";
import { SpotCapture } from "./SpotCapture";
import { ConditionsScreen } from "./ConditionsScreen";
import { SpotScreen } from "./SpotScreen";
import { TabBar, type TabKey } from "./TabBar";
import { WaterScreen } from "./WaterScreen";
import { TYPE } from "./type";
import { flowRamp, type Palette, type ThemeName } from "./theme";

/** No highlight — one frozen empty list, so the map's effect is not re-run every render. */
const NONE: readonly SectionKey[] = [];

/** Where the map starts the FIRST time. After that the camera is whatever the user left. */
const HOME: Camera = { lon: -121.85, lat: 49.15, zoom: 9.4 };

/**
 * WHERE THE FIELD HANDS OVER TO THE RIVERS. Mirrors `BASIN_HANDOVER_Z` in the tile builder
 * and `minzoomByView` in the style — three places, one number, and a test holds them equal.
 *
 * Below it the Conditions map is catchments and NOTHING IS TAPPABLE: a tap would open a
 * reach sheet for a shape the size of a valley, answering a question nobody asked. Above it
 * the rivers are back and they are what you tap.
 */
const HANDOVER_Z = 9;

export function Shell({ source, palette, theme, themeName, onTheme, tiles,
                       spots = [], onSaveSpot, weather, onRefreshSpots, onDeleteSpot, refreshing, feed,
                       attribution }: {
  source: RegsSource; palette: Palette;
  /** Passed straight to the map. Every theme the style defines is a real map theme,
   *  so nothing has to be laundered into light/dark on the way. */
  theme: string; themeName: ThemeName; onTheme: (t: ThemeName) => void;
  /** Where the two pmtiles archives are served from. */
  tiles: TileEndpoints;
  /** The user's pins. Empty is the normal first-run state, not a failure. */
  spots?: readonly Spot[];
  /** Where a captured spot goes. Absent -> the Add button is inert and says nothing. */
  onSaveSpot?: (spot: Spot) => void | Promise<void>;
  /** Weather at capture time. Absent -> the spot records that it does not know. */
  weather?: WeatherSource;
  /** Fill in what spots could not know when they were made. */
  onRefreshSpots?: () => void;
  onDeleteSpot?: (id: string) => void;
  /**
   * The live gauge feed. Absent means the Conditions view colours nothing — which is the
   * honest rendering of "we could not check", and never a map of droughts.
   */
  feed?: import("@app/data").GaugeFeed;
  refreshing?: boolean;
  attribution?: readonly string[];
}) {
  /*
   * HOW MUCH DATA IS IN HERE — read, never stated.
   *
   * These five figures used to arrive as props from `App.tsx`, hardcoded from the design
   * mock: 255 waters, 6,967 reaches, 35 surveyed lakes, 5 stations and a fixed timestamp.
   * The bundle the app actually opens holds 19,699 waters and 2,324 stations. A screen may
   * not assert a fact about the data it is sitting on; it asks.
   */
  const facts = useDataFacts(source, feed);
  const counts = facts.state === "ready" ? facts.value.counts : null;
  const [tab, setTab] = useState<TabKey>("map");
  const [item, setItem] = useState<ItemId | null>(null);
  /**
   * The water chosen from SEARCH, which the map opens on and lights when the reader comes
   * back from its page — riffle's `goTo`: frame the water, then open it. Cleared by
   * anything that changes the subject (a tap on another water, a tab change), so a later
   * remount of the map never drags the camera back to an old search.
   */
  const [picked, setPicked] = useState<ItemId | null>(null);
  const pickFocus = usePickFocus(source, picked);
  const [layersOpen, setLayersOpen] = useState(false);
  /** Adding a spot takes over the whole screen: it is a flow, not a mode of the map. */
  const [adding, setAdding] = useState(false);
  const [openSpot, setOpenSpot] = useState<string | null>(null);
  // Non-null only while editing. The draft lives here rather than in SpotScreen so that
  // leaving the screen cannot strand half an edit inside an unmounted component.
  const [edit, setEdit] = useState<{ title: string; notes: string } | null>(null);
  // A pin dropped on the map when you ask to see a spot there. Cleared when you pan away
  // to something else, so it never lingers as a mystery dot.
  const [focus, setFocus] = useState<{ lat: number; lon: number } | null>(null);
  /**
   * The reaches the map currently has rendered — what the Conditions view asks the panel
   * about. Viewport-scoped because the whole table is far too big to hold for a question
   * about the few hundred reaches actually on screen.
   */
  const [visible, setVisible] = useState<readonly SectionId[]>([]);
  // The reach tapped while in Conditions. A tap there asks "what is THIS water doing",
  // which is a different question from the regulations sheet a tap on the Map tab opens.
  const [condSection, setCondSection] = useState<SectionId | null>(null);
  /** Where on that reach the tap landed — the "you are here" end of the route map. */
  const [condAt, setCondAt] = useState<{ lat: number; lon: number } | null>(null);
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
  // Which quantity the Conditions map is coloured by. Held here rather than in the map,
  // because the sheet a tap opens has to be about the same thing the map is showing.
  /*
   * WHICH QUESTION THE CONDITIONS VIEW IS ASKING — one control for all of them.
   *
   * This already existed as Both/Flow/Level and drove the water colouring. Temperature
   * joins it rather than getting a picker of its own: a second control offering Flow and
   * Depth beside one offering Flow and Level is two answers to "where do I change this",
   * which is how a screen stops being learnable. `both` stays because the WATER can be
   * coloured by either percentile at once; temperature cannot join that, because it is
   * not a percentile and nothing can carry it to the water yet.
   */
  const [flowParam, setFlowParam] = useState<FlowParam>("discharge");
  // HOW FAR AHEAD the Conditions map is painted. 0 is now, and is where it opens: a
  // forecast is what you ask for, never what you are shown without asking.
  const [horizon, setHorizon] = useState<Horizon>(0);
  const quantity: GaugeQuantity =
    flowParam === "temperature" ? "temperature"
      : flowParam === "level" ? "level" : "flow";
  // The water keeps its last percentile question under temperature — there is nothing to
  // colour it with, so `standings` is simply not asked for.
  //
  // FROM THE PANEL, not from `section_gauge`. The single-station join left the Harrison
  // grey for its whole length because the station matched to those reaches had gone quiet,
  // while a tap on that same grey opened a sheet answering confidently from two other
  // gauges. One arithmetic now serves both — see `usePanelStandings`.
  const standings = usePanelStandings(source, feed, visible,
                                      // Under temperature the rivers stay plain (below),
                                      // so this quantity is not drawn — it just has to be
                                      // a real one.
                                      flowParam === "temperature" ? "discharge" : flowParam,
                                      // Temperature has no forecast — the models publish
                                      // flow and stage. Asking ahead would colour nothing,
                                      // so it stays on today's degrees.
                                      flowParam === "temperature" ? 0 : horizon);
  /*
   * THE FIELD, and only where it is the answer.
   *
   * Not under temperature: the models publish flow and stage, so a temperature field would
   * be a province coloured from nothing. Not above the handover either — the rivers carry
   * the colour there and the whole table would be read to paint shapes nobody can see.
   */
  const [zoomedOut, setZoomedOut] = useState(HOME.zoom < HANDOVER_Z);
  const fieldOn = tab === "conditions" && flowParam !== "temperature" && zoomedOut;
  const basins = useBasinStandings(source, feed, fieldOn,
                                   flowParam === "temperature" ? "discharge" : flowParam,
                                   horizon);
  // Streams, lakes and gauges each carry their own colouring, as the design has it —
  // plain rivers while the lakes show Stocked is a normal thing to want.
  //
  // DECLARED BEFORE THE HOOKS THAT READ IT. `useGaugeGeoJSON` needs to know whether it is
  // fetching the temperature roster or the flow one, and a `const` read above its own
  // initialiser is a ReferenceError at render — which is a blank screen, not a type error,
  // so the compiler said nothing and only opening the page found it.
  const [layers, setLayers] = useState<LayersState>(
    { lake: "plain", basemap: "map" });
  /*
   * WHAT THE CONDITIONS VIEW IS ASKING. Flow, depth or temperature — a question about the
   * water, so it is asked ON the map rather than filed in the Layers sheet, and it only
   * exists while the Conditions view is open. `flow` on every other tab, so the dots never
   * appear somewhere the reader did not ask a question.
   */
  const onConditions = tab === "conditions";
  const gauges = useGaugeGeoJSON(source, feed, onConditions, quantity);

  // The readings on screen, as positions on the legend's own scale. Sentinels (-0.01,
  // "gauged but no history") are excluded: they are a state, not a point on the scale.
  const visibleMarks = useMemo(
    () => [...standings.values()].filter((p) => p >= 0).sort((a, b) => a - b),
    [standings]);
  // Into the map's OWN per-feature channel — `setData` already pushes these to
  // feature-state. A second mechanism beside it would be two ways to colour one map.
  //
  // × 100 BECAUSE THE STYLE'S RAMP IS A PERCENTAGE. `color.flow` stops at 0 / 50 / 100
  // while every percentile in this app is 0–1 (that is what `@app/core`'s `standing()`
  // takes, and what the feed publishes). Feeding 0.05 into a 0–100 ramp put every river in
  // the province onto the bottom one percent of the scale — which renders as one flat
  // colour and looks exactly like data that never arrived. Converting HERE, at the one
  // point where app data meets the style, keeps 0–1 as the only representation anything
  // else has to know about.
  const conditionData = useMemo(
    () => ({ stream: Object.fromEntries(
      // × 100 for the ramp's percentage scale — but the -0.01 sentinel ("gauged, no
      // history") scales to -1, which is exactly the stop the style reserves for it.
      [...standings].map(([section, p]) => [section, { standing: p * 100 }])),
      // THE SAME VALUES ON THE LAKES. `standings` is keyed by section and a lake section
      // is a section — the map just has to be told twice, because feature-state is per
      // layer and the lake polygons are a different layer from the stream lines.
      lake: Object.fromEntries(
        [...standings].map(([section, p]) => [section, { standing: p * 100 }])),
      // AND THE FIELD, on the same scale from the same readings — see `useBasinStandings`.
      // Empty above the handover zoom, where the layer is not drawn and the rivers answer.
      basin: Object.fromEntries(
        [...basins].map(([id, p]) => [id, { standing: p * 100 }])) }),
    [standings, basins]);
  /**
   * The camera survives leaving the map.
   *
   * Opening a water's sheet unmounts the map, so without this it remounted at HOME and a
   * reader who had panned to the Skeena came back to Chilliwack. A ref rather than state:
   * every pan would otherwise re-render the whole shell, and nothing on screen depends on
   * the camera except the map itself.
   */
  const camera = useRef<Camera>(HOME);
  const [groups, setGroups] = useState<Record<string, boolean>>(
    () => Object.fromEntries(toggleableGroups().map((g) => [g.id, g.defaultVisible])));

  // The Conditions TAB is a different question, so it overrides the stream colouring while
  // you are on it. Everything else is whatever Layers was left set to.
  // A Layers choice is not always a colour mode. Resolve each one through its declaration
  // rather than passing the key straight to the map — "depth" is a layer, and handing it
  // over as a mode threw from inside a render effect and took the whole tree down.
  /*
   * TILES AND BUNDLE FROM ONE ATLAS, or nothing is coloured.
   *
   * A section is an integer handle into the atlas's table, and both artifacts carry it. A
   * mixed pair does not fail to match — it matches the WRONG SECTION, so every colour on
   * screen would be a real reading about a different river. That is the one outcome this
   * app may never produce, and it has happened once already: the tiles were rebuilt and
   * `bundle.sqlite` was left behind, and the map went quietly grey.
   *
   * `ok === false` is the only refusal. `null` means the sidecar has not answered yet or the
   * deployment has none, which is not evidence of a mismatch — see `useVintage`.
   */
  const vintage = useVintage(source, tiles?.atlas);
  const mixedPair = vintage.ok === false;
  /*
   * THE DAY'S STATUS, on the Map tab. The index is refused unless it was built against this
   * bundle's atlas (`useStatusIndex`), and nothing is coloured from it on a mixed pair. Until it
   * has loaded the water stays plain: "not asked" is never drawn as "base".
   */
  const statusIndex = useStatusIndex(source, tiles?.atlas);
  const statusOk = statusIndex !== null && !mixedPair;
  const statusMapData = useMemo(
    () => (statusOk && tab === "map" ? statusData(statusIndex, visible, new Date()) : undefined),
    [statusOk, statusIndex, tab, visible]);

  const lakeChoice = lakeChoices(palette).find((c) => c.k === layers.lake);
  const modes = {
    /*
     * COLOURED FOR FLOW AND FOR LEVEL; PLAIN ONLY UNDER TEMPERATURE.
     *
     * This used to paint rivers for flow alone, on the reasoning that a depth cannot be
     * carried from a station to the water around it — which is true, and was the right
     * guard while the data layer would SUBSTITUTE. It no longer does: a level question is
     * answered from level donors or not at all (see `Quantity` in @app/data), so a reach
     * with nothing to say now arrives with no standing and draws as unmeasured. The guard
     * has moved to where the arithmetic is, and the map can stop refusing a question it is
     * able to answer — which it was doing while the LAKES beside it answered the same one.
     *
     * Temperature stays plain, and for the original reason: nothing carries a temperature
     * between waters, so colouring a river from a gauge 40 km away would be inventing a
     * reading for water nobody measured. The dots say what they know; the rivers say
     * nothing.
     */
    // On the Map tab: the day's status, once the index has loaded and matched; plain before.
    stream: mixedPair ? "plain"
      : onConditions
        ? (quantity === "temperature" ? "plain" : "standing")
        : statusOk ? "status" : "plain",
    // Lakes answer the same question as the rivers here, from `lake_gauge` — a station
    // sitting IN the lake. Under depth and temperature they go plain for the same reason
    // the rivers do: nothing can carry either to a water with no station of its own.
    lake: mixedPair ? "plain"
      : onConditions
        ? (flowParam === "temperature" ? "plain" : "standing")
        // A lake left "plain" in Layers wears the day's status like the rivers; Stocked and
        // Depth are questions the reader asked instead.
        : (lakeChoice?.mode ?? "plain") === "plain" && statusOk ? "status"
          : lakeChoice?.mode ?? "plain",
    gauges: quantity === "temperature" ? "temperature" : "standing",
    /*
     * THE FIELD. It has to be in this list or it is never painted at all: the map sets a
     * layer's colour from `modes`, and a layer nobody names keeps whatever the style shipped
     * — which for this one is nothing, so it drew as an invisible polygon over the province.
     *
     * `standing` only where the rivers are also on `standing` — which now includes LEVEL.
     * The field is the zoomed-out form of the same answer, so gating it more tightly than
     * the rivers meant zooming out of a coloured Level map turned the province blank.
     * Under temperature the models publish no field to colour it from; on the Map tab it is
     * not the question being asked.
     */
    basin: mixedPair ? "plain"
      : onConditions && quantity !== "temperature" ? "standing" : "plain",
  };
  const activeGroups = { ...groups };
  if (lakeChoice?.group) activeGroups[lakeChoice.group] = true;

  /**
   * Tapping the map. A tile carries a `section_id`, and a section is not something a person
   * asked about — they tapped a river. So resolve it to the ITEM and open that water's
   * sheet. A tap that lands on water with no registry item resolves to null, and nothing
   * opens, which is the honest outcome: there is no sheet to show.
   */
  const onPressFeature = useCallback(async (_layer: string, featureId: SectionKey) => {
    // The tile's feature id IS the section handle — see SectionId in @app/data.
    const found = await source.itemForSection(Number(featureId) as SectionId);
    if (found) { setPicked(null); setItem(found); }
  }, [source]);

  /**
   * Open a water from a list — and forget whichever reach the Conditions tab was on.
   *
   * The remembered reach exists so the Regulations round trip comes back where it started.
   * It must not survive a jump to a DIFFERENT water, or the Conditions toggle on the
   * Coquihalla's sheet would open the Fraser.
   */
  const openWater = useCallback((id: ItemId) => {
    setCondSection(null);
    setCondAt(null);
    setItem(id);
  }, []);

  const viewing = openSpot === null ? null : spots.find((s) => s.id === openSpot) ?? null;
  if (viewing) {
    const dirty = edit !== null
      && (edit.title !== viewing.title || edit.notes !== viewing.notes);
    const close = () => { setOpenSpot(null); setEdit(null); };
    return (
      <View style={{ flex: 1, backgroundColor: palette.card }}>
        <View style={{ flex: 1 }}>
          <SpotScreen spot={viewing} palette={palette}
                      mode={edit ? "edit" : "view"}
                      title={edit?.title} notes={edit?.notes}
                      onTitle={edit ? (t) => setEdit({ ...edit, title: t }) : undefined}
                      onNotes={edit ? (n) => setEdit({ ...edit, notes: n }) : undefined}
                      dirty={dirty}
                      onBack={close}
                      onEdit={onSaveSpot &&
                        (() => setEdit({ title: viewing.title, notes: viewing.notes }))}
                      onDiscard={() => setEdit(null)}
                      onSave={edit && onSaveSpot ? () => {
                        void onSaveSpot({ ...viewing, title: edit.title || viewing.title,
                                          notes: edit.notes, updatedAt: Date.now() });
                        setEdit(null);
                      } : undefined}
                      onDelete={onDeleteSpot && (() => { close(); onDeleteSpot(viewing.id); })}
                      onShowOnMap={() => {
                        // The camera is a ref, so setting it and then mounting the map is
                        // what moves it — MapScreen reads the ref when it mounts.
                        camera.current = { lon: viewing.lon, lat: viewing.lat, zoom: 14 };
                        setFocus({ lat: viewing.lat, lon: viewing.lon });
                        close();
                        setItem(null);
                        setTab("map");
                      }}
                      onRefresh={onRefreshSpots && (() => onRefreshSpots())}
                      refreshing={refreshing} />
        </View>
        <TabBar active={tab} palette={palette}
                onChange={(k) => { close(); setItem(null); setCondSection(null);
                                   setPicked(null); setTab(k); }} />
      </View>
    );
  }

  if (adding && onSaveSpot)
    return (
      <SpotCapture source={source} weather={weather} tiles={tiles} palette={palette}
                   // So the spot freezes the ESTIMATE the reader was looking at,
                   // not just the matched station's own number.
                   feed={feed}
                   theme={theme} camera={camera.current}
                   onCancel={() => setAdding(false)}
                   onSaved={(s) => { setAdding(false); void onSaveSpot(s); setTab("spots"); }} />
    );

  const body = item !== null
    ? <WaterScreen source={source} item={item} palette={palette}
                   // NO `feed`. The sheet quotes no number, so it reads no gauge data —
                   // see the note in WaterScreen about why the hooks came out with the UI.
                   // The toggle LEAVES for the one conditions screen rather than rendering
                   // a second one here. Opening it from a list means there is no tapped
                   // point, so the route map has no "you are here" — which is honest: the
                   // reader did not choose one.
                   /*
                    * BACK TO THE REACH YOU CAME FROM, when there is one.
                    *
                    * `sec` is this water's FIRST reach — the right answer when the sheet
                    * was opened from a search or a list, and the wrong one when the reader
                    * arrived from a tap two hundred kilometres upstream. `condSection` is
                    * that tap, kept across the round trip and cleared by every route that
                    * opens a DIFFERENT water (search, a map tap, a tab change).
                    */
                   onConditions={(sec) => {
                     if (!condSection) { setCondAt(null); setCondSection(sec); }
                     setItem(null); setTab("conditions");
                   }}
                   onBack={() => setItem(null)} />
    : tab === "search"
      ? <SearchScreen source={source} palette={palette}
                      onPick={(id) => {
                        // Frame it on the map AND open it: back from the page lands on
                        // the water, lit, rather than on the search list.
                        setPicked(id); setTab("map"); openWater(id);
                      }}
                      total={counts?.waters} tiles={tiles} theme={theme}
                      basemap={layers.basemap} />
      : tab === "conditions" && condSection
        ? <ConditionsScreen source={source} section={condSection} palette={palette}
                            tiles={tiles} theme={theme}
                            // Temperature is not a hydrograph, so the chart falls back to
                            // whatever the station itself leads with. Flow and Level are
                            // now passed straight through — the map and the chart ask the
                            // same question, which they could not while "both" existed.
                            parameter={flowParam === "temperature" ? undefined : flowParam}
                            onParameter={setFlowParam}
                            from={condAt}
                            // The same section -> item resolution a map tap uses, so the
                            // two cannot answer differently.
                            /*
                             * THE REACH IS REMEMBERED, not discarded.
                             *
                             * This cleared it, and the way back — the Regulations sheet's
                             * own Conditions toggle — then had nothing to return to and
                             * sent the reader to the FIRST reach of the whole water. Tap a
                             * spot on the Fraser near Hope, look at the rules, come back,
                             * and you were at Deas Island, 200 km downstream, with no "you
                             * are here". One component, two completely different answers,
                             * which reads as two different screens.
                             */
                            onRegulations={(sec) => {
                              setItem(null);
                              void onPressFeature("stream", sec);
                            }}
                            onBack={() => setCondSection(null)} feed={feed}
                            credits={attribution} horizon={horizon}
                            onHorizon={setHorizon} />
      : tab === "map" || tab === "conditions"
        ? <MapScreen at={tiles} palette={palette} theme={theme}
                     camera={camera.current}
                     marker={focus}
                     basemap={layers.basemap}
                     // THE CHOSEN WATER, whole, on the Map tab only — the Conditions view
                     // lights a gauge's route with the same feature-state and must not
                     // inherit a search.
                     fit={tab === "map" && pickFocus
                       ? { key: pickFocus.key, bbox: pickFocus.bbox, refine: pickFocus.refine }
                       : null}
                     highlight={tab === "map" ? pickFocus?.highlight ?? NONE : NONE}
                     data={tab === "conditions" ? conditionData : statusMapData}
                     // The dots belong to the Conditions tab — AND to the temperature
                     // choice, which is a question about the stations themselves and so
                     // must be answerable from the map without changing tab.
                     gauges={onConditions ? gauges ?? undefined : undefined}
                     onVisible={noteVisible}
                     onMoved={(at) => {
                       camera.current = at;
                       // The ref is deliberate — see below — but ONE BIT of the camera is
                       // render state: which side of the handover we are on decides whether
                       // the field is drawn and whether a tap does anything. Set only when
                       // it changes, so panning still costs no renders.
                       setZoomedOut((was) => (at.zoom < HANDOVER_Z) === was
                         ? was : at.zoom < HANDOVER_Z);
                     }}
                     view="plain" modes={modes}
                     /*
                      * NOT `groups`. Which layers the Conditions view draws is stated in
                      * the style (`views[].hide`), because that is a property of the view
                      * rather than of a screen. This tried `{...activeGroups, admin: false}`
                      * and `admin` is deliberately not toggleable — the adapter refused the
                      * call, the boundaries kept drawing, and the console filled up.
                      */
                     groups={activeGroups}
                     // The list is the style's; the trigger is this screen's, because the
                     // app drives by mode and never renders the named view.
                     hide={tab === "conditions" ? hiddenLayers("conditions") : []}
                     // The forecast horizons, on the Conditions tab. Temperature has no
                     // forecast, so it gets none.
                     horizons={tab === "conditions" && flowParam !== "temperature"
                       ? { days: HORIZONS, value: horizon,
                           onPick: (d) => setHorizon(d as Horizon) }
                       : undefined}
                     // NO LAYERS BUTTON IN THE CONDITIONS VIEW. Everything the sheet
                     // offers — how to colour streams and lakes — is decided by the
                     // Showing control here instead, so the button would open a sheet
                     // whose choices this tab overrides. A control that does nothing is
                     // worse than a missing one.
                     onLayers={onConditions ? undefined : () => setLayersOpen(true)}
                     onPressFeature={tab === "conditions"
                       // The COORDINATE too: "how does this spot reach the gauge" is a
                       // question about a point on a river, not about the river.
                       //
                       // AND NOTHING IS TAPPABLE BELOW THE HANDOVER. Down there the map is
                       // catchments, and the only thing under a finger is a shape the size
                       // of a valley — opening a reach sheet for it would answer a question
                       // about one river with a number about a region. The rivers come back
                       // at z9 and they are what you tap.
                       ? (_l, id, lat, lon) => {
                           if ((camera.current?.zoom ?? HOME.zoom) < HANDOVER_Z) return;
                           // Converted, not cast — see noteVisible above. This is the
                           // Conditions tap, so a string here would open a sheet whose
                           // every lookup misses while the screen looks fine.
                           setCondSection(Number(id) as SectionId);
                           setCondAt(lat !== undefined && lon !== undefined
                             ? { lat, lon } : null);
                         }
                       : onPressFeature}
                     onError={(e) => console.error("map:", e.message)} />
        : <SpotsScreen palette={palette} spots={spots}
                       onOpen={setOpenSpot}
                       onAdd={() => setAdding(true)}
                       onRefresh={onRefreshSpots} refreshing={refreshing} />;

  /*
   * THE LEGEND BELONGS TO THE MAP, so it goes when the map does.
   *
   * `condSection` means the Conditions tab has been drilled into one reach and the map is
   * no longer on screen. The Showing control and the colour ramp were still rendered under
   * that sheet, where they control nothing a reader can see — switching to Temp there
   * repainted a map behind the panel, and the ramp described a legend for water that is not
   * being drawn. Same reason `item` hides them: a detail view is not a map.
   */
  /*
   * THE MAP TAB HAS A LEGEND ONLY WHEN SOMETHING IS COLOURED BY A VALUE: the day's status
   * (once the index is in), or the lakes' Stocked colouring. Plain water needs no key.
   */
  const showLegend = item === null && condSection === null
    && (tab === "conditions"
        || (tab === "map" && (layers.lake === "stocked" || statusOk)));

  return (
    <View style={{ flex: 1, backgroundColor: palette.card }}>
      {/*
        * SAY WHY THE MAP IS GREY.
        *
        * Refusing to colour a mixed pair is only half the fix — an uncoloured map and a map
        * whose data is unusable look identical, and that ambiguity IS the bug this guards:
        * when it happened, the Conditions map went quietly grey and read as a slow feed.
        * So the refusal is stated, in the one place a reader is already looking for what
        * the colours mean.
        */}
      {mixedPair && (
        <View style={{ paddingVertical: 8, paddingHorizontal: 12,
                       backgroundColor: palette.closed }}>
          {/* The error red is the closed red, and its words the on-accent knock-out — the
              same pair a danger button wears. It was its own #7A1F2B behind a white typed
              in twice, a third red with no theme. */}
          <Text style={{ color: palette.onAccent, fontSize: 12, fontWeight: "600" }}>
            Map data is out of step — the tiles and the data bundle came from different
            builds, so nothing is coloured. Rebuild or re-download both.
          </Text>
          <Text style={{ color: palette.onAccent, fontSize: 11, opacity: 0.85, marginTop: 2 }}>
            tiles {vintage.tiles ?? "unknown"} · bundle {vintage.bundle ?? "unknown"}
          </Text>
        </View>
      )}
      <View style={{ flex: 1 }}>{body}</View>
      {showLegend && (
        <LegendStrip palette={palette} full={onConditions}>
          {onConditions && (
            /*
             * THE CONTROL LIVES IN THE LEGEND, not floating over the map.
             *
             * It was absolutely positioned above the legend, which works on a desktop
             * viewport and overlaps it on a phone — the two things at the bottom of the
             * screen were fighting for the same rows. Putting it IN the strip makes the
             * layout do the spacing, so it cannot collide at any width.
             *
             * It also sits directly above the scale it controls, which is the right place
             * for it: the ramp underneath is the answer to whichever of these is chosen.
             */
            <View style={{ flexDirection: "row", justifyContent: "flex-end",
                           marginBottom: 8 }}>
              <ChartControls<FlowParam> palette={palette} value={flowParam}
                                        label="Showing" onPick={setFlowParam}
                                        options={[["discharge", "Flow"],
                                                  ["level", "Level"],
                                                  ["temperature", "Temp"]] as const} />
            </View>
          )}
          {tab === "conditions" ? (
            // READ FROM THE STYLE, never restated. These seven hex values used to be
            // written out here while the map's own ramp resolved to three shades of one
            // blue — so the legend promised red-through-cyan and the map drew a wash of
            // blue. A legend that disagrees with its map is worse than no legend.
            flowParam === "temperature" ? (
              // A DIFFERENT SCALE ENTIRELY, so a different legend. The flow ramp under
              // temperature dots was the map promising a ranking it was not drawing —
              // and these thresholds are absolute degrees, not positions in a record.
              // ONE ROW. The Conditions strip renders its children in a plain column so
              // the flow ramp can span the screen — which stacked these three bands
              // vertically and took three lines of map to say what fits on one. A legend
              // is a caption; the moment it needs three rows it has become a panel.
              <View style={{ flexDirection: "row", alignItems: "center", gap: 18,
                             flexWrap: "wrap" }}>
                <LegendCount palette={palette} colour={palette.open} label="under 18 °C" />
                <LegendCount palette={palette} colour={palette.restricted} label="18–20 °C" />
                <LegendCount palette={palette} colour={palette.closed} label="20 °C and over" />
              </View>
            ) : (
              <LegendRamp palette={palette}
                          low={flowParam === "level" ? "low stage for the date"
                               : flowParam === "discharge" ? "low flow for the date"
                               : "low for the date"}
                          mid="normal" high="high"
                          stops={flowRamp(theme)} marks={visibleMarks} />
            )
          ) : layers.lake === "stocked" ? (
            palette.stock.map((c, i) => (
              <LegendCount key={c} palette={palette} colour={c}
                           label={STOCK_BANDS[i]!.label} />
            ))
          ) : (
            // The three statuses, in the words and colours every surface uses (@app/core
            // WATER_STATUS through the map's theme) — for today.
            WATER_STATUSES.map((s) => (
              <LegendCount key={s} palette={palette} colour={palette.waterStatus[s]}
                           line
                           label={s === "closed" ? "Closed today" : WATER_STATUS[s].label} />
            ))
          )}
        </LegendStrip>
      )}
      {/* A tab change is a change of subject: the remembered reach goes with it. */}
      <TabBar active={tab} palette={palette}
              onChange={(k) => { setItem(null); setCondSection(null); setCondAt(null);
                                 setPicked(null); setTab(k); }} />

      <LayersSheet open={layersOpen} onClose={() => setLayersOpen(false)} palette={palette}
                   state={layers} onState={setLayers}
                   theme={themeName} onTheme={onTheme}
                   reaches={counts?.reaches} surveyed={counts?.surveyed ?? undefined}
                   stations={facts.state === "ready"
                     ? facts.value.liveStations ?? counts?.stations : undefined}
                   fetchedAt={facts.state === "ready" ? facts.value.fetchedAt : undefined}
                   attribution={attribution} />
    </View>
  );
}

