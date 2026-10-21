/**
 * The app: four tabs, the map as home, and a legend that always says what the colours mean.
 *
 * Drawn to `design/riffle.html`. The shell owns the state that more than one screen needs —
 * which tab, which water, which colouring, which layers, which palette — and hands it down.
 * Every answer on every screen comes from a hook (rule 25). The Map tab colours water by the
 * day's status from the status index (closed / own regulations / base); the regulation records
 * themselves are not integrated, so a water's sheet shows a placeholder where they will go.
 */
import { dayLabel, TEMP_LEGEND, WATER_STATUS, WATER_STATUSES, type SectionKey, type TempBand } from "@app/core";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Platform, Text, View } from "react-native";
import type { ItemId, RegsSource, SectionId } from "@app/data";
import { bottomBarShown, spotSaveFailed, useMapModes, usePhotoEdit, useRoute, type BrowseTab, type FlowParam,
         type SpotPhotos } from "@app/ui";
import { useBack, useEdgeBack, usePartSwipe, type SystemBack } from "@app/ui";
import { useBasinStandings, useDataFacts, useGaugeGeoJSON, usePanelStandings,
         usePickFocus, usePairing, useRegulationKeys, useStatusIndex, statusData,
         type Horizon, type PairingInput, type StatusSource } from "@app/ui";
import type { Spot, WeatherSource } from "@app/data/spots";
import { hiddenLayers, toggleableGroups,
         type Camera, type TileEndpoints } from "@app/map";
import { ChartControls } from "./ChartControls";
import { LegendCount, LegendRamp, LegendStrip } from "./Chrome";
import { MapDay } from "./MapDay";
import { LayersSheet, STOCK_BANDS, lakeChoices, type LayersState } from "./LayersSheet";
import { MapScreen } from "./MapScreen";
import { SearchScreen } from "./SearchScreen";
import { SpotsScreen } from "./SpotsScreen";
import { SpotCapture } from "./SpotCapture";
import { ConditionsScreen } from "./ConditionsScreen";
import { SpotScreen } from "./SpotScreen";
import { TabBar, type TabKey } from "./TabBar";
import { WaterScreen } from "./WaterScreen";
import { EdgeBack } from "./EdgeBack";
import { PartSwipe } from "./PartSwipe";
import type { FishImages } from "./regs/types";
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

/** A temperature band's colour: the "may I fish here" colours, as the map paints the dots (runtime-style). */
const tempColour = (p: Palette): Record<TempBand, string> =>
  ({ cool: p.open, warm: p.restricted, critical: p.closed });

export function Shell({ source, palette, theme, themeName, onTheme, tiles,
                       spots = [], onSaveSpot, weather, onRefreshSpots, onDeleteSpot, refreshing, feed,
                       spotsError, attribution, fishImages, photos, status = null, pairing = null,
                       systemBack = null }: {
  source: RegsSource; palette: Palette;
  /**
   * Where the day's status comes from: the release's paint (P11), or on the dev bundle path status_index.bin. Absent
   * -> the water is drawn plain.
   */
  status?: StatusSource | null;
  /** What says the data and the tiles are one pair (P11 `usePairing`). Absent -> not established: nothing coloured. */
  pairing?: PairingInput | null;
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
  onRefreshSpots?: () => void | Promise<void>;
  /** Awaited like a save: a rejection is shown and the spot stays open (P01). */
  onDeleteSpot?: (id: string) => void | Promise<void>;
  /** The spots could not be read (a corrupt file, set aside): said on the Spots tab, never an empty list (P01). */
  spotsError?: string | null;
  /**
   * The live gauge feed. Absent means the Conditions view colours nothing — which is the
   * honest rendering of "we could not check", and never a map of droughts.
   */
  feed?: import("@app/data").GaugeFeed;
  refreshing?: boolean;
  attribution?: readonly string[];
  /** The fish pictures by species code, registered by the app (T20). Absent -> silhouettes. */
  fishImages?: FishImages;
  /** S1: a spot's photographs — the store and the platform's picker (the app's). Absent -> no photos offered. */
  photos?: SpotPhotos;
  /** G1: the platform's own back (Android `BackHandler`, the browser's history), from the app. Absent -> ‹ only. */
  systemBack?: SystemBack | null;
}) {
  /*
   * HOW MUCH DATA IS IN HERE — read, never stated.
   *
   * These five figures used to arrive as props from `App.tsx`, hardcoded from the design
   * mock: 255 waters, 6,967 reaches, 35 surveyed lakes, 5 stations and a fixed timestamp.
   * The bundle the app actually opens holds 19,699 waters and 2,324 stations. A screen may
   * not assert a fact about the data it is sitting on; it asks — when a screen that shows them is up (P11: on the
   * release the counts open core, which a cold start does not fetch before the first map). Read below, by `factsWanted`.
   */
  /*
   * THE ROUTE (P13): what is on screen is ONE value, moved by one reducer (`@app/ui` route.ts). The tab, the water
   * open, the spot, the add flow and the Conditions drill are read from it; every way of moving dispatches to it.
   * Water mode keeps the browse state underneath (`from`), so back restores it whole.
   */
  const [route, go] = useRoute();
  const browse = route.mode === "browse" ? route : route.from;
  const tab: TabKey = browse.tab;
  const item: ItemId | null = route.mode === "water" ? route.stack[route.stack.length - 1]!.item : null;
  // the water's regulations read (P11): on the release, opening it opens core, water and its region's file — a water in
  // another region fetches that region's. The page that draws them is T01/T03's; until then the placeholder stands.
  useRegulationKeys(source, item);
  const adding = route.mode === "browse" && route.adding;
  const openSpot = route.mode === "browse" ? route.spot : null;
  const condSection: SectionId | null = route.mode === "browse" ? route.cond?.section ?? null : null;
  const condAt = route.mode === "browse" ? route.cond?.at ?? null : null;
  const toTab = (k: BrowseTab) => { setPicked(null); go({ type: "tab", tab: k }); };
  /*
   * G1: ONE BACK for every input — ‹, the platform's system back, the iOS edge swipe — and the sideways part swipe.
   * The part order is the water map's (P15 `useWaterMap`), not read yet: until it is, the swipe is off.
   */
  const { back, depth } = useBack(route, go, systemBack);
  const edge = useEdgeBack(back, Platform.OS, depth);
  const partSwipe = usePartSwipe(route, go, null);
  /** The day the map's status colours answer for; null is today. */
  const [mapDay, setMapDay] = useState<Date | null>(null);
  /**
   * The water chosen from SEARCH, which the map opens on and lights when the reader comes
   * back from its page — riffle's `goTo`: frame the water, then open it. Cleared by
   * anything that changes the subject (a tap on another water, a tab change), so a later
   * remount of the map never drags the camera back to an old search.
   */
  const [picked, setPicked] = useState<ItemId | null>(null);
  const pickFocus = usePickFocus(source, picked);
  const [layersOpen, setLayersOpen] = useState(false);
  // the figures are drawn by the search screen (its total) and the layers sheet (reaches, stations, age)
  const factsWanted = tab === "search" || layersOpen;
  const facts = useDataFacts(source, feed, factsWanted);
  const counts = facts.state === "ready" ? facts.value.counts : null;
  // Non-null only while editing. The draft lives here rather than in SpotScreen so that
  // leaving the screen cannot strand half an edit inside an unmounted component.
  const [edit, setEdit] = useState<{ title: string; notes: string } | null>(null);
  // S1: the photos of the spot being edited; files added in an edit that is not saved are deleted with it
  const photoEdit = usePhotoEdit(photos);
  // P01: a failed save or delete, said on the spot's screen; and a tab change asked for while an edit is unsaved
  const [spotError, setSpotError] = useState<string | null>(null);
  const [pendingTab, setPendingTab] = useState<TabKey | null>(null);
  // A pin dropped on the map when you ask to see a spot there. Cleared when you pan away
  // to something else, so it never lingers as a mystery dot.
  const [focus, setFocus] = useState<{ lat: number; lon: number } | null>(null);
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
  // HOW FAR AHEAD the Conditions panel answers. 0 is now, and is where it opens: a
  // forecast is what you ask for, never what you are shown without asking.
  const [horizon, setHorizon] = useState<Horizon>(0);
  /*
   * BACK TO NOW WHEN THE PANEL CLOSES (P16 item 2). The horizon is chosen in the Conditions
   * panel's header and nowhere else — the map's corner copy is gone — so the map, which has
   * no control saying which horizon it paints, must not keep painting one picked in a panel
   * that is no longer on screen.
   */
  useEffect(() => { if (condSection === null) setHorizon(0); }, [condSection]);
  // Which side of the handover zoom the camera is on — the one bit of the camera that is render state (see onMoved).
  const [zoomedOut, setZoomedOut] = useState(HOME.zoom < HANDOVER_Z);
  // Streams, lakes and gauges each carry their own colouring, as the design has it — plain rivers while the lakes
  // show Stocked is a normal thing to want.
  const [layers, setLayers] = useState<LayersState>(
    { lake: "plain", basemap: "map" });
  const [groups, setGroups] = useState<Record<string, boolean>>(
    () => Object.fromEntries(toggleableGroups().map((g) => [g.id, g.defaultVisible])));
  /*
   * TILES AND BUNDLE FROM ONE ATLAS, or nothing is coloured.
   *
   * A section is an integer handle into the atlas's table, and both artifacts carry it. A
   * mixed pair does not fail to match — it matches the WRONG SECTION, so every colour on
   * screen would be a real reading about a different river. That is the one outcome this
   * app may never produce, and it has happened once already: the tiles were rebuilt and
   * `bundle.sqlite` was left behind, and the map went quietly grey.
   *
   * COLOUR ONLY ON A PROVEN PAIR (P11, `usePairing`): `ok === true` colours. `false` is a mixed
   * pair, refused and SAID (the banner below); `null` — not established yet, or no tile version
   * known — colours nothing and raises no alarm, which is not evidence of a mismatch.
   */
  const vintage = usePairing(pairing);
  const paired = vintage.ok === true;
  const mixedPair = vintage.ok === false;
  /*
   * THE DAY'S STATUS, on the Map tab, from the release's paint (`useStatusIndex`; checked against
   * the release's handles when decoded). Until it has loaded the water stays plain: "not asked" is
   * never drawn as "base".
   */
  const statusIndex = useStatusIndex(status);
  const statusOk = statusIndex !== null && paired;
  const lakeChoice = lakeChoices(palette).find((c) => c.k === layers.lake);
  /*
   * WHAT EACH LAYER IS COLOURED BY (P12): the quantity, the percentile asked of the panel, the field and every layer's
   * mode, decided in `@app/ui` (`useMapModes`) and only drawn here.
   */
  const mm = useMapModes({ tab, flowParam, horizon, zoomedOut, paired, statusOk, lake: lakeChoice, groups });
  const { onConditions, quantity, modes, activeGroups } = mm;
  // FROM THE PANEL, not from `section_gauge`. The single-station join left the Harrison
  // grey for its whole length because the station matched to those reaches had gone quiet,
  // while a tap on that same grey opened a sheet answering confidently from two other
  // gauges. One arithmetic now serves both — see `usePanelStandings`.
  const standings = usePanelStandings(source, feed, visible, mm.standingParam, mm.standingHorizon);
  // THE FIELD, and only where it is the answer — see `mapModes` for when.
  const basins = useBasinStandings(source, feed, mm.fieldOn, mm.standingParam, horizon);
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
  const statusMapData = useMemo(
    () => (statusOk && tab === "map" ? statusData(statusIndex, visible, mapDay ?? new Date()) : undefined),
    [statusOk, statusIndex, tab, visible, mapDay]);

  /**
   * Tapping the map. A tile carries a `section_id`, and a section is not something a person
   * asked about — they tapped a river. So resolve it to the ITEM and open that water's
   * sheet. A tap that lands on water with no registry item resolves to null, and nothing
   * opens, which is the honest outcome: there is no sheet to show.
   */
  const onPressFeature = useCallback(async (_layer: string, featureId: SectionKey) => {
    // The tile's feature id IS the section handle — see SectionId in @app/data.
    const sid = Number(featureId) as SectionId;
    const found = await source.itemForSection(sid);
    if (found) { setPicked(null); go({ type: "openWater", item: found, tapSid: sid }); }
  }, [source, go]);

  /**
   * Open a water from a list — and forget whichever reach the Conditions tab was on.
   *
   * The remembered reach exists so the Regulations round trip comes back where it started.
   * It must not survive a jump to a DIFFERENT water, or the Conditions toggle on the
   * Coquihalla's sheet would open the Fraser.
   */
  const openWater = useCallback((id: ItemId) => {
    go({ type: "tab", tab: "map" });
    go({ type: "openWater", item: id });
  }, [go]);

  const viewing = openSpot === null ? null : spots.find((s) => s.id === openSpot) ?? null;
  if (viewing) {
    const dirty = edit !== null
      && (edit.title !== viewing.title || edit.notes !== viewing.notes
          || photoEdit.refs.join("\n") !== viewing.photos.join("\n"));
    // an unsaved edit's added photos go with it; after a save there are none left to delete
    const endEdit = () => { if (edit) void photoEdit.discard(); setEdit(null); };
    const close = () => { endEdit(); setSpotError(null); setPendingTab(null); back(); };
    const goTab = (k: TabKey) => { endEdit(); setSpotError(null); setPendingTab(null); toTab(k); };
    return (
      <View style={{ flex: 1, backgroundColor: palette.card }}>
        <View style={{ flex: 1 }}>
          <SpotScreen spot={viewing} palette={palette}
                      mode={edit ? "edit" : "view"}
                      photoStore={photos?.store} photoEdit={edit ? photoEdit : undefined}
                      title={edit?.title} notes={edit?.notes}
                      onTitle={edit ? (t) => setEdit({ ...edit, title: t }) : undefined}
                      onNotes={edit ? (n) => setEdit({ ...edit, notes: n }) : undefined}
                      dirty={dirty}
                      onBack={close}
                      onEdit={onSaveSpot && (() => {
                        setEdit({ title: viewing.title, notes: viewing.notes });
                        photoEdit.begin(viewing.id, viewing.photos);
                      })}
                      onDiscard={() => { endEdit(); if (pendingTab) goTab(pendingTab); }}
                      error={spotError}
                      askToDiscard={pendingTab !== null}
                      onKeepEditing={() => setPendingTab(null)}
                      onSave={edit && onSaveSpot ? async () => {
                        try {
                          await onSaveSpot({ ...viewing, title: edit.title || viewing.title,
                                             notes: edit.notes, photos: [...photoEdit.refs],
                                             updatedAt: Date.now() });
                          photoEdit.committed();
                          setEdit(null);
                          setSpotError(null);
                          if (pendingTab) goTab(pendingTab);
                        } catch (e) {
                          setPendingTab(null);
                          setSpotError(spotSaveFailed(e));    // the edit stays open, nothing dropped
                        }
                      } : undefined}
                      onDelete={onDeleteSpot && (async () => {
                        try { await onDeleteSpot(viewing.id); close(); }
                        catch (e) { setSpotError(spotSaveFailed(e)); }
                      })}
                      onShowOnMap={() => {
                        // The camera is a ref, so setting it and then mounting the map is
                        // what moves it — MapScreen reads the ref when it mounts.
                        camera.current = { lon: viewing.lon, lat: viewing.lat, zoom: 14 };
                        setFocus({ lat: viewing.lat, lon: viewing.lon });
                        goTab("map");
                      }}
                      onRefresh={onRefreshSpots && (() => onRefreshSpots())}
                      refreshing={refreshing} />
        </View>
        <TabBar active={tab} palette={palette}
                // an unsaved edit is never dropped by a tab change without asking (P01)
                onChange={(k) => (dirty ? setPendingTab(k) : goTab(k))} />
      </View>
    );
  }

  if (adding && onSaveSpot)
    return (
      <SpotCapture source={source} weather={weather} tiles={tiles} palette={palette} photos={photos}
                   // So the spot freezes the ESTIMATE the reader was looking at,
                   // not just the matched station's own number.
                   feed={feed}
                   theme={theme} camera={camera.current}
                   onCancel={back}
                   // awaited: a rejection reaches SpotCapture, which shows it and stays open (P01)
                   onSaved={async (s) => { await onSaveSpot(s); toTab("spots"); }} />
    );

  const body = item !== null
    ? <WaterScreen source={source} item={item} palette={palette} fishImages={fishImages}
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
                     const kept = route.mode === "water" ? route.from.cond : null;
                     go({ type: "back" });
                     go({ type: "condSection", section: kept?.section ?? sec, at: kept?.at ?? null });
                   }}
                   onBack={back} />
    : tab === "search"
      ? <SearchScreen source={source} palette={palette} status={status}
                      onPick={(id) => {
                        // Frame it on the map AND open it: back from the page lands on
                        // the water, lit, rather than on the search list.
                        openWater(id); setPicked(id);
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
                            onRegulations={(sec) => { void onPressFeature("stream", sec); }}
                            onBack={back} feed={feed}
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
                     // NO HORIZONS ON THE MAP (P16 item 2): the one horizon control is the
                     // Conditions panel's header. See the reset by `condSection` above.
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
                           go({ type: "condSection", section: Number(id) as SectionId,
                                at: lat !== undefined && lon !== undefined ? { lat, lon } : null });
                         }
                       : onPressFeature}
                     onError={(e) => console.error("map:", e.message)} />
        : <SpotsScreen palette={palette} spots={spots} error={spotsError ?? null} photoStore={photos?.store}
                       onOpen={(id) => go({ type: "openSpot", id })}
                       onAdd={() => go({ type: "addSpot" })}
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
            tiles {vintage.tiles ?? "unknown"} · bundle {vintage.data ?? "unknown"}
          </Text>
        </View>
      )}
      <View style={{ flex: 1 }}>
        <EdgeBack drag={edge}>{item !== null ? <PartSwipe swipe={partSwipe}>{body}</PartSwipe> : body}</EdgeBack>
      </View>
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
              // The bands and their words are the publisher's (`TEMP_LEGEND`, from `_TEMP_BANDS`); the colours are
              // the map's own for them (runtime-style `tempExpression`).
              <View style={{ flexDirection: "row", alignItems: "center", gap: 18,
                             flexWrap: "wrap" }}>
                {TEMP_LEGEND.map((b) => (
                  <LegendCount key={b.band} palette={palette} colour={tempColour(palette)[b.band]}
                               label={b.label} />
                ))}
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
            <>
              <MapDay palette={palette} day={mapDay} onDay={setMapDay} />
              {WATER_STATUSES.map((s) => (
                <LegendCount key={s} palette={palette} colour={palette.waterStatus[s]}
                             line
                             label={s === "closed" ? (mapDay ? `Closed on ${dayLabel(mapDay)}` : "Closed today")
                                    : WATER_STATUS[s].label} />
              ))}
            </>
          )}
        </LegendStrip>
      )}
      {/* A tab change is a change of subject: the remembered reach goes with it. Hidden in water mode (P13/P14). */}
      {bottomBarShown(route) && <TabBar active={tab} palette={palette} onChange={toTab} />}

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

