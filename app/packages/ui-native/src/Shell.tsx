/**
 * The app: four tabs, the map as home, and a legend that always says what the colours mean.
 *
 * Drawn to `design/riffle.html`. The shell owns the state that more than one screen needs —
 * which tab, which water, which date, which colouring, which layers, which palette — and
 * hands it down. It holds no regulation logic (rule 25): every answer on every screen comes
 * from a hook.
 */
import { useCallback, useMemo, useRef, useState } from "react";
import { Text, View } from "react-native";
import { statusWord, type Outcome, type PlainDate, type SpeciesGroup } from "@app/core";
import type { ItemId, Parameter, RegsSource, SectionId } from "@app/data";
import { useDataFacts, useGaugeGeoJSON, useStandings, useStatuses,
         type GaugeQuantity } from "@app/ui";

/** What the Conditions view is showing. `both` colours the water by either percentile. */
type FlowParam = Parameter | "both" | "temperature";
import type { Spot, WeatherSource } from "@app/data/spots";
import { toggleableGroups, type Camera, type TileEndpoints } from "@app/map";
import { ChartControls } from "./ChartControls";
import { DateSheet } from "./DateSheet";
import { LegendCount, LegendRamp, LegendStrip } from "./Chrome";
import { LayersSheet, STOCK_BANDS, lakeChoices, streamChoices,
         type LayersState } from "./LayersSheet";
import { MapScreen } from "./MapScreen";
import { SearchScreen } from "./SearchScreen";
import { SpotsScreen } from "./SpotsScreen";
import { SpotCapture } from "./SpotCapture";
import { ConditionsScreen } from "./ConditionsScreen";
import { SpotScreen } from "./SpotScreen";
import { TabBar, type TabKey } from "./TabBar";
import { WaterScreen } from "./WaterScreen";
import { TYPE } from "./type";
import { flowRamp, outcomeColour, type Palette, type ThemeName } from "./theme";

/** Every outcome, in the order a legend reads. Rule 29: each one must be coloured. */
const OUTCOMES: readonly Outcome[] = ["closed", "restricted", "open", "unknown"];

/** Where the map starts the FIRST time. After that the camera is whatever the user left. */
const HOME: Camera = { lon: -121.85, lat: 49.15, zoom: 9.4 };

export function Shell({ source, palette, theme, themeName, onTheme, on, onDateChange, group, tiles,
                       spots = [], onSaveSpot, weather, onRefreshSpots, onDeleteSpot, refreshing, feed,
                       attribution }: {
  source: RegsSource; palette: Palette;
  /** Passed straight to the map. Every theme the style defines is a real map theme,
   *  so nothing has to be laundered into light/dark on the way. */
  theme: string; themeName: ThemeName; onTheme: (t: ThemeName) => void;
  on: PlainDate; group: SpeciesGroup;
  /**
   * Change the date the whole app is answering for. Absent -> the pill does not react,
   * which is at least honest; it used to LOOK pressable and do nothing, because MapScreen
   * declared `onDate` and nothing ever passed one.
   */
  onDateChange?: (d: PlainDate) => void;
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
  const [layersOpen, setLayersOpen] = useState(false);
  const [dateOpen, setDateOpen] = useState(false);
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
   * The reaches the map currently has rendered.
   *
   * BOTH map tabs report now. It used to be Conditions only — but the legend on the
   * regulations map is supposed to carry counts ("29 closed · 33 restricted · …"), which is
   * how `design/riffle.html` draws it and why `LegendCount` has always had an `n` prop with
   * a comment reading "the count is the point". Nothing ever passed one, so the legend was
   * a key rather than a reading: it said what the colours mean and not how much of the
   * screen is each one.
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
    (ids: readonly string[]) => setVisible(ids as readonly SectionId[]), []);
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
  const [flowParam, setFlowParam] = useState<FlowParam>("both");
  const quantity: GaugeQuantity =
    flowParam === "temperature" ? "temperature"
      : flowParam === "level" ? "level" : "flow";
  // The water keeps its last percentile question under temperature — there is nothing to
  // colour it with, so `standings` is simply not asked for.
  const standings = useStandings(source, feed, visible,
                                 flowParam === "temperature" ? "both" : flowParam);
  // Outcomes for what is on screen, for the legend's counts. Same viewport-scoped shape as
  // `useStandings` beside it — the whole table is far too big to hold to answer a question
  // about the few hundred reaches actually rendered.
  const shown = useStatuses(source, tab === "map" ? visible : [], on, group);
  const tally = useMemo(() => {
    const n = new Map<Outcome, number>();
    if (shown.state !== "ready") return n;
    for (const st of shown.value.values())
      n.set(st.outcome, (n.get(st.outcome) ?? 0) + 1);
    return n;
  }, [shown]);
  // Streams, lakes and gauges each carry their own colouring, as the design has it —
  // Rules on the rivers while the lakes show Stocked is a normal thing to want.
  //
  // DECLARED BEFORE THE HOOKS THAT READ IT. `useGaugeGeoJSON` needs to know whether it is
  // fetching the temperature roster or the flow one, and a `const` read above its own
  // initialiser is a ReferenceError at render — which is a blank screen, not a type error,
  // so the compiler said nothing and only opening the page found it.
  const [layers, setLayers] = useState<LayersState>(
    { stream: "rules", lake: "rules", basemap: "map" });
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
        [...standings].map(([section, p]) => [section, { standing: p * 100 }])) }),
    [standings]);
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
  const streamChoice = streamChoices(palette).find((c) => c.k === layers.stream);
  const lakeChoice = lakeChoices(palette).find((c) => c.k === layers.lake);
  const modes = {
    // The Conditions TAB asks a different question, so it overrides the stream colouring
    // while you are on it — EXCEPT under temperature, where the rivers must stay plain.
    // Nothing here can carry a temperature from a station to the water around it yet, and
    // colouring a river from a gauge 40 km away would be inventing a reading for water
    // nobody measured. The dots say what they know; the rivers say nothing.
    // THE RIVERS ARE ONLY COLOURED FOR FLOW. Nothing here can carry a depth or a
    // temperature from a station to the water around it yet — that needs the donor panel —
    // and colouring a river from a gauge 40 km away would be inventing a reading for water
    // nobody measured. Under those two the dots say what they know and the rivers say
    // nothing, which is the truth today.
    stream: onConditions
      ? (quantity === "flow" ? "standing" : "plain")
      : streamChoice?.mode ?? "plain",
    // Lakes answer the same question as the rivers here, from `lake_gauge` — a station
    // sitting IN the lake. Under depth and temperature they go plain for the same reason
    // the rivers do: nothing can carry either to a water with no station of its own.
    lake: onConditions
      ? (flowParam === "temperature" ? "plain" : "standing")
      : lakeChoice?.mode ?? "plain",
    gauges: quantity === "temperature" ? "temperature" : "standing",
  };
  const activeGroups = { ...groups };
  for (const c of [streamChoice, lakeChoice])
    if (c?.group) activeGroups[c.group] = true;

  /**
   * Tapping the map. A tile carries a `section_id`, and a section is not something a person
   * asked about — they tapped a river. So resolve it to the ITEM and open that water's
   * sheet. A tap that lands on water with no registry item resolves to null, and nothing
   * opens, which is the honest outcome: there is no sheet to show.
   */
  const onPressFeature = useCallback(async (_layer: string, featureId: string) => {
    const found = await source.itemForSection(featureId as SectionId);
    if (found) setItem(found);
  }, [source]);

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
                                   setTab(k); }} />
      </View>
    );
  }

  if (adding && onSaveSpot)
    return (
      <SpotCapture source={source} weather={weather} tiles={tiles} palette={palette}
                   theme={theme} camera={camera.current} on={on} group={group}
                   onCancel={() => setAdding(false)}
                   onSaved={(s) => { setAdding(false); void onSaveSpot(s); setTab("spots"); }} />
    );

  const body = item !== null
    ? <WaterScreen source={source} item={item} on={on} group={group} palette={palette}
                   // The toggle LEAVES for the one conditions screen rather than rendering
                   // a second one here. Opening it from a list means there is no tapped
                   // point, so the route map has no "you are here" — which is honest: the
                   // reader did not choose one.
                   onConditions={(sec) => { setCondAt(null); setCondSection(sec);
                                            setItem(null); setTab("conditions"); }}
                   onBack={() => setItem(null)} />
    : tab === "search"
      ? <SearchScreen source={source} palette={palette} onPick={setItem}
                      total={counts?.waters} tiles={tiles} theme={theme} />
      : tab === "conditions" && condSection
        ? <ConditionsScreen source={source} section={condSection} palette={palette}
                            tiles={tiles} theme={theme}
                            // "both" is a MAP setting, not a chart one: a chart has to be
                            // about one quantity or its axis means nothing. The sheet then
                            // falls back to whatever the station itself leads with.
                            parameter={flowParam === "both" || flowParam === "temperature"
                                         ? undefined : flowParam}
                            onParameter={setFlowParam}
                            from={condAt}
                            // The same section -> item resolution a map tap uses, so the
                            // two cannot answer differently.
                            onRegulations={(sec) => { setCondSection(null);
                                                      void onPressFeature("stream", sec); }}
                            onBack={() => setCondSection(null)} />
      : tab === "map" || tab === "conditions"
        ? <MapScreen at={tiles} palette={palette} theme={theme} on={on}
                     camera={camera.current}
                     marker={focus}
                     data={tab === "conditions" ? conditionData : undefined}
                     // The dots belong to the Conditions tab — AND to the temperature
                     // choice, which is a question about the stations themselves and so
                     // must be answerable from the map without changing tab.
                     gauges={onConditions ? gauges ?? undefined : undefined}
                     onVisible={noteVisible}
                     onMoved={(at) => { camera.current = at; }}
                     view="plain" modes={modes} groups={activeGroups}
                     onDate={onDateChange ? () => setDateOpen(true) : undefined}
                     // NO LAYERS BUTTON IN THE CONDITIONS VIEW. Everything the sheet
                     // offers — how to colour streams and lakes — is decided by the
                     // Showing control here instead, so the button would open a sheet
                     // whose choices this tab overrides. A control that does nothing is
                     // worse than a missing one.
                     onLayers={onConditions ? undefined : () => setLayersOpen(true)}
                     onPressFeature={tab === "conditions"
                       // The COORDINATE too: "how does this spot reach the gauge" is a
                       // question about a point on a river, not about the river.
                       ? (_l, id, lat, lon) => {
                           setCondSection(id as SectionId);
                           setCondAt(lat !== undefined && lon !== undefined
                             ? { lat, lon } : null);
                         }
                       : onPressFeature}
                     onError={(e) => console.error("map:", e.message)} />
        : <SpotsScreen palette={palette} spots={spots}
                       onOpen={setOpenSpot}
                       onAdd={() => setAdding(true)}
                       onRefresh={onRefreshSpots} refreshing={refreshing} />;

  const showLegend = item === null && (tab === "map" || tab === "conditions");

  return (
    <View style={{ flex: 1, backgroundColor: palette.card }}>
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
                                        options={[["both", "Both"], ["discharge", "Flow"],
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
              <>
                <LegendCount palette={palette} colour={palette.open} label="under 18 °C" />
                <LegendCount palette={palette} colour={palette.restricted} label="18–20 °C" />
                <LegendCount palette={palette} colour={palette.closed} label="20 °C and over" />
              </>
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
            // The word comes from core, not from a ternary here. This line used to say
            // "limited" where StatusPill says "RESTRICTED" — two vocabularies for one
            // outcome, in one screen, which is the drift rule 23 exists to stop.
            OUTCOMES.map((k) => (
              <LegendCount key={k} palette={palette} colour={outcomeColour(palette, k)}
                           /*
                            * Undefined until the answer arrives; 0 once it has.
                            *
                            * The distinction is the same one `count()` makes: before the
                            * query returns we do not know, and a flashed "0" would say we
                            * looked. After it returns, an outcome with no reaches on screen
                            * genuinely has none — and "0 closed" is worth reading, because
                            * it is the difference between "nothing here is shut" and "we
                            * did not check". Riffle draws all four for the same reason.
                            */
                           n={tab === "map" && shown.state === "ready"
                             ? tally.get(k) ?? 0 : undefined}
                           label={statusWord({ outcome: k, provenance: "specific",
                                               from: [] }).toLowerCase()} />
            ))
          )}
        </LegendStrip>
      )}
      <TabBar active={tab} onChange={(k) => { setItem(null); setTab(k); }} palette={palette} />

      {onDateChange && (
        <DateSheet open={dateOpen} onClose={() => setDateOpen(false)} palette={palette}
                   value={on} onChange={onDateChange} />
      )}

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

