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
import type { ItemId, RegsSource, SectionId } from "@app/data";
import { useGaugeGeoJSON, useStandings } from "@app/ui";
import type { Spot, WeatherSource } from "@app/data/spots";
import { toggleableGroups, type Camera, type TileEndpoints } from "@app/map";
import { LegendCount, LegendRamp, LegendStrip } from "./Chrome";
import { LayersSheet, STOCK_BANDS, lakeChoices, streamChoices,
         type LayersState } from "./LayersSheet";
import { MapScreen } from "./MapScreen";
import { SearchScreen } from "./SearchScreen";
import { SpotsScreen } from "./SpotsScreen";
import { SpotCapture } from "./SpotCapture";
import { SpotScreen } from "./SpotScreen";
import { TabBar, type TabKey } from "./TabBar";
import { WaterScreen } from "./WaterScreen";
import { TYPE } from "./type";
import { flowRamp, outcomeColour, type Palette, type ThemeName } from "./theme";

/** Every outcome, in the order a legend reads. Rule 29: each one must be coloured. */
const OUTCOMES: readonly Outcome[] = ["closed", "restricted", "open", "unknown"];

/** Where the map starts the FIRST time. After that the camera is whatever the user left. */
const HOME: Camera = { lon: -121.85, lat: 49.15, zoom: 9.4 };

export function Shell({ source, palette, theme, themeName, onTheme, on, group, waters, tiles,
                       spots = [], onSaveSpot, weather, onRefreshSpots, onDeleteSpot, refreshing, feed,
                       reaches, surveyed, stations,
                       fetchedAt, attribution }: {
  source: RegsSource; palette: Palette;
  /** Passed straight to the map. Every theme the style defines is a real map theme,
   *  so nothing has to be laundered into light/dark on the way. */
  theme: string; themeName: ThemeName; onTheme: (t: ThemeName) => void;
  on: PlainDate; group: SpeciesGroup;
  /** How many named waters the bundle holds, for the search header. */
  waters?: number;
  /** Where the two pmtiles archives are served from. */
  tiles: TileEndpoints;
  /** Scale, for the Layers panel: how much map each choice affects. */
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
  reaches?: number; surveyed?: number; stations?: number;
  fetchedAt?: string | null;
  attribution?: readonly string[];
}) {
  const [tab, setTab] = useState<TabKey>("map");
  const [item, setItem] = useState<ItemId | null>(null);
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
  // The reaches the map currently has rendered. Only the Conditions view needs them, so
  // nothing is queried while the map is showing regulations.
  const [visible, setVisible] = useState<readonly SectionId[]>([]);
  // Percentiles for what is on screen. Empty while the feed is unreachable, which paints
  // nothing rather than painting every reach as a drought.
  // STABLE IDENTITY MATTERS HERE. As an inline arrow this changed every render, so the
  // effect holding the map's `idle` listener tore itself down and re-registered constantly
  // — and `idle` fires once when movement settles, so the listener was frequently absent
  // at the only moment it had anything to hear. The map reported nothing, `visible` stayed
  // empty, and every reach painted as `missing`.
  const noteVisible = useCallback(
    (ids: readonly string[]) => setVisible(ids as readonly SectionId[]), []);
  const standings = useStandings(source, feed, visible);
  const gauges = useGaugeGeoJSON(source, feed, tab === "conditions");
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
      [...standings].map(([section, p]) => [section, { standing: p * 100 }])) }),
    [standings]);
  // Streams and lakes carry their own colouring, as the design has it — Rules on the
  // rivers while the lakes show Stocked is a normal thing to want.
  const [layers, setLayers] = useState<LayersState>(
    { stream: "rules", lake: "rules", basemap: "map" });
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
    // while you are on it.
    stream: tab === "conditions" ? "standing" : streamChoice?.mode ?? "plain",
    lake: lakeChoice?.mode ?? "plain",
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
                onChange={(k) => { close(); setItem(null); setTab(k); }} />
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
                   onBack={() => setItem(null)} />
    : tab === "search"
      ? <SearchScreen source={source} palette={palette} onPick={setItem} total={waters}
                      tiles={tiles} theme={theme} />
      : tab === "map" || tab === "conditions"
        ? <MapScreen at={tiles} palette={palette} theme={theme} on={on}
                     camera={camera.current}
                     marker={focus}
                     data={tab === "conditions" ? conditionData : undefined}
                     gauges={tab === "conditions" ? gauges ?? undefined : undefined}
                     onVisible={tab === "conditions" ? noteVisible : undefined}
                     onMoved={(at) => { camera.current = at; }}
                     view="plain" modes={modes} groups={activeGroups}
                     onLayers={() => setLayersOpen(true)}
                     onPressFeature={onPressFeature}
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
        <LegendStrip palette={palette}>
          {tab === "conditions" ? (
            // READ FROM THE STYLE, never restated. These seven hex values used to be
            // written out here while the map's own ramp resolved to three shades of one
            // blue — so the legend promised red-through-cyan and the map drew a wash of
            // blue. A legend that disagrees with its map is worse than no legend.
            <LegendRamp palette={palette} low="low for the date" high="high"
                        stops={flowRamp(theme)} />
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
                           label={statusWord({ outcome: k, provenance: "specific",
                                               from: [] }).toLowerCase()} />
            ))
          )}
        </LegendStrip>
      )}
      <TabBar active={tab} onChange={(k) => { setItem(null); setTab(k); }} palette={palette} />

      <LayersSheet open={layersOpen} onClose={() => setLayersOpen(false)} palette={palette}
                   state={layers} onState={setLayers}
                   theme={themeName} onTheme={onTheme}
                   reaches={reaches} surveyed={surveyed} stations={stations}
                   fetchedAt={fetchedAt} attribution={attribution} />
    </View>
  );
}

