/**
 * WHAT THE COLOURS MEAN, and why the map is grey when it is (T62 3.4: out of the Shell, unchanged).
 *
 * THE LEGEND BELONGS TO THE MAP, so it goes when the map does: in water mode (any view, from either map) the browse
 * map is not on screen, and a Showing control or ramp there would control nothing a reader can see. THE MAP TAB HAS A
 * LEGEND ONLY WHEN SOMETHING IS COLOURED BY A VALUE: the day's status (once the index is in), or the lakes' Stocked
 * colouring. Plain water needs no key. Whether it is shown is the Shell's call; what it says is here.
 */
import { CHANGE_LEGEND, changeMarkWords, closedLegend, FLOW_LEGEND, NOT_FISHING_WORDS, TEMP_LEGEND, unprovenLine, WATER_STATUS,
         WATER_STATUS_LINE, WATER_STATUSES, waterLegend, salmonLegend, GEAR_LEGEND, METHOD_WORDS,
         RUN_WORDS, slotsFor }
  from "@app/core";
import { gearEdgeColour, notFishingColours, salmonStatusColour } from "@app/map";
import { useMemo, useState } from "react";
import { Pressable, Text, View } from "react-native";
import { RunDayCaption, RunDaySlider } from "../RunsControls";
import { LegendCount, LegendRamp, LegendStrip } from "../Chrome";
import { legendColour } from "../LayersSheet";
import { LEGEND_TEXT, TYPE } from "../type";
import { changeRampStops, flowRampStops, type Palette } from "../theme";
import type { MapColouring } from "./useMapColouring";

export function MapLegend({ col, conditions, palette, theme, mapDay }: {
  col: MapColouring;
  /** The Conditions tab's legend (a ramp), else the Map tab's (the status key or the water colouring's). */
  conditions: boolean;
  palette: Palette; theme: string;
  /** The day shown: the legend says "Closed on Oct 10" for it. */
  mapDay: Date | null;
}) {
  const { flowParam, horizon, layers, mm, cond } = col;
  const onConditions = mm.onConditions;
  // The readings on screen, as positions on the legend's own scale (sentinels excluded: a state, not a point).
  const visibleMarks = cond.marks;
  return (
    <LegendStrip palette={palette} full={onConditions} wrap={!onConditions}>
      {/* NO QUANTITY SWITCH HERE (owner 2026-10-22): Flow · Level · Temperature is the Layers sheet's Show, one
          choice — a second switch in the strip would be two answers to "where do I change this". The ramp below
          names the quantity shown; the horizon control stays on the map (`horizons`). */}
      {conditions ? (
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
          // the map's own for them (`palette.temp`, from @app/core `TEMP_BAND_TOKEN`, as the dots are painted).
          <View style={{ flexDirection: "row", alignItems: "center", gap: 18,
                         flexWrap: "wrap" }}>
            {TEMP_LEGEND.map((b) => (
              <LegendCount key={b.band} palette={palette} colour={palette.temp[b.band]}
                           label={b.label} />
            ))}
          </View>
        ) : horizon === "change" ? (
          // THE EXPECTED CHANGE (T52): the same map mode's change stops, its words from core — falling · steady ·
          // rising, the bracket over "little change" — and the gauges on screen as marks, like the flow legend
          <>
            <LegendRamp palette={palette} title={CHANGE_LEGEND.title}
                        low={CHANGE_LEGEND.low} mid={CHANGE_LEGEND.mid} high={CHANGE_LEGEND.high}
                        band={CHANGE_LEGEND.steady} markWords={changeMarkWords}
                        describe={CHANGE_LEGEND.describe}
                        stops={changeRampStops(theme)} marks={visibleMarks} />
            {/* (T157) the low-water caveat on the map: what a dot's "low-water model" mark means */}
            <Text testID="change-legend-low-water" style={{ ...TYPE.caption, lineHeight: 15, color: palette.sub }}>
              {CHANGE_LEGEND.lowWater}
            </Text>
          </>
        ) : (
          <LegendRamp palette={palette}
                      title={FLOW_LEGEND.title[flowParam === "level" ? "level" : "discharge"]}
                      low={FLOW_LEGEND.low} mid={FLOW_LEGEND.mid} high={FLOW_LEGEND.high}
                      stops={flowRampStops(theme)} marks={visibleMarks} />
        )
      ) : layers.water.drawn !== "regulations" ? (
        // ONE COLOURING FOR ALL WATER: the measure's key, ending with what draws neutral and why
        waterLegend(layers.water.drawn).map((r) => (
          <LegendCount key={r.label} palette={palette} colour={legendColour(palette, r.swatch)} label={r.label} />
        ))
      ) : col.regsLayer === "salmon" ? (
        // (T211) DFO's salmon: the salmon key in the salmon colours, and whose rules they are. The switch is under the
        // Layers button and the day in the map's corner (T213)
        <>
          {salmonLegend().map((r) => (
            <LegendCount key={r.status} palette={palette} colour={salmonStatusColour(theme, r.status)} line
                         label={r.label} />
          ))}
          {/* (owner 2026-10-28) the provincial gear marks hold for salmon too: the same two rows as Standard */}
          {col.gearMarked && GEAR_LEGEND.map((m) => (
            <LegendCount key={m} palette={palette} colour={gearEdgeColour(theme)} edge={m === "fly" ? "solid" : "dashed"}
                         label={METHOD_WORDS.legend[m]} />
          ))}
        </>
      ) : (
        // The three statuses, in the words and colours every surface uses (@app/core
        // WATER_STATUS through the map's theme) — for the day shown. (T213) Then the gear marks drawn ON them: an ink
        // edge, solid for fly fishing only, dashed for a bait ban — the one compact key for status and marks.
        <>
          {WATER_STATUSES.map((s) => (
            <LegendCount key={s} palette={palette} colour={palette.waterStatus[s]}
                         line dotted={WATER_STATUS_LINE[s] === "dotted"}
                         label={s === "closed" ? closedLegend(mapDay) : WATER_STATUS[s].label} />
          ))}
          {col.gearMarked && GEAR_LEGEND.map((m) => (
            <LegendCount key={m} palette={palette} colour={gearEdgeColour(theme)} edge={m === "fly" ? "solid" : "dashed"}
                         label={METHOD_WORDS.legend[m]} />
          ))}
          {/* (T292) sewage lagoons, tailings ponds, dumps: the off colour and its dashed rim, no status at all */}
          <LegendCount palette={palette} colour={notFishingColours(theme).fill} rim={notFishingColours(theme).edge}
                       label={NOT_FISHING_WORDS.legend} />
        </>
      )}
    </LegendStrip>
  );
}

/**
 * (T261, T276) THE RUNS TAB'S FOOT: the day and species line with a "?" that replays the tutorial, and the date slider.
 * That is all: the colour key and the count scale are the tutorial's, the credit is the Sources page's, and the one
 * line below appears only when something is wrong (or loading) — never the routine offline line. Tap it for the whole text.
 */
export function RunsLegend({ col, palette, theme, onHelp }: { col: MapColouring; palette: Palette; theme: string; onHelp?: () => void }) {
  const r = col.runs;
  const slots = useMemo(() => (r.facts ? slotsFor(r.facts, r.chosen, r.year) : []), [r.facts, r.chosen, r.year]);
  const season = useMemo(() => (r.facts ? r.facts.season(slots) : []), [r.facts, slots]);
  const [open, setOpen] = useState(false);
  const note = r.problem ? (r.problem === "stale" ? RUN_WORDS.stale : RUN_WORDS.failed) : r.loading ? RUN_WORDS.loading : null;
  return (
    <View testID="runs-legend" style={{ borderTopWidth: 1, borderTopColor: palette.line, backgroundColor: palette.card,
                                        paddingHorizontal: 14, paddingTop: 8, paddingBottom: 8, gap: 6 }}>
      <View style={{ width: "100%", maxWidth: 760, alignSelf: "center", gap: 6 }}>
        <RunDayCaption pentad={r.pentad} today={r.today} title={r.chosen.length ? RUN_WORDS.legendTitle(r.chosen.map((s) => s.name)) : null}
                       onToday={() => r.setPentad(r.today)} onHelp={onHelp} palette={palette} />
        <RunDaySlider pentad={r.pentad} today={r.today} playing={r.playing} onPentad={r.setPentad} onPlay={r.setPlaying}
                      season={season} theme={theme} palette={palette} />
        {note && (
          <Pressable testID={r.problem ? "runs-legend-problem" : "runs-legend-loading"} onPress={() => setOpen((o) => !o)}
                     accessibilityRole="button" accessibilityLabel={note}>
            <Text numberOfLines={open ? undefined : 1}
                  style={{ ...TYPE.small, fontSize: LEGEND_TEXT, lineHeight: 20, color: palette.sub }}>{note}</Text>
          </Pressable>
        )}
      </View>
    </View>
  );
}

/**
 * SAY WHY THE MAP IS GREY.
 *
 * Refusing to colour a mixed pair is only half the fix — an uncoloured map and a map
 * whose data is unusable look identical, and that ambiguity IS the bug this guards:
 * when it happened, the Conditions map went quietly grey and read as a slow feed.
 * So the refusal is stated, in the one place a reader is already looking for what
 * the colours mean.
 */
export function PairBanner({ col, palette }: { col: MapColouring; palette: Palette }) {
  const { mixedPair, unproven, vintage } = col;
  return (
    <>
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
      {unproven && (
        <Text style={{ paddingVertical: 6, paddingHorizontal: 12, color: palette.sub, fontSize: 12 }}>
          {unprovenLine(vintage.data)}
        </Text>
      )}
    </>
  );
}
