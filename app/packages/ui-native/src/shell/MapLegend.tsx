/**
 * WHAT THE COLOURS MEAN, and why the map is grey when it is (T62 3.4: out of the Shell, unchanged).
 *
 * THE LEGEND BELONGS TO THE MAP, so it goes when the map does: in water mode (any view, from either map) the browse
 * map is not on screen, and a Showing control or ramp there would control nothing a reader can see. THE MAP TAB HAS A
 * LEGEND ONLY WHEN SOMETHING IS COLOURED BY A VALUE: the day's status (once the index is in), or the lakes' Stocked
 * colouring. Plain water needs no key. Whether it is shown is the Shell's call; what it says is here.
 */
import { CHANGE_LEGEND, changeMarkWords, closedLegend, FLOW_LEGEND, TEMP_LEGEND, unprovenLine, WATER_STATUS,
         WATER_STATUS_LINE, WATER_STATUSES, waterLegend, SALMON_WORDS, salmonLegend, salmonLegendSource, METHOD_LEGEND, METHOD_WORDS }
  from "@app/core";
import { methodClassColour, salmonStatusColour } from "@app/map";
import { Pressable, Text, View } from "react-native";
import { LegendCount, LegendRamp, LegendStrip } from "../Chrome";
import { MapDayButton } from "../DayCalendar";
import { legendColour } from "../LayersSheet";
import { TYPE } from "../type";
import { changeRampStops, flowRampStops, type Palette } from "../theme";
import type { MapColouring } from "./useMapColouring";

export function MapLegend({ col, conditions, palette, theme, mapDay, onDay }: {
  col: MapColouring;
  /** The Conditions tab's legend (a ramp), else the Map tab's (the status key or the water colouring's). */
  conditions: boolean;
  palette: Palette; theme: string;
  mapDay: Date | null;
  /** Open the map's date sheet (the Map tab's day button). */
  onDay: () => void;
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
            <Text testID="change-legend-low-water" style={{ ...TYPE.small, fontSize: 11.5, lineHeight: 15, color: palette.sub }}>
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
        // (T211) DFO's salmon: the toggle, the day, the salmon key in the salmon colours, and whose rules they are
        <>
          <RegsLayerToggle col={col} palette={palette} />
          <MapDayButton palette={palette} day={mapDay} onPress={onDay} />
          {salmonLegend().map((r) => (
            <LegendCount key={r.status} palette={palette} colour={salmonStatusColour(theme, r.status)} line
                         label={r.label} />
          ))}
          <Text testID="salmon-legend-source" style={{ ...TYPE.small, fontSize: 11.5, lineHeight: 15, color: palette.sub }}>
            {salmonLegendSource(col.salmonFacts)}
          </Text>
        </>
      ) : col.regsLayer === "method" ? (
        // (T213) BAIT & FLY: the toggle, the day, and the key — fly only and bait ban read apart from every other open
        // water; closed is the Standard map's red (the status index's closures)
        <>
          <RegsLayerToggle col={col} palette={palette} />
          <MapDayButton palette={palette} day={mapDay} onPress={onDay} />
          {METHOD_LEGEND.map((c) => (
            <LegendCount key={c} palette={palette} colour={methodClassColour(theme, c)} line
                         label={c === "closed" ? closedLegend(mapDay) : METHOD_WORDS.legend[c]} />
          ))}
        </>
      ) : (
        // The three statuses, in the words and colours every surface uses (@app/core
        // WATER_STATUS through the map's theme) — for today.
        <>
          {(col.salmonOffered || col.methodOffered) && <RegsLayerToggle col={col} palette={palette} />}
          <MapDayButton palette={palette} day={mapDay} onPress={onDay} />
          {WATER_STATUSES.map((s) => (
            <LegendCount key={s} palette={palette} colour={palette.waterStatus[s]}
                         line dotted={WATER_STATUS_LINE[s] === "dotted"}
                         label={s === "closed" ? closedLegend(mapDay) : WATER_STATUS[s].label} />
          ))}
        </>
      )}
    </LegendStrip>
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
/**
 * (T211) STANDARD / SALMON — the Map tab's regulations toggle (owner 2026-10-27), first in the legend it switches. A press
 * moves only the map's modes: the salmon chains are already on every tile (`useSalmonColour`), so it is instant.
 */
export function RegsLayerToggle({ col, palette }: { col: MapColouring; palette: Palette }) {
  const W = SALMON_WORDS.toggle;
  return (
    <View testID="regs-paint-toggle" accessibilityRole="radiogroup" accessibilityLabel={W.label}
          style={{ flexDirection: "row", borderWidth: 1.5, borderColor: palette.ink, borderRadius: palette.r.chip }}>
      {(["standard", "salmon", "method"] as const)
        .filter((k) => k === "standard" || (k === "salmon" ? col.salmonOffered : col.methodOffered)).map((k) => {
        const on = col.regsLayer === k;
        const word = k === "method" ? METHOD_WORDS.mode : W[k];
        return (
          <Pressable key={k} testID={`regs-paint-${k}`} onPress={() => col.setRegsLayer(k)}
                     accessibilityRole="radio" aria-checked={on} accessibilityLabel={word}
                     style={{ minHeight: 44, minWidth: 44, justifyContent: "center", paddingHorizontal: 10,
                              backgroundColor: on ? palette.ink : "transparent" }}>
            <Text style={{ ...TYPE.small, fontSize: 11.5, fontWeight: "700", color: on ? palette.card : palette.ink }}>
              {word}
            </Text>
          </Pressable>
        );
      })}
    </View>
  );
}

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
