/**
 * The furniture that floats over a map: pills, round buttons, and the legend strip.
 *
 * Separated from the screens because all four tabs use the same shapes, and because the
 * map itself must never be asked to draw UI — everything here sits above it.
 */
import { percentileOrdinal } from "@app/core";
import { Pressable, ScrollView, Text, View, type ViewStyle } from "react-native";
import { TYPE } from "./type";
import type { Palette } from "./theme";

/** A floating capsule. Used for the date, the layers button, and map hints. */
export function Pill({ palette, children, onPress, style, label }: {
  palette: Palette; children: React.ReactNode; onPress?: () => void;
  style?: ViewStyle; label?: string;
}) {
  const body = (
    <View style={{ flexDirection: "row", alignItems: "center", gap: 8,
                   backgroundColor: palette.card, borderRadius: palette.r.pill,
                   // Riffle's chip: 8/14 at 13.5px. The map is the content; its furniture
                   // should be the smallest thing that is still a comfortable target.
                   paddingVertical: 8, paddingHorizontal: 14,
                   borderWidth: 1, borderColor: palette.line2, ...palette.lift, ...style }}>
      {children}
    </View>
  );
  return onPress
    ? <Pressable onPress={onPress} accessibilityRole="button" accessibilityLabel={label}>
        {body}
      </Pressable>
    : body;
}

/**
 * The strip between the map and the tabs.
 *
 * It is a legend, not decoration: whatever the map is currently colouring by has to say
 * what its colours mean, or the map is a picture rather than an answer.
 */
export function LegendStrip({ palette, children, full }:
  { palette: Palette; children: React.ReactNode; full?: boolean }) {
  /*
   * ONE LINE, and it scrolls sideways rather than wrapping, so adding a category can never
   * take a second row of the map. This was a two-row block of counted swatches that ate a
   * fifth of the screen — on a map-first app the legend is a caption, not a panel.
   *
   * `full` turns the scroller off. A ramp that is meant to span the screen cannot live in
   * a horizontal ScrollView: the content sizes to itself, so `width: "100%"` resolves
   * against the content rather than the screen and the bar collapses to its minimum.
   */
  if (full) {
    return (
      <View style={{ flexGrow: 0, borderTopWidth: 1, borderTopColor: palette.line,
                     backgroundColor: palette.card,
                     paddingHorizontal: 14, paddingVertical: 8 }}>
        {children}
      </View>
    );
  }
  return (
    <ScrollView horizontal showsHorizontalScrollIndicator={false}
                style={{ flexGrow: 0, borderTopWidth: 1, borderTopColor: palette.line,
                         backgroundColor: palette.card }}
                contentContainerStyle={{ alignItems: "center", gap: 14,
                                         paddingHorizontal: 14, paddingVertical: 7 }}>
      {children}
    </ScrollView>
  );
}

/** One "29 closed" entry. The count is the point — it says how much map is that colour. */
export function LegendCount({ palette, colour, n, label, weight }:
  { palette: Palette; colour: string; n?: number; label: string;
    /**
     * Draw the swatch as a LINE of this relative weight instead of a square — for a key to
     * lines whose width carries meaning too (a closed water is drawn wider than the rest).
     */
    weight?: number }) {
  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: 5 }}>
      <View style={weight === undefined
        ? { width: 9, height: 9, borderRadius: palette.r.chip, backgroundColor: colour }
        : { width: 14, height: Math.round(3 * weight), backgroundColor: colour }} />
      {n !== undefined && (
        <Text style={{ ...TYPE.figure, fontSize: 11.5, color: palette.ink }}>{n}</Text>
      )}
      <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.sub }}>{label}</Text>
    </View>
  );
}

/**
 * The Conditions legend: a continuous ramp, because flow against the record is continuous.
 * Discrete swatches would imply buckets the data does not have.
 */
export function LegendRamp({ palette, stops, low, high, marks, mid }: {
  palette: Palette; stops: readonly string[]; low: string; high: string;
  /**
   * Where the gauges currently on screen sit on this scale, 0–1.
   *
   * This is what turns a legend from a key into a READING. Without them the ramp says
   * "there is a scale"; with them it says "and the rivers you are looking at are down
   * here" — which is the whole question a person opened this view to ask.
   */
  marks?: readonly number[];
  /** Optional centre label, e.g. "normal". */
  mid?: string;
}) {
  /*
   * A DISTRIBUTION, NOT A KEY — which is what the marks were always for and what the old
   * size would not let them be.
   *
   * At 7 px tall and 2.5 px wide, forty gauges on the same stretch of the scale drew as
   * one indistinguishable smear: you could see THAT the rivers were low and not HOW low,
   * or how tightly they agreed. The bar is now tall enough for the marks to have height,
   * and the marks are binned so that where many gauges land the tick is TALLER rather than
   * merely repainted over itself. That is a histogram, and reading it is the point: a
   * narrow spike at the bottom is a province uniformly in drought, and a wide spread is
   * one doing several things at once.
   *
   * Full width, because it is the answer to the question the view was opened for and it
   * was sharing a line with a control.
   */
  const BINS = 40;
  const counts = new Array<number>(BINS).fill(0);
  for (const m of marks ?? []) {
    const i = Math.min(BINS - 1, Math.max(0, Math.floor(m * BINS)));
    counts[i] = (counts[i] ?? 0) + 1;
  }
  const peak = Math.max(1, ...counts);
  const H = 26;

  return (
    <View style={{ width: "100%", gap: 5 }}>
      <View style={{ height: H, justifyContent: "flex-end" }}>
        <View style={{ flexDirection: "row", height: 14, borderRadius: palette.r.chip,
                       overflow: "hidden" }}>
          {stops.map((c, i) => (
            <View key={i} style={{ flex: 1, backgroundColor: c }} />
          ))}
        </View>
        {/*
          THE TICKS STAND ON THE BAR RATHER THAN CROSSING IT. Crossing, they hid the very
          colour they were pointing at; standing, the height is free to mean something.
        */}
        {counts.map((n, i) => n === 0 ? null : (
          <View key={i}
                accessibilityLabel={`${n} ${n === 1 ? "gauge" : "gauges"} near the `
                                    + `${percentileOrdinal((i + 0.5) / BINS)} percentile`}
                style={{ position: "absolute", bottom: 14,
                         left: `${(i / BINS) * 100}%`, width: `${(1 / BINS) * 100}%`,
                         paddingHorizontal: 0.5 }}>
            <View style={{ height: Math.max(3, (n / peak) * (H - 14)),
                           backgroundColor: palette.ink, opacity: 0.75,
                           borderTopLeftRadius: 1, borderTopRightRadius: 1 }} />
          </View>
        ))}
      </View>
      <View style={{ flexDirection: "row", justifyContent: "space-between" }}>
        <Text style={{ ...TYPE.small, fontSize: 10.5, color: palette.faint }}>{low}</Text>
        {mid && (
          <Text style={{ ...TYPE.small, fontSize: 10.5, color: palette.faint }}>{mid}</Text>
        )}
        <Text style={{ ...TYPE.small, fontSize: 10.5, color: palette.faint }}>{high}</Text>
      </View>
    </View>
  );
}
