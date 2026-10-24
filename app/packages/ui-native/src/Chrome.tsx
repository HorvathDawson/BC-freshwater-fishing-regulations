/**
 * The furniture that floats over a map: the legend strip and its counts and ramp.
 *
 * Separated from the screens because all four tabs use the same shapes, and because the
 * map itself must never be asked to draw UI — everything here sits above it.
 */
import { legendCount, legendTickLabel, percentileOrdinal, standing } from "@app/core";
import { ScrollView, Text, View } from "react-native";
import Svg, { Defs, LinearGradient, Rect, Stop } from "react-native-svg";
import { TYPE } from "./type";
import type { Palette } from "./theme";

/**
 * The strip between the map and the tabs.
 *
 * It is a legend, not decoration: whatever the map is currently colouring by has to say
 * what its colours mean, or the map is a picture rather than an answer.
 */
export function LegendStrip({ palette, children, full, wrap }:
  { palette: Palette; children: React.ReactNode; full?: boolean;
    /**
     * WRAP instead of scrolling sideways (layers menu B, the Map tab): the key measured 613 px in a 390 px phone and
     * hid "Has its own regulations" and "Base regulations only" off the edge, where nobody scrolls a caption. Wrapped,
     * every entry is on screen at any width; a second row is the price.
     */
    wrap?: boolean }) {
  if (wrap) {
    return (
      <View testID="legend-strip"
            style={{ flexGrow: 0, flexDirection: "row", flexWrap: "wrap", alignItems: "center", columnGap: 14,
                     rowGap: 5, borderTopWidth: 1, borderTopColor: palette.line, backgroundColor: palette.card,
                     paddingHorizontal: 14, paddingVertical: 7 }}>
        {children}
      </View>
    );
  }
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

/**
 * (T165) A DOTTED line swatch, the same 14 × 3 box as a solid one: three dashes with gaps, the map's `stream-noaccess`
 * pattern at legend size (Views, so it draws the same on web and native — `borderStyle: "dotted"` does not).
 */
export function DottedSwatch({ colour, width = 14 }: { colour: string; width?: number }) {
  const n = Math.max(2, Math.round(width / 5));
  return (
    <View testID="swatch-dotted" style={{ width, height: 3, flexDirection: "row", justifyContent: "space-between" }}>
      {Array.from({ length: n }, (_, i) => <View key={i} style={{ width: 3, height: 3, backgroundColor: colour }} />)}
    </View>
  );
}

/** One "29 closed" entry. The count is the point — it says how much map is that colour. */
export function LegendCount({ palette, colour, n, label, line, dotted }:
  { palette: Palette; colour: string; n?: number; label: string;
    /**
     * Draw the swatch as a LINE instead of a square — for a key to lines. Every line swatch is
     * the same width: no status is drawn wider than another, so the key must not suggest one.
     */
    line?: boolean;
    /** (T165) the line DOTTED, as the map draws no public access (`WATER_STATUS_LINE`) */
    dotted?: boolean }) {
  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: 5 }}>
      {line && dotted ? <DottedSwatch colour={colour} /> : <View style={line
        ? { width: 14, height: 3, backgroundColor: colour }
        : { width: 9, height: 9, borderRadius: palette.r.chip, backgroundColor: colour }} />}
      {n !== undefined && (
        <Text style={{ ...TYPE.figure, fontSize: 11.5, color: palette.ink }}>{n}</Text>
      )}
      <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.sub }}>{label}</Text>
    </View>
  );
}


/**
 * WHERE "NORMAL" IS ON THE SCALE: the percentiles core's `standing` calls normal (the middle half of the record, 25–75),
 * read from that function rather than restated, so the legend's bracket and every "NORMAL" word agree.
 */
const NORMAL: readonly [number, number] = (() => {
  const at = Array.from({ length: 101 }, (_, i) => i / 100).filter((p) => standing(p) === "normal");
  return [at[0] ?? 0.25, Math.min(1, (at.at(-1) ?? 0.74) + 0.01)];
})();
/** the legend's widest: past this a ramp across a desktop window is a stripe, not a scale (centred in the strip) */
export const LEGEND_RAMP_MAX = 560;
const BAR = 10, TICKS = 14;

/**
 * The Conditions legend: a continuous ramp, because flow against the record is continuous.
 * Discrete swatches would imply buckets the data does not have.
 *
 * (refinement pass, owner 2026-10-22: "the histogram bar at bottom of condition map could be made a little better") —
 * what changed, and why:
 *   - THE RAMP IS THE MAP'S, STOP FOR STOP: a gradient with each colour at the percentile it names (`stops`, from the
 *     style's metadata via `flowRampStops`). Seven equal blocks put a 20th-percentile gauge's mark on f2 while the map
 *     painted it nearly f3, and squeezed "normal" into one seventh of the bar.
 *   - IT SAYS WHAT THE SCALE IS: a title ("Flow vs. past years, this date") and a bracket under the middle half marked
 *     Normal, with Low and High at the ends — the percentile's meaning without a number. It was three faint 10.5 px words.
 *   - IT SAYS HOW MANY: "12 reaches coloured" beside the title, the count the histogram is of (T176: it read "12 gauges
 *     in view", but it counts the reaches on screen with a standing — core `legendCount` says what it counts).
 *   - Words in `sub`, not `faint` (T40: a legend's words are read, so they meet the text contrast floor), at 11.5–12 px.
 *   - At most `LEGEND_RAMP_MAX` wide, centred: on a desktop the 1250 px stripe read as decoration.
 */
export function LegendRamp({ palette, stops, title, low, high, marks, mid, band = NORMAL, markWords, describe }: {
  palette: Palette;
  /** the map's ramp: each colour at its percentile, 0–1, in order (`flowRampStops`) */
  stops: readonly { at: number; colour: string }[];
  /** what is being ranked, e.g. "Flow vs. past years, this date" */
  title: string;
  low: string; high: string;
  /**
   * Where the reaches currently on screen with a standing sit on this scale, 0–1 (one mark per reach, T176).
   *
   * This is what turns a legend from a key into a READING. Without them the ramp says
   * "there is a scale"; with them it says "and the rivers you are looking at are down
   * here" — which is the whole question a person opened this view to ask.
   */
  marks?: readonly number[];
  /** the word under the bracket over the middle half, e.g. "Normal" */
  mid: string;
  /** the bracket's span, 0–1 (default: the middle half of past years, NORMAL); the Change legend's is "steady" */
  band?: readonly [number, number];
  /** a mark's position for a screen reader (default: "near the 20th percentile") */
  markWords?: (at: number) => string;
  /** the bar's screen-reader name (default: the percentile scale's) */
  describe?: string;
}) {
  /*
   * A DISTRIBUTION, NOT A KEY. The marks are binned so that where many gauges land the tick is TALLER rather than merely
   * repainted over itself: a narrow spike at the bottom is a province uniformly in drought, a wide spread one doing
   * several things at once. THE TICKS STAND ON THE BAR RATHER THAN CROSSING IT: crossing, they hid the very colour they
   * were pointing at.
   */
  const BINS = 40;
  const counts = new Array<number>(BINS).fill(0);
  for (const m of marks ?? []) {
    const i = Math.min(BINS - 1, Math.max(0, Math.floor(m * BINS)));
    counts[i] = (counts[i] ?? 0) + 1;
  }
  const peak = Math.max(1, ...counts);
  const n = marks?.length ?? 0;
  const count = legendCount(n);
  const pct = (x: number) => `${Math.round(x * 1000) / 10}%` as const;
  const word = { ...TYPE.small, fontSize: 11.5, lineHeight: 15, color: palette.sub } as const;

  return (
    <View testID="legend-ramp" style={{ width: "100%", maxWidth: LEGEND_RAMP_MAX, alignSelf: "center" }}>
      <View style={{ flexDirection: "row", justifyContent: "space-between", alignItems: "baseline", gap: 12,
                     marginBottom: 2 }}>
        <Text numberOfLines={1} style={{ ...TYPE.bodyStrong, fontSize: 12.5, lineHeight: 17, color: palette.ink,
                                         flexShrink: 1 }}>{title}</Text>
        {count !== null && (
          <Text testID="legend-count" style={{ ...word, fontSize: 12, lineHeight: 17 }}>{count}</Text>
        )}
      </View>
      <View style={{ height: TICKS + BAR }}>
        {counts.map((c, i) => c === 0 ? null : (
          <View key={i}
                accessibilityLabel={legendTickLabel(c, markWords ? markWords((i + 0.5) / BINS)
                                                    : `near the ${percentileOrdinal((i + 0.5) / BINS)} percentile`)}
                style={{ position: "absolute", bottom: BAR, left: pct(i / BINS), width: pct(1 / BINS),
                         paddingHorizontal: 0.5 }}>
            <View style={{ height: Math.max(3, (c / peak) * TICKS), backgroundColor: palette.ink, opacity: 0.8 }} />
          </View>
        ))}
        <View testID="legend-bar" accessibilityLabel={describe ?? `${title}: colours from ${low.toLowerCase()} to `
                                                      + `${high.toLowerCase()}; ${mid.toLowerCase()} is the middle half of past years`}
              style={{ position: "absolute", left: 0, right: 0, bottom: 0, height: BAR }}>
          <Svg width="100%" height={BAR} viewBox={`0 0 100 ${BAR}`} preserveAspectRatio="none">
            <Defs>
              <LinearGradient id="legend-ramp" x1="0" y1="0" x2="1" y2="0">
                {stops.map((s) => <Stop key={s.at} offset={s.at} stopColor={s.colour} />)}
              </LinearGradient>
            </Defs>
            <Rect x={0} y={0} width={100} height={BAR} fill="url(#legend-ramp)" />
          </Svg>
        </View>
      </View>
      {/* THE SCALE UNDER IT: Low · a bracket over the middle half, "Normal" · High */}
      <View style={{ height: 24 }}>
        <View testID="legend-normal"
              style={{ position: "absolute", top: 2, height: 5, left: pct(band[0]), width: pct(band[1] - band[0]),
                       borderLeftWidth: 1, borderRightWidth: 1, borderBottomWidth: 1, borderColor: palette.sub }} />
        <Text style={{ ...word, position: "absolute", top: 8, left: 0 }}>{low}</Text>
        <Text style={{ ...word, position: "absolute", top: 8, left: pct(band[0]), width: pct(band[1] - band[0]),
                       textAlign: "center" }}>{mid}</Text>
        <Text style={{ ...word, position: "absolute", top: 8, right: 0 }}>{high}</Text>
      </View>
    </View>
  );
}
