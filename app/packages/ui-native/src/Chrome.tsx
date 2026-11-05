/**
 * The furniture that floats over a map: the legend strip and its counts and ramp.
 *
 * Separated from the screens because all four tabs use the same shapes, and because the
 * map itself must never be asked to draw UI — everything here sits above it.
 */
import { FLOW_LEGEND, KEY_ISOLATE_WORDS, legendCount, legendTickLabel, percentileOrdinal, standing } from "@app/core";
import { ScrollView, View } from "react-native";
import { Text } from "./Text";
import { Pressable } from "./Pressable";
import { hitProps } from "./regs/hit";
import Svg, { Defs, LinearGradient, Rect, Stop } from "react-native-svg";
import { LEGEND_TEXT, TYPE } from "./type";
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
            style={{ flexGrow: 0, flexDirection: "row", flexWrap: "wrap", alignItems: "center", columnGap: 16,
                     rowGap: 6, borderTopWidth: 1, borderTopColor: palette.line, backgroundColor: palette.card,
                     paddingHorizontal: LEGEND_ROWS.padX, paddingVertical: 9 }}>
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
export function DottedSwatch({ colour, width = 14, thick = 3 }: { colour: string; width?: number; thick?: number }) {
  const n = Math.max(2, Math.round(width / (thick + 2)));
  return (
    <View testID="swatch-dotted" style={{ width, height: thick, flexDirection: "row", justifyContent: "space-between" }}>
      {Array.from({ length: n }, (_, i) => <View key={i} style={{ width: thick, height: thick, backgroundColor: colour }} />)}
    </View>
  );
}

/**
 * (T377) A STRIPED line swatch, the same box as a solid one: the line's colour with short dashes of `stripe` down its
 * middle, closed's stripe at legend size (Views, so it draws the same on web and native).
 */
export function StripedSwatch({ colour, stripe, width = 14, thick = 4 }: { colour: string; stripe: string; width?: number; thick?: number }) {
  return (
    <View testID="swatch-striped" style={{ width, height: thick, backgroundColor: colour, flexDirection: "row",
                                            alignItems: "center", justifyContent: "space-evenly" }}>
      {[0, 1].map((i) => <View key={i} style={{ width: 2, height: Math.max(1, Math.round(thick * 0.4)), backgroundColor: stripe }} />)}
    </View>
  );
}

/**
 * (T396) THE REGULATIONS KEY'S MEASURES, in Trout & more and in Salmon alike: the wrap strip's side padding, and a tight
 * entry's swatch widths, the gap to its label and the gap between entries. Chosen so Trout & more's longer row (No
 * access · Fly only · Bait ban · Not fishable, in Archivo at `LEGEND_TEXT`) fits a 390 dp phone with room to spare —
 * pinned by tools/legend-rows.test.ts against the font's own advance widths.
 */
export const LEGEND_ROWS = { padX: 12, column: 8, gap: 4, line: 14, edge: 16, square: 12 } as const;

/**
 * (T396) THE KEY IN TWO FIXED ROWS: each mode decides which entries share a row, so Trout & more and Salmon both stand two
 * lines tall at a phone's width and the bottom bar does not jump between them. A row too wide for a narrow screen still
 * wraps (nothing is cut).
 */
export function LegendRows({ rows }: { rows: readonly React.ReactNode[] }) {
  return (
    <View testID="legend-rows" style={{ width: "100%", rowGap: 6 }}>
      {rows.map((r, i) => (
        <View key={i} testID="legend-row" style={{ flexDirection: "row", flexWrap: "wrap", alignItems: "center",
                                                   columnGap: LEGEND_ROWS.column, rowGap: 6 }}>
          {r}
        </View>
      ))}
    </View>
  );
}

/** One "29 closed" entry. The count is the point — it says how much map is that colour. */
export function LegendCount({ palette, colour, n, label, line, dotted, striped, edge, rim, tight, onPress, alone, faded }:
  { palette: Palette; colour: string; n?: number; label: string;
    /**
     * Draw the swatch as a LINE instead of a square — for a key to lines. Every line swatch is
     * the same width: no status is drawn wider than another, so the key must not suggest one.
     */
    line?: boolean;
    /** (T165) the line DOTTED, as the map draws no public access (`WATER_STATUS_LINE`) */
    dotted?: boolean;
    /** (T377) the line STRIPED down its middle with dashes in this colour, as the map draws closed (`closedCuePaint`) */
    striped?: string;
    /**
     * (T213) a GEAR MARK's key: the edge the map draws round a line of any status colour, in `colour` (the gear ink) —
     * a hollow line, solid for fly fishing only, dashed for a bait ban. No fill: the mark rides on every status.
     */
    edge?: "solid" | "dashed";
    /** (T292) a DASHED RIM round the square in this colour, as the map draws not-fishing water's edge */
    rim?: string;
    /** (T396) the regulations key's narrower swatches and closer label (`LEGEND_ROWS`), so its rows fit a phone */
    tight?: boolean;
    /** (T377) a TAPPABLE class: a tap shows it alone on the map (`isolate`), again shows every class */
    onPress?: () => void;
    /** (T377) this class is the one shown alone (its label in ink, the same width); `faded`: another one is (the entry dimmed, as its water) */
    alone?: boolean; faded?: boolean }) {
  const lineW = tight ? LEGEND_ROWS.line : 20;
  const row = { flexDirection: "row" as const, alignItems: "center" as const, gap: tight ? LEGEND_ROWS.gap : 7,
                opacity: faded ? 0.4 : 1 };
  const body = (<>
      {edge ? <View testID={`legend-edge-${edge}`}
                    style={{ width: tight ? LEGEND_ROWS.edge : 22, height: 10, borderWidth: 1.5, borderColor: colour,
                             borderStyle: edge }} />
        : line && dotted ? <DottedSwatch colour={colour} width={lineW} thick={4} />
        : line && striped ? <StripedSwatch colour={colour} stripe={striped} width={lineW} /> : <View style={line
        ? { width: lineW, height: 4, backgroundColor: colour }
        : { width: 12, height: 12, borderRadius: palette.r.chip, backgroundColor: colour,
            ...(rim ? { borderWidth: 1, borderColor: rim, borderStyle: "dashed" as const } : {}) }}
        {...(rim ? { testID: "legend-rim" } : {})} />}
      {n !== undefined && (
        <Text style={{ ...TYPE.figure, fontSize: LEGEND_TEXT, color: palette.ink }}>{n}</Text>
      )}
      <Text style={{ ...TYPE.small, fontSize: LEGEND_TEXT, lineHeight: 20, color: alone ? palette.ink : palette.sub }}>{label}</Text>
  </>);
  if (!onPress) return <View testID="legend-entry" style={row}>{body}</View>;
  return (
    <Pressable testID="legend-entry" onPress={onPress} accessibilityRole="button" aria-pressed={!!alone}
               accessibilityLabel={KEY_ISOLATE_WORDS.entry(label, !!alone)} {...hitProps({ w: 44, h: 20 })} style={row}>
      {body}
    </Pressable>
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
  const word = { ...TYPE.caption, lineHeight: 15, color: palette.sub } as const;

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
          // (T243) a labelled picture, said as one: a label on a role-less view is ignored by screen readers (axe)
          <View key={i} accessibilityRole="image"
                accessibilityLabel={legendTickLabel(c, markWords ? markWords((i + 0.5) / BINS)
                                                    : FLOW_LEGEND.near(percentileOrdinal((i + 0.5) / BINS)))}
                style={{ position: "absolute", bottom: BAR, left: pct(i / BINS), width: pct(1 / BINS),
                         paddingHorizontal: 0.5 }}>
            <View style={{ height: Math.max(3, (c / peak) * TICKS), backgroundColor: palette.ink, opacity: 0.8 }} />
          </View>
        ))}
        <View testID="legend-bar" accessibilityRole="image" accessibilityLabel={describe ?? FLOW_LEGEND.scale(title, low, high, mid)}
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
