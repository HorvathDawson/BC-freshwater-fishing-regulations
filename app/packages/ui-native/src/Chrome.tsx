/**
 * The furniture that floats over a map: pills, round buttons, and the legend strip.
 *
 * Separated from the screens because all four tabs use the same shapes, and because the
 * map itself must never be asked to draw UI — everything here sits above it.
 */
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
                   backgroundColor: palette.card, borderRadius: 999,
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

/** The stacked +/− and satellite controls on the map's right edge. */
export function ButtonStack({ palette, items }: {
  palette: Palette;
  items: readonly {
    label: string; glyph?: string; icon?: React.ReactNode;
    /** Pressed state, for a toggle like satellite. */
    on?: boolean;
    onPress: () => void;
  }[];
}) {
  return (
    <View style={{ borderRadius: 12, overflow: "hidden", backgroundColor: palette.card,
                   borderWidth: 1, borderColor: palette.line2, ...palette.lift }}>
      {items.map((it, i) => (
        <Pressable key={it.label} onPress={it.onPress} accessibilityRole="button"
                   accessibilityLabel={it.label}
                   accessibilityState={it.on === undefined ? undefined : { selected: it.on }}
                   // 36px, from the design. A map control is a thing you reach past to see
                   // the map; 44 is the touch minimum for a BUTTON you are aiming at, and
                   // these sit under the thumb by accident far more often than on purpose.
                   style={{ width: 36, height: 36, alignItems: "center",
                            justifyContent: "center",
                            backgroundColor: it.on ? palette.tint : "transparent",
                            borderTopWidth: i === 0 ? 0 : 1, borderTopColor: palette.line }}>
          {it.icon ?? (
            <Text style={{ color: it.on ? palette.accent : palette.sub,
                           fontSize: 17, lineHeight: 20 }}>{it.glyph}</Text>
          )}
        </Pressable>
      ))}
    </View>
  );
}

/**
 * The strip between the map and the tabs.
 *
 * It is a legend, not decoration: whatever the map is currently colouring by has to say
 * what its colours mean, or the map is a picture rather than an answer.
 */
export function LegendStrip({ palette, children }:
  { palette: Palette; children: React.ReactNode }) {
  // ONE LINE. This was a two-row block of counted swatches and it ate a fifth of the
  // screen — on a map-first app the legend is a caption, not a panel. It scrolls sideways
  // rather than wrapping, so adding a category can never take a second row of the map.
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
export function LegendCount({ palette, colour, n, label }:
  { palette: Palette; colour: string; n?: number; label: string }) {
  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: 5 }}>
      <View style={{ width: 9, height: 9, borderRadius: 2.5, backgroundColor: colour }} />
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
  return (
    <View style={{ flex: 1, gap: 4, minWidth: 240 }}>
      <View style={{ height: 9, justifyContent: "center" }}>
        <View style={{ flexDirection: "row", height: 7, borderRadius: 4,
                       overflow: "hidden" }}>
          {stops.map((c, i) => (
            <View key={i} style={{ flex: 1, backgroundColor: c }} />
          ))}
        </View>
        {(marks ?? []).map((m, i) => (
          <View key={i} accessibilityLabel={`gauge at ${Math.round(m * 100)}%`}
                style={{ position: "absolute", left: `${Math.min(99, Math.max(0, m * 100))}%`,
                         width: 2.5, height: 11, marginLeft: -1.25, borderRadius: 1.5,
                         backgroundColor: palette.ink,
                         borderWidth: 1, borderColor: palette.card }} />
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
