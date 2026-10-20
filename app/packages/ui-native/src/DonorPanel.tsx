/**
 * WHO SAID SO — the gauges behind an estimate, where they are, and what each one counted.
 *
 * A single percentile with nothing behind it asks a reader to trust an arithmetic they
 * cannot see, on water that may have no gauge within fifty kilometres. This is the working:
 * which stations spoke, where they stand, how the water reaches them, how far off they are
 * in catchment size, and what each contributed. It is also the honest place to show that
 * the answer often rests on ONE gauge — true for most of the province, and invisible in a
 * number.
 *
 * THE ANSWER IS A RANGE, NOT A POINT, and that is measured rather than stylistic: over
 * 9,495 nested gauge pairs even a donor of nearly identical size is out by 11.7 percentile
 * points at the median. Nothing here is precise enough to be a single number.
 *
 * THE MAP IS PART OF THE ARGUMENT, not decoration beside it. The panel's whole claim is
 * that these particular gauges are entitled to speak for this particular water, and that
 * claim is about geography.
 *
 * IT DRAWS A VIEW MODEL AND DECIDES NOTHING (AGENTS 25, P12a). Every sentence is composed
 * in core (`conditions.ts`, pinned whole) and every choice — which notes show, which pin
 * absorbs "you are here", the day the range is for — is made in `donorView` in @app/ui.
 */
import type { SectionKey } from "@app/core";
import { useState } from "react";
import { LayoutChangeEvent, Pressable, Text, View } from "react-native";
import type { TileEndpoints } from "@app/map";
import type { StationId } from "@app/data";
import { useDonorView, type Horizon, type PanelAnswer } from "@app/ui";
import { MiniMap } from "./MiniMap";
import { TYPE } from "./type";
import type { Palette } from "./theme";

/** The route map's height. Read by the camera as well as by the map, so they agree. */
const MAP_H = 210;

export function DonorPanel({ palette, value, day, at, theme, from, selected, onSelect,
                             horizon = 0, onHorizon, chain: chainProp, data }: {
  palette: Palette; value: PanelAnswer;
  /**
   * The day the answer is FOR — today on a live sheet, the visit on a saved spot. The
   * range's "for early September" is said of this day plus the horizon, never of whenever
   * the panel happens to be drawn.
   */
  day: Date;
  /** The tiles, when the caller has them — then the donors are DRAWN as well as listed. */
  at?: TileEndpoints; theme?: string;
  /** Where the person actually is, so the map can mark it. */
  from?: { lat: number; lon: number } | null;
  /**
   * Which donor the chart above is currently showing, and how to change it. Both optional:
   * a saved spot has no chart to steer, and a row that cannot do anything must not look as
   * though it can.
   */
  selected?: StationId | null; onSelect?: (station: StationId) => void;
  /**
   * The reaches between here and the gauges, computed by the caller (it needs the bundle
   * and the feed); derived from the routes otherwise.
   */
  chain?: readonly SectionKey[];
  data?: Record<string, Record<string, Record<string, unknown>>>;
  /** How far ahead this estimate is for — 0 is now. Only the WORDS change for a forecast. */
  horizon?: Horizon;
  /** Change the horizon from here, when the caller owns it. Absent, no chips. */
  onHorizon?: (d: Horizon) => void;
}) {
  /*
   * MEASURED, NOT ASSUMED. The panel is rendered at every width from a phone to a desktop
   * column, and the camera has to know which axis is the tight one.
   */
  const [mapWidth, setMapWidth] = useState(360);
  const onLayout = (e: LayoutChangeEvent) => {
    const w = Math.round(e.nativeEvent.layout.width);
    if (w > 0 && w !== mapWidth) setMapWidth(w);
  };
  const vm = useDonorView(value, { day, horizon, from: from ?? null,
                                   size: { width: mapWidth, height: MAP_H },
                                   selected: selected ?? null, pickable: !!onSelect });

  if (!vm.ok) {
    return (
      <View style={{ paddingVertical: 14 }}>
        <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.6,
                       color: palette.faint, marginBottom: 6 }}>{vm.heading}</Text>
        <Text style={{ ...TYPE.body, fontSize: 13.5, color: palette.sub, lineHeight: 19 }}>
          {vm.reason}
        </Text>
      </View>
    );
  }
  /**
   * A donor's colour — GREY WHEN IT IS QUIET. A station that is not reporting contributes
   * nothing, and a live tone would say otherwise; grey is the map's own "nothing to say".
   */
  const toneOf = (i: number | null) => {
    if (i === null) return palette.accent;
    return vm.rows[i]?.quiet ? palette.quiet : palette.donor[i % palette.donor.length]!;
  };
  const map = vm.map;

  return (
    // Measured on the ROOT, which renders on the first pass — so the width is known before
    // the map mounts and the map never has to remount to be framed correctly.
    <View style={{ paddingVertical: 14 }} onLayout={onLayout}>
      {/* ONE LINE, ALWAYS: the horizon chips sit right below, and a heading that wraps
          moves the chip the reader just tapped out from under their finger. */}
      <Text numberOfLines={1} style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.6,
                                       color: palette.faint }}>
        {vm.heading}
      </Text>
      {/* THE PLAIN SENTENCE FIRST, the percentile second — the same numbers, in the order
          a person reads them. Two lines of room whatever it says, so nothing below shifts
          as the reader moves between horizons. */}
      <Text style={{ ...TYPE.name, fontSize: 22, color: palette.ink, marginTop: 3,
                     lineHeight: 27, minHeight: 54 }}>
        {vm.headline}
        {vm.headlineSuffix ? (
          <Text style={{ color: palette.sub }}>{" "}{vm.headlineSuffix}</Text>
        ) : null}
      </Text>
      <Text style={{ ...TYPE.body, fontSize: 13.5, color: palette.sub, marginTop: 2,
                     lineHeight: 19 }}>
        {vm.sentence}
      </Text>
      <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.faint, marginTop: 4 }}>
        {vm.range}
      </Text>

      {/* THE SAME DAYS, IN THE SAME ORDER, as the map's own corner control. */}
      {onHorizon && (
        <View style={{ flexDirection: "row", gap: 6, marginTop: 10 }}>
          {vm.horizons.map((h) => (
            <Pressable key={h.day} onPress={() => onHorizon(h.day)} accessibilityRole="button"
                       aria-selected={h.on} accessibilityLabel={h.label}
                       style={{ paddingVertical: 6, paddingHorizontal: 12,
                                borderRadius: palette.r.pill, borderWidth: 1,
                                borderColor: h.on ? palette.accent : palette.line,
                                backgroundColor: h.on ? palette.accent : "transparent" }}>
              <Text style={{ ...TYPE.micro, fontSize: 12.5, fontWeight: "600",
                             color: h.on ? palette.onAccent : palette.sub }}>
                {h.chip}
              </Text>
            </Pressable>
          ))}
        </View>
      )}

      {/* THE MAP WAITS FOR THE ROUTES (`map` is null until they are in): a MapLibre map
          reads its opening camera once, on mount, and a map mounted early stays framed on
          the tap alone. Keyed on the camera for the same reason. */}
      {at && theme && map && (
        <View style={{ marginTop: 12, gap: 8 }}>
          <MiniMap key={map.key}
                   at={at} palette={palette} theme={theme} camera={map.camera} height={MAP_H}
                   bare view="conditions" highlight={chainProp ?? map.chain} data={data}
                   /* WATER ONLY: at 210 px every other layer competes for the same pixels. */
                   groups={{ water: true, admin: false, protected: false, access: false,
                             ownership: false, depth: false }}
                   pins={map.pins.map((p) => ({ lat: p.lat, lon: p.lon, tone: toneOf(p.row),
                                                title: p.title }))}
                   />
          <View style={{ flexDirection: "row", flexWrap: "wrap", gap: 14 }}>
            {map.keys.map((k) => (
              <Key key={k.label} palette={palette} tone={toneOf(k.row)} label={k.label} />
            ))}
          </View>
          {/* A DONOR WITH NO ROUTE IS SAID SO, not quietly dropped from the key. */}
          {map.unplaced && (
            <Text style={{ ...TYPE.small, fontSize: 11, color: palette.faint }}>
              {map.unplaced}
            </Text>
          )}
        </View>
      )}

      {onSelect && (
        <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.6,
                       color: palette.faint, marginTop: 16 }}>
          {vm.tapToChart}
        </Text>
      )}
      <View accessibilityRole="list" accessibilityLabel={vm.listLabel}
            style={{ marginTop: onSelect ? 6 : 14, borderTopWidth: 1,
                     borderTopColor: palette.line }}>
        {vm.rows.map((r, i) => {
          const pick = onSelect ? () => onSelect(r.station as StationId) : undefined;
          return (
          <Pressable key={r.station} onPress={pick} disabled={!pick}
                accessibilityRole={pick ? "button" : "text"}
                // `aria-selected`, not `accessibilityState`: react-native-web >= 0.21
                // ignores the latter (AGENTS 34).
                aria-selected={pick ? r.on : undefined}
                accessibilityLabel={r.label}
                style={{ paddingVertical: 10, borderBottomWidth: 1,
                         borderBottomColor: palette.line, gap: 5,
                         // THE SELECTED ROW IS MARKED ON THE LEFT, not by a tint: the row
                         // already carries a colour that means something (its pin).
                         paddingLeft: 8, marginLeft: -8,
                         borderLeftWidth: 3,
                         borderLeftColor: r.on ? toneOf(i) : "transparent" }}>
            <View style={{ flexDirection: "row", alignItems: "center", gap: 9 }}>
              {/* The same tone as its pin, so the map and the table are one thing. */}
              <View style={{ width: 9, height: 9, borderRadius: 5,
                             backgroundColor: toneOf(i) }} />
              <Text style={{ ...TYPE.body, fontSize: 13, color: palette.ink, flex: 1 }}>
                {r.station}
                {r.name ? (
                  <Text style={{ color: palette.faint }}>{"  "}{r.name}</Text>
                ) : null}
              </Text>
            </View>
            {/* EVERY NUMBER SAYS WHAT IT IS — the weight, labelled, beside its bar. */}
            <View style={{ flexDirection: "row", alignItems: "center", gap: 9,
                           marginLeft: 18 }}>
              <View style={{ flex: 1, height: 5, backgroundColor: palette.line2,
                             borderRadius: 3, overflow: "hidden" }}>
                <View style={{ width: r.bar, height: 5, backgroundColor: toneOf(i) }} />
              </View>
              <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.sub }}>
                {r.weightLine}
              </Text>
            </View>
            {/* WHAT THIS GAUGE ITSELF READS, in the headline's words. */}
            <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.sub,
                           marginLeft: 18 }}>
              {r.reads}
            </Text>
            <Text style={{ ...TYPE.small, fontSize: 11, color: palette.faint,
                           marginLeft: 18 }}>
              {r.facts}
            </Text>
            {/* WHY IT COUNTS WHAT IT COUNTS, in the model's own terms. */}
            {r.because && (
              <Text style={{ ...TYPE.small, fontSize: 11, color: palette.faint,
                             marginLeft: 18 }}>
                {r.because}
              </Text>
            )}
          </Pressable>
          );
        })}
      </View>

      {/* A SHORT RECORD, A DAM, A DISAGREEMENT — each said once, plainly, above the small
          print, for the reader deciding whether to trust the headline. */}
      {vm.shortRecord && (
        <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.sub, marginTop: 9,
                       lineHeight: 16 }}>
          {vm.shortRecord}
        </Text>
      )}
      {vm.regulated && (
        <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.sub, marginTop: 9,
                       lineHeight: 16 }}>
          {vm.regulated}
        </Text>
      )}
      {vm.disagree && (
        <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.restricted,
                       marginTop: 9, lineHeight: 16 }}>
          {vm.disagree}
        </Text>
      )}

      <Text style={{ ...TYPE.small, fontSize: 11, color: palette.faint, marginTop: 10,
                     lineHeight: 16 }}>
        {vm.explainer}
      </Text>
    </View>
  );
}

/** One entry in the map's key — a coloured dot and what it means. */
function Key({ palette, tone, label }: { palette: Palette; tone: string; label: string }) {
  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: 5 }}>
      <View style={{ width: 8, height: 8, borderRadius: 4, backgroundColor: tone }} />
      <Text style={{ ...TYPE.micro, fontSize: 10, color: palette.faint }}>{label}</Text>
    </View>
  );
}
