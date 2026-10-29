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
 * ONE READING ORDER (P16). The panel is laid out top to bottom as a reader asks:
 *
 *   header     the horizon control — the panel's place to change "when"; the Conditions
 *              map's corner offers the same chips over the same state (owner 2026-10-20)
 *   headline   what the water is doing, and the range
 *   `detail`   the lead gauge's own reading (the caller's), as the headline's detail rows
 *   `chart`    the lead gauge's hydrograph (the caller's)
 *   donors     the map, its key, and one row per gauge
 *   fold       "Counts this much because…", the notes (a short record, a dam, a
 *              disagreement), the explainer, and `foot` (the chart's disclaimer and the
 *              credits) — closed by default, the words unchanged
 *
 * The caller's blocks are SLOTS rather than siblings so the order is decided here, once:
 * stacked as siblings, the reading and the trace sat between the donor list and the chart
 * and the screen read as three answers.
 *
 * IT DRAWS A VIEW MODEL AND DECIDES NOTHING (AGENTS 25, P12a). Every sentence is composed
 * in core (`conditions.ts`, pinned whole) and every choice — which notes show, which pin
 * absorbs "you are here", the day the range is for — is made in `donorView` in @app/ui.
 */
import type { SectionKey } from "@app/core";
import { CONDITIONS_WORDS } from "@app/core";
import { useState, type ReactNode } from "react";
import { LayoutChangeEvent, Pressable, Text, View } from "react-native";
import type { MapProps, TileEndpoints } from "@app/map";
import type { StationId } from "@app/data";
import { useDonorView, type DonorRowVM, type GaugeSides, type Horizon, type HorizonChipVM, type PanelAnswer,
         type Quantity } from "@app/ui";
import { DonorMap, routeMapHeight } from "./DonorMap";
import { CornerStrip } from "./CornerStrip";
import { GaugeGroups } from "./GaugeGroups";
import { Icon } from "./icons";
import { TYPE } from "./type";
import { SPACE, type Palette } from "./theme";

/** The bullet's inset, so a row's detail lines align under its name and not its dot. */
const INSET = 9 + SPACE.s2;

/**
 * THE DESKTOP BREAK (owner 2026-10-21: "desktop falls apart"). At or above this width the panel is two columns — the
 * answer and its chart on the left, the gauges behind it (map, list, notes) on the right — instead of one phone column
 * stretched to 1400 px with a donor bar the width of the screen. Below it, nothing changes.
 */
export const WIDE_PANEL = 880;

/** The gap between the two columns. */
const COLUMN_GAP = SPACE.s4 * 2;

/**
 * A donor's colour — GREY WHEN IT IS QUIET. A station that is not reporting contributes nothing, and a live tone would
 * say otherwise; grey is the map's own "nothing to say". One function for the pin, the row, the key and the chart's
 * gauge switch, so the four are one thing.
 */
export function donorTone(palette: Palette, row: number | null, quiet: boolean): string {
  if (row === null) return palette.accent;
  return quiet ? palette.quiet : palette.donor[row % palette.donor.length]!;
}

/** The middle of a water's box: the point the route map closes in about when no tap says where the reader is. */
const placeMid = (b: readonly [number, number, number, number] | null | undefined) =>
  (b ? { lon: (b[0] + b[2]) / 2, lat: (b[1] + b[3]) / 2 } : null);

export function DonorPanel({ palette, value, day, at, theme, from, selected, onSelect,
                             horizon = 0, onHorizon, chain: chainProp, standing,
                             detail, chart, foot, quantity, sides, count, waterLine, place }: {
  palette: Palette; value: PanelAnswer;
  /**
   * The quantity the answer is in, when the caller knows it (the conditions sheet: the one the map paints). Only the
   * tide's sentence reads it — Level only (T78). A saved spot passes none and says nothing of the tide.
   */
  quantity?: Quantity;
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
  /** The route's colours, through the engine's standing channel — `routeColour.ts`. */
  standing?: MapProps["standing"];
  /** How far ahead this estimate is for — 0 is now. Only the WORDS change for a forecast. */
  horizon?: Horizon;
  /** Change the horizon from here, when the caller owns it. Absent, no chips. */
  onHorizon?: (d: Horizon) => void;
  /** Drawn under the headline: the lead gauge's reading, or the no-gauge sentence. */
  detail?: ReactNode;
  /** Drawn between the headline and the donor list: the lead gauge's chart. */
  chart?: ReactNode;
  /** Drawn last, inside the fold: the chart's disclaimer and the data credits. */
  foot?: ReactNode;
  /**
   * (T168) Each member's side — on this water or a donor (`useConditionsView`'s `sides`). With it the rows group, own
   * first, and the not-reporting fold away; without it (a saved spot) one run, as before.
   */
  sides?: GaugeSides;
  /** (T168) The count line: "2 gauges on this water · 5 estimating from other waters · 6 not reporting". */
  count?: string | null;
  /** (T175) The water's own sections: the route map keeps their line at every zoom, opening no further out than it. */
  waterLine?: readonly SectionKey[];
  /** (T175) The water's box, framed with the gauges when no tap says where the reader is (`DonorViewOptions.place`). */
  place?: readonly [number, number, number, number] | null;
}) {
  /*
   * MEASURED, NOT ASSUMED. The panel is rendered at every width from a phone to a desktop
   * column, and the camera has to know which axis is the tight one.
   */
  const [mapWidth, setMapWidth] = useState(360);
  // The route map's height, read by the camera as well as by the map so they agree — see
  // `routeMapHeight` (the conditions map owns it, with the "Larger map" switch).
  const [larger, setLarger] = useState(false);
  const mapH = routeMapHeight(mapWidth, larger);
  const onMapLayout = (e: LayoutChangeEvent) => {
    const w = Math.round(e.nativeEvent.layout.width);
    if (w > 0 && w !== mapWidth) setMapWidth(w);
  };
  // The panel's own width decides one column or two; the map's column is measured separately (above).
  const [width, setWidth] = useState(0);
  const onLayout = (e: LayoutChangeEvent) => {
    const w = Math.round(e.nativeEvent.layout.width);
    if (w > 0 && w !== width) setWidth(w);
  };
  const wide = width >= WIDE_PANEL;
  const vm = useDonorView(value, { day, horizon, from: from ?? null,
                                   size: { width: mapWidth, height: mapH },
                                   selected: selected ?? null, pickable: !!onSelect,
                                   ...(quantity ? { quantity } : {}), sides: sides ?? null, place: place ?? null });

  if (!vm.ok) {
    // THE NO-ESTIMATE CASE IS THE SAME LAYOUT with fewer blocks: the kicker and the reason
    // stand where the headline stands, and everything else keeps its place. The header
    // stays when the refusal is about the chosen day (`no-forecast`, e.g. +10 past every
    // run), so the reader can step back from it here.
    return (
      <View style={{ gap: SPACE.s4 }} onLayout={onLayout}>
        {onHorizon && vm.horizons && (
          <HorizonHeader palette={palette} chips={vm.horizons} onPick={onHorizon} />
        )}
        <Block palette={palette} first>
          <Kicker palette={palette}>{vm.heading}</Kicker>
          <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{vm.reason}</Text>
          {count ? <CountLine palette={palette}>{count}</CountLine> : null}
          {/* EVERY MEMBER SILENT: the same line the table carries, naming them all (owner 2026-10-22). */}
          {vm.silent ? <Silent palette={palette}>{vm.silent}</Silent> : null}
          {detail}
        </Block>
        {chart && <Block palette={palette}>{chart}</Block>}
        {foot && <Fold palette={palette}>{foot}</Fold>}
      </View>
    );
  }
  /**
   * A donor's colour — GREY WHEN IT IS QUIET. A station that is not reporting contributes
   * nothing, and a live tone would say otherwise; grey is the map's own "nothing to say".
   * BY THE ROW'S TONE INDEX (`r.row`, its place in the panel's weight order), which its pin, route and key carry too:
   * silent members are not drawn, so a row's place in the list is not its index.
   */
  const quietAt = new Map([...vm.rows, ...(vm.groups ?? []).flatMap((g) => [...g.shown, ...g.folded])]
    .map((r) => [r.row, r.quiet]));
  const toneOf = (i: number | null) => donorTone(palette, i, i !== null && !!quietAt.get(i));
  const map = vm.map;
  const because = vm.rows.filter((r) => r.because);
  /** One donor row; `i` 0 is the first of its list (no hairline above it). */
  const row = (r: DonorRowVM, i: number) => {
    const pick = onSelect ? () => onSelect(r.station as StationId) : undefined;
    return (
    <Pressable onPress={pick} disabled={!pick}
          accessibilityRole={pick ? "button" : "text"}
          // `aria-selected`, not `accessibilityState`: react-native-web >= 0.21
          // ignores the latter (AGENTS 34).
          aria-selected={pick ? r.on : undefined}
          accessibilityLabel={r.label}
          style={{ paddingVertical: SPACE.s2, gap: 2,
                   borderTopWidth: i === 0 ? 0 : 1, borderTopColor: palette.line,
                   // THE SELECTED ROW IS MARKED ON THE LEFT, not by a tint: the row
                   // already carries a colour that means something (its pin).
                   paddingLeft: SPACE.s2, marginLeft: -SPACE.s2 - 3,
                   borderLeftWidth: 3,
                   borderLeftColor: r.on ? toneOf(r.row) : "transparent" }}>
      {/* A TIGHT ROW: dot · id · name · its share, on one line; what it reads under it. */}
      <View style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s2 }}>
        {/* The same tone as its pin, so the map and the table are one thing. */}
        <View testID={`donor-dot-${r.station}`}
              style={{ width: 9, height: 9, borderRadius: 5, backgroundColor: toneOf(r.row) }} />
        <Text style={{ ...TYPE.figure, color: palette.ink }}>{r.station}</Text>
        {r.name ? (
          <Text numberOfLines={1} style={{ ...TYPE.metaSemi, color: palette.sub, flex: 1, minWidth: 0 }}>
            {r.name}
          </Text>
        ) : <View style={{ flex: 1 }} />}
        {/* EVERY NUMBER SAYS WHAT IT IS — the weight, labelled. */}
        <Text style={{ ...TYPE.metaBold, color: palette.ink }}>{r.weightLine}</Text>
      </View>
      {/* WHAT THIS GAUGE ITSELF READS, in the headline's words. */}
      <Text style={{ ...TYPE.meta, color: palette.ink, marginLeft: INSET }}>
        {r.reads}
      </Text>
      {/* WHEN A QUIET GAUGE LAST SPOKE (owner 2026-10-21), or that it is not known; then its facts. */}
      <Text style={{ ...TYPE.meta, color: palette.sub, marginLeft: INSET }}>
        {[r.lastReport, r.facts].filter(Boolean).join(" · ")}
      </Text>
      {/* HOW IT RELATES to the water being read — a gauge on another water says where the two meet (owner 2026-10-28). */}
      {r.relation ? (
        <Text testID={`donor-relation-${r.station}`} style={{ ...TYPE.meta, color: palette.faint, marginLeft: INSET }}>
          {r.relation}
        </Text>
      ) : null}
    </Pressable>
    );
  };

  const answerSide = (
    <>
      {/* THE PANEL HEADER: the horizon control, `horizonChips` over the Shell's one state —
          the Conditions map's corner draws the same chips (owner 2026-10-20). Above the
          headline it changes, never under it. */}
      {onHorizon && <HorizonHeader palette={palette} chips={vm.horizons} onPick={onHorizon} />}

      <Block palette={palette} first>
        {/* ONE LINE, ALWAYS: a heading that wraps moves everything under it as the reader
            steps through the horizons. */}
        <Kicker palette={palette} oneLine>{vm.heading}</Kicker>
        {/* THE PLAIN SENTENCE FIRST, the percentile second — the same numbers, in the order
            a person reads them. Two lines of room whatever it says, so nothing below shifts
            as the reader moves between horizons. */}
        <Text style={{ ...TYPE.name, color: palette.ink, minHeight: 2 * TYPE.name.lineHeight }}>
          {vm.headline}
          {vm.headlineSuffix ? (
            <Text style={{ color: palette.sub }}>{" "}{vm.headlineSuffix}</Text>
          ) : null}
        </Text>
        <Text style={{ ...TYPE.body, color: palette.ink }}>{vm.sentence}</Text>
        <Text style={{ ...TYPE.meta, color: palette.sub }}>{vm.range}</Text>
        {/* (T157) the low-water caveat for the spot's own Change, beside the answer it qualifies (never folded) */}
        {vm.lowWater && (
          <Text testID="section-low-water" style={{ ...TYPE.meta, color: palette.ink }}>{vm.lowWater}</Text>
        )}
      </Block>

      {detail && <Block palette={palette}>{detail}</Block>}
      {chart && <Block palette={palette}>{chart}</Block>}
    </>
  );

  const gaugeSide = (
    <>
      <Block palette={palette} first={wide}>
        {/* (T168) THE COUNT LINE first: how many gauges stand on this water, how many estimate from others */}
        {count ? <CountLine palette={palette}>{count}</CountLine> : null}
        <View onLayout={onMapLayout} />
        {/* THE MAP WAITS FOR THE ROUTES (`map` is null until they are in): a MapLibre map
            reads its opening camera once, on mount, and a map mounted early stays framed on
            the tap alone. Keyed on the camera for the same reason. */}
        {/* THE ROUTE MAP and its key — `DonorMap` (owner 2026-10-21: routes up and down
            apart, the spot its own pin, each gauge's share on the map, zoom and fit). */}
        {at && theme && map && (
          <View style={{ marginBottom: SPACE.s2 }}>
            <DonorMap vm={map} at={at} theme={theme} palette={palette} toneOf={toneOf}
                      height={mapH} larger={larger} onLarger={setLarger}
                      highlight={chainProp ?? map.chain} selected={selected ?? null} standing={standing}
                      waterLine={waterLine?.length ? { sections: waterLine, at: from ?? placeMid(place) } : undefined} />
          </View>
        )}

        {vm.groups ? (
          <View accessibilityRole="list" accessibilityLabel={vm.listLabel}>
            <GaugeGroups palette={palette} groups={vm.groups} keyOf={(r) => r.station} testID="donor-groups"
                         render={(r, first) => row(r, first ? 0 : 1)} />
          </View>
        ) : (
          <View accessibilityRole="list" accessibilityLabel={vm.listLabel}>
            {vm.rows.map((r, i) => <View key={r.station}>{row(r, i)}</View>)}
          </View>
        )}
        {/* THE HINT, a quiet caption under the list it is about (it was a kicker, a heading of its own). */}
        {onSelect && <Text style={{ ...TYPE.meta, color: palette.faint }}>{vm.tapToChart}</Text>}
        {/* THE SILENT MEMBERS, NAMED (owner 2026-10-22): not rows, not pins — one line, each with its last data. */}
        {vm.silent && !vm.groups ? <Silent palette={palette}>{vm.silent}</Silent> : null}
      </Block>

      {/* THE WORKING'S WORKING, closed (P16 item 3): why each gauge counts what it counts,
          the notes that qualify the answer, how it is carried, and whose data it is. Every
          word as before, in the order it was said before, one tap away. */}
      <Fold palette={palette}>
        {because.map((r) => (
          <View key={r.station} style={{ gap: SPACE.s1 }}>
            <Text style={{ ...TYPE.figure, color: palette.ink }}>{r.station}</Text>
            <Text style={{ ...TYPE.meta, color: palette.sub }}>{r.because}</Text>
          </View>
        ))}
        {/* A SHORT RECORD, A DAM, A DISAGREEMENT — each said once, plainly, marked by a rule
            on its left; a disagreement in the restricted tone, the others a hairline. The tide (T78) and a lake's
            backwater (T160) are said the same way, in the Level view only. */}
        {vm.shortRecord && <Caveat palette={palette} tone={palette.line2}>{vm.shortRecord}</Caveat>}
        {vm.regulated && <Caveat palette={palette} tone={palette.line2}>{vm.regulated}</Caveat>}
        {vm.tidal && <Caveat palette={palette} tone={palette.line2}>{vm.tidal}</Caveat>}
        {vm.backwater && <Caveat palette={palette} tone={palette.line2}>{vm.backwater}</Caveat>}
        {vm.disagree && <Caveat palette={palette} tone={palette.restricted}>{vm.disagree}</Caveat>}
        <Text style={{ ...TYPE.meta, color: palette.sub }}>{vm.explainer}</Text>
        {foot}
      </Fold>
    </>
  );

  // Measured on the ROOT, which renders on the first pass. ONE READING ORDER either way: in two columns the left is
  // read first (header → headline → reading → chart), then the right (donors → the fold).
  return wide ? (
    <View style={{ flexDirection: "row", alignItems: "flex-start", gap: COLUMN_GAP }} onLayout={onLayout}>
      <View style={{ flex: 3, minWidth: 0, gap: SPACE.s4 }}>{answerSide}</View>
      <View style={{ flex: 2, minWidth: 0, gap: SPACE.s4 }}>{gaugeSide}</View>
    </View>
  ) : (
    <View style={{ gap: SPACE.s4 }} onLayout={onLayout}>
      {answerSide}
      {gaugeSide}
    </View>
  );
}

/**
 * The panel header's horizon chips — drawn from the VM's `horizons` (decided in @app/ui), as ONE compact segmented row
 * (the map's own day strip, `CornerStrip`), never a wrapping run of chips: on a phone "Now · +1d · +3d · +5d · +10d ·
 * Change" was two lines. Change is the calendar glyph at the end, said aloud as its label.
 */
function HorizonHeader({ palette, chips, onPick }: {
  palette: Palette; chips: readonly HorizonChipVM[]; onPick: (d: Horizon) => void;
}) {
  return (
    <View style={{ flexDirection: "row" }}>
      <CornerStrip palette={palette} testID="panel-horizons"
                   segments={chips.map((h) => ({
                     key: String(h.day), chip: h.day === "change" ? "" : h.chip, label: h.label, on: h.on,
                     ...(h.note ? { note: h.note } : {}), ...(h.day === "change" ? { icon: "calendar" as const } : {}),
                     onPress: () => onPick(h.day) }))} />
    </View>
  );
}

/** An all-caps section kicker — the regs page's `.lab` (T22). */
export function Kicker({ palette, children, oneLine }: {
  palette: Palette; children: ReactNode; oneLine?: boolean;
}) {
  return (
    <Text numberOfLines={oneLine ? 1 : undefined}
          style={{ ...TYPE.kicker, color: palette.faint }}>{children}</Text>
  );
}

/**
 * One block in the reading order. A hairline and the same step above every block but the
 * first, so the panel reads as a stack of parts rather than one run of text.
 */
export function Block({ palette, children, first }: {
  palette: Palette; children: ReactNode; first?: boolean;
}) {
  return (
    <View style={{ gap: SPACE.s2,
                   ...(first ? {} : { paddingTop: SPACE.s4, borderTopWidth: 1,
                                      borderTopColor: palette.line }) }}>
      {children}
    </View>
  );
}

/** (T168) The count line: how many gauges stand on this water, how many estimate from others, how many are silent. */
function CountLine({ palette, children }: { palette: Palette; children: ReactNode }) {
  return <Text testID="gauge-count" style={{ ...TYPE.metaBold, color: palette.ink }}>{children}</Text>;
}

/** The one line naming the panel's silent members — quieter than a row, never a row. */
function Silent({ palette, children }: { palette: Palette; children: ReactNode }) {
  return <Text style={{ ...TYPE.meta, color: palette.sub }}>{children}</Text>;
}

/** A note that qualifies the answer, marked by a rule on its left. */
function Caveat({ palette, tone, children }: {
  palette: Palette; tone: string; children: ReactNode;
}) {
  return (
    <View style={{ borderLeftWidth: 3, borderLeftColor: tone, paddingLeft: SPACE.s2 }}>
      <Text style={{ ...TYPE.meta, color: palette.ink }}>{children}</Text>
    </View>
  );
}

/**
 * THE ONE FOLD. Closed by default; its label is core's, and opening it changes no words —
 * it only stops the small print competing with the answer.
 */
export function Fold({ palette, children }: { palette: Palette; children: ReactNode }) {
  const [open, setOpen] = useState(false);
  return (
    <View style={{ borderTopWidth: 1, borderTopColor: palette.line }}>
      <Pressable onPress={() => setOpen(!open)} accessibilityRole="button"
                 aria-expanded={open} accessibilityLabel={CONDITIONS_WORDS.fold}
                 style={{ flexDirection: "row", alignItems: "center",
                          paddingVertical: SPACE.s3, gap: SPACE.s2 }}>
        <Text style={{ ...TYPE.kicker, color: palette.sub, flex: 1 }}>
          {CONDITIONS_WORDS.fold}
        </Text>
        {/* The set's chevron (N1): down while closed, up while open; it carries no words. */}
        <Icon name={open ? "collapse" : "expand"} colour={palette.sub} size={16} />
      </Pressable>
      {open && <View style={{ gap: SPACE.s3, paddingBottom: SPACE.s3 }}>{children}</View>}
    </View>
  );
}

