/**
 * THE ROUTE MAP — how the gauges behind an estimate reach the place being read.
 *
 * Owner 2026-10-21, point by point, and where each is answered:
 *
 *   "it should show downstream and upstream separately"
 *        a gauge downstream is joined to you by a SOLID line, one upstream by a DASHED one,
 *        each in the gauge's own tone; the key under the map says which is which
 *   "the icon for where you are should be different than gauges"
 *        a pin standing on the spot (the accent) against round dots for gauges — the shape
 *        says it before the colour does, in either theme
 *   "the map should say how much of the contribution the gauge gives"
 *        every dot is labelled with its share of the estimate, the panel's own number
 *   "if it is zoomed out too much the stream ... dont even draw"
 *        the routes are copied out of the tiles when the panel opens (`trace` on the map)
 *        and drawn as an overlay that shows at every zoom
 *   "the ability to zoom and look at data better"
 *        zoom buttons, pinch and wheel; a Fit button to come back; and a larger map
 *
 * IT DRAWS A VIEW MODEL (`DonorMapVM`, built in @app/ui) AND DECIDES NOTHING: which pins,
 * which routes, which words and the camera are all made there. The tones are the palette's.
 */
import { useMemo, useState, type ReactNode } from "react";
import { Pressable, Text, View } from "react-native";
import Svg, { Circle, Line, Path } from "react-native-svg";
import type { SectionKey } from "@app/core";
import type { MapProps, TileEndpoints, TraceProp } from "@app/map";
import type { DonorMapVM } from "@app/ui";
import { MiniMap } from "./MiniMap";
import { GaugeGroups } from "./GaugeGroups";
import { TYPE } from "./type";
import { SPACE, type Palette } from "./theme";

/**
 * The map's height for its measured width. Read by the camera too (DonorPanel hands it to
 * `useDonorView`), so the frame the camera fits and the frame drawn are the same box.
 *
 * Proportional, within bounds: a fixed 210 px was a letterbox in a desktop column and the
 * pins crowded into a strip. Larger, it is about square — room to read a valley.
 */
export function routeMapHeight(width: number, larger: boolean): number {
  return larger
    ? Math.round(Math.min(620, Math.max(380, width * 0.95)))
    : Math.round(Math.min(340, Math.max(220, width * 0.6)));
}

export function DonorMap({ vm, at, theme, palette, toneOf, height, larger, onLarger,
                           highlight, standing, waterLine }: {
  vm: DonorMapVM; at: TileEndpoints; theme: string; palette: Palette;
  /** A donor row's tone (grey when quiet), or the accent for null — the panel's own rule. */
  toneOf: (row: number | null) => string;
  height: number;
  larger: boolean;
  onLarger: (on: boolean) => void;
  highlight?: readonly SectionKey[];
  /** The route's colours: the engine's standing channel (`routeColour.ts`). */
  standing?: MapProps["standing"];
  /** (T175) The water's own line at every zoom — see MapProps.waterLine. */
  waterLine?: MapProps["waterLine"];
}) {
  /*
   * WHAT THE MAP COPIES OUT OF THE TILES. Keyed on the routes alone: a horizon change moves
   * the shares (the pins) and must not refetch a tile; a new section, new routes, does.
   */
  const routes = useMemo(() => vm.routes.map((r) => ({
    role: r.role, tone: toneOf(r.row), path: r.path,
    from: r.from ? { lon: r.from.lon, lat: r.from.lat } : null,
    to: { lon: r.to.lon, lat: r.to.lat },
  })), [vm.routes, toneOf]);
  const routeKey = routes.map((r) => `${r.role}:${r.tone}:${r.path.join(",")}`).join("|");
  const pins = vm.pins.map((p) => ({ lat: p.lat, lon: p.lon, kind: p.kind,
                                     tone: toneOf(p.row), label: p.label, rank: p.rank }));
  const trace: TraceProp = { key: routeKey, routes, pins };

  // FIT: sent on a press and at no other time, so it never fights a reader's own pan.
  const [fits, setFits] = useState(0);
  const fit = fits > 0 ? { key: `fit:${fits}`, bbox: framed(vm.bbox) } : null;

  return (
    <View style={{ gap: SPACE.s2 }}>
      <View style={{ height, borderRadius: palette.r.box, overflow: "hidden" }}>
        <MiniMap key={vm.key} at={at} palette={palette} theme={theme} camera={vm.camera}
                 height={height} bare zoomControls view="conditions"
                 highlight={highlight ?? vm.chain} standing={standing}
                 trace={trace} fit={fit} waterLine={waterLine}
                 /* WATER ONLY: every other layer competes for the pixels the route needs. */
                 groups={{ water: true, admin: false, protected: false, access: false,
                           ownership: false, depth: false }} />
        {/* TOP-LEFT: the zoom buttons hold the top-right, and the bottom is where the camera
            puts a gauge far down the river (measured: Skeena at Usk under the old bottom row). */}
        <View style={{ position: "absolute", left: SPACE.s2, top: SPACE.s2,
                       flexDirection: "row", gap: SPACE.s2 }}>
          <MapButton palette={palette} label={vm.words.fitLabel} onPress={() => setFits(fits + 1)}>
            {vm.words.fit}
          </MapButton>
          <MapButton palette={palette} label={larger ? vm.words.smaller : vm.words.larger}
                     onPress={() => onLarger(!larger)} on={larger}>
            {larger ? vm.words.smaller : vm.words.larger}
          </MapButton>
        </View>
      </View>

      {/* THE KEY: what each mark and each line means, then the gauges by name and share. */}
      <View style={{ gap: SPACE.s1 }}>
        {vm.keys.some((k) => k.row === null) && (
          <KeyRow palette={palette} label={vm.keys.find((k) => k.row === null)!.label}>
            <HereGlyph fill={palette.accent} edge={palette.card} />
          </KeyRow>
        )}
        {vm.legend.map((l) => (
          <KeyRow key={l.role} palette={palette} label={l.label}>
            <LineGlyph tone={palette.sub} dashed={l.role === "up"} />
          </KeyRow>
        ))}
      </View>
      {/* (T168) grouped, own first, the not-reporting folded — the rows' and the switch's groups */}
      {vm.keyGroups ? (
        <GaugeGroups palette={palette} groups={vm.keyGroups} keyOf={(k) => k.label} wrap testID="key-groups"
                     render={(k) => (
                       <KeyRow palette={palette} label={k.label}>
                         <DotGlyph tone={toneOf(k.row)} edge={palette.sub} />
                       </KeyRow>
                     )} />
      ) : (
        <View style={{ flexDirection: "row", flexWrap: "wrap", columnGap: SPACE.s3,
                       rowGap: SPACE.s1 }}>
          {vm.keys.filter((k) => k.row !== null).map((k) => (
            <KeyRow key={k.label} palette={palette} label={k.label}>
              <DotGlyph tone={toneOf(k.row)} edge={palette.sub} />
            </KeyRow>
          ))}
        </View>
      )}
      {/* A DONOR WITH NO ROUTE IS SAID SO, not quietly dropped from the key. */}
      {vm.unplaced && <Text style={{ ...TYPE.meta, color: palette.faint }}>{vm.unplaced}</Text>}
    </View>
  );
}

/**
 * The box "fit" returns to, never a point: a single gauge at the spot has no extent, and
 * fitting a point flies the camera to the deepest zoom. ~1 km either way at least.
 */
function framed(b: readonly [number, number, number, number]):
    readonly [number, number, number, number] {
  const dx = Math.max(0, 0.012 - (b[2] - b[0]) / 2);
  const dy = Math.max(0, 0.008 - (b[3] - b[1]) / 2);
  return [b[0] - dx, b[1] - dy, b[2] + dx, b[3] + dy];
}

/** A small control over the map's corner — the app's chip, on the card. */
function MapButton({ palette, label, onPress, on, children }: {
  palette: Palette; label: string; onPress: () => void; on?: boolean; children: string;
}) {
  return (
    <Pressable onPress={onPress} accessibilityRole="button" accessibilityLabel={label}
               aria-pressed={on}
               style={{ paddingVertical: SPACE.s1 + 2, paddingHorizontal: SPACE.s3,
                        borderRadius: palette.r.pill, borderWidth: 1,
                        borderColor: on ? palette.accent : palette.line2,
                        backgroundColor: palette.card, ...palette.lift }}>
      <Text style={{ ...TYPE.metaBold, color: on ? palette.accent : palette.ink }}>
        {children}
      </Text>
    </Pressable>
  );
}

function KeyRow({ palette, label, children }: {
  palette: Palette; label: string; children: ReactNode;
}) {
  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s2 }}>
      <View style={{ width: 22, alignItems: "center" }}>{children}</View>
      <Text style={{ ...TYPE.meta, color: palette.sub, flexShrink: 1 }}>{label}</Text>
    </View>
  );
}

/** The map's you-are-here pin, small: the same silhouette as `here-mark.ts` draws. */
export function HereGlyph({ fill, edge }: { fill: string; edge: string }) {
  return (
    <Svg width={12} height={16} viewBox="0 0 22 30">
      <Path d="M11 29 L3.6 15.4 A10 10 0 1 1 18.4 15.4 Z" fill={fill} stroke={edge}
            strokeWidth={2} />
      <Circle cx={11} cy={11} r={3.6} fill={edge} />
    </Svg>
  );
}

/** A gauge's round dot. */
export function DotGlyph({ tone, edge }: { tone: string; edge: string }) {
  return (
    <Svg width={12} height={12} viewBox="0 0 12 12">
      <Circle cx={6} cy={6} r={5} fill={tone} stroke={edge} strokeWidth={1.5} />
    </Svg>
  );
}

/** A route sample: solid for a gauge downstream, dashed for one upstream. */
function LineGlyph({ tone, dashed }: { tone: string; dashed: boolean }) {
  return (
    <Svg width={22} height={6} viewBox="0 0 22 6">
      <Line x1={1} y1={3} x2={21} y2={3} stroke={tone} strokeWidth={3.5}
            strokeDasharray={dashed ? "5 3.5" : undefined} />
    </Svg>
  );
}
