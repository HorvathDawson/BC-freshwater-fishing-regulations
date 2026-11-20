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
 *        zoom buttons, pinch and wheel
 *
 * Owner 2026-11-18 (T881): "we should choose one map size maybe larger fit only" — ONE SIZE, the larger one
 * (`ROUTE_MAP_SIZE`), opened fitted to the water and its gauges; the Fit and Larger map buttons are gone.
 *
 * IT DRAWS A VIEW MODEL (`DonorMapVM`, built in @app/ui) AND DECIDES NOTHING: which pins,
 * which routes, which words and the camera are all made there. The tones are the palette's.
 */
import { useMemo, type ReactNode } from "react";
import { View } from "react-native";
import { Text } from "./Text";
import Svg, { Circle, Line, Path } from "react-native-svg";
import type { SectionKey } from "@app/core";
import type { MapProps, TileEndpoints, TraceProp } from "@app/map";
import type { DonorMapVM } from "@app/ui";
import { MiniMap } from "./MiniMap";
import { TYPE } from "./type";
import { SPACE, type Palette } from "./theme";

/**
 * THE MAP'S SIZES, as a share of its measured width within bounds: a fixed 210 px was a letterbox in a desktop column
 * and the pins crowded into a strip. `large` is about square — room to read a valley; `small` is the old default.
 */
const ROUTE_MAP_SIZES = {
  large: { share: 0.95, min: 380, max: 620 },
  small: { share: 0.6, min: 220, max: 340 },
} as const;

/** (T881, owner 2026-11-18: "maybe larger") THE ONE SIZE the route map is drawn at — change it here to go back. */
const ROUTE_MAP_SIZE: keyof typeof ROUTE_MAP_SIZES = "large";

/**
 * The map's height for its measured width. Read by the camera too (DonorPanel hands it to
 * `useDonorView`), so the frame the camera fits and the frame drawn are the same box.
 */
export function routeMapHeight(width: number): number {
  const s = ROUTE_MAP_SIZES[ROUTE_MAP_SIZE];
  return Math.round(Math.min(s.max, Math.max(s.min, width * s.share)));
}

export function DonorMap({ vm, at, theme, palette, toneOf, height,
                           highlight, standing, waterLine, selected }: {
  vm: DonorMapVM; at: TileEndpoints; theme: string; palette: Palette;
  /** A donor row's tone (grey when quiet), or the accent for null — the panel's own rule. */
  toneOf: (row: number | null) => string;
  height: number;
  highlight?: readonly SectionKey[];
  /** The route's colours: the engine's standing channel (`routeColour.ts`). */
  standing?: MapProps["standing"];
  /** (T175) The water's own line at every zoom — see MapProps.waterLine. */
  waterLine?: MapProps["waterLine"];
  /** The gauge picked in the list: its route is drawn heavier, the others thinner. */
  selected?: string | null;
}) {
  /*
   * WHAT THE MAP COPIES OUT OF THE TILES. Keyed on the routes alone: a horizon change moves
   * the shares (the pins) and must not refetch a tile; a new section, new routes, does.
   */
  const routes = useMemo(() => vm.routes.map((r) => ({
    role: r.role, tone: toneOf(r.row), path: r.path, on: !!selected && r.station === selected,
    from: r.from ? { lon: r.from.lon, lat: r.from.lat } : null,
    to: r.to ? { lon: r.to.lon, lat: r.to.lat } : null,
    ...(r.mark ? { mark: r.mark } : {}),
  })), [vm.routes, toneOf]);
  const routeKey = routes.map((r) => `${r.role}:${r.tone}:${r.on ? 1 : 0}:${r.path.join(",")}${r.mark ? `:${r.mark.label}` : ""}`)
    .join("|");
  const pins = vm.pins.map((p) => ({ lat: p.lat, lon: p.lon, kind: p.kind,
                                     tone: toneOf(p.row), label: p.label, rank: p.rank }));
  const trace: TraceProp = { key: routeKey, routes, pins };

  return (
    <View style={{ gap: SPACE.s2 }}>
      {/* (T831) testID: the iPhone bench finds the map's frame by it (the pin is counted inside it, not in the key below) */}
      <View testID="donor-route-map" style={{ height, borderRadius: palette.r.box, overflow: "hidden" }}>
        <MiniMap key={vm.key} at={at} palette={palette} theme={theme} camera={vm.camera}
                 height={height} bare zoomControls view="conditions"
                 highlight={highlight ?? vm.chain} standing={standing}
                 trace={trace} waterLine={waterLine}
                 /* WATER ONLY: every other layer competes for the pixels the route needs. */
                 groups={{ water: true, admin: false, protected: false, access: false,
                           ownership: false, depth: false }} />
      </View>

      {/* THE KEY: ONE LINE of small inline symbols — you are here, a route down, a route up. The gauges are not
          listed again here: the list under the map (its dots the same tones as the pins) names each one. */}
      {(vm.keys.some((k) => k.row === null) || vm.legend.length > 0) && (
        <View style={{ flexDirection: "row", flexWrap: "wrap", columnGap: SPACE.s3, rowGap: 2 }}>
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
      )}
      {/* A DONOR WITH NO ROUTE IS SAID SO, not quietly dropped from the key. */}
      {vm.unplaced && <Text style={{ ...TYPE.meta, color: palette.faint }}>{vm.unplaced}</Text>}
    </View>
  );
}

function KeyRow({ palette, label, children }: {
  palette: Palette; label: string; children: ReactNode;
}) {
  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s2 }}>
      <View style={{ minWidth: 14, alignItems: "center" }}>{children}</View>
      <Text style={{ ...TYPE.micro, color: palette.sub, flexShrink: 1 }}>{label}</Text>
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
