/**
 * T173 — THE FISH OBSTACLE ICONS: one tiny drawing per type, as DATA on a 16 x 16 grid, so the map (which rasterises
 * them for `addImage` / the phone's `Images`: `@app/map` `obstacle-icons.ts`), the tap sheet's header and the Layers
 * sheet's key (`@app/ui-native` `ObstacleIcon`) all draw the same marks from one table.
 *
 * Neutral ink with a light halo (the map's two existing tokens, `color.obstacle.mark` / `color.obstacle.ring`): no hue,
 * because the colours on this map are answers (a status, a gauge's reading) and an obstacle is context. Strokes are round
 * 1.7 wide, so a mark is still a shape at 14 px and at 3x.
 *
 *   falls     water dropping off a ledge       cascade   a stepped run         canyon   two walls, a narrow gap
 *   velocity  fast chevrons                    dam       a wall across         culvert  a pipe, seen in perspective
 *   other     a dot (a type this app does not know, or none)
 */
import { OBSTACLE_TYPES } from "./obstacles";

export const OBSTACLE_ICON_KINDS = [...OBSTACLE_TYPES, "other"] as const;
export type ObstacleIconKind = (typeof OBSTACLE_ICON_KINDS)[number];

/** The grid every drawing is on, and the halo's width (grid units) around the ink. */
export const OBSTACLE_ICON_GRID = 16;
export const OBSTACLE_ICON_HALO = 1.4;

export type Pt = readonly [x: number, y: number];
export type IconShape =
  | { readonly stroke: readonly Pt[]; readonly w: number }                 // an open polyline, round caps and joins
  | { readonly fill: readonly Pt[] }                                       // a closed polygon
  | { readonly ring: readonly [cx: number, cy: number, rx: number, ry: number]; readonly w: number }
  | { readonly disc: readonly [cx: number, cy: number, r: number] };

const W = 1.7;
export const OBSTACLE_ICON_SHAPES: Readonly<Record<ObstacleIconKind, readonly IconShape[]>> = {
  falls: [
    { stroke: [[1.5, 3], [14.5, 3]], w: 2 },                                            // the ledge
    { stroke: [[4.5, 6.5], [4.5, 9.5]], w: W }, { stroke: [[8, 6.5], [8, 13]], w: W },   // the water, dropping (ragged, so it is not a column)
    { stroke: [[11.5, 6.5], [11.5, 10.5]], w: W },
  ],
  cascade: [{ stroke: [[2, 3], [5.5, 3], [5.5, 7.5], [10, 7.5], [10, 12], [14, 12]], w: W }],
  canyon: [
    { fill: [[1.5, 2], [5.8, 2], [4.8, 5.5], [6, 8], [4.6, 11], [5.4, 14], [1.5, 14]] },
    { fill: [[10.2, 2], [14.5, 2], [14.5, 14], [10.6, 14], [11.4, 11], [10, 8], [11.2, 5.5]] },
  ],
  velocity: [
    { stroke: [[3, 3.5], [7, 8], [3, 12.5]], w: W },
    { stroke: [[8.5, 3.5], [12.5, 8], [8.5, 12.5]], w: W },
  ],
  dam: [
    { fill: [[1.5, 3], [14.5, 3], [14.5, 8], [1.5, 8]] },                                // the wall
    { stroke: [[1.5, 12.5], [4.2, 10.8], [6.9, 12.5], [9.6, 10.8], [12.3, 12.5], [14.5, 11.2]], w: 1.4 },  // the water under it
  ],
  culvert: [
    { stroke: [[2, 3.5], [10, 3.5]], w: W }, { stroke: [[2, 12.5], [10, 12.5]], w: W },
    { ring: [10, 8, 3.3, 4.5], w: W },
  ],
  other: [{ disc: [8, 8, 3.4] }],
};

/** The icon for a tile record's `type` (anything not a known type is the dot — never dropped). */
export function obstacleIconKind(type: unknown): ObstacleIconKind {
  return (OBSTACLE_TYPES as readonly unknown[]).includes(type) ? (type as ObstacleIconKind) : "other";
}

/**
 * Who is placed first when marks crowd (MapLibre places the lowest first and a later one that would overlap is dropped):
 * what stops fish outright, then what may be passable, then the dot.
 */
export const OBSTACLE_ICON_PRIORITY: readonly ObstacleIconKind[] = ["dam", "falls", "canyon", "cascade", "velocity", "culvert", "other"];

/** The drawing as a standalone SVG (a 16 grid), the halo under the ink — what the key and the header show, and the shots' sheet. */
export function obstacleIconSvg(kind: ObstacleIconKind, ink: string, halo: string | null = null, size = 16): string {
  const els = (colour: string, grow: number): string => OBSTACLE_ICON_SHAPES[kind].map((s) => {
    const pts = (p: readonly Pt[]) => p.map(([x, y], i) => `${i ? "L" : "M"}${x} ${y}`).join(" ");
    if ("stroke" in s) return `<path d="${pts(s.stroke)}" fill="none" stroke="${colour}" stroke-width="${s.w + grow}" stroke-linecap="round" stroke-linejoin="round"/>`;
    if ("fill" in s) return `<path d="${pts(s.fill)} Z" fill="${colour}" stroke="${colour}" stroke-width="${grow}" stroke-linejoin="round"/>`;
    if ("ring" in s) return `<ellipse cx="${s.ring[0]}" cy="${s.ring[1]}" rx="${s.ring[2]}" ry="${s.ring[3]}" fill="none" stroke="${colour}" stroke-width="${s.w + grow}"/>`;
    return `<circle cx="${s.disc[0]}" cy="${s.disc[1]}" r="${s.disc[2] + grow / 2}" fill="${colour}"/>`;
  }).join("");
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${size}" height="${size}" viewBox="0 0 16 16">`
    + (halo ? els(halo, OBSTACLE_ICON_HALO * 2) : "") + els(ink, 0) + "</svg>";
}
