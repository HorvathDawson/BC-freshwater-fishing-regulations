/**
 * T131 — YOUR SPOTS ON THE MAP (owner 2026-10-22: "make spots on the map a) look better and b) clickable to open the
 * spot"). Every map that shows your spots (the Map tab, the Conditions map, a water's map) asks THIS file which marks to
 * draw at its zoom, and what a tap on one does; `@app/map` places them and `@app/ui-native` `SpotPin` draws the face.
 *
 *   SPOT_PIN        the one set of numbers: the 44 px target, the drawn face, how close two spots may be on screen
 *                   before they are one group, and the zoom names start at
 *   spotMarkers     the marks at a zoom: a spot alone (its name from `labelZoom` in, unless it would cross another
 *                   name or mark), or several close spots as ONE group, with the zoom that parts it
 *   spotPress       a tap: a spot opens; a group zooms in to where it parts; a group no zoom parts (spots on one point)
 *                   opens the Spots list
 *
 * Platform-free: the distances are Web Mercator pixels at the zoom (512 px tiles, MapLibre's on both platforms), so the
 * web and the phone group the same spots the same way without asking their map to project anything.
 */
import { CAMERA_MAX_ZOOM } from "./ladder";
import { WATER_BACK } from "./waterViews";

export const SPOT_PIN = {
  /** The target, px square — a finger (WCAG 2.5.5); the face is drawn inside it. */
  hit: 44,
  /** The face, px square: the spot's mark (the Spots tab's icon) on the spot ground. */
  face: 30,
  /** The selected spot's face (the one last opened or shown): larger, and inverted. */
  selectedFace: 36,
  /** The mark inside the face. */
  icon: 18,
  /** Two spots closer than this on screen are ONE group: two faces nearer than this would touch. */
  groupPx: 40,
  /** Names from this zoom in (a valley on a phone); further out the marks alone. */
  labelZoom: 11,
  /** A name's widest, px: a longer one wraps (the face clips nothing). */
  labelMaxWidth: 160,
  /** Between a face's top and its name, px. */
  labelGap: 6,
} as const;

/** What each mark says aloud: a spot by its name; a group by how many, and what a tap does. */
export const SPOT_PIN_WORDS = {
  open: (title: string) => `Open spot ${title}`,
  zoom: (n: number) => `${n} spots here. Zoom in to see them`,
  list: (n: number) => `${n} spots at this place. Open the Spots list`,
} as const;

/**
 * A SPOT'S ‹ SAYS WHERE BACK GOES (one back layer): the tab the spot is open over, by the name the bottom bar prints
 * (the Spots list, or the map its pin was tapped on), or — over a water, its pin tapped on the water's map — "Back",
 * as a water opened from inside a water says.
 */
export function spotBack(overTab: string | null): string {
  return overTab ?? WATER_BACK;
}

/** A saved spot as a map receives it: its id, place, and the name its Spots row shows. */
export interface SpotPoint { id: string; lat: number; lon: number; title: string }

export interface SpotMarker {
  /** Stable while the mark's members are: `spot:<id>` or `group:<ids>`. */
  key: string;
  /** The spots it stands for, in id order: one for a spot. */
  ids: readonly string[];
  /** Where it is drawn: the spot, or the middle of the group. */
  lat: number; lon: number;
  count: number;
  /** The spot's name, drawn above it; null further out, for a group, or where it would cross another. */
  label: string | null;
  /** It holds the selected spot (the one last opened or shown). */
  selected: boolean;
  /** A group: the zoom at which it parts; null for a spot, and for a group no zoom parts (spots on one point). */
  splitZoom: number | null;
  a11y: string;
}

/** The face a mark is drawn at, px: the selected one larger. The ONE size — names are placed against it here, and
 *  `SpotPin` draws it. */
export function spotFace(m: Pick<SpotMarker, "selected">): number {
  return m.selected ? SPOT_PIN.selectedFace : SPOT_PIN.face;
}

/** Web Mercator pixels at `zoom`, 512 px tiles. */
function px(p: { lat: number; lon: number }, zoom: number): [number, number] {
  const world = 512 * 2 ** zoom;
  const s = Math.sin((p.lat * Math.PI) / 180);
  return [((p.lon + 180) / 360) * world, (0.5 - Math.log((1 + s) / (1 - s)) / (4 * Math.PI)) * world];
}
const dist = (a: [number, number], b: [number, number]) => Math.hypot(a[0] - b[0], a[1] - b[1]);
const byId = (a: SpotPoint, b: SpotPoint) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0);

/** A name's box, px: about 7 px a character at the label's 12 px bold, plus its padding, at most `labelMaxWidth`. */
function labelWidth(title: string): number {
  return Math.min(SPOT_PIN.labelMaxWidth, title.length * 7 + 14);
}
const LABEL_HEIGHT = 22;
type Box = [x0: number, y0: number, x1: number, y1: number];
const crosses = (a: Box, b: Box) => a[0] < b[2] && b[0] < a[2] && a[1] < b[3] && b[1] < a[3];

/**
 * The marks at `zoom`. Grouping is greedy in id order from each group's FIRST member (its anchor), so it does not
 * depend on the order the spots arrive in, and a group's `splitZoom` — where its farthest member is `groupPx` from the
 * anchor, on the half zoom — really parts it there (distances only grow with zoom, so no anchor changes).
 */
export function spotMarkers(points: readonly SpotPoint[], zoom: number, selected: string | null): SpotMarker[] {
  const sorted = [...points].sort(byId);
  const groups: { anchor: [number, number]; members: SpotPoint[] }[] = [];
  for (const p of sorted) {
    const at = px(p, zoom);
    const g = groups.find((x) => dist(x.anchor, at) < SPOT_PIN.groupPx);
    if (g) g.members.push(p);
    else groups.push({ anchor: at, members: [p] });
  }
  const marks: SpotMarker[] = groups.map(({ anchor, members }) => {
    const ids = members.map((m) => m.id);
    const isSelected = selected !== null && ids.includes(selected);
    if (members.length === 1) {
      const p = members[0]!;
      return { key: `spot:${p.id}`, ids, lat: p.lat, lon: p.lon, label: null, selected: isSelected, count: 1,
               splitZoom: null, a11y: SPOT_PIN_WORDS.open(p.title) };
    }
    const far = Math.max(...members.map((m) => dist(anchor, px(m, zoom))));
    const parts = far > 0 ? Math.ceil((zoom + Math.log2(SPOT_PIN.groupPx / far)) * 2) / 2 : Infinity;
    const splitZoom = parts <= CAMERA_MAX_ZOOM ? parts : null;
    const mean = (k: "lat" | "lon") => members.reduce((s, m) => s + m[k], 0) / members.length;
    return { key: `group:${ids.join(",")}`, ids, lat: mean("lat"), lon: mean("lon"), label: null,
             selected: isSelected, count: members.length, splitZoom,
             a11y: splitZoom === null ? SPOT_PIN_WORDS.list(members.length) : SPOT_PIN_WORDS.zoom(members.length) };
  });
  if (zoom < SPOT_PIN.labelZoom) return marks;

  // NAMES, the selected first: each above its face, kept only where it crosses no name kept and no other face
  const faces: Box[] = marks.map((m) => {
    const [x, y] = px(m, zoom), half = spotFace(m) / 2;
    return [x - half, y - half, x + half, y + half];
  });
  const kept: Box[] = [];
  const order = marks.map((_, i) => i).sort((a, b) => Number(marks[b]!.selected) - Number(marks[a]!.selected) || a - b);
  for (const i of order) {
    const m = marks[i]!;
    if (m.count !== 1) continue;
    const title = sorted.find((p) => p.id === m.ids[0])!.title;
    const [x, y] = px(m, zoom);
    const w = labelWidth(title);
    const bottom = y - spotFace(m) / 2 - SPOT_PIN.labelGap;
    const box: Box = [x - w / 2, bottom - LABEL_HEIGHT, x + w / 2, bottom];
    if (kept.some((k) => crosses(k, box)) || faces.some((f, j) => j !== i && crosses(f, box))) continue;
    kept.push(box);
    marks[i] = { ...m, label: title };
  }
  return marks;
}

/** What a tap on a mark does. */
export type SpotPress =
  | { kind: "open"; id: string }
  | { kind: "zoom"; camera: { lat: number; lon: number; zoom: number } }
  | { kind: "list"; ids: readonly string[] };

export function spotPress(m: SpotMarker): SpotPress {
  if (m.count === 1) return { kind: "open", id: m.ids[0]! };
  return m.splitZoom === null ? { kind: "list", ids: m.ids }
    : { kind: "zoom", camera: { lat: m.lat, lon: m.lon, zoom: m.splitZoom } };
}
