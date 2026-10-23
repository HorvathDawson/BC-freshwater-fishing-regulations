/**
 * THE APP'S ONE ICON SET (ticket N1; owner 2026-10-22, decisions.md "icons: Lucide, rounded 1.75"): every chrome glyph
 * — the bottom bar, the map rail, search, back, close, the calendar, the Map view's parts list (once the regs header's Parts), a fold's open / closed mark — is drawn HERE and
 * nowhere else (`tools/one-icon-set.test.ts` refuses a hand-drawn one beside it).
 *
 * The drawings are Lucide's, copied verbatim from lucide-static@0.460.0 (`ICON_SET` in `@app/core`, ISC licence; the
 * notice is on the menu's Data sources page and in `app/LICENSES/lucide.txt`). Copied, not installed: thirteen icons are
 * not worth a runtime dependency (deps.md, "minimal prod packages"). Drawn as Lucide draws them — a 24 px grid, round
 * caps and joins, no fill — at the owner's 1.75 stroke rather than Lucide's 2, which sits with the bar's 9.5 px labels.
 *
 * The app's name for each icon is what it MEANS here; `LUCIDE_NAME` says which Lucide icon draws it, so a re-copy from a
 * newer release knows what to fetch. Conditions is `waves` (the audit: `activity` reads as a heartbeat at 20 px), More
 * is `ellipsis` (a hamburger promises a drawer, and More is a tab), Spots is `map-pin`.
 *
 * react-native-svg draws them on the web and on a phone alike; the colour is always a palette token, passed in.
 */
import { Text, View, type TextStyle } from "react-native";
import Svg, { Circle, Path, Rect } from "react-native-svg";

export type IconName =
  | "map" | "conditions" | "spots" | "more" | "layers"
  | "search" | "back" | "next" | "close" | "calendar" | "parts" | "expand" | "collapse" | "sync";

/** Which Lucide icon (lucide-static@0.460.0, `icons/<name>.svg`) each app icon is a copy of. */
export const LUCIDE_NAME: Readonly<Record<IconName, string>> = {
  map: "map",
  conditions: "waves",
  spots: "map-pin",
  more: "ellipsis",
  layers: "layers",
  search: "search",
  back: "chevron-left",
  next: "chevron-right",
  close: "x",
  calendar: "calendar",
  parts: "list",
  expand: "chevron-down",
  collapse: "chevron-up",
  sync: "refresh-cw",
};

/** The owner's stroke (Lucide ships 2): round caps and joins on the 24 grid. */
export const ICON_STROKE = 1.75;

type Shape =
  | { readonly path: string }
  | { readonly circle: readonly [cx: number, cy: number, r: number] }
  | { readonly rect: readonly [x: number, y: number, w: number, h: number, rx: number] };

/** THE TABLE: the only path data for a chrome glyph in the app. Each entry is its Lucide file's elements, in order. */
export const ICONS: Readonly<Record<IconName, readonly Shape[]>> = {
  map: [
    { path: "M14.106 5.553a2 2 0 0 0 1.788 0l3.659-1.83A1 1 0 0 1 21 4.619v12.764a1 1 0 0 1-.553.894l-4.553 2.277a2 2 0 0 1-1.788 0l-4.212-2.106a2 2 0 0 0-1.788 0l-3.659 1.83A1 1 0 0 1 3 19.381V6.618a1 1 0 0 1 .553-.894l4.553-2.277a2 2 0 0 1 1.788 0z" },
    { path: "M15 5.764v15" },
    { path: "M9 3.236v15" },
  ],
  conditions: [
    { path: "M2 6c.6.5 1.2 1 2.5 1C7 7 7 5 9.5 5c2.6 0 2.4 2 5 2 2.5 0 2.5-2 5-2 1.3 0 1.9.5 2.5 1" },
    { path: "M2 12c.6.5 1.2 1 2.5 1 2.5 0 2.5-2 5-2 2.6 0 2.4 2 5 2 2.5 0 2.5-2 5-2 1.3 0 1.9.5 2.5 1" },
    { path: "M2 18c.6.5 1.2 1 2.5 1 2.5 0 2.5-2 5-2 2.6 0 2.4 2 5 2 2.5 0 2.5-2 5-2 1.3 0 1.9.5 2.5 1" },
  ],
  spots: [
    { path: "M20 10c0 4.993-5.539 10.193-7.399 11.799a1 1 0 0 1-1.202 0C9.539 20.193 4 14.993 4 10a8 8 0 0 1 16 0" },
    { circle: [12, 10, 3] },
  ],
  more: [
    { circle: [12, 12, 1] },
    { circle: [19, 12, 1] },
    { circle: [5, 12, 1] },
  ],
  layers: [
    { path: "m12.83 2.18a2 2 0 0 0-1.66 0L2.6 6.08a1 1 0 0 0 0 1.83l8.58 3.91a2 2 0 0 0 1.66 0l8.58-3.9a1 1 0 0 0 0-1.83Z" },
    { path: "m22 17.65-9.17 4.16a2 2 0 0 1-1.66 0L2 17.65" },
    { path: "m22 12.65-9.17 4.16a2 2 0 0 1-1.66 0L2 12.65" },
  ],
  search: [
    { circle: [11, 11, 8] },
    { path: "m21 21-4.3-4.3" },
  ],
  back: [{ path: "m15 18-6-6 6-6" }],
  next: [{ path: "m9 18 6-6-6-6" }],
  close: [
    { path: "M18 6 6 18" },
    { path: "m6 6 12 12" },
  ],
  sync: [
    { path: "M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8" },
    { path: "M21 3v5h-5" },
    { path: "M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16" },
    { path: "M8 16H3v5" },
  ],
  calendar: [
    { path: "M8 2v4" },
    { path: "M16 2v4" },
    { rect: [3, 4, 18, 18, 2] },
    { path: "M3 10h18" },
  ],
  parts: [
    { path: "M3 12h.01" },
    { path: "M3 18h.01" },
    { path: "M3 6h.01" },
    { path: "M8 12h13" },
    { path: "M8 18h13" },
    { path: "M8 6h13" },
  ],
  expand: [{ path: "m6 9 6 6 6-6" }],
  collapse: [{ path: "m18 15-6-6-6 6" }],
};

/** One icon from the table, `size` px square, stroked in `colour` (a palette token). Decorative: its button speaks. */
export function Icon({ name, colour, size = 24 }: { name: IconName; colour: string; size?: number }) {
  const line = { stroke: colour, strokeWidth: ICON_STROKE, strokeLinecap: "round", strokeLinejoin: "round", fill: "none" } as const;
  return (
    <View testID={`icon-${name}`} aria-hidden importantForAccessibility="no-hide-descendants"
          style={{ width: size, height: size }}>
      <Svg width={size} height={size} viewBox="0 0 24 24" fill="none">
        {ICONS[name].map((s, i) =>
          "path" in s ? <Path key={i} d={s.path} {...line} />
          : "circle" in s ? <Circle key={i} cx={s.circle[0]} cy={s.circle[1]} r={s.circle[2]} {...line} />
          : <Rect key={i} x={s.rect[0]} y={s.rect[1]} width={s.rect[2]} height={s.rect[3]} rx={s.rect[4]} {...line} />)}
      </Svg>
    </View>
  );
}

/**
 * An icon before its words, on one line — the way back ("‹ Map"), "‹ All results": the chevron is the set's, not a
 * typed "‹". `text` is drawn in `style`; the icon takes the text's colour and sits a hair under its line height.
 */
export function IconText({ name, text, style, size = 16, gap = 4 }: {
  name: IconName; text: string; style: TextStyle & { color: string }; size?: number; gap?: number;
}) {
  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap }}>
      <View style={{ marginLeft: -3 }}><Icon name={name} colour={style.color} size={size} /></View>
      <Text style={style}>{text}</Text>
    </View>
  );
}
