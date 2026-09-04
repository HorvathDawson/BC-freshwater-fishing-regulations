/**
 * The tab icons, as paths. Four shapes on a 24 grid at stroke width 2, MITRED AND BUTTED.
 *
 * Hand-drawn rather than pulled from an icon set: four icons is not worth a dependency
 * (deps.md exists because v1 grew chart.js, pdf-lib, pdfjs, fuse and suncalc without anyone
 * deciding to), and an icon font cannot be tinted per-state the way these can.
 *
 * TWO THINGS CHANGED HERE, and they are the same change.
 *
 * The first is the drawing style. These were round-capped and round-joined at 1.75, which
 * is the house style of every icon set shipped in the last decade — soft, friendly, and at
 * odds with an app whose corners are cut square and whose shadows are offset slabs. Butt
 * caps and mitre joins at 2.0 make a stroke end in a flat edge instead of a lozenge, which
 * is what makes a line read as drawn rather than extruded.
 *
 * The second is WHAT they draw. They were the four most generic shapes available — a folded
 * tourist map, a magnifying glass, three ripples, a teardrop pin — none of which says
 * anything about this app. A fishing regulations map for British Columbia is about rivers,
 * their confluences, and how much water is in them against the record. So:
 *
 *   map         a confluence inside a frame — two streams joining, which is the unit of
 *               geography this whole atlas is built on, not a paper roadmap
 *   search      a SQUARE lens. A round lens over square chrome is the one circle on the
 *               screen, and it looked like it had wandered in from another app
 *   conditions  a hydrograph: a baseline, a plotted trace and the reading marked on it.
 *               Literally the picture the Conditions screen draws, rather than "water"
 *   spots       a square-headed pin over its own ground mark. A pin is a place you stood;
 *               the teardrop is the Google Maps glyph and reads as somebody else's product
 */
import Svg, { Path, Rect, Line } from "react-native-svg";

export type IconName = "map" | "search" | "conditions" | "spots";

/**
 * Two weights of the same drawing.
 *
 * `frame` is the outline — the container or the ground. `mark` is the thing inside it that
 * carries the meaning, drawn a little heavier so the icon still reads at 22px when the two
 * are the same colour.
 */
const SHAPES: Record<IconName, { frame: string[]; mark: string[] }> = {
  map: {
    frame: ["M3 3.5H21V20.5H3Z"],
    // A tributary meeting a mainstem, then leaving the frame at the bottom: the join is the
    // point. Straight segments with hard angles, the way a schematic draws a channel.
    mark: ["M8 3.5V9L12.5 13.5V20.5", "M17.5 3.5V7L12.5 13.5"],
  },
  search: {
    frame: ["M4 4.5H16V16.5H4Z"],
    mark: ["M16.5 17L21 21.5"],
  },
  conditions: {
    // The axis: a baseline and one tick, so the trace has something to be measured against.
    frame: ["M3.5 19.5H21", "M3.5 19.5V5"],
    // The trace, and the reading standing on it.
    mark: ["M3.5 15L8 10.5L12 14L16 6.5L21 11", "M16 6.5V19.5"],
  },
  spots: {
    frame: ["M7 3.5H17V12.5H7Z"],
    mark: ["M12 12.5V20.5", "M7.5 20.5H16.5"],
  },
};

export function Icon({ name, colour, size = 22 }:
  { name: IconName; colour: string; size?: number }) {
  const { frame, mark } = SHAPES[name];
  return (
    <Svg width={size} height={size} viewBox="0 0 24 24" fill="none">
      {frame.map((d, i) => (
        <Path key={`f${i}`} d={d} stroke={colour} strokeWidth={1.8}
              strokeLinecap="butt" strokeLinejoin="miter" />
      ))}
      {mark.map((d, i) => (
        <Path key={`m${i}`} d={d} stroke={colour} strokeWidth={2.2}
              strokeLinecap="butt" strokeLinejoin="miter" />
      ))}
    </Svg>
  );
}

/** The layers stack, for the map's own control. Three flat plates, seen edge-on. */
export function LayersIcon({ colour, size = 16 }: { colour: string; size?: number }) {
  return (
    <Svg width={size} height={size} viewBox="0 0 24 24" fill="none">
      {/* Squared off to match the tab icons: the diamond stack every mapping library ships
          is the same borrowed glyph problem as the teardrop pin. */}
      <Rect x={3} y={3.5} width={18} height={5} stroke={colour} strokeWidth={2}
            strokeLinejoin="miter" />
      <Line x1={3} y1={12} x2={21} y2={12} stroke={colour} strokeWidth={2} />
      <Line x1={3} y1={16.5} x2={21} y2={16.5} stroke={colour} strokeWidth={2} />
      <Line x1={3} y1={21} x2={21} y2={21} stroke={colour} strokeWidth={2} />
    </Svg>
  );
}
