/**
 * The tab icons, as paths. Four shapes, drawn on a 24 grid at stroke width 1.75.
 *
 * Hand-drawn rather than pulled from an icon set: four icons is not worth a dependency
 * (deps.md exists because v1 grew chart.js, pdf-lib, pdfjs, fuse and suncalc without anyone
 * deciding to), and an icon font cannot be tinted per-state the way these can.
 */
import Svg, { Path, Circle } from "react-native-svg";

export type IconName = "map" | "search" | "conditions" | "spots";

const PATHS: Record<IconName, string[]> = {
  // a folded paper map: three panels, alternating fold direction
  map: ["M2.5 6.2 8.6 3.6v14.2L2.5 20.4Z", "M8.6 3.6 15.4 6.2v14.2L8.6 17.8Z",
        "M15.4 6.2 21.5 3.6v14.2L15.4 20.4Z"],
  search: ["M20.5 20.5 16.2 16.2"],
  // three stacked ripples — the conditions view is about water moving
  conditions: ["M2.6 8.4c2.6-2.5 5.1-2.5 7.7 0s5.1 2.5 7.7 0 3.4 0 3.4 0",
               "M2.6 13c2.6-2.5 5.1-2.5 7.7 0s5.1 2.5 7.7 0 3.4 0 3.4 0",
               "M2.6 17.6c2.6-2.5 5.1-2.5 7.7 0s5.1 2.5 7.7 0 3.4 0 3.4 0"],
  spots: ["M12 21.5s7-5.7 7-11a7 7 0 1 0-14 0c0 5.3 7 11 7 11Z"],
};

export function Icon({ name, colour, size = 23 }:
  { name: IconName; colour: string; size?: number }) {
  return (
    <Svg width={size} height={size} viewBox="0 0 24 24" fill="none">
      {PATHS[name].map((d, i) => (
        <Path key={i} d={d} stroke={colour} strokeWidth={1.75}
              strokeLinecap="round" strokeLinejoin="round" />
      ))}
      {name === "search" && (
        <Circle cx={10.8} cy={10.8} r={7.3} stroke={colour} strokeWidth={1.75} fill="none" />
      )}
      {name === "spots" && (
        <Circle cx={12} cy={10.5} r={2.6} stroke={colour} strokeWidth={1.75} fill="none" />
      )}
    </Svg>
  );
}

/** The layers stack, for the map's own control. */
export function LayersIcon({ colour, size = 18 }: { colour: string; size?: number }) {
  return (
    <Svg width={size} height={size} viewBox="0 0 24 24" fill="none">
      <Path d="M12 2.8 22 8l-10 5.2L2 8Z" stroke={colour} strokeWidth={1.9}
            strokeLinejoin="round" />
      <Path d="M2.6 13 12 17.9 21.4 13" stroke={colour} strokeWidth={1.9}
            strokeLinecap="round" strokeLinejoin="round" />
    </Svg>
  );
}


