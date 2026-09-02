/**
 * Draws a hydrograph. Computes nothing.
 *
 * Every number here arrived from `@app/ui`'s `buildHydrograph`. That is what makes the
 * desktop chart free: it is a different renderer over identical arithmetic, so the two
 * cannot disagree about what the water did.
 */
import type { Hydrograph as Shape } from "@app/ui";
import { Text, View } from "react-native";
import Svg, { G, Line, Path, Circle, Text as SvgText } from "react-native-svg";
import type { Palette } from "./theme";

export function Hydrograph({ shape, palette, colour, label, caption, unit }:
  { shape: Shape; palette: Palette; colour: string; label?: string;
    /** What this chart is actually showing. Never a fixed sentence — see below. */
    caption?: string; unit?: string }) {
  const { box } = shape;
  return (
    <View accessibilityRole="image"
          accessibilityLabel={label ?? "River flow against its normal range"}>
      <Svg width="100%" height={box.height} viewBox={`0 0 ${box.width} ${box.height}`}>
        {/* gridlines first, so nothing draws on top of the data */}
        <G>
          {shape.yTicks.map((t) => (
            <Line key={`g${t.value}`} x1={box.padLeft} y1={t.at}
                  x2={box.width - box.padRight} y2={t.at}
                  stroke={palette.line} strokeWidth={1} />
          ))}
        </G>
        {/* widest envelope first: min-max, then 10th-90th, then the middle half */}
        {shape.envelopes.map((e, i) => (
          <Path key={`e${i}`} d={e.d} fill={palette.ink}
                fillOpacity={0.06 + i * 0.05} />
        ))}
        {shape.median !== "" && (
          <Path d={shape.median} fill="none" stroke={palette.sub}
                strokeWidth={1.2} strokeDasharray="5,4" />
        )}
        {shape.line !== "" && (
          <Path d={shape.line} fill="none" stroke={colour} strokeWidth={2.4}
                strokeLinejoin="round" strokeLinecap="round" />
        )}
        {/* THE FORECAST, VISIBLY NOT A MEASUREMENT. It sits past the boundary rule, its
            band is fainter than the record's, and its line is dashed — three cues, because
            one is a legend nobody read. A model run drawn in the same stroke as an
            observation is the worst thing this chart could do. */}
        {shape.forecast && (
          <G>
            <Line x1={shape.forecast.at} y1={box.padTop}
                  x2={shape.forecast.at} y2={box.height - box.padBottom}
                  stroke={palette.line} strokeWidth={1} strokeDasharray="2,3" />
            {shape.forecast.band !== "" && (
              <Path d={shape.forecast.band} fill={colour} fillOpacity={0.13} />
            )}
            <Path d={shape.forecast.line} fill="none" stroke={colour} strokeWidth={1.8}
                  strokeDasharray="4,3" strokeLinecap="round" />
          </G>
        )}
        {shape.now && (
          <Circle cx={shape.now.x} cy={shape.now.y} r={4}
                  fill={colour} stroke={palette.card} strokeWidth={2} />
        )}
        <G>
          {shape.yTicks.map((t) => (
            <SvgText key={`y${t.value}`} x={box.padLeft - 6} y={t.at + 3.5}
                     fontSize={9.5} fill={palette.faint} textAnchor="end">
              {t.label}
            </SvgText>
          ))}
          {shape.xTicks.map((t) => (
            <SvgText key={`x${t.value}`} x={t.at} y={box.height - 7}
                     fontSize={9.5} fill={palette.faint} textAnchor="middle">
              {t.label}
            </SvgText>
          ))}
        </G>
      </Svg>
      {/* THE CAPTION IS PART OF THE CHART. It used to be one fixed sentence about the
          middle half, printed under a chart that might have had no envelope at all — so a
          station with too thin a record to build one was captioned as though it had. What
          is on screen decides what this says. */}
      <Text style={{ fontSize: 11.5, color: palette.sub, marginTop: 6 }}>
        {caption ?? (shape.envelopes.length
          ? "Shaded is the middle half of everything this station has recorded for these " +
            "days; the dashed line is the median."
          : "No envelope: this station's record is too short to say what is normal here.")}
        {shape.forecast
          ? " Past the dotted rule is a forecast, not a reading."
          : ""}
        {unit ? ` Values in ${unit}.` : ""}
      </Text>
    </View>
  );
}
