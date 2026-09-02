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

export function Hydrograph({ shape, palette, colour, label }:
  { shape: Shape; palette: Palette; colour: string; label?: string }) {
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
      <Text style={{ fontSize: 11.5, color: palette.sub, marginTop: 6 }}>
        Shaded is the middle half of everything this station has recorded for these days.
      </Text>
    </View>
  );
}
