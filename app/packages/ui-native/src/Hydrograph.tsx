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
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function Hydrograph({ shape, palette, colour, label, caption, unit,
                             disclaimer, forecastLabel }:
  { shape: Shape; palette: Palette; colour: string; label?: string;
    /** What this chart is actually showing. Never a fixed sentence — see below. */
    caption?: string; unit?: string;
    /** The forecast provider's own disclaimer, verbatim. Shown only with a forecast. */
    disclaimer?: string | null;
    /** e.g. "ELF · 30 days" — which model is drawing the dashes. */
    forecastLabel?: string }) {
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
        {/* EARLIER YEARS, thin and quiet. Each is a real year of record, so they are lines
            rather than another band — a reader comparing this summer with last one needs to
            see a year, not an average of years. Faded by age so the most recent reads first. */}
        {shape.priorYears.map((y, i) => (
          <Path key={y.year} d={y.d} fill="none" stroke={palette.sub}
                strokeWidth={1.1} strokeOpacity={0.65 - i * 0.2}
                strokeLinejoin="round" strokeLinecap="round" />
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
        {/* TODAY, drawn whether or not a forecast follows it. On the seasonal chart the
            record simply stops here and the rest of the frame is band; without the rule a
            reader cannot tell where the measuring ended and the season's shape took over. */}
        {shape.todayX !== null && (
          <G>
            <Line x1={shape.todayX} y1={box.padTop}
                  x2={shape.todayX} y2={box.height - box.padBottom}
                  stroke={palette.sub} strokeWidth={1} strokeDasharray="2,3" />
            <SvgText x={shape.todayX + 3} y={box.padTop + 8}
                     fontSize={8.5} fill={palette.faint}>TODAY</SvgText>
          </G>
        )}
        {shape.forecast && (
          <G>
            {shape.forecast.band !== "" && (
              <Path d={shape.forecast.band} fill={colour} fillOpacity={0.13} />
            )}
            <Path d={shape.forecast.line} fill="none" stroke={colour} strokeWidth={1.8}
                  strokeDasharray="4,3" strokeLinecap="round" strokeLinejoin="round" />
            {shape.forecast.end && (
              <Circle cx={shape.forecast.end.x} cy={shape.forecast.end.y} r={2.6}
                      fill={palette.card} stroke={colour} strokeWidth={1.6} />
            )}
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
      {/* A KEY, because four things share one frame and only one of them is a measurement.
          Drawn from the same palette and the same `colour` the paths use, so a swatch
          cannot end up describing a line of a different shade. */}
      <View style={{ flexDirection: "row", flexWrap: "wrap", columnGap: 14, rowGap: 5,
                     marginTop: 8 }}>
        <Key palette={palette} label="reading" swatch="line" colour={colour} />
        {shape.envelopes.length > 0 && (
          <>
            <Key palette={palette} label="middle half" swatch="fill" colour={palette.ink} />
            <Key palette={palette} label="10th–90th" swatch="faintfill"
                 colour={palette.ink} />
            <Key palette={palette} label="median" swatch="dash" colour={palette.sub} />
          </>
        )}
        {shape.priorYears.map((y) => (
          <Key key={y.year} palette={palette} label={String(y.year)} swatch="line"
               colour={palette.sub} />
        ))}
        {shape.forecast && (
          <Key palette={palette} label={forecastLabel ?? "forecast"} swatch="dash"
               colour={colour} />
        )}
      </View>

      {/* THE CAPTION IS PART OF THE CHART. It used to be one fixed sentence about the
          middle half, printed under a chart that might have had no envelope at all — so a
          station with too thin a record to build one was captioned as though it had. What
          is on screen decides what this says. */}
      <Text style={{ fontSize: 11.5, color: palette.sub, marginTop: 6, lineHeight: 17 }}>
        {caption ?? (shape.envelopes.length
          ? "Shaded is the middle half of everything this station has recorded for these " +
            "days; the dashed line is the median."
          : "No envelope: this station's record is too short to say what is normal here.")}
        {shape.forecast
          ? " Past TODAY is a model forecast, not a reading."
          : ""}
        {unit ? ` Values in ${unit}.` : ""}
      </Text>

      {/* THE CENTRE'S OWN WORDS, verbatim, out of the CSV header — required wherever a
          forecast appears and not ours to paraphrase. Shown only when one is on screen. */}
      {shape.forecast && disclaimer && (
        <Text style={{ fontSize: 10.5, color: palette.faint, marginTop: 6, lineHeight: 15 }}>
          {disclaimer}
        </Text>
      )}
    </View>
  );
}


/**
 * One entry in the chart's key.
 *
 * A LEGEND, not a caption, because the frame carries four different kinds of mark and three
 * of them are shades of the same colour. Without it "shaded" and "faintly shaded" are two
 * things a reader has to guess the difference between — and the difference is the middle
 * half of the record against the tenth-to-ninetieth, which is the whole point of drawing
 * both.
 */
function Key({ palette, label, swatch, colour }: {
  palette: Palette; label: string; colour: string;
  swatch: "line" | "dash" | "fill" | "faintfill";
}) {
  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: 5 }}>
      <View style={{ width: 16, height: 9, justifyContent: "center" }}>
        {swatch === "fill" || swatch === "faintfill" ? (
          <View style={{ height: 9, borderRadius: 2, backgroundColor: colour,
                         opacity: swatch === "fill" ? 0.11 : 0.06 }} />
        ) : (
          <View style={{ height: 0, borderTopWidth: 2, borderTopColor: colour,
                         borderStyle: swatch === "dash" ? "dashed" : "solid" }} />
        )}
      </View>
      <Text style={{ ...TYPE.micro, fontSize: 10, color: palette.faint }}>{label}</Text>
    </View>
  );
}
