/**
 * HOW THIS SPOT REACHES THE GAUGE. Drawn to `design/riffle.html`.
 *
 * ONE component, TWO sources — which is the whole point of it existing.
 *
 *   Conditions   asks live: `useGaugeTrace(source, section)`
 *   a saved spot replays the trace frozen the day it was pinned
 *
 * Neither knows about the other and neither renders its own version. A gauge's
 * representativeness is the single most misreadable number in this app — the Fraser at
 * Hope drains 216,600 km² and knows nothing about a creek above Chilliwack — so the
 * sentence explaining it must be the same sentence everywhere. The words come from
 * `@app/core/trace`; this file only lays them out.
 */
import { Text, View } from "react-native";
import { distanceWord, onTheReach, shareWord, traceSentence, trustWord,
         type GaugeTrace as Trace } from "@app/core";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function GaugeTrace({ trace, palette, title = "How this spot reaches the gauge" }:
  { trace: Trace; palette: Palette; title?: string }) {
  const facts: [string, string | null][] = [
    ["distance", onTheReach(trace) ? "on the reach" : distanceWord(trace.metres)],
    ["this reach", trace.reachMagnitude === null ? null : `magnitude ${trace.reachMagnitude}`],
    ["at the gauge", trace.gaugeMagnitude === null ? null : `magnitude ${trace.gaugeMagnitude}`],
    ["share", shareWord(trace)],
    ["so it shows", trace.trust === null ? null : trustWord(trace.trust)],
  ];

  return (
    <View style={{ gap: 12 }}>
      <View style={{ flexDirection: "row", alignItems: "baseline" }}>
        <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.6,
                       color: palette.faint }}>{title.toUpperCase()}</Text>
        {trace.path.length > 0 && (
          <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.6,
                         color: palette.faint, marginLeft: "auto" }}>
            {trace.path.length} {trace.path.length === 1 ? "REACH" : "REACHES"}
          </Text>
        )}
      </View>

      <View>
        {facts.map(([label, value], i) => (
          <View key={label}
                style={{ flexDirection: "row", justifyContent: "space-between",
                         alignItems: "baseline", paddingVertical: 10, gap: 16,
                         borderTopWidth: i === 0 ? 0 : 1, borderTopColor: palette.line }}>
            <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.2,
                           color: palette.faint }}>{label.toUpperCase()}</Text>
            {/* A missing value says so. An empty cell reads as zero, and "0% of the
                gauge's watershed" is a very different claim from "we have not worked
                it out". */}
            <Text style={{ ...TYPE.figure, fontSize: 12,
                           color: value === null ? palette.faint : palette.sub,
                           textAlign: "right", flexShrink: 1 }}>
              {value ?? "not known"}
            </Text>
          </View>
        ))}
      </View>

      <Text style={{ ...TYPE.small, fontSize: 11.5, lineHeight: 17, color: palette.faint }}>
        {traceSentence(trace)}
      </Text>
    </View>
  );
}
