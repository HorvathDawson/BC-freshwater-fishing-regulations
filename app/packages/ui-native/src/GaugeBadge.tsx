/**
 * Whether this water is measured at all — the line that comes BEFORE any number.
 *
 * The app spent a long time being able to show a discharge and unable to say whether the
 * discharge was about this river. This is the other half: four states, each of which is a
 * different sentence, because collapsing them is how a reader ends up trusting the wrong
 * one.
 *
 *   live         a station is on this water and transmitting  -> a number is available
 *   record only  a station is on this water and has stopped   -> history, never "now"
 *   unchecked    a station is here; we could not reach the feed -> say THAT, not "stopped"
 *   distant      only a far branch is measured (weak trust)   -> a trend, not a level
 *   none         nothing drains enough of it to speak for it  -> say so, plainly
 *
 * The third exists because liveness now comes from the feed, so "offline" and "the station
 * shut down" arrive as the same absence. Collapsing them tells every reader without signal
 * that the province's gauges have all stopped.
 *
 * The fourth is the common case in BC and gets the same visual weight as the others. An
 * ungauged river must not look like a screen that failed to load.
 */
import { Text, View } from "react-native";
import type { GaugeLink } from "@app/data";
import { gaugeSentence } from "@app/core";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function GaugeBadge({ gauge, palette, waterName, record }: {
  gauge: GaugeLink | null;
  palette: Palette;
  /** Named in the sentence, because "this water" is vague on a screen full of water. */
  waterName?: string;
  /** Years of record behind this station's percentiles. Omitted when not known. */
  record?: { fromYear: number; toYear: number; years: number } | null;
}) {
  const water = waterName ?? "this water";

  if (!gauge) {
    return (
      <DotBlock palette={palette} dot={palette.faint} head="Not measured">
        <Text style={{ ...TYPE.small, color: palette.sub }}>
          No hydrometric station drains enough of {water} to speak for it. The nearest one
          would still give you a number.
        </Text>
      </DotBlock>
    );
  }

  const weak = gauge.trust === "weak";
  const head = gauge.live === false ? "Gauged — record only"
             : gauge.live === null ? "Gauged — not checked"
             : weak ? "Gauged nearby"
             : "Gauged";
  const dot = gauge.live !== true ? palette.faint
            : weak ? palette.restricted : palette.live;

  return (
    <DotBlock palette={palette} dot={dot} head={head}>
      <Text style={{ ...TYPE.small, color: palette.sub }}>
        <Text style={{ color: palette.ink }}>{gauge.name}</Text>
        {gauge.areaKm2 != null && ` · drains ${Math.round(gauge.areaKm2).toLocaleString()} km²`}
      </Text>
      {record && (
        // The weight behind any percentile this station reports. "4th percentile" backed
        // by 97 years and by 11 are different claims, and a reader who cannot see which is
        // being made will assume the stronger one.
        <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.faint }}>
          {record.years} years of record · {record.fromYear}–{record.toYear}
        </Text>
      )}
      {/* The WHOLE sentence comes from core. It used to be spliced here — `It ${word}.` —
          and the fragment only read after "It" for one of the three bands. */}
      <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.faint }}>
        {gaugeSentence(gauge.trust, gauge.live ?? null)}
      </Text>
    </DotBlock>
  );
}

/** A coloured dot, a heading, and whatever the state has to say under it. */
function DotBlock({ palette, dot, head, children }: {
  palette: Palette; dot: string; head: string; children: React.ReactNode;
}) {
  return (
    <View accessibilityLabel={head}
          style={{ flexDirection: "row", gap: 10, alignItems: "flex-start" }}>
      <View style={{ width: 8, height: 8, borderRadius: 4, marginTop: 6,
                     backgroundColor: dot }} />
      <View style={{ flex: 1, gap: 3 }}>
        <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.4,
                       color: palette.faint }}>{head.toUpperCase()}</Text>
        {children}
      </View>
    </View>
  );
}
