/**
 * (T1194) A GAUGE'S OWN SCREEN, opened by a tap on a gauge dot of the Conditions map (or `?gauge=<station>`).
 *
 * *** A PLACEHOLDER, NOT THE FINAL PAGE. *** The owner (2026-11-26) asked for the website's gauge page, or one like it,
 * with no drift between the two: the final page is a PROPOSAL (tickets/T1194, `## Owner`), not built here. This screen
 * proves the tap, the route, the address and Back: the station's name from the release and its number, the line saying
 * so, and a Back of its own (>= 44 px). It shows no reading, no chart and no waters; it decides nothing.
 */
import { View } from "react-native";
import { GAUGE_PAGE_WORDS } from "@app/core";
import type { StationId, WaterSource } from "@app/data";
import { useAsync } from "@app/ui";
import { Pressable } from "./Pressable";
import { Text } from "./Text";
import { Icon } from "./icons";
import { ErrorState } from "./ErrorState";
import { hitProps } from "./regs/hit";
import { TYPE } from "./type";
import { GUTTER, SPACE, type Palette } from "./theme";

export function GaugeScreen({ station, source, palette, onBack }: {
  station: StationId; source: Pick<WaterSource, "gaugePoints">; palette: Palette; onBack: () => void;
}) {
  const got = useAsync(async () => (await source.gaugePoints()).find((g) => g.station === station)?.name ?? null, `gauge-name:${station}`);
  const name = got.state === "ready" ? got.value : null;
  return (
    <View testID="gauge-screen" style={{ flex: 1, backgroundColor: palette.card }}>
      <View style={{ flexDirection: "row", alignItems: "center", paddingHorizontal: GUTTER, paddingTop: SPACE.s3, minHeight: 56 }}>
        <Pressable testID="gauge-back" onPress={onBack} accessibilityRole="button" accessibilityLabel={GAUGE_PAGE_WORDS.back}
                   {...hitProps({ w: 44, h: 44 })}
                   style={{ minWidth: 44, minHeight: 44, flexDirection: "row", alignItems: "center", gap: SPACE.s1 }}>
          <Icon name="back" colour={palette.accent} size={20} />
          <Text numberOfLines={1} style={{ ...TYPE.bodyStrong, color: palette.accent }}>{GAUGE_PAGE_WORDS.back}</Text>
        </Pressable>
      </View>
      <View style={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s4, gap: SPACE.s3 }}>
        <Text testID="gauge-title" style={{ ...TYPE.title, color: palette.ink }}>{name ?? GAUGE_PAGE_WORDS.stationLine(station)}</Text>
        {name && <Text testID="gauge-station" style={{ ...TYPE.meta, color: palette.sub }}>{GAUGE_PAGE_WORDS.stationLine(station)}</Text>}
        {got.state === "ready" && got.value === null &&
          <ErrorState palette={palette} size="body" testID="gauge-unknown" message={GAUGE_PAGE_WORDS.unknown(station)} />}
        {got.state === "failed" && <ErrorState palette={palette} size="body" message={GAUGE_PAGE_WORDS.releaseFailed} />}
        <Text testID="gauge-placeholder" style={{ ...TYPE.body, color: palette.sub }}>{GAUGE_PAGE_WORDS.placeholder}</Text>
      </View>
    </View>
  );
}
