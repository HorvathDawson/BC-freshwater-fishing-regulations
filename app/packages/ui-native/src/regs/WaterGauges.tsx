/**
 * (T1045) "RIVER GAUGES FOR THIS WATER": the gauges that read this water (core `pickWaterGauges`, three at most), each a link
 * to its page on the site. On the web it is a real anchor (`href`, react-native-web's Text): a crawler and a reader get the
 * same link, in the page they both see. On the phone a press opens the water's Conditions view, the app's own door to the gauges.
 * Nothing is drawn for a water with no good gauge; a failed read is said (and was reported by `useAsync`), never an empty list.
 */
import { Platform, View } from "react-native";
import { gaugePath, WATER_GAUGES_WORDS, waterGaugeLabel, type WaterGauge } from "@app/core";
import type { Async } from "@app/ui";
import { Text } from "../Text";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { MIN_HIT } from "./hit";

/** react-native-web's anchor props: react-native's types do not list them (native ignores them) */
const anchor = (href: string): Record<string, unknown> => (Platform.OS === "web" ? { href, hrefAttrs: { rel: "noopener" } } : {});

export function WaterGauges({ gauges, palette, onOpen }: { gauges: Async<readonly WaterGauge[]>; palette: Palette; onOpen?: () => void }) {
  if (gauges.state === "loading") return null;
  if (gauges.state === "failed") return <Text testID="regs-gauges-failed" style={{ ...TYPE.caption, color: palette.sub }}>{WATER_GAUGES_WORDS.failed}</Text>;
  if (gauges.value.length === 0) return null;
  return (
    <View testID="regs-gauges" style={{ marginTop: 16 }}>
      <Text accessibilityRole="header" style={{ ...TYPE.metaBold, color: palette.ink }}>{WATER_GAUGES_WORDS.heading}</Text>
      {gauges.value.map((g) => (
        <Text key={g.station} testID={`gauge-link:${g.station}`} accessibilityRole="link" onPress={Platform.OS === "web" ? undefined : onOpen}
              {...anchor(gaugePath(g.station, g.name))}
              style={{ ...TYPE.meta, color: palette.accent, minHeight: MIN_HIT, paddingVertical: 12 }}>
          {waterGaugeLabel(g)}
        </Text>
      ))}
    </View>
  );
}
