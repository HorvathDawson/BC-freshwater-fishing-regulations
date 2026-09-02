/**
 * The Conditions tab's detail view: a back control and the panel.
 *
 * ALL of the answer lives in `ConditionsPanel`, and that is the point of this file being
 * this short. There used to be a second, fuller implementation of the same screen behind
 * the water sheet's Conditions face — two layouts, two vocabularies, and only one of them
 * had the chart controls. Both now render the same component.
 */
import { Pressable, Text, View } from "react-native";
import type { Parameter, RegsSource, SectionId } from "@app/data";
import type { TileEndpoints } from "@app/map";
import { ConditionsPanel } from "./ConditionsPanel";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function ConditionsScreen({ source, section, palette, onBack, tiles, theme,
                                   parameter, onParameter, from }: {
  source: RegsSource; section: SectionId; palette: Palette; onBack: () => void;
  tiles?: TileEndpoints; theme?: string;
  /** Kept in step with the map's own flow/level switch — see ConditionsPanel. */
  parameter?: Parameter; onParameter?: (p: Parameter) => void;
  /** Where the tap landed, for the route map's "you are here". */
  from?: { lat: number; lon: number } | null;
}) {
  return (
    <View style={{ flex: 1, backgroundColor: palette.card }}>
      <View style={{ paddingHorizontal: 18, paddingTop: 14 }}>
        <Pressable onPress={onBack} accessibilityRole="button" accessibilityLabel="Back">
          <Text style={{ ...TYPE.micro, fontSize: 13, color: palette.accent }}>
            ‹  Conditions
          </Text>
        </Pressable>
      </View>
      <ConditionsPanel source={source} section={section} palette={palette}
                       tiles={tiles} theme={theme}
                       parameter={parameter} onParameter={onParameter} from={from} />
    </View>
  );
}
