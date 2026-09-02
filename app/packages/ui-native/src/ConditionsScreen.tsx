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
import { Faces } from "./Faces";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function ConditionsScreen({ source, section, palette, onBack, tiles, theme,
                                   parameter, onParameter, from, onRegulations }: {
  source: RegsSource; section: SectionId; palette: Palette; onBack: () => void;
  tiles?: TileEndpoints; theme?: string;
  /** Kept in step with the map's own flow/level switch — see ConditionsPanel. */
  parameter?: Parameter; onParameter?: (p: Parameter) => void;
  /** Where the tap landed, for the route map's "you are here". */
  from?: { lat: number; lon: number } | null;
  /**
   * Leave for the rules about this REACH — by section, not by item.
   *
   * The screen does not resolve which water this is, and that is deliberate: the lookup is
   * asynchronous, so a screen holding it has a button that silently does nothing for the
   * first few hundred milliseconds after it appears. The shell already owns exactly this
   * resolution for a map tap; asking it to do the same thing here means one implementation
   * and a control that is never dead.
   */
  onRegulations?: (section: SectionId) => void;
}) {
  return (
    <View style={{ flex: 1, backgroundColor: palette.card }}>
      <View style={{ paddingHorizontal: 18, paddingTop: 14, gap: 12 }}>
        <Pressable onPress={onBack} accessibilityRole="button" accessibilityLabel="Back">
          <Text style={{ ...TYPE.micro, fontSize: 13, color: palette.accent }}>
            ‹  Conditions
          </Text>
        </Pressable>
        {/* The same two buttons the rules sheet shows, and the same behaviour: pressing the
            other one LEAVES. Two surfaces answering one question is what this replaced. */}
        <Faces palette={palette} face="conditions"
               onFace={(f) => { if (f === "regulations") onRegulations?.(section); }} />
      </View>
      <ConditionsPanel source={source} section={section} palette={palette}
                       tiles={tiles} theme={theme}
                       parameter={parameter} onParameter={onParameter} from={from} />
    </View>
  );
}
