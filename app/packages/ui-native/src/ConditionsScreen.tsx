/**
 * The Conditions tab's detail view: a back control and the panel.
 *
 * ALL of the answer lives in `ConditionsPanel`, and that is the point of this file being
 * this short. There used to be a second, fuller implementation of the same screen behind
 * the water sheet's Conditions face — two layouts, two vocabularies, and only one of them
 * had the chart controls. Both now render the same component.
 */
import { Text, View } from "react-native";
import type { Parameter, RegsSource, SectionId } from "@app/data";
import type { TileEndpoints } from "@app/map";
import { useWaterName } from "@app/ui";
import { ConditionsPanel } from "./ConditionsPanel";
import { FaceBar } from "./Faces";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function ConditionsScreen({ source, section, palette, onBack, tiles, theme,
                                   parameter, onParameter, from, onRegulations, feed,
                                   credits }: {
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
  /** The live index, for the donor panel — see ConditionsPanel. */
  feed?: { index(): Promise<any> };
  /** Data credits for the foot of the screen — see ConditionsPanel. */
  credits?: readonly string[];
}) {
  // THE WATER'S NAME, in the same place the Regulations face puts it. Without it this
  // screen opened on a chart and a river's worth of numbers with nothing saying which
  // river — and the two faces of one water looked like two different screens.
  const water = useWaterName(source, section);
  const name = water.state === "ready" ? water.value?.name ?? null : null;

  return (
    <View style={{ flex: 1, backgroundColor: palette.card }}>
      <View style={{ paddingHorizontal: 18, paddingTop: 14, gap: 10 }}>
        <FaceBar palette={palette} face="conditions" onBack={onBack} backLabel="Conditions"
                 onFace={(f) => { if (f === "regulations") onRegulations?.(section); }} />
        {/* Rendered only once it is known. A placeholder here would be a title that
            changes after the reader has started reading it. */}
        {name && <Text style={{ ...TYPE.title, color: palette.ink }}>{name}</Text>}
      </View>
      <ConditionsPanel source={source} section={section} palette={palette}
                       tiles={tiles} theme={theme} feed={feed}
                       parameter={parameter} onParameter={onParameter} from={from}
                       credits={credits} />
    </View>
  );
}
