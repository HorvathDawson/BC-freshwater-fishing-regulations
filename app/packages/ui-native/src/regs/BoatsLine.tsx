/**
 * T209 — the vessel restrictions, one row per kind on this section ("Electric motors only", "No powered boats", "Speed
 * limit 8 km/h", "No towing"), each opening its rule's sheet. (T395, owner 2026-11-05) Drawn in the Vessel restrictions
 * section under Methods (`MethodsTab`), no longer as chips at the top of the page. Nothing is drawn for a water with no
 * vessel rule. No logic: it draws `page.boats`.
 */
import { View } from "react-native";
import { Text } from "../Text";
import type { RegsPage, SheetKey } from "@app/core";
import { Icon } from "../icons";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { RegsPress } from "./RegsPress";

export function BoatsLine({ boats, palette, onSheet }: { boats: RegsPage["boats"]; palette: Palette; onSheet: (key: SheetKey) => void }) {
  if (!boats?.length) return null;   // a hand-built page (the golden walk) may carry none
  return (
    <View testID="regs-boats">
      {boats.map((b) => (
        <RegsPress key={`${b.kind}:${b.rule}`} testID={`boat:${b.kind}`} accessibilityRole="button" onPress={() => onSheet(`rule:${b.rule}`)}
                   box={{ w: 44, h: 40 }}
                   style={{ minHeight: 40, flexDirection: "row", alignItems: "center", gap: 6, paddingVertical: 8,
                            borderBottomWidth: 1, borderBottomColor: palette.line2 }}>
          <Text style={[TYPE.bodyStrong, { color: palette.ink, flex: 1 }]}>{b.text}</Text>
          <Icon name="next" colour={palette.sub} size={14} />
        </RegsPress>
      ))}
    </View>
  );
}
