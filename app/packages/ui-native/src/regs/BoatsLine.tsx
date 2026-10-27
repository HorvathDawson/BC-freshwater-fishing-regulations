/**
 * T209 — the boats line near the top of the page: one chip per kind of vessel restriction on this section ("Electric
 * motors only", "No powered boats", "Speed limit 8 km/h", "No towing"), each opening its rule's sheet. Nothing is drawn
 * for a water with no vessel rule. No logic: it draws `page.boats`.
 */
import { Text, View } from "react-native";
import type { RegsPage, SheetKey } from "@app/core";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { WaterInset } from "../WaterFrame";
import { RegsPress } from "./RegsPress";

export function BoatsLine({ boats, palette, onSheet }: { boats: RegsPage["boats"]; palette: Palette; onSheet: (key: SheetKey) => void }) {
  if (!boats?.length) return null;   // a hand-built page (the golden walk) may carry none
  return (
    <WaterInset testID="regs-boats" style={{ paddingTop: 10, flexDirection: "row", flexWrap: "wrap", gap: 6 }}>
      {boats.map((b) => (
        <RegsPress key={b.kind} testID={`boat:${b.kind}`} accessibilityRole="button" onPress={() => onSheet(`rule:${b.rule}`)} box={{ w: 44, h: 32 }}
                   style={{ minHeight: 32, justifyContent: "center" }}>
          <View style={{ backgroundColor: palette.tint, paddingVertical: 4, paddingHorizontal: 10 }}>
            <Text style={[TYPE.metaBold, { color: palette.ink }]}>{b.text}</Text>
          </View>
        </RegsPress>
      ))}
    </WaterInset>
  );
}
