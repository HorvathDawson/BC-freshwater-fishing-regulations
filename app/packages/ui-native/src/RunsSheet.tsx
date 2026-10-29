/**
 * (T199) A TAPPED SECTION UNDER RUN TIMING: which runs are here, how (spawning / passing / surveyed), their dates in their
 * source's terms, whether each is in the water on the map's day, and whose data it is. The words are `@app/core`
 * `runSheet`'s, whole; this draws them. A bottom sheet at every width, like the tidal and obstacle sheets. "Open this
 * water" continues to the regulations for the same section.
 */
import type { RunSheetVM } from "@app/core";
import { RUN_WORDS } from "@app/core";
import { Pressable, ScrollView, Text, View } from "react-native";
import { Sheet } from "./Sheet";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function RunsSheet({ vm, palette, onClose, onOpenWater }: {
  vm: RunSheetVM | null; palette: Palette; onClose: () => void; onOpenWater?: () => void;
}) {
  return (
    <Sheet open={vm !== null} onClose={onClose} title={vm?.title ?? ""} sub={null} palette={palette}>
      {vm && (
        <ScrollView testID="runs-sheet" style={{ maxHeight: 460 }}
                    contentContainerStyle={{ paddingHorizontal: 20, paddingTop: 12, paddingBottom: 16, gap: 12 }}>
          {vm.noneOfSpecies && <Text style={{ ...TYPE.body, color: palette.ink }}>{vm.noneOfSpecies}</Text>}
          {vm.rows.map((r) => (
            <View key={r.key} testID="runs-row" style={{ gap: 2, borderBottomWidth: 1, borderBottomColor: palette.line,
                                                         paddingBottom: 10 }}>
              <View style={{ flexDirection: "row", justifyContent: "space-between", gap: 8 }}>
                <Text style={{ ...TYPE.bodyStrong, color: palette.ink, flexShrink: 1 }}>{r.title}</Text>
                <Text style={{ ...TYPE.small, color: r.now ? palette.ink : palette.sub }}>{r.nowWords}</Text>
              </View>
              <Text style={{ ...TYPE.small, color: palette.sub }}>{r.population} · {r.relation}</Text>
              {r.dates && <Text style={{ ...TYPE.small, color: palette.ink }}>{r.dates}</Text>}
              {r.survey && <Text style={{ ...TYPE.small, color: palette.sub }}>{r.survey}</Text>}
            </View>
          ))}
          {vm.riverEntry && <Text style={{ ...TYPE.small, color: palette.sub }}>
            {vm.km ? `${vm.riverEntry} ${vm.km}` : vm.riverEntry}</Text>}
          <Text testID="runs-credit" style={{ ...TYPE.small, fontSize: 11.5, color: palette.sub }}>{vm.credit}</Text>
          {onOpenWater && (
            <Pressable accessibilityRole="button" onPress={onOpenWater} style={{ paddingVertical: 8 }}>
              <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{RUN_WORDS.openWater}</Text>
            </Pressable>
          )}
        </ScrollView>
      )}
    </Sheet>
  );
}
