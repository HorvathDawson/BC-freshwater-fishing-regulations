/**
 * (T173) A TAPPED FISH OBSTACLE: what the mark is — its type or a dam's name, the water it is on, its height, when it was
 * surveyed or commissioned, and whose record it is. The words are `@app/core` `obstacleSheet`'s, whole; this draws them.
 * A bottom sheet at every width, like the other sheets that sit over the map: the mark stays visible above it.
 */
import { obstacleSheet } from "@app/core";
import { ObstacleIcon } from "./icons";
import { Text, View } from "react-native";
import { Sheet } from "./Sheet";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function ObstacleSheet({ props, palette, onClose }: {
  /** The tapped mark's tile properties; null: the sheet is down. */
  props: Readonly<Record<string, unknown>> | null; palette: Palette; onClose: () => void;
}) {
  const vm = props ? obstacleSheet(props) : null;
  return (
    <Sheet open={vm !== null} onClose={onClose} title={vm?.title ?? ""} sub={vm?.sub ?? null} palette={palette}
           icon={props ? <ObstacleIcon type={props.type} ink={palette.ink} size={24} /> : undefined}>
      <View testID="obstacle-sheet" style={{ paddingHorizontal: 20, paddingTop: 12, gap: 8 }}>
        {vm?.rows.map((r) => (
          <View key={r.label} style={{ flexDirection: "row", gap: 12 }}>
            <Text style={{ ...TYPE.small, color: palette.sub, width: 104 }}>{r.label}</Text>
            <Text style={{ ...TYPE.body, color: palette.ink, flex: 1 }}>{r.value}</Text>
          </View>
        ))}
        <Text style={{ ...TYPE.small, color: palette.sub, paddingTop: 6 }}>{vm?.note ?? ""}</Text>
        <Text style={{ ...TYPE.small, color: palette.faint }}>{vm?.licence ?? ""}</Text>
      </View>
    </Sheet>
  );
}
