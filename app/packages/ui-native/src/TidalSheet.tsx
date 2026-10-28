/**
 * (T166) A TAPPED TIDAL WATER AREA: why there is no freshwater answer here, and where the tidal rules are. The words are
 * `@app/core` `tidalSheet`'s, whole; this draws them. A bottom sheet at every width, like the obstacle sheet.
 */
import { tidalSheet } from "@app/core";
import { Text, View } from "react-native";
import { Sheet } from "./Sheet";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function TidalSheet({ open, palette, onClose }: { open: boolean; palette: Palette; onClose: () => void }) {
  const vm = open ? tidalSheet() : null;
  return (
    <Sheet open={vm !== null} onClose={onClose} title={vm?.title ?? ""} sub={null} palette={palette}>
      <View testID="tidal-sheet" style={{ paddingHorizontal: 20, paddingTop: 12, gap: 8 }}>
        {vm?.body.map((p) => <Text key={p} style={{ ...TYPE.body, color: palette.ink }}>{p}</Text>)}
      </View>
    </Sheet>
  );
}
