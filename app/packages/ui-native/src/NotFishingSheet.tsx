/**
 * (T292) A TAPPED SEWAGE LAGOON, TAILINGS POND OR DUMP: what it is, and that it is not a fishing water — never its
 * regulations (it has none). The words are `@app/core` `notFishingSheet`'s, whole; this draws them. A bottom sheet at
 * every width, like the tidal and obstacle sheets.
 */
import { notFishingSheet, type NotFishingKind } from "@app/core";
import { Text, View } from "react-native";
import { Sheet } from "./Sheet";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function NotFishingSheet({ kind, palette, onClose }: {
  /** The tapped feature's kind; null: a tile with no kind (an older atlas); undefined: no sheet. */
  kind: NotFishingKind | null | undefined; palette: Palette; onClose: () => void;
}) {
  const vm = kind === undefined ? null : notFishingSheet(kind);
  return (
    <Sheet open={vm !== null} onClose={onClose} title={vm?.title ?? ""} sub={null} palette={palette}>
      <View testID="not-fishing-sheet" style={{ paddingHorizontal: 20, paddingTop: 12, gap: 8 }}>
        {vm?.body.map((p) => <Text key={p} style={{ ...TYPE.body, color: palette.ink }}>{p}</Text>)}
      </View>
    </Sheet>
  );
}
