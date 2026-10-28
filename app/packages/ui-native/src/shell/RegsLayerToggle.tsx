/**
 * (T211) STANDARD / SALMON — the Map tab's regulations switch (owner 2026-10-27). T213 (owner 2026-10-28: "weird in the
 * bar — near the layer icon would be better"): out of the legend strip and under the Layers button (`MapRail`'s
 * children), compact — one card the rail's shape, its two choices stacked, the chosen one filled. A press moves only the
 * map's modes: the salmon chains are already on every tile (`useSalmonColour`), so it is instant. Bait ban and fly only
 * are no choice here: they are marks on Standard (`@app/map` `gearPaint`). (T182, owner 2026-10-28) On the web the first
 * press of Salmon reads the salmon file: the choice reads "Loading…" (and is busy) until the map can paint it.
 */
import { SALMON_WORDS } from "@app/core";
import { Pressable, Text, View } from "react-native";
import { hitProps } from "../regs/hit";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import type { MapColouring } from "./useMapColouring";

/** a choice: the rail's width at least, 30 tall by its style, grown to 44 × 44 to the touch */
const CHOICE_HIT = hitProps({ w: 64, h: 30 });

export function RegsLayerToggle({ col, palette }: { col: Pick<MapColouring, "regsLayer" | "setRegsLayer"> &
                                                   Partial<Pick<MapColouring, "salmonLoading">>; palette: Palette }) {
  const W = SALMON_WORDS.toggle;
  return (
    <View testID="regs-paint-toggle" accessibilityRole="radiogroup" accessibilityLabel={W.label}
          style={{ backgroundColor: palette.card, borderWidth: 1, borderColor: palette.line2, borderRadius: palette.r.box,
                   overflow: "hidden", ...palette.lift }}>
      {(["standard", "salmon"] as const).map((k, i) => {
        const on = col.regsLayer === k;
        const busy = k === "salmon" && col.salmonLoading === true;
        return (
          <Pressable key={k} testID={`regs-paint-${k}`} onPress={() => col.setRegsLayer(k)}
                     accessibilityRole="radio" aria-checked={on} aria-busy={busy} accessibilityLabel={W[k]} {...CHOICE_HIT}
                     style={({ pressed }) => ({
                       minWidth: 64, height: 30, justifyContent: "center", alignItems: "center", paddingHorizontal: 8,
                       backgroundColor: on ? palette.ink : pressed ? palette.tint : palette.card,
                       borderTopWidth: i ? 1 : 0, borderTopColor: palette.line })}>
            <Text style={{ ...TYPE.small, fontSize: 11.5, fontWeight: "700", color: on ? palette.card : palette.ink }}>
              {busy ? W.loading : W[k]}
            </Text>
          </Pressable>
        );
      })}
    </View>
  );
}
