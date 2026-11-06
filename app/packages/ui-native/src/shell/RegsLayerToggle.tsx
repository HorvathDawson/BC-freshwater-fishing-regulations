/**
 * (T211, T425) TROUT / SALMON — the Map tab's regulations colouring switch. T211 made it Standard | Salmon under Layers;
 * T213/T367 moved it beside the day, then to the head of the legend bar; (T425, owner 2026-11-05) it lives on the map's
 * RIGHT RAIL, under locate + layers: two stacked square buttons, each a fish (the app's own fish art, the ID pane's
 * thumbnail) over a small-caps word. A radiogroup of two radios; a press moves only the map's modes (`setRegsLayer`, the
 * one handler the legend's switch had): the salmon chains are already on every tile, so it is instant. (T182) On the web
 * the first press of Salmon reads the salmon file: the choice is busy until the map can paint it (T257: the fish turns in
 * its picture's place). Bait ban and fly only are no choice here: they are marks on Standard (`@app/map` `gearPaint`).
 * Same card look as the rail: 44 wide, square corners, the lift; the chosen one wears the rail's selected tint and an
 * accent bar. Absent the art (a test, a build without it) the app's `fish` icon stands in.
 */
import { Image, View } from "react-native";
import { SALMON_WORDS } from "@app/core";
import { LoadingState } from "../LoadingState";
import { Icon } from "../icons";
import { Text } from "../Text";
import { Pressable } from "../Pressable";
import { useCoachTarget } from "../CoachMarks";
import { hitProps } from "../regs/hit";
import type { FishImages } from "../regs/types";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import type { MapColouring } from "./useMapColouring";

/** the rail's button size (MapRail `SIZE`) */
const SIZE = 44;
const HIT = hitProps({ w: SIZE, h: SIZE });

export function RegsLayerToggle({ col, palette, fishImages }: {
  col: Pick<MapColouring, "regsLayer" | "setRegsLayer"> & Partial<Pick<MapColouring, "salmonLoading" | "salmonOffered">>;
  palette: Palette; fishImages?: FishImages;
}) {
  const W = SALMON_WORDS.toggle;
  const target = useCoachTarget("modes");
  // (T261) Run timing is no choice here any more: it has its own tab, Runs
  const keys = (["standard", "salmon"] as const).filter((k) => k === "standard" || col.salmonOffered !== false);
  return (
    <View ref={target} collapsable={false} testID="regs-paint" accessibilityLabel={W.label} accessibilityRole="radiogroup"
          style={{ backgroundColor: palette.card, borderWidth: 1, borderColor: palette.line2, borderRadius: palette.r.box,
                   overflow: "hidden", ...palette.lift }}>
      {keys.map((k, i) => {
        const on = col.regsLayer === k;
        const busy = k === "salmon" && col.salmonLoading === true;
        const art = fishImages?.[W.art[k]]?.[0]?.thumb;
        return (
          <Pressable key={k} testID={`regs-paint-${k}`} onPress={() => col.setRegsLayer(k)} accessibilityRole="radio"
                     aria-checked={on} aria-busy={busy} accessibilityLabel={W.said[k]} accessibilityHint={W.hint} {...HIT}
                     style={{ width: SIZE, height: SIZE, alignItems: "center", justifyContent: "center", gap: 1,
                              backgroundColor: on ? palette.tint : palette.card,
                              borderTopWidth: i ? 1 : 0, borderTopColor: palette.line }}>
            {busy ? <LoadingState palette={palette} size={15} colour={palette.ink} />
              : art ? <Image source={art} resizeMode="contain" accessibilityIgnoresInvertColors style={{ width: 32, height: 16 }} />
              : <Icon name="fish" colour={palette.ink} size={16} />}
            <Text style={{ ...TYPE.micro, fontSize: 9, lineHeight: 11, fontWeight: on ? "800" : "600", letterSpacing: 0.4,
                           textTransform: "uppercase", color: palette.ink }}>{W.rail[k]}</Text>
            {on && <View testID={`regs-paint-bar-${k}`} aria-hidden style={{ position: "absolute", left: 0, right: 0, bottom: 0,
                                                                              height: 3, backgroundColor: palette.accent }} />}
          </Pressable>
        );
      })}
    </View>
  );
}
