/**
 * (T211, T425) TROUT / SALMON — the Map tab's regulations colouring switch. T211 made it Standard | Salmon under Layers;
 * T213/T367 moved it beside the day, then to the head of the legend bar; (T425, owner 2026-11-05) it lives on the map's
 * RIGHT RAIL, under locate + layers: two stacked square buttons, each a fish (the app's own fish art, the ID pane's
 * thumbnail) over a small-caps word. A radiogroup of two radios; a press moves only the map's modes (`setRegsLayer`, the
 * one handler the legend's switch had): the salmon chains are already on every tile, so it is instant. (T182) On the web
 * the first press of Salmon reads the salmon file: the choice is busy until the map can paint it (T257: the fish turns in
 * its picture's place). Bait ban and fly only are no choice here: they are marks on Standard (`@app/map` `gearPaint`).
 * Same card look as the rail: 44 wide, square corners, the lift; the chosen one is FILLED with the accent (T986: a bar on the
 * line between the two read as belonging to either) and its art/word turn to the on-accent ink. Absent the art (a test, a build without it) the app's `fish` icon stands in.
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
import { TYPE, LABEL_PROPS, LABEL_SCALE, labelMinWidth } from "../type";
import { useScaleUpTo } from "../textScale";
import { CornerStrip } from "../CornerStrip";
import type { Palette } from "../theme";
import type { MapColouring } from "./useMapColouring";
import { usePainting, type HeldLookStore } from "@app/ui";

/** the rail's button size (MapRail `SIZE`) */
const SIZE = 44;
const HIT = hitProps({ w: SIZE, h: SIZE });
/** the cell's word: 9 px caps (T425); its width at a larger text size sizes the cell (`labelMinWidth`) */
const LABEL = { fontSize: 9, lineHeight: 11, letterSpacing: 0.4 } as const;

/**
 * (T856) ON A DESKTOP-WIDTH WINDOW (from 1280) the switch is in the map's top row, directly after the day strip, drawn
 * with the same strip (`CornerStrip`: same height, border, lift, a filled chosen segment): Trout | Salmon, a fish and
 * the word. Same ids and the same handler as the rail's buttons; never both are drawn (`BrowseHost`).
 */
export function RegsLayerStrip({ col, palette, look }: {
  col: Pick<MapColouring, "regsLayer" | "setRegsLayer"> & Partial<Pick<MapColouring, "salmonLoading" | "salmonOffered">>;
  palette: Palette; look?: HeldLookStore<unknown>;
}) {
  const painting = usePainting(look);
  const W = SALMON_WORDS.toggle;
  const keys = (["standard", "salmon"] as const).filter((k) => k === "standard" || col.salmonOffered !== false);
  return (
    <CornerStrip palette={palette} testID="regs-paint" label={W.label} radio coach="modes"
                 segments={keys.map((k) => {
                   const on = col.regsLayer === k;
                   return { key: k, chip: W.rail[k], icon: "fish" as const, label: W.said[k], on,
                            busy: (k === "salmon" && col.salmonLoading === true) || (on && painting),
                            onPress: () => col.setRegsLayer(k) };
                 })} />
  );
}

export function RegsLayerToggle({ col, palette, fishImages, look }: {
  col: Pick<MapColouring, "regsLayer" | "setRegsLayer"> & Partial<Pick<MapColouring, "salmonLoading" | "salmonOffered">>;
  palette: Palette; fishImages?: FishImages;
  /** (T566) the key held until the map draws the chosen colouring (`HeldLookStore`): meanwhile the chosen one is busy */
  look?: HeldLookStore<unknown>;
}) {
  const painting = usePainting(look);
  const W = SALMON_WORDS.toggle;
  const target = useCoachTarget("modes");
  const scale = useScaleUpTo(LABEL_SCALE);
  // (T261) Run timing is no choice here any more: it has its own tab, Runs
  const keys = (["standard", "salmon"] as const).filter((k) => k === "standard" || col.salmonOffered !== false);
  return (
    <View ref={target} collapsable={false} testID="regs-paint" accessibilityLabel={W.label} accessibilityRole="radiogroup"
          style={{ backgroundColor: palette.card, borderWidth: 1, borderColor: palette.line2, borderRadius: palette.r.box,
                   overflow: "hidden", ...palette.lift }}>
      {keys.map((k, i) => {
        const on = col.regsLayer === k;
        const busy = (k === "salmon" && col.salmonLoading === true) || (on && painting);
        const ink = on ? palette.onAccent : palette.ink;
        const art = fishImages?.[W.art[k]]?.[0]?.thumb;
        return (
          <Pressable key={k} testID={`regs-paint-${k}`} onPress={() => col.setRegsLayer(k)} accessibilityRole="radio"
                     aria-checked={on} aria-busy={busy} accessibilityLabel={W.said[k]} accessibilityHint={W.hint} {...HIT}
                     // (T1201) exactly 44 square at the default text size (as in 0.9.0); at a larger one the cell GROWS (the rail is right-aligned, so it
                     // grows leftwards, both cells alike) before its word wraps or shrinks (type.ts, the one rule)
                     style={{ minWidth: scale > 1 ? Math.max(SIZE, ...keys.map((w) => labelMinWidth(W.rail[w], LABEL, scale, 2))) : SIZE, minHeight: SIZE, alignItems: "center", justifyContent: "center", gap: 1,
                              backgroundColor: on ? palette.accent : palette.card,
                              borderTopWidth: i ? 1 : 0, borderTopColor: palette.line }}>
            {busy ? <LoadingState palette={palette} size={15} colour={ink} />
              : art ? <Image source={art} resizeMode="contain" accessibilityIgnoresInvertColors style={{ width: 32, height: 16 }} />
              : <Icon name="fish" colour={ink} size={16} />}
            <Text {...LABEL_PROPS}
                  style={{ ...TYPE.micro, ...LABEL, fontWeight: on ? "800" : "600", textTransform: "uppercase", color: ink }}>{W.rail[k]}</Text>
          </Pressable>
        );
      })}
    </View>
  );
}
