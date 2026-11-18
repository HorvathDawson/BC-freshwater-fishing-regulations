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
import { NAV_RAIL } from "../NavRail";
import type { Palette } from "../theme";
import type { MapColouring } from "./useMapColouring";
import { usePainting, type HeldLookStore } from "@app/ui";

/**
 * (T856 round 2) THE LOOK OF THE SWITCH IN THE DESKTOP NAV RAIL, two to choose from, both drawn as part of the rail (its
 * width, its type, square, no card box): "A" two sub-items of Map (indented, a rule down their left edge, the chosen one
 * tinted with a heavier rule); "B" one segmented block, Trout | Salmon, the chosen half filled as the rail fills its tab.
 * On the web `?railLook=A|B` picks one, to shoot both from one build.
 */
const asked = typeof window !== "undefined" ? new URLSearchParams(window.location?.search ?? "").get("railLook") : null;
export const RAIL_LOOK: "A" | "B" = asked === "B" ? "B" : "A";

/** the rail's button size (MapRail `SIZE`) */
const SIZE = 44;
const HIT = hitProps({ w: SIZE, h: SIZE });

/**
 * (T856) WHERE THE SWITCH IS DRAWN, one rule for both homes: only on the Map tab, the day's regulations drawn and the
 * Salmon file offered, search not over it. On a desktop-width window it is in the left rail under the Map button
 * (`NavRail`'s `mapExtra`); anywhere narrower it is on the map's right rail (`MapRail` children). Never both.
 */
export function regsToggleShown(a: { tab: string; searching: boolean; col: Pick<MapColouring, "statusOk" | "layers" | "salmonOffered"> }): boolean {
  return a.tab === "map" && !a.searching && a.col.statusOk && a.col.layers.water.drawn === "regulations" && !!a.col.salmonOffered;
}

export function RegsLayerToggle({ col, palette, fishImages, look, inRail = false }: {
  /** (T856) in the desktop nav rail: centred under the Map button, no lift (the rail is the card) */
  inRail?: boolean;
  col: Pick<MapColouring, "regsLayer" | "setRegsLayer"> & Partial<Pick<MapColouring, "salmonLoading" | "salmonOffered">>;
  palette: Palette; fishImages?: FishImages;
  /** (T566) the key held until the map draws the chosen colouring (`HeldLookStore`): meanwhile the chosen one is busy */
  look?: HeldLookStore<unknown>;
}) {
  const painting = usePainting(look);
  const W = SALMON_WORDS.toggle;
  const target = useCoachTarget("modes");
  // (T261) Run timing is no choice here any more: it has its own tab, Runs
  const keys = (["standard", "salmon"] as const).filter((k) => k === "standard" || col.salmonOffered !== false);
  if (inRail) {
    const A = RAIL_LOOK === "A";
    return (
      <View ref={target} collapsable={false} testID="regs-paint" accessibilityLabel={W.label} accessibilityRole="radiogroup"
            style={{ width: NAV_RAIL.width, backgroundColor: palette.card, ...(A
              ? { paddingLeft: 8 } : { flexDirection: "row", borderTopWidth: 1, borderBottomWidth: 1, borderColor: palette.line2 }) }}>
        {keys.map((k, i) => {
          const on = col.regsLayer === k;
          const busy = (k === "salmon" && col.salmonLoading === true) || (on && painting);
          const art = fishImages?.[W.art[k]]?.[0]?.thumb;
          // A: quiet selected = the tint ground, ink type, a heavier ink rule; B: the rail's own selected fill (ink)
          const fg = A || !on ? (on ? palette.ink : palette.sub) : palette.card;
          const glyph = busy ? <LoadingState palette={palette} size={15} colour={fg} />
            : A && art ? <Image source={art} resizeMode="contain" accessibilityIgnoresInvertColors style={{ width: 20, height: 10 }} />
            : <Icon name="fish" colour={fg} size={16} />;
          return (
            <Pressable key={k} testID={`regs-paint-${k}`} onPress={() => col.setRegsLayer(k)} accessibilityRole="radio"
                       aria-checked={on} aria-busy={busy} accessibilityLabel={W.said[k]} accessibilityHint={W.hint}
                       {...hitProps({ w: A ? NAV_RAIL.width - 8 : NAV_RAIL.width / 2, h: A ? 40 : 56 })}
                       style={A
                         ? { flexDirection: "row", alignItems: "center", gap: 4, height: 40, paddingLeft: 8,
                             backgroundColor: on ? palette.tint : "transparent" }
                         : { flex: 1, flexDirection: "column", alignItems: "center", justifyContent: "center", gap: 3, height: 56,
                             backgroundColor: on ? palette.ink : "transparent",
                             borderLeftWidth: i ? 1 : 0, borderLeftColor: palette.line2 }}>
              {A && <View aria-hidden testID={`regs-paint-bar-${k}`}
                          style={{ position: "absolute", left: 0, top: 0, bottom: 0, width: on ? 3 : 1, backgroundColor: on ? palette.ink : palette.line2 }} />}
              {glyph}
              <Text numberOfLines={1} style={{ ...TYPE.tab, letterSpacing: 0.5, color: fg, fontWeight: on ? "800" : "600" }}>{W.rail[k]}</Text>
            </Pressable>
          );
        })}
      </View>
    );
  }
  return (
    <View ref={target} collapsable={false} testID="regs-paint" accessibilityLabel={W.label} accessibilityRole="radiogroup"
          style={{ backgroundColor: palette.card, borderWidth: 1, borderColor: palette.line2, borderRadius: inRail ? 0 : palette.r.box,
                   overflow: "hidden", ...(inRail ? { alignSelf: "center" as const, marginVertical: 8 } : palette.lift) }}>
      {keys.map((k, i) => {
        const on = col.regsLayer === k;
        const busy = (k === "salmon" && col.salmonLoading === true) || (on && painting);
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
