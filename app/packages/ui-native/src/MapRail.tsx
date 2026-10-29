/**
 * THE MAP RAIL (owner 2026-10-21, after Fish Washington's side stack): the browse maps' own buttons, stacked on the
 * right — Layers ALONE since T48 (owner 2026-10-22: Spots went back to the bottom bar as a tab; one place for each
 * kind of thing). A stack of one keeps the rail's shape, so a later map tool joins it without a new place. One card
 * column, a hairline between buttons, the lift; square, like everything here. Its icon is the app's one set (`icons.tsx`).
 *
 * Under MapLibre's zoom stack on the web (top-right, `@app/map` controls.css); at the top on a phone, where the map
 * has none. An icon alone is a guess, so every button says its name aloud; the words are `@app/core` `MAP_RAIL`.
 */
import type { ReactNode } from "react";
import { LAYER_WORDS, MAP_RAIL } from "@app/core";
import { Platform, Pressable, Text, View } from "react-native";
import { Icon } from "./icons";
import { TYPE } from "./type";
import type { Palette } from "./theme";

/** One button of the stack: 40 px square, reaching 48 with its slop (a thumb's target). */
const SIZE = 40;
/** (N1) the rail's icon: 20 px, the bar's size, so Layers and the tabs are one set at one weight. */
const RAIL_ICON = 20;
/**
 * How far under the map's top bar (`top`) the rail starts: clear of MapLibre's zoom stack on the web (two 28 px buttons
 * from 10 px in, then 16 px), 8 px on a phone, whose map draws none.
 */
export const RAIL_BELOW = Platform.OS === "web" ? 84 : 8;
/** On the web its right edge lines up with that zoom stack's (MapLibre's 10 px control margin); 14 elsewhere. */
export const RAIL_RIGHT = Platform.OS === "web" ? 10 : 14;

export function MapRail({ palette, onLayers, layersChanged = 0, top = 0, children }: {
  palette: Palette;
  /** (T48) the bottom edge of the bar over the map (the search field), in px: the rail starts `RAIL_BELOW` under it */
  top?: number;
  /**
   * How many layer choices differ from the defaults (`layersVM().changed`): a numbered badge on Layers, and said aloud
   * ("Choose layers, 2 changed") — after Done, the closed map still says it is not showing the defaults.
   */
  layersChanged?: number;
  /** Absent: no Layers button. Both browse maps pass it (owner 2026-10-22): each opens the sheet with its own entries. */
  onLayers?: () => void;
  /**
   * any further map switch, stacked under the rail, right-aligned with it. None today: Standard / Salmon moved to the
   * top-left corner beside the day strip (owner 2026-10-28).
   */
  children?: ReactNode;
}) {
  const buttons = [
    onLayers && { k: "layers", label: LAYER_WORDS.railLabel(layersChanged), onPress: onLayers,
                  icon: <Icon name="layers" colour={palette.ink} size={RAIL_ICON} />, badge: layersChanged },
  ].filter((b): b is Exclude<typeof b, undefined> => !!b);
  if (buttons.length === 0) return null;
  return (
    <View testID="map-rail-column" pointerEvents="box-none"
          style={{ position: "absolute", right: RAIL_RIGHT, top: top + RAIL_BELOW, alignItems: "flex-end", gap: 8 }}>
    <View accessibilityRole="toolbar" accessibilityLabel={MAP_RAIL.label}
          style={{ backgroundColor: palette.card,
                   borderWidth: 1, borderColor: palette.line2, borderRadius: palette.r.box, overflow: "hidden",
                   ...palette.lift }}>
      {buttons.map((b, i) => (
        <Pressable key={b.k} onPress={b.onPress} accessibilityRole="button" accessibilityLabel={b.label}
                   hitSlop={4}
                   style={({ pressed }) => ({
                     width: SIZE, height: SIZE, alignItems: "center", justifyContent: "center",
                     backgroundColor: pressed ? palette.tint : palette.card,
                     borderTopWidth: i === 0 ? 0 : 1, borderTopColor: palette.line,
                   })}>
          {b.icon}
          {b.badge > 0 && (
            <View testID={`rail-badge-${b.k}`} importantForAccessibility="no-hide-descendants" aria-hidden
                  style={{ position: "absolute", top: 3, right: 3, minWidth: 14, height: 14, paddingHorizontal: 3,
                           alignItems: "center", justifyContent: "center", backgroundColor: palette.accent }}>
              <Text style={{ ...TYPE.figure, fontSize: 9.5, lineHeight: 12, color: palette.onAccent }}>{b.badge}</Text>
            </View>
          )}
        </Pressable>
      ))}
    </View>
    {children}
    </View>
  );
}
