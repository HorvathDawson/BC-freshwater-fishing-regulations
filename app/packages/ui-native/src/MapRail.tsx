/**
 * THE MAP RAIL (owner 2026-10-21, after Fish Washington's side stack): the browse maps' own buttons, stacked on the
 * right — Layers (moved here from the bottom-left pill) and Spots. Drawn as riffle's `.tr .stack`
 * (`design/riffle.html`): one card column, a hairline between buttons, the lift; square, like everything here.
 *
 * Under MapLibre's zoom stack on the web (top-right, `@app/map` controls.css); at the top on a phone, where the map
 * has none. An icon alone is a guess, so every button says its name aloud; the words are `@app/core` `MAP_RAIL`.
 */
import { MAP_RAIL } from "@app/core";
import { Platform, Pressable, View } from "react-native";
import { Icon, LayersIcon } from "./icons";
import type { Palette } from "./theme";

/** One button of the stack: 40 px square (riffle's 36, raised for a thumb), reaching 48 with its slop. */
const SIZE = 40;
/** Clear of MapLibre's zoom stack on the web (two 28 px buttons from 10 px in); a phone's map draws none. */
export const RAIL_TOP = Platform.OS === "web" ? 84 : 14;
/** On the web its right edge lines up with that zoom stack's (MapLibre's 10 px control margin); riffle's 14 elsewhere. */
export const RAIL_RIGHT = Platform.OS === "web" ? 10 : 14;

export function MapRail({ palette, onLayers, onSpots }: {
  palette: Palette;
  /** Absent: no Layers button (the Conditions map decides its own colouring, so the sheet would control nothing). */
  onLayers?: () => void;
  /** Absent: no Spots button (spots cannot be read or saved here). */
  onSpots?: () => void;
}) {
  const buttons = [
    onLayers && { k: "layers", label: MAP_RAIL.layersLabel, onPress: onLayers,
                  icon: <LayersIcon colour={palette.ink} size={18} /> },
    onSpots && { k: "spots", label: MAP_RAIL.spotsLabel, onPress: onSpots,
                 icon: <Icon name="spots" colour={palette.ink} size={20} /> },
  ].filter((b): b is Exclude<typeof b, undefined> => !!b);
  if (buttons.length === 0) return null;
  return (
    <View accessibilityRole="toolbar" accessibilityLabel={MAP_RAIL.label}
          style={{ position: "absolute", right: RAIL_RIGHT, top: RAIL_TOP, backgroundColor: palette.card,
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
        </Pressable>
      ))}
    </View>
  );
}
