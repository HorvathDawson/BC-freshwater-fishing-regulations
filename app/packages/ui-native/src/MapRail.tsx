/**
 * THE MAP RAIL (owner 2026-10-21, after Fish Washington's side stack): the browse maps' own buttons, stacked on the
 * right — Layers ALONE since T48 (owner 2026-10-22: Spots went back to the bottom bar as a tab; one place for each
 * kind of thing). A stack of one keeps the rail's shape, so a later map tool joins it without a new place. One card
 * column, a hairline between buttons, the lift; square, like everything here. Its icon is the app's one set (`icons.tsx`).
 *
 * Under MapLibre's zoom stack on a wide web window (top-right, `@app/map` controls.css); at the top, level with the day
 * chip, on a phone, where the map has none (T1212). An icon alone is a guess, so every button says its name aloud; the words are `@app/core` `MAP_RAIL`.
 */
import type { ReactNode } from "react";
import { LAYER_WORDS, MAP_RAIL, MY_LOCATION } from "@app/core";
import { Platform, View } from "react-native";
import { Text } from "./Text";
import { Pressable } from "./Pressable";
import { Icon } from "./icons";
import { TYPE } from "./type";
import type { Palette } from "./theme";
import { hitProps } from "./regs/hit";
import { useCoachRefs } from "./CoachMarks";
import { useLayoutClass, type LayoutClass } from "@app/ui";
import { OpenSettings } from "./OpenSettings";

/** One button of the stack: 44 px square (T258, owner 2026-10-29: grown from 40 so the web reaches the 44 target as drawn). */
const SIZE = 44;
/** (N1) the rail's icon: 20 px, the bar's size, so Layers and the tabs are one set at one weight. */
const RAIL_ICON = 20;
/**
 * (T1212, owner 2026-11-26: "definitely drop +/-"; option A "Tidy") THE + / − ARE FOR WIDE WINDOWS ONLY. The web map draws
 * MapLibre's zoom stack on a tablet or desktop window (`MapProps.zoomButtons`); on a phone-wide window it draws none, as the
 * phone app's map never did. A phone zooms by pinch and double tap (`MAP_GESTURES`).
 */
export function zoomButtonsShown(layout: LayoutClass): boolean {
  return layout !== "phone";
}
/**
 * How far under the map's top bar (`top`) the rail starts: 8 px, level with the day chip in the left corner (both 44 px:
 * one line, T1212), unless the web's zoom stack is above it, when it starts clear of it (two 28 px buttons from 10 px in,
 * then 16 px: `RAIL_BELOW_ZOOM`).
 */
export const RAIL_BELOW = 8;
export const RAIL_BELOW_ZOOM = 84;
/** Its right edge: 14 px, the search field's edge; under the zoom stack, that stack's (MapLibre's 10 px control margin). */
export const RAIL_RIGHT = 14;
export const RAIL_RIGHT_ZOOM = 10;
/** is the web's zoom stack drawn above the rail? Only the web draws one, and only where `zoomButtonsShown` */
const ZOOM_STACK_HERE = Platform.select({ web: true, default: false });

export function MapRail({ palette, onLayers, layersChanged = 0, top = 0, me, children }: {
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
   * (T303) My location, above Layers (under the zoom): the dot is `on` once asked, the map `following` it; the line says why
   * it could not be shown. Absent: no button (no location on this platform).
   */
  me?: { on: boolean; following: boolean; busy: boolean; notice: string | null; offered?: boolean; press: () => void; dismiss: () => void } | null;
  /**
   * any further map switch, stacked under the rail, right-aligned with it: the Trout / Salmon colouring on the regulations
   * map (T425, owner 2026-11-05: back from the legend bar; `shell/RegsLayerToggle`).
   */
  children?: ReactNode;
}) {
  const buttons = [
    me && { k: "locate",
            label: me.following ? MY_LOCATION.labelFollowing : me.on ? MY_LOCATION.labelOn : MY_LOCATION.label,
            onPress: me.press,
            icon: <Icon name="locate" colour={me.on ? palette.accent : palette.ink} size={RAIL_ICON} />, badge: 0,
            selected: me.following },
    onLayers && { k: "layers", label: LAYER_WORDS.railLabel(layersChanged), onPress: onLayers,
                  icon: <Icon name="layers" colour={palette.ink} size={RAIL_ICON} />, badge: layersChanged },
  ].filter((b): b is Exclude<typeof b, undefined | null> => !!b);
  const coach = useCoachRefs();   // (T293) the walkthrough points at the layers button (`layers`)
  const underZoom = ZOOM_STACK_HERE && zoomButtonsShown(useLayoutClass());
  if (buttons.length === 0) return null;
  return (
    <View testID="map-rail-column" pointerEvents="box-none"
          style={{ position: "absolute", right: underZoom ? RAIL_RIGHT_ZOOM : RAIL_RIGHT, top: top + (underZoom ? RAIL_BELOW_ZOOM : RAIL_BELOW),
                   alignItems: "flex-end", gap: 8 }}>
    <View accessibilityRole="toolbar" accessibilityLabel={MAP_RAIL.label}
          style={{ backgroundColor: palette.card,
                   borderWidth: 1, borderColor: palette.line2, borderRadius: palette.r.box, overflow: "hidden",
                   ...palette.lift }}>
      {buttons.map((b, i) => (
        <Pressable {...hitProps({ w: SIZE, h: SIZE })} key={b.k} ref={coach(b.k)} onPress={b.onPress} accessibilityRole="button" accessibilityLabel={b.label}
                   aria-pressed={"selected" in b ? b.selected : undefined}
                   style={({ pressed }) => ({
                     width: SIZE, height: SIZE, alignItems: "center", justifyContent: "center",
                     backgroundColor: pressed || ("selected" in b && b.selected) ? palette.tint : palette.card,
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
    {me?.offered && !me.on && !me.busy && !me.notice && (
      // (T982) the open's location ask was dismissed or refused: one line, a card, a tap asks again (refused for good, it says how)
      <Pressable testID="my-location-offer" onPress={me.press} accessibilityRole="button" accessibilityLabel={MY_LOCATION.offer}
                 {...hitProps({ w: 44, h: 44 })}
                 style={{ flexDirection: "row", alignItems: "center", gap: 6, paddingVertical: 10, paddingHorizontal: 10,
                          backgroundColor: palette.card, borderWidth: 1, borderColor: palette.line2,
                          borderRadius: palette.r.box, ...palette.lift }}>
        <Icon name="locate" colour={palette.accent} size={16} />
        <Text style={{ ...TYPE.small, fontWeight: "600", color: palette.ink }}>{MY_LOCATION.offer}</Text>
      </Pressable>
    )}
    {me?.notice && (
      // why it is not shown, and how to turn it on; a tap on the line puts it away, the button retries
      <Pressable testID="my-location-notice" onPress={me.dismiss} accessibilityRole="alert"
                 style={{ maxWidth: 220, padding: 8, backgroundColor: palette.card, borderWidth: 1, borderColor: palette.line2,
                          borderRadius: palette.r.box, ...palette.lift }}>
        <Text style={{ ...TYPE.small, color: palette.ink }}>{me.notice}</Text>
      </Pressable>
    )}
    <OpenSettings palette={palette} line={me?.notice} inset={0} />
    {children}
    </View>
  );
}
