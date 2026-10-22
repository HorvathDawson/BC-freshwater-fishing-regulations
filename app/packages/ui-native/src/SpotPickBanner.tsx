/**
 * PICK ON THE MAP (owner 2026-10-22): the slim banner over the Map tab while a spot's place is being picked — what to
 * do ("Tap where your spot is") and Cancel. The map under it pans and zooms as usual; its first tap places the
 * provisional pin and raises the drop sheet (the Shell, `@app/ui` `useMapRail`). It stands where the search field
 * does, which it replaces while it is up.
 *
 * A BACK LAYER (G1): while it is up, back — Android's, the browser's, the iOS edge — and Escape on the web leave pick
 * mode, one step, with nothing made; the step after that is the route's own.
 */
import { useEffect } from "react";
import { Platform, Pressable, Text, View, useWindowDimensions } from "react-native";
import { SPOT_PICK } from "@app/core";
import { useBackLayer } from "@app/ui";
import { TOP_BAR, searchPanelWidth } from "./TopBar";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function SpotPickBanner({ palette, top = TOP_BAR.top, onCancel }: {
  palette: Palette;
  /** Where its top edge goes: the search field's, by default. */
  top?: number;
  /** Cancel, back, Escape: pick mode ends and nothing is made. */
  onCancel: () => void;
}) {
  const requestClose = useBackLayer(true, onCancel);
  // Escape is a back on the web (a sheet's Modal hears it itself; this banner is no Modal)
  useEffect(() => {
    if (Platform.OS !== "web" || typeof window === "undefined") return;
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") requestClose(); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [requestClose]);
  const panel = searchPanelWidth(useWindowDimensions().width);
  return (
    <View testID="spot-pick-banner" pointerEvents="box-none"
          style={{ position: "absolute", top, left: TOP_BAR.side,
                   ...(panel === null ? { right: TOP_BAR.side } : { width: panel }) }}>
      <View style={{ minHeight: TOP_BAR.field, flexDirection: "row", alignItems: "center", gap: 10, paddingLeft: 14,
                     backgroundColor: palette.card, borderWidth: 1.5, borderColor: palette.accent,
                     borderRadius: palette.r.box, ...palette.lift }}>
        <Text accessibilityLiveRegion="polite" style={{ ...TYPE.bodyStrong, flex: 1, color: palette.ink }}>
          {SPOT_PICK.banner}
        </Text>
        <Pressable onPress={onCancel} accessibilityRole="button" accessibilityLabel={SPOT_PICK.cancel} hitSlop={4}
                   style={({ pressed }) => ({ alignSelf: "stretch", justifyContent: "center", paddingHorizontal: 14,
                                              minWidth: 44, opacity: pressed ? 0.6 : 1 })}>
          <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{SPOT_PICK.cancel}</Text>
        </Pressable>
      </View>
    </View>
  );
}
