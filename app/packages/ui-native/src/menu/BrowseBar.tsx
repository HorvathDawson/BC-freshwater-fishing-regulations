/**
 * A browse screen's title row with the menu at its left — the Search screen's own header form (its insets, the screen
 * face), for a screen that had no header of its own (Spots). With no menu above it (a test, a screen mounted alone) it
 * draws nothing, so the screen below is exactly as it was.
 */
import { useContext } from "react";
import { Text, View } from "react-native";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { MenuButton, MenuContext } from "./MenuButton";

export function BrowseBar({ palette, title }: { palette: Palette; title: string }) {
  if (!useContext(MenuContext)) return null;
  return (
    <View testID="browse-bar" style={{ flexDirection: "row", alignItems: "center", gap: 10, paddingHorizontal: 18,
                                       paddingTop: 16, paddingBottom: 12 }}>
      <MenuButton palette={palette} variant="bar" />
      <Text accessibilityRole="header" style={{ ...TYPE.screen, color: palette.ink }}>{title}</Text>
    </View>
  );
}
