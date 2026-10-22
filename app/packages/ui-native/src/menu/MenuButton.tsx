/**
 * THE HAMBURGER (owner 2026-10-21): top-left on every browse screen, it opens the menu drawer. It reads the Shell's menu
 * from `MenuContext`, so a screen drops it in with one line and passes nothing down; with no menu above it (a screen
 * mounted alone, a test) it draws nothing.
 *
 *   float   over the map: the map's own pill (Chrome `Pill`), beside the chips that share the corner
 *   bar     in a screen's header row (Search, Spots): the bare glyph at the header's size
 */
import { createContext, useContext } from "react";
import { Pressable, View } from "react-native";
import { MENU_WORDS } from "@app/core";
import type { MenuControls } from "@app/ui";
import { Pill } from "../Chrome";
import type { Palette } from "../theme";

export const MenuContext = createContext<MenuControls | null>(null);

/**
 * Three square-ended rules, like the tab icons' butt caps (icons.tsx). Drawn with Views rather than react-native-svg, so
 * the button mounts in the render tests (RENDER-TESTS.md: an svg component cannot).
 */
export function MenuIcon({ colour, size = 18 }: { colour: string; size?: number }) {
  const bar = { height: Math.max(2, Math.round(size / 9)), backgroundColor: colour };
  return (
    <View style={{ width: size, height: size, justifyContent: "space-between", paddingVertical: Math.round(size * 0.17),
                   paddingHorizontal: Math.round(size * 0.08) }}>
      <View style={bar} /><View style={bar} /><View style={bar} />
    </View>
  );
}

export function MenuButton({ palette, variant = "float" }: { palette: Palette; variant?: "float" | "bar" }) {
  const menu = useContext(MenuContext);
  if (!menu) return null;
  if (variant === "float")
    return (
      <View testID="menu-button">
        <Pill palette={palette} onPress={menu.open} label={MENU_WORDS.open} style={{ paddingHorizontal: 10 }}>
          <MenuIcon colour={palette.ink} size={17} />
        </Pill>
      </View>
    );
  return (
    <Pressable testID="menu-button" onPress={menu.open} accessibilityRole="button" accessibilityLabel={MENU_WORDS.open}
               hitSlop={10} style={{ minWidth: 32, minHeight: 32, justifyContent: "center" }}>
      <MenuIcon colour={palette.ink} size={22} />
    </Pressable>
  );
}
