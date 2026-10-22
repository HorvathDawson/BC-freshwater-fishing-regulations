/**
 * (T48) THE MORE TAB (owner 2026-10-22, Claude's recommendation): what the top-left hamburger's drawer held — Glossary,
 * Data sources & dates, Display, About, Report a problem, Contact — as the bottom bar's fourth tab. MOVED, not copied:
 * the drawer and its button are gone, so there is one list of these pages in the app. A page opens over the tab
 * (`MenuHost`'s `MenuPage`, a back layer), and back from it is this tab; back from the tab is the Map tab.
 *
 * Draws `menuVM()` and decides nothing.
 */
import { Pressable, ScrollView, Text, View } from "react-native";
import { menuVM, type MenuPageKey } from "@app/core";
import { TYPE } from "../type";
import { GUTTER, type Palette } from "../theme";

export function MoreScreen({ palette, onPick }: { palette: Palette; onPick: (page: MenuPageKey) => void }) {
  const vm = menuVM();
  return (
    <View testID="more-screen" style={{ flex: 1, backgroundColor: palette.card }}>
      <View style={{ paddingHorizontal: GUTTER, paddingTop: 16, paddingBottom: 12,
                     borderBottomWidth: 1.5, borderBottomColor: palette.ink }}>
        <Text accessibilityRole="header" style={{ ...TYPE.screen, color: palette.ink }}>{vm.title}</Text>
      </View>
      <ScrollView>
        {vm.items.map((it) => (
          <Pressable key={it.k} testID={`menu-item-${it.k}`} onPress={() => onPick(it.k)}
                     accessibilityRole="button" accessibilityLabel={it.label} accessibilityHint={it.hint}
                     style={({ pressed }) => ({ paddingVertical: 14, paddingHorizontal: GUTTER, gap: 2,
                                                borderBottomWidth: 1, borderBottomColor: palette.line,
                                                backgroundColor: pressed ? palette.wash : palette.card })}>
            <Text style={{ ...TYPE.bodyStrong, fontSize: 16, color: palette.ink }}>{it.label}</Text>
            <Text style={{ ...TYPE.small, color: palette.sub }}>{it.hint}</Text>
          </Pressable>
        ))}
      </ScrollView>
    </View>
  );
}
