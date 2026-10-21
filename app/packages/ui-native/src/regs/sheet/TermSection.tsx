/**
 * One glossary term inside a sheet (the multi-term "What these words mean" sheet's `section.gterm`, the hatch sheet's
 * `section.htw`): its title, then its body (drawn by SheetBody). No logic.
 */
import { Text, View } from "react-native";
import { TYPE } from "../../type";
import type { Palette } from "../../theme";

export function TermSection({ title, palette, children }: { title: string; palette: Palette; children: React.ReactNode }) {
  return (
    <View style={{ marginTop: 14, paddingTop: 8, borderTopWidth: 1, borderTopColor: palette.line }}>
      <Text accessibilityRole="header" style={[TYPE.regsBodyBold, { color: palette.ink }]}>{title}</Text>
      {children}
    </View>
  );
}
