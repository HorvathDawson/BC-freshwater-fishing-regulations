/**
 * T34/T35 — a Gear / Methods / Licence section (prototype `sect()` → `.gpart`): a 1.5 px ink rule, the heading ("Gear",
 * "Methods", "Licence"; `.gpart > h3`, display face --t2), then the section's blocks. No logic.
 */
import type { ReactNode } from "react";
import { Text, View } from "react-native";
import { TYPE } from "../type";
import { GUTTER, SPACE, type Palette } from "../theme";

export function TabSection({ title, palette, children, testID }: { title: string; palette: Palette; children?: ReactNode; testID?: string }) {
  return (
    <View testID={testID} style={{ marginHorizontal: GUTTER, marginTop: SPACE.s4, paddingTop: SPACE.s4, paddingBottom: SPACE.s2, borderTopWidth: 1.5, borderTopColor: palette.ink }}>
      <Text accessibilityRole="header" style={[TYPE.sectionHead, { color: palette.ink, marginBottom: 10 }]}>{title}</Text>
      {children}
    </View>
  );
}

/** A cell of the Gear / Methods grid (`.gxc`, all `wide` on a phone): its small label (`.gxl`), then its body. */
export function Cell({ label, palette, children, testID }: { label: string; palette: Palette; children?: ReactNode; testID?: string }) {
  return (
    <View testID={testID} style={{ paddingVertical: 9, paddingRight: 8, borderBottomWidth: 1, borderBottomColor: palette.line2 }}>
      <Text style={[TYPE.metaBold, { lineHeight: 15.6, color: palette.sub }]}>{label}</Text>
      {children}
    </View>
  );
}
