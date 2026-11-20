/**
 * (T723) The phone's: see `KeyboardBar.d.ts`. iOS only, said here and not left to `InputAccessoryView` (as `DoneBar`):
 * Android lifts the page's own row with the keyboard (T819) and gets nothing from this.
 */
import type { ReactNode } from "react";
import { InputAccessoryView, Platform, View } from "react-native";
import { GUTTER, SPACE, type Palette } from "./theme";

export function KeyboardBar({ id, palette, children }: { id: string; palette: Palette; children: ReactNode }): ReactNode {
  if (Platform.OS !== "ios") return null;
  return (
    <InputAccessoryView nativeID={id}>
      <View testID={id} style={{ flexDirection: "row", gap: SPACE.s2, paddingHorizontal: GUTTER, paddingVertical: SPACE.s2,
                                 backgroundColor: palette.card, borderTopWidth: 1, borderTopColor: palette.line }}>
        {children}
      </View>
    </InputAccessoryView>
  );
}
