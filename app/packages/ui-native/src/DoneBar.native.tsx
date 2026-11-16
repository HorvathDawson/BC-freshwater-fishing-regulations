/**
 * (T723 G10) The phone's: see `DoneBar.d.ts`. iOS only, said here and not left to `InputAccessoryView` (which also
 * returns null off iOS): Android's pads keep their own Done key and get nothing from this.
 */
import type { ReactNode } from "react";
import { InputAccessoryView, Keyboard, Platform, View } from "react-native";
import { SHEET_WORDS } from "@app/core";
import { Button } from "./Button";
import { GUTTER, SPACE, type Palette } from "./theme";

export function DoneBar({ id, palette }: { id: string; palette: Palette }): ReactNode {
  if (Platform.OS !== "ios") return null;
  return (
    <InputAccessoryView nativeID={id}>
      <View style={{ flexDirection: "row", justifyContent: "flex-end", paddingHorizontal: GUTTER, paddingVertical: SPACE.s1,
                     backgroundColor: palette.card, borderTopWidth: 1, borderTopColor: palette.line }}>
        <Button palette={palette} kind="link" grow={false} label={SHEET_WORDS.done} testID={`${id}-done`}
                onPress={() => Keyboard.dismiss()} />
      </View>
    </InputAccessoryView>
  );
}
