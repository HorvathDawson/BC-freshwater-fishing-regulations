/**
 * (T231) "UPDATED TO <date>": said for a few seconds when a downloaded release is put in use at a safe moment (the
 * reader on a map with nothing open, or back from the background), so what is on screen never changes unannounced.
 * A small card low over the map, above the tab bar; polite live region; tap to dismiss. The words are `@app/core`
 * `updatedToast`'s.
 */
import { useEffect } from "react";
import { Pressable, Text, View } from "react-native";
import { TYPE } from "./type";
import type { Palette } from "./theme";

/** How long the toast stays. */
export const TOAST_MS = 4000;

export function UpdateToast({ text, palette, onDone, bottom = 84 }: {
  /** the line, or null: no toast */
  text: string | null; palette: Palette; onDone: () => void;
  /** clear of the tab bar */
  bottom?: number;
}) {
  useEffect(() => {
    if (text === null) return;
    const t = setTimeout(onDone, TOAST_MS);
    return () => clearTimeout(t);
  }, [text, onDone]);
  if (text === null) return null;
  return (
    <Pressable testID="update-toast" onPress={onDone} accessibilityRole="alert" accessibilityLiveRegion="polite"
               accessibilityLabel={text}
               style={{ position: "absolute", left: 0, right: 0, bottom, alignItems: "center" }}>
      <View style={{ backgroundColor: palette.ink, paddingVertical: 10, paddingHorizontal: 16, borderRadius: palette.r.box,
                     ...palette.lift }}>
        <Text style={{ ...TYPE.bodyStrong, fontSize: 14, color: palette.card }}>{text}</Text>
      </View>
    </Pressable>
  );
}
