/**
 * A disclosure closed by default (the prototype's `<details>`: "In the book’s words", "Technical detail", "Overruled (N)",
 * "Rules that don’t apply here (N)"). Its body is drawn only when opened. tokens.md §5 `Sheet · fold`: Archivo 700
 * 12.48 in the accent. No logic: the summary is the VM's.
 */
import { useState } from "react";
import { Pressable, Text, View } from "react-native";
import { TYPE } from "../../type";
import type { Palette } from "../../theme";

export function Fold({ summary, palette, children, testID }: { summary: string; palette: Palette; children: React.ReactNode; testID?: string }) {
  const [open, setOpen] = useState(false);
  return (
    <View testID={testID} style={{ marginTop: 8 }}>
      <Pressable accessibilityRole="button" aria-expanded={open} accessibilityLabel={summary} onPress={() => setOpen((o) => !o)}
                 style={{ minHeight: 44, justifyContent: "center" }}>
        <Text style={[TYPE.metaBold, { color: palette.accent }]}><Text aria-hidden>{open ? "▾ " : "▸ "}</Text>{summary}</Text>
      </Pressable>
      {open ? <View style={{ paddingBottom: 4 }}>{children}</View> : null}
    </View>
  );
}
