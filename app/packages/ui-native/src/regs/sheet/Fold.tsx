/**
 * A disclosure closed by default (the prototype's `<details>`: "In the book’s words", "Technical detail", "Overruled (N)",
 * "Rules that don’t apply here (N)"). Its body is drawn only when opened. tokens.md §5 `Sheet · fold`: Archivo 700
 * 12.48 in the accent. No logic: the summary is the VM's.
 */
import { useState } from "react";
import { View } from "react-native";
import { IconText } from "../../icons";
import { TYPE } from "../../type";
import type { Palette } from "../../theme";
import { RegsPress } from "../RegsPress";

export function Fold({ summary, palette, children, testID }: { summary: string; palette: Palette; children: React.ReactNode; testID?: string }) {
  const [open, setOpen] = useState(false);
  return (
    <View testID={testID} style={{ marginTop: 8 }}>
      <RegsPress accessibilityRole="button" aria-expanded={open} accessibilityLabel={summary} onPress={() => setOpen((o) => !o)} box={{ w: 44, h: 44 }}
                 style={{ minHeight: 44, justifyContent: "center" }}>
        {/* (T60) the fold's mark is the set's expand / collapse (the icon is decorative; the button says `summary`) */}
        <IconText name={open ? "collapse" : "expand"} text={summary} size={14} style={{ ...TYPE.metaBold, color: palette.accent }} />
      </RegsPress>
      {open ? <View style={{ paddingBottom: 4 }}>{children}</View> : null}
    </View>
  );
}
