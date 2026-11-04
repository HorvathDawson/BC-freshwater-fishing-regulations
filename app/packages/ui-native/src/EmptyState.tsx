/**
 * (T260) THE EMPTY STATE AND THE "NONE" LINE — extracted unchanged from the copies the screens had typed out.
 *
 *   EmptyState  a page with nothing on it yet: a name-face title and one quiet line under it (the Spots tab's, first).
 *   NoneLine    "nothing here" inside a block: one quiet line in `sub` — small words in a list or form, body words where the
 *               block is a section of its own (a gauge that may not speak, "no change coming").
 *
 * `tools/one-button.test.ts` refuses the title-and-line typed out again.
 */
import { View, type TextStyle } from "react-native";
import { Text } from "./Text";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function EmptyState({ palette, title, body, testID }: { palette: Palette; title: string; body: string; testID?: string }) {
  return (
    <View testID={testID} style={{ paddingHorizontal: 20, paddingTop: 18, paddingBottom: 6, gap: 6 }}>
      <Text style={{ ...TYPE.name, fontSize: 18, color: palette.ink }}>{title}</Text>
      <Text style={{ ...TYPE.small, lineHeight: 19.5, color: palette.sub }}>{body}</Text>
    </View>
  );
}

export function NoneLine({ palette, children, size = "small", testID, style }: {
  palette: Palette; children: string; size?: "body" | "small"; testID?: string; style?: TextStyle;
}) {
  return <Text testID={testID} style={{ ...(size === "body" ? TYPE.body : TYPE.small), color: palette.sub, ...style }}>{children}</Text>;
}
