/**
 * One rule in a Sources sheet (prototype `.prule`): its title and who says it, as one button that opens the rule's own
 * sheet, then where it applies and its Rule book pages. No logic: every word is the VM's.
 */
import { Pressable, Text, View } from "react-native";
import type { Line, SheetKey } from "@app/core";
import { TYPE } from "../../type";
import type { Palette } from "../../theme";
import { LineText, type KeepActions } from "../LineText";

export function RuleRow({ title, level, applies, book, open, palette, actions }: {
  title: string; level: string; applies: readonly string[]; book: Line; open: SheetKey; palette: Palette; actions: KeepActions;
}) {
  return (
    <View style={{ paddingVertical: 8, borderTopWidth: 1, borderTopColor: palette.line }}>
      <Pressable accessibilityRole="button" accessibilityLabel={`${title} · ${level}`} onPress={() => actions.onSheet(open)} style={{ minHeight: 44 }}>
        <Text style={[TYPE.regsBodySemi, { color: palette.ink }]}>{title}</Text>
        <Text style={[TYPE.metaBold, { color: palette.sub }]}>{level}</Text>
      </Pressable>
      {applies.map((a, i) => <Text key={i} style={[TYPE.meta, { color: palette.sub }]}>{a}</Text>)}
      {book.length > 0 && <LineText line={book} style={TYPE.meta} color={palette.sub} palette={palette} actions={actions} />}
    </View>
  );
}
